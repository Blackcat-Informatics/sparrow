"""Exercise one fatal RocksDB WAL append in an owned Sparrow process."""

import argparse
import json
import os
from pathlib import Path
import selectors
import signal
import socket
import subprocess
import sys
import tempfile
import time

SEED = b"confirmed bytes before a fatal WAL append\n"
PROBE = b"retry the same request after the open\n"


def tracing_parent(binary: Path, config: Path, directory: Path) -> None:
    # Preserve the daemon's actual parent PID when replacing this process with strace.
    environment = os.environ | {"NOTIFY_SOCKET": str(directory / "notify")}
    with (directory / "daemon.log").open("wb") as log:
        daemon = subprocess.Popen(
            [binary, "--config", config, "--no-intake"],
            stdin=subprocess.DEVNULL, stdout=log, stderr=log, env=environment,
        )
    try:
        print(daemon.pid, flush=True)
        wal = Path(sys.stdin.readline().rstrip("\n"))
        if wal.parent != directory / "store" or wal.suffix != ".log":
            raise RuntimeError(f"WAL outside the owned store: {wal}")
        os.execvp("strace", [
            "strace", "-f", "-yy", "-p", str(daemon.pid), "-P", str(wal),
            "-e", "trace=write", "-e", "inject=write:error=EIO:when=1",
            "-o", str(directory / "trace.log"),
        ])
    finally:
        daemon.send_signal(signal.SIGINT)
        try:
            daemon.wait(timeout=20)
        except subprocess.TimeoutExpired:
            daemon.kill()
            daemon.wait()


def event(source, child: int | None, deadline: float, reason: str) -> None:
    with selectors.DefaultSelector() as selector:
        selector.register(source, selectors.EVENT_READ, False)
        if child is not None:
            selector.register(child, selectors.EVENT_READ, True)
        ready = selector.select(max(0, deadline - time.monotonic()))
    if any(key.data for key, _ in ready):
        raise RuntimeError(f"daemon exited while {reason}")
    if not ready:
        raise TimeoutError(reason)


def line(stream, child: int | None, deadline: float, reason: str) -> str:
    data = bytearray()
    while not data.endswith(b"\n"):
        event(stream, child, deadline, reason)
        byte = os.read(stream.fileno(), 1)
        if not byte:
            raise RuntimeError(f"unexpected EOF while {reason}: {data!r}")
        data.extend(byte)
    return data.decode().strip()


def await_serving(notify: socket.socket, child: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        event(notify, child, deadline, "waiting for Sparrow to serve")
        message = notify.recv(4096).decode()
        print(message.replace("\n", " "), flush=True)
        if "STATUS=serving" in message.splitlines():
            return


def current_wal(pid: int, store: Path) -> Path:
    candidates = set()
    for descriptor in Path(f"/proc/{pid}/fd").iterdir():
        try:
            path = descriptor.readlink()
            first = Path(f"/proc/{pid}/fdinfo/{descriptor.name}").read_text()
        except FileNotFoundError:
            continue
        if path.parent == store and path.suffix == ".log":
            flags = next(row.split()[1] for row in first.splitlines() if row.startswith("flags:"))
            if (int(flags, 8) & os.O_ACCMODE) in (os.O_WRONLY, os.O_RDWR):
                candidates.add(path)
    if len(candidates) != 1:
        raise RuntimeError(f"expected one writable WAL, found {sorted(candidates)}")
    return candidates.pop()


def command(binary: Path, endpoint: Path, *args, failure: str | None = None):
    result = subprocess.run(
        [binary, endpoint, *args], capture_output=True, text=True, timeout=20, check=False,
    )
    if failure is not None:
        if result.returncode == 0 or failure not in result.stderr:
            raise RuntimeError(f"expected {failure} for {args}, got {result}")
        print(result.stderr.strip(), flush=True)
        return None
    if result.returncode != 0:
        raise RuntimeError(f"{args}: {result.stderr.strip()}")
    return json.loads(result.stdout)


def verify_injection(directory: Path, wal: Path) -> None:
    trace = (directory / "trace.log").read_text()
    injected = [entry for entry in trace.splitlines() if "INJECTED" in entry]
    if len(injected) != 1 or f"<{wal}>" not in injected[0] or "EIO" not in injected[0]:
        raise RuntimeError(f"expected one EIO on the owned WAL: {injected}")
    print(injected[0], flush=True)


def verify_stopped(directory: Path) -> None:
    # RocksDB can buffer its operator log until it closes the stopped handle.
    paths = (directory / "store").glob("LOG*")
    stopped = [
        row for path in paths for row in path.read_text(errors="replace").splitlines()
        if "auto_recovery=0" in row and "stop=1" in row
    ]
    if not stopped:
        raise RuntimeError("RocksDB did not log the fatal stopped-handle state")
    print(stopped[0], flush=True)


def exercise(args) -> None:
    directory = Path(tempfile.mkdtemp(prefix="eio-", dir=Path(__file__).resolve().parent))
    print(f"EVIDENCE={directory}", flush=True)
    store = directory / "store"
    endpoint = store / "rpc.sock"
    config = directory / "server.toml"
    config.write_text(
        f"socket_path = {json.dumps(str(endpoint))}\n"
        f"data_dir = {json.dumps(str(store))}\n"
        f"text_lexicon_dir = {json.dumps(str(args.lexicons))}\n"
        "disk_budget = 1073741824\nmaintenance_reserve = 8388608\n"
        "min_free_bytes = 1048576\nmax_records = 10000\nmax_pin_members = 100\n"
        "max_concurrent_requests = 4\nbuffered_flush_interval_ms = 1000\n"
    )
    seed, probe, export = (directory / name for name in ("seed.log", "probe.log", "readback.log"))
    seed.write_bytes(SEED)
    probe.write_bytes(PROBE)
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as notify:
        notify.bind(str(directory / "notify"))
        with subprocess.Popen(
            [sys.executable, "-I", __file__, "--tracing-parent", args.sparrowd, config, directory],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        ) as tracer:
            pid = int(line(tracer.stdout, None, time.monotonic() + 10, "starting the owned daemon"))
            child = os.pidfd_open(pid)
            try:
                await_serving(notify, child, 30)
                original_socket = endpoint.stat().st_ino
                accepted = command(args.sparrow, endpoint, "ingest", "eio-seed", "fault", "log", seed)
                original = command(args.sparrow, endpoint, "status")
                assert accepted["durability"] == "durable", accepted
                wal = current_wal(pid, store)
                tracer.stdin.write(os.fsencode(wal) + b"\n")
                tracer.stdin.flush()
                message = line(tracer.stderr, child, time.monotonic() + 10, "attaching WAL tracer")
                if "attached" not in message or str(pid) not in message:
                    raise RuntimeError(message)
                request = ("ingest", "eio-probe", "fault", "log", probe)
                command(args.sparrow, endpoint, *request, failure="STORAGE_PRESSURE")
                latched = command(args.sparrow, endpoint, "status")
                assert latched["durability"]["failure"], latched
                verify_injection(directory, wal)
                if not args.fault_only:
                    await_serving(notify, child, 190)
                    assert endpoint.stat().st_ino == original_socket
                    resumed = command(args.sparrow, endpoint, "status")
                    assert resumed["store_id"] == original["store_id"], resumed
                    before, after = original["durability"], resumed["durability"]
                    assert after["session_id"] != before["session_id"] and not after["failure"], after
                    store_id = original["store_id"]
                    command(args.sparrow, endpoint, "flush", store_id, before["session_id"],
                            before["durable_through"], failure="STALE_WATERMARK")
                    command(args.sparrow, endpoint, "bytes", store_id, accepted["artifact_id"],
                            "0", str(len(SEED)), export)
                    assert export.read_bytes() == SEED
                    retry = command(args.sparrow, endpoint, *request)
                    assert retry["durability"] == "durable", retry
                    print(f"pid={pid} socket={original_socket} store={store_id} "
                          f"bytes={len(SEED)} new_session={after['session_id']}", flush=True)
            finally:
                tracer.stdin.close()
                try:
                    signal.pidfd_send_signal(child, signal.SIGINT)
                except ProcessLookupError:
                    pass
                try:
                    tracer.wait(timeout=25)
                except subprocess.TimeoutExpired:
                    try:
                        signal.pidfd_send_signal(child, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    try:
                        tracer.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        tracer.kill()
                        tracer.wait()
                        raise
                finally:
                    os.close(child)
        verify_stopped(directory)
        verify_injection(directory, wal)


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "--tracing-parent":
        tracing_parent(*(Path(arg) for arg in sys.argv[2:]))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sparrowd", type=lambda value: Path(value).resolve(strict=True))
    parser.add_argument("sparrow", type=lambda value: Path(value).resolve(strict=True))
    parser.add_argument("lexicons", type=lambda value: Path(value).resolve(strict=True))
    parser.add_argument("--fault-only", action="store_true", help="check the fault on a baseline daemon")
    exercise(parser.parse_args())
