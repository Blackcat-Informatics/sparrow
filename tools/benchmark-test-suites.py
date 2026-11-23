# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

"""Compare the three longest release suites using existing Cargo test binaries."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import sys
from time import monotonic


SUITES = (
    ("crates/sparrow-core/Cargo.toml", "storage"),
    ("crates/sparrow-server/tests/Cargo.toml", "rpc"),
    ("crates/sparrow-server/Cargo.toml", "sparrow_server"),
)


def read_suites(cargo_messages, workspace):
    expected = {workspace / path: name for path, name in SUITES}
    suites = {}
    with cargo_messages.open() as messages:
        for line in messages:
            message = json.loads(line)
            if message["reason"] != "compiler-artifact" or not message["profile"]["test"]:
                continue
            manifest = Path(message["manifest_path"])
            if expected.get(manifest) != message["target"]["name"]:
                continue
            if not message["fresh"] or not message["executable"] or manifest in suites:
                raise SystemExit(f"expected one fresh test executable for {manifest}")
            suites[manifest] = Path(message["executable"])
    if suites.keys() != expected.keys():
        raise SystemExit(f"missing test executables for {sorted(expected.keys() - suites.keys())}")
    return [(name, suites[manifest], manifest.parent) for manifest, name in expected.items()]


def run_suite(suite, log_dir, mode, threads):
    name, executable, directory = suite
    log = log_dir / f"{mode}-{name}.log"
    started = monotonic()
    with log.open("w") as output:
        result = subprocess.run(
            [executable, f"--test-threads={threads}"],
            cwd=directory,
            stdout=output,
            stderr=subprocess.STDOUT,
            check=False,
        )
    if result.returncode:
        print(f"{mode} {name} failed:\n" + "\n".join(log.read_text().splitlines()[-30:]), file=sys.stderr)
    return {"suite": name, "seconds": round(monotonic() - started, 2), "exit": result.returncode}


def measure(mode, suites, log_dir):
    cpus = len(os.sched_getaffinity(0))
    workers = 1 if mode == "serial" else min(cpus, len(suites))
    threads = max(1, cpus // workers)
    started = monotonic()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(lambda suite: run_suite(suite, log_dir, mode, threads), suites))
    print(json.dumps({
        "mode": mode,
        "wall_seconds": round(monotonic() - started, 2),
        "runner_cpus": cpus,
        "suite_threads": threads,
        "suites": results,
    }), flush=True)
    if any(result["exit"] for result in results):
        raise SystemExit(f"suite failed in {mode} mode, see {log_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cargo_messages", type=Path)
    parser.add_argument("log_dir", type=Path)
    parser.add_argument("--parallel-first", action="store_true")
    args = parser.parse_args()
    suites = read_suites(args.cargo_messages, Path.cwd())
    args.log_dir.mkdir(parents=True, exist_ok=True)
    order = ("parallel", "serial") if args.parallel_first else ("serial", "parallel")
    for mode in order:
        measure(mode, suites, args.log_dir)
