"""Exercise the release consumer's real TTY detection."""
import errno
import os
from pathlib import Path
import subprocess

for no_color in (False, True):
    env = dict(os.environ)
    env.pop("NO_COLOR", None)
    if no_color:
        env["NO_COLOR"] = "1"
    master, slave = os.openpty()
    try:
        result = subprocess.run(
            ["target/release/examples/console"], stdout=subprocess.PIPE, stderr=slave, env=env, check=True
        )
    finally:
        os.close(slave)
    captured = bytearray()
    try:
        while True:
            try:
                data = os.read(master, 8192)
            except OSError as error:
                if error.errno != errno.EIO:
                    raise
                break
            if not data:
                break
            captured.extend(data)
    finally:
        os.close(master)
    assert not result.stdout
    assert (b"\x1b[" in captured) != no_color
    for message in (b"Reading 12 files", b"Import complete", b"Upload deferred", b"Connection refused"):
        assert captured.count(message) == 1
    print(f"terminal NO_COLOR={no_color} exit={result.returncode} bytes={len(captured)} checked=yes")
