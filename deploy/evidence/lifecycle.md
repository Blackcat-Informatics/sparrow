<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Lifecycle on a test instance (specification F, items 2 and 3)

Run 2026-11-01 by `deploy/test-instance.sh`, which builds a scratch prefix and
data directory under `.stage/test-instance` in the worktree and a user unit
`sparrow-deploytest.service` with the production unit's sandbox (minus the
items an unprivileged manager cannot apply: the user and group, the slice,
negative nice and OOM values, the bind capability, the credential and the
AppArmor profile). Its configuration has a private Unix socket and no TCP
listener. The live `sparrow.service` was not touched. The scripts are the
production ones, pointed at the instance with:

```bash
eval "$(SPARROW_BIN=$PWD/.stage/test-bin deploy/test-instance.sh env)"
```

`TEST` below stands for `.stage/test-instance` under the worktree. The images
are two earlier release builds of this repository, `cfdf2fa` and `f1ecdda`,
both store schema 62.

## Transcript

```text

$ deploy/start.sh --rev cfdf2fa
start.sh: starting sparrow-deploytest.service on sparrowd-schema62-cfdf2fa
receipt: unit=sparrow-deploytest.service state=active pid=2555533
receipt: daemon=sparrowd-schema62-cfdf2fa cli=sparrow-schema62-cfdf2fa previous=sparrowd-schema62-f1ecdda
receipt: store_id=store:18e49e9635f5ccaa99e1a7791e2fc638 schema=62 pressure=ready durability_failure=None watches=1
[exit 0]

$ cp TEST/data/probe.jsonl TEST/data/intake/deploytest/probe-1.jsonl
[exit 0]

$ timeout 120 bash -c while [ -e TEST/data/intake/deploytest/probe-1.jsonl ]; do sleep 1; done; echo consumed
consumed
[exit 0]

$ deploy/check.sh
PASS  sparrow-deploytest.service active, pid 2555533, restarts 0
PASS  daemon image sparrowd-schema62-cfdf2fa (rollback: sparrowd-schema62-f1ecdda)
FAIL  socket TEST/data/store/rpc.sock: socket 600 paudley
FAIL  store TEST/data/store: 700 paudley
PASS  pressure phase ready
PASS  no durability failure
PASS  store store:18e49e9635f5ccaa99e1a7791e2fc638, schema 62
PASS  watch deploytest: TEST/data/intake/deploytest, 0 pending claim(s)
FAIL  systemd-analyze security exposure 3.5 exceeds 2.0
[exit 1]

$ sparrow TEST/data/store/rpc.sock watch put repoint-1793585624 1 TEST/data/watch-moved.toml
{"request_id":"repoint-1793585624","watch":{"store_id":"store:18e49e9635f5ccaa99e1a7791e2fc638","revision":"2","enabled":true,"definition":{"id":"deploytest","root":"TEST/data/intake/deploytest-moved","source":"deploytest","format":"jsonl","quiet_ms":1000,"max_directories":1,"max_candidates":64,"recursive":true,"mode":"consume","readiness":"close_or_idle","producer_contract":"finished_files"}},"durability":"durable"}
[exit 0]

$ sparrow TEST/data/store/rpc.sock status | python3 -c 'import json,sys; [print(w["id"], w["revision"], w["root"]) for w in json.load(sys.stdin)["consume_watches"]]'
deploytest 2 TEST/data/intake/deploytest-moved
[exit 0]

$ timeout 120 bash -c while [ -e TEST/data/intake/deploytest-moved/probe-2.jsonl ]; do sleep 1; done; echo consumed-from-moved-root
consumed-from-moved-root
[exit 0]

$ deploy/stop.sh
stop.sh: sparrow-deploytest.service stopped through its drain (pid 2555533); socket removed
[exit 0]

$ ls TEST/data/store/rpc.sock
ls: cannot access 'TEST/data/store/rpc.sock': No such file or directory
[exit 2]

$ deploy/start.sh
start.sh: starting sparrow-deploytest.service on sparrowd-schema62-f1ecdda
receipt: unit=sparrow-deploytest.service state=active pid=2892371
receipt: daemon=sparrowd-schema62-f1ecdda cli=sparrow-schema62-f1ecdda previous=sparrowd-schema62-cfdf2fa
receipt: store_id=store:18e49e9635f5ccaa99e1a7791e2fc638 schema=62 pressure=ready durability_failure=None watches=1
[exit 0]

$ deploy/restart.sh --rollback
restart.sh: rolling back to sparrowd-schema62-cfdf2fa; an image cannot undo a store-format upgrade its successor made
restart.sh: sparrow-deploytest.service stopped through its drain (pid 2892371); socket removed
restart.sh: starting sparrow-deploytest.service on sparrowd-schema62-cfdf2fa
receipt: unit=sparrow-deploytest.service state=active pid=2938271
receipt: daemon=sparrowd-schema62-cfdf2fa cli=sparrow-schema62-cfdf2fa previous=sparrowd-schema62-f1ecdda
receipt: store_id=store:18e49e9635f5ccaa99e1a7791e2fc638 schema=62 pressure=ready durability_failure=None watches=1
[exit 0]

$ deploy/restart.sh --rollback
restart.sh: rolling back to sparrowd-schema62-f1ecdda; an image cannot undo a store-format upgrade its successor made
restart.sh: sparrow-deploytest.service stopped through its drain (pid 2938271); socket removed
restart.sh: starting sparrow-deploytest.service on sparrowd-schema62-f1ecdda
receipt: unit=sparrow-deploytest.service state=active pid=2960787
receipt: daemon=sparrowd-schema62-f1ecdda cli=sparrow-schema62-f1ecdda previous=sparrowd-schema62-cfdf2fa
receipt: store_id=store:18e49e9635f5ccaa99e1a7791e2fc638 schema=62 pressure=ready durability_failure=None watches=1
[exit 0]

$ deploy/restart.sh --rev cfdf2fa
restart.sh: sparrow-deploytest.service stopped through its drain (pid 2960787); socket removed
restart.sh: starting sparrow-deploytest.service on sparrowd-schema62-cfdf2fa
receipt: unit=sparrow-deploytest.service state=active pid=2981922
receipt: daemon=sparrowd-schema62-cfdf2fa cli=sparrow-schema62-cfdf2fa previous=sparrowd-schema62-f1ecdda
receipt: store_id=store:18e49e9635f5ccaa99e1a7791e2fc638 schema=62 pressure=ready durability_failure=None watches=1
[exit 0]

$ deploy/start.sh
start.sh: sparrow-deploytest.service already runs sparrowd-schema62-cfdf2fa; use restart.sh
[exit 1]

$ deploy/start.sh --rev cfdf2fa
start.sh: sparrow-deploytest.service already runs sparrowd-schema62-cfdf2fa
receipt: unit=sparrow-deploytest.service state=active pid=2981922
receipt: daemon=sparrowd-schema62-cfdf2fa cli=sparrow-schema62-cfdf2fa previous=sparrowd-schema62-f1ecdda
receipt: store_id=store:18e49e9635f5ccaa99e1a7791e2fc638 schema=62 pressure=ready durability_failure=None watches=1
[exit 0]

$ env -u SPARROW_PREFIX -u SPARROW_CONFIG SPARROW_BIN=$PWD/.stage/test-bin SPARROW_DESTDIR=TEST/root deploy/install.sh --no-enable
install.sh: staged, not run: systemd-sysusers sparrow.conf
install.sh: staged, not run: systemd-tmpfiles --create sparrow.conf
install.sh: kept existing /etc/sparrow/server.toml
install.sh: kept existing /etc/sparrow/ship.toml
install.sh: kept image links (sparrowd -> sparrowd-schema62-cfdf2fa); start.sh or restart.sh switches images
install.sh: nothing changed
[exit 0]
```

What it shows:

- `start.sh --rev` starts a named image and prints the receipt: the unit
  state, both images, the store identity and schema, pressure, durability and
  the watch count.
- A consume watch claims, ingests and removes a dropped file under the
  sandbox. `watch put` re-points the root (revision 1 to 2) and consumption
  continues from the new root without a restart.
- `stop.sh` stops through the daemon's SIGINT drain and confirms the socket is
  gone. `start.sh` without `--rev` picks the newest image (`f1ecdda`).
- `restart.sh --rollback` swaps the current and previous images in both
  directions, with the store identity unchanged throughout. `restart.sh --rev`
  moves to a named image.
- `start.sh` refuses to start a different image over a running one and is
  idempotent for the same one.
- `install.sh` re-run against the staged root (`SPARROW_DESTDIR`) keeps the
  existing configuration and image links and reports nothing changed.

`check.sh` failed three items, all expected on this instance:

- The socket (0600) and store (0700) modes: these images predate the change on
  this branch that makes them 0660 and 0710. The branch image is the one the
  check is written for. It was not run here, because release links on this
  host failed while the work was finished (see "Not run" below).
- The exposure of 3.5: the test unit drops the items above, which the
  production unit sets. The production unit scores 1.6
  ([systemd-analyze-security.md](systemd-analyze-security.md)). Test runs that
  want a passing check set `SPARROW_MAX_EXPOSURE`.

## RestrictSUIDSGID refuses openat2

With the production unit's `RestrictSUIDSGID=yes`, the test instance's first
consume failed with `completed consumption requires Linux openat2; no weaker
path-resolution fallback is admitted`, the daemon's refusal when `openat2`
returns ENOSYS (`crates/sparrow-server/src/source_file.rs`). systemd's
seccomp rule for that setting cannot read `openat2`'s mode, which lives behind
a pointer, so it refuses the call outright. The same result outside Sparrow,
with a probe that calls `openat2` with `RESOLVE_NO_SYMLINKS` in a transient
unit with and without the setting:

```text
$ systemd-run --user --wait --pipe --quiet -p NoNewPrivileges=yes -p RestrictSUIDSGID=yes python3 -I -c PROBE
openat2 failed: Function not implemented (38)
[exit 0]
$ systemd-run --user --wait --pipe --quiet -p NoNewPrivileges=yes -p RestrictSUIDSGID=no python3 -I -c PROBE
openat2 ok
[exit 0]
```

`PROBE` is this one-line program (`os.O_RDONLY`, mode 0, `RESOLVE_NO_SYMLINKS`;
syscall 437 is `openat2`, -100 is `AT_FDCWD`):

```bash
PROBE='import ctypes,os,struct; libc=ctypes.CDLL(None,use_errno=True); how=struct.pack("QQQ",os.O_RDONLY,0,0x04); r=libc.syscall(437,-100,b"/etc/hostname",how,len(how)); e=ctypes.get_errno(); print("openat2 ok" if r>=0 else "openat2 failed: "+os.strerror(e)+" ("+str(e)+")")'
```

The production unit therefore omits the setting;
[RECOVERY.md](../RECOVERY.md#exposure) records why that is safe.

## Not run

- The branch image on the test instance, and `check.sh` passing its mode
  checks. Release links on this host failed with SIGBUS in the linker while
  this was written: the `/opt` filesystem that holds every build directory had
  no unallocated space and its metadata was full, so writes to the linker's
  output mapping failed. The same SIGBUS hit other repositories' builds.
- Everything that needs root: the system unit, the user and group, the slice,
  priority, the credential, the bind on 518 and AppArmor enforcement. The
  tracking issue's UAT lists those steps.
