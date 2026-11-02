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

`check.sh` failed three items here, all expected: the socket (0600) and store
(0700) modes, because these images predate the change on this branch that
makes them 0660 and 0710, and the exposure of 3.5, because the test unit drops
the root-only items above (the production unit scores 1.6,
[systemd-analyze-security.md](systemd-analyze-security.md)). The branch image
below passes the mode checks; test runs set `SPARROW_MAX_EXPOSURE=3.5`.

## The branch image, 2026-11-02

Image `ba6ebe7` (schema 63), installed by `make install-bin` into `./bin`, on
the same test store. `check.sh` passes every item. The rollback that follows
went to the schema 62 image, which could not open the store the schema 63
image had just upgraded; the unit then failed in a restart loop until its
start limit:

```text
$ deploy/start.sh --rev ba6ebe7
start.sh: starting sparrow-deploytest.service on sparrowd-schema63-ba6ebe7
receipt: unit=sparrow-deploytest.service state=active pid=4066128
receipt: daemon=sparrowd-schema63-ba6ebe7 cli=sparrow-schema63-ba6ebe7 previous=sparrowd-schema62-cfdf2fa
receipt: store_id=store:18e49e9635f5ccaa99e1a7791e2fc638 schema=63 pressure=ready durability_failure=None watches=1
[exit 0]

$ cp TEST/data/probe.jsonl TEST/data/intake/deploytest-moved/probe-4.jsonl
[exit 0]

$ timeout 120 bash -c while [ -e 'TEST/data/intake/deploytest-moved/probe-4.jsonl' ]; do sleep 1; done; echo consumed
consumed
[exit 0]

$ stat -c %n %a %G TEST/data/store TEST/data/store/rpc.sock
TEST/data/store 710 paudley
TEST/data/store/rpc.sock 660 paudley
[exit 0]

$ env SPARROW_MAX_EXPOSURE=3.5 deploy/check.sh
PASS  sparrow-deploytest.service active, pid 4066128, restarts 0
PASS  daemon image sparrowd-schema63-ba6ebe7 (rollback: sparrowd-schema62-cfdf2fa)
PASS  socket TEST/data/store/rpc.sock is mode 0660, group paudley
PASS  store TEST/data/store is mode 0710, group paudley
PASS  pressure phase ready
PASS  no durability failure
PASS  store store:18e49e9635f5ccaa99e1a7791e2fc638, schema 63
PASS  watch deploytest: TEST/data/intake/deploytest-moved, 0 pending claim(s)
PASS  systemd-analyze security exposure 3.5 (at most 3.5)
[exit 0]

$ deploy/restart.sh --rollback
restart.sh: rolling back to sparrowd-schema62-cfdf2fa; an image cannot undo a store-format upgrade its successor made
restart.sh: sparrow-deploytest.service stopped through its drain (pid 4066128); socket removed
restart.sh: starting sparrow-deploytest.service on sparrowd-schema62-cfdf2fa
● sparrow-deploytest.service - Sparrow deploy test instance (deploy/test-instance.sh)
     Loaded: loaded (/run/user/1000/systemd/user/sparrow-deploytest.service; static)
     Active: activating (auto-restart) (Result: exit-code) since Mon 2026-11-02 02:02:26 CST; 380ms ago
 Invocation: 831eeaa8169747a2ac0b3e3d550ecf33
       Docs: file:///usr/local/share/doc/sparrow/deploy/README.md
    Process: 4068819 ExecStart=TEST/root/usr/local/libexec/sparrow/sparrowd --config TEST/root/etc/sparrow/server.toml (code=exited, status=1/FAILURE)
   Main PID: 4068819 (code=exited, status=1/FAILURE)
   Mem peak: 97.9M
        CPU: 671ms
restart.sh: sparrow-deploytest.service is not active
[exit 1]
```

The daemon's journal for the refused rollback:

```text
sparrowd[4068819]: Error: Error { code: "STORAGE_FAILURE", message: "Invalid argument: Column families not opened: rdf_reverse" }
```

Three fixes followed, in `deploy/lib.sh`, `start.sh` and `restart.sh`: they
refuse an image whose schema number is lower than the current image's before
stopping anything; they refuse to start a unit that hit its start limit and
name the reset command; and `stop.sh` stops a unit caught between restarts
instead of reporting it inactive. The same instance afterwards:

```text
$ deploy/start.sh --rev ba6ebe7
start.sh: sparrow-deploytest.service hit its start limit; diagnose it (RECOVERY.md, "Unit start-limit hit"), then run: systemctl --user reset-failed sparrow-deploytest.service
[exit 1]

$ systemctl --user reset-failed sparrow-deploytest.service
[exit 0]

$ deploy/start.sh --rev ba6ebe7
start.sh: starting sparrow-deploytest.service on sparrowd-schema63-ba6ebe7
receipt: unit=sparrow-deploytest.service state=active pid=4090833
receipt: daemon=sparrowd-schema63-ba6ebe7 cli=sparrow-schema63-ba6ebe7 previous=sparrowd-schema62-cfdf2fa
receipt: store_id=store:18e49e9635f5ccaa99e1a7791e2fc638 schema=63 pressure=ready durability_failure=None watches=1
[exit 0]

$ deploy/restart.sh --rollback
restart.sh: rolling back to sparrowd-schema62-cfdf2fa; an image cannot undo a store-format upgrade its successor made
restart.sh: sparrowd-schema62-cfdf2fa is schema 62, older than the current sparrowd-schema63-ba6ebe7; it cannot open a store that image upgraded. Fix forward, or restore the backup taken before the upgrade (RECOVERY.md, "Rollback")
[exit 1]

$ env SPARROW_BIN=$PWD/.stage/test-bin deploy/restart.sh --rev cfdf2fa
restart.sh: sparrowd-schema62-cfdf2fa is schema 62, older than the current sparrowd-schema63-ba6ebe7; it cannot open a store that image upgraded. Fix forward, or restore the backup taken before the upgrade (RECOVERY.md, "Rollback")
[exit 1]

$ env SPARROW_MAX_EXPOSURE=3.5 deploy/check.sh
PASS  sparrow-deploytest.service active, pid 4090833, restarts 0
PASS  daemon image sparrowd-schema63-ba6ebe7 (rollback: sparrowd-schema62-cfdf2fa)
PASS  socket TEST/data/store/rpc.sock is mode 0660, group paudley
PASS  store TEST/data/store is mode 0710, group paudley
PASS  pressure phase ready
PASS  no durability failure
PASS  store store:18e49e9635f5ccaa99e1a7791e2fc638, schema 63
PASS  watch deploytest: TEST/data/intake/deploytest-moved, 0 pending claim(s)
PASS  systemd-analyze security exposure 3.5 (at most 3.5)
[exit 0]

$ deploy/stop.sh
stop.sh: sparrow-deploytest.service stopped through its drain (pid 4090833); socket removed
[exit 0]
```

Before this run, a daemon left from the AppArmor trace (started by hand on the
same store) was still running. The unit's daemon then failed with `IO error:
While lock file: TEST/data/store/LOCK: Resource temporarily unavailable`: the
store's exclusive lock refused a second writer, as designed. The traced daemon
was stopped with SIGINT and removed its socket.

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

The rehearsal ([rehearsal.md](rehearsal.md), gap G1) exercised `stop.sh` on a
unit caught between restarts: `stop.sh: sparrow-deploytest.service was
activating (no drain: the daemon was not serving); stopped`.

## Rollback across a schema change, 2026-11-02

The review of 2026-11-02 (R2) asked for a way back across a schema change. On a
fresh test instance (`R4`, images from both revisions), started on the schema
62 image: `sparrow-backup` took a backup through the unit, and its `SOURCE`
records the image that last opened the store with the store's identity,
schema and format, and keeps the image pair; `restart.sh` refused to raise the
schema until the named `pre-upgrade-IMAGE` copy existed, then upgraded; after
a further ingest, the store was replaced by the backup's and
`start.sh --restored-from` started exactly the schema 62 image on it, with the
same store identity, although the links named the schema 63 image. The test
instance's links were reset to none before the first start (`setup` points
them at the newest image), as on a host that runs schema 62 today. The
backup ran from the worktree with the test instance's environment
(`SPARROW_SCOPE`, `SPARROW_UNIT`, `SPARROW_PREFIX`, `RUNTIME_DIRECTORY`):

```text
$ deploy/start.sh --rev cfdf2fa
start.sh: starting sparrow-deploytest.service on sparrowd-schema62-cfdf2fa
receipt: unit=sparrow-deploytest.service state=active pid=1752772
receipt: daemon=sparrowd-schema62-cfdf2fa cli=sparrow-schema62-cfdf2fa previous=none
receipt: store_id=store:f5397155e72d7d906ba0028422b23973 schema=62 pressure=ready durability_failure=None watches=1
[exit 0]

$ drop before-upgrade
consumed before-upgrade
[exit 0]

$ backup
sparrow-backup: R4/backups/20261102T161210Z complete; 4.8s service downtime; kept 1 backup(s)
[exit 0]

$ grep -E ^(image|store_id|schema_version|store_format)= R4/backups/20261102T161210Z//SOURCE
image=sparrowd-schema62-cfdf2fa
store_id=store:f5397155e72d7d906ba0028422b23973
schema_version=62
store_format=sparrow.store/79
[exit 0]

$ ls R4/backups/20261102T161210Z//images
sparrow-schema62-cfdf2fa
sparrowd-schema62-cfdf2fa
[exit 0]

$ deploy/restart.sh --rev ba6ebe7
restart.sh: sparrowd-schema63-ba6ebe7 raises the schema from 62 to 63; take the named pre-upgrade backup R4/backups/pre-upgrade-sparrowd-schema62-cfdf2fa first (README.md, "Upgrades and rollback")
[exit 1]

$ cp -a --reflink=always R4/backups/20261102T161210Z/ R4/backups/pre-upgrade-sparrowd-schema62-cfdf2fa
[exit 0]

$ deploy/restart.sh --rev ba6ebe7
restart.sh: pre-upgrade backup R4/backups/pre-upgrade-sparrowd-schema62-cfdf2fa holds the store as sparrowd-schema62-cfdf2fa left it
restart.sh: sparrow-deploytest.service stopped through its drain (pid 1755944); socket removed
restart.sh: starting sparrow-deploytest.service on sparrowd-schema63-ba6ebe7
receipt: unit=sparrow-deploytest.service state=active pid=1756969
receipt: daemon=sparrowd-schema63-ba6ebe7 cli=sparrow-schema63-ba6ebe7 previous=sparrowd-schema62-cfdf2fa
receipt: store_id=store:f5397155e72d7d906ba0028422b23973 schema=63 pressure=ready durability_failure=None watches=1
[exit 0]

$ drop after-upgrade
consumed after-upgrade
[exit 0]

$ deploy/restart.sh --rollback
restart.sh: rolling back to sparrowd-schema62-cfdf2fa; an image cannot undo a store-format upgrade its successor made
restart.sh: sparrowd-schema62-cfdf2fa is schema 62, older than the current sparrowd-schema63-ba6ebe7; it cannot open a store that image upgraded. Fix forward, or restore the pre-upgrade backup (RECOVERY.md, "Rollback")
[exit 1]

$ deploy/start.sh
start.sh: sparrow-deploytest.service already runs sparrowd-schema63-ba6ebe7
receipt: unit=sparrow-deploytest.service state=active pid=1756969
receipt: daemon=sparrowd-schema63-ba6ebe7 cli=sparrow-schema63-ba6ebe7 previous=sparrowd-schema62-cfdf2fa
receipt: store_id=store:f5397155e72d7d906ba0028422b23973 schema=63 pressure=ready durability_failure=None watches=1
[exit 0]

$ deploy/stop.sh
stop.sh: sparrow-deploytest.service stopped through its drain (pid 1756969); socket removed
[exit 0]

$ mv R4/data/store R4/data/store.upgraded
[exit 0]

$ cp -a --reflink=always R4/backups/pre-upgrade-sparrowd-schema62-cfdf2fa/store R4/data/store
[exit 0]

$ deploy/start.sh --restored-from R4/backups/pre-upgrade-sparrowd-schema62-cfdf2fa
start.sh: restored store: starting sparrowd-schema62-cfdf2fa, the image that last opened it (R4/backups/pre-upgrade-sparrowd-schema62-cfdf2fa)
start.sh: starting sparrow-deploytest.service on sparrowd-schema62-cfdf2fa
receipt: unit=sparrow-deploytest.service state=active pid=1758446
receipt: daemon=sparrowd-schema62-cfdf2fa cli=sparrow-schema62-cfdf2fa previous=sparrowd-schema63-ba6ebe7
receipt: store_id=store:f5397155e72d7d906ba0028422b23973 schema=62 pressure=ready durability_failure=None watches=1
[exit 0]

$ deploy/start.sh --restored-from R4/backups/pre-upgrade-sparrowd-schema62-cfdf2fa
start.sh: sparrow-deploytest.service is running; stop it before starting a restored store
[exit 1]

$ deploy/stop.sh
stop.sh: sparrow-deploytest.service stopped through its drain (pid 1758446); socket removed
[exit 0]

$ deploy/test-instance.sh teardown R4
test-instance.sh: sparrow-deploytest.service removed; R4 kept
[exit 0]
```

A second instance (`R5`) after the backup also began keeping the lexicon
bundle; the same sequence passed, and the backup holds:

```text
$ ls R5/backups/20261102T161543Z/ R5/backups/20261102T161543Z//images R5/backups/20261102T161543Z//lexicons
R5/backups/20261102T161543Z/:
SOURCE
config
images
intake
lexicons
store

R5/backups/20261102T161543Z//images:
sparrow-schema62-cfdf2fa
sparrowd-schema62-cfdf2fa

R5/backups/20261102T161543Z//lexicons:
lexicons-a47940e1f130bf3739c6d59aaf89643209905ea4
[exit 0]
```

## Install preconditions and the user-scope guard, 2026-11-02

`install.sh` now checks, before it changes anything, that the slice exists
when enabling and that the operator's old user unit is neither enabled nor
running (review R3). The check is read-only; run as the operator against this
host, where the old user unit runs and the slice is not installed yet, it
reports all three, so `sudo deploy/install.sh` would refuse and change nothing.
That refusal itself needs root and was not run. The lifecycle scripts refuse
`SPARROW_SCOPE=user` without a test unit name (review O10); the refusal comes
from `lib.sh` as it is sourced, before any `systemctl` call:

```text
$ bash -c source deploy/lib.sh; operators
paudley
[exit 0]

$ bash -c source deploy/lib.sh; old_user_unit_problems paudley
enabled (/home/paudley/.config/systemd/user/default.target.wants/sparrow.service)
active
[exit 0]

$ systemctl show -p FragmentPath --value lillith.slice

[exit 0]

$ env SPARROW_SCOPE=user deploy/stop.sh
stop.sh: SPARROW_SCOPE=user needs SPARROW_UNIT naming a test unit (deploy/test-instance.sh env), never sparrow.service
[exit 1]

$ env SPARROW_SCOPE=user SPARROW_UNIT=sparrow.service deploy/check.sh
check.sh: SPARROW_SCOPE=user needs SPARROW_UNIT naming a test unit (deploy/test-instance.sh env), never sparrow.service
[exit 1]

$ deploy/install.sh
install.sh: run as root (sudo deploy/install.sh), or set SPARROW_DESTDIR to stage into a directory
[exit 1]
```

## Not run

- `install.sh`'s refusal and its partial-change report as root, and the
  backup as the root system unit.
- Everything that needs root: the system unit, the user and group, the slice,
  priority, the credential, the bind on 518 and AppArmor enforcement. The
  acceptance steps of issue #22, the cutover's tracking issue, list those steps.
