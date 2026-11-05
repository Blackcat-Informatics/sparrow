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

## Round-2 review fixes, 2026-11-02

A fresh instance `R6` from `.stage/combo-bin` (the schema 62 image `cfdf2fa`
and the schema 63 image `ba6ebe7`), driven only through
`sparrow-deploytest.service`. `check.sh` results are omitted: they match the
earlier runs (the schema 62 image keeps 0700 and 0600; the test unit's exposure
is 3.5). The sequence covers four things:

- `pre-upgrade-backup.sh`: it succeeds once, then refuses the existing name.
- The guard's age bound, with `SPARROW_PRE_UPGRADE_MAX_HOURS=0`.
- A restore of the pre-upgrade backup. After it, the guard refuses that same
  copy, because it predates the restore's start. Once it is retired, a new
  backup lets the upgrade through.
- A staged `uninstall.sh` of the test root, run twice.

The daemon's `live_records` was 1 before the upgrade, 2 after the
`after-upgrade` record, and 1 again after the restore. So the restored store
does not hold the record ingested after the backup (review P3).

```text
$ drop before-upgrade
consumed before-upgrade
[exit 0]

## N4: take it; a second run is refused
$ deploy/pre-upgrade-backup.sh
pre-upgrade-backup.sh: backing up the store as sparrowd-schema62-cfdf2fa left it (started 20261102T164539Z)
sparrow-backup: R6/backups/20261102T164539Z complete; 4.3s service downtime; kept 1 backup(s)
pre-upgrade-backup.sh: pre-upgrade backup R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa:
  taken_utc=20261102T164539Z
  image=sparrowd-schema62-cfdf2fa
  store_id=store:d1ee69672ce02902ecf77cd44adbe6e9
  schema_version=62
  store_format=sparrow.store/79
[exit 0]

$ deploy/pre-upgrade-backup.sh
pre-upgrade-backup.sh: R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa already exists (taken_utc=20261102T164539Z); a new pre-upgrade backup never reuses it. Retire it first: mv -T R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa.retired-20261102T164544Z
[exit 1]

## N4: the age bound
$ env SPARROW_PRE_UPGRADE_MAX_HOURS=0 deploy/restart.sh --rev ba6ebe7
restart.sh: R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa was taken at 20261102T164539Z, more than 0 hours ago; retire it (mv -T R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa.retired-20261102T164539Z) and take a new one: sudo deploy/pre-upgrade-backup.sh
[exit 1]

## upgrade
$ deploy/restart.sh --rev ba6ebe7
restart.sh: pre-upgrade backup R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa (taken 20261102T164539Z) holds the store as sparrowd-schema62-cfdf2fa left it
restart.sh: sparrow-deploytest.service stopped through its drain (pid 3430117); socket removed
restart.sh: starting sparrow-deploytest.service on sparrowd-schema63-ba6ebe7
receipt: unit=sparrow-deploytest.service state=active pid=3430948
receipt: daemon=sparrowd-schema63-ba6ebe7 cli=sparrow-schema63-ba6ebe7 previous=sparrowd-schema62-cfdf2fa
receipt: store_id=store:d1ee69672ce02902ecf77cd44adbe6e9 schema=63 pressure=ready durability_failure=None watches=1
[exit 0]

$ drop after-upgrade
consumed after-upgrade
[exit 0]

## restore the pre-upgrade backup
$ deploy/stop.sh
stop.sh: sparrow-deploytest.service stopped through its drain (pid 3430948); socket removed
[exit 0]

$ mv R6/data/store R6/data/store.upgraded
[exit 0]

$ cp -a --reflink=always R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa/store R6/data/store
[exit 0]

$ deploy/start.sh --restored-from R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa
start.sh: restored store: starting sparrowd-schema62-cfdf2fa, the image that last opened it (R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa)
start.sh: starting sparrow-deploytest.service on sparrowd-schema62-cfdf2fa
receipt: unit=sparrow-deploytest.service state=active pid=3431714
receipt: daemon=sparrowd-schema62-cfdf2fa cli=sparrow-schema62-cfdf2fa previous=sparrowd-schema63-ba6ebe7
receipt: store_id=store:d1ee69672ce02902ecf77cd44adbe6e9 schema=62 pressure=ready durability_failure=None watches=1
[exit 0]

## N4: the old pre-upgrade backup predates the restore's start
$ deploy/restart.sh --rev ba6ebe7
restart.sh: R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa was taken at 20261102T164539Z, before sparrowd-schema62-cfdf2fa was last started (20261102T164554Z); it may predate a restore. Retire it (mv -T R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa.retired-20261102T164539Z) and take a new one: sudo deploy/pre-upgrade-backup.sh
[exit 1]

$ deploy/pre-upgrade-backup.sh
pre-upgrade-backup.sh: R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa already exists (taken_utc=20261102T164539Z); a new pre-upgrade backup never reuses it. Retire it first: mv -T R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa.retired-20261102T164603Z
[exit 1]

$ mv -T R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa.retired-20261102T164539Z
[exit 0]

$ deploy/pre-upgrade-backup.sh
pre-upgrade-backup.sh: backing up the store as sparrowd-schema62-cfdf2fa left it (started 20261102T164603Z)
sparrow-backup: R6/backups/20261102T164603Z complete; 29.9s service downtime; kept 1 backup(s)
pre-upgrade-backup.sh: pre-upgrade backup R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa:
  taken_utc=20261102T164603Z
  image=sparrowd-schema62-cfdf2fa
  store_id=store:d1ee69672ce02902ecf77cd44adbe6e9
  schema_version=62
  store_format=sparrow.store/79
[exit 0]

$ deploy/restart.sh --rev ba6ebe7
restart.sh: pre-upgrade backup R6/backups/pre-upgrade-sparrowd-schema62-cfdf2fa (taken 20261102T164603Z) holds the store as sparrowd-schema62-cfdf2fa left it
restart.sh: sparrow-deploytest.service stopped through its drain (pid 3441725); socket removed
restart.sh: starting sparrow-deploytest.service on sparrowd-schema63-ba6ebe7
receipt: unit=sparrow-deploytest.service state=active pid=3443165
receipt: daemon=sparrowd-schema63-ba6ebe7 cli=sparrow-schema63-ba6ebe7 previous=sparrowd-schema62-cfdf2fa
receipt: store_id=store:d1ee69672ce02902ecf77cd44adbe6e9 schema=63 pressure=ready durability_failure=None watches=1
[exit 0]

$ deploy/stop.sh
stop.sh: sparrow-deploytest.service stopped through its drain (pid 3443165); socket removed
[exit 0]

$ ls R6/backups
pre-upgrade-sparrowd-schema62-cfdf2fa
pre-upgrade-sparrowd-schema62-cfdf2fa.retired-20261102T164539Z
[exit 0]

$ deploy/test-instance.sh teardown R6
test-instance.sh: sparrow-deploytest.service removed; R6 kept
[exit 0]

## N2: staged uninstall of the test root
$ find R6/root/etc R6/root/usr/local/bin ( -type f -o -type l )
R6/root/etc/sparrow/server.toml
R6/root/etc/sparrow/ship.toml
R6/root/etc/sysusers.d/sparrow.conf
R6/root/etc/tmpfiles.d/sparrow.conf
R6/root/etc/systemd/system/sparrow-backup.service
R6/root/etc/systemd/system/sparrow-ship.service
R6/root/etc/systemd/system/sparrow.service
R6/root/etc/systemd/system/sparrow-backup.timer
R6/root/etc/systemd/system/sparrow-ship.timer
R6/root/etc/apparmor.d/sparrow
R6/root/usr/local/bin/sparrow
[exit 0]

$ env -u SPARROW_PREFIX -u SPARROW_CONFIG SPARROW_DESTDIR=R6/root deploy/uninstall.sh
uninstall.sh: staging: the units' states and the loaded profile are not checked
uninstall.sh: staged, not run: systemctl disable sparrow.service sparrow-ship.timer sparrow-backup.timer
uninstall.sh: staged, not run: systemctl daemon-reload
uninstall.sh: staged, not run: chown paudley:root /SPARROW
uninstall.sh: staged, not run: chmod 0700 /SPARROW
uninstall.sh: staged, not run: setfacl -b /SPARROW
uninstall.sh: staged, not run: chown -R paudley:paudley /SPARROW/store
uninstall.sh: staged, not run: setfacl -R -b /SPARROW/store
uninstall.sh: staged, not run: find /SPARROW/store -type d -exec chmod 0700 {} +
uninstall.sh: staged, not run: chown -R paudley:paudley /SPARROW/intake
uninstall.sh: staged, not run: setfacl -R -b /SPARROW/intake
uninstall.sh: staged, not run: find /SPARROW/intake -type d -exec chmod 0700 {} +
uninstall.sh: changed:
  removed /etc/systemd/system/sparrow-backup.service
  removed /etc/systemd/system/sparrow-ship.service
  removed /etc/systemd/system/sparrow.service
  removed /etc/systemd/system/sparrow-backup.timer
  removed /etc/systemd/system/sparrow-ship.timer
  removed /etc/apparmor.d/sparrow
  removed /etc/tmpfiles.d/sparrow.conf
  removed /usr/local/bin/sparrow
uninstall.sh: kept: /etc/sparrow, the sparrow user and group (/etc/sysusers.d/sparrow.conf), the images, helpers, lexicons and documents under /usr/local, /SPARROW/ship and /SPARROW/backups
[exit 0]

$ env -u SPARROW_PREFIX -u SPARROW_CONFIG SPARROW_DESTDIR=R6/root deploy/uninstall.sh
uninstall.sh: staging: the units' states and the loaded profile are not checked
uninstall.sh: staged, not run: chown paudley:root /SPARROW
uninstall.sh: staged, not run: chmod 0700 /SPARROW
uninstall.sh: staged, not run: setfacl -b /SPARROW
uninstall.sh: staged, not run: chown -R paudley:paudley /SPARROW/store
uninstall.sh: staged, not run: setfacl -R -b /SPARROW/store
uninstall.sh: staged, not run: find /SPARROW/store -type d -exec chmod 0700 {} +
uninstall.sh: staged, not run: chown -R paudley:paudley /SPARROW/intake
uninstall.sh: staged, not run: setfacl -R -b /SPARROW/intake
uninstall.sh: staged, not run: find /SPARROW/intake -type d -exec chmod 0700 {} +
uninstall.sh: nothing changed
uninstall.sh: kept: /etc/sparrow, the sparrow user and group (/etc/sysusers.d/sparrow.conf), the images, helpers, lexicons and documents under /usr/local, /SPARROW/ship and /SPARROW/backups
[exit 0]
```

After the staged uninstall, `R6/root/etc` holds only `sparrow/server.toml`,
`sparrow/ship.toml` and `sysusers.d/sparrow.conf`, and `R6/root/usr/local/bin`
is empty.

`install.sh` refusals in staged mode change nothing (review P1). The first
stages into a directory that does not exist yet, and it stays absent. The
second stages over `R6/root`, and a listing of every path, mtime and size is
identical before and after:

```text
$ env -u SPARROW_PREFIX -u SPARROW_CONFIG SPARROW_BIN=R6/bin-nolex SPARROW_DESTDIR=R6/stage-nolex deploy/install.sh --no-enable
install.sh: staging: the host preconditions (the slice, the old user unit) are not checked
install.sh: refused; nothing was changed:
  no lexicon bundle in R6/bin-nolex; run make install-bin
[exit 1]

$ test -e R6/stage-nolex
[exit 1]

$ env -u SPARROW_PREFIX -u SPARROW_CONFIG SPARROW_BIN=R6/bin-clash SPARROW_DESTDIR=R6/root deploy/install.sh --no-enable --rev ba6ebe7
install.sh: staging: the host preconditions (the slice, the old user unit) are not checked
install.sh: refused; nothing was changed:
  R6/root/usr/local/libexec/sparrow/sparrowd-schema63-ba6ebe7 exists and differs from R6/bin-clash/sparrowd-schema63-ba6ebe7; an installed image is never overwritten
[exit 1]

$ diff R6/before.txt R6/after.txt
[exit 0]
```

The old user-unit check now refuses when it cannot tell (review N1). This
probe calls `old_user_unit_problems` for an account that does not exist, so it
dies, through the new capture and through the old process substitution. It
reads only `getent` and addresses no unit:

```text
$ SPARROW_SCOPE=system bash -c 'set -euo pipefail; source deploy/lib.sh; found=$(old_user_unit_problems nosuchaccount_r2) || exit 1; echo CONTINUED'
bash: no account nosuchaccount_r2
[exit 1]

$ SPARROW_SCOPE=system bash -c 'set -euo pipefail; source deploy/lib.sh; while read -r p; do echo "problem $p"; done < <(old_user_unit_problems nosuchaccount_r2); echo CONTINUED'
CONTINUED
[exit 0]
```

The user-scope guard (review P2) compares the unit name without its
`.service` suffix: `sparrow` and `sparrow.service` are refused and
`sparrow-deploytest.service` is allowed. This was checked by evaluating the
guard expressions of `lib.sh` and `sparrow-backup` on the three names, without
running a script against any unit.

## Backup phases, 2026-11-02

A fresh instance `R7`, driven only through `sparrow-deploytest.service`.
`sparrow-backup` now copies the image pair, the lexicon bundle and the
configuration before the drain wait, while the daemon runs. The closed window
holds only the stop and the copy of the store and intake. The sync runs after
the daemon answers again and before the rename that publishes the backup. It
logs each phase's time (review Q1). After this run, the schema raise passed
the guard, as in the run above.

```text
$ deploy/pre-upgrade-backup.sh
pre-upgrade-backup.sh: backing up the store as sparrowd-schema62-cfdf2fa left it (started 20261102T170342Z)
sparrow-backup: phases: drain wait 0.0s, copy before stop 0.2s, stop 0.1s, closed copy 0.0s, restart until ready 2.1s, sync 0.9s
sparrow-backup: R7/backups/20261102T170342Z complete; 2.2s service downtime; kept 1 backup(s)
pre-upgrade-backup.sh: pre-upgrade backup R7/backups/pre-upgrade-sparrowd-schema62-cfdf2fa:
  taken_utc=20261102T170342Z
  image=sparrowd-schema62-cfdf2fa
  store_id=store:eec84b2318a4c60cc80b874e248d9739
  schema_version=62
  store_format=sparrow.store/79
[exit 0]

$ ls R7/backups/pre-upgrade-sparrowd-schema62-cfdf2fa R7/backups/pre-upgrade-sparrowd-schema62-cfdf2fa/images R7/backups/pre-upgrade-sparrowd-schema62-cfdf2fa/lexicons R7/backups/pre-upgrade-sparrowd-schema62-cfdf2fa/config
R7/backups/pre-upgrade-sparrowd-schema62-cfdf2fa:
SOURCE
config
images
intake
lexicons
store

R7/backups/pre-upgrade-sparrowd-schema62-cfdf2fa/config:
server.toml
sparrow

R7/backups/pre-upgrade-sparrowd-schema62-cfdf2fa/images:
sparrow-schema62-cfdf2fa
sparrowd-schema62-cfdf2fa

R7/backups/pre-upgrade-sparrowd-schema62-cfdf2fa/lexicons:
lexicons-a47940e1f130bf3739c6d59aaf89643209905ea4
[exit 0]
```

## Reverting intake with the store, 2026-11-02

A fresh instance `R8`, driven only through `sparrow-deploytest.service`
(review N5). It ran on schema 62 and took the pre-upgrade backup, then went to
schema 63 and ingested `after-upgrade`. After stopping, two leftovers were
planted in its root, as a failed system unit could leave them: an entry in
`.sparrow-inflight` that the restored store never journaled, and a waiting
file. These are the results:

- With the store restored and the roots kept (the round-3 revert), the
  schema 62 daemon refuses to start.
- With the intake also restored from the backup and the roots set aside as
  `intake.cutover` (the round-4 revert), the daemon starts. It holds the
  backup's single record and its watch has no pending claims.
- The leftovers stay in `intake.cutover` for inspection.

`check.sh`'s socket, store and exposure failures are the schema 62 image's and
the test unit's, as in the runs above.

```text
# leftovers the system unit could leave: a claim journaled only in its store, and a waiting file
$ install -m 0600 /dev/null R8/data/intake/deploytest/.sparrow-inflight/import-leftover
[exit 0]

$ install -m 0640 /dev/null R8/data/intake/deploytest/waiting.jsonl
[exit 0]

$ ls -A R8/data/intake/deploytest R8/data/intake/deploytest/.sparrow-inflight
R8/data/intake/deploytest:
.sparrow-inflight
waiting.jsonl

R8/data/intake/deploytest/.sparrow-inflight:
.lock
import-leftover
[exit 0]

# the round-3 revert: the store restored, the roots kept
$ mv -T R8/data/store R8/data/store.cutover
[exit 0]

$ cp -a --reflink=always R8/backups/pre-upgrade-sparrowd-schema62-cfdf2fa/store R8/data/store
[exit 0]

$ deploy/start.sh --restored-from R8/backups/pre-upgrade-sparrowd-schema62-cfdf2fa
start.sh: restored store: starting sparrowd-schema62-cfdf2fa, the image that last opened it (R8/backups/pre-upgrade-sparrowd-schema62-cfdf2fa)
start.sh: starting sparrow-deploytest.service on sparrowd-schema62-cfdf2fa
● sparrow-deploytest.service - Sparrow deploy test instance (deploy/test-instance.sh)
     Loaded: loaded (/run/user/1000/systemd/user/sparrow-deploytest.service; static)
     Active: activating (auto-restart) (Result: exit-code) since Mon 2026-11-02 11:17:33 CST; 149ms ago
       Docs: file:///usr/local/share/doc/sparrow/deploy/README.md

start.sh: sparrow-deploytest.service is not active
[exit 1]

Error: State("private claim directory exceeds its bounded journal registry; reconciliation required")

$ deploy/stop.sh
stop.sh: sparrow-deploytest.service was activating (no drain: the daemon was not serving); stopped
[exit 0]

# the round-4 revert: store and intake both from the backup, the roots set aside
$ mv -T R8/data/store R8/data/store.failed
[exit 0]

$ cp -a --reflink=always R8/backups/pre-upgrade-sparrowd-schema62-cfdf2fa/store R8/data/store
[exit 0]

$ mv -T R8/data/intake R8/data/intake.cutover
[exit 0]

$ cp -a --reflink=always R8/backups/pre-upgrade-sparrowd-schema62-cfdf2fa/intake R8/data/intake
[exit 0]

$ deploy/start.sh --restored-from R8/backups/pre-upgrade-sparrowd-schema62-cfdf2fa
start.sh: restored store: starting sparrowd-schema62-cfdf2fa, the image that last opened it (R8/backups/pre-upgrade-sparrowd-schema62-cfdf2fa)
start.sh: starting sparrow-deploytest.service on sparrowd-schema62-cfdf2fa
receipt: unit=sparrow-deploytest.service state=active pid=4145622
receipt: daemon=sparrowd-schema62-cfdf2fa cli=sparrow-schema62-cfdf2fa previous=sparrowd-schema63-ba6ebe7
receipt: store_id=store:9d62a5ff85bde673ff6b55f544581d8a schema=62 pressure=ready durability_failure=None watches=1
[exit 0]

$ records
live_records 1
[exit 0]

$ ls -A R8/data/intake.cutover/deploytest R8/data/intake.cutover/deploytest/.sparrow-inflight
R8/data/intake.cutover/deploytest:
.sparrow-inflight
waiting.jsonl

R8/data/intake.cutover/deploytest/.sparrow-inflight:
.lock
import-leftover
[exit 0]

# R1: start.sh and restart.sh refuse while a backup holds the lock
$ deploy/restart.sh
restart.sh: a backup holds R8/backups/.lock; wait for it to finish (journalctl -u sparrow-backup.service) and run this again
[exit 1]
```

While another process holds `R8/backups/.lock`, `restart.sh` refuses (review
R1):

```text
$ deploy/restart.sh
restart.sh: a backup holds R8/backups/.lock; wait for it to finish (journalctl -u sparrow-backup.service) and run this again
[exit 1]
```

## Rerunning the revert's swaps, 2026-11-02

A fresh instance `R9`, driven only through `sparrow-deploytest.service`
(review P2). The revert's swap loop and its `ship.user` block were taken
verbatim from "Revert the cutover to the user unit" in RECOVERY.md. In the
copies below, `/SPARROW` is mapped to the test data directory, `sudo` is
dropped, and the shipper's state is a scratch directory:

```bash
set -euo pipefail
for part in store intake; do
    if [ ! -e "$D/$part.cutover" ]; then
        mv -T "$D/$part" "$D/$part.cutover"
    fi
    if [ ! -e "$D/$part" ]; then
        rm -rf "$D/$part.restoring"
        cp -a --reflink=always "$B/$part" "$D/$part.restoring"
        mv -T "$D/$part.restoring" "$D/$part"
    fi
done
```

```bash
set -euo pipefail
if [ ! -e $D/ship.user ]; then
    rm -rf $D/ship.user.partial
    cp -a --reflink=auto $SHIPSRC $D/ship.user.partial
    mv -T $D/ship.user.partial $D/ship.user
fi
```

The instance ran on schema 62, took the pre-upgrade backup `B`, went to schema
63 with a second record (`live_records 2`), and stopped. An unjournaled claim
entry was planted in its root. A first revert run was then cut short: the store
was set aside and its copy left half made. These are the results:

- The rerun completes both swaps. The half-made copy is discarded, and the
  store and intake equal the backup's.
- The leftover claim stays in `intake.cutover`.
- A second rerun changes nothing. A listing of every path, type, size and
  mtime is identical.
- `ship.user` is made once, and a rerun keeps it.
- The restored daemon starts with the backup's one record.
- A rerun while that daemon serves leaves its store alone.

```text
# a first run interrupted: the store set aside, its copy half made
$ mv -T R9/data/store R9/data/store.cutover
[exit 0]

$ mkdir R9/data/store.restoring
[exit 0]

$ touch R9/data/store.restoring/half-copied
[exit 0]

# the rerun completes the swaps
$ bash R9/swap.sh
[exit 0]

$ ls -A R9/data
intake
intake.cutover
store
store.cutover
[exit 0]

$ test -e R9/data/store/half-copied
[exit 1]

$ diff -r R9/backups/pre-upgrade-sparrowd-schema62-cfdf2fa/store R9/data/store
[exit 0]

$ diff -r R9/backups/pre-upgrade-sparrowd-schema62-cfdf2fa/intake R9/data/intake
[exit 0]

$ test -e R9/data/intake.cutover/deploytest/.sparrow-inflight/import-leftover
[exit 0]

# a second rerun changes nothing
$ bash R9/swap.sh
[exit 0]

$ diff R9/before.txt R9/after.txt
[exit 0]

# ship.user: made once, kept on a rerun
$ bash R9/ship.sh
[exit 0]

$ bash R9/ship.sh
[exit 0]

$ cat R9/data/ship.user/ledger
changed
[exit 0]

$ ls -A R9/data
intake
intake.cutover
ship.user
store
store.cutover
[exit 0]

# the restored store and intake serve
$ deploy/start.sh --restored-from R9/backups/pre-upgrade-sparrowd-schema62-cfdf2fa
start.sh: restored store: starting sparrowd-schema62-cfdf2fa, the image that last opened it (R9/backups/pre-upgrade-sparrowd-schema62-cfdf2fa)
start.sh: starting sparrow-deploytest.service on sparrowd-schema62-cfdf2fa
receipt: unit=sparrow-deploytest.service state=active pid=501535
receipt: daemon=sparrowd-schema62-cfdf2fa cli=sparrow-schema62-cfdf2fa previous=sparrowd-schema63-ba6ebe7
receipt: store_id=store:c725807a9ea33af6f39dcd83f1851114 schema=62 pressure=ready durability_failure=None watches=1
[exit 0]

$ records
live_records 1
[exit 0]

# a rerun while the restored daemon serves leaves its store alone
$ bash R9/swap.sh
[exit 0]

$ records
live_records 1
[exit 0]
```

## The revert's helper wait and set-aside markers, 2026-11-02

This run used no Sparrow unit and no root (review V1 and Q1). The helper wait
and the swap loop were taken verbatim from "Revert the cutover to the user
unit" in RECOVERY.md, with three changes: `systemctl show` becomes
`systemctl --user show`, the helper names become transient user oneshot units
named `deploytest-probe-*` from `systemd-run --user`, and the 60-minute bound
becomes `$LIMIT`. The swap loop also drops `sudo` and maps `/SPARROW` onto
scratch directories.

The results:

- A running oneshot reports `activating`, and `is-active --quiet` exits 3 on
  it, so round 5's loop would have ended at once.
- The new wait holds while one probe runs (`activating`) and the other runs
  its `ExecStopPost` (`deactivating`), then continues once both have settled.
  Both `inactive` and `failed` count as settled.
- With `LIMIT=20`, a probe still running at the bound stops the subshell
  with exit 1.

```text
# the extracted wait
set -euo pipefail
    waited=0
    while :; do
        busy=
        for helper in deploytest-probe-ship.service deploytest-probe-backup.service; do
            state=$(systemctl --user show -p ActiveState --value "$helper")
            case $state in inactive | failed) ;; *) busy="$busy $helper=$state" ;; esac
        done
        [ -z "$busy" ] && break
        [ "$waited" -lt $LIMIT ] || { echo "still running after 60 minutes:$busy" >&2; exit 1; }
        echo "waiting for$busy"
        sleep 10; waited=$((waited + 10))
    done

# round 5's loop, for comparison: is-active --quiet while a oneshot runs
state: activating
$ systemctl --user is-active --quiet deploytest-probe-ship.service deploytest-probe-backup.service
[exit 3]

# the new wait: one helper running, one in ExecStopPost
waiting for deploytest-probe-ship.service=activating deploytest-probe-backup.service=activating
waiting for deploytest-probe-ship.service=activating deploytest-probe-backup.service=deactivating
[exit 0 after 20s]

states after: ship=inactive backup=failed

# the bound: a helper still running when it is reached
waiting for deploytest-probe-ship.service=activating
waiting for deploytest-probe-ship.service=activating
still running after 60 minutes: deploytest-probe-ship.service=activating
[exit 1 after 20s]
```

The set-aside markers:

- A `store.cutover` without a marker is refused, and nothing changes.
- Without one, the swaps run and write their markers.
- A rerun of the same revert changes nothing.
- A revert from a backup with another `taken_utc` refuses the earlier
  set-aside.

```text
# the swap markers
set -euo pipefail
MARK="revert from $B taken_utc=$(sed -n 's/^taken_utc=//p' "$B/SOURCE")"
for part in store intake; do
    mark=$D/$part.cutover.revert
    if [ -e "$D/$part.cutover" ] && [ "$(cat "$mark" 2>/dev/null || true)" != "$MARK" ]; then
        echo "$D/$part.cutover exists, but this revert did not set it aside ($mark); move it away first" >&2
        exit 1
    fi
    if [ ! -e "$D/$part.cutover" ]; then
        printf '%s\n' "$MARK" | tee "$mark" >/dev/null
        mv -T "$D/$part" "$D/$part.cutover"
    fi
    if [ ! -e "$D/$part" ]; then
        rm -rf "$D/$part.restoring"
        cp -a --reflink=always "$B/$part" "$D/$part.restoring"
        mv -T "$D/$part.restoring" "$D/$part"
    fi
done

# a store.cutover this revert did not make
$ bash R10/swap.sh
R10/data/store.cutover exists, but this revert did not set it aside (R10/data/store.cutover.revert); move it away first
[exit 1]

$ ls -A R10/data
intake
store
store.cutover
[exit 0]

# without it, the swaps run and write their markers
$ bash R10/swap.sh
[exit 0]

$ ls -A R10/data
intake
intake.cutover
intake.cutover.revert
store
store.cutover
store.cutover.revert
[exit 0]

$ cat R10/data/store.cutover.revert R10/data/store/data R10/data/store.cutover/data
revert from R10/backup taken_utc=20261102T120000Z
backup
live
[exit 0]

# a rerun of the same revert changes nothing
$ bash R10/swap.sh
[exit 0]

$ diff R10/before.txt R10/after.txt
[exit 0]

# a revert from another backup (another taken_utc) refuses the earlier set-aside
$ bash R10/swap.sh
R10/data/store.cutover exists, but this revert did not set it aside (R10/data/store.cutover.revert); move it away first
[exit 1]
```

## The closing count through a real stop, 2026-11-03

A fresh instance, `TEST` as above, on images built from `a3b53f1` (store schema
66) by `make install-bin`, driven only through `sparrow-deploytest.service` in
user scope. `sparrow-backup` runs as `pre-upgrade-backup.sh` runs it in user
scope, with `RUNTIME_DIRECTORY` and the scope, unit, prefix, configuration,
backups and intake from `deploy/test-instance.sh env`. The live
`sparrow.service` kept one invocation through the whole run.

A clean stop. The backup reads the unit's invocation ID before the stop, then
reads that invocation's journal under the user manager after the restart, and
records the count the stopped daemon logged:

```text
$ deploy/start.sh
start.sh: starting sparrow-deploytest.service on sparrowd-schema66-a3b53f1
receipt: unit=sparrow-deploytest.service state=active pid=2826181
receipt: daemon=sparrowd-schema66-a3b53f1 cli=sparrow-schema66-a3b53f1 previous=none
receipt: store_id=store:0f986cf28d35acc1e163479422f5dcf0 schema=66 pressure=ready durability_failure=None watches=1
[exit 0]

$ cp $TEST/data/probe.jsonl $TEST/data/intake/deploytest/probe-1.jsonl
[exit 0]

$ timeout 120 bash -c "while [ -e $TEST/data/intake/deploytest/probe-1.jsonl ]; do sleep 1; done; echo consumed"
consumed
[exit 0]

$ systemctl --user show -p InvocationID --value $SPARROW_UNIT
d155b82a63c2464a915d6fe8ba2c825a
[exit 0]

$ mkdir -p $TEST/rt && RUNTIME_DIRECTORY=$TEST/rt python3 -I $SPARROW_PREFIX/libexec/sparrow/sparrow-backup $SPARROW_CONFIG $SPARROW_BACKUPS 3 $SPARROW_INTAKE $(dirname $SPARROW_CONFIG)
sparrow-backup: phases: drain wait 0.0s, copy before stop 0.1s, stop 0.1s, closed copy 0.0s, restart until ready 2.2s, sync 0.7s
sparrow-backup: TEST/backups/20261103T141651Z complete; 2.3s service downtime; kept 1 backup(s)
[exit 0]

$ ls $SPARROW_BACKUPS
20261103T141651Z
[exit 0]

$ cat $SPARROW_BACKUPS/2*Z/SOURCE
data_dir=TEST/data/store
server_config=TEST/root/etc/sparrow/server.toml
intake=TEST/data/intake
taken_utc=20261103T141651Z
drain_seconds=0
waiting_files=0
pending_claims=0
image=sparrowd-schema66-a3b53f1
store_id=store:0f986cf28d35acc1e163479422f5dcf0
schema_version=66
store_format=sparrow.store/79
live_records=1
extra=TEST/root/etc/sparrow
unique_payload_chunks=1
[exit 0]
```

The invocation lookup by hand, under the user manager, and the restarted unit's
new invocation:

```text
$ journalctl --user -u $SPARROW_UNIT --invocation=d155b82a63c2464a915d6fe8ba2c825a -o cat --no-pager | grep -E "^sparrowd: closed "
sparrowd: closed store=store:0f986cf28d35acc1e163479422f5dcf0 unique_payload_chunks=1
[exit 0]

$ systemctl --user show -p ActiveState,InvocationID $SPARROW_UNIT
ActiveState=active
InvocationID=a720989a538847e28988a3773228e7d3
[exit 0]
```

`verify-backup.sh` refuses a scratch root under `TEST`, since the copy's socket
path would exceed the Unix limit; nothing is copied:

```text
$ mkdir -p $TEST/verify && SPARROW_VERIFY=$TEST/verify deploy/verify-backup.sh $SPARROW_BACKUPS/20261103T141651Z
verify-backup.sh: socket path TEST/verify/verify-pelFrT/store/verify.sock is 110 bytes; a Unix socket path is limited to 107. Choose a shorter data_dir (for a test instance, a shorter SCRATCH)
[exit 1]

$ ls -A $TEST/verify
[exit 0]
```

Under the default scratch root `/SPARROW/verify`, which is on the backup's
btrfs filesystem and outside the live store, every check passes and the copy is
removed:

```text
$ SPARROW_VERIFY=/SPARROW/verify deploy/verify-backup.sh $SPARROW_BACKUPS/20261103T141651Z
verify-backup: PASS query-only: status and startup report query_only; no stored watch root is open
verify-backup: PASS store-id: store:0f986cf28d35acc1e163479422f5dcf0
verify-backup: PASS status: ready, schema 66, format sparrow.store/79
verify-backup: PASS payload-chunks: 1 unique payload chunks, as SOURCE recorded
verify-backup: PASS records: 1 rows on the first page between 2026-11-03T13:16:51Z and 2026-11-03T14:16:52Z
verify-backup: PASS artifact: artifact:86bda214ee7d63314b6ae01f03383cda: 37 bytes read back to blake3:e7617fa7ba21e504e9c328ae68da4293500dbe566ce74c89f6143e0da2b5a52b
verify-backup: PASS shutdown: the daemon stopped cleanly and logged 1 unique payload chunks; removing /SPARROW/verify/verify-TWucCS
verify-backup: TEST/backups/20261103T141651Z verified
[exit 0]

$ ls -A /SPARROW/verify
[exit 0]
```

A SIGKILL instead of a clean stop. A drop-in that exists only for the test
unit sets `KillSignal=SIGKILL`, so the `systemctl stop` that `sparrow-backup`
issues kills the daemon before it can flush and log its closing count. The
backup waits 30 seconds for the count, fails, and publishes nothing: only the
unpublished `.partial-STAMP` stays, and the unit runs again on a new
invocation:

```text
$ mkdir -p $DROPIN && printf "[Service]\nKillSignal=SIGKILL\n" > $DROPIN/kill.conf && systemctl --user daemon-reload && systemctl --user show -p KillSignal,ActiveState,InvocationID $SPARROW_UNIT
ActiveState=active
InvocationID=a720989a538847e28988a3773228e7d3
KillSignal=9
[exit 0]

$ RUNTIME_DIRECTORY=$TEST/rt python3 -I $SPARROW_PREFIX/libexec/sparrow/sparrow-backup $SPARROW_CONFIG $SPARROW_BACKUPS 3 $SPARROW_INTAKE $(dirname $SPARROW_CONFIG)
sparrow-backup: the journal of sparrow-deploytest.service invocation a720989a538847e28988a3773228e7d3 has no closing count; the daemon did not stop cleanly, or its image predates the closing count
[exit 1]

$ ls -A $SPARROW_BACKUPS
.lock
.partial-20261103T141731Z
20261103T141651Z
[exit 0]

$ systemctl --user show -p ActiveState,InvocationID $SPARROW_UNIT
ActiveState=active
InvocationID=58a862963f8a4d15ae27f855b091cb15
[exit 0]

$ rm -r $DROPIN && systemctl --user daemon-reload && systemctl --user show -p KillSignal $SPARROW_UNIT
KillSignal=2
[exit 0]
```

The journal of the run so far holds one closing line, from the clean stop, and
none for the killed invocation. The next clean backup removes the leftover
partial copy and publishes normally:

```text
$ journalctl --user -u $SPARROW_UNIT --since "-5min" -o cat --no-pager | grep -E "Main process exited|closed store|Stopped|Started"
Started Sparrow deploy test instance (deploy/test-instance.sh).
sparrowd: closed store=store:0f986cf28d35acc1e163479422f5dcf0 unique_payload_chunks=1
Stopped Sparrow deploy test instance (deploy/test-instance.sh).
Started Sparrow deploy test instance (deploy/test-instance.sh).
sparrow-deploytest.service: Main process exited, code=killed, status=9/KILL
Stopped Sparrow deploy test instance (deploy/test-instance.sh).
Started Sparrow deploy test instance (deploy/test-instance.sh).
[exit 0]

$ RUNTIME_DIRECTORY=$TEST/rt python3 -I $SPARROW_PREFIX/libexec/sparrow/sparrow-backup $SPARROW_CONFIG $SPARROW_BACKUPS 3 $SPARROW_INTAKE $(dirname $SPARROW_CONFIG)
sparrow-backup: phases: drain wait 0.7s, copy before stop 6.6s, stop 0.4s, closed copy 0.0s, restart until ready 2.2s, sync 4.0s
sparrow-backup: TEST/backups/20261103T141834Z complete; 2.6s service downtime; kept 2 backup(s)
[exit 0]

$ ls -A $SPARROW_BACKUPS
.lock
20261103T141651Z
20261103T141834Z
[exit 0]

$ grep -h unique_payload_chunks $SPARROW_BACKUPS/2*Z/SOURCE
unique_payload_chunks=1
unique_payload_chunks=1
[exit 0]
```

`deploy/test-instance.sh teardown` then removed the test unit; the live
`sparrow.service` reported the same invocation ID before and after.

## Backups across the image that adds the closing count, 2026-11-03

Review round 3 (R1) found that the helper above fails every backup of an image
that predates the closing line. That includes the pre-upgrade backup that
`restart.sh` requires before the raise from 65 to 66. `sparrow-backup` now reads
the running image before the stop, looking for the line's text. For an image
without it, the backup is published and `SOURCE` records
`payload_chunks=unrecorded (image predates the closing count)`. For an image with
it, the count is taken only when the stopped unit's `Result` is `success`, that
is, when the daemon exited 0 (review O1). A missing line still fails the
backup. A failing `journalctl` now ends with a message naming systemd 257, the
first release with `journalctl --invocation=` (review O2; the host runs 262).

A fresh instance `TEST` = `.stage/ti4`, driven only through
`sparrow-deploytest.service` in user scope. It starts with only
`sparrowd-schema65-0221e87`, the image the live service runs, in its
`SPARROW_BIN`. The helper is this branch's. The daemon images for schema 66 are
the `a3b53f1` builds; the daemon code has not changed since. The live
`sparrow.service` kept one invocation through the whole run.

The upgrade order of README "Upgrades and rollback", on the old image: the
pre-upgrade backup publishes without a count, and the raise to 66 accepts it:

```text
$ deploy/start.sh
start.sh: starting sparrow-deploytest.service on sparrowd-schema65-0221e87
receipt: unit=sparrow-deploytest.service state=active pid=3227431
receipt: daemon=sparrowd-schema65-0221e87 cli=sparrow-schema65-0221e87 previous=none
receipt: store_id=store:aaa9f714c9dca04f7bd27aff4a7f132d schema=65 pressure=ready durability_failure=None watches=1
[exit 0]

$ cp $TEST/data/probe.jsonl $TEST/data/intake/deploytest/probe-1.jsonl && timeout 120 bash -c 'while [ -e $TEST/data/intake/deploytest/probe-1.jsonl ]; do sleep 1; done; echo consumed'
consumed
[exit 0]

$ deploy/pre-upgrade-backup.sh
pre-upgrade-backup.sh: backing up the store as sparrowd-schema65-0221e87 left it (started 20261103T143946Z)
sparrow-backup: phases: drain wait 0.0s, copy before stop 0.0s, stop 0.1s, closed copy 0.0s, restart until ready 2.1s, sync 1.6s
sparrow-backup: TEST/backups/20261103T143946Z complete; 2.2s service downtime; kept 1 backup(s)
pre-upgrade-backup.sh: pre-upgrade backup TEST/backups/pre-upgrade-sparrowd-schema65-0221e87:
  taken_utc=20261103T143946Z
  image=sparrowd-schema65-0221e87
  store_id=store:aaa9f714c9dca04f7bd27aff4a7f132d
  schema_version=65
  store_format=sparrow.store/79
[exit 0]

$ grep -E "^(image|schema_version|unique_payload_chunks|payload_chunks)=" $SPARROW_BACKUPS/pre-upgrade-sparrowd-schema65-0221e87/SOURCE
image=sparrowd-schema65-0221e87
schema_version=65
payload_chunks=unrecorded (image predates the closing count)
[exit 0]

$ cp -a --reflink=auto bin/sparrowd-schema66-a3b53f1 bin/sparrow-schema66-a3b53f1 $SPARROW_BIN/
[exit 0]

$ deploy/restart.sh --rev a3b53f1
restart.sh: pre-upgrade backup TEST/backups/pre-upgrade-sparrowd-schema65-0221e87 (taken 20261103T143946Z) holds the store as sparrowd-schema65-0221e87 left it
restart.sh: sparrow-deploytest.service stopped through its drain (pid 3228445); socket removed
restart.sh: starting sparrow-deploytest.service on sparrowd-schema66-a3b53f1
receipt: unit=sparrow-deploytest.service state=active pid=3229091
receipt: daemon=sparrowd-schema66-a3b53f1 cli=sparrow-schema66-a3b53f1 previous=sparrowd-schema65-0221e87
receipt: store_id=store:aaa9f714c9dca04f7bd27aff4a7f132d schema=66 pressure=ready durability_failure=None watches=1
[exit 0]
```

The probe, and the old-image backup opened with the new image (`--image`). It
skips the count check and reports the backup as not verified:

```text
$ grep -c -a -F "sparrowd: closed store=" $L/sparrowd-schema65-0221e87 $L/sparrowd-schema66-a3b53f1
TEST/root/usr/local/libexec/sparrow/sparrowd-schema65-0221e87:0
TEST/root/usr/local/libexec/sparrow/sparrowd-schema66-a3b53f1:1
[exit 0]

$ SPARROW_VERIFY=/SPARROW/verify deploy/verify-backup.sh --image $L/sparrowd-schema66-a3b53f1 $SPARROW_BACKUPS/pre-upgrade-sparrowd-schema65-0221e87
verify-backup: PASS query-only: status and startup report query_only; no stored watch root is open
verify-backup: PASS store-id: store:aaa9f714c9dca04f7bd27aff4a7f132d
verify-backup: PASS status: ready, schema 66, format sparrow.store/79
verify-backup: SKIP payload-chunks: 1 chunks; SOURCE records no unique_payload_chunks, so this backup or its image predates the count
verify-backup: PASS records: 1 rows on the first page between 2026-11-03T13:39:46Z and 2026-11-03T14:39:47Z
verify-backup: PASS artifact: artifact:01a004b3517fdfe0e82e06dfa7e0faa8: 37 bytes read back to blake3:e7617fa7ba21e504e9c328ae68da4293500dbe566ce74c89f6143e0da2b5a52b
verify-backup: PASS shutdown: the daemon stopped cleanly and logged 1 unique payload chunks; removing /SPARROW/verify/verify-T75BGl
verify-backup: NOT VERIFIED: SOURCE records no unique_payload_chunks; take a new backup with sparrow-backup
verify-backup: daemon log:
sparrowd: store=store:aaa9f714c9dca04f7bd27aff4a7f132d capnp=/SPARROW/verify/verify-T75BGl/store/verify.sock data=/SPARROW/verify/verify-T75BGl/store max_rpc_connections=16 max_concurrent_requests=4 query_only=true
sparrowd: closed store=store:aaa9f714c9dca04f7bd27aff4a7f132d unique_payload_chunks=1
[exit 1]
```

On the new image, a clean backup records the count and verifies end to end,
with exit 0 and every check PASS:

```text
$ cp $TEST/data/probe.jsonl $TEST/data/intake/deploytest/probe-2.jsonl && timeout 120 bash -c 'while [ -e $TEST/data/intake/deploytest/probe-2.jsonl ]; do sleep 1; done; echo consumed'
consumed
[exit 0]

$ RUNTIME_DIRECTORY=$TEST/rt python3 -I $SPARROW_PREFIX/libexec/sparrow/sparrow-backup $SPARROW_CONFIG $SPARROW_BACKUPS 3 $SPARROW_INTAKE $(dirname $SPARROW_CONFIG)
sparrow-backup: phases: drain wait 0.0s, copy before stop 0.1s, stop 0.1s, closed copy 0.0s, restart until ready 1.5s, sync 2.1s
sparrow-backup: TEST/backups/20261103T143957Z complete; 1.7s service downtime; kept 1 backup(s)
[exit 0]

$ ls $SPARROW_BACKUPS
20261103T143957Z
pre-upgrade-sparrowd-schema65-0221e87
[exit 0]

$ grep -E "^(image|schema_version|unique_payload_chunks|payload_chunks)=" $SPARROW_BACKUPS/2*Z/SOURCE
image=sparrowd-schema66-a3b53f1
schema_version=66
unique_payload_chunks=1
[exit 0]

$ SPARROW_VERIFY=/SPARROW/verify deploy/verify-backup.sh $SPARROW_BACKUPS/2*Z
verify-backup: PASS query-only: status and startup report query_only; no stored watch root is open
verify-backup: PASS store-id: store:aaa9f714c9dca04f7bd27aff4a7f132d
verify-backup: PASS status: ready, schema 66, format sparrow.store/79
verify-backup: PASS payload-chunks: 1 unique payload chunks, as SOURCE recorded
verify-backup: PASS records: 2 rows on the first page between 2026-11-03T13:39:57Z and 2026-11-03T14:39:58Z
verify-backup: PASS artifact: artifact:01a004b3517fdfe0e82e06dfa7e0faa8: 37 bytes read back to blake3:e7617fa7ba21e504e9c328ae68da4293500dbe566ce74c89f6143e0da2b5a52b
verify-backup: PASS shutdown: the daemon stopped cleanly and logged 1 unique payload chunks; removing /SPARROW/verify/verify-Snxyae
verify-backup: TEST/backups/20261103T143957Z verified
[exit 0]
```

On the new image, a stop forced to SIGKILL by a drop-in that exists only for
the test unit fails at once on the stopped unit's `Result`, with no 30-second
wait for the line. Only `.partial-STAMP` stays, and the unit runs again:

```text
$ mkdir -p $DROPIN && printf "[Service]\nKillSignal=SIGKILL\n" > $DROPIN/kill.conf && systemctl --user daemon-reload && systemctl --user show -p KillSignal,ActiveState $SPARROW_UNIT
ActiveState=active
KillSignal=9
[exit 0]

$ RUNTIME_DIRECTORY=$TEST/rt python3 -I $SPARROW_PREFIX/libexec/sparrow/sparrow-backup $SPARROW_CONFIG $SPARROW_BACKUPS 3 $SPARROW_INTAKE $(dirname $SPARROW_CONFIG)
sparrow-backup: sparrow-deploytest.service invocation 2be03cfeeffa49639b1d3e1a698b8ea2 did not exit 0 when stopped (Result=signal); its closing count is not recorded
[exit 1]

$ ls -A $SPARROW_BACKUPS
.lock
.partial-20261103T144005Z
20261103T143957Z
pre-upgrade-sparrowd-schema65-0221e87
[exit 0]

$ systemctl --user show -p ActiveState $SPARROW_UNIT
ActiveState=active
[exit 0]

$ rm -r $DROPIN && systemctl --user daemon-reload && systemctl --user show -p KillSignal $SPARROW_UNIT
KillSignal=2
[exit 0]
```

`Result` was chosen over `ExecMainStatus` because the latter reads 0 once the
unit is inactive. A stop and `systemctl show` of the test unit before this run
showed `ExecMainCode=0`, `ExecMainStatus=0`, `Result=success`. A transient user
unit (`systemd-run --user --unit=qo-probe`, a shell trapping SIGINT) gave the
`Result` for each kind of stop. A SIGINT stop exiting 3 read `exit-code`, a
SIGKILL stop read `signal`, and a SIGINT stop exiting 0 read `success`. A failing
`journalctl` was not provoked.

`deploy/test-instance.sh teardown .stage/ti4` then removed the test unit.

## Not run

- As root: `install.sh`'s refusal and its partial-change report,
  `uninstall.sh` (only staged here), `pre-upgrade-backup.sh` through
  `sparrow-backup.service` (only the user-scope path ran here), the backup as
  the root system unit, including its root `journalctl --invocation=` read of
  the closing count inside the backup unit's sandbox (kept for the system
  cutover window), and the revert to the user unit
  (RECOVERY.md, "Revert the cutover to the user unit").
- Everything that needs root: the system unit, the user and group, the slice,
  priority, the credential, the bind on 518 and AppArmor enforcement. The
  acceptance steps of issue #22, the cutover's tracking issue, list those steps.
