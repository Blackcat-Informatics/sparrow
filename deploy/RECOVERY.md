<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Sparrow recovery

Each entry gives the symptom, the diagnosis, the recovery and whether the
recovery is reversible. Paths and units are those of [README.md](README.md).
Commands prefixed with `sudo` need root.

## Where state lives and what is kept

Sparrow backs up nothing except the age-out to GTS files on a network share (the archive worker,
docs/interfaces.md, "Archive"). Sparrow takes no online store backup. There is
no copy of the store to restore: a lost or damaged store is not recoverable from this
directory.

| State | Location | Kept by |
| --- | --- | --- |
| Store (catalog, bytes, indexes, WAL, watch catalog, consume journals) | `/var/lib/sparrow/store` | Nothing; aged-out evidence is archived to GTS files on the network share before it is retired |
| Files waiting in consume roots | `/var/lib/sparrow/intake/NAME` | Nothing |
| Shipper queue, ledgers, lock | `/var/lib/sparrow/ship` | Nothing |
| Configuration, watch definitions, CHAP secret | `/etc/sparrow` | Nothing; the watch definitions applied are also in the store's catalog |
| Images and lexicons | `/usr/local/libexec/sparrow`, `/usr/local/share/sparrow` | The kept previous pair (`.previous`); everything else is rebuilt from the repository into `./bin` |
| Units, profile, layout | `/etc/systemd/system`, `/etc/apparmor.d`, `/etc/{sysusers,tmpfiles}.d` | This directory (`install.sh`) |

## Verify health

```bash
sudo deploy/check.sh
systemctl status sparrow.service
journalctl -u sparrow.service -b --no-pager | tail -50
sparrow /var/lib/sparrow/store/rpc.sock status
```

In `status`, `pressure.phase` is `ready`, `durability.failure` is `null` and
every watch in `consume_watches` names a root that exists.

## A shared filesystem fills and durability latches

A full filesystem can cause RocksDB to fail a write or WAL sync. The daemon
then refuses writes with retryable `STORAGE_PRESSURE`, and `durability.failure`
names the cause. The configured `disk_budget` accounts for store bytes.
Other writers on the filesystem can consume the remaining space.

Restore space on the store filesystem and observe the daemon's recovery.
Below `min_free_bytes`, new writes receive a floor-labelled refusal and reads
remain available. Consume watches keep their files. At startup, the open floor
also includes space for replaying the WAL left by an unclean stop.
[Free-space behavior](../docs/interfaces.md#the-free-space-pause-257)

## Store latched: durability uncertain

The maintenance worker retries proof of durability after adequate space has
been available for 15 seconds. Success clears the latch in place. After
120 seconds of adequate space without proof, the daemon drains the old
session and reopens the original store in the same process.

During the transition, the listeners stay bound. Existing sessions are retired
and clients can receive a retryable refusal or disconnect. The journal and
systemd status identify close, reopen and retry phases. A repairable refusal
continues on a bounded retry schedule. A terminal refusal ends recovery
with the cause recorded.

After recovery, check the store identity, durability status and confirmed
payloads. A fence from the retired session returns `STALE_WATERMARK`.
Inspect the original request or retry with its stable ID. Check the journal
for buffered occurrence IDs reported missing or still unverified.
[Session replacement](../docs/interfaces.md#daemon-teardown)

## Full disk

- Symptom: the journal shows `free space is below the floor` (writes paused
  below `min_free_bytes`; connections are still accepted and reads served),
  write clients get `STORAGE_PRESSURE` with the floor class and hold, `status`
  shows a nonzero `floorPause.pausedSince`, or, only when free space is below
  the maintenance reserve (capped at `min_free_bytes`) plus every byte of the
  write-ahead log a stop left behind, the unit waits at startup with
  `waiting to open the store` (nothing can answer then, so `check.sh` cannot
  connect). Above that amount a restart opens the store and serves reads.
- Diagnose: `df -h /var/lib/sparrow /home`, then find what grew:
  `sudo du -xsh /home/* /var/lib/sparrow/* | sort -h | tail`.
- Recover: free space on the filesystem (another program's files, a read-only
  pre-move copy once approved). No backup is kept on it. The daemon
  resumes by itself within a second of crossing the floor; nothing needs a
  restart. Do not lower `min_free_bytes` to get past it.
- Reversible: yes, unless the space came from deleting something.

## Start waiting

- Symptom: the unit is `active (running)` but nothing answers, and the journal
  holds a line `sparrowd: waiting to <what>: <reason>; checking again every N s`,
  repeated each minute while the reason stands. The daemon waits inside the
  running process for these conditions:
  - `open the store`: free space below the maintenance reserve plus the
    write-ahead log left in `data_dir` (checked each second); an open that
    failed with no space left on the device, or with `STORAGE_PRESSURE` because
    a pending upgrade (the owned RDF metadata backfill) needs more data-write
    allowance than `disk_budget` leaves (attempted again every 30 s); or
    another process (an offline `sparrow` command, a daemon still stopping)
    holds the store's `LOCK` file (attempted again each second). Find the
    holder with `sudo fuser -v <data_dir>/LOCK`.
  - `bind the public TCP address`, `bind the local endpoint`, `bind the ingest
    socket`, `bind the syslog listeners`: a departing owner still holds the
    address or socket name (tried each second). Find it with
    `sudo ss -ltnp 'sport = :518'`.
- Recover: free the space, or raise `disk_budget` in `/etc/sparrow/server.toml`
  (the wait reads `disk_budget`, `maintenance_reserve` and `min_free_bytes` from
  the file again before each attempt that follows a refused open and journals
  the change), or stop the
  process that holds the address or the store's lock. The daemon proceeds by itself and journals
  `the condition cleared after N s; proceeding`. No restart is needed.
- Where it shows: the journal lines above, and `systemctl show -p StatusText
  sparrow.service` (`waiting to open the store: only N bytes free ...`; the
  daemon sets it through `NotifyAccess=main`, and it reads `serving` once the
  daemon serves). Nothing answers during a wait: the Unix socket needs the
  store's lock, which the open takes, and the sockets a later bind wait holds
  accept nothing until the daemon serves.
- Stop: `systemctl stop` sends SIGINT, which ends the wait at once. The
  unit stays `active (running)` through a wait because it is `Type=exec`: a
  readiness gate would hold the unit in `activating` and its start timeout
  would kill a wait that is meant to last as long as the condition.
- Not a wait: a condition named as deterministic, which cannot pass without a
  change of configuration, image or store: a bad argument,
  configuration key or secret file; a store format (`UNSUPPORTED`) or
  identity, analyzer, lexicon, profile or archive-fence refusal
  (`INVALID_ARGUMENT`, including a missing `[archive]` table after the store's
  archive fence); a watch configuration that contradicts a pending claim; a socket
  name that is not a socket, an ingest socket
  directory or socket with the wrong owner or mode. The daemon journals
  `sparrowd: refused to start: <reason>` and exits with status 78, which the
  unit lists in `RestartPreventExitStatus=`. The unit remains `failed` with
  `status=78/CONFIG`. Correct the cause (restore the image, lexicon or
  configuration that wrote the store, see "Refused open"), then
  `sudo systemctl reset-failed sparrow.service && sudo deploy/start.sh`.
  Any other failure before the daemon serves (corruption or an I/O error at the
  open, a descriptor limit, a mount that has not come up) is not classified as
  permanent: the daemon journals `sparrowd: start failed: ...; not classified as
  permanent` and exits 1. The unit retries with the crash recovery delay.
- Reversible: yes.

## Crash recovery

The unit automatically retries ordinary failures. The restart delay starts
at 10 seconds and increases over six steps to two minutes. Retries continue
at the maximum delay. Use `journalctl -u sparrow.service -b --no-pager` to
see each failed exit and scheduled restart. `systemctl status sparrow.service`
shows the current state and the most recent service messages. After the cause
clears, the next attempt can start the service.

To switch images during a retry loop, stop the unit with `sudo deploy/stop.sh`
before running `deploy/restart.sh`. Stopping cancels the pending retry.

## Unit start-limit hit

An installed unit with a locally configured start limit can report
`start-limit-hit`. The deployment scripts preserve that state until the
operator diagnoses it and resets the unit.

- Diagnose: the cause is in the failed runs' output:
  `journalctl -u sparrow.service -b --no-pager | tail -100`. Common causes are
  an AppArmor denial, a crash, an out-of-memory kill, or a worker that ended
  (`... worker terminated unexpectedly`).
- Recover: fix the cause, then
  `sudo systemctl reset-failed sparrow.service && sudo deploy/start.sh`.
- Reversible: yes.

## AppArmor denial

- Symptom: a start fails with permission denied on a path the configuration
  names, or `check.sh` reports denials.
- Diagnose: `sudo journalctl -k -b --no-pager | grep 'apparmor="DENIED"' | grep 'profile="sparrow"'`.
  Each line names the operation, the path (`name=`) and the requested access.
  `sudo aa-status | grep sparrow` shows whether the profile is loaded and in
  enforce mode.
- Recover: a new path the daemon must use (a new syslog listener, a moved
  archive) belongs in `apparmor/sparrow`; commit it and run
  `sudo deploy/install.sh`, which reloads the profile, then restart. An exec
  denial is the profile working: the daemon runs nothing. To diagnose a
  denial you do not understand, `sudo aa-complain /etc/apparmor.d/sparrow`
  logs instead of refusing; return to enforce with
  `sudo aa-enforce /etc/apparmor.d/sparrow` before leaving the host.
- Not loaded: the unit refuses to start while the profile is not loaded.
  `sudo apparmor_parser -r -W /etc/apparmor.d/sparrow`.
- Reversible: yes.

## Refused open

- Symptom: the unit fails at once and the journal shows one of
  `UNSUPPORTED: unsupported store format; migration required`,
  `UNSUPPORTED: text analyzer identity changed; explicit index migration required`,
  `UNSUPPORTED: RDF profile changed; explicit projection/index migration required`,
  `UNSUPPORTED: RDF profile changed and the store holds N Markdown projection(s) ...`,
  a refusal naming the archive table on a store that has been fenced for
  archiving, or
  `STORAGE_FAILURE: log time interpretation law and writer fence disagree`
  on a store with a log time profile.
  The `UNSUPPORTED` and archive refusals end the start with status 78; the
  `STORAGE_FAILURE` one exits 1 and follows the crash recovery delay.
- Diagnose: which image runs (`readlink /usr/local/libexec/sparrow/sparrowd`),
  which image last opened the store (the receipts and the journal), and
  whether `text_lexicon_dir` changed (`grep text_lexicon_dir /etc/sparrow/server.toml`).
- Recover: an unsupported format means an image older than the store; return
  to the image that last opened it (`sudo deploy/restart.sh --rev REV`). On a
  log time store an older image reports the law and fence disagreement because
  it does not know the newer fence identity. Cancel any pending retry with
  `sudo deploy/stop.sh` before switching back.
  An analyzer change means the lexicon bundle differs from the one the store was
  built with; point `text_lexicon_dir` back at that bundle. A Markdown
  count means the store still holds Markdown projections from before PurRDF
  a PurRDF revision that advances the RDF profile: return to the previous image (`sudo deploy/restart.sh
  --rollback`) until those records retire. The newer image advances the
 profile itself once none remains. The plain profile refusal on an
  image built before that revision means a newer image has already advanced the
  profile: see "Rollback". A store fenced for
  archiving needs its `[archive]` table restored. Never delete or rebuild the
  store to get past a refusal.
- Reversible: yes; a refused open changes nothing.

## Rollback

- When: a new image misbehaves after `restart.sh`.
- If the unit is between automatic retries, stop it with `sudo deploy/stop.sh`.
  See [Crash recovery](#crash-recovery).
- Do: `sudo deploy/restart.sh --rollback`. It verifies the previous pair runs,
  stops through the drain, swaps the links (the bad pair becomes `.previous`),
  starts and checks the store identity.
- Limit: an image cannot undo a store-format upgrade its successor made (the store fence).
  `start.sh` and `restart.sh` therefore refuse an image whose schema number
  (`sparrowd-schemaN-REV`) is lower than the current image's, before stopping
  anything. There is no backup to restore: a format-changing upgrade has no undo, so stay on
  the new image and fix forward. The one exception is a copy of the closed store taken with
  `deploy/copy-on-write.sh copy SOURCE DEST` before the switch (`deploy/README.md`, "Upgrades and rollback"). The kept previous image and the link swap are all an
  upgrade relies on: an older image refuses a store a newer schema has opened.
- Limit without a schema change: a move to a PurRDF revision can keep the
  schema but advance the store's RDF profile at the first open. An image
  built before it then refuses with `RDF profile changed`, and the schema
  check does not catch this, so `--rollback` stops the service. Run
  `--rollback` again to swap forward.
- Reversible: yes; `--rollback` again swaps forward.

## Watch root missing, replaced or unreadable

- Symptom: the journal names `configured consume root changed`,
  `watch root was replaced`, or
  `claim directory must be owned by the daemon and mode 0700`, `watch list`
  shows a `failure` on that watch alone, or files stay in a root without being
  consumed. The daemon and the other watches keep running, and the watch is
  retried every minute at most. A root missing when the daemon starts is the same
  case: the daemon starts and serves, reports that watch, and the watch begins
  when the root appears.
- Diagnose: `sparrow /var/lib/sparrow/store/rpc.sock watch list`;
  `ls -la /var/lib/sparrow/intake/NAME /var/lib/sparrow/intake/NAME/.sparrow-inflight`;
  `getfacl /var/lib/sparrow/intake/NAME`. A file the daemon cannot read is mode 0600
  or not in the `sparrow` group.
- Recover: no restart is needed: the watch resumes by itself within a minute
  of the exact root being restored. Recreate a missing root with plain `mkdir`
  as a member of `sparrow`. A replaced root with pending claims must be restored to the
  original directory (the claim binds its inode); never discard
  `.sparrow-inflight`. A root that lost group access: `sudo systemd-tmpfiles
  --create sparrow.conf` restores the parent; fix the root itself with
  `sudo chmod 0770 DIR` followed by
  `sudo setfacl -m g:sparrow:rwx,m::rwx,d:u::rwx,d:g::---,d:g:sparrow:rwx,d:m::rwx,d:o::--- DIR`
  (no setgid bit: the claim directory must stay exactly 0700). Make an unreadable waiting file
  readable with `sudo chgrp sparrow FILE && sudo chmod 0640 FILE`.
- Reversible: yes.

## Stale socket

- Symptom: `rpc.sock` exists while the unit is stopped.
- Do nothing by hand. The next start takes the store's exclusive lock first and
  only then removes a socket nobody listens on. Never remove the socket until
  the previous owner is proven terminal.

## Archive share unavailable

- Symptom: the journal has `sparrowd: archive pass failed; the watermark did not move`
  followed by an I/O error, a missing archive directory, a `STORE_ID` naming another store or a
  directory with content but no `STORE_ID`, or "archive directory ... is not on its expected
  mount".
- Effect: none on ingestion. The archive worker runs on its own thread and the daemon starts and
  serves without the share; the store keeps what it could not archive and the watermark does not
  move, so nothing is retired before it is archived. The disk budget and the free-space floor
  then apply as usual.
- "Not on its expected mount" means the archive directory is on the same device as `/`, or on a local
  filesystem of the same type as `/`'s (the message names which): the share is not mounted and
  its mount point is an ordinary local directory. The worker wrote, moved and
  deleted nothing. Never create the archive on the local disk instead.
- Diagnose: `findmnt -T /mnt/archive/sparrow-archive` must show `nfs4` from
  `nas.example:/export/archive`.
- Recover: restore the mount. The next pass retries by itself. The default
  unit and profile need explicit access to the configured archive path.
  [Archive configuration](../docs/deployment.md#archive-and-backups)

## Exposure

`systemd-analyze security` scores the units 1.6 (`sparrow.service`) and 0.4
(`sparrow-ship.service`), each under the target of 2.0.
Each remaining item and why it stays:

| Unit | Item | Why |
| --- | --- | --- |
| all | `RootDirectory=`/`RootImage=` | The daemon and helpers use the host's libraries; `ProtectSystem=strict` makes the host read-only to them instead. |
| all | `RestrictAddressFamilies=~AF_UNIX` | The private socket and the journal are Unix sockets. |
| all | `PrivateUsers=` | The daemon's port 518 needs the bind capability in the host's user namespace, and every unit reads or writes files whose real owners must stay mapped (the store, the roots, the producers' files). |
| all | `DeviceAllow=char-rtc:r` | Added by `ProtectClock=yes` itself; read-only clock access. |
| `sparrow` | `PrivateNetwork=`, `RestrictAddressFamilies=~AF_(INET\|INET6)`, `IPAddressDeny=` | The public Cap'n Proto listener on 0.0.0.0:518 serves remote agents on any trusted network; CHAP guards it. |
| `sparrow` | `AmbientCapabilities=`, `CAP_NET_BIND_SERVICE` | Binding port 518; the bounding set holds nothing else. |
| `sparrow` | `RestrictSUIDSGID=` | systemd implements it with a seccomp rule that cannot inspect `openat2`'s mode, so it refuses `openat2` outright, and the daemon needs `openat2` to open files beneath a consume root without following symlinks. `NoNewPrivileges=yes` makes a setuid file inert, and the daemon creates every file with a fixed mode. |
| `ship` | `ProtectHome=read-only` | It reads the producers' directories, which may be under `/home`. |

Hardening settings that the units do not use, and why:
`StateDirectory=`, `RuntimeDirectory=`, `CacheDirectory=` and `LogsDirectory=`
on `sparrow.service`, because the store is `/var/lib/sparrow/store`, the
daemon keeps no runtime or cache files and it logs to the journal; `WatchdogSec=`, because the
daemon does not implement the systemd notification protocol (`Type=exec`).

The timer's service is not confined by AppArmor: it is a Python
program that runs `rsync`, `b3sum`, `sync` and the CLI, and its profile is
follow-up work. Their systemd sandboxes confine their writes.
