<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Sparrow recovery

Each entry gives the symptom, the diagnosis, the recovery and whether the
recovery is reversible. Paths and units are those of [README.md](README.md).
Commands prefixed with `sudo` need root.

## Where state lives and what is backed up

| State | Location | Backed up by |
| --- | --- | --- |
| Store (catalog, bytes, indexes, WAL, watch catalog, consume journals) | `/SPARROW/store` | `sparrow-backup` (closed-store copy); the NAS archive once enabled |
| Files waiting in consume roots | `/SPARROW/intake/NAME` | `sparrow-backup`, taken only when every root is empty |
| Shipper queue, ledgers, lock | `/SPARROW/ship` | `sparrow-backup` |
| Configuration, watch definitions, CHAP secret | `/etc/sparrow` | `sparrow-backup` |
| Images and lexicons | `/usr/local/libexec/sparrow`, `/usr/local/share/sparrow` | Rebuilt from the repository and `./bin` |
| Units, profile, layout | `/etc/systemd/system`, `/etc/apparmor.d`, `/etc/{sysusers,tmpfiles}.d` | This directory (`install.sh`) |

The closed-store backups in `/SPARROW/backups` share the store's disk, so they
protect against software faults and operator error, not against losing the
disk. The NAS archive is the off-host backup.

## Verify health

```bash
sudo deploy/check.sh
systemctl status sparrow.service
journalctl -u sparrow.service -b --no-pager | tail -50
sparrow /SPARROW/store/rpc.sock status
```

In `status`, `pressure.phase` is `ready`, `durability.failure` is `null` and
every watch in `consume_watches` names a root that exists.

## 2026-11-01: /home filled and durability latched

- Symptom: from 08:39 every write was refused with `STORAGE_FAILURE: durability
  uncertain; reopen and inspect the store`, 103,274 refusals in all.
- Cause: `/home` filled. RocksDB hit `ENOSPC` writing its WAL, which latches
  durability as uncertain until the store is reopened. Sparrow's own
  `disk_budget` bounds only the store's bytes; other programs filled the shared
  filesystem.
- Recovery: once space returned, a restart reopened the store and it served
  again.
- Prevention since 600ee97: `min_free_bytes` (default 4 GiB) is a floor on
  free space in the filesystem that holds `data_dir`. Below it the daemon
  refuses writes with a retriable `STORAGE_PRESSURE`, pauses Unix and TCP
  connections and claims no consumed files; it logs one line on pausing and one
  on resuming. A write that still fails with durability uncertain makes the
  daemon exit, and `Restart=on-failure` reopens the store; at startup it waits
  for the floor before opening, so a full disk does not spend the start limit.
- `/SPARROW` is on the same btrfs device as `/home`, so the move to `/SPARROW`
  did not isolate Sparrow from a full `/home`.
- Reversible: yes; the restart only reopens the store.

## Store latched: durability uncertain

- Symptom: refusals with `durability uncertain; reopen and inspect the store`,
  or the journal line `store durability uncertain; exiting so it is reopened`
  followed by a restart.
- Diagnose: `df -h /SPARROW`, `sudo btrfs filesystem usage /`,
  `journalctl -u sparrow.service -b | grep -i -e durab -e space -e ENOSPC`.
- Recover: make space if the disk is full (see "Full disk"), then
  `sudo systemctl restart sparrow.service` if the daemon has not already
  restarted itself. Confirm `durability.failure` is `null`.
- Reversible: yes.

## Full disk

- Symptom: the journal shows the pause line (writes and connections paused
  below `min_free_bytes`), clients get `STORAGE_PRESSURE`, `check.sh` cannot
  connect, or the unit waits at startup with `waiting to open the store`.
- Diagnose: `df -h /SPARROW /home`, then find what grew:
  `sudo du -xsh /home/* /SPARROW/* | sort -h | tail`.
- Recover: free space on the filesystem (another program's files, old backups
  in `/SPARROW/backups`, a read-only pre-move copy once approved). The daemon
  resumes by itself within a second of crossing the floor; nothing needs a
  restart. Do not lower `min_free_bytes` to get past it.
- Reversible: yes, unless the space came from deleting something.

## Unit start-limit hit

- Symptom: `systemctl status sparrow.service` shows `start-limit-hit`; five
  failed starts within ten minutes stopped the unit.
- Diagnose: the cause is in the failed starts' output:
  `journalctl -u sparrow.service -b --no-pager | tail -100`. Common causes are
  a refused open (below), an AppArmor denial, a missing lexicon bundle or
  credential, a watch root that does not exist, or port 518 taken by another
  process (`sudo ss -ltnp 'sport = :518'`).
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
  or a refusal naming the archive table on a store that has been fenced for
  archiving.
- Diagnose: which image runs (`readlink /usr/local/libexec/sparrow/sparrowd`),
  which image last opened the store (the receipts and the journal), and
  whether `text_lexicon_dir` changed (`grep text_lexicon_dir /etc/sparrow/server.toml`).
- Recover: an unsupported format means an image older than the store; return
  to the image that last opened it (`sudo deploy/restart.sh --rev REV`). An
  analyzer change means the lexicon bundle differs from the one the store was
  built with; point `text_lexicon_dir` back at that bundle. A store fenced for
  archiving needs its `[archive]` table restored. Never delete or rebuild the
  store to get past a refusal.
- Reversible: yes; a refused open changes nothing.

## Rollback

- When: a new image misbehaves after `restart.sh`.
- Do: `sudo deploy/restart.sh --rollback`. It verifies the previous pair runs,
  stops through the drain, swaps the links (the bad pair becomes `.previous`),
  starts and checks the store identity.
- Limit: an image cannot undo a store-format upgrade its successor made.
  `start.sh` and `restart.sh` therefore refuse an image whose schema number
  (`sparrowd-schemaN-REV`) is lower than the current image's, before stopping
  anything. Stay on the new image and fix forward, or restore the closed-store
  backup taken before the upgrade
  ([evidence/lifecycle.md](evidence/lifecycle.md) shows a schema 62 image
  refusing a store a schema 63 image had opened). Reverting `/etc/sparrow/server.toml` is a
  separate step, kept in its backup.
- Reversible: yes; `--rollback` again swaps forward.

## Watch root missing, replaced or unreadable

- Symptom: startup fails validating a watch, the journal names
  `configured consume root changed`, `watch root was replaced`, or
  `claim directory must be owned by the daemon and mode 0700`; or files stay
  in a root without being consumed.
- Diagnose: `sparrow /SPARROW/store/rpc.sock watch list`;
  `ls -la /SPARROW/intake/NAME /SPARROW/intake/NAME/.sparrow-inflight`;
  `getfacl /SPARROW/intake/NAME`. A file the daemon cannot read is mode 0600
  or not in the `sparrow` group.
- Recover: recreate a missing root with plain `mkdir` as a member of
  `sparrow`. A replaced root with pending claims must be restored to the
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

## NAS archive unavailable

- The archive worker is not built yet; this entry applies once `[archive]` is
  configured. An unreachable archive at startup starts the daemon degraded; it
  does not refuse to start, and no unarchived evidence is deleted while the
  archive is down. A hung NFS mount can still delay the unit's start while
  systemd sets up `ReadWritePaths=`; check with `findmnt /net/synology/archives`
  and `timeout 5 stat /net/synology/archives/sparrow`.

## Restore from a backup

A backup `/SPARROW/backups/STAMP` holds `store`, `intake` and `config` (the
server configuration, `/etc/sparrow` with the secret, and `/SPARROW/ship`)
from one moment. Qualify it on a disposable copy first with the recovery probe
(docs/deployment.md, "Recovery qualification"), and never start `sparrowd` on
such a copy: its watch catalog names the live consume roots.

```bash
sudo systemctl stop sparrow-backup.timer sparrow-ship.timer
sudo deploy/stop.sh
sudo mv /SPARROW/store /SPARROW/store.damaged
sudo mv /SPARROW/intake /SPARROW/intake.damaged
sudo mv /SPARROW/ship /SPARROW/ship.damaged
sudo cp -a --reflink=auto /SPARROW/backups/STAMP/store /SPARROW/store
sudo cp -a --reflink=auto /SPARROW/backups/STAMP/intake /SPARROW/intake
sudo cp -a --reflink=auto /SPARROW/backups/STAMP/config/ship /SPARROW/ship
sudo systemd-tmpfiles --create sparrow.conf
sudo deploy/start.sh
sudo deploy/check.sh --store-id STORE_ID
sudo systemctl start sparrow-ship.timer sparrow-backup.timer
```

`STORE_ID` is the identity recorded before the failure. Restore
`/etc/sparrow` from `BACKUP/config/sparrow` only if it was lost. The shipper
then resends every file shipped after the backup whose original still exists;
records producers sent directly after the backup are lost unless they resend
them. On a fresh host, follow README.md steps 1 to 3 first, then this
procedure with the backup copied onto the host. This whole-service restore has
not been exercised; the drill covers reading a restored store only.

## Exposure

`systemd-analyze security` scores the units 1.6 (`sparrow.service`), 0.4
(`sparrow-ship.service`) and 1.2 (`sparrow-backup.service`), each under the
fleet target of 2.0 ([evidence/systemd-analyze-security.md](evidence/systemd-analyze-security.md)).
Each remaining item and why it stays:

| Unit | Item | Why |
| --- | --- | --- |
| all | `RootDirectory=`/`RootImage=` | The daemon and helpers use the host's libraries; `ProtectSystem=strict` makes the host read-only to them instead. |
| all | `RestrictAddressFamilies=~AF_UNIX` | The private socket and the journal are Unix sockets. |
| all | `PrivateUsers=` | The daemon's port 518 needs the bind capability in the host's user namespace, and every unit reads or writes files whose real owners must stay mapped (the store, the roots, the producers' files, the backup's preserved owners). |
| all | `DeviceAllow=char-rtc:r` | Added by `ProtectClock=yes` itself; read-only clock access. |
| `sparrow` | `PrivateNetwork=`, `RestrictAddressFamilies=~AF_(INET\|INET6)`, `IPAddressDeny=` | The public Cap'n Proto listener on 0.0.0.0:518 serves remote agents on any trusted network; CHAP guards it. |
| `sparrow` | `AmbientCapabilities=`, `CAP_NET_BIND_SERVICE` | Binding port 518, Patrick's ruling of 2026-11-01; the bounding set holds nothing else. |
| `sparrow` | `RestrictSUIDSGID=` | systemd implements it with a seccomp rule that cannot inspect `openat2`'s mode, so it refuses `openat2` outright, and the daemon needs `openat2` to open files beneath a consume root without following symlinks (measured, [evidence/lifecycle.md](evidence/lifecycle.md)). `NoNewPrivileges=yes` makes a setuid file inert, and the daemon creates every file with a fixed mode. |
| `ship` | `ProtectHome=read-only` | It reads the producers' directories under `/home/paudley`. |
| `backup` | `User=` root, `CAP_CHOWN`, `CAP_FOWNER`, `CAP_DAC_*`, `@privileged` (`@chown`) | It stops and starts the system unit and copies trees owned by `sparrow`, `paudley` and root with their owners preserved. Its writes are confined to `/SPARROW/backups`. |

The two timers' services are not confined by AppArmor: they are Python
programs that run `cp`, `sync`, `systemctl` and the CLI, and their profiles are
follow-up work. Their systemd sandboxes confine their writes.
