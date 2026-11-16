<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Sparrow recovery

Each entry gives the symptom, the diagnosis, the recovery and whether the
recovery is reversible. Paths and units are those of [README.md](README.md).
Commands prefixed with `sudo` need root.

## Where state lives and what is kept

Sparrow backs up nothing except the age-out to GTS files on the NAS (the archive worker,
docs/interfaces.md, "Archive"). The online store backup (#156) was removed in #330. There is
no copy of the store to restore: a lost or damaged store is not recoverable from this
directory.

| State | Location | Kept by |
| --- | --- | --- |
| Store (catalog, bytes, indexes, WAL, watch catalog, consume journals) | `/SPARROW/store` | Nothing; aged-out evidence is archived to GTS files on the NAS before it is retired |
| Files waiting in consume roots | `/SPARROW/intake/NAME` | Nothing |
| Shipper queue, ledgers, lock | `/SPARROW/ship` | Nothing |
| Configuration, watch definitions, CHAP secret | `/etc/sparrow` | Nothing; the watch definitions applied are also in the store's catalog |
| Images and lexicons | `/usr/local/libexec/sparrow`, `/usr/local/share/sparrow` | The kept previous pair (`.previous`); everything else is rebuilt from the repository into `./bin` |
| Units, profile, layout | `/etc/systemd/system`, `/etc/apparmor.d`, `/etc/{sysusers,tmpfiles}.d` | This directory (`install.sh`) |

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
  refuses writes with a retriable `STORAGE_PRESSURE` that carries the
  `freeSpaceFloor` class, keeps accepting Unix, ingest, TCP and syslog TCP
  connections, serves reads, and claims no consumed files (consume and tail
  watches keep their files); it logs a line when the pause begins, one a minute
  while it lasts and one when it ends. A write that still fails with durability uncertain makes the
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

- Symptom: the journal shows `free space is below the floor` (writes paused
  below `min_free_bytes`; connections are still accepted and reads served),
  write clients get `STORAGE_PRESSURE` with the floor class and hold, `status`
  shows a nonzero `floorPause.pausedSince`, or the unit waits at startup with
  `waiting to open the store` (nothing can answer then, so `check.sh` cannot
  connect).
- Diagnose: `df -h /SPARROW /home`, then find what grew:
  `sudo du -xsh /home/* /SPARROW/* | sort -h | tail`.
- Recover: free space on the filesystem (another program's files, a read-only
  pre-move copy once approved). No backup is kept on it. The daemon
  resumes by itself within a second of crossing the floor; nothing needs a
  restart. Do not lower `min_free_bytes` to get past it.
- Reversible: yes, unless the space came from deleting something.

## Unit start-limit hit

- Symptom: `systemctl status sparrow.service` shows `start-limit-hit`; five
  failed starts within ten minutes stopped the unit.
- Diagnose: the cause is in the failed starts' output:
  `journalctl -u sparrow.service -b --no-pager | tail -100`. Common causes are
  a refused open (below), an AppArmor denial, a missing lexicon bundle or
  credential, a watch root that does not exist, a `socket_path` longer than
  107 bytes (`path must be shorter than SUN_LEN`; `start.sh` refuses one), or
  port 518 taken by another process (`sudo ss -ltnp 'sport = :518'`).
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
- Diagnose: which image runs (`readlink /usr/local/libexec/sparrow/sparrowd`),
  which image last opened the store (the receipts and the journal), and
  whether `text_lexicon_dir` changed (`grep text_lexicon_dir /etc/sparrow/server.toml`).
- Recover: an unsupported format means an image older than the store; return
  to the image that last opened it (`sudo deploy/restart.sh --rev REV`). On a
  log time store an older image reports the law and fence disagreement
  instead, because it does not know the newer fence identity: recover it the
  same way. An
  analyzer change means the lexicon bundle differs from the one the store was
  built with; point `text_lexicon_dir` back at that bundle. A Markdown
  count means the store still holds Markdown projections from before PurRDF
  fb02dd38b: return to the previous image (`sudo deploy/restart.sh
  --rollback`) until those records retire. The newer image advances the
  profile itself once none remains (#28). The plain profile refusal on an
  image built before fb02dd38b means a newer image has already advanced the
  profile: see "Rollback". A store fenced for
  archiving needs its `[archive]` table restored. Never delete or rebuild the
  store to get past a refusal.
- Reversible: yes; a refused open changes nothing.

## Rollback

- When: a new image misbehaves after `restart.sh`.
- Do: `sudo deploy/restart.sh --rollback`. It verifies the previous pair runs,
  stops through the drain, swaps the links (the bad pair becomes `.previous`),
  starts and checks the store identity.
- Limit: an image cannot undo a store-format upgrade its successor made (the store fence).
  `start.sh` and `restart.sh` therefore refuse an image whose schema number
  (`sparrowd-schemaN-REV`) is lower than the current image's, before stopping
  anything. There is no backup to restore: a format-changing upgrade has no undo, so stay on
  the new image and fix forward. The kept previous image and the link swap are all an
  upgrade relies on ([evidence/lifecycle.md](evidence/lifecycle.md) shows a schema 62 image
  refusing a store a schema 63 image had opened).
- Limit without a schema change: the move to PurRDF fb02dd38b (#28) keeps
  schema 67 but advances the store's RDF profile at the first open. An image
  built before it then refuses with `RDF profile changed`, and the schema
  check does not catch this, so `--rollback` stops the service. Run
  `--rollback` again to swap forward.
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

- Symptom: the journal has `sparrowd: archive pass failed; the watermark did not move`
  followed by an I/O error, a missing `STORE_ID`, or "archive directory ... is not on its
  expected mount".
- Effect: none on ingestion. The archive worker runs on its own thread and the daemon starts and
  serves without the NAS; the store keeps what it could not archive and the watermark does not
  move, so nothing is retired before it is archived. The disk budget and the free-space floor
  then apply as usual.
- "Not on its expected mount" means the archive directory is on the same device as `/`: the share
  is not mounted and its mount point is an ordinary local directory. The worker wrote, moved and
  deleted nothing. Never create the archive on the local disk instead.
- Diagnose: `findmnt -T /net/synology/archives/sparrow-archive` must show `nfs4` from
  `192.168.1.1:/volume1/archives`.
- Recover: restore the mount; the next pass retries by itself. The unit and the profile
  grant no archive path yet. Sparrow issues #15 to #17 add the archive worker's deployment.

## Revert the cutover to the user unit

- When: the system unit cannot serve after the cutover of Sparrow issue #22,
  and the operator's old user unit must serve `/SPARROW` again.
- Limit: this revert used to restore the issue's step 5a online backup. That backup feature was
  removed in #330 and none exists, so the revert keeps the cutover's store in place and serves it
  with the user unit's image, which must be able to open it (the store fence refuses an image
  older than the one that last upgraded the store's format). Anything the system unit
  ingested stays in the store.
- Before the cutover (`stat`, 2026-11-02), `/SPARROW` was `paudley:root` 0700
  with no ACL entries, `/SPARROW/store` was `paudley:paudley` 0700, and
  nothing else existed under `/SPARROW`.

`deploy/uninstall.sh` makes the host changes needed to return to that state:

- It disables and removes the system units.
- It unloads and removes the AppArmor profile.
- It removes `/etc/tmpfiles.d/sparrow.conf`, which would re-own `/SPARROW` at
  the next boot, and the `/usr/local/bin/sparrow` link.
- It runs `daemon-reload`.
- It gives `/SPARROW` back to the operator as `paudley:root` 0700 without
  ACLs. `/SPARROW/store` and `/SPARROW/intake` go to `paudley:paudley`, with
  directories at 0700 and no ACLs.

It checks first, changing nothing, that no Sparrow system unit is active and
that the sysusers file names exactly one operator. It leaves `/etc/sparrow`,
the `sparrow` user and group, the installed images and `/SPARROW/ship` in place, and says so.
Once `/SPARROW` is paudley's again, the user manager does not need the `sparrow` group.

Stop the shipper timer and the system unit through its drain first (`sudo systemctl stop
sparrow-ship.timer`, then `sudo deploy/stop.sh`), run `sudo deploy/uninstall.sh`, point the user
shipper's `ship.toml` at `/SPARROW/intake`, and enable the user units. Reversible: run
`install.sh` again.

Unverified: `uninstall.sh` was exercised only in staged mode
(`SPARROW_DESTDIR`) against a test instance's root, where it removes the
staged files and prints the host commands without running them
([evidence/lifecycle.md](evidence/lifecycle.md)). Neither the script as root
nor this revert has run on a host.

## Exposure

`systemd-analyze security` scores the units 1.6 (`sparrow.service`), 0.4
(`sparrow-ship.service`), each under the
fleet target of 2.0 ([evidence/systemd-analyze-security.md](evidence/systemd-analyze-security.md)).
Each remaining item and why it stays:

| Unit | Item | Why |
| --- | --- | --- |
| all | `RootDirectory=`/`RootImage=` | The daemon and helpers use the host's libraries; `ProtectSystem=strict` makes the host read-only to them instead. |
| all | `RestrictAddressFamilies=~AF_UNIX` | The private socket and the journal are Unix sockets. |
| all | `PrivateUsers=` | The daemon's port 518 needs the bind capability in the host's user namespace, and every unit reads or writes files whose real owners must stay mapped (the store, the roots, the producers' files). |
| all | `DeviceAllow=char-rtc:r` | Added by `ProtectClock=yes` itself; read-only clock access. |
| `sparrow` | `PrivateNetwork=`, `RestrictAddressFamilies=~AF_(INET\|INET6)`, `IPAddressDeny=` | The public Cap'n Proto listener on 0.0.0.0:518 serves remote agents on any trusted network; CHAP guards it. |
| `sparrow` | `AmbientCapabilities=`, `CAP_NET_BIND_SERVICE` | Binding port 518, Patrick's ruling of 2026-11-01; the bounding set holds nothing else. |
| `sparrow` | `RestrictSUIDSGID=` | systemd implements it with a seccomp rule that cannot inspect `openat2`'s mode, so it refuses `openat2` outright, and the daemon needs `openat2` to open files beneath a consume root without following symlinks (measured, [evidence/lifecycle.md](evidence/lifecycle.md)). `NoNewPrivileges=yes` makes a setuid file inert, and the daemon creates every file with a fixed mode. |
| `ship` | `ProtectHome=read-only` | It reads the producers' directories under `/home/paudley`. |

Hardening settings the specification lists that the units do not use, and why:
`StateDirectory=`, `RuntimeDirectory=`, `CacheDirectory=` and `LogsDirectory=`
on `sparrow.service`, because the store is `/SPARROW/store` by ruling, the
daemon keeps no runtime or cache files and it logs to the journal; `WatchdogSec=`, because the
daemon does not implement the systemd notification protocol (`Type=exec`).

The timer's service is not confined by AppArmor: it is a Python
program that runs `rsync`, `b3sum`, `sync` and the CLI, and its profile is
follow-up work. Their systemd sandboxes confine their writes.
