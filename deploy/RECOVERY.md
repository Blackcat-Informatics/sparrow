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
| Store (catalog, bytes, indexes, WAL, watch catalog, consume journals) | `/SPARROW/store` | `sparrow-backup` (closed-store copy) |
| Files waiting in consume roots | `/SPARROW/intake/NAME` | `sparrow-backup`, taken only when every root is empty |
| Shipper queue, ledgers, lock | `/SPARROW/ship` | `sparrow-backup` |
| Configuration, watch definitions, CHAP secret | `/etc/sparrow` | `sparrow-backup` |
| Images and lexicons | `/usr/local/libexec/sparrow`, `/usr/local/share/sparrow` | Each backup's `images/` and `lexicons/` keep the pair that last opened its store and its bundle; everything else is rebuilt from the repository into `./bin` |
| Units, profile, layout | `/etc/systemd/system`, `/etc/apparmor.d`, `/etc/{sysusers,tmpfiles}.d` | This directory (`install.sh`) |

The closed-store backups in `/SPARROW/backups` share the store's disk, so they
protect against software faults and operator error, not against losing the
disk. The NAS archive, the off-host backup, is not deployed yet (Sparrow
issues #15 to #17).

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
  anything. Stay on the new image and fix forward, or restore the named
  pre-upgrade backup `/SPARROW/backups/pre-upgrade-IMAGE` ("Restore from a
  backup"). `deploy/pre-upgrade-backup.sh` takes it, and `restart.sh` requires
  it before it raises the schema. It must be no older than the image link's
  last start and no older than 24 hours (README.md, "Upgrades and rollback")
  ([evidence/lifecycle.md](evidence/lifecycle.md) shows a schema 62 image
  refusing a store a schema 63 image had opened, and the restore). Reverting `/etc/sparrow/server.toml` is a
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

- Not applicable yet: the archive worker is not deployed, and neither the unit
  nor the profile names the NAS, so a NAS outage does not affect the service.
  Sparrow issues #15 to #17 add the archive and this entry's procedure.

## Restore from a backup

A backup `/SPARROW/backups/NAME` (a `STAMP`, or a named `pre-upgrade-IMAGE`)
holds `store`, `intake` and `config` (the server configuration, `/etc/sparrow`
with the secret, and `/SPARROW/ship`) from one moment, `images/` with the
image pair that last opened the store, `lexicons/` with the bundle it read, and `SOURCE`, which names that image
(`image=`) and the store's identity, schema and format. Qualify it on a disposable copy first with the recovery probe
(docs/deployment.md, "Recovery qualification"), and never start `sparrowd` on
such a copy: its watch catalog names the live consume roots.

```bash
sudo systemctl stop sparrow-backup.timer sparrow-ship.timer
sudo deploy/stop.sh
sudo mv /SPARROW/store /SPARROW/store.damaged
sudo mv /SPARROW/intake /SPARROW/intake.damaged
sudo mv /SPARROW/ship /SPARROW/ship.damaged
sudo cp -a --reflink=auto /SPARROW/backups/NAME/store /SPARROW/store
sudo cp -a --reflink=auto /SPARROW/backups/NAME/intake /SPARROW/intake
sudo cp -a --reflink=auto /SPARROW/backups/NAME/config/ship /SPARROW/ship
sudo systemd-tmpfiles --create sparrow.conf
sudo deploy/start.sh --restored-from /SPARROW/backups/NAME
sudo deploy/check.sh --store-id STORE_ID
sudo systemctl start sparrow-ship.timer sparrow-backup.timer
```

`start.sh --restored-from` starts exactly the image `SOURCE` names, taken from
`/usr/local/libexec/sparrow`, `./bin` or the backup's `images/`, and fails
unless the daemon reports the store identity `SOURCE` records. Never start a
restored store with plain `start.sh`: it picks the newest image, which may
upgrade the restored store and lose the way back again. `STORE_ID` is the
identity recorded before the failure. Restore
`/etc/sparrow` from `BACKUP/config/sparrow` only if it was lost; on a fresh
host it always was, so run this before `deploy/start.sh`:

```bash
sudo cp -a --reflink=auto /SPARROW/backups/NAME/config/sparrow/. /etc/sparrow/
sudo stat -c '%U %a' /etc/sparrow/tcp.secret
ls /usr/local/share/sparrow
grep text_lexicon_dir /etc/sparrow/server.toml
```

Expect `root 600`, and a `text_lexicon_dir` naming a bundle installed on this
host: the same PurRDF revision, or the open is refused ("Refused open"). If
`install.sh` installed a different bundle, install the backup's:
`sudo cp -a --no-preserve=ownership /SPARROW/backups/NAME/lexicons/. /usr/local/share/sparrow/`. The shipper
then resends every file shipped after the backup whose original still exists;
records producers sent directly after the backup are lost unless they resend
them. On a fresh host, follow README.md steps 1 to 3 first, then this
procedure with the backup copied onto the host. The store part of this
procedure (a backup by `sparrow-backup`, a schema raise, then the restored
store started with `--restored-from` on the recorded image) was exercised on a
test instance ([evidence/lifecycle.md](evidence/lifecycle.md)); the whole
system-unit restore has not been.

A restored `pre-upgrade-IMAGE` copy is older than the start that used it, so
the schema guard refuses it before the next upgrade. Rename it
(`.retired-STAMP`) and take a new one with `sudo deploy/pre-upgrade-backup.sh`.

## Revert the cutover to the user unit

- When: the system unit cannot serve after the cutover of Sparrow issue #22,
  and the operator's old user unit must serve `/SPARROW` again.
- Needs: the issue's step 5a backup,
  `/SPARROW/backups/pre-upgrade-sparrowd-schema62-600ee97`, taken from the
  closed store before `install.sh` ran, and the user units, disabled but not
  removed.
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
the `sparrow` user and group, the installed images, `/SPARROW/ship` and
`/SPARROW/backups` in place, and says so. Once `/SPARROW` is paudley's again,
the user manager does not need the `sparrow` group.

First, if the system unit still serves, drain it as the issue's step 3 does.
Stop the timers, then repeat the two counts until both are `0`:

```bash
sudo systemctl stop sparrow-ship.timer sparrow-backup.timer
sudo /usr/local/bin/sparrow /SPARROW/store/rpc.sock status | jq '[.consume_watches[].pending_claims] | add'
sudo find /SPARROW/intake -type f -not -path '*/.sparrow-inflight/*' | wc -l
```

A unit that cannot serve is not drained. The steps below set its intake aside
anyway.

```bash
(
set -euo pipefail
B=/SPARROW/backups/pre-upgrade-sparrowd-schema62-600ee97
sudo grep -x image=sparrowd-schema62-600ee97 "$B/SOURCE"
# Before install.sh has run there is no system unit to stop.
if [ -e /etc/systemd/system/sparrow.service ]; then
    sudo systemctl stop sparrow-ship.timer sparrow-backup.timer
    sudo deploy/stop.sh
fi
# The store and the intake both go back to step 5a, so they agree: the system unit's roots can
# hold claims journaled only in its store, and the restored store's daemon then refuses to start.
sudo mv -T /SPARROW/store /SPARROW/store.cutover
sudo cp -a --reflink=always "$B/store" /SPARROW/store
sudo mv -T /SPARROW/intake /SPARROW/intake.cutover
sudo cp -a --reflink=always "$B/intake" /SPARROW/intake
# Gives /SPARROW and the restored store and intake back to the operator.
sudo deploy/uninstall.sh
# The shipper renames queued items into the roots, so its state must share their mount; /home
# is a separate mount. A fresh copy of the pre-cutover state, so it resends what the restored
# store lacks.
test ! -e /SPARROW/ship.user
cp -a --reflink=auto ~/.local/state/sparrow/ship /SPARROW/ship.user
sed -i -e 's|^intake_root = .*|intake_root = "/SPARROW/intake"|' \
    -e 's|^state_dir = .*|state_dir = "/SPARROW/ship.user"|' ~/.config/sparrow/ship.toml
grep -qx 'intake_root = "/SPARROW/intake"' ~/.config/sparrow/ship.toml
grep -qx 'state_dir = "/SPARROW/ship.user"' ~/.config/sparrow/ship.toml
# The old backup timer stays disabled: its unit drains the old intake root (see below).
systemctl --user enable --now sparrow.service sparrow-ship.timer
~/.local/bin/sparrow /SPARROW/store/rpc.sock status | jq -r .store_id
)
```

The subshell stops at the first failure. Expect the store identity recorded
before the cutover, served by `sparrowd-schema62-600ee97`. Then:

- The shipper. The backup was taken after the watches moved to
  `/SPARROW/intake`, so the user shipper's `intake_root` moves there too.
  The shipper moves each queued item into a root with `rename`, which fails
  across mounts, and `/home` is a separate mount from `/`, where `/SPARROW`
  lives. So `state_dir` moves onto that mount as well, as
  `/SPARROW/ship.user`. That is a copy of the pre-cutover state from
  `~/.local/state/sparrow/ship`, not the system shipper's `/SPARROW/ship`.
  The system shipper's ledgers mark everything it shipped as delivered, and
  the restored store lacks those records. From the older state, the user
  shipper resends every file shipped since the backup whose original still
  exists. `/SPARROW/ship` does not exist before `install.sh` runs, and is
  empty until the issue's step 7.
- The backup timer. The old user `sparrow-backup.timer` stays disabled. Its
  unit passes `~/.local/state/sparrow/intake` as the intake root, so its drain
  check would ignore files waiting under `/SPARROW/intake`; it had also failed
  before the cutover. Re-enable it only after its intake argument names
  `/SPARROW/intake`. Until then, the step 5a backup and
  `/SPARROW/store.cutover` are the copies.
- The intake. The system unit's roots go aside as `/SPARROW/intake.cutover`,
  and the step 5a roots replace them. Those are empty, hold no claims, and
  agree with the restored store. Kept roots could hold files claimed under
  `.sparrow-inflight/` and journaled only in the system store. The restored
  store does not know those claims, so its daemon refuses to start. It
  reports "unrecognized private claim entry", or "private claim directory
  exceeds its bounded journal registry" when the restored store journals no
  claim for that root ([evidence/lifecycle.md](evidence/lifecycle.md)). Kept roots could also hold
  waiting files that the user shipper then sends again, which ingests them
  twice. Nothing in `intake.cutover` is lost: each file there is a copy that
  the system shipper fed, and the user shipper resends it from its original.
- The system unit. Records sent directly to it, not through the shipper, are
  lost. Keep `/SPARROW/store.cutover` and `/SPARROW/intake.cutover` until that
  loss is accepted.

Reversible: run `install.sh` again.

Unverified: `uninstall.sh` was exercised only in staged mode
(`SPARROW_DESTDIR`) against a test instance's root, where it removes the
staged files and prints the host commands without running them. Neither the
script as root nor this revert has run on a host.

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

Hardening settings the specification lists that the units do not use, and why:
`StateDirectory=`, `RuntimeDirectory=`, `CacheDirectory=` and `LogsDirectory=`
on `sparrow.service`, because the store is `/SPARROW/store` by ruling, the
daemon keeps no runtime or cache files and it logs to the journal (the backup
unit does use `RuntimeDirectory=` for its marker); `WatchdogSec=`, because the
daemon does not implement the systemd notification protocol (`Type=exec`).

The two timers' services are not confined by AppArmor: they are Python
programs that run `cp`, `sync`, `systemctl` and the CLI, and their profiles are
follow-up work. Their systemd sandboxes confine their writes.
