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
| Store (catalog, bytes, indexes, WAL, watch catalog, consume journals) | `/SPARROW/store` | The daemon's online backup (`sparrowd` writes it into `backup_root`, asked by `sparrow-backup`) |
| Files waiting in consume roots | `/SPARROW/intake/NAME` | `sparrow-backup`, copied online, so it may hold files the store has not claimed |
| Shipper queue, ledgers, lock | `/SPARROW/ship` | `sparrow-backup` |
| Configuration, watch definitions, CHAP secret | `/etc/sparrow` | `sparrow-backup` |
| Images and lexicons | `/usr/local/libexec/sparrow`, `/usr/local/share/sparrow` | Each backup's `images/` and `lexicons/` keep the pair that last opened its store and its bundle; everything else is rebuilt from the repository into `./bin` |
| Units, profile, layout | `/etc/systemd/system`, `/etc/apparmor.d`, `/etc/{sysusers,tmpfiles}.d` | This directory (`install.sh`) |

The online backups live on the NAS, under
`/net/synology/archives/sparrow` (an NFSv4 mount of the Synology), and nowhere
else: Sparrow never backs up to a local disk (Patrick, 2026-11-08). No backup
stops, pauses or drains the service. The daemon copies its store with RocksDB's
backup engine into `engine/` there, and `sparrow-backup` copies the files beside
the store into `sets/ID`, where `ID` is the engine's backup id. Table files are
shared between backups, so every backup after the first copies only what is
new, and the engine keeps the newest `backup_keep`. Each set's `MANIFEST.b3`
holds the b3sum of its files and of the engine files its backup references,
read back from the NAS before the set is renamed into place. The daemon's own
archive worker, which will write per-store directories under the same root, is
not deployed yet (Sparrow issues #15 to #17).

The store in a backup is the store as of a point inside the daemon's copy. The
set's files are copied after that point, so they may be newer than the store: a
file the shipper or a producer put in the intake after it is in the set and not
in the store, and a file the daemon consumed between the two is in neither. That
is the same loss window as any record accepted after the backup point; "Restore
from a backup" says what the shipper does about it.

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
- Limit: an image cannot undo a store-format upgrade its successor made.
  `start.sh` and `restart.sh` therefore refuse an image whose schema number
  (`sparrowd-schemaN-REV`) is lower than the current image's, before stopping
  anything. Stay on the new image and fix forward, or restore the named
  pre-upgrade backup `/net/synology/archives/sparrow/pre-upgrade-IMAGE` ("Restore from a
  backup"). `deploy/pre-upgrade-backup.sh` takes it, and `restart.sh` requires
  it before it raises the schema. It must be no older than the image link's
  last start and no older than 24 hours (README.md, "Upgrades and rollback")
  ([evidence/lifecycle.md](evidence/lifecycle.md) shows a schema 62 image
  refusing a store a schema 63 image had opened, and the restore). Reverting `/etc/sparrow/server.toml` is a
  separate step, kept in its backup.
- Limit without a schema change: the move to PurRDF fb02dd38b (#28) keeps
  schema 67 but advances the store's RDF profile at the first open. An image
  built before it then refuses with `RDF profile changed`, and the schema
  check does not catch this, so `--rollback` stops the service. Run
  `--rollback` again to swap forward, or restore the named pre-upgrade
  backup. `restart.sh` does not require that backup when the schema stays
  the same: take it by hand with `deploy/pre-upgrade-backup.sh` before that
  restart.
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

- Symptom: `sparrow-backup` fails with "backup root ... is on autofs (or
  btrfs, or an unknown filesystem) ..., not a network filesystem", or the
  daemon's backup ends `failed` (`sparrow SOCKET status`, `backup.failure`)
  with an I/O error or naming a root whose filesystem is not a network one (the
  daemon judges the root at every backup, not at startup, so a NAS that is down
  at boot never stops ingestion), or a copy or read-back step of the client fails.
- Effect: none on ingestion. The backup runs on its own lowest-priority
  thread and takes no lock the daemon's writers use, so ingestion continues
  while the NAS is down or slow. The client refuses a root on the wrong
  filesystem before it asks the daemon for anything. A failed run leaves
  `sets/ID.partial` on the NAS and possibly unreferenced engine files, which
  the engine discards at its next open and the next run removes. While a backup
  runs, RocksDB keeps the files that become obsolete, which a backup hung on a
  dead mount would keep indefinitely. They do not count toward the disk budget
  or eviction, and the daemon's guard deletes them once free space falls below
  twice `min_free_bytes`, failing that backup; the store's disk use then shows
  in `df` rather than in `physical_bytes`.
- Diagnose: `findmnt -T /net/synology/archives/sparrow` must show `nfs4` from
  `192.168.1.1:/volume1/archives`. `sparrow SOCKET status` shows the backup's
  `phase`, `started_at` and `failure`.
- Recover: restore the mount, then run the backup again. A daemon shutdown
  during a backup abandons it. Never point the backup at a local directory
  instead: the client refuses it before asking, the daemon fails the backup and
  creates nothing there, and Patrick's ruling forbids it.
- The daemon's archive worker is not deployed. The daemon unit and the profile
  grant only the backup root. Sparrow issues #15 to #17 add the archive worker
  and its procedure.

## Verify a backup

A backup counts as verified only after `deploy/verify-backup.sh` passes on it.
Run it as the operator, who must be able to read the backup and the live server
configuration. `BACKUP` is a set, `BACKUP_ROOT/sets/ID` (or the
`pre-upgrade-IMAGE` link to one):

```bash
deploy/verify-backup.sh /net/synology/archives/sparrow/sets/ID; echo "exit=$?"
```

The script reads the live `data_dir` and `socket_path` from `SPARROW_CONFIG`
(default `/etc/sparrow/server.toml`) and refuses to run when it cannot read
them, since those are the paths a copy must never touch. It never opens the
backup itself and never starts a daemon on the live store, socket or intake.

1. It checks every file `MANIFEST.b3` lists, the set's files and the engine
   files the backup references, against the NAS with `b3sum`, and compares the
   set's file list with the manifest both ways.
2. It restores engine backup `ID` into a scratch directory below
   `/SPARROW/verify/` (`SPARROW_VERIFY` overrides it) with the image `SOURCE`
   names, `sparrowd --restore-backup`, which checks every file's checksum as it
   reads it. The scratch filesystem must have the backup's `engine_bytes` plus
   10 GiB free. The scratch copy is not a backup: the script removes it however
   the run ends. Before it creates the directory, it refuses any path that
   resolves under the live store, the live socket, the intake
   (`SPARROW_INTAKE`), the backup root, or the `data_dir` and `intake` the
   set's `SOURCE` and configuration copy record.
3. It writes a configuration for the copy with its own socket and the lexicon
   bundle the set keeps, and starts that image with `--query-only`. In that mode
   the daemon opens no watch root, claims no file, runs no retention or eviction
   and refuses every write (docs/interfaces.md, "Query-only daemon"). Opening
   still rewrites some catalog rows and table files in the copy, which is why it
   works only on a disposable copy.

It prints one line per check:

| Check | Passes when |
| --- | --- |
| `manifest` | Every file `MANIFEST.b3` lists matches its b3sum on the NAS, and the set holds no other file |
| `restore` | The backup restored from the root with every file's checksum checked |
| `query-only` | Status and the startup line report `query_only`, and no stored watch root is open |
| `store-id` | The copy's store identity equals `store_id` in `SOURCE` |
| `status` | `pressure.phase` is `ready` and `durability.failure` is `null` |
| `records` | A `records` window over the hour before `taken_utc` returns rows |
| `artifact` | The first row's artifact reads back to the BLAKE3 digest its catalog row records |
| `shutdown` | The daemon exits cleanly after SIGINT and the copy is removed |

It exits 0 only when every check passed, 1 at the first failed check, and 2 on a
usage error. It always stops the daemon and removes the scratch copy. A backup is
not compared by record or payload count: the store is copied while it ingests,
so no count taken at the live daemon describes the same instant.

The `records` and `artifact` checks run only while the window is younger than the
retention window the daemon reports (`retention_seconds`, 30 days), because a
record expires by the reader's clock. After that the script prints that both
were skipped. An image without query-only mode (schema 66 and older) is refused
before it starts, because it would consume the live intake folders. Such a
backup can be opened with a newer image instead, with `--image DAEMON`; the
newer image may upgrade the disposable copy as it opens it.

`--time-records` prints, after the checks, five elapsed times of the same
`records` window, for comparing read latency on an older store format with the
live service. `--window FROM BEFORE` selects that window (RFC 3339 instants), so
it can be an hour held by both the backup and the live store. The window must be
younger than the retention window, or the timing fails. The script prints the
exact command, so the live times can be taken the same way.

## Restore from a backup

A set `/net/synology/archives/sparrow/sets/ID` (or a `pre-upgrade-IMAGE` link to
one) holds `intake` and `config` (the server configuration as `server.toml`,
`/etc/sparrow` as `sparrow` with the secret, and `/SPARROW/ship` as `ship`),
`images/` with the image pair that last opened the store, `lexicons/` with the
bundle it read, `SOURCE`, and `MANIFEST.b3`. `SOURCE` names the image (`image=`),
the engine backup (`backup_id=`), the store's identity, schema and format,
`taken_utc`, the data and intake paths and the engine's file and byte counts. The
store itself is not in the set. It is engine backup `ID` in
`BACKUP_ROOT/engine`, and `sparrowd --restore-backup` rebuilds it. Restore only a
backup that `deploy/verify-backup.sh` has passed ("Verify a backup").

`sparrowd --restore-backup BACKUP_ROOT [BACKUP_ID] EMPTY_DIR` runs offline, with
no daemon, socket or store open. It restores into an empty or absent directory
and refuses a directory that holds anything, so it can never overwrite a store.
It checks every file's checksum as it reads it. Without `BACKUP_ID` it restores
the latest backup, which is not necessarily the one a set describes, so name the
set's `backup_id`.

```bash
sudo systemctl stop sparrow-backup.timer sparrow-ship.timer
sudo deploy/stop.sh
sudo mv /SPARROW/store /SPARROW/store.damaged
sudo mv /SPARROW/intake /SPARROW/intake.damaged
sudo mv /SPARROW/ship /SPARROW/ship.damaged
ROOT=/net/synology/archives/sparrow
B=$ROOT/sets/SET
IMAGE=$(sed -n 's/^image=//p' "$B/SOURCE")
ID=$(sed -n 's/^backup_id=//p' "$B/SOURCE")
sudo mkdir /SPARROW/store
sudo chattr +C /SPARROW/store
sudo "$B/images/$IMAGE" --restore-backup "$ROOT" "$ID" /SPARROW/store
sudo cp -a "$B/intake" /SPARROW/intake
sudo cp -a "$B/config/ship" /SPARROW/ship
sudo sh -c "cd $ROOT && grep -E '^\\\\?[0-9a-f]{64}  sets/$ID/(intake|config)/' $B/MANIFEST.b3 | b3sum --check --quiet"
sudo systemd-tmpfiles --create sparrow.conf
sudo deploy/start.sh --restored-from "$B"
sudo deploy/check.sh --store-id STORE_ID
sudo systemctl start sparrow-ship.timer sparrow-backup.timer
```

The restored store directory has copy-on-write off before anything is written
into it, so every restored file inherits that ("COW off everywhere"). The
`b3sum --check` line reads the restored intake and configuration back from the
set and prints nothing when every file matches its manifest line; any output, or
a nonzero exit, means the copy is damaged, so copy again before starting. It
checks only the files of the set: `--restore-backup` has already checked the
store's files. Replace `SET` in `B` with the set's number; `ID` is read from its
`SOURCE`.

`start.sh --restored-from` takes the set directory, reads `image=` and
`store_id=` from its `SOURCE`, and starts exactly that image, taken from
`/usr/local/libexec/sparrow`, `./bin` or the set's `images/`. It fails unless the
daemon reports the store identity `SOURCE` records. It does not restore or move
the store: it expects `data_dir` to be restored already, and it refuses while a
backup client holds `/net/synology/archives/sparrow/.lock`. It needs
`BACKUP/SOURCE` and `BACKUP/images/`, which a set has and the bare backup root
does not, so pass the set, never the root. Never start a restored store with
plain `start.sh`: it picks the newest image, which may upgrade the restored store
and lose the way back again. `STORE_ID` is the identity recorded before the
failure. Restore `/etc/sparrow` from `BACKUP/config/sparrow` only if it was lost;
on a fresh host it always was, so run this before `deploy/start.sh`:

```bash
sudo cp -a /net/synology/archives/sparrow/sets/ID/config/sparrow/. /etc/sparrow/
sudo stat -c '%U %a' /etc/sparrow/tcp.secret
ls /usr/local/share/sparrow
grep text_lexicon_dir /etc/sparrow/server.toml
```

Expect `root 600`, and a `text_lexicon_dir` naming a bundle installed on this
host: the same PurRDF revision, or the open is refused ("Refused open"). If
`install.sh` installed a different bundle, install the set's:
`sudo cp -a --no-preserve=ownership /net/synology/archives/sparrow/sets/ID/lexicons/. /usr/local/share/sparrow/`.

The intake and the store are not from the same instant. The set's intake is
copied online after the store's backup point, and the daemon consumes into the
store while it is copied. A restored intake can therefore hold a file the store
already ingested before the backup point (it is ingested again), and a file
consumed between the backup point and the copy is in neither. An intake root's
`.sparrow-inflight/` can also hold a private claim entry the restored store does
not journal; the daemon then refuses to start with "unrecognized private claim
entry" (or "private claim directory exceeds its bounded journal registry"). Move
each such entry out of the intake root, keeping its bytes, never delete it, and
start again. A claim the restored store journals whose private source is missing
never completes: `pending_claims` shows it and the watch failure reads
"unpublished claimed source is missing". Once its bytes are confirmed lost,
retire it with `sparrow SOCKET watch retire-claim WATCH_ID CLAIM_ID "REASON"`
(docs/interfaces.md, "Retiring an unrecoverable claim").

The shipper then resends every file shipped after the backup whose original
still exists; records producers sent directly after the backup are lost unless
they resend them. On a fresh host, follow README.md steps 1 to 3 first, then this
procedure with the NAS mounted. The recorded test-instance run
([evidence/lifecycle.md](evidence/lifecycle.md)) restored a store with
`start.sh --restored-from` on the recorded image, but from the earlier
closed-store backup; neither the online backup's restore (`--restore-backup`,
the set's intake and configuration) nor the whole system-unit restore has been
exercised on a host.

A restored `pre-upgrade-IMAGE` link is older than the start that used it, so the
schema guard refuses it before the next upgrade. Rename it (`.retired-STAMP`)
and take a new one with `sudo deploy/pre-upgrade-backup.sh`.

## Revert the cutover to the user unit

- When: the system unit cannot serve after the cutover of Sparrow issue #22,
  and the operator's old user unit must serve `/SPARROW` again.
- Needs: the issue's step 5a backup and the user units, disabled but not
  removed. The step 5a copy,
  `/SPARROW/backups/pre-upgrade-sparrowd-schema62-600ee97`, was a closed
  copy on the local disk, and was deleted on 2026-11-08 with every local backup
  (Patrick's ruling forbids them). A revert now needs an online backup set on
  the NAS, taken while a daemon with the backup operation served the store
  (`/net/synology/archives/sparrow/pre-upgrade-IMAGE`, or a `sets/ID`); set `B`
  to it. The commands below restore its store with the image its `SOURCE`
  names. A schema 62 daemon has no backup operation, so the issue's original
  step 5a cannot be repeated online.
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
the `sparrow` user and group, the installed images, `/SPARROW/ship` and the
backups on the NAS in place, and says so. Once `/SPARROW` is paudley's again,
the user manager does not need the `sparrow` group.

Optionally, drain a system unit that still serves. This only lets it consume
what its shipper already fed, so those records land in `store.cutover` rather
than waiting as bytes in `intake.cutover`. The restored store gains nothing
either way, so skip the drain when the unit cannot serve. It gives up after
10 minutes (60 rounds of 10 s). If either count is still above `0` then,
continue with the revert anyway; whatever remains stays in `intake.cutover`.

```bash
sudo systemctl stop sparrow-ship.timer sparrow-backup.timer
for i in $(seq 60); do
    claims=$(sudo /usr/local/bin/sparrow /SPARROW/store/rpc.sock status | jq '[.consume_watches[].pending_claims] | add') || claims=unknown
    waiting=$(sudo find /SPARROW/intake -type f -not -path '*/.sparrow-inflight/*' | wc -l)
    echo "pending claims $claims, waiting files $waiting"
    [ "$claims" = 0 ] && [ "$waiting" = 0 ] && break
    sleep 10
done
```

Then revert. The subshell stops at the first failure and is safe to run
again after one: each step skips once it is done.

```bash
(
set -euo pipefail
OLD="$HOME/.local/bin/sparrow /SPARROW/store/rpc.sock"
B=/net/synology/archives/sparrow/pre-upgrade-IMAGE
IMAGE=$(sudo sed -n 's/^image=//p' "$B/SOURCE")
[ -n "$IMAGE" ]
SET=$(readlink -f "$B")
ROOT=${SET%/sets/*}
ID=$(sudo sed -n 's/^backup_id=//p' "$B/SOURCE")
# Names this revert in the marker each set-aside writes, so a .cutover left by anything else
# (an earlier revert from another backup, or a hand move) is refused, not taken as done.
MARK="revert from $B taken_utc=$(sudo sed -n 's/^taken_utc=//p' "$B/SOURCE")"
# Before install.sh has run, and after uninstall.sh, there is no system unit to stop.
if [ -e /etc/systemd/system/sparrow.service ]; then
    sudo systemctl stop sparrow-ship.timer sparrow-backup.timer
    # A helper run already under way finishes on its own. Both helpers are oneshot units,
    # "activating" while they run, never "active", so test the state rather than is-active's
    # exit status. A backup copies over the network and can take a long time, so wait up to
    # 60 minutes, then stop here.
    waited=0
    while :; do
        busy=
        for helper in sparrow-ship.service sparrow-backup.service; do
            state=$(systemctl show -p ActiveState --value "$helper")
            case $state in inactive | failed) ;; *) busy="$busy $helper=$state" ;; esac
        done
        [ -z "$busy" ] && break
        [ "$waited" -lt 3600 ] || { echo "still running after 60 minutes:$busy" >&2; exit 1; }
        echo "waiting for$busy"
        sleep 10; waited=$((waited + 10))
    done
    sudo deploy/stop.sh
fi
# The store and the intake both go back to step 5a, so they agree: the system unit's roots can
# hold claims journaled only in its store, and the restored store's daemon then refuses to start.
# The live name is set aside once, after its marker is written; the copy is made under .restoring
# and renamed into place only when complete. So after the set-aside, an existing live name is
# always the full copy.
for part in store intake; do
    mark=/SPARROW/$part.cutover.revert
    if [ -e "/SPARROW/$part.cutover" ] && [ "$(sudo cat "$mark" 2>/dev/null || true)" != "$MARK" ]; then
        echo "/SPARROW/$part.cutover exists, but this revert did not set it aside ($mark); move it away first" >&2
        exit 1
    fi
    if [ ! -e "/SPARROW/$part.cutover" ]; then
        printf '%s\n' "$MARK" | sudo tee "$mark" >/dev/null
        sudo mv -T "/SPARROW/$part" "/SPARROW/$part.cutover"
    fi
    if [ ! -e "/SPARROW/$part" ]; then
        sudo rm -rf "/SPARROW/$part.restoring"
        sudo mkdir "/SPARROW/$part.restoring"
        sudo chattr +C "/SPARROW/$part.restoring"
        if [ "$part" = store ]; then
            sudo "$B/images/$IMAGE" --restore-backup "$ROOT" "$ID" "/SPARROW/$part.restoring"
        else
            sudo cp -a "$B/$part/." "/SPARROW/$part.restoring/"
        fi
        sudo mv -T "/SPARROW/$part.restoring" "/SPARROW/$part"
    fi
done
# Gives /SPARROW and the restored store and intake back to the operator.
sudo deploy/uninstall.sh
# The shipper renames queued items into the roots, so its state must share their mount; /home
# is a separate mount. A fresh copy of the pre-cutover state, so it resends what the restored
# store lacks.
if [ ! -e /SPARROW/ship.user ]; then
    rm -rf /SPARROW/ship.user.partial
    cp -a --reflink=auto ~/.local/state/sparrow/ship /SPARROW/ship.user.partial
    mv -T /SPARROW/ship.user.partial /SPARROW/ship.user
fi
sed -i -e 's|^intake_root = .*|intake_root = "/SPARROW/intake"|' \
    -e 's|^state_dir = .*|state_dir = "/SPARROW/ship.user"|' ~/.config/sparrow/ship.toml
grep -qx 'intake_root = "/SPARROW/intake"' ~/.config/sparrow/ship.toml
grep -qx 'state_dir = "/SPARROW/ship.user"' ~/.config/sparrow/ship.toml
# The old backup timer stays disabled: its unit names the old intake root (see below).
systemctl --user enable --now sparrow.service sparrow-ship.timer
for i in $(seq 60); do $OLD status >/dev/null 2>&1 && break; sleep 2; done
$OLD status | jq -r .store_id
)
```

If it stops, fix the cause and run it again. For example, `uninstall.sh`
refuses while a system unit is still active. Each set-aside first writes
`/SPARROW/store.cutover.revert` or `/SPARROW/intake.cutover.revert`, naming
the backup and its `taken_utc`. A `.cutover` directory without a matching
marker was not made by this revert, so the subshell refuses it rather than
skipping the swap.

Expect `uninstall.sh` to list what it removed and ran, then the store identity
recorded before the cutover, served by `sparrowd-schema62-600ee97`, the image
the backup's `SOURCE` names.

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
  unit passes `~/.local/state/sparrow/intake` as the intake root, so it would
  copy the wrong tree; it must also carry the online lines of README.md,
  "Interim user unit". Re-enable it only after its intake argument names
  `/SPARROW/intake`. Until then, the step 5a backup and
  `/SPARROW/store.cutover` are the copies.
- The intake. The system unit's roots go aside as `/SPARROW/intake.cutover`,
  and the set's intake replaces them. It was copied online, so it can hold
  waiting files and private claim entries the restored store does not journal
  ("Restore from a backup" says what to do). Kept roots could hold files claimed under
  `.sparrow-inflight/` and journaled only in the system store. The restored
  store does not know those claims, so its daemon refuses to start. It
  reports "unrecognized private claim entry", or "private claim directory
  exceeds its bounded journal registry" when the restored store journals no
  claim for that root (rehearsed on a test instance,
  [evidence/lifecycle.md](evidence/lifecycle.md)). Kept roots could also hold
  waiting files that the user shipper then sends again, which ingests them
  twice. A file in `intake.cutover` is not lost if its original still exists:
  the user shipper resends it.

What the revert loses. The served store is the step 5a backup plus what the
user unit ingests afterwards. Three kinds of change made after the cutover
are not in it:

1. Records sent directly to the system unit, over TCP or its socket, not
   through the shipper. They survive only in `store.cutover`.
2. Files shipped after the cutover whose originals no longer exist, because a
   producer rotated or deleted them. The user shipper resends only files
   whose original still exists. Their records survive only in
   `store.cutover`, or as bytes in `intake.cutover` if the file was still
   waiting there. The longer the system unit ran, the more of these there
   are.
3. Catalog changes made on the system unit after step 8: watch definitions
   applied with `watch put`, pins, and other mutations. The restored catalog
   is the one from step 5a.

Keep `/SPARROW/store.cutover` and `/SPARROW/intake.cutover` until that loss is
accepted. Read `store.cutover` only on a copy with the recovery probe
(docs/deployment.md, "Recovery qualification"). Never start a daemon on it:
its watch catalog names the live roots.

To reapply the watches, start from the definitions installed at step 7, which
stay in `/etc/sparrow/watches/` (`uninstall.sh` keeps `/etc/sparrow`). The
restored store already holds the step 4 watches; the first command below lists
them. Reapply each changed or missing definition with the restored store's
current revision, or `0` for a new watch. A watch first applied after step 8
names a root that the restored step 5a intake lacks, so create it first, with
plain `mkdir` as the operator:

```bash
OLD="$HOME/.local/bin/sparrow /SPARROW/store/rpc.sock"
$OLD status | jq -r '.consume_watches[] | [.id, .revision, .root] | @tsv'
f=/etc/sparrow/watches/NAME.toml
sudo cat "$f" > ~/sparrow-revert-NAME.toml
id=$(sed -n 's/^id = "\(.*\)"$/\1/p' ~/sparrow-revert-NAME.toml)
root=$(sed -n 's/^root = "\(.*\)"$/\1/p' ~/sparrow-revert-NAME.toml)
[ -d "$root" ] || mkdir "$root"
rev=$($OLD status | jq -r --arg id "$id" '[.consume_watches[] | select(.id == $id) | .revision][0] // "0"')
$OLD watch put "revert-$id-$(date +%s)" "$rev" ~/sparrow-revert-NAME.toml
```

Pins and other mutations have no such record and must be redone by hand.

Reversible: run `install.sh` again.

Unverified: `uninstall.sh` was exercised only in staged mode
(`SPARROW_DESTDIR`) against a test instance's root, where it removes the
staged files and prints the host commands without running them. The swap
loop and the helper wait were rehearsed on a test instance's paths and a
transient oneshot unit
([evidence/lifecycle.md](evidence/lifecycle.md)). Neither the script as root
nor this revert has run on a host.

## Exposure

`systemd-analyze security` scores the units 1.6 (`sparrow.service`), 0.4
(`sparrow-ship.service`) and 1.2 (`sparrow-backup.service`), each under the
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
| `backup` | `User=` root, `CAP_DAC_OVERRIDE`, `CAP_DAC_READ_SEARCH` | It reads trees owned by `sparrow`, `paudley` and root and preserves no owners. It stops and signals nothing. Its writes are confined to the NAS backup root `/net/synology/archives/sparrow`. |

Hardening settings the specification lists that the units do not use, and why:
`StateDirectory=`, `RuntimeDirectory=`, `CacheDirectory=` and `LogsDirectory=`
on `sparrow.service`, because the store is `/SPARROW/store` by ruling, the
daemon keeps no runtime or cache files and it logs to the journal; `WatchdogSec=`, because the
daemon does not implement the systemd notification protocol (`Type=exec`).

The two timers' services are not confined by AppArmor: they are Python
programs that run `rsync`, `b3sum`, `sync` and the CLI, and their profiles are
follow-up work. Their systemd sandboxes confine their writes.
