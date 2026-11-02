<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Deploying Sparrow

This directory holds everything a host needs to run Sparrow in production: the
systemd units, the AppArmor profile, configuration templates, the helper
scripts and these documents. After a restore from backups onto a fresh host,
this directory and a clone of the repository are enough to deploy the service
again. Each backup keeps the image pair that last opened its store and the
lexicon bundle it read, which is enough to start the restored store (RECOVERY.md, "Restore from a backup"). To
build new images, and for the lexicon bundle and the recovery probe, step 2
needs the build prerequisites of step 1 and network access to the PurRDF
source that `Cargo.lock` names, or a copy of the old host's `./bin`. Secrets are never kept here; [Secrets](#secrets) names where each one
lives.

| Path | Contents |
| --- | --- |
| `README.md` | This guide: from a scratch clone to a running, verified service. |
| `RECOVERY.md` | Failure modes, how to diagnose and recover each, rollback, restore, exposure. |
| `MANUAL.md` | Running the daemon by hand without systemd, with the unit's environment. |
| `install.sh` | Idempotent install of everything below onto the host; prints what changed. |
| `uninstall.sh` | Undoes `install.sh`'s host changes so the old user unit can serve `/SPARROW` again. |
| `pre-upgrade-backup.sh` | Takes the named backup `restart.sh` requires before raising the schema. |
| `start.sh`, `stop.sh`, `restart.sh` | The lifecycle scripts; `restart.sh --rollback` swaps back. |
| `check.sh` | Post-deploy verification. |
| `test-instance.sh` | A disposable unprivileged test instance for rehearsing the scripts. |
| `lib.sh` | Code shared by the scripts above. |
| `systemd/` | `sparrow.service`, `sparrow-ship.{service,timer}`, `sparrow-backup.{service,timer}`. |
| `apparmor/sparrow` | The daemon's AppArmor profile, enforce mode. |
| `config/` | Templates: server, shipper, watch, sysusers and tmpfiles configuration. |
| `libexec/` | `sparrow-ship` (the shipper) and `sparrow-backup` (closed-store backups). |
| `evidence/` | Verification records for this deployment (security exposure, rehearsal, lifecycle, AppArmor, credentials). |

The units are the source of truth. The scripts are thin wrappers over them
and over the kept images in `./bin`.

## Rulings

Patrick's decisions for this deployment, 2026-11-01:

- Unit scope: "System unit, own user (Recommended)". `sparrow.service` is a
  system unit in the fleet's production slice, running as the `sparrow` user
  and group.
- Store location, verbatim: "sparrow's final store location is /SPARROW". The
  store is `/SPARROW/store` and its socket `/SPARROW/store/rpc.sock`.
- Sparrow is not real time. It joins the production slice so that development
  yields to it: nice -20, best-effort IO priority 0, OOM score adjustment -900,
  real-time scheduling refused.
- The public Cap'n Proto listener on port 518 stays. The unit grants the bind
  capability as an ambient capability; no file capability is set on images.
- Only the operator (`paudley`) reaches the private socket, for the CLI and
  administration, through a group, with filesystem ACLs where they help.
  Fleet producers ingest through the ingest-only group socket of Sparrow
  issue #10 (Forgejo `fleet/sparrow`), never the private socket; this
  directory does not implement it.
- The NAS archive (`/net/synology/archives/sparrow`) is the backup. Its worker
  is not deployed and nothing here grants its path; Sparrow issues #15 to #17
  add it.
- Restarts of the live service are approved and announced by the coordination
  session.

## Layout

| Path | Owner, mode | Contents |
| --- | --- | --- |
| `/usr/local/libexec/sparrow/sparrowd-schemaN-REV` | root, 0755 | Daemon images, one per revision, never overwritten: `N` is the client schema version, `REV` the short Sparrow commit. |
| `/usr/local/libexec/sparrow/sparrowd` | link | The selected daemon image; `sparrowd.previous` is the rollback. |
| `/usr/local/libexec/sparrow/sparrow` | link | The matching CLI image; `sparrow.previous` is the rollback. |
| `/usr/local/bin/sparrow` | link | The CLI on `PATH`, through the link above. |
| `/usr/local/libexec/sparrow/sparrow-ship`, `sparrow-backup` | root, 0755 | The timers' helpers, from `libexec/`. |
| `/usr/local/share/sparrow/lexicons-PURRDF_REV/` | root, 0755 | The five native CBOR lexicons; `PURRDF_REV` is the full PurRDF commit the images were built against. |
| `/usr/local/share/doc/sparrow/deploy/` | root, 0644 | Copies of these documents and of `evidence/`. |
| `/etc/sparrow/server.toml` | root:sparrow, 0640 | The daemon's configuration. |
| `/etc/sparrow/ship.toml` | root:sparrow, 0640 | The shipper's sources. |
| `/etc/sparrow/watches/` | root:sparrow, 0750 | The applied watch definitions, kept for a restore. |
| `/etc/sparrow/tcp.secret` | root, 0600 | The CHAP secret (see [Secrets](#secrets)). |
| `/SPARROW` | root:sparrow, 0750 | The service's tree. |
| `/SPARROW/store` | sparrow, 0710 | The store and `rpc.sock` (0660). |
| `/SPARROW/intake/NAME` | the operator who made it (plain `mkdir`; the cutover gives existing roots to `sparrow`), 0770, ACL `group:sparrow:rwx` | One consume root per watch. |
| `/SPARROW/ship` | paudley:sparrow, 2700 | The shipper's queue, staging, ledgers and lock. |
| `/SPARROW/backups` | root, 0700 | Closed-store backups: three daily `STAMP` copies, and the named `pre-upgrade-IMAGE` copies, which the rotation never prunes. |
| `/etc/systemd/system/sparrow*.{service,timer}` | root, 0644 | The units, from `systemd/`. |
| `/etc/apparmor.d/sparrow` | root, 0644 | The profile, from `apparmor/`. |
| `/etc/sysusers.d/sparrow.conf`, `/etc/tmpfiles.d/sparrow.conf` | root, 0644 | The user, group and layout, from `config/`. |

`/SPARROW` is on the root filesystem's subvolume, which is on the same btrfs
device as `/home`. Filling `/home` still fills `/SPARROW`
([RECOVERY.md](RECOVERY.md#2026-11-01-home-filled-and-durability-latched)).

## Secrets

- The CHAP secret for the public listener is `/etc/sparrow/tcp.secret`, root
  0600, 32 random bytes. systemd hands it to the daemon as the credential
  `/run/credentials/sparrow.service/tcp.secret` (mode 0400, owned by `sparrow`),
  which `rpc_tcp_secret_file` names. The daemon refuses a secret file with any
  group or other permission bit, so it cannot be shared through a group or an
  ACL.
- Every remote agent host keeps its own private copy, mode 0600, passed with
  `sparrow --secret-file PATH tcp://ADDRESS:518 COMMAND`. The operator's copy is
  `~/.config/sparrow/tcp.secret`.
- `sparrow-backup` copies `/etc/sparrow`, so each backup under
  `/SPARROW/backups` holds the secret; those directories are root-only.
- Custody: the secret is generated on the host, never committed, never kept in
  this directory, and never passed on a command line. Rotating it means
  replacing the file, restarting the service and replacing every remote copy.
  Anonymous peers may only discover, ingest and import; every other TCP
  operation needs the secret.

No other credential exists. The Unix socket admits members of the `sparrow`
group without a secret.

## Permissions

The modes, ACLs and the writers' umasks are designed together, because an
explicit `chmod` or a umask can cancel an ACL grant. The rules:

- The store `/SPARROW/store` and its socket are reached through the owning
  group. The daemon sets the store to 0710 and the socket to 0660 every time
  it starts, so a member of `sparrow` can connect and cannot list or read the
  store. The unit's `UMask=0077` keeps every store file owner-only. There is no
  ACL on the store: the daemon's own `chmod` would reset the ACL mask and cancel
  it.
- Membership of `sparrow` is full local administration of Sparrow: the Unix
  socket admits every operation. The operator is the only member
  (`config/sysusers-sparrow.conf`), for the CLI and administration, and the
  shipper runs as the operator. Do not add a fleet producer to the group:
  producers belong on the ingest-only group socket of issue #10, which is
  implemented there.
- `/SPARROW/intake` carries a default ACL with a named entry for the `sparrow`
  group. A root made with plain `mkdir` by any member therefore grants the
  group `rwx` whatever that member's umask, and the daemon can claim and
  delete its files. Never make a root with `install -d` or `mkdir -m`: their
  explicit mode resets the ACL mask and takes the daemon's access away
  (measured in [evidence/permissions.md](evidence/permissions.md)). The roots
  carry no setgid bit, because the daemon's claim directory
  (`.sparrow-inflight`) must be exactly 0700 and a setgid parent would pass
  the bit on. The daemon creates that directory and its lock (0600) with
  explicit modes, which the inherited ACL's mask follows, so its ownership
  checks still pass.
- The shipper renames each queued item into a root, so its state lives on the
  same filesystem, in `/SPARROW/ship`. That directory is setgid `sparrow`, so
  every staged item belongs to the group, and the shipper sets each item to
  0640, because a reflink copy takes the producer's own mode.
- `systemd-tmpfiles` re-asserts the modes, owners and ACLs of `/SPARROW`, the
  store, the intake parent, the shipper's state and the backup directory at
  boot from `/etc/tmpfiles.d/sparrow.conf`. Each root keeps what `mkdir` and
  the inherited ACL gave it.

## Priority

The unit joins the fleet's production slice, which Katamari owns and installs;
this repository ships no slice and no drop-in for it. The slice's CPU and IO
weights are 10000 against `user.slice` at 100, so under contention development
slows and Sparrow does not; when Sparrow is idle nothing is taken from
development. Within the slice Sparrow keeps `IOWeight=20` and `CPUQuota=100%`,
so an ingest backlog cannot crowd out the other production services, which
take precedence over it. Per process it runs at nice -20, best-effort IO
priority 0 and OOM score adjustment -900, with real-time scheduling refused.
The two timers' services run in the same slice at the same nice and IO
priority, but with OOM score adjustment -500: under the specification's
OOM ranking (section B, 2026-11-02) a helper is less protected than the
service it serves, so a host OOM caused by a copy job kills the job. They are
bounded too: the shipper at 512 MiB, 64 tasks and half a CPU, the backup at
1 GiB, 64 tasks and one CPU.

## From a scratch clone to a running service

Every command below is run from the repository root unless it says otherwise.
Commands prefixed with `sudo` need root.

### 1. Host prerequisites

- Linux with systemd 256 or newer, AppArmor enabled (`aa-enabled` prints
  `Yes`) and cgroup v2. btrfs is preferred, because images, backups and the
  shipper's copies are reflinks.
- Packages: `git` 2.54 or newer (for `make install-hooks`), `tar`, `capnp` (Cap'n Proto compiler), `python3` 3.11 or
  newer, `util-linux` (`flock`, `setpriv`), `acl`, `apparmor`, a C/C++ compiler
  and `clang` (RocksDB and bindgen), and Rust 1.98 or newer through Stage's
  managed cargo wrapper or `rustup`.
- Katamari's production slice, installed from the Katamari repository's
  `deploy/systemd/` (Forgejo `fleet/katamari`) as its `deploy/README.md`
  describes. `install.sh`
  refuses to enable the units without it. Check:

  ```bash
  systemctl show -p FragmentPath --value lillith.slice
  ```

  The output must be a path, normally the slice file under
  `/etc/systemd/system/`.

### 2. Clone and build the kept images

Clone from the lab Forgejo, `fleet/sparrow` (on the lab host itself,
`http://127.0.0.1:7555/fleet/sparrow.git`; from another host, use the
Forgejo's address). The build fetches PurRDF from
`https://github.com/Blackcat-Informatics/purrdf` at the revision `Cargo.lock`
names.

```bash
git clone http://127.0.0.1:7555/fleet/sparrow.git ~/Active/sparrow
cd ~/Active/sparrow
make install-hooks
make check
make install-bin
ls -l bin/
```

`make install-bin` refuses an uncommitted tree. It builds the release daemon,
CLI and recovery probe and copies them into `./bin` as
`sparrowd-schemaN-REV`, `sparrow-schemaN-REV` and
`sparrow-recovery-schemaN-REV`, with the lexicon bundle as
`bin/lexicons-PURRDF_REV/`. `./bin` is ignored by Git. A cold release build
took 362 s on this host (docs/deployment.md, "Kept images and seeded builds");
`make check` takes longer. The recovery probe stays in `./bin`; RECOVERY.md
uses it from there.

### 3. Install

```bash
sudo deploy/install.sh
```

It creates the `sparrow` user and group and adds the operator to the group,
lays out `/SPARROW` with its modes and ACLs, creates `/etc/sparrow` and
installs the configuration templates where no file exists, then installs the
units, loads the AppArmor profile, installs the helpers, images, lexicons and
these documents, reloads systemd and enables the service and both timers. It
starts nothing. Run it again at any time: it changes only what differs and
prints what it changed. Log in again so the operator's new group membership
applies.

Every check that can refuse runs before the first change: the image pair and
lexicon bundle in `./bin`, an installed image or bundle of the same name that
differs, the secret's owner and mode, the slice and the old user unit. A
refusal lists every problem and changes nothing. A failure after that point
lists what had changed. `sudo deploy/uninstall.sh` undoes the host changes
(Sparrow issue #22, the revert).

### 4. Configure

Generate the CHAP secret, once per host:

```bash
sudo sh -c 'umask 077; head -c 32 /dev/urandom > /etc/sparrow/tcp.secret'
sudo stat -c '%U %a' /etc/sparrow/tcp.secret
```

Expect `root 600`. Then edit `/etc/sparrow/server.toml`: replace `REV` in
`text_lexicon_dir` with the installed bundle's suffix (`ls
/usr/local/share/sparrow`), and review the budgets. For a restored host, take
the configuration from the backup instead (RECOVERY.md, "Restore from a
backup").

### 5. Intake roots, watches and shipper sources

For each producer directory the shipper copies from, choose a name, make its
consume root, apply its watch and add its source:

```bash
mkdir /SPARROW/intake/NAME
sudo install -m 0640 -g sparrow deploy/config/watch.toml.example /etc/sparrow/watches/NAME.toml
sudoedit /etc/sparrow/watches/NAME.toml
sudoedit /etc/sparrow/ship.toml
```

Run the `mkdir` as the operator (a member of `sparrow`), not as root, and
without `-m`. Set `id`, `root`, `source` and `format` in the watch definition,
and the matching `[[source]]` entry in `ship.toml`. Watches are applied once
the service runs (step 7).

### 6. Start

```bash
sudo deploy/start.sh
```

It installs the newest image pair from `./bin` (if `install.sh` has not),
verifies that both images run on this host and that the configuration names a
socket inside the store and the five lexicons, points the links at the pair,
starts `sparrow.service` and waits until the daemon answers on its socket. It
ends with a receipt: unit state, process, images, store identity, schema,
pressure phase, durability and the number of watches.

### 7. Apply watches, start the timers, verify

```bash
for watch in /etc/sparrow/watches/*.toml; do
    sparrow /SPARROW/store/rpc.sock watch put "$(basename "$watch" .toml)-$(date -u +%Y%m%dT%H%M%SZ)" 0 "$watch"
done
sudo systemctl start sparrow-ship.timer sparrow-backup.timer
sudo deploy/check.sh
```

Run this as the operator in a new login session, so that the `sparrow` group
membership from step 3 applies; `sparrow` is on `PATH` through the
`/usr/local/bin/sparrow` link that `install.sh` made. The first argument after
`put` is a fresh request ID; keep it if the call must be retried. `check.sh` exits 0 when every check passes: the unit, images,
socket and store modes, pressure, durability, store identity, each watch's
root, the unit's security exposure, AppArmor confinement with no denials, both
timers and the slice. Exit 2 means a check needed root.

### 8. Remote agents

Copy `/etc/sparrow/tcp.secret` privately to each agent host (mode 0600) and
verify from there:

```bash
sparrow --secret-file ~/.config/sparrow/tcp.secret tcp://SERVER_ADDRESS:518 status
```

## Upgrades and rollback

Build, then take a named closed-store backup of the store as the current image
left it, then switch images:

```bash
git pull && make check && make install-bin
sudo systemctl stop sparrow-ship.timer
sudo deploy/pre-upgrade-backup.sh && sudo deploy/restart.sh && sudo deploy/check.sh
sudo systemctl start sparrow-ship.timer
```

`pre-upgrade-backup.sh` runs `sparrow-backup.service` once and stops at the
first failure. While the daemon runs, the service copies the configuration
and the image pair that last opened the store. It then stops the daemon
through its drain, copies the store and intake, and starts it again. Its `SOURCE` records that image (`image=`), the
store's identity, schema and format, and `taken_utc`. The script accepts only
the backup this run took, with `taken_utc` at or after its own start and
`image=` equal to the current image, and renames it to
`/SPARROW/backups/pre-upgrade-CURRENT`, which the three-day rotation never
prunes. It refuses if that name already exists, as it does after a restore of
it: rename the old copy first, for example
`sudo mv -T /SPARROW/backups/pre-upgrade-CURRENT /SPARROW/backups/pre-upgrade-CURRENT.retired-STAMP`.
Remove a pre-upgrade copy by hand once the upgrade is settled.

`restart.sh` installs and verifies the newest pair while the old daemon still
runs, records the store identity, stops through the drain, points the links at
the new pair (the old pair becomes `.previous`), starts, and fails unless the
store identity is unchanged. `sudo deploy/restart.sh --rev REV` selects another
kept pair; `sudo deploy/restart.sh --rollback` swaps back to the previous pair.
`start.sh` and `restart.sh` hold the backup's lock (`/SPARROW/backups/.lock`)
while they run, and refuse while a backup holds it, so a backup and an image
switch never interleave.

A newer image may upgrade the store's format, and an older image then refuses
to open it. The scripts enforce this by the schema number in the image names
(`sparrowd-schemaN-REV`), not by reading the store: `start.sh` and `restart.sh`
refuse an image with a lower number than the current one, and refuse one with
a higher number unless `/SPARROW/backups/pre-upgrade-CURRENT` exists, names
the current image and is fresh. Fresh means two things:

- its `taken_utc` is at or after the time the image link was last set (every
  start and restart sets it, a restore's start included), so a copy older
  than a restore is refused;
- it was taken within the last 24 hours (`SPARROW_PRE_UPGRADE_MAX_HOURS`).

The store's actual format is `store_format` in the daemon's
status and in each backup's `SOURCE`. Going back across a schema change is a
restore of the pre-upgrade backup, which starts exactly the image recorded in
it (RECOVERY.md, "Restore from a backup").

`sudo deploy/stop.sh` stops the service through the drain and reports what it
still held.

## Moving a running service

This is the procedure used on 2026-11-01 to move the live store from
`~/.local/state/sparrow/development-v5` to `/SPARROW/store`, generalized to
any new store location. It needs a window announced by the coordination
session.

1. Record the identity and watches: `sparrow SOCKET status > before.json`.
   Note `store_id`, each watch's `root` and `pending_claims`.
2. Stop the shipper so no file arrives: `sudo systemctl stop sparrow-ship.timer`
   (for the old user unit, `systemctl --user stop sparrow-ship.timer`). Wait
   until every `pending_claims` is zero.
3. Stop the service through its SIGINT drain: `sudo deploy/stop.sh` (for the
   old user unit, `systemctl --user stop sparrow.service`). Confirm the unit is
   inactive with result `success` and the socket is gone.
4. Copy the store: `cp -a --reflink=always OLD_DATA_DIR NEW_DATA_DIR`.
5. Compare file lists and sizes: `(cd OLD_DATA_DIR && find . -printf '%P %s\n' | sort) > old.txt`,
   the same for the new copy, and `diff old.txt new.txt` must print nothing.
6. Switch `data_dir` and `socket_path` in the configuration to the new location.
7. Start the service and verify: `sudo deploy/start.sh` and
   `sudo deploy/check.sh --store-id STORE_ID`, comparing the watches with
   `before.json`.
8. Start the shipper timer again.
9. Make the old copy read-only (`chmod -R a-w OLD_DATA_DIR`) and keep it until
   its removal is approved.

Moving from the old user unit to this system unit adds a change of owner and
of the consume roots' location. The acceptance steps of Sparrow issue #22,
the cutover's tracking issue, give that cutover in full; in outline: drain, re-point each watch under
`/SPARROW/intake` while the old unit still runs (the system unit cannot reach
`/home`), stop the old unit and disable it, take the named pre-upgrade backup
of the closed store with the old image recorded in its `SOURCE` (the
issue's step 5a), move the shipper's state to
`/SPARROW/ship`, give `/SPARROW/store` and the roots to `sparrow` with
`chown -R`, run `install.sh`, then `start.sh` and `check.sh --store-id`. The
issue's revert goes back to the user unit: `uninstall.sh` removes the system
units, the profile, the tmpfiles entry and the CLI link, and returns `/SPARROW`
to the operator; then the step 5a backup's store replaces the cutover store.

## Test instance

`deploy/test-instance.sh setup` stages a disposable instance under
`.stage/test-instance` without root: `install.sh` with `SPARROW_DESTDIR`, a
scratch store and socket, no TCP listener, and a user unit
`sparrow-deploytest.service` rendered from `systemd/sparrow.service` with the
production sandbox minus what an unprivileged manager cannot apply. It prints
the environment that points `start.sh`, `stop.sh`, `restart.sh`,
`pre-upgrade-backup.sh` and `check.sh` at it;
`deploy/test-instance.sh teardown` removes the unit.
[evidence/lifecycle.md](evidence/lifecycle.md) records a run.

```bash
SPARROW_BIN=$PWD/bin deploy/test-instance.sh setup
eval "$(SPARROW_BIN=$PWD/bin deploy/test-instance.sh env)"
deploy/start.sh
SPARROW_MAX_EXPOSURE=3.5 deploy/check.sh
deploy/stop.sh
deploy/test-instance.sh teardown
```

Set `SPARROW_BIN` when the kept images are not in this checkout's `./bin`.
`check.sh` needs `SPARROW_MAX_EXPOSURE=3.5` because the test unit drops the
root-only hardening, and it exits 2 with `NOT CHECKED` for AppArmor, the
timers and the slice, which a user unit does not have. A Unix socket path is
limited to 107 bytes; when the default scratch path makes the socket longer,
`setup` refuses, so pass a shorter absolute `SCRATCH` as its second argument
(and to `env` and `teardown`).
