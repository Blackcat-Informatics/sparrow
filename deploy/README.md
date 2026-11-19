<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Deploying Sparrow

This directory holds everything a host needs to run Sparrow in production: the
systemd units, the AppArmor profile, configuration templates, the helper
scripts and these documents. On a fresh host, this directory and a clone of the repository
are enough to deploy the service again (the store itself is not recovered by them: Sparrow keeps
no backup of it). To build new images, and for the lexicon bundle and the recovery probe, step 2
needs the build prerequisites of step 1 and network access to the PurRDF
source that `Cargo.lock` names, or a copy of the old host's `./bin`. Secrets are never kept here; [Secrets](#secrets) names where each one
lives.

| Path | Contents |
| --- | --- |
| `README.md` | This guide: from a scratch clone to a running, verified service. |
| `RECOVERY.md` | Failure modes, how to diagnose and recover each, rollback, exposure. |
| `MANUAL.md` | Running the daemon by hand without systemd, with the unit's environment. |
| `install.sh` | Idempotent install of everything below onto the host; prints what changed. |
| `uninstall.sh` | Undoes `install.sh`'s host changes so the old user unit can serve `/SPARROW` again. |
| `user-unit-upgrade.sh` | Switches the old user unit's image, with `--keep-copy DIR` keeping a copy of the closed store, and installs its `LimitNOFILE` drop-in (`systemd/user/sparrow.service.d/limits.conf`), until #22 retires that unit. |
| `start.sh`, `stop.sh`, `restart.sh` | The lifecycle scripts; `restart.sh --rollback` swaps back. |
| `check.sh` | Post-deploy verification. |
| `test-instance.sh` | A disposable unprivileged test instance for rehearsing the scripts. |
| `lib.sh` | Code shared by the scripts above. |
| `systemd/` | `sparrow.service`, `sparrow-ship.{service,timer}`; under `user/`, the interim user units: the `LimitNOFILE` drop-in and `katamari-journal-spool.service` and `katamari-manager-spool.service`. |
| `apparmor/sparrow` | The daemon's AppArmor profile, enforce mode. |
| `config/` | Templates: server, shipper, watch, sysusers and tmpfiles configuration. |
| `libexec/` | `sparrow-ship` (the shipper), `katamari-journal-spool` and `katamari-manager-spool` (the Katamari journal forwarders, `docs/deployment.md`, "Katamari's journal in the spool"). |
| `evidence/` | Verification records for this deployment (security exposure, rehearsal, lifecycle, AppArmor, credentials). |

The units are the source of truth. The scripts are thin wrappers over them
and over the kept images in `./bin`.

## Deployment requirements

- `sparrow.service` is a system unit in the fleet's production slice, running
  as the `sparrow` user and group.
- The store location is `/SPARROW`: the store is `/SPARROW/store` and its
  socket `/SPARROW/store/rpc.sock`.
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
- Sparrow backs up nothing except the age-out to GTS files on the NAS: the archive worker
  writes aged-out segments under `[archive] dir` (`/net/synology/archives/sparrow-archive`) and
  never to a local disk; no Sparrow backup is ever written to a local disk. The online store backup (#156) was removed in #330. The archive
  worker is implemented, and these files do not enable it: the shipped configuration has no
  `[archive]` table, so the live daemon runs no worker (see
  [Archive status](../docs/archive-format.md#archive-status)). Once enabled, it refuses a directory that
  is on the root filesystem's device, or on a local filesystem of the root's type (an unmounted
  share), and one whose `STORE_ID` names another store or which holds content but no `STORE_ID`.
  It writes `STORE_ID` itself into an empty directory on its first pass. A file naming another
  store means `dir` is that store's archive or the wrong mount. `dir` must be a directory inside
  the share, never the mount point itself.
- Restarts of the live service are approved and announced by the coordination
  session.

## Layout

Copy-on-write is off for the store. `install.sh`
sets the btrfs no-copy-on-write attribute on `/SPARROW/store` while it is empty, so every file
created in it inherits it. `restart.sh` rewrites a closed store that
still has copy-on-write files, using `deploy/copy-on-write.sh convert`, and keeps the original
beside it as `store.cow-STAMP`; remove that copy once the converted store has been checked. The store gives up
btrfs checksums and compression; its own digests and compression cover both.

| Path | Owner, mode | Contents |
| --- | --- | --- |
| `/usr/local/libexec/sparrow/sparrowd-schemaN-REV` | root, 0755 | Daemon images, one per revision, never overwritten: `N` is the client schema version, `REV` the short Sparrow commit. |
| `/usr/local/libexec/sparrow/sparrowd` | link | The selected daemon image; `sparrowd.previous` is the rollback. |
| `/usr/local/libexec/sparrow/sparrow` | link | The matching CLI image; `sparrow.previous` is the rollback. |
| `/usr/local/bin/sparrow` | link | The CLI on `PATH`, through the link above. |
| `/usr/local/libexec/sparrow/sparrow-ship` | root, 0755 | The timer's helper, from `libexec/`. |
| `/usr/local/libexec/sparrow/katamari-journal-spool`, `katamari-manager-spool` | root, 0755 | The forwarders' scripts, for the operator to copy to `~/.local/libexec/sparrow/`; their unit files are under `/usr/local/share/sparrow/user-units/`. |
| `/usr/local/share/sparrow/lexicons-PURRDF_REV/` | root, 0755 | The five native CBOR lexicons; `PURRDF_REV` is the full PurRDF commit the images were built against. |
| `/usr/local/share/doc/sparrow/deploy/` | root, 0644 | Copies of these documents and of `evidence/`. |
| `/etc/sparrow/server.toml` | root:sparrow, 0640 | The daemon's configuration. |
| `/etc/sparrow/ship.toml` | root:sparrow, 0640 | The shipper's sources. |
| `/etc/sparrow/watches/` | root:sparrow, 0750 | The applied watch definitions. |
| `/etc/sparrow/tcp.secret` | root, 0600 | The CHAP secret (see [Secrets](#secrets)). |
| `/SPARROW` | root:sparrow, 0750 | The service's tree. |
| `/SPARROW/store` | sparrow, 0710, no copy-on-write (`C`) | The store and `rpc.sock` (0660). |
| `/SPARROW/intake/NAME` | the operator who made it (plain `mkdir`; the cutover gives existing roots to `sparrow`), 0770, ACL `group:sparrow:rwx` | One consume root per watch. |
| `/SPARROW/ship` | paudley:sparrow, 2700 | The shipper's queue, staging, ledgers and lock. |
| `/net/synology/archives/sparrow-archive` | on the NAS (NFSv4), 0700; create it once, owned by the daemon's user | The archive worker's per-store directories and the `STORE_ID` file. |
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
  store, the intake parent and the shipper's state at
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
The timer's service runs in the same slice at the same nice and IO
priority, but with OOM score adjustment -500: under the specification's
OOM ranking (section B, 2026-11-02) a helper is less protected than the
service it serves, so a host OOM caused by a copy job kills the job. It is
bounded too: the shipper at 512 MiB, 64 tasks and half a CPU.

## From a scratch clone to a running service

Every command below is run from the repository root unless it says otherwise.
Commands prefixed with `sudo` need root.

### 1. Host prerequisites

- Linux with systemd 256 or newer, AppArmor enabled (`aa-enabled` prints
  `Yes`) and cgroup v2. btrfs is preferred, because images and the
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
/usr/local/share/sparrow`), and review the budgets. The server file refuses the removed
`backup_root` and `backup_keep` keys; Sparrow takes no online backup.

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
sudo systemctl start sparrow-ship.timer
sudo deploy/check.sh
```

Run this as the operator in a new login session, so that the `sparrow` group
membership from step 3 applies; `sparrow` is on `PATH` through the
`/usr/local/bin/sparrow` link that `install.sh` made. The first argument after
`put` is a fresh request ID; keep it if the call must be retried. `check.sh` exits 0 when every check passes: the unit, images,
socket and store modes, pressure, durability, store identity, each watch's
root, the unit's security exposure, AppArmor confinement with no denials, the
timer and the slice. Exit 2 means a check needed root.

### 8. Remote agents

Copy `/etc/sparrow/tcp.secret` privately to each agent host (mode 0600) and
verify from there:

```bash
sparrow --secret-file ~/.config/sparrow/tcp.secret tcp://SERVER_ADDRESS:518 status
```

## Upgrades and rollback

Build, then switch images:

```bash
git pull && make check && make install-bin
sudo deploy/restart.sh && sudo deploy/check.sh
```

Sparrow keeps no backup of the store, so an upgrade relies on the kept previous image and the
link swap, and nothing else. `restart.sh` installs and verifies the newest pair while the old
daemon still runs, records the store identity, stops through the drain, points the links at
the new pair (the old pair becomes `.previous`), starts, and fails unless the
store identity is unchanged. `sudo deploy/restart.sh --rev REV` selects another
kept pair; `sudo deploy/restart.sh --rollback` swaps back to the previous pair.

A newer image may upgrade the store's format, and an older image then refuses
to open it (the store fence). The scripts enforce this by the schema number in the image names
(`sparrowd-schemaN-REV`), not by reading the store: `start.sh` and `restart.sh`
refuse an image with a lower number than the current one. A format-changing upgrade
therefore has no undo: after the new image has opened the store, the way out is forward.
The store's actual format is `store_format` in the daemon's status.

`sudo deploy/stop.sh` stops the service through the drain and reports what it
still held.

### Interim user unit

While the live store runs under the operator's old user unit,
`deploy/user-unit-upgrade.sh --from FROM --to TO [--keep-copy DIR]` switches images. It takes no
backup and requires none: it relies on the kept previous image and the link swap, which end when the
new image opens the store and upgrades its format (the store fence). For a format-changing switch,
`--keep-copy DIR` copies the closed store to the new directory `DIR` between the daemon's stop
and the new image's start, and that copy is the way back (`docs/deployment.md`, "Switch 1" and
"Switch 2" for the live procedures); without one the only way out is forward on the
new image. `deploy/copy-on-write.sh copy SOURCE DEST` makes the same copy for a restart that
changes the store without switching images. It starts only `sparrow-ship.timer` again. If the shipper is still running when the
script has stopped the timer, it starts the timer again if it was active before and changes
nothing else. Before it stops anything it refuses while `SPARROW_CONFIG` still sets `backup_root`
or `backup_keep`: those keys were removed and the new image refuses the file, so delete those
two lines first (the old image accepts the file without them). `tools/user-unit-upgrade-timers-check.sh` records both behaviors against a stubbed
`systemctl`.

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
`/home`), stop the old unit and disable it, move the shipper's state to
`/SPARROW/ship`, give `/SPARROW/store` and the roots to `sparrow` with
`chown -R`, run `install.sh`, then `start.sh` and `check.sh --store-id`. The
issue's revert goes back to the user unit: `uninstall.sh` removes the system
units, the profile, the tmpfiles entry and the CLI link, and returns `/SPARROW`
to the operator; the cutover store is not restored from a backup, because none is taken.

## Test instance

`deploy/test-instance.sh setup` stages a disposable instance under
`.stage/test-instance` without root: `install.sh` with `SPARROW_DESTDIR`, a
scratch store and socket, no TCP listener, and a user unit
`sparrow-deploytest.service` rendered from `systemd/sparrow.service` with the
production sandbox minus what an unprivileged manager cannot apply. It prints
the environment that points `start.sh`, `stop.sh`, `restart.sh`
and `check.sh` at it.
[evidence/lifecycle.md](evidence/lifecycle.md) records a lifecycle run.

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
limited to 107 bytes; when the default scratch path makes a socket longer,
`setup` refuses, so pass a shorter absolute `SCRATCH` as its second argument
(and to `env`, `teardown` and `purge`).

### A scratch instance for a test load

A full test load sends its cruft to a scratch instance, never to the live
store. The instance is a separate store: it has its own `store_id`, created
when the daemon first starts on `SCRATCH/data/store`, its configuration sets
`instance_role = "scratch"`, and `discover` and `status` report that role. It
never shares a path, socket or store with `/SPARROW`.

| Item | Value |
| --- | --- |
| Store | `.stage/test-instance/data/store` in this repository (`SCRATCH/data/store` for another `SCRATCH`) |
| Census socket | `.stage/test-instance/data/store/rpc.sock`, mode owner-only, every operation |
| Ingest socket | `.stage/test-instance/ingest/ingest.sock`, producer operations only |
| Ingest group | `SPARROW_TEST_INGEST_GROUP` at `setup`, default the invoking user's primary group; `setup` gives `SCRATCH` that group with mode `0710` and the socket directory `2750`, so members can reach the socket but cannot list the scratch tree or read the store; a sender must belong to the group and be able to search every parent of `SCRATCH` |
| Disk budget | 16 GiB (`SPARROW_TEST_DISK_BUDGET`, bytes); one roll measured 7.66 GB of cruft |
| Free-space floor | 4 GiB (`SPARROW_TEST_MIN_FREE`, bytes) on the filesystem holding the store |
| Archive | off unless `SPARROW_TEST_ARCHIVE_DIR` names a directory (see below); without it the configuration has no `archive` table, the daemon runs no worker and nothing retires |

The ingest socket is a stable interface. A scratch instance set up from the
main checkout with the default `SCRATCH` has it at
`<main checkout>/.stage/test-instance/ingest/ingest.sock`, on this host
`/home/paudley/Active/sparrow/.stage/test-instance/ingest/ingest.sock`. The
location does not move without an issue in the consuming repository's forge
first. A sender finds it by that path, or by `deploy/test-instance.sh env`,
which prints `export SPARROW_TEST_INGEST_SOCKET=<path>`. A setup run from a
linked worktree defaults `SCRATCH` to a path inside that worktree, which is
refused when the socket path exceeds the 107-byte limit, so the stated
location requires running `setup` from the main checkout (or passing `SCRATCH`).

A sender configures the ingest socket path and nothing else; it ingests with
the repository's CLI or client, for example `sparrow
SCRATCH/ingest/ingest.sock ingest REQUEST_ID SOURCE FORMAT FILE`. Before it
sends anything it asks the same socket `sparrow SCRATCH/ingest/ingest.sock
discover`, which prints `store_id` and `instance_role`; a sender refuses
`production` and `unspecified` and sends only to `scratch`. The ingest
socket refuses every other operation. A census reads each file's fate from the
census socket with the same CLI, for example `sparrow SCRATCH/data/store/rpc.sock
values --source SOURCE`, or `records`, `text` and `sparql`. Size the budget and
floor for the load before `setup`: a load that exceeds the budget is refused
with `STORAGE_PRESSURE`, and free space below the floor pauses writes.
Free space on the filesystem must cover the budget plus the floor.

#### Archive on the scratch instance

`SPARROW_TEST_ARCHIVE_DIR=DIR deploy/test-instance.sh setup` turns the archive
worker on. It appends `[archive]` with `dir = "DIR"` and every other key at its
default to the configuration, adds `DIR` to the unit's `ReadWritePaths`, and
records `DIR` in `SCRATCH/archive-dir`. It refuses a `DIR` that is absent (the
daemon never creates the directory), that the invoking user cannot write, that is
not empty (it may hold another store's archive), that is on the same device as
the scratch tree, or that breaks the daemon's own-mount rule, and says which. A
path may hold only letters, digits and `. _ / + @ : = , -`. The own-mount rule is
the daemon's
([Archive-before-delete gate](../docs/interfaces.md#archive-before-delete-gate-schema-63)):
a device other than the root filesystem's, and a filesystem type that is a
network filesystem or differs from the root's; `setup` applies the same two
tests. The scratch tree lives on the root btrfs, so a btrfs directory on another
subvolume is refused for its type. A tmpfs such as `/dev/shm` satisfies the rule
on this host, so create `mkdir -m 0700 /dev/shm/NAME` first and pass it.
The daemon writes `DIR/STORE_ID` and `DIR/<store_id>/` on its first pass, and only
into an empty directory.

There is no retention knob: the window is the constant 30 days. A test ages
items early by size. Set `SPARROW_TEST_DISK_BUDGET` small and load past
`start_fraction * (disk_budget - maintenance_reserve)`, where `start_fraction` is
0.75 and the script's `maintenance_reserve` is 64 MiB; the budget must exceed the
reserve. Everything is then due, oldest first, but a segment never extends past
`now - max(10 minutes, buffered_flush_interval)`, so the worker archives an
item only once it is that old. `status` reports the worker under
`archive_worker` (`phase`, the watermark `through` and the last failure), and
the size trigger's input as `pressure.linked_bytes`, which counts the whole
store (the text index and projections too, many times an artifact's own bytes),
so read it from `status` rather than from file sizes. A refused ingest still
adds to it. Once the archive
holds an item, `sparrow-archive list|extract|verify DIR` reads it offline; match
the payload id that `payload STORE_ID PAYLOAD_ID` finds live
([Finding a sent item](../docs/interfaces.md)). The item stays live until
disk pressure retires it. A tmpfs holds the archive in memory, so size the
load for it.

`teardown` stops and removes the unit and keeps the store. Only the explicit
step `deploy/test-instance.sh purge` removes the store, the intake and ingest
directories and the staged files, and the recorded archive directory's
`STORE_ID` file and `<store_id>/` subtree (never the directory or anything else
in it). It removes the subtree only when `STORE_ID` names this instance's own
store: `teardown` saves the store id in `SCRATCH/store-id` while the unit runs,
and `purge` falls back to the unit's journal line for this socket. When the id
is unknown or differs, `purge` removes nothing and names the cause; delete
`SCRATCH/archive-dir` to leave that archive in place and purge the rest. A
`setup` without `SPARROW_TEST_ARCHIVE_DIR` drops the stale record and leaves the
directory alone. `purge` refuses while the unit exists or runs, and
when `SCRATCH` holds no configuration written by this script (it checks the script's header line, which every version wrote). A scratch store created
before instance roles existed has no role row, so the daemon refuses to start
on it once the configuration sets `instance_role`; `setup` detects it from its
older configuration and refuses with this remedy: run `purge`, then `setup` for
a fresh store. [evidence/test-load.md](evidence/test-load.md) records a run: setup, one
artifact ingested over the ingest socket, found by `values_search`, teardown and purge. The same file records a run with the archive on tmpfs: the worker advances, the item is found live by `payload` and in `sparrow-archive list`, and `extract` returns its bytes.
