<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Deploying Sparrow

This directory holds everything a host needs to run Sparrow in production: the
systemd units, the AppArmor profile, configuration templates, the helper
scripts and these documents. After a restore from backups onto a fresh host,
this directory and a clone of the repository are enough to deploy the service
again. Secrets are never kept here; [Secrets](#secrets) names where each one
lives.

| Path | Contents |
| --- | --- |
| `README.md` | This guide: from a scratch clone to a running, verified service. |
| `RECOVERY.md` | Failure modes, how to diagnose and recover each, rollback, restore, exposure. |
| `MANUAL.md` | Running the daemon by hand without systemd, with the unit's environment. |
| `install.sh` | Idempotent install of everything below onto the host; prints what changed. |
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
  Fleet producers ingest through the ingest-only group socket of issue #10,
  never the private socket; this directory does not implement it.
- The NAS archive (`/net/synology/archives/sparrow`) is the backup. The unit
  allows writes there for the archive worker, which is not built yet.
- Restarts of the live service are approved and announced by the coordination
  session.

## Layout

| Path | Owner, mode | Contents |
| --- | --- | --- |
| `/usr/local/libexec/sparrow/sparrowd-schemaN-REV` | root, 0755 | Daemon images, one per revision, never overwritten. |
| `/usr/local/libexec/sparrow/sparrowd` | link | The selected daemon image; `sparrowd.previous` is the rollback. |
| `/usr/local/libexec/sparrow/sparrow` | link | The matching CLI image; `sparrow.previous` is the rollback. |
| `/usr/local/bin/sparrow` | link | The CLI on `PATH`, through the link above. |
| `/usr/local/libexec/sparrow/sparrow-ship`, `sparrow-backup` | root, 0755 | The timers' helpers, from `libexec/`. |
| `/usr/local/share/sparrow/lexicons-REV/` | root, 0755 | The five native CBOR lexicons for the PurRDF revision `REV`. |
| `/usr/local/share/doc/sparrow/deploy/` | root, 0644 | Copies of these documents. |
| `/etc/sparrow/server.toml` | root:sparrow, 0640 | The daemon's configuration. |
| `/etc/sparrow/ship.toml` | root:sparrow, 0640 | The shipper's sources. |
| `/etc/sparrow/watches/` | root:sparrow, 0750 | The applied watch definitions, kept for a restore. |
| `/etc/sparrow/tcp.secret` | root, 0600 | The CHAP secret (see [Secrets](#secrets)). |
| `/SPARROW` | root:sparrow, 0750 | The service's tree. |
| `/SPARROW/store` | sparrow, 0710 | The store and `rpc.sock` (0660). |
| `/SPARROW/intake/NAME` | sparrow, 0770, ACL `group:sparrow:rwx` | One consume root per watch. |
| `/SPARROW/ship` | paudley:sparrow, 2700 | The shipper's queue, staging, ledgers and lock. |
| `/SPARROW/backups` | root, 0700 | Closed-store backups (three daily copies). |
| `/net/synology/archives/sparrow` | sparrow (on the NAS) | The archive, once its worker is enabled. |
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
The two timers' services run in the same slice with the same settings.

## From a scratch clone to a running service

Every command below is run from the repository root unless it says otherwise.
Commands prefixed with `sudo` need root.

### 1. Host prerequisites

- Linux with systemd 256 or newer, AppArmor enabled (`aa-enabled` prints
  `Yes`) and cgroup v2. btrfs is preferred, because images, backups and the
  shipper's copies are reflinks.
- Packages: `git`, `tar`, `capnp` (Cap'n Proto compiler), `python3` 3.11 or
  newer, `util-linux` (`flock`, `setpriv`), `acl`, `apparmor`, a C/C++ compiler
  and `clang` (RocksDB and bindgen), and Rust 1.98 or newer through Stage's
  managed cargo wrapper or `rustup`.
- Katamari's production slice, installed from Katamari's
  `deploy/systemd/` as described in Katamari's `deploy/README.md`. `install.sh`
  refuses to enable the units without it. Check:

  ```bash
  systemctl show -p FragmentPath --value lillith.slice
  ```

  The output must be a path, normally the slice file under
  `/etc/systemd/system/`.

### 2. Clone and build the kept images

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
`bin/lexicons-PURRDF_REV/`. `./bin` is ignored by Git.

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

The first argument after `put` is a fresh request ID; keep it if the call must
be retried. `check.sh` exits 0 when every check passes: the unit, images,
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

```bash
git pull && make check && make install-bin
sudo deploy/restart.sh
```

`restart.sh` installs and verifies the newest pair while the old daemon still
runs, records the store identity, stops through the drain, points the links at
the new pair (the old pair becomes `.previous`), starts, and fails unless the
store identity is unchanged. `sudo deploy/restart.sh --rev REV` selects an
older kept pair; `sudo deploy/restart.sh --rollback` swaps back to the previous
pair. A newer image may upgrade the store's format, and an older image then
refuses to open it, so rollback is safe only between images with the same
store format (RECOVERY.md, "Rollback").

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
of the consume roots' location. The tracking issue's acceptance steps give
that cutover in full; in outline: drain, re-point each watch under
`/SPARROW/intake` while the old unit still runs (the system unit cannot reach
`/home`), stop the old unit and disable it, move the shipper's state to
`/SPARROW/ship`, give `/SPARROW/store` and the roots to `sparrow` with
`chown -R`, run `install.sh`, then `start.sh` and `check.sh --store-id`.

## Test instance

`deploy/test-instance.sh setup` stages a disposable instance under
`.stage/test-instance` without root: `install.sh` with `SPARROW_DESTDIR`, a
scratch store and socket, no TCP listener, and a user unit
`sparrow-deploytest.service` rendered from `systemd/sparrow.service` with the
production sandbox minus what an unprivileged manager cannot apply. It prints
the environment that points `start.sh`, `stop.sh`, `restart.sh` and `check.sh`
at it; `deploy/test-instance.sh teardown` removes the unit.
[evidence/lifecycle.md](evidence/lifecycle.md) records a run.
