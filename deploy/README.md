<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Deploying Sparrow

This directory holds example material for running Sparrow on a Linux host: the
systemd units, the AppArmor profile, configuration templates, the helper
scripts and these documents. Treat every path, user name and address as a default
to review and adapt. On a fresh host, this directory and a clone of the repository
are enough to deploy the service (the store itself is not recovered by them: Sparrow keeps
no backup of it). To build new images, and for the lexicon bundle and the recovery probe, step 2
needs the build prerequisites of step 1 and network access to the PurRDF
source that `Cargo.lock` names, or a copy of another host's `./bin`. Secrets are never kept here; [Secrets](#secrets) names where each one
lives.

| Path | Contents |
| --- | --- |
| `README.md` | This guide: from a scratch clone to a running, verified service. |
| `RECOVERY.md` | Failure modes, how to diagnose and recover each, rollback, exposure. |
| `MANUAL.md` | Running the daemon by hand without systemd, with the unit's environment. |
| `install.sh` | Idempotent install of everything below onto the host; prints what changed. |
| `start.sh`, `stop.sh`, `restart.sh` | The lifecycle scripts; `restart.sh --rollback` swaps back. |
| `check.sh` | Post-deploy verification. |
| `test-instance.sh` | A disposable unprivileged test instance for rehearsing the scripts. |
| `lib.sh` | Code shared by the scripts above. |
| `systemd/` | `sparrow.service`, `sparrow-ship.{service,timer}`. |
| `apparmor/sparrow` | The daemon's AppArmor profile, enforce mode. |
| `config/` | Templates: server, shipper, watch, sysusers and tmpfiles configuration. |
| `libexec/` | `sparrow-ship`, the shipper. |

The units are the source of truth. The scripts are thin wrappers over them
and over the kept images in `./bin`.

## Deployment requirements

- `sparrow.service` is a system unit running as the `sparrow` user and group.
- The store location is `/var/lib/sparrow`: the store is `/var/lib/sparrow/store` and its
  socket `/var/lib/sparrow/store/rpc.sock`.
- Sparrow is not real time. It runs ahead of ordinary work on the host: nice -20,
  best-effort IO priority 0, OOM score adjustment -900, real-time scheduling
  refused.
- The public Cap'n Proto listener on port 518 stays. The unit grants the bind
  capability as an ambient capability; no file capability is set on images.
- Only the administrator and the shipper reach the private socket, for the CLI and
  administration, through a group, with filesystem ACLs where they help.
  Producers ingest through the ingest-only group socket
  ([Ingest socket](../docs/deployment.md#ingest-only-group-socket)), never the private socket; this
  directory does not configure it.
- Sparrow backs up nothing except the age-out to GTS files on a network share: the archive worker
  writes aged-out segments under `[archive] dir` (for example `/mnt/archive/sparrow-archive`) and
  never to a local disk; no Sparrow backup is ever written to a local disk. Sparrow takes no online
  store backup. The archive
  worker is implemented, and these files do not enable it: the shipped configuration has no
  `[archive]` table, so the daemon runs no worker (see
  [Archive status](../docs/archive-format.md#archive-status)). Once enabled, it refuses a directory that
  is on the root filesystem's device, or on a local filesystem of the root's type (an unmounted
  share), and one whose `STORE_ID` names another store or which holds content but no `STORE_ID`.
  It writes `STORE_ID` itself into an empty directory on its first pass. A file naming another
  store means `dir` is that store's archive or the wrong mount. `dir` must be a directory inside
  the share, never the mount point itself.

## Layout

Copy-on-write is off for the store. `install.sh`
sets the btrfs no-copy-on-write attribute on `/var/lib/sparrow/store` while it is empty, so every file
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
| `/usr/local/share/sparrow/lexicons-PURRDF_REV/` | root, 0755 | The five native CBOR lexicons; `PURRDF_REV` is the full PurRDF commit the images were built against. |
| `/usr/local/share/doc/sparrow/deploy/` | root, 0644 | Copies of these documents. |
| `/etc/sparrow/server.toml` | root:sparrow, 0640 | The daemon's configuration. |
| `/etc/sparrow/ship.toml` | root:sparrow, 0640 | The shipper's sources. |
| `/etc/sparrow/watches/` | root:sparrow, 0750 | The applied watch definitions. |
| `/etc/sparrow/tcp.secret` | root, 0600 | The CHAP secret (see [Secrets](#secrets)). |
| `/var/lib/sparrow` | root:sparrow, 0750 | The service's tree. |
| `/var/lib/sparrow/store` | sparrow, 0710, no copy-on-write (`C`) | The store and `rpc.sock` (0660). |
| `/var/lib/sparrow/intake/NAME` | the member who made it (plain `mkdir`), 0770, ACL `group:sparrow:rwx` | One consume root per watch. |
| `/var/lib/sparrow/syslog-spill` | sparrow, 0700; only for a TCP syslog listener, see [the install steps](../docs/deployment.md#syslog-tcp-listener-install-steps) | The frames a TCP syslog listener held at a stop. |
| `/var/lib/sparrow/ship` | sparrow-ship:sparrow, 2700 | The shipper's queue, staging, ledgers and lock. |
| `/mnt/archive/sparrow-archive` | on the network share (NFSv4), 0700; create it once, owned by the daemon's user | The archive worker's per-store directories and the `STORE_ID` file. |
| `/etc/systemd/system/sparrow*.{service,timer}` | root, 0644 | The units, from `systemd/`. |
| `/etc/apparmor.d/sparrow` | root, 0644 | The profile, from `apparmor/`. |
| `/etc/sysusers.d/sparrow.conf`, `/etc/tmpfiles.d/sparrow.conf` | root, 0644 | The user, group and layout, from `config/`. |

When `/var/lib/sparrow` shares a device with other trees (for example `/home` on the same
btrfs device), filling them fills the store's device
([RECOVERY.md](RECOVERY.md#a-shared-filesystem-fills-and-durability-latches)).

## Secrets

- The CHAP secret for the public listener is `/etc/sparrow/tcp.secret`, root
  0600, 32 random bytes. systemd hands it to the daemon as the credential
  `/run/credentials/sparrow.service/tcp.secret` (mode 0400, owned by `sparrow`),
  which `rpc_tcp_secret_file` names. The daemon refuses a secret file with any
  group or other permission bit, so it cannot be shared through a group or an
  ACL.
- Every remote agent host keeps its own private copy, mode 0600, passed with
  `sparrow --secret-file PATH tcp://ADDRESS:518 COMMAND`. The administrator's copy is
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

- The store `/var/lib/sparrow/store` and its socket are reached through the owning
  group. The daemon sets the store to 0710 and the socket to 0660 every time
  it starts, so a member of `sparrow` can connect and cannot list or read the
  store. The unit's `UMask=0077` keeps every store file owner-only. There is no
  ACL on the store: the daemon's own `chmod` would reset the ACL mask and cancel
  it.
- Membership of `sparrow` is full local administration of Sparrow: the Unix
  socket admits every operation. The shipper's user `sparrow-ship` and the administrator
  (`config/sysusers-sparrow.conf`) are the members, for the CLI and administration. Do not add a
  producer to the group: producers belong on the ingest-only group socket
  ([Ingest socket](../docs/deployment.md#ingest-only-group-socket)).
- `/var/lib/sparrow/intake` carries a default ACL with a named entry for the `sparrow`
  group. A root made with plain `mkdir` by any member therefore grants the
  group `rwx` whatever that member's umask, and the daemon can claim and
  delete its files. Never make a root with `install -d` or `mkdir -m`: their
  explicit mode resets the ACL mask and takes the daemon's access away. The roots
  carry no setgid bit, because the daemon's claim directory
  (`.sparrow-inflight`) must be exactly 0700 and a setgid parent would pass
  the bit on. The daemon creates that directory and its lock (0600) with
  explicit modes, which the inherited ACL's mask follows, so its ownership
  checks still pass.
- The shipper renames each queued item into a root, so its state lives on the
  same filesystem, in `/var/lib/sparrow/ship`. That directory is setgid `sparrow`, so
  every staged item belongs to the group, and the shipper sets each item to
  0640, because a reflink copy takes the producer's own mode.
- `systemd-tmpfiles` re-asserts the modes, owners and ACLs of `/var/lib/sparrow`, the
  store, the intake parent and the shipper's state at
  boot from `/etc/tmpfiles.d/sparrow.conf`. Each root keeps what `mkdir` and
  the inherited ACL gave it.

## Priority

Sparrow runs at nice -20, best-effort IO priority 0 and OOM score adjustment -900, with
real-time scheduling refused, so under contention other work slows and Sparrow does not;
when Sparrow is idle nothing is taken from other work. It keeps `IOWeight=20` and
`CPUQuota=100%`, so an ingest backlog cannot crowd out the host's other services. To place it
in a slice with its own weights, add `Slice=` in a drop-in; the unit sets none. The timer's
service runs at the same nice and IO priority, but with OOM score adjustment -500: a helper is
less protected than the service it serves, so a host OOM caused by a copy job kills the job. It
is bounded too: the shipper at 512 MiB, 64 tasks and half a CPU.

## From a scratch clone to a running service

Every command below is run from the repository root unless it says otherwise.
Commands prefixed with `sudo` need root.

### 1. Host prerequisites

- Linux with systemd 256 or newer, AppArmor enabled (`aa-enabled` prints
  `Yes`) and cgroup v2. btrfs is preferred, because images and the
  shipper's copies are reflinks.
- Packages: `git` 2.54 or newer (for `make install-hooks`), `b3sum`, `tar`, `capnp` (Cap'n Proto compiler), `python3` 3.11 or
  newer, `util-linux` (`flock`, `setpriv`), `acl`, `apparmor`, a C/C++ compiler
  and `clang` (RocksDB and bindgen), and Rust 1.98.1 or newer through `rustup`
  for the helper census and build.
### 2. Clone and build the kept images

Clone Sparrow and the sibling PurRDF repository. Cargo uses its locked
registry packages. The checks and lexicon bundle use the PurRDF revision in
`registry/compatibility.toml`.

```bash
git clone https://github.com/Blackcat-Informatics/sparrow.git
cd sparrow
git clone https://github.com/Blackcat-Informatics/purrdf ../purrdf
make install-census
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
takes several minutes (docs/deployment.md, "Kept images"); `make check`
takes longer. The recovery probe stays in `./bin`; RECOVERY.md
uses it from there.

### 3. Install

```bash
sudo deploy/install.sh
```

It creates the `sparrow` and `sparrow-ship` users and the `sparrow` group,
lays out `/var/lib/sparrow` with its modes and ACLs, creates `/etc/sparrow` and
installs the configuration templates where no file exists, then installs the
units, loads the AppArmor profile, installs the helpers, images, lexicons and
these documents, reloads systemd and enables the service and shipper timer. It
starts nothing. A repeat install changes only what differs and prints what
it changed. Stop the daemon through its drain before installing a changed
AppArmor profile. Move an existing store or shipper layout to the example
paths before installing these units and profile. See [Moving a store](#moving-a-store).
Add the administrator to the group (`sudo usermod -aG sparrow ADMIN`) and log in again so the
membership applies. Grant `sparrow-ship` read access to each producer directory.

Every check that can refuse runs before the first change: the image pair and
lexicon bundle in `./bin`, an installed image or bundle of the same name that
differs, the installed layout, the daemon state before a profile replacement,
and the secret's owner and mode. A
refusal lists every problem and changes nothing. A failure after that point
lists what had changed.

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
mkdir /var/lib/sparrow/intake/NAME
sudo install -m 0640 -g sparrow deploy/config/watch.toml.example /etc/sparrow/watches/NAME.toml
sudoedit /etc/sparrow/watches/NAME.toml
sudoedit /etc/sparrow/ship.toml
```

Run the `mkdir` as an administrator (a member of `sparrow`), not as root, and
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

### 7. Apply watches, start the shipper timer, verify

```bash
for watch in /etc/sparrow/watches/*.toml; do
    sparrow /var/lib/sparrow/store/rpc.sock watch put "$(basename "$watch" .toml)-$(date -u +%Y%m%dT%H%M%SZ)" 0 "$watch"
done
sudo systemctl start sparrow-ship.timer
sudo deploy/check.sh
```

Run this as the administrator in a new login session, so that the `sparrow` group
membership from step 3 applies; `sparrow` is on `PATH` through the
`/usr/local/bin/sparrow` link that `install.sh` made. The first argument after
`put` is a fresh request ID; keep it if the call must be retried. `check.sh` exits 0 when every check passes: the unit, images,
socket and store modes, pressure, durability, store identity, each watch's
root, the unit's security exposure, AppArmor confinement with no denials, the
timer. Exit 2 means a check needed root.

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

`deploy/copy-on-write.sh copy SOURCE DEST` copies a closed store for a restart that
changes the store without switching images; that copy is the way back from a format-changing
change. Before a restart that switches images, `SPARROW_CONFIG` must not set the removed
`backup_root` or `backup_keep` keys: the image refuses a file that has them, so delete those
two lines first.

## Moving a store

The installed example uses `/var/lib/sparrow`. Moving the store requires a
maintenance window and coordinated changes to the daemon configuration, the
systemd unit's `WorkingDirectory`, `RequiresMountsFor` and `ReadWritePaths`,
and the AppArmor store rules. Also update the shipper's socket path if it reaps
acknowledged files. If intake moves, update its watch roots and the shipper's
configuration and writable paths. Keep the shipper queue and intake roots on
the same filesystem for atomic renames.

1. Record the store identity, watch roots and pending claims. Stop the
   shipper timer, let the current shipper run finish and settle pending
   claims before moving their roots.
2. Stop the daemon through `deploy/stop.sh`. Confirm a successful drain
   and copy the closed store with `deploy/copy-on-write.sh copy OLD NEW`.
   Keep the original available for rollback.
3. Apply the path changes, verify ownership and modes, reload the unit and
   AppArmor profile, and start the daemon. Verify the same store identity,
   watch coverage, durability and payload reads before resuming the timer.
