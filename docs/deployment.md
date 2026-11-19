<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Installing Sparrow on Linux

The production deployment lives in [`deploy/`](../deploy/README.md): a system
unit, `sparrow.service`, running as the `sparrow` user in the fleet's
production slice, with its store and private socket under `/SPARROW/store`,
its consume roots under `/SPARROW/intake` and its configuration in
`/etc/sparrow`. `deploy/README.md` walks from a scratch clone to a running,
verified service, `deploy/RECOVERY.md` covers failures, rollback and restore,
and `deploy/MANUAL.md` runs the daemon by hand. This document explains the
configuration and the behavior those procedures rely on.

## Kept images and seeded builds

Run `make check`, commit through the normal verification hook, then run
`make install-bin`. It builds the release daemon, CLI, recovery probe and role stamp
through the managed cargo wrapper and copies them into `./bin` with
`cp --reflink=auto` under revision-qualified names:
`bin/sparrowd-schemaN-REV`, `bin/sparrow-schemaN-REV`,
`bin/sparrow-recovery-schemaN-REV` and `bin/sparrow-stamp-schemaN-REV` (the
offline role stamp), where `N` is the client schema version and
`REV` the short commit. It also copies the five native CBOR lexicons, with
their provenance manifest, notices and licenses, from the PurRDF revision the
lockfile resolves into `bin/lexicons-PURRDF_REV/`. It refuses an uncommitted
tree and never overwrites a kept name with other bytes. `./bin` is ignored by
Git, and a kept image survives the automatic cull of `target/`. The copy is a
reflink only when `target/` and the checkout share a filesystem; on this host
the managed `target/` is on another one, so `./bin` holds full copies (about
the size of the images, verified byte-identical with `cmp`).
`deploy/install.sh` and `deploy/start.sh` install from `./bin` into
`/usr/local/libexec/sparrow` with `cp --reflink=auto`, so every deployed image
has a kept source copy, and never overwrite an executing image.

Release builds and tests run through the managed cargo wrapper. On a new
worktree's first build it seeds the managed `target` directory by reflink from
the most reusable sibling checkout (`cargo --stage-seed-only` does only that
step), so worktree builds start warm. Seeding copies every gitignored file of
the primary checkout as well; `.config/wt.toml` excludes credentials (`/.env`,
`/.env.*`, `*.secret`), kept images, stores, backups, intake roots and probe
data, and `make seed-excludes-check` fails when an exclusion no longer names
anything this host keeps. Do not set `STAGE_CARGO_SEED=off` except to measure
a cold build.

Build times for `cargo build --locked --release -p sparrow-server`, measured
the same way as the draws repository (cold, and seeded plus seeding), each in a
new worktree at commit `c894b19` on 2026-11-02, on the 32-CPU host. The kache
compiler cache stays active in every run, including the cold one;
`STAGE_CARGO_SEED=off` only skips the reflink seeding of `target`.

| Run | Time | Load average (1, 5, 15 min) at start |
| --- | --- | --- |
| Cold (`STAGE_CARGO_SEED=off`) | 361.7 s | 36.9, 40.3, 39.9 |
| Seeded, including seeding | 232.4 s | 32.2, 39.0, 40.1 |
| Of which seeding (the wrapper's report) | 6.2 s | |

The seeded build still recompiled the workspace crates and the units whose
cached paths named another checkout (the wrapper reported 20). An earlier
seeding on 2026-11-01, at a load average near 188, took 1201.8 s.

## Configuration

Configuration supplies the store/socket paths, disk budget, maintenance reserve,
operation ceilings and intake definitions. `deploy/config/server.toml.example`
is the production template; `config/development.toml` is a host-specific
example for owned synthetic intake, not production source authority. Copy the
lexicon bundle from `./bin` into a revision-qualified directory (install.sh
does this under `/usr/local/share/sparrow/`) and configure `text_lexicon_dir`
to that installed bundle; the daemon's native analyzer resolver verifies the
required artifact identities before serving. Runtime configuration must not
depend on an editable sibling checkout. Preserve an existing store's
configuration and catalog watch revisions during migration. Consume entries
seed only a missing catalog; typed watch administration controls later
changes. Never add production sample directories as destructive watches merely
because they were sample inputs.

`instance_role` (`"production"` or `"scratch"`, optional) fixes the role of a
store when the daemon creates it, and `discover` and `status` report it as
`instanceRole` (`unspecified` for a store with no role). At startup a configured
role that differs from the stored one stops the daemon with a message naming both
values. A configured role on a store that has none also stops it; that message
names only the configured role and offers two remedies: remove the key, or stamp
the store as a separate operator step (see [Stamp the role](#stamp-the-role)). The
role never changes after creation, and the daemon does not stamp an existing store.
Leave the key out of a configuration for a store that has no role row.

#### Stamp the role

`bin/sparrow-stamp-schemaN-REV DATA_DIR production|scratch`, installed by
`make install-bin` beside the other images, writes the one `instance_role` row into
a stopped store that has none. It runs only with the daemon stopped: the daemon
holds the store's lock while it runs, so a running daemon makes the tool refuse
(`CONFLICT`). It opens the existing store without the daemon's open path, so no
format upgrade, fence or migration runs, and writes the single row with a synced
write. It writes nothing else and changes no format. It prints `store_id` and
`instance_role`, which the operator records. It refuses, leaving the store
unchanged, a store that already has a role row (`CONFLICT`, naming it; a stored
role never changes), a store at a format this build does not know
(`UNSUPPORTED`; run it from the build that opens the store) and a directory that is
not a Sparrow store (`INVALID_ARGUMENT`).

After the stamp, start the daemon with the key set to the stamped word or absent;
`sparrow SOCK discover` reports the role. Running the tool against the live store is
a lifecycle act ordered by the coordination session; no procedure runs it unordered.

`deploy/test-instance.sh` writes `instance_role = "scratch"` into the scratch
configuration it generates, together with an ingest socket for senders, a disk
budget and a free-space floor sized for a test load, and an `archive` table only
when `SPARROW_TEST_ARCHIVE_DIR` names a directory.
A scratch store created before roles existed has no role row, so the daemon
refuses to start on it; `test-instance.sh setup` detects it and asks for
`test-instance.sh purge` followed by a fresh `setup`. `deploy/README.md`,
"Test instance", gives the paths and teardown steps.

`max_concurrent_requests` admits simultaneous storage work, including intake
workers (1..64). `max_rpc_connections` independently admits Unix and TCP RPC
streams from one shared ceiling (1..256, default 16). An idle agent capability
holds a transport lease without consuming a storage permit. The existing
60-second connection lease still applies. The daemon takes a connection slot
before it accepts a peer, so a peer beyond the connection ceiling waits in the
kernel's listen backlog and is served when a slot frees (a connection ends or
its lease expires); it is no longer reset. A stalled peer therefore holds its
slot until the lease expires. An intake operation (ingest, events, imports,
flush, pins, discovery) that finds every storage permit taken waits for one, in
arrival order, for at most 3 seconds, then receives the retryable
`RESOURCE_LIMIT`. About a quarter of the permits (rounded up, none when there is
only one) are reserved: queries and status reads may hold the rest, so a query
storm cannot starve ingest, and a read beyond its share is refused at once. These
are separate finite resource budgets; `status` reports each wait.

Sparrow has no configurable per-artifact byte limit. A large artifact ingests
through the staged import within `disk_budget` and `min_free_bytes`, up to one
fixed bound: an import above 8515420160 bytes (about 7.93 GiB) is refused before
upload with a non-retryable `INVALID_ARGUMENT` (advertised as
`Discovery.maxImportBytes`). That is the largest artifact whose catalog row one
expiry or eviction pass can read, and it is below what one atomic publication can
carry. The v2 store format (#5) removes the bound. A single document above
128 MiB is stored as consecutive raw records, so within the bound expiry and
eviction retire it in
bounded passes. That guarantee also needs pins to stay within their store-wide
caps of 606 pin rows and 36594 pinned members; creating or extending a pin past
either cap is refused with `RESOURCE_LIMIT`, and releasing a pin always succeeds. A file above the bound that something other than `sparrow-ship`
places in a consume root is refused on every claim attempt and retried with
backoff indefinitely; the watch's failure in `status` names the refusal, and the
file must be removed from the root and split. The
removed `max_artifact_bytes` key must be deleted from `server.toml` before
installing this binary; a configuration that still sets it refuses to start and
names the key.

`min_free_bytes` (default 4 GiB) is a floor on free space in the filesystem
that holds `data_dir`. `disk_budget` bounds only the store's own bytes, while
other programs on the host can fill the same disk. A write that fails because
the disk is full leaves durability uncertain until the store is reopened. To
avoid that, every write is checked first: below the floor it is refused with a
retriable `STORAGE_PRESSURE`, and buffered writes that were already accepted
are still synchronized. The daemon checks free space every second. Below the
floor it keeps accepting Unix, ingest and TCP connections, including syslog TCP
connections (#257): the connection ceiling and the 60-second lease still bound
them. It answers every write with the retryable `STORAGE_PRESSURE` carrying the
`freeSpaceFloor` class, so a producer knows the floor is the cause and holds
(the Rust client keeps its batch and retries), and it serves reads, with the
disk a query stages limited to 32 MiB in total, within the free space above half the floor.
It claims no consumed files and runs only WAL synchronization; consume and tail
watches keep their files. A syslog TCP frame is held and its sender waits.
Syslog UDP datagrams that still arrive are read and queued by each UDP listener,
up to its `hold_bytes` (default 64 MiB; the sum over listeners at most 512 MiB), and
published in order when writes resume. Beyond that the reader stops and the kernel
buffer takes the arrivals, and the kernel counts the datagrams it drops. A daemon exit
that is not a clean stop (a crash, a kill, an out-of-memory kill, or the exit on a full
disk) loses the queue and the kernel buffer uncounted, so size `hold_bytes` against
`MemoryHigh` and see `docs/interfaces.md` for what a clean stop counts.
Status reports `floorPause` counters (connections accepted, writes refused, reads
served while paused). The daemon logs a line when a pause begins, one a minute while
it lasts and one when it ends. A daemon-side file spool is not built; UDP is held in
the daemon's memory and then the kernel buffer only. At startup it waits for the floor
before opening the store, so a restart on a full disk does not use up the
unit's bounded restarts. If a write
still fails with durability uncertain, for example because another program
filled the disk between checks, the daemon exits with an error and
`Restart=on-failure` reopens the store.

Each UDP syslog socket asks for an 8 MiB receive buffer. The kernel clamps the
request to `net.core.rmem_max` (stock 212992) and stores twice the result; the
unit cannot force more, so raise the sysctl on the host if status shows a small
grant. The unit sets `ProcSubset=pid`, which hides `/proc/sys`, so the daemon
cannot read `rmem_max`: status reports the granted size read back from the
socket. The daemon reads the socket's kernel drop count from
`/proc/self/net/udp`, which stays visible under that setting.

Public TCP requires `rpc_tcp_secret_file`. Generate a random secret with mode
0600, for example `sudo sh -c 'umask 077; head -c 32 /dev/urandom > /etc/sparrow/tcp.secret'`,
and do not use a passphrase: an observer of one CHAP exchange can test guesses
offline. The unit passes it to the daemon as a systemd credential, so
`rpc_tcp_secret_file` names `/run/credentials/sparrow.service/tcp.secret`.
Copy the same file privately to each remote agent host and pass it with
`sparrow --secret-file PATH tcp://ADDRESS:518 COMMAND`. Producers that only
discover, ingest and import need no secret. CHAP does not encrypt traffic or
authenticate the server, so restrict the listener to trusted networks.

Port 518 is below 1024. The unit grants `CAP_NET_BIND_SERVICE` as an ambient
capability and holds nothing else in its bounding set; no file capability is
set on any image, and copying an image therefore needs no `setcap`. The client
needs no capability.

The private Unix socket is `rpc.sock` directly inside `data_dir`. The daemon
sets `data_dir` to mode 0710 and the socket to 0660 each time it starts, so
members of the daemon's group can connect and cannot list or read the store;
store files stay owner-only under the unit's `UMask=0077`. Only the operator
belongs to that group, for the CLI and administration. Fleet producers ingest
through the ingest-only group socket below, not this socket.

### Ingest-only group socket

Daemons that run as other accounts, such as `katamari-stored`, log into Sparrow
through a second Unix socket. It admits only producer operations (`discover`,
`ingest`, the import operations, `eventsIngest` and `flush`) and refuses every
other operation with `PERMISSION_DENIED`. Each artifact and record it accepts
records the caller's uid, gid and pid, which the kernel reports for the
connection (`SO_PEERCRED`).

1. Create the group and add each producer account to it:

   ```bash
   sudo groupadd --system sparrow-ingest
   sudo usermod -aG sparrow-ingest katamari
   ```

2. Install the tmpfiles entry, which creates `/run/sparrow-ingest` owned by the
   daemon's account, group `sparrow-ingest`, mode 2750, at every boot:

   ```bash
   sudo install -m 0644 packaging/tmpfiles/sparrow-ingest.conf /etc/tmpfiles.d/
   sudo systemd-tmpfiles --create /etc/tmpfiles.d/sparrow-ingest.conf
   ```

   Edit the owner in the entry if the daemon runs as another account.

3. Set the three keys above the first table in `server.toml` and restart:

   ```toml
   ingest_socket_path = "/run/sparrow-ingest/ingest.sock"
   ingest_socket_gid = 948          # getent group sparrow-ingest | cut -d: -f3
   max_ingest_connections = 16      # optional; 1..256, default 16
   ```

The socket's directory must be outside `data_dir`. At startup the daemon
refuses to start unless that directory is a real directory, not a symbolic
link, owned by the daemon's uid, with group `ingest_socket_gid` and mode exactly
2750. After binding, the socket must have that group, inherited from the setgid
directory, and the daemon sets mode 0660; a mismatch also stops startup. The
daemon's own account does not need to be in the group: it owns the directory,
and the setgid bit, not its membership, gives the socket the group.

`max_ingest_connections` is a ceiling separate from `max_rpc_connections`, so
producers cannot use up the connections the CLI and MCP need. Over it, a
producer connection waits in the kernel backlog until a slot frees and is not
reset. A connection whose credentials the kernel cannot report is refused and
logged. `status` lists the socket's path, group, ceiling, active connections,
refused connections, ceiling waits and lease expiries. An import is bound
to the account that began it; another account's attempt to continue it is
refused with `PERMISSION_DENIED`. A query-only daemon refuses
`ingest_socket_path`.

Validate the unit files, and their security exposure, without loading them.
Before `install.sh` has run, `verify` reports that
`/usr/local/libexec/sparrow/sparrowd` is missing; that line is expected, and
only the exposure score is meaningful then:

```bash
systemd-analyze verify deploy/systemd/sparrow.service
systemd-analyze security --offline=yes deploy/systemd/sparrow.service
```

For a migration, capture the current typed status, image identity, unit and
configuration, and follow `deploy/README.md`, "Moving a running service". Stop
the old Sparrow unit through its SIGINT shutdown path and verify it is
terminal, with its Unix socket removed. Never open two owners on one store.
Verify store identity, schema/format, retained counts, watches, byte reads and
saved mutation retries over Unix and TCP after adoption. Stop the new unit
before reverting an image/configuration; reverting a binary alone cannot undo a
store-format upgrade.

## Recovery qualification

For isolated recovery qualification, work on a disposable copy of a store, made while its
daemon is stopped, in a separate empty directory, and copy the deployment configuration with
`data_dir` and `socket_path` changed to that directory. Keep the source immutable. The recovery
example uses the existing configuration/store readers directly, without starting
listeners, maintenance or saved watch workers:

```bash
bin/sparrow-recovery-schemaN-REV restored.toml EXPECTED_STORE RFC3339 EVIDENCE_ID...
```

`make install-bin` keeps the probe in `./bin`; without it, build it with
`cargo build --locked --release -p sparrow-server --example recovery` and run
`target/release/examples/recovery` instead.

Supply explicit artifact, record or finished file-generation IDs from captured
discovery pages, following every continuation. The example admits at most 1024
distinct targets and 1 GiB of aggregate reads, with 1 MiB byte calls. It checks
store identity, retained/complete-byte eligibility and native byte integrity;
streams native BLAKE3 over each target; compares full artifacts with their
catalog payload identity; and reports existing native RDF inspection for records.
Compare target bytes/hashes and RDF inspection with independently captured
evidence. The output is one native JSON report after all checks; a failure has
nonzero exit status and no successful stdout prefix. Missing dependencies,
unfinished generations and expired evidence are failures, not skipped targets.

This is a selected-evidence recovery probe, not a whole-engine integrity scrub or
a query-performance qualification. Its target report defines its coverage. A
missing blob may not affect a small inline target, so include all required
evidence when qualifying a copy. Opening a store can perform required format
upgrades; always work on a disposable copy. A live store is protected
by the existing single-owner database lock. The example never activates its
saved source paths, even when the copied watch catalog names original roots.

## Unit behavior

The unit uses SIGINT because the daemon implements worker drain and WAL fencing
on that signal. It gives shutdown 120 seconds and reports failure if the process
cannot exit. It restarts on failure, including host OOM kills, after 10 seconds.
Startup takes the store's exclusive lock before it removes a refused stale Unix
socket, so a restart never creates a second writer. Five failed starts within
ten minutes stop the unit for diagnosis, such as missing dependencies or a
refused store open. Never remove a socket by hand until the prior owner is
proven terminal. As a system unit enabled under `multi-user.target`, it starts
at boot without a login session.

The kernel protects 1 GiB of the daemon's memory from reclaim (`MemoryLow`),
throttles it above 2 GiB (`MemoryHigh`) and kills it at 4 GiB (`MemoryMax`), so
a leak cannot deepen host-wide memory pressure. The production slice outweighs
development (`user.slice`) 100 to 1 in CPU and IO, and the daemon runs at nice
-20 with best-effort IO priority 0, so development yields to it under
contention. Within the slice, `CPUQuota=100%` holds the daemon to one core and
`IOWeight=20` makes it yield disk to the other production services: a
sustained deep-JSON backlog once saturated the host's CPU and disk, and under
these limits ingest slows instead.

`LimitNOFILE=65536` gives the store its descriptors. At open the store reads
its soft `RLIMIT_NOFILE`, keeps 1024 descriptors for sockets, connections,
logs, the WAL, intake and archive files, and gives the rest to RocksDB's
table cache (`max_open_files`), which closes the least recently used table
file instead of exceeding it. Below a soft limit of 2048 the store refuses to
open, naming `LimitNOFILE=65536`. The reserve covers the connection ceilings
at their defaults and one descriptor per watched directory at the live
coverage; ceilings and watch coverage near 1024 need a higher limit. The
startup line ends `nofile=N max_open_files=M`. Tests and `make check` open
stores too, so they need a shell whose `ulimit -Sn` is at least 2048. The old user unit sets no limit of its own and
inherited the user manager's soft 1024, which a store of 917 table files
exhausted (issue #148). `deploy/user-unit-upgrade.sh` installs
`deploy/systemd/user/sparrow.service.d/limits.conf` for it. Journal output is
available through `journalctl -u sparrow.service`.

The system unit filters system calls: `SystemCallFilter=@system-service`, then
`~@privileged @resources`, then `sched_setscheduler setpriority`. systemd applies
the entries in order, the first sets the default and each later one adds or
removes calls, so the last entry returns exactly those two calls from the
denied `@resources` group and leaves the rest of it denied. RocksDB's
low-priority pool calls them: the store lowers that pool's CPU priority so
compactions yield to intake and to flushes, which run in the high-priority pool
at unchanged priority (this holds write stalls to 0.1 percent of the time,
against 6.5 percent without it). Each low-pool thread makes
`sched_setscheduler(SCHED_OTHER)` and `setpriority(PRIO_PROCESS, 19)` once,
before its first job. A filter without the entry kills the daemon with SIGSYS
and a core dump at the first low-pool job, usually a compaction, shortly after
start, and the unit restarts into the same death. The two calls only lower
priority here: the unit keeps no capability but `CAP_NET_BIND_SERVICE`, sets
`NoNewPrivileges=yes`, and with `RestrictRealtime=yes` and the kernel's own
checks refuses real-time policies; `SCHED_OTHER` is not one. `ioprio_set`, also in
`@resources`, stays denied because the store never lowers I/O priority.
`deploy/install.sh` installs `deploy/systemd/sparrow.service` as
`/etc/systemd/system/sparrow.service`; `deploy/test-instance.sh` renders the
test unit from the same file, so both carry the entry. The old user unit has
no filter. `sparrow-ship.service` runs no RocksDB and keeps the two-entry
filter.

After a release that changes the table options profile, the daemon rewrites
older table files once in the background. `sparrow SOCKET status` reports it as
`recompression`: `running`, `paused` with a `waiting` reason, or `complete`,
with the older tables' `remaining_bytes` and any latched `failure`. RocksDB's
own periodic compaction does the work one file at a time, below
size-triggered compaction. The daemon turns it off while pressure maintenance
is outstanding or a job's 1 GiB of headroom would not fit the data allowance
or the free-space floor. The rewrite resumes after a restart, and a running
job is aborted when the store closes, so shutdown does not wait for it.

The first open of a store written before schema 69 serves at once and runs the
owner-to-graph index build and the other online migrations in the
background (see "Online store migrations" in the interfaces). `status` reports
them under `migrations` with `done`, `total`, `watermark` and any `failure`, and
the journal logs a progress line every 64 pages and a completion line per
migration. A restart resumes after the last committed page.

Schema 70 images (#146) first seed the store counters, the `store-counters`
migration, ahead of the others. Until it completes, `status` reports
`counts=pending` and every migration's `total` as `counting`; `live_records`
and the two unique counts then cover only the keys seeded so far and are not
totals. Every consumer of those counts says so rather than printing a partial
number as a total: the closing line logs `unique_payload_chunks=pending`
and `deploy/user-unit-upgrade.sh` prints `counts=` beside `live_records`.
`tools/install-bin.sh` names the pair from `SCHEMA_VERSION`, so a
schema 71 build installs as `sparrowd-schema71-REV` and `sparrow-schema71-REV`.

## Shipping finished files from producers that keep them

A consume watch owns its root: it deletes each file after publication. Never make a producer's own directory a consume root.
`deploy/libexec/sparrow-ship` copies finished files from such directories
into a private queue under its `state_dir`, then feeds queued items into
Sparrow-owned roots. It only reads the originals. Each `[[source]]` in its
TOML configuration (see `deploy/config/ship.toml.example`) names the consume
root `intake_root/NAME`, the source directory, a glob and one finishing rule:

- `daily`: dated files; the newest name may still be appended to.
- `sealed`: rotated segments, finished once `NAME.manifest.json` exists.
- `settled`: written-once documents, finished after a quiet period.

Every rule also requires `settle_seconds` without modification. Small files are
reflink copies, sharing extents on btrfs. Line-oriented files (`jsonl`, `log`)
above `max_chunk_bytes` or `max_chunk_lines` are split on line boundaries into
`NAME.partNNNNN.EXT` chunks. Other formats, such as single JSON documents, are
shipped whole up to Sparrow's 7.93 GiB import bound; a larger file is reported
as a failed file on every run and never queued. The removed `max_whole_bytes` key must be deleted
from the shipper configuration; a configuration that still sets it is refused
before anything ships. The split matters for two reasons. The consume worker retries a
refused publication without limit, backing off from five seconds to one attempt
every 15 minutes after consecutive failures, so a file over the record limit
would be uploaded again and again. Deadline expiries back off too, even when the
resumed claim advances, so a chunk that needs many attempts finishes later; keep `max_chunk_lines` below
the server's `max_records`. And a consume publication has a 45-second deadline.
Retries resume the durable upload and preparation, but each expiry spends the
single consume worker. Ingest time grows faster than artifact size for deeply
nested JSON: on 2026-10-27, under a host load average near 45, 17 KiB-per-line
records took 1.5 seconds for 0.95 MB, 19 seconds for 2.2 MB and 47 seconds for
4.4 MB. The example's 1 MiB `max_chunk_bytes` keeps such chunks well inside the
deadline.

`sparrow-ship` refuses its configuration at load, before it reads or writes anything,
when a source `dir` equals, contains or lies inside `intake_root` or `state_dir`. It
compares real paths, so a symlink does not hide the overlap, and the error names the
source, both paths and the key. Such a source would ship the shipper's own queue,
ledgers and intake items again on every run.

`sparrowd` refuses a consume or tail watch root that equals, contains or lies inside
`data_dir`, `socket_path`, `ingest_socket_path` or `archive.dir`, naming
the watch and both paths. It applies the rule to the `consume` seeds in `server.toml`
when it loads them, to the stored catalog at startup, and to every `watch put`. The
check is lexical. `server.toml` names no log file, so the daemon cannot check where its
standard error is redirected; keep any such file outside every watch root.

Each file is staged completely and checked against the original's size and
identity before any part of it is queued. Its copies or chunks then enter the
queue together, the queue directory is synchronized, and a per-source ledger
under `state_dir` records the shipped identity (inode, size,
modification time). The device number is not part of it, because it can change
at boot; older four-field ledger lines that lead with the device are read as the
same entry. Inode reuse after a delete and create is not detected, and the
device number never detected that either. A crash between queueing and recording
the identity queues the file again. Each run
then feeds the oldest queued items into their roots while the bytes waiting in
all roots together stay within `max_pending_bytes`; an item larger than that
budget is fed only into empty roots. Each move synchronizes the root and the
queue. An identical item already waiting under the same name is dropped as a
leftover of an interrupted run, and a different one waits until Sparrow
consumes the earlier copy. A finished file that changes later has a new
identity and is shipped again whole, so its earlier records appear twice.
`max_files_per_run` and `max_queue_bytes` bound queueing, so a backlog drains
over successive runs. Failures exit nonzero and leave originals untouched for
the next run.

`state_dir` must be on the same filesystem as `intake_root`, because each
queued item is renamed into its root; production keeps both under `/SPARROW`.
The daemon runs as its own user and reads each item through its group, so the
shipper sets every staged item to mode 0640, and `state_dir` is setgid to the
daemon's group (`deploy/README.md`, "Permissions"). Create each consume root
and its watch before the first run. `deploy/install.sh` installs the shipper
and `sparrow-ship.service`, a system unit that runs it as the operator in the
production slice, with `sparrow-ship.timer`.

The timer runs five minutes after each completed run. Shipped data arrives when
its producer finishes a file, not as it is written.

## Katamari's journal in the spool

Two interim user units on the operator's host spool `katamari-stored.service`
into `~/.local/state/sparrow/katamari-journal/` as JSON Lines in ten-minute
files, which the shipper source `katamari.stored` sends on. Their scripts and
unit files are in the repository (`deploy/libexec/`, `deploy/systemd/user/`);
the units run the copies under `~/.local/libexec/sparrow/`.

| Unit and script | Journal | Records | Files | Cursor |
| --- | --- | --- | --- | --- |
| `katamari-journal-spool` | the `katamari` namespace (`--namespace=katamari`), where the node logs since 2026-11-09 16:12:47 | the node's own lines (`event`, `vault`, `message` ...) | `katamari-stored-YYYYMMDDTHHMM.jsonl` | `katamari-journal-ns.cursor` |
| `katamari-manager-spool` | the default journal (`--unit=katamari-stored.service`) | systemd's lines about the unit, each with `"source":"systemd-manager"` | `katamari-manager-YYYYMMDDTHHMM.jsonl` | `katamari-manager.cursor` |

The namespace journal holds only the node's lines. systemd (PID 1) writes its
own to the default journal: starting and stopping, `Main process exited`,
`Failed with result`, `The kernel OOM killer killed some processes in this
unit` and `Consumed ... memory peak`. Without the second unit those are in
journald only, and rotation loses them (the OOM kill of 2026-11-09 18:37:07 was
found there, not in Sparrow). The manager forwarder drops the unit's own
process, so a record in a `katamari-manager-*` file comes from the manager or from another journal entry that names the unit (a coredump, for example); `source` says which journal it came from, not which process.

The manager lines use their own file prefix and cursor, never the node's.
`sparrow-ship` keys a file by inode, size and modification time and
ships a changed file again whole, so appending to a bucket the other forwarder
wrote would send it twice. A manager bucket can still be appended to after it
shipped when a line arrives more than `settle_seconds` after the previous one in
the same ten minutes; that file is then shipped again and its earlier lines
appear twice. The bucket comes from the entry's own time, so a backfill lands
in the buckets it belongs to.

The shipper reads both prefixes through one source, because both go to the same
consume root and watch. `~/.config/sparrow/ship.toml` needs the pattern
widened, with no new watch and no change to the ledger (it keys each file):

```toml
[[source]]
name = "katamari.stored"
dir = "/home/paudley/.local/state/sparrow/katamari-journal"
pattern = "katamari-*.jsonl"
rule = "settled"
format = "jsonl"
settle_seconds = 120
```

Deploy (the operator; nothing here restarts the node or the namespace forwarder):

1. `install -m 0755 deploy/libexec/katamari-manager-spool ~/.local/libexec/sparrow/` and
   `install -m 0644 deploy/systemd/user/katamari-manager-spool.service ~/.config/systemd/user/`.
2. Widen the `pattern` above in `~/.config/sparrow/ship.toml`.
3. `systemctl --user daemon-reload && systemctl --user enable --now katamari-manager-spool.service`.
   The unit sets `KATAMARI_MANAGER_SINCE`, which backfills the first start
   from that time; it is ignored once the cursor file exists. The backfill is
   one pass that spools everything since then, saves the cursor of the last
   entry it saw, and follows from there.
4. Check `journalctl --user -u katamari-manager-spool.service` is quiet, and that
   `~/.local/state/sparrow/katamari-journal/katamari-manager-*.jsonl` holds the
   lines (`Stopping Katamari store daemon...` and its five companions of
   2026-11-09 18:36:36 to 18:37:10 are the first).

`katamari-journal-spool` and its unit are copied into the repository unchanged
in behavior (the license header, and a `umask` for `mkdir -m`, are the only
edits). The live copies are not replaced by installing them.

`tools/katamari-manager-spool-check.sh SCRATCH_DIR` is the recorded check
(it needs a user manager and journal, so it is not in `make check`). It starts a
transient user unit with `systemd-run --user`, follows it with the script
(`KATAMARI_MANAGER_SCOPE=user` selects `--user-unit`) under its own cursor and a
scratch spool, stops the unit, and asserts the `Stopping ...` and `Stopped ...`
records are there with `source` set, the unit's own output is not, and the
cursor was saved. It touches nothing but the scratch directory and its own unit.

## Archive and backups

Sparrow backs up nothing except the age-out to GTS files on the NAS: the archive worker
(docs/interfaces.md, "Archive-before-delete gate") writes each aged-out segment under `[archive] dir`, here
`/net/synology/archives/sparrow-archive`, before the store retires it. The worker is implemented
and is not running on the live daemon; see [Archive status](archive-format.md#archive-status). The online store backup
(#156) was removed in #330, with `sparrow-backup`, its restore, `deploy/verify-backup.sh`,
`deploy/pre-upgrade-backup.sh` and the nightly timer. The configuration keys `backup_root` and
`backup_keep` were removed too. The server file refuses unknown keys, so a `server.toml` that still
names either one fails to load with the parser's `unknown field` error: delete the lines.

The archive worker also refuses to write when `[archive] dir` is not on its own mount. An
unmounted NFS share leaves its mount point as an ordinary directory on the local root disk, and
writing archives there would fill it. Two checks apply, in order: the directory's device must
differ from `/`'s, and its filesystem type must be a network filesystem (NFS, SMB/CIFS, Ceph, AFS,
9P, Lustre, NCP, Coda) or differ from `/`'s type. The second closes the case of an unmounted share
whose parent is another local mount of the root's type, such as a btrfs subvolume under `/home`.
The refusal is the named error "archive directory ... is not on its expected mount", which names
the device or filesystem type found, is journaled on the pass failure line, and writes, moves and
deletes nothing; the daemon itself starts and serves as before, and the worker retries on its next
pass. A second local btrfs disk mounted as the archive is refused too, with no override. A mount
point inside a local filesystem of a type other than `/`'s passes both checks, and when it is
empty the worker adopts it by writing `STORE_ID`, so segments would go to the local disk;
`STORE_ID` stops only a directory with content or another store's id. Set `[archive] dir` to a
directory inside the share (as `/net/synology/archives/sparrow-archive` is), never to the mount
point itself: an unmounted share leaves no such subdirectory, so the root is missing and refused.

The worker writes `STORE_ID` itself. On its first pass, a directory that is on its mount and
empty (no entry at all, apart from a `STORE_ID.partial` left by an interrupted first pass) gets a
`STORE_ID` holding this store's id and a newline, written durably (temp file, fsync, rename,
directory fsync). An existing `STORE_ID` naming this store is accepted as is; one naming another
store is refused and never overwritten. A directory that holds any other entry but no `STORE_ID`,
such as segments whose id file was lost, is refused and not adopted: write the id by hand only
once you have confirmed the directory is this store's archive.

## Switching the user unit's image

Until Sparrow issue #22 moves the live store to the system unit, it runs under
the operator's old user unit, `sparrow.service` under `systemctl --user`. The
system unit's scripts refuse that unit. `deploy/user-unit-upgrade.sh` switches
it from one kept image pair to another:

```bash
deploy/user-unit-upgrade.sh --from sparrowd-schema71-fda2812 --to sparrowd-schema71-b420503 --keep-copy /SPARROW/store.before-b420503
```

The live service runs `sparrowd-schema71-fda2812`. The next image keeps schema 71, so no CLI or
producer changes with it; a switch that changes the schema number makes the daemon refuse the
clients of every other schema, since it admits only its own. "Switch 1" and "Switch 2" below write
out the two steps that bring the live store to the next image and then enable the archive worker.

Both arguments name daemon images under `~/.local/libexec/sparrow`. Each CLI
image is the daemon's name with `sparrowd-` replaced by `sparrow-`, and the
schema numbers come from the names. Run it as the operator, only on the
coordination session's go. `SPARROW_PREFIX` (default `~/.local`),
`SPARROW_CONFIG` (default `~/.config/sparrow/server.toml`, which names the store
and its sockets) and `SPARROW_BIN` override the layout; `--help` lists them.

What an upgrade relies on: there is no backup to restore from. The undo is the kept previous
image and the link swap that points the daemon and CLI links back at it. That undo ends when
`--to` opens the store and upgrades its format: the `--from` image refuses it (the store fence).
`--keep-copy DIR` keeps a copy of the closed store for that case: after the daemon has stopped
and before `--to` starts, the script copies the store to the new directory `DIR` (a reflink copy
when the store and every file in it have copy-on-write off, a full copy otherwise), compares the
copy's file list with the store's, and gives `DIR` its name only then. The way back after `--to`
has opened the store is that copy in the store's place ("Switch 1"); without a copy the only way
out is forward on `--to`.

`--hold-after-copy` (with `--keep-copy`) pauses the switch after the copy is complete and before
the links change or `--to` starts, so a rehearsal can run on a second copy while the daemon is
down. The script prints the kept copy and the seconds down, and waits with no timeout for
`proceed` (switch and start as usual) or `abort` (start `--from` again on the unchanged store,
restore the timer, exit non-zero). It reads the answer from the terminal, or, whenever
`--hold-answer FIFO` is given, from that named pipe; the prompt goes to `/dev/tty` even when the
output is redirected to a log. Ctrl-C, SIGTERM, a hangup, end of input and a removed or empty
pipe abort; the abort starts the old image before it writes anything and records its outcome in
`COPY.abort.log` beside the kept copy and in the journal. It refuses the option without
`--keep-copy` and without a terminal or pipe.

Before anything changes it refuses unless all of these hold:

- The daemon link names `--from`.
- All four images are executable.
- The `--to` pair matches `./bin` byte for byte.
- The `--to` daemon carries `cap_net_bind_service`.
- `server.toml` sets the ingest keys.
- The store has copy-on-write off.
- With `--keep-copy`, `DIR` is a new name in a writable directory outside the store, and the
  filesystem holds the store's size plus 10 GiB.
- The unit's drop-in directory holds only an identical `limits.conf`.
- The running daemon reports the `--from` schema.

The capability is a prerequisite done by a human with root,
`sudo setcap cap_net_bind_service=ep ~/.local/libexec/sparrow/IMAGE`; the
script checks it with `getcap` and never runs `setcap`.

Then it stops `sparrow-ship.timer` and waits for the shipper, stops the unit, copies the store
when `--keep-copy` is given, installs the `LimitNOFILE` drop-in, switches the links and starts
the unit. At the end it starts
`sparrow-ship.timer` again. If the shipper is still running after the timer stops, the script
starts the timer again if it was active before and changes nothing else.

It waits up to 10 minutes, the switch gate, for the first status answer. The
measured first open of schema 69 on a copy of the live store under the unit's
limits answered in 117.2 seconds (2026-11-07). It does not wait for pressure to report `ready`: that phase tracks
disk-budget maintenance, not the migrations. After the first answer it fails
unless the store identity is unchanged and the schema is the `--to` schema. It
prints each migration's `done`, `total` and `failure`, the watches and the
ingest socket, starts `sparrow-ship.timer`, and exits 0. The backfill continues online
in the running daemon. Watch it with `sparrow /SPARROW/store/rpc.sock status`
(`migrations`) and with `journalctl --user -u sparrow.service`, which logs a
`sparrowd: migration` start line per migration, a progress line every 64
pages and a completion line per migration, each with its pages and `done`; the
progress and completion lines carry `elapsed_seconds` since that migration's
first page, so the completion line gives each migration's own duration (see
"Online store migrations" in the interfaces).

`--help` gives the recovery for each failure point. A failure after the start
leaves the `--to` daemon running with the timer stopped, and it may already
have accepted ingest. The recovery goes forward first: wait for status, confirm
the store identity and schema, then start `sparrow-ship.timer`. Pointing the links back at
`--from` is possible only while `--to` did not change the store's format; after that the way back
is the kept copy.

### Which unit the switches address

The live daemon runs the user unit `~/.config/systemd/user/sparrow.service` (Sparrow issue 386 tracks
putting that file in the repository), with the drop-in `sparrow.service.d/limits.conf` that
`deploy/systemd/user/sparrow.service.d/limits.conf` supplies. The user unit sets
`KillSignal=SIGINT`, `TimeoutStopSec=120`, `Restart=on-failure` with `RestartSec=10`, at most five
starts in ten minutes, `MemoryHigh=2G`, `MemoryMax=4G`, `CPUQuota=100%`, `IOWeight=20` and
`Nice=10`, and it has no sandbox and no `SystemCallFilter`. `deploy/systemd/sparrow.service` is the
system unit; the live daemon does not run it, and neither switch reads or installs it. The image
carries the storage engine's priority calls that the system unit's `SystemCallFilter` admits
(#375), but that allowance is a unit-file change: it reaches a daemon only when the system unit is
installed, which is the cutover of Sparrow issue 22, and it does not reach the user unit.

### Switch 1: the payload index image, without an archive

Switch 1 moves the live daemon from `sparrowd-schema71-fda2812` to the image built from `main`
revision `b420503`, `sparrowd-schema71-b420503`. Two revisions are named separately. The image
revision, `b420503`, names the `--to` pair in `SPARROW_BIN`, whose sums the pull request that
merged this procedure records; the script requires that pair byte-identical to the kept copy in
`~/.local/libexec/sparrow`. The checkout that runs `deploy/user-unit-upgrade.sh` is `main` at or
after that merge, because only that script carries `--keep-copy`; its own revision has no bearing
on the image.

The schema number stays 71, so no client or producer needs a new build to keep working. The live
store reads `sparrow.store/340` (status of 2026-11-19). The new image does not set `[archive]` in
switch 1, so no archive worker runs, and the switch crosses only the payload index:

- At the first open, before the daemon serves, one synced batch lifts the store format from `/340`
  to `/395` (the payload index family, #323), deletes any stale `payload-id:` keys and seeds the
  `payload-index-upgrade` checkpoint and two zero counter rows. The batch is atomic: a kill leaves
  `/340` untouched or the whole of `/395`. It is the point of no return:
  `sparrowd-schema71-fda2812` refuses `/395` with `UNSUPPORTED: unsupported store format;
  migration required`. The downgrade check in `deploy/lib.sh` compares schema numbers only, so it
  cannot see this fence (Sparrow issue 385); the rules below, not the script, keep the old image
  off the store.
- The online `payload-index` migration then runs in the maintenance worker, with the daemon
  serving throughout. Pages of 1,024 artifacts or 64 MiB each commit atomically with their
  checkpoint, the migration resumes after a kill or power loss without redoing or skipping a
  page, and it waits below the free-space floor. The format stays `/395`. Until it completes,
  `payload` answers `pending`.
- Nothing else is crossed at `/340`: the store counters, owner-graph index, source summary and
  source context are already complete, the RDF profile does not advance, and no archive fence
  applies. The format after switch 1 is `/395`.

The stopped-daemon copy (below) holds the store before the first open, so it covers both the fence
and the migration. Restoring it discards everything the new image ingested after the switch.

Run it only on the coordination session's go, which names the window. The switch is a restart of
the live service: the coordination session announces it, before and after (see "While the daemon
is down").

Variables used below (set them in the shell that runs the steps):

```bash
LIB=~/.local/libexec/sparrow
OLD=sparrowd-schema71-fda2812;  OLD_CLI=sparrow-schema71-fda2812
NEW=sparrowd-schema71-b420503;  NEW_CLI=sparrow-schema71-b420503
CONF=~/.config/sparrow/server.toml
SOCK=$(sed -n 's/^socket_path *= *"\([^"]*\)".*/\1/p' "$CONF")
STORE=$(sed -n 's/^data_dir *= *"\([^"]*\)".*/\1/p' "$CONF")      # /SPARROW/store
COPY=/SPARROW/store.before-b420503     # a new sibling of $STORE (check: "$STORE" is /SPARROW/store)
BIN=…/bin       # the directory holding the b420503 pair; the script reads it as SPARROW_BIN
RECORD=…/.stage/image/switch-1   # under the main checkout's .stage/, never /tmp
```

#### Before the window

The daemon keeps running through these steps; none of them changes the store.

Keep the previous images beside the new pair, byte-identical, because `~/.local/libexec/sparrow`
holds the only copies of the `fda2812` pair today:

```bash
for f in "$OLD" "$OLD_CLI" sparrow-recovery-schema71-fda2812; do
    [[ -e $BIN/$f ]] || cp -p -- "$LIB/$f" "$BIN/$f"
    cmp -- "$LIB/$f" "$BIN/$f"
done
```

`BIN` holds the new pair and its `sparrow-recovery-schema71-b420503` from `make install-bin`; check the
pair's `sha256sum` against the sums recorded with the pull request that merged this
procedure.
Install the new pair, never over a name that holds other bytes, then have a human with root
give the daemon its bind capability (the script checks it with `getcap` and never runs
`setcap`):

```bash
for f in "$NEW" "$NEW_CLI"; do
    [[ -e $LIB/$f ]] || install -m 0755 -- "$BIN/$f" "$LIB/$f"
    cmp -- "$BIN/$f" "$LIB/$f"
done
sudo setcap cap_net_bind_service=ep "$LIB/$NEW"
getcap "$LIB/$NEW"
```

Record the live figures. `status` is read-only:

```bash
mkdir -p "$RECORD"
time ~/.local/bin/sparrow "$SOCK" status > "$RECORD/before.json"
df -B1 "$(dirname "$STORE")" > "$RECORD/df-before.txt"
du -s -B1 "$STORE" > "$RECORD/store-bytes.txt"
readlink "$LIB/sparrowd" ~/.local/bin/sparrow > "$RECORD/links-before.txt"
python3 -I -c 'import json,sys
v=json.load(sys.stdin)
for k in ("store_id","store_format","schema_version","counts","live_records","physical_bytes",
          "disk_budget","text_analyzer","rdf_profile","instance_role"):
    print(k, v.get(k))
print("pressure", v["pressure"]["phase"], v["pressure"]["failure"])
print("durability", {k: v["durability"][k] for k in ("flush_lag_ms","oldest_pending_ms","pending_writes","failure")})
for m in v.get("migrations", []): print("migration", m["name"], m["done"], m["total"], m["failure"])
for w in v["consume_watches"]: print("watch", w["id"], w.get("pending_claims"), w.get("failure"))
' < "$RECORD/before.json" | tee "$RECORD/figures-before.txt"
```

Record `store_id`, `store_format`, `text_analyzer` and `rdf_profile`; the checks after the
window compare them. The figures the checks need are free space and store bytes, which bound
the copy, and the ingest figures `flush_lag_ms`, `oldest_pending_ms` and `pending_writes`, which
stand for ingest latency and which the new image's figures are compared with. The CLI link
today names an older CLI than the daemon (Sparrow issue 387), so the live `sparrow` may lack
commands the image has; the script switches the link to the new CLI.
Check what the window needs.
- `status` shows `pressure.failure` and `durability.failure` null, every migration with `done`
  equal to `total` and `failure` null, and `counts` exact.
- The configuration names no `[archive]` table, no `instance_role` and no `backup_root` or
  `backup_keep`: `grep -nE '^(\[archive\]|instance_role|backup_root|backup_keep)' "$CONF"` prints
  nothing. A configured `instance_role` on a store that has no role row stops the daemon at its
  open; leave the key out (stamping is a later separate act, [Stamp the role](#stamp-the-role)).
- The copy has a home. The store directory and every file in it must have copy-on-write off for
  a reflink copy: `lsattr -d "$STORE"` shows `C`, and
  `find "$STORE" -type f -exec lsattr -- {} + | awk '$1 !~ /C/' | wc -l` prints 0. Then the
  copy is a reflink copy, which writes almost nothing when it is made. Any other result makes it
  a full copy, which writes every byte of the store (`$RECORD/store-bytes.txt`) and compares
  every file. Either way the filesystem must hold that size plus 10 GiB, because a reflink copy
  keeps every extent the running store later rewrites or compacts away and can grow to the
  store's whole size. `COPY` must not exist and its parent must be on the store's
  filesystem; the script refuses otherwise before it stops anything.
- The recorded `text_analyzer` and `rdf_profile` stay the same after the switch. The new image
  reads the lexicon bundle that `text_lexicon_dir` names; the PurRDF revision in
  `registry/compatibility.toml` is unchanged since `fda2812`, so the bundle built with the image
  (`bin/lexicons-PURRDF_REV`) is the same tree. A changed `text_analyzer` after the open means the
  directory differs from the one the store was built with ("Refused open" in `deploy/RECOVERY.md`).

Give the notice (below) and wait for the go.

#### The window

```bash
cd MAIN_CHECKOUT       # main at or after the merge of this procedure; the script finds lib.sh beside itself
SPARROW_BIN="$BIN" deploy/user-unit-upgrade.sh --from "$OLD" --to "$NEW" --keep-copy "$COPY" \
    > "$RECORD/switch.log" 2>&1; echo "EXIT=$?" >> "$RECORD/switch.log"
```

Read `$RECORD/switch.log`. The script refuses, before anything changes, unless the daemon link names
`--from`, the four images are executable, the new pair is byte-identical to `SPARROW_BIN`, the new
daemon has `cap_net_bind_service`, `server.toml` carries the ingest keys and no removed backup
keys, the store is no-COW, the drop-in directory holds only the repository's `limits.conf` (the
live host already has an identical one), the running daemon reports the `--from` schema and the
copy's name and room are sound. Then it stops `sparrow-ship.timer` and waits for the shipper,
stops the unit through the SIGINT drain (up to 120 s), **copies the closed store to `COPY`**, links
the new pair, starts the unit and waits up to 600 s for the first status answer. It checks the
store id and the schema, prints the migrations and the ingest socket, starts the timer and exits 0.
The online backfill continues in the running daemon.

The copy sits between the stop and the start, so the new image has not opened the store while it is
made. The script copies the store with `cp -a`, `--reflink=always` when `lsattr` shows the whole
store no-COW and `--reflink=never` with a byte comparison of every file otherwise, into
`COPY.partial`, compares the two file lists (names, types, sizes, modes and link targets, sockets
left out) and renames `COPY.partial` to `COPY` only when they agree, so a `COPY` that exists is a
complete copy. It logs the method, the file count and the bytes. A failed copy leaves the daemon
stopped, the links on the old pair and the store untouched: remove the one `COPY.partial` path,
start `sparrow.service` and `sparrow-ship.timer`, and rerun (`deploy/user-unit-upgrade.sh --help`
gives the recovery for each failure point).

What the file list does not prove. It proves every file is present at its size. A reflink copy
shares the store's extents, so its bytes are the store's bytes by construction; the full copy
compares them. Neither proves the store's own contents are sound. The stronger offline check the
repository offers is the recovery probe, `sparrow-recovery-schema71-fda2812 COPY.toml
STORE_ID RFC3339 EVIDENCE_ID...`, which opens the copy with the old image, checks the store id,
and reads and hashes the named artifacts and records (up to 1,024 targets, 1 GiB; see "Recovery
qualification"). It is a selected-evidence probe, not a scrub of the whole engine, and opening
the copy adds a WAL and manifest to it, so run it only on a second reflink copy
(`deploy/copy-on-write.sh copy "$COPY" "$COPY.probe"`) whose configuration names that directory
for `data_dir` and `socket_path`, and remove that one probe path afterwards. Neither the new daemon
nor `sparrow-recovery-schema71-b420503` may open `COPY` or `COPY.probe`: either lifts the format and
spoils the copy as a way back.

#### After the window

Record `status > "$RECORD/after.json"` and compare with `before.json`:

- `store_id`, `text_analyzer` and `rdf_profile` equal the recorded ones; `schema_version` is 71;
  `store_format` is the new image's (it differs from the recorded one: that is the fence).
- `migrations` lists `payload-index` with its `done` and `total` and `failure` null; watch it
  until `done` equals `total`, with
  `journalctl --user -u sparrow.service -g 'sparrowd: (migration|online migration failed)'`.
  The journal holds a start line, a progress line every 64 pages and a completion line with
  `elapsed_seconds`. The line `sparrowd: store_open_ms ... total=N` gives the open's own time.
- `sparrow "$SOCK" payload STORE_ID PAYLOAD_ID` for a payload id taken from an earlier ingest
  receipt (`ingest-get`) lists that artifact; `answer_certainty` reads `pending` while the migration
  runs and `exact` once `payload-index` is complete.
- `pressure.failure` and `durability.failure` are null; `flush_lag_ms`, `oldest_pending_ms` and
  `pending_writes` are within the recorded range; every watch has its `pending_claims`
  draining.
- `sparrow "$SOCK" discover` (and the same command on the ingest socket) prints
  `instance_role` as `unspecified`.
- `readlink "$LIB/sparrowd" ~/.local/bin/sparrow` names the new pair.

The measured window. Downtime, from the unit stop to the first status answer: [to be measured]
seconds, with the copy's share [to be measured]. The `payload-index` migration: the artifact count is
the migration's `total` (the status does not carry it before the open), at 1,024 artifacts a page,
so `ceil(total / 1024)` pages. The rehearsal on a scratch store without resource limits
(`.stage/rehearsal-323-migration/REPORT.md`) ran 13,100 artifacts in 11 pages and 1.846 s after
2 pages were already done. The live figure under the unit's `CPUQuota=100%`, `IOWeight=20` and
live intake: [to be measured] pages in [to be measured] seconds.

#### The way back

The decision is one question: did the new image open the store? It did when the journal for its
start holds the line `sparrowd: store_open_ms`; any other doubt also counts as opened. The open is
where the format fence commits.

- Not opened (the new unit failed before `store_open_ms`: an unreadable configuration, the public
  port 518 refused, a refused capability, the free-space wait). The store is untouched. Stop the
  unit, which may be in its restart loop, point both links back, clear a spent start limit and
  start:

  ```bash
  systemctl --user stop sparrow-ship.timer sparrow.service
  ln -sfn "$OLD" "$LIB/.sparrowd.next" \
    && mv -T -- "$LIB/.sparrowd.next" "$LIB/sparrowd" \
    && ln -sfn "$LIB/$OLD_CLI" ~/.local/bin/.sparrow.next \
    && mv -T -- ~/.local/bin/.sparrow.next ~/.local/bin/sparrow \
    && test "$(readlink "$LIB/sparrowd")" = "$OLD" \
    && test -d /SPARROW/store \
    && systemctl --user reset-failed sparrow.service \
    && systemctl --user start sparrow.service
  ```

  The start runs only when both links name the old pair. If a link command fails, start nothing
  (the new image would open and fence the store): read the message, fix the cause and repeat the
  chain. Then check `status` against `before.json` and start `sparrow-ship.timer`. Remove `COPY` by the rule
  below once the old image has served a day.
- Opened. The old image refuses the store, so the way back is the copy, which holds the store as it
  was when the daemon stopped:

  ```bash
  systemctl --user stop sparrow-ship.timer sparrow.service     # SIGINT drain, up to 120 s
  systemctl --user show -p ActiveState --value sparrow.service  # inactive
  ps -C sparrowd                                                # prints no process
  mv -T -- /SPARROW/store /SPARROW/store.opened-by-b420503 \
    && mv -T -- /SPARROW/store.before-b420503 /SPARROW/store \
    && test -d /SPARROW/store \
    && test ! -e /SPARROW/store.before-b420503 \
    && ln -sfn "$OLD" "$LIB/.sparrowd.next" \
    && mv -T -- "$LIB/.sparrowd.next" "$LIB/sparrowd" \
    && ln -sfn "$LIB/$OLD_CLI" ~/.local/bin/.sparrow.next \
    && mv -T -- ~/.local/bin/.sparrow.next ~/.local/bin/sparrow \
    && test "$(readlink "$LIB/sparrowd")" = "$OLD" \
    && systemctl --user reset-failed sparrow.service \
    && systemctl --user start sparrow.service
  ```

  The chain starts the daemon only when `/SPARROW/store` exists as the restored copy and both links
  name the old pair. A start on a missing store makes a new empty store with a new id, so when any
  command fails, start nothing. If the first `mv` fails, nothing moved: fix the cause and repeat. If
  the first succeeded and the second failed, `/SPARROW/store` is absent: put the opened store back
  with `mv -T -- /SPARROW/store.opened-by-b420503 /SPARROW/store`, fix the cause, and begin the
  chain again. If a link command failed after both moves, the store is in place: repeat the link
  commands from the chain, then the `reset-failed` and the start.

  The configuration is unchanged: `data_dir` still names `$STORE`. Check `store_id`, `store_format`
  and the migrations against `before.json`, then start `sparrow-ship.timer`. The restored store
  lacks everything the new image accepted between its start and the stop: the records and files it
  ingested, and the files its consume watches took and deleted. Count that loss before the
  swap (`live_records` in `after.json` less `before.json`, the start and stop times) and report it
  with those figures; `/SPARROW/store.opened-by-b420503` still holds it, and the new image can open that
  directory later from a configuration that names it. Fixing forward on the new image is the
  other choice, and the way back is for a defect that forward cannot fix.

Keep the copy until both hold: every migration reports `done` equal to `total` with no failure,
and a full day has passed since the first status answer with `pressure.failure`,
`durability.failure` and every migration `failure` null and no `online migration failed` line in the
journal. The operator then removes it with one command on one exact path, typed in full in a fresh shell
and after listing it:

```bash
ls -ld /SPARROW/store.before-b420503
```

```bash
rm -r -- /SPARROW/store.before-b420503
```

If the way back was taken, the copy is the store now, and the path to remove under the same rule,
on the coordination session's word, is the new store the new image opened:
`/SPARROW/store.opened-by-b420503`.

#### While the daemon is down

The window runs from the unit stop to the first status answer: the drain (up to 120 s), the copy,
the first open and its migrations' first page. The first status answer of the new image is not the
end of the work; the index fills afterwards with the daemon serving.

- Ingest socket (`ingest_socket_path`, the `sparrow-ingest` group) and the public TCP listener on port
  518 refuse connections. Every daemon that logs into Sparrow (`katamari-stored`, the Lillith daemons
  and `lillith_memetics`' cruft sender) is affected. A client that holds and retries, such as the
  Rust client with its queue, delivers after the start; a sender without a queue loses what it emits
  meanwhile and the store cannot count that loss ("A floor refusal requests reclamation"). The
  coordination session holds which senders queue, so the notice goes to it before the stop, and
  asks it to say whether the organism runs in the window: her logs are the logs of those daemons.
- The consume and tail roots under `~/.local/state/sparrow` stay on disk: a file that arrives waits and
  the watches read it after the start. The script stops `sparrow-ship.timer` first and starts it
  again at the end; the shipper reads only the producers' files and writes only into the roots, so the
  spool units (`katamari-journal-spool`, `katamari-manager-spool`) keep writing their files and lose
  nothing. A producer that rotates or deletes its own file in the window is outside Sparrow's reach.
- `lillith_decodes`, the CLI and the MCP tools get connection refused until the first status answer.
- No syslog listener is configured at the last recorded status, so no datagram is exposed; check
  `syslog_listeners` in `before.json`.

Notice. Before: the window's start and its expected length (the downtime figure above), that the
live daemon stops, that senders without a queue lose their output, that the switch is one-way once
the store opens, and that a copy of the store is taken and kept. After: the first status answer's
time, the image, `store_id`, that `payload` lookups read `pending` until the index migration
completes (and `exact` after), that nothing needs a new client, and the day on which the copy goes.

#### What each sender sees of the role

The live store has no role row, so `discover` and `status` report `instance_role: unspecified`
after the switch as before it. A sender that must not reach the live store sends only when the
role reads `scratch` and refuses `production` and `unspecified`, so a scratch-only sender, such as
the test-load sender, refuses the live store. Senders to the live store do not check the role and
see no change. Stamping the live store `production` is a separate later act, not part of switch 1: the
offline stamp ([Stamp the role](#stamp-the-role)) runs against the stopped store on the
coordination session's order, and the configuration keeps `instance_role` out until then.

### Switch 2: the archive worker

Switch 2 adds the `[archive]` table to `server.toml` and restarts the same image. It begins only
after switch 1 is complete (every migration `done` equal to `total`) and when the archive
directory's mount is present: the mount is host state this procedure never changes. It is a second
one-way fence: the first open with a policy lifts the store from the payload index intake family
(`/395` after switch 1) into the payload index archive family, `/396` to `/406` (the store keeps its
position in the family, so `/395` becomes `/406`; `docs/archive-format.md`, section 7,
"Deletion gate and store fence"). Older images refuse it, and reopening it without `[archive]`
fails with `INVALID_ARGUMENT`. The worker then
archives the whole backlog to the NAS before anything is retired, and from then on an unpinned item
is retired once it is due and archived.

Verify the mount first; the worker refuses a directory that is on the root filesystem's device or
on a local filesystem of the root's type, and never creates `dir`:

```bash
ARCH=/net/synology/archives/sparrow-archive
findmnt -no TARGET,FSTYPE,SOURCE -T "$ARCH"      # TARGET is the share's mount point, not /, and not $ARCH itself; FSTYPE is nfs4 or another network type
[[ $(stat -c %d "$ARCH") != $(stat -c %d /) ]]   # its own device
[[ -d $ARCH ]]                                   # the directory exists on the share
t=$(mktemp -d -p "$ARCH" .probe.XXXXXX) && rmdir -- "$t"   # writable by the operator; leaves the directory empty
ls -A "$ARCH"                                    # empty, or this store's STORE_ID and <store_id>/ only
```

If the directory is missing on a mounted share, confirm with `findmnt -T /net/synology/archives`
that the parent is the share's mount (a local directory on `/` means the mount is absent: stop),
then the operator creates that one directory with `mkdir`; no other change is made to the share. A directory that holds other entries without a
`STORE_ID` is refused by the worker and is not adopted.

Verify memory next:

```bash
systemctl --user show -p MemoryHigh -p MemoryMax -p MemoryCurrent sparrow.service
grep -E 'MemAvailable|SwapFree' /proc/meminfo
```

`MemAvailable` must exceed `MemoryMax` (4 GiB).

Named risk: memory. The worker holds one segment resident while it encodes it: up to 390 bytes a
quad, 3,000,000 quads (`max_segment_quads`) is 1.17 GB, and with the daemon's roughly 100 MB that is
1.27 GB, about 59 per cent of the user unit's `MemoryHigh=2G` with `MemoryMax=4G` above it
(`crates/sparrow-core/src/storage/archive.rs`, `ARCHIVE_SEGMENT_QUADS`). The figure is a derived
bound, not a measurement on the live daemon, and the daemon's own memory during a backlog has
reached the 2 GiB throttle before. The measurement that settles it, not yet run: a scratch
instance (`deploy/test-instance.sh` with `SPARROW_TEST_ARCHIVE_DIR`) archives a pass at the full
segment size under `systemd-run --user --scope -p MemoryHigh=2G -p MemoryMax=4G`, recording the
scope's peak resident memory (`memory.peak`) and any throttling (`memory.events`, the `high`
counter). The default segment settings stay unless that measurement says otherwise; lowering
`max_segment_quads` in `[archive]` shrinks the bound.

The window, with the daemon stopped around a copy as in switch 1:

```bash
COPY2=/SPARROW/store.before-archive    # a new sibling of $STORE
cp -p -- "$CONF" "$CONF.before-archive"
systemctl --user stop sparrow-ship.timer       # wait until sparrow-ship.service is inactive
systemctl --user stop sparrow.service          # SIGINT drain
```

Make the copy from the checkout at the merged revision. Go on only when it exits 0:

```bash
deploy/copy-on-write.sh copy "$STORE" "$COPY2"; echo "copy exit $?"
```

If it exits nonzero the store is untouched and `[archive]` is not set. Remove the one
`/SPARROW/store.before-archive.partial` path if it exists, start `sparrow.service` and
`sparrow-ship.timer` as they were, and stop the switch: the cause (name exists, open file, space)
is read from the command's message, and the switch starts again from the notice. Only on exit 0:

```bash
printf '\n[archive]\ndir = "%s"\n' "$ARCH" >> "$CONF"
systemctl --user start sparrow.service
sparrow "$SOCK" status                          # repeat until it answers
systemctl --user start sparrow-ship.timer
```

`[archive]` goes last in the file, after the top-level keys. The defaults are `start_fraction`
0.75, `max_segment_bytes` 64 MiB, `max_segment_records` 10,000, `max_segment_quads` 3,000,000,
`check_secs` 60, `stall_warn_secs` 120 and `io_timeout_secs` 900. The checks before, during and
after are the switch 1 list (the figures, the copy rule and the notice apply as there), and in
addition `archive_worker.phase` moves from `notConfigured` to `idle`, `preparing`, `writing` or
`verifying` with `failure` null; `STORE_ID` appears in `$ARCH` holding the store id; segment files
appear under `$ARCH/<store_id>/`; and `sparrow-archive verify "$ARCH"` reads them back. A
`failure` naming `ARCHIVE_NOT_ON_MOUNT`, `ARCHIVE_ROOT` or `ARCHIVE_IO` means the mount is wrong or the
share refuses: the daemon serves, the worker retries each pass, and nothing is deleted. The first
pass's length and its demand on the NAS: [to be measured].

The way back. If the new unit failed before `sparrowd: store_open_ms`, restore `$CONF.before-archive`
over `$CONF`, stop the unit (it may be in its restart loop), run
`systemctl --user reset-failed sparrow.service`, start the unit, and check `status`. After it, the store is fenced and the old
configuration refuses it, so the way back is the copy, with the guard of switch 1 (stop the timer and
the unit first, and confirm the unit is inactive):

```bash
mv -T -- /SPARROW/store /SPARROW/store.opened-archive \
    && mv -T -- /SPARROW/store.before-archive /SPARROW/store \
    && test -d /SPARROW/store \
    && test ! -e /SPARROW/store.before-archive \
    && cp -p -- "$CONF.before-archive" "$CONF" \
    && systemctl --user reset-failed sparrow.service \
    && systemctl --user start sparrow.service
```

The start runs only when the copy is in place and the configuration is restored. When a command
fails, start nothing, and recover as in switch 1: if the first `mv` fails, nothing moved; if only the
second failed, run `mv -T -- /SPARROW/store.opened-archive /SPARROW/store` and begin again. The
restored store lacks what the daemon took in after its start, and the archive directory holds
segments for a store that no longer carries the matching watermark: leave the directory as it is
and report it. Keep `/SPARROW/store.before-archive` under the same rule as the switch 1 copy, removed as one literal path: migrations done, a full day of clean
status and a clean `archive_worker`.

### Part sets and older builds

A consume root that holds a file above the import bound is split into parts and a
manifest by this build. An older build that meets an unrecorded set claim stalls on
that claim only, loudly: the claim stays pending, the source file stays in place,
and the failure is logged and recorded; run this build to finish it. A set claim
that this build has recorded but not yet removed cannot be finished by any build
once an older build has run the expiry sweep on the store more than two days after
the manifest was accepted: the sweep deletes the manifest's receipt, which this
build needs to prove the publication before it removes the source. Do not run an
older build on a store that holds recorded set claims that are not removed. If it
has happened, follow the recovery steps below. No shipped command clears such a
claim yet (issue 283 builds the tool that does); until then the claim stays
pending, the source stays in place, the stored manifest and parts remain, and the
claim holds one of the sixteen pending-claim slots (`MAX_CONSUMPTION_JOURNALS` =
16), so sixteen such claims stop all new claims on that daemon. One stuck claim
does not block the others, because the worker takes any due claim; each failed
attempt does drop the shared consume connection, so every retry costs a
reconnect. This build does not upload a stuck claim again: on each retry it records
the named failure and nothing else. An older build also lets an ingest-socket
producer write a `consumeset:` request id; this build refuses such a manifest
receipt by name. There is no stored-format fence. Clear every publish hold
(`publish_hold`, see "Publish hold") before any older build runs: an older build
refuses a catalog that carries a hold, by naming the field.

Recovery for a recorded set claim whose manifest receipt is gone:

1. Stop the older build and run this build. It protects every receipt still
   present; it does not restore a deleted one.
2. List the affected claims with `watch claims` or by ingestion status. A claim is
   affected when its pending reason or status names the missing publication
   receipt.
3. Confirm the data is published: compare the artifact ids the manifest lists
   (the manifest is the claim's `published_artifact`) with the artifacts present
   in the store. Keep the source in place under the claim's private name in the
   root's `.sparrow-inflight/`.
4. Do not delete the source by hand. A hand deletion does not clear the claim:
   recovery of a missing source still needs the proof and fails the same way.
5. Leave the claim pending. It holds a journal slot and its attempt text keeps
   naming the state. Clear it with `watch clear-set <claim>` once issue 283 is
   built.

### Publish hold

A watch definition carries `publish_hold`, false unless an operator sets it with a
watch put. A held finished-file watch still detects and claims files: each claim is
journaled and moved into the root's `.sparrow-inflight/` as an interrupted claim is,
and then it waits. Nothing is published, dropped or cancelled, and the wait has no
deadline. Clearing the hold with another watch put (a put that omits the field clears
it) lets the claims publish in pending order. A tail watch cannot be held.

The hold exists for an operator's deliberate pause of one watch. It is not a way to refuse input: held files are still taken
from the producer, and a held claim keeps one of the sixteen pending-claim slots
(`MAX_CONSUMPTION_JOURNALS`) until the hold clears.

A hold never stops another watch or a tail. A held watch reserves at most four
files; further ready files stay in the producer's directory, unclaimed and
untouched, and are claimed in order once the hold clears and slots free. All held
watches together hold at most twelve pending claims (the sixteen slots less four), so
at least four slots stay free for unheld watches and tail generations; a held claim
that would take the total past twelve waits instead. Status names the state: `publish_hold`, `held_claims` (the watch's
pending finished-file claims) and `oldest_held_first_seen`, which is the
reservation time of the oldest of those claims. It is not the time the hold was
set, so a claim that waited before the hold shows its own age. The daemon writes
one `WARNING` line per hold at start and when a hold is set.

A catalog that carries a hold is refused by older builds, which name the
`publish_hold` field: clear every hold before an older build runs.

A held claim whose move into `.sparrow-inflight/` fails is recorded as a pending
attempt and logged once, and again only when the failure text changes.

A part of a set keeps its receipt for as long as its claim's journal exists, as the manifest
receipt does. A part sent again after an outage, even one longer than two days, with the same
cut returns its saved receipt for as long as its artifact is stored, and is not stored a second
time, so a records query over the source returns its bytes once. A part whose artifact eviction
or deletion retired is sent again from the claim's source, and its bytes come back under a new
publication time; while only some of its records are retired the claim is pending and nothing is
sent. A resumed set reads the daemon's recorded parts on every attempt and cuts only the rest
under the advertised limits as they stand, so a change of limits is absorbed. A recorded part
whose span the new limits no longer admit cannot be cut again, because later parts start where
it ends: the claim stays pending, holding its source, until limits that admit that span are
restored. No retirement removes such a claim while its source is held.
