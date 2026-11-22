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

Configure producer spools on local storage outside every consume root.
Each writer requires its own private directory (mode `0700`). Size
`spool_max_bytes` for the expected offline interval and retain free space for
metadata and checkpoint replacement. Resume with the same source and spool
directory. The SDK drains and removes confirmed segments automatically.
For an explicit drain, run the `sparrow-client` example
`producer ENDPOINT SOURCE lossless SPOOL_DIR 0 1 drain` after the previous
writer releases the directory. Inspect its final JSON progress and exit code.

The first conditional sealed request eligible for new publication enables the receipt-retirement
watermark and its raw writer format. Use a daemon that supports store writer
families 407 through 461 when reopening an upgraded store. To roll back to an
older binary, restore a compatible pre-upgrade store snapshot. See
[publication and durability](architecture.md#61-publication-and-durability).

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
the disk is full latches durability uncertain, and writes are refused until the proof of
durability succeeds or the store is reopened. To avoid that, every write is checked first: below the floor it is refused with a
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
the daemon's memory and then the kernel buffer only. At startup it waits for the maintenance
reserve plus the write-ahead log an unclean stop left (the open replays it) before opening the store, so a restart on a full disk does not use up the
unit's bounded restarts; below the floor alone it opens and serves reads. If a write
still fails with durability uncertain, for example because another program
filled the disk between checks, writes are refused with the retryable
`STORAGE_PRESSURE`. The maintenance worker leaves the store alone while free space is
below RocksDB's 32 MiB recovery buffer, proves durability once space has been adequate for
15 s, and clears the latch in place when the proof succeeds; the daemon stays up. A latch
that still stands 120 s after space became adequate, which is what an I/O error other than
no space (EIO) is expected to leave until #449 lands, makes the daemon exit with an ordinary
failure status so that `Restart=on-failure` reopens the store and the start waits for
space.

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
sets `data_dir` to mode 0710 at start and the socket to 0660 each time it binds it (at
start, and again when the name was removed while it ran), so
members of the daemon's group can connect and cannot list or read the store;
store files stay owner-only under the unit's `UMask=0077`. Only the operator
belongs to that group, for the CLI and administration. Fleet producers ingest
through the ingest-only group socket below, not this socket.

### Syslog TCP listener install steps

A `[[syslog]]` listener with `transport = "tcp"` keeps the frames it holds at a stop in
`/SPARROW/syslog-spill`, beside the store: a frame the store still refuses for pressure when the
daemon stops is written there and published at the next start (`docs/interfaces.md`, syslog TCP).
The shipped unit, profile and tmpfiles entry do not grant that directory, so the host needs these
steps before the first such listener is configured, and the daemon refuses to start with an error
naming the directory when it cannot write there:

1. Add `d /SPARROW/syslog-spill 0700 sparrow sparrow -` to `/etc/tmpfiles.d/sparrow.conf` and run
   `systemd-tmpfiles --create sparrow.conf`.
2. Add `/SPARROW/syslog-spill` to the unit's `ReadWritePaths=` and run `systemctl daemon-reload`.
3. Add `/SPARROW/syslog-spill/ rw,` and `/SPARROW/syslog-spill/** rwk,` to
   `/etc/apparmor.d/sparrow` and reload the profile with `apparmor_parser -r`.
4. Restart the daemon on the new unit.

A configuration with only UDP listeners needs none of this.

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
`NAME.partNNNNN.EXT` chunks. An unterminated final line counts as a record.
Set `max_chunk_lines` to an integer from 1 through 32,768.
Other formats, such as single JSON documents, are
shipped whole up to Sparrow's 7.93 GiB import bound; a larger file is reported
as a failed file on every run and never queued. The removed `max_whole_bytes` key must be deleted
from the shipper configuration; a configuration that still sets it is refused
before anything ships. The split matters for two reasons. The consume worker moves a file
the daemon refuses at an input bound, such as one over the record limit, into a quarantine
directory beside the watch root and retires its claim; it is never published (see
[Quarantining a permanently refused source](interfaces.md#quarantining-a-permanently-refused-source)).
Every other failed publication retries without limit, backing off from five seconds to one
attempt every 15 minutes after consecutive failures. Deadline expiries back off too, even when the
resumed claim advances, so a chunk that needs many attempts finishes later. Keep
`max_chunk_lines` below the server's `max_records`. A consume publication has a
45-second deadline.
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

### Deleting acknowledged spool files

A source with `delete_acknowledged = true` is a spool the shipper owns, such as
the Katamari journal spool below; never set it on a producer's own directory.
At the end of each run the shipper deletes a finished file of that source once
the store holds every byte of it, and nothing else ever deletes one. It hashes
each part the file ships as (the whole file, or each line-bounded part, split
exactly as when it was queued) with `b3sum` and asks the store
`sparrow SOCKET payload STORE_ID blake3:HEX`. The file goes only when every part
lists an artifact, which the store answers only while it holds all of that
artifact's bytes ([Payload id index](interfaces.md#payload-id-index-store-format)).
The shipper also accepts a matching whole-file artifact for files an earlier shipper
sent whole because it excluded the unterminated last record from the count.
A file that is still queued, waiting in a consume root, refused under storage
pressure, quarantined, or held once and since retired lists nothing and stays.
The ledger keeps its entry either way.

The source needs the top-level keys `sparrow` (the CLI) and `socket` (the store
socket the CLI queries), and `b3sum` on the path; the configuration is refused
at load without them. While the store does not answer, the run reports
`store unreachable` and deletes nothing, however long the outage lasts, without
failing.

Deletion takes two runs, because a spool script opens a file by name and appends
a line a moment later, so a line can land in a file the store already holds. In
the run that finds every part of a file held, the shipper links the file to
`.reaping-NAME` and unlinks NAME, which neither the shipper nor the spool scripts read; a script
that opens NAME afterwards creates a new file under it. At its next run the
shipper looks at each `.reaping-` file again and deletes it only if the store
holds the entire file and its identity is stable through the lookup.
A file that now holds a line the store
lacks goes back under NAME (under `NAME-late-NS` when NAME exists again) and
ships like any finished file, so those lines appear twice at most. A file whose
parts the store held and has since retired goes back too and stays in the spool.
The run reports how many files it deleted, set aside, and did not find
acknowledged. The parts are cut with the shipper's current `max_chunk_bytes` and
`max_chunk_lines`; after a change of either, a file queued earlier hashes
differently, is reported as not acknowledged and stays.

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
widened and `delete_acknowledged` set, with no new watch and no change to the
ledger (it keys each file):

```toml
sparrow = "/home/paudley/.local/bin/sparrow"
socket = "/SPARROW/store/rpc.sock"

[[source]]
name = "katamari.stored"
dir = "/home/paudley/.local/state/sparrow/katamari-journal"
pattern = "katamari-*.jsonl"
rule = "settled"
format = "jsonl"
settle_seconds = 120
delete_acknowledged = true
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
deploy/user-unit-upgrade.sh --from sparrowd-schema71-fda2812 --to sparrowd-schema71-a50820e --keep-copy /SPARROW/store.before-a50820e
```

The live service runs `sparrowd-schema71-fda2812`. Both images in the example use schema 71.
A schema change requires a matching CLI and an upgrade of producers built before profile
negotiation. Starting with schema 72, a producer can connect across schema versions when both
ends support `sparrow-producer/1`. "Switch 1" and "Switch 2" below write
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
revision `a50820e`, `sparrowd-schema71-a50820e`. Two revisions are named separately. The image
revision, `a50820e`, names the source the binaries were built from: the pair was built with
`make install-bin` at that commit, which holds the shutdown cancel of #394 (`6b4d4b4`) and every
change Switch 1 takes. A commit that changes only documentation after it does not change the
binaries, so the image revision stays `a50820e` while `main` moves; `git diff a50820e..HEAD --stat`
at a later head shows whether anything but documentation changed. At `caeb2b7`, `git diff a50820e..caeb2b7 --stat` lists only `docs/deployment.md`,
`deploy/user-unit-upgrade.sh` and `tools/user-unit-upgrade-timers-check.sh`: no crate and no lock file changed, so the image revision is `a50820e` at that head. The image revision names the
`--to` pair in `SPARROW_BIN`, whose sums the pull request that merged this procedure records; the
script requires that pair byte-identical to the kept copy in `~/.local/libexec/sparrow`. The
checkout that runs `deploy/user-unit-upgrade.sh` is `main` at or after that merge, because only
that script carries `--keep-copy`; its own revision has no bearing on the image.

The schema number stays 71, so no client or producer needs a new build to keep working. The live
store reads `sparrow.store/340` (status of 2026-11-19). The new image does not set `[archive]` in
switch 1, so no archive worker runs, and the switch crosses only the payload index:

- At the first open, before the daemon serves, one synced batch lifts the store format from `/340`
  to `/395` (the payload index family, #323), deletes any stale `payload-id:` keys and seeds the
  `payload-index-upgrade` checkpoint and two zero counter rows. The batch is atomic: a kill leaves
  `/340` untouched or the whole of `/395`. It is the point of no return: `sparrowd-schema71-fda2812`
  refuses a lifted store at its open and exits with status 1. The refusal observed on stores
  lifted from `/332` to `/387` is `Error: Error { code: "STORAGE_FAILURE", message: "log time
  interpretation law and writer fence disagree" }` (below, "What the measurement covers"). On the live store's own copy lifted from `/340` to `/395`
  it was the same text with exit status 1 (`.stage/image/switch-1/rehearsal-old-refusal.log`). The code is `STORAGE_FAILURE`, not
  `UNSUPPORTED`. The downgrade check in `deploy/lib.sh` compares schema numbers only, so it cannot
  see this fence (Sparrow issue 385); the rules below, not the script, keep the old image off the
  store.
- The online `payload-index` migration then runs in the maintenance worker, with the daemon
  serving throughout. Each page examines up to 1,024 artifact entries, and ends sooner when the
  stored bytes of the entries it has read reach 64 MiB (the catalog rows, not payload;
  `crates/sparrow-core/src/storage/payload_index.rs`, `PAGE_BYTES` and `page_of`). A page commits atomically with its
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
NEW=sparrowd-schema71-a50820e;  NEW_CLI=sparrow-schema71-a50820e
CONF=~/.config/sparrow/server.toml
SOCK=$(sed -n 's/^socket_path *= *"\([^"]*\)".*/\1/p' "$CONF")
STORE=$(sed -n 's/^data_dir *= *"\([^"]*\)".*/\1/p' "$CONF")      # /SPARROW/store
COPY=/SPARROW/store.before-a50820e     # a new sibling of $STORE (check: "$STORE" is /SPARROW/store)
BIN=…/bin       # the main checkout's ./bin, which holds the a50820e pair; the script reads it as SPARROW_BIN
SUMS=…/.stage/408/sha256-a50820e.txt   # the recorded sha256 sums of the pair, named as they are in $BIN
RECORD=…/.stage/image/switch-1   # under the main checkout's .stage/, never /tmp
REH=/SPARROW/store.rehearsal-a50820e   # the rehearsal copy, a new sibling of $STORE
REH_CONF=$RECORD/rehearsal.toml        # its configuration; its socket is $REH/rpc.sock
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

`BIN` is the main checkout's `./bin` (the variables block writes the checkout's path as `…`), never
the `./bin` of a worktree: a worktree's `./bin` goes when the worktree goes, and a worktree that
holds the only copy of an image, or of the pair, is not cleaned. It holds the new pair and
`sparrow-recovery-schema71-a50820e` from `make install-bin`. The pair (`sparrowd` and `sparrow`) is
kept twice, in `BIN` and in `~/.local/libexec/sparrow`; the recovery binary is kept in `BIN`. The
sums are recorded in `$SUMS`, and the pair built from `a50820e` carries exactly those sums, so a
rebuild from that revision gives the same bytes (`rebuild.log` and `sha256-bin.txt` under
`.stage/image/switch-1/`). Check them; `--strict` makes a malformed line of the sums file fail, and the command exits nonzero on
any difference, a missing file or a malformed line:

```bash
( cd "$BIN" && sha256sum --strict -c -- "$SUMS" )
```

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
No payload sample is taken here, and nothing in this section depends on a file the operator
holds. The sample is taken in the rehearsal, from the copy ("The rehearsal on a copy", step 4):
the live `fda2812` daemon answers `payload` with `Unimplemented: Method not implemented` (one
lookup with the `a50820e` CLI, `.stage/image/switch-1/report-372.md`), so it cannot verify one.

Read free space on the store's filesystem again immediately before the window and state it in the
notice. The room rule is two copies (`COPY` and `REH`), each the store's bytes plus 10 GiB, against
free space. This command reads both figures, writes them to `$RECORD/room-now.txt` and exits
nonzero when `du` fails, when the Python step fails, or when free space is below the two copies. The
subshell runs with `set -e`, so the first failing step ends it with that step's status, and
`EXIT` is that status:

```bash
( set -euo pipefail
du -s -B1 -- "$STORE" > "$RECORD/store-bytes-now.txt"
python3 -I - "$STORE" "$RECORD/store-bytes-now.txt" > "$RECORD/room-now.txt" <<'ROOM'
import os, sys
used = int(open(sys.argv[2]).read().split()[0])
if used <= 0:
    sys.exit("du read 0 bytes for the store")
st = os.statvfs(os.path.dirname(sys.argv[1]))
free, need = st.f_bavail * st.f_frsize, 2 * (used + (10 << 30))
line = f"free {free} store {used} two copies {need} margin {free - need}"
print(line)
sys.exit(0 if free >= need else "room rule fails: free space is below two copies: " + line)
ROOM
cat "$RECORD/room-now.txt" )
echo "EXIT=$?"
```

The reading of 2026-11-19 (`df-before.txt`, `store-bytes.txt` and `06-arithmetic.log` under
`.stage/image/switch-1/`) is one reading, not a constant. Arithmetic on it: `du` 38,924,779,520
plus 10 GiB is 49,662,197,760 bytes (49.66 GB) per copy and 99,324,395,520 bytes (99.32 GB) for
two, against 115,870,515,200 bytes free: a margin of 16,546,119,680 bytes (16.55 GB). The
coordination session tells the other sessions that no large build runs during the window, which
removes the builds that would consume that margin; other writers to the filesystem remain.

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
- No scratch or rehearsal daemon from an earlier session runs. The script's check after the stop of
  `sparrow.service` (`deploy/user-unit-upgrade.sh`, the `ps -e -o comm=` list and the glob over it)
  stops the switch when any process whose name begins `sparrowd` is listed, and by then the live
  daemon is already stopped. The process list alone cannot be the gate before the stop, because the
  live daemon runs and is listed. The gate here reads each unit's `ActiveState`, and only `inactive`
  or `failed` passes: `active`, `activating`, `deactivating`, `reloading`, an empty value and a
  failing `systemctl` all stop. `is-active` is not used, because its exit 3 also covers a failed
  unit and, from systemd's source, a unit that is still activating or deactivating, whose `sparrowd`
  may still run. `show` of a unit that does not exist prints `inactive` and exits 0 (seen on this
  host with `LoadState=not-found`), so an absent unit passes, as it should:

  ```bash
  ok=1
  for u in sparrow-deploytest.service sparrow-rehearsal.scope sparrow-rehearsal-old.scope; do
      v=$(systemctl --user show -p ActiveState --value "$u") \
          || { ok=0; printf '%s: systemctl show failed, stop\n' "$u"; continue; }
      [[ $v == inactive || $v == failed ]] \
          || { ok=0; printf '%s: ActiveState %s, stop\n' "$u" "${v:-empty}"; }
  done
  ((ok))
  ```

  The loop prints a line for each unit that does not pass, and the last line exits 0 only when all
  three pass. It covers those three names. A `sparrowd` started outside them does not show there:
  read `ps -e -ww -o pid,user,comm,args` and expect exactly one `sparrowd` line, the live daemon, whose pid
  equals `MainPID` of `sparrow.service`. A `failed` unit can keep a process that survived systemd's final
  SIGKILL, in practice one stuck in uninterruptible I/O, such as a call blocked on the hard NFS mount, and
  the `failed` state passes the gate above; this listing catches it. The listing is read by the operator;
  the script's own check after the stop is the one that fails closed on a process.
- The recorded `text_analyzer` and `rdf_profile` stay the same after the switch. The new image
  reads the lexicon bundle that `text_lexicon_dir` names; the PurRDF revision in
  `registry/compatibility.toml` is unchanged since `fda2812`, so the bundle built with the image
  (`bin/lexicons-PURRDF_REV`) is the same tree. A changed `text_analyzer` after the open means the
  directory differs from the one the store was built with ("Refused open" in `deploy/RECOVERY.md`).

Take the live daemon's listing, which the rehearsal's check compares with the copy's ("The rehearsal on
a copy", step 4). Write `payload-check.py` first (the block under that heading). `--list` runs `$NEW_CLI`
against the live `fda2812` daemon: the wire delta is additive status fields and the listing path is the
same paged code, but no log shows this pairing working, and a failure stops this step with nothing
changed. The walk takes the writer lock for each page, about 5,055 pages at the live record count
(arithmetic from the walk's rate, below), so stop the shipper timer before it to keep the shipper's
ingest out of the walk, and start the timer again right after it, whether the listing passed or
failed. `deploy/user-unit-upgrade.sh` records at its start whether the timer is active and restores it
on an abort only then (lines 336-337, `restore_ship_timer`). The spool units delete no spool file, so
a timer left stopped by a called-off window delays shipping and loses nothing. Ingest between the
listing and the stop is expected: the comparison tolerates it. The walk has the stall limit of the rehearsal's
check, and the step records the walk's calls, wall time, the clock on both sides of the status read and the ingest figures
`flush_lag_ms` and `pending_writes` before and after it, in `live-listing.tsv.counts`. The block's
`list EXIT` is the listing's own status:

```bash
systemctl --user stop sparrow-ship.timer
python3 -I "$RECORD/payload-check.py" --list "$LIB/$NEW_CLI" "$SOCK" "$RECORD/live-listing.tsv" > "$RECORD/live-listing.log" 2>&1
list_exit=$?
systemctl --user start sparrow-ship.timer
echo "list EXIT=$list_exit timer $(systemctl --user is-active sparrow-ship.timer)"
cat "$RECORD/live-listing.log"
```

A nonzero `list EXIT` ends the step. Take the listing as late as the window allows. No threshold for
abandoning a walk that slows ingest can be derived: `before.json` holds `flush_lag_ms` and
`pending_writes` at 0 and no figure for the walk's effect. As a choice, read `status` in another shell
during the walk and stop the listing command (Ctrl-C, which changes nothing in the store) when
`flush_lag_ms` stays above `buffered_flush_interval_ms` of the configuration or `pending_writes`
keeps rising; then run the timer line of the block by hand.

Give the notice (below) and wait for the go.

#### Before the stop: the stop timeout, the quiesce and the watch

Three acts precede or accompany the script's stop, in this order.

1. Install a runtime drop-in that raises the unit's stop timeout for the window. The first stop of
   Switch 1 is the stop of the installed old image, `sparrowd-schema71-fda2812`, under the live
   user unit's `TimeoutStopSec=120` and `KillSignal=SIGINT`. A stop that outlasts the timeout ends
   in SIGKILL. The fix for the stop (#394, below) cannot reach that first stop, because it is in
   the new image; the stops of the new image measured below ran without it. The image `a50820e` differs from `b420503` in more than that fix (`git log b420503..a50820e` also lists, for example, the #392 upload cancellation changes and the instance role label), so the `b420503` figures below describe an image that is not the one switched to. The old image under the unit's limits stopped in 98.8 s, 82 percent of 120 s,
   on a store of 4.76 GB status `physical_bytes` at `/332` after six artifacts of 5,000 records
   (`220-E-diag-old.log`; no kill). The new image built from `b420503` (before the fix), under the same limits, stopped in 50.5 s, 25.6 s
   and 13.1 s on a base of 3.46 GB (`210-D-diag-new.log`, `212-D-diag-new2.log`,
   `213-D-diag-quiesce.log`; the 13.1 s run waited first for the compaction threads to go idle).

   The proposed value is `TimeoutStopSec=1800`. Basis, all arithmetic: 98.8 s at 4.76 GB scales by
   the live status `physical_bytes` of 38.98 GB to 98.8 / 4.76 x 38.98 = 809 s, and 1800 s is 2.2
   times that. The 38.98 GB is a live status read saved on 2026-11-19 (`.stage/image/live-status-1.json`,
   `physical_bytes` 38,980,324,844), not a measurement log; read it again from `before.json` in
   the window and redo the arithmetic with that value. The scaling assumes the stop follows the
   store's size, which the measurements neither prove nor refute. By status `physical_bytes` the
   stops were 61.4 s at 2.38 GB (1.57 GB on disk before the ingest; `122-C-bigNew-status.log`), 13.1 s to 50.5 s at
   about 5.1 to 5.4 GB (3.46 GB on disk before), 98.8 s at 4.76 GB with the old image, and 234.4 s
   at 7.38 GB (4.32 GB on disk before): they do not order by store size alone (the figures are
   under "What the measurement covers"). A longer timeout costs nothing when the stop is quick,
   because the unit is stopped when the daemon exits.

   The user unit is not in the repository (Sparrow issue 386). The script refuses any drop-in
   under `~/.config/systemd/user/sparrow.service.d` but its own `limits.conf` (it reads only that
   directory), so the timeout goes in the runtime directory, which the script does not read and a
   reboot removes:

   ```bash
   RT=${XDG_RUNTIME_DIR:?}/systemd/user/sparrow.service.d
   mkdir -p -- "$RT"
   printf '[Service]\nTimeoutStopSec=1800\n' > "$RT/timeout.conf"
   systemctl --user daemon-reload
   systemctl --user show -p TimeoutStopUSec --value sparrow.service     # 30min
   ```

   The daemon keeps running through the reload. If the last command prints `2min`, another setting
   overrides the drop-in: do not start the window. `systemctl --user set-property --runtime ...
   TimeoutStopSec=N` is refused (`Cannot set property TimeoutStopUSec, or unknown property`,
   `230-E-runtime-timeout.log`). The drop-in took effect both before a stop and 3 s into a stop
   in progress (`231-E-runtime-dropin.log`), on a scratch user unit running a stand-in process,
   not the daemon. This route has not been run on the real daemon or its unit.

   Remove the drop-in when the switch is complete: the script exited 0 and the checks after the
   window passed. The reload that the script runs after its stop does not touch the runtime
   drop-in, and a logout or reboot removes it earlier, which returns the stop to the unit's 120 s.
   The way back stops the new image under the same timeout, so write the same file again before
   that stop. Remove it as one command on one exact path, after listing it:

   ```bash
   ls -l "$XDG_RUNTIME_DIR/systemd/user/sparrow.service.d/timeout.conf"
   ```

   ```bash
   rm -- "$XDG_RUNTIME_DIR/systemd/user/sparrow.service.d/timeout.conf"
   ```

   then `systemctl --user daemon-reload`.

2. Quiesce, as a precondition check. Read `status` until `pressure.pending_flushes` and
   `pressure.pending_compactions` are both 0 and `durability.pending_writes` is 0 on three
   consecutive reads, ten seconds apart, within a bound of 10 minutes:

   ```bash
   ~/.local/bin/sparrow "$SOCK" status | python3 -I -c 'import json,sys
   v=json.load(sys.stdin)
   print(v["pressure"]["pending_flushes"], v["pressure"]["pending_compactions"], v["durability"]["pending_writes"])'
   ```

   The bound is a proposal: 10 minutes is 300 flush intervals of the 2,000 ms
   `buffered_flush_interval_ms` (arithmetic), long enough for the daemon's own buffered work and
   short enough to hold a window. The quiesce does not shorten the stop. The counters read 0 before
   every stop that had a status read, including the stops of 50.5 s and 98.8 s
   (`210-D-diag-new.log`, `220-E-diag-old.log`, `33-post-ingest-status.log`,
   `122-C-bigNew-status.log`); a zero reading did not predict a short stop. The counters read above 0 under a pressure phase of `requested` (`pend_comp=3`,
   `52-A-ingest.log` to `59-A-ingest3.log`), so a nonzero reading is real. If the bound passes
   with a counter above 0, do not stop the daemon under that load: wait, read `pressure.phase` and
   the journal, and report the figures to the coordination session. Stopping anyway, which
   accepts a longer stop and the SIGKILL risk below, is the coordination session's decision.

3. Watch the stop, and wait it out. The cause of a long stop, at thread level: the daemon's own
   shutdown steps (worker joins, admission drain and `store.flush`) end at the journal line
   `sparrowd: closed store=` 0.4 to 1.2 s after SIGINT in every run of the series (`210`, `212`,
   `213`, `220`); the process then lives on for 12 to 233 s in store teardown. In the 50.5 s run
   the three `rocksdb:low` (compaction) threads ran, used about 30 CPU-seconds, and the cgroup
   wrote 343 MB in 44 s, the last thread leaving at +44 s (`211-D-diag-derived.log`). In the
   13.1 s run nothing was computing: threads were in IO wait at `IOWeight=20` (`213-D-diag-quiesce.log`).
   The exact RocksDB call is inferred: gdb is refused without root (`ptrace: Operation not
   permitted` under `ptrace_scope=1`, `210-D-diag-new.log`), and so is `eu-stack`
   (`124-C-bigNew-stop-timed.log`); no stack was taken. The 234 s stop of an earlier run did not reproduce and is unexplained.

   While the script waits in `stop sparrow.service`, read these from a second shell, none of which
   needs root. A long stop is waited out, never ended by hand.
   - `journalctl --user -u sparrow.service -g 'closed store='` shows the line within about a second
     of SIGINT. From then on the process is in teardown.
   - `systemctl --user status sparrow.service` shows `deactivating (stop-sigterm)` while the stop
     runs.
   - The cgroup's written bytes advance while compaction runs:

     ```bash
     CG=/sys/fs/cgroup$(systemctl --user show sparrow.service -p ControlGroup --value)
     grep wbytes "$CG/io.stat"
     ```

   - The compaction threads' CPU time advances while they compute:
     `ps -L -o tid,comm,stat,time -p "$(systemctl --user show sparrow.service -p MainPID --value)"`
     lists the `rocksdb:low` threads, `R` when running and `D` in IO wait.

   Judge a stop stuck only if neither the cgroup's written bytes nor the `rocksdb:low` threads'
   CPU time advance for 10 consecutive minutes. That bound is a proposal. In the logs the longest
   interval without an advance of the cgroup's written bytes is one sample interval of 10.7 s
   (`220-E-diag-old.log`, +33.3 s to +44.0 s), so 10 minutes is 56 times it (arithmetic: 600
   over 10.7). A stuck stop is still not ended by hand before the unit's timeout. Report it to
   the coordination session and wait. At `TimeoutStopSec` systemd sends SIGKILL, and the way back
   stands, because the script takes the kept copy only after the stop.

   What a kill leaves. The live store is left as a SIGKILL leaves it. Measured with the image
   built from `b420503`, on a store of 1.57 GB
   at `/387` after three acknowledged ingests (15,000 records): the next open took 2,058 ms in the
   storage engine, against 23 to 65 ms for the new image's other opens of that store under the limits (clean
   opens 23, 27, 36 and 37 ms in logs `120`, `125`, `128` and `131`, and 65 ms in `150` before the
   kill; the old image took 199 ms in `140` and the new image 229 ms without limits in `160`), and read 165,000 live
   records, the 150,000 of the base and all 15,000 acknowledged (`154-C-kill-daemon2.log`,
   `155-C-kill-post-status.log`). This is not measured at live size. Not measured: a SIGKILL during
   an in-flight ingest, a SIGKILL of the old image, and a kept copy taken from a store a SIGKILL
   left. The script's own checks decide whether it continues: it requires the unit to have settled and
   no `sparrowd` process to remain, and `keep_store_copy` refuses an open store (read from
   `deploy/user-unit-upgrade.sh` and `deploy/lib.sh`, not run). The script's log stamps the lines
   `stop sparrow.service` and `daemon stopped`; a gap equal to `TimeoutStopSec` means SIGKILL ended
   the stop.

The stop of the new image after #394. The fix cancels background work after the shutdown flush
(`Store::cancel_background_work()` called after `store.flush()` succeeds). It is on `main` as
`6b4d4b4` (pull request 406) and the image `a50820e` holds it. Measured on a scratch store at
`/387` with a base of 280,000 records (3.06 GB status `physical_bytes`), the live unit's limits and
a SIGINT with a 600 s deadline, after six ingests of 5,000 records (30,000 records), with the same
source built with and without the call, in the order with, without, without, with, with, without
(`.stage/394/41-with-1.log` to `46-without-3.log`, `51-summary.log`):

- With the call: 0.45 s, 0.97 s and 0.75 s (`41`, `44`, `45`).
- Without it: 16.3 s, 33.3 s and 21.5 s (`42`, `43`, `46`). The RocksDB log shows a compaction
  finishing 16.7 s, 33.0 s and 21.9 s after the SIGINT without the call, and 0.4 to 0.7 s after it
  with the call, with jobs aborted by `Shutdown in progress` (`50-rocksdb-log-evidence.log`).
- All six runs reopened with 310,000 live records, the 280,000 of the base plus the 30,000
  acknowledged, and every acknowledged artifact was found once.

Limits. The runs are on a store of 3.7 to 4.2 GB before the stop (status `physical_bytes`), not at the live size of about 39 GB. The stop under
the unit's 120 s `TimeoutStopSec` is not measured with the call, nor is the 234.4 s stop. The
mechanism by which the call shortens the stop is not established. The first stop of Switch 1 is the
stop of `sparrowd-schema71-fda2812`, which lacks the call, so it has the times above and the 1800 s
drop-in stands for it. Every later stop of the new image, including the way back's stop after an
open, has the call.

#### The window

Do not start the window's stop between minute 55 and minute 10 of any hour. In that part of the hour
the live daemon has expired the 45 s consume publication deadline (`PUBLICATION_DEADLINE` in
`crates/sparrow-server/src/watches.rs`) for the `katamari-stored` watch, twice in the readings of
issue 421; the cause is not known. The stop of the old image is the step whose length matters most
(the stop timeout above).

```bash
cd MAIN_CHECKOUT       # main at or after the merge of this procedure; the script finds lib.sh beside itself
SPARROW_BIN="$BIN" deploy/user-unit-upgrade.sh --from "$OLD" --to "$NEW" --keep-copy "$COPY" \
    --hold-after-copy > "$RECORD/switch.log" 2>&1; echo "EXIT=$?" >> "$RECORD/switch.log"
```

Read `$RECORD/switch.log`. The script refuses, before anything changes, unless the daemon link names
`--from`, the four images are executable, the new pair is byte-identical to `SPARROW_BIN`, the new
daemon has `cap_net_bind_service`, `server.toml` carries the ingest keys and no removed backup
keys, the store is no-COW, the drop-in directory holds only the repository's `limits.conf` (the
live host already has an identical one), the running daemon reports the `--from` schema and the
copy's name and room are sound. Then it stops `sparrow-ship.timer` and waits for the shipper,
stops the unit through the SIGINT drain (up to `TimeoutStopSec`), **copies the closed store to `COPY`**, holds
for the rehearsal's answer ("The rehearsal on a copy"), links the new pair, starts the unit and waits up to 600 s for the first status answer. It checks the
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
nor `sparrow-recovery-schema71-a50820e` may open `COPY` or `COPY.probe`: either lifts the format and
spoils the copy as a way back.

#### The rehearsal on a copy

The rehearsal opens a second copy of the closed store with the new image and proceeds to the live
open only if the copy passes. It runs after `COPY` exists and before the new image opens the live
store. The first copy, `COPY`, stays untouched as the way back; the rehearsal copy is taken from it.
`--hold-after-copy` (see the paragraph on it under "Switching the user unit's image") holds the switch at
that point: the daemon is down, the links name the old pair and the store is untouched. The hold has
no timeout, and the daemon is down for all of it; the script repeats the seconds down at each
prompt. It prints `HOLD: sparrow.service has been down N s. The closed store is kept at COPY.` and
the prompt `proceed or abort> ` on the terminal (`/dev/tty`, even though the window command writes
its output to a log). Run the rehearsal steps below in a second shell, in which the variables block above is set
too, and answer in the first. A
shell that cannot hold the terminal gives `--hold-answer FIFO` an existing named pipe and answers
with `echo proceed > FIFO`.

Write the payload check of step 4 once, in the second shell, before the rehearsal starts. It is a
read-only client of the rehearsal daemon's socket:

```bash
cat > "$RECORD/payload-check.py" <<'CHECK'
import datetime, json, os, pathlib, subprocess, sys, time

# check:  CLI SOCKET RECORD_DIR CEILING_BYTES LIVE_LISTING
# list:   --list CLI SOCKET OUTPUT_FILE     writes the listing of a daemon as id, source and receipt lines, and its counts
#                                           to OUTPUT_FILE.counts, with the ingest figures before and after the walk
LIST = sys.argv[1] == "--list"
args = sys.argv[2:] if LIST else sys.argv[1:]
cli, sock = args[0], args[1]
out_path = pathlib.Path(args[2])
record = out_path
CEILING = int(args[3]) if len(args) > 3 else 4 << 30   # most bytes the check reads
LIVE = pathlib.Path(args[4]) if len(args) > 4 else None  # the live daemon's listing, from --list
if not LIST and LIVE is None:
    sys.exit("usage: payload-check.py CLI SOCKET RECORD_DIR CEILING_BYTES LIVE_LISTING (the live daemon's --list output)")
TARGET, LARGEST, CHUNK, REPORT_EVERY = 50, 5, 1 << 20, 1000
AGE_DAYS = [0, 1, 3, 7, 14, 21]   # window edges, days before now; the last window is open-ended


def fail(why):
    sys.exit("payload check FAILED: " + why)


def run(argv):
    done = subprocess.run(argv, capture_output=True, text=True)
    if done.returncode != 0:
        fail(f"{' '.join(argv[:4])} exited {done.returncode}: {done.stdout.strip()} {done.stderr.strip()}")
    return done.stdout


def call(*args):
    return json.loads(run([cli, sock, *args]))


def pages(limit, *args):
    cursor = []
    for _ in range(limit):
        page = call(*args, *cursor)
        yield page
        if page.get("next_cursor") is None:
            return
        cursor = ["--cursor", page["next_cursor"]]
    fail(f"{' '.join(args[:2])}: still more pages after {limit} calls")


def length_of(row):
    origin = row["origin"]
    if origin is None or origin.get("byte_length") is None:
        fail(f"{row['artifact_id']}: the catalog row has no byte_length")
    return int(origin["byte_length"])


def walk_listing(status, store):
    counters = [m for m in status["migrations"] if m["name"] == "store-counters"]
    if len(counters) != 1 or counters[0]["watermark"] is not None or counters[0]["done"] != counters[0]["total"]:
        fail(f"status has no complete store-counters migration row: {counters}")
    sources_done = [m for m in status["migrations"] if m["name"] == "source-summary"]
    if len(sources_done) != 1 or sources_done[0]["watermark"] is not None or sources_done[0]["done"] != sources_done[0]["total"]:
        fail(f"status has no complete source-summary migration row: {sources_done}")
    call_limit = int(counters[0]["total"]) + 1
    stall_limit = int(status["max_records_per_artifact"]) + 1
    walk_calls, listed, last, stalled, started = 0, {}, None, 0, time.monotonic()
    cursor = []
    while True:
        if walk_calls >= call_limit:
            fail(f"the listing walk made {walk_calls} calls, the bound of {call_limit} catalog entries, and is not finished")
        page = call("artifacts", store, "--page-size", "1000", *cursor)
        walk_calls += 1
        last = page
        for r in page["rows"]:
            if r["artifact_id"] in listed:
                fail(f"the listing returned {r['artifact_id']} twice")
            listed[r["artifact_id"]] = r
        stalled = 0 if page["rows"] else stalled + 1
        if stalled > stall_limit:
            fail(f"the listing walk made {stalled} consecutive calls without a new row after {len(listed)} artifacts; "
                 f"one artifact needs at most {stall_limit} (max_records_per_artifact plus 1)")
        if walk_calls % REPORT_EVERY == 0:
            print(f"listing walk: {walk_calls} calls, {len(listed)} artifacts, {time.monotonic() - started:.0f} s", flush=True)
        if page.get("next_cursor") is None:
            break
        cursor = ["--cursor", page["next_cursor"]]
    walk_seconds = time.monotonic() - started
    return listed, walk_calls, last, walk_seconds


def migration(status, name):
    rows = [m for m in status["migrations"] if m["name"] == name]
    if len(rows) != 1:
        fail(f"status has no single {name} migration row")
    return rows[0]


def artifacts_total(status):
    # The artifacts counter: the payload-index migration total where there is one, otherwise the source-summary
    # total (artifacts plus records) minus the source-context total (records): an inference, not a reading.
    if any(m["name"] == "payload-index" for m in status["migrations"]):
        return int(migration(status, "payload-index")["total"])
    return int(migration(status, "source-summary")["total"]) - int(migration(status, "source-context")["total"])


def ingest(status):
    return {"flush_lag_ms": status["durability"]["flush_lag_ms"], "pending_writes": status["durability"]["pending_writes"]}


clock_before = datetime.datetime.now(datetime.timezone.utc)   # the clock on both sides of the status read that
status = call("status")                                         # gives the live total
clock_after = datetime.datetime.now(datetime.timezone.utc)
store = status["store_id"]
if LIST:
    rows_listed, calls, _, seconds = walk_listing(status, store)
    after = call("status")
    out_path.write_text("".join(f"{r['artifact_id']}\t{r['source']}\t{r['origin']['received_at']}\n"
                                for r in rows_listed.values() if r["origin"] is not None))
    pathlib.Path(str(out_path) + ".counts").write_text(json.dumps({
        "store_id": store, "clock_before_status": clock_before.isoformat(), "clock_after_status": clock_after.isoformat(),
        "listed": len(rows_listed), "calls": calls, "walk_seconds": round(seconds, 1),
        "artifacts_total": artifacts_total(status),
        "ingest_before": ingest(status), "ingest_after": ingest(after)}) + "\n")
    print(f"listing of {len(rows_listed)} artifacts in {calls} calls and {seconds:.0f} s written to {out_path}; "
          f"artifacts counted {artifacts_total(status)}; "
          f"ingest before {ingest(status)}, after {ingest(after)}")
    sys.exit(0)
rows = [m for m in status["migrations"] if m["name"] == "payload-index"]
if len(rows) != 1:
    fail("status has no single payload-index migration row")
row = rows[0]
if row["failure"] is not None or row["watermark"] is not None or row["done"] != row["total"]:
    fail(f"payload-index migration is not complete: {row}")
total = int(row["total"])

sources = [r for p in pages(100000, "sources", store) for r in p["rows"]]
held = {r["source_id"]: int(r["complete_artifacts"]) + int(r["fragment_parents"]) for r in sources}
counted = sum(held.values())
if total != counted:
    fail(f"the artifacts counter {total} differs from the sources sum {counted}: an artifact was evicted or lost, "
         "which nothing does on a store without an archive")

# One full walk of the artifact listing: no call sorts by size or filters by it. A page holds at most 31
# rows and fewer when its charged catalog bytes run out first, so a page can hold none. A page that ends
# having consumed nothing happens only for a resumed artifact whose next record cannot fit a fresh page
# (a record entry over 512 KiB, or pins that use up the page's admission) and at the edge of the
# source-summary window (only while that migration runs), so the check requires that migration complete.
# Otherwise every page consumes at least one catalog entry. The listing omits a counted artifact that has
# no live bytes and no live record, and the omitted ones of a source sit together at its head, so one run of
# pages without a new row can cross all of them: the code gives no bound on a run shorter than the walk. The
# stall limit is a size-based heuristic that fails a stalled walk early: one artifact's records plus 1. The
# store-counters total counts every catalog entry and is the backstop on the calls that the code guarantees.
# Cursors carry a new snapshot id on every page, so a repeated cursor is not a stall signal and is not checked.
listed, walk_calls, last, walk_seconds = walk_listing(status, store)
if last is None or last["answer_certainty"] != "exact" or int(last["matching_artifacts"]) != len(listed):
    fail(f"the listing walk is not complete and exact: {len(listed)} rows after {walk_calls} calls, "
         f"last page answers {last and last['answer_certainty']}, {last and last['matching_artifacts']} matching")
per_source = {}
for r in listed.values():
    per_source[r["source"]] = per_source.get(r["source"], 0) + 1
# The listing walks the per-source index, which holds exactly the artifacts that are complete or have a
# retained record, the same artifacts `sources` counts. The listing drops those with no live bytes and no
# live record (expired and not retired), so it is never larger than the counts and may be smaller.
for source, count in per_source.items():
    if count > held.get(source, 0):
        fail(f"source {source}: the listing returned {count} artifacts, sources reports {held.get(source, 0)}")
if len(listed) > counted:
    fail(f"the listing holds {len(listed)} artifacts, sources reports {counted}")
omitted = counted - len(listed)
unlisted_sources = len([s for s, c in held.items() if c and not per_source.get(s)])
# The gap per source, for the operator to read. An omitted artifact has no live bytes and no live record, so
# its receipt is before the cutoff, now minus retention_seconds (nothing retires without an archive). The
# oldest listed receipt of a source is printed beside its gap; the omitted ones lie before it in that source.
now = datetime.datetime.now(datetime.timezone.utc)
cutoff = now - datetime.timedelta(seconds=int(status["retention_seconds"]))
oldest, newest, live_records = {}, {}, {}
for r in listed.values():
    at = r["origin"]["received_at"] if r["origin"] is not None else None
    if at is not None:
        oldest[r["source"]] = min(oldest.get(r["source"], at), at)
        newest[r["source"]] = max(newest.get(r["source"], at), at)
    live_records[r["source"]] = live_records.get(r["source"], 0) + int(r["retained_records"])
gap_lines = [f"cutoff {cutoff.strftime('%Y-%m-%dT%H:%M:%SZ')} (now minus retention_seconds {status['retention_seconds']})",
             "source\tcounted\tlisted\tomitted\toldest listed\tnewest listed\trecords counted\trecords live in listed"]
for r in sorted(sources, key=lambda r: r["source_id"]):
    name = r["source_id"]
    gap_lines.append(f"{name}\t{held[name]}\t{per_source.get(name, 0)}\t{held[name] - per_source.get(name, 0)}\t"
                     f"{oldest.get(name, '-')}\t{newest.get(name, '-')}\t{r['retained_records']}\t{live_records.get(name, 0)}")
gap_lines.append(f"total\t{counted}\t{len(listed)}\t{omitted}\t\t\t\t")
(record / "listing-gap.tsv").write_text("\n".join(gap_lines) + "\n")
(record / "listing-ids.txt").write_text("".join(i + "\n" for i in sorted(listed)))
print("\n".join(gap_lines), flush=True)

# The live daemon's listing, taken before the stop, against this copy's. Every artifact that listing returned
# and that is still within retention must be here; the copy may hold more, written between the listing and the
# stop. Past-deadline artifacts are in no listing, so a loss among them shows only in the counts: no call
# enumerates them (every listing, record and text read filters on deadline; `payload` returns them only for a
# known digest) or counts them at this size (retention-metrics admits at most 100,000 catalog visits).
live_rows = {}
for line in LIVE.read_text().splitlines():
    i, src, at = line.split("\t")
    live_rows[i] = (src, datetime.datetime.fromisoformat(at))
live_counts = json.loads(pathlib.Path(str(LIVE) + ".counts").read_text())
if live_counts["store_id"] != store:
    fail(f"the live listing is of {live_counts['store_id']}, this copy is {store}")
missing = sorted(i for i, (src, at) in live_rows.items() if i not in listed and at >= cutoff)
expired_since = len([i for i, (src, at) in live_rows.items() if i not in listed and at < cutoff])
extra = sorted(i for i in listed if i not in live_rows)
unlisted_live = live_counts["artifacts_total"] - live_counts["listed"]
unlisted_copy = total - len(listed)
print(f"live listing {len(live_rows)} ids, copy listing {len(listed)} ids; absent from the copy {len(missing)} within retention "
      f"and {expired_since} past the cutoff; in the copy only {len(extra)} (written after the live listing, expected); "
      f"artifacts counted live {live_counts['artifacts_total']} and copy {total}; not listed live {unlisted_live}, "
      f"copy {unlisted_copy}, difference {unlisted_copy - unlisted_live}", flush=True)
if missing:
    fail(f"{len(missing)} artifacts of the live listing are absent from the copy's listing: {', '.join(missing[:20])}"
         + (" ..." if len(missing) > 20 else ""))
# The live total was read before the walk and ingest went on during it: an artifact received after that read
# and listed by the walk is in the live listing but not in the live total, so it is added to what the copy may
# hold more. Ids stamped between the clock reads on either side of the status read may or may not be in the live
# total; they are not added (so no false failure), and their count is printed as the margin. The server stamps an
# artifact's receipt before it takes the writer lock, so an artifact stamped before the first clock read and saved
# after the status snapshot is in neither the total, nor `during`, nor the margin: each artifact in flight across
# the status read can hide one lost artifact unreported. That is the stated limit; no lease allowance is added.
after_read = datetime.datetime.fromisoformat(live_counts["clock_after_status"])
before_read = datetime.datetime.fromisoformat(live_counts["clock_before_status"])
during = len([i for i, (src, at) in live_rows.items() if at >= after_read])
margin = len([i for i, (src, at) in live_rows.items() if before_read <= at < after_read])
print(f"received during the live walk {during}; stamped within the live status read {margin} (normally 0)", flush=True)
if total - live_counts["artifacts_total"] < len(extra) + during:
    fail(f"the copy counts {total} artifacts, the live daemon counted {live_counts['artifacts_total']} before its walk, and the "
         f"copy accounts for {len(extra)} ids the live listing lacks and {during} received during the walk: "
         "counted artifacts are missing")

edges = [now - datetime.timedelta(days=d) for d in AGE_DAYS]


def window_of(row):
    at = datetime.datetime.fromisoformat(row["origin"]["received_at"])
    return next((i for i, edge in enumerate(edges[1:]) if at >= edge), len(edges) - 1)


eligible = [r for r in listed.values()
            if r["origin"] is not None and r["complete_bytes_available"] and length_of(r) > 0]
cells = {}
for r in eligible:
    cells.setdefault((r["source"], window_of(r)), []).append(r)
names = sorted({r["source"] for r in eligible})


def select():
    chosen = sorted(eligible, key=lambda r: (-length_of(r), r["artifact_id"]))[:LARGEST]
    seen = {r["artifact_id"] for r in chosen}
    spent = sum(length_of(r) for r in chosen)
    if chosen and length_of(chosen[0]) > CEILING:
        fail(f"the largest artifact, {chosen[0]['artifact_id']} of {length_of(chosen[0])} bytes, exceeds the "
             f"ceiling of {CEILING} bytes; raise the ceiling (fourth argument)")
    if spent > CEILING:
        fail(f"the {LARGEST} largest artifacts total {spent} bytes, over the ceiling of {CEILING}; raise the ceiling")
    while True:
        before = len(chosen)
        for lap in range(len(edges)):
            for j, source in enumerate(names):
                pool = cells.get((source, (lap + j) % len(edges)), [])
                fits = next((r for r in pool if r["artifact_id"] not in seen and spent + length_of(r) <= CEILING), None)
                if fits:
                    seen.add(fits["artifact_id"])
                    chosen.append(fits)
                    spent += length_of(fits)
                if len(chosen) >= TARGET:
                    return chosen
        if len(chosen) == before:
            fail(f"only {len(chosen)} artifacts found within the ceiling of {CEILING} bytes, need {TARGET}; "
                 "raise the ceiling or find why the copy has so few")


def read_whole(artifact, length, path):
    # successive reads of at most CHUNK bytes, appended to one file that is hashed as one stream
    piece = path.with_suffix(".chunk")
    offset = 0
    with open(path, "xb") as whole:
        while offset < length:
            want = min(CHUNK, length - offset)
            call("bytes", store, artifact, str(offset), str(want), str(piece))
            data = piece.read_bytes()
            os.unlink(piece)
            if len(data) != want:
                fail(f"{artifact}: read at offset {offset} returned {len(data)} bytes, asked for {want}")
            whole.write(data)
            offset += want
    if path.stat().st_size != length:
        fail(f"{artifact}: chunks total {path.stat().st_size} bytes, the catalog says {length}")


chosen = select()
out = record / "payload-sample-bytes"
out.mkdir()
report, read = [], 0
for n, hit in enumerate(chosen):
    artifact, length = hit["artifact_id"], length_of(hit)
    path = out / f"{n:04d}.bin"
    read_whole(artifact, length, path)
    read += length
    payload_id = "blake3:" + run(["b3sum", "--no-names", str(path)]).strip()
    answers = list(pages(100000, "payload", store, payload_id, "--page-size", "1000"))
    if not all(isinstance(p["artifact_ids"], list) for p in answers):
        fail(f"{artifact}: payload answered artifact_ids that is not a list")
    exact = all(p["answer_certainty"] == "exact" for p in answers)
    listed_here = any(artifact in p["artifact_ids"] for p in answers)
    if not exact or not listed_here:
        fail(f"{artifact}: payload {payload_id} exact={exact}, lists the artifact={listed_here}")
    report.append(f"{artifact}\t{hit['source']}\t{hit['origin']['received_at']}\t{length}\t{payload_id}")
(record / "payload-sample.tsv").write_text("\n".join(report) + "\n")
sizes = [length_of(h) for h in chosen]
print(f"payload check passed: {len(report)} artifacts; sources holding artifacts {len([s for s, c in held.items() if c])}, "
      f"with readable artifacts {len(names)}, sampled {len({h['source'] for h in chosen})}; "
      f"age windows covered {len({window_of(h) for h in chosen})} of {len(edges)}; "
      f"sizes {min(sizes)} to {max(sizes)} bytes, {read} bytes read of the {CEILING} ceiling; "
      f"listing walk of {walk_calls} calls and {walk_seconds:.0f} s, {len(listed)} artifacts; "
      f"payload-index done=total={total}; live ids absent 0, copy-only ids {len(extra)}; artifacts counted by sources {counted}, listed {len(listed)}, "
      f"not listed {omitted} (expired, or lost; the check cannot tell, the live listing comparison decides; {unlisted_sources} "
      f"sources list none); artifacts counter {total}, gap to sources {total - counted}")
CHECK
```


1. Copy. Take a reflink copy of `COPY` with the repository's own script, which refuses an open
   store, compares the file lists and gives `REH` its name only when they agree:

   ```bash
   deploy/copy-on-write.sh copy "$COPY" "$REH"
   ```

2. Configure. The copy's stored watch catalog names the live consume roots, which delete each file
   after publication, so the rehearsal daemon runs with `--no-intake`: it opens the store writable
   with its maintenance worker, starts no consume worker, no ingest socket and no public listener,
   and opens no root. The configuration may not set `consume`, `syslog`, `rpc_tcp_address`,
   `ingest_socket_path` or `archive`; the daemon refuses the open and names the key. Keep the keys
   that shape the open and name the copy:

   ```bash
   { grep -E '^(disk_budget|maintenance_reserve|min_free_bytes|max_records|max_pin_members|max_concurrent_requests|max_rpc_connections|buffered_flush_interval_ms|text_lexicon_dir) *=' "$CONF"
     printf 'socket_path = "%s/rpc.sock"\ndata_dir = "%s"\n' "$REH" "$REH"; } > "$REH_CONF"
   ```

   `text_lexicon_dir` must equal the live one, so the analyzer is the live analyzer. The
   mode was measured on a scratch store ("What the measurement covers", the `--no-intake`
   paragraph). It then ran on the copy of the live store (`intake=false` and the payload-index
   migration in `.stage/image/switch-1/rehearsal-daemon.log`).

3. Open. Start the new image on the copy in a scratch scope with the live unit's limits, never the
   live unit and never the live socket. The descriptor limit is raised in the scope's shell, as the
   unit's `limits.conf` does for the live daemon (the store refuses to open below 2048):

   ```bash
   date +%s.%N > "$RECORD/rehearsal-t0.txt"
   systemd-run --user --scope --quiet --unit=sparrow-rehearsal \
       -p MemoryHigh=2G -p MemoryMax=4G -p CPUQuota=100% -p IOWeight=20 -- \
       bash -c 'ulimit -n 65536; exec nice -n 10 "$0" --config "$1" --no-intake' \
       "$LIB/$NEW" "$REH_CONF" > "$RECORD/rehearsal-daemon.log" 2>&1 &
   ```

   Run it in the background and read `$RECORD/rehearsal-daemon.log`, which gains the `store_open_ms`
   line (with `migration_fence=`), the `migration payload-index start` line and the
   `migration payload-index complete pages=P done=N elapsed_seconds=S` line. `N` is the live
   artifact count and `P` its page count, `ceil(N / 1024)` or more when the entries a page reads reach
   64 MiB. The lines carry no timestamps. Take the time before the start, in the same shell, so the
   wall time is known: `date +%s.%N > "$RECORD/rehearsal-t0.txt"` before the `systemd-run`, and
   `date +%s.%N` before and after each `status` read in step 4. The open's own time is `total=` in the
   `store_open_ms` line (milliseconds), and the migration's is `elapsed_seconds`.

4. Check. Proceed to the live open only if all of these hold.
   - The copy lifts to the new format: `"$LIB/$NEW_CLI" "$REH/rpc.sock" status` shows
     `store_format` `sparrow.store/395` and the recorded `store_id`. (A `/340` store lifts to `/395`,
     read from the code; the dry run's `/332` scratch store lifted to `/387`.)
   - The payload-index migration completes: the log holds the `start` line and a `complete` line
     with `pages=P`, `done=N` and `N` equal to `total`, and `status` shows `migrations[]`
     `payload-index` with `done` equal to `total` and `failure` null. A migration of one page, or of
     pages quicker than a `status` call, is first seen complete (the dry run's 308 artifacts went in
     one page in 1.636 s, and the first `status` read already showed it done), so the `complete`
     line, not a rising `done`, is the check. The lookup check below needs it, so completion gates
     `proceed`.
   - A sample of payload lookups is exact, and the counts agree. After the completion line, the
     check below chooses at least 50 artifacts from the rehearsal copy: the 5 largest in the copy,
     and a spread over its sources and ages. No call lists artifacts by size or filters by it. The
     check therefore walks the whole `artifacts STORE_ID` listing once, without a source or an
     interval; the listing orders rows by source, then receipt time, and gives each row's source,
     `received_at` and `byte_length`. A page holds at most 31 rows (`PAGE_ITEMS` in
     `crates/sparrow-core/src/storage/artifact_read.rs`) and often fewer, because the server cuts a
     page by the catalog bytes it charges, not by rows. Reading an artifact folds each of its
     records (`fold_index_record`), charges 64 times each record entry's stored bytes against a
     32 MiB admission (`charge_catalog` in `storage.rs`, `SCAN_BYTES` in `catalog_lease.rs`), and a
     page ends when that is spent, in the middle of an artifact when need be (`artifact_index_page`).
     The CLI's `--scan-budget` (at most 100,000 entries, default 10,000) does not lower the call
     count: 32 MiB divided by 64 is 524,288 bytes of record entries, so a record entry of 53 bytes or
     more exhausts the bytes before 10,000 entries, and the check leaves the default. A page that
     ends before the last normally consumes at least one catalog entry, an artifact or one of its
     records, or the call fails with `RESOURCE_LIMIT`. Two pages can end having consumed nothing: a
     resumed artifact whose next record cannot fit a fresh page (`artifact_read.rs`, the
     `RESOURCE_LIMIT` arm of `artifact_index_page`), which needs a record entry over `ENTRY_BYTES`
     (512 KiB) or pins that use up the page's admission, and the edge of the source-summary window
     (`Listed::Window`), which exists only while the `source-summary` migration runs. The check
     requires that migration complete, and the live store at its last reading has it complete and no
     active pins (`.stage/image/switch-1/before.json`); whether a record entry over 512 KiB exists on
     the live store is not verified, and the stall check below catches it. Under those conditions every
     page consumes at least one catalog entry. The listing omits a counted artifact that has no live
     bytes and no live record, and the omitted artifacts of a source sit together at its head (rows
     are ordered by source, then receipt time), so one run of pages without a new row can cross all
     of them, not one artifact. The code gives no bound on such a run shorter than the walk. The stall
     limit is therefore a size-based heuristic that fails a stalled walk early, not a code guarantee:
     it fails a walk with more than `max_records_per_artifact` (100,000 in that `before.json`)
     plus 1 consecutive pages without a new row. The limit is sized on the dry run's record density,
     where the whole walk is about 5,055 pages (arithmetic below), and is not a code guarantee. Every
     page carries a new random snapshot id in its
     cursor, so a repeated cursor does not signal a stall and is not checked. At 27.9 calls per
     second that bound is 100,001 calls, about 3,584 s (arithmetic). The walk also ends within the
     `store-counters` migration `total` plus 1 calls, which counts every catalog entry; the check
     takes that as its last backstop on the call count: the live `total` of 17,764,214
     (`.stage/image/switch-1/before.json`, not the window's own `before.json`) is 17,764,215 calls,
     about 636,710 s or 7.4 days at 27.9 calls per second (arithmetic). The check also fails when the
     listing returns an artifact twice, when the walk ends without an exact final
     page, or when its rows differ from the listing's own `matching_artifacts` count. The call limit is a bound, not an expectation. The dry run's scratch store,
     308 artifacts of about 1,330 records (409,300 records, arithmetic), took 694 calls: 424 pages
     held no row, 232 one and 38 two, at 27.9 calls per second (`.stage/372-dryrun/logs/12-walk-rate.log`),
     about 590 records per call (arithmetic). Scaling the calls by records to the live
     2,981,454 `live_records` (`.stage/image/switch-1/before.json`) gives about 5,055 calls and, at
     27.9 calls per second, about 181 s (arithmetic; the rate came from one scratch store, the host
     load was not logged, and live record entries may differ in size, which
     moves the call count). The call rate varies: on one lifted copy of 291 artifacts, a walk of 249
     calls took 51 s, 4.9 calls per second, and a later walk of the same copy took 5 s, about 50 calls
     per second (arithmetic on the calls and seconds in `.stage/372-dryrun2/logs/10-checker.log` and
     `11-guards.log`), a spread of about tenfold whose cause is not isolated. A live duration planned
     from one measurement can be off by that factor. The check prints a progress line every 1,000 calls and the walk's calls and seconds
     at the end. The `sources` rows and the per-source index count an artifact until it is retired,
     whatever its deadline (`source_summary.rs`, module header), and the listing omits an artifact with
     no live bytes and no live record (`finish_row` in `artifact_read.rs`). Nothing retires on the live
     daemon, which has no archive (`archived` in `archive.rs`, `retire_without_archive` false in
     `config.rs`), so artifacts past their 30-day deadline stay counted and are not listed; no
     field of `before.json` counts them, so their number on the copy is not known until the check prints it. The
     check therefore does not require equality. It fails when a source's listed rows exceed that
     source's `complete_artifacts` plus `fragment_parents`, or the listed total exceeds the sum of
     those counts, which the code excludes, and when the `artifacts` counter differs from the `sources`
     sum, which nothing changes on a store without an archive (eviction and retirement are gated by
     `archived` in `archive.rs`). It prints, and writes to `$RECORD/listing-gap.tsv`, for each
     source and in total the counted artifacts, the listed ones, the not listed ones, the oldest and
     newest listed receipt, the counted records and the live records of the listed artifacts, with the
     cutoff (now minus `retention_seconds`); it writes the listed ids to `$RECORD/listing-ids.txt`. A
     smaller listing means artifacts that are expired, or lost: the check cannot tell which from the
     listing alone, and without the live listing comparison below a loss smaller than the gap passes.

     The live listing comparison is a condition of `proceed`. The check takes the live daemon's listing
     (`--list`, taken before the stop, "Before the window") as its last argument and requires:
     every id of it received at or after the cutoff is in this copy's listing, or the check fails and
     names the ids (the first 20); the `artifacts` counter of the copy is not below the live one plus
     the number of ids in the copy only and the number of live ids received at or after the later
     clock reading of the live status read (`during`). Ids in the copy only were written between the live listing
     and the stop: they are printed as a count and do not fail. Ids of the live listing received before
     the cutoff and absent here are printed as a count (they can have expired since). A source with
     no listed row is covered the same way: every id the live listing holds for it, received at or
     after the cutoff, must be here; a source that lists nothing on both sides holds only artifacts
     past their deadline. The check prints the not listed counts of the live daemon and the copy and
     their difference.

     Residual. Past-deadline artifacts are in no listing: every call that lists or reads artifacts or
     records filters on the deadline (`live_artifact` and `live_record`: `artifact_read.rs`,
     `query_membership.rs`, `context.rs`, `text_index.rs`, `rdf_view.rs`), so no call enumerates
     them. `payload STORE_ID blake3:HASH` returns them with no deadline filter (`payload_index.rs`, the
     module contract and `payload_lookup`), but only for a known digest, and a read by id
     (`Store::artifact`) needs an id. No call counts them at live size: `retention-metrics` admits at
     most 100,000 catalog visits (`RetentionReadLimits::validate`,
     `sparrow-client/src/retention_metrics.rs`), and the live store holds about 3 million records
     (`live_records` in `before.json`). A loss among past-deadline artifacts is therefore caught
     only by the count relation above. The live total is read once, before the walk, and ingest goes on
     during it. The check reads the clock on both sides of that status read. Live ids stamped at or
     after the later reading were accepted after the total was read, so the check adds them (`during`)
     to what the copy may hold more, as are the ids in the copy only. Ids stamped between the two
     readings may or may not be in the live total, so the check does not add them (which keeps it
     from failing a store with no loss) and prints their count as the margin, normally 0; each id in
     the margin is one artifact of loss that can pass. The server stamps an artifact's receipt before
     it takes the writer lock (`rpc.rs`, `now()` before the operation; `storage.rs`, `self.lock()` in
     ingest), so an artifact stamped before the first clock reading and saved after the status
     snapshot is in neither the live total, nor `during`, nor the margin: each artifact in flight
     across the status read can hide one lost artifact unreported. That is the stated limit, and the
     check adds no allowance for it. The expected count is near 0: the artifacts counter is 39,061
     (3,020,515 minus 2,981,454 in `.stage/image/switch-1/before.json`) over the store's age of
     2,821,860 s (from 2026-10-17 22:06, the file time of the earliest status file, to the
     2026-11-19 13:57 reading), about 0.014 artifacts per second (arithmetic, an inference from
     those two readings), so each second between stamp and save expects about 0.014 artifacts; the
     interval itself is not measured. The clock readings and the stamps are one host clock. A copy row
     with no `origin` counts as copy-only, because the listing file holds rows with an origin only; the
     live store has no such row (`fragment_parents` 0 in its `sources` reading). The live
     `artifacts` counter has no `payload-index` row on `fda2812`, so the live listing takes it as the
     `source-summary` total minus the `source-context` total (an inference from their definitions).

     What gap to expect, as an inference. An omitted artifact is past its deadline, so its receipt is
     before the cutoff (30 days before the run; 2026-10-20 for a run on 2026-11-19, arithmetic),
     assuming artifacts were stored with a deadline of receipt plus retention, which is unverified for
     the store's earliest era (`sparrow.store/37`). The live store held records on or before 2026-10-17:
     it reported 217 `live_records` under the same `store_id` in two status files whose modification
     times are 2026-10-17 and 2026-10-18 (the only time evidence they carry;
     `.stage/native-graph-integration/runtime-recovery-final-status.json` and
     `schema52-live-before-status.json`), so artifacts received from then until the cutoff are
     expected in the gap. `.stage/image/switch-1/before.json` cannot size it: status `live_records` is
     the retained-records counter, which counts every record entry not evicted or retired, so it
     includes records past their deadline (`status_report` in `storage.rs`, `store_counters.rs`), and the
     file holds no receipt times and no per-source counts. It bounds the gap above only by the
     `artifacts` counter, 39,061 (3,020,515 minus 2,981,454, arithmetic on its `source-summary` and
     `source-context` totals). Under an assumed uniform ingest over the 33 days from 2026-10-17 to
     2026-11-19, the 3 days before the cutoff would hold about 3,550 artifacts (arithmetic on that
     assumption; the real rate is not known). The `artifacts` and `sources` calls expose no receipt
     time of an unlisted artifact, so the gap is read from the not listed counts, not sized in advance. The 5 largest artifacts by
     `byte_length` come from the whole walk, and the spread is drawn from the same rows by source and
     by age window (the last 1, 1 to 3, 3 to 7, 7 to 14 and 14 to 21 days, and everything older;
     source `j` takes window `(lap + j) mod 6` in each lap, so ages mix across sources). Only rows that report
     `complete_bytes_available` and a `byte_length` above 0 can be sampled: an expired artifact has no
     live bytes, appears as no row and is never sampled, so the oldest window holds only artifacts
     still within retention or pinned, and an empty artifact has no bytes to hash. A row without `byte_length` fails the check.
     For each artifact it fetches the exact bytes from the rehearsal daemon with
     `bytes STORE_ID ARTIFACT_ID OFFSET LENGTH FILE`, in successive reads of at most 1 MiB
     (the call's limit), appended in order to one file under `$RECORD`, and hashes that file with
     `b3sum --no-names` outside the daemon, so an artifact over 1 MiB is hashed as one stream. It
     then asks `payload STORE_ID blake3:HASH`. The hash comes from the bytes read back, not from the
     index. Each lookup must answer `answer_certainty` `exact` on every page, return `artifact_ids`
     as a list and list that artifact. A lookup that lists none, or answers `pending` after the
     completion line, fails the rehearsal.
     The check fails when any read returns fewer bytes than asked (a short read, a truncated last
     chunk, an empty last chunk), when the chunks total other than the catalog's `byte_length`, and,
     through the hash, when a read returns bytes from the wrong offset. The server refuses a range
     that passes the artifact's end with `INVALID_ARGUMENT` ("byte range is outside evidence";
     `in_range` in `crates/sparrow-core/src/storage.rs`) and never truncates it.
     The check also requires the `payload-index` migration `done` to equal its
     `total` with `failure` and `watermark` null.
     The total bytes read are capped by a ceiling, 4 GiB (a choice; the fourth argument, in
     bytes, sets it). If the largest artifact alone exceeds the ceiling, or the
     5 largest together do, the check fails and says to raise it; it never skips them silently. No
     read rate of the live copy has been measured, so no duration is stated for it; at an assumed
     100 MiB/s, which is an assumption and not a measurement, 4 GiB is about 41 s of reading
     (arithmetic) before hashing, the lookups and the listing walk. The dry run's whole check took
     90.5 s on the scratch store, with at least 879 CLI calls and 108,120,791 bytes read in 133 `bytes` calls
     (`.stage/372-dryrun/logs/11-rehearsal-4b-checker.log`, `13-checker-call-count.log`). The check prints the artifact count, how many sources hold
     artifacts, how many hold readable ones and how many were sampled, how many of the 6 age windows
     the sample covers, the smallest and the largest artifact in bytes, the total bytes read and the
     walk's call count. Any failure exits nonzero and names the artifact. `bytes` is a read call that the `--no-intake` daemon serves
     (only `--query-only` refuses writes, and `bytes` writes nothing in the store); this is read
     from `bytes_read` in `crates/sparrow-server/src/rpc.rs` and `Store::bytes` in
     `crates/sparrow-core/src/storage.rs`, and was not run on a no-intake daemon.

     Run it once the completion line has appeared; `EXIT=0` is the pass:

     ```bash
     python3 -I "$RECORD/payload-check.py" "$LIB/$NEW_CLI" "$REH/rpc.sock" "$RECORD" 4294967296 "$RECORD/live-listing.tsv" > "$RECORD/payload-check.log" 2>&1
     echo "EXIT=$?"
     cat "$RECORD/payload-check.log"
     ```

     The script refuses to reuse `$RECORD/payload-sample-bytes`, so a repeat needs a new `RECORD` or
     that directory removed by itself, one exact path. The sample's bytes stay under `$RECORD` with
     `payload-sample.tsv`, which lists artifact, source, receipt time, length and payload id. The 5
     largest artifacts are kept whole, so the files total at most the ceiling (4 GiB by default, or
     the fourth argument); `$RECORD` needs that much space. Each 1 MiB chunk file is removed right
     after it is appended, so the peak extra space is one chunk beyond the sample files; a run that
     fails during a read can leave that one chunk file. `$RECORD` is under the main checkout's `.stage/`
     on the same filesystem `/` as the store, so its space adds to the room rule's two copies.
     Arithmetic on the reading of 2026-11-19: 99,324,395,520 bytes for two copies plus 4,294,967,296
     bytes (4 GiB) is 103,619,362,816 bytes, against 115,870,515,200 bytes free: a margin of
     12,251,152,384 bytes (12.25 GB). Read it again with the room command before the window.
   - A stop of the scratch daemon is clean: SIGINT to the one process in the scope, then the line
     `sparrowd: closed store=` in `$RECORD/rehearsal-daemon.log` (the scratch scope's output goes
     there, not to a unit's journal) and an exit; the dry run exited 0.236 s after the signal.

     ```bash
     kill -INT "$(cat /sys/fs/cgroup"$(systemctl --user show sparrow-rehearsal.scope -p ControlGroup --value)"/cgroup.procs)"
     ```

5. Record the old image's refusal. The old image accepts `--no-intake`, so the log holds the
   fence refusal, not an argument error. At `fda2812`, `arguments()` in
   `crates/sparrow-server/src/main.rs` matches `"--no-intake"` (line 39) and its usage string lists
   it (line 19). `main` then reads the configuration with `ServerConfig::read` (line 132; `config.rs` line 286),
   which parses it with `toml::from_str` (`config.rs` line 298) and refuses a key that `fda2812`
   does not know, because the struct is `deny_unknown_fields` (`config.rs` line 17). It then calls
   `check_no_intake()` (line 145; `config.rs` lines 376-380, which delegates to `refuse_intake`,
   line 402), which refuses a configured `consume`, `syslog`, `rpc_tcp_address`,
   `ingest_socket_path` or `archive`. Both happen before the store opens. `open_store()` runs at line 191 and fails on a lifted store with the
   fence refusal; the consume worker starts only at line 224, in the branch that `!intake` skips
   (line 217). So the old image cannot start a consume worker on the live roots with `$REH_CONF`
   through either path: the open fails first on a lifted copy, and the flag skips the worker on any
   copy. This is read from the source, with `git show fda2812:...`. Observed with this command on a
   `/332`-derived scratch store lifted to `/387` by the `a50820e` image: exit status 1 and the line
   `Error: Error { code: "STORAGE_FAILURE", message: "log time interpretation law and writer fence
   disagree" }` (`.stage/372-dryrun/logs/15-rehearsal-5-old-refusal.log`). A second dry run gave the
   same exit status and text on a `/332` store lifted to `/387`
   (`.stage/372-dryrun2/logs/13-old-refusal.log`). On the copy of the live store lifted from `/340`
   to `/395`, the same text and exit status 1 were observed
   (`.stage/image/switch-1/rehearsal-old-refusal.log`).

   Run the step only when the configuration holds none of those keys, that is, when this command
   prints nothing:

   ```bash
   grep -nE '^(\[\[consume|consume|\[\[syslog|syslog|rpc_tcp_address|ingest_socket_path|\[archive|archive)' "$REH_CONF"
   ```

   If it prints a line, do not run the old image. Read `$RECORD/rehearsal-old-refusal.log` after the
   run: an argument error there (the usage line) means the installed old image does not accept the
   flag, and the step proves nothing about the fence. A refusal changes nothing in the copy.

   ```bash
   systemd-run --user --scope --quiet --unit=sparrow-rehearsal-old -- \
       bash -c 'ulimit -n 65536; exec "$0" --config "$1" --no-intake' "$LIB/$OLD" "$REH_CONF" \
       > "$RECORD/rehearsal-old-refusal.log" 2>&1
   ```

6. Remove the rehearsal copy after listing it, as one command on one exact path:

   ```bash
   ls -ld /SPARROW/store.rehearsal-a50820e
   ```

   ```bash
   rm -r -- /SPARROW/store.rehearsal-a50820e
   ```

7. Answer the prompt: `proceed` only if every check passed, the live listing comparison of step 4 included, which switches the links and starts
   the new image on the live store. Otherwise answer `abort`.

The script's check for a remaining `sparrowd` runs once, after the stop of `sparrow.service` and
before the copy and the hold (`deploy/user-unit-upgrade.sh` lists processes in one place only). No
check runs while the hold waits for an answer, and none runs after it. The rehearsal's scratch
daemon has the image's name, which begins `sparrowd`, so a check would match it; no check does
during the hold. Step 4 stops it, and the steps end it before step 7: the stop is confirmed by the
`closed store=` line and an exit, and the old image of step 5 exits by itself with its refusal. A
scratch daemon left running at `proceed` is not seen by the script, which goes on to switch the
links and start the unit. The way-back chain for an opened store ("The way back", below) runs later and begins with the same
process test, so a leftover `sparrowd` blocks it: confirm that no scratch daemon runs before
answering.

An `abort` starts the old image again on the unchanged store, waits for its first status answer
(up to 600 s), starts `sparrow-ship.timer` again only if it was active when the script began (the live listing
step above restarts it) and the old image answered, writes the outcome
to `COPY.abort.log` beside the kept copy, to the journal and to the terminal, and exits 1. End of
input, a removed answer pipe, Ctrl-C, SIGTERM and SIGHUP abort the same way. The new image never
opens the live store after an abort, so the "Not opened" way back below is not needed; read
`COPY.abort.log` for what the restore did.

During the abort's restore the script ignores Ctrl-C, SIGTERM, SIGHUP and a closed pipe until the
restore ends, so a Ctrl-C shows only `^C` on the terminal and does nothing. The restore ends when
the old image answers status or its start fails. The way out of a restore that blocks is
`systemctl --user stop sparrow.service` from another shell; the script polls the unit every 5 s and
ends the wait as soon as the unit is `inactive` or `failed`, and a stop during the start cancels the
start. The restore then fails within seconds, the script records the failure and exits 1 (read
from the script, not run).

What the rehearsal adds to the downtime, all arithmetic. The copy of `COPY` costs the copy time
below. The migration costs `P` pages times the per-page time, 0.108 s median at 1,024 artifacts of
3 records, 0.159 s at most there, and 0.263 to 0.602 s for the single page of 101 larger artifacts
(figures and logs under "What the measurement covers"); the dry run's single page of 308 artifacts of
about 1,330 records took 1.636 s, so the per-page time probably follows the size of the entries (inference), and its open
(`total=417` ms in `store_open_ms`) and scratch stop (0.236 s) on a 4.24 GB copy (4,239,423,985 bytes,
`07-copy-COPY.log`; `physical_bytes` 4,239,153,288, `10-rehearsal-4a-status.log`) are in
`.stage/372-dryrun/logs/14-rehearsal-4d-stop.log`. An illustration with an assumed count, not
the live one: 1,000,000 artifacts is 977 pages, 106 s at 0.108 s per page and 588 s at 0.602 s. Add
the open and the scratch stop (0.015 to 0.025 s idle at a store of 164 MB, 1.85 to 3.47 s at
4.3 GB; logs `76`, `85`, `92`, `38`, `47`), and the lookups. The rehearsal reads `N` itself, so
the figure is computed at the window, not here.

The disk the rehearsal needs. The reflink copy writes almost nothing when it is made. `check_store_copy`
asks for the store's size plus 10 GiB free for each copy, which the unchanged free space satisfies
again after a reflink. The lifted copy then diverges from `COPY` by what the lift and the migration
write. Measured: 68.2 MB in the cgroup and 106 MB in the process at a store of 8,160 artifacts and 164 MB
(`73-B-lift-cgroup.log`), and 1.19 MB in the cgroup and 9.76 MB in the process at a store of
4.32 GB and 101 artifacts (`31-lift-measure.log`). The write follows the artifact count, not the
store's size: about 8.4 kB and 11.8 kB per artifact (arithmetic, 68.2 MB over 8,160 and 1.19 MB
over 101, two points that differ in artifact size). The live figure is not measured; it is the
live `N` times about 8 to 12 kB, and the lifted copy's compaction may rewrite more.

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

#### What the measurement covers

Every figure below was measured on a scratch store under a user scope with the live unit's
limits (`MemoryHigh=2G`, `MemoryMax=4G`, `CPUQuota=100%`, `IOWeight=20`, `nice 10`, read back from
the cgroup) with the image `sparrowd-schema71-b420503`, built before the #394 fix and not the image `a50820e`, which differs from it in more than that fix, unless a line
says otherwise. Logs are under `.stage/372-measure/logs/` in the main checkout. Nothing was
measured at live size: the live store is about 39 GB and holds an unknown number of artifacts, and
the pages of the migration number `ceil(artifacts / 1024)`, or more where the artifact entries
of a page reach 64 MiB. The
live artifact count is read at the rehearsal. The host load was not logged, so no figure carries
one.

The lift was measured from `sparrow.store/332` to `/387`, not from `/340`. A store at `/340`
cannot be made with `sparrowd-schema71-fda2812`: `/340` is the tenth format of the intake family
`/330` to `/340`, and its only writer is the `artifact-retirement:` key, which the old image writes
only during expiry or eviction; with no `[archive]` it never retires (`retire_without_archive` is
false in `sparrow-server/src/config.rs`), and with `[archive]` the format is the `/341` to `/351`
family. An image from before that gate with a tiny budget reached `phase=requested` with no pass
and no eviction (logs `50` to `59`), and then wedged ingest. The lift keeps the family index (read
from the code, not run from `/340`), so `/340` lifts to `/395`. The core test
`a_340_intake_manifest_store_is_lifted_to_395_at_open_and_stays_there` (#398) proves that a
`/340` intake-manifest store, built synthetically from the legacy fixture, is lifted to `/395` at
open and stays there. It does not prove what the live store holds: no older writer is in the tree,
and what retirement left in the live store cannot be built. The rehearsal on the live store's own
copy covers that: the copy at `/340` lifted to `/395` with the `a50820e` image, the payload-index
migration ran 39 pages over 39,080 artifacts in 6.154 s, and the old image refused the lifted copy with
the text above (`.stage/image/switch-1/rehearsal-daemon.log`, `rehearsal-status.json`,
`rehearsal-old-refusal.log`).

Lift at a store of 4.32 GB on disk, 101 artifacts, 392,095 live records, `/332` to `/387`
(`21-pre-lift-old-status.log`, `31-lift-measure.log`):

- Open-time fence 61 ms; the whole store open 230 ms (`30-lift-daemon.log`).
- The first status answered 2.51 s after the launch; the open line came at 2.48 s, so about 2.2 s
  is the scope and process start (arithmetic from the 230 ms open).
- The `payload-index` migration took 1 page and 0.263 s, and completed 2.78 s after the launch.
  Repeats on copies: 0.546 s (`40-interrupt-run1-daemon.log`) and 0.602 s
  (`45-interrupt2-run2-daemon.log`).
- Peak memory 164 MB; disk written 1.19 MB in the cgroup and 9.76 MB in the process.
- Format after: `/387`.

Lift at a store of 8,160 artifacts of 3 records (24,480 records; status `physical_bytes`
262.7 MB before and 164.7 MB after, `65-B-pre-status.log`, `74-B-lift-status.log`), `/332` to
`/387` (`70-B-lift-daemon.log`, `73-B-lift-cgroup.log`, `91-B-per-page.log`):

- Open-time fence 6 ms; the whole store open 99 ms.
- 8 pages (1,024 artifacts each, the last 992), 0.870 s in all (the journal's `elapsed_seconds`).
- Per page, from the first sighting of each `done` count by a `status` sampler, 8 intervals:
  minimum 0.089 s, median 0.108 s, maximum 0.159 s. The resolution is one `status` call, 7 to 9 ms.
  The journal prints only the start and the completion, and the pages are about 100 ms apart, the
  daemon's writer-yield window (inferred).
- Peak memory 407 MB; disk written 68.2 MB in the cgroup and 106 MB in the process.
- Payload lookups after the lift: 8,160 of 8,160 answered with exactly one artifact
  (`75-B-lift-payload-verify.log`).

SIGKILL between pages, on the 8,160-artifact store, twice. Killed at `done=1024` of 8,160, the next
open's start line read `done=2048`, the migration completed with 7 more pages, and 8,160 of 8,160
lookups were exact (logs `80` to `84`). Killed at `done=5120`, the next open's start line read
`done=6144`, the migration completed in 0.191 s, and the lookups were exact (logs `86` to `90`). No
page was redone or skipped in either. A SIGKILL before the first page, on the 101-artifact store,
left the next open already at `/387` with fence 0 ms and the migration complete 101 of 101
(`43` to `46`).

The kept copy and the way back, on the 4.32 GB store (`23-keepcopy.log`, `24-interruptcopy.log`,
`42-interrupt2-copy.log`, `36-wayback-old-daemon.log`, `37-wayback-measure.log`):

- `deploy/copy-on-write.sh copy` made a reflink copy of 92 files and 4,317,743,679 bytes in 6.6 s,
  and further copies in 2.7 s and 1.5 s. At live size, not measured; arithmetic, 39 GB over 4.32 GB is 9.03 times
  the bytes: 24 s at 2.7 s and 60 s at 6.6 s, if the time follows the bytes.
- After the lift and 30,000 further records on the new image, the kept copy opened under the old
  image in 193 ms and read `sparrow.store/332` with 392,095 live records. The 30,000 records of
  the new image are absent, as the way back says.
- The old image refuses the lifted store. Command, as run: the old daemon in a systemd scope with
  `ulimit -n 65536` and `nice 0`, `sparrowd-schema71-fda2812 --config CONFIG`, on the lifted store.
  It printed `Error: Error { code: "STORAGE_FAILURE", message: "log time interpretation law and
  writer fence disagree" }` and exited with status 1, on the 4.32 GB store lifted to `/387`
  (`39-old-on-lifted-refusal.log`) and on the 8,160-artifact store lifted to `/387`
  (`170-A-old-refuses-lifted.log`). The rehearsal's step 5 command, which runs the old image with
  `--no-intake`, gave the same text and exit status 1 on a `/332`-derived store lifted to `/387`
  (`.stage/372-dryrun/logs/15-rehearsal-5-old-refusal.log`).

Stop time, SIGINT, on stores of 0.16 to 4.32 GB on disk after the same ingest, 6 artifacts of
5,000 records. The runs of logs `104`, `124`, `133`, `143` and `163` (`stop_timed.sh`) had a
SIGKILL at 120 s as the live unit does, never reached. The runs of logs `35`, `38`, `47`, `76`,
`85` and `92` (`stopd.sh`) sent no SIGKILL and waited up to 300 s:

- New image under the limits, 1.57 GB `/332` base (150,000 records): 61.4 s and 25.0 s
  (`124-C-bigNew-stop-timed.log`, `133-C-bigNew-d-stop-timed.log`).
- Old image under the limits: 18.5 s (`143-C-bigOld-stop-timed.log`). New image without limits:
  18.6 s (`163-C-unlimited-new-stop.log`).
- New image under the limits on the 4.32 GB store after 30,000 records: 234.4 s
  (`35-stop-new.log`), which the live unit's 120 s would have ended in SIGKILL. On the 8,160-artifact
  store: 1.99 s (`104-C-new-stop-timed.log`).
- No new ingest: 0.015 to 0.025 s on the 8,160-artifact store (`76`, `85`, `92`), 1.85 s and 3.47 s
  on the 4.32 GB store (`38`, `47`).

Stop time on a 3.46 GB base (186,285 records, `/332`) after six artifacts of 5,000 records, SIGINT
under the limits, with a SIGKILL deadline of 600 s for the new image runs (logs `210`, `212`, `213`)
and 120 s for the old image run (log `220`, the live unit's own timeout). The new image, lifted to `/387` by the run, status
`physical_bytes` 5.11 GB at the SIGINT: 50.5 s (`210-D-diag-new.log`) and 25.6 s
(`212-D-diag-new2.log`); 13.1 s after waiting for the compaction threads to go idle
(`213-D-diag-quiesce.log`). The old image at `/332`, status `physical_bytes` 4.76 GB: 98.8 s, no
kill (`220-E-diag-old.log`). In every run the `closed store` journal line came 0.4 to 1.2 s after
SIGINT; the rest was teardown. The slow stop is not particular to the new image. In the 50.5 s run
the compaction threads ran (about 30 CPU-seconds, 343 MB written in 44 s;
`211-D-diag-derived.log`); in the 13.1 s run threads were in IO wait. The exact call is inferred
(gdb and `eu-stack` were refused without root; no stack was taken), and the 234.4 s stop above did not reproduce. At the older
1.57 GB series the process wrote about 351 MB in 18 s during a 25 s stop (`133-C-bigNew-d-stop-timed.log`,
`write_bytes` 7,523,508,224 at 2 s and 7,874,908,160 at 20 s).

The stop does not order by store size across these runs. On-disk size before the ingest, then status
`physical_bytes` at the SIGINT where a log has it:

- New image: 61.4 s on 1.57 GB on disk (2.38 GB status, `122-C-bigNew-status.log`); 25.0 s on the
  same base (no status read); 13.1 s to 50.5 s on 3.46 GB on disk (5.11 to 5.38 GB status).
- Old image: 98.8 s on the 3.46 GB base on disk (4.76 GB status).
- New image, round 1: 234.4 s on 4.32 GB on disk (7.38 GB status, `33-post-ingest-status.log`).

The `TimeoutStopSec` above scales one of these by status size and is arithmetic.

`--no-intake`, on a scratch store at `/387` (`300-F-setup.log` to `311-F-control-stop.log`). A store
with one consume watch, bootstrapped from a `[[consume]]` block into the catalog, and three files
waiting in the watched root was reflink-copied twice. The copy opened with `sparrowd --no-intake`
and a configuration that sets neither `consume` nor `ingest_socket_path` (a `--no-intake`
configuration may not set them) logged `intake=false`; after it ran, the three files were present
with identical sha256 sums (`304-F-root-before.log`, `307-F-nointake-after.log`), `status` and a
payload lookup answered (exact, one artifact), the watch read `reconciling`, and the stop took
1.25 s (`308-F-nointake-stop.log`). The control copy without the flag consumed and deleted all three
files (`candidates=3`) and `live_records` went from 200 to 320 (`310-F-control-after.log`; stop
0.31 s). Limits: a scratch store with 200 records at `/387`, not a lifted copy of the live store, one
watch and three files.

Pending-compaction observation. `pressure.pending_flushes` and `pressure.pending_compactions` read 0
before the stops that had a status read (`33-post-ingest-status.log`, `102-C-new-pre-stop-status.log`,
`122-C-bigNew-status.log`, `152-C-kill-pre-status.log`), and read `pend_comp=3` under pressure phase
`requested` on a store with a tiny budget (`52-A-ingest.log` to `59-A-ingest3.log`).

Ingest under the limits, a throttling observation. On the 4.32 GB store after the lift, six
artifacts of 5,000 records took 55 to 176 s each (`32-post-lift-ingest.log`); the scope hit
`memory.high` 9,988 times, peaked at 2.15 GB, was CPU-throttled in 4,912 of 6,553 periods, and
wrote 12.3 GB for the 30,000 records (`34-lift-scope-after-ingest.log`). On the 1.57 GB store the
same six artifacts took 410 s and 577 s in the two runs under the limits on the new image
(`121-C-bigNew-ingest.log`, `132-C-bigNew-d-ingest.log`), 422 s on the old image (`141-C-bigOld-ingest.log`)
and 365 s on the new image without limits (`161-C-unlimited-new-ingest.log`); peak memory was
2.15 GB under the limits and 3.61 GB without (`123`, `142`, `162`; the figure includes page
cache). The lift's migration took 0.5 to 0.9 s under the limits and 0.867 s without (`120`,
`70`, `160`). No difference in what the new image needs from the limits was visible at these
sizes. Ingest under the limits is heavily throttled (Sparrow issue 396).

SIGKILL's cost, on the 1.57 GB store at `/387` after three acknowledged ingests: the next open
took 2,058 ms in the storage engine and read 165,000 live records (`154-C-kill-daemon2.log`,
`155-C-kill-post-status.log`). Not measured: a SIGKILL during an in-flight ingest, and a
SIGKILL of the old image.

Downtime at live size is not measured. Its parts are the drain (up to `TimeoutStopSec`), the copy
(24 to 60 s, arithmetic above), the rehearsal and the first open and status, which at 4.32 GB
and 164 MB took 0.23 and 0.10 s to open and 2.5 s to the first answer including scope start.
The `payload-index` migration at live size, under the unit's `CPUQuota=100%`, `IOWeight=20` and
live intake, is not measured: pages `ceil(N / 1024)`, `N` read at the rehearsal. The rehearsal
without resource limits on a scratch store (`.stage/rehearsal-323-migration/REPORT.md`) ran 13,100
artifacts in 11 pages and 1.846 s after 2 pages were already done.

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
  was when the daemon stopped. Write the runtime timeout drop-in again before the stop (the
  commands under "Before the stop: the stop timeout, the quiesce and the watch"): the stop of the
  new image runs under the unit's own `TimeoutStopSec=120` otherwise.

  ```bash
  systemctl --user stop sparrow-ship.timer sparrow.service     # SIGINT drain, up to TimeoutStopSec
  test "$(systemctl --user show -p ActiveState --value sparrow.service)" = inactive \
    && procs=$(ps -e -o comm=) \
    && [[ $'\n'$procs != *$'\n'sparrowd* ]] \
    && mv -T -- /SPARROW/store /SPARROW/store.opened-by-a50820e \
    && mv -T -- /SPARROW/store.before-a50820e /SPARROW/store \
    && test -d /SPARROW/store \
    && test ! -e /SPARROW/store.before-a50820e \
    && ln -sfn "$OLD" "$LIB/.sparrowd.next" \
    && mv -T -- "$LIB/.sparrowd.next" "$LIB/sparrowd" \
    && ln -sfn "$LIB/$OLD_CLI" ~/.local/bin/.sparrow.next \
    && mv -T -- ~/.local/bin/.sparrow.next ~/.local/bin/sparrow \
    && test "$(readlink "$LIB/sparrowd")" = "$OLD" \
    && systemctl --user reset-failed sparrow.service \
    && systemctl --user start sparrow.service
  ```

  The chain is bash (the `[[` glob needs it; it does no I/O and does not depend on `pipefail`).
  It begins with tests, and it stops there unless the unit's `ActiveState` is `inactive`,
  the process list can be read, and no process whose name begins `sparrowd` is in it: a stop that is
  still draining, a unit that never stopped, or a `ps` that fails moves nothing. The daemon runs as
  the `sparrowd` link, so its name is `sparrowd`; a name cut to 15 characters from an image name
  still begins `sparrowd`. If a test fails, wait for the stop to end, read `ActiveState` and
  `ps -e -ww -o pid,args`, and begin the chain again. A `sparrowd` process outside the unit that blocks
  the guard is reported to the coordination session; the operator never kills it.

  The chain starts the daemon only when `/SPARROW/store` exists as the restored copy and both links
  name the old pair. A start on a missing store makes a new empty store with a new id, so when any
  command fails, start nothing. If the first `mv` fails, nothing moved: fix the cause and repeat. If
  the first succeeded and the second failed, `/SPARROW/store` is absent: put the opened store back
  with `mv -T -- /SPARROW/store.opened-by-a50820e /SPARROW/store`, fix the cause, and begin the
  chain again. If a link command failed after both moves, the store is in place: repeat the link
  commands from the chain, then the `reset-failed` and the start.

  The configuration is unchanged: `data_dir` still names `$STORE`. Check `store_id`, `store_format`
  and the migrations against `before.json`, then start `sparrow-ship.timer`. The restored store
  lacks everything the new image accepted between its start and the stop: the records and files it
  ingested, and the files its consume watches took and deleted. Count that loss before the
  swap (`live_records` in `after.json` less `before.json`, the start and stop times) and report it
  with those figures; `/SPARROW/store.opened-by-a50820e` still holds it, and the new image can open that
  directory later from a configuration that names it. Fixing forward on the new image is the
  other choice, and the way back is for a defect that forward cannot fix.

Keep the copy until both hold: every migration reports `done` equal to `total` with no failure,
and a full day has passed since the first status answer with `pressure.failure`,
`durability.failure` and every migration `failure` null and no `online migration failed` line in the
journal. The operator then removes it with one command on one exact path, typed in full in a fresh shell
and after listing it:

```bash
ls -ld /SPARROW/store.before-a50820e
```

```bash
rm -r -- /SPARROW/store.before-a50820e
```

If the way back was taken, the copy is the store now, and the path to remove under the same rule,
on the coordination session's word, is the new store the new image opened:
`/SPARROW/store.opened-by-a50820e`.

#### While the daemon is down

The window runs from the unit stop to the first status answer: the drain (up to `TimeoutStopSec`), the copy,
the rehearsal, the first open and its migrations' first page. The first status answer of the new image is not the
end of the work; the index fills afterwards with the daemon serving.

The window's length is unbounded until the rehearsal reads `N`. The hold waits with no timeout, so
the daemon stays down for as long as the rehearsal and the answer take, and the migration at live
size is not measured: the live artifact count `N` is unknown until the rehearsal reads it, and the
only figures are scratch measurements at other sizes (see "What the measurement covers"). The
notice states this and gives the length only after the rehearsal has read `N`.

- Ingest socket (`ingest_socket_path`, the `sparrow-ingest` group) and the public TCP listener on port
  518 refuse connections. A client that holds and retries, such as the Rust client with its queue,
  delivers after the start; a sender without a queue loses what it emits meanwhile and the store
  cannot count that loss ("A floor refusal requests reclamation"). Which senders connect to the
  socket and the port, and which of them queue, is not established here; the notice goes to the
  coordination channel before the stop. `lillith_memetics`' cruft
  sender is named by [Cruft](interfaces.md#cruft); whether it queues is not read.
- The Lillith organism's logs do not reach Sparrow. Her members' stdout and stderr go to journald
  and to a file in her logs directory, and no source, watch or spool unit carries them: the shipper
  configuration on the host (`~/.config/sparrow/ship.toml`, read on the host and saved in
  `.stage/416/host-reads.log`) lists no source for them, the seven consume watches of the saved live
  status reading (`.stage/418-decodes/live-status.json`, roots listed in `host-reads.log`) have roots
  only under the shipper's intake roots and two development roots, none of them her logs directory, and her unit drop-in and her decision record 0105 name no taker (read in her repository, not
  on the host). The stop of the daemon loses none of her logs, because none arrive. Switch 1 does
  not stop her daemons for logging reasons.
- No Katamari process connects to Sparrow: `katamari-stored`, `katamari-unlock` and `katamari-gitd` log
  only to the local journal, the console and an optional file (Katamari issue 83; Sparrow issue 372,
  comment 19354), so the window changes nothing for them.
- Shipped kinds ride through the window. The consume and tail roots under `~/.local/state/sparrow` stay
  on disk: a file that arrives waits and the watches read it after the start. The script stops
  `sparrow-ship.timer` first and starts it again at the end; the shipper reads only the producers'
  files and writes only into the roots.
  - Four Lillith sources, `lillith-corrections`, `lillith-coordinator-corrections`,
    `lillith-discards` and `lillith-refusals`, read the `corrections`, `coordinator-corrections`,
    `discards` and `refusal-evidence` directories under her `lillith_soul/inner-core` (the host's
    `ship.toml`). Their files wait in the producer directories and in the shipper queue, which holds
    1 GiB (`max_queue_bytes`), stops adding at that bound and drops nothing, takes at most 64 files a
    run, and runs from a timer 5 minutes after each run, with an fsynced ledger
    (`deploy/libexec/sparrow-ship`, lines 317-331; `deploy/systemd/sparrow-ship.timer`, read in the
    repository). The daemon reads them after the restart. How long an outage they survive is bounded
    by the producer directories' retention, which is not determined.
  - `katamari.stored` is spooled from the journal into files by the interim user units
    `katamari-journal-spool` and `katamari-manager-spool`. This repository holds their scripts
    (`deploy/libexec/`) and unit files (`deploy/systemd/user/`); the user manager runs the copies
    under `~/.local/libexec/sparrow/` with `Restart=always`. They never connect to Sparrow, so they
    keep writing from their own units while the daemon is down, and the shipper sends those files
    after the start. Neither unit deletes a spool file (`katamari-stored-*.jsonl` or
    `katamari-manager-*.jsonl`); the shipper deletes one only once the store holds every part of it
    ([Deleting acknowledged spool files](#deleting-acknowledged-spool-files)), so an outage of any
    length loses no spooled line. The spool grows by about 4 MB a day (read for 17 to 19 November
    2026) while the store is away. Both units and `sparrow-ship.timer` read `active` on the host
    (`systemctl --user is-active`, saved in `.stage/416/host-reads.log`).
  A producer that rotates or deletes its own file in the window is outside Sparrow's reach.
- Her CLI and MCP tools (`lillith_decodes`) and every other client of the CLI and the MCP tools get
  connection refused until the first status answer. Which of her operations use them is not read.
- No syslog listener is configured at the last recorded status, so no datagram is exposed; check
  `syslog_listeners` in `before.json`.

Notice. Before: the window's start; that its length is unbounded until the rehearsal reads `N`,
because the hold has no timeout and the migration at live size is not measured (the parts listed
under "What the measurement covers" are scratch figures, and the live total is not measured), with
the length given when the rehearsal has read `N`; that the live daemon stops, that senders without a queue lose their output, that the switch is one-way once
the store opens, that a copy of the store is taken and kept, the free space read immediately before the
window with its two-copy figure ("Before the window"), and that no large build runs during it. After: the first status answer's
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
directory's share is mounted and the daemon's account can write there (the checks follow): the mount is host state this procedure never changes. It is a second
one-way fence: the first open with a policy lifts the store from the payload index intake family
(`/395` after switch 1) into the payload index archive family, `/396` to `/406` (the store keeps its
position in the family, so `/395` becomes `/406`; `docs/archive-format.md`, section 7,
"Deletion gate and store fence"). Older images refuse it, and reopening it without `[archive]`
fails with `INVALID_ARGUMENT`. The worker then
archives the whole backlog to the NAS before anything is retired, and from then on an unpinned item
is retired once it is due and archived.

Check the mount first. The worker refuses a directory that is on the root filesystem's device or
on a local filesystem of the root's type, and never creates `dir`
(`crates/sparrow-server/src/archive.rs`, `require_own_mount` and `archive_segment`). The share is
`/net/synology/archives` (nfs4, source `192.168.1.1:/volume1/archives`, mounted `hard`: `findmnt -no OPTIONS -T "$SHARE"` shows it, so a lost server hangs a call and the stall path applies rather than an error); the directory `dir` names
lies inside it, so the share's mount point is never `dir` itself. Each command chain below ends in
an exit status and stops at its first failure, and a lookup that errors stops the chain: no result
is read from a pipe or from text that an error could leave empty. Run the chains in bash. The mount, directory and write blocks share `SHARE`, `ARCH` and `my_uid`, so run them in one shell.

```bash
SHARE=/net/synology/archives
ARCH=$SHARE/sparrow-archive
m=$(findmnt -no TARGET,FSTYPE,SOURCE -T "$SHARE") \
    && [[ $m == "$SHARE nfs4 "* ]] \
    && mountpoint -q -- "$SHARE" \
    && share_dev=$(stat -c %d -- "$SHARE") \
    && root_dev=$(stat -c %d -- /) \
    && [[ -n $share_dev && -n $root_dev && $share_dev != "$root_dev" ]] \
    && printf 'on its own mount: %s\n' "$m"
```

The chain exits 0 only when `findmnt` names the share's mount point as the mount of the path,
with type `nfs4`, `mountpoint` agrees, and the share's device number differs from `/`'s. An unmounted
share leaves a directory on `/`: `findmnt` then names `/`, or the devices are equal, and the chain
exits nonzero. A `findmnt`, `mountpoint` or `stat` that fails, or prints nothing, exits nonzero
too. The type test is stricter than the worker, which accepts any of its network types; `nfs4` is the
type this share has. Stop when the chain exits nonzero: the mount is host state this procedure never
changes.

The directory `sparrow-archive` does not exist on the share. Neither the daemon, nor the worker, nor
any script of the repository creates it, and nothing in this procedure's preparation does. The
operator creates it as an ordered step of this switch, once, after the mount check passes and
before the write test:

```bash
mkdir -m 0700 -- "$ARCH"
```

The owner is the account that runs the daemon, so the command runs in that account's shell, not
under `sudo`, and not as root (a root-squashed export would store it as `nobody`). The mode is 0700
because the worker needs only its own account: it lists `dir`, creates `STORE_ID` and
`<store_id>/` in it and writes below, and the archive code sets no mode or ownership
(files take the account's umask, `write_synced`; the data directory's mode is set elsewhere, in `config.rs`). The account must hold read, write and search on
the directory, which 0700 gives. `sparrow-archive verify` run by the same account reads it. Another
account that must read the archive needs a mode and a group chosen for that reader, which the code does not
require. `mkdir` without `-p` fails when the directory exists, so a second run changes nothing; no other change is made to the share.

Check the directory and the account next. The daemon's account is the owner of the running
`sparrow.service` process; a user unit runs in its owner's user manager, and this text
names no other account. The commands compare the daemon's process owner with the shell's own uid,
and the directory's stored owner and mode with both:

```bash
pid=$(systemctl --user show -p MainPID --value sparrow.service) \
    && [[ $pid != 0 ]] \
    && daemon_uid=$(stat -c %u -- "/proc/$pid") \
    && my_uid=$(id -u) \
    && [[ -n $daemon_uid && $daemon_uid == "$my_uid" ]] \
    && t=$(findmnt -no TARGET -T "$ARCH") \
    && [[ $t == "$SHARE" ]] \
    && [[ -d $ARCH ]] \
    && stored=$(stat -c '%u %a' -- "$ARCH") \
    && [[ $stored == "$my_uid 700" ]] \
    && printf 'directory %s: owner uid %s, mode 700, on %s\n' "$ARCH" "$my_uid" "$t"
```

`MainPID` is 0 for a stopped unit, which stops the chain; the second `findmnt` stops it when
`$ARCH` is a local directory under the mount point of an absent share.

Write a small file there from the daemon's own account, read it back and remove it. This runs on the
share itself and covers identity mapping and root squash, which the worker meets on its first
`STORE_ID` write. The file has one literal name:

```bash
f=$ARCH/.switch2-write-test
printf 'switch 2 write test\n' > "$f" \
    && sync -- "$f" \
    && cmp -- "$f" <(printf 'switch 2 write test\n') \
    && owner=$(stat -c %u -- "$f") \
    && [[ $owner == "$my_uid" ]] \
    && stat -c 'stored owner %U %u mode %a' -- "$f"
```

The chain exits 0 only when the write, the flush to the server (`sync` of the one file, as the
worker syncs each file it writes), the read-back and the owner comparison all succeed. A failure is
one of these. `Permission denied` (EACCES) or `Operation not permitted` (EPERM) at the write: the
mode or owner of `$ARCH` is wrong, or the export squashes this account. A file that is created and
stored as `nobody` or uid 65534: root squash or all-squash on the export (an all-squash export can map to another uid; the owner comparison catches any uid but this account's), or an identity mapping that
does not map this account (`stat -c '%U %u'` shows the owner the server stored; the chain's
comparison fails on it). A differing read-back: the client cache or the export. The server's export
options are not visible from the client, and the exported `sync` option that
[the archive format](archive-format.md) requires cannot be read here; this test does not prove it.
Then remove the file by its literal path, whatever the chain's result, and check the directory is empty:

```bash
rm -- "$ARCH/.switch2-write-test"
ls -A "$ARCH"
```

`ls -A` prints nothing, or this store's `STORE_ID` and `<store_id>/` only. A directory that holds other entries without a
`STORE_ID` is refused by the worker and is not adopted. Do not start the switch until every chain above exits 0 in
the daemon's account, the file is removed and the directory is empty.

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

If the mount goes away while the daemon runs, the code shows this (`crates/sparrow-server/src/archive.rs`).
The worker checks the mount once per segment, at its start: `archive_segment` tests that `dir` is a
directory (`is_dir` reports any error, `ESTALE` and `EIO` included, as false) and then
reads its device and filesystem type (`require_mounted`). A directory that is gone fails
as `ARCHIVE_IO` ("not a directory; is it mounted?"); a directory left on the root disk fails as
`ARCHIVE_NOT_ON_MOUNT`. Once past the check, each later step (create, write, sync, rename, read back)
fails as `ARCHIVE_IO` with the step, the path and the operating system's error, and the check is not
repeated inside the segment. Every failure leaves the watermark where it was
(`archive_commit` runs only after the read-back), the pass ends, `status` reports
`archive_worker.phase` `holding` with the code, the segment file and the reason, the daemon
journals `sparrowd: archive pass failed; the watermark did not move: ...`, and the
worker tries again after `check_secs`. A call that hangs instead of failing blocks the worker's
thread inside the system call: the watchdog journals `sparrowd: archive stalled step=...` every
`stall_warn_secs`, and a step that ran `io_timeout_secs` marks the pass as overrun, which is checked only after the
write and the read-back succeed (`archive_segment`). A slow call that returns an error fails the pass as `ARCHIVE_IO`, and a later
failing step fails with its own code (for example `ARCHIVE_IO`, `ARCHIVE_VERIFY`, `ARCHIVE_NOT_ON_MOUNT` or
`ARCHIVE_ROOT`: after a slow step that succeeds, the mount and root checks that follow can fail too; these are
examples, not a complete list); `ARCHIVE_OVERRAN` is the result only
when every step succeeded. The segment is not committed in any of these cases. The daemon keeps serving in each case. A segment file
that a failed pass left in `<store_id>/` is kept and read back on the next pass, never overwritten
(`write_segment`); one that differs from what the worker prepared fails as
`ARCHIVE_VERIFY` and the operator moves it aside.

Nothing is retired because of the loss. Expiry and pressure eviction both stop at the first entry the
watermark does not cover (`ExpiryFrontier::ArchiveLag`, `crates/sparrow-core/src/storage/expiry.rs`;
`EvictionFrontier::ArchiveLag`, `eviction.rs`), and only `archive_commit` moves the
watermark. Entries the watermark already covered stay retirable while the mount is away: neither
path reads the mount, so an archived entry is retired when it is due. The code reading does not
show these: whether a write that the kernel acknowledged before the mount vanished reached the NAS
(the worker syncs each file and its directory, but the export's `sync` option is not visible
from the client); how long a `hard` mount holds a call before an error, if one ever comes (`timeo` and `retrans` are set, but no run was made); and any
run of the worker against a lost mount on the live daemon, where the worker is not running.

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
