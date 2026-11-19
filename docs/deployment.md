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
`make install-bin`. It builds the release daemon, CLI and recovery probe
through the managed cargo wrapper and copies them into `./bin` with
`cp --reflink=auto` under revision-qualified names:
`bin/sparrowd-schemaN-REV`, `bin/sparrow-schemaN-REV` and
`bin/sparrow-recovery-schemaN-REV`, where `N` is the client schema version and
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
the store as a separate operator step. The role never changes after creation, and
the daemon does not stamp an existing store. Leave the key out of a configuration
for a store that has no role row.

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
deploy/user-unit-upgrade.sh --from sparrowd-schema68-2aa56a9 --to sparrowd-schema71-REV
```

The live service runs `sparrowd-schema68-2aa56a9` since the schema 69 rollback,
so the switch goes from schema 68 straight to schema 71. After it, the daemon
refuses schema 68, 69 and 70 clients, since it admits only its own schema;
switch every CLI and producer to the schema 71 pair with it.

Both arguments name daemon images under `~/.local/libexec/sparrow`. Each CLI
image is the daemon's name with `sparrowd-` replaced by `sparrow-`, and the
schema numbers come from the names. Run it as the operator, only on the
coordination session's go. `SPARROW_PREFIX` (default `~/.local`),
`SPARROW_CONFIG` (default `~/.config/sparrow/server.toml`, which names the store
and its sockets) and `SPARROW_BIN` override the layout; `--help` lists them.

What an upgrade relies on: there is no backup to restore from. The undo is the kept previous
image and the link swap that points the daemon and CLI links back at it. A format-changing
upgrade has no undo: once `--to` opens the store and upgrades its format, the `--from` image
refuses it (the store fence), so the only way out is forward on `--to`.

Before anything changes it refuses unless all of these hold:

- The daemon link names `--from`.
- All four images are executable.
- The `--to` pair matches `./bin` byte for byte.
- The `--to` daemon carries `cap_net_bind_service`.
- `server.toml` sets the ingest keys.
- The store has copy-on-write off.
- The unit's drop-in directory holds only an identical `limits.conf`.
- The running daemon reports the `--from` schema.

The capability is a prerequisite done by a human with root,
`sudo setcap cap_net_bind_service=ep ~/.local/libexec/sparrow/IMAGE`; the
script checks it with `getcap` and never runs `setcap`.

Then it stops `sparrow-ship.timer` and waits for the shipper, stops the unit, installs the
`LimitNOFILE` drop-in, switches the links and starts the unit. At the end it starts
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
`--from` is possible only while `--to` did not change the store's format.

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
