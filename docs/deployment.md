<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Installing Sparrow on Linux

The example deployment lives in [`deploy/`](../deploy/README.md): a system
unit, `sparrow.service`, running as the `sparrow` user, with its store and private socket under `/var/lib/sparrow/store`,
its consume roots under `/var/lib/sparrow/intake` and its configuration in
`/etc/sparrow`. `deploy/README.md` walks from a scratch clone to a running,
verified service, `deploy/RECOVERY.md` covers failures and rollback,
and `deploy/MANUAL.md` runs the daemon by hand. This document explains the
configuration and the behavior those procedures rely on.

## Kept images

Install the helper census with `make install-census`. Run the gate from the
checkout being qualified. Choose a new log under the canonical checkout's
`.stage/` and create its parent directory first.

```sh
tools/run-gate.sh /path/to/main/.stage/build-check.log -- make check
```

The runner takes the shared Cargo lock and logs the working directory, commit,
UTC time, command and output. Its final `EXIT=` line and its process status
both report the gate command's exit status.
Commit through the normal verification hook and run `make install-bin`.
It builds the release daemon, CLI, recovery probe and role stamp
through cargo and copies them into `./bin` with
`cp --reflink=auto` under revision-qualified names:
`bin/sparrowd-schemaN-REV`, `bin/sparrow-schemaN-REV`,
`bin/sparrow-recovery-schemaN-REV` and `bin/sparrow-stamp-schemaN-REV` (the
offline role stamp), where `N` is the client schema version and
`REV` the short commit. It also copies the five native CBOR lexicons, with
their provenance manifest, notices and licenses, from the PurRDF revision the
lockfile resolves into `bin/lexicons-PURRDF_REV/`. It refuses an uncommitted
tree and never overwrites a kept name with other bytes. `./bin` is ignored by
Git, and a kept image survives the automatic cull of `target/`. The copy is a
reflink only when `target/` and the checkout share a filesystem; otherwise
`./bin` holds full copies (about the size of the images).
`deploy/install.sh` and `deploy/start.sh` install from `./bin` into
`/usr/local/libexec/sparrow` with `cp --reflink=auto`, so every deployed image
has a kept source copy, and never overwrite an executing image.

## Configuration

Configuration supplies the store/socket paths, disk budget, maintenance reserve,
operation ceilings and intake definitions. `deploy/config/server.toml.example`
is the deployment template; `config/development.toml.example` (store under
`/srv/sparrow-dev`) is an example for owned synthetic intake, not a production
configuration; copy it to the gitignored `config/development.toml` to use it. Copy the
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

Schema 74 rebuilds the derived source summaries in resumable pages. Opening
a store selects writer family 572 through 626 without a sealed receipt
watermark, or 627 through 681 with one. The first eligible sealed request
activates the watermark and advances the writer family within those ranges.
During the rebuild, source listings combine rebuilt rows with totals from the
remaining catalog entries. See [publication and durability](architecture.md#61-publication-and-durability).

The image schema guard refuses a previous daemon with a lower schema before
stopping the service. To use an older daemon after the format upgrade, restore
a compatible pre-upgrade store snapshot.

Upgrade the CLI with the daemon when the wire schema changes. Producers that
support `sparrow-producer/1` can connect across compatible schema versions.
Upgrade producers built before profile negotiation with the daemon. The live
store's writer format is independent of its wire schema. [Producer contract](interfaces.md#producer-contract-sparrow-producer1)

`instance_role` (`"production"` or `"scratch"`, optional) fixes the role of a
store when the daemon creates it, and `discover` and `status` report it as
`instanceRole` (`unspecified` for a store with no role). At startup a configured
role that differs from the stored one stops the daemon with a message naming both
values. A configured role on a store that has none also stops it; that message
names only the configured role and offers two remedies: remove the key, or stamp
the store as a separate administrative step (see [Stamp the role](#stamp-the-role)). The
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
`instance_role`, which the administrator records. It refuses, leaving the store
unchanged, a store that already has a role row (`CONFLICT`, naming it; a stored
role never changes), a store at a format this build does not know
(`UNSUPPORTED`; run it from the build that opens the store) and a directory that is
not a Sparrow store (`INVALID_ARGUMENT`).

After the stamp, start the daemon with the key set to the stamped word or absent;
`sparrow SOCK discover` reports the role. Running the tool against the live store is
a lifecycle act for the administrator; no procedure runs it unasked.

`deploy/test-instance.sh` writes `instance_role = "scratch"` into the scratch
configuration it generates, together with an ingest socket for senders, a disk
budget and a free-space floor sized for a test load, and an `archive` table only
when `SPARROW_TEST_ARCHIVE_DIR` names a directory.
A scratch store created before roles existed has no role row, so the daemon
refuses to start on it; `test-instance.sh setup` detects it and asks for
`test-instance.sh purge` followed by a fresh `setup`. `deploy/README.md`,
"Test instance", gives the paths and teardown steps.

`max_concurrent_requests` admits simultaneous storage work, including intake
workers (1..64). `max_rpc_connections` admits private Unix RPC streams
(1..256, default 16). The group ingest socket has its own ceiling. An idle
capability holds a transport lease without consuming a storage permit. The
daemon takes a Unix connection slot before accepting a peer, so excess peers
wait in the kernel backlog until a slot frees. Public TCP uses an independent
acceptance path. It closes silent peers after 10 seconds. Positive reads renew
the 60-second idle lease, and every connection closes by 10 minutes after
admission. An intake operation (ingest, events, imports,
flush, pins, discovery) that finds every storage permit taken waits for one, in
arrival order, for at most 3 seconds, then receives the retryable
`RESOURCE_LIMIT`. About a quarter of the permits (rounded up, none when there is
only one) are reserved: queries and status reads may hold the rest, so a query
storm cannot starve ingest, and a read beyond its share is refused at once. These
are separate finite resource budgets; `status` reports each wait.

`max_import_journals` defaults to 16. Set it to 20 to make four additional
slots available to private Unix, group ingest, CHAP and consume. Anonymous
TCP can start a journal while total occupancy is below 16. `status` reports
the limits and occupied slots under `import_journals`. Use the current writer
to drain the registry before rolling back to an older image.
[Import journal rollback](../deploy/RECOVERY.md#roll-back-expanded-import-journal-admission)

Sparrow has no configurable per-artifact byte limit. A large artifact ingests
through the staged import within `disk_budget` and `min_free_bytes`, up to one
fixed bound: an import above 8515420160 bytes (about 7.93 GiB) is refused before
upload with a non-retryable `INVALID_ARGUMENT` (advertised as
`Discovery.maxImportBytes`). That is the largest artifact whose catalog row one
expiry or eviction pass can read, and it is below what one atomic publication can
carry. A single document above
128 MiB is stored as consecutive raw records, so within the bound expiry and
eviction retire it in bounded passes. Pin rows and their members share a
separate retirement budget. Creating or extending a pin beyond that budget
returns `RESOURCE_LIMIT` with the current totals. Releasing a pin frees its
charge.

A consume watch splits an over-bound file into ordered parts when the daemon
advertises the required limits. The watch keeps the claim and names the
refusal in status if those limits are unavailable. The shipper may refuse an
over-bound non-line file before it reaches the watch.
[Part sets](interfaces.md#part-sets-for-consume-roots)

`min_free_bytes` (default 4 GiB) is a floor on free space in the filesystem
that holds `data_dir`. `disk_budget` bounds only the store's own bytes, while
other programs on the host can fill the same disk. A write that fails because
the disk is full latches durability uncertain, and writes are refused until the proof of
durability succeeds or the store is reopened. To avoid that, every write is checked first: below the floor it is refused with a
retryable `STORAGE_PRESSURE`, and buffered writes that were already accepted
are still synchronized. The daemon checks free space every second. Below the
floor it keeps accepting Unix, ingest and TCP connections, including syslog TCP
connections. Unix connection ceilings and transport deadlines still apply.
It answers every write with the retryable `STORAGE_PRESSURE` carrying the
`freeSpaceFloor` class, so a producer knows the floor is the cause and holds
(the Rust client keeps its batch and retries), and it serves reads, with the
disk a query stages limited to 32 MiB in total, within the free space above half the floor.
It claims no consumed files and runs only WAL synchronization; consume and tail
watches keep their files. A syslog TCP frame is held and its sender waits.
Syslog UDP datagrams that still arrive are read and queued by each UDP listener,
up to its `hold_bytes` (default 64 MiB; the sum over listeners at most 512 MiB), and
published in order when writes resume. Beyond that the reader stops and the kernel
buffer takes the arrivals, and the kernel counts the datagrams it drops. A crash
or forced kill loses the queue and kernel buffer without reporting those losses.
Size `hold_bytes` against
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
15 seconds, and clears the latch in place when the proof succeeds. If it still
cannot establish durability after 120 seconds of adequate space, the daemon
replaces the storage session in the same process. Writes and old RPC
capabilities are fenced during the transition. Watch for the named recovery
state and the matching store identity when it resumes.
[Session replacement](interfaces.md#daemon-teardown)

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
`sparrow --secret-file PATH tcp://ADDRESS:518 COMMAND`. A producer can use the
[producer operations](interfaces.md#producer-contract-sparrow-producer1) without
the shared secret. CHAP does not encrypt traffic or authenticate the server,
so restrict the listener to trusted networks.

Port 518 is below 1024. The unit grants `CAP_NET_BIND_SERVICE` as an ambient
capability and holds nothing else in its bounding set; no file capability is
set on any image, and copying an image therefore needs no `setcap`. The client
needs no capability.

The private Unix socket is `rpc.sock` directly inside `data_dir`. The daemon
sets `data_dir` to mode 0710 at start and the socket to 0660 each time it binds it (at
start, and again when the name was removed while it ran), so
members of the daemon's group can connect and cannot list or read the store;
store files stay owner-only under the unit's `UMask=0077`. Only the administrator and the shipper's user
belong to that group, for the CLI and administration. Producers ingest
through the ingest-only group socket below, not this socket.

### Syslog TCP listener install steps

A `[[syslog]]` listener with `transport = "tcp"` keeps the frames it holds at a stop in
`/var/lib/sparrow/syslog-spill`, beside the store: a frame the store still refuses for pressure when the
daemon stops is written there and published at the next start (`docs/interfaces.md`, syslog TCP).
The shipped unit, profile and tmpfiles entry do not grant that directory, so the host needs these
steps before the first such listener is configured, and the daemon refuses to start with an error
naming the directory when it cannot write there:

1. Add `d /var/lib/sparrow/syslog-spill 0700 sparrow sparrow -` to `/etc/tmpfiles.d/sparrow.conf` and run
   `systemd-tmpfiles --create sparrow.conf`.
2. Add `/var/lib/sparrow/syslog-spill` to the unit's `ReadWritePaths=` and run `systemctl daemon-reload`.
3. Add `/var/lib/sparrow/syslog-spill/ rw,` and `/var/lib/sparrow/syslog-spill/** rwk,` to
   `/etc/apparmor.d/sparrow` and reload the profile with `apparmor_parser -r`.
4. Restart the daemon on the new unit.

These write permissions are required only for TCP listeners. Startup still scans
the spill directory even when no syslog listener is configured. If it names a
file without a matching TCP listener, restore that listener to replay the file
or move the file out of the spill directory by hand.

### Ingest-only group socket

Daemons that run as other accounts log into Sparrow through a second Unix
socket. It admits the [producer operations](interfaces.md#producer-contract-sparrow-producer1)
and refuses read and administrative operations with `PERMISSION_DENIED`.
Sparrow records the caller's uid, gid and pid on every accepted artifact
and record, using the kernel's connection credentials (`SO_PEERCRED`).

1. Create the group and add each producer account to it:

   ```bash
   sudo groupadd --system sparrow-ingest
   sudo usermod -aG sparrow-ingest PRODUCER_ACCOUNT
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

The shipped system unit grants write access to `/run/sparrow-ingest` when
the tmpfiles entry creates it. The AppArmor profile allows only
`/run/sparrow-ingest/ingest.sock` in that directory. Configure a corresponding
unit drop-in and profile rule if using another socket path.

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

## Producer source bindings

Use `[[source_bindings]]` in `server.toml` to restrict which RPC producers
can write a source. Use `prefix = true` for a literal prefix. The daemon
refuses overlapping selectors and empty permission lists at startup.
A single rule can allow multiple caller classes.

```toml
[[source_bindings]]
source = "worker."
prefix = true
allow = [
    { class = "private-unix", uid = 1000 },
    { class = "ingest-socket", uid = 1001 },
]

[[source_bindings]]
source = "remote"
allow = [{ class = "chap" }]
```

The Unix classes use the connecting account's kernel-reported UID.
`chap` grants the source to authenticated TCP sessions and
`anonymous-tcp` grants it to TCP peers using the anonymous capability.
Unbound sources keep their endpoint's ordinary access. See
[producer source admission](interfaces.md#producer-source-admission).

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
a leak cannot deepen host-wide memory pressure. The daemon runs at nice
-20 with best-effort IO priority 0, so other work yields to it under
contention. `CPUQuota=100%` holds the daemon to one core and
`IOWeight=20` makes it yield disk to the host's other services: a
sustained deep-JSON backlog can saturate a host's CPU and disk, and under
these limits ingest slows instead.

`LimitNOFILE=65536` gives the store its descriptors. At open the store reads
its soft `RLIMIT_NOFILE`, keeps 1024 descriptors for sockets, connections,
logs, the WAL, intake and archive files, and gives the rest to RocksDB's
table cache (`max_open_files`), which closes the least recently used table
file instead of exceeding it. Below a soft limit of 2048 the store refuses to
open, naming `LimitNOFILE=65536`. Size the descriptor limit for the Unix
ceilings, public TCP traffic and one descriptor per watched directory at the
configured coverage. The
startup line ends `nofile=N max_open_files=M`. Tests and `make check` open
stores too, so they need a shell whose `ulimit -Sn` is at least 2048. A unit
that sets no limit inherits the manager's soft 1024, which a store of about 900 table
files exhausts. Journal output is available through `journalctl -u sparrow.service`.

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
test unit from the same file, so both carry the entry. `sparrow-ship.service` runs no RocksDB and keeps the two-entry
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

Schema 70 and later images first seed the store counters, the `store-counters`
migration, ahead of the others. Until it completes, `status` reports
`counts=pending` and every migration's `total` as `counting`; `live_records`
and the two unique counts then cover only the keys seeded so far and are not
totals. Every consumer of those counts says so rather than printing a partial
number as a total: the closing line logs `unique_payload_chunks=pending`.
`tools/install-bin.sh` names the pair from `SCHEMA_VERSION`, so a
schema N build installs as `sparrowd-schemaN-REV` and `sparrow-schemaN-REV`.

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
Other formats, such as single JSON documents, ship whole. A file above the
daemon's single-import limit arrives in the watch root unchanged. The consume
watch publishes its parts in order and publishes the manifest only after the
parts are durable. Remove `max_whole_bytes` from the shipper configuration.
The shipper refuses a configuration that still sets it before anything ships.
The consume worker moves a file
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
nested JSON: under a host load average near 45, 17 KiB-per-line
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

If a source ledger cannot be read or has a malformed entry, the shipper names
the source and ledger and pauses queueing and reaping for that source. It still
feeds previously staged items and processes healthy sources. Restore the
affected ledger before the next run to resume queueing and reaping.

`state_dir` must be on the same filesystem as `intake_root`, because each
queued item is renamed into its root; production keeps both under `/var/lib/sparrow`.
The daemon runs as its own user and reads each item through its group, so the
shipper sets every staged item to mode 0640, and `state_dir` is setgid to the
daemon's group (`deploy/README.md`, "Permissions"). Create each consume root
and its watch before the first run. `deploy/install.sh` installs the shipper
and `sparrow-ship.service`, a system unit that runs it as the `sparrow-ship` user,
with `sparrow-ship.timer`.

The timer runs five minutes after each completed run. Shipped data arrives when
its producer finishes a file, not as it is written.

### Spooling one unit's journal lines

`deploy/libexec/sparrow-unit-spool UNIT SPOOL_DIR CURSOR_FILE` follows the default system journal
for one unit and writes JSON Lines in ten-minute files, `SPOOL_DIR/unit-spool-YYYYMMDDTHHMM.jsonl`.
The file name is a transport grouping, not the time of the entry: a record carries its own `time` and
`realtime_usec`. A line that arrives after a newer bucket exists joins the newest bucket while that file is open.
A bucket closes once its file has gone unwritten for 600 seconds (`SPOOL_QUIET_SECONDS`). A closed file is never
appended to: a line that is not newer than it starts the bucket of the current time, or the bucket after the newest
one when the clock is behind it.

Every shipper source that reads these files must settle at least 300 seconds after the spool's quiet period:
`settle_seconds` of 900 or more, with `rule = "settled"`. Then a file is shipped only after it has closed, and an
accepted file does not change. `deploy/tests/spool-settle.sh SHIP_CONFIG` checks a shipper configuration and exits 1
for a source that breaks this. Without an argument it checks its own fixtures.
Each record carries `source`, `unit`, `time`, `realtime_usec`, `priority`, `message`, and `manager` when the
entry names a command. Lines the service manager writes about the unit (stops, exits, OOM kills) have
`"source":"systemd-manager"`. The unit's own process lines are dropped by default. With
`SPARROW_UNIT_SPOOL_OWN_LINES=keep`, they are kept with `"source":"systemd-unit"`, which a unit that logs through
stdout or stderr to the default journal needs. The unit name is exact: a unit whose name extends another's is not
that unit's line.

The cursor file is written after each record is appended, so it never runs ahead of the spool.
Across a process crash, a record appended before the cursor write is spooled again on the next run: the spool
is at-least-once. The records are not fsynced. A host crash or power loss can therefore lose records the kernel
had not yet written to disk, and a crash during the backfill spools the whole backfill again. A first start with
no cursor and no `SPARROW_UNIT_SPOOL_SINCE` spools the unit's entire retained journal.

The commands in this section use placeholders: `UNIT` is the unit's name without `.service`, `REPO` the checkout,
`NAME` the source name, `SOURCE` the name of an existing source whose files are appended to after they settle. A
host's literal commands belong in that host's install document, not here, because this document carries generic
examples (AGENTS.md).

The spool account must be able to read the system journal. A spool for one unit uses its own directory and
cursor file, both under the account's state directory (`~/.local/state/sparrow/unit-spool`). Install the tracked
program under its own name, `sparrow-unit-spool`, beside any spool program the host already runs. The template
unit never changes or replaces another file in `~/.local/libexec/sparrow`, such as the program an existing spool
unit runs: that unit keeps running its own program until it moves to the tracked program in a separate change.

One template, `deploy/systemd/user/sparrow-spool@.service`, serves every unit. Its instance is the unit's name
without `.service`, so `sparrow-spool@UNIT.service` spools `UNIT.service`. The template reads the instance with
`%i`, which keeps it verbatim. The unescaping specifier `%I` would turn the hyphens of a name such as
`example-sync-nightly` into slashes, so the template does not use it. The test renders the template and runs the
program with a hyphenated instance.

Find a unit's lines with the unit name:

```sh
jq -c 'select(.unit == "UNIT.service")' ~/.local/state/sparrow/unit-spool/UNIT.service/*.jsonl
```

#### The shipper

On a host whose shipper runs in the user manager (`sparrow-ship.timer`, configuration `~/.config/sparrow/ship.toml`,
consume roots under `~/.local/state/sparrow/intake`), the shipper is `~/.local/libexec/sparrow/sparrow-ship`. The
system-install paths of this document (`/etc/sparrow`, `/var/lib/sparrow`) apply only to a system install.

- Configuration: the shipper reads its configuration on each run, so a new `[[source]]` takes effect at the next run
  without a restart.
- Required directories: a source's `dir` must exist when the configuration is loaded. Its consume root
  `intake_root/NAME` must exist when a run starts. A missing consume root aborts that run before any source is
  queued, so it fails every source, not only the new one.
- Watch: the consume root needs a consume watch on the daemon. A new watch takes expected revision `0`. Adding it is
  a live store operation: it registers one more consume root with the running daemon, without a restart, and the
  other watches keep running. This document does not record a measurement of the daemon during that operation.
- In flight: a run holds the shipper's lock. A stop leaves queued items queued. A stop between queueing a file and
  recording it in the ledger can queue that file again on the next run. Neither loses a record.
- Waiting: nothing on the write path waits on the shipper. The sync writes to the journal whether or not the spool
  or shipper runs. The journal keeps unread lines only within its own retention limits, so a spool that stays
  stopped longer than that loses the lines it has not read.
- A silent or stopped unit: the spool does not fail. `journalctl --follow` waits for new entries of the unit, so the
  spool idles and writes nothing.
- Latency in normal running: the shipper ships a bucket `settle_seconds` after its last write, plus one shipper run.
  The required `settle_seconds` is 900, so a line reaches the store about 15 minutes after its bucket's last write.
  A stopped unit's last bucket therefore ships within 15 minutes of its final line.

#### Install

Each step has a check that it took and the single command that undoes it. Stop at the first failed check and undo
that step before anything else. Step 1 creates a consume root, step 2 registers a watch on the running daemon, step 4 starts the unit, and step 6
replaces the live shipper configuration. Each of these needs its own explicit authorization.

0. Back up the shipper configuration; the backup is kept until the removal step deletes it.
   `cp -p ~/.config/sparrow/ship.toml ~/.config/sparrow/ship.toml.pre-NAME`
   Check: `cmp ~/.config/sparrow/ship.toml ~/.config/sparrow/ship.toml.pre-NAME` prints nothing.
   Undo: `rm -f ~/.config/sparrow/ship.toml.pre-NAME`

1. Create the consume root, with the mode the other roots use.
   `install -d -m 0700 ~/.local/state/sparrow/intake/NAME`
   Check: `test -d ~/.local/state/sparrow/intake/NAME`
   Undo: `rmdir ~/.local/state/sparrow/intake/NAME` (removes it only while empty)

2. Register the consume watch on the production daemon. The definition copies an existing watch definition, with
   its source and root changed to `NAME` and `~/.local/state/sparrow/intake/NAME`. The expected revision is `0`.
   `sparrow SOCKET watch put REQUEST_ID 0 DEFINITION.toml`
   Check: `sparrow SOCKET watch list` shows a watch with that root, enabled.
   Undo: `sparrow SOCKET watch remove REQUEST_ID WATCH_ID REVISION`, with the watch ID and current revision that
   `watch list` reports.

3. Install the program and the template unit, not started. Install the template once: a second unit needs only
   step 4 for its own instance.
   `install -D -m 0755 REPO/deploy/libexec/sparrow-unit-spool ~/.local/libexec/sparrow/sparrow-unit-spool`
   `install -D -m 0644 REPO/deploy/systemd/user/sparrow-spool@.service ~/.config/systemd/user/sparrow-spool@.service`
   `systemctl --user daemon-reload`
   Check: `cmp` of each installed file against its repository file prints nothing.
   Undo: `rm -f ~/.local/libexec/sparrow/sparrow-unit-spool ~/.config/systemd/user/sparrow-spool@.service`
   (while no other instance uses them), then `systemctl --user daemon-reload`.

4. Start the spool unit (authorized by the sync's install).
   `systemctl --user enable --now sparrow-spool@UNIT.service`
   Check: `systemctl --user is-active sparrow-spool@UNIT.service` prints `active`, and
   `test -d ~/.local/state/sparrow/unit-spool/UNIT.service` succeeds. The directory exists once the unit has started,
   whether or not the sync has logged yet.
   Undo: `systemctl --user disable --now sparrow-spool@UNIT.service`

5. Write the candidate beside the live file, with the entry below appended. Validate it with the installed shipper's
   loader, which writes nothing. The loader checks the entry's spool directory, which step 4 creates.
   ```toml
   [[source]]
   name = "NAME"
   dir = "/home/USER/.local/state/sparrow/unit-spool/UNIT.service"
   pattern = "unit-spool-*.jsonl"
   rule = "settled"
   format = "jsonl"
   settle_seconds = 900
   ```
   Check: `test -d ~/.local/state/sparrow/intake/NAME` (step 1), `deploy/tests/spool-settle.sh ~/.config/sparrow/ship.toml.candidate`
   prints PASS, and the loader prints `valid`:
   `python3 -I -B -c 'import importlib.machinery,importlib.util,sys; l=importlib.machinery.SourceFileLoader("ship",sys.argv[1]); s=importlib.util.spec_from_loader("ship",l); m=importlib.util.module_from_spec(s); l.exec_module(m); m.load_config(sys.argv[2]); print("valid")' ~/.local/libexec/sparrow/sparrow-ship ~/.config/sparrow/ship.toml.candidate`
   Undo: `rm -f ~/.config/sparrow/ship.toml.candidate`

6. Save the live file, then rename the candidate into place. The rename is atomic within the directory; the live
   file is never written in place.
   `cp -p ~/.config/sparrow/ship.toml ~/.config/sparrow/ship.toml.pre-swap-NAME`
   `mv -f ~/.config/sparrow/ship.toml.candidate ~/.config/sparrow/ship.toml`
   Check: the validation command of step 5, on `ship.toml`, prints `valid`. The next timer run logs `NAME:` in
   `journalctl --user -u sparrow-ship.service`.
   Undo: `cp -p ~/.config/sparrow/ship.toml.pre-swap-NAME ~/.config/sparrow/ship.toml.restore && mv -f ~/.config/sparrow/ship.toml.restore ~/.config/sparrow/ship.toml`

The source has no `delete_acknowledged` key: the existing configuration has no store socket or CLI for reaping, so
the spool's files are kept.

#### Arrival check

After step 6, expect nothing in the store for the first minutes. A bucket reaches the store `settle_seconds` (900)
after its last write, plus one shipper run, so the first arrival takes about 15 minutes. Silence in that time is not a
failure.

- In the spool, for one unit's lines: `jq -c 'select(.unit == "UNIT.service")' ~/.local/state/sparrow/unit-spool/UNIT.service/*.jsonl`
- In the store: `sparrow SOCKET records --text UNIT.service`. Its `total_items` grows as lines arrive. A stored
  record's Sparrow `source` is the shipper source name. Read one record back with `bytes`: the spool line inside it
  has `"source":"systemd-unit"` for the unit's own lines and `"unit":"UNIT.service"`.

#### Separate step: the bucket rule of a source whose files grow

This step is declined independently of the install above. It changes two lines of the shipper configuration and is
made only after install steps 0 to 6 have held.

A. Back up the configuration next to the live file.
   `cp -p ~/.config/sparrow/ship.toml ~/.config/sparrow/ship.toml.pre-SOURCE-settled`
   Check: `cmp` of the two files prints nothing.
   Undo: `rm -f ~/.config/sparrow/ship.toml.pre-SOURCE-settled`

B. Write a candidate beside the live file, with two lines changed in the `[[source]]` block whose name is
   `SOURCE`: `rule` becomes `"settled"` and `settle_seconds` becomes `900`, which is at least 300 seconds
   after the spool's 600-second quiet period. No other line changes.
   Validate it with the loader command of install step 5.
   Undo: `rm -f ~/.config/sparrow/ship.toml.candidate`

C. Rename the candidate into place: `mv -f ~/.config/sparrow/ship.toml.candidate ~/.config/sparrow/ship.toml`.
   Check: the next timer run logs `SOURCE: queued` in `journalctl --user -u sparrow-ship.service`. After that
   run, each bucket file settled past `settle_seconds` appears in the ledger exactly once, and the repeated-path
   count does not grow: `cut -f2 ~/.local/state/sparrow/ship/SOURCE.ledger | sort | uniq -d | wc -l`.
   Undo: `cp -p ~/.config/sparrow/ship.toml.pre-SOURCE-settled ~/.config/sparrow/ship.toml.restore && mv -f ~/.config/sparrow/ship.toml.restore ~/.config/sparrow/ship.toml`

Under the settled rule with `settle_seconds = 900`, the newest bucket reaches Sparrow about 15 minutes after its
last line, whether or not a newer bucket exists. That delay is the expected behaviour, not a fault.

#### Remove

Undo the install in reverse order. Each step has a check that it took. Do not restore a whole saved configuration
to remove an entry: that would also revert any later change to the file. Remove the entry by a candidate instead.

1. Remove the source entry: write a candidate without the `[[source]]` block, validate it with the loader, and rename
   it into place atomically.
   Check: the loader prints `valid`, and the next run no longer logs `NAME:`.
2. Stop and disable the unit: `systemctl --user disable --now sparrow-spool@UNIT.service`.
   Check: `systemctl --user is-enabled sparrow-spool@UNIT.service` prints `disabled` or reports no such unit, and
   `systemctl --user is-active sparrow-spool@UNIT.service` prints `inactive`.
3. Remove the program and the template unit file (while no other instance uses them), then
   `systemctl --user daemon-reload`.
   Check: `test ! -e ~/.local/libexec/sparrow/sparrow-unit-spool && test ! -e ~/.config/systemd/user/sparrow-spool@.service`.
4. Wait until the consume root is empty, so that every item already fed has been consumed.
   Check: `find ~/.local/state/sparrow/intake/NAME -mindepth 1 -print -quit` prints nothing.
5. Remove the watch: `sparrow SOCKET watch remove REQUEST_ID WATCH_ID REVISION`, with the values `watch list`
   reports.
   Check: `watch list` no longer shows the root.
6. Remove the consume root: `rmdir ~/.local/state/sparrow/intake/NAME` (only while empty).
   Check: `test ! -e ~/.local/state/sparrow/intake/NAME`.
7. Remove both configuration backups: `rm -f ~/.config/sparrow/ship.toml.pre-NAME ~/.config/sparrow/ship.toml.pre-swap-NAME`.
   Check: `ls ~/.config/sparrow/ship.toml.pre-*NAME` finds neither.

The spool directory, the cursor file, and the shipper's ledger and queue stay. The spool files hold records the store
may not yet hold; whether to remove them is a separate decision, made outside this procedure.

#### Moving an existing spool unit to the tracked program

An existing spool unit may run an untracked copy of an earlier program that reads the journal with
`journalctl --cursor-file=CURSOR --follow`. journalctl writes that cursor file when it ends by SIGTERM or SIGINT, and
when its output pipe closes while SIGPIPE is ignored, as a systemd service has it by default. It writes nothing on
SIGKILL. Entries journalctl has printed but the spool has not
appended when it ends this way are lost from the spool. A SIGKILL replays from the last written cursor instead, so
it duplicates entries rather than losing them. The `--help` and man page text say only that the cursor is written at
the end; the signal and closed-pipe cases come from running journalctl, not from its documentation. The stop step
below uses SIGTERM, so it does not depend on the closed-pipe case.

The tracked program writes its cursor after each append, so a stop or crash never skips an entry it has not
appended: the next run resumes after the last appended entry. The two programs store the same journal cursor
string, so the tracked program resumes from the existing cursor file. The move does not recover entries the
untracked copy already lost.

The move changes live state: the running unit, its unit file and the shipper source. Each step needs its own
authorization for the window in which it is run. Placeholders: `EXISTING_UNIT` is the existing unit's name with
`.service`, `UNIT_FILE` its unit file, `EXISTING_DIR` its spool directory, `EXISTING_CURSOR` its cursor file,
`OLD_PATTERN` the file pattern the untracked copy writes, `SOURCE` its shipper source name, `OLD_RULE`,
`OLD_PATTERN_IN_SOURCE` and `OLD_SETTLE` the values step 0 records, and `WINDOW` the quiet period the stop uses.

0. Record the state, read-only: `systemctl --user is-active EXISTING_UNIT`, `ls EXISTING_DIR | tail -n 3`,
   `cat EXISTING_CURSOR`, and the source's current `rule`, `pattern` and `settle_seconds` lines from the shipper configuration. Check:
   the values are recorded in the task record before step 1. Undo: none.

   Stop before step 1 if `pattern` in SOURCE matches files other than `OLD_PATTERN`, or if `EXISTING_DIR` is shared
   with another forwarder. The tracked program writes `unit-spool-*.jsonl`, which the existing pattern does not
   match, so step 5 would stop shipping those other files. That case needs a decision on the shipper configuration
   before step 1, and this move does not make it.

1. Back up the unit file and the cursor file.
   `cp -p UNIT_FILE UNIT_FILE.pre-move` and `cp -p EXISTING_CURSOR EXISTING_CURSOR.pre-move`
   Check: `cmp` of each pair prints nothing.
   Undo: `rm -f UNIT_FILE.pre-move EXISTING_CURSOR.pre-move`

2. Stop the existing unit during a WINDOW in which it has logged nothing for several minutes.
   `systemctl --user stop EXISTING_UNIT`
   Check: `systemctl --user is-active EXISTING_UNIT` prints `inactive`.
   Undo: `systemctl --user start EXISTING_UNIT`
   The quiet period keeps the set of printed-but-unappended entries empty in practice, which is the loss above. The
   journal keeps unread entries only within its retention limits.

3. Replace the unit file with the template's content for this unit: the tracked program on the same spool directory
   and cursor file, with `SPARROW_UNIT_SPOOL_OWN_LINES=keep` and `IgnoreSIGPIPE=no`, as the template sets them.
   Write the candidate beside the unit file, move it into place, and reload the user manager so that it reads the new
   definition.
   `mv -f CANDIDATE UNIT_FILE && systemctl --user daemon-reload`
   Check: `systemd-analyze --user verify UNIT_FILE` exits 0, and `systemctl --user cat EXISTING_UNIT` shows
   `sparrow-unit-spool` in `ExecStart` and `IgnoreSIGPIPE=no`.
   Undo: `mv -f UNIT_FILE.pre-move UNIT_FILE && systemctl --user daemon-reload`

4. Start the unit with the tracked program.
   `systemctl --user start EXISTING_UNIT`
   Check: `systemctl --user is-active EXISTING_UNIT` prints `active`, and the spool directory holds files named
   `unit-spool-*.jsonl` once the unit's next entry is written.
   Undo: `systemctl --user stop EXISTING_UNIT`

5. Let the old files finish shipping, then move the source to the new file pattern. Two candidate writes, each
   changing the `[[source]]` entry's lines named below, each candidate validated by the loader command of the install's
   step 5 and renamed into place.
   a. Set `rule = "settled"` for SOURCE, unless step 0 recorded it as settled already. The old files are no longer
      appended to, so a settled rule ships the newest one; a daily rule would hold it indefinitely. Check: after a
      shipper run that reports no further queued files for SOURCE and neither a per-run file limit nor a full queue, every
      name matching `OLD_PATTERN` in `EXISTING_DIR` is in the ledger, by its path: `ls EXISTING_DIR/OLD_PATTERN | xargs -n1 basename` against
      `cut -f2 ~/.local/state/sparrow/ship/SOURCE.ledger`.
      Undo: write the candidate with the rule step 0 recorded, `OLD_RULE`.
   b. Set `pattern = "unit-spool-*.jsonl"`, `rule = "settled"` and `settle_seconds = 900` for SOURCE, three lines in one
      candidate. Check: `deploy/tests/spool-settle.sh CANDIDATE` prints PASS, and after a shipper run more than 900
      seconds after the first `unit-spool-*.jsonl` file's last write, the ledger names that file. The run's
      `SOURCE: queued N` line alone does not prove this, because the shipper prints it with N = 0.
      Undo: write one candidate with the pattern step 0 recorded, `OLD_PATTERN_IN_SOURCE`, the rule `OLD_RULE` and
      `settle_seconds` `OLD_SETTLE`.

Each undo changes the recorded lines by a candidate (step 5b changes three lines together, and its undo does the same),
never by restoring a saved whole file, so a later change to the configuration survives.

Until step 5b is complete, the new files are not shipped; the old files are shipped by the old pattern. A file
under both names is never shipped twice, because each name is matched by one pattern only.

### Deleting acknowledged spool files

A source with `delete_acknowledged = true` is a spool the shipper owns, such as
a spool of another service's journal lines; never set it on a producer's own directory.
At the end of each run the shipper deletes a finished file of that source once
the store holds every byte of it, and nothing else ever deletes one. It hashes
each part the file ships as (the whole file, or each line-bounded part, split
exactly as when it was queued) with `b3sum` and asks the store
`sparrow SOCKET payload STORE_ID blake3:HEX`. The file goes only when every part
lists an artifact, which the store answers only while it holds all of that
artifact's bytes ([Payload id index](interfaces.md#payload-id-index-store-format)).
The shipper also accepts a matching whole-file artifact for files an earlier shipper
sent whole because it excluded the unterminated last record from the count.

For a whole file above the discovered import limit, the reaper finds its
completed claim by consume root and relative path. The claim must name a durable
ordered manifest in its source. The verifier compares the original against each
held member's digest in manifest order and requires the lengths to cover the
entire file. The shipper saves that manifest identity before setting the file
aside, and verifies the manifest and current bytes again on the next run.
If the saved receipt is missing, the shipper restores the original for a new
two-run confirmation. Use a CLI and daemon that support exact claim lookup and input matching.

An unacknowledged file stays in the spool. The ledger keeps its entry. If a
completed claim expires before the reaper captures its manifest identity,
re-offer a copy under its original intake name. For a file below a producer
subdirectory, the intake name replaces each path separator with `__`. Prepare
the copy outside the consume root on the same filesystem and rename it into
place once the target path is free. Leave the original and ledger in place
while the watch processes the fresh copy.

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
`max_chunk_lines`. A change can make the reaper hash a different partition from
the one originally queued. The ledger still skips the unchanged original.

For a quarantined spool file, find the retired claim with `watch list` and
read its `.refusal` sidecar. Correct the source format or chunk limits as
needed. Prepare parts that match the corrected shipper settings outside the
watch root on the same filesystem. Rename the completed parts into the
matching consume root under fresh names. Keep the original and its ledger
entry. Verify each part through the payload lookup. Until all current parts
are held, the shipper reports the original as `not yet acknowledged` on each
run. Once they are held, the two-run deletion applies.

## Forwarding the systemd journals

`sparrow-journal` runs one forwarder per journal instance. Install the binary, the
configuration and the user unit, then enable both instances:

```sh
install -D -m 0755 target/release/sparrow-journal ~/.local/libexec/sparrow/sparrow-journal
install -D -m 0600 journal.toml ~/.config/sparrow/journal.toml
install -D -m 0644 deploy/systemd/sparrow-journal@.service ~/.config/systemd/user/sparrow-journal@.service
systemctl --user daemon-reload
systemctl --user enable --now sparrow-journal@system.service sparrow-journal@user.service
```

The configuration names the private Sparrow socket and the state directory:

```toml
socket = "/var/lib/sparrow/store/rpc.sock"
state_dir = "/home/example/.local/state/sparrow/journal"
```

The invoking account reads each journal it follows. `journalctl --system` must work for
the account without `sudo`, or the `system` instance cannot start.

Read the stats line with `journalctl --user -u sparrow-journal@system.service`. Its
`held` field is `null` when delivery is running. The cursor file in each instance's
state directory names the last entry the daemon confirmed. The forwarder restarts on
failure after ten seconds, and it keeps spooling while the daemon is unreachable.

## Archive and backups

Sparrow backs up nothing except the age-out to GTS files on a network share: the archive worker
(docs/interfaces.md, "Archive-before-delete gate") writes each aged-out segment under `[archive] dir`, for example
`/mnt/archive/sparrow-archive`, before the store retires it. The worker is implemented
and is off unless the configuration enables it; see [Archive status](archive-format.md#archive-status). Sparrow takes no online store
backup, so the configuration keys `backup_root` and `backup_keep` do not exist. The server file refuses unknown keys, so a `server.toml` that still
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
directory inside the share (as `/mnt/archive/sparrow-archive` is), never to the mount
point itself: an unmounted share leaves no such subdirectory, so the root is missing and refused.

Enable the archive only after the share is mounted. Create the directory inside
the mount with read and write access for the `sparrow` service account. In a
systemd service drop-in, add `ReadWritePaths=-/mnt/archive/sparrow-archive`
with your configured path. The leading hyphen lets the daemon start when the
mount is absent. Grant the same directory `rw` and its contents `rwk` in
AppArmor, then reload systemd and the profile. Check `archive_worker` in
status after enabling `[archive]`, and verify a committed segment with
`sparrow-archive verify`. [Offline commands](archive-format.md#10-readers)

The worker writes `STORE_ID` itself. On its first pass, a directory that is on its mount and
empty (no entry at all, apart from a `STORE_ID.partial` left by an interrupted first pass) gets a
`STORE_ID` holding this store's id and a newline, written durably (temp file, fsync, rename,
directory fsync). An existing `STORE_ID` naming this store is accepted as is; one naming another
store is refused and never overwritten. A directory that holds any other entry but no `STORE_ID`,
such as segments whose id file was lost, is refused and not adopted: write the id by hand only
once you have confirmed the directory is this store's archive.

If a filesystem operation exceeds `io_timeout_secs`, the archive worker
reports `ARCHIVE_OVERRAN` with the step and path and leaves the watermark where
it was. The filesystem worker keeps its single slot until the call returns.
Shutdown can release the store while that call remains blocked. A later pass
verifies any segment files the call left before advancing the watermark.
[Archive write protocol](archive-format.md#8-write-protocol)

## Part sets and older builds

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
claim yet; until then the claim stays
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
   naming the state. No shipped command clears it yet.

## Publish hold

A watch definition carries `publish_hold`, false unless an administrator sets it with a
watch put. A held finished-file watch still detects and claims files: each claim is
journaled and moved into the root's `.sparrow-inflight/` as an interrupted claim is,
and then it waits. Nothing is published, dropped or cancelled, and the wait has no
deadline. Clearing the hold with another watch put (a put that omits the field clears
it) lets the claims publish in pending order. A tail watch cannot be held.

The hold exists for an administrator's deliberate pause of one watch. It is not a way to refuse input: held files are still taken
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
