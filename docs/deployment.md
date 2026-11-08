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

`max_concurrent_requests` admits simultaneous storage work, including intake
workers (1..64). `max_rpc_connections` independently admits Unix and TCP RPC
streams from one shared ceiling (1..256, default 16). An idle agent capability
holds a transport lease without consuming a storage permit. The existing
60-second connection lease still applies. Peers beyond the connection ceiling
are disconnected before capability admission; storage saturation returns the
existing typed operation refusal. These are separate finite resource budgets.

Sparrow has no configurable per-artifact byte limit. A large artifact ingests
through the staged import within `disk_budget` and `min_free_bytes`, up to one
fixed bound: an import above 8515420160 bytes (about 7.93 GiB) is refused before
upload. That is the largest artifact whose catalog row one expiry or eviction
pass can read, and it is below what one atomic publication can carry. The v2
store format (#5) removes the bound. A single document above 128 MiB is stored
as consecutive raw records, so within the bound expiry and eviction retire it in
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
floor it stops accepting Unix and TCP connections, including syslog TCP
connections. It claims no consumed files and runs only WAL synchronization.
Syslog UDP packets that still arrive are refused and counted in memory, and the
listener status reports that those counts are not durable. The daemon logs one
line when it pauses and one when it resumes. At startup it waits for the floor
before opening the store, so a restart on a full disk does not use up the
unit's bounded restarts. If a write
still fails with durability uncertain, for example because another program
filled the disk between checks, the daemon exits with an error and
`Restart=on-failure` reopens the store.

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
producer connection is dropped unanswered. A connection whose credentials the
kernel cannot report is refused and logged. `status` lists the socket's path,
group, ceiling, active connections and refused connections. An import is bound
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
store-format upgrade. A closed-store backup is a recovery artifact, and its
existence does not prove restoration has been exercised.

## Recovery qualification

For isolated recovery qualification, copy a closed-store backup to a separate
directory and copy the deployment configuration with `data_dir` and
`socket_path` changed to that directory. Keep the backup immutable. The recovery
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
evidence when qualifying a restore. Opening a store can perform required format
upgrades; always work on a disposable restored copy. A live store is protected
by the existing single-owner database lock. The example never activates its
saved source paths, even when the copied watch catalog names original roots.
To check a whole backup rather than selected evidence, use
`deploy/verify-backup.sh` (`deploy/RECOVERY.md`, "Verify a backup").

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
these limits ingest slows instead. `LimitNOFILE=65536` covers RocksDB, which
keeps every table file open. Journal output is available through
`journalctl -u sparrow.service`.

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
owner-to-graph index build and the receipt-time fallback backfill in the
background (see "Online store migrations" in the interfaces). `status` reports
them under `migrations` with `done`, `total`, `watermark` and any `failure`, and
the journal logs a progress line every 64 pages and a completion line per
migration. A restart resumes after the last committed page.

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

Each file is staged completely and checked against the original's size and
identity before any part of it is queued. Its copies or chunks then enter the
queue together, the queue directory is synchronized, and a per-source ledger
under `state_dir` records the shipped identity (device, inode, size,
modification time). A crash between the two queues the file again. Each run
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

## Closed-store backups

Sparrow has no online checkpoint, so a consistent backup needs a closed store.
`deploy/libexec/sparrow-backup` first copies, while the service still runs,
what the daemon does not change: the image pair, the lexicon bundle, the server
configuration and any extra paths. It then waits for the intake to drain, stops
`sparrow.service` through its SIGINT drain, copies `data_dir` and the consume
intake root, and starts the service again. It refuses to run when the
service is not already active, so it never starts a service an operator
stopped. Once it has stopped the service it restarts it after a failure or a
SIGTERM, and the unit's `ExecStopPost` restarts it if the script is killed
outright, guided by a marker in the runtime directory. On btrfs the copy is
a reflink: it takes seconds at any store size and later writes never change
it. The copy is synchronized after the
service answers again, so the sync is outside the closed window. Only then is
the backup renamed into place and the oldest beyond `KEEP` removed. The script refuses to
run unless free space covers twice the store plus 10 GiB, in case extents are
not shared. Each backup lists its sources in `SOURCE`, with the image the link names and
the store's `live_records` before the stop and the `unique_payload_chunks` the
stopped daemon logged in its closing line, taken only when the daemon exited 0.
The script reads the journal of the stopped invocation with
`journalctl --invocation=`, which needs systemd 257 or newer. An image that
predates the closing line (schema 66 and older) is recognised before the stop,
from the line's text missing in the image. Its backup is still published, and
records `payload_chunks=unrecorded (image predates the closing count)`. For an
image that prints the line, a missing line or a nonzero exit fails the backup. It
refuses when that image's schema number differs from the running daemon's
`schema_version`, because then the link does not name the running image. The journal records the measured
downtime and each phase's time: the drain wait, the copy before the stop, the
stop, the closed copy, the restart until ready, and the sync. A lock prevents overlapping runs.

`deploy/install.sh` installs it with `sparrow-backup.service`, a system unit
that runs it as root, because it stops and starts the system unit and copies
trees of several owners with their owners preserved; its writes are confined to
the backup root.

An unfinished consume claim binds the inode of its intake root and claimed
file, which no restored copy can reproduce, so a backup must contain no
unfinished claims. Before stopping the service the script lets consumption
drain: it holds the shipper's lock, so no new files arrive, and waits until
every intake root is empty and every watch's `pending_claims` is zero. It never
moves files under the running daemon, because a reserved claim binds the file's
identity, including its change time. If the roots have not drained within 30
minutes the run fails without stopping the service; a file that never drains is
itself a fault to investigate. The shipper's global `max_pending_bytes` bounds
the wait: with the example's 16 MiB budget and 1 MiB chunks, the roots hold at
most 16 MiB, which drains in about eight minutes at the measured 30 seconds per
MiB. A whole file larger than the budget is fed only when every consume root is
empty, and then alone, so the wait then grows with that file.

The unit keeps three daily copies under `/SPARROW/backups/`,
taken between 04:00 and 04:15 local time. It also copies the shipper ledgers and
watch definitions and holds the shipper's lock throughout, so the store, the
files awaiting ingest and the ledgers all describe the same moment. Reflink copies share unchanged extents with the live
store, but compaction rewrites files, so the copies gradually stop sharing.
Budget up to three times the store size. A copy on the same disk protects
against software faults and operator error, not disk loss.

The NAS archive (`/net/synology/archives/sparrow`) is the off-host backup
once its worker is deployed (Sparrow issues #15 to #17); the unit does not
grant its path until then. To restore, stop `sparrow-backup.timer`, `sparrow-ship.timer` and
`sparrow.service`. Move the damaged `data_dir`, the intake root directory and
the shipper's `state_dir` aside rather than deleting them. Copy `BACKUP/store`
to `data_dir`, `BACKUP/intake` to the intake root and `BACKUP/config/ship` to
`state_dir`, each with `cp -a --reflink=auto`, and restore any needed
configuration from `BACKUP/config`. Then start the service and both timers. The
three must come from the same backup, so that the files awaiting ingest match
the ledgers. The shipper then resends every file shipped after the backup whose
original still exists. Records that producers sent directly after the backup
are lost unless they resend them. This whole-service procedure has not been
exercised; the drill covers only reading a restored store. `deploy/RECOVERY.md`,
"Restore from a backup", gives the commands. Before replacing a live store,
verify the backup with `deploy/verify-backup.sh BACKUP` (`deploy/RECOVERY.md`,
"Verify a backup"). It opens a reflinked copy with `sparrowd --query-only`,
which opens no watch root and refuses every write, and checks the store
identity, status, the stored payload chunk count, and, within the retention
window, a records window and one artifact's bytes. A backup counts as verified only after the script passes on it.

### Measure the online migration

`deploy/verify-backup.sh --measure-migration [--image DAEMON] BACKUP` measures
the online migrations, the schema 69 pair and `source-summary` ("Online store
migrations" and "Per-source counters and artifact index" in the interfaces)
on a writable reflinked copy of the backup's store below `SPARROW_VERIFY`
(default `/SPARROW/verify`). The switch gate is a first open under 10 minutes
measured this way. The copy has its own socket, and its scratch configuration
takes the backup's budgets and lexicons as the verify mode's does, with no
ingest socket, TCP listener, syslog listener, watch or archive. The image opens
it with `sparrowd --no-intake`: the maintenance worker runs the migration pages,
and, as in query-only mode, the stored watch catalog is reported but no root is
opened, so the live intake folders it names are never consumed. An image
without `--no-intake` in its usage line is refused. The daemon runs under
`systemd-run --user --scope` with `CPUQuota=100%`, `IOWeight=20`,
`MemoryHigh=2G` and `MemoryMax=4G`, the limits of the service unit.

The disk guard reads the free space of the filesystem holding
`SPARROW_VERIFY`. The run refuses to start under 50 GB free, before anything
is copied, and aborts under 40 GB, stopping the daemon and removing the copy.
A `df` that cannot be read ends the run with its own message.
`SPARROW_MEASURE_MIN_FREE_GB` and `SPARROW_MEASURE_ABORT_FREE_GB` may raise
either floor, never lower it. On the first status answer the script reads the
scope's limits back from `systemctl --user show` and fails unless they are the
ones requested. The run polls status every 15 seconds until every entry in
`migrations` is complete with no failure, then stops the daemon with SIGINT and
removes the copy.

The run fails, removing the copy, with one `verify-backup: FAIL CHECK:` line
on standard error. These are every failure of the mode:

- `FAIL open`: the daemon exited. A `--no-intake` daemon also exits when its
  maintenance worker ends.
- `FAIL store-id`: the copy's store identity is not the one SOURCE records.
- `FAIL no-intake`: status or the startup line does not report no-intake
  mode, or a stored watch root is open.
- `FAIL scope`: `systemctl --user show` cannot read the scope, or its limits
  are not the ones requested.
- `FAIL durability`: status reports a `durability.failure`.
- `FAIL status`: status failed to answer three times running after its first
  answer, or it reports no readable migrations.
- `FAIL migrate`: a migration reports a failure; the line carries its text.
- `FAIL stall`: the migrations' done/total has not moved for 600 seconds
  after the first status. A failing page already ends the run as
  `FAIL migrate`, so this catches stops that set no failure, such as the
  daemon pausing writes below its own free-space floor. The worker retries a
  failed page every 60 seconds. The live step-1 open averaged about 0.9 s per
  64-graph page (34 minutes over 2,360 pages, an average not measured page by
  page), so a healthy backfill moves at every poll. Ten retry intervals
  without movement mean the migration has stopped, and 600 seconds is also
  the switch gate's whole 10-minute budget.
- `FAIL disk`: free space fell under the abort floor, or `df` could not read
  it during the run.
- `FAIL shutdown`: the daemon exited non-zero after SIGINT, or did not log its
  closing count.

There is no ceiling before the first status answer, because the length of the
first open is the measured quantity. Interrupt the script to stop early; the
copy is still removed. However the run ends, the daemon's own migration lines
(`sparrowd: migration ...` progress and completion, and
`sparrowd: online migration failed: ...`) are printed prefixed with
`verify-backup: daemon:` before the copy and its log are removed.

Each measurement is one `verify-backup: MEASURE` line on standard output:

| Line | Meaning |
| --- | --- |
| `image=PATH sha256=HEX` | The daemon image and its digest |
| `start_utc=TIME` | Clock time when the daemon was started |
| `first_status_seconds=S` | Seconds from start to the first status answer |
| `progress elapsed_seconds=S NAME=DONE/TOTAL ...` | Each migration's progress, every minute |
| `free_space phase=start\|minute\|end path=P bytes=N gb=N` | Free space at start, every minute and at the end |
| `scope=UNIT CPUQuotaPerSecUSec=1s IOWeight=20 MemoryHigh=N MemoryMax=N` | The scope's limits as systemd applied them |
| `backfill_seconds=S` | Seconds from start until every migration was complete |
| `end_utc=TIME` | Clock time when every migration was complete |
| `live_records=N` | `live_records` from the final status |

This deployment does not establish whole-corpus throughput, physical reclamation,
restoration or full architecture acceptance. Current installed image and live
acceptance evidence are recorded in [Development runtime](development-runtime.md).

## Switching the user unit's image

Until Sparrow issue #22 moves the live store to the system unit, it runs under
the operator's old user unit, `sparrow.service` under `systemctl --user`. The
system unit's scripts refuse that unit. `deploy/user-unit-upgrade.sh` switches
it from one kept image pair to another and takes the named pre-upgrade backup
in the stopped window:

```bash
deploy/user-unit-upgrade.sh --from sparrowd-schema68-2aa56a9 --to sparrowd-schema69-3a64e08
```

Both arguments name daemon images under `~/.local/libexec/sparrow`. Each CLI
image is the daemon's name with `sparrowd-` replaced by `sparrow-`, and the
schema numbers come from the names. Run it as the operator, only on the
coordination session's go. `SPARROW_PREFIX` (default `~/.local`),
`SPARROW_CONFIG` (default `~/.config/sparrow/server.toml`, which names the store
and its sockets), `SPARROW_BACKUPS` and `SPARROW_BIN` override the layout;
`--help` lists them.

Before anything changes it refuses unless all of these hold:

- `pre-upgrade-FROM` is absent from the backup directory.
- The daemon link names `--from`.
- All four images are executable.
- The `--to` pair matches `./bin` byte for byte.
- The `--to` daemon carries `cap_net_bind_service`.
- `server.toml` sets the ingest keys.
- The store has copy-on-write off.
- The backup directory is on the store's filesystem, which the reflink copy needs.
- The store's filesystem has the store's size plus 10 GiB free.
- No nightly backup is running.
- The running daemon reports the `--from` schema.

The capability is a prerequisite done by a human with root,
`sudo setcap cap_net_bind_service=ep ~/.local/libexec/sparrow/IMAGE`; the
script checks it with `getcap` and never runs `setcap`.

It then stops both timers and waits for the shipper, stops the unit and checks
that no `sparrowd` remains. Next it copies the closed store by reflink into a
no-copy-on-write `pre-upgrade-FROM` and compares the file lists. The backup
also keeps the old image pair, `server.toml`, `ship.toml` and a `SOURCE` file.
Last, it switches the links and starts the unit.

It waits up to 10 minutes, the switch gate, for the first status answer. The
measured first open of schema 69 on a copy of the live store under the unit's
limits answered in 117.2 seconds (`verify-backup.sh --measure-migration`,
2026-11-07). It does not wait for pressure to report `ready`: that phase tracks
disk-budget maintenance, not the migrations. After the first answer it fails
unless the store identity is unchanged and the schema is the `--to` schema. It
prints each migration's `done`, `total` and `failure`, the watches and the
ingest socket, starts the timers, and exits 0. The backfill continues online
in the running daemon. Watch it with `sparrow /SPARROW/store/rpc.sock status`
(`migrations`) and with `journalctl --user -u sparrow.service`, which logs a
`sparrowd: migration` progress line every 64 pages and a completion line per
migration.

`--help` gives the recovery for each failure point. A failure after the start
leaves the `--to` daemon running with both timers stopped, and it may already
have accepted ingest. The recovery goes forward first: wait for status, confirm
the store identity and schema, then start the timers. A rollback restores the
pre-upgrade backup and drops everything `--to` accepted.
