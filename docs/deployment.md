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
store-format upgrade. An online backup is a recovery artifact, and its
existence does not prove restoration has been exercised.

## Recovery qualification

For isolated recovery qualification, restore a backup into a separate empty
directory with `sparrowd --restore-backup` ("Online backups") and copy the deployment configuration with `data_dir` and
`socket_path` changed to that directory. Keep the backup immutable: the restore reads it and never changes it. The recovery
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
`data_dir`, `socket_path`, `ingest_socket_path`, `backup_root` or `archive.dir`, naming
the watch and both paths. It applies the rule to the `consume` seeds in `server.toml`
when it loads them, to the stored catalog at startup, and to every `watch put`. The
check is lexical. `server.toml` names no log file, so the daemon cannot check where its
standard error is redirected; keep any such file outside every watch root.

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
`sparrow-ship` keys a file by device, inode, size and modification time and
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

## Online backups

Backups go to the NAS archive `/net/synology/archives/sparrow` (an NFSv4 mount
of `192.168.1.1:/volume1/archives`) and nowhere else (Patrick, 2026-11-08:
"Sparrow and katamaria are FORBIDDEN from backing up to local disks"). They are
online. A backup never stops, pauses or drains the service, never takes a lock
the daemon's writers or the shipper use, and never changes what ingestion does
(Patrick, 2026-11-08: "Backup may not: stop the server, pause the server, or in
any way perturb the ingestion."). There is no closed-store window.

The daemon takes the backup of its own store with RocksDB's backup engine
(docs/interfaces.md, "Online backup"). The operator or the nightly unit asks for
it with `sparrow SOCKET backup`, which returns at once, and `sparrow SOCKET
status` reports a `backup` object with the phase (`idle`, `running`, `complete`
or `failed`), start and finish times, backup id, file and byte counts and any
failure. The copy runs on one thread at nice 19 (and the lowest best-effort I/O
priority, which the BFQ scheduler honors), and
does not take the writer lock or flush the memtable. It copies the live table,
manifest and write-ahead log files into `BACKUP_ROOT/engine`. Table files are
shared between backups, so the first backup copies the whole store over the
network and each later one only the tables written since. The engine keeps the
newest `backup_keep` (default 3). While a backup runs, RocksDB keeps the files that
become obsolete until the copy ends. The held files do not count toward `physical_bytes`, the disk budget or eviction, so a backup never refuses a write or evicts evidence. A guard thread at the daemon's normal priority, which never touches the backup root, measures free space every second; once it falls below twice `min_free_bytes`, the guard re-enables file deletions, RocksDB deletes the held files at once, and the backup fails if it still needed one. A backup stalled on an unresponsive NAS therefore never brings ingest to the free-space floor. One backup runs at a
time; a second request is refused with `CONFLICT`. A daemon shutdown during a
backup abandons it, and the engine discards the partial files at its next open.
`BACKUP_ROOT/STORE_ID` binds the root to one store, and another store is
refused.

Configure it in `server.toml`:

```toml
backup_root = "/net/synology/archives/sparrow"
backup_keep = 3
```

Startup refuses a relative root, one with `.` or `..`, and one inside or
containing `data_dir`. It does not touch the root, so a daemon that starts while
the NAS is down still serves. Every backup judges the root before it creates
anything: it must be on `nfs`, `nfs4`, `cifs` or `smb3` and not on the store's
filesystem (`/proc/self/mountinfo`, longest mount prefix). A local root makes
the backup fail and creates nothing there. The root must exist, owned by the
daemon's user; create it once on the NAS. `sparrow.service` grants it with
`ReadWritePaths=-/net/synology/archives/sparrow` and the AppArmor profile names
it.

`sparrow-backup.service` runs `deploy/libexec/sparrow-backup SERVER_CONFIG
INTAKE [EXTRA...]` daily between 04:00 and 04:15 local time:

```text
/usr/local/libexec/sparrow/sparrow-backup /etc/sparrow/server.toml /SPARROW/intake /etc/sparrow /SPARROW/ship
```

The unit has `RequiresMountsFor` the NAS root, `Nice=19` and
`IOSchedulingClass=idle`, writes only to the NAS root, runs as root to read the
trees it copies, and has no `ExecStopPost`, no `flock` and no stop or start of
`sparrow.service`. It never stops or signals anything. The client:

1. Refuses a backup root that is local before it asks the daemon, and holds
   `BACKUP_ROOT/.lock`, the lock the image-switch scripts take, never the
   shipper's.
2. Asks the daemon for a backup and polls status every 10 seconds until it
   completes or fails.
3. Copies the image pair that runs the store (`images/`), the lexicon bundle
   (`lexicons/`), the server configuration and each extra path (`config/NAME`)
   and the intake (`intake/`) into `BACKUP_ROOT/sets/ID.partial`, where `ID` is
   the engine's backup id.
4. Writes `SOURCE` (`backup_id`, `store_id`, `schema_version`, `store_format`,
   `image`, `cli`, `taken_utc`, `finished_utc`, `sets_copied_utc`,
   `engine_files`, `engine_bytes`, `data_dir`, `intake`, `text_lexicon_dir`) and
   `MANIFEST.b3`, the b3sum of every set file and every engine file the backup
   references, with paths relative to the backup root. The hashes are read back
   from the NAS after a sync with cached pages dropped.
5. Renames the set into place and removes the sets whose engine backup the
   engine purged.

`taken_utc` is the instant the daemon started the backup. The set's files are
copied after the store's backup point, so they may be newer than the store, and
files consumed between the two are in neither. That loss window is the same as
for any record accepted after the backup point: the shipper resends a shipped
file whose original still exists, and a record a producer sent directly is lost
unless the producer resends it. The first run copies the whole store, so budget
the NAS for the store plus the changed tables of the later backups and one set
of the files beside it for each kept backup. The unit's `TimeoutStartSec` is six
hours.

The interim user unit `~/.config/systemd/user/sparrow-backup.service` is not in
the repository. It must carry these lines, and `~/.config/sparrow/server.toml`
must set `backup_root` and `backup_keep`:

```ini
[Unit]
RequiresMountsFor=/net/synology/archives/sparrow

[Service]
Environment=PATH=%h/.local/bin:/usr/bin
ExecStart=%h/.local/libexec/sparrow/sparrow-backup %h/.config/sparrow/server.toml /SPARROW/intake %h/.config/sparrow /SPARROW/ship
TimeoutStartSec=6h
```

`PATH` gives the client the `sparrow` CLI link. The unit has no `ExecStopPost`
and no `flock`. An image without the online backup refuses to start with
`backup_root` or `backup_keep` in its configuration, so add them immediately
before the switch to an image that has it (`deploy/user-unit-upgrade.sh`
refuses without them) and remove them before any rollback to an older image. Install the repository's `deploy/libexec/sparrow-backup` as
`~/.local/libexec/sparrow/sparrow-backup`, then run
`systemctl --user daemon-reload`.

Verify a backup before relying on it: `deploy/verify-backup.sh
BACKUP_ROOT/sets/ID` (`deploy/RECOVERY.md`, "Verify a backup") checks
`MANIFEST.b3` against the NAS and the set's file list, restores engine backup
`ID` with the image `SOURCE` records into a scratch copy below `SPARROW_VERIFY`
(removed however the run ends), opens it with `sparrowd --query-only`, and
checks the store identity, status, within the retention window a records window
and one artifact's bytes. A backup counts as verified only after the script
exits 0.

To restore, run `sparrowd --restore-backup BACKUP_ROOT [BACKUP_ID] EMPTY_DIR`
with the image `SOURCE` records. It is offline, restores into an empty or absent
directory only (never over a store) and checks every file's checksum. Then place
the set's configuration, lexicons and intake and start the service with
`deploy/start.sh --restored-from SET`; `deploy/RECOVERY.md`, "Restore from a
backup", gives the commands and what the intake copy can leave behind. This
whole-service procedure has not been exercised; the drill covers only reading a
restored store.

An unfinished consume claim binds the inode of its intake root and claimed
file, which no restored copy can reproduce, so a set's intake can hold a private
claim entry the restored store does not journal, and a restored store can
journal a claim whose private source is missing. A claim whose private source is
missing never completes: status shows it in `pending_claims`, and `sparrow
SOCKET watch claims` lists it as pending with the reason "unpublished claimed
source is missing", durably across restarts (schema 71 and newer). The watch
failure text names it too, but another claim's retry can replace that text.
Once its bytes are confirmed lost, retire it with `sparrow SOCKET watch
retire-claim WATCH_ID CLAIM_ID REASON` (schema 70 and newer; interfaces,
"Retiring an unrecoverable claim"). It leaves `pending_claims` and stays
listed as `retired-unrecoverable`. If its staged import had begun, the next
reclaim pass frees that import's staged chunks and slot. The watch failure
clears only if that claim caused it. A claim whose private source still exists
or whose publication is recorded is refused; reconcile the private entry first,
keeping its bytes.

### Measure the online migration

`deploy/verify-backup.sh --measure-migration [--image DAEMON] BACKUP` measures
the online migrations, the schema 69 pair, `source-summary` and, when the open
found the source timeline cache stale, `source-context` ("Online store
migrations", "Per-source counters and artifact index" and "Single context
capture admission" in the interfaces)
on a writable scratch copy restored from the backup below `SPARROW_VERIFY`
(default `/SPARROW/verify`). The switch gate is a first open under 10 minutes
measured this way. The copy has its own socket, and its scratch configuration
takes the backup's budgets and lexicons as the verify mode's does, with no
ingest socket, TCP listener, syslog listener, watch or archive. The image opens
it with `sparrowd --no-intake`: the maintenance worker runs the migration pages,
and, as in query-only mode, the stored watch catalog is reported but no root is
opened, so the live intake folders it names are never consumed. An image
without `--no-intake` in its usage line is refused. The daemon runs under
`systemd-run --user --scope` with `CPUQuota=100%`, `IOWeight=20`,
`MemoryHigh=2G` and `MemoryMax=4G`, the limits of the service unit, and at the
nice level of the user unit that serves the live store. The script reads that
level with `systemctl --user show` from `sparrow.service`, or from the unit
`SPARROW_MEASURE_UNIT` names, and refuses to run when the unit is not loaded or
reports no nice level: a copy measured at nice 0 does not predict a unit that
runs at `Nice=10`. The unit must also run under the default time-sharing
policy (`CPUSchedulingPolicy=0`), and the daemon is started through
`chrt --other 0`, so a caller under `SCHED_IDLE` does not hand that policy to
the scope. The run fails `FAIL scope` unless the daemon reports class `TS`.

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
  failed page every 60 seconds. A healthy migration page commits well inside a
  poll, so a healthy migration moves at every poll. Ten retry intervals
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
| `scope=UNIT CPUQuotaPerSecUSec=1s IOWeight=20 MemoryHigh=N MemoryMax=N Nice=N Policy=TS` | The scope's limits as systemd applied them, and the daemon's nice level and scheduling class |
| `backfill_seconds=S` | Seconds from start until every migration was complete |
| `end_utc=TIME` | Clock time when every migration was complete |
| `live_records=N counts=exact\|pending` | `live_records` and `counts` from the final status |

### Measure the online migration with intake and traffic

The schema 69 gate measured its backfill with no intake and no queries: 2,614
pages in 7,914 s, about 3.0 s per page. Live on 2026-11-07, with intake,
watches and queries, the same backfill ran at more than 16 s per page, status,
records and sparql were cut off at 60 s, and katamari publications expired.
`deploy/verify-backup.sh --measure-migration --with-traffic --image DAEMON
BACKUP` measures the backfill the way the live store runs it. `BACKUP` is an
online backup set, `BACKUP_ROOT/sets/ID`. The measured copy and the query-only
prep copy are each restored from it with the image's `--restore-backup`, so
the scratch filesystem must keep `SPARROW_VERIFY_MARGIN_GB` (default and minimum 40 GB) free after both copies of the set's `engine_bytes`. The
daemon runs under the same user scope, limits and nice level as above.

- The image opens the copy with `sparrowd --private-intake DIR`. That is a
  writable daemon with its watches and its own ingest socket, for a disposable
  copy only. Every watch it opens or accepts is rebound to `DIR/NAME`, where
  `NAME` is the final component of its stored root. The script creates each
  root under the scratch directory, and the shipper's `intake_root/NAME`
  layout feeds the same roots. The daemon refuses a `DIR` that is, holds or
  lies under a watch's configured root, so a hand run on a live intake
  directory is refused, and its catalog keeps naming the configured roots: a
  watch mutation on the copy persists them, never the private ones. The mode
  refuses `consume`, `syslog`, `rpc_tcp_address`, `archive` and `backup_root`
  in its configuration (a copy never backs up); the ingest socket is
  allowed and is the copy's own. Claims the copy journals for its original
  roots never refuse the open and are never recovered: each recovery refuses a
  claim bound to another root, and the watch reports it as its failure. The
  startup line reports `query_only=false intake=private`. On the first status
  answer the run fails (`FAIL private-intake`) unless every watch root is
  under `DIR` and the ingest socket is the copy's. An image without
  `--private-intake` in its usage line is refused before anything is restored.
- Replay source: what the copy itself received in the hour before the
  backup's `taken_utc`, so the replay needs no producer spool, reads no
  producer directory, and covers every shipper rule. A `daily` file whose
  modification time is its last append, after the window, and a `sealed`
  segment whose manifest decides when it ships, are replayed as the store
  received them, not by their file times. Every artifact received in the
  window is read once, before the run, from a separate query-only prep copy,
  a second restore of the set, so the measured open is still the copy's
  first. An artifact whose source is
  a watch's source replays into that watch's private root, released at its
  recorded receipt offset. When `BACKUP/config/sparrow/ship.toml`, the set's
  copy of `/etc/sparrow/ship.toml`, has a source named
  after the root, as live, the item goes through a scratch producer directory
  and `deploy/libexec/sparrow-ship` ships it into the root with a scratch
  configuration, the live budgets and a scratch state, 300 s after each
  completed run, as `sparrow-ship.timer` does. Each scratch source is
  `settled` with no settle time, because what it replays is what the live
  shipper already found finished. A watch with no shipper source has its item
  placed in its root directly. Any other artifact, a direct producer's, is
  re-sent over the copy's ingest socket at its recorded offset; live it
  arrived over the ingest socket or TCP, and the copy has only the socket.
  Syslog artifacts are skipped, since the mode refuses the listener. Both
  replays loop over the window for the whole backfill, each later cycle under
  new file names and request IDs. Nothing is read from or written to the live
  intake roots, sockets, store, shipper state or producer directories; each
  is a guarded path the scratch directory must not lie under.
- Replay checks, before the measured open: the run fails (`FAIL prep`) when a
  `ship.toml` source names no watch root (no watch root ends in that name),
  when a `ship.toml` source's watch received nothing in the window, or when
  the window holds nothing at all. `SPARROW_MEASURE_REPLAY_SECONDS` (3600 to
  86400) widens the window for a source whose deliveries one hour does not
  hold.
- Traffic: `status`, `records` (received in the last hour, 100 rows) and
  `sparql` (record sources received in the last hour, 100 rows) each start
  every 30 seconds, from the first status answer until the backfill ends. A
  call that outlasts its period skips the ticks it covered. A call still
  running at the end finishes and is counted, and one still running after 600
  seconds is killed and counted failed.

The extra `MEASURE` lines:

| Line | Meaning |
| --- | --- |
| `replay source=NAME via=ship\|direct\|socket artifacts=N bytes=N skipped=N` | Per source, what the window holds to replay: a watch root through the shipper or placed directly, or a socket source; `skipped` has no ingestible origin |
| `replay watch_artifacts=N socket_artifacts=N window_from=T window_before=T` | The saved items in the window |
| `prep_seconds=S` | Seconds spent preparing the replay, before the measured open |
| `backup_live_records=N counts=exact source=backup-image\|prep-query-only` | The backup's own record count, before any replay (below); `backup_live_records=pending counts=pending source=none` when no exact count exists |
| `pages migration=NAME pages=P seconds=S pages_per_second=R` | Each migration, from the previous completion, or the start, to its own; ` shared_poll=A,B shared_pages=P` follows when migrations were first seen complete at the same poll, and the rate is then theirs together |
| `pages overall pages=P seconds=S pages_per_second=R` | Every page over the backfill seconds |
| `replay_added=N since_utc=T` | Records the copy received since the load started, all of them replayed |
| `live_records=N counts=exact\|pending` | The copy's record count at the end, replay included |
| `watch_claims_completed=N` | Claims the copy's watches completed since the first status |
| `watch_claims_refused=N` | Claim reservations the daemon refused (`reservation refused` lines in its log) |
| `watch_intake watch=NAME via=ship\|direct\|none released=N due=N completed=N inflight_allowance_seconds=S` | Per watch: the items released to it, those released before the final status less its in-flight allowance (due), and the claims it completed since the first status; `via=none` is a watch the window gave nothing to replay, so its due count is 0 |
| `rpc kind=KIND calls=N failed=N over_60s=N p50=S p95=S max=S` | Latency of `status`, `records`, `sparql` and the replayed `ingest` calls |
| `replayed via=watch\|socket source=NAME released=N failed=N` | Per source, the items released into a root or the shipper, or sent to the socket |
| `replay watch_items_released=N release_failures=N socket_ingests=N cycles=N ship_runs=N ship_failures=N ship_held_back=N ship_run_max_seconds=S` | What the replay did; `ship_held_back` counts the distinct items a shipper run left queued because the roots were over `max_pending_bytes` |
| `publication_deadline_expiries=N` | Consume publications that hit their 45-second deadline, from the daemon's log |
| `gate name=NAME value=V threshold=T margin=M verdict=PASS\|FAIL` | One line per gate threshold below; a positive margin passes |

Page counts come from the daemon's `sparrowd: migration` lines, timed at the
15-second status poll that first sees them, so a migration's seconds are
accurate to one poll. The backfill ends at the first poll that sees every
migration complete, and `backfill_seconds` keeps its meaning.

`backup_live_records` is what a store of the backup's size holds, to compare
with live's count. It is read before anything is replayed by the image the
backup records (the set's `images/`), query-only on the prep copy: that image
neither migrates nor expires, so its count is the store's as it was taken. A
newer image can upgrade the prep copy and report its counts pending, so the
backup's image opens it first. When that image is missing or also reports
pending, the measured image's answer on the same prep copy, also query-only,
counts if its counts are exact. Otherwise the line says pending: a count taken
while the store counters seed covers only the records below the seeding
watermark, so it is never printed as the backup's. The writable copy never
supplies it, because its maintenance worker may run an expiry pass before its
first answer.
`replay_added` counts, page by page, the records received since the load
started. No record of the backup was received that late, so all of them are
the replay's. The final `live_records` includes them and lacks any backup
record retention expired during the run.

The gate thresholds, which the script enforces:

- `first_status_seconds`: the first status answer within 600 seconds, the
  switch's own gate.
- `pages_per_second`: at least 0.125 migration pages per second over the
  backfill, 8 seconds per page. The floor rejects the failed live backfill
  (over 16 s per page). It is about 38 percent of the schema 69 copy without
  traffic (0.33 pages per second), which leaves the writers, served first,
  their share. At the floor a 2,614-page backfill ends within 5.8 hours,
  inside one night before the 04:00 backup.
- `rpc_calls_over_limit`: no call of 60 seconds or more, the connection lease
  that cut calls off live. Its margin is 60 less the slowest call.
- `rpc_calls_failed`: no failed call. A call that fails fast still means the
  store did not serve it.
- `traffic_answered`: `status`, `records` and `sparql` each answered at least
  once, so a run without measured traffic cannot pass.
- `publication_deadline_expiries`: none.
- `replay_failures`: no item failed to release into the shipper or a root, no
  shipper run failed, and no shipper run left an item queued because the roots
  were over `max_pending_bytes`. This shows the replay delivered its items to
  the roots, not that the watches consumed them; the next gates check that.
- `watch_claims_refused`: no claim reservation refused by the daemon.
- `watch_claims_completed`: at least one claim completed by the copy's
  watches, so a run whose watch intake never reached the store cannot pass.
- `watch_intake:NAME`, one per watch: the claims the watch completed since the
  first status reach the items released to it before the final status less its
  in-flight allowance. The allowance is the watch's `quiet_ms` from the copy's
  status, plus the daemon's 45-second publication deadline (a compiled
  constant, not configuration), plus, for a shipped watch, the ship interval
  and the longest shipper run. A shipped part is within the shipper's split
  limits and a direct item is one file, so one released item is at least one
  claim. A tail watch's finished generations are claims too. A pending claim
  the backup journals is not counted: its source is not in the empty private
  root, so it cannot complete. Its margin is completed less due.
- `socket_ingests_accepted`: at least one replayed socket ingest accepted
  when any socket artifact was saved.

When every gate passes the run exits 0. When the run completes and a gate
fails it prints one `verify-backup: FAIL gate:` line per failed gate on
standard error and exits 4. The failures of the plain mode still exit 1, and
so do `FAIL prep`, when the replay cannot be prepared, `FAIL private-intake`,
and `FAIL load`, when the load helper fails or exits before the backfill
completes (it is checked at every poll). A load worker (one traffic kind, a
replay or the shipper) that raises stops the helper, which exits 1, so the run
never measures without that load. `SPARROW_MEASURE_GATE_FIRST_STATUS_SECONDS`,
`SPARROW_MEASURE_GATE_RPC_SECONDS`,
`SPARROW_MEASURE_GATE_PAGES_PER_SECOND` and
`SPARROW_MEASURE_GATE_INFLIGHT_SECONDS`, a ceiling on each watch's in-flight
allowance, may only tighten their gate.
`SPARROW_MEASURE_REPLAY_SECONDS` may only widen the window, to a day.
`SPARROW_MEASURE_SHIP_SECONDS` may only shorten the shipper interval, and
`SPARROW_MEASURE_MIN_TRAFFIC_SECONDS` (up to 300) keeps the load running that
long after the first status when the backfill ends sooner.

`--rehearse-rollback` adds, after the measurement, the rollback rehearsal on
the measured copy and scratch copies of it, with the measured image in every role and
every watch rebound below the rehearsal's own directory (see "Claims after a
rollback" below). A watch put on the copy feeds record batches through it, and
`max_records` decides what each daemon can publish. A daemon claims a batch it
cannot publish and stops, and a scratch copy of its stopped store stands in for
the store a restore makes from a set taken at that point: the daemon takes
online backups only to a network `backup_root`, so a scratch run cannot take
one. A newer daemon publishes that batch, claims another it cannot publish,
and stops. The restored daemon on the backup must refuse to start with
"unrecognized private claim entry" (`PASS rehearsal-refusal`).
`deploy/reconcile-inflight.sh inflight` must move the unknown entry back to
its root under its batch name with the same inode (`PASS rehearsal-inflight`).
After the restored daemon starts, `deploy/reconcile-inflight.sh missing` must
re-ship the batch the newer store published and retire its claim
(`PASS rehearsal-missing`). Any other outcome is `FAIL rehearsal`.

The measurement target is an online set; a closed-store backup is never one
(coordination, 2026-11-09). Before the D147 switch to online backups, the
target is a set the online path takes from a test store with live intake, as
the measurement's own test fixture builds it. After the switch it is the first
real online set on the NAS. The `--image` image, the `--to` image of the
switch, must be built from a revision with `--private-intake` and
`--restore-backup`:

```bash
deploy/verify-backup.sh --measure-migration --with-traffic --rehearse-rollback \
    --image ~/.local/libexec/sparrow/sparrowd-schema71-REV \
    /net/synology/archives/sparrow/sets/ID
```

This deployment does not establish whole-corpus throughput, physical reclamation,
restoration or full architecture acceptance. Current installed image and live
acceptance evidence are recorded in [Development runtime](development-runtime.md).

## Switching the user unit's image

Until Sparrow issue #22 moves the live store to the system unit, it runs under
the operator's old user unit, `sparrow.service` under `systemctl --user`. The
system unit's scripts refuse that unit. `deploy/user-unit-upgrade.sh` switches
it from one kept image pair to another and takes the named pre-upgrade backup
online, before anything stops:

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
and its sockets), `SPARROW_BACKUPS` and `SPARROW_BIN` override the layout;
`--help` lists them.

Before anything changes it refuses unless all of these hold:

- `pre-upgrade-FROM` is absent from the backup directory.
- The daemon link names `--from`.
- All four images are executable.
- The `--to` pair matches `./bin` byte for byte.
- The `--to` daemon carries `cap_net_bind_service`.
- `server.toml` sets the ingest keys, and its `backup_root` equals
  `SPARROW_BACKUPS` (default `/net/synology/archives/sparrow`): the backup never
  goes to a local disk.
- The store has copy-on-write off.
- No nightly backup is running.
- The running daemon reports the `--from` schema.
- Unless `--no-backup` is given, the running daemon reports a backup state other
  than `unconfigured`.

The capability is a prerequisite done by a human with root,
`sudo setcap cap_net_bind_service=ep ~/.local/libexec/sparrow/IMAGE`; the
script checks it with `getcap` and never runs `setcap`.

With the daemon still serving, it takes the pre-upgrade backup online through
`sparrow-backup` and points `pre-upgrade-FROM` at the new set (a relative link
to `sets/ID`), as `deploy/pre-upgrade-backup.sh` does for the system unit. Only
then does it stop both timers and wait for the shipper, stop the unit, install
the `LimitNOFILE` drop-in, switch the links and start the unit. Nothing stops
for the backup; the unit stops only for the image switch itself.

At the end it starts `sparrow-ship.timer` only. `sparrow-backup.timer` stays
stopped, because it restarts only after a first online backup has been verified
and the installed `sparrow-backup.service` may still be the old closed-store
form. The run prints the steps: take the first online backup, run
`deploy/verify-backup.sh SET`, and only after it exits 0 run
`systemctl --user start sparrow-backup.timer`. If the shipper or the backup is
still running after the timers stop, the script starts again only the timers
that were active before (the backup timer only if it was running) and changes
nothing else.

`--no-backup REASON` switches without a backup, for a running daemon that has no
online backup (an image older than the backup operation, such as schema 68).
`REASON` must not be empty, and the run logs it. Without the option, a daemon
that lacks the backup makes the script refuse and name `--no-backup`. The script
never takes a stopped copy instead, because no backup may stop the server. After
`--no-backup` there is no store to restore the old format from, so a rollback
can only go forward on `--to`. Take the first online backup as soon as status
answers on the new image: `sparrow SOCKET backup`, and watch `status`.

It waits up to 10 minutes, the switch gate, for the first status answer. The
measured first open of schema 69 on a copy of the live store under the unit's
limits answered in 117.2 seconds (`verify-backup.sh --measure-migration`,
2026-11-07). It does not wait for pressure to report `ready`: that phase tracks
disk-budget maintenance, not the migrations. After the first answer it fails
unless the store identity is unchanged and the schema is the `--to` schema. It
prints each migration's `done`, `total` and `failure`, the watches and the
ingest socket, starts `sparrow-ship.timer` (not the backup timer, below), and exits 0. The backfill continues online
in the running daemon. Watch it with `sparrow /SPARROW/store/rpc.sock status`
(`migrations`) and with `journalctl --user -u sparrow.service`, which logs a
`sparrowd: migration` start line per migration, a progress line every 64
pages and a completion line per migration, each with its pages and `done`; the
progress and completion lines carry `elapsed_seconds` since that migration's
first page, so the completion line gives each migration's own duration (see
"Online store migrations" in the interfaces).

`--help` gives the recovery for each failure point. A failure after the start
leaves the `--to` daemon running with both timers stopped, and it may already
have accepted ingest. The recovery goes forward first: wait for status, confirm
the store identity and schema, then start `sparrow-ship.timer`; start
`sparrow-backup.timer` only after a first online backup verifies. A rollback with a
pre-upgrade backup stops the unit, points the links at `--from`, and restores
into an empty `chattr +C` directory with an image that has the restore option:
`TO --restore-backup BACKUPS ID STORE`, where `ID` is `backup_id` in
`BACKUPS/pre-upgrade-FROM/SOURCE`. It drops everything accepted after the backup
started. After `--no-backup` there is none: go forward. A rollback must then
reconcile the consume claims, as below.

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

The hold exists for two uses: the rollback rehearsal in `deploy/verify-backup.sh`,
which needs a claim in flight when the daemon halts, and an operator's deliberate
pause of one watch. It is not a way to refuse input: held files are still taken
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

### Claims after a rollback

The schema 69 to 68 rollback of 2026-11-07 found two kinds of claim the
restored store cannot settle by itself:

1. The newer daemon claimed a file after the backup and moved it into its
   root's `.sparrow-inflight/`. The restored catalog has no journal for it, so
   the restored daemon exits at the end of each startup with "unrecognized
   private claim entry; inspect and reconcile it without discarding its
   bytes".
2. The newer daemon published a file the backup still journals as claimed,
   then removed it. The restored daemon records "unpublished claimed source is
   missing" as that claim's pending reason on each attempt, retried every few
   seconds to 15 minutes, until the claim is retired. Recovery
   needs the exact claimed file, inode and change time included, so no
   recreated file can complete it.

`deploy/reconcile-inflight.sh` handles both, asking the set-aside newer store
what each claim was. Never delete an entry or a claim's bytes:

1. Stop `sparrow.service` and both timers, set the newer store aside and
   restore the backup, as `deploy/user-unit-upgrade.sh --help` describes. Keep
   the set-aside store.
2. Open the restored store with the old image and the set-aside store with the
   newer image, each with `--query-only` on its own scratch configuration:
   `query_only = true`, `data_dir`, a scratch `socket_path`,
   `text_lexicon_dir` and the live budgets, with no `consume`, ingest socket,
   TCP listener or archive. Query-only mode opens no root.
3. `deploy/reconcile-inflight.sh inflight --restored OLD_CLI OLD_SOCKET
   --newer NEW_CLI NEW_SOCKET`. For each consume root of the restored store,
   it moves every entry of `ROOT/.sparrow-inflight/` that the restored
   store's `ingest-get` does not know back to the root under the original
   name the newer store records. The move is one link and unlink, so the
   inode and bytes stay the same. It stops, moving nothing more, at an
   entry neither store knows or whose original name is taken.
4. Stop the restored query-only daemon and start `sparrow.service`.
5. `deploy/reconcile-inflight.sh missing --restored OLD_CLI SOCKET --newer
   NEW_CLI NEW_SOCKET`. For each claim that `sparrow SOCKET watch claims`
   lists as pending with the reason "unpublished claimed source is missing",
   the newer store must record that claim as published. Its artifact's bytes
   are read back from the newer store and checked against the artifact's
   payload digest. They are then renamed into the root as
   `NAME.reshipped.EXT`, a new file the watch ingests, so the records the
   rollback dropped come back. Last, the claim is retired with `sparrow
   SOCKET watch retire-claim` (schema 70 and newer). A restored claim keeps
   the reason it had at the backup until the restored daemon's first attempt
   rewrites it, so it is not reshipped before that attempt. The script exits
   4 while any watch holds a pending claim, with or without a reason, that
   it did not reconcile. Then inspect `watch claims`: a claim just attempted
   needs a rerun, and a claim of another kind (for example "watch root was
   replaced") needs its own decision, so do not rerun for ever. Exit 0 means
   no watch held a pending claim this run left alone, so `reshipped=0` then
   means nothing was owed. An image older than schema 71 has no `watch
   claims`; the script then compares each watch's `pending_claims` with the
   claims it reshipped and retired, and exits 4 for any excess. The claim a
   watch's volatile failure text names may no longer be pending, so it is
   acted on only when the restored store's `ingest-get` reports it reserved
   or claimed. An image without `watch retire-claim` makes it exit 3 after
   re-shipping, when no other pending claim is unreconciled (else 4): the claim
   stays pending until a schema 70 image retires it.
6. Stop the newer query-only daemon and start `sparrow-ship.timer`. Start
   `sparrow-backup.timer` only after a first online backup of the restored store
   verifies (`deploy/verify-backup.sh` exits 0).

`deploy/verify-backup.sh --rehearse-rollback` rehearses both cases on a copy
(see "Measure the online migration with intake and traffic").
