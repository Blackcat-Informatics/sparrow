<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Installing Sparrow on Linux

The repository supplies a persistent systemd user unit at
`packaging/systemd/sparrow.service`. It runs the same typed daemon as the
development unit, with a private Unix socket and configured public Cap'n Proto
TCP socket. The selected public binding is `0.0.0.0:518`.

Run `make check`, commit through the normal verification hook, then run
`make build-runtime`. Install the explicitly selected release binaries under
unique revision-qualified names in `~/.local/libexec/sparrow/`. Compare their
hashes with the build artifacts. Keep previous images for an explicit rollback;
do not overwrite an executing image. Installation and live verification are
separate from the source gate.

The unit uses these stable paths:

| Path | Contents |
| --- | --- |
| `~/.local/libexec/sparrow/sparrowd` | Symlink to the selected installed daemon image. |
| `~/.local/bin/sparrow` | Symlink to the selected installed CLI image. |
| `~/.config/sparrow/server.toml` | Reviewed deployment configuration with absolute paths. |
| `~/.config/systemd/user/sparrow.service` | Copy of the repository unit. |

Create private configuration/state directories with mode 0700 and configuration
with mode 0600. Existing paths belong to the operator: inspect them before
installation or replacement. Copy the five required native CBOR lexicons and
their provenance manifest, notices and licenses into a revision-qualified directory under
`~/.local/share/sparrow/`; configure `text_lexicon_dir` to that installed bundle.
Compare each copied artifact with its source. The daemon's existing native
analyzer resolver verifies the required artifact identities before serving.
Runtime configuration must not depend on an editable sibling checkout.

Configuration supplies the store/socket paths, disk budget, maintenance reserve,
operation ceilings and intake definitions. `config/development.toml` is a
host-specific example for owned synthetic intake, not production source
authority. Preserve an existing store's configuration and catalog watch
revisions during migration. Consume entries seed only a missing catalog;
typed watch administration controls later changes. Never add production sample
directories as destructive watches merely because they were sample inputs.

`max_concurrent_requests` admits simultaneous storage work, including intake
workers (1..64). `max_rpc_connections` independently admits Unix and TCP RPC
streams from one shared ceiling (1..256, default 16). An idle agent capability
holds a transport lease without consuming a storage permit. The existing
60-second connection lease still applies. Peers beyond the connection ceiling
are disconnected before capability admission; storage saturation returns the
existing typed operation refusal. These are separate finite resource budgets.

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
0600, for example `(umask 077; head -c 32 /dev/urandom > ~/.config/sparrow/tcp.secret)`,
and do not use a passphrase: an observer of one CHAP exchange can test guesses
offline. Copy the same file privately to each remote agent host and pass it with
`sparrow --secret-file PATH tcp://ADDRESS:518 COMMAND`. Producers that only
discover, ingest and import need no secret. CHAP does not encrypt traffic or
authenticate the server, so restrict the listener to trusted networks.

Port 518 requires the daemon image to have `cap_net_bind_service=ep`, or another
explicitly configured host privilege arrangement. Apply a file capability to the
final installed image, not the symlink or a volatile build artifact; copying an
image does not preserve that capability. Verify with `getcap` before cutover.
The client needs no file capability. The unit does not run a privileged shell
or introduce another transport.

Validate the installed unit with:

```bash
systemd-analyze --user verify ~/.config/systemd/user/sparrow.service
```

For a migration, capture the current typed status, image identity, unit and
configuration. Stop the old Sparrow unit through its SIGINT shutdown path and
verify it is terminal, with its Unix socket removed. Never open two owners on
one store. Then load and enable the installed unit:

```bash
systemctl --user daemon-reload
systemctl --user enable --now sparrow.service
systemctl --user status sparrow.service
sparrow --secret-file ~/.config/sparrow/tcp.secret tcp://192.168.2.4:518 status
sparrow --secret-file ~/.config/sparrow/tcp.secret tcp://192.168.2.4:518 mcp --stdio
```

Use the actual reachable server address for remote clients. Verify store
identity, schema/format, retained counts, watches, byte reads and saved mutation
retries over Unix and TCP after adoption. Stop the new unit before reverting an
image/configuration; reverting a binary alone cannot undo a store-format upgrade.
A closed-store backup is a recovery artifact, and its existence does not prove
restoration has been exercised.

For isolated recovery qualification, copy a closed-store backup to a separate
directory and copy the deployment configuration with `data_dir` and
`socket_path` changed to that directory. Keep the backup immutable. The recovery
example uses the existing configuration/store readers directly, without starting
listeners, maintenance or saved watch workers:

```bash
cargo build --locked --release -p sparrow-server --example recovery
target/release/examples/recovery restored.toml EXPECTED_STORE RFC3339 EVIDENCE_ID...
```

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

For boot startup independently of an interactive login, verify
`loginctl show-user "$USER" -p Linger`. Enabling lingering when absent is a host
administration action. The enabled unit and lingering establish boot
configuration; a reboot test is separate evidence and must not be claimed from
unit validation alone.

The unit uses SIGINT because the daemon implements worker drain and WAL fencing
on that signal. It gives shutdown 120 seconds and reports failure if the process
cannot exit. It restarts on failure, including host OOM kills, after 10 seconds.
Startup takes the store's exclusive lock before it removes a refused stale Unix
socket, so a restart never creates a second writer. Five failed starts within
ten minutes stop the unit for diagnosis, such as missing dependencies or a
refused store open. Never remove a socket by hand until the prior owner is
proven terminal.
The kernel throttles the unit's memory above 2 GiB (`MemoryHigh`) and kills it
at 4 GiB (`MemoryMax`), so a leak cannot deepen host-wide memory pressure.
`CPUQuota=100%` holds the daemon to one core and `IOWeight=20` gives it low disk
priority; `Nice=10` only orders its own threads. A sustained deep-JSON backlog
once saturated the host's CPU and disk; under these limits ingest slows instead.
Journal output remains available through `journalctl --user -u sparrow.service`.

## Shipping finished files from producers that keep them

A consume watch owns its root: it deletes each file after publication. Never make a producer's own directory a consume root.
`packaging/shipper/sparrow-ship` copies finished files from such directories
into a private queue under its `state_dir`, then feeds queued items into
Sparrow-owned roots. It only reads the originals. Each `[[source]]` in its
TOML configuration (see `packaging/shipper/ship.example.toml`) names the consume
root `intake_root/NAME`, the source directory, a glob and one finishing rule:

- `daily`: dated files; the newest name may still be appended to.
- `sealed`: rotated segments, finished once `NAME.manifest.json` exists.
- `settled`: written-once documents, finished after a quiet period.

Every rule also requires `settle_seconds` without modification. Small files are
reflink copies, sharing extents on btrfs. Line-oriented files (`jsonl`, `log`)
above `max_chunk_bytes` or `max_chunk_lines` are split on line boundaries into
`NAME.partNNNNN.EXT` chunks. Unsplittable formats above `max_whole_bytes` are
refused. The split matters for two reasons. The consume worker retries a
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

Create each consume root and its watch before the first run, then install:

```bash
install -m 0755 packaging/shipper/sparrow-ship ~/.local/libexec/sparrow/
cp packaging/systemd/sparrow-ship.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now sparrow-ship.timer
```

The timer runs five minutes after each completed run. Shipped data arrives when
its producer finishes a file, not as it is written.

## Closed-store backups

Sparrow has no online checkpoint, so a consistent backup needs a closed store.
`packaging/backup/sparrow-backup` stops `sparrow.service` through its SIGINT
drain, copies `data_dir`, the consume intake root, the server configuration and
any extra paths, and starts the service again. It refuses to run when the
service is not already active, so it never starts a service an operator
stopped. Once it has stopped the service it restarts it after a failure or a
SIGTERM, and the unit's `ExecStopPost` restarts it if the script is killed
outright, guided by a marker in the runtime directory. On btrfs the copy is
a reflink: it takes seconds at any store size and later writes never change
it. A backup is renamed into place only after it is fully copied and
synchronized, then the oldest beyond `KEEP` are removed. The script refuses to
run unless free space covers twice the store plus 10 GiB, in case extents are
not shared. Each backup lists its sources in `SOURCE`; the journal records the
measured downtime. A lock prevents overlapping runs.

```bash
install -m 0755 packaging/backup/sparrow-backup ~/.local/libexec/sparrow/
cp packaging/systemd/sparrow-backup.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now sparrow-backup.timer
```

An unfinished consume claim binds the inode of its intake root and claimed
file, which no restored copy can reproduce, so a backup must contain no
unfinished claims. Before stopping the service the script lets consumption
drain: it holds the shipper's lock, so no new files arrive, and waits until
every intake root is empty and every watch's `pending_claims` is zero. It never
moves files under the running daemon, because a reserved claim binds the file's
identity, including its change time. If the roots have not drained within 30
minutes the run fails without stopping the service; a file that never drains is
itself a fault to investigate. The shipper's global `max_pending_bytes` bounds
the wait: with the example's 16 MiB budget, 1 MiB chunks and 16 MiB
`max_whole_bytes`, the roots never hold more than 16 MiB, which drains in about
eight minutes at the measured 30 seconds per MiB.

The unit keeps three daily copies under `~/.local/share/sparrow/backups/`,
taken between 04:00 and 04:15 local time. It also copies the shipper ledgers and
watch definitions and holds the shipper's lock throughout, so the store, the
files awaiting ingest and the ledgers all describe the same moment. Reflink copies share unchanged extents with the live
store, but compaction rewrites files, so the copies gradually stop sharing.
Budget up to three times the store size. A copy on the same disk protects
against software faults and operator error, not disk loss.

To restore, stop `sparrow-backup.timer`, `sparrow-ship.timer` and
`sparrow.service`. Move the damaged `data_dir`, the intake root directory and
the shipper's `state_dir` aside rather than deleting them. Copy `BACKUP/store`
to `data_dir`, `BACKUP/intake` to the intake root and `BACKUP/config/ship` to
`state_dir`, each with `cp -a --reflink=auto`, and restore any needed
configuration from `BACKUP/config`. Then start the service and both timers. The
three must come from the same backup, so that the files awaiting ingest match
the ledgers. The shipper then resends every file shipped after the backup whose
original still exists. Records that producers sent directly after the backup
are lost unless they resend them. This whole-service procedure has not been
exercised; the drill covers only reading a restored store. Before replacing a live store, qualify the backup on a disposable copy
with the recovery example above. Never start `sparrowd` on such a copy: its
watch catalog names the live consume roots.

This packaging does not establish whole-corpus throughput, physical reclamation,
restoration or full architecture acceptance. Current installed image and live
acceptance evidence are recorded in [Development runtime](development-runtime.md).
