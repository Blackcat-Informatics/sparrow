# Issue #16: Run the NAS archive worker that writes, verifies and commits segments

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Summary

Add the daemon's archive worker. It writes each prepared segment to the NAS with the specified write protocol, reads it back and verifies it, and only then advances the store's archive watermark so retention can delete the covered evidence. The worker starts degraded when the NAS is unreachable, never blocks queries, ingest or maintenance on a hung NAS, and exposes its state and a bounded quarantine listing in status. With this phase, the service keeps its 30-day searchable window and moves aged-out logs to `/net/synology/archives/sparrow` instead of discarding them.

## Current behavior

Verified on `main` at `ac6277f` and on this host.

- `crates/sparrow-server/src/config.rs:59-62,67-85` parses an optional, strict `[archive]` table: `dir`, `start_fraction`, `max_segment_bytes`, `max_segment_records`, `stall_warn_secs` (default 120) and `io_timeout_secs` (default 900). `validate` (`:114-138`) makes lexical checks only, and its comment says "probing the archive device may block, so it is not done here".
- No daemon code uses `ArchiveConfig` beyond building the store policy (`config.rs:262`). `crates/sparrow-server/src/main.rs` starts no archive task. An archiving store therefore never advances its watermark: expiry waits, and pressure reports `awaiting_archive` (`crates/sparrow-server/src/pressure.rs:65,77`).
- `docs/interfaces.md:216-276` says: "Do not enable `[archive]` on a production store before the worker ships. The fence is permanent." It says worker status "will use a later schema".
- `crates/sparrow-core/src/storage/archive.rs:376-432`: `archive_intend` and `archive_commit` exist. The segment encoder issue (#15) adds `archive_prepare`, `archive_rebuild` and the `PreparedSegment` this worker consumes.
- `docs/archive-format.md` section 8 specifies the write protocol and crash recovery; section 6 calls for "a bounded quarantine listing, which status and an operator listing operation report".
- On this host, `/net/synology/archives` is an NFS 4.1 `hard` mount from `192.168.1.1:/volume1/archives` with `fsc` (FS-Cache) enabled. `/net/synology/archives/sparrow` does not exist yet. A hard mount blocks callers in uninterruptible I/O while the NAS is unreachable. With `fsc`, a plain read-back can be served from the local cache.
- If the NAS were unmounted, `/net/synology/archives` would be an empty directory on the root btrfs filesystem, which shares `/dev/nvme0n1p3` with `data_dir`.

## Work

**Configuration (`crates/sparrow-server/src/config.rs`).**
- Add a required `mount_point` (an absolute path that `dir` lies below) and `probe_interval_secs` (default 60).
- The NAS thread treats the archive as unavailable unless `mount_point` appears as a mount point in `/proc/self/mountinfo`. A missing NAS mount can therefore never redirect writes to the local disk.

**NAS thread (new `crates/sparrow-server/src/archive/nas.rs`).**
- One named thread, `sparrow-archive-nas`, runs every filesystem operation on the archive: Probe, Write and Verify. Jobs arrive on a channel of capacity 1, so there is never more than one in flight. The thread records the current operation and its start time in atomics.
- Nothing else touches the NAS: not RPC, status, maintenance or shutdown. Shutdown never joins this thread.
- An operation running longer than `stall_warn_secs` sets phase `stalled`. One running longer than `io_timeout_secs` sets `unavailable`, latched until the call returns.
- **Write** follows `docs/archive-format.md` section 8:
  1. Write each table into a fresh `.staging/<staging-id>/` directory and fsync it.
  2. Write `manifest.nq` last and fsync it.
  3. Fsync the staging directory.
  4. Create the missing `segments/YYYY/MM/DD/` parents, fsyncing each and its parent.
  5. Check that the target is absent, then rename. The single-writer rule makes the check and the rename safe; add a `helpers-ledger.toml` `[[distinct]]` row explaining why there is no `RENAME_NOREPLACE` on NFS.
  6. Fsync the parent.
  7. Read every file back with `O_DIRECT`, so neither the page cache nor FS-Cache answers, and check its BLAKE3.
- Quarantine files are written under `quarantine/YYYY/MM/DD/<segment-id>/` by the same steps, before the segment rename.
- An existing target that verifies counts as written. One that does not verify is a hard failure, and the watermark does not move.
- Stale staging directories are removed before each new write.
- **Store binding.** `<root>/<store-hex>/STORE.nq` is created only while the watermark is still empty. Once any segment has committed, a missing `STORE.nq` means the archive is unavailable, never something to recreate. A `STORE.nq` naming another store latches phase `failed`.

**Worker task (new `crates/sparrow-server/src/archive/mod.rs`, wired in `main.rs`).**
- A Tokio task, separate from the maintenance worker, loops:
  1. `archive_prepare(now)` (archive-p2), on `spawn_blocking`.
  2. Send the Write job.
  3. Wait for verification.
  4. `archive_commit(segment_id)`.
- It processes the backlog back to back and then waits for the next due entry or a pressure request.
- On restart with an intent present, it calls `archive_rebuild(&intent)` and writes again. If the target already exists and verifies, it adopts the segment.
- **Startup.** The daemon sends one Probe and waits at most 10 seconds for it. A mismatched `STORE.nq`, a root without write permission, or a root on the same filesystem as `data_dir` (compared by `statfs` filesystem ID) refuses startup with an actionable message. A probe that times out, hits `EIO` or finds `mount_point` not mounted starts the daemon with phase `unavailable` and retries every `probe_interval_secs`. The probe checks `mount_point` in `/proc/self/mountinfo` first; when it is not mounted, no other check runs, so an unmounted archive on the root filesystem never trips the same-filesystem refusal.

**Quarantine listing (`crates/sparrow-core/src/storage/archive.rs`).**
- Each committed segment adds its quarantine entries to catalog rows `archive-quarantine:<position>`, in the same batch as the commit.
- The listing keeps the most recent 10,000 entries and drops the oldest beyond that; `quarantined_total` stays exact. Every entry also stays in its segment manifest on the NAS.
- Add `archiveQuarantineList(storeId, schemaVersion, cursor, pageSize)` with the shared catalog receipt and page limits, and CLI `sparrow SOCK archive-quarantine STORE_ID [--cursor CURSOR] [--page-size N]` in the command table in `crates/sparrow-server/src/agent/mod.rs`, with the MCP tool `archive_quarantine_list` sharing its renderer. The document has `rows`, each `{evidence_id, reason, segment_id, path}`, plus `quarantined_total` and `quarantine_listing_bound` as decimal strings, and the shared `page` object.

**Status.** Add `Status.archive`, rendered by CLI and MCP `status` as `archive`:
- `phase`: `disabled`, `idle`, `encoding`, `writing`, `verifying`, `committing`, `stalled`, `unavailable` or `failed`;
- `watermark` (position and receipt time);
- `oldest_unarchived_age_ms`, `waiting_entries` and `expiry_waiting` (entries past their deadline that the gate holds);
- `last_segment` (ID, path, records, payload bytes, committed at);
- `session`: totals since the daemon started, `{segments, records, bytes, quarantined}`;
- `quarantined_total` and `quarantine_listing_bound`;
- `in_flight` (operation, started at, elapsed), `null` when idle;
- `failure`, `mount_point` and `dir`.

Counts and times use the status block's existing encodings: decimal strings for counts and RFC 3339 for times. This advances the schema version by one.

**Docs.**
- `docs/interfaces.md`, "Archive-before-delete gate": remove the warning against enabling archiving, and document the status block, `archive-quarantine`, `mount_point` and `probe_interval_secs`.
- `docs/archive-format.md`: mark P3 implemented and describe the `O_DIRECT` read-back and the `STORE.nq` creation rule.
- `docs/deployment.md`: add an "Archiving to the NAS" section. Cover the Synology NFS rule for the `archives` share, which must not enable asynchronous writes, creating the root, the config table, the first backlog catch-up and reading `status.archive`.

## Out of scope

- Offline reader tools (archive-p4).
- Re-importing archived evidence into a store.
- Pruning the NAS archive.

## Depends on

The segment encoder issue (#15).

## Automated acceptance

Tests in `crates/sparrow-server`, using a temporary directory and a fake NAS that can block or fail on command:

- `archive_then_expiry_retires_only_committed_evidence`: an end-to-end run with an injected clock. Records retire only after their segment commits.
- `crash_at_each_write_step_recovers_without_duplicates`: uses `sparrow_test_support::crash`. Kill after the intent, after staging, after the rename and after verification. Restart rebuilds or adopts, and each range has exactly one segment.
- `hung_nas_reports_stalled_while_rpc_and_maintenance_continue`: with the fake NAS blocked, status shows `stalled` and then `unavailable`, while `status`, `records` and an ingest each complete within 2 seconds.
- `unmounted_archive_starts_degraded_and_recovers`: with `mount_point` not mounted, the daemon starts with `unavailable` and archives after it appears.
- `mismatched_store_binding_refuses_startup`: also covers a root without write permission and one on the same filesystem as `data_dir`.
- `missing_store_binding_after_commit_is_unavailable_not_recreated`.
- `verification_failure_never_moves_the_watermark`.
- `quarantine_listing_is_bounded_and_total_is_exact`: 10,050 quarantined entries give a 10,000-row listing and `quarantined_total = 10050`.
- `shutdown_does_not_wait_for_a_hung_nas_thread`.

`make check` passes.

## UAT

Run on this host after the change is deployed. Enabling archiving fences the live store permanently.

0. Prepare:
   ```bash
   SOCK=/SPARROW/store/rpc.sock
   SP="$HOME/.local/bin/sparrow $SOCK"
   STORE=$($SP status | jq -r .store_id); HEX=${STORE#store:}
   findmnt -n -o FSTYPE,OPTIONS -T /net/synology/archives
   systemctl --user start sparrow-backup.service; systemctl --user show sparrow-backup.service -p Result
   mkdir -p /net/synology/archives/sparrow
   ```
   Expected: `nfs4` with `hard`; `Result=success`. In the Synology control panel, the NFS rule for the `archives` shared folder has "Enable asynchronous" turned off.
1. Enable archiving: add this table to the end of `~/.config/sparrow/server.toml`, then restart:
   ```toml
   [archive]
   dir = "/net/synology/archives/sparrow"
   mount_point = "/net/synology/archives"
   ```
   ```bash
   systemctl --user restart sparrow.service; sleep 20
   systemctl --user is-active sparrow.service
   $SP status | jq '.archive | {phase, waiting_entries, oldest_unarchived_age_ms, failure}'
   ```
   Expected: `active`; the phase is one of `encoding`, `writing`, `verifying` or `committing`; `failure` is `null`.
2. Segments appear on the NAS and verify:
   ```bash
   cat /net/synology/archives/sparrow/$HEX/STORE.nq
   D=$(find /net/synology/archives/sparrow/$HEX/segments -mindepth 4 -maxdepth 4 -type d | head -1)
   b3sum $D/*.parquet; grep -o 'blake3> "[0-9a-f]*"' $D/manifest.nq
   ```
   Expected: `STORE.nq` names `store:$HEX`; each digest from `b3sum` appears in the manifest.
3. Wait for the backlog to finish:
   ```bash
   until [ "$($SP status | jq -r .archive.phase)" = idle ]; do sleep 300; done
   $SP status | jq '.archive | {watermark, expiry_waiting, oldest_unarchived_age_ms, session}'
   ```
   Expected: `expiry_waiting` is `"0"`; `oldest_unarchived_age_ms` is below 22.5 days (1,944,000,000 ms), since entries are archived once they reach 75% of retention; `session.segments` is above 0.
4. Ingest and queries stay responsive while the worker runs:
   ```bash
   time $SP records --received-from "$(date -u -d '-1 hour' +%FT%TZ)" --page-size 50 > /dev/null
   $SP status | jq -r .pressure.phase
   ```
   Expected: the query finishes in under 5 seconds (the target; it takes about 2 seconds on 2026-11-01); `ready`.
5. The quarantine listing works:
   ```bash
   $SP archive-quarantine $STORE --page-size 10 | jq '{rows: (.rows | length), total: .quarantined_total, bound: .quarantine_listing_bound}'
   ```
   Expected: `bound` is `"10000"`; `rows` is at most 10; `total` equals `status.archive.quarantined_total`.
6. Degraded start: point the archive at a directory that is not a mount point (`dir` must stay below `mount_point`), restart, and confirm the service still ingests:
   ```bash
   sed -i -e 's#^dir = "/net/synology/archives/sparrow"#dir = "/net/synology/not-mounted/sparrow"#' -e 's#^mount_point = .*#mount_point = "/net/synology/not-mounted"#' ~/.config/sparrow/server.toml
   systemctl --user restart sparrow.service; sleep 20
   systemctl --user is-active sparrow.service
   $SP status | jq -r .archive.phase
   cd ~/Active/sparrow && cargo build --locked --release -p sparrow-client --example delivery
   target/release/examples/delivery $SOCK uat.archive-degraded $(date +%s%3N) "degraded ingest"
   ```
   Expected: `active`; `unavailable`; the delivery example reports `Durable sequence=1`.
7. Restore the configuration:
   ```bash
   sed -i -e 's#^dir = "/net/synology/not-mounted/sparrow"#dir = "/net/synology/archives/sparrow"#' -e 's#^mount_point = .*#mount_point = "/net/synology/archives"#' ~/.config/sparrow/server.toml
   systemctl --user restart sparrow.service; sleep 20
   $SP status | jq '.archive | {phase, failure}'
   systemctl --user show sparrow.service -p NRestarts
   ```
   Expected: phase `idle` or an active writing phase; `failure` `null`; `NRestarts=0`.

Done when every UAT step passes on the live service.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
