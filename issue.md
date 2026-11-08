# Issue #14: Make `sources` and `artifacts` work on a store of any size

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Summary

Make `sources` answer from per-source counters that the store keeps exact in every write, and make `artifacts --source` walk a per-source index, so neither operation reads the whole catalog. On the live store both commands always fail with `RESOURCE_LIMIT`, with or without `--source`. Operators therefore cannot list the store's sources, find a source's producer windows, or list a source's artifacts. Each failed call also holds the store's writer lock for 12 to 18 seconds, which stalls ingest.

## Current behavior

Verified on `main` at `ac6277f` and against the live store on 2026-11-01 (89,631 live records).

**Observed.**
- `sparrow SOCK sources` fails with `sources requires STORE_ID`. With the store ID, `sources STORE_ID --page-size 10 --scan-budget 100000` fails after 12 seconds, and `sources STORE_ID --source development-consume --page-size 10 --scan-budget 1000` fails after 17 seconds. Both report `RESOURCE_LIMIT: catalog capture exceeds operation admission`.
- `artifacts STORE_ID --source development-consume --page-size 1` fails the same way after 18 seconds.

**Code path.**
- The CLI (`crates/sparrow-server/src/agent/sources.rs:14-36`) sends `sourcesList`. `rpc.rs:3300-3386` runs `Store::source_read_with_syslog_cancellable` (`crates/sparrow-core/src/storage/source_read.rs:246`).
- That function takes the writer lock (`source_read.rs:308`) for the whole capture, sets `remaining = SCAN_BYTES`, 32 MiB (`:311`, `catalog_lease.rs:19`), and calls `Writer::artifact_capture` (`artifact_read.rs:397`) with only a source selector.
- `artifact_capture` first scans every `pin:` entry (`storage.rs:997-1031`). With no pin or record selection it then iterates every `artifact:` key and then every `record:` key in the store (`artifact_read.rs:458-477`). Each entry goes through `ArtifactCapture::push` (`:276`).
- `push` charges the entry before anything else: `CaptureAdmission::entry` (`catalog_lease.rs:38-51`) counts it against `scan_budget` (at most 100,000) and calls `Writer::charge_catalog`, which subtracts 64 times the encoded bytes from `remaining` (`storage.rs:818-827`). The source filter runs only afterwards (`artifact_eligible`, `artifact_read.rs:330` and `:497`).
- The capture therefore refuses once the whole store's `artifact:` and `record:` entries pass 512 KiB encoded (32 MiB divided by 64) or 100,000 entries, whatever `--source` says. The live store passes both bounds.
- `artifacts` uses the same capture (`artifact_read.rs:233` and `:241`).
- The counts `sources` reports are `complete_artifacts`, `fragment_parents` and `retained_records` per source (`source_read.rs:83-110`, `:329-335`). They are documented as exact in `docs/interfaces.md:1738-1760`.

## Work

**Per-source counters (new `crates/sparrow-core/src/storage/source_summary.rs`).**
- Keep one catalog row per source, `source-summary:<SHA-256 hex of the source label>`, holding the source label and three exact counts:
  - `retained_records`: record entries present and not evicted;
  - `complete_artifacts`: artifact entries present and not evicted;
  - `fragment_parents`: artifacts whose entry is retired while at least one of their records is retained.
- Add one owner, `source_summary::apply(batch, source, delta)`, and call it in the same `WriteBatch` as every change to those states. The writers are publication (`publication.rs:542` and `:578`, `publication/raw.rs:246`, `storage.rs:1559`), eviction (`eviction.rs:287` and `:361`) and manifest retirement (`manifest_retire.rs:346`). Find any other writer of `record:` or `artifact:` keys with `grep` and route it through the owner too.
- To keep `fragment_parents` exact, keep `artifact-retained:<artifact id>`, the count of that artifact's retained records, in the same batches. Retiring an artifact entry while the count is above 0 adds one fragment parent. Retiring the last retained record of a retired artifact removes one.
- A counter that would go below 0, or a missing summary row for a source that has entries, is a `STORAGE_FAILURE` naming the source. It is never clamped.
- These counts follow retirement, not the retention deadline: a record past its deadline that is not yet retired, for example while the archive gate holds it, still counts. Status `live_records` keeps its deadline meaning.

**`sources` reads the counters.**
- `source_read_with_syslog_cancellable` stops calling `artifact_capture`. It reads `source-summary:` rows, one point read with `--source` and a prefix walk without, and merges them with the watch, syslog, producer-window, receiver-window, tail-generation, time-profile and diagnostic-profile bindings it already collects.
- Each row read is one entry against `scan_budget` and its encoded size against the byte budget. The existing cursor and page handling (`catalog_lease::Reads`) stay.
- Take a RocksDB snapshot under the writer lock and release the lock before reading, so a listing never blocks ingest.

**`artifacts` walks a per-source index.**
- Keep `source-artifact:<source SHA-256 hex>:<received_at as 16 hex digits>:<artifact id>` rows, written and deleted in the same batches as the artifact entry through the same owner.
- With `--source`, `artifact_capture` walks only that prefix, between the `--received-from` and `--received-before` bounds when they are given, and reads each artifact's entry and its records through the artifact's `record_ids`. The scan and byte budgets are charged only for entries it reads.
- Without `--source`, it walks the `source-artifact:` index source by source in the same way, so an unfiltered listing costs what its selected rows cost and stops at `scan_budget` with the existing `RESOURCE_LIMIT`, instead of failing on the size of the whole store.

**Migration and fence.** At open, seed the summary, `artifact-retained:` and `source-artifact:` rows from the existing `artifact:` and `record:` entries with a `catalog_upgrade::Job`, in durable pages, as `provenance_rdf::upgrade` does. Fence the store with a new writer-format family in `writer_format.rs` so older writers cannot leave the counters stale. Measure the job on a reflink copy of the newest closed-store backup and record its duration in `docs/load-qualification.md`. The target is under 60 seconds for the live store.

**Docs.**
- `docs/interfaces.md`, the `sources_list` section (lines 1738-1760): state that counts follow retirement, and that a listing costs one entry per source row.
- `docs/interfaces.md`, the artifact listing section: document the per-source walk.
- `docs/architecture.md` section 7: list the three new catalog row families.

This changes the store format and no wire schema.

## Out of scope

- Per-source byte totals.
- The record search scan budget for wide time windows.

## Depends on

None.

## Automated acceptance

Tests in `crates/sparrow-core/tests/storage.rs` unless noted:

- `source_listing_cost_is_independent_of_store_size`: a store with 20,000 records across three sources and 4 MiB of record catalog lists all sources and one source with `scan_budget = 10`, where today's capture refuses. This protects against the whole-catalog scan.
- `source_counters_match_a_full_recount_after_every_lifecycle_step`: after ingest, import publish, typed events, eviction under pressure, expiry with a pin and pin release, the summary rows equal a recount from the catalog entries. This protects counter exactness.
- `fragment_parent_counts_follow_pinned_records`: an artifact retired while a pinned record survives counts as one fragment parent until the pin is released and the record retires.
- `negative_or_missing_source_counter_is_a_storage_failure`.
- `artifacts_by_source_reads_only_that_source`: with a second source holding 20,000 records, `artifacts --source A` succeeds with `scan_budget` equal to A's row count.
- `source_index_migration_seeds_exact_counters_and_fences_older_writers`: a store written before this change reopens with exact counters, and an older writer format refuses it.
- `crates/sparrow-server` test `sources_listing_does_not_hold_the_writer_lock`: an ingest issued during a listing completes before the listing finishes.

`make check` passes.

## UAT

Run on this host after the change is deployed.

0. Set up:
   ```bash
   SOCK=/SPARROW/store/rpc.sock
   SP="$HOME/.local/bin/sparrow $SOCK"
   STORE=$($SP status | jq -r .store_id)
   RUN=$(date +%s)
   cd ~/Active/sparrow && cargo build --locked --release -p sparrow-client --example events
   EV=~/Active/sparrow/target/release/examples/events
   ```
1. Confirm the deployment:
   ```bash
   systemctl --user is-active sparrow.service
   $SP status | jq -r .store_format
   ```
   Expected: `active`; a store format newer than `sparrow.store/92`.
2. List every source:
   ```bash
   TIMEFORMAT=%R; time $SP sources $STORE --page-size 1000 | jq -c '{n: (.rows | length), sources: [.rows[] | {source_id, retained_records}]}'
   ```
   Expected: exit status 0; at least the six watched sources from `$SP watch list`; elapsed time under 2 seconds (the target).
3. List one source, as before the fix:
   ```bash
   $SP sources $STORE --source development-consume --scan-budget 1000 | jq -c '.rows[0] | {source_id, complete_artifacts, retained_records, watches: [.watches[].watch_id]}'
   ```
   Expected: exit status 0, with `watches` `["development-completed"]`.
4. Counts are exact for a new source:
   ```bash
   for i in 1 2 3; do $EV $SOCK uat.sources-$RUN uat-sources-$RUN-$i $(openssl rand -hex 16) $(date +%s%3N) "sources check $i" durable 1; done
   $SP sources $STORE --source uat.sources-$RUN | jq -c '.rows[0] | {complete_artifacts, fragment_parents, retained_records}'
   $SP artifacts $STORE --source uat.sources-$RUN --page-size 10 | jq '.rows | length'
   ```
   Expected: `{"complete_artifacts":"3","fragment_parents":"0","retained_records":"3"}`, then `3`.
5. The counts cover the deadline count, while the pressure phase is `ready`:
   ```bash
   LIVE=$($SP status | jq -r .live_records)
   SUM=$($SP sources $STORE --page-size 1000 | jq '[.rows[].retained_records | tonumber] | add')
   echo "live=$LIVE sum=$SUM"; [ "$SUM" -ge "$LIVE" ] && echo COUNTS-COVER
   ```
   Expected: `COUNTS-COVER`. The sum can exceed `live_records` only by records past their deadline that are not yet retired.

Done when every UAT step passes on the live service.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
