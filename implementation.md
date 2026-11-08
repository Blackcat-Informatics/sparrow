# Issue 14 implementation notes

Worktree `.worktrees/14-make-sources-and-artifacts-work-on-a`, branch
`paudley/14-make-sources-and-artifacts-work-on-a`, base `main` at 3a64e08. Nothing is
committed (the task forbids committing). Line numbers refer to the final working tree.

## Design decisions (made during implementation)

1. One owner, `source_summary::apply(writer, &mut batch)`
   (crates/sparrow-core/src/storage/source_summary.rs:546), is a WriteBatch observer called
   in `Writer::apply` (crates/sparrow-core/src/storage/durability.rs:143), right after
   `source_context::update`. Every runtime write commits through `Writer::apply`. I checked
   this: the only runtime `write_opt` is durability.rs:233. `catalog_upgrade::commit`
   (catalog_upgrade.rs:146) runs only at open, for derived rows. New-store init
   (storage.rs:1396) and legacy registry seeding (diagnostic.rs:721) write no catalog
   entries. So no writer can change `record:` or `artifact:` state without the owner.
   - The issue sketches the owner as `apply(batch, source, delta)` called by each writer.
     An observer gives the same single owner, but no writer can forget to call it, so it
     is the smaller design.
   - Both batch observers (source context and source summary) fold the batch through one
     shared `durability::observe_batch` and its `BatchObserver` trait (durability.rs:339
     and :348).
2. Observed keys are `artifact:<id>`, `artifact-retirement:<id>` and `record:<id>`.
   - `artifact-retirement:<id>` is the manifest retirement state. Byte 16 is the evicted
     flag, so evicting a parent never rewrites `artifact:`.
   - A merge on an observed key is refused. So is a batch that writes the derived rows
     itself together with catalog changes.
3. Per-artifact state:
   - complete: the entry is present, its JSON `evicted` is false, and its retirement state
     is not evicted;
   - retained: present, non-evicted records;
   - fragment: not complete and retained > 0;
   - listable: complete or retained > 0.
4. Rows:
   - `source-summary:<hash>`: JSON {source, retained_records, complete_artifacts,
     fragment_parents}. The row is deleted when all three are 0.
   - `artifact-retained:<id>`: 9 bytes, a u64 big-endian retained count and a mode byte
     (0 full, 1 partial orphan). Present iff listable.
   - `source-artifact:<hash>:<received_at, 16 hex digits>:<id>`: present iff listable.
   - Hash: lower-case BLAKE3 hex of the source label, through the one helper
     `source_summary::source_hash` (source_summary.rs:53). It is the store's existing
     source-key hash: `source_context::prefix`/`hashed_prefix` (source_context.rs:26/:30),
     `source_time` (source_time.rs:398) and `diagnostic` (diagnostic.rs:407) now all route
     through it.
   - The issue says SHA-256. PurRDF at the pinned revision has no SHA-256. Adding `sha2`
     would be a new direct dependency (a build-system change, which the task forbids) and
     a second source-key hash. The hash is an internal key and never appears on the wire.
5. Migration `source-summary` is the third online migration in the #36 framework
   (migration.rs:40/:48/:57, fence :122, pending :142-146, page :156, status :186).
   - Fence (source_summary.rs:370) at open:
     - moves the format into the new source summary writer families;
     - seeds checkpoint `source-summary-upgrade` (`[0]` complete, `[1]+id` artifact phase,
       `[2]+id` record phase), set complete when the store has no `artifact:` or `record:`
       key.
   - Rows found under an older writer identity are not trusted. The fence discards them in
     the same batch (`delete_range` per family, :379, plus the checkpoint) and the
     migration recounts every source. This mirrors the owner-graph fence, which restarts
     its build.
   - Phase 1 (`artifact_page`, :772) walks `artifact:` keys, at most 512 artifacts or
     16,384 records per page, always counting one artifact whole. It counts each artifact
     without a full row from its entry and its `record_ids`.
   - Phase 2 (`record_page`, :866) walks `record:` keys. It counts the retained records of
     orphan parents (entry deleted, for example by expiry a day after the deadline while a
     pin keeps a record) as partial rows.
   - What the observer counts while the migration runs:
     - artifacts created in the batch and artifacts with a full row are counted;
     - an entry-present artifact is counted iff its id is at or before the artifact
       watermark, or the migration is in phase 2 or complete;
     - an orphan's record changes count iff the record is at or before the record
       watermark;
     - other changes are skipped, and the migration reads their current state when it
       reaches them.
6. Readers while the migration is pending: `sources` and the live `artifacts` listing
   refuse with INDEX_NOT_READY. The refusal names the migration and its watermark
   (`source_summary::ready`, source_summary.rs:353). Callers are source_read.rs:367 and
   artifact_read.rs `artifact_index_page` (:710).
   - Why refuse instead of marking coverage incomplete: the issue forbids a wire schema
     change, and neither `SourceSummary` nor the live artifact page has a coverage channel
     that could mark per-source counts incomplete. Answering only for the sources the
     watermark has passed would therefore present partial counters as exact.
   - Strict reads already refuse with INDEX_NOT_READY while the schema 69 migrations are
     pending (#36), so this follows the existing precedent.
   - Explicit artifact selections, record reads, SPARQL and search are served, because
     they do not read these rows.
7. Listing order is source by source (source-hash order), then receipt, then ID. This is
   the unfiltered per-source walk the issue specifies.
8. An orphan parent's records are found through the source-context record index at the
   parent's receipt millisecond (`hashed_prefix(hash)` + `{received_at:020}:`). Records
   share their parent's receipt.
9. Writer formats: SOURCE_SUMMARY_TRACING / LOG_TIME / ARCHIVE / INTAKE / INTAKE_ARCHIVE are
   `sparrow.store/242` to `/296` (writer_format.rs:297/:310/:323/:336/:349). Each lifts its
   owner-graph counterpart by 55 and keeps its switches, without renumbering anything.
   `has_source_summary` / `enable_source_summary` are at :428/:434, and the test-support
   `lower_source_summary` is at :424.

## What changed (file:line)

- crates/sparrow-core/src/storage/source_summary.rs (new):
  - key helpers :53-76;
  - `Counts` :98;
  - `counts` :161 and `merge` :173 (a negative count or missing row is STORAGE_FAILURE
    naming the source, never clamped, via `shift` :152);
  - `Progress` :280, `progress` :329, `ready` :353, `fence` :370, `migration_counts` :398;
  - `apply` :546;
  - migration `page` :733, `artifact_page` :772, `count_artifact` :797, `record_page` :866;
  - test-support `remove_source_summary_for_legacy_fixture` :942, `SourceCountAudit` :968,
    `Store::source_count_audit_for_tests` :971, `recount` :982.
- crates/sparrow-core/src/storage/durability.rs: `source_summary::apply` call :143; shared
  `BatchObserver` :339 and `observe_batch` :348.
- crates/sparrow-core/src/storage/source_read.rs:
  - The writer lock is held only to read the bounded registries and take `db.snapshot()`
    (:362). It is dropped at :363.
  - Test-support pause hook: `pause_source_reads_for_tests` :222.
  - The pending refusal is at :367.
  - Counter rows: a point read with `--source` (:402) or a prefix walk without one. Each row
    is one admission entry against `scan_budget` and its bytes against the byte budget.
  - Every listed or selected source without a counter row gets one probe of its artifact
    index; finding a row is STORAGE_FAILURE naming the source (:646).
  - `artifact_capture` is no longer called.
- crates/sparrow-core/src/storage/artifact_read.rs:
  - `Position`/`Partial` :181 (the partial row is boxed to keep `ResultPage` small);
  - live listing `artifact_index_page` :710, `next_index_row` :804, `fold_index_row` :885,
    `fold_index_record` :1032, `selected_prefixes` :1146, `refuse_row` :1169,
    `finish_row` :1194;
  - the old receipt walk (`artifact_receipt_page`, `Stage`, `Indexes`, `RECORDLESS`) is
    removed;
  - `artifact_capture` refuses a live listing (:687), since a live listing is paged.
- crates/sparrow-core/src/storage/manifest_retire.rs: `State::decode` :90, `evicted_flag`
  :119, `Header::record_ids` :303.
- crates/sparrow-core/src/storage/migration.rs: the third migration (lines in decision 5).
- crates/sparrow-core/src/storage/rdf_index.rs:313: `fence_owner_graphs` returns the fenced
  format.
- crates/sparrow-core/src/storage/writer_format.rs: the families and helpers in decision 9,
  and the unit test extended.
- crates/sparrow-core/src/storage/source_context.rs, source_time.rs, diagnostic.rs: these
  route through `source_hash`. The source context observer now uses `observe_batch`
  (source_context.rs:232).
- crates/sparrow-core/src/storage/temporal_index.rs: removed the unused `entry_time`.
- crates/sparrow-core/src/storage.rs:
  - `mod source_summary` :67;
  - test-support re-exports :26;
  - `source_read_pause` field :494, initialised at :1497.
- crates/sparrow-core/src/lib.rs:120: test-support re-exports.
- crates/sparrow-test-support/src/lib.rs:201: `pending_source_summary_migration`.
- crates/sparrow-server/src/maintenance.rs:387: the maintenance test now makes all three
  migrations pending. The worker finishes them, and the counters then equal a full recount
  (`[300, 1, 0]`).
- docs:
  - docs/interfaces.md:
    - live pages walk the per-source index (:304);
    - migrations list (:609);
    - new section "Per-source counters and artifact index (store format)" (:639), with the
      pending-reader table and the measurement;
    - `sources_list` counts follow retirement and cost one entry per counter row (:2244).
  - docs/architecture.md:559: section 7 lists the three row families.
  - docs/deployment.md: "Measure the online migration" now covers `source-summary`.
  - docs/load-qualification.md: "Source summary migration (issue #14)" replaces the
    60-second target with the online criterion and records the measurement as pending.
  - deploy/verify-backup.sh: the header comment names the source-summary migration. Its
    logic already waits for every migration in status.
- helpers-ledger.toml:
  - new job `source-summary-counters` (:159);
  - `artifact-receipt-fold` replaced by `artifact-index-fold` (:285);
  - `entry_time` removed from `temporal-backend-candidates`;
  - `hashed_prefix` and `observe_batch` added to `source-context-timeline-cache`;
  - `source-catalog-discovery` (:2906) re-described.

## Every writer of `record:`/`artifact:` keys, routed through the owner

All of these commit through `Writer::apply`, so `source_summary::apply` observes them in the
same batch:

- import publish: publication.rs:543 (records) and :581 (artifact), in one batch;
- raw publication: publication/raw.rs:240 writes only the `import-record:` preparation key
  (publication.rs:425 likewise). The public catalog entries come from the import publish
  above;
- direct and typed-producer ingest: storage.rs:1767 (records) and :1790 (artifact);
- eviction: eviction.rs:387 (recordless artifact delete), :462 (record evicted put), :494
  (parent retirement state through `manifest_retire::State::stage`);
- manifest retirement: manifest_retire.rs:426 (`stage` puts `artifact-retirement:`), and
  `remove` :141;
- expiry with pins and recordless artifacts: expiry.rs:223 (recordless delete), :251
  (artifact delete), :297 (record put), :300 (record delete);
- retirement: retire.rs:104 (`artifact.stage`);
- test-only writers: recordless.rs:380/:479/:480/:548 and query_membership.rs:421 write
  through raw test DBs, not the runtime.

## Tests (all pass)

- New, in crates/sparrow-core/tests/storage.rs:
  - `source_listing_cost_is_independent_of_store_size` :19236. 20,000 records across three
    sources in 1,000-record artifacts, within the fixture's per-artifact ceiling. The
    record catalog is asserted to be at least 4 MiB. Listing all sources and one source
    with `scan_budget = 10` both succeed.
  - `source_counters_match_a_full_recount_after_every_lifecycle_step` :19311. The audit
    runs after ingest, import publish, typed events, pressure eviction, expiry with a pin,
    and pin release.
  - `fragment_parent_counts_follow_pinned_records` :19394.
  - `negative_or_missing_source_counter_is_a_storage_failure` :19431. A missing row refuses
    both the listing and the next ingest. An undercounted row refuses the eviction that
    would go below 0, naming the source and committing nothing.
  - `artifacts_by_source_reads_only_that_source` :19504.
  - `source_index_migration_seeds_exact_counters_and_fences_older_writers` :19563. It
    covers more than 512 artifacts (several pages), an orphan, writes during the
    migration, a restart mid-migration, completion equal to a recount, a missing checkpoint
    refusing, and an unknown newer identity refusing.
  - `source_summary_migration_serves_at_once_and_listings_wait_for_it` :19694. This is one
    test for three of the added cases: open serves with the migration pending; sources and
    artifacts return INDEX_NOT_READY while record reads are served; the duty steps finish
    and the counters then equal a full recount.
- New, in crates/sparrow-server/tests/rpc.rs:
  - `sources_listing_does_not_hold_the_writer_lock` :13181. An ingest issued while a
    listing is paused after its snapshot completes before the listing finishes.
- The maintenance worker case is crates/sparrow-server/src/maintenance.rs:387 (worker
  completes all three migrations; counters equal a recount). The no-intake copy daemon also
  finishes it (rpc.rs:4240).
- Updated for the intended behavior changes, with their invariants kept:
  - format numbers shifted by 55;
  - relative offsets `legacy + 217`, `current + 209`, `version + 233`;
  - archive families 264-274;
  - the schema 69 open test lists three migrations;
  - a receipt window over a reclaimed parent now finds it as a fragment without an origin,
    through the index (storage.rs:10152 and rpc.rs:2885), where it used to refuse with
    INDEX_NOT_READY.
- Budget of `artifacts_by_source_reads_only_that_source` (review-1 R1): `scan_budget` =
  A's catalog rows (3 artifact entries + 3 records), and `scanned == rows` exactly. Index
  rows are positions and are not charged. The shared pin scan now charges admission only
  for pin rows it reads (see "Review 1 fixes").

## Checks (logs under .stage/make-sources-and-artifacts-work-on-a/logs/)

Final tree:

| Check | Log | Exit | Result |
| --- | --- | --- | --- |
| `cargo fmt --all --check` | fmt-3.log | 0 | clean |
| `cargo clippy --release -p sparrow-core -p sparrow-server -p sparrow-test-support --all-targets -- -D warnings` | clippy-4.log | 0 | clean |
| `cargo test --release -p sparrow-core --no-fail-fast` | core-tests-5.log | 0 | 36 lib + 227 storage passed, 0 failed |
| `cargo test --release -p sparrow-server --bin sparrowd maintenance` | server-maintenance-3.log | 0 | 5 passed |
| `cargo test --release -p sparrow-server --test rpc --no-fail-fast` | server-rpc-3.log | 0 | 83 passed, 0 failed |
| `make census-check` | census-check-2.log | 0 | 317 ledger jobs and 1 distinct row hold; no unsanctioned isomorphic group |

Earlier runs, each followed by the fix described:

- check1.log: 0.
- core-tests-1: 101. The fence refused rows of lowered fixtures, and expectations were out
  of date.
- core-tests-2: 101. Nine failures: fixture sizes, relative format offsets, the
  missing-row probe for a selected source, and the lifecycle order.
- storage-tests-3: 101. Disk budget and the expiry instant.
- core-tests-3 and core-tests-4: 0.
- clippy-1: 101. A large enum variant, type complexity and an explicit counter.
- clippy-2 and clippy-3: 0.
- server-rpc-1: 101. Copy daemon migration count and the fragment receipt window.
- server-rpc-2: 0.
- census-check-1: 2. Three isomorphic groups, resolved by sharing `observe_batch` and
  removing a duplicate `is_empty`, with no ledger exception.

## Review 1 fixes (reviews/review-1.md)

- R1, pin admission charged only for pin rows read:
  - `Writer::capture_pins`, crates/sparrow-core/src/storage.rs:1173-1180: the loop is now
    `for entry in self.db.prefix_iterator("pin:")`, and it calls `admit()` only after the
    `pin:` prefix check. The probe that ends the range is never charged.
  - Callers whose limit allowed for that probe:
    - eviction.rs:319: `pins > limits.scan_budget`, was `+ 1`;
    - expiry.rs:114: `pin_visits > limits.scan_budget`, was `+ 1`;
    - retention_metrics.rs:209: comment now says the callback counts pin rows read.
  - Exact counts updated in crates/sparrow-core/tests/storage.rs:
    - :10344, live narrow page, 5 to 4;
    - :10764, selected capture, 5 to 4;
    - :10257 and :10419, the two budget-1 refusal tests now spend the only entry on a live
      pin row, so they still prove a refusal by name;
    - :10458, page-span comment;
    - :19561/:19575, `artifacts_by_source_reads_only_that_source`, `scan_budget: rows` and
      `scanned == rows`.
- R2, the `artifact-retained:` byte is described as a full or partial count:
  docs/interfaces.md:653 and docs/architecture.md:565 (source_summary.rs `Mode`).
- R3, stale listing text rewritten:
  - MCP `result_next` and `artifacts_list` descriptions,
    crates/sparrow-server/src/agent/tool/catalog.rs:67 and :75: per-source index pages,
    ordered by source key, then receipt, then ID; receipt windows find reclaimed parents;
    INDEX_NOT_READY while seeding.
  - `sources_list` description, catalog.rs:71: counts come from counter rows.
  - docs/interfaces.md:364: `sources` reads the counter rows, not the artifact index.
  - rpc.rs:2975: comment.
- O1, deterministic migration concurrency, `source_index_migration_seeds_exact_counters_and_fences_older_writers`
  (storage.rs:19589):
  - Eviction order is pinned (each pressure pass must retire the next bulk artifact's
    record).
  - Expiry runs bounded passes until there is no continuation (:19625). Default passes
    retire at most 32 records, which had left orphan records older than the bulk.
  - After the first page, the test evicts until one evicted artifact is at or before the
    artifact watermark and one is beyond it (:19723).
  - It then ingests until a new artifact lies beyond the watermark (:19747). That is the
    path where a later page skips a row already counted Full.
  - 17,000 more records make the record phase take more than one page. During the record
    phase, the orphan's lowest record (at or before the record watermark, asserted) and
    highest record (beyond it, asserted) are released and expired. This exercises the
    observer's Partial-row path on both sides of the watermark.
  - The finished counters equal a recount, and the orphan's row is gone.
- O2, exact `scanned` in `source_listing_cost_is_independent_of_store_size`
  (storage.rs:19295/:19317): 3 counter rows + 3 bounded registry entries (producer,
  receiver, file-generation) for all sources, and 1 + 3 for one source.
- O3, a complete migration's totals come from its recorded state. This is one
  implementation in crates/sparrow-core/src/storage/migration.rs. The existing two
  migrations also recounted on every status call, so all three now share the fix.
  - `TOTAL_PREFIX` :32 and `total_key` :112.
  - `migration_page` :171: the page that completes a migration puts `migration-total:NAME`
    (counted once, under the writer lock, in the maintenance duty) in the same batch.
  - `migration_complete` :214 reads only the checkpoint; `pending_migration` :149 uses it.
  - `migration_status` :230 counts keys only for a running migration and reports the
    recorded total for a complete one.
  - A migration that completed before totals were recorded (for example the live store's
    two schema 69 migrations) is counted once per session and cached in `State.totals`
    (:109).
  - The shared fence deletes a re-fenced migration's recorded total (:128/:132/:136).
  - Documented in docs/interfaces.md:620.
- O6: crates/sparrow-core/src/storage/result_read.rs:5 comment updated. sparrow.capnp is
  untouched.
- O4: the measurement paragraph is left to the coordinator, as directed.

Review 1 checks, one cargo invocation at a time (logs/):

| Check | Log | Exit |
| --- | --- | --- |
| affected storage tests (8) | r1-storage-targeted-1.log | 0 |
| `cargo fmt --all --check` | r1-fmt.log | 0 |
| sparrow-core tests (36 lib + 227 storage) | r1-core-tests-2.log | 0 |
| sparrowd maintenance tests (5) | r1-server-maintenance.log | 0 |
| sparrow-server rpc tests (83) | r1-server-rpc.log | 0 |
| clippy, three crates, all targets, `-D warnings` | r1-clippy.log | 101 (`while_let_loop` in the new pin loop) |
| clippy after the loop fix | r1-clippy-2.log | 101 (`while_let_on_iterator`) |
| clippy after `for entry in ...` | r1-clippy-3.log | 0 |
| `make census-check` | r1-census-check.log | 0 |
| sparrow-core tests on the final loop (36 lib + 227 storage) | r1-core-tests-3.log | 0 |

## Not done, and exactly why

- The migration's duration on the newest closed-store backup
  (`deploy/verify-backup.sh --measure-migration`) was not measured. The task forbids
  running verify-backup against real data and touching /SPARROW or backups. The
  coordinator will run it after the commit and replace the pending paragraph in
  docs/load-qualification.md (review-1 O4).
- `make check` was not run. The task forbids it. The focused gates above cover formatting,
  release-profile Clippy with warnings denied, the sparrow-core tests, the affected
  sparrow-server tests and the census gate. `make census-check` ran the census with
  `--cross --check` against the pinned PurRDF revision. The rest of the workspace's tests
  were not run.
- The UAT steps need a deployed service, which is outside this worker's authority. They
  were not run.
- Nothing is committed or pushed, as the task requires.
- Pre-existing, not changed: prose-check reports `UInt64`/`UInt32` joins in
  docs/interfaces.md at lines that predate this change. None are in the edited text.
