Reviewer model: claude-opus-5-5 (opus)
VERDICT: PASS

# #176 / PR #190 independent delivery review

## Scope

- Head reviewed: `955948bd7f3b6c5f631ef17b05f625381d735223` (branch
  `paudley/176-keep-the-source-summary-owed-completion`), base `bce6adae2d56` (main). One commit,
  12 files, +780/-153.
- Contract: issue #176 with both comments (`.stage/176/issue.txt`). The binding decision and the
  restated acceptance (1)-(4) are in the 2026-11-10 06:02 comment. Rulings: the three entries in
  `plan.md` `## User rulings`: build (b) plus c3 plus per-migration timing in the journal lines,
  test resume under either order, do not build option (a); no new loop or timer firing more than
  once a second; land with `Refs #176`. Design: `.stage/176/design.md` section 4.
- Method: a static read of the diff, the touched functions in full (`source_summary.rs` passes,
  `Base`, `Pass`, `Completion::owed`, `artifact_page`, `record_page`, `count_artifact`;
  `listing_build.rs`; `source_read::source_capture`; `artifact_read::owed_index_rows` and the
  window and page path; `migration.rs` `pending_migration` and `migrate_step`;
  `store_counters` `SourceSummaryDone` terms and `Positions`; `maintenance.rs` journal and worker
  select), the four new tests and the shared helpers they use, the docs and the ledger diff, and the
  logs. I ran no cargo, make, tests or prose-check.

## Evidence

- `.stage/176/hook1.log`: commit hook for 955948b. Covers `cargo fmt --check`, release workspace
  clippy `-D warnings`, release workspace tests, census self-test and the pinned cross check.
  Result `EXIT=0`, 12:27:40Z to 12:37:09Z. The commit line `[... 955948b] Run source-summary
  second ...` is inside the log, so it ran at this head. All four new tests show `... ok` (log
  lines 273, 372, 387, 409). So do the order-sensitive existing tests:
  `reads_report_pending_mid_migration_and_the_worker_finishes_it` (546),
  `every_read_answers_completely_while_online_migrations_run_and_intake_flows` (400) and
  `a_live_listing_across_migration_pages_and_expiry_lists_the_finished_index` (206). `storage`
  has 246 passed, 0 failed.
- `prose.log` (EXIT=1): the only hits are pre-existing `UInt64`/`UInt32` lines outside the
  edited sections. I confirmed that none falls in the edited ranges (interfaces.md 626-672,
  842-848, 892-910).
- Commit message ends with `Refs #176`, not Closes, as ruled. No attribution.
- No wire or format change: the diff touches no capnp, client or `writer_format` file.
  `sparrow_client::SCHEMA_VERSION` is still 71. No new derived row family, so option (a) is not
  built.

### Acceptance mapping

1. **Run order.** `Migration::RUN_ORDER` (migration.rs:85) is used by `pending_migration`
   (migration.rs:296), the one production caller, reached through `migrate_step`, which the
   maintenance worker calls. `ALL` and `index()` stay the status order. Test
   `the_worker_runs_the_source_summary_migration_directly_after_the_store_counters` puts all five
   migrations in the pending state and checks two things: the pages run back to back in
   `RUN_ORDER`, and `migrations()` keeps the `ALL` names. Under the old order the sequence
   assertion fails. Met.
2. **Re-base.** `Worker::entry` checks every 4,096 entries. It calls `Base::overtaken`, which
   decides "complete, or checkpoint moved and done-since-base > read + 4,096". Its units are
   consistent. `done:source-summary` counts `artifact:` keys through the artifact watermark plus
   `record:` keys through the record watermark (store_counters.rs:186-197, 363-372). The pass
   counts one entry per artifact entry, then one per record entry. When the check stops the pass,
   `Completion::owed` returns `Pass::Overtaken`. Both callers then loop with a fresh snapshot,
   fresh `Base` and fresh `total`. `source_capture` also re-reads the registries under the writer
   lock. Everything counted so far is dropped, because `Completion` and `deltas` are rebuilt for
   each pass.
   - Exactness: each result comes from a single snapshot through the unchanged counting law, so
     no rows are lost or duplicated.
   - Termination: each re-base needs the live checkpoint to have moved and at least 4,096 pass
     entries, and the migration is finite.
   - Error paths: `Base::read`, `overtaken`, the checkpoint and the lease failures all propagate
     with `?`. Nothing is swallowed.
   - Test-support counter: `entries` is added before `State::Done` is published, so the tests'
     reads are ordered after it.

   Tests:
   - `a_source_listing_build_the_migration_overtakes_delivers_the_finished_rows_at_its_next_check`
     covers the completion case (sources). Without the change the build reads all 5,010 owed
     entries (`read <= 4_096` fails) and omits `rebase-late`.
   - `a_live_artifact_window_build_re_bases_onto_a_newer_snapshot_mid_migration` covers the
     mid-migration case (artifacts). Without the change `entries == owed` (24,010), so
     `entries < owed` fails. I traced the page sizes: artifact pages of 3 and 3, then one record
     page of 16,384, which gives done = 16,390 and leaves the migration unfinished.

   Met.
3. **Listings after completion read rows directly.** This was already covered by existing tests,
   and they pass at this head. Met.
4. **Per-migration timing.** The journal now records each migration's start, end, pages and
   done. It prints a `start` line on the first page of the session, and `elapsed_seconds` on
   progress and complete lines. A failed page returns `Err` and never reaches `journal.page`, so
   "first committed page" is accurate. This is documented in interfaces.md 658-672 and
   deployment.md 970-974. The 2.67M run is excluded, as ruled. Met.
5. **Resume under either order (ruling).** Test
   `a_store_part_way_through_its_migrations_under_either_order_resumes_to_the_finished_rows`
   covers two states:
   - The state the old order leaves: backfill part way, source-summary not started. Next the
     source summary runs to the end while the backfill watermark holds.
   - The state the new order leaves: source-summary done, backfill pending. It resumes at the
     same watermark.

   The test then checks counters against a recount, the owner index against the declarations,
   and the listings and fallback facts against the pre-migration answers. Under the old order its
   `SourceSummary` assertion fails. Met. An older binary resuming a new-order store cannot be run
   in tree. That direction rests on per-checkpoint independence, which I confirmed in
   `pending_migration` and `Positions::read`.

### Other checks

- One-second rule: no new timer or polling loop. The re-base check is work-proportional and rides
  the existing 4,096-entry check. The tests wait on the build's condvar (`await_listing_builds`,
  60 s budget). The 100 ms build wait has precedent at base (storage.rs 20004 and 20115) and is a
  single wait, not a loop.
- Progress semantics: `read - rebased` saturates; `rebased <= read` always holds anyway.
  `total(None)` now stores 0. This is the same as before, because a fresh `Shared` starts at 0 and
  a re-base never needs an unseeded total.
- Ledger: the `listing-build` row names `Worker::entry`, `Worker::rebase` and
  `Base::overtaken`. The census held in the hook.

## Findings

1. **OPTIONAL**: `crates/sparrow-core/src/storage/source_summary.rs:978` (with
   `listing_build.rs:121` and `docs/interfaces.md:898`).
   - Problem: in the artifact phase, `entry` runs once per artifact entry, while
     `count_artifact` point-reads and parses every record of that artifact. So the re-base check,
     and the existing artifact-build lease and cancellation check, fire only every 4,096
     artifacts. At about 1,000 records per artifact (the design's fixture; the live count was not
     checked) a build never reaches a check in its artifact phase.
   - Effect: a build started during the migration's artifact phase does its whole artifact
     remainder, about half a full pass, before its first chance to re-base. Restated acceptance
     (2) still holds as worded, in entry units. In practice the build reaches its record phase
     before the migration completes, because the migration has the record phase still to do.
     However, "redundant passes stop competing" only applies from the build's record phase on,
     and "every 4,096 entries" in the doc does not tell the reader this.
   - Fix (either):
     - Run the re-base, lease and cancellation check on a separate work counter that also counts
       the records `count_artifact` reads, leaving `read`/`total` in their current unit.
     - Or state in the "Listing builds" paragraph that in the artifact phase the check counts
       artifact entries.
2. **OPTIONAL**: `crates/sparrow-server/src/maintenance.rs:114-115` and
   `docs/interfaces.md:668`.
   - Problem: `began` is taken inside `Job::Migrate`. A migration's first page is often
     admitted through the background `deferred_page` with `defer: false`, for example
     source-summary's first page right after store-counters' last. That wait happened before
     `began`, so `elapsed_seconds` leaves out the first page's writer deferral, at most
     max(100 ms, the previous hold). This contradicts "including the waits for writers before
     each page" and the code comment.
   - Fix: say "from the start of its first page's job", or take the instant when the deferral is
     spawned. The effect is sub-second.
3. **OPTIONAL**: `docs/interfaces.md:841`.
   - Problem: "the third online migration, `source-summary`" is unedited. It now matches
     neither the run order (second) nor the status order (fourth).
   - Fix: drop the ordinal or name the order it refers to.
4. **OPTIONAL**: `crates/sparrow-core/src/storage/migration.rs:85`.
   - Problem: `RUN_ORDER` is a new `pub` item, and nothing outside the module reads it.
   - Fix: make it `pub(crate)` per the visibility rule, unless public rustdoc for the run order is
     wanted (the public `migrate_step` doc links it).

No REQUIRED findings.
