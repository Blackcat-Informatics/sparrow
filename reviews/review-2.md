# Delivery review 2: issue 14, recheck of the review 1 fixes

## Scope

- Recheck of the changed work only: R1, R2, R3, O1, O2, O3 and O6, as listed under "Review 1 fixes" in `implementation.md`. Unchanged history from review 1 is not re-judged.
- Read in the worktree:
  - `Writer::capture_pins` (storage.rs:1162-1196) and every caller's admission closure: eviction.rs:319, expiry.rs:114, retention_metrics.rs:192-209, ingest_status.rs:348, tail_ranges.rs:113, rdf_view.rs:1060, rdf_view/scope.rs:388 and artifact_read.rs `artifact_protected`.
  - The changed tests at storage.rs :10257, :10344, :10419, :10458 and :10764.
  - The rewritten `source_index_migration_seeds_exact_counters_and_fences_older_writers`.
  - The O2 assertions at :19295 and :19317.
  - All of `migration.rs` after the O3 change.
  - The text and doc diffs in catalog.rs, result_read.rs, rpc.rs, architecture.md and interfaces.md.
- Not run: `make check`, any test, and anything that touches /SPARROW or the live service.

## Evidence

- My one cargo invocation was `cargo fmt --all --check` on the final tree. It exited 0 (log `/tmp/review14/fmt-r2.log`). I ran it because storage.rs (mtime 21:15:28) was edited after the implementer's `r1-fmt.log` (21:02:56).
- `prose-check` on all added doc lines plus the changed MCP description strings: exit 0.
- Implementer logs. The last source edit is storage.rs at 21:15:28.
  - These ran on the final tree:
    - `r1-clippy-3.log` (21:16) EXIT=0;
    - `r1-core-tests-3.log` (21:24) EXIT=0, 36 lib and 227 storage tests passed.
  - These ran before the last storage.rs edit:
    - `r1-server-rpc.log` (21:12) EXIT=0, 83 passed;
    - `r1-server-maintenance.log` (21:09) EXIT=0, 5 passed;
    - `r1-census-check.log` (21:14) EXIT=0.
  - The notes say that last edit only reshaped the already-changed pin loop to satisfy Clippy (loop, then `while let`, then `for`). The behavior is the same: the server tests already ran with admission moved after the prefix check, which is the semantic change. Still, the server tests and the census have not run on the final loop shape. The commit hook's `make check` covers both.

## Rechecks

### R1: resolved

- **The helper.** `capture_pins` now iterates `prefix_iterator("pin:")`, breaks on the first non-`pin:` key, and calls `admit()` only after that check. The probe that ends the range is never charged. The per-row byte charge, size cap and decode are unchanged.
- **Callers.**
  - eviction.rs:319 and expiry.rs:114 dropped their `+ 1`. Before, `> scan_budget + 1` allowed `scan_budget` pin rows plus the probe. Now `> scan_budget` allows the same `scan_budget` rows, so their limits are unchanged.
  - ingest_status.rs:353 had no `+ 1`, so it now admits one more pin row, `scan_budget` in total. That matches "charged for rows read".
  - In retention_metrics the reported `visits` falls by the uncharged probe. Its comment is updated, and the rpc test compares typed and CLI `visits` against each other, not against a fixed number.
  - rdf_view no longer makes one budget request for the probe.
  - All of these follow from the fix review 1 asked for. No doc describes the probe charge.
- **The two refusal tests are not weakened.**
  - `artifact_capture_refuses_budget_and_corruption_without_publishing_prefixes` (:10257) and `artifacts_listing_page_over_its_bound_is_refused_by_name` (:10419) now create one pin before reading at `scan_budget: 1`. The pin is live: `days = 1`, read at `now = 1` ms.
  - Its row spends the only entry. The first index row's `admission.visit()` then fails with no progress, which reaches `refuse_row`.
  - The tests still assert `RESOURCE_LIMIT`, that no handle is published, and the page size, bound, artifact ID and receipt time named in the message. Neither test can reach that state any more without a pin row, so the pin replaces the old probe.
- **The count changes.**
  - `artifacts_listing_reads_only_its_page` (:10344, 5 to 4) and `artifact_selection_seeks_members_without_reading_unrelated_corrupt_catalog_entries` (:10764, 5 to 4) create no pins (grep of both bodies). The old fifth entry was the empty-range probe, so 4 is correct.
  - `artifacts_by_source_reads_only_that_source` now uses `scan_budget: rows` and asserts `scanned == rows`, which matches the issue's acceptance text.

### O3: correct, with one wording nit (O-a)

- **Exactness.**
  - The completing page computes `migration_counts(migration).0.1` under the writer lock, before its own batch commits, and puts `migration-total:NAME` in that batch.
  - No page changes the source-key population it counts: source-summary pages write only derived rows and the checkpoint, owner-graph pages write `rdf:o:` mirrors, and fallback pages rewrite graphs without adding any. So the recorded total equals the entries present at the instant the migration completed.
- **Crash consistency.** The total, the derived rows and the checkpoint are one atomic batch through `Writer::apply`. A crash leaves either all of them or none, and the page is retried.
- **No stale cache after a re-fence.**
  - `State.totals` lives in the `Writer`'s `RefCell`, built fresh by every `Store::open` (storage.rs:1482).
  - `migration::fence` is called only from `Store::open` (storage.rs:1446), so a re-fence always starts a new, empty cache.
  - The fence also deletes the re-fenced migration's recorded total in its own batch (migration.rs:128/132/136). That matters only when a re-fenced migration is complete at fence time (an empty catalog); otherwise a running migration reports live counts and its completing page overwrites the total.
- **Wire output.** The `MigrationStatus` fields and the status encoding are unchanged; no rpc or recompression source changed. One meaning changed: a complete migration's `total` is now frozen at completion, or at the first status read of the session for one that completed before totals existed, where before it tracked later ingest. The struct comment and interfaces.md say so. `MigrationStatus::complete()` (`watermark.is_none() && done == total`) still holds. verify-backup.sh reads only the watermark and the pending progress.
- **Hard failures.**
  - A recorded total that is not 8 bytes is a `STORAGE_FAILURE` naming the migration.
  - A count error propagates through `?`, and the cache is written only after a successful count.
  - A missing total row is a legitimate state, not a fault: the store completed before totals existed, or a new store is complete at fence. It is recounted, never invented. The cache hides no error, since status counting was never an integrity check.
- `pending_migration` now walks `Migration::ALL` through `migration_complete`. That is the same order and the same checkpoint reads as before.

### O1: resolved, and the coverage is real

The random artifact IDs remain, but each required path is now forced by a loop or guarded by an assertion that fails loudly. I checked each one by its mechanism:

- **Artifact phase, both sides of the watermark.**
  - Pressure eviction retires the bulk artifacts in receipt order, and the test asserts each report names the expected record.
  - Evictions continue until one evicted artifact is at or before the committed watermark and one is beyond it, capped at 500 with a named failure.
  - The watermark is the last of up to 512 of about 617 random IDs, so each eviction is a coin with p roughly between 0.03 and 0.85. A miss within 500 draws is negligible.
  - "At or before" takes the observer's Full-row path; "beyond" takes the path where it skips and the migration reads current state later.
- **A new artifact beyond the watermark.** Ingests continue, up to 200, until one ID exceeds the watermark. The watermark sits near 0.83 of the ID space at most, so a miss is negligible. A later page must then skip that artifact's Full row (the Full-row early return in `count_artifact`).
- **Record phase, Partial rows.**
  - The orphan has 2,000 records, and the pins hold its lowest and highest IDs.
  - 17,000 wide records force a second record page. The loop stops after exactly one record page, because the transition watermark is `record phase start`.
  - The test asserts `low <= record_watermark && high > record_watermark`. Releasing the pin and expiring then drives the observer's `Counted::Watermark` path on both sides: the counted record gives -1 and the row is deleted; the uncounted record is filtered.
  - The finished counters equal a recount, and the orphan's row is gone.
- The restart in the middle of the migration and both fence refusals are kept.

### R2, R3, O2, O6: R2, O2 and O6 match the code; R3 is fixed except one clause (R-a)

- **R2.** architecture.md:564 and interfaces.md:652 now describe the full/partial count, matching `Mode` and `record_page`.
- **R3.**
  - The `result_next` and `sources_list` MCP descriptions match.
  - interfaces.md:363-364 now says `sources` reads the counter rows.
  - The rpc.rs:2975 comment matches.
  - The `artifacts_list` description has one inaccurate clause; see R-a.
- **O2.** `scanned == 3 + 3` (all sources) and `1 + 3` (one source) match the code: one entry per counter row, plus the producer, receiver and file-generation registry entries. No source in the test lacks a counter row, so no probe is charged.
- **O6.** The result_read.rs:5 comment matches. sparrow.capnp is untouched, as suggested.

## Findings

### R-a (REQUIRED): the MCP `artifacts_list` description overclaims for explicit scopes

- `crates/sparrow-server/src/agent/tool/catalog.rs:75` now says, without qualification: "Original receipt-time filters find reclaimed parents at their indexed receipt, without origin metadata".
- That holds only for the live listing (`finish_row(..., indexed = true)`).
- An explicit scope still runs the capture path. `ArtifactCapture::push` (artifact_read.rs:571) and `finish_row(..., false)` (:1217) call `artifact_eligible`, which still refuses `INDEX_NOT_READY` for an unknown origin under a receipt filter (artifact_read.rs:1240-1243). interfaces.md:2437 still documents that refusal. The agent-facing text and the code therefore disagree for explicit scopes.
- Correction: scope the clause to the live listing, and keep the explicit-scope refusal. For example: "a live listing's receipt-time filters find reclaimed parents at their indexed receipt, without origin metadata; an explicit scope's refuse INDEX_NOT_READY for an unknown origin".

### O-a (OPTIONAL): one sentence in interfaces.md contradicts the O3 rule

The `Status.migrations` paragraph (interfaces.md:610-611) still opens with "`total` counts the migration's source entries now in the store". The O3 sentences later in the same paragraph say a complete migration reports its recorded total. The paragraph corrects itself, but "now in the store, while it runs" would remove the contradiction.

### O-b (OPTIONAL): the completed-Partial wording could be sharper

interfaces.md:656 says of a partial count that "the migration completes it". The count does become complete. The mode byte, however, stays partial after the migration finishes, and the observer then counts every record (`record_counted` is true once complete). "Afterwards it counts every retained record, though still marked partial" would be exact.

## Open state (not findings)

- `make check` has not run.
- The server tests and the census have not run on the final pin-loop shape (see Evidence); the commit hook covers both.
- The migration duration is not yet measured. The coordinator owns that and the review 1 O4 paragraph.
- Nothing is committed, pushed or merged.
- The UAT has not run.

VERDICT: BLOCKED
