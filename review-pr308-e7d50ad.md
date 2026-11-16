# Review: PR #308 (issue #304) at e7d50ad

Reviewed 2026-11-15 20:39 CST. Head e7d50ad6eb25125c8efe05b86c4d9e9502d97f66, checked with
`git rev-parse HEAD` in `.worktrees/304-archive-prepare-read-cost-grows-with`. The worktree
was clean. Base 5f6202b174a1a6da27acc593b71e4ac7dc703dd9 (the merge-base with origin/main).
The PR spans 7dcd8bd and e7d50ad, and I reviewed the full `git diff 5f6202b..e7d50ad`.

Open state: `origin/paudley/304-archive-prepare-read-cost-grows-with` is still at 7dcd8bd, so
e7d50ad is not pushed. The forge PR body describes 7dcd8bd (see finding 5).

## Scope

What I read:
- Requirements:
  - Issue #304 (`stagectl issue 304`), which has no comments.
  - The PR #308 body (`tea pulls`).
  - The previous review `.stage/archive/review-pr308.md`. Its finding 1 is the open REQUIRED item.
  - `docs/architecture.md` §4.4 (lines 280-300) and the writer-before-pages text (lines 618-632).
  - AGENTS.md: Tests, Errors, Evidence.
- Diff and head source:
  - `rdf_view.rs:1288-1549`: `with_rdf_records`, `with_rdf_record_layers`,
    `with_rdf_scope_ids`, `capture_scope`, `ScopeCapture`, `Capture` and `RowAllowance`.
  - `rdf_view.rs:540-600` (point and charge accounting) and `:1131-1180` (`resolve`).
  - `rdf_view/scope.rs:134-205`: `include_owner`, `allow_owner_rows` and
    `include_artifact_owner`. Also `:343-355` (`rdf_read_lease`). Every `include_owner` call
    site (`:202,258,284,295,320`) is the only route by which owners join a scope; `rdf_view.rs:317`
    serves only the non-scope constructor path.
  - `rdf_view/budget.rs`: `allow` and the ceilings in `admit`.
  - `rdf_index.rs:172-185` (`manifest_stream_counts`).
  - `storage.rs:741-746` (`live_record`), `:1367-1418` (`protected_matching`, `capture_pins`)
    and `:1878-1900` (`lock`, `lock_page`, `lock_uncounted`).
  - `source_time.rs:521-560` (`validate_event_record`, both forms) and
    `store_counters.rs:395-430` (`Reader::db` and `Reader::snapshot`).
  - `writer_queue.rs`, the whole file.
  - `archive_segment.rs:143-518`: `select`, `archive_held`, `archive_prepare` with its halving
    loop, and `build_segment`.
  - `archive_encode.rs`: the full diff.
  - `archive.rs:339-380` (`archive_range`) and `:404-470` (`archive_intend`, `archive_commit`).
  - `eviction.rs:531-576` (`eviction_entries`).
  - `sparrow-server/src/archive.rs:435-468` (`drain`).
- Tests:
  - `tests/archive_segment.rs:204-246`.
  - `tests/storage.rs:12829-12910` (`stored_rdf_snapshot_survives_concurrent_pin_release_retirement_and_ingestion`).
    This test drives the new two-pass capture: a pinned expired record at `now = 31 days`.
- PurRDF (sibling, read-only): `crates/markdown/src/claims.rs`, only to check whether Markdown
  projections emit triple terms. They do: `rdf:reifies` lines at `:258-281` and `:448-455`.
- Evidence logs in `.stage/archive/lane304/`: `cal2-log-1500.log`, `cal2-json-800.log`,
  `lock2-log-50000.log`, `storage-two.log`, `archive-segment-final.log`, `commit-hook.log`
  (searched for result lines and the tail), and `lock-hold.md`.
- `prose-check` on the 85 added comment lines and on the e7d50ad commit message: both EXIT=0.
  The inputs and logs are in `.stage/archive/review-e7d50ad/`.

What I did not read or run:
- No cargo, build, test or hook, as instructed. The figures below come from the logs.
- The probe source (`probe304*.rs`) and the instrumentation that printed `PROBE304`. Neither
  is committed.
- The internals of PurRDF `DatasetImporter`, so the per-row cost model is inferred from the
  charge sites, not traced end to end.
- `include_record` beyond its owner calls, and `hold_reason`.
- The rest of `docs/interfaces.md`.

## Evidence

Each log names the worktree, prints its own `EXIT=` line, and names HEAD 7dcd8bd: they ran on the
uncommitted tree that became e7d50ad.

| log | profile | scope | result | figures |
|---|---|---|---|---|
| `cal2-log-1500.log` | release | probe, 1,500 log records | EXIT=0 | `worst_ingest_us=2735`, 379,509 requests, 48,056 row visits |
| `cal2-json-800.log` | release | probe, 1 json record, 800 keys | EXIT=0 | `worst_ingest_us=4351`, 252,910 requests, 24,110 row visits |
| `lock2-log-50000.log` | release | probe, 50,000 log records | EXIT=0 | `worst_ingest_us=8727`, `total_ms=31804`, 15,244 ingests, 12.4M requests |
| `storage-two.log` | debug | 2 storage disk tests | EXIT=0 | 2 passed |
| `archive-segment-final.log` | debug | `tests/archive_segment.rs` | EXIT=0 | 7 passed, both new tests included |
| `commit-hook.log` | release | full `make check` | EXIT=0 | 19 result lines, all ok |

The claimed figures appear as stated: 8.7 ms and 31,804 ms at 50,000, 2.7 ms at 1,500, and
4.4 ms for json-800.

Notes on the evidence:
- **The cal2 runs used a probe allowance.** Their printed allowance is 2,000 per row
  (36,040,000 = 2,000 × 18,020 rows; 24,086,000 = 2,000 × 12,043). They measure cost, not the
  committed 42/4 ceilings.
- **The 50,000 run used the committed ceilings.** Its allowance is 42 × 600,500 rows:
  - Requests: 12.42M against 26.03M. That ceiling is 10,000 + 50,001 × 16 + 25,221,000.
  - Row visits: 1.60M against 3.20M.
  - The margin is about 2× on both.
- **The timings ran on an instrumented tree.** The cal2 and lock2 trees carried the probe
  instrumentation, so they are not the committed tree.
- **The committed tree is covered by the hook.** `commit-hook.log` ends with the commit line for
  e7d50ad after the full gate. That run includes:
  - `archive_segment`: 7 passed, both new tests among them.
  - `storage`: 314 passed, including the two-pass pin test.
  - `archived_segment_reimports_byte_exact` and the census.
- `lock-hold.md` is stale. It describes the pre-fix tree (266 ms stall).
- Derived per-row cost: 21.0 requests per row for log and for json, and 2.0 to 2.7 row visits
  per row. This matches the commit message.

## What holds

1. **Capture snapshot correctness (`rdf_view.rs:1384-1517`).**
   - Every scope read goes through `view.snapshot`, and the snapshot is pinned under the
     writer. That covers catalog rows (`catalog_point`), provenance markers, manifests
     (`allow_owner_rows` uses `snapshot.get_pinned`) and graph rows.
   - The operation already ran after the writer was released in the base.
   - The only live-db read is the pin catalog (`protected_matching`, `:1405-1430`). It runs
     under the same hold that pins the snapshot, so pins and snapshot are as of one moment,
     as before.
   - Event-time validation moved from `Reader::db` under the writer to `Reader::snapshot`.
     The source profile bindings are immutable once bound, and the snapshot is the more
     consistent source.
   - `live_record` is the same free function the writer method forwarded to.
   - First pass with no expired records: protection is irrelevant, because `live_record` is
     true for `now < expires_at` and false for evicted records whatever the pins.
   - Second pass: an expired record not named by pass 1 fails with retryable `RESOURCE_LIMIT`
     (`:1465-1473`) instead of guessing. At a fixed `now`, `expires_at` would have to change
     between snapshots for this to fire, so it is effectively unreachable and has a way out.
   - The archive passes `now = 0` (`archive_encode.rs`), so it never takes the second pass.
   - I found nothing that lets a segment encode ineligible evidence, miss evidence, or encode
     a retired record that the base would not also have encoded.
   - Records retired between `select` and capture were already possible in the base, which
     also dropped the writer between select and encode. Such a record fails as
     `EVIDENCE_UNAVAILABLE`, then halves or errors, and the next pass re-selects it as retired.
     It is not stuck.
2. **Batched selection (`archive_segment.rs:189-341`).**
   - `eviction_entries` starts inclusive at `floor` in both prefixes. The `position <= floor`
     check skips at most the floor entry itself. The 257th entry is re-read by the next batch,
     not lost.
   - The artifact loop restarts at the unprocessed key inclusively.
   - Neither loop can spin, skip or duplicate.
   - The watermark is read once. Only the single archive worker moves it.
   - New entries arrive after the horizon. `archive_intend` re-verifies the range and counts
     under one hold (`archive.rs:404-430`). A mismatch is an error that halves or retries,
     never a wrong intent.
3. **`lock_page` and `writer_queue`.**
   - `defer_page` waits on the condition variable, bounded by `max(100 ms, last_page)`, and
     ends early when the last waiter withdraws. It holds no writer lock while it waits.
   - Archive pages do not register as waiters, so they and migration pages do not wait on
     each other.
   - The writer guard in `archive_prepare` (`:423-432`) is a temporary dropped at the end of
     the statement, before `select` takes the lock.
   - `capture_scope` takes the writer while it holds the `rdf_read` lease. Nothing takes the
     lease while it holds the writer, and the lease uses `try_lock`. No deadlock.
   - Starvation: every page is bounded, so the archive cannot be kept from preparing
     indefinitely. Under the deletion gate, pressure refuses data writes, waiting writers
     drain, and the deferrals end at once. That is not a state with no way out.
   - `last_page` is only the migration's last hold. Archive pages do not record theirs. That
     matches "as a migration page does" and is not a defect.
4. **The ceiling (finding 1 of the previous review is closed).**
   - `include_owner` is the only route by which an owner joins a scope. Each new owner adds
     42 requests and 4 row visits per row in its stored manifest (`scope.rs:157-185`).
   - The scope reads exactly those rows, so the allowance follows data the scope owns.
   - A single json record with about 12,000 rows now prepares. The test and the hook both show it.
   - Runaway reads still stop. The lease (60 s), `live_bytes` (2 GiB) and `io_bytes` (8 GiB)
     are fixed. A read costing more than 42 requests per stored row on average is refused.
   - The old growth is still refused at 5 records:
     - Row visits: 3,964 against about 2,416. That is 2,000 + 6 × 16 + about 80 rows × 4,
       taking 12 rows per log record plus the artifact's graph, inferred from the 18,020 rows
       of the 1,500-record run.
     - Requests: 14,161 against about 12,600 to 13,500.
   - `with_rdf_record_layers` keeps the 1..1000 query bound (`rdf_view.rs:1315-1320`).
   - `with_rdf_scope_ids` stays `pub(super)`. Its only callers are that function and
     `archive_encode`.
5. **Tests.**
   - `a_json_record_with_thousands_of_graph_rows_prepares` protects the finding-1 regression
     on the real `archive_prepare` and `archive_commit` path. It would fail at 7dcd8bd, where
     the ceiling was 11,024 requests against a cost of about 253,000.
   - The changed doc on the 1,100-record test corrects finding 3 of the previous review.
   - The uncommitted deadlock test never existed in a commit:
     `git log 5f6202b..e7d50ad -S deadlock` is empty. Its absence needs no justification.
6. **Leftovers.**
   - No `PROBE`, `probe_allowance`, `dbg!`, `eprintln!` or `println!` in the diff.
   - The only new `unwrap` and `expect` calls are in tests.
   - The `.expect("scope capture precedes owned query admission")` calls are pre-existing
     context lines.
   - No `probe304` file in the tree. New items are `pub(super)` or private, so no visibility
     is widened.
   - The `#[allow(clippy::too_many_arguments)]` at `rdf_view.rs:1340` matches an existing use
     in `scope.rs`.

## Findings

1. **OPTIONAL: Markdown is not calibrated, and its rows plausibly cost more per row than json
   or log.**
   - Per-row cost is driven by term resolution. `resolve` (`rdf_view.rs:1131-1175`) does one
     lookup for a literal's datatype and three for a triple term.
   - Sparrow's Markdown projection emits RDF 1.2 triple terms through `rdf:reifies` (PurRDF
     `markdown/src/claims.rs:258-281,448-455`). Json and log rows carry none.
   - The 2× margin probably absorbs this. The allowance is a scope total, and only citation
     rows carry triple terms.
   - If a large Markdown document did exceed 42 requests per row, that single record would be
     the no-way-out case that finding 1 described.
   - This is an inference; no defect was observed. One release probe on a large Markdown
     record with citations would settle it.
2. **OPTIONAL: archive reads hold the store's single RDF snapshot lease for a whole segment.**
   - `rdf_read_lease` (`scope.rs:343`) is a `try_lock`. While the archive reads, every
     `with_rdf_scope` query is refused with `RESOURCE_LIMIT` "store already has an active RDF
     snapshot".
   - With segments now up to 50,000 records, that window approaches the 31.8 s measured for
     the whole prepare. The log does not split out the read portion.
   - This is not a regression: the base held the same lease, and its quadratic read ran to the
     60 s lease.
   - It does bear on the ruling that backups "perturb no operations". I recommend a follow-up
     issue for a decision.
3. **OPTIONAL: worst-case prepare time under a saturated writer lock.**
   - `build_segment` takes one `lock_page` per record (`archive_segment.rs:486`). Each page may
     defer up to `max(100 ms, last migration page)` when writers are always queued.
   - The theoretical bound at 50,000 records is about 83 minutes. The measured 1 ms-paced
     ingest added little.
   - Reading record bytes in batches of `SELECT_BATCH` would cut the bound by about 256×. This
     is not a stuck state.
4. **OPTIONAL: lease headroom.**
   - 31.8 s against a 60 s lease at 50,000 records, with only the probe's ingest thread as load.
   - On a loaded daemon a full segment may expire. It would halve and retry, losing nothing,
     after spending up to one lease.
5. **OPTIONAL: text accuracy.**
   - The PR #308 body still describes the 7dcd8bd ceilings ("10,000 + 512 per entry ...
     64 per entry") and the old evidence. Update it with the push, because the merge record
     is built from it.
   - `tests/archive_segment.rs:237-238` says the entries-only ceiling "allowed 10,512". With
     one record and its artifact (2 entries), 7dcd8bd allowed 11,024.
   - The commit message's "ceiling of 12,600" at 5 records omits the artifact graph's rows.
     About 13,500 is likelier, and it is still below 14,161.
6. **OPTIONAL: documentation.** Two points in `docs/architecture.md` could be stated:
   - The archive's selection and record reads now defer to waiting writers. Today line 626
     names only the maintenance worker.
   - The archive read takes the single RDF snapshot lease. Neither omission is contradicted
     by the current text.

No REQUIRED findings. The previous REQUIRED finding 1 is resolved at this head.

VERDICT: PASS at e7d50ad6eb25125c8efe05b86c4d9e9502d97f66
