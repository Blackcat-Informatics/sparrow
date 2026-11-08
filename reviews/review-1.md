# Delivery review 1: issue 14, `sources` and `artifacts` on a store of any size

## Scope

- Worktree `.worktrees/14-make-sources-and-artifacts-work-on-a`, base `main` 3a64e08. The work is uncommitted: 23 modified files plus the untracked `crates/sparrow-core/src/storage/source_summary.rs`.
- Read myself: `issue.md` (body only; `raw/issue.json` confirms 0 comments), `plan.md` with its Decisions and `## User rulings`, and `implementation.md`. I treated the implementer's notes as claims and checked them against source.
- Repository rules: AGENTS.md, the Rust skill and the house guide.
- Code read in full: `source_summary.rs`; the diffs of `durability.rs`, `migration.rs`, `source_read.rs`, `artifact_read.rs`, `writer_format.rs`, `manifest_retire.rs`, `rdf_index.rs`, `source_context.rs`, `source_time.rs`, `diagnostic.rs`, `temporal_index.rs`, `storage.rs`, `lib.rs` and `maintenance.rs`; test-support; `helpers-ledger.toml`; the docs; `deploy/verify-backup.sh`.
- Also read: the new and rewritten tests in `crates/sparrow-core/tests/storage.rs` and `crates/sparrow-server/tests/rpc.rs`, plus the writers of `record:`/`artifact:` keys (ingest in `storage.rs`, import publish in `publication.rs`, `capture_pins`, `catalog_upgrade` users, every `write_opt`/`delete_range`).
- Not run: no cargo invocation (none was needed for runtime proof; the one finding that depends on runtime is proven by the shipped test's own assertion), no `make check`, no UAT. I did not touch /SPARROW, the live service, backups or ~/.config/sparrow.

## Evidence

- Implementer logs, final tree. Each log's mtime is later than the last source edit it covers (source_summary.rs 19:59:17, ledger 19:59:59):
  - `logs/fmt-3.log` EXIT=0.
  - `logs/clippy-4.log` EXIT=0: release Clippy for core, server and test-support, all targets, `-D warnings`.
  - `logs/core-tests-5.log` EXIT=0: 36 lib and 227 storage tests passed, including all seven new storage cases and the rewritten `artifact_discovery_keeps_reclaimed_parent_unknown_and_never_invents_complete_bytes`.
  - `logs/server-rpc-3.log` EXIT=0: 83 passed, including `sources_listing_does_not_hold_the_writer_lock` and `artifact_listing_shares_typed_pages_unknown_origins_and_agent_refusals`.
  - `logs/server-maintenance-3.log` EXIT=0: 5 passed, including `reads_report_pending_mid_migration_and_the_worker_finishes_it`.
  - `logs/census-check-2.log` EXIT=0: 317 ledger jobs and 1 distinct row hold, cross-checked against pinned PurRDF c6d670c7.
- `make check` was not run (the task forbade it). Its status stays open until the commit hook runs it. The UAT has not run either; it needs a deployment.
- `prose-check` on all added doc lines (`git diff -U0 docs/`): exit 0, clean.
- `probe.log` shows a diagnostic `PROBE` run. A grep finds no `PROBE`, `dbg!` or new `eprintln!` in the changed sources and tests, so the probe was removed.

## Judgement on the specific questions

1. **Acceptance and named tests.**
   - All seven named tests exist on the shipped entry points (`Store::source_read`, `Store::artifact_read`, the RPC `sourcesList`, `migrate_step`/the maintenance worker). The lifecycle test covers ingest, import publish, typed producer events, pressure eviction (`evict_oldest`), expiry with a pin and pin release. Each step is checked against a recount and against the listed counts.
   - `source_listing_cost_is_independent_of_store_size` stores 20,000 records in 3 sources, checks a record catalog of at least 4 MiB, and lists all sources and one source at `scan_budget = 10`.
   - The budget in `artifacts_by_source_reads_only_that_source` departs from the issue; see R1.
2. **Counter exactness.**
   - Every runtime write goes through `Writer::apply` (durability.rs:143). The only runtime `write_opt` is durability.rs:233, and `catalog_upgrade::commit` writes no catalog entries. No `delete_range` or merge touches `record:`/`artifact:` at runtime, and merges on observed keys are refused. So the observer sees import publish (one batch, publication.rs:543/581), ingest (storage.rs:1767/1790), eviction, manifest retirement (`artifact-retirement:` byte 16), expiry, retirement and migration pages.
   - The derived rows ride the same atomic WAL batch. A buffered (unsynced) write loses them together with its catalog change. Artifact IDs are random (`random_id`), so a refused "new artifact already has a retained count" cannot be reached by re-ingest.
   - I traced the migration observer by hand: the artifact phase on both sides of the watermark, created artifacts, entry deletion with pinned survivors on either side, Partial rows in the record phase, and a Partial row dropping to 0 and being re-added by a later page. I found no double count and no missed row.
   - Pages run under the writer lock and commit through `apply`, so they serialize with writes. A restart resumes from the atomically committed checkpoint.
3. **Departures.**
   - (a) INDEX_NOT_READY while the migration is pending is acceptable. The plan's Decision delegated the choice. Neither `SourceSummary` nor the live page has a coverage channel, and the issue forbids a wire change. The documented precedent holds: strict reads refuse while schema 69 is pending.
   - (b) BLAKE3 is acceptable. It is already the store's source-key hash (`source_context::prefix`, `source_time::source_key`, `diagnostic::source_key` on main all use it), and the change consolidates those callers on one helper. It is an internal key, not on the wire, and needs no new dependency. Its observable behavior is identical to SHA-256.
   - (c) The listing order change is what the issue mandates ("walks the `source-artifact:` index source by source"). The rewritten tests still assert the protected behavior: no invented origin and no invented complete bytes. The new answer is proven, because `fold_index_record` checks each record's `received_at` and source hash against the index row. The rewrite is justified, but two shipped texts still state the old behavior (R3).
   - (d) This is not an open-time rebuild. The fence adds a `delete_range` per family plus the checkpoint to its one synchronized batch, which costs O(1) and does no scan, and the recount runs online through the migration. No production binary can leave these rows under an older identity, so the path only matters for fixtures.
   - (e) The +1 does not meet the issue's contract; see R1.
4. **Discipline.**
   - There are no clamps. `shift` and `checked_add_signed` refuse and name the source. The `unwrap_or(false)` on an absent retirement state and the skip of a manifest record deleted after its deadline are legitimate absences, not swallowed errors.
   - The writer-lock snapshot in `sources` is correct: the registries and `db.snapshot()` are read under the lock, the lock is dropped, then every row is read from the view. The RPC test proves an ingest completes while a listing is paused after the snapshot.
   - JSON goes through `purrdf_lex::json`, hashing through the existing blake3/hex re-exports, and both batch observers share `observe_batch`. The census passes and the ledger rows are updated.
5. **Measurement doc.** See O4.

## Findings

### R1 (REQUIRED): `artifacts --source A` needs A's row count + 1, against the acceptance text and the issue's charging rule

- What the issue says:
  - The acceptance: "`artifacts --source A` succeeds with `scan_budget` equal to A's row count".
  - The Work section: "The scan and byte budgets are charged only for entries it reads."
- What ships: `artifacts_by_source_reads_only_that_source` (storage.rs:19529-19553) uses `scan_budget: rows + 1` and asserts `page.scanned == rows + 1`. So at `scan_budget = rows` the shipped entry point cannot finish the listing in one page.
- Cause: `Writer::capture_pins` (storage.rs:1174-1177) calls `admit()` at the top of every loop turn, before it checks that a `pin:` row exists. The terminal probe of an empty pin range is therefore charged as an entry when no entry is read. `artifact_protected` (artifact_read.rs:1111) counts that charge against `scan_budget`.
- The implementer kept it because changing the shared helper "is outside this issue". That is a scope choice made on the implementer's own authority, which the 2026-11-07 ruling forbids.
- Correction: charge pin admission only for a pin row actually read (move the `admit()` after the prefix check), update the exact `scanned` expectations this changes, and set the test to `scan_budget: rows` with `scanned == rows`. The alternative is an explicit ruling from Patrick accepting the constant, recorded under `## User rulings`.

### R2 (REQUIRED): the docs misdescribe the `artifact-retained:` mode byte

- docs/architecture.md (section 7 list) says the row holds the count "and whether its entry is still present".
- docs/interfaces.md ("Per-source counters and artifact index") says it holds the count "and whether its entry is still present".
- The code disagrees. The byte is `Mode::Full`/`Mode::Partial` (source_summary.rs:226-233, 260). Partial means only a parent whose entry was deleted *before the migration reached it*. A Full row persists after its entry is deleted, for example on expiry with a pinned survivor (the observer keeps `mode`, source_summary.rs:689).
- Correction: describe the byte as whether the count covers all the artifact's retained records (full) or only those the record phase has passed (partial, a parent deleted before the migration).

### R3 (REQUIRED): shipped text still states the replaced listing behavior

- `crates/sparrow-server/src/agent/tool/catalog.rs:75` is the MCP `artifacts_list` description. It says "a live listing reads receipt-ordered pages" and "Original receipt-time filters refuse reclaimed parents with unknown origin metadata". Both are now false for live listings: the unfiltered order is source key, then receipt, then ID, and a receipt window finds a reclaimed parent (the rewritten tests prove it). `catalog.rs:67` (`result_next`) repeats "receipt-ordered page".
- `docs/interfaces.md` (the paragraph closing "Live artifact pages (schema 66)", about line 363) still says "The `sources` inventory reads the same indexes over its whole interval and remains one immutable capture". `sources` now reads counter rows, not the artifact index.
- Correction: update the two MCP descriptions and that sentence, and run `prose-check` on them.

### O1 (OPTIONAL): migration concurrency coverage is probabilistic

- In `source_index_migration_seeds_exact_counters_and_fences_older_writers`, the mid-migration ingest and eviction hit artifacts with random IDs. They fall beyond the 512-artifact watermark only about 15% of the time.
- So the paths "uncounted artifact changed, the migration reads it later" and "a page skips an artifact a write already counted as Full" are exercised only on some runs. The observer's Partial-row path (writes during the record phase) is never exercised.
- I read these paths as correct. A deterministic case would pin them, for example by choosing the evicted record by its artifact's position relative to the checkpoint, and adding one write during the record phase.

### O2 (OPTIONAL): a redundant assertion

`source_listing_cost_is_independent_of_store_size` asserts `scanned <= 10`, which the budget of 10 already guarantees. An exact `scanned` would pin the documented "one entry per counter row".

### O3 (OPTIONAL): extra status cost

`Store::migrations()` holds the writer lock and `migration_counts` counts every `artifact:` and `record:` key on each `status` call, even after completion. This follows the existing precedent for the other two migrations, but it adds one more catalog key walk under the lock per status call.

### O4 (OPTIONAL): wording for the pending measurement

- Once a number is filled in, the doc text in load-qualification.md will be correct, provided:
  - the "That measurement is pending" paragraph is replaced, not appended to;
  - the record states which migrations the backup copy owed. `backfill_seconds` in verify-backup.sh:399 times all pending migrations together, not `source-summary` alone. The per-minute `MEASURE progress` lines let you split the time per migration if needed.
- The script needs no change: it waits on every row of `status.migrations`.

### O5 (OPTIONAL): availability gap during the migration

While the migration is pending, a live `artifacts --source` refuses. On main it was served through the source record index. This is acceptable under the plan's Decision; the window lasts as long as the pending measurement shows.

### O6 (OPTIONAL): two stale comments

`crates/sparrow-client/schema/sparrow.capnp:60` and `crates/sparrow-core/src/storage/result_read.rs:5` still say "receipt-ordered page". Leaving the capnp comment alone avoids touching the wire schema file.

## Open state (not findings)

- `make check` has not run.
- The duration measurement is pending.
- Nothing is committed, pushed or merged.
- The UAT has not run.

VERDICT: BLOCKED
