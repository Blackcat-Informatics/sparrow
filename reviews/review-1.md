# Delivery review 1: issue #36, step 1 (page the schema 69 event-fallback job)

## Scope

- Worktree `/home/paudley/Active/sparrow/.worktrees/36-bound-the-schema-69-first-open`, branch
  `paudley/36-bound-the-schema-69-first-open`, base `main` 758b41a. Uncommitted diff
  (`git diff`): `catalog_upgrade.rs`, `event_fallback_rdf.rs`, `provenance_rdf.rs`,
  `rdf_index.rs`, `docs/interfaces.md` (294 insertions, 61 deletions). No other files changed.
- Contract read from `issue.md` and the two verbatim rulings in `plan.md` (2026-11-07 about
  06:05 and 06:35 CST, lillith_coordination as Patrick). The 06:35 ruling sets the order:
  page #20 first (many graphs per page, one synced commit per page, skip source-`eventTime`
  graphs without decoding). Then re-measure on a copy under unit-like limits. Step 2
  (online duty) is required only if that open takes more than 10 minutes. The diff is step 1
  only, which matches the rulings. No `validation.md` exists. `implementation.md` was
  treated as claims.
- Not reviewed: the #13 owner-index job (unchanged) and step 2 (not started; it depends on
  the measurement).

## Evidence

Commands I ran:
- `cargo test --locked --release -p sparrow-core --lib storage::` exited 0 with 35 passed,
  including `catalog_upgrade::tests::byte_frontiers_resume_after_reopen_and_bad_pages_publish_nothing`
  and `event_fallback_rdf::tests::pages_share_one_writer_skip_source_times_and_resume_after_a_failed_page`
  (log `reviews/review-1-unit.log`, `EXIT=0`).
- `prose-check` on the added `interfaces.md` lines and the added code comments exited 0 with
  no findings.

Implementer logs I inspected but did not rerun: `it-storage-final.log` (220 passed),
`clippy-final.log` (release, `-D warnings`, finished), `it-rpc.log` (5 selected legacy-open
RPC tests passed, run before the last test-helper and docs edit), and the probe logs
`probe-before.log` (10,000 graphs, 198.2 s), `probe-after-10000.log` (3.0 s), `perf-run.log`
(60,000 graphs, 13.1 s, 15 pages) and `probe-after-151000.log` (105.4 s). The probe source was
removed, so I could not inspect its graph shape beyond the report's description. `make check`
was not run, per the brief. It runs in the commit hook.

What I verified by reading the code:

1. **Other jobs are unchanged.** `Job<P = Publish>` keeps every existing literal at `P = Publish`.
   `impl Publication for Publish` has these properties: `open` returns `db`, `publish` calls the
   same function pointer with the same arguments, `full` is the default `false`, and `finish`
   does nothing. In `step`, `visited == maximum || (visited > 0 && full(..))` short-circuits to
   the old condition. The charge check, `remaining`, checkpoint `put`, single `admit` and
   synced `commit` are in the same order. The only new work for the other jobs is a no-op
   `finish` before the checkpoint `put`. `provenance_rdf::publish` now builds its own
   `RdfWrite::new(db)` before `stored_metadata` and still calls `rdf.finish(batch)` at line 140,
   one graph per page as before. The only effect is that a missing-counter failure would come
   before the manifest checks.
2. **Multi-graph page correctness.** One `RdfWrite` per page (`Fallback::open`) and one
   `finish` per page, in the same batch as the checkpoint. I traced `remove` and
   `graph_inner`: reference counts come from `self.references` and fall back to the committed
   counter only on the first touch, and new terms come from `self.terms`. So shared terms
   across the page's graphs accumulate correctly, and the new `eventTimeFallback` and
   `eventTimeBasis` terms are allocated once per page. Every graph ID in a page is distinct,
   so `replace_metadata`'s staged-owner refusal cannot fire, and `remove` reads committed
   manifests that the page has not changed. The test's final removal of every graph down to
   `rdf:term-count == 0` would underflow the `eventTimeFallback` reference if each graph used
   its own writer in one batch. So the test does protect the shared-writer law. A page fails
   atomically: the writer and batch are dropped before `commit`, and the test shows that
   `record:e` stays byte-identical and the checkpoint stays at `record:c`. Resume works from
   any old checkpoint, including one written with `page_entries: 1`, because the checkpoint
   format and key prefix are unchanged.
   - Page bounds: 4,096 visited entries (skipped graphs count too), 16 MiB of input
     (`charge = bytes.len()`), and a 16 MiB batch checked before each later entry. The decode
     allowance is now per graph: `json_charge(bytes, 32 MiB)` limits a manifest to about
     504 KiB, as before, and `GRAPH_BYTES - charge` is the term-decoding budget, as before.
     Worst case, a page before its last graph is under 16 MiB. The last graph's `remove`
     deletes plus the `rows * 576` reservation stay under 64 MiB, because the 504 KiB
     manifest cap keeps row counts far below 32,768. So `MAX_BATCH` cannot fail just because
     pages grew.
3. **Skip rule.** The old test was a decoded primary row with
   `values[s] == entity_iri(id)` and an IRI predicate `NS + eventTime|eventTimeFallback`.
   The new test is a stream-0 manifest row whose subject equals
   `known(entity_iri(id))` and whose predicate is one of `known(eventTime)` or
   `known(eventTimeFallback)`. If a term is missing from the dictionary, `known` gives `None`
   and the graph is not skipped. That is correct, because no committed row can reference a
   missing term. A term this page staged (`eventTimeFallback` on a first open) gets a fresh
   ID above every committed ID, so it cannot match a committed row. `known` never stages,
   so a term is never both staged and committed. Graphs with no catalog record are still
   covered: the job iterates `rdf:graph:record:` and never reads the catalog, and the test
   uses graphs that have no catalog record.
4. **The new hard failure cannot fire on a consistent store.** `stored_metadata` checks
   `rdf.closure(value) == id` for every decoded term through the same writer's `lookup`. So
   a decoded row whose subject and predicate values equal those IRIs has exactly the IDs the
   skip check looked up, and the skip would already have returned. The check only catches
   dictionary corruption that `stored_metadata` did not already refuse.
5. **One lookup.** `intern` calls `lookup` and stages only on a miss. `known` is
   `lookup(&encoded(value)?)`. The forward/reverse mismatch check and the bounded `stored`
   cache are shared. `stored_metadata` has three callers (event fallback, provenance, the
   legacy fixture) and all pass a writer. `StoredMetadata` no longer carries a lifetime.
6. **Counters exist before the job.** `Store::open` requires `rdf:next`, `rdf:term-count`
   and `rdf:max-encoded` before any upgrade (`storage.rs:1391-1393`). So opening the writer
   eagerly in `Fallback::open`, even when no record graphs exist, cannot fail on a store
   that the old code accepted.
7. **Per-page open cost.** There is one synced commit per page. `artifact_rdf::admit` walks
   the directory (`linked_physical_bytes`) only when the batch puts `provenance-rdf:` or
   `artifact-rdf:` keys. `graph_inner` writes `provenance-rdf:` on provenance-format stores,
   so every page with a rebuild walks the directory once. A page where every graph was
   skipped does not. At about 40 to 220 pages, fsyncs and directory walks together cost
   seconds, not minutes.

Timing credibility: the before/after ratio (198 s against 3 s at 10,000 graphs) is consistent
with removing about 10,000 synced commits and directory walks, and the old cost of about
20 ms per graph matches the observed 76 minutes. The absolute after numbers do not establish
the 10-minute bound:
- Cost per graph grows with N (about 0.30, 0.22 and 0.70 ms per graph at 10,000, 60,000 and
  151,000 graphs).
- The synthetic graphs have 6 to 8 quads, compared with about 20 rows in the report's
  estimate for real graphs.
- The probe was unconstrained, on a multicore workstation, with `open_test_db` default options.
- Under `CPUQuota=100%`, RocksDB's two background jobs share one core with the job. The total
  write volume (index deletes plus puts for every rebuilt graph, about 2 GB if every graph is
  rebuilt) is then compacted under `IOWeight=20`.

The remaining per-graph work is all CPU and random reads: three manifest parses, term point
reads and decoding, reverse and reference-count reads, and six index orders written twice. A
rough scale-up of 2 to 4 times for graph size and 1.5 to 2 times for one core gives about
5 to 14 minutes. The change could plausibly pass or fail the bound, and only the copy
measurement decides it.

## Findings

1. **REQUIRED: the issue's acceptance measurement is missing.** The issue's acceptance and
   the 06:35 ruling both require re-measuring the open on a copy of the 20261107T100413Z
   nightly under `CPUQuota=100%`, `IOWeight=20` and `MemoryHigh=2G`. Step 1 is accepted only
   if that open takes 10 minutes or less. Otherwise step 2 becomes mandatory. The only timing
   evidence is an unconstrained synthetic probe whose source was removed, and as shown above
   it does not bound the real open. Run the measurement with the image built from this
   change and record the command, the limits, the total open time and the page count. If the
   open takes more than 10 minutes, deliver step 2 as the ruling describes. Use the live
   unit's `disk_budget` and `maintenance_reserve` for the copy: admission now asks for about
   4 times the page batch plus 64 KiB per page (tens of MiB, up to about 200 MiB), where it
   used to ask for a single graph's worth. A store near its data-write ceiling could
   therefore stop with `STORAGE_PRESSURE` where it used to make progress.

2. **OPTIONAL: no test for the new batch bound.** No test covers the new `full` bound
   (`PAGE_BATCH`). `visited > 0 && full(..)` is what keeps a page under `MAX_BATCH`. Only the
   count bound (through `step(.., 3, ..)`) and the existing input-byte bound are tested. A
   small case in the existing pager test, with a publisher whose `full` returns true after
   one entry, would protect it. This is cheap, but the contract does not need it.

3. **OPTIONAL: docs and comment wording.** `docs/interfaces.md` says "Each page rebuilds up
   to 4,096 graphs". The 4,096 bound counts every graph visited, including skipped ones.
   "reads up to 4,096 graphs" would be accurate. The `stored_metadata` doc says "`remaining`
   bounds term decoding within a migration page". For the event-fallback job that budget is
   now per graph.

4. **OPTIONAL: three manifest parses per rebuilt graph.** Each rebuilt graph parses its
   manifest three times: the skip check, `stored_metadata`, and `RdfWrite::remove`, which
   reads it from the database again. Passing the parsed rows from the skip check into
   `stored_metadata` would remove one parse. Check this only if the measurement shows
   decoding dominates.

5. **OPTIONAL (observation): skipped graphs are no longer validated.** A graph that already
   has `eventTime` is now skipped before `stored_metadata`'s ownership, closure and
   term-ownership checks. A malformed graph of that kind therefore passes this job silently,
   where it used to fail the open. The issue asks for exactly this (skip without decoding),
   and validation is not this job's contract, so no change is needed. It is recorded so the
   change in failure behavior is known.

VERDICT: BLOCKED
