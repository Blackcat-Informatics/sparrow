Refs #193. The issue stays open for the read-time measurement and the live 71-to-71 check.

## What changed

- Ingest stores no `eventTimeFallback` / `eventTimeBasis` triples for a record without a source time.
- The `event-fallback-rdf` online migration is removed: its job, `Migration` variant, `done:event-fallback-rdf` counter row, counter bound, status entry and ledger row. `status` now lists four migrations.
- The SPARQL view derives the facts at read time for every selected record graph that has no `eventTime` and whose stored rows lack `eventTimeFallback`. This was the pending-reader path; it is now the one path, with no frontier check.
- docs/architecture.md section 1 gains the mandate, the Katamari/Sparrow razor and the three worked examples. docs/interfaces.md and docs/deployment.md describe read-time derivation.

## Design decisions

1. **Opening a store with the pending marker.** The writer format identity is unchanged. `migration::fence` still moves a pre-fallback store to the fallback writer family (a format-string put, no data work) so older binaries stay fenced, but seeds no checkpoint and no job. A store carrying `event-fallback-rdf-upgrade` (pending or complete) opens as is: the checkpoint and its counter row are no longer read and stay as inert keys. No format bump and no rewrite. Writer fences are identical to before, so a schema 71 store such as `/SPARROW/store` reads and opens unchanged.
2. **`eventFallbackPending` afterwards.** The wire field is unchanged. It counts selected receipt-time record graphs whose facts the reader derived because the graph does not store them. It is a permanent read-time count, not pending work, and does not fall to zero; docs/interfaces.md and the capnp comment say so.
3. **Already-written fallback triples.** Left in place and served as stored. Nothing is rewritten or deleted. Graphs that carry them are skipped by the derivation, so facts are never doubled.

## One-way switch

Only binaries older than schema 69 stay fenced. The previous schema 71 image (before this change) opens a store that carries the retired checkpoint, but it does not derive facts for graphs written without them, so rolling back serves those graphs without the receipt-time facts. docs/interfaces.md states this.

## Test value

- `receipt_time_fallback_facts_are_derived_not_stored_and_a_pending_marker_store_opens`: 70 plain log records give 70 derived fallback rows with coverage 70, and a store with the pending marker keeps its store format, reports no `event-fallback-rdf` migration, answers identically before and after its migrations, and rewrites no record graph. It was not run on main; on main ingest stores the facts, so its coverage assertion cannot hold there.
- `graphs_that_store_their_fallback_facts_are_served_once_and_not_derived`: a legacy fixture stores the facts on 35 of 70 graphs and leaves a partial `event-fallback-rdf-upgrade` checkpoint and the `done:event-fallback-rdf` counter row, the shape of the live store. The store opens without a rewrite, a mixed scope answers one row per record (none doubled), and coverage counts only the 35 graphs without stored facts.
- Exact coverage equality across the migrations is asserted again in `open_serves_before_the_schema_69_migrations_...`. Tests that used the backfill as the paging migration now use `source-summary` and `source-context`.

## Evidence

- Head 0d0dd74 was committed through the pre-commit hook, which ran the full gate (fmt, release clippy with warnings denied, release workspace tests, census). The hook log `/home/paudley/Active/sparrow/.stage/193-commit7.log` ends with its own `EXIT=0`.
- The first commit, 54d2a64, was admitted by an earlier hook run; the log recorded as admitting it (`193-commit4.log`) ended `EXIT=1`, and the passing log `193-commit5.log` finished after that commit existed. That commit is superseded by 0d0dd74 and is not cited as verified.
- Focused run `193-test-core5.log` (EXIT=0): the new mixed-store test, the new marker test and `open_serves_before...`.
- Earlier failed hook runs `193-commit.log` to `193-commit3.log`, `193-commit6.log` (clippy `manual_is_multiple_of`) ended `EXIT=1` and were fixed before the next run.

## Not done

- The read-time cost measurement (the #158 live-size query set, read-only, against the live store) is not done. It will be recorded on the issue after the switch; the facts stay if a normal query then exceeds 30 s.
- The live 71-to-71 switch is a lifecycle action and was not touched.
