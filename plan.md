# Plan: issue 20, receipt-time fallback for event time

## Behavior
Implement the Work section of issue.md exactly (items 1 to 5): a read-time event-time basis
(`source` or `receipt`), fallback derived from `received_at` with the original Unknown reason kept,
intervals, sorting and event-clock correlation including fallbacks, an `event_time_fallback` count
on every response that selects or orders by event time, record output with `basis` and `reason`,
RDF `sparrow:eventTime` for source times only plus `sparrow:eventTimeBasis "receipt"` and
`sparrow:eventTimeFallback`, and docs.

## Dependencies
None. Base main 2aa56a9 (schema 68 after #10).

## Exclusions
Stored records and source profiles do not change. No inference from message text.

## Acceptance
The four named tests in issue.md plus the hook's make check. UAT on the live service after deploy.

## Risk
Wire and RDF vocabulary change: follow the schema-version rules in docs/interfaces.md. Text index
counts and correlation change meaning for previously excluded records; tests asserting exclusion
must be updated to the new contract, not deleted.

## User rulings
- 2026-11-06 15:25 CST, lillith_coordination relaying Patrick ("Have the agents assess and close/or
  finish all closeable items."): "Your order is set: #10 first ..., then #20, then #14; #11 and #13
  after cutover; #22 and #25 stay on Patrick's window."
- 2026-11-06 19:45 CST, lillith_coordination: "Your order becomes #20, #13, #14; #23 after." (#13 moved to the cutover, C24.)

## Decisions
- 2026-11-06 (revised after review-1 R1): backfill eventTimeFallback/eventTimeBasis into metadata graphs
  published before schema 69, through the existing catalog_upgrade job pattern (provenance_rdf.rs,
  tracing_rdf.rs precedent). Reason: the issue's "no migration" explains read-time derivation for records and
  does not forbid rebuilding derived RDF index rows; without a backfill a SPARQL query on eventTimeFallback
  silently misses every older fallback record with no coverage signal. Stored records stay unchanged.
