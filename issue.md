# Issue #20: Fall back to receipt time, marked as such, when a record has no event time

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Summary

When a record carries no source event time, Sparrow uses its receipt time as the record's event time for event-time reads, and marks every such value as receipt-derived. Producers that cannot or do not stamp an event time (some cruft items, plain logs) then appear on the event timeline instead of disappearing from it. A reader can always tell a producer's own timestamp from a fallback.

## Current behavior

- `EventTime` (`crates/sparrow-core/src/storage/source_time.rs:188-216`) is either `Known { millis, profile_id, occurrence, .. }` or `Unknown { reason, profile_id }`. A record whose source profile yields no instant stays `Unknown`.
- `docs/interfaces.md:389-391` says an event-time interval "excludes unknown event times and reports their count when known; it never substitutes receipt time", and `docs/interfaces.md:1061` says "Event-time unknowns never use receipt time as a fallback."
- Event-time consumers: `records --event-from/--event-before` (`crates/sparrow-server/src/agent/mod.rs:452-453`), event-clock correlation (`storage/correlation.rs:207`), the text index's event counts (`storage/text_index.rs:507-675`, which reports `unknown`), the RDF `sparrow:eventTime` predicate (`storage/rdf_index.rs:938-940`), and record field output (`storage/record_fields.rs:237`).
- The memetics cruft sink spec relies on a received-time fallback for any item without `observedAt`.

## Work

1. Add an event-time basis to every event-time read result: `source` (from the source profile) or `receipt` (fallback). Derive the fallback at read time from the record's `received_at`. No stored record changes, so no migration is needed, and the original `Unknown { reason }` stays inspectable.
2. Event-time intervals, sorting and event-clock correlation include fallback records at their receipt instant. Every response that selects or orders by event time reports `event_time_fallback` (the count of records placed by receipt time) next to the existing counts.
3. Record output (`records`, `record`, MCP and RPC) shows `event_time` with `basis` and, for fallbacks, the original unknown `reason`.
4. RDF: keep `sparrow:eventTime` for source times only, and add `sparrow:eventTimeBasis "receipt"` and the fallback instant as a distinct predicate `sparrow:eventTimeFallback`. A SPARQL query can then never confuse the two.
5. Update `docs/interfaces.md` (lines 389-391 and 1061, the record shape, the RDF vocabulary) and `docs/architecture.md`. Wire field changes follow the schema-version rules in `docs/interfaces.md`.

## Out of scope

- Changing stored records or source profiles.
- Inferring event time from message text.

## Depends on

None.

## Automated acceptance

- `event_interval_includes_receipt_fallback`: a record with an `Unknown` event time is returned by an event-time interval containing its receipt time, with basis `receipt` and the fallback count 1.
- `source_event_time_is_never_marked_fallback`: a record with a `Known` time keeps basis `source` and is placed by its source instant even when receipt time differs.
- `correlation_event_clock_uses_fallback_marked`: event-clock correlation includes a fallback record and labels it.
- `sparql_distinguishes_fallback_time`: `sparrow:eventTime` is absent for a fallback record, and `sparrow:eventTimeFallback` is present.
- `make check` passes.

## UAT

Run after deployment against the live store (`S=/SPARROW/store/rpc.sock`, `ID=$(~/.local/bin/sparrow $S status | jq -r .store_id)`).

1. Ingest one plain log line with no timestamp under a scratch source: `printf 'uat fallback %s\n' $(openssl rand -hex 8) > /tmp/uat-fallback.log` (keep the token), then `~/.local/bin/sparrow $S ingest uat-fallback-$(date +%s) uat-fallback log /tmp/uat-fallback.log`. Expect a receipt with one record.
2. `~/.local/bin/sparrow $S records $ID --source uat-fallback --event-from <one minute before> --event-before <one minute after>`. Expect the record, with `event_time.basis` equal to `receipt`, and `event_time_fallback` equal to 1.
3. Repeat step 2 for a record from a source whose profile supplies event time (for example a typed producer event). Expect `basis` equal to `source`.
4. Run a SPARQL `SELECT ?t WHERE { ?r <urn:sparrow:vocab:eventTime> ?t }` limited to the step 1 record. Expect no rows. Then the same query with `eventTimeFallback`: expect one row.

Done when every UAT step passes on the live service.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
