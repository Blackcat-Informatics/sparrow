Closes #323.

## Change

The store indexes each held artifact by payload id so it can say whether it holds some content: key `payload-id:<64-hex blake3>:<artifact id>`, empty value, default column family. `payload_index::apply` runs inside `Writer::apply`, so the key is written in the batch that creates the artifact entry and removed in the batch that releases its bytes. An artifact counts as held only while its entry exists, is not evicted, and its manifest retirement state has neither evicted it nor retired any chunk (`manifest_retire::releases_bytes`). Expiry releases bytes a day before it deletes the entry, so the key leaves at release, not with the entry. The per-source counters look only at the evicted flags and have that day-long gap; they are unchanged here.

Artifacts stored before the index existed are indexed by an online migration (`Migration::PayloadIndex`, run order: counters, source summary, payload index, owner graph, source context). Each page commits its keys and checkpoint in one batch under the writer lock, so it resumes after a restart. Counters `payload-index` / `done:payload-index` are kept.

## Stored format (one-way)

A writer-format fence `sparrow.store/352`..`406` lifts each store-counter family, as earlier migrations did. Older binaries refuse to write a store this binary has opened, so installing an image with this change cannot be rolled back to fda2812 or ca6cff4. Approved in principle by the operator (via coordination).

## Wire, CLI, MCP

`payloadLookup @56` (additive; schema stays 71): `storeId`, `schemaVersion`, `payloadId`, `cursor`, `pageSize` (1..1000, default 200) → `artifactIds`, `next`, `complete`. `complete` is false while the migration runs: listed artifacts are held, unlisted are unknown. CLI `payload STORE_ID PAYLOAD_ID [--cursor C] [--page-size N]`; MCP tool `payload_lookup` (`answer_certainty` exact or pending). Listed in the query-only and ingest-gate read sets.

## Measured (release scratch store; host load 40 to 60, so indicative)

| Measure | Result |
|---|---|
| Key cost | about 112 physical bytes per artifact (of about 4,600 total) |
| Lookup, 20,000 present + 20,000 absent | p50 4 µs, p99 10 µs |
| Rebuild, 100,000 artifacts | 3.92 s, 99 pages, worst page 297 ms |
| Ingest during rebuild | p50 2.1 ms, p99 32.8 ms, max 102.6 ms |
| Resident set | 217 MB with index (144 MB baseline), 288 MB peak during rebuild |
| Ingest cost, 30,000 artifacts, alternating pairs | within noise |

Not measured: a quiet host; the daemon maintenance worker's own scheduling of migration pages; a live store.

## Tests

- `crates/sparrow-core/tests/payload_index.rs`: `the_same_bytes_under_two_requests_are_both_found_and_an_absent_digest_is_exactly_empty`, `every_age_out_removes_the_key_with_the_entry_and_keeps_other_holders` (includes expiry at the deadline, before entry deletion), `the_migration_indexes_artifacts_ingested_before_it_existed_and_new_ingest_stays_exact`.
- Unit: `the_key_travels_in_the_batch_that_creates_retires_or_deletes_the_entry`, `the_migration_resumes_after_a_restart_between_pages`, extended writer-format test.
- `crates/sparrow-server/tests/rpc.rs`: `payload_lookup_finds_every_artifact_of_a_digest_through_cli_and_mcp`.

## Evidence

Main checkout `.stage/evidence-keep/323/`: probe and A/B logs; full `make check` on the merged head f88c0f3 (`check-f88c0f3.log`, EXIT=0); hook for e5cd505 (`logs/commit6.log`, EXIT=0). Review `.stage/archive/review-323.md`: BLOCKED at f88c0f3 on the expiry-window stale key, no other required findings. Fixed at e5cd505 (diff read by the primary); the reviewer's helper-duplication and `#[must_use]` notes are also taken.
