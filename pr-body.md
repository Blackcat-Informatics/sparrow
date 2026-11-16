Closes #319.

## Change

The archive's segment read gets its own RDF read slot. Before, it held the store's single slot for the whole prepare (tens of seconds at 10,000-record segments), so every concurrent RDF query was refused with `RESOURCE_LIMIT`. `rdf_read_lease` now takes a `ReadSlot { Query, Archive }`. `archive_encode::encode` takes `Archive`; `capture_rdf_scope`, `with_rdf_record_layers` and `with_rdf_records` take `Query`. Two queries still exclude each other, and so do two archive reads.

## Memory

Each read charges its own `Budget` (no store-wide live-byte counter). With both slots busy, the bound is the query limit (at most 128 MiB, `rpc/sparql.rs`) plus the archive's 2 GiB (`archive_encode.rs`), about 2.125 GiB instead of 2 GiB. Whether that fits a host memory ceiling was not checked.

## Measurement (release, 50,000 records held, one 10,000-record segment prepared, a query on 3 record ids every 5 ms)

| Run | Prepare | Queries served | Refused | p50 | p99 | max |
|---|---|---|---|---|---|---|
| No live ingest | 6.8 s | 1,248 | 0 | 365 µs | 1.59 ms | 2.5 ms |
| Live ingest, about 1 ms cadence | 9.84 s | 1,578 | 0 | 827 µs | 5.2 ms | 7.8 ms |

Single runs on a loaded host. The query used `with_rdf_record_layers` (same slot and writer-lock capture as `with_rdf_scope`). The probe is a temporary, uncommitted test.

## Tests

- New: `a_query_is_served_during_an_archive_read_and_still_excludes_another_query` (channel handshakes, no sleeps).
- Changed: `a_prepare_halved_by_an_encode_failure_reports_the_shortening_and_counts_it` now holds the archive slot to force its first attempt to fail.

## Docs

`docs/architecture.md` 4.4 and `docs/development-runtime.md` describe the two slots and the bound; the `with_rdf_records` comment and the query refusal message say "query".

## Evidence

Main checkout `.stage/evidence-keep/319/`: hooks `commit2.log` (f333884) and `commit3.log` (b6313d0), both EXIT=0; probe logs `probe319-noingest.log`, `probe319-live.log` and `probe319.rs`. Review `.stage/archive/review-319.md`: BLOCKED at f333884 on two stale doc claims and the missing measurement. Both are fixed at b6313d0 (doc-only change plus the measurement above, read by the primary).
