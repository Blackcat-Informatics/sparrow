Closes #328.

## Profile first

A 10,000-record segment outlasted the 60 s read lease under the production CPU cap, so prepares were halved (23 halvings in the 50,000-record rehearsal). Profiling the read at 400 and 2,000 records, and for one 800-key JSON record, refuted the cursor-walk hypothesis: the scoped view already makes one range scan per owner graph (about 2.09 cursor steps per stored row). Of 19.0 requests per row, about 16 were dictionary checks: each index row read the forward and reverse dictionary rows of all four term ids, and every row is read twice (named graph and default graph). PurRDF's importer already memoizes terms, so its share was under one request per row.

## Change

- **Verified-identity memo.** A view checks each term id once, the first time a row names it, and charges the entry (32 bytes) to its live-byte budget. The snapshot cannot change, so the check holds for the view's life. Uses PurRDF's public `FastSet`. Log records fall from 19.0 to 2.85 requests per row (400 records prepared in 654 ms instead of 2,460 ms); the 800-key JSON record costs 4.78. `ARCHIVE_ROW_ALLOWANCE.requests` goes from 42 to 10, so the archive tests guard against the old cost.
- **Lease renewal.** `LeaseRenewal { Fixed, OnProgress }` in `rdf_view/budget.rs`; `ReadSlot::lease()` gives the archive slot OnProgress and the query slot Fixed. Every admitted read renews the archive's lease, so it ends only after 60 s with no progress. The request, row and io ceilings still bound total work. A stalled prepare still ends in `RESULT_EXPIRED` and is halved and reported. Segment size stays at 10,000. No wire or stored-format change.
- Test-only clock skew (`#[cfg(test)]`) lets the lease test move time without sleeping.

## Rehearsal (50,000 records, CPUQuota=100%, MemoryHigh=2G, nfs4 target, the earlier harness)

| | Before (70d2f63) | After |
|---|---|---|
| Segments | 16 | 6 |
| Halvings | 23 | 0 |
| Prepare total | 2,098 s | 144.6 s (2.89 ms per record) |
| Whole pass | 2,117 s | 160 s |
| Worst segment | 204 s | 33.1 s (10,000 records) |
| VmHWM | 863 MB | 1,365 MB |

Projected first live pass (about 2.96M records): about 2.4 h of prepare plus about 2 minutes of NAS writes; an extrapolation, not a measurement.

Limits: VmHWM includes the ingest phase of the same process, and the old run's segments were halved, so the two peaks are not like for like (see `.stage/archive/memory-peak-reading.md`). The rehearsal ran on the tree that became a5a1088, 20 minutes before its commit. Markdown and concordance row costs are unmeasured (review O1).

## Tests

- `a_renewing_lease_ends_only_when_progress_stops_and_a_fixed_one_at_its_length` (10 s lease, 6 s steps).
- The 1,100-record, JSON and Markdown archive tests now run under the lowered allowance.

## Evidence

Main checkout `.stage/evidence-keep/328/`: probe logs, `rehearsal.log`, hooks (EXIT=0) at a5a1088 and 4557f90 (the latter on the tree merged with f4ae7a1), merge log. Review `.stage/archive/review-328.md`: PASS at a5a1088, no required findings; the test-margin point (O3) is taken at 4557f90.
