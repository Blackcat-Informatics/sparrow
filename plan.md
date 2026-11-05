# Plan: artifacts listing answers at live size (fleet/sparrow #24)

## Behavior
A paged artifacts listing reads only the rows its page selects: it seeks by source and
received-time range, charges only the rows it read, continues from the last row, and keeps
a typed RESOURCE_LIMIT, naming the page size and bound, for a page that cannot fit one row.
This is schema 66.

## Dependencies
Schema 65 on main (0221e87).

## Exclusions
Out of scope: records, keys and values queries; store format changes; O9 (recordless
artifacts of other sources charged on a source listing), which is recorded, not done.

## Acceptance
The issue's Work items 1 to 4, shown by the review (review-r2.md, VERDICT: PASS), plus the
optional O7 regression test and the O8 wording, both done before merge.

## Risk
Shared RPC schema bump (66); #25 rebases to 67 after this merge.

## User rulings
- 2026-11-04 (Patrick via lillith_coordination, verbatim): "all restarted ... You have the go signal, let'er rip." Coordination: #24 merges first (schema 66), then #25 (67).
