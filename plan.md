# Plan: issue 13, bound SPARQL work by the request's scope

## Behavior
Implement the Work section of issue.md exactly: the owner-to-graph index rdf:o:<owner><graph> written and
deleted beside every rdf:g declaration, a resumable open-time build job beside rdf_index::upgrade_reverse with
a new writer-format family fence, scope-bounded per-graph evaluation (rdf_view.rs, rdf_view/sparse.rs) merged
through the existing ScopedRows heap, graph_rows through the same owner walk, the whole-live-store plan
unchanged, requests and row visits charged exactly as today, and the docs. Store format changes; no wire
schema change.

## Dependencies
None. Base main 8e7a191 (schema 69, store-format families through /186 from #20).

## Exclusions
Raising host ceilings, query-time JSON projection (#5), correlation-specific planning beyond sharing this
evaluation.

## Acceptance
The four named tests in issue.md plus the hook's make check. UAT on the live service after deploy,
including the sp:peerUid check moved here from #10 (see the issue comment).

## Risk
The fence adds another writer-format family set; follow the existing multiplication pattern (archive,
intake, log time, event fallback) and do not renumber existing families. The open-time job must be resumable
on a 13 GB live store and must not break a store with an unfinished staged import (a lesson from #20 review:
prepared record graphs exist without catalog records). Ordering of rows and union-default deduplication must
match the store-wide plan exactly.

## User rulings
- 2026-11-06 15:25 CST, lillith_coordination relaying Patrick: "Have the agents assess and close/or finish
  all closeable items."
- 2026-11-06 19:45 CST, lillith_coordination: "Your order becomes #20, #13, #14; #23 after." and #13 moved to
  the cutover (C24) because every live SPARQL request on the store exhausts the ceiling.
