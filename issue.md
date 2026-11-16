# Issue #340: Ingest refuses a JSON artifact with RDF write staging exceeds 64 MiB, uncounted

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Ingest refuses a single JSON artifact with "RDF write staging exceeds 64 MiB" (a log store refusing a log).

Found by the #336 memory measurement: an artifact of 100 JSON records with 100 keys each was refused at ingest with RESOURCE_LIMIT, so the measurement had to use 10 records per artifact. The refusal is raised in `crates/sparrow-core/src/storage/rdf_index.rs` (about line 627, the `MAX_BATCH` check on the anticipated batch size). It is returned to the ingesting client only. No daemon counter records it, so the status output cannot show how often it happens in real traffic (the journal logs nothing for it either).

Acceptance: decide whether a single artifact's RDF should be split across batches, or the refusal be reported as a counted, named condition (a counter in the status output) with a clear client error. Show, from real traffic, how many refusals occur, which requires the counter first. Acceptance also includes a test for the chosen behaviour at the real ingest path.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
