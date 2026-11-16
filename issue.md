# Issue #328: Archive prepare read renews its lease while it makes progress (no halving waste)

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Problem

An archive prepare reads its segment through one RDF read with a fixed 60 s lease (`record_read_lease_ms`). A segment that is still making progress is cut off with `RESULT_EXPIRED` ("RDF read lease ended"), `archive_prepare` halves it and starts again, and the failed attempt's work is thrown away.

## Evidence

Rehearsal on the 70d2f63 code under the unit's limits (CPUQuota=100%, MemoryHigh=2G), 50,000 katamari-shaped JSON-line records, real NFS target (`.worktrees/326-*/.stage/rehearsal/logs/rehearsal.log`): 15 of 16 segments hit `RESULT_EXPIRED`, 23 halvings, segments ended at 1,250 to 5,000 records, and about 23 of the run's 35 minutes were failed attempts. A 10,000-record segment never fit. Cost per successful record was 8.5 to 17 ms. The earlier 50,000-record probe on merged main hit the same lease (#321).

## Decision (coordination, 2026-11-16)

The archive's own read renews its lease while it makes progress. The segment size stays at 10,000 or larger. The default is not lowered as a patch; no halving waste.

## Proposed acceptance

- The archive prepare's RDF read keeps its lease alive while the read advances (rows or requests consumed since the last renewal), and ends only when it makes no progress for a bounded time, not when a wall-clock total passes. Ordinary query reads keep their 60 s lease unchanged.
- A prepare that is progressing is never halved for the lease; a prepare that stalls still fails with `RESULT_EXPIRED` and is reported (#321, #322).
- A test with a small lease: a read longer than the lease that progresses completes; one that stops progressing expires.
- Re-run the 50,000-record rehearsal (same harness and limits) and report halvings (expected 0), prepare time per record and total.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
