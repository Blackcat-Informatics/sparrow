# Issue #319: Archive prepare holds the single RDF read slot for the whole segment; concurrent queries are refused

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Problem

An archive prepare holds the store's single RDF read slot for the whole segment, up to about 30 s at 50,000 records in a release build. Concurrent SPARQL and other RDF reads are refused with a retryable `RESOURCE_LIMIT` for that time. This predates #304 (the base held the slot longer), but it bears on the operator's ruling that archiving must "perturb no operations".

## Evidence

Independent review of PR #308 at e7d50ad (`.stage/archive/review-pr308-e7d50ad.md`, optional finding 2). Measured prepare time 31.8 s at 50,000 records (`.stage/evidence-keep/304/lock2-log-50000.log`).

## Proposed acceptance

- An RDF read issued while an archive segment is being prepared is served, not refused, or waits a bounded time that is stated and measured.
- The archive read keeps its snapshot guarantee.
- A measurement with a concurrent query during a 50,000-record prepare.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
