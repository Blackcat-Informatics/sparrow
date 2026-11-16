# Issue #320: Archive prepare reads record bytes one writer-deferral per record; batch them and document the yielding

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Problem

`build_segment` reads each record's bytes under its own `lock_page()` hold, which first defers to waiting writers for up to the longer of 100 ms and the last migration page hold. Under constant writer queueing a 50,000-record segment could take about 83 minutes in theory. It is slow, not stuck, and nothing is lost; the segment halves and retries at the 60 s read lease.

## Evidence

Independent review of PR #308 at e7d50ad (`.stage/archive/review-pr308-e7d50ad.md`, optional findings 3 and 4). Measured worst ingest stall 8.7 ms and total prepare 31.8 s at 50,000 records without sustained queueing.

## Proposed acceptance

- Record bytes are read in batches (for example 256 per hold, like the selection), so the number of deferrals grows with batches, not records.
- The 50,000-record release probe with live ingest still shows no stall over 100 ms, and the total under sustained ingest is measured and stated against the lease.
- `docs/architecture.md` section 4.4 says archive reads give way to waiting writers and take the single RDF read slot.


## Comments (1)

### paudley — 2026-11-16T03:28:55Z

Correction (independent review of PR #308 at a91cdde): a slow per-record byte read does not hit the 60 s read lease. The lease starts later, when the RDF read begins, so a slow byte read only delays the segment; it does not cause the halve-and-retry described above.

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
