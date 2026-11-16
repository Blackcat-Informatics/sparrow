# Issue #321: archive_prepare halves the segment on any encode error and drops the error

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Problem

`Store::archive_prepare` (crates/sparrow-core/src/storage/archive_segment.rs, the loop that sets `cap = Some(entries / 2)`) retries a failed encode with half the entries whenever the error code is not `UNSUPPORTED`. The error that caused the halving is dropped: it is not logged, counted or returned. The only trace is a smaller segment.

## Evidence

On merged main 9d33967 a release probe asked for a 50,000-record segment and got `records=25000` after 113 s (`.stage/probe-9d33967/probe.log`); nothing says which error halved it. An instrumented rerun (`probe2.log`, same tree plus a temporary print) showed it: `RESULT_EXPIRED` "RDF read lease ended" at 63,349 ms, i.e. the 60 s read lease, under a host load average of about 79 on 32 cores. The 63 s of read work was thrown away and the retry at 25,000 records took until 116 s. The earlier run on the uncommitted tree that became e7d50ad prepared all 50,000 in 31.8 s.

## Proposed acceptance

- Every halving logs a structured `tracing` warning with the error code, message, entry count and elapsed time, and increments a counter visible in archive status.
- A halving that ends with a single entry still returns the error naming that entry, as now.
- A test drives a real encode failure and observes the warning and the counter.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
