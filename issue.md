# Issue #510: Diagnose the bulk import replay timeout under parallel RPC load

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Observation

On signed head `21afbde`, suite iteration 31 of the #382 repeat run failed
in `bulk_reopens_an_unacknowledged_range_and_publishes_while_input_is_open`.
The test's outer 30-second timeout expired at
`crates/sparrow-server/tests/rpc.rs:8918`. The process finished with
139 passed, one failed and `EXIT=101`. The first 30 suite runs passed.
The buffered replay test passed on this iteration.

Evidence is in `.stage/queues/forge-assessment/382/rpc-repeat.log` under
`RPC suite iteration 31 of 50`. The panic identifies the outer timeout.
The stalled phase is unknown.

## Acceptance

Identify the phase that can stall under parallel RPC load and exercise it
with a fixture that observes explicit progress. Preserve the checks for
durable publication while input stays open and receipt recovery after
reopening an unacknowledged range. Record the focused result and a full
parallel RPC-suite result.

[Repeat-run source pull request](http://lillith:7555/fleet/sparrow/pulls/509)


## Comments (1)

### paudley — 2026-11-24T06:54:42Z

The bulk-import fixture timed out again on signed head `24848a3` during
the documentation gate for #271. Its only changes relative to
`6499015` are the two Markdown files. The outer deadline failed at
`crates/sparrow-server/tests/rpc.rs:8921`, with 142 RPC tests passed
and one failed. Cargo returned 101 and `make check` returned 2.

The gate log is `.stage/queues/forge-assessment/271/make-check.log`.
The outer timeout still leaves the stalled phase unknown.


## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
