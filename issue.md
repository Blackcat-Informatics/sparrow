# Issue #514: Diagnose an extra producer batch attempt in the profile-upgrade RPC fixture

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Parallel RPC suite run 12 for #510 failed in
`missing_producer_profile_resumes_with_negotiated_limits_and_immutable_retries`.
The fixture expected batches with event counts `[1, 2, 2]` and observed
`[1, 1, 2, 2]` after the producer profile became available. The fixture sets
a 250 ms RPC timeout. The observed sizes alone cannot identify whether the
first two calls carried the same request or committed duplicate records.

The bulk replay test passed on that iteration. The release RPC suite reported
133 passed and 10 failed, with Cargo exit 101. Nine other failures pointed to
startup or readiness deadlines. The next suite passed all 143 tests.
Evidence is in `.stage/queues/forge-assessment/510/phase-repeat.log`,
iteration 12, on base `879a1d2` with the #510 test phase probe.

Verify the request identities and durable occurrences in the repeated first
batch. Preserve the checks for deferred admission, negotiated batch limits,
and immutable retry after a lost reply. The fixture must distinguish an
idempotent retry from an extra accepted occurrence under parallel load.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
