# Issue #347: Flaky test: an_ingest_peer_over_the_ceiling_waits_and_is_served_when_a_slot_frees fails to connect once

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Seen once in the commit hook for #320 (PR #345, head 00fc96c): `an_ingest_peer_over_the_ceiling_waits_and_is_served_when_a_slot_frees` in `sparrow-server` failed with "No such file or directory" connecting to the ingest socket. The same commit passed on an unchanged rerun. The change touched only `sparrow-core` archive code and docs, so it is unrelated to it.

Log: `.stage/evidence-keep/320/commit2-hook-failed.log` in the main checkout (passing rerun: `commit3-hook.log`).

Suspected cause (inference, not read): the test connects before the ingest socket exists, a startup race. Acceptance: read the test and the server's socket-start path, show the cause, and make the test wait for readiness through the server's own readiness signal rather than time.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
