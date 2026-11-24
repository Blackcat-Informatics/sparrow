# Issue #64: Report a CHAP nonce randomness failure with its own error code

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

Row B-31 of docs/open-items.md. #1 delivery review, finding O2: S1/reviews/delivery-review.md:184-188. Marked by the delivery review as deferred by the coordinator; no verbatim coordinator quote is recorded.

## Current behavior

If the OS random source fails while building the CHAP nonce, the failure is reported as STORAGE_FAILURE with the message "CHAP nonce: ..." (`crates/sparrow-server/src/tcp_gate.rs:52-53`). The random source is not storage.

## Work

Report the failure with an error code that names an unavailable randomness source (or INTERNAL, if the code set has no better one) and keep the retryable flag honest.

## Done when

A unit test named `chap_nonce_failure_is_not_a_storage_failure` injects a failing nonce source into `challenge` and asserts the error code is not STORAGE_FAILURE. Today it is STORAGE_FAILURE, so the test fails.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
