# Issue #187: Client flush and shutdown wait out the delivery retry delay (up to 30 s) (REQUIRED, from #186 review)

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

Independent review of PR #186 (#184 row 8, `.stage/184b/review-2.md`; recorded there, not fixed): the client's delivery retry now backs off from 1 s, doubling to `retry_max_interval` (default 30 s, `crates/sparrow-client/src/delivery.rs`). A flush or shutdown during a delivery outage can therefore wait out the current retry delay, up to 30 s, where the old fixed 250 ms bound it at 250 ms. That is a latency regression on shutdown and explicit flush paths.

## Work

Make the retry wait an event wait that a flush or shutdown request wakes at once (a channel or `Notify` the delay waits on, as the watch worker's waits do in PR #185), keeping the backoff otherwise unchanged and the stable retry identity.

## Done when

A test shows a shutdown (and a flush) issued during a backoff delay returns within a small bound that does not depend on the delay, with a rationale comment; the existing backoff tests still pass.

## Waits on

Nothing. Not a prerequisite of the D147 image. Nothing waits on Patrick.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
