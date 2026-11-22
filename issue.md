# Issue #462: Import files and live byte streams through the producer SDK

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Accept a completed file, a bounded-concurrency collection of files, and Tokio AsyncRead streams. Reuse the resumable completed-file and part-set owners in #280. Publish live input incrementally with stable byte offsets, stream identity and ordering. Preserve invalid UTF-8, final unterminated input and caller-owned files. Bound prefetch, stage offline input safely and keep large imports from starving operational logs. Expose per-input progress and receipts. Exercise a stream before EOF, a retry after a lost reply and a mixed import and logging workload.


## Comments (1)

### paudley — 2026-11-21T23:29:14Z

Completed inputs and live streams have different recovery boundaries. For a completed file, retain the caller's file and check its identity across staging and retries. For live input, reserve local spool capacity before reading, persist exact bytes, and expose a resumable byte offset after local persistence. A reader that cannot seek needs its upstream to replay from that offset after a crash. Publish complete ranges while input is still open. Treat only an actual zero-byte read into a nonempty buffer as EOF.

Use the existing completed-file identity, upload checkpoint, part planner and artifact receipt checks. The consume worker's part request namespace belongs to consume claims. Regular producer imports need their own stable request IDs and ordered offsets. Persist the request and destination before first transmission and certify its exact durable receipt before releasing local bytes.

File concurrency and read-ahead need separate bounds. Use an independent bulk queue with bounded in-flight requests so a large import leaves ordinary logging admission available. Report per-input staged and remotely confirmed offsets and the current upload phase. Verify a live prefix before EOF, binary and unterminated input, reconnect after commit without a reply, and delivery of ordinary log events during an import.


## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
