# Issue #452: Quarantine replay stores held parts once and the command is run end to end

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Problem
Two open items from #448 (merged 283de17), ordered after #445 to #447 and before the switch image.

1. `quarantine-replay` can store a file twice. A quarantined file exists because delivery's outcome was not certain, so part of it may already be held. Replay sends under a derived request and instance identity (the daemon refuses sequences behind the original instance's accepted positions), so the store's own dedup does not answer and the evidence is written twice. crates/sparrow-client/src/replay.rs.
2. The command a person runs has never been run. Only the library function was exercised, through rpc tests.

## Acceptance
- Replay asks the store first which parts it holds (the payload lookup the shipper's reaper uses) and sends only the rest, or carries the original batch's identity so the store's own dedup answers. The choice and its reason are in the code.
- A test with a file half held: the held half is not stored again, the rest is, and the file is removed only after every part is accounted for.
- `sparrow ENDPOINT quarantine-replay FILE` is run end to end against a scratch daemon with a multi-batch file and a failure in the middle (the daemon refuses or is stopped partway): the file stays with the refusal recorded, a second run completes it, nothing is stored twice. The transcript is posted on this issue.
- The auth case: a producer refused UNAUTHENTICATED or PERMISSION_DENIED holds until the configuration admits it, and a rotated TCP secret needs a new `Delivery::start`, because the secret is read once. Decide whether the client re-reads the secret source on reconnect after UNAUTHENTICATED so a rotation clears without a restart; if not, state what a client should do (start a new Delivery and hand over the pending batch). Update the 444 table row.


## Comments (1)

### paudley — 2026-11-21T22:39:13Z

At 73ebdb9 the existing quarantine file contains source, event sequence, producer instance and request identity. It does not record a destination store. Replay derives a different producer instance and request per part. That changes the canonical event bytes, including their payload hash.

Reconciliation must distinguish the original identity from any earlier replay identities. A payload lookup alone does not establish request identity or producer provenance. The current producer-only endpoints refuse access to artifact and ingest-status queries. A retained matching request receipt can certify an attempt. An expired receipt and a partial or expired producer window leave the result unknown. Preserve the file whenever that uncertainty remains. Exercise the half-held case through the actual command and verify stored artifact identities.

The new outage spool in #7 can record the destination and exact request before first transmission and persist the remote confirmation before reclaiming the local segment. This provides a stronger basis for automatic recovery than the historical quarantine header.


## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
