# Plan: issue 14, make `sources` and `artifacts` work on a store of any size

## Behavior
As the issue's Work section states: exact per-source counters (`source-summary:`), `artifact-retained:` counts, and the `source-artifact:` index, all written through one owner in the same batches as every record/artifact state change; `sources` reads the counters from a snapshot without holding the writer lock; `artifacts` walks the per-source index; new writer-format family fences older writers; docs.

## Decisions
- Migration runs online, not at open (2026-11-07). The issue predates the ruling that disk-format updates follow #18's online-migration criterion. The seeding of the three row families becomes a third migration in the #36 step 2 framework (`storage/migration.rs`, the maintenance duty, `status.migrations`), fenced and seeded at open. While it is pending, `sources` and `artifacts` must be explicit: they report the migration as pending and do not present partial counters as exact (the exact behavior — refuse with INDEX_NOT_READY, or answer only for sources the watermark has passed with coverage marked incomplete — is decided against the existing coverage channel and recorded in implementation.md). The issue's "under 60 seconds at open" target is replaced by: open serves at once; backfill duration measured with `deploy/verify-backup.sh --measure-migration` and recorded in docs/load-qualification.md.

## Dependencies
#36 step 2 (60a92fd) migration framework and --measure-migration; writer_format families from #20/#13/#36.

## Exclusions
As the issue: per-source byte totals; the record search scan budget for wide time windows (issue #123, docs/open-items.md row C-14).

## Acceptance
The issue's seven named tests, plus: open serves with the new migration pending; sources/artifacts during the migration are explicit; the migration completes in the duty and counters then equal a recount. make check via hook. UAT after deploy.

## Risk
Counter exactness across every writer of record:/artifact: keys (publication, raw publication, eviction, manifest retirement, import publish, expiry with pins); fragment_parents edge cases; ingest throughput cost of extra rows per batch.

## User rulings
- 2026-11-06 19:45 CST (lillith_coordination): "Your order becomes #20, #13, #14; #23 after."
- 2026-11-07 ~06:05 CST (lillith_coordination, as Patrick): "C18's own criterion for disk-format updates is the online migration (#18: rebuild derived rows page by page while the daemon keeps serving)"
- 2026-11-07 ~11:1x CST (relayed by lillith_coordination, Patrick's rulings of this morning): "nothing is optional, deferred or a follow-up on your own authority; requests to cut count as failure and evasion; every claim of done carries the number that proves it"
- 2026-11-07 ~13:5x CST (lillith_coordination, Patrick): "Unmerged WORK is UNDONE WORK": a branch or an open PR counts as undone; merge reviewed work at once.
