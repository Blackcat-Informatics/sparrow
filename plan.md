# Plan: query-only mode and verify-backup (fleet/sparrow #25)

## Behavior
As issue.md states, with its amended UAT in issue-comments.md: `sparrowd --query-only` opens a
restored store with no workers and one write gate for Unix, CHAP and anonymous TCP;
`deploy/verify-backup.sh` verifies a backup; `sparrow-backup` records the closing payload-chunk
count from the stopped daemon's shutdown line.

## Dependencies
Schema 65 on main. #24 takes schema 66 first, and this branch then rebases to schema 67.

## Exclusions
The NAS archive (#15 to #17). The system-unit cutover (#22, which waits on Patrick's window).

## Acceptance
Issue acceptance and UAT steps. Round 3 (review-r3.md) was BLOCKED on R1. The fix took option
(i), as decided before the pause: an image that predates the closing line publishes with
`payload_chunks=unrecorded`, while a capable image with no closing line still fails. O1 and O2
were addressed as STATE.md records: the count is recorded only when the unit's Result is
success, and a journalctl failure gives a clear message (needs systemd 257, the host runs 262).

## Risk
Backup tooling on the live host, and the shared RPC schema bump.

## User rulings
- 2026-11-04 (Patrick via lillith_coordination, verbatim): "all restarted ... You have the go signal, let'er rip." Coordination: #25 (schema 67) after #24, with the #19 baseline through its verify-backup script.
