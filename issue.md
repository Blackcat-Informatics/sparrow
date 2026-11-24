# Issue #81: Verify a running daemon pauses and resumes across the filesystem floor

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Runtime free-space transition

The daemon keeps accepting connections and serves reads when available
filesystem space falls below the configured floor. Writes receive a
retryable STORAGE_PRESSURE refusal classified as freeSpaceFloor.
The consume worker pauses before claiming inputs. The floor's resume
edge wakes it when space returns.

The serving tests cover acceptance, the typed refusal, counters, and
the beginning and end of a pause using a controlled measurement.
The watch tests cover their resume edge. The RPC suite covers starting
below the floor and recovery after crossing the separate store budget.
This issue covers crossing the actual filesystem floor down and up
within one daemon process.

## Acceptance

Run a daemon in an isolated, size-limited scratch filesystem. Start
above the floor, then allocate test-owned ballast so free space falls
below the floor while staying above the maintenance reserve. Confirm
that the daemon logs the pause, stays alive, serves a read, returns the
classified retryable write refusal and leaves an offered consume file
unclaimed. Record the refusal count.

Release the ballast. Confirm the end of the pause, acceptance of the
same write and publication of the waiting consume file by the same
daemon. Keep the startup tests and #78 tail recovery proof independent.

## Source

Row B-51 in the original open-items review and space-floor review
finding 4. The current contracts are in the interface guide's
free-space pause section. #263 covers accepting connections under the
floor and #441 covers startup under the floor.


## Comments (1)

### paudley — 2026-11-24T11:49:21Z

The existing serving fixtures exercise the production accept loop, the
retryable floor refusal, continued reads, write recovery on the same
connection, and the pause and resume journal messages. The watch fixture
covers the resume event. PR #536 adds the separate missing-tail
checkpoint case. The subprocess startup fixture covers a daemon that
opens below the floor and serves reads.

Keep this issue for one daemon episode that starts above the floor,
crosses it at runtime, and verifies process and watch behavior through
recovery.


## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
