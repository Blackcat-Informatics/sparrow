# Plan: issue 184, first PR (watch worker and worker health)

## Behavior

- The watch worker (`crates/sparrow-server/src/watches.rs`) never sleeps on a fixed tick. Each
  pass ends either by running again at once (a scan, a claim or a tail backlog is in progress)
  or by blocking on its wake sources: the stop channel, the command channel, readable inotify
  descriptors (directory watches and, for tail watches, one per-file watch on each held
  descriptor), a freed operation permit when a tail drain or tail claim waits for one, and the
  earliest real deadline (candidate quiet clocks, periodic rescan, claim retry backoff, the
  one-second connect throttle, a one-second free-space recheck only while space is short).
- Inventory rows 1 to 5 (`TICK` and its uses, `stopped()` poll) are removed; row 3 waits for
  `connect_after` (one second) instead of 50 ms.
- Row 6: `worker_health` in `main.rs` awaits worker completion instead of a 200 ms interval.
- Every journal writer runs on the worker itself (reserve, handoff, tail finish and retire
  arrive as commands; tail capture runs in the pass), so a new pending claim is seen by the
  pass that made it; no extra notify is needed. Recorded in the code.

## Dependencies

- tokio `test-util` feature in sparrow-server dev-dependencies (paused clock in tests).

## Exclusions

- Rows 7, 8, 9 (floor), 16, 17 and the KEEP reasons for rows 10 to 15 stay open in #184.

## Acceptance

- No `TICK`, no sub-second periodic sleep in the watch worker or worker health.
- Tests (paused tokio clock): idle worker passes stay at or under one per second over a
  fixed window; a stop request ends an idle worker with zero clock advance; a freed permit
  wakes a waiting tail drain within one millisecond of mock time; an administrative command
  wakes an idle worker with zero clock advance.
- `make check` green through the commit hook.

## Risk

- Self-generated inotify events (the worker's own opens of the root and parents) could keep
  the worker awake. Mitigated by draining every enabled detector right before blocking.
- Tail backlog: one drain per pass; the worker keeps running until a full rotation of held
  descriptors makes no progress.

## User rulings

- 2026-11-10 (Patrick, relayed by coordination, issue comment 11394): "everything on this
  list is to be corrected as the coding lane becomes free, without interrupting the lane in
  progress. Remove busy waiting and needless sampling: code that wakes on a clock to ask
  whether something has happened, or samples faster than anything needs. A wait is an event
  the thread blocks on. A row stays sub-second only where this issue shows, with file and
  line, that an event or a wait of a second or longer cannot do the work."
- 2026-11-10 (coordination brief): one PR for the watch worker rows 1 to 5 plus the
  worker-health tick unless too large; use `Refs #184`, not Closes.
