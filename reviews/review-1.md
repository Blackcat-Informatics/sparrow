Reviewer model: claude-opus-5-5 (opus)
Head: cb644d4a2c0b8ab8deb1e58cda232638938e3f1b

# Delivery review: Sparrow PR #185 (issue #184, rows 1 to 6)

## Scope

- Worktree `/home/paudley/Active/sparrow/.worktrees/184-replace-or-slow-the-sub-second-ticks-50`,
  HEAD confirmed as cb644d4a2c0b8ab8deb1e58cda232638938e3f1b and the tree is clean.
  Diff range c82bfa4..cb644d4: 8 files, +616/-73 (`watches.rs`, `watches/tests.rs`,
  `tailing.rs`, `main.rs`, `maintenance.rs`, `syslog_listener.rs`, the sparrow-server
  `Cargo.toml` dev-dependency, `docs/architecture.md`).
- Contract: the issue body, the binding ruling comment (Patrick, relayed 2026-11-10), and
  `plan.md` `## User rulings`. The ruling allows the watch worker (rows 1 to 5) to ship as its
  own PR first. The coordinator brief scopes this PR to rows 1 to 6 with `Refs #184`. The
  commit message uses `Refs #184`, so the remaining rows (7, 8, 9 floor, 16, 17 and the KEEP
  reasons for rows 10 to 15) stay open in #184 as allowed. This review does not judge whether
  #184 is complete overall.
- Static review only. I ran no cargo, build, test or daemon command. My only commands were
  `git diff`/`git log`, `grep`, file reads, and `prose-check`, which is read-only.

## Evidence

- Commit hook `make check` at cb644d4: `/home/paudley/Active/sparrow/.stage/184/hook1.log`
  ends `EXIT=0`. The log shows the four new watch tests passing in release (lines 498 to 506),
  `maintenance::tests::the_health_loop_fails_a_no_intake_daemon_whose_maintenance_worker_ends`
  passing (line 531), the rpc integration suite passing (93 tests), and the census check
  passing. The hook ran as part of creating cb644d4, so it covers this exact head.
- Focused run: `/home/paudley/Active/sparrow/.stage/184/test1.log`,
  `cargo test --release -p sparrow-server --lib watches::`, 12 passed, `EXIT=0`. It names the
  four new tests.
- `prose-check` on `docs/architecture.md`, the six changed `.rs` files and the commit
  message: exit 0.
- Static trace of each state the old 50 ms tick served, at head line numbers:
  - Stop request: `watch::Sender<bool>`, set in `shutdown` and `Drop`. It is awaited through
    `wait_for` (`watches.rs:955-959`, cancel-safe, checks the current value first) in the idle
    select (biased first, `:1050`) and in the connect and publication selects (`:1623`,
    `:1694`). The loop head also checks it (`:1386`). No missed wake.
  - Commands: `commands.recv()` in the idle select (`:1051`) is cancel-safe. The received
    command is handled first by the next pass (`:1406-1409`). A command queued during a pass is
    returned at once by `recv`.
  - Inotify: each enabled detector's directory fd, plus the new per-held-file `appends` fd for
    tail owners (`:797-802`, `tailing.rs:42-48`, `:304-309`), is registered as `AsyncFd` after
    the pre-block drain. An epoll ADD of an fd that is already readable reports it ready, so an
    event between the drain and the registration is not lost.
  - Operation permit: `acquire_owned` arm only when a tail drain or tail claim waits
    (`:1015`, `:1035-1041`, `:1647`). It is cancel-safe. Any carried permit is dropped at the
    start of every wait (`:1001`) and before an ordinary publication (`:1661`).
  - Deadlines: candidate quiet clocks when claims are admitted, and the periodic rescan
    (`due`, `:782-794`). Claim retry backoff (`:1744-1746`, plus `blocked` at `:1753-1758` for
    the due-claim waits). Connect throttle (`CONNECT_RETRY` at `:1630`, `wake.by(connect_after)`
    at `:1637`). Free-space recheck at 1 s only while short (`:1392-1394`).
  - Consume IN_OPEN quiet clock: `drain` still restarts it (`:707-725`). `due` is computed
    after the pre-block drain, so a reset clock moves the deadline. The worker's own `observe`
    is a stat through `/proc/self/fd`; its directory opens and `root_binding` raise only
    ISDIR or nameless events, which `drain` ignores. This is the self-event loop the
    pre-block drain removes.
  - Tail backlog: `busy` (`:762-770`) keeps passes running while a held descriptor is not
    caught up and space and a permit allow it. `idle` counts consecutive no-progress drains
    over a rotation (`tailing.rs:333-335`, `:453-455`) and is reset by attach, finish, held-file
    events and progress.
  - Tail finish: comes as a command; `finish` resets `idle` (`tailing.rs:191-192`).
  - Held tail files whose names left coverage: per-descriptor `MODIFY|ATTRIB` watch.
    `IN_IGNORED` from `release` resets `idle`, which costs at most one rotation.
  - Pending claims: every consumption-journal writer is a `ClaimRoot` method reached only from
    the worker. Tail store writes (`tail_start`, `tail_append`, `tail_discontinue`) create no
    consumption journals. `pending` is re-read after commands and capture (`:1530`). The
    plan's "no pending-claim notify needed" claim holds.
  - Deferred-capacity rescan (PR #181, consume only): the condition is unchanged (`:1601-1609`).
    One wake gap; see Finding 1.
- Busy-spin check: a past deadline the pass cannot act on was not found. Past retries make
  `work` Some. A past `next_reconcile` triggers a rescan or comes from a scan already in
  progress. A past quiet clock is either claimed or reset by `ready`. `connect_after` is always
  in the future when it is added. Every `busy` pass does scan or capture work. The worker's
  own events are consumed before it blocks. The worker's own capture reads raise only
  IN_ACCESS, which neither watch mask treats as activity.
- Worker health (`main.rs:70-120`): a oneshot sender held in the thread closure
  (`watches.rs:884`) or the spawned task (`maintenance.rs:38-41`, `syslog_listener.rs:229`).
  It closes on return, on unwind, and when the task is dropped or aborted. The `health` future
  is pinned once and is not polled again after it resolves, so the "await at most once" rule
  holds.
- No sub-second timer remains in `watches.rs` or `main.rs` (grep for `sleep`, `interval(`,
  `from_millis`, `TICK`, `AtomicBool`). The remaining `yield_now` calls run only after real
  work.

## Findings

1. **REQUIRED**: missed-wake race in the pre-block drain.
   - Location: `crates/sparrow-server/src/watches.rs:1006-1018`, together with `:1530-1531`
     and `:1601-1609`.
   - What happens: `pause` drains every enabled detector just before it blocks. This drain
     consumes events that arrived during the pass and changes detector state. The only state
     changes `pause` then re-evaluates are `busy` and `due`. Two effects of those events are
     never re-evaluated, and the inotify queue they came from is now empty, so nothing wakes
     the worker for them:
     - (a) Deferred-capacity rescan. A consume watch sits at its candidate limit
       (`deferred == true`, `candidates.len() == max_candidates`) with excess files waiting.
       The producer deletes or moves out one candidate while the worker is mid-pass. The
       DELETE or MOVED_FROM is read by the pre-block drain, the candidate is removed, and the
       worker blocks. The "deferred candidate capacity available" rescan (`:1601-1609`) runs
       only in a later pass with claims admitted. With no other candidate left, that pass
       comes at the periodic rescan, which is at least 60 s and up to `2*quiet_ms + 1s`. With
       other candidates left, it comes after their quiet interval. The old tick ran it within
       50 ms. The coordinator listed this state as one that needs a real wake source.
     - (b) Status freshness. `registry.update` runs before `pause` (`:1531`). Candidates,
       `deferred`, `failure` and tail descriptor counts changed by the pre-block drain are
       published only at the next wake. Example: a tail watch with no held descriptors has
       an attach refused in the pre-block drain. `candidate()` sets `failure` and
       `deferred`, `caught_up()` is true, the watch is not busy, and `watch list` hides the
       failure until the next rescan. Example: a new consume candidate stays invisible for
       up to `quiet_ms`, or longer while claims are not admitted. Before, status lagged by at
       most 50 ms.
   - Both effects are bounded by the periodic rescan, and no data is lost. They are still a
     change in semantics and a missed wake, which this PR's scope ("change only how the
     threads wait, keeping semantics") and the review brief rule out.
   - Suggested fix (small): after the pre-block drain in `pause`, (i) treat
     `claims && covered && deferred && Owner::Consume && todo empty && scan none &&
     candidates.len() < max_candidates` as due now; adding it in `due(claims)` as
     `Some(Instant::now())` is enough, since the next pass rescans or claims and cannot spin.
     (ii) Republish the registry after the drain: pass `registry` and the pass's `pending`
     into `Waiting`. Do not answer this with a blanket "run again whenever the drain read
     anything", which would bring back the self-event loop the drain exists to stop. Add
     one paused-clock case for (a) if it is cheap. It can avoid std-clock deadlines by
     asserting the rescan count (`reconciliations` or `directories`) right after a DELETE
     lands mid-pass. If that is impractical, record the reason.

2. **OPTIONAL**: three of the four new tests sit on the old 50 ms grid, so they would likely
   pass against the old tick.
   - Location: `crates/sparrow-server/src/watches/tests.rs:807-811`, `:828-841`,
     `:863-871`.
   - Why: on the paused clock the old worker's ticks fell on exact multiples of 50 ms from
     the start. Each of these tests acts at exactly 1000 ms, which is the old worker's 20th
     tick. `LocalSet::run_until` polls the test body before its local tasks. So at 1000 ms the
     test sends stop, sends the command, or drops the lease, and then the old worker's
     1000 ms tick handles it with no clock change, or within the test's 1 ms window.
   - Their docstrings say they prove the old 50 ms poll is gone ("as the 50 ms stop poll
     needed", "not at the next tick"). They do not prove that. They do still guard the new
     arms: removing the stop, command or permit arm would leave the worker blocked until its
     60 s rescan, and the tests would fail.
   - `an_idle_worker_wakes_at_most_once_a_second` (`:774-793`) does discriminate: the old
     tick gives 600 passes in 30 s against a bound of 30. As a suite the contract is covered.
   - Suggested fix: act at an instant off the 50 ms grid (for example
     `sleep(Duration::from_millis(1025))` before stop, the command, or the lease drop), or
     reword the docstrings to the invariant they actually protect.

3. **OPTIONAL**: a carried permit can be held across the consume connect attempt.
   - Location: `crates/sparrow-server/src/watches.rs:1620-1633` against `:1658-1662`.
   - Scenario: a wait acquires a permit for a tail drain or tail claim (`:1057-1061`). In the
     next pass a queued command (`try_recv`) disables or retires the tail detector or claim,
     so neither consumes the permit. The selected work is then an ordinary claim with no
     client and `connect_after` passed, so the worker holds one shared operation permit
     through the connect select, for up to 10 s. The permit is dropped only at `:1661`, after
     the connect. The window is narrow, and the connect is to the local socket.
   - Suggested fix: move `*waiting.permit = None` to the top of the `!is_tail` branch, before
     the connect.

Findings 2 and 3 do not block. Finding 1 blocks.

VERDICT: BLOCKED
