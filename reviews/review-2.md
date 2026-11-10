Reviewer model: claude-opus-5-5 (opus)
Head: ca4c9c2416a65fdf0fcff8c94bd30149721b777d

# Delivery recheck: Sparrow PR #185 (issue #184, rows 1 to 6)

## Scope

- Worktree `/home/paudley/Active/sparrow/.worktrees/184-replace-or-slow-the-sub-second-ticks-50`.
  HEAD is confirmed as ca4c9c2416a65fdf0fcff8c94bd30149721b777d and the tree is clean.
  ca4c9c2 is one commit on top of cb644d4 ("Report and act on the events the worker drains
  before it waits") and changes 2 files (+32/-11): `crates/sparrow-server/src/watches.rs` and
  `crates/sparrow-server/src/watches/tests.rs`.
- This is a recheck. It covers the changed work and the three open findings from
  `/home/paudley/Active/sparrow/.stage/184/review-1.md`. Unchanged code that review-1 already
  passed is not re-reviewed.
- Static review only. I ran `git diff cb644d4..ca4c9c2`, `git log`, `grep` and `prose-check`.
  I ran no cargo, build, test or daemon command.

## Evidence

- Commit hook at ca4c9c2: `/home/paudley/Active/sparrow/.stage/184/hook2.log`.
  - It shows the hook commit line for ca4c9c2 (line 1182) and `EXIT=0` (line 1184).
  - The four wake tests pass in release (lines 427 to 434), and so does
    `maintenance::tests::the_health_loop_fails_a_no_intake_daemon_whose_maintenance_worker_ends`
    (line 456).
  - The log has 14 `test result: ok` lines and no `FAILED` or `panicked` lines.
- `prose-check` on both changed files and on the ca4c9c2 commit message: exit 0.

### Review-1 Finding 1 (REQUIRED, missed wake in the pre-block drain): resolved

- **(a) Deferred-capacity rescan.**
  - New code: `Detector::due(claims)` (`watches.rs:782-805`) returns `Some(Instant::now())`
    when all of these hold: the watch is enabled, covered, has an empty `todo`, no scan in
    progress, `claims` is true, it is `deferred`, it is a consume watch, and
    `candidates.len() < max_candidates`.
  - `due` is evaluated after the pre-block drain, so freeing capacity during that drain now
    ends the wait at once.
  - This is the same condition as the rescan arm in `run` (`:1616-1626`), plus the
    `enabled` / `claims` guards that the claims block applies before reaching that arm.
- **Spin check for the due-now case.** The case can only arise in the final pause, the only
  one that passes `claims` through. The client-none, permit and first-branch pauses pass
  `false`. In the pass that the immediate deadline starts, one of these happens:
  - **Claims still admitted and no detector's `ready()` returns `Some`.** The claims loop
    reaches the deferred arm for this detector, because it breaks only on `Some`. The arm
    rescans: `todo` refills and `deferred` becomes false. Now `due` returns `None` and the
    detector is `busy`, which is real scan work.
  - **Some detector's `ready()` returns `Some`.** The pass reserves a claim, `work` becomes
    `Some`, and it publishes, so no pause runs.
  - **`ready()` fails.** The error propagates.
  - **Claims are no longer admitted** (`work` is `Some`, or `pending` is at
    `MAX_CONSUMPTION_JOURNALS`). Every pause in that pass passes `claims == false`, so the
    due-now branch is off.
  - **Space is short.** The first branch pauses with `claims == false` and a 1 s recheck.
  - **The top-of-pass periodic rescan or a drain reconcile clears `deferred` or refills
    `todo`.** Then `due` returns `None`.

  So no pass can return to an immediate deadline without doing scan or claim work.
- **(b) Status freshness.** `pause` (`watches.rs:1018-1025`) now calls
  `registry.update(detectors, &store.consumption_pending()?)` right after the pre-block
  drain, using the `store` / `registry` fields added to `Waiting` (`:986-995`, filled at
  `:1548-1555`).
  - Errors propagate with `?`.
  - The update reads only the store (`consumption_pending`, and `TailRoot::status` for tail
    watches). The store cannot overlap a watched root, so the update queues no inotify event
    and cannot wake the wait it precedes.
  - The cost is one extra pending read per blocking wait, not per tick. The busy path, which
    returns at `:1012-1015`, skips it, which is correct because the next pass republishes.

### Review-1 Finding 2 (OPTIONAL, tests on the 50 ms grid): resolved

- `SETTLED = 1025 ms` (`tests.rs:772-773`) replaces the 1 s settle in all four tests. The old
  ticks fall at 1000 ms and 1050 ms, so a ticking worker would sleep through the action at
  1025 ms:
  - The stop and command tests would observe 1050 ms rather than 1025 ms, and fail.
  - The permit test checks at 1026 ms, before the 1050 ms tick, and would fail.
- The doc comment now states the reason for the offset. The tests now discriminate the old
  tick as their invariants claim.

### Review-1 Finding 3 (OPTIONAL, permit held across the connect): resolved

- `*waiting.permit = None` moved to the top of the `!is_tail` branch (`watches.rs:1630-1633`),
  before the lease-replacement check and the connect select.
- The tail branch still consumes the carried permit through `admit`.
- The old drop in the `else` arm was removed, which leaves one drop point for ordinary work.

## Findings

1. **OPTIONAL**: the validation index is stale.
   - Location: `.stage/replace-or-slow-the-sub-second-ticks-50/validation.md:7` and `:13`.
   - Problem: it still names head cb644d4 and `hook1.log`. Someone reading the index would
     not know which evidence covers the published head.
   - Fix: record ca4c9c2 with `/home/paudley/Active/sparrow/.stage/184/hook2.log` (EXIT=0),
     and list this review.
2. **OPTIONAL**: there is no regression case for the deferred-capacity wake or the
   post-drain registry update.
   - Location: `crates/sparrow-server/src/watches/tests.rs`.
   - The fix is small, and I checked it statically above.
   - A paused-clock case would protect it against a later refactor of `due`: freed capacity
     in a deferred watch should be rescanned with no clock advance, observed through
     `reconciliations` or `directories` in the registry.
   - Review-1 offered this test as optional, so leaving it out does not block.

No REQUIRED findings.

VERDICT: PASS
