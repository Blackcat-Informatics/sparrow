Reviewer model: claude-opus-5-5 (opus)
Head: 4954964431ca0af4937103c17dc895048cf8abbb

# Delivery recheck: Sparrow PR #185 (issue #184, rows 1 to 6)

## Scope

- Worktree `/home/paudley/Active/sparrow/.worktrees/184-replace-or-slow-the-sub-second-ticks-50`.
  HEAD is confirmed as 4954964431ca0af4937103c17dc895048cf8abbb and the tree is clean.
- 4954964 is one test-only commit on top of ca4c9c2. It adds 71 lines to
  `crates/sparrow-server/src/watches/tests.rs` and changes nothing else under `crates/`.
- This recheck covers the new test's value and correctness. It also covers the two optional
  findings that review-2 left open (`/home/paudley/Active/sparrow/.stage/184/review-2.md`).
- Static review only. I ran `git diff ca4c9c2..4954964`, `git log`, `grep` and
  `prose-check`. I ran no cargo, build, test or daemon command.

## Evidence

- Commit hook at 4954964: `/home/paudley/Active/sparrow/.stage/184/hook3.log`. The hook's
  commit line for 4954964 is at line 1184 and the log ends with `EXIT=0` (line 1186). The new
  test `the_drain_before_a_wait_reports_and_wakes_for_what_it_found` passes there in release
  (line 433). The log has no `FAILED` line.
- `prose-check` on `watches/tests.rs` and on the 4954964 commit message: exit 0.
- The validation index is now current. It names head 4954964, `hook1.log` to `hook3.log`
  each with `EXIT=0`, the focused run (13 passed, `test3.log`), and reviews 1 and 2. This
  closes review-2 Finding 1.

### The new test (`tests.rs:890-959`): correct and valuable

1. **Setup.**
   - `Detector::open` reconciles and queues the root.
   - One `scan_step` covers the root and adds `first.bin` as the only candidate, which fills
     `max_candidates` (1).
   - `drain` then consumes the scan's own directory-open events. Those events are ISDIR or
     have no name, so `drain` ignores them.
   - The test asserts that the detector is covered and has one candidate.
2. **Events during the pass.** The test writes `second.bin` and deletes `first.bin`. The writes
   are synchronous, so their events (CREATE, OPEN, MODIFY, CLOSE_WRITE for `second.bin`, then
   DELETE for `first.bin`) are already queued before `pause` runs.
3. **`pause` call.** The test calls `pause` with `busy = false`, `claims = true` and
   `space_ok = true`, with stop already requested.
   - The pre-wait drain reads `second.bin` while the candidate set is full, which sets
     `deferred`. It then reads the DELETE, which removes `first.bin` and leaves no candidates.
   - Next comes the registry update added in ca4c9c2.
   - The detector is not busy, so `pause` reaches the biased select. The stop arm wins at
     once, so the test cannot hang.
4. **Assertions.**
   - The published status shows `deferred` and `candidates == 0`. This proves the registry
     update runs after the drain.
   - `due(true) <= now`. This proves the deferred-capacity condition makes the rescan due at
     once.
   - `due(false) > now`, which is the rescan about 60 s away. This proves the `claims` gate,
     so the due-now branch cannot fire in the waits where claims are not admitted.
5. **It fails against the pre-fix code at cb644d4.**
   - Without the registry update in `pause`, the registry stays empty and
     `snapshot().pop().unwrap()` panics.
   - Without the due-now branch, `due(true)` returns `next_reconcile`, which is about 60 s
     ahead, so the assertion fails.
6. **It is deterministic.**
   - It uses the real clock but no timers, and it never waits on wall time.
   - The `due(true) <= Instant::now()` comparison is ordered correctly: the `now` inside `due`
     is taken earlier than the one in the assertion.
   - It does not depend on how inotify coalesces events. Any one of the `second.bin` events
     sets `deferred` while the candidate set is full, and the DELETE is queued after them.
7. **What it protects.** Two production behaviors through the real inotify path and the real
   `pause`:
   - the status publication after the pre-wait drain;
   - the deferred-capacity wake.

   It is not a mirror of the implementation. It checks the wake decision (`due`) rather than
   an observed wake. That is the right seam here, because the select arms are already covered
   by the paused-clock tests. Building `Waiting` and `Control` directly ties the test to
   private types, but it sits in the module's own test file, so that coupling is acceptable.

## Findings

Findings: none.

VERDICT: PASS
