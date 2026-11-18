# Review of PR #354 (issue #348), head 55d9ebd

Reviewed 2026-11-17T22:42:58-06:00. Read-only: no edits, builds, tests or commits.

## Scope

- Branch `paudley/348-flaky-test-expiry-below-the-floor-hands`, head
  `55d9ebd6b6631d981f7043d5a806e1b8e9d3d309`, two commits over merge base `9ed5943`
  (`65498f7`, `55d9ebd`).
- Net diff vs `origin/main` (`ec9e0c6`): `crates/sparrow-core/src/storage/forced_compaction.rs`
  only, +16. The test file `crates/sparrow-core/tests/storage.rs` nets to no change: `65498f7`
  added `store.await_native_quiet()` before `drop(store)` and `55d9ebd` removed it.
- Requirement (issue #348): show the cause of the ENOLCK failure from the code, and correct the
  test or the code so it cannot recur.

## What I read

- `stagectl issue 348` and `stagectl issue 354` (the PR body); `git show 65498f7` and
  `git show 55d9ebd`.
- `crates/sparrow-core/src/storage/forced_compaction.rs`, all 420 lines (thread body 163-204,
  `wait_finished` 257-267, unit tests 348-420).
- `crates/sparrow-core/src/storage/pressure.rs` 600-704 (`end_wait`, `collect_forced`,
  `no_archive_floor`), 840-1060 (`retire_pass`, including the `Step::Native` / `Forced` /
  `Measured` machine at 972-1015), 1180-1222 (`await_native_quiet`, `pressure_maintain`), and
  `begin_wait` at 710-724.
- `crates/sparrow-core/tests/storage.rs` 6788-6890 (the test, all three sections).
- Hook log `.stage/flaky/commit348b.log` (head, tail, test-result and named-test lines).
- `.stage/flaky/body354.md`, checked with `prose-check` (exit 0).
- `git merge-tree --write-tree origin/main 55d9ebd`: exit 0 (clean). `git diff --stat
  9ed5943 origin/main` on `forced_compaction.rs`, `pressure.rs` and `tests/storage.rs`: empty,
  so main has not moved under these files.

## What I did not read

- The rest of `pressure.rs` and `storage.rs`, `catalog_db` and `Store::open`.
- Other tests that drop a store and reopen its directory: I did not check whether any of them
  can do so while a forced compaction is still `Running` (a case this change does not address,
  and not the case in this test).
- The rust-rocksdb and RocksDB sources; the reading that "lock hold by current process" is
  RocksDB's in-process lock table reporting ENOLCK is from the error text, not from source.

## Evidence assessment

- Cause. Before the change, the compaction thread wrote `Slot::Finished`, notified, called the
  waker, and only then dropped its moved-in `Arc<DB>` at the closure's end. `wait_finished`
  returns once the slot is not `Running`, so a caller could see the end, drop the store, and
  reopen the directory while the thread still held the last `Arc<DB>`. The thread's handle
  keeps the DB, and so its LOCK file, open. That gives exactly the observed in-process message.
  The reasoning holds from the code, and the PR body labels it an inference.
- Fix. `drop(db)` now runs before the slot write, under program order in the same thread, and
  the slot mutex orders it before any `wait_finished` return that observes `Finished`. The
  closure keeps only `owner` (`Arc<Forced>`), and `Forced` holds no DB handle (its `waker` is
  an `Arc<Mutex<Option<RetentionWaker>>>`, unset in this test). If the thread holds the last
  handle, RocksDB closes on that thread before `Finished` is published, which is harmless. The
  fix is correct.
- Removing the test's extra wait is safe. A forced compaction starts only in
  `Step::Native` (pressure.rs 975-997), and that branch always returns with phase
  `AwaitingReclamation` (`state.enter(...)` at 978), never `Blocked`. `Blocked` comes from
  `no_archive_floor` (700) or the floor-bound branch, and an error makes the test's `unwrap()`
  panic first. So the pass that ends the loop starts no compaction. Every pass that does start
  one is followed by another iteration, and that iteration calls `await_native_quiet` before
  its pass, so the compaction is seen as `Finished` (and, with the fix, has released its DB)
  before `drop(store)`. Sections two and three call no pass that can reach `Step::Native`
  (section two only reads status; section three's pass returns `Ready` with free space back).
- Unit assertions. `Arc::strong_count(&db) == 1` after `wait_finished` states the invariant
  the fix creates ("finished means no database handle") on the real thread path, for success,
  a panic, and a native error. With the fix it is deterministic, because the happens-before
  chain above makes it so. It adds no test, only two assertions to existing behavioural tests.
- Hook: `commit348b.log` shows the tree (`.worktrees/348-...`), parent `65498f7`, the commit
  `55d9ebd`, and `EXIT=0`. The two forced-compaction unit tests (lines 479-480) and
  `expiry_below_the_floor_hands_its_retirement_to_the_pressure_wait` (line 669) ran and passed.
- PR body: its claims match the code and the log. It says the failure was not reproduced and
  that the causal link is inferred. It cites one run and names it.

## Findings

- **OPTIONAL**: the `strong_count` assertions catch a regression of the drop order only
  probabilistically. Without the fix, the window between the slot write and the closure's end
  is a few instructions, so the assertion would usually still pass. It documents the invariant
  and is cheap; it is not a deterministic regression test, and the PR body does not claim one.

No REQUIRED findings.

VERDICT: PASS
