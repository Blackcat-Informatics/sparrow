# Delivery review: PR #367 (issue #365), head 2b42a8e24c32bfd30485b6e02f0d7de16412a751

Branch `paudley/365-show-the-archive-worker-s-phase`, worktree
`/home/paudley/Active/sparrow/.worktrees/365-show-the-archive-worker-s-phase`, base
`b2ba7151833e9f9018c8701a70e6b5138b4836b7` (origin/main). This was a read-only review: no edits,
builds or commits in the worktree.

## Scope

Requirement: `stagectl issue 365` (body, acceptance; zero comments). Diff:
`git diff origin/main...HEAD` (one commit, 8 files, +409/-9).

Read in full: the diff; `crates/sparrow-client/schema/sparrow.capnp` `struct Status` and the new
types; `crates/sparrow-server/src/archive/status.rs`; `archive.rs` `Flight`, `ArchiveError::code`,
`archive_segment`, `write_segment`, `verify_segment`, `set_aside`, `publish_position`, `drain`,
`drain_segments`, `Worker::start`, `run`, and the test with its helpers (`archiving_store`, `nas`,
`flight`); `rpc.rs` `status` (3187-3360) and `now()`; `main.rs` 295-352; `agent/status.rs`
`archive_worker`; `docs/interfaces.md` 636-642 and "Archive worker status (#365)";
`docs/archive-format.md` "Archive status" and section 8. In sparrow-core: `archive_prepare`,
`select` (rebuild path), `archive_held`, `hold_reason`, `archive_state`, `ArchiveState`,
`status_report`/`status_counted` head.

Not read: the rest of `rpc.rs` and `agent/status.rs`; `encode_selection`, `archive_intend` and
`archive_commit` bodies; `pressure_status`; the watchdog (`watch`) body; MCP wiring beyond
`agent::status::execute`. I did not run any test or build.

## Evidence

- `/home/paudley/Active/sparrow/.stage/365/commit.log`: full `make check` (fmt, release Clippy
  `-D warnings`, check-shipped, release tests, census self-tests, `--cross --check`) in this
  worktree, ending `EXIT=0`; line 637 shows
  `archive::tests::status_shows_a_refused_segment_as_holding_and_clears_when_a_pass_commits ... ok`.
  The log records HEAD `b2ba715` at its start and finished at 08:56:58, after the commit's
  08:39:43 date. I checked that this is consistent: a probe commit in a scratch repository (since
  removed) showed that Git stamps author and committer dates before the pre-commit hook runs.
- `/home/paudley/Active/sparrow/.stage/365/test.log`: the focused test, 1 passed, 140 filtered.
- `git grep -i -E 'patrick|ruling|ruled' -- . ':!AGENTS.md' ':!CITATION.cff'`: exit 1, no match.
- `prose-check docs/interfaces.md docs/archive-format.md .../archive/status.rs`: exit 1, but
  every flag (`UInt64`/`UInt32` at interfaces.md 138, 1989, 2143, 2145, 2202, 2286, 3591, 3596,
  4583) is on a line outside this diff. `prose-check --stdin < pr.md`: exit 0.

## Judgements on the questions asked

1. **Additive wire change: yes.** `Status` ordinals 0..64 are unique; the highest on origin/main
   is 63 and `archiveWorker` is @64. The schema diff removes no line. `ArchivePhase`,
   `ArchiveWorkerStatus`, `ArchiveFailure` and `OptionalArchiveFailure` are new names numbered
   from 0, and no other name in the schema collides with them. `SCHEMA_VERSION` stays 71; the
   additive `archiveHalvings` (#324) did not bump it either.
2. **Status cannot block on the worker or the writer lock through this field.** `encode` takes
   only the `ArchiveStatus` mutex. The worker holds that mutex only for in-memory assignments:
   `publish_position` reads `store.archive_state()` before locking, `pass_failed` gets its
   reason already formatted, and no other lock is taken while it is held. The worker does its
   NAS I/O without the store's writer lock, so a hung mount leaves `phase` at `writing` or
   `verifying`, readable. A poisoned mutex is recovered with `PoisonError::into_inner`, the same
   as `Flight::state`. Each field is written whole inside one critical section, and nothing in
   those sections can panic except allocation, so the recovered value is a coherent snapshot and
   no error is lost. Production has one `Rpc` (main.rs 299), and it gets `with_archive_status`.
   The other `Rpc::new` calls are all in test modules. The rest of the `status` call (pressure,
   durability) predates this change and was not assessed.
3. **`since` = 0 on a failed clock read.** When only the clock fails, the clock error itself is
   the reported failure (code `INVALID_ARGUMENT` from `rpc::now`, reason `clock: ...`), so that
   error is not swallowed. Epoch 0 is unambiguous in practice. Two things are still wrong. When
   both the store id and the clock fail, `now.unwrap_or(0)` discards the clock error: it is not
   in the cause and not journaled. That turns an error into a default, which AGENTS.md forbids.
   And the schema already has `OptionalInstant`, the repository's type for an instant that may
   be absent. This field is new, so the type is free to change now; after merge, any change is
   no longer additive. See finding R1.
4. **Clearing on any clean pass is correct for failure-class holds.** While an intent stands,
   `select` rebuilds that range and ignores whether it is due. So the pass after a refusal always
   retries the refused range, and a pass ends clean only if it committed or no intent stands.
   During a retry, `phase` shows `preparing`/`writing`/`verifying` with the old failure still
   present, which matches the docs. A record held at the head (`HoldReason`) is not a failure:
   the pass ends clean and `phase` returns to `idle`. That hold appears in
   `pressureStatus.archiveHeld` and in the journal, and the docs say the held listing is not in
   the schema. The published position, however, is wrong after every start until the first
   in-pass publish, and it is never published on store-id or clock-failure passes. See finding
   R2.
5. **The test drives the real pass.** It calls `drain`, the function `run` calls, against a real
   `Store` and a real directory, with real write, read-back and commit. Its claims hold. The
   forged file is kept by `write_segment` ("a file already at its name ... is kept and
   verified"), so read-back refuses it with "contents differ". `through` stays `None` and the
   intent stays in flight. `since` keeps `LATER` across a second pass. After the file is
   renamed, one segment commits, the failure clears, `phase` is `idle`, and `through` equals the
   segment's `through` with no intent left. Not covered: `Worker::start` (it publishes `idle`)
   and `run`'s store-id and clock branches, `encode` and the CLI/MCP render (plain mappers, which
   repository rules exempt), `since` resetting when the code or segment changes, the
   decoder-refusal set-aside path, and reason truncation. The issue's "shows the failure in
   status" is met at the snapshot that `encode` serializes.
6. **The docs match the code except in two places.** The `through` text ("empty before the first
   commit") is contradicted by R2. The `since` text is accurate to the code but changes with R1.
   The anchors `#8-write-protocol` and `#archive-worker-status-365` resolve. The statement that
   status "never waits on the worker or takes the store's writer lock" is accurate for this field.

## Findings

**R1. REQUIRED: `since` turns a failed clock read into the value 0. In one branch it also drops
the clock error.**
`archive.rs` `run`, the `(Err(error), now)` arm: `pass_failed(error.code, "", &cause, now.unwrap_or(0))`.
When `now` is `Err`, the clock error disappears. The `(_, Err(error))` arm hard-codes 0.
`ArchiveFailure.since @3 :UInt64` therefore freezes a sentinel into a new wire field, and
`Failure.since: u64` carries "unknown" as a number. Required change:
- On the wire: `since @3 :OptionalInstant`, absent when the clock failed.
- In memory: `since: Option<u64>`.
- In the double-failure arm, put the clock error in the cause, for example
  `store id: ...; clock: ...`.
- If the standing `since` is absent and the same failure repeats once the clock works, fill it
  in. As written, the keep-start rule keeps 0 for as long as that failure repeats.
- Update the capnp comment, `interfaces.md` and `agent/status.rs` (render `since` as null when
  absent).

**R2. REQUIRED: until the first in-pass publish, the published watermark and intent are wrong.**
`Worker::start` publishes only `Phase::Idle`. `drain_segments` first publishes the position after
`archive_prepare` returns, and preparing can take a while because it encodes a whole segment.
`run`'s store-id and clock-failure arms never publish it at all. So after every daemon start,
status reports `through` empty and `intentInFlight` false even when a durable watermark and a
refused intent exist. If every pass fails before `drain`, that state lasts indefinitely. This
contradicts the documented contract ("the durable `archive:through` watermark; empty before the
first commit") and the issue's requirement that status report the watermark. Required change:
publish the position from `store.archive_state()` when the worker starts, before the first pass.
Publishing it at the top of each pass, before `archive_prepare`, also works. Either way, the
startup and pre-drain failure paths must carry the real watermark. The doc text can stay as it
is once the code matches it.

**O1. OPTIONAL: `let _ = publish_position(flight, store);` in `drain`'s error path discards a
store error without any record.** The pass failure is still reported, and the stale position is
usually still right: the position was already published after prepare, and a failed prepare
records nothing new. Once R2 publishes the position at start, this call adds almost nothing.
Either remove it, or journal its error with its code instead of discarding it.

**Unrelated (one line):** the shared hook config sets `hook.sparrow-verify.command` to
`make precommit`, but neither this tree nor main's `Makefile` has that target
(`make -n precommit` exits 2), while `make install-hooks` writes `make check`. The next commit in
any checkout will fail at the hook.

VERDICT: BLOCKED
