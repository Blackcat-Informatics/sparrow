# Delivery review: issue #365, head 16dd3644386161c8a2ea1b18a042fd3fa5488c69

Branch `paudley/365-show-the-archive-worker-s-phase`, worktree
`/home/paudley/Active/sparrow/.worktrees/365-show-the-archive-worker-s-phase`. The diff is
`git diff main...HEAD`, with main at `87dfe48`. It has two commits: `2b42a8e`, the change, and
`16dd364`, the fixes for the prior review, plus a merge of main (`be1972f`). The diff touches
8 files (+490/-14). This was a read-only review: no edits to the worktree, and no cargo, make
or gate runs.

## Scope

Requirement: the three acceptance points of issue #365 as given in the brief. Prior findings:
`/home/paudley/Active/sparrow/.stage/365/review-2b42a8e.md` (R1, R2 REQUIRED; O1 OPTIONAL).

Read in full: the whole diff. That covers the schema (`struct Status` tail and the new types),
`crates/sparrow-server/src/archive/status.rs`, and in `archive.rs` `ArchiveError::code`,
`archive_segment`, `publish_position`, `segment_file_name`, `drain`, `drain_segments`,
`Worker::start`, `fail_pass`, `pass`, `run` and the two new tests. It also covers `rpc.rs`
(field, `with_archive_status`, the `encode` call at 3354), `main.rs` 295-352,
`agent/status.rs` `archive_worker`, `docs/interfaces.md` and `docs/archive-format.md`. Also
read: `OptionalInstant`/`Instant` in the schema, `agent::instant`, the `status::execute`
callers (CLI default and MCP tool), `Store::archive_state` and `ArchiveState` in sparrow-core,
the `Worker` struct (it has no `Drop`/join; dropping the senders stops the threads at their
next wait), and `.stage/365/pr.md`.

Not read: the rest of `rpc.rs` `status`; `write_segment`/`verify_segment` bodies (relied on the
prior review's reading, unchanged in 16dd364); `archive_prepare`/`select`/`archive_commit`
bodies; the watchdog `watch` body; `tests/rpc.rs`.

## Evidence

- `.stage/365/check.log`: header names the worktree and `16dd3644...`; full `make check`
  ending `EXIT=0` (mtime 15:13, after the 14:58 commit). Lines 808 and 813 show both new
  tests `ok`; 20 `test result: ok` lines, no `FAILED`.
- `.stage/365/commit2.log`: hook run for `be1972f`, `EXIT=0`.
- `git diff main...HEAD | grep '^+' | grep -iE 'patrick|ruling|ruled|said'`: exit 1, no match.
- Added `unwrap()`/`expect(` lines: all in the two test functions; none in production code.
- `prose-check` on the added lines of both docs, the schema and `status.rs`: exit 0.
  `prose-check .stage/365/pr.md`: exit 0.
- `Status` ordinals: no duplicates; the last two are @63 and @64. The new names
  (`ArchivePhase`, `ArchiveWorkerStatus`, `ArchiveFailure`, `OptionalArchiveFailure`) are
  numbered from 0 and do not collide. The schema diff removes no line.

## Prior findings at 16dd364

- **R1 (since as sentinel 0; clock error dropped): mostly closed.**
  `ArchiveFailure.since` is `@3 :OptionalInstant`. `Failure.since` is `Option<u64>`.
  `encode` writes `absent` or `value.unixMillis`. `agent/status.rs` renders `null` or the
  instant. `pass_failed` keeps a standing start with `standing.since.or(now)`, so a missing
  start is filled in once the clock reads. When both the store id read and the clock fail, the
  cause carries `store id: ...; clock: ...`. The clock-only branch reports `clock: ...` with
  `since` absent. One branch added in this head still drops the clock error; see R3.
- **R2 (watermark wrong until first in-pass publish): closed.** `Worker::start` calls
  `publish_position` before it spawns the threads, so the published state is correct before
  any pass runs. `pass` publishes at its top, before the store id and clock branches, so passes
  that fail before `drain` carry the real watermark too. `drain_segments` republishes after
  prepare, after each commit and after the loop. If the start-time read fails, `Worker::start`
  returns `io::Error::other(ArchiveError)`, which keeps its source, and the daemon's `main`
  propagates it with `?`. `archive_state` fails only when the store lock fails.
- **O1: closed.** The secondary `publish_position` error in `drain` is journaled to stderr and
  `tracing::warn!` with a structured field.

## Judgements on the questions asked

1. **Additive and unique wire ordinals: yes** (see Evidence). A reader of an older daemon gets
   a null struct: phase `notConfigured`, an empty `through` and an `absent` failure, as the
   doc comment in `agent/status.rs` says.
2. **The failure clears after the refused file is removed and a pass commits: yes, by the
   code.** `drain` calls `pass_succeeded` (phase `Idle`, `failure = None`) only when
   `drain_segments` returns `Ok`. That happens only after `archive_prepare` returns `None` and
   `archive_held` succeeds, so a pass that retries the rebuilt intent must commit it before
   anything clears. The test goes through the real `drain`, a real `Store` and a real
   directory. It forges the file and fails two passes: `since` stays `LATER`, `through` is
   `None` and the intent is in flight. It then renames the file and commits one segment, after
   which the failure is gone, the phase is `Idle`, `through` equals the segment's `through` and
   no intent is left.
3. **Fields populated at start: yes** (R2 above).
4. **No unwrap/expect in production paths: yes.** `unwrap_or("")` on `Option` in `encode` and
   `unwrap_or_else(PoisonError::into_inner)` are not panics.
5. **Errors preserved:** yes, except R3.
6. **No mirror tests:** the first test drives the real pass and asserts the published
   behaviour. The second is behavioural but weak as a regression guard (O2).
7. **No quotations or attributed requirements:** none found.
8. **Docs:** `interfaces.md` lists the fields, ordinals, codes, the 512-character cut, the
   `since` rules, and the CLI/MCP rendering. `archive-format.md` section 8 gives the
   operator's way out: move the refused or conflicting file aside, mount the share or correct
   `STORE_ID`. The next clean pass clears the hold, with no restart. Both match the code.

## Findings

**R3. REQUIRED: the new position-read branch in `pass` drops a clock error, the same defect
R1 closed in the store-id branch.**
`crates/sparrow-server/src/archive.rs:816-817`:
`return Err(fail_pass(flight, error.code(), error.to_string(), at));`. When
`publish_position` fails and `crate::now()` has also failed, `at` is `None` and the clock
error is in neither the status cause nor the journal line `run` writes. The fix is the same as
the store-id arm at 820-825: append `; clock: {clock}` to the cause when `clock` is `Err`.

**O2. OPTIONAL: `a_started_worker_reports_the_stores_watermark_and_intent`
(`archive.rs:1323`) does not reliably catch R2 regressing.** The worker thread runs a pass at
once, and that pass's top-of-pass publish writes the same values. Without the start-time
publish, the test would still pass whenever the thread wins the race. The thread is also
detached and outlives the test's `TempDir`. That is harmless here, because the pass fails at
the missing root. A deterministic variant would assert after a call that cannot publish, or
drop the test once `pass`'s top publish is covered.

**O3. OPTIONAL: one schema comment line runs to about 140 columns**
(`crates/sparrow-client/schema/sparrow.capnp:330`). The surrounding lines wrap near 100.

VERDICT: BLOCKED

Required fixes:
1. `crates/sparrow-server/src/archive.rs:816-817`: when `publish_position` fails and the clock
   read also failed, put the clock error in the cause (`...; clock: {clock}`), as the store-id
   arm already does.
