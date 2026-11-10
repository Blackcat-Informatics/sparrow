Reviewer model: claude-opus-5-5 (opus)
VERDICT: BLOCKED

# #158 delivery review 8: the delivery-failure hook at 950a741

## Scope

Head 950a741c5b3a836f77a6e43c99d5d61204240b2b on `paudley/158-serve-every-query-and-status-call`
(PR #169, WIP). The feature diff is `git diff f1219ee...HEAD`. This review covers commit 950a741,
the three REQUIRED findings of review-7, the PR body
(`/home/paudley/Active/sparrow/.stage/158/pr-body-final.md`) and `validation.md` against the logs
in `/home/paudley/Active/sparrow/.stage/158/`. Rulings were read from `plan.md` `## User rulings`.
The coordinator rulings for the 5 s build wait (`plan.md:65`), the 824.5 s source-summary bound
(`plan.md:66`) and the #183 deferral (`plan.md:62,67`) are recorded verbatim there, and I take them
as given. That closes review-7 O3.

This was a static review. I ran no cargo, make or build command, wrote nothing to /tmp, and edited
no source. I used `git show`, `git log`, `git ls-files`, `grep`, file reads, the logs and
`prose-check`.

## Evidence

- `158-commit8.log`: the commit hook's `make check` (fmt, release Clippy with `-D warnings`,
  release workspace tests with 241 storage tests passed, census self-test, and the pinned cross
  check "OK: 322 ledger jobs") ends with the commit line `[... 950a741]` and `EXIT=0`.
- `158-focused7.log`: EXIT=0, release profile, 2 passed and 239 filtered. The two tests are
  `a_live_artifact_listing_during_the_source_summary_migration_resumes_its_build` and
  `a_source_listing_during_the_source_summary_migration_is_a_build_its_cursor_resumes`. The log was
  written at 02:27:44, before the 02:28:37 commit. It therefore covers the working tree that became
  950a741. This is an inference from the timestamps, because the log does not echo its command.
- `158-push6.log`: EXIT=0, `ce095e2..950a741`.
- The other cited logs end as the PR and `validation.md` say: `158-commit7.log` (ce095e2),
  `158-commit6.log` (0f2857c), `158-commit4.log` (b3162ae) and `158-commit2.log` (8ba701d) have
  EXIT=0. `158-commit5.log` has EXIT=1. `158-clippy7.log` has EXIT=0. `158-rpc4.log` has EXIT=0
  with 91 passed. `158-sync1.log` has EXIT=1 and `158-sync2.log` has EXIT=0. Both
  `158-measure-300.log` and `158-measure-2670.log` have EXIT=0. The measurement cells were checked
  in review-7 and have not changed.
- The merge f5788dd: the PR (`pr-body-final.md:91`) and `validation.md:40-41` both say that its
  own hook output is not logged. Neither claims it.
- `prose-check` exits 0 on `pr-body-final.md` and on `validation.md`.
- Attribution and branding: no agent credit, trailer or model name appears in the commit messages
  of f1219ee..HEAD, the added diff lines or `pr-body-final.md`. Review-7 R3 is closed.
  `validation.md:16` says "opus", but `.stage/` is not tracked (`git ls-files .stage` is empty), so
  that text is not in source, commits or the PR.

### Review-7 R1: the tests now fail on the pre-fix ordering

950a741 moves the hook to after the page is built and before the retirement:
`source_read.rs:473-476` comes after `publish_sources` at `:472`, and `artifact_read.rs:693-698`
comes after `artifact_pending_page` at `:693`.

- Pre-fix sources (8ba701d `source_read.rs:454-461`): `remove(id)` runs first, then `result?`,
  then `publish_sources`. With the hook placed after the page, the build is already gone when the
  hook returns `CANCELLED`. `tests/storage.rs:19973-19977` asserts `(1, 0, 1)` and would read
  `(0, 0, 1)`, so the test fails. The retry on the cursor would also get `RESULT_EXPIRED`, and the
  `unwrap()` at `:19987` would panic.
- Pre-fix artifacts (8ba701d `artifact_read.rs:674-682`): the context is cloned, then
  `builds.remove(id)`, then the page. The same placement gives `(0, 0, 1)` against the `(1, 0, 1)`
  at `tests/storage.rs:20112-20116`, so the test fails.
- On the fix, the hook fires on the `Done` path only. The `let ... else` returns the rows-free page
  before the hook while the build runs, so the artifact test's polling loop does not consume the
  flag early. The build is still registered when the hook fires. Both tests pass, as
  `158-focused7.log` and `158-commit8.log` show.
- Both tests reach the code through the cursor path (`source_build_poll`, `artifact_build_poll`).
  These paths do not remove a build on error. The start paths, which do (`source_read.rs:375-380`,
  `artifact_read.rs:597-602`), are not involved.

The tests now protect the retirement order that was actually wrong. R1 is closed.

### Lock order and bounded state from the hook position

- Lock order: there is no nesting. `publish_sources` releases `source_reads` when it returns.
  `artifact_build_page` drops `builds` (`artifact_read.rs:690`) before `artifact_pending_page` takes
  `artifact_reads`. Each hook acquires its registry inside an `if` condition, which is a temporary
  scope, so the guard is released before the return and before the separate `remove` statement.
  The pages returned hold no guards.
- State after an injected failure: one published capture (sources) or one live page (artifacts) is
  left that no caller holds a cursor for. The build also stays, and the retry publishes a second
  page. The orphan is bounded. It lives under its own 60 s lease, it is dropped by `reads.purge`
  on the next publish or by the expiry pass, and it counts against `MAX_HANDLES` and
  `CAPTURE_BYTES`, so repeated failures cannot grow it without limit. The build itself is bounded
  by its lease (`Builds::purge`). In production, the only fallible step between the page and the
  `remove` is the poisoned-lock error of `source_builds()` or `artifact_builds()`, so the state is
  test-only in practice. After the core returns, the RPC-layer case releases the page's capture
  through `QueryReply::drop` (`rpc.rs:143-149`), so that path leaves no orphan either. This is an
  acceptable bounded state.

### Review-7 R2: the PR's delivery guarantee

`pr-body-final.md:15` now says that the build is retired once its page has been built. It names the
case where the RPC reply is cancelled after that and the retry gets `RESULT_EXPIRED`, and `:70`
lists that window as a known limit. Both match `rpc.rs:143-149,406-412` and
`docs/interfaces.md:883-885`. That part of R2 is closed. The `validation.md` part is not; see
finding 2.

## Findings

### 1. REQUIRED: four comments still say the hook fails before the page is built

950a741 moved the hook to after the page is built, but did not update the text that describes it:

- `crates/sparrow-core/src/storage/listing_build.rs:362`: "make the next call that has a finished
  build fail before it builds the page."
- `crates/sparrow-core/src/storage.rs:2936-2937`, the public test-support API doc: "fail before it
  builds the page, as a cancelled or refused delivery does."
- `crates/sparrow-core/tests/storage.rs:19969`: "The next call finds the build finished and fails
  before it builds the page."
- `crates/sparrow-core/tests/storage.rs:20087-20089`: "fails before it builds the page".

Each of these now states the opposite of the behavior. They describe the placement that made
review-7 R1 a finding, so a maintainer who follows them could move the hook back above the
retirement point without any test failing. Fix: say that the call fails after its page is built
and before the build is retired.

### 2. REQUIRED: validation.md omits the head's hook run, and the PR misstates how the optional findings were handled

- `validation.md:7` says "Head: see the PR (the last hook-verified head is named in the evidence
  table)". The table (`:23-38`) names ce095e2 (`158-commit7.log`) as the last hook-verified head
  and has no row for `158-commit8.log` (950a741, EXIT=0). The file was last written at 02:28:16,
  before that hook finished at 02:39. The PR body (`pr-body-final.md:88`) has the row. Fix: add the
  950a741 / `158-commit8.log` row and name the head.
- `pr-body-final.md:94` says: "The findings of each are addressed in the head or listed above."
  Several optional findings are neither fixed nor listed:
  - The memory bound, review-6 O4 and review-7 O1. Line `:74` still says that a build's memory "is
    bounded by the window and capture limits". A `sources` build holds its `Completion` deltas for
    the whole owed pass, and `SCAN_BYTES` is checked against them only afterwards
    (`source_read.rs:547-574`). The docs have the same wording (`docs/interfaces.md:859-860`).
  - Review-6 O5: there are still no 300k "after" figures, and "roughly the slow tail ... can time
    out" (`:36`) is still vague.
  - Review-5 item 5: the missing abandoned-artifact-build test. `validation.md:19` lists it as open,
    but the PR does not.
  - Review-7 O4: one hook arms both registries, and a poisoned-lock error replaces the original
    error on the start path.

  These are optional, so leaving them unfixed is acceptable. The PR sentence about them is not
  true, though. Fix: state that the required findings are fixed and name the optional findings
  that remain open. Alternatively, correct the `:74` wording and list the rest.

### O1. OPTIONAL: a theoretical race in the artifact test's census assertion

`tests/storage.rs:20112-20116` asserts `running == 0` right after the failed call. The build
thread sets `Done` and notifies (`listing_build.rs:284-285`) before it drops `_running`. The source
test waits for `running == 0` first (`:19961-19967`), and the artifact test does not. A preempted
build thread could make the assertion read `(1, 1, 1)`. The window is a few instructions long, and
the ce095e2 version had the same window. Waiting for `running == 0` before the assertion, as the
source test does, would close it.

### O2. OPTIONAL: the stale verdict line in validation.md

`validation.md:3` says "review-5 predates it". Reviews 6 and 7 also exist, and both were BLOCKED.
