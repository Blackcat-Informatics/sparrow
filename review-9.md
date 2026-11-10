Reviewer model: claude-opus-5-5 (opus)
VERDICT: BLOCKED

# #158 delivery review 9: comment, test-wait and evidence fixes at a9f1fe9

## Scope

Head a9f1fe917311aa38f0aab9aa940e448a4eaa83ff on `paudley/158-serve-every-query-and-status-call`
(PR #169, WIP). The feature diff is `git diff f1219ee...HEAD`. This recheck covers commit a9f1fe9
and the open findings of review-8. It checks the PR body
(`/home/paudley/Active/sparrow/.stage/158/pr-body-final.md`) and `validation.md` against the logs in
`/home/paudley/Active/sparrow/.stage/158/`. I take these rulings as given, as the caller directs and
as `plan.md` `## User rulings` records them: the 5 s build wait (slowest page with rows 6.47 s), the
824.5 s bound for source-summary alone, the deferral of the context and records tail to #183, and
the fact that the merge f5788dd's own hook output is not logged.

This was a static review. I ran no cargo, make or build command, wrote nothing to /tmp, and edited
no source. I used `git show`, `git log`, `git status`, `grep`, file reads, the logs and
`prose-check`.

## Evidence

- `158-commit9.log`: the hook ran `cargo clippy --locked --release --workspace --all-targets -- -D
  warnings` and `cargo test --locked --release --workspace`. Both changed tests pass (`:147`
  `a_live_artifact_listing_during_the_source_summary_migration_resumes_its_build ... ok`, `:321`
  `a_source_listing_during_the_source_summary_migration_is_a_build_its_cursor_resumes ... ok`). The
  storage target reports 241 passed and 0 failed (`:336`). The cross check passes ("OK: 322 ledger
  jobs", `:1174`), and the log ends with `[... a9f1fe9]` and `EXIT=0`.
- `158-push7.log`: EXIT=0, `950a741..a9f1fe9`. The worktree is clean (`git status --short` is
  empty).
- The other logs the PR cites end as the PR says. `158-commit8.log` (950a741), `158-commit7.log`,
  `158-commit6.log` and `158-commit4.log` each end with EXIT=0. `158-focused7.log` has EXIT=0 with
  2 passed. `158-clippy7.log` has EXIT=0. `158-rpc4.log` has EXIT=0 with 91 passed.
  `158-sync2.log` has EXIT=0. `158-measure-300.log` and `158-measure-2670.log` both have EXIT=0,
  and the second finished in 15321.65 s.
- `prose-check` exits 0 on `pr-body-final.md` and on `validation.md`. On `docs/interfaces.md` it
  reports only `UInt32`/`UInt64` joins, and none of those are on lines in the feature diff.
- Attribution and branding: no agent credit, trailer, tool brand or model name appears in the
  commit messages of f1219ee..HEAD, the added diff lines or `pr-body-final.md`.

### Review-8 R1: the hook comments. Closed.

`grep` finds no "before it builds the page" or "before the page is built" in `crates/` or
`docs/`. All four sites now describe the placement that is in the code:

- `crates/sparrow-core/src/storage/listing_build.rs:362-363`
- `crates/sparrow-core/src/storage.rs:2936-2937` (the public test-support API)
- `crates/sparrow-core/tests/storage.rs:19969-19970`
- `crates/sparrow-core/tests/storage.rs:20088-20090`

### The added test wait (tests/storage.rs:20113-20120). Sound.

- The build thread drops `_running` when its closure ends. That happens after it stores
  `State::Done` and notifies (`listing_build.rs:274-286`). The hook fires only on a `Done` build, so
  the thread has already passed its work when the call returns `CANCELLED`. `running` therefore
  reaches 0 within a few instructions. The loop closes the window that review-8 O1 described.
- The loop cannot hide the regression it protects. It waits only on `running` (field 1). The full
  tuple `(1, 0, 1)` is asserted afterward at `:20121-20125`, so the pre-fix ordering still reads
  `(0, 0, 1)` and fails. No second build can start between the failed call and the census read,
  because the test makes no listing call in between.
- If the thread never ends, the 10 s deadline fails the test with "the build thread kept running";
  it does not hang. The pattern is the same as the source test's (`:19961-19967`).

### Review-8 R2: the memory wording, validation.md and the PR sentence

- The memory wording is closed. `docs/interfaces.md:859-862` and `pr-body-final.md:74` now say
  that a `sources` build holds its count changes for the whole owed pass and that the capture
  admission is checked when the pass ends. That matches `source_read.rs:548-574`:
  `Completion::owed` runs to the end, and only then is `SCAN_BYTES.checked_sub(owed_bytes)`
  evaluated. Review-6 O4 and review-7 O1 are closed with it.
- The PR sentence is closed. `pr-body-final.md:95` now says that the REQUIRED findings are fixed
  and lists the optional findings that remain open: the abandoned-artifact-build test, the 300k
  "after" figures, the uncounted slow tail, the hook arming both registries, and the poisoned-lock
  error on a failed start. Review-8 O1 (the census race) is fixed by the wait above. Review-8 O2
  (the stale verdict line) is fixed at `validation.md:3`.
- `validation.md` now has the 950a741 row (`:35`, `158-commit8.log`, EXIT=0), which matches the
  log. It still omits the current head; see finding 1.

## Findings

### 1. REQUIRED: validation.md again omits the head's hook run

`validation.md:7` names the head as "950a741 plus the follow-up named in the PR", and the evidence
table (`:23-39`) has no row for `158-commit9.log` (a9f1fe9, EXIT=0). The file was written at
02:44:31, before the hook finished at 02:53:21. The head's only test-run evidence for the new
test wait is in that log, and the PR body has the row (`pr-body-final.md:89`). This is the same
defect as review-8 finding 2, now at the next head. Fix: name head a9f1fe9 at `:7` and add the row
"Commit hook (`make check`), a9f1fe9 | 158-commit9.log | EXIT=0". `.stage/` is untracked
(`git ls-files .stage` is empty), so the fix does not need a commit.

### O1. OPTIONAL: the PR's review list stops at review-7

`pr-body-final.md:95` lists review-5, review-6 and review-7, but not review-8 (BLOCKED at 950a741).
Its findings are fixed in the head, so the sentence is not false, but the list is incomplete.

### O2. OPTIONAL: an overlong comment line

`crates/sparrow-core/tests/storage.rs:20089` is 117 columns wide, while the lines around it wrap
near 90. rustfmt does not wrap comments, which is why the hook passed. Rewrapping the comment
would match its neighbours.

### O3. OPTIONAL: a circular phrase in the memory text

"An artifact window is bounded by its window" (`docs/interfaces.md:860`, `pr-body-final.md:74`)
says nothing. Stating the bound, at most one owed window of 32,768 rows at 64 bytes plus the key
per row, would make the sentence informative.
