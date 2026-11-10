Reviewer model: claude-opus-5-5 (opus)
VERDICT: BLOCKED

# #158 delivery review 7: the delivery-failure hook at ce095e2

## Scope

Head ce095e26f0312d8133f409360ae0940c0af4678f on `paudley/158-serve-every-query-and-status-call`
(PR #169, WIP). The feature diff is `git diff f1219ee...HEAD`. This review covers the changed work
since review-6 (commit ce095e2, read as a patch), the open review-6 findings, and the PR text
(`/home/paudley/Active/sparrow/.stage/158/pr-body-final.md`) and `validation.md` against the logs.
Rulings were read from `issue.md` and `plan.md` `## User rulings`. I took the coordinator decisions
(a), (b) and (c) given for this review as rulings. Ruling (c) is recorded verbatim at
`plan.md:62`. Rulings (a) and (b) are not recorded there; see O3.

This was a static review. I ran no cargo, make or build command, wrote nothing to /tmp, and edited
no source. I used `git show`, `git diff`, `git grep`, file reads, the logs and `prose-check`.

## Evidence

- Commit hook `make check` at ce095e2: `158-commit7.log` EXIT=0, with the commit line
  `[paudley/158-serve-every-query-and-status-call ce095e2]`. The log shows 241 storage tests
  passed and the census cross-check OK.
- Focused run: `158-focused6.log` EXIT=0, 2 passed, 239 filtered. The two listing-build tests
  ran, release profile. The log was written at 02:03:51, before the 02:04:30 commit, so it covers
  the working tree that became ce095e2.
- Push: `158-push5.log` EXIT=0, `0f2857c..ce095e2`.
- Every cell of the latency table (`pr-body-final.md:23-32`), the TO_ROWS table (`:45-48`), the
  migration totals (6029 pages, 824.5 s; 682 pages, 39.4 s) and the 5000 to 5099 ms and 6.47 s
  figures match `158-measure-2670.log:7-29` and `158-measure-300.log:18-30`.
- `prose-check` exited 0 on `pr-body-final.md` and on `validation.md`.
- No agent credits appear in the commit messages f1219ee..HEAD or in the added diff lines. The PR
  body names a model; see finding 3.
- The `test-support` feature is enabled only through `sparrow-test-support`, which is a
  dev-dependency of `sparrow-core` and `sparrow-server`. The hook is not in shipped binaries.

Coordinator rulings, checked against the PR text:

- (a) `pr-body-final.md:35` states plainly that the 5 s is the build wait added to page assembly,
  that rows-free pages take 5000 to 5100 ms, and that the slowest page with rows took 6.47 s. It
  also states that the test asserts under 6 s. This matches `158-measure-2670.log:10,11,17`.
- (b) `pr-body-final.md:39` says the source-summary time is a bound, 824.5 s, not a measurement.
  It says the per-migration timing goes to #176. This matches `158-measure-2670.log:22`.
  `validation.md:62-63` says only that source-summary is not timed, which is consistent.
- (c) `pr-body-final.md:36-37,71` defers the context and records treatment to #183 and states
  that the median meets 30 s while p95 and max do not (26.5 s / 27.7 s against 41.9 s / 48.3 s,
  and 83.4 s / 48.7 s). This matches `158-measure-2670.log:9,12`. I did not check that #183
  exists (static review); the ruling says the coordinator files it.

ce095e2, other checks:

- Leaks: none found. The start paths now retire their build on any error
  (`source_read.rs:375-380`, `artifact_read.rs:597-602`), which closes review-6 O2. `remove` is a
  map removal; `Build::drop` (`listing_build.rs:146-150`) only cancels and never joins, so no
  call blocks under the registry lock. Removing a build that already failed is a no-op.
- Lock order: no new nesting. The source hook takes and drops `source_builds` inside one `if`
  condition (`source_read.rs:459`) before `publish_sources` takes `source_reads`. The artifact
  hook runs under the `builds` guard that the function already holds (`artifact_read.rs:678`).
  The start-path removals run after the page call has returned and released every guard.
  `fail_next_listing_delivery_for_tests` (`storage.rs:2939-2952`) takes the two registries one
  after the other, never both at once.
- Duplicate helpers: the flag has one generic implementation on `Builds<C, T>`
  (`listing_build.rs:362-372`), shared by both listing kinds. Census passed in `158-commit7.log`.

## Findings

### 1. REQUIRED: the delivery-failure hook fires before the point where the pre-fix code retired the build, so the tests pass on the pre-fix ordering

The hook returns `CANCELLED` as the first step after the wait's `let ... else`:

- `crates/sparrow-core/src/storage/source_read.rs:458-461`
- `crates/sparrow-core/src/storage/artifact_read.rs:677-680`

The pre-fix code (8ba701d) retired the build after that point:

- sources, `source_read.rs:454` at 8ba701d: `self.source_builds()?.remove(id);`, then
  `result?` and `publish_sources`.
- artifacts, `artifact_read.rs:674-682` at 8ba701d: clone the context, `builds.remove(id)`,
  then `artifact_pending_page`.

Put the hook into that pre-fix body at the same place, and it returns before the removal. The
census then still reads `(1, 0, 1)`. The retry polls, finds the finished build, retires it,
publishes the page and ends at `(0, 0, 1)`. Every assertion at `tests/storage.rs:19970-19995`
and `:20090-20128` passes. The tests fail only if someone moves the `remove` above a test-only
line. They do not fail on the ordering that was actually broken.

The real delivery failures that the pre-fix code mishandled all happen after its removal point:

- the cancellation and handle checks inside `reads.publish` (`source_read.rs:489-496`);
- the checkpoint at `artifact_read.rs:711`;
- `artifact_page`.

The hook does not model any of them. So review-6 finding 1 is not closed. No test protects
"a retry after a failed delivery resumes", and the PR body (`pr-body-final.md:84`) and
`validation.md:16,34` say that one does.

Fix: fail at the last fallible step before the page exists, after every point where a build
could be retired. For example:

- have the armed flag make the delivery checkpoint fail, at the top of `publish_sources` after
  the guard and at `artifact_read.rs:711`; or
- arm it just before the post-publish `remove` (`source_read.rs:477`, `artifact_read.rs:698`).
  This version also covers any retirement inside page construction.

Either way, a `remove` placed anywhere before the page exists then makes the census assertion
and the retry fail.

### 2. REQUIRED: the PR overstates the delivery guarantee, and validation.md describes an older head

- `pr-body-final.md:15` says: "A finished build is retired only after its page is delivered, so a
  retry after a failed or cancelled delivery resumes it." Core retires the build as soon as the
  page is built (`source_read.rs:476-477`, `artifact_read.rs:697-698`). `read_work` can then
  still cancel the call (`crates/sparrow-server/src/rpc.rs:410`). The undelivered `QueryReply`
  then releases the page's handle (`rpc.rs:143-149`), and a retry on the build cursor gets
  `RESULT_EXPIRED`. This is review-6 O1, which was not fixed. The PR body does not state it, and
  the known limits at `pr-body-final.md:68-75` leave it out.
  `docs/interfaces.md:883-885` already limits the guarantee correctly, to a call that fails
  while its page waits for the writer. Fix: qualify `:15` the same way and list the RPC-layer
  window as a known limit, or fix O1.
- `validation.md:7` names the head as 0f2857c. The evidence table (`:25-38`) has no row for the
  ce095e2 hook run (`158-commit7.log`). Fix: update the head and add the row.

### 3. REQUIRED: the PR body carries a model name

`pr-body-final.md:92` quotes "`Reviewer model: claude-opus-5-5 (opus)`" and "model opus".
`AGENTS.md` says: "Do not add tool branding or agent credits to source, commits, issues, or
PRs." The coordinator's request to "Name model use" (`plan.md:60`) is about its report, not the
PR. Fix: name the reviews by file and verdict without the model. If the coordinator wants the
model named in the PR, record that ruling first.

### O1. OPTIONAL: the PR repeats the imprecise memory bound (review-6 O4)

`pr-body-final.md:73` says a build's memory "is bounded by the window and capture limits". That
holds for an artifact window. A `sources` build holds its `Completion` deltas for the whole owed
pass. `SCAN_BYTES` is checked against them only after the pass (`source_read.rs:547-574`), so
during the build they have no bound. The docs (`docs/interfaces.md:860`) have the same wording.

### O2. OPTIONAL: other review-6 optionals, as the text states them

- O3 (the abandoned-build test cannot tell cancellation from completion): the PR makes no claim
  that it can. No finding.
- O5: the 300k "after" figures are still missing from the PR table. "roughly the slow tail of
  calls ... can time out" (`pr-body-final.md:36`) is still vague. The records max is now given.
- `validation.md:20` calls O5 "artifact retry coverage" and says the hook test closes it. Review-5
  item 5 was the missing abandoned-artifact-build test, and the same line says that coverage is
  still open. Use the original label.

### O3. OPTIONAL: rulings (a) and (b) are not recorded verbatim

`plan.md` `## User rulings` has the deferral (c) at `:62`, but not the 5 s wait decision (a) or
the 824.5 s bound (b). `pr-body-final.md:35` calls (a) "the author's decision, accepted by the
coordinator". Record both verbatim, with their times.

### O4. OPTIONAL: test-hook and error-path details in ce095e2

- `fail_next_listing_delivery_for_tests` arms both registries. Each test consumes only one flag,
  and the other stays armed for the rest of that test. Arming one kind would be more exact.
- If the registry lock is poisoned, the start-path removal (`source_read.rs:378`,
  `artifact_read.rs:600`) returns the poisoned-lock error in place of the page's original error.
  Keeping the original error would preserve its context.
