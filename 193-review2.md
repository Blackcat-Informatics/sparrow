Reviewer model: claude-opus-5-5 (opus)

# Delivery re-review: PR #194 (issue #193), head 0d0dd74

## Scope

- Static review of `git diff main...0d0dd74` (main 4acd306; 19 files, +400/-627) and of the fix
  commit on its own, `git diff 54d2a64 0d0dd74` (9 files, +173/-18), in
  `/home/paudley/Active/sparrow/.worktrees/193-drop-the-event-fallback-rdf-backfill`.
  HEAD is 0d0dd74, the worktree is clean, and `origin/paudley/193-...` points at 0d0dd74.
- Inputs: the issue text `.stage/fallback-issue.md`, the coordinator rulings in
  `.stage/STATUS.md:115`, the PR body `.stage/193-pr-body.md`, the logs `.stage/193-commit6.log`,
  `193-commit7.log` and `193-test-core5.log`, and the first review `.stage/193-review.md`.
- I ran no cargo or build command and did not touch `/SPARROW/store` or the daemon. I used only
  git reads, greps inside the worktree, reads of `.stage` files, `ls --time-style=full-iso`,
  `git config --get-regexp '^hook\.'`, and `prose-check`.

## Evidence

### Finding 1: stored-facts test (first review R1): fixed

- New fixture `store_event_fallback_rdf_for_legacy_fixture`
  (`crates/sparrow-core/src/storage/event_fallback_rdf.rs:46-116`).
  - It is behind `#[cfg(feature = "test-support")]`, and so are its imports and both
    re-exports (`storage.rs:20-21`, `lib.rs:110-111`).
  - `test-support` is enabled only by `sparrow-test-support`. That crate is a
    `[dev-dependencies]` entry in sparrow-core, sparrow-server and sparrow-client, the same
    pattern as the existing `remove_*_for_legacy_fixture` helpers.
- It is not a second production implementation.
  - It builds the facts through the one law, `rdf_index::fallback_metadata`, which
    `rdf_view.rs:377` also uses and ledger job line 92 still owns.
  - It loads graphs through `rdf_index::stored_metadata`, writes through
    `RdfWrite::replace_metadata`, and commits with `catalog_upgrade::commit`. This is the same
    write path main's retired `publish` used.
  - Its only local logic is the `receivedAt` lookup, which is fixture-only.
  - The census `--cross --check` in `193-commit7.log` passed: 321 jobs, no unsanctioned group.
- The fixture leaves the live shape:
  - `event-fallback-rdf-upgrade` = `{"after":"<key>","complete":false}`, main's
    `catalog_upgrade::Progress` encoding;
  - `store-counter:done:event-fallback-rdf` as a big-endian u64, main's counter encoding.
- Test `graphs_that_store_their_fallback_facts_are_served_once_and_not_derived`
  (`tests/storage.rs:19739-19796`):
  - It stores the facts on 35 of 70 graphs.
  - On that mixed scope, the rows of both `eventTimeBasis` and `eventTimeFallback` must equal
    the all-derived rows, so a doubled fact fails the test (105 rows instead of 70).
  - Coverage must be exactly 35.
  - `migrations()` must not list `event-fallback-rdf`.
  - After `migrate`, coverage is still 35 and `assert_counts_match_recount` holds.
  - Coverage 35 after open and after `migrate` shows that no rewrite added or removed facts.
- `open_serves_before_the_schema_69_...` again asserts exact coverage equality. At
  `tests/storage.rs:19629-19637` the offset is `+1` for the wide scopes only, where the late
  ingest landed. This replaces the `>=` the first review flagged.
- Execution: `193-commit7.log:204, 242, 256, 477` show the new test,
  `open_serves_before...`, the marker test and `reads_report_pending_mid_migration...` passing.
  `193-test-core5.log` ends `EXIT=0` (3 passed, 245 filtered out).

### Finding 2: PR body (first review R2): fixed

- The body opens with "Refs #193. The issue stays open for the read-time measurement and the
  live 71-to-71 check". It has a "One-way switch" section, and "Not done" lists both open
  acceptance items.
- This fits the coordinator ruling at `STATUS.md:115`: "drop of event-fallback-rdf is mine to
  decide (default drop; record read-time cost on the issue)".
- Issue acceptance items 2 (read-time number) and 3 (live switch) remain open on the issue.
  They are tracked, not claimed as done.
- prose-check on the PR body: exit 0.

### Finding 3: doc sentences and stale text (first review R3, O2): fixed

- `docs/architecture.md:599-601` now describes permanent read-time derivation counted in
  coverage.
- `docs/interfaces.md:601-604` narrows the fence to "binaries older than schema 69" and adds
  the one-way statement. See O1 for a precision point.
- `docs/open-items.md` B-7 to B-11 are marked "Moot".
- `docs/deployment.md:655-659` and `deploy/verify-backup.sh:155-157` drop the 64-graph page
  rationale.
- The `carries_event_time` doc no longer says "needs no backfill".
- prose-check on the four docs reports only the same pre-existing `UInt64`/`UInt32` joins
  (lines shifted by 2). None is in a changed hunk.
- The commit message passes prose-check (`--stdin`, exit 0).

### Finding 4: evidence log (first review R4): fixed

- `193-commit7.log` ends with its own `EXIT=0`. It contains:
  - fmt;
  - release clippy with `-D warnings` (passing; the failing `manual_is_multiple_of` lint from
    `193-commit6.log` is fixed);
  - release workspace tests, every `test result: ok`, storage 248 passed;
  - the census self-test and `--cross --check` OK.
- Timestamps:

  | Event | Time |
  |---|---|
  | `193-commit6.log` (`EXIT=1`, clippy) finished | 12:10:37.47 |
  | `tests/storage.rs` modified (the lint fix) | 12:10:41.98 |
  | 0d0dd74 author and committer date (`%aI`/`%cI`), and its single reflog `commit:` entry | 12:10:42 |
  | `193-commit7.log` finished | 12:19:33.24 |
  | `193-pr-body.md` written | 12:19:47 |

- So the log finishes 8 min 51 s after the commit's recorded time. That matches the log's own
  durations: clippy 5.4 s, compile 5 min 18 s, then tests and census.
- Git sets the author and commit date when `git commit` starts, before the pre-commit hook
  runs. A hook log that finishes after the recorded date is therefore the expected shape.
  This is from my knowledge of Git's commit sequence; I did not test it here.
- The hook is wired: the local config has `hook.sparrow-verify.event pre-commit` with
  `command make check`. The log's first lines are `make check`'s recipe.
- The log has no Git summary line, which is consistent with a `-q` commit. Statically I cannot
  rule out a separate `make check` started in the same second, but the reflog has exactly one
  commit attempt and the source edit falls 0.03 s before it.
- Side note on the first review's R4: under the same timing, `193-commit5.log` (finished
  11:52:57, five minutes after 54d2a64's 11:47:53 date) was probably that commit's hook run, so
  the original R4 likely misread the timing. The new body's statement about 54d2a64 is true
  and conservative, and 0d0dd74 supersedes that commit, so nothing more is needed.

### Regression check of the fixes

- The fixture is compiled only with test-support. The production build of sparrow-server takes
  sparrow-core without that feature.
- The test uses `is_multiple_of`, and the clippy run passed on it.
- No production code changed in the fix commit except doc comments and the test-gated fixture.
- No new ledger row is needed: the comparable fixtures `remove_store_counters_for_legacy_fixture`
  and `remove_source_summary_for_legacy_fixture` have none, and the census check passed.
- The fixture's `after` is the last kept key, which here is the final graph. So the checkpoint
  is pending but not a mid-store frontier. The new code never reads the checkpoint, so this has
  no effect on the behavior under test.

## Findings

### O1 (OPTIONAL): the one-way sentence overstates rollback loss when the checkpoint is pending

`docs/interfaces.md:602-604` (and the PR body) say that the pre-#193 image "does not derive the
facts for graphs written without them". On main, with the checkpoint pending at frontier `after`:

- `fallback_pending` (`main:rdf_view.rs:511-543`) derives for graphs with id > `after` that lack
  the facts;
- the resumed backfill would then store those facts;
- only graphs at or before `after` are served without them (all of them once the checkpoint is
  complete).

Record ids are random (`storage.rs:1998`, `random_id("record")`), so a rollback of the live
store, which is pending, would lose only the share of new graphs below its frontier. It would
also restart the rewrite this PR removed.

The conclusion is still sound: the switch is one-way and rollback is lossy. A more exact
wording: "serves graphs written without the facts at or before the retired backfill's frontier
without them, and resumes rewriting the rest".

### O2 (OPTIONAL): remaining stale text

- `tests/storage.rs:21704-21706`: the #158 invariant doc comment still lists "the schema 69 owner
  index and event fallback backfill run" among the running migrations. This text predates the PR
  and is outside the diff.
- `docs/architecture.md:598-601` now lists permanent derivation as an example under "Readers
  take an explicit pending path until a migration completes". The word "permanently" makes it
  accurate, but it could sit in its own sentence. The line break before "scoped SPARQL reads
  the" is also uneven.

Findings: no REQUIRED findings. All four required findings from the first review are fixed at
0d0dd74, and the fixes introduce no regression.

VERDICT: PASS
