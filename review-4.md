# #158 delivery re-review (review 4)

## Scope

This is a narrow recheck of review 3's two open findings in the uncommitted worktree
`/home/paudley/Active/sparrow/.worktrees/158-serve-every-query-and-status-call` (`git diff main`):

- R1: the test `a_live_listing_across_migration_pages_and_expiry_lists_the_finished_index`
  (`crates/sparrow-core/tests/storage.rs:19550-19697`) and the skip branch it should exercise
  (`crates/sparrow-core/src/storage/artifact_read.rs:898-902`).
- R2: the evidence in `validation.md`.

I ran no cargo, make, build or test command, and I edited no source. I used file reads,
`git diff`, `grep`, `ls`, log reads and `prose-check`. Review 3's optional findings O1 and O2
are unchanged and not rechecked here.

## Evidence

Logs are under `/home/paudley/Active/sparrow/.stage/r82/`:

- `158-skipprobe4.log` (16:45:22): EXIT=101. The single selected test failed at
  `storage.rs:19636:14`, the `.unwrap()` in the `read` closure, with `STORAGE_FAILURE` "the
  per-source index lists artifact:52356fa8... without its entry or a retained record". That is
  the error the skip branch prevents, raised on the current file's line numbers.
- `158-focused17.log` (16:53:56): EXIT=0. 1 passed, 235 filtered out.
- Mtimes: `tests/storage.rs` was last written at 16:38:05, before both builds started.
  `artifact_read.rs` was last written at 16:45:22, which is the restore after probe 4 and
  before the `focused17` build. The skip branch is present at `:898-902`. The added lines of
  `git diff main -- artifact_read.rs` contain no probe residue (`if false`, `false &&`,
  `dbg!`, `eprintln`). I cannot check "byte for byte" without the pre-probe copy, but the
  restored code is the reviewed code.
- `158-focused16.log` (EXIT=101, 8 against 12 at `:19611`) was built before the 16:38 edit.
  It does not test the current file.

I traced the current test through the code:

- Fixture: each source holds 6 artifacts. Indices 0 and 1 are received at `DAY` and `DAY + 1`,
  and indices 2 to 5 at `20*DAY + index`. `before` is past both early deadlines, but the grace
  period has not ended (`expires_at + DAY > before`). `after = before + 20_000` satisfies
  `expires_at + DAY <= after` for both early artifacts (`expiry.rs:222`, `:305`) and is far
  before the late deadlines. At `before`, the early rows still have entries, so
  `fold_index_row` produces a summary and `finish_row` filters it. That matches `migrated.len()
  == 8`.
- Migration order: `pending_source_summary_migration` removes the store counters and the source
  summary. `SEEDED` has 6 prefixes (`store_counters.rs:55`), and each prefix is far below
  `SEED_PAGE`, so steps 1 to 6 seed the counters. `pending_migration`
  (`migration.rs:279-303`) then reaches `SourceSummary` at step 7. Its first page copies every
  remaining artifact in one batch (`PAGE_ARTIFACTS = 512`).
- Windows of 5, with A for the source that lists first and B for the other:
  - The first page reads {A0..A4} with `through` A4. It folds and filters A0 and A1, then lists
    A2.
  - Steps 1 and 2: A3 and A4 are listed. After A4 the next row is `Listed::Window`.
  - Step 3: the continuation recomputes {A5, B0, B1, B2, B3}, with `through` B3, before the
    expiry. It lists A5. The first loop ends after 3 steps, all store-counter steps. The new
    assertion `!complete && done == 0` holds, and it fails loudly if the order changes.
- `expire(after)` deletes the entries and records of A0, A1, B0 and B1. Their index rows were
  never stored.
- The rest loop:
  - Step 4: B0 and B1 are owed in the held window, and `summary` is `None`, so they reach the
    skip branch at `:898`. B2 is listed.
  - Step 5: B3 is listed, then `Window`.
  - Step 6: the next window is {B4, B5}, not truncated. B4 is listed.
  - Step 7 is the first source-summary page and copies B5. The next read finds stored B5 equal
    to owed B5. The `key < stored` filter (`:981-985`) returns the stored row, so B5 is listed
    once and the listing ends.

Answers to the caller's questions:

- **Skip:** protected. Without the branch, B0 and B1 raise `STORAGE_FAILURE`. My trace agrees
  with probe 4. The skip is robust to migration timing, because the window holding B0 and B1
  is read at step 3, before the expiry. The new `done == 0` assertion pins that no
  source-summary page ran before it.
- **Copied-row dedupe:** exercised once, at B5 (step 7 copies it after the {B4, B5} window was
  read). `assert_eq!(rest, second)` is an exact sequence comparison, so listing B5 twice or
  dropping it fails. A tie-break flip (owed wins on equal keys) would still pass, but the
  output would be identical, so that flip changes no behavior. See O1 for the one
  unguarded assumption.
- **Finished-index comparison:** correct.
  - The first-source part is compared with `migrated[..firsts]`, the fully indexed listing at
    `before`.
  - The remainder is compared with `finished`, the listing after `migrate_step` runs to
    completion and the expiry, filtered to the second source. The comparison includes
    `retained_records` and `complete_bytes_available`.
  - The surviving IDs are also checked against `kept`.
- **Doc comment (`:19550-19554`):** accurate. Windows of five do cross window ends (three
  windows). "A row the migration copies after its window was read is listed once" is B5. "An
  owed artifact that expiry deletes after its window was read is skipped" is B0 and B1. The
  fixture comment at `:19560-19562` is also accurate. The comments at `:19661-19662` and
  `:19665-19666` repeat the fixture comment; this is harmless.

R2, from validation.md (written at 16:54:19):

- The new section "Evidence on the post-ruling tree" lists `check13/14`, `focused11/14`, `storage9`,
  `rpc1/3`, `fmt1` and `measure6`, each with its exit status. It also records the R1 probe pair.
  That part is accurate.
- The original "## Evidence" table (`:12-20`) and its `measure5` latency table (`:22-34`) are
  still presented as current. The table's "none on the current tree; not run" row contradicts
  the later section.
- `158-measure6.log` is EXIT=101 ("the page released the writer early"). It produced only the
  during phase, and its numbers differ widely from the `measure5` table (status p50 2189 ms
  against 0.30 ms; sources 13.8 s against 9.9 s). No after phase and no stalled-writer status
  latency exist for the current code.
- These logs predate the last test edit (16:38) and the probe restore (16:45): Clippy `check14`
  (06:03), `storage9` (14:51), `fmt1` (15:26). No `make check` or pre-commit hook run exists on the
  final tree.
- `crates/sparrow-core/tests/measure_158.rs` is still present, which is expected while the probe
  rerun is pending.
- `prose-check validation.md` exited 1 with one hit, `focused10`, at `:16`. It is in the stale
  table.

## Findings

### R1 (review 3): closed

The test now drives both the owed-retired skip and the equal-key dedupe through the production
`artifact_read` entry point. The probe pair shows that it fails without the skip branch and
passes with it. The doc comment matches what the test drives.

### R2 REQUIRED (review 3, still open): the latency report and the final-tree gate are missing

The caller says this work is pending. The contract still needs it:

- Issue line 31 says "The answer latency during the rebuild is measured and reported ... against
  the latency after it". The coordinator ruling (plan.md `:69`) requires "a status call that
  returns while writes are stalled or stopped, with its latency recorded".
- The only run on the current code, `measure6`, failed before its after phase.

To close it:

- Rerun the probe to EXIT=0. Record the during and after tables, plus status latency while the
  writer is held.
- Replace or clearly mark as superseded the original Evidence and `measure5` latency tables at
  `validation.md:12-34`, and update the top `VERDICT:` line.
- Run the mandatory `make check` (directly or through the pre-commit hook) on the final, synced
  tree. Record its log and exit status. That run also covers the fmt, Clippy and storage-target
  runs that the 16:38 and 16:45 edits invalidated.
- Delete `measure_158.rs` before commit.

### O1 OPTIONAL: the dedupe depends on the step count, and no assertion pins it

The dedupe is exercised only because step 7, the first source-summary page, falls inside the
rest loop. If store-counter seeding took more steps, for example because a seeded prefix was
added, no source-summary page would run before the listing ends. The test would still pass
without exercising the copied-row path. The new assertion guards only the other direction
(no source-summary page before the expiry). A one-line guard would make that loud, for
example asserting that `source-summary` has `done > 0` before the last continuation, or
counting `MigrationPage::migration == SourceSummary` in `step`.

### O2 OPTIONAL: validation.md overstates what the earlier probes showed

`validation.md:149-150` says skipprobe, skipprobe2 and skipprobe3 "showed that the window-2 and
window-5 fixtures never reached the branch". Only `158-skipprobe.log` (EXIT=0 with the branch
disabled) shows that. `158-skipprobe2.log` failed with `RESULT_EXPIRED` at the read closure, and
`158-skipprobe3.log` failed at the `8 == 12` fixture assertion. Both are fixture faults that
prove nothing about the branch. Reword the sentence to say so.

VERDICT: BLOCKED
