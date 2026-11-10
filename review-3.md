Reviewer model: claude-opus-5-5 (opus)
VERDICT: PASS

# #176 / PR #190 independent delivery re-review, third round

## Scope

- Head reviewed: `c6622845fe9c7549c8c5abea50376e1981a9e441` on branch
  `paudley/176-keep-the-source-summary-owed-completion`, which equals
  `origin/paudley/176-keep-the-source-summary-owed-completion`. The worktree is clean. Base
  `bce6adae2d56` (main).
- The PR has three commits:
  - `955948b`, passed by review-1;
  - `155f4b4`, passed by review-2 with two OPTIONAL comment findings;
  - `c662284`, the commit under review. It touches 3 files, +10/-9.
- The whole PR is 12 files, +827/-166.
- Contract: issue #176 verbatim (`.stage/176/issue.txt`), including the restated acceptance
  (1)-(4) in the 2026-11-10 06:02 comment. Rulings: `plan.md` `## User rulings`, re-read for
  this review.
- Method: a static read of `git diff 155f4b43fefd..c6622845fe9c` in full, and of the code
  around each changed comment:
  - `Base::overtaken`;
  - the artifact window re-base test;
  - `Job::Migrate` and `MigrationJournal::page`.

  I also read every added comment and doc in `git diff bce6adae2d56..c6622845fe9c` that
  mentions the re-base lead, check intervals, overtaking or the journal timing: all of the
  docs, `listing_build.rs`, `migration.rs` and the added test comments.

  For everything c662284 does not touch, I reused review-1 and review-2.
- Not run: cargo, make, tests and prose-check. The caller limited this review to static,
  read-only work.

## Evidence

- **c662284 changes only comments.** I filtered every `+` and `-` line of
  `git diff -U0 155f4b43fefd..c6622845fe9c` to drop `//` and `///` lines. Nothing was left
  (grep exit 1). No code, test assertion, doc file or ledger changed. The commit message ends
  `Refs #176` and has no attribution trailer.
- **Commit hook at this head.** `.stage/176/hook3.log` runs from 13:18:55Z to 13:31:58Z. It
  covers:
  - `cargo fmt --all --check`;
  - release workspace clippy with `--all-targets -D warnings`;
  - release workspace tests (`storage` 246 passed, 0 failed; no failing result in the log);
  - the census self-test and the pinned cross check ("OK: 322 ledger jobs").

  The log contains the commit line `[... c662284] Name the re-base lead and the journal wait
  exactly in comments` and ends `EXIT=0`. `validation.md` lines 40-41 record this log and the
  push.
- **Intra-doc link.** The new link `[`super::listing_build::CHECK_EVERY`]` resolves:
  `listing_build` is a sibling module (`storage.rs:45`) and `CHECK_EVERY` is `pub(super)`
  (`listing_build.rs:46`). `overtaken` is `pub(super)`, so no public doc links a private item.

### Review-2 findings, rechecked

1. **Re-base lead wording: fixed.**
   - The doc at `source_summary.rs:806-812` now says the lead is
     "[`CHECK_EVERY`] entries, all in the pass's own unit (artifact and record entries at or
     before the checkpoint, which is what `done` counts)". That matches the code at `:827`:
     `done - base > read + CHECK_EVERY`, where `read` is progress entries.
   - The test doc at `tests/storage.rs:20461-20463` now says "plus a lead of 4,096, in the
     migration's own unit". Both are accurate.
2. **Journal wait wording: fixed.**
   - The new comment at `maintenance.rs:114-115` says the page's time includes the writer wait
     the job runs inline (`defer: true` calls `defer_migration_page` after `began`). It also
     says a wait that ran in the background is not part of it.
   - That matches the code: `began` is taken before the inline deferral, and
     `MigrationJournal::page` keeps only the first page's `began` (`get_or_insert`).
   - It also agrees with interfaces.md:666-674. Elapsed time is wall clock from the first
     page's job, so it includes every later wait. Only the background wait before the first
     page is not counted.

### Whole-PR text on the lead and the journal timing

I found no remaining incorrect statement. The following text is consistent with the code:

- **Check clock.** `listing_build.rs` describes the clock in catalog entries (module doc,
  `CHECK_EVERY`, `Worker::work` and `Worker::entry`).
- **Re-base timing.** architecture.md:645-648 says "at its next 4,096-entry check". The check
  clock is correct there.
- **Lead in interfaces.md.** interfaces.md:897-914 says the build checks "each time it has
  read another 4,096 catalog entries (an artifact's records count, though `read` counts the
  artifact once)", and that the lead is "than the build has read plus 4,096". It keeps the two
  units apart correctly.
- **Journal timing.** The `MigrationJournal` doc ("each timed from the start of that first
  page"), deployment.md:970-974 and interfaces.md:659-674 agree with `MigrationJournal::page`.

One test comment still says "check intervals" for a quantity in the migration's unit. It does
not describe the lead, so it is optional finding 1.

### Acceptance and rulings at this head

c662284 changes no behavior, so review-2's acceptance assessment carries over unchanged:

- Acceptance (1)-(4) are met.
- The either-order resume test, the "no new sub-second loop" ruling, the "no wire or schema
  change" ruling and `Refs #176` all hold.
- All #176 tests pass in hook3.

## Findings

1. **OPTIONAL**: `crates/sparrow-core/tests/storage.rs:20540-20541`.
   - Problem: the comment "Run pages until the migration has passed more than two check
     intervals beyond the held build's snapshot" labels `2 * 4_096` of the migration's `done`
     entries as "check intervals". Since 155f4b4, a check interval is 4,096 catalog entries on
     the build's work clock. This is the same mix-up that c662284 fixed in the two doc
     comments. The loop itself is correct, and the comment does not describe the lead.
   - Fix: say "more than twice the 4,096-entry lead, in the migration's unit, beyond the held
     build's snapshot".
2. **OPTIONAL**: `crates/sparrow-core/src/storage/source_summary.rs:812` (118 columns) and
   `crates/sparrow-core/tests/storage.rs:20463` (107 columns).
   - Problem: c662284 did not reflow these two doc lines, so they are wider than the
     surrounding 100-column comment text. rustfmt does not wrap comments, so the gate passes.
     The issue is cosmetic only.
   - Fix: reflow both paragraphs to the 92-100 column width of their neighbours.

No REQUIRED findings.

VERDICT: PASS
