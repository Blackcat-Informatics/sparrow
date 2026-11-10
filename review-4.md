Reviewer model: claude-opus-5-5 (opus)
VERDICT: PASS

# #176 / PR #190 independent delivery re-review, fourth round

## Scope

- Head reviewed: `8bd913cce11192ef314ce3d3834f839b4baaac50` on branch
  `paudley/176-keep-the-source-summary-owed-completion`. It equals
  `origin/paudley/176-keep-the-source-summary-owed-completion`, and the worktree is clean.
  Base is `bce6adae2d56` (main).
- The PR has four commits: `955948b` (review-1), `155f4b4` (review-2), `c662284` (review-3,
  PASS with two OPTIONAL findings) and `8bd913c`, the commit under review. `8bd913c` touches
  2 files, +11/-10. The whole PR is 12 files, +828/-166.
- Contract: issue #176 verbatim (`.stage/176/issue.txt`), including the restated acceptance,
  and the rulings in `plan.md` `## User rulings`. The rulings exclude the 2.67-million-record
  measurement and require `Refs #176`, not Closes.
- Method:
  - I read `git diff c6622845fe9c..8bd913cce111` in full.
  - I swept every added comment and doc line in `git diff bce6adae2d56..8bd913cce111` for
    statements about the re-base lead, the check clock, the run order and the journal
    timing, and checked them against the code: `Base::overtaken`, `Worker::entry`,
    `CHECK_EVERY`, `Migration::RUN_ORDER`, `pending_migration`, `Job::Migrate` and
    `MigrationJournal::page`.
  - I measured the width of every added line in the PR.
  - For everything 8bd913c does not touch, I reused review-1 to review-3.
- Not run: cargo, make, tests and prose-check. The caller limited this review to static,
  read-only work.

## Evidence

- **8bd913c changes only comments.** I filtered the `+` and `-` lines of
  `git diff -U0 c6622845fe9c..8bd913cce111` to drop `//` and `///` lines. Nothing was left
  (grep exit 1). The commit message ends `Refs #176` and has no attribution trailer.
- **Commit hook at this head.** `.stage/176/hook4.log` runs from 13:43:24Z to 13:53:15Z. It
  runs:
  - `cargo fmt --all --check`;
  - release workspace clippy with `--all-targets -- -D warnings`;
  - the release tests (`storage` 246 passed, 0 failed; no failing test result in the log);
  - the census check ("OK: 322 ledger jobs and 1 [[distinct]] rows hold").

  It shows the commit line `[... 8bd913c] Word the artifact re-base test's lead in the
  migration's unit` and `EXIT=0`. `validation.md` records hook4 and push4 (EXIT=0).

### Review-3 findings, rechecked

1. **Test comment unit: fixed.** `tests/storage.rs:20540-20542` now reads "Run pages until
   the migration's `done` leads the held build's snapshot by more than 2 x 4,096 entries,
   past the re-base lead plus what the build reads before its first check".
   - This matches the loop `while summary(&store).done <= base + 2 * 4_096`. `base` is read
     after `read(None)` registers the held build and before any `SourceSummary` page, so it
     is the build snapshot's `done`.
   - The first check comes when the work clock crosses 4,096 catalog entries
     (`listing_build.rs:133`). By then `progress().read`, which counts each artifact once, is
     at most 4,096. So `done - base > 8,192 >= read + CHECK_EVERY`, and
     `source_summary.rs:827` re-bases at that check. The comment is accurate, and it no
     longer calls the quantity "check intervals".
2. **Unwrapped doc lines: fixed.** `source_summary.rs:806-815` and
   `tests/storage.rs:20461-20467` are reflowed. Every line is at most 100 columns, and the
   wording is unchanged.

### Whole-PR sweep at this head

- **Re-base lead.** All three statements of the lead agree with
  `done.saturating_sub(base) > read.saturating_add(CHECK_EVERY)`:
  - the `Base::overtaken` doc, which uses the pass's own unit;
  - the test doc ("a lead of 4,096, in the migration's own unit");
  - interfaces.md:902-905 ("than the build has read plus 4,096").
- **Check clock.** The clock is described in catalog entries, counting an artifact's
  records, and matches `Worker::entry`. The places checked are: the `CHECK_EVERY` doc, the
  `Worker` and `Worker::entry` docs, the `listing_build.rs` module doc, the
  `source_summary.rs` pass doc, interfaces.md:899-901 and architecture.md:645-648 ("next
  4,096-entry check").
- **Run order.** `RUN_ORDER` is counters, source summary, owner index, backfill, then
  source context, and `pending_migration` iterates it. The following agree with it:
  - migration.rs module doc;
  - the `ALL` and `RUN_ORDER` docs;
  - the `Migration` enum doc, which now names the status order;
  - interfaces.md:629-638 and 847-849;
  - architecture.md:576-580;
  - the maintenance test comment and loop. Pages run until the first `EventFallbackRdf`
    page, through the counters, the source summary and the owner index.
- **Journal timing.** The following agree with the code:
  - the `MigrationJournal` doc;
  - the `Job::Migrate` comment;
  - deployment.md:970-974;
  - interfaces.md:659-674.

  `began` is taken before the inline deferral. `get_or_insert` keeps the first page's
  `began`, so `elapsed_seconds` is wall clock from that page's job.
- **Width.** No added comment line in any Rust file exceeds 100 columns. The added lines
  over 100 columns are:
  - three `eprintln!` format strings in `maintenance.rs:181,185,190`. These are string
    literals, not comments; splitting them would change the journal text.
  - the two `helpers-ledger.toml` rows, which are data;
  - three Markdown lines in `docs/interfaces.md`. These are listed under OPTIONAL finding 1.

### Acceptance and rulings at this head

8bd913c changes no behavior, so review-3's acceptance assessment carries over unchanged:

- Acceptance (1)-(4) are met.
- The either-order resume test holds.
- The "no new sub-second loop", "no wire or schema change" and `Refs #176` rulings hold.
- The 2.67-million-record run is excluded by ruling.

All #176 tests pass in hook4.

## Findings

1. **OPTIONAL**: `docs/interfaces.md:638` (119 columns), `:849` (112) and `:914` (128).
   - Problem: the PR added a sentence to the end of three existing lines without
     reflowing them. Their paragraphs are otherwise wrapped near 80 columns. Two shorter
     ragged lines come from the same edits: `interfaces.md:902` ("few point reads). It") and
     `migration.rs:17` ("committed page. A page yields the writer to waiting"). The text is
     accurate. The file already has other lines over 100 columns, and no gate checks
     Markdown width, so the problem is cosmetic only.
   - Fix: reflow those three paragraphs and the migration.rs module-doc paragraph to the
     width of their neighbours.

No REQUIRED findings. I found no other issue at this head.

VERDICT: PASS
