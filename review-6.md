Reviewer model: claude-opus-5-5 (opus)
VERDICT: PASS

# #176 / PR #190 independent delivery re-review, sixth round

## Scope

- Head reviewed: `3d40401ebca9a505b814ccbea8aecce437f40b3a` on branch
  `paudley/176-keep-the-source-summary-owed-completion`. It equals
  `origin/paudley/176-keep-the-source-summary-owed-completion`. Base is `bce6adae2d56`
  (main). The PR has six commits: `955948b`, `155f4b4`, `c662284`, `8bd913c`, `3b996eb` and
  `3d40401`. In total it changes 12 files, +878/-217.
- Contract: issue #176 verbatim (`issue.md`, including the restated acceptance from comment
  11617) and the three rulings in `plan.md` `## User rulings`. Those rulings are: build (b)
  with c3 and the journal timing, not (a); no new loop faster than once a second; land with
  `Refs #176`, not Closes, because the 2.67-million-record measurement is still to come.
- Method:
  - I read the whole production diff `bce6adae2d56..3d40401`: `listing_build.rs`,
    `source_summary.rs` (`Base`, `Pass`, `Completion::owed`, the entry closures),
    `source_read.rs`, `artifact_read.rs`, `migration.rs` (`RUN_ORDER`,
    `pending_migration`, `migrate_step`), `maintenance.rs` (`MigrationJournal`,
    `Job::Migrate`), `storage.rs`, the ledger row, and the edited docs.
  - I read the doc comments of the added tests.
  - I compared `3b996eb..3d40401` word by word.
  - I checked how `deploy/verify-backup.sh` parses the journal lines, and searched for any
    remaining claim about the old run order.
  - I read `hook6.log`, `push6.log`, `commit-msg6.txt`, `validation.md`, `review-debt.md`,
    the squash notes and `pr-body.md`.
- Not run: cargo, make and tests, as the caller instructed. I ran `prose-check` (read only)
  on the three docs, the commit message, the squash notes and `validation.md`.
- Process note: during this review I wrote one scratch file, `.git-a`, into the worktree
  root by mistake. It was untracked and never staged. I deleted it at once, and
  `git status --short` is clean again. No tracked file was touched.

## Evidence

- **`3d40401` changes only wording.** `git diff --word-diff 3b996eb 3d40401` shows these
  word changes and nothing else, all in docs and one `///` comment:
  - interfaces "It is selected fairly" became "The migration duty is selected fairly";
  - interfaces "it has read ... a build also reads" became "the build has read ... it also
    reads";
  - "pays for what it [still] owes until it completes" became "first reads what the
    migration still owes" in interfaces, and "until the source summary completes ... first
    reads what it still owes" in architecture;
  - architecture "It runs one bounded page" became "The worker runs one bounded page";
  - the `MigrationJournal` comment now says "then progress ... and completion, which carry
    the time since the start of that first page".
- **Review-5 REQUIRED finding 1: resolved.** `docs/interfaces.md:639` now reads "The
  migration duty is selected fairly with buffered synchronization, ...". The "it" in "Below
  the free-space floor it is skipped" therefore refers to the duty, so `status` no longer
  reads as skippable. Line 640, flagged as ragged in review-5, is reflowed.
- **Review-5 optional findings:**
  - 2 is resolved (`architecture.md:580`, "The worker runs").
  - 3 is resolved in `interfaces.md:632-634`, but only partly in `architecture.md:577-579`
    (Finding 1 below).
  - 4 is resolved (`interfaces.md:913`).
  - 5 is resolved. The comment now matches `MigrationJournal::page`: the start line prints
    no `elapsed_seconds`, while the progress, complete and failed lines do. `started` is
    set from `began` at the session's first page, and `began` is taken before the inline
    `defer_migration_page`.
  - 6 is resolved. `validation.md` now records `hook5.log` and `push5.log` with EXIT=0.
- **Code and docs agree** (re-confirmed at this head):
  - `pending_migration` walks `Migration::RUN_ORDER[1..]`, which is counters, source
    summary, owner index, backfill, source context. `ALL` stays the status order.
  - `Base::overtaken` returns true when the migration is complete, false when the
    checkpoint has not moved, and otherwise compares the `done` delta with
    `read + CHECK_EVERY`. That matches interfaces 913-927, the `listing_build` module doc
    and the ledger summary.
  - `Worker::entry` checks on crossings of `CHECK_EVERY` in catalog entries read, counting
    the records an artifact's count reads. That matches the "an artifact's records count,
    though `read` counts the artifact once" text.
  - A failed page returns `Err` from `migrate_step`, so it gets no journal line.
  - `verify-backup.sh:819` still parses `progress|complete pages=` with the added
    `elapsed_seconds` field. `DONE_ORDER` records the observed order, so the new run order
    does not break it.
  - No doc, script or source comment still says that `source-summary` runs after the owner
    index or backfill. `interfaces.md:846` was updated.
- **Acceptance (restated, plan.md):**
  - (1) run order: tested by `the_worker_runs_the_source_summary_migration_directly_after_the_store_counters`.
  - (2) re-base for `sources` and live `artifacts`: tested by
    `a_source_listing_build_the_migration_overtakes_delivers_the_finished_rows_at_its_next_check`
    and `a_live_artifact_window_build_re_bases_onto_a_newer_snapshot_mid_migration`. Both
    tests check rows equal to the finished listing and nothing lost or duplicated.
  - (3) was already tested before this PR.
  - (4) start line and `elapsed_seconds` are in place. The 2.67M run is excluded by ruling,
    and the commit and squash notes use `Refs #176`.
  - (5) either-order resume: tested by
    `a_store_part_way_through_its_migrations_under_either_order_resumes_to_the_finished_rows`.
  - Each test names the invariant it protects. Reviews 1 to 4 already established their
    value, and `3d40401` changes no code.
  - Rulings: there is no new timer or loop. The re-base check is clocked by entries read,
    not by time. There is no wire or schema change, and option (a) is absent.
- **Gate at this head.** `/home/paudley/Active/sparrow/.stage/176/hook6.log` ran from
  14:27:30Z to 14:33:30Z:
  - `cargo fmt --all --check`;
  - `cargo clippy --locked --release --workspace --all-targets -- -D warnings`;
  - `cargo test --locked --release --workspace`: 14 `test result: ok` lines, `storage` 246
    passed and 0 failed;
  - the census self-test PASS lines;
  - "OK: 322 ledger jobs and 1 [[distinct]] rows hold".

  The log shows the commit line `[... 3d40401]` and `EXIT=0`. Its `Error:` lines (500, 540
  and 600) are the expected output of refusal tests that pass. `push6.log` shows
  `3b996eb..3d40401` and `EXIT=0`. `commit-msg6.txt` ends `Refs #176` and has no
  attribution trailer.
- **Prose-check at this head** (run by this review):
  - On the three docs it exits 1. Every hit is a `UInt64`/`UInt32` line (interfaces 70,
    1370, 1524, 1526, 1583, 1667, 2624, 2629 and 3547), and none falls inside the PR's
    interfaces hunks (627-641, 645-673, 842-857 and 912-928). The diff adds no `UInt`.
  - `commit-msg6.txt` exits 0, the squash notes exit 0 and `validation.md` exits 0.
- **Squash notes are accurate for this head.** They list six commits ending with `3d40401eb`
  and contain no TODO lines. The file table matches `git diff --stat` (12 files,
  +878/-217), and the body says `Refs #176` and that #176 stays open.

## Findings

1. **OPTIONAL**: `docs/architecture.md:577-579` only partly fixes review-5 optional 3.
   - The sentence reads "since until the source summary completes every source and live
     artifact listing first reads what it still owes;".
   - With no comma after "completes", it first reads as "completes every source".
   - "it" binds to the main clause's subject, the listing, which owes nothing. The
     `interfaces.md` version is correct: "what the migration still owes".
   - Fix: "since, until the source summary completes, every source and live artifact
     listing first reads what that migration still owes;".
2. **OPTIONAL**: `review-debt.md` is stale for this head.
   - Its Evidence stops at `155f4b43fefd` and review-1.
   - It records no dispositions for reviews 2 to 5, including review-5's REQUIRED finding
     and how it was resolved. `validation.md` carries those dispositions, so nothing is
     lost.
   - Fix: refresh `review-debt.md` to `3d40401` and list the review-2 to review-6
     dispositions.
3. **OPTIONAL**: `validation.md` needs final touches.
   - Its verdict line still reads "BLOCKED (pending the hook and the independent re-review
     at the final pushed head)". Both have now happened: `hook6.log` EXIT=0, and this
     review passes.
   - Its prose line cites "`prose.log` and later runs", but `prose.log` is from the first
     revision (06:17) and no log records the later runs. This review's prose-check results
     above can be cited.
   - "all fixed in 3d40401ebca9" slightly overstates review-5 optional 3 (Finding 1).
   - Fix: set the verdict, cite the prose-check evidence for this head, and note Finding 1's
     disposition.

No REQUIRED findings. Review-5's required finding is resolved, and `3d40401` changes only
wording with no regression in code, tests, docs or ledger. Acceptance items (1) to (5) and
all three rulings hold at this head. The 2.67-million-record measurement stays open under
`Refs #176` by ruling.

VERDICT: PASS
