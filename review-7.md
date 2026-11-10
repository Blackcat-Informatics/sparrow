Reviewer model: claude-opus-5-5 (opus)
VERDICT: PASS

# #176 / PR #190 independent delivery re-review, seventh round

## Scope

- Head reviewed: `4354a4a4b1ef0d601608fb949a2cb5388dbab3c8` on branch
  `paudley/176-keep-the-source-summary-owed-completion`. It equals
  `origin/paudley/176-keep-the-source-summary-owed-completion` and the PR head in
  `status.json` (observed 14:50:13Z). The base is `bce6adae2d56`. There are seven commits,
  `955948b` to `4354a4a`. In total they change 12 files, +874/-213. The worktree is clean.
- Contract: issue #176 verbatim (`issue.md`, with the restated acceptance from comment
  11617) and the three rulings in `plan.md` `## User rulings`:
  - build (b) with c3 and the journal timing, not (a);
  - no new loop or timer faster than once a second;
  - land with `Refs #176`, not Closes.
- Method:
  - I read the whole diff `bce6adae2d56..4354a4a`: `listing_build.rs`,
    `source_summary.rs` (`done`, `Base`, `Pass`, `Completion::owed`, `artifact_page`,
    `record_page`), `source_read.rs` (`source_capture` loop), `artifact_read.rs`
    (`owed_index_rows` loop), `migration.rs` (`RUN_ORDER`, `pending_migration`),
    `maintenance.rs` (`MigrationJournal`, `Job::Migrate`), `storage.rs`, the four new
    tests, the updated server test, the ledger row and all three docs.
  - I compared `3d40401..4354a4a` word by word.
  - I read `hook7.log`, `push7.log`, `commit-msg7.txt`, `validation.md`, `review-debt.md`,
    `status.json`, `pr-body.md` and the squash notes.
  - I checked how `deploy/verify-backup.sh` reads and reprints the journal lines.
- Not run: cargo, make and tests, as the caller instructed. I ran `prose-check` (read only).

## Evidence

- **`4354a4a` changes punctuation and wording only.** `git diff --word-diff=porcelain
  3d40401 4354a4a` shows exactly four token changes:
  - `since` became `since,`;
  - `completes` became `completes,`;
  - `it` became `that migration`.

  The rest of the hunk is the reflow of `docs/architecture.md:576-585`, and no line in the
  paragraph is over 80 columns. The sentence now reads "since, until the source summary
  completes, every source and live artifact listing first reads what that migration still
  owes". That is review-6's suggested fix, so **review-6 optional 1 is resolved**.
- **Review-6 optional 2 (`review-debt.md`) is resolved.** The file now records the
  dispositions of reviews 1 to 6, including review-5's REQUIRED finding and how it was
  finished across `3d40401` and `4354a4a`.
- **Review-6 optional 3 (`validation.md`) is resolved.**
  - The verdict line now says BLOCKED pending this review at `4354a4a4b1ef`, which matches
    the caller's statement.
  - The prose line states that the later prose-check runs kept no log.
  - Review-5's disposition now names both commits.
  - `hook7.log` and `push7.log` are recorded, and their entries match the logs (next item).
- **Gate at this head.** `/home/paudley/Active/sparrow/.stage/176/hook7.log` ran from
  14:44:27Z to 14:48:18Z:
  - `cargo fmt --all --check`;
  - `cargo clippy --locked --release --workspace --all-targets -- -D warnings`;
  - `cargo test --locked --release --workspace`: 14 `test result: ok` lines with no
    non-zero `failed` count, and `tests/storage.rs` 246 passed, 0 failed;
  - the census self-test PASS lines;
  - "OK: 322 ledger jobs and 1 [[distinct]] rows hold".

  The log then shows the commit line `[... 4354a4a]` and `EXIT=0`. `push7.log` shows
  `3d40401..4354a4a` and `EXIT=0`. `commit-msg7.txt` ends `Refs #176` and has no
  attribution trailer. The forge CI check in `status.json` is `skipped`, so the hook is the
  only gate run at this head.
- **The code is unchanged since review-6, and it still matches the docs.**
  - `pending_migration` walks `Migration::RUN_ORDER[1..]`, which runs counters, source
    summary, owner index, backfill, source context. `ALL` keeps the `status` order.
  - `Worker::entry` checks each time the catalog entries read cross a multiple of
    `CHECK_EVERY`, counting an artifact plus the records its count read. It then returns
    `!overtaken(progress.read)`, so `false` means "re-base", as its doc says.
  - `Base::overtaken` returns true when the migration is complete and false when the
    checkpoint has not moved. Otherwise it compares the `done` delta with
    `read + CHECK_EVERY`. That matches `interfaces.md:912-927`, the `listing_build`
    module doc and the ledger summary.
  - When `Pass::Overtaken` is returned, both callers run `worker.rebase()` and loop to a
    fresh snapshot. `source_capture` also re-reads its registries under the writer lock.
    A capture without a build (`worker == None`) always answers `Ok(true)`, so it never
    loops.
  - Errors from the `entry` callback, `checkpoint`, `overtaken` and the overflow guard in
    `artifact_page` all propagate as `Err`. Nothing is swallowed.
  - The re-base check is clocked by entries read, not by time, so there is no new timer.
  - The journal `start` line has no `elapsed_seconds`; the progress, complete and failed
    lines do. `started` comes from `began`, which is taken before the inline
    `defer_migration_page`. This matches `interfaces.md` and `deployment.md`.
  - `verify-backup.sh:819` still parses `progress|complete pages=`. `verify-backup.sh:450`
    reprints every `sparrowd: migration` line, `start` included, so the PR body's
    "already reprints them" is accurate.
- **Acceptance (restated, `plan.md`) holds at this head:**
  - (1) run order: `the_worker_runs_the_source_summary_migration_directly_after_the_store_counters`;
  - (2) re-base for `sources` and for live `artifacts`, with rows equal to the finished
    listing and nothing lost or duplicated:
    `a_source_listing_build_the_migration_overtakes_delivers_the_finished_rows_at_its_next_check`
    and `a_live_artifact_window_build_re_bases_onto_a_newer_snapshot_mid_migration`;
  - (3) was already tested before this PR;
  - (4) the start line and `elapsed_seconds` are in place, and the 2.67M run is excluded
    by ruling;
  - (5) resume under either order:
    `a_store_part_way_through_its_migrations_under_either_order_resumes_to_the_finished_rows`.

  Each test names the invariant it protects and exercises the real `Store` entry points
  (`migrate_step`, `source_read`, `artifact_read`). All of them passed in `hook7.log`
  (`storage` 246 passed).
- **Rulings:** option (a) is absent; there is no wire, schema or writer-format change;
  there is no new loop or timer; and the commits, squash notes and PR body use `Refs #176`.
- **Squash notes are accurate for this head.**
  - "Commits squashed (7)" lists `4354a4a4b` to `955948bd7`, matching `git log`.
  - The file table matches `git diff --stat bce6adae2d56..4354a4a` (12 files, +874/-213).
  - The body says the 2.67M measurement has not been run and that #176 stays open.
- **Prose-check (run by this review):**
  - `docs/architecture.md` exits 0.
  - `validation.md`, `review-debt.md`, the squash notes and `commit-msg7.txt` exit 0
    together.

## Findings

1. **OPTIONAL**: `review-debt.md`'s Evidence line still cites the forge snapshot taken at
   `155f4b43fefd` ("stagectl status and preflight at 155f4b43fefd").
   - A newer snapshot exists: `status.json`, observed at 14:50:13Z with head `4354a4a`,
     shows 0 comments, reviews and inline comments, all with complete coverage, and
     `pr-comments.md` was rewritten then.
   - The conclusion is unchanged, but the citation should name the snapshot at the final
     head.
2. **OPTIONAL (bookkeeping, expected)**: `validation.md`'s verdict line is still BLOCKED,
   pending this review. Once this PASS is recorded, it should be set to PASS and cite
   `review-7.md`. `review-debt.md` should add the review-7 entry. `status.json` lists
   `validation.md` as its only open merge artifact.

No REQUIRED findings. Review-6's three optional findings are resolved, and `4354a4a`
changes only the punctuation and wording of one paragraph, with no regression. Acceptance
items (1) to (5) and all three rulings hold at this head. The 2.67-million-record
measurement stays open under `Refs #176` by ruling.

VERDICT: PASS
