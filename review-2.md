Reviewer model: claude-opus-5-5 (opus)
VERDICT: PASS

# #176 / PR #190 independent delivery re-review

## Scope

- Head reviewed: `155f4b43fefd0fcd74e8d17855df3c64589f5b8f` (branch
  `paudley/176-keep-the-source-summary-owed-completion`), base `bce6adae2d56` (main). The PR has
  two commits: `955948b`, which review-1 passed, and the fix `155f4b4`. The fix touches 7 files,
  +87/-54. The whole PR is 12 files, +826/-166.
- Contract: issue #176 and its comments (`.stage/176/issue.txt`), with the restated acceptance
  (1)-(4) in the 2026-11-10 06:02 comment. Rulings: the three entries in `plan.md`
  `## User rulings`:
  - build (b) plus c3 plus the per-migration journal timing, test resume under either order,
    and do not build (a);
  - no new loop or timer firing more than once a second;
  - land with `Refs #176`.
- Method: a static read of `git diff 955948bd7f3b..155f4b43fefd` and of every function the fix
  touches, in full:
  - `listing_build::Worker`, `entry` and `CHECK_EVERY`;
  - in `source_summary`: `Completion::owed`, `artifact_page`, `record_page`, `count_artifact`,
    `page`, `Base::overtaken`;
  - the closures in `artifact_read::owed_index_rows` and `source_read::source_capture`;
  - `migration.rs` docs and visibility;
  - `maintenance.rs` `Job::Migrate` and `MigrationJournal`;
  - the test-support hold and entry counter (`storage.rs` 2915-3011, tests `hold_listing_builds`);
  - the two re-base tests;
  - the edited docs.

  For the unchanged parts of the PR I relied on review-1. I ran no cargo, make, tests or
  prose-check, because the caller limited this review to static, read-only work.

## Evidence

- `.stage/176/hook2.log` is the commit hook for this head. It runs `cargo fmt --all --check`,
  release workspace clippy with `-D warnings`, release workspace tests, the census self-test and
  the pinned cross check. The log runs from 12:58:43Z, contains the commit line
  `[... 155f4b4] Pace listing build checks by the catalog entries read`, and ends `EXIT=0`.
  Results:
  - `storage`: 246 passed, 0 failed.
  - All four #176 tests pass.
  - The order-sensitive existing tests pass:
    `every_read_answers_completely_while_online_migrations_run_and_intake_flows` and
    `a_live_listing_across_migration_pages_and_expiry_lists_the_finished_index`.
  - The held-build test whose assert reads `progress.read == 1` (storage.rs:19998) passes.
- `round2.log` (EXIT=0): focused release clippy for core and server, the ten selected `storage`
  migration and listing tests, and the five `maintenance` tests.
- `round2b.log` (EXIT=0): the two re-base tests after the source bound was tightened to 4.
- Commit message: ends `Refs #176`, with no attribution trailer.
- No wire or format file is touched, so the "no wire/schema change" ruling still holds.

### Review-1 findings, rechecked

1. **Check clock in the artifact phase: fixed.**
   - `Worker` (listing_build.rs:104-109) now holds a per-build `work: Cell<u64>`.
     `Worker::entry(entries, ..)` (111-138) still adds 1 to the progress `read`. It adds
     `entries` to `work` and runs the cancellation, lease and `overtaken` check whenever
     `work / CHECK_EVERY` increases.
   - `artifact_page` passes `ids.len() + 1`: the artifact entry plus the record ids that
     `count_artifact` point-read (source_summary.rs:987-999). `record_page` passes 1 (1097).
   - For a pass with no artifacts this gives the old schedule (4,096, 8,192, ...). In the
     artifact phase a check fires at the first artifact that crosses the next multiple. The only
     coarser case is an artifact with more than 4,096 records: it is counted whole before the
     check. The migration's own page counts it whole too.
   - Progress `read` and `total` keep the migration's unit, so `owed_entries`, `Base::overtaken`
     and `BuildProgress` stay consistent.
   - The `Cell` makes `Worker` `!Sync`. It is used only on its own build thread through
     `&Worker`, and that compiles in the hook.
2. **Journal timing wording: fixed in the published docs.** interfaces.md:666-670 now says
   that the run starts at the job that committed the first page, and names the one wait that is
   not counted. A code comment still says the opposite; see optional finding 2.
3. **"third online migration": fixed** (interfaces.md:843).
4. **`RUN_ORDER` visibility: fixed.** It is `pub(crate)` (migration.rs:86). The public docs of
   `ALL` (migration.rs:70-72) and `migrate_step` (341) no longer link it. The module doc at
   migration.rs:11 still links it, but `storage::migration` is `pub(crate)` (storage.rs:48), so
   no public doc links a private item.

### Behavior changed by 155f4b4

- **Moved stop point in `artifact_page`.**
  - The page-limit test still runs before the artifact is counted. `entry(read)` now runs after
    `count_artifact` and after `last = Some(id)`. When it returns false, `full = true`, so the
    page returns `Artifacts(last)`. `Completion::owed` then sees `stopped` and returns
    `Pass::Overtaken` (source_summary.rs:903-906, 927-928). Nothing counted is kept, as before.
  - An `entry` error propagates through `scan` with `?`, as before.
  - Cost of the move: a cancelled or overtaken pass now counts one more artifact before it
    stops. That artifact is discarded.
- **Migration pages.** `page` passes `&mut |_| Ok(true)`, so the stop never fires. The limit
  order, the `records` accumulation and the checkpoint are unchanged. The only new path is the
  `usize`-to-`u64` and `+1` overflow error, which cannot trigger on a real record count. It fails
  loudly with the artifact id rather than wrapping. Migration page behavior is unchanged.
- **Test-support hold and entry counter.**
  - The hold still fires when progress `read == at`, before the work and check step. It now
    fires after the first artifact is counted rather than before. The hold doc ("once it has read
    `entry` entries") is now more accurate, and the existing `progress.read == 1` assert still
    holds.
  - In the source re-base test, the hold-then-check order means the first check runs in the
    same `entry` call as the hold, or a later one, once the 5,000-record artifact has been
    counted.
  - The entry counter still adds progress `read` before `State::Done` is published. It is
    unchanged in meaning.
- **Tightened source re-base bound** (storage.rs:20447-20448, `read <= 4`).
  - Fixture: four artifacts (5,000 + 3 x 2 records), so owed is 5,010.
  - Whatever their key order, `work` crosses 4,096 at the wide artifact, at progress read
    1 to 4. That check sees the migration complete and re-bases. The fresh pass returns
    `Pass::Complete` without entries, so the build reads at most 4 entries.
  - Under the 955948b clock, the first check runs at progress read 4,096, so `read <= 4` fails
    there. The tightened bound therefore protects the new clock, not just the re-base.
- **Artifact window re-base test, still discriminating.** The first check now fires at the
  first 6,000-record artifact, read 1. The migration has done 16,390 entries (the page trace
  from review-1), more than 1 + 4,096, so the build re-bases and then reads the 7,620 entries
  still owed. `entries < owed` (24,010) holds, and without re-basing it fails.
- **One-second ruling.** The fix adds no timer or loop; checks remain work-proportional.

### Acceptance at this head

The fix changes only acceptance (2), and it improves it:
- In the artifact phase, "one 4,096-entry check after the migration completes" is now reached
  after 4,096 catalog reads instead of 4,096 artifacts.
- The rows still come from one snapshot through the unchanged counting law.
- The re-base termination argument from review-1 is unchanged: each re-base needs the
  checkpoint to have moved and a lead of more than 4,096 progress entries.

Acceptance (1), (3), (4) and the either-order resume ruling are unaffected. Their tests pass in
hook2. All remain met.

## Findings

1. **OPTIONAL**: `crates/sparrow-core/src/storage/source_summary.rs:806-812`, and the test doc
   at `crates/sparrow-core/tests/storage.rs:20461-20463`.
   - Problem: `Base::overtaken` still calls its threshold "plus one build check interval" and
     says "the lead of one interval". After this commit, a check interval is 4,096 catalog
     entries counting an artifact's records. The threshold is 4,096 entries in the pass's
     progress unit, which in the artifact phase is 4,096 artifacts. The code is consistent, and
     interfaces.md:902-903 says "plus 4,096" correctly. The doc comment now names a different
     quantity.
   - Fix: say "plus [`CHECK_EVERY`] entries in the pass's unit" (or similar) in both places.
2. **OPTIONAL**: `crates/sparrow-server/src/maintenance.rs:114`.
   - Problem: the comment "The page's time includes its wait for writers" is left over from
     review-1 finding 2, which the fix corrected only in the docs. For a `defer: false` page,
     the writer wait ran earlier in the background deferral, outside `began`. interfaces.md now
     documents exactly that.
   - Fix: limit the comment to the inline (`defer`) wait, or point it to the journal rule in
     interfaces.md.

No REQUIRED findings.
