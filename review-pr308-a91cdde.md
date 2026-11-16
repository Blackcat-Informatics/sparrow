# Review: PR #308 (issue #304) at a91cdde

Reviewed 2026-11-15 21:25 CST.

- Head: a91cddee4d63c4a896dfe220912ceee84ccc9daa, checked with `git rev-parse HEAD` in
  `.worktrees/304-archive-prepare-read-cost-grows-with`.
- The worktree was clean, and `origin/paudley/304-archive-prepare-read-cost-grows-with` was at
  the same head.
- The previous review passed e7d50ad (`review-pr308-e7d50ad.md`). This recheck covers what
  changed since then: b7de656 (test and figures) and a91cdde (merge of origin/main bf42118).
  It also covers the draft PR body.

## Scope

What I read:
- `git diff e7d50ad b7de656`: only `crates/sparrow-core/tests/archive_segment.rs:228-262`.
- The b7de656 commit message.
- `archive_encode.rs` ceilings at head (unchanged since e7d50ad) and at 7dcd8bd
  (`git show 7dcd8bd:.../archive_encode.rs:60-81`).
- The merge:
  - `git merge-base b7de656 bf42118` = 5f6202b.
  - `git diff b7de656 a91cdde` compared byte for byte with `git diff 5f6202b bf42118`.
  - `git diff e7d50ad a91cdde` over `archive_encode.rs`, `archive_segment.rs`, `archive.rs`,
    `rdf_view.rs`, `rdf_view/`, `rdf_index.rs`, `writer_queue.rs` and `storage.rs`.
  - The added lines in `table_options.rs`.
- Evidence in `.stage/evidence-keep/304/`: `md-segment.log`, `mdprobe.log`, `commit2-hook.log`
  (header, result lines, tail), `sync.log` and the draft `pr-body.md`.
- To check PR-body claims:
  - `.stage/archive/lane304/fix2-lock-50000.log` and the `worst_ingest_us` lines of the other
    lock logs.
  - The failure section of `fix2-storage-all.log`.
  - The `after_import req=147493` probe line.
  - Issues #319 and #320 (`stagectl issue`).

What I did not read or run:
- No cargo, build, test or hook, as instructed.
- The probe sources (`mdprobe.rs`, `probe304.rs`). Neither is committed: no file matching
  `probe` is in the a91cdde tree.
- The merge commit's hook output. None was kept, so I have no direct evidence of the gate on
  a91cdde.
- The forge CI for the head.
- The merged #313/#314 changes beyond confirming that they do not touch the archive path.
- PurRDF's markdown citation rules, so whether the fixture's footnotes produce triple terms is
  unknown.

## Evidence

- `md-segment.log`:
  - Worktree named; HEAD e7d50ad plus the uncommitted test change; debug.
  - `tests/archive_segment.rs`: 7 passed, including
    `a_json_or_markdown_record_with_thousands_of_graph_rows_prepares`. EXIT=0.
- `mdprobe.log`:
  - HEAD b7de656 plus an uncommitted probe; debug; EXIT=0.
  - The markdown fixture's projection graph has `streams=[26706, 0, 0]`: 26,706 primary rows and
    no reifier or annotation rows. The record graph has 13 rows and the artifact graph 10.
- `commit2-hook.log`:
  - Full release gate, EXIT=0, ending with the commit line for b7de656.
  - `archive_segment`: the new test is ok (line 178).
  - `storage`: 314 passed (line 501). 19 `test result: ok` lines, none failed.
- `sync.log`: `Sync: completed`, EXIT=0. It holds no hook output.
- Merge a91cdde:
  - `diff <(git diff b7de656 a91cdde) <(git diff 5f6202b bf42118)` is empty. The merge brings
    exactly main's nine-file change and nothing else.
  - None of those files is on the archive path (`table_options.rs` adds a WAL size cap).
  - `git diff e7d50ad a91cdde` over the archive, rdf_view, writer-queue and storage files is
    empty.
  - The production code reviewed at e7d50ad is therefore unchanged at a91cdde.

## What holds

- **The test protects the named behaviour.**
  - One table drives the real `archive_prepare` and `archive_commit` path, each case on a fresh
    store.
  - Cases: a json record (about 12,043 rows) and a markdown record (26,706 rows). That is near
    the store's `MAX_ROWS` (32,768) per graph.
  - Both exceed the entries-only ceiling of 7dcd8bd by far, so the test fails on that ceiling.
    It passes under the committed per-row ceiling.
  - Markdown is now covered by execution, which closes optional finding 1 of the e7d50ad review
    as far as this fixture goes.
- **The test's doc figures agree with the code and the evidence.**
  - 800 keys at about 15 rows per key: 12,043 / 800 = 15.05.
  - About 250,000 requests: 252,910 in `cal2-json-800.log`.
  - 11,024 for one record and its artifact: 10,000 + 2 × 512 at 7dcd8bd.
  - "Thousands of graph rows" holds for both cases.
- **The commit message's corrected figures hold.**
  - 11,024 at 7dcd8bd.
  - About 13,500 at 5 records: 10,000 + 6 × 16 + about 80 rows × 42 = 13,456. That is still
    below 14,161.
- **PR-body claims checked against evidence:**
  - Ceiling constants: match the code.
  - 21 requests and 2.0 to 2.7 row visits per row: derived from the two cal2 logs.
  - 12.4M requests against a 26.0M ceiling: `lock2-log-50000.log`.
  - 689 ms / 2,735 µs, 528 ms / 4,351 µs, 31,804 ms / 8,727 µs and 15,244 ingests: the cal2 and
    lock2 logs.
  - 660 ms before `lock_page`: `worst_ingest_us=660730` in `fix2-lock-50000.log`.
  - 147,493 through the union: the probe line `after_import req=147493`.
  - The two storage tests that failed earlier: `fix2-storage-all.log` shows time-admission
    errors (`RESOURCE_LIMIT` staging admission and `RESULT_EXPIRED`) in a 797 s debug run.
    They pass alone (`storage-two.log`), and 314 storage tests pass in both hooks.
  - Markdown 26,706 rows with 0 reifier rows, and the honest note that triple-term cost is not
    measured: `mdprobe.log`.
  - #319 and #320 exist and are open.

## Findings

1. **REQUIRED (false claim in the PR body): the entries-only threshold is overstated about
   fourfold.**
   - The body says the entries-only ceiling of 7dcd8bd "refused a single JSON or Markdown record
     above about 2,100 rows".
   - That ceiling allowed 11,024 requests and 2,128 row visits for one record and its artifact.
   - At the measured cost of 21 requests per row, the request ceiling is reached at roughly
     500 rows (11,024 / 21 ≈ 525, less the record and artifact rows).
   - The 2,100 figure assumed one request per row. It came from the first review's estimate,
     which predates the per-row measurement.
   - Fix: say "above roughly 500 rows (11,024 requests at the measured 21 per row)". Or drop
     the number.
   - Text only; no code change. The e7d50ad commit message carries the same figure, but it is
     history and need not be rewritten.
2. **REQUIRED (unsupported evidence claim once the placeholder is filled): the merge hook entry
   points to a log that has no hook output.**
   - The body lists "`sync.log` (merge of main, hook HEAD_PLACEHOLDER)" among the hook evidence.
   - `sync.log` holds only `Sync: completed`, and no hook output was kept for a91cdde.
   - Fix: state that the merge commit's hook ran and the commit succeeded, but its output was
     not kept. Cite `sync.log` only for the sync.
3. **OPTIONAL: the ff88ab3 review is misdescribed.** "BLOCKED at ff88ab3 (entries-only ceiling,
   writer hold)" makes the writer hold a blocking reason. In that review it was optional
   finding 2; only the entries-only ceiling blocked.
4. **OPTIONAL: acceptance numbers at 70 and 700 records are not cited.**
   - Issue #304 asks for measurements at 70, 700 and more than 1,000 records.
   - They exist in `.stage/archive/lane304/prepare-cost-after.md` and `probe-after-{70,700,1500}.log`:
     a flat 255 requests per record, in debug, on the same read path.
   - The new body's evidence list omits them, and `evidence-keep/304/` does not hold copies.
   - Cite or keep them so the acceptance trail survives cleanup of `lane304`.
5. **OPTIONAL: issue #320 overstates one point.** It says the segment "halves and retries at the
   60 s read lease". The lease starts when the RDF read begins, after the record bytes are read,
   so slow record reads do not hit it.
6. **OPTIONAL: the merged tree has no direct gate evidence.**
   - The merge adds only main's change, which is disjoint from the PR, and the commit exists
     under a mandatory hook.
   - Forge CI showed "skipped" on the earlier PR view.
   - No observed gate output covers a91cdde. I did not check CI for this head.

The code at a91cdde is the code that passed at e7d50ad, plus a test that protects the
behaviour, plus main's unrelated change. The two REQUIRED items are corrections to the PR body
text before merge; neither needs a code change or a new head.

VERDICT: BLOCKED at a91cddee4d63c4a896dfe220912ceee84ccc9daa
