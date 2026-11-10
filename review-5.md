Reviewer model: claude-opus-5-5 (opus)
VERDICT: BLOCKED

# #176 / PR #190 independent delivery re-review, fifth round

## Scope

- Head reviewed: `3b996eb412007d225bd47d1c23c5718bf6027390` on branch
  `paudley/176-keep-the-source-summary-owed-completion`. It equals
  `origin/paudley/176-keep-the-source-summary-owed-completion`, and the worktree is clean.
  Base is `bce6adae2d56` (main).
- The PR has five commits: `955948b`, `155f4b4`, `c662284`, `8bd913c` (reviews 1 to 4, all
  PASS) and `3b996eb`, the commit under review. `3b996eb` touches 3 files, +68/-68. The
  whole PR is 12 files, +867/-205.
- Contract: issue #176 verbatim (`.stage/176/issue.txt`) and the rulings in `plan.md`
  `## User rulings`. The rulings exclude the 2.67-million-record measurement and require
  `Refs #176`, not Closes.
- Method:
  - I read `git diff 3b996eb4120^ 3b996eb4120` in full.
  - I compared word streams before and after `3b996eb` for each of the three files.
  - I read the "Online store migrations" and "Listing builds" sections of
    `docs/interfaces.md` as they stand at this head.
  - I swept every added doc line and every added Rust comment in
    `git diff bce6adae2d56..3b996eb4120`. I checked them against their surrounding prose and
    against the code they describe: `MigrationJournal::page`, `Job::Migrate`,
    `Worker::entry`, `owed_index_rows` and `source_capture`.
  - For behavior that `3b996eb` does not touch, I reused reviews 1 to 4.
- Not run: cargo, make, tests and prose-check. The caller limited this review to static,
  read-only work.

## Evidence

- **3b996eb changes no words.** I split each file into whitespace-separated words, with
  `//!` markers dropped, at `3b996eb^` and at `3b996eb`:
  - `migration.rs`: the two word sequences are identical (2,214 words each).
  - `architecture.md`: the two word sequences are identical (6,937 words each).
  - `interfaces.md`: the word counts match (32,454 each). Only one block of words moved:
    the 204 words from "The migration's own pages keep reading" to "pages for CPU." are
    identical in both versions. With that block removed, the two files are identical.
    Before the commit, the block was followed by "Following that cursor waits on the...".
    Now it follows "...the process exit ends it at once."

  So the commit changes line breaks and moves one paragraph, and changes nothing else. All
  three files are docs or `//!` comments.
- **Review-4 finding: fixed.** The three over-long lines (`interfaces.md` 638, 849 and 914
  at `8bd913c`) and the ragged `migration.rs:17` line are reflowed. No line that `3b996eb`
  adds is wider than 92 columns. The only line in the PR's added docs that is wider than 82
  columns is the code span at `interfaces.md:662`, and that span cannot be wrapped.
  - One ragged line remains, `interfaces.md:640` ("retention and the table rewrite. Below
    the"). It is cosmetic and is fixed together with REQUIRED finding 1.
- **Listing builds now read correctly** (`interfaces.md:884-929`):
  - "Its `snapshot_id` is the build's." is again followed directly by "Following that
    cursor waits on the same build again", so the cursor sentence has its antecedent
    (`next_cursor`, line 894).
  - The re-base paragraph now comes after the cursor, lease, release and limit text it
    relies on, and it is consistent with "Once the migration is complete no listing
    builds."
  - The paragraph's statements still match the code that reviews 1 to 4 checked:
    `Base::overtaken`, `CHECK_EVERY` and `Worker::entry`/`rebase`.
- **Commit hook at this head.** `.stage/176/hook5.log` runs `cargo fmt --all --check`,
  release workspace clippy with `--all-targets -- -D warnings`, and the release tests:
  - 14 `test result: ok` lines and no `FAILED` result;
  - `storage` 246 passed, 0 failed;
  - the census self-test and the ledger check ("OK: 322 ledger jobs and 1 [[distinct]]
    rows hold").

  The log shows the commit line `[... 3b996eb] Reflow the edited doc paragraphs and restore
  the build cursor sentence` and `EXIT=0`. Its `Error:` lines (539, 597 and 606) are the
  expected output of refusal tests that pass. `push5.log` shows `8bd913c..3b996eb` and
  `EXIT=0`. The commit message ends `Refs #176` and has no attribution trailer.
- **Acceptance and rulings.** `3b996eb` changes no behavior, so review-4's assessment
  carries over unchanged:
  - acceptance items (1) to (4) are met;
  - the either-order resume test holds;
  - the "no new sub-second loop", "no wire or schema change" and `Refs #176` rulings hold;
  - the 2.67-million-record run is excluded by ruling.

## Findings

1. **REQUIRED**: `docs/interfaces.md:638-642`, the pronoun "It" now points at `status`.
   - Problem: before this PR, "It is selected fairly with buffered synchronization, ...
     Below the free-space floor it is skipped" directly followed the sentence that
     introduces the migration duty ("runs them as one duty, ... one page per admission").
     The PR inserted three sentences in between, and the last of them begins "`status`
     lists them in a fixed order ...". Read literally, the paragraph now says that `status`
     is selected fairly with the maintenance duties and is skipped below the free-space
     floor. That is false: `status` is an RPC read and is always answered. An operator
     could take it to mean that `status` is unavailable when space is low. This is the
     same kind of fault as the orphaned cursor sentence that `3b996eb` fixed: an insertion
     cut a sentence off from what it refers to. It is in the interface spec, and this PR
     introduced it.
   - Fix: change "It is selected fairly" to "The migration duty is selected fairly". The
     second "it" ("Below the free-space floor it is skipped") then reads correctly. Another
     option is to move the three inserted sentences after the sentence about the
     free-space floor. While editing, reflow line 640.
2. **OPTIONAL**: `docs/architecture.md:579`, "It runs one bounded page per admission,
   fairly with its other duties".
   - Problem: the PR changed "The joined maintenance worker runs one bounded page" to "It
     runs" after inserting a clause that ends "each migration resumes from its own
     checkpoint, so the order can change between images." The nearest subject is now "each
     migration", which has no "other duties". The meaning can still be recovered.
   - Fix: write "The worker runs one bounded page per admission".
3. **OPTIONAL**: the phrase "pays for what it [still] owes until it completes", at
   `docs/interfaces.md:633` and `docs/architecture.md:578`.
   - Problem: grammatically, "it" can be read as the listing, which owes nothing. The
     meaning is the source-summary migration.
   - Fix: write "pays for what the source-summary migration still owes until that migration
     completes".
4. **OPTIONAL**: `docs/interfaces.md:914-915`, "So each time it has read another 4,096
   catalog entries".
   - Problem: "it" comes before its referent ("a build", later in the sentence), and the
     sentence before it is about the migration's pages, which also read entries. The
     wording is unchanged since review-1, but the paragraph now starts a fresh context, and
     that makes the ambiguity easier to notice.
   - Fix: write "So each time a build has read another 4,096 catalog entries ..., when it
     checks its cancellation and lease, it also reads ...".
5. **OPTIONAL**: `crates/sparrow-server/src/maintenance.rs:148-150`, the `MigrationJournal`
   doc ("a start line ..., progress every few pages, and completion, each timed from the
   start of that first page").
   - Problem: "each" also covers the start line. In `MigrationJournal::page` the start line
     carries no `elapsed_seconds`; only the progress, complete and failed lines do. The
     `interfaces.md` text at 660-675 is correct.
   - Fix: write "progress every few pages and completion, both timed from the start of that
     first page's job".
6. **OPTIONAL**: `.stage/keep-the-source-summary-owed-completion/validation.md:37-38`.
   - Problem: the file does not yet record `hook5.log` (EXIT=0) or `push5.log` (EXIT=0) for
     `3b996eb`. Its own verdict is still marked pending this review.
   - Fix: record the hook and push for the final head when the verdict is updated.

Nothing else in the PR's docs and comments is misplaced, broke surrounding prose, or states
something false. Run order, re-base lead, check clock, journal timing and the
`helpers-ledger.toml` row all still agree with the code, as reviews 1 to 4 established.

VERDICT: BLOCKED
