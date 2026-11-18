# Delivery re-review: PR #364 (issue #352)

- Head: `ccdb2e0dc355dcdbd4cd3eacd206b9c958c83cff` (parent 0c9b17c), branch
  `paudley/352-archive-worker-does-not-check-record`. The remote ref equals the head and the
  worktree is clean.
- Worktree: `/home/paudley/Active/sparrow/.worktrees/352-archive-worker-does-not-check-record`.
- Diff judged: `git diff 0c9b17c..ccdb2e0` (2 files, +66 -29), against the optional findings
  O1 to O3 of `review-0c9b17c.md`.
- Reviewed 2026-11-18T14:21Z. Read-only: no builds, no tests run, no edits in the worktree.

## Scope

Narrow recheck of the follow-up commit. It renames a refused file to
`<name>.<segment id hex>.refused`, keeps the decoder's reason if the set-aside fails, syncs the
directory after the renames, adds a hint to the read-back mismatch reason, extends the refusal
test with a second refusal of different bytes, and rewrites sections 3 and 8 of
`docs/archive-format.md`.

## What I read

- `git diff 0c9b17c..ccdb2e0` in full and the commit message of ccdb2e0.
- `crates/sparrow-server/src/archive.rs` 70-160 (`Flight`, `step`, `StepGuard`), 220-420
  (`ArchiveError` and its `Display`, `io_at`, `sync_dir`, `publish`, `write_segment`,
  `verify_segment`, the new `set_aside`), 578-621 (`archive_segment`), and 940-1050 (the
  read-back test and the changed refusal test).
- `crates/sparrow-core/src/storage/archive_segment.rs` 70-95 (`PreparedSegment::id_hex`) and
  440-465 (`segment_id`).
- `docs/archive-format.md` 150-178 (section 3) and 328-350 (section 8).
- `git grep -n '\.refused' ccdb2e0` and `git grep refused -- docs`: there are no stale
  references to the old `<name>.refused` name.
- Hook log `.stage/352/commit6.log`. Line 1 names the tree, line 2 has HEAD 0c9b17c plus the
  staged change, and the reflog shows ccdb2e0 committed directly on 0c9b17c with no amend. The
  log covers fmt, release Clippy with `-D warnings` (workspace and the shipped crates alone),
  `cargo test --locked --release --workspace`, the census self-test and the pinned cross gate.
  It ends `EXIT=0`. There are 20 `test result: ok` lines, no FAILED or panicked line, and no
  result with a non-zero failed count. The relevant tests ran and passed:
  `archive::tests::a_record_whose_bytes_differ_from_its_link_digest_is_refused_before_the_commit`
  (line 832), `archive::tests::the_watermark_stays_when_the_read_back_differs` (838),
  `archive::tests::a_pass_after_a_crash_between_the_rename_and_the_commit_recovers_by_itself`
  (844) and `archive_gate_holds_expiry_until_a_committed_segment_covers_the_evidence` (421).
- `git grep -i -l -E 'patrick|ruling|ruled' ccdb2e0`: only `AGENTS.md` and `CITATION.cff`.
- `prose-check docs/archive-format.md`: exit 0.

## What I did not read

- Everything unchanged since 0c9b17c. The prior reviews' reading stands for `archive_intend`,
  `archive_commit`, `select`, `build_segment`, the decoders and the storage gate test.
- The PR body and the live PR conversation. I did not compare the log's staged tree with ccdb2e0
  byte for byte. I inferred the match from line 2 and the reflog.

## Answers

**O1 (second refusal overwrote the first): fixed.** `set_aside` (archive.rs 398-412) renames
each file to `format!("{name}.{}.refused", segment.id_hex())`. In production `segment_id`
(archive_segment.rs 447-462) is BLAKE3 over the format, store id, range and, for each file,
its name and the BLAKE3 of its bytes. So two refusals of different bytes have different
identities and different aside names, and every refused file of distinct bytes is kept. The
same bytes refused again give the same identity, and the rename replaces an identical copy, so
nothing is lost. The aside names still end in `.refused`, so the reader's `.gts` discovery
ignores them. The test drives a second `archive_segment` with other forged bytes and asserts
that the first aside file still holds the `bravo` bytes and the second holds the `charlie`
bytes. The test fabricates the id `[6; 32]` rather than hashing the bytes, but that does not
weaken the naming property, which comes from `segment_id` in production.

**O2 (no directory sync, and a rename failure dropped the reason): fixed.** `sync_dir(flight,
store_dir)` runs after the rename loop, as the last step of `set_aside`. In `verify_segment`, a
set-aside failure is appended to the decoder's reason (`format!("{reason}; {failure}")`). The
result is still `ArchiveError::Verify`, so the journal keeps both why the segment was refused
and why the move failed. The rename failure text names the source, the target and the
`io::Error`. A sync failure uses `ArchiveError::Io`'s `Display`, which names the step, the
path and the source. No error is swallowed and nothing turns into a default. Each rename now
runs under a `flight.step`, so a stalled rename is reported and counts toward `Overran` like
the other steps. One narrow case remains. If a later rename in a multi-file loop fails, the
earlier renames are not synced. That case is reported in the error, and the current format
has one file per segment, so it changes no behaviour.

**O3 (section 3 overstated, and the operator step was not named): fixed.** Section 3 now limits
"every later pass fails" to a file left in place that the decoder did not refuse, and says it
lasts until an operator moves that file aside, with the journal line naming it. The read-back
mismatch reason now ends "if an earlier pass left a valid file under another identity, move
`<path>` aside", and the `Verify` display puts the path first. So the journal does name the
file and the step, as the documents say.

**Documents against code.** Section 8 matches `set_aside`: the name pattern, kept and never
deleted, no collisions between different bytes, one copy for the same bytes, the watermark and
the intent unchanged, and the manual step for a crash-left valid file. Section 3 matches the
byte-equal check in `verify_segment`.

## Findings

Findings: none. O1, O2 and O3 are closed. Prior O4 (drive the recovery leg through `drain`)
and O5 (no test for the hash-comparison line in `decode.rs`) are unchanged by this commit and
stay optional.

VERDICT: PASS
