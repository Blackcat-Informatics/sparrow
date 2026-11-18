# Delivery re-review: PR #364 (issue #352)

- Head: `0c9b17c548e4a38f307b98289d8e2a1c092367a2` (parent 31538c1), branch
  `paudley/352-archive-worker-does-not-check-record`, pushed (remote ref equals head), worktree clean.
- Worktree: `/home/paudley/Active/sparrow/.worktrees/352-archive-worker-does-not-check-record`.
- Diff judged: `git diff 31538c1..0c9b17c` (4 files, +107 -24), plus the open findings of
  `review-31538c1.md`.
- Reviewed 2026-11-18T13:55Z. Read-only: no builds, no tests run, no edits in the worktree.

## Scope

The prior review was BLOCKED on R1: after a decoder refusal the in-flight intent named the
refused bytes, and a rebuilt segment of another identity was refused with `CONFLICT` forever.
This head adds a replacement rule to `archive_intend` (same first and last positions, after the
unchanged range and count validation), renames a refused file to `<name>.refused`, and updates
the tests, section 3, 7 and 8 of the format document, and the Archive status line (prior O4).

## What I read

- `git diff 31538c1..0c9b17c` in full and the commit message of 0c9b17c.
- `crates/sparrow-core/src/storage/archive.rs` 1-140 (types, gate), 300-484 (`archive_range`,
  `archive_state`, `archive_intend`, `archive_commit`).
- `crates/sparrow-core/src/storage/archive_segment.rs` 225-443 (`select`, the rebuild branch),
  445-463 (`segment_id`), 510-675 (`archive_prepare`, halving guard, `build_segment`).
- `crates/sparrow-server/src/archive.rs` 225-399 (`ArchiveError`, `publish`, `write_segment`,
  `verify_segment` with the rename), 560-717 (`archive_segment`, `drain`, `run`), 719-759 (test
  helpers), 933-1120 (the read-back, digest, crash-recovery and verified-segment tests).
- `crates/sparrow-server/src/archive/decode.rs` (all) and `crates/sparrow-archive/src/decode.rs`
  (all); `crates/sparrow-archive/src/segment.rs` 69-84 (segment discovery by `.gts` extension).
- `crates/sparrow-core/tests/storage.rs` 21380-21510 (the gate test and its new replacement leg).
- `docs/archive-format.md` 150-176 (section 3), 276-305 (section 7), 307-350 (section 8 and the
  crash table).
- Hook log `.stage/352/commit5.log`: tree named on line 1, HEAD 31538c1 plus the staged change,
  fmt, release Clippy, shipped crates, release tests, census self-tests and the pinned cross gate;
  ends `EXIT=0`; 20 `test result: ok` summaries, none with a non-zero failed count. The three
  relevant tests run and pass: `archive_gate_holds_expiry_until_a_committed_segment_covers_the_evidence`
  (line 238), `archive::tests::a_record_whose_bytes_differ_from_its_link_digest_is_refused_before_the_commit`
  (659), `archive::tests::a_pass_after_a_crash_between_the_rename_and_the_commit_recovers_by_itself` (663).
- `git grep -i -l -E 'patrick|ruling|ruled' 0c9b17c`: only `AGENTS.md` and `CITATION.cff`.
- `prose-check` on `docs/archive-format.md`, `.stage/352/msg.txt` and `.stage/352/pr-body.md`:
  exit 0 each. The live PR body (`stagectl issue 364`) matches `pr-body.md`.

## What I did not read

- Unchanged since 31538c1 and not re-read: `sparrow-archive` `lib.rs`, `extract.rs`, `main.rs`,
  `sparrow-test-support` `forged_segment`, `archive_encode.rs`, `storage.rs` chunk verification,
  PurRDF reader and writer sources (the prior review's reading stands for them).
- `crates/sparrow-core/tests/archive_segment.rs`, `retire.rs`, expiry and eviction internals.
- `commit2.log`, `sync2.log`, `commit4.log` (judged in the prior review or superseded).

## Answers

(1) Replacement rule. Safe. `archive_intend` (archive.rs 435-454) accepts a different intent
only when `first` and `last` are equal to the in-flight ones, then runs the same
`archive_range` check as a fresh intent (start at the first entry after the watermark, counts
equal to the catalog, at most `max_segment_records`), and changes the in-memory state only after
the batch commits. A miscounted same-range intent is refused with `CONFLICT` and leaves the old
intent. In production the only caller is `build_segment` (archive_segment.rs 657), and with an
intent in flight `select` rebuilds exactly that range: it starts after the watermark, skips the
closing rules and stops at `intent.last` (292-394). So the rule only fires for a rebuild of the
frozen range. It cannot move the watermark over unchecked bytes. The intent's identity is
`segment_id` over the files just encoded (447-462, 656). `archive_commit` (459-483) commits only
the identity in flight. Its one production caller, `archive_segment` (server archive.rs 599-606),
passes the prepared segment's identity, and only after `write_segment`, a byte-equal read-back and
the decoder have passed on that segment's own files, with every step propagating through `?`.
A replaced identity cannot commit: the storage test asserts `CONFLICT` for the old id (storage.rs
21466-21469). A file already in place under the name that differs from the rebuild fails the
byte-equal check, so a replacement never adopts stale bytes.

(2) Refused file. `verify_segment` (server archive.rs 379-398) renames each file to
`<name>.refused` only on a decoder refusal, after the read-back matched. The file is moved,
not deleted, and the reader ignores it: `segment.rs` 84 lists only `.gts` extensions, and
`<name>.gts.refused` has the extension `refused`. A second refusal collides. `fs::rename` replaces
an existing destination on Linux and over NFS, so the second rename succeeds and silently
overwrites the first `.refused`. If the fault is deterministic, the bytes are identical (same
identity) and no evidence is lost. If two refusals carry different bytes, the earlier refused
bytes are gone, and section 8 says "never deleted" (O1). This is diagnostic evidence, not
stored data: the originals stay unretired, so no store data is lost or stranded. The rename is
also not followed by a directory sync (O2).

(3) Way out, traced. Pass N: `drain` → `archive_prepare` records intent X (hash of the bad
bytes) → `archive_segment` writes and reads back, the decoder refuses, the file moves to
`.refused`, a `Verify` error returns, `run` journals it, and the watermark stays. Pass N+1:
`archive_prepare` sees X in flight → `select` rebuilds the same range → `build_segment` encodes.
- Same bytes (deterministic fault): identity X, an idempotent intent, the freed name is
  rewritten, refused again and renamed over the previous `.refused`. The pass fails every time
  until the encoder or PurRDF changes. This is correct: undecodable bytes must never be archived.
- New bytes (transient fault, encoder fix or PurRDF repin): identity Y, the same-range
  replacement passes `archive_range`, `write_segment` takes the free name, verification passes,
  `archive_commit(Y)` advances the watermark, and expiry resumes.
So R1 is closed: no refused state is left without an exit. One related state still needs an
operator: a valid file is in place from a crash after the rename, and the rebuild then has a new
identity (repin). The replacement succeeds, but the byte-equal check refuses the old file at
the name until an operator moves it. Before this head that state had no exit at all. Now it
has a manual one, which the format document does not name (O3).

(4) Tests. The storage test drives the real `Store` API: replacement, the old identity refused at
commit, a narrower range refused, and an identical retry. The worker test drives the real
`archive_segment` for the refusal, the rename and the recovery commit, and asserts the `.refused`
content, the freed name, the kept intent and the unmoved watermark. Its recovery leg supplies a
fabricated identity `[7; 32]` with a direct `archive_intend` instead of driving `drain` →
`archive_prepare` → `build_segment`. The intent in flight names the genuine bytes, so it inverts
the production state, where the intent names the refused bytes and the rebuild brings the new
identity. The PR body discloses this honestly ("The encoder fault itself is not injected; the
rebuilt identity is supplied"). By reading, the production path reaches the same
`archive_intend` branch the storage test covers, so I do not count this as a test that misstates
what it does. It is O4. The O2 gap from the prior review is disclosed in both places: the test
doc comment says the mismatch "surfaces as a record with no blob under its link's digest, not as a
hash comparison", and the PR body's "Not shown" says the hash-comparison branch is not reached.

(5) Documents. Section 7 matches `archive_intend` (same range replaces after the same checks, a
different range is `CONFLICT`, identical retry idempotent). The crash table row matches the code.
Section 8's refusal paragraph matches the rename and the rebuild behaviour, except for "never
deleted" on a second refusal (O1). In section 3, "Every later pass then fails until an
operator resolves it" reads as if it covers both the replacement case and the file-in-place
case. It is true only for the second (O3). The Archive status line no longer cites issue 352
(prior O4 closed).

## Findings

Prior R1: closed (answer 3).

**O1 OPTIONAL: a second refusal overwrites the earlier `.refused` file.** `fs::rename(&path,
&aside)` (server archive.rs 386) replaces an existing `<name>.refused`. When the two refusals
carry different bytes, the earlier evidence is lost, and section 8's "kept for inspection,
never deleted" is not true then. Identical refusals lose nothing. Either refuse to overwrite
(for example, check first or add a suffix naming the identity) or state in section 8 that the
latest refusal replaces the previous one.

**O2 OPTIONAL: the set-aside rename is not made durable, and its failure drops the decode
reason.** No `sync_dir` follows the rename. After a crash in that window, the name can still
hold the refused bytes. A same-identity rebuild then renames them again, but a new-identity
rebuild hits the byte-equal check, which leads to the manual state in O3. If the rename itself
fails, the returned `Io` error ("set a refused segment aside") replaces the decoder's reason, so
the journal never says why the segment was refused.

**O3 OPTIONAL: name the operator step and narrow section 3's sentence.** A file left in place
by a crash, followed by a rebuild of another identity, fails every pass until someone moves that
file aside. Neither the journal line ("read-back of ... contents differ") nor section 3 says so.
Section 3's "Every later pass then fails" should apply only to the file-in-place case.

**O4 OPTIONAL: drive the recovery through `drain`.** Record an intent of another identity over
the same range with `archive_intend` (now permitted), then call `drain(&flight(), &store, &root,
"s", LATER)` and assert one commit. That covers `archive_prepare` → `build_segment` → replacement
→ commit, in the production orientation (the in-flight intent does not name the rebuilt bytes).
It would replace the manual `PreparedSegment { segment_id: [7; 32], .. }` leg, whose identity
does not hash its own bytes.

**O5 OPTIONAL (carried, unchanged): the hash-comparison line `decode.rs` 112 has no test.** It is
now disclosed in the test comment and the PR body. Prior O1 (reconcile decoded records with the
selection), O3 (halve the verify memory) and O5 (typed error from `sparrow_archive::verify`) are
also unchanged and stay optional.

VERDICT: PASS
