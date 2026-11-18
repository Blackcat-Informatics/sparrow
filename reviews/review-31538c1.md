# Delivery review: PR #364 (issue #352)

- Head: `31538c1ba680b6584418ab5d47f0bde0fa2f422b` (merge of `origin/main` 49aa9a0 into 0550a8b),
  branch `paudley/352-archive-worker-does-not-check-record`.
- Worktree: `/home/paudley/Active/sparrow/.worktrees/352-archive-worker-does-not-check-record`.
- Diff: `git diff origin/main...HEAD` (merge base = origin/main 49aa9a0), 9 files, +245 -85.
- Reviewed 2026-11-18T06:20Z. Read-only: no builds, no tests run, no edits in the worktree.

## Scope

Issue #352 asks for one decision and one correction: hash each record's bytes against its
link digest before the archive commit, with one decoder shared by worker and reader, and fix
the stale comment in `archive/decode.rs`. The issue has no comments and no rulings. PR #364
claims a way out after a refusal ("the operator clears it and the next pass writes the
segment again") and adds two worker tests.

## What I read

- `stagectl issue 352` (body, 0 comments) and `stagectl issue 364` (body, 0 comments).
- Full diff, and `git show --remerge-diff 31538c1 -- docs/archive-format.md`.
- `crates/sparrow-server/src/archive.rs` 1-1073 (all), `archive/decode.rs` 1-23 (all).
- `crates/sparrow-archive/src/lib.rs` 1-30, `src/decode.rs` 1-170 (all), `Cargo.toml`,
  `tests/archive.rs` 68-115.
- `crates/sparrow-test-support/src/lib.rs` `forged_segment` (new, ~297-376).
- `crates/sparrow-core/src/storage/archive.rs` 120-140, 310-345, 405-470 (gate, `archive_intend`,
  `archive_commit`); `archive_segment.rs` 100-130, 440-600, 600-675 (`hold_reason`,
  `segment_id`, `archive_prepare`, `build_segment`); `archive_encode.rs` 1-172 (all);
  `storage.rs` 124-147, 1139-1177 (`record_bytes`, verified `payload_chunk_view`);
  `rdf_view.rs` 1400-1605 (`with_rdf_scope_ids`, record presence); `rdf_index.rs` 1163-1180
  (every record's metadata graph carries the `Record` type).
- PurRDF 4.0.2 (the locked version): `purrdf-gts` `reader.rs` 1237-1262 (blobs keyed by the
  frame's declared digest), `writer.rs` 1348-1404 (declared digest computed from the raw
  data), `model.rs` 631-640, 850-852 (lazy blob decode caches the decoded bytes).
- `docs/archive-format.md` Archive status (34-53), section 3 (154-176), 7 (278-305),
  8 (307-346), 10 (356-380).
- Hook logs `.stage/352/commit2.log` (tree 0550a8b's parent ee79931 plus the change, EXIT=0,
  both new worker tests and the reader's forged test `ok`) and `.stage/352/sync2.log`
  (merge tree, EXIT=0, same three tests `ok`, 20 `test result: ok` lines each).
- `git grep -i -E 'patrick|ruling|ruled' HEAD`: only `AGENTS.md` and `CITATION.cff` (exit 1
  with those excluded); the diff has no match.
- `prose-check` on the five changed text files and `.stage/352/pr-body.md`: exit 0 for both.

## What I did not read

- `sparrow-core` `select` (archive_segment.rs before 440), `retire.rs`, expiry and eviction
  callers of the gate beyond `Writer::archived`/`require_archived`.
- `sparrow-archive` `extract.rs`, `segment.rs`, `main.rs`.
- `crates/sparrow-core/tests/archive_segment.rs`.
- The sync evidence summary under the worktree's `.stage/.../sync/`.
- The cost measurement itself (none is saved that I found).

## Answers

(1) Watermark over unchecked bytes. The only production caller of `archive_commit` is
`archive_segment` (archive.rs 588), reached only after `write_segment`, `verify_segment`
(read-back byte-equal, then `decode::verify` → `sparrow_archive::verify` → `decode::decode`)
and the overrun check. Every step returns through `?`; `drain` propagates; `run` journals
each failure on stderr and through `tracing::error!` and never commits on an error. No error
is turned into a default on this path. The bytes the encoder links are read through
`payload_chunk_view`, which checks each chunk against its stored BLAKE3 (storage.rs 1170),
and `encode` computes the link digest from those same bytes (archive_encode.rs 136-142), so
the chain store digest → link digest → decoded blob is closed. Residual, not reachable today:
`decode` checks only subjects typed `Record` and its result is discarded, so it does not
reconcile the decoded records with the prepared selection (see O1).

(2) Way out after a refusal: no, for the corruption this check exists to catch (R1).

(3) Tests: crash recovery drives `drain` and `archive_prepare` with the store's `now`
(`LATER`), no sleeps, and proves keep/replace/commit-once/idempotent. The digest test proves
the refusal and the watermark staying, but its recovery leg is not the production path (R1),
and it reaches the "no blob under the link's digest" branch rather than the hash comparison
(O2).

(4) Cost is bounded per segment: the prepared file, plus the reader's copy of the raw blobs,
replaced on access by their decoded bytes, plus a clone of each record's decoded bytes kept in
`Decoded` until `verify` drops it, so about the file plus twice the decoded payload of one
segment (segment cap `ARCHIVE_MAX_SEGMENT_BYTES` 256 MiB, record hold bound 128 MiB). Nothing
accumulates across segments. The PR scopes its 14.0 s / +115 MB figure honestly as one
unrepeated measurement on a non-production store; it does not state the bound (O3).

(5) Merge: the conflict in section 8 keeps main's wording ("equals the prepared bytes",
"fails with a verify error") and the branch's added sentences; nothing from either side is
lost. No attributed rulings or quotations.

## Findings

**R1 REQUIRED: after a digest refusal from the real path the store has no way out, and the
digest test's recovery leg proves a state production cannot reach.**
`build_segment` hashes the encoded file into the segment identity and records it as the
durable intent (archive_segment.rs 654-662), so the intent always names the bytes the worker
is handed. A link/blob mismatch comes from a fault inside the encoder or the GTS writer, before
`build_segment` returns, so the in-flight intent names the refused bytes. Then:
- If the fault is deterministic, every pass rebuilds the same bytes with the same identity, and
  `verify` refuses them again. Clearing the file only gets the same bytes written again.
- If the fault clears, or the code is fixed or PurRDF is repinned, the rebuild has a different
  identity. `archive_intend` refuses it with `CONFLICT` ("another archive segment is in
  flight; commit it first", archive.rs 432-437), and `archive_prepare` returns that error
  because halving is skipped when an intent is in flight (archive_segment.rs 553).
Nothing deletes `archive:intent` except `archive_commit` with the in-flight identity
(archive.rs 467), which needs a verify the refused bytes can never pass. No public method or
operator command abandons an intent. The watermark is frozen for good: nothing is lost, but
expiry stops, and under pressure ingest is refused with `STORAGE_PRESSURE`. The journal then
says "commit it first", which an operator cannot do. Section 3 of the format already says
such passes fail "until an operator resolves it", but nothing lets an operator resolve it.
The new section 8 sentence ("the operator clears it before the worker writes the segment
again") and the PR body promise a recovery that does not happen.
The test hides this. `archiving_store` records the intent from the genuine bytes. The test
then swaps forged bytes into the `PreparedSegment` while keeping the genuine `segment_id`,
and recovers by calling `archive_segment` with the genuine bytes, bypassing `archive_prepare`.
That covers only memory corruption after `build_segment`.
Correction: give a refused in-flight segment an exit. For example, a store method that
releases the intent naming a given identity, called by the worker on a decode refusal, so the
next prepare records a fresh intent over the same range under the existing fence and count
checks. Another option is an `archive_intend` rule that replaces an uncommitted intent with a
rebuilt identity over the identical range and counts. Prove it through `drain` and
`archive_prepare` with an intent whose identity names the refused bytes, and make the journal
line and section 8 name the real recovery step. If this exit is judged separate work, that
is a scope cut for `lillith_coordination` or Patrick to rule on. Until then the PR text and
section 8 must not claim recovery.

**O1 OPTIONAL: reconcile decoded records with the selection.** `sparrow_archive::verify`
discards `Decoded`, so a segment missing a covered record, or with a record subject that lacks
its `Record` type quad, would pass and the watermark would cover it. Today every record's
metadata graph carries the type (rdf_index.rs 1163-1180) and every encoded record gets a link,
so I found no reachable path. Comparing the decoded record count with the encoded count would
close it.

**O2 OPTIONAL: the hash-comparison branch has no test.** PurRDF keys a blob by the digest the
frame declares, which its writer computes from the raw data. `forged_segment` therefore stores
`actual` under `digest(actual)`, and the lookup by `digest(claimed)` fails with "record
record:bb has no blob". The `digest_string(&bytes) != digest` line in `decode.rs` 112 is never
reached, so the case the issue names (a transformed blob whose decoded bytes differ from its
declared digest) is not exercised. The test assertion `contains("does not decode: record
record:bb")` accepts both messages. The behaviour the test names (refused before the commit)
does hold. This gap predates the PR (#327 fixture).

**O3 OPTIONAL: state the memory bound and halve it.** Peak is about twice one segment's
decoded payload, because `decode` keeps every record's cloned bytes for a caller that drops
them. A verify path that borrows through `blob_bytes` and keeps nothing would halve it. The PR
could also state the bound alongside its one measurement.

**O4 OPTIONAL: Archive status goes stale on merge.** `docs/archive-format.md` line 47 says
running the worker on the live daemon is "behind issue 352 and an authorized image switch".
The PR closes #352 but leaves that sentence unchanged.

**O5 OPTIONAL: typed error.** `pub fn sparrow_archive::verify` returns `Result<(), String>`.
The crate already has `ArchiveError`, and AGENTS.md asks for typed, matchable library errors.
The worker wraps the string in `ArchiveError::Verify`, so nothing is lost today.

VERDICT: BLOCKED
