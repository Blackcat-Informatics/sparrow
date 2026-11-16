# Delivery review: #327 sparrow-archive extract, remove restore

Head reviewed: `86d15f1172780fd88f97d341f126f5d139b70076` on `paudley/327-sparrow-archive-extract-take-one`.
Base: `70d2f63c2c1731eafa9df3d9b684fe0583d4d683`.
Worktree: `/home/paudley/Active/sparrow/.worktrees/327-sparrow-archive-extract-take-one`.
Reviewer: independent and read-only. No cargo, no builds, no edits to the tree.

## Scope

Read:
- `git diff 70d2f63 86d15f1`, every file: `crates/sparrow-archive/src/{decode,error,extract,lib,main}.rs`, removed `restore.rs`, `tests/archive.rs`, `crates/sparrow-core/src/storage.rs`, `storage/ingest.rs`, `docs/archive-format.md`.
- Unchanged context: `crates/sparrow-archive/src/segment.rs`, `decode.rs` (whole), `Cargo.toml` of the crate, `sparrow-core/src/storage/archive_encode.rs` (lines 1-140), `rdf_index.rs` artifact graph (byteLength, payload), `instant.rs::parse_instant`, `docs/archive-format.md` sections 9 and 10.
- PurRDF at the locked version (`purrdf-gts 4.0.2`, registry source): `reader.rs` `h_blob_frame`, `blob_digest`, `emit_blob`, frame self-hash and `prev` check, `ReadOptions`; `model.rs` `BlobEntry::decode`, `blob_bytes(_cloned)`; `writer.rs` `add_blob_transformed`, `blob_pub_meta`, `digest_string`.
- Issue text: `.stage/sparrow-archive-extract-take-one/issue.md` (#327), plus the coordinator's relayed rulings in the review brief.
- Evidence: `logs/commit2.log` (hook run), `logs/test-archive.log`.
- Whole-repository `git grep` at the head for `restore_artifact`, `Origin::Restore`, `ingest::Restore`, `RestoreReport`, `sparrow_archive::restore`, `reimports_byte_exact`, `.restore()` and every `restor*` under crates, docs, deploy, registry and `helpers-ledger.toml`.
- `prose-check` on the new section 10 paragraph: exit 0, no findings.

Not read: `plan.md` and `validation.md` (no such files under the stage directory; the brief and the logs stand in), the rest of `commit2.log` beyond the archive tests, header and exit lines, `sparrow-core` tests, `sparrow-server`, and PurRDF's codec internals beyond `decode_chain`. No test or binary was run by me.

## Evidence

- `logs/commit2.log`: the pre-commit `make check` run over the staged tree (parent `70d2f63`), resulting commit `86d15f1`, `EXIT=0`. It includes `cargo fmt --check`, release Clippy with `-D warnings`, and the release `tests/archive.rs` run with all 6 tests passing (`extract_*` five, `verify_reports_*` one), and the census gates.
- `logs/test-archive.log`: debug run of `-p sparrow-archive` before commit, 6 passed, `EXIT=0`.
- Tests drive the real binary (`CARGO_BIN_EXE_sparrow-archive`) on segments produced by a real `Store` through `archive_prepare`/`archive_commit` and the one segment encoder. Each test names its behaviour. Good.

Confirmed by reading:
- Restore removal is complete and was used by nothing else. The only remaining `restore` symbol is `storage/backup.rs::restore` (RocksDB backup, re-exported as `restore_backup`), unrelated. No docs, deploy, ledger or registry text referred to the removed items.
- Normal ingest identity generation is unchanged: the removed branch was `match origin.restore() { Some(..) => .., None => random_id("artifact")? }` and the per-record equivalent; the head keeps exactly the `None` arms. The restore-only count and conflict checks ran only on `Some`. `ingest.rs:549` (`matches!(origin, Origin::Ordinary)`) is untouched.
- Stored format unchanged: the encoder and `rdf_index.rs` are not in the diff.
- `byteLength` in the new paragraph is accurate: it is the archived predicate (`urn:sparrow:vocab:byteLength`, written by `rdf_index.rs`, read by `decode.rs`). `list` emits it as `byte_length`, and the paragraph says so. `received_at` is Unix milliseconds (`parse_instant`). Accurate.
- Archive-side failures never touch stdout: `take` builds the whole `Vec<u8>` before locking stdout. Unknown id is `NotFound` naming the id; missing metadata, a gap or a short artifact is `Incomplete` naming the artifact; a damaged file is `Corrupt` naming the path. All exit 1. A failed stdout write exits 2 and may leave partial bytes in the destination, which is unavoidable for a stream and is not covered by the "nothing on stdout" promise (that promise is about archive failures, and the doc says exactly that).
- Spanning: `extract` scans every segment for records and for the artifact metadata, which may sit in a segment holding none of its records (section 9). Correct.

## Findings

### 1. REQUIRED: the bytes are not checked against any digest the archive carries, and four texts say they are

The ruling requires `extract` to check the bytes against the digests the archive already carries. The new text claims this happens:
- `extract.rs` doc: "Each record's bytes were already checked by the decoder against the digest the archive carries for them (the blob digest named by its link quad)."
- `docs/archive-format.md` new paragraph: "only after every segment decoded with its blob digests checked".
- `main.rs`: "`extract` prints one artifact's bytes, and only after they are verified"; `take`: "checked".
- (pre-existing, now relied on) `decode.rs`: "The reader checks the frame hash chain and every blob digest".

What the code does at the locked PurRDF (`purrdf-gts 4.0.2`): Sparrow's encoder writes each record with `add_blob_transformed(.., ["zstd" | "zstd-rsyncable"], ..)`. That call always writes a declared `pub.digest` (BLAKE3 of the plaintext; `writer.rs` `blob_pub_meta`). In `reader.rs` `h_blob_frame`, a frame with a declared digest takes the `if let Some(digest) = declared_digest { .. return; }` branch, storing the compressed bytes as `BlobEntry::lazy` under the declared digest without hashing anything (the sink flag it passes is `digest_computed: false`). Later, `decode.rs` calls `blob_bytes_cloned(&digest)`, which only runs `decode_chain` (decompress). Nothing compares the decompressed record bytes with the `payloadBlake3` link digest or the `payloadSha256` link literal (`decode.rs` ignores `payloadSha256` entirely). `extract.rs` checks only offsets and `byte_length`.

What is checked: each frame's self-hash (canonical content ID including its `d` and `pub`) and the `prev` chain. That catches on-disk damage to the compressed frame, which is why the flipped-byte tests pass. It does not verify the decompressed output against the digest, so the claim "blob digests checked" is false. The issue's acceptance also asks for a test that a wrong digest exits nonzero with nothing on stdout, and there is none.

Correction: perform the check on the reader side (no stored-format change). In `decode.rs`, after `blob_bytes_cloned`, compare `purrdf::gts::writer::digest_string(&bytes)` with the link's `blake3:<hex>`, or compare the SHA-256 against the `payloadSha256` link literal, and refuse the segment by name on a mismatch. BLAKE3 costs little next to zstd decoding. Add one test that drives the binary on a segment whose blob digest does not match its bytes. Correcting the four texts to say "frame self-hashes checked" instead would leave the ruling's digest check undelivered.

### 2. REQUIRED: section 9 still says a reader checks the rebuilt artifact against its BLAKE3 `payload`

`docs/archive-format.md` line 176: "A reader rebuilds an artifact by joining its records' blobs in source order. It checks the result against the artifact's `byteLength` and its BLAKE3 `payload`." `extract` is now the one shipped reader that rebuilds an artifact, and the operator ruled the BLAKE3 equality out. It checks `byteLength` only. Before this change the removed restore path did re-derive `payload_id` (BLAKE3) on ingest, and its test asserted equality, so this sentence was true then and is false now. Correction: drop the BLAKE3 clause, or say it is not checked.

### 3. OPTIONAL: an unrelated damaged segment blocks every extraction; the doc does not say how to work around it

`extract` decodes every segment and fails on the first corrupt one, even when that segment holds nothing of the artifact. This is the documented design and matches the ruling ("a damaged archive is a named error"). But one bad file among thousands makes every artifact unextractable until the person points `DIR` at a directory without it. Nothing is lost, since `DIR` is any directory. One clause in the usage paragraph would let a person recover without reading code.

### 4. OPTIONAL: cost of `extract` and `list`

- `extract` reads and fully decodes every segment under `DIR`. That decompresses and clones every record's blob (`decode.rs` calls `blob_bytes_cloned` for every record) and keeps only the target's. Time is O(archive bytes) over the NAS for each extract. Peak memory is about one segment file plus its decoded records plus the target. This is bounded but slow on a large archive. Skipping blob materialization for records of other artifacts would cut most of the cost without weakening the corruption check (the frame self-hash runs in `read` regardless).
- `list` now emits every artifact of a segment on its one JSON line: at the 10,000-record default, roughly a megabyte per line at most. This is bounded per segment and acceptable. Its decode cost is unchanged from before.

### 5. OPTIONAL: duplicate records are kept silently

`records.dedup_by(|a, b| a.id == b.id)` keeps the first copy of a duplicated record (section 3 says duplicates can appear after a PurRDF revision change). Section 10 says readers "report duplicates rather than merging conflicting content". When conflicting copies hold the same offsets, the extraction keeps one and reports nothing. The removed restore behaved the same way, so this is not a regression.

### 6. OPTIONAL: exit status 2 is shared

A usage error and a failed stdout write both exit 2. Neither is documented in the new paragraph, so nothing in it is false.

### 7. OPTIONAL: pre-existing, outside this change

`docs/archive-format.md` lines 159, 167-171 and 332-335 say blobs are keyed by SHA-256 and readers follow `payloadSha256`. The encoder keys blobs by BLAKE3 and the decoder follows `payloadBlake3`. `docs/open-items.md` A-15 says no `sparrow-archive` tool exists. Neither text was introduced here.

## Verdict

Removing restore is clean, and normal ingest is unchanged. Extraction, spanning, not-found, missing-segment and damaged-segment behaviour is correct and tested through the real binary. Two things block. First, the digest check the ruling requires is not performed, and four texts claim it is. Second, section 9 still claims a BLAKE3 check that the shipped reader does not make.

VERDICT: BLOCKED
