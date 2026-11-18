# Issue #352: Archive worker does not check record bytes against link digests before commit

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

The archive worker's pre-commit check does not compare any record's bytes with its link digest, so a blob corrupted before the write is committed and caught only offline.

Evidence: `crates/sparrow-server/src/archive/decode.rs::verify` (called from `archive.rs::verify_segment`) requires one `.gts` file and no GTS reader diagnostic, and its comment says the reader checks every blob digest, but PurRDF documents that the digest of a transformed blob cannot be checked there. The check that hashes each record's bytes against its `payloadBlake3` link is `crates/sparrow-archive/src/decode.rs::decode`, which `sparrow-archive verify` and `extract` run; `sparrow-server` does not depend on `sparrow-archive`. Review: `.stage/archive/review-324b.md` finding 1.

Does the reader run before evidence can be retired? No. The deletion gate is a position comparison only (`Writer::archived` and `require_archived` in `crates/sparrow-core/src/storage/archive.rs` compare the eviction position with `archive:through`), and `archive_commit` advances the watermark after the worker's check. Nothing runs `sparrow-archive` before retirement, so a corrupt blob is found at `extract` time, after the original may be gone. Acceptance: decide whether the worker should hash each record's bytes against its link before the commit (one decoder shared by worker and reader), and correct the stale comment in `archive/decode.rs`.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
