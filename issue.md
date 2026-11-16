# Issue #323: Index artifacts by payload id: the store cannot answer whether it already holds a content

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Problem

The store cannot answer "do I already hold this content". An artifact's `payload_id` is `blake3:` plus the hash of its bytes, but nothing indexes it: the `payload` column family is chunk-digest ownership, the artifact id derives from the request id, and no listing carries a payload id to match.

On 2026-11-15 the shipper re-queued 1,928 duplicate items (held aside with a manifest of SHA-256 digests, 128 MB). Proving that each earlier ship was consumed had no cheap read; the only option was a page walk over a source's artifacts (34,915 shipped files for one source).

## Design (the operator's: an index of held BLAKE3 ids)

An index from payload id to the artifacts that hold it, maintained at ingest, so "do I already hold this content" is a point read.

- **Key and value.** One key per artifact in the default family: `payload-id:` + the 64-hex BLAKE3 digest + `:` + the artifact id. The value is empty. The prefix scan for a digest returns every artifact holding that content (duplicates are expected, which is why the artifact id is part of the key).
- **Ingest.** The key is written in the same atomic batch that writes the artifact catalog row, so the index is exact and crash-consistent with it. The `payload_id` is already computed in that path (`storage.rs`, `format!("blake3:...")`).
- **Age-out.** Whatever deletes or retires an artifact catalog row in a batch (expiry, pressure eviction, retirement) deletes its index key in the same batch. An archived artifact stays indexed until its row is deleted; the index describes what the store holds, not what the archive holds.
- **Records already in the store.** An online migration, like the others (`store-counters`, `source-summary`): the open seeds a checkpoint, the maintenance worker runs bounded pages that write the keys and the checkpoint atomically through the ordinary write path, each page waits for waiting writers (`lock_page`), and a restart resumes after the last committed page. Until it completes, the lookup says it is incomplete (explicit pending state, never a silent "absent"); `status` reports its progress. Nothing is perturbed beyond a migration page.
- **Counters.** A `store-counter:` row for the index, kept by the one batch observer, so a count that disagrees with the artifact count is detectable.
- **Wire.** One new read method after the current last ordinal (additive): payload digest in; page of artifact ids out, bounded by a page size, exact when the migration is complete.

## What it makes cheap

- The check of the 1,928 held items: BLAKE3 of 128 MB of local files, then 1,928 point reads, no store-wide walk.
- Refusing a re-ship before it is ingested: the consume path (or the shipper through the read) can ask whether the digest is already held.

## Open and unmeasured

- Cost per artifact in bytes and in ingest time: NOT measured yet. By construction the key is about 11 + 64 + 1 + the artifact id length (about 41 for `artifact:` + 32 hex) in bytes before RocksDB overhead, one key per artifact (not per record). Acceptance below requires measuring both on a scratch store before this merges; no figure here is a measurement.
- Which ingest paths produce an artifact whose `payload_id` covers all its bytes (tail checkpoints, imports in parts, recordless artifacts) must be confirmed from the code before the key is defined for them.
- This is a stored-format addition. The operator approved the index in principle (via coordination, 2026-11-15); the exact format needs the ruling recorded before code is written, per AGENTS.md.

## Proposed acceptance

- The design above, with the open items closed by reading the ingest paths.
- Measurement on a scratch store (never the live store): bytes per artifact and ingest time with and without the index, at least 100,000 artifacts, release build, quiet host (load recorded).
- Online migration: builds the index for an existing store while ingest runs; ingest stall measured under it; restart mid-migration resumes.
- Tests: the same bytes ingested under two requests are both found; an absent digest gives an exact empty answer once complete and a pending answer before; age-out removes the key.
- Docs: architecture and interfaces describe the index, the migration and the method.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
