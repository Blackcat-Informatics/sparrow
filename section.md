### Payload id index (store format)

An artifact's `payload_id` is `blake3:` and the 64 lower-case hex digits of the
BLAKE3 hash of all its own bytes. The store indexes every complete artifact by
it, so "does the store already hold this content" is a point read instead of a
page walk over a source's artifacts. One key per artifact in the default family:

```
payload-id:<64 hex digest>:<artifact id>        value: empty
```

The artifact id is part of the key because the same bytes ingested under two
requests make two artifacts, and a prefix scan of one digest returns them all.
An artifact is indexed exactly while it is complete, the completeness the
per-source counters use: its entry exists and neither the entry nor its manifest
retirement state is marked evicted. One owner observes every write batch before
it is written and adds the key changes to the same batch, so no writer can
create, retire or delete an artifact entry without its key, and a crash cannot
leave a catalog row with no key or a key with no row. The `payload-index`
store counter (`store-counter:payload-index`) counts the keys through the same
observer as every other counter, and
`done:payload-index` counts the artifact entries the migration has passed.
The count of keys equals the count of complete artifacts, so a count that
disagrees with the catalog is detectable.

Every entry is removed in the batch that ends its completeness:

- expiry deletes an artifact entry (with the last of its records, or, for a
  recordless artifact, with its time-index key);
- pressure eviction marks an artifact evicted through its manifest retirement
  state, or deletes a recordless artifact entry;
- an import's reclamation deletes only import staging rows and the import
  manifest, never an artifact entry, so it changes no key.

An archived artifact stays indexed until its entry is deleted or retired: the
index describes what the store holds, not what the archive holds.

Which artifacts carry a digest of all their bytes. Inline ingestion hashes the
whole input, and that covers a tail checkpoint (the artifact is exactly its
checkpointed byte range, so the digest is of that range, not of the whole file
that grew past it) and a recordless artifact (zero bytes, the digest of no
bytes). A published import verifies the hash of the whole staged artifact
against the manifest before publishing. Parts of a consume-root set are
separate artifacts, each indexed under its own digest; the digest of the whole
source file is never an artifact's `payload_id`, so a file cut into parts is
found by its parts' digests and its manifest artifact's digest, not by its own.
An evicted artifact, whose bytes are no longer all held, is not indexed. No
other path exists: every `Artifact` entry is created by inline ingestion or by
import publication.

`payloadLookup` (schema 71, additive, ordinal 56) takes a store ID, a payload id,
an optional cursor (the last artifact id of the previous page) and a page size
(1 to 1000, default 200) and returns the artifact ids in order, the next
cursor, and `complete`. It reads one snapshot and takes no writer lock. The CLI
is `payload STORE_ID PAYLOAD_ID [--cursor CURSOR] [--page-size N]` and the
read-only MCP tool is `payload_lookup`; both render `answer_certainty`:
`exact` once the migration is complete (an empty list then means no artifact
holds the digest), `pending` before (every listed artifact is held, and an
unlisted one is unknown, never absent).

A store written before the index is fenced at open with a new writer family
(`sparrow.store/352` to `406`, lifting the store counter families) so an older
binary cannot write or delete an artifact without its key. The open seeds the
`payload-index-upgrade` checkpoint and zero counter rows. The `payload-index`
migration then walks the artifact entries in id order, 1,024 entries at a time,
puts the key of each complete one and advances the checkpoint in one batch
through the ordinary write path, after the same wait for waiting writers as every
migration page. A write during the migration keeps the keys it creates or retires
exact whatever the checkpoint says; the migration puts the key of an entry it
reaches again as a no-op. A restart resumes after the last committed page, and a
failed page is retried, never skipped. `status` reports its done count and
watermark with the other migrations.

