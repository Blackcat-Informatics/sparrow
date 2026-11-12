<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Sparrow archive format: `sparrow-archive/1`

Sparrow keeps a searchable 30-day window of disposable logs. Before it deletes
aged-out evidence, it can write that evidence to a large, slower archive, such
as a NAS. This document defines the archive's on-disk format, the write
protocol, and the rules that keep deletion behind the archive. Tools written
later must be able to read the archive without a running Sparrow.

Patrick's rulings bind this design:

- The tables use PurRDF's Parquet codec (`purrdf::columnar`).
- Sparrow never deletes unarchived evidence, even when the archive is down.
- Archiving starts early, at 75% of the retention window or of the data budget.
  A record counts as archived only once its segment is written and verified.
- The archive root is `/net/synology/archives/sparrow`. The `dir` setting is
  required and has no built-in default.
- An unreachable archive at startup starts the daemon degraded; it does not refuse to start.
- When archiving is enabled on an existing store, the whole retained backlog is
  archived before anything is deleted.
- An entry that cannot be encoded is quarantined, and archiving continues.
- Content projection graphs are archived.
- Sparrow has no configurable per-artifact byte limit (2026-11-01: "Remove the
  limit"). Entries are records. New layouts cap a record at 128 MiB, but stores
  laid out earlier may hold larger raw records, and either can exceed the
  closing target, so a segment must be able to hold an entry larger than its
  target.

Phase status: the specification and the deletion gate with its store fence
(phases P0 and P1) are implemented. Segment encoding (P2), the archive worker
and NAS thread (P3), and the offline reader tools (P4) are not implemented yet.
Until P3 lands, an archiving store retires nothing on its own. Expiry waits, and
under pressure ingest is refused with `STORAGE_PRESSURE`.

## 1. Layout

```text
<root>/<store-hex>/STORE.nq
<root>/<store-hex>/segments/YYYY/MM/DD/<segment-id>/terms.parquet
<root>/<store-hex>/segments/YYYY/MM/DD/<segment-id>/quads.parquet
<root>/<store-hex>/segments/YYYY/MM/DD/<segment-id>/reifiers.parquet
<root>/<store-hex>/segments/YYYY/MM/DD/<segment-id>/annotations.parquet
<root>/<store-hex>/segments/YYYY/MM/DD/<segment-id>/blobs.parquet
<root>/<store-hex>/segments/YYYY/MM/DD/<segment-id>/manifest.nq
<root>/<store-hex>/segments/YYYY/MM/DD/<segment-id>/payloads/<n>.bin
<root>/<store-hex>/quarantine/YYYY/MM/DD/<segment-id>/<n>.bin
<root>/<store-hex>/.staging/<staging-id>/
```

- `<store-hex>` is the hexadecimal part of the store identity `store:<hex>`.
  One root can hold several stores.
- `YYYY/MM/DD` is the UTC receipt date of the segment's first entry.
- `<segment-id>` is the lowercase hexadecimal segment identity (section 3).
- The five table files are exactly the files `purrdf::columnar::write`
  produces, with their PurRDF file names, in columnar schema 1 with Zstandard
  page compression.
- `payloads/` holds the bytes of records too large for the tables (section 2).
  `<n>` is the entry's decimal index in the segment's manifest. Most segments
  have no `payloads/` directory.
- `quarantine/` holds raw bytes of quarantined entries (section 6), grouped by
  the segment that covers them. `<n>` is the entry's decimal index in that
  segment's manifest.
- `.staging/` holds partial writes. Readers ignore it, and the writer removes
  stale staging directories before it starts a new write.

Segments are immutable. Nothing in a committed segment directory is rewritten,
appended to or deleted by Sparrow.

`STORE.nq` binds the directory to one store and is written once, when the
archive is first used:

```text
<urn:sparrow:archive:store:HEX> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:sparrow:vocab:ArchiveStore> .
<urn:sparrow:archive:store:HEX> <urn:sparrow:vocab:storeId> "store:HEX" .
<urn:sparrow:archive:store:HEX> <urn:sparrow:vocab:archiveFormat> "sparrow-archive/1" .
```

Startup compares it with the opened store and refuses a mismatch.

## 2. Segment boundaries

A segment covers one contiguous run of the store's eviction order. That order
merges the record index `time:{received_at:020}:{id}` with the recordless
artifact index `empty-time:{received_at:020}:{id}`. Both share the position
suffix `{received_at:020}:{id}`, which also orders an artifact that has records:
its position is `{received_at:020}:artifact:<hex>`.

A segment closes at the first of these limits:

| Limit | Default | Bounds |
| --- | --- | --- |
| Raw payload bytes | 64 MiB (`max_segment_bytes`) | At least 1 MiB, at most 192 MiB. |
| Eviction entries | 50,000 (`max_segment_records`) | At least 1. |
| UTC day | End of the first entry's receipt day | Fixed. |
| Horizon | Section 5 | Fixed. |

`max_segment_bytes` is a closing target, not a capacity. A segment closes before
an entry whose record bytes would take its payload past the target, unless the
segment is still empty. An entry whose bytes alone exceed the target therefore
forms a segment of its own, and the segment is never split inside a record.

Such an oversized record's bytes do not go into `blobs.parquet`. The writer
streams them from the store, one 64 KiB chunk at a time, into
`payloads/<n>.bin`, hashing as it writes. The record's metadata graph, its
projection graphs and its link quad go into the tables as for any other record.
The manifest lists the file as a `PayloadFile` entry (section 9), and the
segment identity covers its digest (section 3). The tables then hold only
metadata for that entry, so the encoder's memory stays bounded by the segment
target, and the payload file costs one chunk of memory at any record size. A
record within the target is always a blob.

The 192 MiB ceiling keeps one table below the codec's 256 MiB table bound. It
will be raised after the PurRDF upgrade that streams and compresses Parquet
optimally; Sparrow adds no local workarounds for the codec's size or memory
limits. Encoding needs memory of roughly three to four times the segment size.
That figure is an estimate to measure in P2.

## 3. Segment identity

```text
segment-id = BLAKE3( frame("sparrow-archive/1")
                   ‖ frame(store-id)
                   ‖ frame(first-position) ‖ frame(last-position)
                   ‖ frame(BLAKE3(terms.parquet)) ‖ frame(BLAKE3(quads.parquet))
                   ‖ frame(BLAKE3(reifiers.parquet)) ‖ frame(BLAKE3(annotations.parquet))
                   ‖ frame(BLAKE3(blobs.parquet))
                   ‖ frame(u64_le(quarantine-count))
                   ‖ frame(BLAKE3(0.bin)) ‖ …
                   ‖ frame(u64_le(payload-count))
                   ‖ frame(BLAKE3(payloads/<n>.bin)) ‖ … )
```

`frame` is PurRDF's `purrdf_hash::frame::frame_le`: an eight-byte little-endian
length followed by the bytes. Digests are the raw 32 bytes. Quarantine and
payload files appear in ascending entry index. The PurRDF codec is
byte-deterministic, so the same range of the same store, encoded by the same
PurRDF revision, always gives the same identity. A restored store backup
therefore rebuilds and adopts its existing segments. A different PurRDF revision
can give a different identity for the same range. Readers then see duplicates
and remove them by record IRI (section 8).

## 4. Content

A segment covers every eviction position after the previous watermark, up to and
including its own last position. The first segment after archiving is enabled
has no previous watermark, so it covers every retained position up to its last.
That range can include an artifact's position even when the artifact is not
itself an entry, because an artifact's position sorts before every record
received in the same millisecond. A segment carries every published entry
in its range, with its dependencies:

| Content | Where |
| --- | --- |
| Record bytes `[byte_start, byte_end)` | One blob per record in `blobs.parquet`, keyed by lowercase hexadecimal SHA-256. A record's chunks are joined in order. A record larger than `max_segment_bytes` is a payload file instead (section 2). |
| Record metadata graph `urn:sparrow:graph:record:<hex>` | `quads.parquet`, including its provenance and tracing facts. |
| Diagnostic graph | `quads.parquet`, under its recorded graph IRI. |
| Artifact graph `urn:sparrow:graph:artifact:<hex>` | `quads.parquet` of the segment whose covered range contains the artifact's position. When artifacts share a millisecond, that can be an earlier segment that holds none of the artifact's records. The gate treats the artifact as archived once that segment commits. |
| Content projection graphs | `quads.parquet`, under each projection's graph IRI. A projection shared by records in several segments appears in each of them. |
| Link quads | One per record with bytes, in the graph `urn:sparrow:graph:archive-links`. |
| Reifiers and annotations of these graphs | `reifiers.parquet` and `annotations.parquet`. |

A link quad ties a record to its blob or payload file. Both use the same
predicate and the SHA-256 of the record's bytes:

```text
<urn:sparrow:record:HEX> <urn:sparrow:vocab:payloadSha256> "64 hex digits" <urn:sparrow:graph:archive-links> .
```

Artifact bytes are not copied separately. Every byte of an artifact with records
belongs to one of its records, and a recordless artifact has no bytes. A reader
rebuilds an artifact by joining its records' blobs in source order. It checks
the result against the artifact's `byteLength` and its BLAKE3 `payload`.

An entry already retired before the store was fenced still has its catalog row
until ordinary cleanup removes it. Its metadata is archived. Its missing bytes
or projection go through the quarantine path, with the reason `retired`.

The archive leaves out text postings, term dictionaries, tail ranges, pins,
request and retry rows, journals, import staging and catalog JSON. These are
derived or operational state. Readers rebuild indexes from the archived graphs.

## 5. Archiving schedule

- **By age**: an entry is due once `now ≥ received_at + start_fraction ·
  (expires_at − received_at)`. With the defaults, that is day 22.5 of 30.
- **By size**: everything is due, oldest first, once
  `linked_physical_bytes ≥ start_fraction · (disk_budget − maintenance_reserve)`,
  or whenever pressure maintenance has been requested.
- **Backlog**: an existing store that turns archiving on is fenced with nothing
  archived. Its whole retained backlog is archived, oldest first, before any of
  it is deleted.
- **Horizon**: no segment extends past the earliest of
  `now − max(10 minutes, buffered_flush_interval)`, the receipt time of the
  oldest open import preparation, and the oldest unsynchronized buffered entry.
  A publication behind the horizon is refused as a clock regression
  (section 7), so the horizon must trail every publication still in flight.

Pins do not affect archiving. A pinned entry is archived in order like any
other entry; its pin only delays deletion.

## 6. Quarantine

An entry the encoder cannot represent is quarantined, so it does not block
retention. This covers metadata the codec rejects, a projection that failed
during ingestion, and bytes retired before the fence. The rest of the segment
is encoded normally:

- The entry's available raw bytes go to `quarantine/…/<segment-id>/<n>.bin`
  without any graph.
- The segment manifest records a `QuarantineEntry` with the evidence IRI, the
  reason, the file's BLAKE3 and SHA-256 digests and its length.
- The segment still covers the entry's position, so the watermark stays
  contiguous.
- The store keeps a bounded quarantine listing, which status and an operator
  listing operation report (P3). Quarantine is visible, never silent.

Reasons are `encode` (the codec refused the entry), `projection` (a recorded
projection failure left part of its content unrepresented) and `retired` (its
bytes or projection were retired before the store was fenced).

## 7. Deletion gate and store fence

The store records its progress in two catalog keys, loaded when the store opens:

| Key | Contents |
| --- | --- |
| `archive:through` | Version byte 1, then the last archived eviction position. An empty position means nothing is archived yet. |
| `archive:intent` | Version byte 1, the 32-byte segment identity, the first and last positions (each behind a one-byte length), then record and recordless-artifact counts as eight-byte big-endian integers. |

One gate decides every deletion. A record or artifact may retire only when its
position is at or before `archive:through`. Expiry checks the gate before it
retires anything. It stops at the first due entry the watermark does not cover,
with the frontier `ArchiveLag` and `ExpiryReport.archive_waiting` of 1, because
the watermark is a prefix of the receipt order and nothing after that entry is
covered either. Ordinary catalog cleanup at `expires_at + 1 day`
follows retirement, so the gate covers it too. Pressure eviction stops at the
first unarchived, unprotected entry with the frontier `ArchiveLag`, and keeps
that entry as its inclusive resume position. Retirement asserts the gate
itself. Reaching unarchived evidence there is a `STORAGE_FAILURE` that commits
nothing. Abandoned private import preparation was never published, so it never
enters the gate.

An intent is recorded before the segment is written. Its range must start at
the first entry after the watermark and match the catalog's counts, or it is
refused with `CONFLICT`. A range larger than `max_segment_records` is refused
with `RESOURCE_LIMIT`. An identical retry succeeds, while a different intent
is refused until the one in flight commits. A commit names the segment
identity. It advances the watermark to the intent's last position and deletes
the intent in one synchronized batch. The commit resets the pressure cursor, so
the next pressure pass starts again from the beginning of the eviction order.

A publication batch whose earliest position is at or before the watermark or the
in-flight intent's end is refused with `STORAGE_FAILURE`. The message names
both positions and says to correct the clock. Without this guard, a backwards
clock step could publish evidence the archive would never cover.

Archiving fences the store format with the `sparrow.store/99` to `/109` family, or `/121` to `/131` once the store is also fenced for intake provenance. A store fenced for receipt-time event fallback RDF (schema 69) uses `/154` to `/164` and `/176` to `/186` respectively. A store that also carries the owner-to-graph index uses `/209` to `/219` and `/231` to `/241` respectively.
It sits above every earlier authority family and keeps the underlying authority
through later transitions. It includes the log time law, so stores in this
family always carry that law marker. Binaries without the gate do not know these
identities and refuse to open the store. A fenced store opened without an
archive policy fails with `INVALID_ARGUMENT`; a watermark without its fence, or
a fence without its watermark, fails with `STORAGE_FAILURE`. Removing archiving
from a store is not supported in this version.

## 8. Write protocol

The worker runs every archive operation on one dedicated thread,
`sparrow-archive-nas`, with at most one operation in flight:

1. Write each staged table and payload file into a fresh
   `.staging/<staging-id>/` directory and fsync it. Payload files are streamed
   from the store chunk by chunk.
2. Write `manifest.nq` last and fsync it.
3. Fsync the staging directory.
4. Create the missing parent directories, fsyncing each one and its parent.
5. Rename the staging directory to its final path. An existing non-empty target
   refuses the rename. NFS has no `RENAME_NOREPLACE`, so the check and the
   rename are separate steps under the single-writer rule.
6. Fsync the parent directory.
7. Read every file back past the client cache and check its BLAKE3.

If the target already exists and verifies, the write counts as done. If it
exists and does not verify, the write fails hard and the watermark does not
move. Only a verified segment is committed. A Synology NFS export must use
synchronous writes (`sync`); with `async` exports fsync proves nothing.

Crash recovery follows from the intent:

| Crash point | Recovery |
| --- | --- |
| Before the intent | Nothing to recover. |
| After the intent | Rebuild the identical segment from the intent's range in a new staging directory. |
| After the rename | Verify and adopt the existing segment, then commit. |
| Range content differs from the intent | `STORAGE_FAILURE`; nothing is deleted. |

## 9. Manifest

`manifest.nq` is N-Quads written by PurRDF's N-Quads serializer, in the default
graph. It holds no wall-clock time, so identical inputs give identical bytes.
The segment subject is `urn:sparrow:archive:segment:<segment-id>`. Terms use
the `urn:sparrow:vocab:` namespace, written `sp:` below.

| Subject | Predicate | Object |
| --- | --- | --- |
| Segment | `rdf:type` | `sp:ArchiveSegment` |
| Segment | `sp:archiveFormat` | `"sparrow-archive/1"` |
| Segment | `sp:columnarSchema` | `"1"^^xsd:nonNegativeInteger` |
| Segment | `sp:compression` | `"zstd"` |
| Segment | `sp:storeId` | `"store:<hex>"` |
| Segment | `sp:rdfProfile` | The store's recorded RDF profile identity. |
| Segment | `sp:purrdfRevision` | The full PurRDF commit that encoded the tables. |
| Segment | `sp:firstPosition`, `sp:lastPosition` | Eviction positions, as strings. |
| Segment | `sp:receivedFrom`, `sp:receivedThrough` | `xsd:dateTime` receipt instants of the first and last entries. |
| Segment | `sp:recordCount`, `sp:artifactCount`, `sp:blobCount`, `sp:quarantineCount`, `sp:payloadFileCount` | `xsd:nonNegativeInteger` counts. |
| Segment | `sp:table` | One table IRI, `<segment>#<table name>`, per Parquet file. |
| Table | `rdf:type` | `sp:ArchiveTable` |
| Table | `sp:fileName` | For example `"quads.parquet"`. |
| Table | `sp:blake3` | Lowercase hexadecimal BLAKE3 of the file. |
| Table | `sp:byteLength`, `sp:rowCount` | `xsd:nonNegativeInteger`. |
| Segment | `sp:quarantined` | One entry IRI, `<segment>#quarantine-<n>`, per entry. |
| Entry | `rdf:type` | `sp:QuarantineEntry` |
| Entry | `sp:evidence` | The record or artifact IRI. |
| Entry | `sp:quarantineReason` | `"encode"`, `"projection"` or `"retired"`. |
| Entry | `sp:fileName`, `sp:blake3`, `sp:payloadSha256`, `sp:byteLength` | The quarantine file and its digests. |
| Segment | `sp:payloadFile` | One entry IRI, `<segment>#payload-<n>`, per payload file. |
| Payload entry | `rdf:type` | `sp:PayloadFile` |
| Payload entry | `sp:evidence` | The record IRI. |
| Payload entry | `sp:fileName`, `sp:blake3`, `sp:payloadSha256`, `sp:byteLength` | For example `"payloads/0.bin"`, and its digests and length. |

## 10. Readers

PurRDF reads a segment directly. `purrdf::columnar::read` over the five files
returns a validated dataset and its content store; every blob is checked
against its SHA-256 digest. The result is ready for SPARQL. Readers verify each
file's BLAKE3 against the manifest first. A record whose link digest is not a
blob names a `PayloadFile` entry with the same `sp:payloadSha256`; readers
stream that file and check its SHA-256 rather than loading it whole.

DuckDB can query the tables without Sparrow. Term ids are local to one
segment, so joins stay inside one segment. To find a record's bytes, join
`quads` through `terms` (predicate IRI `urn:sparrow:vocab:payloadSha256`) to
`blobs.digest`. To scan several segments, use
`read_parquet('<root>/<store-hex>/segments/*/*/*/*/quads.parquet', filename = true)`
and join on the segment directory as well as the term id.

Duplicates can appear after a PurRDF revision change (section 3). Readers keep
one copy per record IRI and report duplicates rather than merging conflicting
content. P4 adds an offline `sparrow-archive verify|list <dir>` tool. It checks
`STORE.nq`, manifests, digests and quarantine files, and reports corruption.
Re-importing archived evidence into a store is deferred (decision 6).
