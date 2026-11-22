<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Sparrow archive format: `sparrow-archive/1`

Sparrow keeps disposable logs searchable until their 30-day deadline. Before it deletes
aged-out evidence, it can write that evidence to a large, slower archive, such
as a NAS. This document defines the archive's on-disk format, the write
protocol, and the rules that keep deletion behind the archive. Tools written
later must be able to read the archive without a running Sparrow.

These requirements bind this design:

- Segments are GTS files, written through PurRDF's GTS writer.
- Sparrow never deletes unarchived evidence, even when the archive is down.
- Archiving starts early, at 75% of the retention window or of the data budget.
  A record counts as archived only once its segment is written and verified.
- The archive directory is set by `[archive] dir`, which is required and has no built-in
  default; typically a network share such as `/mnt/archive/sparrow-archive`.
- An unreachable archive at startup starts the daemon degraded; it does not refuse to start.
- When archiving is enabled on an existing store, the whole retained backlog is
  archived before anything is deleted.
- An entry that cannot be archived is held, never skipped or deleted: archiving stops
  before it and reports it (section 6).
- Content projection graphs are archived.
- Sparrow has no configurable per-artifact byte limit. Entries are records. New layouts cap a record at 128 MiB, but stores
  laid out earlier may hold larger raw records, and either can exceed the
  closing target, so a segment must be able to hold an entry larger than its
  target.

## Archive status

Configure `[archive]` to start the archive worker on a writable daemon.
The worker encodes and verifies GTS segments before advancing the store's
archive watermark. A new store without archiving retains unarchived
evidence past its deadline. Ingestion can receive `STORAGE_PRESSURE`
when the budget fills. A store that has adopted an archive policy requires
compatible archive configuration at the next writable open.

Status reports the worker phase, watermark and last failure. The offline
`sparrow-archive` command lists, verifies and extracts from GTS files.
[Write protocol](#8-write-protocol) · [Offline commands](#10-readers)

## 1. Layout

```text
<root>/STORE_ID
<root>/<store-id>/<first-position>-<last-position>.gts
```

- `<root>` is the `[archive] dir`. It must already exist, be on its own mount and be empty
  (apart from a leftover `STORE_ID.partial`) before the first segment, or already hold this
  store's `STORE_ID`. The worker creates nothing else in `<root>`.
- `STORE_ID` holds the store identity, `store:<hex>`, and a newline. The worker writes it
  once, on the first pass into an empty root, through a `STORE_ID.partial` that is synced
  and renamed. It is never overwritten: a root whose `STORE_ID` names another store, or
  that holds other entries but no `STORE_ID`, is refused and not adopted. One root holds one
  store.
- `<store-id>` is the whole store identity including its `store:` prefix, so the directory
  is named `store:<hex>`. The worker creates it on the first segment.
- A segment is one file, `<first-position>-<last-position>.gts`, directly in the store
  directory. The positions are the eviction positions of the segment's first and last
  entries (section 2), `{received_at:020}:{id}` or `{received_at:020}:artifact:<hex>`, so
  the files sort in eviction order. There are no date directories, no subdirectories and no
  separate manifest.
- A segment file is written by PurRDF's GTS writer (profile `sparrow-archive`). The encoder
  is the one module that knows its layout; the offline reader has one decoder module.
- A file being written has the suffix `.partial` until it is synced and renamed into
  place. A stale `.partial` from a crashed pass is truncated by the next write of the same
  name. Readers ignore it, because they read only `*.gts`.

Segments are immutable. Sparrow never rewrites, appends to or deletes a committed segment
file. A file already at its name when a pass writes it is a retry of an earlier pass: it is
kept and checked, not overwritten (section 8).

## 2. Segment boundaries

A segment covers one contiguous run of the store's eviction order. That order
merges the record index `time:{received_at:020}:{id}` with the recordless
artifact index `empty-time:{received_at:020}:{id}`. Both share the position
suffix `{received_at:020}:{id}`, which also orders an artifact that has records:
its position is `{received_at:020}:artifact:<hex>`.

A segment closes at the first of these limits:

| Limit | Default | Bounds |
| --- | --- | --- |
| Raw payload bytes | 64 MiB (`max_segment_bytes`) | At least 1 MiB, at most 256 MiB. |
| Eviction entries | 10,000 (`max_segment_records`) | At least 1. |
| Quads | 3,000,000 (`max_segment_quads`) | At least 1. |
| UTC day | End of the first entry's receipt day | Fixed. |
| Horizon | Section 5 | Fixed. |

`max_segment_bytes` is a closing target, not a capacity. A segment closes before
an entry whose record bytes would take its payload past the target, unless the
segment is still empty. An entry whose bytes alone exceed the target therefore
forms a segment of its own, and the segment is never split inside a record.

`max_segment_quads` bounds the memory a segment costs while it is encoded, which is
up to 390 bytes a quad (measured, #336): 3,000,000 quads is 1.17 GB, and with the
daemon's own 100 MB that is 59 per cent of a 2 GiB soft limit. A record's quads are
counted before encoding, as an upper bound: twice the rows of its stored graph manifests
(each row is a quad in its named graph and again in the default graph) and two link
quads, both exact; plus 4 quads for the event-time fallback when the record has no
known source event time, a bound because the fallback facts are derived at encoding and
no manifest holds them; plus its artifact's graph the first time the segment meets it.
An artifact graph added only because its artifact sits inside the segment's range, with
no record in it, is small and not counted. A segment closes before an entry
that would take it past the budget, unless the segment is still empty, and a record
whose quads alone exceed the budget forms a segment of its own. The limit bounds how
much one segment carries, not what is stored; a rebuilt segment keeps the range its
intent recorded.

A record that is oversized in bytes is a segment of its own, and its bytes stay a blob in
the segment's GTS files like any other record's.

The GTS writer holds the whole segment in memory and costs about three times a
record while it writes (measured at 128 MiB). A record whose bytes exceed 128 MiB
is therefore not written. Only records from layouts before 2026-11-02 can be that
large. The format for large payloads is undecided and with the operator, and
nothing is built for them. Until it is decided, such a record is held: it is
neither archived nor deleted. Selection
stops before it, so the watermark never passes an unarchived record, and the
writer reports it by identity and size in a structured warning.
A record whose content projection failed at ingest is held the same way, because
its evidence cannot be read whole. `Store::archive_held` reports the held record
and the reason when it is the first unarchived entry. A segment that fails to
encode is halved until the failing entry stands alone, which is then named in
the error; the worker journals that failed pass and the watermark does not move. The segment's read renews its lease while it makes
progress, so a long segment is not a failure; only a read that stalls for the
lease's 60 seconds ends with `RESULT_EXPIRED` and is halved. Every live record a segment covers has its graph in the file (its parent
artifact graph included), and every artifact the segment covers has its graph unless it was retired
before the fence, in which case it is counted in `retired_artifacts`. A record that was
evicted or retired before the fence is counted in `retired_records` and is absent from the file:
no graph, blob or link quad. The count of held records and the bytes waiting behind the first are not
scanned, because that would read every record row on each pass.

The 256 MiB ceiling and the hold bound apply to Sparrow's use of PurRDF's
in-memory GTS writer. Adopting PurRDF's streaming writer for single oversized
records is tracked in #301. Qualifying a larger segment target, including its
peak memory and verified readback, is tracked in #125.
Encoding needs memory of roughly three to four times the segment size. That
figure is an estimate to measure.

## 3. Segment identity

```text
segment-id = BLAKE3( frame("sparrow-archive/1")
                   ‖ frame(store-id)
                   ‖ frame(first-position) ‖ frame(last-position)
                   ‖ frame(file-name) ‖ frame(BLAKE3(file))
                   ‖ frame(u64_le(0)) )
```

`frame` is PurRDF's `purrdf_hash::frame::frame_le`: an eight-byte little-endian
length followed by the bytes. Digests are the raw 32 bytes. A segment has one file. The
final term is the quarantine count, which is always zero: nothing is quarantined (section 6).
The identity is not stored in the archive; it is the name a commit uses (section 7). The PurRDF codec is
byte-deterministic, so the same range of the same store, encoded by the same
PurRDF revision, always gives the same identity. A copy of a store
therefore rebuilds and adopts its existing segments. A different PurRDF revision can give
a different identity for the same range. The file name depends only on the range, so no
second file is written. A rebuild of the same range under another identity replaces the
uncommitted intent (section 7), and one against a file already in place that the decoder
did not refuse fails the byte-equal check in section 8: that pass fails, and every later pass
fails the same way, until an operator moves that file aside (the journal line names it); the
watermark does not move.

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
| Record bytes `[byte_start, byte_end)` | One blob per record in the segment's GTS file, keyed by its BLAKE3 digest string `blake3:<hex>`, compressed with zstd (independent 64 KiB zstd frames from 8 MiB up). A record's chunks are joined in order. |
| Record metadata graph `urn:sparrow:graph:record:<hex>` | the segment's GTS files, including its provenance and tracing facts. |
| Diagnostic graph | the segment's GTS files, under its recorded graph IRI. |
| Artifact graph `urn:sparrow:graph:artifact:<hex>` | the segment's GTS files of the segment whose covered range contains the artifact's position. When artifacts share a millisecond, that can be an earlier segment that holds none of the artifact's records. The gate treats the artifact as archived once that segment commits. |
| Content projection graphs | the segment's GTS files, under each projection's graph IRI. A projection shared by records in several segments appears in each of them. |
| Link quads | Two per encoded record, zero-length ones included (below), in the graph `urn:sparrow:graph:archive-links`. |
| Reifiers and annotations of these graphs | the segment's GTS files. |

Two link quads tie a record to its blob, one carrying the SHA-256 of the record's bytes
(64 hexadecimal digits) and one the BLAKE3 digest string that keys the blob:

```text
<urn:sparrow:record:HEX> <urn:sparrow:vocab:payloadSha256> "64 hex digits" <urn:sparrow:graph:archive-links> .
<urn:sparrow:record:HEX> <urn:sparrow:vocab:payloadBlake3> "blake3:64 hex digits" <urn:sparrow:graph:archive-links> .
```

Artifact bytes are not copied separately. Every byte of an artifact with records
belongs to one of its records, and a recordless artifact has no bytes. A reader
rebuilds an artifact by joining its records' blobs in source order. `sparrow-archive`
hashes each record's bytes against the `payloadBlake3` digest its link quad names, checks that the
records are contiguous in byte order from zero, and checks that their total equals the
artifact's `byteLength`. It does not compare the whole artifact with its BLAKE3
`payload`, which `list` prints as `payload_id`.

An entry already retired before the store was fenced still has its catalog row
until ordinary cleanup removes it. A record evicted or retired before the fence has no
metadata or bytes to read: selection counts it in the prepared segment's `retired_records`
and writes nothing for it, and an artifact with no graph is counted in `retired_artifacts`.
Only a live record with zero bytes gets an empty blob and its two link quads, so a reader
finds every encoded record the same way.

The archive leaves out text postings, term dictionaries, tail ranges, pins,
request and retry rows, journals, import staging and catalog JSON. These are
derived or operational state. Readers rebuild indexes from the archived graphs.

## 5. Archiving schedule

- **By age**: an entry is due once `now ≥ received_at + start_fraction ·
  (expires_at − received_at)`. With the defaults, that is day 22.5 of 30.
- **By size**: everything is due, oldest first, once
  `linked_physical_bytes ≥ start_fraction · (disk_budget − maintenance_reserve)`.
- **Backlog**: an existing store that turns archiving on is fenced with nothing
  archived. Its whole retained backlog is archived, oldest first, before any of
  it is deleted.
- **Horizon**: no new segment extends past `now − max(10 minutes, buffered_flush_interval)`.
  Nothing else moves it: an open import preparation or an unsynchronized buffered entry
  does not hold it back. A publication at or behind the watermark or the in-flight segment's end is
  refused with `ARCHIVE_WATERMARK` (section 7), so an import that outlives the horizon is
  refused, not waited for. A rebuild of an in-flight segment follows the intent's range and
  applies no horizon, size or day limit.

Pins do not affect archiving. A pinned entry is archived in order like any
other entry; its pin only delays deletion.

## 6. Held entries

Nothing is quarantined: the format has no quarantine files and no quarantine records.
An entry that cannot be archived is held, so it does not leave the store unarchived and
unreported:

- A record whose bytes exceed 128 MiB (`TooLarge`), or whose content projection failed at
  ingest (`ProjectionFailed`), is held at selection. Selection stops before it, so the
  watermark never passes it and nothing after it is archived or deleted until it is
  resolved. `Store::archive_held` reports the held record and the reason when it is the
  first unarchived entry, and the worker journals it.
- An entry the encoder refuses is found by halving the segment until it stands alone (a
  rebuild of an in-flight segment, and an `UNSUPPORTED` error, are not halved: the error
  then names the range's first entry). The prepare fails with the error naming that entry and its code, the worker journals
  the failed pass, and the watermark does not move. The entry is retried on the next pass.
- In every case the entry stays in the store, unarchived and undeleted, until it is
  archived by a later pass.

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
with `RESOURCE_LIMIT`. An identical retry succeeds, and an intent over the same first and
last positions replaces the uncommitted one (a rebuild under another identity), after the
same range and count checks. An intent over a different range is refused until the one
in flight commits. A commit names the segment
identity. It advances the watermark to the intent's last position and deletes
the intent in one synchronized batch. The commit resets the pressure cursor, so
the next pressure pass starts again from the beginning of the eviction order.

A publication batch whose earliest position is at or before the watermark or the
in-flight intent's end is refused with `ARCHIVE_WATERMARK`, a code typed apart from
`STORAGE_FAILURE` so a caller that stamped the batch early can offer it again with a later
instant. The message names the position and the watermark and says that the clock moved
backwards or an import outlived the archive horizon. Without this guard, a backwards
clock step could publish evidence the archive would never cover.

Archiving fences the store format with the `sparrow.store/99` to `/109` family, or `/121` to `/131` once the store is also fenced for intake provenance. A store fenced for receipt-time event fallback RDF (schema 69) uses `/154` to `/164` and `/176` to `/186` respectively. A store that also carries the owner-to-graph index uses `/209` to `/219` and `/231` to `/241` respectively; the source summary adds `/264` to `/274` and `/286` to `/296`; the store counters add `/319` to `/329` and `/341` to `/351`; and the payload index adds `/374` to `/384` and `/396` to `/406` (the first range of each pair without intake provenance, the second with it). A store opened by the current binary is lifted into the payload-index families.
It sits above every earlier authority family and keeps the underlying authority
through later transitions. It includes the log time law, so stores in this
family always carry that law marker. Binaries without the gate do not know these
identities and refuse to open the store. A fenced store opened without an
archive policy fails with `INVALID_ARGUMENT`; a watermark without its fence, or
a fence without its watermark, fails with `STORAGE_FAILURE`. Removing archiving
from a store is not supported in this version.

## 8. Write protocol

The store worker, `sparrow-archive`, reserves the process's single archive filesystem
slot before preparing a segment and recording its durable intent. It gives the
prepared bytes and their destination to `sparrow-archive-io`. That thread holds
no store handle. It:

1. Checks that `<root>` is a directory on its own mount (a different device from `/`, and a
   network filesystem type or a type different from `/`'s), and that it holds this store's
   `STORE_ID`, writing it on the first pass into an empty root (section 1).
2. Creates `<root>/<store-id>` if it is missing. It syncs `<root>` on each
   attempt to make the store directory and `STORE_ID` links durable, including
   when a prior attempt created them and stopped before the sync completed.
3. Writes each segment file as `<name>.partial`, syncs it and renames it to `<name>`. A file
   already at `<name>` is kept as is. Then it syncs the store directory.
4. Reads every file back and requires it byte-equal to what it prepared, then decodes the
   set with the decoder `sparrow-archive verify` and `extract` run (`sparrow_archive::verify`,
   called from `sparrow-server/src/archive/decode.rs`): the set is exactly one `.gts` file,
   PurRDF's GTS reader reports no diagnostic (which covers the frame hash chain), the record
   and artifact metadata is well formed, and each record's bytes hash to the digest its link
   quad names. A blob corrupted before the write is refused here, before the commit.
5. Returns the verification result to the store worker. The store worker calls
   `archive_commit` with the verified segment identity and advances the watermark
   (section 7).

If a step reaches `io_timeout_secs` or the store worker stops, the store worker
abandons its reply and leaves the intent in place. The filesystem thread retains
its slot until its job returns. The next pass verifies any existing files before
it can advance the watermark, and the abandoned reply cannot commit.

If the target already exists and equals the prepared bytes, the write counts as done. If
it exists and differs, the pass fails with a verify error and the watermark does not move.
Only a verified segment is committed: verification decodes the segment with the same
decoder as `sparrow-archive verify`, so each record's bytes must hash to the digest its
link quad names. When the decoder refuses a segment, the worker renames the file to
`<name>.<segment id hex>.refused` (kept for inspection, never deleted; the identity covers
the bytes, so refusals of different bytes never collide and the same bytes refused again
leave one copy), the watermark stays and the intent
stays. The next pass rebuilds the range; bytes of another identity replace the intent and take
the freed name, and the same bytes are refused again. A valid file a crash left under the
range's name, followed by a rebuild under another identity, fails the byte-equal check; the
journal line names the file, and the operator moves it aside. Configure NFS
exports for synchronous writes (`sync`) so a successful fsync certifies
the committed bytes.

A held worker is visible in status. After a failed pass `status` reports `archive_worker`
(`Status.archiveWorker`) with `phase` `holding`, the watermark in `through`, `intent_in_flight`,
and a `failure` naming the error `code`, the `segment` file, the `reason` and when it
began (`since`). The worker retries every `check_secs`. To leave the hold, remove the cause the
failure names: move a conflicting or refused file under `<root>/<store-id>/` aside, mount the
share, or correct the `STORE_ID`. The next pass that ends without error clears the failure and
returns the phase to `idle`, and the watermark has advanced if a segment committed. Nothing
needs a restart.

Crash recovery follows from the intent:

| Crash point | Recovery |
| --- | --- |
| Before the intent | Nothing to recover. |
| After the intent | The next prepare rebuilds the segment from the intent's range, and the worker's next write truncates a stale `.partial`. |
| After the rename, before the commit | The next pass finds the file, checks it against the rebuilt bytes and commits. |
| Range content differs from the intent | A rebuild over the same range replaces the uncommitted intent after the range and count checks; a different range is refused by `archive_intend` with `CONFLICT`; an intent that now covers a held record fails with `STORAGE_FAILURE`. Nothing is deleted either way. |

## 9. No manifest

The archive writes no manifest. A segment's contents are its GTS file: the graphs of its
records and artifacts, the link quads and the record blobs. The per-segment facts that
other tools need are in that file or are derived from it: `sparrow-archive list` prints each
segment's record, artifact and blob counts and its artifacts' `id`, `source`, `received_at`,
`byte_length` and `payload_id`. The store's own progress is in its catalog, not in the archive
(section 7).

## 10. Readers

PurRDF's GTS reader reads a segment directly and checks its frame hash chain; it does not
hash a blob's decompressed bytes, so the decoder hashes each record's bytes and compares
them with the digest its link quad names. To find a record's bytes, follow the link quad
(predicate IRI `urn:sparrow:vocab:payloadBlake3`) to the blob with that digest. A file the
reader refuses, or with no segment head, is a refusal and never a partial read.

No reader detects duplicate segments: `list` and `verify` treat each file alone, and
`extract` drops a repeated record id silently after sorting an artifact's records by byte
position. The offline `sparrow-archive list|verify DIR` tool (crate `sparrow-archive`)
walks `DIR` for `*.gts` files (it also skips a directory named `.staging`, which this
format does not write), decodes each in memory through one decoder module, and
prints one JSON line per segment with its record, artifact and blob counts. `verify`
also fails any segment the decoder refuses, with `"ok":false` and exit 1. The tool never
opens a live store and writes nothing.

To take one artifact out of the archive with only the NAS directory and the binary:
`sparrow-archive list DIR` prints one JSON line per segment, whose `entries` array gives
each artifact's `id`, `source`, `received_at` (Unix milliseconds), `byte_length` and
`payload_id` (the BLAKE3 hash of the artifact's whole bytes, `blake3:` and 64 hex digits, read from the artifact's graph);
`sparrow-archive verify DIR` checks every segment; and
`sparrow-archive extract DIR ARTIFACT_ID > file` joins the artifact's records from every
segment under `DIR` in byte order, wherever they were archived, and writes them to stdout
only after every segment decoded with each record's bytes matching its link digest, the
records contiguous in byte order and their total equal to the artifact's recorded
`byteLength`. A damaged segment anywhere under `DIR` blocks extraction; to extract from the
rest, point `DIR` at a directory without it. A damaged segment, an unknown id or an artifact with a
missing record exits nonzero, names the file or artifact on stderr and writes nothing to
stdout. There is no restore into Sparrow.
