# Broad query paging

Record queries now stage their complete admitted result metadata
incrementally into an anonymous disk file before publishing a page. This includes
field qualification, source-time ordering, native ranked text and artifact grouping.
They retain at most 10,000 compact offset/length pairs in memory; each page reads
only its admitted rows. JSON discovery still uses the earlier 2 MiB resident
capture and remains to be migrated. The 10,000-match and scan-work ceilings remain
explicit admission boundaries.

## Bounded immutable disk staging

The serialized catalog writer fixes membership, protection, diagnostics, field
witnesses and artifact counts during one scan through the existing matcher. It
writes one native JSON summary at a time, rather than accumulating a result-size
row vector. After the complete scan succeeds, an immutable file and bounded
offsets supply replayable byte-limited pages. Later writes and logical retirement
do not affect those captured summaries. This supplies metadata paging without
retaining old RocksDB versions for this operation; RDF reads continue to use
their separate native snapshot path. The shared native read helpers remain useful
for those backends.

Source-time captures sort bounded `(millis, record ID, offset ordinal)` keys and
then keep only reordered offsets; they do not collect complete row bodies. Sort
admission includes string storage and vector growth, with a 2 MiB ceiling checked
before admitting each key. Text captures retain their existing bounded native
membership and global scorer. Once that scorer orders all admitted matches, it
stages one complete scored row at a time. No page-local corpus or scoring law is
introduced. Artifact counts accumulate only over final matching rows.

Before staging, reserve the smaller of 64 MiB and one eighth of the configured
data allowance (disk budget less maintenance reserve), checking it against the
store's complete admission footprint. Keep 64 KiB of that reservation outside
the row-data bound for allocation overhead. After capture, replace the reservation
with the greater of measured allocated blocks and rounded written data plus
64 KiB allocation overhead, preserving admission during delayed writeback.
Ingestion and control-write admission
include this shared staging charge. The reported admission footprint therefore
includes anonymous query files and in-progress reservations, which directory
enumeration alone cannot see. A filesystem quota is still required for a hard
physical ceiling. Row size remains 32 KiB, metadata 32 KiB, resident lease capacity
2 MiB per result/8 MiB aggregate, eight handles, pages 256 KiB and leases 60 seconds.

Linux `O_TMPFILE` creates no linked query pathname. Closing the last descriptor
on release, purge, final single-page completion or owner shutdown reclaims its
allocation; process death also closes it. A filesystem without anonymous-file
support gives an actionable storage failure, not a resident fallback. The
existing periodic expiry path purges expired leases. Page I/O is fallible through
the shared lease contract, and any staging or page failure returns no successful
prefix. No durable query receipt, evidence pin or renewed retention is created.

Schema 44 and host admission profile `sparrow-query-capture/3` identify this
change. Record and text receipts advertise a 64 MiB maximum disk capture;
the configured store allowance can admit less or refuse with storage pressure.
Other captured query families still advertise 2 MiB. Native ranked membership
keeps its separate 64 MiB admission; output staging does not remove that bound.

## Native read foundation

The production query candidate path now delegates temporal reads to
`query_membership::temporal_candidates`. Current and native-snapshot backends
provide their iterator and catalog lookup. One implementation supplies the
half-open native seek/upper bound, index decoding, record identity/time checks,
missing-record failure and preservation of other storage errors. Selected
reference resolution remains with its existing owner.

`record_fields::qualify_from_projection` owns native same-record occurrence
matching, array-parent binding, strict coverage and field witnesses. Its backend
supplies projection bytes. The production writer uses a native pinned slice,
avoiding an extra copy of encoded facts. PurRDF still owns JSON parsing and
projection. `matched_record_row` provides one chronological/ranked record summary;
the native ranker continues to own scores and text witnesses.

The core test `snapshot_worker_preserves_temporal_versions_and_shared_integrity_checks`
uses one joined thread with bounded one-message channels. That thread owns the
database Arc and borrows its native snapshot locally. After snapshot admission,
the other thread atomically deletes the original catalog/temporal entries and
projection and inserts a replacement. The snapshot backend returns the original
metadata and exact native `/job` field witness over bytes `[8,16)`; the current
backend sees the replacement. Corrupt catalog bodies outside `[20,30)` are never
read. Missing catalog references and identity disagreement produce
`STORAGE_FAILURE`; another injected operational error retains its original code.
This is a backend metadata/field ownership test, not daemon paging or a complete
production expiry/retention qualification.

The safe RocksDB snapshot borrows its database, so storing it beside an owning
Arc would require a self-referential structure. The joined-owner test verifies
an alternative: keep both local to one owned worker and send bounded page
requests to it. No unsafe lifetime extension, dependency change, resident RDF
replay or copied native matcher is needed for that ownership model.

## Qualification

Signed source `5ae4fd7` passed the complete release and census gate, including the
snapshot backend ownership test. Its installed daemon preserves the exact live
source/time/field rows and native ranked witnesses captured before cutover. An
independent 7,058-record disk corpus retains predecessor source, temporal and text
pages, including artifact counts, while the broad query still refuses at the
existing capture bound. See [runtime adoption evidence](development-runtime.md#shared-native-read-adoption).
This establishes a shared production/helper foundation, not a snapshot-backed
result lease or removal of the broad capture limit.

## Remaining integration

Qualify the disk-backed traversal against the real copied corpus,
including exact replay, release, expiry, owner restart, disk-pressure refusal and
all transport renderers. The chronological writer remains serialized during
capture; concurrency and cancellation latency require measurement rather than a
claim of asynchronous native snapshot execution. Read leases do not create pins
or renew retention. Typed, CLI and MCP rendering must retain their contracts.

JSON discovery needs its own qualified bounded execution strategy through its
native occurrence and aggregation owners. A chronological stream must not
silently substitute receipt time for source time or claim incomplete counts as
exact. Other captured query families retain their previous implementation until
their integration and complete acceptance scenarios pass. Full broad query
acceptance remains unfinished.
