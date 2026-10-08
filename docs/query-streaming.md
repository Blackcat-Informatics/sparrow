# Broad query paging

Record, text, key and value queries stage their complete admitted result metadata
incrementally into an anonymous disk file before publishing a page. This includes
field qualification, source-time ordering, native ranked text and artifact grouping.
They retain at most 10,000 compact offset/length pairs in memory; each page reads
only its admitted rows. The 10,000-match and scan-work ceilings remain
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

Schema 45 and host admission profile `sparrow-query-capture/4` identify this
path. Record, text, key and value receipts advertise a 64 MiB maximum disk capture;
the configured store allowance can admit less or refuse with storage pressure.
Native ranked membership
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

Signed source `6176223` passed the full source hook and the disk-backed traversal
returned all 7,058 copied-corpus records over 48 pages, exactly matching the
original ingestion receipts. The gate covers replay, release, absolute expiry,
owner restart and disk-pressure refusal; live TCP/Unix/CLI/MCP checks passed.
The private SIGKILL recovery check preserves evidence and rejects old cursors,
with explicit removal of the verified dead owner's stale Unix socket. See
[adoption evidence](development-runtime.md#disk-backed-record-query-adoption).
Later signed source `5ca7eeb` qualifies recovery by the production daemon itself,
without a driver-side unlink, on a copied 7,058-record Btrfs store. See
[owned endpoint recovery](development-runtime.md#owned-local-endpoint-recovery)
for ownership checks, preserved names and the unchanged service restart policy.

The chronological writer remains serialized during capture. Mixed finite
concurrency is measured in the [performance baseline](performance-baseline.md).
Earlier private Unix and TCP client interruptions during partially written captures
showed that admitted work finished and abandoned result leases retained their staging
until owner maintenance purged them at the sixty-second deadline; see
[abandoned query cleanup](development-runtime.md#abandoned-query-capture-cleanup).
That evidence qualifies the predecessor's bounded lease cleanup. Cooperative
in-flight cancellation is described below; asynchronous native snapshot execution
and production cancellation resource/latency qualification remain unfinished.
Read leases do not create pins or renew retention.
Typed, CLI and MCP rendering must retain their contracts.

JSON discovery uses the same anonymous staging owner. The native fact matcher
stages each value occurrence directly. Key counts aggregate completely in a
32 MiB charged map, preserving duplicate occurrences separately from record
presence, then stage sorted rows. The typed key aggregation ceiling is absent
for other query operations. Exhausted work, item or disk admission refuses before
publication. Native pinned projection slices avoid copying encoded facts.

The shared dev-only `wide_jsonl_keys` fixture uses PurRDF's native JSON codec.
The core qualification captures 5,002 keys and 5,002 values, each exceeding
2 MiB, verifies duplicate counts and exact original-byte spans, then replays
after pin release and evidence retirement. The RPC qualification traverses
5,001 keys and 5,000 values over TCP and Unix with identical CLI/MCP pages and
typed admission evidence. The full release gate passed with 144 storage and
53 RPC scenarios, all remaining workspace tests, seeded census self-tests and
224 pinned ledger jobs without distinct exceptions. The large RPC scenario also
passed with its private store on the deployment's Btrfs filesystem at
`~/.local/state/sparrow/json-pages-btrfs.kQY5Pme1`; its retained log records
that path. Installed runtime adoption evidence is recorded separately.

A chronological stream must not
silently substitute receipt time for source time or claim incomplete counts as
exact. Other captured query families retain their previous implementation until
their integration and complete acceptance scenarios pass. Full broad query
acceptance remains unfinished.

## Cooperative query cancellation

Each admitted production TCP or Unix connection has its own cancellation owner.
Connection completion, failure, lease expiry or task teardown cancels its child
record/text/key/value captures. Each RPC also has an independent caller lifetime;
cancelling one child does not cancel sibling operations. A worker keeps its
concurrency permit until it exits. Mutation workers still finish admitted work,
and cancellation does not establish non-delivery of an intake or pin mutation.

The native storage read and spool share one host cancellation token. Checkpoints
precede capture admission/publication and occur at candidate records, JSON facts,
text postings, corpus identity/scoring entries, staged rows and sort completion.
Cancellation returns `CANCELLED` without a successful prefix, drops private
staging and releases its disk reservation. It does not interrupt an individual
native database call, codec/projection call, sort, or a wait for a storage mutex;
the next host checkpoint observes it. PurRDF still owns analysis and scoring.

A completed capture has a reply guard until typed response rendering finishes.
If the blocking worker's result is abandoned or rendering fails, that guard
releases the snapshot through the existing result owner. Cleanup failures are
reported, not swallowed. Successfully rendered captures keep their ordinary
replayable lease: rendering is not proof that the peer received the response,
and a later disconnect does not revoke a cursor intended for reconnect/retry.
Explicit cancellation-message propagation through every adapter and production
tail-latency/resource bounds remain separate acceptance work.
