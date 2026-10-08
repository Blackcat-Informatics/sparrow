# Broad query paging

Broad record queries currently capture their complete admitted result metadata
before paging. They refuse above 10,000 matches or 2 MiB; the 7,058-record copied
corpus exceeds the chronological byte ceiling. Page size does not reduce this
capture. Increasing the ceiling or issuing independent narrowed queries does
not implement one complete, immutable broad traversal.

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

## Remaining integration

The next implementation must replace chronological full-row capture with a
finite native snapshot lease and bounded page staging. It must freeze the
eligibility clock, selected references and pin revision, protection, diagnostic
coverage and complete counts; later ingestion, pin changes and retirement must
not change replayed rows. Snapshot and page buffers, request queues, cursor
boundaries, decode/scan work and retained native versions need explicit admission.
An output failure must publish no successful prefix. Cursor-only continuation,
replay, release, absolute expiry, owner restart and shutdown must all use the
actual lease owner and join its worker. Read leases do not create pins or renew
retention. Typed, CLI and MCP rendering must retain their existing contracts.

Source-time ordering, native global ranking and artifact counts need their own
qualified bounded execution strategy through the same helpers. A chronological
stream must not rescore pages as independent corpora, silently substitute receipt
time for source time, or claim incomplete counts as exact. The existing captured
query implementation remains the production path until that integration and
its complete acceptance scenarios pass. The broad capture limitation remains
unfinished.
