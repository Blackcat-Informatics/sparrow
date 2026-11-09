<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Sparrow interfaces

Schema 45 uses anonymous, disk-admitted immutable staging for record, text, key
and value pages, including field filters, source-time ordering and artifact grouping. Each page remains bounded
and replayable with the original absolute deadline; release removes only the
query allocation. Host admission profile `sparrow-query-capture/7` advertises a
64 MiB maximum disk capture for this path, subject to the configured store budget.
Row offsets are disk-backed; a bounded resident page map preserves replay.
Chronological record and JSON captures report the requested scan budget as their
item ceiling. Source-time sorting and ranking retain 10,000 items and
their separate resident working bounds. Key aggregation has a separate 32 MiB
charged working allowance. See
[query streaming](query-streaming.md) for admission, cleanup and remaining work.

This document specifies the first-version contracts. Implemented schema sections
describe current typed operations; other design examples retain their required
target behavior. [Development runtime](development-runtime.md) identifies the
installed and verified surfaces, and [broad query paging](query-streaming.md)
records the remaining capture limitation. Lifecycle, storage and recovery
requirements are in [Architecture](architecture.md).

Admitted production TCP and Unix connections own cooperative lifetimes for their
record/text/key/value and source/artifact/pin captures. Disconnect, connection failure/expiry or serving
task teardown cancels those children at host scan/staging checkpoints. A worker
retains its permit until exit; private files and reservations are dropped, and
fresh captures abandoned before response rendering are released. Successfully
rendered cursors retain their ordinary replayable lease, including after an
abandoned continuation. A live artifact continuation releases its new handle if
it is not rendered, and the handle it continued only after rendering (schema 66). Store identity validation does not wait for the catalogue
writer. Cancellation does not
establish non-delivery of a mutation, interrupt individual native calls or mutex
waits, or qualify direct Cap'n Proto promise cancellation. MCP cancellation
notifications abort only the selected tool's owned SDK connection and retain its
four-call admission slot until the job is dropped; cancelled tools emit no reply.
See [cooperative query cancellation](query-streaming.md#cooperative-query-cancellation).

MCP and CLI adapters call the generated Cap'n Proto daemon client. They must not
implement separate filtering, retention, pin, or query semantics. The daemon
owns the database; the local CLI and stdio MCP adapter connect to it.

### Frozen scope receipts (schema 56)

Record, text, artifact-group, key and value captures include a typed
`QueryCapture.scope`, rendered as `receipt.scope` by the shared CLI/MCP adapter.
Profile `sparrow-scope-receipt/1` supplies a compact `scope:<BLAKE3>` identity,
selection kind (`live`, `evidence`, or `pin`), direct selected-reference count and
expanded record count before source/time/diagnostic filters. Live membership
counts are explicitly unknown. A pin receipt includes its stable ID, actual
resolved revision and absolute deadline, even when no revision guard was sent.
These are captured at the same serialized writer boundary as membership; later
renewal, release or reuse of a pin name cannot change a continuation receipt.

The identity frames the profile, store identity, selection (unique sorted evidence
IDs or resolved pin ID/revision), optional exact source, unique sorted OR
alternatives by source/diagnostic dimension, half-open receipt-time interval and
scope-level `require_complete_index`. PurRDF's native streaming frame and BLAKE3
helpers own encoding and hashing. The receipt reports `has_exact_source`, alternative
and dimension counts, rather than potentially hundreds of KiB of selector values.
An exact-source selector need not fit a stored source name; it can validly select
no records. Its full value participates in the fingerprint without enlarging the receipt.
The original selector remains bound to the continuation through the existing
lease owner. Set normalization describes the logical identity; it does not relax
continuation selector equality.

`received_from_ms`, `received_before_ms` and pin `expires_at_ms` retain raw UInt64
milliseconds, rendered as decimal strings; `received_before_ms=18446744073709551615`
is the existing unbounded upper selector, never a formatted civil timestamp.
Membership counts and revisions also render as decimal strings. Page size and
scan budget remain in capture limits, outside the logical scope identity. Record
predicates, event-time intervals and text are operation inputs outside this scope
fingerprint. The identity is neither a content hash nor a snapshot or ranked corpus
identity: those retain their separate capture/profile fields. Scope-level strict
index input does not replace a record query's additional strictness setting.

Host admission profile advances to `sparrow-query-capture/7`; row, metadata, page,
lease and disk ceilings stay unchanged. Store format stays 70. Older endpoints
fail the existing exact-schema discovery check. The adapter refuses a missing or
incoherent required scope receipt. Schema 57 extends this same logical scope law
to SPARQL and context below. Correlation queries and production-volume acceptance
remain required.
The installed runtime version is recorded in [Development runtime](development-runtime.md).

### Graph and context scope receipts (schema 57)

Native graph pages carry the same `QueryScopeReceipt` in `SparqlPage.scope`,
rendered as top-level `scope` beside the native certificate and independent
coverage. Context carries it in `ContextCapture.scope`, rendered as `receipt.scope`.
Both use the same selection resolver, logical fingerprint, typed encoder and
CLI/MCP decoder as record/text/key/value captures. The redundant record-only
membership wrapper is removed. Scope receipts are captured under the writer
boundary; native graph execution still runs over its original immutable snapshot
after releasing the writer. Native partial, unknown and query-failure pages retain
their scope without changing certainty, diagnostics, row classification or coverage.
Continuation reuses the captured pin revision/deadline after renewal or release.
Graph text, bindings and layers, and the context anchor, implicit source-local
restriction and neighbor counts are operation inputs outside this scope identity.
The scope counts still describe resolved selection membership before those operations.

Source admission profile `sparrow-rdf-source-v4` reserves 64 KiB for the bounded
scope value and hashing work before capture, and another 64 KiB through the
existing owned native workspace seam before cloning that value into a result.
The result owns that second reservation, including for native query errors, after
the snapshot/view ends. Page metadata independently charges the frozen scope's
typed/agent envelope against the existing 64 KiB metadata and 256 KiB page limits.
The common source workspace, retained-handle and governor ceilings are unchanged.
`limits.scope_receipt_bytes` advertises the per-copy 64 KiB allowance, not measured
RSS or physical I/O.

Context admission profile `sparrow-context-capture/3` reserves the same 64 KiB
within its existing 32 MiB catalog allowance, and charges the encoded scope's
typed/agent envelope to the existing 256 KiB aggregate response allowance before
admitting the anchor and neighbors. Its `scope_receipt_bytes` limit reports that
partition. A direction-scan partial still identifies the complete requested scope;
it does not imply every selected member was returned. Context remains one reply
without a continuation lease. Record/text/key/value admission profile stays `/7`.
Schema discovery advances to 57; persistent store format remains 70.
Adapters refuse missing/incoherent scope receipts and a mismatched admission
profile or scope-workspace allowance before rendering a successful result.

### Local endpoint recovery

The Unix socket is directly inside the private store directory. Startup first
obtains the native exclusive store lock and validates the watch catalog. If bind
finds an occupied name, the daemon requires a socket (never a symlink or regular
file), a connection refusal from a bounded 250 ms probe and an unchanged
device/inode before removing that stale name and rebinding. A live connection,
probe timeout, other error or changed identity refuses startup and preserves the
name. Recovery coordinates with other Sparrow owners through the store lock;
the private directory remains its pathname ownership boundary. Shutdown removes
only the original bound socket identity and reports a replacement as a failure.
Socket-bind refusal joins the already-started consume worker before returning.
The public TCP bind still happens before opening or migrating the store.

### Daemon teardown

One native SIGINT subscription is registered before opening storage or starting
workers and retained through serving. Maintenance runs in its joined owner, so
the serving loop can select the signal while a native maintenance pass runs.
A signal during startup remains pending until serving can request teardown.
It does not interrupt an individual native operation or
certify a shutdown latency bound.

Socket-bind refusal, syslog-start refusal, terminal serving/worker failure and
graceful shutdown route through one teardown owner. It attempts intake and maintenance joins
and owned socket-name cleanup even when an earlier step fails; the final store
flush requires the successful RPC drain described below. Errors retain their
original cause and label each cleanup failure;
a later flush or cleanup failure cannot replace the original error. During
normal serving teardown the RPC LocalSet remains active while intake workers
join. Endpoint cleanup preserves replacement names under the ownership rules
above. Failed startup returns a failure after cleanup, allowing a corrected
configuration to reopen the same store.

### Daemon maintenance scheduling

One joined worker owns retention, pressure and periodic buffered synchronization.
It admits exactly one native job at a time, awaiting complete blocking execution
before selecting the next duty. Existing finite storage pass limits remain in
force. RPC and intake retain their request budget independently, as with the
previous serial maintenance path. Socket acceptance, worker health checks and
SIGINT selection continue on the serving loop.

Expiry retains its bounded pass limits and resumes an explicit continuation
after yielding, without waiting one second between every pass. The next
traversal starts 60 seconds after an exhausted one ends. Each traversal parses
the whole catalog under the writer lock, and restarting it every second starved
ingest on a 10 GB store. Expiry already applies to reads, so only physical
retirement waits.
Import journal reclamation runs on its own one-second tick, because a published
journal holds one of the 16 import slots until it is reclaimed. Pressure keeps
its one-second tick, and buffered synchronization keeps its configured interval.
Eligible duties are selected fairly, including when a short flush interval
remains due; stop admission takes priority. An expiry refusal reports its error
and returns to the ordinary tick rather than repeatedly retrying a busy loop.

Shutdown prevents further maintenance admission and joins an already admitted
native job. Teardown joins intake and maintenance before reserving the complete
operation budget for the final WAL fence. An admitted native job is never aborted
because its worker caller disappears. This scheduling contract is not a bound
on sustained turnover, query latency or native shutdown duration.

After intake workers join, teardown reserves all configured operation slots
through Tokio's fair admission queue. Fresh storage dispatch cannot overtake
that reservation; admitted workers retain their slots until they finish, even
when their caller disappears. Once the reservation completes, admission closes
and remains reserved through the final store flush and endpoint cleanup.
Closed storage admission returns retryable `SHUTTING_DOWN`; preserve the same
immutable intent when reconnecting. While the reservation is pending, ordinary
capacity refusal remains `RESOURCE_LIMIT`. A failed admission drain is reported
and withholds the final flush rather than claiming a fence over active workers.
During intake joins, existing RPC connections remain available for completing
owned producer work. This establishes ordering, not a worst-case drain deadline.

### Pressure retirement recovery

Pressure retirement publishes its physical recovery floor and refused-write
estimate in the same durable native batch as evidence retirement. Reopening
restores the wait; counters and native maintenance observations remain
session-local. A later pass must observe a smaller linked store footprint before
retiring more evidence. Anonymous query allocations remain included in write
admission, but their release cannot establish stored-file reclamation. Temporary
query allocation alone cannot authorize evidence retirement.

The first such batch fences the store at format 38, or 39 when a watch registry
is present. The shared publication owner preserves this fence through later
watch, consumption and handoff updates; clearing a recovered wait retains the
fence. Older binaries refuse these formats. Existing stores retain their format
until durable pressure retirement requires the new invariant. Missing space,
native failures and malformed or oversized recovery markers remain failures;
logical retirement does not certify forced native physical reclamation.

### Archive-before-delete gate (schema 63)

An optional `[archive]` server table makes the store archive evidence before it
deletes it. The format, schedule and write protocol are in
[the archive format](archive-format.md). Phase P1 implements the store side:
the deletion gate, the watermark and the writer fence. The archive worker and
its NAS thread come later. Until then an archiving store retires nothing on
its own.

Do not enable `[archive]` on a production store before the worker ships. The
fence is permanent: with no worker to advance the watermark, retention stops,
and under disk pressure ingestion is eventually refused.

```toml
[archive]
dir = "/net/synology/archives/sparrow"  # required, absolute, outside data_dir
start_fraction = 0.75                   # 0.5..0.95
max_segment_bytes = 67108864            # 1 MiB..192 MiB; a larger entry gets its own segment
max_segment_records = 50000             # at least 1
stall_warn_secs = 120                   # 0 < stall_warn_secs < io_timeout_secs
io_timeout_secs = 900
```

Unknown fields are refused. Configuration checks are lexical: `dir` must be
absolute, contain no `..`, and neither contain nor sit inside `data_dir`. The
device and writability probes run at startup on the archive thread, because a
hung NAS must not block startup. An unreachable archive starts the daemon
degraded. Core `Config.archive` carries the store-side `ArchivePolicy`; the
destination and timers stay in the daemon.

The first open with a policy fences the store at formats 99 to 109. These keep
the underlying authority family and include the log time law. Older binaries
refuse them. The fence records `archive:through` with nothing archived, so an
existing backlog is archived before anything is deleted. Reopening a fenced
store without `[archive]` fails with `INVALID_ARGUMENT`. A fence without its
watermark, or a watermark without its fence, fails with `STORAGE_FAILURE`.
Stores without the fence or the table behave exactly as before.

One gate guards every deletion. Expiry leaves entries the watermark does not
cover and counts them in `ExpiryReport.archive_waiting`. Pressure eviction stops
at the first unarchived, unprotected entry with frontier `archiveLag`. If that
pass retired nothing, pressure reports phase `awaitingArchive` rather than
`blocked`. Ingest above the data allowance still receives `STORAGE_PRESSURE`.
Retirement asserts the gate itself; reaching unarchived evidence there is a
`STORAGE_FAILURE` that commits nothing. Pins only delay deletion: an archived
pinned record survives until its pin ends.

`Store::archive_intend` durably records the segment about to be written. Its
range must start at the first unarchived entry and match the catalog's
counts (`CONFLICT` otherwise), and must fit `max_segment_records`
(`RESOURCE_LIMIT`). Identical retries succeed. `Store::archive_commit(segment
id)` advances the watermark and removes the intent in one synchronized batch.
`Store::archive_state` reads both. A publication whose earliest receipt position
is at or before the watermark or the in-flight intent's end is refused with
`STORAGE_FAILURE`, and the message says to correct the clock.

Schema 63 adds the enum values `PressurePhase.awaitingArchive` and
`EvictionFrontier.archiveLag`. CLI and MCP status render them as
`awaiting_archive` and `archive_lag`. Archive worker status, with its phase,
watermark, backlog age, last segment and quarantine listing, belongs to the
worker phase and will use a later schema.

### Table rewrite status (schema 64)

Schema 64 adds `Status.recompression`, a `RecompressionStatus`. It reports the
running binary's table options `profile` and a `phase` of `running`, `paused`
or `complete`. It also reports `remainingBytes`, the table bytes of the
rewritten families still written under an older profile as of the last step,
and an optional `waiting` reason and latched `failure`. CLI and MCP status
render it as `recompression`, with `remaining_bytes` as a decimal string. A new
store reports `complete` with zero remaining bytes. A store whose files predate
the profile reports `running` until the rewrite finishes, or `paused` while a
step holds native rewriting off for pressure, the data allowance or the
free-space floor. `Store::recompress_step(now, settle)` observes progress and
turns RocksDB periodic compaction on or off without compacting anything
itself. `Store::recompression_status` reads the last observation.

### Live artifact pages (schema 66)

Schema 66 changes the live artifact listing, the one with no explicit selection,
from one immutable capture into pages read from the per-source artifact index. A page reads only
the rows it selects, so the listing answers at any store size. Explicit
selections keep one immutable capture, and the earlier statements still hold
for them.

How a page reads:

- A page walks the per-source artifact index (`source-artifact:`, see
  "Per-source counters and artifact index") from its position. With a source,
  it walks only that source's rows, seeking straight to the
  `--received-from` bound and stopping at `--received-before`, so it never reads
  or charges another source's rows. Without a source it walks the index source
  by source in the same way, so an unfiltered page costs what its selected rows
  cost. Index rows are positions and are not charged.
- For each index row the page reads the artifact's entry and then its records
  through the entry's `record_ids`. For a parent whose entry was retired while
  pinned records survive, it reads those records through the source's record
  receipt index (`source-context-v1:`) at the parent's receipt time.
- Rows are ordered by source (an opaque per-source key order, not the label's
  order), then original receipt time, then artifact ID.
- A page stops at its page size, at most 31 rows. It charges each catalog row
  it reads against its own admission: the scan budget in entries and the
  32 MiB conservative catalog charge. Artifact rows are decoded without owning
  their record and chunk arrays.
- An artifact whose records exceed one page's admission spans pages, and the
  cursor resumes inside it. A page that exhausts its admission
  after reading a row ends early with a cursor, possibly with no rows. Only a
  page that cannot fit a single catalog row refuses with `RESOURCE_LIMIT`. The
  message names the page size, both bounds, the artifact and its receipt time.

What a page reports:

- Each page is its own lease, with its own snapshot ID, `as_of` and scan
  evidence. `ArtifactListPage.receiptWalk` marks such a page.
- `total_items` counts the rows delivered through that page, and `page_start`
  the rows before it. Diagnostic coverage covers the rows that page delivered or
  excluded.
- While a live listing continues, CLI and MCP render `matching_artifacts` as
  null, `answer_certainty` as `incomplete` and `coverage.catalog_complete` as
  false. Only the final page of one walk reports an exact count.
- Evidence received or retired between pages can appear in or leave later
  pages, but no row repeats.

Continuation:

- A cursor binds the selector and limits and continues after the page's last
  row, through `artifacts_list` or `result_next`. Continuing reads the store as
  it is then and publishes a new handle.
- The daemon releases the continued page's handle only after it renders the
  new page. A cursor the client already holds therefore survives an abandoned
  or failed continuation. After a rendered continuation it returns
  `RESULT_EXPIRED`. A reply lost after rendering still spends the cursor the
  client held, and the new page's handle lives out its lease.
- A retained record whose parent catalog was reclaimed appears under its
  parent's index row, at the parent's receipt time, with no origin. The index
  keeps that receipt time, so receipt filtering finds it.
- While the online `source-summary` migration is seeding the index, a live
  listing refuses with `INDEX_NOT_READY` naming the migration (see "Per-source
  counters and artifact index").

For live listings only, this section supersedes four groups of statements. The
first is the replay and "never creates another handle" statements under
cursor-only continuation. The second is the schema 27 statements that page
coverage is identical across continuation, that `total_items` counts captured
proven rows, and that continuation serves the immutable capture. The third is
the schema 24 receipt with a fixed store, snapshot and as-of. The fourth is
the earlier MCP descriptions of `artifacts_list` and `result_next`. The
`sources` inventory does not read the artifact index: it reads the per-source
counter rows (see `sources_list` below) and remains one immutable capture.

### Large artifacts (schema 65)

Patrick ruled on 2026-11-01: "Remove the limit". The configurable per-artifact
limit is gone. The server configuration no longer has `max_artifact_bytes`. A
configuration that still sets it refuses to start, and the error says to delete
the key. The disk budget, the maintenance reserve and the free-space floor still
bound what the store admits, and `max_records` still bounds the records in one
artifact.

One fixed bound remains until the v2 store format (#5). It is the smaller of
two limits, and `MAX_IMPORT_BYTES` is derived from both, with a compile-time
assertion that ties the admitted chunk count to each:

- Expiry and eviction read an artifact's whole catalog row in one pass. The 32
  MiB pass catalog budget, less a 4 MiB reserve for the record row the pass reads
  with it, lets the row carry 129935 chunk identities, counting a record identity
  for each chunk. This bound binds.
- Publication commits an artifact's catalog in one atomic write batch of at most
  128 MiB, which carries every 64 KiB chunk identity four times: in the record
  rows, the artifact row, the saved publication receipt and the ingest receipt.
  That admits 438210 chunks.

`import_begin` therefore refuses a declared input above `MAX_IMPORT_BYTES`,
which is 8515420160 bytes (129935 chunks, about 7.93 GiB), before any byte is
uploaded. The `RESOURCE_LIMIT` error states the bound and says to split the
input. An artifact of many small records can still need more chunks or record
identities than its size implies. Layout preparation then refuses it with
`RESOURCE_LIMIT` when its row would exceed the retirement pass, and publication
checks again for layouts prepared earlier and refuses record rows that would
exceed the batch. An inline `ingest` is held to the same retirement check.
Format #5 replaces the whole-row read and the single batch, and with them this
bound.

Every expiry and eviction pass also reads every pin row, expired rows included,
before any artifact. That capture has its own 64 MiB budget, apart from the
artifact's share, so pins cannot crowd out an artifact row. To keep the capture
inside that budget, a store holds at most `MAX_PINS` pin rows (606) and
`MAX_PINNED_MEMBERS` members across all of them (36594), both derived from the
budget with a compile-time assertion that a pin set at both caps fits. Creating
or extending a pin past either cap fails with `RESOURCE_LIMIT`, and the error
names the caps and the store's current totals. Expired rows count until expiry
deletes them, which it does in its own batch before any phase that could stop the
pass. Releasing a pin is never refused.

A larger artifact ingests through the staged import: the CLI `ingest` command,
completed-file consumption and the SDK `import_file` calls append it in 64 KiB
chunks. Sealing, layout, preparation and publication each read the staged chunks
one at a time. A record above 16 MiB is stored raw in pages of 128 chunks, with
a recorded projection failure instead of a JSON projection. A record above
128 MiB (2048 chunks) is laid out as consecutive raw records: every piece but
the last holds 112 MiB, and the last holds more than 16 MiB and at most
128 MiB. Each record's chunk manifest then fits one retirement pass and one
catalog read. An artifact admitted under `MAX_IMPORT_BYTES` and the layout check
fits one pass alongside a record row, and pins stay within their caps, so expiry
and eviction retire the pieces and their artifact in bounded passes.
Stores laid out before this change keep their records; only new layouts split.
No step holds more than 16 MiB of an artifact's bytes in memory. Sealing,
layout and publication of an input of many GiB each run as one call, so they
can outlast the 45-second consumption deadline; resumable steps are also part
of #5.

One inline `ingest` message, and the single write batch behind it, carries at
most 16 MiB. That fixed bound protects the daemon's memory. A larger inline
message is refused with `RESOURCE_LIMIT`, and the message names the staged
import. The SDK `ingest` and `ingest_with_durability` calls stream any larger
input through the staged import, and the receipt is durable whatever durability
was requested. The bound is advertised as `Discovery.maxInlineArtifactBytes`
and `Status.maxInlineArtifactBytes`; CLI and MCP status render it as
`max_inline_artifact_bytes`. These replace `maxArtifactBytes` at the same
ordinals, so the schema advances to 65, and older endpoints fail the exact
schema discovery check. The store format is unchanged.

Byte reads of a record, or of an artifact with at most one record, go straight
to the chunk that holds the requested offset. An artifact with several records
finds the record holding the offset by a binary search over its ordered record
rows, then seeks within that record, so a document split into raw records reads
back byte-exact. Besides the requested chunks, every artifact read decodes the
whole artifact row once, about 8.7 MB of chunk identities at the bound, and a
few record rows. A tail capture keeps its
fixed 1 MiB window. The archive's `max_segment_bytes` is a closing target from
1 MiB to 192 MiB, and an entry larger than the target is archived in a segment
of its own; see [the archive format](archive-format.md).

### Query-only daemon (schema 67)

`sparrowd --config FILE --query-only`, or `query_only = true` in the server
configuration, opens a store to serve reads only. It exists so that a copy of a
store, such as a backup, can be opened and queried. A copy's watch catalog names
the live intake folders, and a normal daemon on the copy would consume them.

A query-only daemon starts no consume or tail worker, syslog listener, TCP
listener or maintenance worker. No periodic buffered flush, expiry, pressure
eviction, import reclamation or table rewrite runs, so retention and eviction
never change the store. The shutdown flush still runs. A configuration that also sets `consume`, `syslog`,
`rpc_tcp_address` or `archive` refuses to start before the store is opened, and
the error names the conflicting key. A store fenced for archiving requires the
`[archive]` table to open, so it cannot be opened query-only.

The daemon serves the Unix socket. Every read answers exactly as a normal
daemon's does. `watchList` and `Status.consumeWatches` report the stored watch
catalog, but no root is opened. Each watch reports coverage `reconciling`, no
directories or candidates, its pending claims from the stored journals, and no
worker counters.

One gate, `sparrow_server::query_only::WriteGate`, wraps the service of every
connection: Unix connections, CHAP sessions and the anonymous TCP gate's
forwards. It classifies every operation once. It forwards each read, and on a
query-only daemon it refuses each write before decoding its request. The writes are `ingest`, `eventsIngest`, `importBegin` through
`importCancel` (except `importGet`), `flush`, `pin`, `pinRenew`, `pinRelease`,
`watchPut`, `watchRemove`, `watchClaimRetire`, `watchHandoff`, `tailFinish`,
`sourceTimeDefine` and `diagnosticDefine`. Each returns the typed error `READ_ONLY`, whose message says
the daemon is in query-only mode. `resultRelease` stays a read: it releases a
query capture held in memory and writes nothing to the store. An operation added
to the schema without a decision in the gate returns Cap'n Proto
`unimplemented`.

A query-only daemon still modifies the store directory, so open only a
disposable copy, never the only one. Every open, including one of a current
store, deletes and rebuilds the source-context catalog rows and their job
checkpoint in synchronous batches. Opening also applies any catalog upgrade that
a store written by an older image still needs, and RocksDB writes its own files
in `data_dir` (the write-ahead log, `MANIFEST`, `OPTIONS` and `LOG`). It replays
a write-ahead log left by an unclean stop and may compact the files that replay
flushes. At shutdown the daemon synchronizes and flushes every column family into
new table files. None of this adds or removes evidence: record, artifact and
payload chunk rows are unchanged, so `Status.uniquePayloadChunks` reads the same
as when the store was closed. Opening a store last written by the schema 62
image left its format, `sparrow.store/92`, unchanged under schema 68. Since
schema 69, any open, query-only included, fences the store for the online
migrations (see "Online store migrations") in one synchronized batch, lifting
that format to `/202`. A query-only daemon runs no migration page: it answers
through the pending read paths and its `status` reports each migration's
progress where the copy left it. A store that still owes the
table rewrite reports `recompression` as `running`, but the rewrite never runs.
The daemon sets `data_dir` to mode 0710, creates its socket name and removes it
at shutdown. Every daemon, query-only or not, logs
`sparrowd: closed store=ID unique_payload_chunks=N` once its workers have stopped
and its final flush has succeeded. A later shutdown error can still follow it, so
`sparrow-backup` records that count only when the daemon then exited 0. Query captures use anonymous temporary files that leave no name
behind.

Schema 67 adds `Status.queryOnly`, true in this mode, and
`Status.retentionSeconds`, how long an unpinned record is retained after it is
received (`sparrow_core::RETENTION_MS`, 30 days). CLI and MCP status render them
as `query_only` and `retention_seconds`, the latter as a decimal string. `deploy/verify-backup.sh` relies on the mode to verify a
backup; see `deploy/RECOVERY.md`, "Verify a backup".

`sparrowd --config FILE --no-intake` opens a store writable with its
maintenance worker, so a disposable copy can run its online migrations, but
starts no intake. Like a query-only daemon, it starts no consume worker and
reports the stored watch catalog without opening any root, and its
configuration may not set `consume`, `syslog`, `rpc_tcp_address`,
`ingest_socket_path` or `archive`. The refusal names the key. The mode has no
configuration key and cannot be combined with query-only mode. Reads and
writes on its Unix socket are served as by a normal daemon, and
`Status.queryOnly` is false. If its maintenance worker ends, the daemon exits
with an error, as a normal daemon does. The startup line reports
`query_only=false intake=false`. `deploy/verify-backup.sh --measure-migration`
uses the mode; see [the deployment guide](deployment.md), "Measure the online
migration".

### Receipt-time event fallback (schema 69)

A record whose source profile yields no event time is placed on the event
timeline at its receipt time, and every such placement is marked as a fallback.
The basis is `source` when the source profile supplied the instant and `receipt`
otherwise. The fallback is derived at read time from the record's `received_at`.
Stored records and source profiles are unchanged, and the stored unknown reason
stays inspectable.

The Cap'n Proto shape is unchanged in structure: the `EventTime` union variant is
the basis (`known` is `source`, `unknown` is `receipt`). Schema 69 adds
`UnknownEventTime.fallback`, the receipt instant that places the record, and
`RecordsResults.eventTimeFallback`, the count of qualified records an event-time
read placed by receipt time (0 for a receipt-time read). Older endpoints fail the
exact schema discovery check.

The CLI and MCP JSON shape for record output (`records`, `record`, `context`)
renders the basis explicitly as `event_time.basis`. A `source` basis keeps
`instant` and its source evidence. A `receipt` basis adds `fallback_instant`
beside the original `state`, `reason` and `profile_id`, and never sets `instant`.
An event-time records read reports top-level `event_time_fallback` as a decimal
string. Correlation profile `sparrow-correlation/2` and the RDF vocabulary below
carry the same law.

The RDF fallback triples are written with each new record metadata graph. A store
last written before schema 69 backfills them online (see "Online store
migrations"): a resumable job (`event-fallback-rdf-upgrade`, through the shared
catalog upgrade pager) rebuilds
every retained record metadata graph that has no `eventTime`, adding
`eventTimeFallback` (the graph's own `receivedAt`) and `eventTimeBasis` and
nothing else. It reads only the graph, so a graph prepared by an unfinished
staged import, which has no public record yet, is covered too, and its later
publication carries the facts. Stored records are not read or changed. Each
maintenance page examines up to 64 graphs, rebuilding those that need facts
through one RDF writer, and commits them with its checkpoint in one synchronized
write, ending sooner at 16 MiB of stored graph manifests or of page batch, so it
holds the writer briefly and an interrupted backfill resumes after its last
committed page. A graph whose manifest rows already give its record `eventTime`
or `eventTimeFallback` is skipped from those rows and the dictionary identities,
without decoding its terms. Before the job starts, the store format moves to a fallback writer
family, `sparrow.store/132` to `/186`, which lifts each tracing, log time,
archive and intake family by 55 and keeps its switches, so older binaries refuse
the store rather than publish graphs without the fallback facts. The fence moves
when the store opens, and the service answers at once while the backfill runs.

### Owner-to-graph index (store format)

Every graph declaration has an owner-to-graph row, written and deleted with it,
which scoped SPARQL reads use to find their graphs (see the cost rule under
SPARQL below). Opening a store written before this index first moves the store
format to an owner-graph writer family, `sparrow.store/187` to `/241`, which lifts
each event fallback family by 55 and keeps its log time, archive and intake
switches, so older binaries refuse the store. A resumable online job
(`owner-graph-index-upgrade`, see "Online store migrations") then copies every
existing declaration in durable pages of 65,536, each committed with its
frontier, so a restart resumes after the last committed page. It reads only
declaration keys, so a graph prepared by an unfinished staged import is covered
before its publication. The build reads each declaration once and writes one
small row for it. Until it completes, scoped SPARQL keeps the whole-store plan.

### Online store migrations (schema 69)

Opening a store that owes the schema 69 migrations applies both writer fences
and seeds both checkpoints in one synchronized batch, then serves; the open
does not wait for either job. The joined maintenance worker runs them as one
duty, `store-counters` first (see "Store counters"), then `owner-graph-index`,
`event-fallback-rdf` and `source-summary` (see "Per-source counters and
artifact index"), one page per admission. It is selected fairly with buffered synchronization, pressure
maintenance, import reclamation, retention and the table rewrite. Below the
free-space floor it is skipped, as pressure maintenance, import reclamation and
retention are; buffered synchronization and the table rewrite still run there.
Each page commits its rows and its checkpoint in one synchronized batch through
the ordinary write path, so it is admitted against the data allowance and a
failed write latches durability uncertainty as any write does; a restart
resumes after the last committed page.
While pages succeed the next one follows at once, except that writers come
first: an ingest, a consume publication or an RPC write that is waiting for the
writer lock takes it before the next page does. Before a page the worker waits
while any writer is waiting, for at most the longer of 100 ms and the time the
previous page held the writer, so the migration still progresses under a
continuous stream of writers. The wait blocks on a condition the last waiting
writer signals; nothing polls. It runs apart from the worker's other duties,
which keep their own schedule, so it delays only the page. When that wait ended
while another duty ran, or the page is the first after a retry, writers may
have arrived since, so the worker waits again immediately before the page,
inline. A failed page commits
nothing, sets that migration's `failure` and is retried every 60 seconds until
it succeeds. Nothing is skipped. The daemon logs
`sparrowd: migration NAME progress pages=P done=D total=T watermark=W` every 64
pages and `sparrowd: migration NAME complete pages=P done=D` when a migration
finishes. Each line reports the counters the page's own batch updated and reads
nothing else; `total` is `counting` while `store-counters` seeds.

Schema 70 adds two flags. `Status.countsPending` (CLI and MCP `counts`,
`pending` or `exact`) is true while the `store-counters` migration seeds;
`MigrationStatus.totalPending` is true for every migration then. While pending,
`live_records`, `unique_payload_chunks` and `unique_json_projections` count
exactly the keys through the `store-counters` watermark, and each migration's
`done` counts its entries through both its own and the seeding watermark; none
of them is a total. A pending `total` is 0 on the wire and `counting` in CLI and
MCP. A query-only daemon never runs a page, so its counts stay pending until a
writable daemon seeds them.

`Status.migrations` (CLI and MCP `migrations`) lists every migration with
`name`, `done` and `total` (decimal strings), `watermark` and `failure`. `total`
counts the migration's source entries now in the store: every counted key for
`store-counters`, graph declarations for `owner-graph-index`, record metadata
graphs (counted by their provenance markers) for `event-fallback-rdf`,
`artifact:` and `record:` entries for `source-summary`. `done` counts those at
or before the watermark, so it moves with ingest and retirement as well as with
pages. The watermark is an opaque position, `start` before the first page and
null once complete, when `done` equals `total` (or `total` is still counting). A
store that never owed a migration reports it complete. Both numbers come from
the store counters, read from the same snapshot as the rest of the status reply
without the writer lock; no status read walks a migration's keys. The `migration-total:NAME` rows schema 69 images recorded are discarded
when the counters are fenced.

Readers are explicit while a migration is pending:

| Reader | Pending | Complete |
| --- | --- | --- |
| Scoped SPARQL (selection, pin, or a source, time or diagnostic filter) | The whole-store plan over every graph declaration, filtered to the scope's owners; the same rows | The owner-to-graph index, at the cost of the scope |
| Whole-live-store SPARQL | The whole-store plan | The whole-store plan |
| Record metadata graphs in a SPARQL scope | A selected record without a source event time whose graph is beyond the backfill watermark and lacks `eventTimeFallback` is counted in `SparqlCoverage.eventFallbackPending` (CLI and MCP `coverage.event_fallback_pending`); the answer's `complete` is false, and a request with `require_complete_index` refuses with `INDEX_NOT_READY`. A correlation reads only `eventTime` and `receivedAt`, so it neither counts nor refuses | Zero; no refusal |
| New graphs written during the migration | Owner-to-graph rows and fallback facts in the same batch | The same |
| Catalog event time (intervals, sorting, correlation clocks, `event_time_fallback`) | Derived from each record's `received_at`; independent of the RDF rewrite | The same |

While the backfill is pending, each candidate record (a selected record without
a source event time whose graph is beyond the backfill watermark) costs an
extra manifest point read and a subject lookup, both charged to the request
budget; a record at or before the watermark costs nothing extra. A large metadata scope can therefore refuse
with `RESOURCE_LIMIT` during the migration and succeed after it.

These fields were added within schema 69, before its first deployment. The
schema 70 flags below were added after it.

### Store counters (store format)

`status` and the migration report take no writer lock and walk no store-sized
key range. They read fixed counter rows, `store-counter:NAME`, each a
big-endian 64-bit count, from one RocksDB snapshot:

| Row | Counts |
| --- | --- |
| `records`, `retained-records` | `record:` entries; those neither evicted nor marked `retired` by expiry |
| `artifacts` | `artifact:` entries |
| `graph-declarations` | `rdf:g:` graph declarations |
| `record-graphs` | `provenance-rdf:record:` markers, one per record metadata graph |
| `payload-chunks`, `json-projections` | `ref:` and `projection-ref:` rows |
| `done:owner-graph-index`, `done:event-fallback-rdf`, `done:source-summary` | The migration's source entries at or before its checkpoint |

Status `live_records` is `retained-records` and keeps its deadline meaning:
records neither evicted nor retired by expiry. Expiry marks a record `retired`
in the batch that retires its evidence, when the record's deadline has passed
and no active pin covers it, so the count drops in that batch rather than when
the entry is deleted a day later. The count equals the deadline rule
(`retention_metrics` `live_records`) once expiry has run at the same instant;
between a deadline or pin release and the next expiry pass, and while the
archive gate holds an expired record, the record still counts. An entry expiry
retired before this marker existed is marked at expiry's next visit to it, so it
stops counting at the next expiry pass. This differs from the per-source `retained_records`, which count entries
until they are deleted. `unique_payload_chunks` and `unique_json_projections` are the two row
counts. `active_pins` reads the pin rows, which the pin caps bound, and
`physical_bytes` measures the database directory, as before. A status read
therefore reads a fixed number of keys plus one per pin row, whatever the size
of the store, seeding or not. The durability, pressure and table rewrite state
the same reply carries is kept by the writer to itself, which may hold it
across commits; when a change ends the writer publishes a small copy, and status
reads only that copy. A status reply therefore waits for no migration page,
write, pressure pass or rewrite step, at most for one short copy. It still
checks, without waiting, whether a writer panicked while holding the writer
lock, and then refuses with `STORAGE_FAILURE` ("catalog writer poisoned") as
every locked operation does, as do the three observed-status reads.

One owner, `store_counters::apply`, runs on every write batch after the
per-source counters and adds its row changes to the same batch. A key the batch
creates, deletes, evicts or restores changes each counter whose position covers
it after the batch. A batch that moves a migration checkpoint adds the keys
between the old and new position, read from the store before the batch. The two
together are exact whichever a batch contains, and a crash keeps or loses the
rows with the change they follow. A count a write would take below zero, a
missing row, a merge on a counted key or checkpoint, or a batch that writes the
rows itself refuses the whole write with `STORAGE_FAILURE` naming the row.
Nothing is clamped. A write batch holding a range delete is refused with
`STORAGE_FAILURE` before any counter follows it, because the batch iterator the
owners read never reports a range; only the open-time fences, which rewrite
these rows themselves, delete ranges.

Opening a store written before the rows moves its format to a store counter
writer family, `sparrow.store/297` to `/351`, which lifts each source summary
family by 55 and keeps its log time, archive and intake switches, so older
binaries refuse the store. The same synchronized batch writes every row as zero
and seeds the first online migration, `store-counters`
(`store-counters-upgrade`); a store with no counted key is complete at once.
The open walks no key range. Each page advances the checkpoint across up to
16,384 keys of one counted prefix, and the owner counts the keys it passes in
the same batch; every row's position is also capped at that checkpoint, so a
write during seeding keeps each row exact for the keys it covers. While the
counters seed, `status` and the migration report read the same fixed rows and
report them as pending (`counts` and each `total`, above), exact through the
seeding watermark; nothing recounts. The migration journal reports
`total=counting`.

### Per-source counters and artifact index (store format)

The store keeps three derived catalog row families exact in every write:

- `source-summary:<source key>`, one per source with retained input: the exact
  source label and its `retained_records`, `complete_artifacts` and
  `fragment_parents` counts. The row is deleted when all three are zero.
- `artifact-retained:<artifact id>`: the artifact's retained record count, and
  whether that count is full or partial. A full count covers every retained
  record of the artifact. A partial count belongs to a parent whose entry was
  deleted before the `source-summary` migration reached it, and covers only the
  records the migration's record phase has passed; the migration completes it,
  and the marker stays partial afterwards.
  The row exists while the artifact is listable: its entry is present and not
  evicted, or at least one of its records is retained.
- `source-artifact:<source key>:<received_at as 16 hex digits>:<artifact id>`,
  the per-source artifact index, present exactly while the artifact is
  listable.

The source key is the lower-case hex BLAKE3 digest of the exact source label,
the same key the source record index (`source-context-v1:`) uses. It is an
internal key and never appears on the wire.

One owner, `source_summary::apply`, observes every catalog write batch before
it commits and adds the row changes to the same batch. It reads the `artifact:`,
`artifact-retirement:` and `record:` changes in the batch, so ingest, import
publication, typed producer events, eviction, manifest retirement, expiry and
pin release all keep the rows exact without their own bookkeeping. A count that
a write would take below zero, a missing counter row for a source with counted
entries, a merge on an observed key, or a batch that writes these rows itself
together with catalog changes refuses the whole write with `STORAGE_FAILURE`
naming the source or the key. Nothing is clamped.

The counts follow retirement, not the retention deadline. A record past its
deadline that has not been retired, for example while the archive gate holds
it, still counts. Status `live_records` keeps its deadline meaning (see "Store
counters").

Opening a store written before these rows moves its format to a source summary
writer family, `sparrow.store/242` to `/296`, which lifts each owner-graph
family by 55 and keeps its log time, archive and intake switches, so older
binaries refuse the store. The same synchronized batch seeds the checkpoint of
the third online migration, `source-summary` (`source-summary-upgrade`), and
the service answers at once. Rows found under an older writer identity are not
trusted: that batch discards them and the migration recounts every source.

The migration runs in the maintenance duty after `owner-graph-index` and
`event-fallback-rdf`, with the same admission, failure, retry and journal rules
(see "Online store migrations"). It has two phases. The first walks `artifact:`
entries and counts each artifact from its entry and its `record_ids`, up to 512
artifacts or 16,384 records per page, always counting one artifact whole. The
second walks `record:` entries and counts the retained records of parents whose
entry was already deleted. Each page commits its rows and its checkpoint in one
synchronized batch, so a restart resumes after the last committed page. Writes
during the migration keep exact every artifact the migration has passed or
that the write creates; the migration reads the current state of the rest when
it reaches them. `total` counts `artifact:` and `record:` entries, and `done`
those at or before the watermark.

Readers while the migration is pending:

| Reader | Pending | Complete |
| --- | --- | --- |
| `sources` (`sourcesList`, CLI `sources`, MCP `sources_list`) | Refuses with `INDEX_NOT_READY` naming the `source-summary` migration and its watermark | Reads the counter rows |
| Live `artifacts` listing | Refuses with `INDEX_NOT_READY` the same way | Walks the per-source index |
| Explicit artifact selections, record reads, SPARQL, search | Served; they do not read these rows | The same |

The listings refuse rather than mark their counts incomplete because
`SourceSummary` and the live artifact page have no coverage field for the
counters, and adding one would change the wire schema, which this format
change does not do. Strict reads refuse in the same way while the schema 69
migrations are pending.

The migration's duration on the live store is measured on a copy of the newest
closed-store backup with `deploy/verify-backup.sh --measure-migration`, recorded
in [load qualification](load-qualification.md).

## 1. Common types and defaults

| Type | Contract |
| --- | --- |
| `StoreId` | Persistent identity of the store; returned with evidence and receipts. A fresh store has a different identity. |
| `ArtifactId`, `RecordId`, `SourceId`, `StreamId`, `PinId` | Opaque store-local strings, stable across restart. Clients preserve them verbatim and do not decode internal counters or RDF term IDs. |
| `PayloadId` | `blake3:` followed by the complete lowercase 256-bit digest of exact bytes. Separate from native projection/representation digests. |
| `Instant` | Cap'n Proto uses typed `Instant.unixMillis`; stdio MCP renders RFC 3339 UTC with `Z` and preserves stored precision. |
| `DayCount` | Positive integer; a day is 24 hours. Zero, negative, fractional, missing, or overflowing durations are rejected. |
| Byte/count values | Cap'n Proto uses unsigned integer fields. Stdio MCP renders logical `u64` offsets, lengths and counts as decimal strings; bounded page sizes and day durations are JSON integers. |
| `ByteRange` | Half-open `[offset, offset + length)` over a specified artifact/record byte sequence. Out-of-range reads fail; bytes are not silently clipped. |
| `RequestId` | Client-generated retry identity for a mutation. Reuse it with identical arguments on retry; conflicting reuse fails. |
| `Cursor`, `ResultId` | Opaque, bounded-lifetime handles bound to a store, snapshot, operation, and scope. They do not create investigation pins. |

Examples such as `artifact:17`, `record:42`, and `pin:8` illustrate opaque IDs;
they do not specify their generated wire grammar.

| Setting | Initial contract |
| --- | --- |
| Retention | Global 30 days after acceptance; earlier unpinned eviction under pressure. |
| Buffered durability | Two-second flush interval, configurable; expose lag and bounded-queue pressure. |
| Watch detection | Linux inotify first, with recursive startup/overflow/periodic reconciliation. |
| Completed-file readiness | `close_or_idle`: writable-close/publication candidates plus scan-stable pre-existing files, within a finished-file producer contract. |
| Completed-file quiet interval | 30 seconds, configurable per watch; observed reopen/write activity postpones consumption. |
| Pin duration | Required on every create/add/renew operation; no permanent or implicit pin. |
| Disk capacity | An explicit deployment budget and maintenance reserve are required. |
| RPC binding | Private Unix-domain socket for the daemon's group: store directory mode 0710, socket mode 0660, so a group member can connect but not list or read the store (2026-11-01); public Cap'n Proto TCP at `0.0.0.0:518`, explicitly authorized on 2026-10-08. Anonymous TCP peers may only discover, ingest and import; every other operation requires a shared-secret CHAP session (2026-10-27). An optional group-accessible ingest-only Unix socket (`ingest_socket_path`, schema 68, issue #10) admits members of `ingest_socket_gid` to the same producer operations and records each caller's `SO_PEERCRED` uid, gid and pid in provenance. |
| Pagination | Default 200 items, maximum 1,000, additionally limited by serialized response bytes. |
| Structured response budget | Default 256 KiB, configurable under a server maximum. |
| Byte-read chunk | Default 64 KiB; maximum 1 MiB per call. |
| Read/result lease | Default 60 seconds; server ceilings also bound operation deadlines and retained handles. |

Page counts are upper bounds, not a guarantee that a byte-limited response can
contain that many items. Never truncate a scalar RDF binding or alter a row to
make it fit. Refuse an oversized row with `RESULT_TOO_LARGE`; callers can select
IDs/spans and retrieve bytes separately. Result leases expire even when clients
stop consuming them. A continuation must remain within the original deadline;
it cannot hold an unbounded native snapshot through repeated calls.

Each mutation receipt states `buffered` or `durable`, its operation/request ID,
and the IDs it published. A durable receipt means the operation's dependencies
have crossed the durable storage boundary. A buffered receipt permits loss on a
crash before that boundary. Retry receipts are retained for a bounded advertised
window, at least 24 hours by default; external clients must not infer exactly-once
delivery beyond that window. Import journals/checkpoints retain their source
generation identity for as long as recovery can still require it.

## 2. Evidence, scope, and query outcomes

### Evidence records

Artifact/record summaries expose:

- Store and occurrence ID, occurrence IRI and graph IRIs, source/stream ID, receipt time, ordinary deadline,
  event time with its `source` or `receipt` basis, media/format, and source name.
- Payload identity, byte length, source byte range when applicable, and whether
  the complete artifact remains available.
- Projection state and codec/profile identifiers, parsed diagnostic fields,
  active protecting pin IDs, and structured diagnostics.

Projection states are `pending`, `ready`, `partial`, `failed`, or
`not_applicable`. They describe derived data rather than raw ingestion success.
`partial` identifies incomplete derived coverage with affected records/diagnostics.
It is not permission to claim a partly retained file has complete original bytes.

Evidence links refer to IDs and byte spans, not arbitrary server paths. Source
paths can appear as provenance; they are not read capabilities. Reads of binary
or invalid UTF-8 data return base64 bytes. Text rendering is explicitly selected
and reports its encoding; raw retrieval remains exact. Retention errors identify
expired/evicted evidence when that state is still available, or `NOT_FOUND` once
its metadata has also been reclaimed.

### Scope

Operations select one scope kind:

```json
{
  "kind": "live",
  "filter": {
    "received_from": "2026-10-01T00:00:00Z",
    "received_to": "2026-10-07T00:00:00Z",
    "service": ["worker"],
    "severity": ["ERROR"]
  }
}
```

```json
{
  "kind": "pin",
  "pin_id": "pin:8"
}
```

```json
{
  "kind": "selection",
  "artifact_ids": ["artifact:17"],
  "record_ids": ["record:42"]
}
```

`live` is the default and includes all currently eligible data, including pins
older than the ordinary window. Time filters are explicit half-open intervals.
All kinds may include a filter. Filter dimensions combine with AND; members of
one filter array combine with OR. Present empty arrays and empty selections are
invalid rather than silently meaning the entire store. Filters support source
IDs, service, host, severity, receipt time, event time, and trace/span IDs; more
expressive conditions use SPARQL.

Temporal reads select receipt time (`received_at`) or source event time
(`event_time`) explicitly. An event-time interval places a record whose source
time is unknown at its receipt time, marks it with basis `receipt`, and reports
both the unknown count and `event_time_fallback`, the count of selected records
placed by receipt time (schema 69). A known source instant is never replaced by
receipt time. A source profile states which field and representation supplies
event time. Chronological sorting uses the selected placement and occurrence ID
as a stable tiebreaker.
Timestamp mappings, lexical/logical field matching, and text interpretation are
advertised profiles rather than guesses based on names or file extensions.

### Plain-log timestamp captures (source schema 60)

`source-time-define REQUEST.json` and MCP `source_time_define` accept exactly one
JSON `path` or one `log_capture`, with an explicit representation. The existing
`source-time SOURCE POINTER REPRESENTATION` command shares the JSON binding owner.
For a log whose first token is an RFC3339 timestamp:

```json
{
  "source": "application-log-v2",
  "representation": "rfc3339",
  "log_capture": {"pattern": "([^ ]+) .*", "group": 1, "flags": "s"}
}
```

`sparrow-log-time/1` uses PurRDF's native XPath 3.1 law, dated 2017-03-21.
The complete original UTF-8 record must match, including retained line terminators.
A native noncapturing wrapper preserves group numbering. Group 0 selects the
whole record; groups 1..32 select a declared capture. Only native `i` and `s`
flags are admitted; `s` allows the body to include a terminal newline.

The capture uses the existing timestamp codecs: `rfc3339`, `unix_millis` or
integer `unix_seconds`. RFC3339 must denote an exact millisecond instant. No
timezone, epoch unit, missing date or timestamp format is inferred. Known evidence
uses the native group number as `occurrence` and exact original artifact byte
bounds, including UTF-8 prefixes and earlier records. Receipt time and retention
remain unchanged.

No match, absent optional capture, invalid UTF-8, invalid instant, unavailable
materialized input and incompatible non-syslog format retain explicit unknown
reasons: `log_no_match`, `log_capture_missing`, `log_invalid_utf8`,
`invalid_instant`, `log_input_unavailable` and `log_wrong_format`. Raw bytes remain
authoritative. Syslog retains its protocol timestamp interpretation. Event-time
queries place unknown clocks at receipt time, marked with basis `receipt`, and
report their count. Strict index readiness does not promise every clock is known;
a source time that depends on a failed JSON projection is not ready, so strict
event-time reads and strict event-clock correlation refuse it with
`INDEX_NOT_READY`.

Patterns admit at most 4096 UTF-8 bytes. Compilation has finite native ceilings
of 100,000 steps, 16,384 nodes and 65,536 construction slots. Each match has
2,000,000 steps, 4096 pending states and 65,536 live slots. Native operational
refusals remain `RESOURCE_LIMIT` failures and prevent successful ingestion-batch
publication; they are not negative matches or partial success. Invalid syntax,
unsupported controls and missing declared groups refuse the definition. PurRDF's
single-program cache owns reuse. Catalog reads verify identity/control bounds
without compiling; definitions and ingestion perform native admission/matching.

Profile identity covers source, whole-record law, pattern, flags, group and
representation. JSON identities are unchanged. Definitions do not reinterpret
earlier records or split a staged import's captured interpretation. The existing
1024-source registry ceiling applies to both selectors. Returned profiles expose
an `extraction` descriptor; the legacy `path` is empty for a log capture.

The first log definition atomically publishes its interpretation marker and
writer fence 88..98, preserving the corresponding prior authority family 77..87.
On a schema 69 store, already lifted by +55, the same definition moves 132..142
to 143..153.
Other stores keep their format. Later watch/receiver/pressure/retirement
transitions preserve the fence; marker/format disagreement fails on open. Older
writers refuse the new family. Source time definition/lookup RPCs require schema
60 explicitly; the CLI/MCP adapter performs discovery and supplies the version.

The public native pattern API is in `purrdf-core`, rather than the facade.
Sparrow directly references that same pinned kernel package already in the
resolved graph, with no version or transitive dependency addition. There is no
local regex or timestamp parser. Runtime adoption is recorded separately in
Development runtime.

The initial agent investigation surfaces include `keys_list`, `values_search`,
and `records_context`, as detailed in the
[implementation plan](implementation-plan.md). JSON fields use native RFC 6901
pointers and occurrence identities. Key presence counts distinguish records from
value occurrences; absent fields, nulls, and duplicate members remain distinct.
Value operators identify their lexical or decoded semantics. Field and text
predicates intersect within one selected record; same-array-element constraints
also bind the parent occurrence. A decoded search view does not alter the native
ordered JSON graph or its original-byte evidence.

Selection expands complete artifacts to their retained records and dependencies.
Record selection includes its minimal provenance and shared content projection.
Missing/expired selections are reported, not omitted without evidence. Pin names
can be resolved using `pin_get`; subsequent reads use the returned stable
`pin_id` to avoid accidentally following a reused name.

Schema 13 implements membership scopes for key/value, text, combined record,
and context reads. The generated `Scope.selection` union defaults to `live`,
accepts a list of typed `Evidence` references, or a stable `PinScope` with
store ID and optional positive revision. Foreign-store references produce a
typed `STORE_MISMATCH` failure. All membership validation precedes source/time
filtering; an expired complete artifact cannot be substituted by its pinned
individual records. Reclaimed references return `NOT_FOUND`; expired or evicted
catalog references return `EVIDENCE_UNAVAILABLE`; expired pin collections return
`PIN_NOT_ACTIVE`. A supplied stale pin revision returns `CONFLICT`.

CLI queries accept `--scope-file FILE`, a regular native-JSON control file of
at most 256 KiB. MCP query tools accept the same closed object as `scope`:
`{"kind":"selection","store_id":"store:...","artifact_ids":["artifact:..."]}`
or `{"kind":"pin","store_id":"store:...","pin_id":"pin:...","expected_revision":"1"}`.
Live scope omits `store_id`; supplied arrays must be nonempty. Up to 1,000
explicit references are accepted, and overlapping artifact/record references
are deduplicated. Scope filters presently use the existing outer source,
receipt-time and combined-query event-time fields. Schema 20 adds a closed
`filter` object to every scope kind, currently requiring `source_ids`:
`{"kind":"live","filter":{"source_ids":["worker","scheduler"]}}`.
These exact logical source IDs use the original ingestion spelling; there is no
case folding or source-name inference. Accept 1..128 alternatives, each 1..512
UTF-8 bytes. An absent filter admits all sources; an empty array, empty ID or
unsupported dimension fails with `INVALID_ARGUMENT`. Alternatives combine with
OR, and intersect the outer exact `source`, receipt interval and resolved
membership. Duplicate alternatives never duplicate records or counts. Context
remains source-local and refuses an anchor excluded by the filter.

Live record, text and JSON discovery
use a forward temporal-index seek at the inclusive receipt start and a native
exclusive iterator bound at the receipt end. They do not load the first record
beyond that interval. Invalid catalog/index data inside the interval remains an
operational failure; an out-of-interval record is not query input.

Schema 21 also supports `service`, `host`, `severity`, `trace_id` and `span_id`
arrays in the same closed filter object. Each dimension uses the same alternative
bounds, OR within an array and AND across dimensions. Before ingesting that
source, define explicit native JSON pointer mappings with `diagnostic_define`,
or `sparrow SOCKET diagnostic-define SOURCE MAPPINGS.json`. The mapping object
is a nonempty subset of those five names, with one concrete RFC 6901 pointer per
dimension. `diagnostic_get` and `diagnostic-get SOURCE` expose the immutable
profile. Identical definitions are idempotent; conflicts require a new source ID.
There are at most 1,024 immutable diagnostic profiles, independently of time
profiles, and definitions obey the shared physical control-admission gate.

The declared interpretation is exact native decoded string equality. It makes
no severity case, integer, host or trace-ID guesses. Known values are nonempty
strings of at most 512 UTF-8 bytes. Their profile, native occurrence and original
string-body byte span are captured atomically with record publication. Duplicate
mapped occurrences remain ambiguous even when their strings are equal. Missing
fields, wrong kinds, decode failures, oversized/empty values, non-JSON inputs and
failed projections retain explicit unknown reasons. Raw bytes and ordered JSON
projections remain unchanged. Profile creation never reinterprets existing
records, and staged preparation captures profile presence or absence once before
any record; later definitions cannot split an import's interpretation.

All five query surfaces filter captured evidence before qualification/ranking;
context applies the same predicate to anchor and same-source neighbors.
Schema 22 returns proven matches by default and reports uncertain scope membership
separately. A known mismatch excludes a record even when another dimension is
unknown. Otherwise, unknown/unprofiled evidence contributes to
`coverage.diagnostic_scope.unknown`, never to proven match rows. The receipt
reports evaluated/matched/excluded/unknown visited units and whether membership
is complete. Eligible and qualified totals are `null` when scope membership or
traversal prevents exact counts; `known_eligible_records` and
`known_qualified_records` expose covered lower bounds. Key presence/occurrence
counts cover proven membership and are lower bounds when coverage is incomplete.
Rankings identify their covered corpus and report incomplete membership; scores
do not certify ranking over unknown records. An output row limit alone does not
erase totals obtained from a complete membership scan.

Every surface accepts `require_complete_index=true` (CLI
`--require-complete-index true`). Unknown diagnostic membership then refuses
with `INDEX_NOT_READY`, without successful rows; JSON discovery also refuses
unavailable applicable projections. Context anchors must be proven members even
in default mode; uncertain neighbors remain visible in coverage. Operational
storage failures always refuse with no successful prefix. Membership references
are validated before filtering, and all existing scan/response ceilings remain.

`Record.diagnostics` and chronological or ranked combined-query rows use a typed snapshot
with a profile reference and per-dimension known/unknown union. The generated
`diagnosticDefine`/`diagnosticGet` methods use typed mapping lists. Each captured
profile produces an occurrence-owned diagnostic RDF graph, separately named
from metadata and shared content; it includes interpreted values or reasons,
profile references, native occurrences and byte evidence. RDF inspection exposes
the graph and exact persisted primary-row count. Pin retention, expiry, eviction
and canceled private preparation use the existing graph retirement owner.
Scoped native views include it without resident-corpus replay.
`Store::with_rdf_record_layers` accepts typed `RdfGraphLayers` flags for
`metadata`, `diagnostic` and `content`, selecting their physical graph owners
through the existing bounded native snapshot. Shared content owners remain
deduplicated. `with_rdf_records` delegates to this implementation with all three
layers, preserving its strict content-readiness behavior. A selected content
projection must be ready; an excluded content projection's failed or corrupt
state does not block metadata/diagnostic reads. Record identity, eligibility,
pins and read admission remain mandatory even for an empty layer selection.
`Store::with_rdf_scope` adds live, explicit artifact/record and revision-guarded
pin membership with the shared source/time/diagnostic filters. Its coverage
counts record occurrences independently of shared graph owners. Permissive reads
expose ready content with failed/not-applicable coverage; strict reads refuse
unavailable applicable content or unknown diagnostic membership. Exhausted
capture admission refuses the whole selection, and the page-size limit never
clips graph membership. The writer lock is released before native execution;
membership and graph values remain on the same captured native snapshot.
Unbound statement reads seek the existing graph-leading indexes for selected
graph declarations, then merge their heads in physical SPOG order. The same
native-identity deduplication preserves named statements and the default union.
This uses memory proportional to active graph cursors; it does not replay a
resident RDF corpus. Source requests, row visits, cursor buffers and deadlines
remain admitted and failures remain sticky across the native read fence.

`Store::sparql_query` passes typed substitutions and governors to PurRDF's native
SELECT, ASK, CONSTRUCT and DESCRIBE implementation. Complete, certified partial
and diagnostic/failure outcomes retain native ownership and certainty. UPDATE is
refused by native read preparation; federation is disabled. These are core
integration surfaces. Schema 51 exposes the same native read through typed
`Sparrow.sparqlQuery`, CLI `sparrow ENDPOINT sparql REQUEST.json`, and MCP
`sparql_query`. Deployed schema 61 also provides the measured explanation surface
below. Native stored-text relations remain unfinished.
Native correlation is implemented in source schema 58, as specified below.
Recordless artifact metadata is implemented and deployed in
schema 52, as qualified below.

The core `sparql_read`/`sparql_read_next` pair publishes immutable native captures
through the existing finite catalog lease owner. Pages share the native outcome
and its original admission; they do not build a second RDF store. Native SELECT
solutions and auxiliary graphs, ASK values, graph declarations (including empty
graphs), ordinary quads, reifier bindings and annotations remain available through
borrowed typed items. Internal term IDs belong only to their accompanying native
dataset and must be resolved there before transport.

Native outcome certainty remains authoritative: item counts describe transport
units, never proven-answer cardinality. Unknown outcomes expose no items, and
query failures retain their native diagnostics without answer items. Certain and
at-most results retain their original certificate and positional-prefix evidence.
The typed `ResultPage.sparql` arm and generic `result_next` resume that same
native capture. The installed development image is identified separately in
`development-runtime.md`; schema additions in source do not imply deployment.

The native registry admits at most eight handles and 8 MiB of aggregate retained
capacity, with a fixed 60-second lease from capture start. Page size is at most
1,000 items and 256 KiB of conservatively admitted structured output, including
metadata and worst-case JSON escaping. Native traversal and term/diagnostic
capacity helpers supply the sizing inputs; Sparrow owns only transport charges.
Each solution includes the escaped projection-name charge because native SPARQL
JSON repeats those names in every row. Result JSON decoding derives its nesting
and input bounds from the admitted term forest and page bytes. Binding-input
depth remains independent; a deeper native result that fits egress admission
does not inherit the binding-input depth limit.
An oversized indivisible item or receipt refuses publication, never clips RDF.
`result_release` releases these captures; status and maintenance share expiry
accounting. Core status reports `sparql_read_handles`, `sparql_read_bytes` and
their effective maxima/lease. No page read renews a lease or pins evidence.

The graph request JSON uses the shared scope object, source/receipt-time filters,
`page_size`, `scan_budget`, `require_complete_index`, optional `base_iri`, and
`layers` flags (all three enabled by default). `bindings` is an array of
`{"name":"predicate","term":<PurRDF canonical term JSON>}`; native projection
validation owns its term semantics. Names are at most 128 UTF-8 bytes, with at
most 64 bindings. JSON member order does not change the native binding identity.
The transport admits at
most 512 expanded transport nodes, 256 KiB term admission, and
native projection nesting limit 16. Example:

```json
{"store_id":"STORE_ID","query":"SELECT ?record WHERE { ?record <urn:sparrow:vocab:payload> ?payload }","page_size":100,"layers":{"content":false}}
```

Schema 53 identifies host read admission as `sparrow-rdf-source-v2` in
`limits.source_profile` and `profile_ids`, independently of the native governor
profile. Capture admits prospective bounded index rows and the actual pinned
catalog body/key bytes before native catalog decoding. Catalog entries retain
their original width, 64x memory charge, scan and time checks. Body admission uses
the same sticky source account without counting a second logical request or row.
All source and native governor ceilings remain unchanged. Terminal iterator
advances are admitted too; these counters describe admitted work, not physical
RocksDB block reads or process RSS. Unknown/missing source profiles refuse at the
agent boundary instead of claiming the current law.

A SPARQL, SPARQL-explain or correlation request's cost follows its scope's graphs
and rows, not the store's size. When the scope is narrower than the whole live
store (a `selection` or `pin` scope, or a `live` scope with any source,
receipt-time or diagnostic filter), the read enumerates the graphs of the captured
owners from the owner-to-graph index and reads only those graphs' rows, in the
same global order and with the same union-default deduplication as the
store-wide plan, so answers and certainty do not change. Requests and row visits
are charged under the same law. A request over the whole live store reads the
store's rows by definition. The host ceilings are unchanged.

Record and empty-artifact timelines share one native borrowed iterator. It
checks key/identity widths before copying or loading a catalog and preserves
native status errors and half-open receipt bounds. This removes the previous
512-KiB charge for every tiny catalog body without relaxing index integrity.
This source change passed the full local gate and is installed as schema 53;
live adoption evidence is recorded in Development runtime.

Schema 54 and source profile `sparrow-rdf-source-v3` add independent
populated-artifact metadata, now installed on the development server with the
qualification evidence in Development runtime. Live graph scopes
include complete eligible artifacts on their own receipt timeline. Explicit
whole-artifact and pin selections include that metadata alongside their record
expansion; selecting an individual record does not select its whole parent.
Diagnostic filters include a populated artifact only when its eligible records
prove a match in the same capture. Empty artifacts retain explicit unknown
diagnostic coverage and strict refusal. Metadata-layer exclusion suppresses all
artifact quads without erasing known membership.

The new timeline and receipt-bearing RDF ownership marker are published atomically
with the catalog. Direct ingestion shares its existing record RDF dictionary
writer; staged imports publish the parent after validating their complete byte
cover. Expiry and eviction retire the parent graph and timeline through that same
reference owner, while individual record pins retain their original evidence.
Startup fences older writers with store formats 55 through 65, preserving each
original authority family. One-owner migration pages retain durable progress and
original clocks. Native streaming manifest headers supply scalar metadata without
materializing large record/chunk arrays; their input and scalar allocations share
the existing finite catalog account. Record catalog bodies retain their 64x DOM
charge. The source/request/row and native governor ceilings remain unchanged.

Historical test fixtures retire artifact-only graphs through a dev-only feature
entry into that production owner before removing feature checkpoints. The shared
test-support crate selects the feature; ordinary runtime builds do not.

Optional `limits` counters use unsigned decimal strings: `fuel`, `answer_rows`,
`intermediate_cells`, `scratch_bytes`, `live_bytes`, `requests`, `io_bytes`,
`row_visits`, `source_lease_ms`. Defaults and actual effective limits are echoed
in each page. Remote requests are always zero. Fuel zero is a real native
governor setting, not a request for an unbounded query.

Typed RDF values use a flat child-before-parent term forest with page-local
UInt32 links, preserving blank scope, datatype, language, direction and nested
triple structure. Unreachable nodes, forward/cyclic references and expanded DAG
amplification refuse admission. Persistent native dictionary IDs never cross
the wire. Graph items distinguish primary/auxiliary streams, named declarations,
quads, reifiers and annotations. Query diagnostics retain native severity,
location, detail, presentation arguments and exact 128-bit capacity values.

Agent `rows.native_result` uses PurRDF's own SPARQL JSON serializer; graph results
carry its extended N-Quads envelope. Explicit `named_graphs` and
`auxiliary_graph` preserve declarations and SELECT auxiliary graphs that the
native JSON result envelope does not otherwise include. Each object describes
one page fragment, not the entire leased answer. Native `Certain` remains
`answer_certainty=certain`, while `AtMost` uses `candidates` instead of `rows`.
Unknown results expose a barrier without answer/candidate data. Page counts
count transport items, never proven answers. Query failures preserve the receipt
and native evidence, set MCP `isError`, and make the CLI exit unsuccessfully after
printing its failure JSON. Releasing or expiring the handle releases only replay
memory. It never changes evidence retention.

Storage formats 13/14/15/16 fence older writers for the basic, consumption,
watch-registry and handoff states. Existing 9/10/11/12 stores durably advance to
the corresponding state before serving, retaining their completed recordless
index and store identity. Earlier recordless upgrades also preserve their
consumption/watch/handoff authority. Absent historical diagnostic fields remain
unprofiled. Schema 22 changes query counts to the typed `Count.known`/`unknown`
union and adds scope coverage. Every query request carries `Scope.schemaVersion`;
older/absent versions refuse with `UNSUPPORTED` before the changed reply is encoded.
CLI/MCP perform current discovery before every query, so older endpoints cannot
silently omit scope coverage or interpret unknown counts as zero. Storage remains
at formats 13/14/15/16 because this changes reads rather than persisted evidence.

The generated contract uses `Scope.sourceIds :OptionalSourceIds`, with distinct
absent and present-list states. CLI scope files and MCP use the same native JSON
decoder and typed encoder. Before sending a filtered read, these adapters require
current discovery through the native client; an older daemon is refused with
`UNSUPPORTED` rather than silently ignoring the added wire field. This discovery
check uses bounded `discover`; explicit operational `status` reads fixed
counter rows and the pin rows (see "Store counters").

Membership expansion permits at most 100,000 direct pin members and 100,000
record visits, including overlaps, and retains only bounded IDs/timestamps.
Exceeding these bounds fails before any result is published. This resolution
work is additional to `scan_budget`, which governs the subsequent chronological
candidate scan; it is not an all-I/O budget. Explicit field/text reads seek only
resolved candidates, ordered by receipt time and ID. Context reads continue
bounded directional scans of the source catalog, excluding unselected records
and reporting a scan frontier when the budget is exhausted. The writer lock
holds membership stable throughout each operation. Schema 28 adds immutable
result captures and cursors for `recordsSearch`, including its same-record field,
source-time and native ranked-text forms. Schema 29 routes standalone text search
through that same capture. Schema 30 routes native key/value discovery through the same result lease and
bounded native JSON page body. Schema 56 adds frozen scope receipts for the
paged record/text/key/value pipeline described below; other query families retain
their separate receipt contracts.

### Exact JSON key path restriction

Schema 34 adds OptionalText path to keysList. CLI `keys --path POINTER` and
MCP keys_list.path restrict discovery to that exact native RFC 6901 path.
Absent or null selects every observed path; the empty string explicitly selects
the document root. `/` selects an empty member name. Escaped names retain their
native spelling: `/a~1b` addresses a member named `a/b`, `/m~0n` addresses `m~n`,
and `/a~01b` addresses `a~1b`. There is no implicit subtree, prefix or wildcard
match. Field/value predicate syntax uses the same public PurRDF pointer parser.
Invalid pointer syntax refuses INVALID_ARGUMENT rather than appearing as no hits.

Keys and values share the existing occurrence predicate and one typed input
admission owner. JSON query paths admit at most 65,536 UTF-8 bytes before the
server allocates an owned spelling. The core enforces the same limit for direct
JSON discovery calls. The updated host capture profile is
sparrow-query-capture/7; QueryLimits.queryPathBytes reports that ceiling for
JSON captures and explicit absence for other query phases. Stored evidence,
native projection identities and storage formats are unchanged.

Key counts still aggregate all matching native occurrences across the selected
membership before pagination. Duplicate members contribute to occurrence counts;
each containing record contributes once per path/kind to record presence.
Scope coverage continues to describe selected records independently of the path
restriction. Captured selectors retain the exact optional path, including root
versus absence. Changing it on an original continuation refuses CONFLICT;
result_next resumes the original frozen selection without repeating the path.
Later ingestion, pin changes and evidence retirement do not recompute counts.

### Record, text and JSON result pagination

Typed `RecordQuery.cursor`, CLI `records --cursor CURSOR --page-size N`, and MCP
`records_search.cursor/page_size` use one captured result. `Scope.limit`, CLI
`--limit`, and MCP `limit` are page item ceilings for this operation; the agent
adapters refuse supplying both page-size spellings. Schema 29 gives `textSearch`
the same page item semantics through typed `TextQuery.cursor`, CLI
`text QUERY --cursor CURSOR --page-size N`, and MCP `text_search.cursor/page_size`.
Schema 30 adds typed cursors and page receipts to keysList and valuesSearch.
CLI keys/values and MCP keys_list/values_search accept the same cursor and
page-size options; their limit is also a page ceiling. Key counts aggregate the
entire admitted membership before pagination, preserving duplicate occurrence
counts separately from record presence. Values retain every matching native
occurrence, including duplicate members. JSON capture uses the existing native
fact matcher, with independent bounded record and value visits; exhausting either
scan budget refuses before publishing a page.
The daemon uses the complete-capture mode of the
existing record matcher and native ranker, rather than paging a truncated prefix.
Candidate scan exhaustion, more than 10,000 matches, and result capacity exhaustion
refuse with `RESOURCE_LIMIT` before a page or handle is published. Diagnostic and
applicable index uncertainty remains explicit in the original query receipt.

`Keys.page`, `Values.page`, `RecordsResults.page` and `TextSearchPage.page` use the shared typed catalog
capture receipt; `TextSearchPage.result` carries the original typed text results.
Agent output
keeps it in `page` alongside the original semantic `receipt`; its store/snapshot,
as-of, absolute deadline, total captured rows, page start and continuation describe
pagination. `total_items` counts captured proven rows, while semantic matching
counts retain their existing unknown/lower-bound laws. Pagination alone does not
mark query execution limited. Query receipt, coverage, time basis, ranking corpus,
native scores and witnesses are frozen across pages. Reading summaries neither
protects evidence nor renews the lease; raw bytes independently check retention.

A capture stages rows in an anonymous file with a maximum 64 MiB disk reservation,
subject to store admission. Retained offsets, metadata and selector capacity remain
bounded by 2 MiB, with 32 KiB per row and 32 KiB metadata ceilings. Keys aggregate
complete counts in a separately charged 32 MiB working map before writing sorted
rows; `QueryLimits.keyAggregationBytes` reports this ceiling only for keys.
Values stage each matching native occurrence in receipt-time/record-ID order,
then native occurrence order. PurRDF's native
codec owns encoding and page reconstruction; no database snapshot versions are
retained. Pages conservatively charge duplicated typed record/text rendering and
may return fewer than the requested 1..1000 items to fit 256 KiB. Variable byte
boundaries are verified by the shared lease owner. Eight handles and 8 MiB retained
result capacity are shared by record, standalone text, key and value reads across Store clones,
with fixed 60-second wall/monotonic
expiry. Status exposes record result handle counts, retained bytes and ceilings.

Cursors bind the operation as well as every membership, predicate, text query,
time interval, strictness and limit. Cursors cannot cross record, text, key or value operations, even when their
predicates otherwise agree.
Continuations replay captured rows after pin mutation/release and evidence
retirement; fresh reads revalidate current evidence. A changed query refuses with
`CONFLICT`, an invalid page boundary with `INVALID_ARGUMENT`, and expired, unknown
or previous-owner handles with `RESULT_EXPIRED`. Current schema 34 is required;
storage formats remain unchanged. `scanned` records candidate visits, not all I/O:
reference expansion, field visits and posting visits retain their independent
documented bounds and original coverage evidence.

### Artifact match grouping

Schema 43 adds explicit artifact grouping to `recordsSearch`: typed
`RecordQuery.groupByArtifact`, CLI `records --group-by-artifact true` and MCP
`records_search.group_by_artifact=true`. Grouping uses the same complete captured
record matcher, including source/time/field predicates, diagnostic coverage and
native ranked text. It does not rescore, reorder, merge or replace independent
record occurrences, or invent a file-level score.

`RecordsResults.artifactGroups` is a typed absent/list union. Agent output uses
`artifact_groups=null` when grouping was not requested and a list when it was,
including an empty list for no captured matches. Each group contains an opaque
`artifact_id`, exact `captured_matches` across the frozen capture (a UInt64 decimal
string in agent output), and `page_indices` into this page's unchanged `rows`
(bounded UInt32 integers). Groups are ordered by artifact ID; indices preserve
the record matcher order. Text scores and evidence remain in the original text
result. The count is of proven captured record occurrences, not all file lines,
all possible matches under incomplete indexing, or a complete file's byte length.
The original certainty and coverage govern those distinctions.

A group can span pages. Its count is frozen across continuations, while its
indices refer only to the current page. An artifact absent from a page has no
group on that page. `page.total_items` and `page_start` remain record positions,
and page size remains a record ceiling. Counts are charged to existing metadata
and capture limits; grouping does not enlarge them or bypass broad-capture
refusals. The grouping flag is part of cursor identity; changing it refuses
`CONFLICT`. Cursor-only continuation, lease release and expiry use their existing
owners. Artifact grouping creates no pin or complete-file download promise;
retrieve the original artifact or record bytes through its concrete evidence ID.
Schema discovery now requires 44; the storage representation is unchanged.

### Cursor-only continuation

Schema 31 implements typed ResultNextQuery and resultNext. CLI
result-next STORE_ID CURSOR and MCP result_next require only the original
store identity and cursor. They resume the operation, selector, page size and
scan ceilings stored in that capture; they accept no replacement predicates or
limits. ResultPage is a typed union over record, standalone text, key, value,
artifact, source and pin pages. Pin pages retain their collections/members union.

Every branch uses the same wire encoder and agent renderer as its original
operation. Query replies now explicitly identify their operation; continuation
output is identical to replay through the original operation with its cursor.
No membership, ranking corpus, source watch inventory or pin name is re-resolved.
The shared lease owner parses the cursor and validates the original variable
page boundary. Reading never renews a handle or evidence retention and never
creates another handle, except that a live artifact listing reads and publishes
its next page (schema 66). Foreign stores refuse STORE_MISMATCH; malformed/non-page
positions refuse INVALID_ARGUMENT; expired, unknown and previous-owner handles
refuse RESULT_EXPIRED. These are typed failures with no successful prefix.
Schema discovery refuses older daemons before requesting continuation.


### Frozen query capture provenance

Schema 32 extends Receipt with an optional typed QueryCapture for successful
record, standalone text, key and value result pages, including cursor-only
continuations. It binds the original CatalogReadReceipt, role-tagged native
profile identities, explicit native governor absence and typed QueryLimits.
The native JSON projection fingerprint comes from the same public profile owner
as ingestion. Analyzer, ranking and corpus identities come from the captured
native ranking result; they are absent when text ranking did not run.
The separately identified host admission profile owns Sparrow's phase ceilings.

CLI/MCP semantic receipts include snapshot_id, as_of, absolute lease deadline,
execution_status, profile_ids, native_governor, limits and stop_reason. The
original execution, certainty and coverage remain unchanged. Inactive phase
budgets are null, rather than fabricated zero. Limits include candidate,
native JSON occurrence, field and posting visits; independent membership member
and record visits; text membership, text input/terms and compiled field admission;
and result item, row, metadata, byte, page, registry and lease ceilings.
Numeric UInt64 ceilings use exact decimal strings; requested page_size remains
a bounded integer. These are phase-specific ceilings, not an all-I/O or RSS claim.
JSON capture refusals identify the exhausted record-visit or native JSON
occurrence budget and its effective ceiling. Both use the requested
`scan_budget`; callers can narrow scope or explicitly raise that request up to
100,000. Result-item and response-byte refusals require narrower scope. No failed
capture publishes a page or a partial-success receipt.

Semantic receipts remain frozen across pages; page position and cursor stay in
the existing separate page object. QueryCapture reuses the typed catalog page
receipt on the wire. Capture evidence is admitted with result metadata and
decoded by PurRDF's native JSON codec. Retention, pin changes and profile
declarations do not recompute it. A successful capture has complete execution
and null stop_reason; capacity/scan exhaustion refuses before a page is published.

These operations do not invoke a native query governor, and native_governor is
explicitly absent. Existing incomplete index/scope certainty is preserved;
it is not relabeled as a native certified partial answer. Native graph queries
separately preserve PurRDF governor certificates through the schema-51 graph
surface. Frozen scope receipts extend these host captures in schema 56 and
graph/context in schema 57. Public measured explanations and stored-text SPARQL
relations remain unfinished, as recorded in [native integration gaps](purrdf-integration-gaps.md).


### Single context capture admission

Schema 33 adds a typed ContextCapture to records_context. Each successful
operation has a fresh snapshot_id and captured as_of, the host admission
profile sparrow-context-capture/3, explicit native governor absence, and
ContextLimits. Context is one complete reply without a continuation or retained
result handle. CLI/MCP receipts report has_more=false and next_cursor=null;
there is no read lease deadline to renew. Original execution, certainty,
directional frontiers, diagnostic coverage and continuity_unknown remain visible.

Chronological visits remain independently bounded per direction. The source
timeline excludes unrelated sources before catalog decoding or visit admission.
Catalog decoding charges the anchor and every visited same-source neighbor,
including members skipped by the requested membership or diagnostic scope,
against one 32 MiB conservative allowance across both scans. The shared catalog
owner charges encoded catalog bytes at its established expansion factor before
decoding. One 256 KiB response allowance includes the anchor, both directional
outputs and a metadata reserve. Native encoded record rows use the same
conservative typed/agent envelope charge as paged searches. Byte exhaustion is
RESOURCE_LIMIT with no successful result; it never publishes a byte-limited
prefix. Request fewer neighbors or narrow the acceptance interval to reduce work.
These are host admission allowances, not a measured RSS or all-I/O guarantee.

The source timeline is a disk-backed derivative of the authoritative record and
primary time catalogs. Every store open clears and rebuilds it with the shared
bounded catalog pager before serving requests. This permits preceding binaries
to write the unchanged catalog format without leaving stale context answers.
Production record and time mutations maintain the derivative in their original
atomic batch. Selected entries must agree with the primary time index and the
record's source; disagreement refuses the capture without a global scan fallback.
Corrupt record metadata found during reconstruction disables all context captures
with an actionable storage failure until catalog repair and reopen; it does not
publish a partial timeline or prevent unrelated evidence reads. Storage I/O and
reconstruction admission failures still fail startup.
Reconstruction adds startup writes and catalog reads; production-scale startup
latency, disk headroom and write amplification remain unqualified.

Limits report direction_visits, catalog_bytes, response_bytes, output_records,
requested_before, requested_after and independent membership member/record visit
ceilings. UInt64 limits remain exact decimal strings; bounded output counts are
integers. Record diagnostic/source-time profile references stay in each immutable
row. Context does not perform JSON projection, text scoring or a native governor
query, so its receipt identifies host admission without inventing those profiles.

A direction scan frontier reports execution_status=partial and
stop_reason=direction_scan_budget. Otherwise execution_status=complete and
stop_reason=null. This describes host traversal only; it does not relabel
incomplete diagnostic membership or producer continuity as a native-certified
answer. A snapshot ID identifies this captured reply, not an investigation pin.


SPARQL additionally selects graph layers `metadata`, `diagnostic`, and `content`,
defaulting to all three. The default RDF graph is the set union of visible
triples; named graphs retain their separate identities. Shared content is not
duplicated per occurrence. Count events through occurrence nodes.

### Read receipts

Every search/query result includes a receipt with:

| Field | Meaning |
| --- | --- |
| `store_id`, `snapshot_id`, `as_of` | The store, eligible membership snapshot, and captured server time. |
| `execution_status` | `complete`, `partial`, or `failed` for execution over that snapshot. |
| `answer_certainty` | `exact`, `certain`, `at_most`, or `unknown`, preserving native governor evidence. |
| `coverage` | Counts of selected eligible units and ready/pending/failed/not-applicable projections; explicitly states whether the requested index coverage is complete. |
| `limits`, `stop_reason` | Effective ceilings and any governor or host limit reached. |
| `has_more`, `next_cursor` | Page continuation; pagination alone is not incomplete query execution. |
| `profile_ids` | Codec, analyzer, ranking, and governor identities applicable to the result. |

An exact result can have incomplete index coverage. A caller must check both
execution certainty and coverage before treating no matches as evidence of
absence. `require_complete_index=true` refuses the operation when selected,
applicable projections are not ready; raw/non-projectable inputs are counted
explicitly rather than treated as parser failures. The default returns available
evidence with the coverage receipt. When unready diagnostic fields prevent exact
scope counts, mark those counts unknown rather than report a fabricated zero.

Certain partial outcomes return only native-certified answers in `rows`.
At-most outcomes expose possible answers in `candidates`, with the native
certificate; they are never labeled proven matches. Unknown outcomes expose no
answer rows. Genuine operational failures expose a typed error and no purported
successful prefix. Counts and aggregate bindings retain the same certainty law.

Search reports the ranking corpus/scope and profile with its scores. Correlation
and explanation operations return supporting IDs/spans and receipts, so an agent
can inspect evidence for each asserted relationship.

## 3. MCP operations

### Native correlation (schema 58)

`correlate` is a typed Cap'n Proto operation with shared CLI/MCP controls. CLI
uses `correlate REQUEST.json`. It runs fixed native governed SELECT recipes;
callers supply typed selector parameters, never interpolated query fragments.
Example:

```json
{
  "store_id": "store:...",
  "scope": {"kind": "live"},
  "selector": {"kind": "evidence", "ids": ["record:..."]},
  "relation": {
    "kind": "cooccurs_in_interval",
    "interval": {
      "clock": "event",
      "from": "2026-10-18T10:00:00Z",
      "before": "2026-10-18T10:05:00Z"
    }
  },
  "page_size": 100
}
```

Selectors are `evidence` (1..32 record/complete-artifact IDs), `diagnostic`
(`dimension` and exact decoded string `value`), `source` (exact label `value`),
or `time` (an `interval` with receipt or event clock). Diagnostic dimensions use
the existing service/host/severity/trace_id/span_id vocabulary. All seed evidence
references are checked for retention and complete-artifact availability through
the original selection owner before filters. Both endpoints must belong to the
same captured outer scope. An eligible seed excluded by its filters produces no
relationship; a missing/unavailable reference fails before native evaluation.

Relations are `shared_trace_id`, `shared_span_id`, `shared_source`, or
`cooccurs_in_interval`. The first two compare known declared strings exactly;
they do not establish a global trace/span identity or causal link. Shared source
means the same recorded Source entity. Co-occurrence requires both endpoint times
in the explicit half-open interval; it is not a guessed relative window or a
producer-order claim. On the event clock an endpoint without a source instant is
placed at its receipt time, derived at read time from `receivedAt`. Profile
`sparrow-correlation/2` (schema 69) returns `seed_window_basis`,
`related_window_basis` and, for an event-clock time selector, `selected_basis`,
each `source` or `receipt`; they are unbound on the receipt clock.

Native rows are directed `(seed, related)` pairs, exclude self-edges, and preserve
distinct occurrences with equal payloads. Reverse pairs may both appear when
both records are seeds. Native SRJ bindings contain `seed_id`, `record_id`, both
occurrence/parent/source IRIs, exact source labels, receipt instants and original
source byte ranges (`seed_start/end`, `related_start/end`). Shared-value recipes
also return `relationship_value`; interval recipes return both window-clock
instants. Rows sort by seed receipt/ID then related receipt/ID. The IDs and spans
support original-byte inspection; a result lease does not retain those bytes.

The native result envelope, governor certificate and all partial/failure laws are
the same as raw SPARQL. `operation=correlate` and a frozen `correlation` descriptor
survive `result_next`. Its `sparrow-correlation/2` profile and
`correlation-query:<BLAKE3>` key cover recipe text and normalized typed parameters;
scope and snapshot identities remain separate. A descriptor is validated through
the same recipe owner before rendering. Source, scope, page and governor controls
share their original decoders/admission/delivery owners; remote reads stay disabled.
Content projection is not needed: correlation reads metadata and diagnostics.

All native graph replies additionally expose `interpretations`: known/unknown
event-time and per-dimension diagnostic counts among known selected occurrences.
Outer diagnostic-membership unknowns remain separate in the existing coverage.
Agent output adds `interpretations.event_time.fallback`, equal to the unknown
count, because every unknown is placed by receipt time.
For correlation, `coverage.complete` also requires known diagnostic interpretations
for all selector/relation fields throughout that selected scope, not only matching
rows. Every record has an event-clock placement, so an unknown source time alone
does not leave it incomplete. An event clock does require that no selected
record's source time depends on a failed JSON projection, the same readiness law
as a strict records read.
Strict `require_complete_index=true` refuses an incomplete interpretation with
`INDEX_NOT_READY`. In permissive mode, available relationships retain their native
certainty while incomplete coverage prevents an empty result proving absence.
Governor at-most results remain candidates and unknown outcomes expose no rows.
Raw SPARQL keeps its existing readiness law and has `correlation=null`.

Schema discovery advances to 58. Persistent storage and native RDF vocabulary
are unchanged; the running version is tracked separately in Development runtime.

Use the standard stdio MCP transport. The adapter translates MCP messages into
generated typed Cap'n Proto client calls over the selected Unix or public TCP endpoint.
The stdio adapter writes protocol messages to stdout and diagnostics to stderr.
Tools use JSON Schema inputs and bounded structured outputs; reads are marked
read-only, while pin mutations are marked as mutations. [MCP transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports),
[MCP tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools).

| Tool | Principal input | Output |
| --- | --- | --- |
| `capabilities` | None | Certified status/profile identities, accepted formats, occurrence vocabulary, implemented tools/input schemas, limits, retention and clock bases. |
| `status` | None | Bounded storage/queue/projection/ingestion/pin metrics and pressure state. |
| `flush` | Store ID, current session ID, decimal sequence | Typed synchronization through that fence; reached sequence, or stale/future/store refusal. |
| `sources_list` | Filter, cursor, page size | Sources, active stream state, and known gaps. |
| `artifacts_list` | Scope, cursor, page size | Artifact summaries with byte availability and projection state. |
| `records_search` | Scope, text and/or filters, cursor, page size | Matching record summaries, text evidence, scores when applicable, and read receipt. |
| `keys_list` | Scope, field/path restriction, cursor, page size | Observed diagnostic fields and JSON pointers, kinds, record-presence counts, and read receipt. |
| `values_search` | Scope, optional JSON pointer/kind/lexical/decoded text, cursor, page size | Exact matching value occurrences and parent evidence IDs/spans, with read receipt. |
| `records_context` | Record ID, scope, ordering basis, bounded before/after counts | Retained source-local neighbors, gap information, and read receipt. |
| `artifact_get` | Artifact ID | Metadata and availability, including retained-record-only provenance after parent expiry. |
| `record_get` | Record ID | Record metadata, parsed fields, projection/profile references, and source spans. |
| `bytes_read` | Artifact/record/completed-generation ID, offset, length, encoding | Exact bounded bytes with original offset/length. Refuse an unavailable complete representation rather than substitute surviving partial bytes. |
| `sparql_query` | Query, scope, graph layers, bindings, limits, coverage requirement | Typed native query result, result handle/page, and receipt. |
| `sparql_explain` | Same closed request contract as `sparql_query` | Structured native measured-work ledger, original governor/source/scope receipts and immutable continuation; no query-answer certificate. |
| `correlate` | Scope and seed IDs or trace/span/source/time selector | Relationships and their supporting evidence; implemented through prepared queries/retrieval. |
| `result_next` | Store ID, result cursor | Next page from the same leased result; an expired handle refuses continuation. |
| `result_release` | Store ID, snapshot ID | Release the captured replay lease and memory; never releases evidence pins. |
| `ingest_get` | Import/request ID | Raw acceptance/durability, projection states, source checkpoint, and deletion state. |
| `pins_list` | Store ID, cursor, page size, scan budget | Active named collections and deadlines under one finite captured metadata lease. |
| `pin_get` | Store ID, exactly one pin name or ID, cursor, page size, scan budget | Collection identity, deadline, revision, and captured members; continuations preserve the captured name resolution. |
| `pin` | Request ID, name, concrete targets, days | Durably created/extended collection and actual expiry. |
| `pin_renew` | Request ID, pin ID, days | Durably extended deadline. |
| `pin_release` | Request ID, pin ID | Durable removal of that collection's protection; ordinary retention still applies. |
| `text_search` | Text, scope, cursor, limits | Native ranked postings, corpus/profile identity, source spans and coverage. |
| `rdf_inspect` | Store ID, record ID | Persisted native projection state, graph references and stream counts. |
| `source_time_define`, `source_time_get` | Source and optional explicit extraction/representation | Immutable JSON-pointer or bounded whole-log timestamp profile; no reinterpretation of existing evidence. |
| `diagnostic_define`, `diagnostic_get` | Source and optional pointer mappings | Immutable normalized diagnostic interpretation profile. |
| `retention_metrics` | Store ID and finite catalog/payload/visit limits | Logical retained window and shared payload ownership; no physical reclamation certificate. |
| `producer_stream_get`, `receiver_window_get` | Store ID and exact stream/window ID | Finite producer/receiver ordering, observed gaps and explicit unknown continuity. |
| `syslog_listener_get` | Store, source and listener IDs | Actual binding, framing, limits and process-scoped counters. |
| `file_generation_get` | Store ID and generation ID | Original file declaration, durable checkpoint and terminal/completion evidence. |
| `file_finish`, `file_finish_get` | Immutable original completion intent | Retained producer-finish admission/progress; no source deletion. |
| `watch_handoff`, `watch_handoff_get` | Immutable original handoff intent | Durable claim/publication/removal progress for completed files or producer-finished tail generations. |

`sparrow ENDPOINT capabilities` and the MCP `capabilities` tool share one
document and the certified status renderer. `transport="capnp"` identifies
the protocol; `transports=["unix","tcp"]` lists supported endpoint kinds,
not enabled listeners or the current connection. The document includes the
implemented tool catalog and its closed input schemas, accepted `formats`,
`vocabulary.occurrence_namespace`, retention basis and record clock choices.
Formats come from native generated enum introspection through the shared SDK
label owner; the vocabulary namespace is shared with occurrence RDF generation.
Its nested `status` is a current operational read, with
the same integrity checks and cost as explicit status; it is not the catalog-free
typed `discover` operation used internally for compatibility admission.

### Measured native explanation (schema 61)

`Sparrow.sparqlExplain`, CLI `sparrow ENDPOINT sparql-explain REQUEST.json` and
MCP `sparql_explain` accept the same closed input schema as `sparql_query`:
query/base IRI, typed substitutions, retained selection, temporal/diagnostic
filters, graph layers and finite source/native governors. They call native
`explain_governed_fallible_view` through the same captured scope and admission
owner as query execution. Remote requests remain zero; UPDATE is refused.

The typed `SparqlOutcome.explanation` carries the original native profile ID,
version, digest, stop polling granularity and charge schedule, plus statement
index support. Its paged entries preserve native node ordinals/depths/labels,
per-charge counters, total fuel, rows, cells and optional planner estimates;
join-order/statistical descriptions; and physical strategies with optional
provider/registry identity, actual work and optional access estimates. Unknown
estimates remain absent. Sparrow does not render/reparse native explanation
text, reprice native counters or evaluate the query a second time for paging.

Query answers and explanations share one eight-handle/8 MiB native capture
family, the existing 256 KiB page/64 KiB metadata ceilings, `result_next`,
`result_release`, cancellation and the original absolute one-minute replay
lease. Repeated cursors replay the same captured range without renewing it.
An entry that cannot fit a page fails before publication. Every page retains the
original native governor, source evidence, independent coverage and frozen scope.

The shared agent renderer reports `operation="sparql_explain"` and
`execution_status="measured"` or `"stopped"`. It deliberately omits
`answer_certainty`: a stopped measured ledger does not certify query answers.
Native parse/operational failures report `"failed"` with original diagnostics
and evidence. Without `text_relations`, declaration lists are explicitly empty.
Schema 62 supplies the optional ordinary stored-text registry described below.
Unsupported custom aggregate or native ranked-retrieval declarations fail
before publication rather than being silently omitted.

The installed schema-61 images passed default-image/private-copy and live
TCP/Unix CLI/MCP qualification, including complete export of the original
2,990-triple selection under unchanged budgets and exact-byte verification.
See [runtime adoption](development-runtime.md#schema-61-live-adoption).
The subsequent [schema-62 adoption](development-runtime.md#schema-62-live-adoption)
adds stored-text integration. Separate current-source
[load/native measurements](load-qualification.md#schema-62-native-graph-and-text-follow-up)
and [turnover](turnover-qualification.md#schema-62-retention-and-native-compaction-policy)
qualify their finite workloads; no production SLA is inferred.

The query interface supports read forms such as SELECT, ASK, CONSTRUCT, and
DESCRIBE through PurRDF. Preserve RDF term kind, datatype, language, base
direction, and triple-term structure using its result serializers. SPARQL UPDATE
does not provide arbitrary mutation of Sparrow's derived storage; it is refused
by the read surface.

### Stored-text graph relations (source schema 62)

`sparql_query` and `sparql_explain` optionally accept the closed object
`text_relations: {"ranked_iri":"urn:investigation:rank", "spans_iri":"urn:investigation:spans"}`.
Both names must be distinct absolute IRIs, each at most 1024 UTF-8 bytes. The
typed RPC uses the same optional pair. Omitting it registers no text relations.

```sparql
SELECT ?doc ?record_id ?score ?rank ?matched WHERE {
  ?doc <urn:investigation:rank> ("needle" ?record_id ?score ?rank ?matched) .
  ?doc a <urn:sparrow:vocab:Record> .
} ORDER BY ?rank
```

The bound string needle uses the stored profile's native analyzer. Distinct
analyzed terms match with OR semantics. Scores use native BM25 over the entire
captured covered corpus, including when the document is bound. Ranks are global
within that corpus, start at one, and break score ties by record ID. `matched`
counts distinct matched terms. `doc` joins the existing occurrence graph;
`record_id` is the opaque ID accepted by `bytes_read`.

```sparql
SELECT ?record_id ?term ?start ?end ?basis ?path ?coarse WHERE {
  ?doc <urn:investigation:spans>
    ("needle" ?record_id ?term ?start ?end ?basis ?path ?coarse) .
}
```

Each span is persisted occurrence evidence, with record-local byte offsets
`[start,end)`. `raw_record` spans identify original lexical bytes and have an
empty path. `decoded_json_scalar` spans carry the native JSON pointer; escaped
strings can have coarse spans covering the original string contents, excluding
quotes. A span does not assert a token ordinal or a narrower exact alignment.
The relation streams stored postings; it does not reanalyze retained payloads
or rebuild a resident text index.

Every response and continuation carries `text_corpus`: profile, analyzer and
ranking identities, corpus ID, document/token counts, relation names and
ready/failed-or-incomplete/not-applicable coverage. Counts use decimal strings.
The corpus identity describes ordered covered records and their lengths; the
snapshot sequence identifies this execution's storage version. Different
corpora's scores are not directly comparable. Empty analyzed records contribute
coverage but no scoring document. Membership completeness and projection
coverage remain independent of native answer certainty. Strict requests refuse
failed text interpretations. The original snapshot, finite host/native budgets,
cancellation, storage-failure precedence and result lease apply to both relations.

Measured explanations page the actual native relation descriptors (IRIs,
arities, volatility and access modes) alongside node work. Custom aggregate and
native ranked-retrieval declarations are not registered by this input.
PurRDF's selected fallible-query return type does not expose its internal
relation-witness ledger; Sparrow does not invent a returned generation or
service-level attestation. Schema 62 is installed and qualified through both
typed transports and CLI/MCP; see
[Development runtime](development-runtime.md#schema-62-live-adoption).

### Recordless artifact RDF writer authority

Schema 52 adds independently captured metadata for complete zero-byte artifacts,
including empty JSONL/log inputs. These have no record occurrence and no content
projection. Their native PurRDF metadata uses `Artifact`, `payload`, `source`,
`sourceName`, `receivedAt`, `expiresAt`, `byteLength` and `format`. Record and
artifact envelopes share one builder implementation; the persisted dictionary,
statement indexes and native query evaluator remain the original owners.

Publication commits the empty-artifact temporal index, RDF owner and catalog in
one batch. Expiry/eviction retire its graph and ownership marker with its evidence;
graph retirement consumes a bounded maintenance unit. Live graph scope streams
the existing `empty-time:` index in the same half-open receipt interval. Concrete
artifact and pin selections use the shared membership owner and do not expand an
empty artifact into a fabricated record. Source filters precede diagnostic
coverage. An otherwise eligible empty artifact cannot satisfy record diagnostic
filters: permissive reads exclude it and count `artifact_diagnostic_unknown`;
strict reads refuse `INDEX_NOT_READY`. Metadata layer exclusion removes its graph.

Graph coverage keeps `known_selected_records` unchanged and separately reports
`artifacts_visited`, `known_selected_artifacts` and
`artifact_diagnostic_unknown`. Completeness requires known artifact membership as
well as the existing record/content conditions. Captures use the original native
snapshot and read/scan/catalog governors; they do not pin evidence. Missing
required temporal/RDF ownership fails without successful rows.

Store formats 44 through 54 add this feature respectively to existing authority
families 33 through 43: basic, consumption, watches, handoff, completed tail,
pressure without/with watches, receivers without/with watches, and manifest
retirement without/with watches. The stored identity fences older writers; one
internal map preserves the underlying authority checks across later transitions.
Public discovery returns the actual fenced identity. No authority is discarded.
Startup fences before backfill, then resumes durable one-owner batches through
the shared catalog pager and RDF writer. This prevents separately resolved term
counters from colliding in one batch. The service stays unavailable until backfill
finishes. A missing checkpoint or unfenced pre-existing owner is a storage failure.
Each new graph batch uses the original linked-file measurement, maintenance
reserve and complete write-batch estimate before its durable migration commit.
Capacity refusal preserves the prior checkpoint and can resume after space or
budget recovery. Direct empty ingestion also admits its final RDF/catalog batch
through the original live publication observer before any dependency is committed.
The development service adopted schema 52 and format 48 after closed-copy
migration qualification and a fresh closed-store backup; live acceptance is
recorded in [Development runtime](development-runtime.md#schema-52-live-adoption).

### Core graph vocabulary

Sparrow's vocabulary is `urn:sparrow:vocab:`. Metadata uses `Artifact`, `Record`,
`Source`, and `Stream` classes. Its initial query predicates are:

| Predicate | Value |
| --- | --- |
| `receivedAt`, `expiresAt`, `eventTime` | Typed UTC `xsd:dateTime`. `eventTime` is a source instant only and is absent when the source time is unknown. `expiresAt` is the ordinary retention deadline, not an implicit pin renewal. |
| `eventTimeFallback`, `eventTimeBasis` | On a record without a source instant: its receipt instant as a typed `xsd:dateTime`, and the simple literal `receipt`. Absent on a record with `eventTime`, so a query cannot confuse the two (schema 69). |
| `source`, `stream`, `artifact` | Entity IRIs linking an occurrence to its origin, generation, or parent. |
| `payload` | Content IRI corresponding to the occurrence's exact payload identity. |
| `projection` | Named graph IRI for a content projection under its recorded profile. |
| `byteLength`, `sourceByteStart`, `sourceByteEnd` | Typed nonnegative integer lengths/half-open offsets. |
| `service`, `host`, `severity`, `traceId`, `spanId` | Normalized diagnostic strings when available; source spelling remains in the original evidence. |
| `sourceName` | Exact logical source label on each occurrence and its `Source` entity. |
| `streamKind` | `producer`, `file-generation`, or `receiver-window`, identifying the native origin represented by a `Stream`. |
| `intakeEndpoint` | `ingest-socket` on an artifact or record accepted on the ingest socket; absent otherwise (schema 68). |
| `peerUid`, `peerGid`, `peerPid` | The ingest-socket caller's `SO_PEERCRED` user, group and process IDs as `xsd:nonNegativeInteger` literals, on the artifact graph and each record's metadata graph. |

Schema 55 makes minimal `Source` and `Stream` provenance queryable in the
metadata layer. The Source IRI is the same identity exposed by `sources_list`.
Records with a declared producer position, tail generation, or receiver window
link through `stream` to that identity; existing `fileGeneration` and
`receiverWindow` links remain available. Ordinary file imports do not invent a
Stream. A receiver window denotes bounded observed intake, not certified sender
continuity. Mutable registry counters, gap history and current stream state
remain available through their typed endpoints rather than immutable RDF facts.

These facts belong to the occurrence's metadata graph. Identical facts collapse
in the default union; named graphs preserve their occurrence contexts. A pinned
record retains its minimal Source and Stream facts after its parent artifact or
stream registry entry expires. Retirement removes them with their last selected
evidence owner. Registry-only sources or windows without retained occurrences
remain discoverable through the typed registry operations. Excluding the metadata
layer excludes these facts without changing scope membership.

Store formats 66–76 preserve the underlying authority families while excluding
older writers. Startup durably fences both provenance migration jobs before
visiting record and artifact graph owners, including private prepared imports.
Each bounded page replaces one owner through the existing native RDF dictionary,
reference and index writer, then checkpoints atomically. Missing terms, conflicting
origin identities, unsupported metadata stream shapes or insufficient admission
fail explicitly. The store is served only after both jobs complete. New graph
publication and retirement maintain an atomic per-owner provenance marker.

Store formats 110–120 (and 121–131 for an archiving store) fence a store for
intake provenance before it first accepts ingest-socket evidence or begins an
ingest-socket import, so an older writer refuses the store rather than
publishing or rewriting evidence without its `intake`, `peerUid`, `peerGid` and
`peerPid` facts. A store that never served the ingest socket keeps its earlier
family. Like the archive family, the fence includes the log time law.

Archive manifests and link quads use the same namespace: `ArchiveStore`,
`ArchiveSegment`, `ArchiveTable` and `QuarantineEntry` classes, and
`payloadSha256`, `archiveFormat`, `storeId`, `table` and the other predicates
listed in [the archive format](archive-format.md#9-manifest). Archive segments
hold only these facts and the store's existing graphs.

The API returns occurrence/graph IRIs; clients do not construct them by guessing
an opaque ID's layout. PurRDF JSON and Markdown graphs retain their own explicitly
selected vocabularies. For example, the following query finds retained error
occurrences rather than counting shared payloads:

```sparql
PREFIX sp: <urn:sparrow:vocab:>
SELECT ?record ?receivedAt ?payload
WHERE {
  ?record a sp:Record ;
          sp:receivedAt ?receivedAt ;
          sp:severity "ERROR" ;
          sp:payload ?payload .
}
ORDER BY DESC(?receivedAt) ?record
LIMIT 200
```

## 4. Named pin example and rules

An agent captures selected evidence with the following `pin` input:

```json
{
  "request_id": "investigation-1-pin-request-1",
  "name": "problem_investigation_1",
  "targets": {
    "artifact_ids": ["artifact:17"],
    "record_ids": ["record:42", "record:43"]
  },
  "days": 10
}
```

An illustrative receipt for a new collection is:

```json
{
  "store_id": "store-example",
  "request_id": "investigation-1-pin-request-1",
  "pin_id": "pin:8",
  "name": "problem_investigation_1",
  "created_at": "2026-10-06T12:00:00Z",
  "expires_at": "2026-10-16T12:00:00Z",
  "revision": "1",
  "direct_member_count": "3",
  "durability": "durable"
}
```

Names are case-sensitive UTF-8 labels, 1–128 bytes, without control characters.
They are not filesystem paths. Targets must be concrete, non-empty, live IDs.
Duplicate direct members are counted once; file expansion/dependency counts are
reported separately. A frozen selection can be built from paginated search
results, but no pin continuously tracks a query or future stream appends.

`pin` adds to the named active collection and uses
`max(current_expiry, now + days)`. `pin_renew` extends that identified collection
with the same rule. Neither operation shortens an existing deadline. Successful
calls return their actual expiry, revision, and durable receipt. A retry with the
same request ID returns the original receipt rather than moving the deadline.

Mutation validation and commit are atomic with respect to expiry and eviction.
One invalid target refuses the whole call. Operations may carry an optional
`expected_revision` for conflict detection; a mismatch makes no change. Exceeding
membership/admission limits refuses the call rather than silently pinning a
subset. Server limits are exposed through `capabilities`.

Release operates on the stable pin ID, so it cannot release a replacement that
reused the same name. It leaves other pins and unexpired ordinary retention in
force. Names can be reused after expiry/release with a fresh ID. Pinning for ten
days does not shorten a record that would ordinarily remain for another twenty.

## 5. Cap'n Proto ingestion, queries, and administration

### File-generation checkpoints and active tail watches

The core has `tail_start`, `tail_append`, `tail_generation` and
`tail_discontinue`. An explicit declaration binds a stable start request,
watch/revision, original root/parent directory identities, raw relative Unix
path, source, line format and opened device/inode.
The observer owns file-descriptor validation; these calls do not probe, rename
or delete producer files. Reusing a declaration preserves its generation and
current checkpoint; changing it conflicts. At most 64 generations and 512 KiB
of encoded checkpoint metadata are retained. Active entries consume this quota;
terminal summaries become eligible for registry replacement 30 days after
completion/discontinuity.

Append accepts only the complete prefix found by the existing log/JSONL record
boundary owner. An unfinished suffix remains at the uncommitted source offset;
it is not acknowledged as stored. Explicit completion admits a final line
without LF, including completion at an already committed EOF. Idle time and
writable close do not imply completion. Generation checkpoint, exact records,
native indexes and immutable retry provenance commit in one durable WAL batch.
Conflicting or failed publication advances no checkpoint. Exact retries reuse
the original occurrence IDs even after later records or a declared discontinuity.

Record metadata retains the original declaration and file byte range, so a
pinned record keeps its provenance after parent/terminal metadata expiry.
The batch artifact represents precisely that accepted prefix; it does not claim
to be the entire source file. Native RDF metadata includes the generation IRI
and original file offsets. A discontinuity stops the old generation at its
accepted offset without inventing overwritten bytes; a replacement has a new
opaque identity. Missing/corrupt registries refuse startup. The 25..28 writer
fences preserve the prior consumption/watch/handoff family across upgrade.

The storage core additionally supplies `tail_finish_start` and `tail_finish_get`.
A stable finish request binds the original generation and watch revision to the
producer's final device/inode, byte length and nanosecond modification/change
clocks. The descriptor coordinator must certify this observation before first
admission; the storage core performs no filesystem probes. Exact request retries
recover the original durable admission before checking current authority.
Changing the request's bound observation conflicts. Admission records no new
occurrence and is distinct from terminal publication.

An admitted finish bounds every subsequent checkpoint by its final length.
Terminal publication requires exactly that length, includes a final unterminated
line, and durably sets the original `finished_at` in the same publication batch.
An empty EOF completion creates no record. Retries preserve earlier occurrence
IDs and acceptance/expiry clocks. A discontinuity retains the admitted intent and
accepted prefix without asserting completion. This core path grants neither
source deletion nor a complete downloadable file. Schema 39 exposes the
explicit retained completion coordinator below.

Storage formats 29..32 fence older writers that cannot enforce the admitted
completion boundary. Stores at 25..28 preserve their entire validated checkpoint
registry while upgrading to the corresponding family. Missing registries and
completion authority found beneath an older fence refuse startup; an upgrade
must never substitute an empty registry for missing checkpoints.

### Schema 39 explicit retained completion

`tailFinish(TailFinishIntent)` admits the original producer assertion through the
one descriptor-owning watch worker. `tailFinishGet` reads current progress for
that exact admitted intent, returning `NOT_FOUND` before first admission.
Both return `TailGeneration` with optional `finish` containing the original
request, first `admittedAt`, and optional original `finishedAt`. A finished
checkpoint is not a whole-file artifact. The SDK certifies the original request,
store, declaration, final size and terminal state before returning a receipt.

First admission requires the original enabled `active_files_retain` watch
revision, root binding and held device/inode. It checks the final descriptor
size and nanosecond clocks before durably binding authority. Capture holds the
same operation semaphore as ordinary tailing, RPC and syslog. Each bounded
read must still match the admitted final observation before and after reading;
a change stops at a durable `identity_changed` gap without publishing the
captured bytes. A final unterminated line is accepted only at the exact final
boundary. Completion at an already accepted EOF, including an empty file,
creates no extra occurrence. Source paths are never removed by these calls.

Pending admissions recover against surviving original descriptors after
restart. Missing generations receive durable `read_gap`; changed final
observations receive `identity_changed`. A known terminal refusal does not
defer unrelated scan entries. Capacity/coverage deferrals retain their existing
missing-generation safeguards. An unchanged completed file is excluded from
readmission under the same watch revision. A changed declared-terminal inode is
refused until explicit new producer authority is established. Exact completion
retries resolve the stored original before present-day watch/path checks,
including after independent source removal.

The client exports `TailFinishIntent`, `TailFinishReceipt`, `Client::tail_finish`
and `Client::tail_finish_get`. Immutable local retry files share the completed
handoff container's 16 KiB ceiling, traversal/nesting limits and refusal of
trailing bytes/messages. CLI commands are:

```sh
sparrow SOCKET file-finish-prepare REQUEST_ID STORE_ID GENERATION_ID FINAL_FILE INTENT.capnp
sparrow SOCKET file-finish INTENT.capnp
sparrow SOCKET file-finish-status INTENT.capnp
```

Preparation observes a producer-supplied final file, verifies its original
tracked device/inode/checkpoint and exclusively saves the synchronized intent
outside enabled intake roots. It never replaces an existing retry file.
MCP `file_finish` and `file_finish_get` share the SDK admission/certification
path. Inputs contain `store_id`, `request_id`, `generation_id`, decimal-string
`watch_revision`, and `observed` with decimal-string `device`, `inode` and
`byte_length`, signed integer `modified_seconds`/`changed_seconds`, and integer
`modified_nanos`/`changed_nanos` in 0..999999999. Persist and retry these exact
inputs; do not recapture replacement files. Generation reads also expose the
original completion observation and clocks.

Schema 40 adds retained byte-cover reads as specified below. Schema 41 adds the
explicit [completed-tail claim/deletion handoff](#schema-41-completed-tail-source-retirement).
Completion alone grants no deletion authority
and never reimports retired prefixes to manufacture a complete downloadable file.

Schema 37 exposes `tailGenerationGet` as a finite metadata read guarded by the
current schema and persistent store identity. CLI `file-generation STORE_ID
GENERATION_ID` and MCP `file_generation_get` share this operation and rendering.
The original relative path is `Data` on the wire and native hex in agent output;
no UTF-8 replacement is used. Record metadata and context include optional typed
file positions with the full original declaration and half-open byte range.
`replacement_eligible_at` is a terminal registry replacement deadline, not an
evidence pin or an assertion that all bytes remain retained. Continuity remains
unknown; a checkpoint reports the accepted prefix, not a whole-file receipt.

Schema 38 enables recursive active observation through the existing watch
catalog and bounded inotify discovery. A watch definition uses
`producer_contract = "active_files_retain"`, `format = "log"` or `"jsonl"`, and
`max_candidates <= 64`. Bootstrap uses the existing `[[consume]]` intake table;
CLI `watch put` accepts the same TOML definition. The wire mode is `tail` with
explicit `activeFilesRetain` authority. Source files remain in the producer root;
tail observation does not create the private completed-consumption directory.

Tail publication acquires the same operation budget as RPC and syslog. Busy
work remains descriptor-owned and reports admission waiting; no checkpoint is
advanced while waiting.

Each watch retains bounded opened regular files across rename/replacement and
drains one descriptor per worker tick in round-robin order. One capture reads
at most 1 MiB; a line exceeding that window produces a visible failure and
preserves the source/checkpoint. Complete
prefixes use the one shared durable record pipeline. Idle and close events keep
a final unterminated line pending. Observed truncation preserves the old offset
and starts a new opaque generation. Same-length observed modification produces
a declared read gap. This is an append-only producer contract; these observations
do not certify detection of every rewrite or overwrite followed by growth.

Restart attaches surviving in-root identities to original checkpoints. After a
complete reconciliation, unavailable active identities receive durable read gaps;
no missing prefix is invented. Status exposes finite `tail_generation_ids`,
`tail_read_bytes`, `tail_descriptors`, `tail_admission_waiting` and
`tail_checkpoint_records`; the latter
counts accepted occurrences across currently known checkpoints rather than live
retention membership. Kernel coverage and producer continuity remain separate.
Active generations bind their watch revision and refuse definition changes or
removal until resolved. Schema 39 provides explicit retained-generation
completion as above. Schema 41 adds the separately admitted completed-tail
handoff; ordinary consumption admission cannot supply that authority.

### Public TCP and syslog packet interpretation

`rpc_tcp_address` enables public Cap'n Proto TCP, as requested on 2026-10-08. It
requires `rpc_tcp_secret_file`, and either setting without the other refuses
startup. Both listeners share connection and operation ceilings and the
60-second connection lease.

A TCP connection bootstraps an anonymous `Sparrow` capability. Anonymous peers
may call `discover`, `ingest`, `eventsIngest`, `flush` and the import operations
(`importBegin` through `importCancel`). Every other operation, including
`status`, returns a typed `UNAUTHENTICATED` failure. An operation added to the
schema without an explicit admission decision returns Cap'n Proto `unimplemented`.
The Unix socket serves the complete capability unchanged (rulings 2026-10-27).

The ingest socket (`ingest_socket_path` and `ingest_socket_gid`, schema 68)
shares this one producer list: anonymous TCP and the ingest socket forward the
same operations, and an operation added later needs one admission decision.
The ingest socket refuses every other operation with `PERMISSION_DENIED` and
refuses `authChallenge` and `authenticate` with `UNSUPPORTED`; it offers no
CHAP session. On accept the daemon reads the peer's uid, gid and pid; a
connection without them is refused and logged. Every artifact and record
accepted on that connection carries `intake`: endpoint `ingest-socket` and
those three identities, in the typed `Artifact` and `Record`, in the CLI
`context` and MCP `records_context` rendering (`null`, or
`{"endpoint":"ingest-socket","uid":"N","gid":"N","pid":"N"}` with decimal
strings), and in the metadata graphs. An import keeps the intake of the
connection that ran `importBegin`; `importAppend` through `importPublish` from
another uid return `PERMISSION_DENIED`, and an inline retry of a request
committed by another caller returns `CONFLICT`. Its connections have their own
ceiling, `max_ingest_connections` (1..256, default 16), and the same 60-second
lease. The private socket records no caller identity. `status.ingestSocket`
reports the path, group, ceiling, active connections and refused connections.

Shared-secret CHAP upgrades a connection. `authChallenge` returns a fresh 32-byte
nonce. `authenticate(response)` consumes it and checks the BLAKE3 keyed hash of
`nonce || storeId`, keyed by a key derived from the exact secret-file bytes. A
correct response returns `TcpSession.session`, the complete typed service for
that connection's lifetime. Each connection gets one attempt: a wrong response,
a response without a challenge or any later attempt returns `UNAUTHENTICATED`,
and the client must reconnect. Unix connections and sessions refuse both
methods with `UNSUPPORTED`. The secret file must be a regular file with no group
or world permissions, holding 32..4096 bytes. Rust clients use
`Endpoint::AuthenticatedTcp(address, TcpSecret::read(path)?)`. The CLI, the stdio
MCP adapter and the `bytes`, `handoff` and `load` examples accept a leading
`--secret-file PATH`; `load` requires it. MCP
reconnects for each tool call and authenticates each connection.

CHAP authenticates only the start of a session. Traffic is not encrypted, the
client does not authenticate the server, and an on-path attacker can observe or
take over an authenticated connection. An observer of one exchange can test
secret guesses offline, so the secret must be random rather than a passphrase.
Anonymous imports share the import journal pool and request-ID space with
authenticated clients, and anonymous ingestion consumes the disk budget. Both
risks were accepted on 2026-10-27.

Explicit `[[syslog]]` entries select TCP or UDP, source, loopback address,
maximum message bytes (480..65536), maximum connections (TCP 1..16; UDP 1),
and complete-frame timeout (1..60000 ms). TCP additionally requires a framing
law: `octet_counting`, `lf`, or `cr_lf`. The framing trailer is excluded from
the original packet; LF framing preserves a preceding CR. No framing guesses
or truncated-prefix publications are allowed. At most four listeners exist.

Each accepted packet is one exact-byte `syslog` record, including embedded
newlines. Its immutable profile records parsed RFC 5424 or RFC 3164 headers,
PRI, ordered structured-data elements (maximum 32) and parameter occurrences
(maximum 128). Parameter values expose original encoded byte spans inside
quotes, preserving repeated names and escapes. Header host/app or legacy TAG
populate diagnostic host/service. Diagnostic severity is decimal PRI modulo 8
with the original PRI digit span; it is a protocol derivation. Millisecond-exact
modern timestamps populate event time through the native RFC3339 parser.
NIL timestamps, finer precision and year/zone-free legacy timestamps remain
explicitly unknown. Failed interpretation preserves raw acceptance and one
diagnostic without claiming successfully parsed header or parameter evidence.

Status exposes configured and actual listener bindings, bounds, active TCP
connections and observed successful publications, interpretation failures,
refusals, oversized datagrams, framing failures, connection refusals and timeouts.
Counters are process-lifetime evidence and reset on restart. Successful counts
follow durable store publication, but syslog provides no typed sender receipt.
TCP sends therefore do not prove publication; UDP kernel/network loss remains
unknown. Identical newly received packets are independent occurrences sharing
payload storage. Sources include configured listeners before their first packet.

Listener and TCP connection task failures stop admission for the entire syslog
group. Both levels join every admitted child, including blocking publications,
before reporting termination to the daemon's final store flush. Returned task
errors and task panics remain visible together; one failure cannot abort sibling
publications or replace another error. Receivers also observe an already-published
stop state before admitting more work.

The daemon contract is `crates/sparrow-client/schema/sparrow.capnp`. Generated
client/server types carry requests, binary payloads, instants, counts,
result/error unions and store-scoped references. Mutation requests carry stable
`requestId` values. Uploads, batches, RPC traversal, operation work and concurrency
are bounded. Domain errors remain distinct from a broken RPC connection.

| Typed operation | Contract |
| --- | --- |
| `ingest`, `importBegin`/`importAppend`/`importGet`, `importSeal`/`importLayout`/`importPrepare`/`importPublish`/`importCancel` | Original bytes, occurrence identity and explicit durability. A partial stream cannot publish a complete artifact. |
| `eventsIngest` | Bounded typed fields, independent event occurrences, producer instance/sequence identities, and stable batch retries. |
| `flush` | Synchronization through a specified store/session/sequence watermark; typed stale-session and future-sequence refusal. |
| `discover`, `status` | Bounded compatibility/admission metadata, and separately measured operational status. |
| `artifactGet`, `recordGet`, `bytesRead` | Store-scoped metadata and exact bounded ranges. |
| `pin`, `pinRenew`, `pinRelease` | Durable all-or-nothing protection with stable retry identities; renewal optionally checks the current collection revision. |
| `pinRead` | Typed active-list/name/ID selector, finite catalog capture and store-scoped continuation pages. |
| `keysList`, `valuesSearch`, `recordsSearch`, `recordsContext`, `sparqlQuery` | Shared native semantics, typed results, scope and query coverage; stored-text SPARQL relations remain required. |
| `watchList`, `watchPut`, `watchRemove` | Durable revision-checked recursive consume/tail administration; removal preserves existing claims and active tail generations guard definition changes/removal. |
| `watchClaimRetire` | Administration (schema 70): retire one claim that can never complete, recording the authenticated caller, time and reason; see "Retiring an unrecoverable claim". |
| `watchHandoff`, `watchHandoffGet` | Immutable store/request/watch/revision/raw-path/file-generation handoff; durable admission and explicit reserved/claimed/published/removed progress. Stable retries recover the original binding before inspecting the current source path. |

Schema 23 adds `discover`: store identity, schema version, native text/RDF profile
identities, upload/event ceilings and bounded durability state. SDK connection,
producer admission and CLI/MCP query admission use this typed operation rather
than `status`. Its work is independent of the retained evidence catalog size;
store-lock contention can still delay it. The pending durability ledger is
bounded by the configured admission ceilings. Older daemons without this method
are refused with `UNSUPPORTED`; there is no fallback to a catalog scan.

Discovery establishes compatibility and admission limits, not store health.
Explicit `status` still reads catalog counts and physical usage, and queries
still validate the evidence they read. Corrupt evidence must produce an
actionable failure on those paths even when discovery succeeds. Status metadata
must agree with discovery. This wire revision does not change storage formats.

Schema 27 uses the common typed `Scope` in `ArtifactListQuery.scope`, including
live/evidence/pin membership and source/diagnostic filter arrays. Membership was
introduced in schema 26.
CLI artifact reads accept the existing bounded native JSON control file through
`--scope-file FILE`; MCP `artifacts_list` accepts the same membership object as
`scope`. The default is live. Explicit selections admit 1..1000 store-scoped
artifact/record references, including overlaps and duplicate references. Pin
selection uses its stable ID and optional positive decimal revision. The shared
membership owner validates every reference before source or original receipt
filtering. Foreign stores, missing/reclaimed references, expired complete
artifacts, expired pins and stale revisions retain their existing typed failures.

Whole-artifact membership includes recordless inputs; record membership exposes
its parent provenance and counts only selected retained record occurrences.
Thus `retained_records` is exact within the captured membership, while
`origin.original_records` describes the original complete artifact. An available
complete parent's byte availability is independent of selecting only one record.
An expired complete artifact cannot be substituted by its pinned record fragments.
Selecting a surviving record can report its unavailable or reclaimed parent.
Overlapping references never duplicate parent rows or record counts.

Selected captures seek only resolved records and parent/whole-artifact metadata;
they do not scan unrelated artifact/record catalog entries. Reference expansion
and metadata folding share one aggregate entry/byte/scan/time admission. The
existing live scan and selected seeks use one collector. `scope.filter.source_ids`
uses OR alternatives intersected with the outer exact source. Diagnostic arrays
use the shared record matcher: AND across dimensions, OR within arrays, and exact
captured decoded values. Source and original parent receipt filters run before
diagnostic evaluation. The outer receipt interval never becomes a record
acceptance-time interval. The typed scope's source/time/limits must retain their
canonical defaults; callers use outer artifact query fields for those values.

With diagnostic filters, rows require at least one proven matching retained
record. Recordless artifacts have no diagnostic matches. Unknown-only parents
contribute coverage without being presented as proven results. Each row retains
its exact `retained_records` within membership and carries shared typed coverage:
`eligible_records` is the matching-record count or unknown, accompanied by a
`known_eligible_records` lower bound and matched/excluded/unknown evidence.
Page coverage includes every source/receipt-eligible record, including parents
without proven matches. For a selection it remains identical across
continuation; a live page covers its own rows (schema 66).

`total_items` and `known_matching_artifacts` count captured proven rows, or on
a continuing live page the rows delivered so far, with `matching_artifacts` null
(schema 66);
`matching_artifacts` is null and `answer_certainty` is incomplete when diagnostic
membership is unknown. CLI `--require-complete-index` and MCP
`require_complete_index=true` refuse with `INDEX_NOT_READY` on applicable unknown
mappings. No prefix or handle is published on refusal. CLI and MCP share scope
decoding, typed coverage encoding and rendering.

Cursors bind the original membership, selector and limits. For a selection,
continuation serves the immutable capture after pin release, collection changes
or evidence retirement, while a live listing reads its next page (schema 66);
fresh reads revalidate current evidence. No read renews protection. Current
discovery schema 34 is required; old clients/daemons are explicitly refused.
Storage formats and retained evidence encodings are unchanged.

Schema 25 adds typed `sourcesList`, CLI `sparrow SOCKET sources STORE_ID`
and MCP `sources_list`. Its optional exact `source` intake key, `cursor`,
`page_size` and `scan_budget` bind one immutable source capture. CLI uses
`--source`, `--cursor`, `--page-size` and `--scan-budget`. Store and current
schema are required on the wire; watch declarations come from the daemon's
watch owner, never from RPC callers. Older schemas refuse with `UNSUPPORTED`.

Each row carries `source_id`, the exact intake key already used by ingestion
and `Scope.sourceIds`, and a separate `rdf_iri` matching native occurrence RDF
metadata. Treat both as opaque; no normalization or label/ID guessing occurs.
Complete artifact occurrences, unavailable parents with retained fragments and
retained record occurrences have separate exact counts. Identical content does
not collapse occurrences. The counts come from the per-source counter rows (see
"Per-source counters and artifact index") and follow retirement, not the
retention deadline: a record past its deadline that has not been retired, for
example while the archive gate holds it, still counts. Status `live_records`
keeps its deadline meaning. A listing reads a snapshot taken
under the writer lock, which it releases before reading, so a listing never
holds up ingest. It costs one entry against `scan_budget`, and that row's
encoded size against the byte budget, per counter row it reads: one point read
with `source`, one row per source without. A selected or bound source without a
counter row costs one more entry, a probe of its artifact index that proves it
has no counted entries; finding one is a `STORAGE_FAILURE` naming the source.
It never reads the record catalog.
Bindings (watches, producer and receiver windows, tail generations, mapping
profiles) are charged as before. Immutable time/diagnostic mapping IDs are optional;
use their existing get operations for definitions. Declared mapping and
configured watch sources remain discoverable with zero retained input.
Watch IDs, revisions and enabled state are captured and sorted within each row.
Producer stream continuity remains `continuity_unknown=true`. Schema 35 adds
`producer_stream_ids` for finite currently retained structured producer windows.
These identities resolve through `producerStreamGet`, CLI `producer-stream
STORE_ID STREAM_ID`, or the read-only closed MCP `producer_stream_get` tool.
Schema 46 adds `tail_generation_ids`, sorted opaque IDs for active or unexpired
terminal file-generation checkpoints under the exact source key. Empty inputs
remain discoverable with zero retained records. IDs resolve through
`tailGenerationGet`, CLI `file-generation STORE_ID ID`, and MCP
`file_generation_get`. The existing native registry decoder owns identity and
metadata validation; registry bytes and entries count against source scan
admission, and reference allocation counts against capture/lease capacity.
Source pages freeze these IDs with their original catalog snapshot: later
generations and terminal metadata expiry change fresh reads, while continuation
retains captured IDs. A captured reference is not a pin or a promise that a
later checkpoint lookup will still succeed. Store formats are unchanged and
older discovery schemas refuse instead of silently omitting the added field.
`page_size` is an item ceiling: response byte limits may require continuation
before that many rows. Follow `next_cursor` to complete the captured inventory.
Schema 47 adds sorted `syslog_listener_ids` for currently configured syslog
listeners under each exact source key, including listeners that have accepted
no packets. Multiple listeners may share a source. The runtime registry owns
these bindings; callers cannot supply them. Listener declarations count against
the source scan and capture admission, and duplicate listener identities or
invalid declarations refuse before publication. Continuation freezes the
original references; fresh reads reflect the current configuration. Removing a
listener does not erase retained packet sources, but removes its binding from
fresh reads. No listener reference is a pin or a durable stream checkpoint.

Resolve each reference with typed `syslogListenerGet`, CLI
`syslog-listener STORE_ID SOURCE LISTENER_ID`, or closed read-only MCP tool
`syslog_listener_get` with `store_id`, `source` and `listener_id`. The exact
store and source are required: a matching listener ID under another source
cannot satisfy the query. Older schemas refuse with `UNSUPPORTED`; a different
store refuses with `STORE_MISMATCH`; an absent or rebound listener refuses with
`NOT_FOUND`. Current source labels are 1..512 bytes and listener IDs 1..128 bytes.
The runtime registry has at most four entries and is read through its existing
fallible snapshot owner. Both source discovery and listener lookup return registry
failures as typed `STORAGE_FAILURE`, rather than a successful empty result or a
transport error. A poisoned-registry RPC unit scenario verifies both error unions
and zero published source leases using the shared dev-only daemon configuration.

The reply uses the existing typed `SyslogListenerStatus`: configured listener
ID, exact source, actual binding, transport/framing, payload/connection/time
bounds, active connections, observed accepted/refused/malformed/truncated counts,
connection refusals/timeouts and explicit failure. The canonical CLI/MCP listener
renderer is also used by status. CLI/MCP wrap it with `store_id`, operation
`syslog_listener_get` and `listener`. This lookup reads current runtime
observations, not the source capture's historical counters. Counters reset on
restart; continuity remains unknown, and UDP kernel/network loss remains
unknown. Durable observations are separate from these process-lifetime counters.

### Durable syslog receiver observations (schema 48)

Configured listeners publish each accepted packet, its immutable `receiver`
position and its receiver window in one synchronized WAL batch. The position's
`window_id` and one-based `sequence` name **durable observation commit order**,
not network arrival order or a sender sequence. Concurrent TCP connections may
commit in a different order than their read timestamps. Failed protocol
interpretation still accepts the complete raw packet and a position. Ordinary
typed syslog uploads have no fabricated receiver position.
Use full typed record/context metadata, including CLI `context STORE_ID
RECORD_ID --before 0 --after 0`, to resolve a search result's receiver position.

Known local refused packets, oversized UDP datagrams, TCP framing failures and
TCP frame deadlines publish classified observations without a packet occurrence.
The listener waits for the same bounded global operation admission before these
control writes; there is at most one waiter per admitted listener/connection.
Shutdown joins admitted control writes. Storage admission refusals may be
classified; an uncertain storage failure stops the intake group without claiming
a known discarded packet. Failed history publication is a visible intake
failure, never a successful counter update. Rejected TCP connections are runtime
counts, since no message observation exists.

`ReceiverWindow` exposes exact observations, accepted/discarded totals, counts
for the four local discard classes, at most 16 recent inclusive discard ordinal
ranges and `gaps_complete`. Trimming older ranges preserves aggregate counts.
Every reply has `continuity_unknown=true` and `pre_commit_loss_unknown=true`:
there is no sender receipt, and sender/network/kernel or observations lost before
their durable commit cannot be reconstructed. These ranges identify known local
discard observations, not unobserved sender sequence gaps.

Each listener startup creates a fresh opaque instance. The registry admits 64
windows and 128 KiB of encoded metadata, with a deadline 30 days after its latest
observation timestamp. Pins protect record positions and bytes but never renew
receiver summaries. A still-running instance after summary expiry receives a
new window ID and starts at ordinal one. Empty listeners remain discoverable
through current bindings without creating invented durable observations.

Sources freeze sorted `receiver_window_ids`, including discard-only sources.
Resolve them with typed `receiverWindowGet`, CLI `receiver-window STORE_ID
WINDOW_ID`, or closed read-only MCP `receiver_window_get` using `store_id` and
`window_id`. Current schema and exact store identity are required; stale schemas,
wrong stores, wrong ID kinds and absent/expired windows refuse explicitly.
References are not pins and do not guarantee a later lookup. Native occurrence
RDF adds `receiverWindow` and typed `receiverSequence` through PurRDF's builder.
The first receiver publication fences the store at format 40, or 41 with watch
state; older writers refuse. Stores without receiver observations keep their
existing fence. Subsequent writes preserve this fence and existing pressure state.

### Structured producer sequence windows (schema 35)

`eventsIngest` commits the normalized batch, its record positions, native RDF
metadata and producer checkpoint in one atomic storage write at the requested
buffered or durable boundary. Ordinary uploads share that record preparation
path and carry no declared producer position. Each complete artifact here is
one complete event batch; it does not imply a finished continuous stream.

The exact source and 16-byte producer instance identify an observation window.
New sequences must increase beyond its last accepted position. Overlapping or
backward batches fail with `CONFLICT` before publishing records or checkpoint
changes. An exact committed retry returns the original occurrence IDs without
advancing the window, even after newer batches or reconnect/reopen. Producer
instance and sequence declarations are part of the immutable retry receipt;
they cannot be attached retrospectively to an ordinary accepted upload.

Records expose an optional typed `ProducerPosition` with opaque stream ID and
UInt64 sequence. Native occurrence metadata includes `stream` and
`producerSequence` triples through PurRDF's dataset builder. `producerStreamGet`
requires the discovered schema version and matching store identity. Its typed
summary reports first/last accepted sequence, accepted event count, aggregate
unobserved sequence count, recent inclusive gap intervals and whether those
intervals cover all observed gaps. CLI/MCP render UInt64 values as decimal
strings. Gaps count positions absent between accepted observations; they do not
identify a loss cause. Positions before the first event and after the last
remain unknown, so every summary retains `continuity_unknown=true`.

The registry admits at most 64 windows and 128 KiB of native encoded metadata.
Each window retains at most 16 recent gap intervals while preserving its exact
aggregate counts; older intervals make `gaps_complete=false`. Metadata expires
30 days after the most recent acceptance. Expired windows are omitted from
discovery and return `NOT_FOUND`; a later observation creates a fresh opaque
window ID, including for the same source and instance. Pins on records preserve
their original positions without extending the stream summary deadline. The
bounded registry reclaims expired entries on its next atomic replacement.
Missing or invalid registry state is a hard storage failure, not an empty
fallback. Full capacity refuses a new window with `RESOURCE_LIMIT`.
The existing Rust `events` example accepts an optional sequence after its
durability argument so separate invocations can demonstrate ordered batches
using one instance. Its default sequence remains 1; exact retries keep all
arguments unchanged, and new events require new request IDs and higher sequences.

Discovery and status advertise these bounds through typed `ProducerLimits`.
Storage families 17/18/19/20 fence earlier writers while preserving base,
consumption, watch and handoff authority respectively. Families 13..16 upgrade
without reinterpreting historical unpositioned records or diagnostic mappings.
Earlier pre-diagnostic families retain their existing invariant checks.

Sources share artifact and pin catalog lease/cursor/receipt ownership. A capture
uses at most 2 MiB, 10,000 source rows, a 32 MiB conservative catalog charge and the
requested aggregate scan budget (including pins, artifacts, records, mappings
and watch declarations). Native catalog entries are bounded before decoding.
The catalog byte charge multiplies encoded bytes by 64 to admit decoded shapes;
32 MiB is the charged ceiling, not an allowance to scan 32 MiB of raw JSON.
Pages have a conservative 256 KiB ceiling, at most 15 source rows per page.
Eight handles and 8 MiB retained source metadata are shared across store clones,
with absolute 60-second wall/monotonic expiry. Cursors bind selector and limits;
new ingestion, declaration changes, evidence retirement or pin release do not
change later pages or renew protection. All scan/capacity/corruption failures
refuse without publishing a successful prefix. Typed status exposes source
handle counts, retained bytes and ceilings. Storage formats remain unchanged.

Schema 24 adds typed `artifactsList`, CLI `sparrow SOCKET artifacts STORE_ID`
and MCP `artifacts_list`. The initial selector accepts an exact source label and
half-open original artifact receipt interval, plus `cursor`, `page_size` and
`scan_budget`. CLI uses `--source`, `--received-from`, `--received-before`,
`--cursor`, `--page-size` and `--scan-budget`; MCP uses their snake-case names and
required `store_id`. RPC requests declare the current discovered schema.

Pages use the shared `CatalogReadReceipt` with fixed store/snapshot/as-of,
absolute lease deadline, total items, page position, scan evidence and limits.
Continuation must retain its original selector and limits. Reading later pages
does not renew the lease or pin any evidence. Schema 66 gives each live page its
own lease instead. Summaries carry original artifact
metadata in an explicit optional union, complete-byte availability at capture
time and exact retained record count. Reclaimed parent catalogs return absent
origin metadata while surviving records retain parent ID/source. Receipt-time
filtering on an applicable unknown origin refuses with `INDEX_NOT_READY`.
Complete-byte availability is an observation at `as_of`; byte reads independently
check current retention. All counts render as decimal strings in agent output.

Status now carries typed artifact catalog handle/byte counts, limits and lease
duration. Artifact and pin pages share both their receipt wire codec and agent
renderer. Schema 27 supersedes this initial scope with membership and diagnostic
filtering. Producer stream discovery now uses `producerStreamGet` and source
summaries; corpus-scale operational selection remains unqualified.

Artifact upload metadata accepts `source_name`, an optional existing `source_id`,
`media_type`, `format`, labels, and requested `durability`. Omitted source identity
uses the configured upload source. Format selects the interpretation profile;
it does not authorize changing original bytes or supplying server receipt time.
Typed `EventBatch` messages carry a 16-byte producer instance, ordered events,
integer producer instants, severity/lifecycle enums, typed fields and span
context. `eventsIngest` uses native JSON serialization to store each event as a
`sparrow.events/1` JSONL record with independently queryable source evidence.
String, boolean, signed/unsigned 64/128-bit integer, exact floating-point bits,
binary, debug and error field variants remain distinguishable. Ordered field
lists preserve repeated names. JSON/JSONL byte uploads remain available through
the ordinary upload operations. Preserve event subspans through the shared JSON
parser rather than round-tripping events through a lossy serialization.

Large upload publication can be asynchronous. An accepted receipt is accompanied by
an import ID and precise accepted/durability state; it does not imply ready
projections. Import status reports raw staging/publication progress without
publishing a partially uploaded artifact as complete. At raw completion, the
caller can request durable confirmation before retiring its own source.

Watch registration is administrative, outside the ordinary MCP read tools. A
watch declares path, mode, recursive scanning, `readiness`, quiet interval,
parser/format hint, and deletion policy. `consume` defaults to
`readiness=close_or_idle`, `idle_for=30s`, and deleting completed claimed files
after durable import. Writable-close/publication events feed the quiet timer;
stable pre-existing files use the scan fallback. `tail` defaults to retaining the
current file, including after an observed writable close, with deletion only for
a completed handoff. Rotation counts as completion only under a configured
producer contract that ends writes to the old generation; otherwise require an
explicit completion signal. Reject overlapping roots and Sparrow's own storage
directories. Updating/removing a watch must preserve outstanding claim journals
for recovery. Status exposes event coverage, overflow/reconciliation state, and
watch-limit failures. Close events do not replace the producer handoff contract.

Schema 15 provides store-scoped `WatchPut` and `WatchRemove`, with stable
`requestId` and exact `expectedRevision`; zero creates an absent definition.
`WatchDefinition.authority` defaults to `unacknowledged` and must explicitly select
`finishedFilesDeleteAfterDurablePublish`. Only recursive consume/close-or-idle
is currently admitted. The daemon owns at most eight active or draining roots,
a 16-command control queue and 128 mutation receipts with a 48-hour retry window;
eight receipt slots are reserved for removing watches rather than further puts.
Changed arguments under an existing request ID and stale revisions are refused.
Retries return the original mutation receipt, which does not reapply its old
definition; list/status describe current authority.

The bounded catalog and mutation receipt commit atomically with a `sparrow.store/7`
writer fence. Startup `[[consume]]` entries seed only a missing catalog; once
initialized, its durable definitions govern restart. Removing a watch with
outstanding claims retains its disabled definition and root owner until those
claims finish. Root rebinding is refused while old claims remain. Updates reset
readiness rather than carry old quiet evidence into new policy. Ordinary status
also includes durable definitions, revisions and admission state. CLI mutations
are explicit administrative commands; no destructive watch mutation was added
to ordinary MCP search tools.

Schema 16 provides `WatchHandoff` requests for explicit finished-file admission
and read-only progress lookup. They carry the original store, stable request ID,
watch and exact revision, relative Unix path as `Data`, and a typed device/inode,
byte-length, modification and change-clock generation. First admission requires
the current enabled watch revision and matching anchored regular-file identity;
it bypasses the quiet timer under the watch's explicit producer contract.
The durable request binding and existing consumption reservation commit together,
with a monotonic `sparrow.store/8` writer fence. Up to 128 bindings and 16 active
claims are retained. Active ownership survives ordinary retry expiry; synchronized
removal starts a 48-hour completed retry window. This ownership is not a pin.

`HandoffReceipt.request` echoes the immutable original intent. The SDK certifies
that echo, durable acknowledgment and phase/artifact consistency. `reserved`
means durable admission; `claimed`, `published` and `removed` expose subsequent
progress through the existing worker. A reserved receipt does not certify source
deletion. Repeated admission returns original current progress before inspecting
today's watch definition or source path. Changed arguments conflict, and progress
lookup never creates a new admission. CLI preparation saves a bounded, strict
single-message Cap'n Proto intent outside consumption roots and refuses overwrite.
Retries submit that saved intent rather than recapture the source path.

No upload or tool-supplied path independently authorizes deleting arbitrary files.
The configured consume watch and its claimed source generation are the deletion
authority. Ordinary file import preserves the caller's file unless it belongs to
that completed-consumption workflow.

### Completed descriptor upload and producer receipt certification

The SDK supplies `Client::import_open_file` and the generated-capability
`import_open_file` helper for an already opened completed regular file. The
caller retains descriptor ownership; upload reads positionally from byte zero and never
reopens or deletes a path. `CompletedFileIdentity::capture` and `require` share
the device/inode, length and nanosecond modification/change-clock guard used
by hashing, uploading, accepted-retry receipts and final publication replies.
An observed source mutation refuses certification even if the daemon has
already committed its original bytes. This observation is not a writer lock
or deletion authority.

CLI and SDK path imports use one `open_completed_file` helper backed by safe
Rustix `openat2` with `NO_SYMLINKS` and nonblocking open flags. Symlinks in any
path component and special files are refused. Unsupported kernels return an
actionable error rather than use weaker resolution; caller-owned regular-file
descriptors remain supported. The native filesystem wrapper is a concrete
requirement of path resolution and later claims, not a second semantic helper.

Direct artifact, event-batch and file uploads share one receipt-certification
owner. Before returning success, they match the discovered store/session,
requested source and format, exact byte length and native BLAKE3 payload,
record limit/count when known, and requested durability. A buffered request
may receive a stronger durable acknowledgment; a durable request cannot receive
a weaker one. Malformed/duplicate occurrence IDs, invalid retention intervals,
and noncanonical payload identities are protocol failures. Inspection also
checks the requested store and artifact identity.

`EventBatch::source_bytes` owns the existing `sparrow.events/1` native JSON
codec for both producer verification and daemon publication; no second source
normalizer is maintained. The bounded delivery worker retires a batch only
after these checks and keeps its immutable pending body and charge on a
permanent receipt mismatch. Ingestion may already have committed: the refusal
certifies no producer retirement or deletion, and does not roll back that write.

The daemon now admits explicit `[[consume]]` configuration, with the required
`producer_contract = "finished_files"`. The owned recursive inotify worker
uses the durable claim journal and the SDK descriptor upload path before retiring
the exact private source. Schema 15 `status.consumeWatches` reports coverage,
bounded candidates/deferral, pending claims, completion counts and failures;
the CLI and stdio MCP share its rendering. No live producer roots are configured
in the development daemon. Completed-tail handoff is implemented through schema
41. Ordinary upload continues to preserve every caller source.

### Completed-file process-crash qualification

The server unit test
`consumption::crash_tests::whole_process_crashes_preserve_claims_publication_and_replacements`
starts with a durable reservation for an owned, finished 80 KiB opaque file.
Each child uses the production claim implementation, SDK descriptor uploader,
generated Cap'n Proto RPC server and RocksDB store over a private Unix socket.
The parent waits for a complete checkpoint marker and sends SIGKILL to that child.
The twelve independently recovered windows are:

- Before and after the atomic rename into private ownership.
- After file-content, destination-directory and original-parent synchronization.
- After the durable claimed-generation checkpoint.
- After the first committed 64 KiB upload chunk, before sealing or publication.
- After durable publication, before returning its receipt.
- Before unlink, after unlink, after directory synchronization and after the
  durable removal checkpoint.

Before recovery, assertions check journal ownership, publication visibility,
private-path presence, removal confirmation and the partial-upload offset.
Recovery and a second reopen must preserve exact bytes, one record and artifact,
accepted identities and clocks, and any independently replaced original path.
Already accepted artifacts retain their original retention deadline. The marker
adds no filesystem synchronization barrier. The checkpoints compile only in the
unit-test library and share the dev-only process helper; release builds expose
no crash setting or additional RPC.

This qualifies process death at these ownership transitions. It does not qualify
power loss, startup directory/lock creation, automatic inotify discovery or
forced overflow, or arbitrary filesystem errors. Those remaining acceptance
requirements must not be inferred from this finite matrix.


### Watcher coverage-loss qualification

`watches::tests::kernel_overflow_and_directory_exhaustion_require_fresh_coverage`
uses the production detector with a real temporary producer root and RocksDB
store. It reads the host's current inotify queue capacity without changing it,
and generates alternating opens and read-only closes beyond that capacity.
An independent owned inotify queue must report a real `IN_Q_OVERFLOW`; the
bounded fixture refuses queue capacities above 65,536 rather than skipping.

The production detector's 4,096-event processing ceiling can reset its queue
before reaching the kernel overflow marker. The test therefore qualifies that
conservative reset as well: coverage becomes uncertain, old readiness and rename
evidence are cleared, the visible loss counter advances and no claim is ready.
A new bounded recursive scan restores coverage with a fresh quiet interval;
previously aged candidates cannot become immediately consumable. Exact source
bytes remain present, with no import journals or published records.

The same test exhausts the configured directory budget while scanning a real
subdirectory. Discovery must return an actionable coverage-limit error and
refuse readiness. Explicitly repairing that fixture budget and reconciling
restores both directory watches and fresh candidates. It does not exhaust the
host's shared kernel watch quota, alter sysctls or touch the running daemon.
This is detector-level Linux acceptance, independent of the process-crash matrix;
it does not prove every watcher scheduling or host power-loss scenario.

## 6. CLI

### Read-only upload progress

Schema 49 adds typed `ingestStatus`, used by
`sparrow_client::get_ingest_status`, CLI `ingest-get` and MCP `ingest_get`.
Supply `store_id` and exactly one `import_id`, `request_id` or
`handoff_request_id`. CLI accepts `ingest-get STORE_ID IMPORT_ID`, or
`ingest-get STORE_ID --request-id REQUEST_ID` / `--handoff-request-id REQUEST_ID`.
Import identities come from native admission; clients do not derive them. Explicit
request namespaces prevent a handoff label from aliasing an upload label.
Direct-ingest status returns a null `import_id`, because that intake has no admitted
upload or consumption import object.

One catalog lock captures `as_of`, the resolved original identities, optional
`staging`, private `preparation`, original `accepted` receipt summary, terminal
`tail` checkpoint, source `ownership`, `deletion` and `removed_at`. The acceptance
summary names the artifact and original record count; retrieve its ordered evidence
membership with `artifact_get`. Tail acknowledgement creates zero new records and
preserves the original generation's accepted offset and count. Private staging and
preparation are inaccessible evidence identities until complete publication.

`json`, `rdf` and `text` classify inspected record occurrences as `complete`,
`failed`, `incomplete` (text only), `not_applicable` or `unavailable`, and report
known unprepared records as `pending`. `total_records` is null until native layout
establishes the total. Failed native projection is distinct from missing or corrupt
catalog state, which refuses the whole read. Retired evidence is unavailable while
the original managed publication receipt remains recoverable; recovery is no pin.

Deletion is `not_managed`, `reserved`, `claimed`, `published`, `removed` or
`retired_unrecoverable`, the last for a claim an administrator retired with
`watchClaimRetire`.
Acceptance alone never proves source removal; only the original synchronized
ownership journal supplies `removed_at`. Completed handoff lookup follows its
existing finite retry lifetime. `watch_handoff_get` continues to certify the exact
saved producer intent when that authority is needed.

### Retiring an unrecoverable claim

A consume claim whose source moved into the private `.sparrow-inflight/`
directory and then vanished before publication can never complete: recovery
needs that exact claimed generation and never republishes a replacement. It
stays in `pending_claims`, its watch's `failure` names the claim and reads
"unpublished claimed source is missing", and `sparrow-backup`, which waits for zero pending claims,
fails every night. `watchClaimRetire` (CLI `watch retire-claim WATCH_ID
CLAIM_ID REASON`, MCP `watch_claim_retire`) lets an administrator retire such a
claim. It is an admin operation: anonymous TCP and the ingest socket refuse it,
and a query-only daemon refuses it as a write. The watch's own claim owner
checks, holding its private claim directory, that the private source is
missing, and the store refuses unless the claim is active, belongs to that
watch, was claimed and has no recorded publication. A claim whose private
source exists, whose source never moved, or whose publication is recorded is
refused with `CONFLICT`, as is a second retirement of the same claim. One write
batch moves the claim out of the active journal set into a retired record that
keeps the journal with the time, the caller and the reason. If the claim's
staged import had begun, the same batch ends its staging lease; the next import
reclaim pass then frees its staged chunks, private preparation and import slot
through the ordinary expired-lease removal, so a retired claim never holds one
of the 16 import slots. Retiring a claim clears the watch failure only when
that claim's recovery set it; a failure another claim or discovery caused
stays. The caller is the
connection's administrator as the daemon identified it: `unix uid=U pid=P` on
the private socket, `tcp-chap peer=ADDRESS` on a CHAP session. The claim leaves
`pending_claims`; `status` and `watch list` list it under `retired_claims` with
state `retired-unrecoverable`, and `ingest_get` reports its deletion as
`retired_unrecoverable`. The daemon logs one line,
`sparrowd: watch=W claim=ID retired-unrecoverable caller=… reason=… path=…`.
A store keeps at most 64 retired claims, which are never removed.

The default scan budget is 10,000 (maximum 100,000) records and pin collections;
aggregate native catalog construction defaults to and is capped at 32 MiB.
MCP accepts integer `scan_budget` / `catalog_bytes`; CLI accepts `--scan-budget` /
`--catalog-bytes`. Over-budget reads fail without a partial summary. SDK status
messages are capped at 64 KiB and certified for identity, count, phase and current
durability-fence agreement. CLI and MCP share that certificate and renderer.
Reads never append, prepare, publish, renew, remove sources or flush writes.
Ordinary expired leases, unknown identities and wrong stores fail explicitly.
The SDK's `get_import_progress` and resumable upload still share the native
`importGet` checkpoint decoder.

Schema 42 adds `resultRelease(ResultReleaseQuery) -> Result(ResultReleased)`.
The query contains store ID, current schema version and snapshot ID, never a
page cursor. The receipt certifies the same store/snapshot and `released`: true
when an active retained capture was removed, false when expired, unknown,
single-page (not retained), already released or from a previous store owner.
This is an ephemeral state-idempotent operation; retries can change true to
false and do not have a durable request journal. Wrong store or inadmissible
identity refuses before retirement. Storage/registry failures remain failures.

The shared registry owner covers records, text, keys, values, artifacts, sources
and pin discovery/member pages. Existing replay and its original expiry stay
valid until explicitly released. Subsequent continuation after release returns
`RESULT_EXPIRED`; a request that already staged an owned page may finish.
Evidence, retention deadlines, pin membership and all disk budgets are unchanged.
The SDK certifies exact receipt identity and discovery compatibility. CLI and
MCP use that same SDK operation and renderer:

```text
sparrow ENDPOINT result-release STORE_ID SNAPSHOT_ID
result_release {"store_id":"STORE_ID","snapshot_id":"SNAPSHOT_ID"}
```

Consumers that need replay must keep the lease. Consumers finished with an
investigation page set may explicitly release its `snapshot_id` to immediately
reclaim capture admission. CLI does not silently release snapshots during page
traversal. MCP marks this operation as an idempotent mutation that destroys the
ephemeral replay result. It does not delete stored evidence. The schema changes
to 42; storage format and helper-census/runtime dependency revisions are unchanged.

The implemented client takes an optional `--secret-file PATH` and then an endpoint:
a Unix socket path or `tcp://IP:PORT`. Without the secret, a TCP endpoint admits
only discovery, ingestion, flush and imports. Numeric IPv4 and bracketed IPv6 addresses use the SDK's
native endpoint parser. CLI operations and every stdio MCP tool call use the same
SDK client constructor and discovery certification over either transport. No
HTTP envelope or alternate query implementation is involved.

```text
sparrow /path/to/rpc.sock status
sparrow --secret-file ~/.config/sparrow/tcp.secret tcp://192.168.2.4:518 status
sparrow --secret-file ~/.config/sparrow/tcp.secret tcp://192.168.2.4:518 records --source SOURCE
sparrow --secret-file ~/.config/sparrow/tcp.secret tcp://192.168.2.4:518 mcp --stdio
sparrow ENDPOINT pin REQUEST_ID STORE_ID NAME DAYS EVIDENCE_ID...
sparrow ENDPOINT pin-release REQUEST_ID STORE_ID PIN_ID
```

CLI `pin` and `pin-release` use the same mutation request owner and receipt
renderers as MCP `pin` and `pin_release`. Each concrete target must identify a
record or artifact in the supplied store; there are one to 10,000 targets and
positive integer days. The daemon owns durable protection, request retry and
conflict semantics. A stable request replay returns the original receipt;
changing arguments refuses. Release takes the immutable pin ID rather than a
name that could identify a replacement collection. Neither command changes
ordinary evidence retention or opens another database writer. No schema or
storage-format change is needed for these existing typed operations.

Completed import publication transfers its dependencies into accepted evidence
and saves an independent durable retry receipt. Maintenance now reclaims those
published staging chunks, layout pages and prepared catalog copies immediately,
without waiting for the staging lease to expire. It does not retire evidence,
alter occurrence IDs, renew retention, cancel consumption authority, or shorten
the durable retry window. `importGet`, begin and publish retries retain their
original accepted receipt through the existing receipt owner. Canceling published
evidence still refuses, including after staging cleanup.

The journal ceiling remains 16. Unpublished recovery leases remain intact until
their original expiry or explicit cancellation; active consumption staging keeps
its source-recovery authority. Each maintenance call retains the 128-unit ceiling
and expensive private retirement admits at most one prepared record. Published
catalog copies receive finite shares across journals so one large completed file
cannot monopolize reclamation of smaller completed uploads. Admission can still
refuse while reclamation is pending; completion does not imply an unbounded upload
queue. This uses the existing store representation and schema 42 wire contract.

File input/output and immutable-intent preparation use the client's filesystem.
Producer-finish/handoff preparation therefore requires access to the actual
producer file; saved-intent replay and progress queries do not recapture paths.
MCP keeps its bounded framing, four-call admission, cancellation and 30-second
tool deadline over both transports. Connection refusal produces a failed tool
result with `RPC_FAILURE`. CLI argument/RPC/storage refusals exit nonzero without
a result. A governed query failure can emit its typed failed receipt on stdout
and also exit nonzero; consumers must inspect the receipt and exit status.

CLI `ingest` uploads over its own connection to the same endpoint. When that
connection is lost, for example to the 60-second connection lease or a daemon
restart, it reconnects, waiting up to 30 seconds for the daemon to accept
connections again, and resumes the same request from its durable checkpoint for
as long as each attempt advances the received bytes, seal, layout or
preparation. A failure that is not retryable, or an attempt that advances
nothing, cancels the staging journal with `importCancel`, so the daemon's bounded
reclamation releases its import slot instead of holding it for the 48-hour retry
window. Until then, retrying the canceled request ID fails with `IMPORT_EXPIRED`.
If the cancellation also fails, the CLI reports `IMPORT_ABANDONED` with both
causes. The SDK `import_file(endpoint, ...)` is this implementation.

The binaries are `sparrowd` for the daemon and `sparrow` for its client.
Every client invocation takes an optional `--secret-file PATH` and then an
endpoint: a Unix socket path or `tcp://IP:PORT`. Operations other than discovery,
ingestion, flush and imports need the secret over TCP. Commands emit JSON envelopes through the same owners as MCP.
Byte reads save the exact range to an exclusively created output file and return
its JSON receipt. Errors and diagnostics go to stderr.

```bash
sparrowd --config /etc/sparrow/config.toml
ENDPOINT="--secret-file $HOME/.config/sparrow/tcp.secret tcp://127.0.0.1:518"
sparrow $ENDPOINT capabilities
sparrow $ENDPOINT status

sparrow $ENDPOINT ingest REQUEST_ID SOURCE jsonl /srv/dumps/run.jsonl
sparrow $ENDPOINT watch list
sparrow $ENDPOINT watch put REQUEST_ID 0 completed-watch.toml
sparrow $ENDPOINT watch put REQUEST_ID CURRENT_REVISION updated-watch.toml
sparrow $ENDPOINT watch remove REQUEST_ID WATCH_ID CURRENT_REVISION
sparrow $ENDPOINT watch handoff-prepare REQUEST_ID WATCH_ID CURRENT_REVISION RELATIVE_PATH INTENT.capnp
sparrow $ENDPOINT watch handoff INTENT.capnp
sparrow $ENDPOINT watch handoff-status INTENT.capnp
sparrow $ENDPOINT watch retire-claim WATCH_ID CLAIM_ID "REASON"
# Save intents outside consumption roots; retry the same intent after uncertain delivery.
# For a producer-finished tail, append TAIL_GENERATION_ID to handoff-prepare.

sparrow $ENDPOINT records --source SOURCE --text timeout --page-size 100
sparrow $ENDPOINT context STORE_ID RECORD_ID --before 2 --after 2
sparrow $ENDPOINT bytes STORE_ID RECORD_ID 0 BYTE_LENGTH evidence.bin

sparrow $ENDPOINT pin REQUEST_ID STORE_ID problem_investigation_1 10 RECORD_ID
sparrow $ENDPOINT pin ANOTHER_REQUEST_ID STORE_ID problem_investigation_1 10 ARTIFACT_ID
sparrow $ENDPOINT pin-get STORE_ID --name problem_investigation_1
sparrow $ENDPOINT pin-renew REQUEST_ID STORE_ID PIN_ID 10 EXPECTED_REVISION
sparrow $ENDPOINT pin-release REQUEST_ID STORE_ID PIN_ID

sparrow $ENDPOINT sparql investigation.json
sparrow $ENDPOINT mcp --stdio
```

Uppercase IDs, clocks, paths and lengths above are caller-supplied values from
the applicable receipts/configuration. Use a fresh request ID per mutation and
preserve it for retries; the CLI does not generate one. Pin create/add use the
`pin` union-and-extension
operation; an existing name is reported in the receipt, not silently replaced.
The SPARQL request file is the JSON object documented above, including store,
query text and optional scope/bindings/limits; its path is a local client input.

## 7. Rust client and logging integration

The separate `sparrow-client` crate keeps producers independent of the database
or semantic engine. The public surface includes:

- A client for artifact/event ingestion, receipt inspection, and explicit flush.
- A `tracing_subscriber::Layer` that composes with the application's existing
  subscriber rather than installing a global subscriber itself.
- Structured events with client instance/sequence identities, producer event
  time, level, target, message/fields, and current span/trace context. Span
  lifecycle/context becomes typed relationships with source evidence.
- Bounded batching/retry queues and visible delivery/drop counters. The default
  logging path is nonblocking and drops new events when full; it never silently
  allocates an unlimited queue or stalls a production thread on network I/O.
- Explicit buffered/durable delivery selection for direct ingestion and
  `flush(durable)`/shutdown that returns the reached receipt watermark or a typed
  error. Durable flush cannot certify events already dropped before enqueue.

Use stable client request/batch identities across retries. Reconnection preserves
pending event identity; application restart is a new client instance. Preserve
Rust event field types through the shared wire contract without substituting
formatted JSON strings for already structured fields. A bridge from the `log`
facade can compose through tracing. A persistent client spool is a later
extension; the initial layer does not promise survival of producer-process loss.
The layer follows the public [tracing subscriber layer contract](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/layer/index.html).

`DeliveryConfig::tcp(SocketAddr, source)` selects public Cap'n Proto for the
same bounded logging worker; `DeliveryConfig::new(path, source)` selects Unix.
The explicit `endpoint: Endpoint` field selects Unix, anonymous TCP or
CHAP-authenticated TCP; there is no second socket fallback. Logging producers
only ingest, so anonymous TCP suffices. Direct clients and workers share discovery, retries,
store identity checks and native field preservation. Both examples use the
shared `Endpoint::parse` spelling, `tcp://IP:PORT` or a Unix path. The tracing
example can therefore send to `tcp://192.168.2.4:518`.

`sparrow_client::TracingLayer::new(delivery.clone())` composes through
`SubscriberExt::with`; it never sets a global subscriber. One Sparrow layer per
subscriber uses bounded extension snapshots (1024 live spans, 16 KiB conservative
charge per snapshot). Text/binary field capture and debug/error rendering each
refuse values over 4 KiB; ordered field capture refuses over 128 fields or 16 KiB
of captured field charge. Context traversal stops at 32 spans and checks byte
admission before cloning each ancestor. Refusal drops the complete observation,
increments the delivery counters, and makes a later flush return an explicit
drop watermark. Oversized or contended span updates invalidate the snapshot;
subsequent context capture refuses rather than emitting stale values.

Primitive visitors preserve signed/unsigned 64/128-bit values, booleans, strings,
bytes and exact float bits. Debug/error renderings are distinct wire variants.
Span creation, updates, enter/exit, close and follows-from are typed observations;
explicit root/parent bindings use the subscriber's declared context. Subscribers
sharing a worker allocate span identities through `Delivery::next_span_id`, so
independent registries cannot collide within the producer instance. Direct typed
span producers sharing that instance must use the same allocator. Subscribers
that remap cloned span IDs are refused visibly rather than silently rewriting
identities. The normal Registry preserves IDs. Application formatters and the
subscriber's own bookkeeping remain application-controlled; the layer bounds its
own buffers and uses nonblocking access to its snapshot and delivery state.
### Tracing relationship observations (schema 59)

Source schema 59 adds occurrence-owned tracing RDF to typed producer records.
It reads the existing native ordered JSON facts for the explicit
`sparrow.events/1` source profile. It uses the original native scalar codecs and
RDF builder; it does not parse event JSON again or replace the typed client's
event validation. Ordinary byte uploads do not acquire producer authority by
claiming this profile. Original fields, repeated names and lexical source spans
remain available through the unchanged JSON projection and byte operations.

Record metadata carries `tracingProfile="sparrow-tracing-rdf/1"` and
`tracingState`: `complete`, `not_applicable`, `projection_unavailable`, or
`invalid_observation`. Invalid/ambiguous claimed observations expose a
`tracingDiagnostic` and no relationship prefix. Corrupt stored projection facts
and missing required migration inputs remain operational failures.
Graph coverage and native query certainty do not replace this interpretation
state: relationship queries must account for records without a complete tracing
interpretation before claiming absence or complete producer history.

Complete observations expose `eventKind`, `eventName`, `eventTarget`,
`eventLevel`, `clientInstance`, optional `observedSpan` and `followsFromSpan`.
The observing Record is the evidence subject; its existing receipt/event clocks,
producer sequence, parent artifact and byte range remain queryable.
Span identities are `urn:sparrow:span:<32 lowercase instance hex>:<nonzero ID>`
with class `Span`, `clientInstance` and typed `spanIdentity`. A process instance
separates reused span IDs. These are declared producer identities, not proof of
authentication or a global distributed trace identity.

Each `spanContext` object is an occurrence-specific context entry, with
`contextIndex`, `observedSpan`, optional `declaredParentSpan`, `eventName`,
`eventTarget`, and exact original `sourceByteStart`/`sourceByteEnd` for that
native context object. Context entries are identified by record ID and ordinal;
the same span observed twice remains two evidence observations. Parent and
follows-from claims stay on their observing entry/record. They do not establish
an inferred global parent or causal ordering. Span lifecycle kinds preserve the
original `span_new`, `span_record`, `span_enter`, `span_exit`, `span_close` and
`span_follows_from` observations; no missing lifecycle event is invented.

These facts share the existing record metadata graph and lifetime. A record pin
retains its observation after parent/producer-window expiry; it does not pin
other observations or span history. Removing metadata from the selected RDF
layers hides the tracing view too. Existing native SPARQL RPC, CLI/MCP, frozen
scopes and continuations query this view without a second execution path.

Writer formats 77..87 add tracing to the corresponding provenance authority
families 66..76. A bounded durable startup job replaces one producer record's
metadata graph per atomic page through the original RDF dictionary/refcount
owner. It reuses persisted native JSON facts, retains raw bytes and occurrence
clocks, and refuses missing inputs or admission exhaustion. Its 128 MiB page
accounts encoded/decoded input at the existing 65x input-plus-decoding law;
native RDF staging and disk maintenance admission remain independently bounded.
Older writers refuse the new format; every later authority transition preserves
the tracing fence. The running version is recorded separately in Development
runtime.

Run `cargo run --release -p sparrow-client --example tracing -- SOCKET SOURCE`
for application-owned scoped subscriber composition and an explicit durable
shutdown. The worker and its queue/receipt/retry implementation are shared with
direct producer events. Schema 12 adds explicit buffered/durable direct artifact
and event-batch ingestion plus server WAL fences; the delivery worker retains
its durable acknowledgment law.

### Server WAL acceptance and synchronization

Schema 17 adds typed `Status.pressureStatus`, shared by CLI and MCP status.
It distinguishes physical disk admission from buffered WAL queue pressure.
Its phases are `ready`, `requested`, `awaitingReclamation` and `blocked`, with
`awaitingArchive` added by schema 63;
integer fields report the outstanding write estimate, session-local retirement
totals and sampled native activity. The last logical pass includes an explicit
exhausted/record-limit/scan-budget/payload-budget frontier. Retirement counts do
not assert physical space freed. Writes are retried with their original request
identity and their own full physical admission check. A batch larger than the
entire data allowance is a `RESOURCE_LIMIT`, rather than a reclamation request.

Schema 18 extends the last logical pass with `catalogBudget` and `continuation`.
The session-local traversal resumes after complete scan/catalog prefixes and
restarts when effective pin protection changes or earlier time-index evidence
is published. Each pass revalidates active pins. Parent manifest cleanup resumes
at its unfinished ownership; hard failures leave the position unchanged.
Exhausted traversal is reused only while protection and publication remain
unchanged. Continuation and retirement counts do not certify physical recovery.

Schema 19 merges record and recordless-artifact eviction timelines by receipt
time and stable evidence ID. `EvictionPass.recordlessArtifacts` counts reclaimed
empty artifact catalogs, also included in its total `artifacts` count.
`PressureStatus.evictedRecordlessArtifacts` is a cumulative session-local count.
The former record-only frontier is named `unitLimit`: one new record or one
recordless artifact consumes one retirement unit. Protected counts include
whole-artifact protection of empty inputs. Reclaimed empty artifacts return
`NOT_FOUND` on metadata/byte reads; accepted request/import retries still return
their original receipt identities and cannot recreate the retired catalog.

`Ingest.durability` and `eventsIngest`'s durability argument default to `durable`.
Buffered mode publishes the same atomic catalog/payload/index batch with WAL
enabled, without promising synchronization. Its artifact receipt carries
`bufferedThrough` with persistent store ID, fresh opened-session ID and native
sequence. Durable receipts carry no buffered fence. Inspection/retry can upgrade
a buffered acknowledgment to durable without changing occurrence IDs. Durability
mode is not part of the original payload/request identity.

The SDK exposes `ingest_with_durability`, `events_with_durability` and `flush`;
CLI `flush STORE_ID SESSION_ID SEQUENCE` and MCP `flush` share the same typed
client implementation. The MCP tool is a mutation and takes decimal-string
`sequence`, `store_id` and `session_id`; its result identifies the reached fence.
Flushing can cover later accepted writes while holding the serialized writer.
Foreign stores, older sessions and future sequences are refused. After reopening,
inspect the original artifact or retry the stable request to learn recovered
state; a numeric sequence from another session cannot prove its survival.

The durability ledger admits at most 256 unsynchronized atomic writes and 64 MiB
of atomic batch byte charge. At either limit, new buffered acceptance refuses
with retryable `STORAGE_PRESSURE` before publication. Default intended WAL sync is
2000 ms, configurable from 1..60000 ms. Status exposes session, accepted/durable
sequences, pending count/bytes, age, interval, lag and latched storage uncertainty.
Any synchronized control write covers earlier WAL writes; empty control batches
use explicit synchronization. Write/WAL-sync failure latches uncertainty and
refuses further receipt certification until reopening. Opening synchronizes
recovered data before starting a fresh session. These fences certify storage
durability, not retention extension, producer drops or power-loss qualification.

## 8. Errors and compatibility

Typed errors carry `code`, a concise `message`, `retryable`, `request_id` when
applicable, and bounded `details`. An error does not include a successful query
prefix after an operational failure. Refusals do not silently select a smaller
scope, shorter pin, altered datatype, or unrelated source generation.

| Code | Meaning |
| --- | --- |
| `INVALID_ARGUMENT` | Invalid IDs, ranges, scopes, durations, or names. |
| `NOT_FOUND` | Unknown or reclaimed evidence/handle. |
| `EVIDENCE_UNAVAILABLE` | Known expired, evicted, or incomplete complete-file representation. |
| `PIN_NOT_ACTIVE` | Expired/released collection cannot be renewed or used as an active scope. |
| `CONFLICT` | Retry identity reuse, revision mismatch, or conflicting source generation. |
| `INDEX_NOT_READY` | Required projection coverage is unavailable, with pending/failed diagnostics. |
| `RESULT_EXPIRED` | Bounded snapshot/result lease ended. |
| `RESOURCE_LIMIT` / `RESULT_TOO_LARGE` | Host admission/output refusal. Native certified governor outcomes retain their read receipts. |
| `STORAGE_PRESSURE` | Insufficient safe write capacity, with retry state. |
| `STORAGE_FAILURE` | Typed I/O, integrity, or commit failure; retryability follows the actual cause. |
| `UNSUPPORTED` | Unsupported format operation, query capability, or mutation on the read surface. Raw artifact preservation can still succeed independently of projection support. |
| `READ_ONLY` | A write sent to a query-only daemon (schema 67). Not retryable against that daemon. |
| `PERMISSION_DENIED` | A non-producer operation on the ingest socket, or an import continued by an account other than the one that began it (schema 68). Not retryable on that connection. |

`STORAGE_FAILURE` also covers a publication at or behind the archive watermark,
which signals a clock regression, and retirement reaching unarchived evidence.
`INVALID_ARGUMENT` covers opening an archiving store without its `[archive]`
table.

Expose API, store-format, codec/profile, and governor identities in discovery.
Changes to filtering, certainty, retention, identity, or pin semantics require
explicit versioning. Existing raw evidence must remain reconstructible through
supported migrations. Transport adapters share contract fixtures for scope,
pagination, pin receipts, certainty, and errors; CLI and MCP must produce the
same domain result for the same snapshot and operation.

### Schema 40 retained complete-file byte reads

`ReadBytes` has a typed target union: its existing `evidence` branch is an
artifact/record reference, and `fileGeneration` is a store-scoped
`TailGenerationQuery` with the current discovered schema. The same `bytesRead`
method, SDK `Client::bytes`, CLI `bytes`, and MCP `bytes_read` serve all targets.
CLI/MCP accept `file-generation:` IDs; offsets are relative to the original file.
Each response certifies the requested store and exact length through one SDK path.

A generation target requires a complete, gap-free terminal checkpoint and its
entire original contiguous byte cover to remain eligible under retention or
explicit artifact/record pins. Checking only the requested subrange is insufficient.
The serialized storage owner validates index/record identity, original declaration,
ordered nonoverlapping positions, final offset and record count. It reuses the
existing checked chunk reader and returns no successful prefix on refusal.
Missing, expired or evicted cover gives `EVIDENCE_UNAVAILABLE`; contradictory
catalog/index provenance gives `STORAGE_FAILURE`. Surviving records remain
independently readable. An empty completed file supports an exact zero-length
read until its terminal metadata deadline.

The range ceiling remains 1 MiB with strict offset/length bounds. Cover admission
has 100,000 pin/range visits, a 64 MiB streaming record catalog byte ceiling and
64 KiB maximum record row. Pin decoding reuses the existing conservative capture
charge with 64 MiB admission and 1 MiB maximum encoded pin row. Exceeding admission
returns `RESOURCE_LIMIT`, never a partial cover certificate. Rows are decoded and
discarded incrementally; only requested bytes are assembled. The read does not
pin evidence, refresh clocks, create occurrences, or consult current source files.
A multi-call download can lose eligibility between calls; retain explicit pins
on the constituent artifacts/records when the investigation requires it.

Generation metadata's `complete` still denotes the original terminal checkpoint,
not current byte availability. A successful zero-length file read can probe full
cover under the same admission. A readable generation grants no source-removal
authority; source retirement requires the explicit handoff below.

### Schema 41 completed-tail source retirement

`WatchHandoff.tailGenerationId` names an existing producer-finished tail
generation. The intent still binds store, request, original watch revision, raw
relative path, and the exact final descriptor observation, including nanosecond
modification and change clocks. An active-file watch requires this target;
an ordinary completed-file watch requires it absent. A close, idle interval,
readable byte range, or retain-watch definition alone grants no retirement.

Admission requires the original complete, gap-free producer checkpoint and
matching final observation. The journal seals that checkpoint, including original
acceptance/completion clocks, before the rooted filesystem claim. The claim
worker acknowledges the existing checkpoint instead of uploading the file again.
It then removes only the private exact claimed generation, after directory
synchronization. Replacements at the original path remain independent inputs.
Pending ownership survives bounded checkpoint-registry expiry without refreshing
record retention or pinning bytes.

The receipt retains `reserved`, `claimed`, `published`, and `removed` phases.
For a tail target, `artifactId` is absent throughout; `tailGenerationId` is absent
until publication and then equals the original target. Ordinary handoff retains
its artifact receipt and no tail publication identity. The SDK certifies the
original intent, durable phase, and matching publication kind before reporting
progress. Only `removed` confirms synchronized source retirement.

CLI preparation adds the target as a final positional argument:

```text
sparrow SOCKET watch handoff-prepare REQUEST WATCH REVISION RELATIVE_PATH INTENT.capnp GENERATION_ID
sparrow SOCKET watch handoff INTENT.capnp
sparrow SOCKET watch handoff-status INTENT.capnp
```

Preparation checks the original terminal producer finish against the current
file observation and saves the immutable Cap'n Proto intent outside watch roots.
Retries use that saved file rather than capturing a replacement path. Existing
ordinary intent files continue to decode with no tail target.

MCP `watch_handoff` and `watch_handoff_get` use the same SDK operation and receipt
renderer. Arguments are `store_id`, `request_id`, `watch`, `watch_revision`
(decimal string), `relative_path_hex` (canonical raw Unix path bytes), `observed`
(the same exact descriptor fields as `file_finish`), and optional
`tail_generation_id`. Mutation is marked destructive; progress lookup is read
only. Unknown fields and oversized/noncanonical raw paths are refused.

## Bounded logical retention metrics

Schema 50 adds `retentionMetrics` through the certified SDK
`get_retention_metrics`, CLI `retention-metrics STORE_ID` and read-only MCP
`retention_metrics`. The query requires the discovered store/schema identity.
Optional admission is `max_visits` (default 10,000, maximum 100,000),
`catalog_bytes` (default/maximum 32 MiB), and `payload_bytes` (default 64 MiB,
maximum 1 GiB). CLI spells those as `--max-visits`, `--catalog-bytes` and
`--payload-bytes`. Integer values must be positive and within their limits;
unknown fields, null limits and repeated CLI options refuse. No partial account,
lease or successful prefix is returned when admission is insufficient.

One serialized read counts eligible published records and complete artifacts,
including empty inputs and evidence protected past ordinary expiry. It reports
the oldest eligible receipt clock and elapsed `retained_window_ms`; both are
independent of source event time. An empty live catalog has absent
`oldest_received_at` and a zero window. Active pins can extend this observed
window beyond thirty days; early eviction can shorten it. The window describes
the oldest surviving occurrence, not uninterrupted coverage of every instant.

Raw payload accounting follows native reference counters and the current
artifact-manifest retirement checkpoint. Each chunk is content-verified once
and counted once, even if multiple records/files share it. `owned_payload_bytes`
covers all chunks with native payload reference counters. `live_payload_bytes`
covers any eligible published owner. `pin_held_payload_bytes` covers any actively
pinned eligible owner and overlaps live bytes, including bytes whose ordinary
retention remains active. `retirable_payload_bytes` covers chunks exclusively
owned by ineligible published artifacts/records. A pinned/live/private owner
prevents a shared chunk from entering that count. `unclassified_payload_bytes`
covers chunks with additional native references outside the published catalogs,
such as private preparation, and may overlap live bytes. These are not additive
categories. Encoded upload staging, projections, indexes, WAL, compression and
physical files are excluded; physical usage remains a separate status sample.

`pinned_records` and `pinned_artifacts` count eligible occurrences under explicit
active protection. `payload_chunks` is the number of unique referenced chunks;
`visits` counts admitted catalog rows, payload ownership references and terminal
pin seeks. Catalog construction and ownership maps are charged before ownership;
payload admission is checked before hashing borrowed native input. Missing or
contradictory reference counts, corrupt payloads and invalid catalog identities
produce `STORAGE_FAILURE`. Cancellation shares the connection/caller read lifetime
and preserves worker admission through retirement. The SDK checks store identity,
wire size, counts, ownership bounds and clock/window agreement before agents
render the result. UInt64 counters use decimal strings in CLI/MCP JSON.

This read does not advance intake, synchronize buffered writes, retire evidence,
renew pins or run compaction. Logical retirement eligibility never certifies
physical bytes freed or recovery of storage admission. Forced bounded physical
reclamation and sustained operational qualification remain separate requirements.
