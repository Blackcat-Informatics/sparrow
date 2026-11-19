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

### Cruft

Cruft is what a producer sends Sparrow as material it has set aside: files and
records the producer no longer wants in its own store but does not want lost
while it may still be wanted. `lillith_memetics` is the producer, and its cruft
specification owns what an item means. Sparrow gives cruft no special path,
type or retention: an item of cruft is an ordinary artifact or record. Sparrow answers only where the item is and how long it
stays.

**Intake.** `lillith_memetics` sends cruft through the typed ingest and import
operations. Sparrow accepts them on the ingest-only Unix socket
(`ingest_socket_path`, see the RPC binding in section 1 and the ingest socket in
section 5) and on the private socket, like any other producer. The ingest socket records the sender's uid,
gid and pid in the item's provenance (`intake`). Every item is under the
source the producer names; the source is the item's lasting address in queries.

**Limits.** Sparrow has no per-artifact byte limit (see
[Large artifacts](#large-artifacts-schema-65)). What bounds an item is:

- the disk budget, the maintenance reserve and the free-space floor
  (`disk_budget`, `maintenance_reserve`, `min_free_bytes`): while nothing can be retired, ingest is
  refused with `STORAGE_PRESSURE`, which is retryable, once the store reaches the
  budget less the reserve or free space is below the floor;
- the inline ingest bound: one inline `ingest` carries at most 16 MiB
  (`Discovery.maxInlineArtifactBytes`) and a larger one is refused with
  `INVALID_ARGUMENT`, which is not retryable. It applies to a single inline
  `ingest`, not to imports; a larger input goes through the staged import;
- `max_records`, the records in one artifact;
- `MAX_IMPORT_BYTES`, 8515420160 bytes (129935 chunks, about 7.93 GiB), a fixed
  bound on one `import_begin` until the v2 store format (#5). A larger input is
  split into parts of at most that many bytes, each imported as its own
  request, and then the manifest is imported;
- the cost of the form sent (see below): an item sent as `jsonl` can be
  refused above bounds that are not fixed byte limits, and it occupies about
  10 to 51 times its size.

**What the store counts as disk use.** `linked_physical_bytes` counts the
write-ahead log (WAL) files and the table files, not the memtables or caches.
In-flight WAL, up to about 256 MiB, is part of it and falls after a memtable
flush. Admission and the archive size trigger both use a figure that includes
it, so a store can refuse ingest or start archiving on bytes that a flush
returns. `status.physical_bytes` is the live figure and `pressure.linked_bytes`
the figure of the last pressure pass. An empty store reads under 0.1 MB.

**What an item costs.** The stored size depends on the record and the format,
not on a fixed overhead. The table gives the settled figure after a restart,
from one scratch measurement at main b420503 with a 20 GiB budget. The producers'
own files, such as the mentation traces, are not measured. Read the factors as
ranges to size from, not as the figure for any one producer.

| Sent as | Input | Stored per raw byte |
| --- | --- | --- |
| `jsonl` | random base64 lines of about 3 KB | 9.9x to 13x |
| `jsonl` | log-like records of about 170 B | 51.3x (about 8.7 KB per record) |
| `opaque` | log text | 0.21x to 0.27x |
| `opaque` | incompressible bytes | 1.03x to 1.08x |

Right after an ingest the figure is higher because of the WAL: 2.7 MB of random
`jsonl` read 44 times its size before the restart. Small records are the worst
case for `jsonl`. Every measured ingest used the CLI `ingest`, which always goes
through the staged import; the inline path under 16 MiB is not measured.

**Refused ingests.** Two measured ingests ended `RESOURCE_LIMIT`. A
single 100 MB random-text file (33,015 lines) ended after 935 s with "atomic
source context update exceeds metadata admission". A 10 MB file of 58,945
log-like records ended after 635 s with "RDF write staging exceeds 64 MiB"; the
store went from 138 MB to 817 MB, so the refusal added about 679 MB. Sent as
`opaque`, 100 MB of random bytes ingested in 9 s; no log sends the refused files
as `opaque`. The smallest triggers measured are 32,769 records of `{"a":N}` (382
KB), refused after 125 s with the source-context message, and 60,000 such
records (840 KB), refused after 204 s for RDF write staging.

A refused file ingest leaves its prepared records. Preparation commits in
checkpoints of at most 256 records or 256 KiB of source, each a synced write, so
the 100 MB file with lines of about 3 KB commits about 85 records at a time.
Both refusals come after every record is prepared, so all of the residue is
preparation state. The CLI `ingest` cancels on the failure, and every measured
ingest used it. A refused SDK `ingest` or `ingest_with_durability` above 16 MiB
does not cancel: it keeps its prepared records and its journal slot until the
2-day retry window ends. Reclaim removes one prepared record per pass, at
between about 14 and 147 records per second in the logged idle windows. Reclaim is starved while an ingest runs: one 935 s refusal
drained about 2,000 records. Space returns only after tombstone compaction.

Repeated refusals can drive `physical_bytes` to `STORAGE_PRESSURE`. Five
back-to-back refusals of 32,769 records took it to 152, 324, 535, 436 and 642
MB. Refusals of two different 100 MB files took it from 713 MB to 2.54 GB, and
the next ingest answered `STORAGE_PRESSURE` at 3.13 GB against a 3.22 GB budget.
The lockout is temporary. A one-record ingest succeeded at 3.15 GB, 13 s after
the last sample that read pressure phase `requested`; a sample in the same
second read `ready`. A 2.7 MB file was then refused `STORAGE_PRESSURE`, and
native compaction later brought physical bytes from 3.45 GB to 2.55 GB. Not
measured: the 16-journal limit (`MAX_IMPORTS`), where an unreclaimed refusal
holds its slot (no log counts the journals held), the exact staging threshold,
and reclaim to zero after the 60,000-record refusal. At about 0.9 GB per 100 MB
refusal, the live status of 2026-11-17 (38.94 GB used of a 53.69 GB budget,
about 14.7 GB of headroom) would reach the ceiling after about 16 refusals (14.7
GB divided by 0.9 GB, before the reserve and the batch estimate); that is an
estimate, not a measurement. Issue #389 tracks the fault.

**Choosing the form.** An item sent `opaque` costs at most about its size (0.21x to
0.27x for log text, 1.03x to 1.08x for random bytes) and is found by payload id only
(`payloadLookup`); there is no search by value. An item sent as `jsonl` is
searchable by value (`values_search`, `records`) and costs about 10 to 51 times
its size. The measurements support this guidance and no more:

- Send `jsonl` in bounded chunks of whole lines, each of at most 32,768 records:
  one publication holds at most that many distinct records, and a larger
  artifact is refused with `RESOURCE_LIMIT` (the RDF staging message may come
  first, as at 60,000 records). Ten files of 10 MB with lines of about 3 KB (100
  MB; 112.7 MB with the two earlier files) ingested. A single 100 MB file and a
  10 MB file of tiny records did not. No safe chunk size is measured for small
  records, and 100 MB of log-like `jsonl` was not run.
- Send large files, and any file whose records need no search by value, as
  `opaque`. A file between 16 MiB and the import bound goes through the staged
  import as one artifact with one payload id; a larger one is cut into parts.
- Back off on `STORAGE_PRESSURE` and `RESOURCE_LIMIT`; do not retry either at a
  fixed interval.

**Finding a sent item.** The one key for a file or an opaque item is its payload
id: `blake3:` and 64 lower-case hex digits, the BLAKE3 hash of the artifact's exact
bytes. A SHA-256 is not an index key. Sparrow computes the payload id of every
complete artifact, so the sender needs no companion record: it hashes the bytes
it holds and asks.

- **Live.** `payloadLookup` (CLI `payload`, MCP `payload_lookup`) returns the
  artifacts that hold those bytes, in pages, with `complete` (CLI and MCP
  `answer_certainty` `exact` or `pending`). Repeated bytes make several
  artifacts, possibly under several sources, and the lookup returns every one,
  so the caller counts rows. One artifact read gives each hit's source. See
  [Payload id index](#payload-id-index-store-format).
- **Archive.** `sparrow-archive list DIR` prints one JSON line per segment with an
  `entries` array; each entry gives the artifact `id`, `source`, `received_at`,
  `byte_length` and `payload_id` (`blake3:` form, the same string `payloadLookup`
  takes). The reader matches the payload id, or the artifact id from the ingest
  receipt as the second route, then runs `sparrow-archive extract DIR ARTIFACT_ID`,
  which writes the artifact's verified bytes to stdout. The reader hashes the
  output and compares it with the payload id, since `extract` checks each record
  and the total length, not the whole-file hash. `sparrow-archive verify DIR`
  checks every segment. All three are offline and read-only. An artifact of
  several records still lists the payload id of its whole bytes. A segment that
  lacks it is refused by `list`, `verify` and `extract`.
- **Not held.** Live: a first page (no cursor) that is empty with `complete = true`.
  Archive: no payload id in the listing of any segment, from a `list` that exits 0
  (`list` prints the segments it decoded before it fails on a bad one, so a
  failed run proves nothing). Not held anywhere means
  both, with the archive read after the archive worker has run on that data.
- **Limits.** Whole artifacts only: a line inside a JSONL record has no payload
  id. Live holding differs from archive holding: the live index lists an artifact
  only while the store holds all its bytes, so an expired or evicted artifact is
  not listed live though the archive may hold it, and not held live is not not
  held. Until the `payload-index` migration completes (`complete = false`), a
  listed artifact is held and an empty list means unknown, never not held. A
  file that is cut into parts is found by its parts, each under its own payload
  id, and the whole file's hash is no artifact's payload id. A file is cut only
  where one artifact cannot hold it (the import bound, the record limit or the
  joint retirement bound; see [Part sets for consume roots](#part-sets-for-consume-roots-280)),
  and a syslog file over the packet limit is cut too. A file below those limits,
  including a staged import above the 16 MiB inline bound, is one artifact found by
  the hash of all its bytes. A tail checkpoint's payload id is the hash of
  its checkpointed range. The `payload` lookup is not scoped to a source.

As a convenience for records, `values_search` over a source finds a JSONL record by an
exact JSON field value, such as a `digest` the sender put in it; that is not the
contract. `records` over a source lists its records.

**Retention Sparrow guarantees.** Nothing is deleted that has not been archived:
the deletion gate, the archive watermark and the store fence enforce it (see
[Archive-before-delete gate](#archive-before-delete-gate-schema-63)). An unpinned item is due at `received_at + 30 days` and is retired then once the
archive holds it. Archiving starts early, by default at 75% of the window, and a
segment counts only once it is written and verified. Disk pressure can retire
archived, unpinned items earlier. A pin keeps an item until the pin ends. The
deletion gate keeps every unarchived item past its deadline, so with no archive
running (the live daemon today) nothing retires and ingest is refused with
`STORAGE_PRESSURE` at the budget less the reserve or below the floor. Source timestamps do not move the deadline. Whether the
archive worker runs on the live daemon is stated in
[Archive status](archive-format.md#archive-status).

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
previous serial maintenance path. Socket acceptance, SIGINT selection and the
wait for each worker's end signal continue on the serving loop; no worker health
check runs on a cadence.

Expiry retains its bounded pass limits and resumes an explicit continuation
after yielding, without waiting one second between every pass, but only while
due work remains. A pass walks the receipt-ordered `time:` and `empty-time:`
indexes from two in-memory cursors, one that retires at the deadline
(`received_at + RETENTION_MS`) and one that deletes a day later, and stops at the
first key whose deadline has not arrived, so its cost follows the due work and
never the store size. Scanning the whole catalog under the writer lock made
reads wait on the same lock for over 30 seconds on a 10 GB store (#183). Entries a
pin holds are skipped, not retired, and are not read again until a pin ends. A
restart re-walks at most about a day of retired rows. Expiry already applies to
reads, so only physical retirement waits.

`ExpiryReport.next_due` is the earliest clock reading at which a pass can find
work with no external event: the next retirement or deletion deadline or the
earliest live pin deadline. The worker sleeps until then, never less than one
second and never more than one hour. The ceiling protects against a wall clock
that stepped forward, since the sleep itself is monotonic. A pin release, a pin
commit and an archive commit wake the worker at once. Below the free-space floor the
pass runs under the maintenance reserve (#267, below); when free space is under the
reserve it is refused before it reads anything, and it then waits for the floor's resume,
which wakes it at once (#247), or for the pressure pass, which wakes it the first time it
finds free space back above the reserve while a floor episode is open, with no retry clock
and no journal line per attempt; the one-hour ceiling is only its backstop. A pass that failed for any other reason retries
after 60 seconds, for want of any event that says it would now succeed. Retry rows
(`request:`) have no time index; one pass scans them once per hour, and their
logical expiry is exact at read time regardless. The purge of expired read
leases runs on its own 60-second tick, not inside expiry. Expiry honours each row's
own `expires_at`: a row whose deadline differs from receipt plus retention is
counted in `ExpiryReport.anomalies` and retires at its own deadline, never
stopping the pass; a parent artifact whose own deadline is later keeps its records
undeleted until then. The count is per pass, and the worker journals it. A
dangling time-index key is the only data-dependent hard failure.
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
immutable intent when reconnecting. When the reservation begins, every waiting
writer and every later one is answered `SHUTTING_DOWN` at once. A failed admission drain is reported
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
logical retirement does not certify native physical reclamation.

#### The wait for reclamation ends (#211)

`awaitingReclamation` used to end only when linked bytes fell below their value at
the retirement. A retirement whose flush is no smaller than what it freed, bytes in
the data directory that RocksDB does not own, or growth during the wait left it
waiting for ever while every data write answered `STORAGE_PRESSURE`, and a restart
restored the same wait. Now, once a pass finds no native flush, compaction or
snapshot pending and linked bytes still not below the wait's start, it forces one
manual compaction of each family. The compaction runs on its own thread, never under
the writer lock or in the maintenance loop; the native call returns no status, so
only the measured linked bytes judge it. The next pass after it ends measures. If
linked bytes are still not below the wait's start, the wait is released
(`waitsReleased`) and that same pass retires more evidence: the earlier
retirement is durable, so waiting longer protects nothing. Evidence still
protected by a pin or the archive watermark is never retired by this, so a store
whose every remaining unit is protected stays `blocked` or `awaitingArchive`
and says so (`waitingOn`, `stalledPasses`); a pin release or archive commit wakes the
pass at once. These cannot end in process by design: the pin and the archive
gate are protections that outrank intake, and RocksDB's background-error count
is cumulative since open and cannot be cleared in place. A background error fails
only the pass that first sees it (#218): the wait ends, the error is counted in
`nativeErrors` and journaled, and the next pass goes on, so an error never holds intake
until a restart.

The pressure pass writes a retirement batch, a flush and the forced compaction. All are
small beside the maintenance reserve the budget withholds for them, so the pass runs
below the free-space floor, where it is what frees space, but needs the reserve itself
free on the filesystem; with less it does nothing that writes and reports why. Its two commits, the retirement batch and the clearing of the wait marker (and only those), are admitted against that reserve instead of the floor; every ordinary write keeps the floor.

Ageing out and the read-lease purge also run below the floor (#267). Ageing out is
retention that was only delayed: it retires evidence past its deadline in the same small
bounded batches as the pressure retirement, so its commits run under the same reserve
admission, and it never retires an unarchived entry or a pinned one. It does not run
import reclamation, and it refuses before reading anything when free space is under the
reserve. It frees bytes only after native flush and compaction, so when it retires
evidence for a floor episode that asked for reclamation it opens an in-memory pressure wait
(no stored marker, no format change) that the pass measures and ends, forcing a compaction
if linked bytes do not fall. Within one floor episode ageing out goes first: the pass waits
("waiting for ageing out to run first") until expiry has run or failed (a refusal at the
reserve is not a run; ageing out is asked again when free space clears the reserve), and
only then retires archived evidence early. Ageing out has no per-episode cap: evidence past
retention is not a policy risk, and a cap would leave aged-out data on disk and writes
refused while an honest avenue remains. What protects the reserve headroom the pass's own
flush and compaction need is the batch size (the ordinary 32 units) and that every batch is
admitted against the reserve, so a batch that would find free space under it is refused;
the duty continues a backlog by its continuation flag (an event) until nothing is due or the
reserve refuses, and a refusal parks it until the pass finds free space above the reserve
again. On a store with no archive policy
ageing out is the only avenue. The lease purge writes nothing and ends the read snapshots a
pass stalls on ("open read snapshots"), so it runs below the floor and wakes the pass.
Import reclamation and online migration write more than they free (a migration page
rewrites data) and still wait for the floor; import reclamation deletes staged chunks and is
a candidate for a follow-up.

The pressure status names the three floor states: reclamation not yet attempted (phase
`requested`, "reclamation requested, not yet attempted"); attempted and in progress
(`requested` while ageing out has not run, `awaitingReclamation` while a retirement of
ageing out or of the pass waits for native work, with what it waits on); attempted and
nothing reclaimable (`blocked` or `awaitingArchive`, "reclamation tried"). Ageing out
that retired evidence shows as in progress, and one that found nothing aged leaves the
episode's other avenues to say what is left.

The worker no longer retries every second and discards the result. After a pass that
made progress it looks again after a second; each pass that made none doubles the delay
to a ceiling of 30 seconds, and a pin release, an archive commit or the end of a forced
compaction, the floor's resume, and a refused write that requests a pass run a pass at once
(#247). The delay is the one observer of native RocksDB flush and compaction state: the store
has no completion hook for it, so the pass reads the pending counts itself, and waiting less
cannot change what native work does. The daemon journals every entry to and exit from a
non-ready phase, and, at most once a minute, the passes that made no progress.

### The free-space pause (#257)

Below `min_free_bytes` the daemon keeps working; only writes wait.

- Accepting. The accept loops of the private socket, the ingest socket, public TCP and
  syslog TCP no longer stop. Connection slots are still taken before accept (the
  ceiling, #246) and the 60-second lease is unchanged, so the floor adds no new
  bound. A syslog TCP frame refused for the floor is held unread past (#243) whether
  or not its connection was accepted before or during the pause. Syslog UDP is held by
  its own hold (below): datagrams queue in the daemon up to `hold_bytes`, then in the
  kernel buffer. Three things stay paused, because they are the writes
  themselves: configured consume and tail watches (their source files are the spool;
  they observe and claim nothing), the daemon's maintenance that writes more than it
  frees (import reclamation and online migration; ageing out, the lease purge and the
  pressure pass run, under the maintenance reserve), and opening the store at startup (nothing can answer before the store opens). A paused
  watch worker owns no timer: it observes the floor through the floor's ledger and
  blocks on the floor's resume edge, which any number of duties subscribe to. The edge
  is fired by whichever observation sees free space return, including the daemon's
  one-second floor observation for space freed outside the daemon.
- Writes. A write is answered with `STORAGE_PRESSURE`, `retryable` true and, new,
  `Failure.class = freeSpaceFloor`. The field is added after `retryable` (no schema
  version change). An older client ignores it and handles the refusal as the retryable
  `STORAGE_PRESSURE` it always did; the new Rust client reads it. The class is set only
  for the floor's own refusal. Budget pressure, `RESOURCE_LIMIT` and every other error
  stay `unclassified`.
- Rust client. A floor-labelled refusal is held: the immutable pending batch is retried
  on the normal backoff, never quarantined or dropped, and later events queue behind it
  up to the existing 16384-event / 64 MiB bound. `DeliveryStats.floor_refusals` counts
  the retries the floor caused, and `last_transient_failure` carries the refusal and
  its class (`Error::is_free_space_floor`) until a batch is acknowledged.
- Reads. Discovery, status, gets, record, text, key, value and artifact queries are
  served. Verified: no read path calls the floor's write check, and a read takes a
  reader slot, not a writer permit. The one read that writes to disk is query staging,
  which spends free space outside the store's own accounting. Sized separately (R5):
  above the floor a query stages what it did before (up to 64 MiB, within the store
  allowance). While paused, all reads together stage at most 32 MiB
  (`PAUSED_READ_STAGING_BYTES`), and the staging reserved by all reads together never
  exceeds the free space above half the floor, counting what earlier reads reserved but
  have not yet written. A query that cannot fit is refused with the same floor-labelled
  `STORAGE_PRESSURE` (retryable; the reader waits for space like a writer) and counted.
  Reads of memory-only or store-resident data need no staging.
- Counters. `Status.floorPause` (field 60, additive): `pausedSince`,
  `connectionsAccepted` (accepted while paused), `writesRefused` (RPC writes answered
  with the floor class), `readsServed` (reads that began paused and succeeded) and
  `readStagingRefused`. CLI and MCP status render them as `floor_pause`. The existing
  `pressureStatus.floorRefusedWrites` counts every refusal at the store, including
  internal writers and held syslog frames offered again.
- Journal. The accept loop observes the floor once a second (the cadence the
  maintenance worker uses; the end of a pause caused by another process freeing space
  has no event). It writes one line when a pause begins, one each minute while it lasts,
  with the counters, and one when it ends. A pause that begins within a minute of the
  last line is announced only if it lasts a minute, so a volume hovering at the floor
  cannot flood the journal, and no long pause is silent.
- Spool. A daemon-side file spool is not built for UDP. A
  spool would append to the filesystem the floor protects, so it would need its own
  higher floor or a separate filesystem. What remains: Syslog UDP datagrams are held in the
  daemon up to `hold_bytes` and then in the kernel buffer, and are lost only past the
  kernel buffer or on an unclean daemon exit (see the UDP hold section); every other source holds at the producer (client queue, ship queue, source
  file, sender socket).

### A floor refusal requests reclamation (#267)

The refusal stays immediate and typed; it now also leaves a pass in motion. A write
refused at the floor (every commit path, and the intake fence) records that someone was
turned away, moves the pressure phase from `ready` to `requested` and wakes the pressure
pass through the same waker a budget refusal uses, once per episode: later refusals cost a
flag read and wake nothing. The episode ends when a pass measures the deficit at zero. The
pass measures the deficit itself (`min_free_bytes` less the free space of the store's
filesystem) and keeps it apart from `requestedBytes`: that field is a store-footprint
quantity compared with the disk budget, and a free-space deficit added to it would make an
under-budget store look ready. The pass acts on the floor only after a write was refused;
the pause edge itself requests nothing.

What the pass may retire for the floor is limited. It uses the same oldest-first avenue as
the budget path, so it never deletes unarchived evidence and never deletes pinned evidence,
and it applies only to a store with an archive policy. Archived evidence has its copy, so
retiring some of it earlier than the budget would, to take the log being offered now, is
allowed up to a bound: the first measured deficit plus `maintenance_reserve`, counted in
linked store bytes freed. Past the bound the pass retires nothing more for that episode and
says the rest is held outside Sparrow. Freed bytes lag retirement until native compaction
runs, so the episode is also capped at 8 retirement batches, whatever they have freed so far. A store with no archive policy retires nothing early
for the floor: nothing there is marked archived, so "archived" protects nothing. There a
floor episode uses ageing out only, described below.

The honest state of a store with no archive policy and nothing expired: at the floor it has
no avenue left and refuses writes until space returns from outside. The counters that show
it are `floorPause.writesRefused` (RPC writes answered with the floor class), the pressure
status `floorRefusedWrites` (every refusal at the store) and the new floor fields below, with
`waitingOn` saying in words that free space is below the floor by N bytes and that the store
has no archive policy, so nothing is retired early.

The status says which of three states the floor is in:

- Reclaim not yet attempted. Phase `requested`, `floorRequests` counts the episode and
  `waitingOn` says a write was refused and reclamation is requested but not yet attempted.
  This lasts until the woken pass has run.
- Attempted and in progress. Phase `requested` or `awaitingReclamation`; `waitingOn` says
  how far below the floor the store is and what it waits on: a native flush or compaction, a
  forced compaction, open read snapshots, or the free space the pass itself needs (the
  maintenance reserve).
- Attempted and nothing reclaimable. Phase `blocked` or `awaitingArchive`; `waitingOn`
  says free space is below the floor by N bytes and that reclamation was tried (passes and
  bytes freed), then why nothing more can go: entries are protected by a pin, the archive
  watermark has not reached the oldest evidence, the store has no archive policy, or the
  episode's bound was reached and the rest is held outside Sparrow.

A client that does not retry is a loss the store cannot count. Sparrow counts the refusal,
not what the producer then does with the data it was refused: the producer's discard is not
visible to the store. The counters that show the refusals are `floorPause.writesRefused`
and the pressure status `floorRefusedWrites`; a producer that holds and retries (the Rust
client, syslog TCP, the UDP hold) is not a loss, and its own counters say how much it held.

The daemon journals the edges of an episode (it begins, it reaches its bound, it ends)
through one limiter of at most one line a minute; a line held back is counted into the next
one, so a volume hovering at the floor cannot flood the journal and a long episode is still
reported by the quiet-pass line each minute with the same counters. No line is written per
refusal.

The status `pressureStatus` extends additively (schema 71, no version change; an older
reader ignores the fields and an older server leaves them zero): `observedAt` (the last
pass's clock), `linkedBytes`, `episodeSince` (intake refused since), `phaseSince`,
`waitSince` and `waitBeforeBytes` (the wait's start and its linked bytes at the start),
`forcedCompactions`, `waitsReleased`, `stalledPasses`, `refusedWrites` (data writes
answered `STORAGE_PRESSURE` at physical admission), `waitingOn` (why the last pass could
do no more), and for the free-space floor `floorPausedSince`, `floorPauses`,
`floorResumes` and `floorRefusedWrites`. #218 adds `forcedCompactionMs` (wall time of the
last forced compaction), `forcedCompactionFamilies` (a list of `family`, `beforeBytes` and
`afterBytes`, the table and blob file bytes of each family around it), `forcedFailures` (forced
compactions that ended in a panic or a raised native error) and `nativeErrors` (new RocksDB
background errors the pressure passes saw); again no version change. #267 adds
`floorRequests` (episodes in which a refused write asked for reclamation),
`floorRequestedBytes` (bytes below the floor at the last pass, zero when none),
`floorPasses` (retirement passes run while the floor was the reason) and
`floorFreedBytes` (linked bytes the current or last episode freed), at ordinals 30 to 33;
CLI and MCP render them under `free_space_floor` as `reclaim_requests`, `requested_bytes`,
`passes` and `freed_bytes`. CLI and MCP render ages as `wait_age_ms`,
`episode_age_ms` and `phase_age_ms`, measured to `observedAt`. Counters are session-local
and the clocks of an episode or wait restored at open start at the first pass; the
stored wait marker is unchanged. #298 adds `diskBudget`, `maintenanceReserve`,
`archiveConfigured`, `archiveHeld` and `reserveHeadroom` at ordinals 34 to 38. CLI and MCP
render them under `pressure` as `disk_budget`, `maintenance_reserve`, `archive_configured`,
`archive_held` and `reserve_headroom`, with `linked_bytes` the physical bytes. `archive_held`
is true while expiry and eviction keep evidence for want of an archive, because the store
has no archive policy or the last pass stopped at the archive watermark; `last_pass.frontier`
names the frontier. `reserve_headroom` is the budget less the reserve less `linked_bytes` at
the last pass, a signed decimal string that is negative once the store is over that line.

### Archive-before-delete gate (schema 63)

An optional `[archive]` server table makes the store archive evidence before it
deletes it. The format, schedule and write protocol are in
[the archive format](archive-format.md). Phase P1 implements the store side:
the deletion gate, the watermark and the writer fence. The daemon's archive
worker (`sparrow-archive` thread) takes each prepared segment, writes its files
under `<dir>/<store_id>/` as `<from>-<through>.gts`, never anywhere else in `dir` except
the `STORE_ID` file below.
Each file is written as `.gts.partial`, synced, renamed into place and the directory
synced; it is then read back byte for byte, decoded with the PurRDF GTS reader (any
diagnostic fails it) and only then does the worker advance `archive:through`. Any
failure leaves the watermark where it was and is journaled with its cause; the next
pass writes the segment again. `dir` must already exist and be on its own mount: a device other than the root
filesystem's, and a filesystem type that is a network filesystem (NFS, SMB/CIFS, Ceph, AFS, 9P,
Lustre, NCP, Coda) or differs from the root's. Otherwise it is refused with the named error
"archive directory ... is not on its expected mount", which names the device or type found,
before anything is read or written, because an unmounted share leaves its mount point as an
ordinary local directory. `dir` must hold this store's `STORE_ID` file (this store's id and a
newline). The worker writes it on its first pass when `dir` is empty (no entry at all, apart
from a leftover `STORE_ID.partial`), durably: temp file, fsync, rename, directory fsync. It
accepts a matching file, never overwrites one, and refuses a directory that holds other
entries but no `STORE_ID`. A missing root, another store's id or a non-empty directory
without `STORE_ID` is a
failure, never a directory made on the local disk, and nothing in `dir` outside
`<store_id>/` and `STORE_ID` is written. A record that cannot be archived (oversized, or its RDF
projection failed at ingest) stops the watermark in front of it, is never deleted or
skipped, and is journaled on every pass with its id, reason, bytes and position; the
count held and the bytes waiting behind it are reported as unknown.

```toml
[archive]
dir = "/net/synology/archives/sparrow-archive"  # required, absolute, outside data_dir
start_fraction = 0.75                   # 0.5..0.95
max_segment_bytes = 67108864            # 1 MiB..256 MiB; a larger entry gets its own segment
max_segment_records = 10000             # at least 1
max_segment_quads = 3000000             # at least 1; a record with more quads gets its own segment
check_secs = 60                         # wait between worker passes when nothing is due
stall_warn_secs = 120                   # 0 < stall_warn_secs < io_timeout_secs
io_timeout_secs = 900
```

A watchdog thread watches the archive step in flight, because a hung NFS mount blocks
the archive thread inside the syscall. Once a step (a read of `STORE_ID`, a write, a
sync, a read-back) has run `stall_warn_secs`, the journal gets one whole line naming the
step, the path and the seconds elapsed (`sparrowd: archive stalled step=... path=...
elapsed_secs=...`) and a `tracing` warning, repeated every `stall_warn_secs` while it
lasts. A blocked call cannot be cancelled, so `io_timeout_secs` does not interrupt it. A
step that ran that long makes the pass fail with "an archive step ran past
io_timeout_secs" once it returns: the segment is not committed on that pass, its files
stay, and the next pass verifies them and commits. The worker starts no other step
meanwhile, and nothing is marked archived because of a timeout.

The archive worker writes `STORE_ID`, holding this store's id, into `dir` on its first pass when
`dir` is on its mount and empty. A directory that has content but no `STORE_ID` is refused, not
adopted; a file naming another store means `dir` points at that store's archive or the wrong
mount, and is never overwritten.

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
A store with no archive policy has archived nothing, so it deletes nothing:
expiry and pressure eviction hold at `ArchiveLag`, and disk use grows toward
`disk_budget` until an archive is configured and has committed segments. Each
expiry pass that retired or held evidence, and each eviction pass, writes one
structured journal line with its counts.

One gate guards every deletion. Expiry stops at the first due entry the watermark
does not cover, with frontier `ArchiveLag` and `ExpiryReport.archive_waiting` of 1,
because the watermark is a prefix of the receipt order and nothing after that
entry is covered either. An archive commit wakes expiry. Pressure eviction stops
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

Both archive commits unlock retirement of archived evidence, so they are
admitted at the maintenance reserve instead of the free-space floor, as the
pressure pass's own commits are. Below the floor and above the reserve they
succeed while ordinary writes are still refused with the floor label. Below the
reserve they fail with `STORAGE_PRESSURE` and the reserve text, and the
watermark does not move; the archive worker must hold and retry on the floor
resume edge.

Schema 63 adds the enum values `PressurePhase.awaitingArchive` and
`EvictionFrontier.archiveLag`. CLI and MCP status render them as
`awaiting_archive` and `archive_lag`. `Status.pressureStatus` reports `archiveConfigured` and `archiveHeld`
(`PressureStatus` ordinals 36 and 37), and `Status` reports `archiveHalvings`
(ordinal 63). `Status.archiveWorker` (ordinal 64) reports the worker; see
[Archive worker status](#archive-worker-status-365). Backlog age, the last
segment and a listing of held entries are not in the schema. The daemon journals a failed
pass and a held entry.

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
  listing also lists the rows the migration still owes, so it answers as it will
  after the migration (see "Per-source counters and artifact index"). The listing
  reads those rows from a snapshot without the writer lock, a window of 32,768
  rows after its position at a time: when it starts, and when a continuation
  passes the window's end. Each window is read by a listing build (see "Listing
  builds"). A page stops before any row after the window, so no owed row is
  skipped. A row the migration has copied since is listed once, and an artifact
  retired since is skipped, as the finished index omits it.

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

Sparrow has no configurable per-artifact limit. The server configuration no longer has `max_artifact_bytes`. A
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
uploaded. The refusal is a non-retryable `INVALID_ARGUMENT`: no retry can change
it. Its message states the bound in bytes and the remedy: split the input into
parts of at most that many bytes, import each part as its own request, then
import the manifest. The refusal is counted in `Status.importOverBoundRefusals`
(additive, in memory, since the daemon started), which names a caller that does
not split. A client learns the bound before it declares from
`Discovery.maxImportBytes` (additive field `@11`; an older daemon sends 0). No
schema version changes and no stored format changes; an older client reads the
refusal as an ordinary non-retryable domain error. An artifact of many small
records can still need more chunks or record identities than its size implies.
Layout preparation then refuses it with `RESOURCE_LIMIT` when its row would
exceed the retirement pass, and publication
checks again for layouts prepared earlier and refuses record rows that would
exceed the batch. An inline `ingest` is held to the same retirement check.
Format #5 replaces the whole-row read and the single batch, and with them this
bound.

### RDF staging refusals are counted and named (#340)

An intake stages its RDF in memory before commit: at most 32 MiB of new dictionary
terms and 64 MiB of anticipated writes per batch (the RDF index rows and manifest, plus
the batch's other pending writes, so earlier records in the batch count). A graph that
would pass either is refused with `RESOURCE_LIMIT` ("RDF dictionary staging exceeds 32
MiB", "RDF write staging exceeds 64 MiB"). The refusal is counted where the write
raises it, so every intake path that stages through a counted write is covered:
inline `ingest`, record batches and the import publication's artifact graph. A
refused projection graph that an ingest absorbs into a failed projection state is
counted too, though no client sees an error; if the batch then commits, the record is
stored without its graph, but a later graph of the same batch can still be refused and
drop the whole batch, and that refusal counts again. The counter therefore names
refused graphs, not refused intakes: one client-visible refusal can add several
counts. A client retry of a refused intake adds its own. Upgrades and migrations
stage uncounted.

`Status.stagingRefusals` (field 62, additive; a reader that does not know it sees no
change) carries `refusals` (every refused graph since the daemon started, in memory),
and the latest refusal: `lastLimit` (`write` or `dictionary`), `lastSource` (the
artifact's source), `lastGraph` (the record, artifact or projection identity) and
`lastBytes` (the bytes the write anticipated). All are empty or 0 before the first.
CLI and MCP status render them as `staging_refusals`; `Store::properties` carries
them as `staging_refusals` and `staging_refused_last_*`.

The daemon journals one stderr line per refusal, with no record bytes:

```
sparrowd: rdf staging refused limit=write source="app" graph="record:..." anticipated_bytes=70123456 refusals_total=3
```

Every counted refusal writes its line; none is thinned. The line is written without
panicking, because it is written while the store's writer lock is held, and a failed
stderr write is dropped (the counter still holds the fact). Whether to stage an artifact's RDF in pieces or store it with
its graph deferred is separate work.

### Archive worker status (#365)

`Status.archiveWorker` (field 64, additive; a reader that does not know it sees no change) is
an `ArchiveWorkerStatus`, a copy the worker publishes as each step ends. `status` reads the
copy and never waits on the worker or takes the store's writer lock, so a worker stuck on a
hung mount still reports where it stopped.

| Field | Ordinal | Meaning |
| --- | --- | --- |
| `phase` | 0 | `ArchivePhase`: `notConfigured` (no worker runs), `idle`, `preparing`, `writing` (root checks and file writes), `verifying` (read-back, decode and commit) or `holding` (the last pass failed and the worker waits to retry). |
| `through` | 1 | The durable `archive:through` watermark; empty before the first commit. The worker reads it, with `intentInFlight`, from the store when it starts and at the top of every pass, so status is right after a restart and on passes that fail before they choose a segment. |
| `intentInFlight` | 2 | A prepared segment is recorded and not yet committed. |
| `failure` | 3 | `OptionalArchiveFailure`: absent, or the last failed pass. |

`ArchiveFailure` carries `code` (ordinal 0: `ARCHIVE_VERIFY`, `ARCHIVE_IO`, `ARCHIVE_ROOT`,
`ARCHIVE_NOT_ON_MOUNT`, `ARCHIVE_OVERRAN`, `INVALID_ARGUMENT`, or the store's own code such as
`STORAGE_PRESSURE`), `segment` (1: the segment file name, empty when the failure came before a
segment was chosen), `reason` (2: the cause, cut to 512 characters; the journal line holds it
whole) and `since` (3: an `OptionalInstant`, when the failure began, kept while the same code and
segment keep failing; absent while the clock could not be read, and filled in on the first pass
that reads it). A pass that ends without error clears
the failure and returns the phase to `idle`. CLI and MCP status render the field as
`archive_worker` with `phase`, `through`, `intent_in_flight` and `failure` (`code`, `segment`,
`reason`, `since` as an instant or null, or the whole failure null).

### Instance role (#371)

A store records whether it is the live store or a scratch one in one metadata
row, `instance_role`, holding `production` or `scratch`. No record or segment
encoding changes. `Discovery.instanceRole` (field 16) and `Status.instanceRole`
(field 65) carry it as the enum `InstanceRole`: `unspecified` (0), `production`
(1) and `scratch` (2). Both fields are additive; a reader that does not know
them sees no change. A store with no row reports `unspecified` and behaves as
before. The Rust client decodes the discovery field as `Discovery.instance_role`,
and CLI and MCP status render the status field as `instance_role`. The ingest socket
admits `discover`, so a sender reads the role there: `sparrow SOCKET discover` prints
the connection's `store_id`, `schema_version` and `instance_role`. A sender that must
not reach the live store sends only when the role reads `scratch` and refuses
`production` and `unspecified`.

The server configuration key `instance_role` (`"production"` or `"scratch"`)
names the role. A store created while the key is set records the role in its
creation batch. At every open, a configured role that differs from the stored
role stops startup with a message naming both values, and a configured role
meeting a store with no row stops startup with a message saying so. No
operation changes a stored role. Stamping an existing store is a separate
operator step, the offline `sparrow-stamp` tool
([deployment](deployment.md#stamp-the-role)); the daemon never performs it. A daemon without the key accepts
any store.

### Archive prepare halvings and the daemon's library warnings (#324)

`Status.archiveHalvings` (field 63, additive; a reader that does not know it sees no
change) counts the times an archive prepare halved its range after an encode error since
the daemon started. Each halving counts, so one prepare can add several. It is in memory
and restarts at zero. CLI and MCP status render it as `archive_halvings`; `Store::properties`
carries it under the same name.

When a prepare returns fewer entries than it selected, the worker writes one stderr line:

```
sparrowd: archive prepare shortened selected=10000 kept=5000 first_code=RESULT_EXPIRED halvings_total=3
```

`first_code` is the code of the first halving of that prepare; a later halving may have had
another, and each halving is warned with its own code. The daemon installs a `tracing`
subscriber that writes warnings and errors from the storage library (`sparrow_core`) to
stderr, one event per line with no timestamp (the journal adds one) and with line breaks in
a field written as `\n`. The daemon's own warnings are not written by it; they have the
`sparrowd:` lines above.

### Part sets for consume roots (#280)

A consume-root file above what one artifact can hold is published as a part set
instead of being refused. Discovery advertises everything the cut needs, all
additive, with no schema version change: `maxImportBytes @11`, `rawRecordBytes @12`
(the longest record a layout keeps whole), and the joint retirement bound
`retireRowBudget @13`, `retireChunkCharge @14` and `retireRecordCharge @15`. A staged
artifact of `chunks` chunks (the sum over its records of the record length over
65536, rounded up) and `records` records is refused `RESOURCE_LIMIT` at layout
exactly when `retireChunkCharge * chunks + retireRecordCharge * records` exceeds
`retireRowBudget`. Each value comes from the store's own properties
(`max_raw_record_bytes`, `retire_row_budget`, `retire_chunk_charge`,
`retire_record_charge`), so what is advertised is what is enforced, test hooks
included. An older daemon sends 0 for each; the client then keeps the single path
and plans no split.

The cut lives in `sparrow_client::parts` (`next_cut`). A `jsonl` or `log` part
holds whole lines while the byte bound, the record limit and the joint bound all
hold, counting each line as the layout does, terminator included. A line too
large to fit alone is cut raw, and the pieces that begin or end inside it are
stored `opaque`. Every other format (`json`, `markdown`, `syslog`, `opaque`) is cut
into raw parts stored `opaque`, so nothing parses a fragment. A syslog part is at most
the 65536-byte syslog packet limit: the daemon stores one syslog artifact as one packet,
so a syslog file above that limit (and within the import bound) is cut at the limit, and a
syslog file at or under it keeps the single path. The packet limit is the daemon's own
constant, named by the consume worker; discovery does not advertise it. Part `k` of claim
`consume:{H}` is the ordinary staged import `consumeset:{H}.part{k:010}` under the
claim's source; its records are readable as soon as it publishes. The manifest,
ingested last and durably as `consumeset:{H}.manifest` in format `json`, is
`{"schema":"sparrow.parts/1","set","source","format","byte_length","parts":[{"index","start","length","format","artifact"}]}`
with the original format and each part's range, stored format and artifact id in
order. The manifest alone defines the set: a reader that needs the file reads the
listed artifacts in order and concatenates their bytes.

For a part-set claim, the journal's `published_artifact`, a handoff receipt's
`artifact_id` and `watch claims` all name the manifest. Ingestion status by
import, request or handoff reports the manifest as the accepted artifact with no
staging. Every other reader of the claim's import identity (begin, get, admit,
cancel, publish) refuses with `CONFLICT` naming the manifest. The manifest's
receipt is kept while the claim's active or completed journal exists, not only
for the retry window. A `consumeset:` request id is refused `INVALID_ARGUMENT` to
an ingest-socket caller, and a manifest receipt that carries an intake is refused
as proof.

`Client::import_open_file_with_progress` uploads as `Client::import_open_file`
does and reports each durable progress raise to a sink, so a caller that drops the
upload at a deadline still knows how far it got.

Limits of the part path:

- On a daemon without the retirement numbers, a dense file within the byte bound
  is uploaded whole before layout refuses it; the worker names the missing values
  in the attempt text. A file above a known `maxImportBytes` whose other values are
  missing is held, named and never uploaded.
- A part whose staging lease lapsed answers `IMPORT_EXPIRED`; it is never
  permanent. Once reclamation deletes its row, the part begins again from byte zero
  under the same request id and publishes under a new artifact id; the manifest
  lists the ids finally published. Every attempt reads the daemon's recorded parts
  first (`ingestPartReceipt`, by request id) and never trusts a cached answer: a part
  the daemon holds is listed without a read, a hash or a begin, and only the parts after
  them are cut, under the limits as they stand now. A part's receipt is kept while its
  claim's journal exists, so a part published before a restart, even after a stall of
  more than two days, returns its saved receipt for as long as its artifact is stored,
  and its records are not stored twice.
- While the claim is pending, a part whose artifact is evicted or deleted is sent again
  from the claim's source under the same request id, and its bytes come back under a
  new publication time. It is sent again only once no record of the old artifact is
  still stored: while eviction has retired some of its records and not the rest, the
  claim is pending and nothing is sent, so no record is read twice. Time expiry still
  runs from each part's own publication, so the earliest part can expire or be
  evicted before the manifest; a part past its time but not yet swept is listed, not
  sent again. The artifact of a claim's single path, and of a manifest accepted and not
  yet recorded, is held against eviction until the source is removed; set parts are not
  held. Removal checks the manifest proof, not that each listed part is still stored.
- A part whose recorded span the limits as they stand no longer admit cannot be cut
  again, because later parts start where it ends; the claim stays pending until limits
  that admit that span are restored. A draft at the frontier that the limits no longer
  admit, or whose bytes are not the file's, is cancelled and waited for, then cut again
  at its index. A daemon without `ingestPartReceipt` leaves every unrecorded claim pending
  with a named text until it has the method.
- The choice between the single path and a split is not durable for a claim with
  no recorded state; a claim whose manifest is accepted or recorded never plans
  again, and a recorded single-path claim returns its saved receipt.
- A part holds one of the 16 import slots until reclaimed, so a fast set can meet
  "journals occupied", which the worker retries.
- A claim that began single-path staging and is later recorded as a set has that
  staging row released by the next reclaim pass.
- Retiring an unrecorded set claim whose source is gone leaves its parts as
  ordinary artifacts with no manifest; after the two-day receipt window their
  order is lost. The retirement reason should name the parts left behind.
- An operator who raises `max_records` far enough can meet the 128 MiB publish
  batch refusal after a whole part was uploaded; it is retryable, so the worker
  repeats it.

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
message is refused with `INVALID_ARGUMENT`, which is not retryable, and the message names the staged
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
1 MiB to 256 MiB, and an entry larger than the target is archived in a segment
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
disposable copy, never the only one. An open whose source-context cache is
stale resets it with one range deletion in the fence batch (see "Single
context capture admission"); an open of a current store leaves it alone. Opening also applies any catalog upgrade that
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
and its final flush has succeeded. A later shutdown error can still follow it. Query captures use anonymous temporary files that leave no name
behind.

Schema 67 adds `Status.queryOnly`, true in this mode, and
`Status.retentionSeconds`, how long an unpinned record is retained after it is
received (`sparrow_core::RETENTION_MS`, 30 days). CLI and MCP status render them
as `query_only` and `retention_seconds`, the latter as a decimal string.

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
`query_only=false intake=false`.

`sparrowd --config FILE --private-intake DIR` opens a disposable copy of a
store writable with its maintenance worker and its watches. Every stored watch
definition, and every definition `watchPut` accepts, is rebound to `DIR/NAME`,
where `NAME` is the final component of its root, before any root opens; a root
without a final component refuses the open, or the put with
`INVALID_ARGUMENT`. So does a configured root that is `DIR`, lies under it or
holds it: rebinding would then reach that root itself, or put the private
roots inside a root another daemon consumes. Paths are compared canonically,
and a configured root present on the host is also compared by device and
inode with `DIR`, its ancestors and `DIR/NAME`, so a bind mount or other alias
of the root is refused too. Two configured roots with one final component
would share a private root, so they refuse the open, or the put with
`INVALID_ARGUMENT`, naming both. Never point `DIR` at a live
intake directory or its parent; the refusal covers the configured roots
only, so use a fresh scratch directory. The rebinding is held in memory: a
later watch mutation persists the configured roots, never the private ones,
so the catalog keeps naming where its watches consume. A consume or
tail claim bound to another root identity, such as the original root's, does
not refuse the open in this mode and is never recovered: each recovery refuses
a claim of another root, which becomes the watch's failure. An active tail
generation of another root is never resumed, adopted or given a gap; a file
under the private root that reuses its file identity is refused. "Another root" is
decided by the root inode alone, as everywhere a stored identity is compared: if
`DIR/NAME` happens to have the original root's inode, the original's claims and
generations are treated as the private root's own, within the disposable copy. The configuration
may not set `consume`, `syslog`, `rpc_tcp_address` or `archive`, and the
refusal names the key; `ingest_socket_path` is allowed. The mode cannot be
combined with query-only mode. The startup line reports
`query_only=false intake=private`.

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

Receipt-time fallback triples are derived at read time and are not stored (#193).
A reader serves `eventTimeFallback` (the record's `received_at`) and
`eventTimeBasis` from each selected record metadata graph that has no source
`eventTime` and whose stored rows lack `eventTimeFallback`, so a graph written
without them answers exactly as one that stores them. Ingest writes none. The
online backfill that once rewrote every retained record graph to store them
(`event-fallback-rdf-upgrade`, #20) is removed: it cost about 190 key writes per
graph and the compaction debt behind them, and the derivation it duplicated is
the one reading path. Graphs that already carry the facts, from the backfill or
an earlier writer, keep them and are served as stored; nothing is rewritten or
deleted. The store format keeps its fallback writer family, `sparrow.store/132`
to `/186`, which lifts each tracing, log time, archive and intake family by 55
and keeps its switches, so binaries older than schema 69 still refuse the store
rather than publish graphs without the facts. The switch is one-way: the schema 71
image before #193 opens the store, does not derive the facts for graphs written
without them, and so serves such graphs without the receipt-time facts. Opening a store that
carries the backfill's checkpoint, finished or pending, applies no migration
and rewrites nothing; the checkpoint and its counter row stay as inert keys.

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

Opening a store that owes the schema 69 migrations applies the writer fences
and seeds the checkpoints in one synchronized batch, then serves; the open does
not wait for any job. The joined maintenance worker runs them as one duty,
`store-counters` first (see "Store counters"), then `source-summary` (see
"Per-source counters and artifact index"), `payload-index` (see "Payload id index"), `owner-graph-index`
and `source-context` (see "Single context capture
admission"), one page per admission. `source-summary` runs second because, until
it completes, every `sources` and live `artifacts` listing first reads what the
migration still owes (#176). The migrations are independent: each reads and
checkpoints its own keys, so a store left part way through by an image that ran
them in another order resumes each one from its own checkpoint. `status` lists
them in a fixed order apart from the run order: `store-counters`,
`owner-graph-index`, `source-summary`, `source-context`, `payload-index`.
The migration duty is selected fairly with buffered synchronization, pressure
maintenance, import reclamation, retention and the table rewrite. Below the
free-space floor it is skipped, as import reclamation and retention are; buffered
synchronization, the table rewrite and pressure maintenance still run there.
Each page commits its rows and its checkpoint in one synchronized batch through
the ordinary write path, so it is admitted against the data allowance and a
failed write latches durability uncertainty as any write does; a restart resumes
after the last committed page. While pages succeed the next one follows at once,
except that writers come first: an ingest, a consume publication or an RPC write
that is waiting for the writer lock takes it before the next page does. Before a
page the worker waits while any writer is waiting, for at most the longer of 100
ms and the time the previous page held the writer, so the migration still
progresses under a continuous stream of writers. The wait blocks on a condition
the last waiting writer signals; nothing polls. It runs apart from the worker's
other duties, which keep their own schedule, so it delays only the page. When
that wait ended while another duty ran, or the page is the first after a retry,
writers may have arrived since, so the worker waits again immediately before the
page, inline. A failed page commits nothing, sets that migration's `failure` and
is retried every 60 seconds until it succeeds; a page skipped below the free-space floor
is not a failure and runs when the floor resumes (#247). Nothing is skipped. The daemon
logs `sparrowd: migration NAME start pages=1 done=D total=T` at a migration's
first committed page of the session,
`sparrowd: migration NAME progress pages=P done=D total=T watermark=W elapsed_seconds=S`
every 64 pages, and
`sparrowd: migration NAME complete pages=P done=D elapsed_seconds=S` when a
migration finishes. Each line reports the counters the page's own batch updated
and reads nothing else; `total` is `counting` while `store-counters` seeds.
`pages` counts this session's committed pages of that migration, and
`elapsed_seconds` runs from the start of the job that committed its first page
of the session, so it includes the wait for writers before every later page. A
wait the worker ran beside its other duties before that first page, at most the
longer of 100 ms and the previous page's hold, is not counted. So the `complete`
line of a migration that started in this session gives its whole duration, its
page count and its final `done`; after a restart part way through, the lines
cover only the remainder. The `start` line's `done` already includes that first
page.

Schema 70 adds two flags. `Status.countsPending` (CLI and MCP `counts`,
`pending` or `exact`) is true while the `store-counters` migration seeds;
`MigrationStatus.totalPending` is true for every migration then. While pending,
`live_records`, `unique_payload_chunks` and `unique_json_projections` count
exactly the keys through the `store-counters` watermark, and each migration's
`done` counts its entries through both its own and the seeding watermark; none
of them is a total. A pending `total` is 0 on the wire and `counting` in CLI and
MCP. The `store-counters` migration's `done` and `watermark` report the seeding's
progress. Status answers from the seeding checkpoint and never counts the
unseeded keys, so its cost is the same at any store size (#158). A query-only
daemon never runs a page, so its counts stay pending until a writable daemon
seeds them.

`Status.migrations` (CLI and MCP `migrations`) lists every migration with
`name`, `done` and `total` (decimal strings), `watermark` and `failure`. `total`
counts the migration's source entries now in the store: every counted key for
`store-counters`, graph declarations for `owner-graph-index`, record metadata
`artifact:` and `record:` entries for `source-summary`, `record:` entries for
`source-context`. `done` counts those at
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
| Scoped SPARQL (selection, pin, or a source, time or diagnostic filter) | The owner-to-graph index for the declarations the migration has copied, plus the declarations past its frontier, filtered to the scope's owners; the same rows. The uncopied declarations are walked without charging the request budget, checking cancellation every 4,096 rows, and each kept graph is charged to the view's reservation | The owner-to-graph index, at the cost of the scope |
| Whole-live-store SPARQL | The whole-store plan | The whole-store plan |
| Record metadata graphs in a SPARQL scope | A selected record without a source event time whose graph lacks `eventTimeFallback` is served the `eventTimeFallback` and `eventTimeBasis` facts derived from the record's receipt time, and is counted in `SparqlCoverage.eventFallbackPending` (CLI and MCP `coverage.event_fallback_pending`). Strict reads answer the same way | The same: derivation is permanent, so the count is the number of selected receipt-time graphs without stored facts, not pending work, and it does not fall to zero |
| New graphs written during the migration | Owner-to-graph rows; no fallback facts | The same |
| Catalog event time (intervals, sorting, correlation clocks, `event_time_fallback`) | Derived from each record's `received_at`; independent of the RDF rewrite | The same |
| Record context (`recordsContext`, CLI `context`, MCP `records_context`), and a live `artifacts` listing row that reads its parent records through the cache | Walks the primary `time:` index and keeps the source's records, never the partial cache; the same neighbors, visits and frontier. Context reads from a snapshot without the writer lock and checks cancellation every 4,096 entries; skipped entries of other sources are not charged | Walks the source timeline cache |

Each candidate record (a selected record without a source event time) costs an
extra manifest point read and a subject lookup, and each derived fact its term
lookups. None of these reads is charged to the request budget, so a metadata
scope near its budget answers as it would with stored facts; the derived facts
and terms are charged to the view's reservation as they are held.

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
| `payload-index` | `payload-id:` index keys, one per complete artifact (see "Payload id index") |
| `done:owner-graph-index`, `done:source-summary`, `done:source-context`, `done:payload-index` | The migration's source entries at or before its checkpoint |

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
`physical_bytes` measures the database directory, except the files RocksDB
no longer references (obsolete table and blob files and write-ahead logs below
the oldest one recovery needs), which RocksDB holds until a reader releases them. Flush and compaction
output RocksDB does not reference yet is not counted either, so a large
compaction can briefly occupy more than `disk_budget` on disk; the free-space
floor still guards the filesystem. A status read
therefore reads a fixed number of keys plus one per pin row, whatever the size
of the store, seeding or not. The read handles each
listing registry holds are counted from a ledger kept beside the registry and
locked only to copy its rows, never while a page reads, so a listing that waits
for the writer while holding its registry does not delay status. The durability, pressure and table rewrite state
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
the `source-summary` online migration (`source-summary-upgrade`), and the
service answers at once. Rows found under an older writer identity are not
trusted: that batch discards them and the migration recounts every source.

The migration runs in the maintenance duty directly after `store-counters`,
before `owner-graph-index`, with the same admission,
failure, retry and journal rules (see "Online store migrations"). It has two
phases. The first walks `artifact:` entries and counts each artifact from its
entry and its `record_ids`, up to 512 artifacts or 16,384 records per page,
always counting one artifact whole. The second walks `record:` entries and
counts the retained records of parents whose entry was already deleted. Each
page commits its rows and its checkpoint in one synchronized batch, so a restart
resumes after the last committed page. Writes during the migration keep exact
every artifact the migration has passed or that the write creates; the migration
reads the current state of the rest when it reaches them. `total` counts
`artifact:` and `record:` entries, and `done` those at or before the watermark.

Readers while the migration is pending:

| Reader | Pending | Complete |
| --- | --- | --- |
| `sources` (`sourcesList`, CLI `sources`, MCP `sources_list`) | A listing build adds the counts the migration still owes to the counter rows of the same snapshot | Reads the counter rows |
| Live `artifacts` listing | Merges a window of the index rows the migration owes, each read by a listing build, with the stored rows, page by page | Walks the per-source index |
| Explicit artifact selections, record reads, SPARQL, search | Served; they do not read these rows | The same |

What the migration owes is computed by running its own counting passes to the
end over the reader's view without writing, so a listing during the migration
equals the listing after it. The pass reads each catalog entry the migration
has not reached once, so its cost shrinks as the migration advances. It runs
from a snapshot without the writer lock, as a listing build: once per `sources`
capture, and for a live `artifacts` listing once when it starts and again for
each further window of 32,768 owed rows its pages pass. CPU is not capped. What
it holds is charged when a page holds it: the owed count changes to the `sources`
capture, and the owed window, at 64 bytes plus the key per row, to each page of
the listing; a window always fits that admission. While a build runs, before its
page exists, its memory is not yet charged to a request. An artifact window is bounded
by its window; a `sources` build holds its count changes for the whole owed pass, and
the capture admission is checked when the pass ends. No
reader refuses because a migration is pending.

#### Listing builds

On the live store the pass costs the unmigrated part of the store, which can
take longer than a caller's deadline (30 seconds for an MCP tool call, and the
60-second connection lease for every RPC connection). So the first call
registers the capture's build and starts it at once on its own thread, which
takes and holds its own snapshot; the writer lock is held only while a `sources`
build reads its registries and takes that snapshot. Each call then waits for the
build for at most 5 seconds. If the build has finished, the call returns its
first page as usual. Otherwise it returns a page with no rows whose
`next_cursor` names the build, and whose `CatalogReadReceipt.build` (CLI and
MCP `build`) reports `read`, the catalog entries the build has read, and
`total`, the entries the migration still owed at its snapshot (`totalPending`,
`counting` in CLI and MCP, while the store counters seed). Its `snapshot_id` is
the build's. Following that cursor waits on the same build again and never
starts another; a capture's lease starts when its first page is delivered. A
build's lease runs 60 seconds from the last call that followed its cursor, and
`resultRelease` with its `snapshot_id` cancels it. An abandoned build is
dropped when its lease ends, at the latest at the next read-lease purge, which
cancels its thread and drops its snapshot. A finished build stays until the page
built from it has been delivered, so a call that fails or is cancelled while its
page waits for the writer is retried on the same cursor. A page with `build` has
no rows and is not a result: `total_items` of a `sources` build page is 0 and does
not mean an empty catalog, so a client follows `next_cursor`. At most eight builds
of each listing run at a time, counting a live `artifacts` window continuation; a
further fresh listing or window refuses with `RESOURCE_LIMIT`. Once the migration
is complete no listing builds. A build's thread keeps its own handle on the store,
so closing the store does not cancel it until its lease ends (at most 60 seconds);
the process exit ends it at once.

The migration's own pages keep reading the same entries while a build runs. So
each time the build has read another 4,096 catalog entries (an artifact's
records count, though `read` counts the artifact once), when it checks its
cancellation and lease, it also reads the migration's checkpoint and `done`
counter (a few point reads). It re-bases when the migration has overtaken it:
the migration is complete, or its checkpoint has moved and it has passed more
entries since the build's snapshot than the build has read plus 4,096. A re-base
drops the build's snapshot and the part of the pass it has read, takes a fresh
snapshot (a `sources` build reads its registries again under the writer lock)
and reads only what the migration owes there. Its `read` and `total` then count
from the fresh snapshot, so `read` can go down. The build keeps its identity,
cursor and lease, and its rows are those of the snapshot it ends on, so a source
or artifact written before the re-base is included. A build started while the
migration runs therefore delivers its rows no later than its next check after
the migration completes, and a redundant pass stops competing with the
migration's pages for CPU.

The migration's duration on the live store is recorded in
[load qualification](load-qualification.md).

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
An artifact is indexed exactly while the store holds all of its bytes: its entry
exists, the entry is not marked evicted, and its manifest retirement state has
neither evicted it nor retired any chunk. This is stricter than the per-source
counters' completeness, which looks only at the evicted flags: expiry retires an
artifact's chunks at its deadline and keeps the entry a day longer, and the index
already does not list it in that window. One owner observes every write batch before
it is written and adds the key changes to the same batch, so no writer can
create, retire or delete an artifact entry without its key, and a crash cannot
leave a catalog row with no key or a key with no row. The `payload-index`
store counter (`store-counter:payload-index`) counts the keys through the same
observer as every other counter, and
`done:payload-index` counts the artifact entries the migration has passed.
The count of keys equals the count of complete artifacts, so a count that
disagrees with the catalog is detectable.

Every entry is removed in the batch that ends its completeness:

- expiry retires an artifact's chunks at its deadline (the manifest retirement
  state records the retired chunks), and the key goes in that batch; the entry is
  deleted a day later, or with its time-index key for a recordless artifact;
- pressure eviction marks an artifact evicted or retires its chunks through its
  manifest retirement state, or deletes a recordless artifact entry;
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
| Retention | Global deadline 30 days after acceptance for an unpinned item, retired once archived; earlier eviction of archived, unpinned items under pressure; a pin or an unarchived item keeps it longer. |
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
report their count. Strict index completeness does not promise every clock is known;
a source time that depends on a failed JSON projection is incomplete evidence, so
strict event-time reads and strict event-clock correlation refuse it with
`EVIDENCE_INCOMPLETE`.

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
1024-source registry ceiling applies to both selectors. A declaration past it is
refused with the non-retryable `INVALID_ARGUMENT`, because no mapping is ever removed and
a retry could never succeed; a log never is (see "Source time mapping on the event
route"). Returned profiles expose an `extraction` descriptor; the legacy `path` is empty
for a log capture.

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
with `EVIDENCE_INCOMPLETE`, without successful rows; JSON discovery also refuses
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

The source timeline (`source-context-v1:`) is a disk-backed derivative of the
authoritative record and primary time catalogs. Production record and time
mutations maintain it in their original atomic batch, so a crash cannot leave
it stale. Its generation marker, `source-context-v1:!generation`, sorts before
every entry. An open keeps the cache when the marker holds the current
generation, no reconstruction failure is latched and the job checkpoint
decodes. Otherwise the open resets it in the migration fence batch: one range
deletion of the whole family, the marker, a checkpoint at `start` and a zero
`done:source-context` row. It writes no point tombstone per entry. Images
before #149 rebuilt the cache on every open by deleting its entries in key
order; their first deletion batch removes the marker, so a cache such an image
touched, or left half cleared, is reset at the next open. The `source-context`
online migration then rebuilds the cache with the shared bounded catalog pager,
one page per maintenance admission, while the daemon serves. Until it
completes, context reads walk the primary `time:` index instead and return the
neighbors the rebuilt cache will give. Context captures its scope, protection
and membership under the writer lock, then releases it and reads the anchor and
both directions from a snapshot taken with it, so a walk that skips many other
sources' entries holds back no write; it checks cancellation every 4,096
entries. Receipt bounds stop each direction before its visit budget, so a
direction that leaves the receipt interval reports the scope boundary. A query-only daemon runs no page, so on a
stale cache its context reads keep using the time index until a writable daemon
completes the rebuild. Selected entries must agree with the primary time index and the
record's source; disagreement refuses the capture without a global scan fallback.
Corrupt record metadata found during reconstruction latches a durable failure:
the migration stops paging, `Status.migrations` reports the failure, and
context keeps walking the primary `time:` index until catalog repair and a
reopen, which resets the cache and rebuilds it. A context read that reaches the
corrupt record itself refuses with a storage failure. The failure does not
publish a partial timeline or prevent unrelated evidence reads. Other page failures, such
as storage I/O or admission, are retried like any migration page. The daemon
prints the wall time of each open phase once at startup as
`sparrowd: store_open_ms rocksdb=N identity=N upgrades=N migration_fence=N archive=N wal_sync=N state=N registries=N total=N`
(milliseconds).

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
`EVIDENCE_INCOMPLETE`. In permissive mode, available relationships retain their native
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
| `payload_lookup` | Store ID, payload id, optional cursor and page size | The held artifacts of one content digest, from the payload id index; exact once the `payload-index` migration completes, pending before. |
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
strict reads refuse `EVIDENCE_INCOMPLETE`. Metadata layer exclusion removes its graph.

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
listed in [the archive format](archive-format.md#9-no-manifest). Archive segments
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
path, source, line format and opened inode (the device number is recorded but never compared).
The observer owns file-descriptor validation; these calls do not probe, rename
or delete producer files. Reusing a declaration preserves its generation and
current checkpoint; changing it conflicts. The stored registry holds at most 64 generations
and 512 KiB of encoded checkpoint metadata; every shipped reader refuses a longer
value, so raising either bound is a stored-format change that rides the next one
(tracked in #230, with the persistence of the counts below).
Active entries consume this quota; terminal summaries expire 30 days after
completion/discontinuity.

A full registry never turns a file away (#224). `tail_start` first drops expired
rows, then retires the one oldest finished row that nothing can still need,
counted, and stores the new row. Only a complete row is retirable, and only when
it finished at least two days ago (the retry window) and no active consumption
journal binds it. Never retired are an active row, a row with a finish that is
not complete, a complete row inside that window or bound by a journal, and a
discontinued row, which is the only durable record of a truncation, identity
change or read gap. A retired complete row answers NOT_FOUND to a file-generation
byte read (its records stay readable). If no row is retirable the
file is tailed under a generation held only in memory: it is read, appended,
finished, discontinued and byte-read like a stored row and its records are stored normally,
but it is never written to the registry and is lost with the process. After a
restart the tailer finds no row for the file and reads it again from offset 0
under a new generation, so the records already stored are stored again. That
duplicate risk is counted, and it is realised only after a restart, when the counts
are gone too. Retiring a complete row carries the same risk if its
file is still present, and is counted too. Status shows both under
`tail_registry_evidence`: the `max_named` (16) files with the most bytes at risk,
by `watch`, `path` (non-UTF-8 bytes shown as U+FFFD), `source`, `generations`,
`records` and `bytes`; the rest in `other_files`, `other_generations`,
`other_records` and `other_bytes` (`other_files_exact` is false when more than
1024 files are tracked); `total_generations`, `total_records` and `total_bytes`
over every file; and `retired_terminal` with the `retired_duplicate_risk` share
that were complete generations. The counts live in memory for the daemon's life.
Live in-memory generations are bounded by the per-watch descriptor quota
(`max_candidates <= 64`, an existing limit this change leaves as it is: a 65th
simultaneous file in one watch waits unconsumed on disk); terminal ones expire after 30 days and are evicted oldest
first above 64, counted in `retired_terminal`. Source listings read the stored
registry and do not list in-memory generations.

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
producer's final inode, byte length and nanosecond modification/change
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
revision, root binding and held inode. It checks the final descriptor
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
tracked inode/checkpoint and exclusively saves the synchronized intent
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
drains one descriptor per worker pass in round-robin order. One capture reads
at most 1 MiB; a line exceeding that window produces a visible failure and
preserves the source/checkpoint. Complete
prefixes use the one shared durable record pipeline. Idle and close events keep
a final unterminated line pending. Observed truncation preserves the old offset
and starts a new opaque generation. Same-length observed modification produces
a declared read gap. This is an append-only producer contract; these observations
do not certify detection of every rewrite or overwrite followed by growth.

Restart attaches surviving in-root identities to original checkpoints. A file, a
root and a parent directory are identified by their inode alone. The device number
is stored as written but is never compared with a stored value, because it changes
across a reboot on btrfs subvolumes, network, overlay and re-enumerated mounts; a
descriptor held while the daemon runs already fixes the object. A stored active
generation whose root inode, watch revision, source and format equal the current
ones resumes whatever its stored device numbers say, and the file with the declared
inode is attached to its checkpoint. A pending finished-file claim likewise stays
bound to its stored root and parent inodes. A file smaller than its checkpoint still
takes the `truncated` discontinuity, an mtime change at equal size within a run still
takes `read_gap`, and a changed root inode still refuses the start. The known
weakness is inode reuse after a delete and create: a new file given the old inode
and larger than the old offset is read on from the checkpoint; the device number
never detected that either, and closing it needs a content digest in the stored
checkpoint, which is a stored-format change. After a
complete reconciliation, unavailable active identities receive durable read gaps;
no missing prefix is invented. Status exposes finite `tail_generation_ids`,
`tail_read_bytes`, `tail_descriptors`, `tail_admission_waiting` and
`tail_checkpoint_records`; the latter
counts accepted occurrences across currently known checkpoints rather than live
retention membership. Kernel coverage and producer continuity remain separate.
A watch's status also carries `publish_hold`, `held_claims` and
`oldest_held_first_seen` (additive, ordinals 20 to 22): the hold flag, how many
pending finished-file claims it holds, and the reservation time of the oldest of
them, which is not when the hold was set. See "Publish hold" in the deployment
guide.
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
The Unix socket serves the complete capability unchanged.

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
reports the path, group, ceiling, active connections and refused connections,
which now counts only a peer whose credentials the kernel cannot report; a peer
over the ceiling waits (see Admission waits).

### Admission waits (#246)

Three limits that used to refuse at once now wait, and every wait is counted.

- Operation permits. `max_concurrent_requests` is the whole budget. A writer (the
  intake path: ingest, events, imports, flush, discovery, pins and handoffs, and the
  daemon's own consume and syslog workers) that finds none free waits in arrival
  order, as Tokio's fair semaphore queue serves, for 3 seconds, which is inside the
  client's default `rpc_timeout` of 5 seconds, then receives the same retryable
  `RESOURCE_LIMIT`. A reader (queries, status, gets) takes a permit only if one is
  free and is first limited to the budget minus the writer reserve, a quarter of it
  rounded up and never so much that no reader can run, so a query storm cannot take
  the last permits. A reader never waits, because it would queue ahead of a later
  writer; beyond its share it is refused at once and counted. `status` is the exception
  to the permit rule: it takes no operation permit (as many status reads run at once
  as readers may, one more is refused and counted), so a budget saturated by writers
  still answers the counters that explain it. It answers `SHUTTING_DOWN` once shutdown
  begins; a read already running is not drained, because it reads a snapshot and
  writes nothing. The UDP syslog
  listener keeps its instant try, which it counts as a refusal, because it must keep
  reading its socket: a wait there lets the kernel drop datagrams uncounted. The
  MCP four-call limit is a per-session cap of that adapter, not this budget, and is
  unchanged. A waiting writer ends its wait on a permit released by finished storage
  work, on shutdown beginning, or on the budget closing (`SHUTTING_DOWN`, retryable);
  the only timer is the 3-second bound, which exists because the client stops
  reading after its own deadline.
- Import journal slots. A begin-import that finds all 16 occupied waits for a slot to
  free, under one bound of 3 seconds that covers the whole call, including any wait for
  an operation permit (which is capped at the time left); no attempt starts once the
  bound has passed. A slot frees when a reclamation commit deletes a journal (a
  published, cancelled or expired import). An explicit cancel, a publication and the
  retirement of a consumption claim whose import had begun each wake the maintenance
  reclamation duty at once, which performs the deletion. A pass bounds its work (128 chunks, or one
  private prepared record or raw step of an unpublished import), so a pass that freed
  something and rewrote a journal it did not finish wakes the duty again for the next, and
  such an import frees over a run of passes with no interval between them. The free-space floor's resume
  edge wakes a pass the pause skipped. A lease that merely expires has no
  event, so the duty's existing one-second interval is the named observer of lease expiry
  and nothing else. Each wake also gives held syslog frames one extra evaluation, which is
  harmless. The deletion commit signals the waiters; a pass
  that deletes nothing signals nobody, and nothing retries on a clock. The wait also
  ends at shutdown, or at the bound, and then answers the same `RESOURCE_LIMIT`. It
  holds no permit while it waits.
- Connection ceilings. The daemon takes a connection slot before it accepts, for the
  private socket and public TCP (one shared ceiling) and for the ingest socket. A
  peer over the ceiling waits in the kernel listen backlog and is served when a slot
  frees; a slot frees when a connection ends, which includes its 60-second lease
  expiring. The lease is unchanged, so a stalled peer holds its slot until then.
  Accepting still stops at once on shutdown. The free-space floor no longer pauses
  accepting (#257, see "The free-space pause"); the ceiling and the lease bound the
  connections below it exactly as above it. A syslog TCP listener already holds its
  arrival and reclaims the longest-idle connection, and is unchanged. The kernel does not report how many
  peers wait in a backlog or overflow it, so a ceiling wait counts the times the
  accept loop had to wait for a slot, not peers.

`status.admission` (added after `sourceTimeLoss`, no schema bump) reports
`writerReserve`, `readerSlots`, `writerWaitLimitMs`, `writerPermitWait`,
`readerRefusals`, `importSlotWait`, `importSlotWaitLimitMs` and `rpcConnections`
(the shared ceiling: `maxConnections`, `activeConnections`, `ceilingWait`,
`leaseExpiries`). Each wait has `waits` (acquisitions that had to wait), `waiting`
(now), `longestWaitMs` (finished or running) and `timeouts`. `status.ingestSocket`
gains `ceilingWait` and `leaseExpiries`. The CLI and MCP status show them as
`admission` and `ingest_socket`. The daemon journals a line when a wait episode
begins, at most one a minute per kind, and a line when an announced episode (or any
episode of a minute or more) ends, with the waits, timeouts and longest wait.
Counters are process-lifetime evidence and reset on restart.

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
and complete-frame timeout (1..60000 ms). A UDP entry may also set `hold_bytes`,
the bound of its queue (1048576..536870912, default 67108864); it is refused on a
TCP entry. The `hold_bytes` of all UDP entries together may not exceed 536870912, which is
also the most one entry can have.
TCP additionally requires a framing
law: `octet_counting`, `lf`, or `cr_lf`. The framing trailer is excluded from
the original packet; LF framing preserves a preceding CR. No framing guesses
are made. At most four listeners exist.

A listener keeps every byte it can and counts every byte it cannot, using the
existing discard reasons. No oversize, bad-start or idle case closes a TCP
stream or discards a frame:

- UDP reads a buffer of the store's 65536-byte syslog ceiling, so each datagram
  is kept whole. `max_message_bytes` frames TCP lines only.
- Octet counting reads exactly the announced length (at most five digits). A
  frame above `max_message_bytes` is kept whole up to 65536 bytes. Above that,
  the first 65536 bytes are kept, the rest is dropped as it arrives and one
  `truncated` is counted. The stream stays open.
- An LF or CRLF line above `max_message_bytes` keeps its first
  `max_message_bytes` bytes, drops the rest up to the newline without holding
  it, and counts one `truncated`. The stream stays open.
- A broken frame is published capped at the store's 65536-byte ceiling, header
  included, with the cut counted `truncated`.
- A bad octet-count start publishes the bytes read, with the rest of that line,
  as a malformed packet, counts one `framing_failure`, and resumes after the
  newline. A frame cut by end of stream publishes the bytes read as a malformed
  packet and counts `framing_failure`. A frame that stalls past the timeout
  publishes the bytes read, counts `timed_out`, and closes the stream. The
  timeout runs from a frame's first byte, so a sender idle between frames is
  not a fault. Bytes dropped from such a frame also count `truncated`.
- When a new connection arrives at the connection ceiling, the connection idle
  longest at a frame boundary is closed for it, and nothing the daemon has read
  is lost. The arrival waits for that slot and is not refused, so
  `rejected_connections` stays zero. A sender that has not yet noticed the close
  and writes again has that write reset by the daemon, and no counter sees it.
  On daemon shutdown a frame in flight is published as a malformed packet and
  counted `framing_failure`; bytes still unread in the socket cannot be counted.
- When the store answers a TCP frame with `STORAGE_PRESSURE`, the frame is held,
  not discarded, and the connection stays open (#243). The listener offers the
  same bytes again and reads no further frame, so the sender's TCP window closes and
  the sender holds the rest. Pressure can last as long as reclamation takes. A held
  frame blocks on a wake, the stop and nothing else (#247): it owns no timer and
  never retries on a clock. A frame waiting for an operation permit blocks on the
  permit and on the observer tick below, which only lets the journal announce a wait that
  outlasts its report interval; the wait keeps its place in the queue. Every ending the
  daemon can see sends the wake, and one observer covers the two it cannot:

  | Ending | Signal | Observer |
  | --- | --- | --- |
  | A pressure pass ends, or fails | wake from the maintenance worker | event |
  | A pin release or commit, an archive commit, a forced compaction ending | wake from the store's retention callback | event |
  | The free-space floor resumes | wake sent by the floor itself in the observation that sees the edge, whichever loop that is | event |
  | A refused write requests a pressure pass | wakes the pass at once; the pass's end wakes the frame | event |
  | Free space returns because another process freed it | none exists | the maintenance worker's one-second floor observation wakes every held frame while any is held |
  | Native flush or compaction work completes | none exists; the store has no listener | the same one-second observation |
  | Stop | the stop channel | event |
  | An operation permit frees | the semaphore | event |
  | An explicit import cancel, a publication, or the retirement of a claim with a begun import, makes a journal reclaimable | wake from the store to the reclamation duty, which deletes the journal and signals the import slot | event |
  | A reclamation pass freed something but left a journal unfinished | the store wakes the duty again for the rest | event |
  | The floor resumes after a reclaim pass was skipped below it | the resume edge wakes the reclamation duty | event |
  | An import lease expires | none exists (time passing is the event) | the reclamation duty's one-second interval, for lease expiry only |
  | A hold's report interval passes (announcing a long hold) | none exists | the same one-second observation, for a permit wait; a pressure hold announces on each wake |

  The one-second observation is the cadence at which the daemon measures the floor; it
  is one tick for every held frame, never a timer per frame. The same table governs the
  other waits: online migration skipped below the floor wakes on its resume, and expiry
  refused under the maintenance reserve wakes on it too, or when the pressure pass finds free
  space back above the reserve while a floor episode is open (an event, never a timer);
  the pressure pass's one-to-30-second delay is the observer of native state above.

  A held connection is not idle. The read timeout does not run while it holds,
  and an arrival at the connection ceiling cannot reclaim it; it is reclaimed
  only after its frame is stored and it waits at a frame boundary. A frame
  waiting for an operation permit is held the same way. The daemon holds one
  frame per connection: at most 65536 bytes plus its 4096-byte read buffer, so
  at most 16 connections times that per listener. Arrivals the listener does not
  accept wait in the kernel backlog only at the connection ceiling; the free-space
  floor no longer stops the listener accepting (#257), and a held frame does not
  depend on accepting. On shutdown a hold ends: the frame gets one last
  attempt and, if the store still refuses it, is counted `refused` as before.
  A wait for an operation permit is not cut short by a stop: the frame must
  still be published, which takes a permit from the budget shutdown waits on
  anyway. Each new offer is a refused write attempt, so the store-wide refusal
  counters (the free-space floor's `refused_writes` and the pressure status's
  `refused_writes`) count attempts and rise by one per offer; the listener's
  held counters count frames.
  Refusals that no retry can change (`RESOURCE_LIMIT`) are not held; they keep
  their counted discard. A full receiver-window registry is not a refusal at all
  (#215): the packet is stored without a window update and the store counts the lost
  evidence.

UDP has no sender to hold, so a UDP listener holds differently (#255). It runs a
reader and a publisher joined by a first-in first-out queue bounded in bytes:

- The reader takes datagrams off the socket as they arrive. It never calls the store
  and never waits for an operation permit. It stamps each datagram with the instant it
  was read, which becomes the record's `received_at`; within a listener the stamp never
  goes backwards, so a clock stepping back stalls the stamp instead of reversing it.
- Each queue entry holds the datagram's bytes and its read instant, and costs its
  length plus 128 bytes against `hold_bytes`, so a flood of tiny datagrams is bounded in
  count as well as in bytes. The memory a listener can use is `hold_bytes` plus the
  one datagram, at most 65536 bytes, that the reader holds while it waits for room.
- The publisher offers the head of the queue to the store through the same hold as a
  TCP frame: a fair wait for an operation permit, and on `STORAGE_PRESSURE` a wait for
  the wake and the stop and nothing else, governed by the table above. The head stays
  the head until it is stored, so the order of the store is the order of arrival.
  For a UDP listener `held_frames`, `holds_storage_pressure`, `holds_no_permit` and
  `longest_hold_ms` describe the head datagram. A queue behind a healthy store is
  burst absorption and is not a hold.
- When the queue is full the reader stops reading, and the kernel socket buffer takes
  the arrivals. Nothing is dropped in the daemon. When the kernel buffer fills too, the
  kernel drops the newest datagrams and counts them in `kernel_drops`, so what is
  stored is an ordered prefix and the loss is counted. The reader's wait ends when the
  publisher frees room. A stop does not end it: the datagram in hand is published if the
  store takes it, and counted lost with the rest of the queue if the store is still under
  pressure at the stop (see the stop below). The wait owns no timer.
- A datagram waits for the store with its read instant. A client that pages by moving
  `received_from` past the last record it saw will not see a datagram stored behind that
  position; a read run again over the range does, and a pin's membership does not change.
  If the store refuses the read instant as at or behind the archive watermark
  (`ARCHIVE_WATERMARK`), the publisher offers the datagram once more with the commit
  instant and counts it `restamped`. If the commit instant is also at or behind the
  watermark (the clock stepped back), the datagram is still not refused: it is stored with
  the instant just past the watermark (the fence plus one millisecond), counted
  `restamped_past_watermark`, and journaled, at most one line a minute. Its `received_at` is
  then that fence-adjusted time and not the clock; the read instant and the clock appear in
  the journal line. A log is never refused for its stamp. Any other `STORAGE_FAILURE` is not
  retried.
- A store error that is neither pressure nor a counted refusal returns from the
  listener at once. The reader stops with it, the datagrams still queued are counted in
  `lost_at_shutdown` (the datagram the reader holds while it waits for room is counted with
  them, and its wait ends, so `reader_full_longest_ms` stops growing) and named in the error,
  and the daemon's worker health gate stops the daemon, so a reader never fills a queue that
  nobody drains. Datagrams still in the kernel buffer at this exit are lost and not counted.
- A socket or stamping error in the reader closes the queue: the publisher stores what is
  queued, and then the listener returns the reader's error. A datagram the reader had taken
  off the socket and not queued when it failed (including one it was holding in a full-queue
  wait, which ends) is counted in `lost_at_shutdown` and named in the error.
- On a stop the reader drains the socket, reading up to the queue bound, and the
  publisher publishes the queue while the store accepts it. A stop that finds the store
  under pressure gives the head one last attempt. If that is refused, the head and the
  rest of the queue are counted once, as `lost_at_shutdown` datagrams and
  `lost_at_shutdown_bytes`, with one journal line and no discard row per datagram.

There is no spool. The loss window is the queue and the kernel buffer on any exit the
daemon does not get to count: a crash, a kill, an out-of-memory kill and the exit on a
full disk lose up to `hold_bytes` of queue and the kernel buffer for each UDP listener,
uncounted, because the process that would count them is gone. A publisher error counts the
queue and the datagram in the reader's hand, but the kernel buffer at that exit is lost
uncounted too. A stop inside the unit's
`TimeoutStopSec` counts the queue's remainder as above; datagrams left in the kernel
beyond the drain bound are lost uncounted.

The waits a UDP listener adds, with what ends each:

| Wait | Ending | Signal or observer |
| --- | --- | --- |
| The publisher holds the head for pressure or a permit | as the table above | as the table above |
| The reader waits for room in a full queue | the publisher stores or counts a datagram and frees its budget | event (the budget semaphore) |
| A full-queue episode's report interval passes | none exists | the same one-second observation, since the reader counts as held while it waits |
| The kernel drop count changes | none exists | read when a status is requested, when the reader leaves a wait and when the queue drains to empty after an episode; no timer |

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
refusals, cut packets (`truncated`), framing failures and timeouts. Connection
refusals stay zero because an arrival at the ceiling is served. Held frames add
`held_frames` (frames held now), `holds_storage_pressure` and `holds_no_permit`
(frames held at least once for that reason, each counted once however often it
is retried) and `longest_hold_ms` (the longest hold, finished or running). They
were added without a schema bump, as were the UDP kernel fields described below.
The daemon journals a line when a hold episode begins and, for every episode it
announced, a line when it ends, with the frames held and the longest wait. Beginning
lines are limited to one a minute per listener. An episode that starts inside that
interval is announced once the interval has passed, and the trigger depends on the
reason: a pressure hold is announced by its next offer to the store (every wake, so at
the latest at the next one-second observation), and a permit wait by that same
observation, since the wait itself has no retry. An episode that starts inside the
interval and ends before it has passed prints no line at all, neither beginning nor end:
a short suppressed episode leaves counters only.

A UDP listener adds kernel evidence, appended to the listener status without a schema
bump: `rcvbuf_requested_bytes` (always 8 MiB, 8388608), `rcvbuf_granted_bytes`,
`kernel_drops`, `kernel_counter_state` and `kernel_counter_reason`. A TCP listener
reports state `not_applicable` and zeros. The daemon asks for the 8 MiB receive buffer
with SO_RCVBUF when it binds and reads the size back with getsockopt. Linux stores
twice the clamped request, so the granted size is that stored size; it can be lower
than twice the request when `net.core.rmem_max` is lower than 8 MiB, which is reported
and is not an error. The daemon journals the request, the grant and `rmem_max` once per
listener at start; `rmem_max` reads as unreadable where `/proc/sys` is hidden. A failed
getsockopt read-back, or the socket's own `fstat`, fails startup with a message
naming the listener. A refused SO_RCVBUF request does not stop the daemon taking
syslog: the socket keeps the default buffer, `rcvbuf_granted_bytes` shows that size,
`rcvbuf_enlarge_error` carries the error text, `rcvbuf_enlarge_failures` is 1 (it is 0 or 1 for a listener, since the request is made once), and the
start-up journal line says the enlarge failed.
`kernel_drops` is the `drops` column of the socket's row in `/proc/self/net/udp`
(`udp6` for an IPv6 address), matched by the socket inode. It counts every datagram
the kernel dropped for that socket, for any cause, since the socket was bound. It is
read when a status is requested, when the reader leaves a wait and when the listener
stops; no timer reads it. When the listener stops, whether cleanly or on an error, its last count is taken, journaled
and frozen: `kernel_counter_final` is then true and later status requests keep that
count, although the closed socket's row has left the kernel table. `kernel_counter_state` is `available` when the count was
read. It is `unavailable` when the file cannot be read or has no row for the socket;
then `kernel_drops` is 0, `kernel_drops_unknown` is true and `kernel_counter_reason`
says why. A zero count is never reported unless the kernel reported it.

UDP syslog is lossy by its nature; loss is counted where it can be (the kernel's
drops, a clean shutdown) and stated plainly where it cannot (a crash, an exit on a
full disk). The shutdown count is the final kernel drop count and `lost_at_shutdown`.

The UDP queue adds these fields to the listener status, also without a schema bump and
zero for TCP: `queued_datagrams` and `queued_bytes` (the datagrams in the queue, the head
included until it is stored, and the budget they use, length plus 128 each), `hold_bytes`
(the bound), `reader_full_waits` (times the reader stopped on a full queue),
`reader_full_longest_ms` (the longest such wait, finished or running), `restamped`
(datagrams published with the commit instant after the archive watermark refused the
read instant), `restamped_past_watermark` (datagrams that the watermark refused at the commit
instant too, stored just past it), and `lost_at_shutdown` with `lost_at_shutdown_bytes`. The daemon journals
an episode of full-queue waits as it does a hold: the entering line gives the queued
datagrams and bytes against the bound and the kernel drops so far, with at most one a
minute per listener. An episode the limit suppressed is announced by the next wait or
observer tick once the minute has passed. The leaving line, printed when the queue next
drains for an episode that was announced, gives the waits, the longest wait and the
kernel drops during the episode. A head hold of a UDP listener prints the same hold lines
as TCP, naming datagrams.

Counters are process-lifetime evidence and reset on restart. Successful counts
follow durable store publication, but syslog provides no typed sender receipt.
TCP sends therefore do not prove publication; UDP network loss remains
unknown beyond the kernel drops above. Identical newly received packets are independent occurrences sharing
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
| `backup` | Removed (#330). The method keeps its ordinal (@53) and answers `UNSUPPORTED`; see "Removed online backup". |
| `watchClaims` | Read-only (schema 71): every pending claim and every completed or abandoned claim whose pending span overlaps a time window, with its history; see "Per-claim state listing". |
| `ingestPartReceipt` | Read-only (schema 71, additive): one set part's receipt state by request id (absent, stored, evicted or evicting) with its artifact id, byte length and format, and no record scan while the artifact is stored (once it is evicted or absent, the records it names are read until one is still stored); used by the part walk of a pending claim. |
| `payloadLookup` | Read-only (schema 71, additive, ordinal 56): the artifacts that hold one payload id (`blake3:` and 64 lower-case hex digits), one page by artifact id, bounded by `pageSize` (1 to 1000, default 200), with `complete` false while the `payload-index` migration runs; see "Payload id index". |
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
`require_complete_index=true` refuse with `EVIDENCE_INCOMPLETE` on applicable unknown
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
connection timeouts (arrivals at the ceiling are never refused) and explicit
failure. The canonical CLI/MCP listener renderer is also used by status. CLI/MCP wrap it with `store_id`, operation
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

A full receiver-window registry never refuses a packet (#215): the packet is stored
without a receiver position, a classified discard is not recorded, and either is
counted per stream in status `stream_evidence_loss` (kind `receiver`, with the source
and receiver instance) instead of counting a pressure discard.

Known local refused packets publish a classified observation without a packet
occurrence. A UDP datagram is kept whole. A TCP frame that is oversize, cut by
end of stream, broken or stalled publishes the bytes read as a packet and also
records its classified observation.
The listener waits for the same bounded global operation admission before these
control writes; there is at most one waiter per admitted listener/connection.
Shutdown joins admitted control writes. Storage admission refusals may be
classified; an uncertain storage failure stops the intake group without claiming
a known discarded packet. Failed history publication is a visible intake
failure, never a successful counter update. A TCP connection at the ceiling is
not rejected, so no connection count exists.

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
fallback. Full capacity never refuses a log (#215): the batch is stored without
stream positions (its records carry no `producer` position) and only that stream's
gap evidence is lost. The loss is counted per stream and shown in status under
`stream_evidence_loss`: the `max_named` (16) streams with the most lost positions
are named by kind, source and instance with their `batches` and `events`, every other
stream folds into `other_streams`, `other_batches` and `other_events`, and
`total_batches` and `total_events` cover all streams. The daemon tracks at most 1024
lost streams; `other_streams_exact` is false when more exist, and their positions are
still in `other_events` and the totals. The counts live in memory for the daemon's
life, and a retry of a request already accepted is idempotent and is not counted
twice. A stream without a window has no sequence overlap check, so the exact retry
(same request ID) is answered from the receipt, while a new request ID that repeats
sequences of an unrecorded stream is stored again, never dropped. A stream that
has a window keeps its overlap `CONFLICT` and gap accounting unchanged.

A window created later, after expiry frees a slot, starts at its first
observed position as every window does. A registry that would pass its byte bound
sheds that window's oldest exact gap intervals (`gaps_complete=false`, counts exact)
rather than refusing. A new window that cannot fit is treated as full, and an
existing window with nothing left to shed takes the batch without a window update
and counts it as lost; a later batch of that stream may then show those positions
as a gap. The same holds for receiver windows.

Source time mapping on the event route (#227). The daemon binds each event source's
`/event_time` (Unix milliseconds) mapping on its first batch. The mapping registry holds at
most 1024 sources and never shrinks. A batch from a source past that ceiling is never refused:
it is stored, its records carry unknown time (`no_profile`, the original `event_time` stays in
the record bytes), and no mapping row is written, so the stored format is unchanged. The
loss is counted per source, in memory for the daemon's life, and shown in status under
`source_time_loss` (schema 71, additive wire field `sourceTimeLoss`): the same bounded
report as `stream_evidence_loss`, with rows of kind `source_time`, an empty `instance`, the
`batches` taken and the `events` (records) stored without a mapping, at most 16 named
sources, the rest in `other_*`, and totals over all sources. A retry of an accepted request
returns its receipt and is not counted twice. A source that already has a mapping keeps it,
and a source with a different mapping still gets `CONFLICT`. Explicit
`define_time_profile`, `define_log_time_profile` and diagnostic profile declarations past the
ceiling are refused, but with the non-retryable `INVALID_ARGUMENT` (#238): they are not logs,
and nothing ever removes a mapping row, so no slot frees and a retry could never succeed.
Once the registry is seen full the daemon remembers it for the life of the process, so a
batch past the ceiling does not walk the mapping rows again.

A batch that no retry can ever accept is not answered with a retryable code (#227). On the
event route an over-bound batch (more records than `max_records`, inline bytes over 16 MiB, an
atomic write over 128 MiB or larger than the whole data budget) answers `INVALID_ARGUMENT`,
which is not retryable, and the client quarantines it; an operator who raises `max_records`
can replay the quarantine file. The size of the joined write, which includes source context
and authority metadata, is judged at write time against the same whole data budget and
answers the same non-retryable code. The `ingest` call answers an inline payload over 16 MiB
with `INVALID_ARGUMENT` too. Other callers keep `RESOURCE_LIMIT`. An input-only refusal
(record count, sequence count) is decided before the source mapping is bound, so a refused
batch leaves no mapping behind. A batch refused after the mapping is bound (at the write-size
checks, a record-level check, or when the write is applied) can still leave the mapping of its
source, which every later batch of that source needs anyway.

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
filtering on an applicable unknown origin refuses with `EVIDENCE_INCOMPLETE`.
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
watch and exact revision, relative Unix path as `Data`, and a typed device and inode (the device informational only),
byte-length, modification and change-clock generation. First admission requires
the current enabled watch revision and matching anchored regular-file identity,
with the opened file on the held root's filesystem by a live device check;
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
the inode, length and nanosecond modification/change-clock guard used
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

### Per-claim state listing (schema 71)

Status reports only per-watch counts. `watchClaims` (CLI `watch claims
[--since RFC3339] [--until RFC3339]`) lists claims one by one, so the zero-loss
check after a write stall is one read: of the claims that were pending at some
point since T, how many completed, which are still pending and for how long, and whether any
was abandoned. It is read-only: anonymous TCP and the ingest socket refuse it,
and a query-only daemon serves it. The daemon reads its own store; the CLI
never opens the store.

Each claim journal keeps a history from schema 71 on: `first_seen`, the
reservation time; `attempts`, one for each publication attempt that left the
claim pending and one for the attempt that completed it; `last_attempt`; and
`last_pending_at` with `last_pending_reason`, the time and error of the latest
attempt that left it pending, such as the 45-second consume publication
deadline. The reason is cut to 320 encoded bytes. The history moves with the
journal when the claim completes or is retired. A journal reserved by an
older binary has no `first_seen`; its span is its latest pending attempt alone.
Reserving a source now refuses a journal that would leave less than 448
bytes of its 8 KiB for this history.

A claim is `pending` while it is in the active recovery set, `complete` once
its publication and source removal are recorded, and `abandoned` once an
administrator retired it unrecoverable. The listing returns every pending
claim, and every complete or abandoned claim whose pending span overlaps the
window. The span runs from `first_seen` to `last_pending_at`, the only times the
journal keeps for pending attempts, so a claim with any pending attempt in the
window is listed even when a later attempt falls after it. A claim that pended
only before and after the window is listed too, because the intermediate
attempt times are not kept. An abandoned claim is also listed when its
retirement falls in the window. A
claim that completed on its first attempt never went pending and is not
listed. A completed journal reserved before schema 71 has no history, so
whether it went pending is unknown; it is listed when its completion falls in
the window, so a window from before the upgrade can still show its claims
complete. The window is inclusive Unix milliseconds; `--since` defaults to the
epoch and `--until` to the daemon's current time. Completed claims are kept
for the two-day retry window, so a window reaching further back can miss
them. One snapshot is read without the writer lock. More than 2048 matching
claims refuse with `RESOURCE_LIMIT` rather than return a partial list.

The CLI prints `as_of`, `since`, `until`, the `listed`, `pending`,
`complete` and `abandoned` counts, and `claims`. Each claim has `claim_id`,
`watch`, `state`, `relative_path_hex`, `first_seen`, `age_ms` (from first
seen to `as_of`, pending claims only), `attempts`, `last_attempt`,
`last_pending_at`, `last_pending_reason` and `finished_at`, the completion or
retirement time. Absent times are null.

```sh
sparrow SOCKET watch claims --since 2026-11-09T00:00:00Z
```

### Removed online backup

Sparrow backs up nothing except the archive age-out to GTS files on the NAS. The online store
backup (#156) was removed in #330, with `sparrow-backup`, the restore, and the `backup_root` and
`backup_keep` configuration keys. The server file refuses unknown fields, so a file that still
names either key fails to load with the parser's `unknown field` error.

The wire stays additive-only, so no ordinal moved. `backup @53` stays in the schema and the
server answers a call with `UNSUPPORTED` ("the online store backup was removed"), except that a
query-only daemon answers `READ_ONLY` and the ingest socket and anonymous TCP refuse it like other
non-producer operations; the `sparrow`
CLI has no `backup` command. `Status.backup @55` stays too, and the daemon always reports it as
before for a daemon without a backup: `phase` `unconfigured`, an empty `root` and no `failure`.
`BackupStatus` and `BackupPhase` remain in the schema for those two. CLI and MCP status no longer
render a `backup` object.

### Retiring an unrecoverable claim

A consume claim whose source moved into the private `.sparrow-inflight/`
directory and then vanished before publication can never complete: recovery
needs that exact claimed generation and never republishes a replacement. It
stays in `pending_claims`; `watchClaims` lists it as pending with the reason
"unpublished claimed source is missing", and its watch's `failure` names the
claim with that text until a later failure of a claim replaces it. A failure
that names no claim is kept beside it, and status shows the claim's failure
first. `watchClaimRetire` (CLI `watch retire-claim WATCH_ID CLAIM_ID REASON`,
MCP `watch_claim_retire`) lets an administrator retire such a claim. It is an
admin operation: anonymous TCP and the ingest socket refuse it, and a
query-only daemon refuses it as a write. The watch's own claim owner
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
of the 16 import slots. Retiring a claim, or that claim completing, clears the
watch failure only when that claim's recovery set it; another claim's failure
stays until that claim resolves. A failure that names no claim (discovery,
reservation, drain) is kept apart from a claim's failure, so neither hides the
other in status, and clears when any claim completes, not when one is retired.
The caller is the connection's administrator as the daemon identified it:
`unix uid=U pid=P` on the private socket, `tcp-chap peer=ADDRESS` on a CHAP
session. The claim leaves `pending_claims`; `status` and `watch list` list it
under `retired_claims` with state `retired-unrecoverable`, and `ingest_get`
reports its deletion as `retired_unrecoverable`. The daemon logs one line,
`sparrowd: watch=W claim=ID retired-unrecoverable caller=… reason=… path=…`.
A store keeps at most 1024 retired claims, which are never removed
(`MAX_RETIRED_CONSUMPTION_CLAIMS`). The retirement itself frees the claim's
journal slot in the same write batch, so a stuck claim holds one of the 16
slots only until an operator retires it. The bound exists because every status
and watch listing reads all retired rows through one iterator and sends them on
the wire. A row is at most 8 KiB stored, so the bound is 8 MiB of rows. On the
wire a row is at most about 3.2 KiB (2048 path bytes, 512 caller, 512 reason and
the claim ID), so 1024 rows are about 3.2 MiB against the 64 MiB message limit,
and about 5.5 MiB as MCP JSON, where the path is hex. Reaching the bound takes
1024 operator retirements. A retirement past it is refused with
`RESOURCE_LIMIT` and leaves the claim pending; no retired row is dropped. The
count of retired claims is the length of `retired_claims` in `status` and
`watch list`. Folding the oldest rows into a summary, which would remove the
refusal, needs a stored-format change and is not part of this bound.

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
  allocates an unlimited queue or stalls a production thread on network I/O. A
  briefly held queue lock makes admission wait, never drop. A permanent daemon
  refusal of a batch (a non-retryable `INVALID_ARGUMENT` or `CONFLICT`, or a batch that can
  never be accepted) is never reduced to a number: the
  client writes the batch's canonical event lines, the refusal code and reason and the event
  count to one file in `quarantine_dir`, and only then settles the batch. The events are
  counted (`quarantined_events`, with `last_quarantine` naming the refusal), reported by the
  next flush as not delivered (`FlushError::Dropped`, whose `quarantined_before_fence` counts
  those kept in files), and later events proceed. Status exposes `quarantine_dir`,
  `quarantine_files`, `last_quarantine_file` and `quarantine_write_failures`. The client never
  deletes or rewrites a quarantine file; its size is bounded only by the disk (a source whose
  time mapping conflicts refuses every later batch, so each becomes one file until the operator
  fixes the source). If the file cannot be written, the batch stays pending, the failure is
  counted and reported as the transient failure, which names the refusal code and reason, the
  directory and the I/O error, and the write is retried, so that batch is not dropped; later
  events wait behind it and are refused as `Full` once the queue is full. Any other
  non-retryable refusal (a replaced store, a read-only daemon) ends delivery as a terminal
  failure, and the pending batch and queued events are first written to quarantine files.
  A batch the client itself cannot encode takes this terminal route too: the client validates
  every batch before it is sent, so the failure ends delivery and the batch is saved by the
  terminal save under the code `CLIENT_REFUSED`, not by the per-batch route above. The
  daemon storing a refused batch as a malformed record with its reason is the preferred form
  and is not yet implemented (issue 216). A busy refusal (`RESOURCE_LIMIT`) is never quarantined:
  the client holds the batch, bounded by the queue limits, with overflow counted
  as `Full`. A full producer registry is not a refusal any more (#215): the daemon
  stores the batch and counts the lost gap evidence, so the client no longer holds
  a batch for it. A source past the 1024-mapping ceiling and an over-bound
  batch are no longer retryable refusals either (#227): the daemon takes the
  first and answers the second `INVALID_ARGUMENT`, which is quarantined. An
  older daemon still answers these with `RESOURCE_LIMIT`, which the client
  holds as before: held bytes are never lost, and the client cannot tell the
  case from a busy daemon. A busy (retryable) refusal retries the same
  batch on the same connection; only a transport failure reconnects.
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
entire data allowance is a `RESOURCE_LIMIT`, rather than a reclamation request (a producer's
event batch gets the non-retryable `INVALID_ARGUMENT` for it, see "Source time mapping on
the event route").

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
scope, shorter pin, altered datatype, or unrelated source generation. The wire
`Failure` also carries a `class` (additive, #257): `freeSpaceFloor` marks a retryable
`STORAGE_PRESSURE` made by the free-space floor, and `unclassified` is every other
refusal and every refusal of an older daemon.

| Code | Meaning |
| --- | --- |
| `INVALID_ARGUMENT` | Invalid IDs, ranges, scopes, durations, or names.  |
| `NOT_FOUND` | Unknown or reclaimed evidence/handle. |
| `EVIDENCE_UNAVAILABLE` | Known expired, evicted, or incomplete complete-file representation. |
| `PIN_NOT_ACTIVE` | Expired/released collection cannot be renewed or used as an active scope. |
| `CONFLICT` | Retry identity reuse, revision mismatch, or conflicting source generation. |
| `EVIDENCE_INCOMPLETE` | A `require_complete_index` read selected evidence whose interpretation is permanently unknown or failed at ingest (a failed projection, unknown captured diagnostics, a recordless artifact under a diagnostic filter, a reclaimed parent's unknown origin). Not retryable; no online migration causes it. |
| `RESULT_EXPIRED` | Bounded snapshot/result lease ended. |
| `RESOURCE_LIMIT` / `RESULT_TOO_LARGE` | Host admission/output refusal. Native certified governor outcomes retain their read receipts. |
| `STORAGE_PRESSURE` | Insufficient safe write capacity, with retry state. |
| `STORAGE_FAILURE` | Typed I/O, integrity, or commit failure; retryability follows the actual cause. |
| `ARCHIVE_WATERMARK` | Publication at or behind the archive watermark. Not retryable as sent. |
| `UNSUPPORTED` | Unsupported format operation, query capability, or mutation on the read surface. Raw artifact preservation can still succeed independently of projection support. |
| `READ_ONLY` | A write sent to a query-only daemon (schema 67). Not retryable against that daemon. |
| `PERMISSION_DENIED` | A non-producer operation on the ingest socket, or an import continued by an account other than the one that began it (schema 68). Not retryable on that connection. |

`ARCHIVE_WATERMARK` is a publication at or behind the archive watermark: a clock
regression, or a datagram stamped with its read instant that waited past the
watermark. It is not retryable as sent. It was `STORAGE_FAILURE` before the UDP
syslog publisher needed to tell it apart, and every other caller treats it exactly
as it treated `STORAGE_FAILURE`: the watch worker, which stops on a storage
failure, stops on it too. Only the UDP syslog publisher handles it, as described
with the UDP hold. `STORAGE_FAILURE` also covers retirement reaching unarchived evidence.
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
