# Sparrow interfaces

This document specifies the planned first-version contracts. The examples are
design examples, not commands supported by an existing executable. Lifecycle,
storage, and recovery requirements are in [Architecture](architecture.md).

MCP and CLI adapters call the generated Cap'n Proto daemon client. They must not
implement separate filtering, retention, pin, or query semantics. The daemon
owns the database; the local CLI and stdio MCP adapter connect to it.

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
| Read binding | Private Unix-domain socket: directory mode 0700, socket mode 0600. Remote transport requires a separately authorized authenticated deployment. |
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
  event time when known, media/format, and source name.
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
(`event_time`) explicitly. An event-time interval excludes unknown event times
and reports their count when known; it never substitutes receipt time. A source
profile states which field and representation supplies event time. Chronological
sorting uses the selected timestamp and occurrence ID as a stable tiebreaker.
Timestamp mappings, lexical/logical field matching, and text interpretation are
advertised profiles rather than guesses based on names or file extensions.

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
remains source-local and refuses an anchor excluded by the filter. Diagnostic
service/host/severity/trace/span dimensions remain planned.

The generated contract uses `Scope.sourceIds :OptionalSourceIds`, with distinct
absent and present-list states. CLI scope files and MCP use the same native JSON
decoder and typed encoder. Before sending a filtered read, these adapters require
current discovery through the native client; an older daemon is refused with
`UNSUPPORTED` rather than silently ignoring the added wire field. This discovery
check uses the existing status scan; it is not a bounded metadata-only handshake.

Membership expansion permits at most 100,000 direct pin members and 100,000
record visits, including overlaps, and retains only bounded IDs/timestamps.
Exceeding these bounds fails before any result is published. This resolution
work is additional to `scan_budget`, which governs the subsequent chronological
candidate scan; it is not an all-I/O budget. Explicit field/text reads seek only
resolved candidates, ordered by receipt time and ID. Context reads continue
bounded directional scans of the source catalog, excluding unselected records
and reporting a scan frontier when the budget is exhausted. The writer lock
holds membership stable throughout each operation. Persistent query snapshots,
query cursors and complete scope receipts remain unfinished.

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

Use the standard stdio MCP transport. The adapter translates MCP messages into
generated typed Cap'n Proto client calls over the daemon's private socket.
The stdio adapter writes protocol messages to stdout and diagnostics to stderr.
Tools use JSON Schema inputs and bounded structured outputs; reads are marked
read-only, while pin mutations are marked as mutations. [MCP transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports),
[MCP tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools).

| Tool | Principal input | Output |
| --- | --- | --- |
| `capabilities` | None | API/store versions, supported formats/profiles, vocabulary and tool descriptions, limits, retention, and durability settings. |
| `status` | None | Bounded storage/queue/projection/ingestion/pin metrics and pressure state. |
| `flush` | Store ID, current session ID, decimal sequence | Typed synchronization through that fence; reached sequence, or stale/future/store refusal. |
| `sources_list` | Filter, cursor, page size | Sources, active stream state, and known gaps. |
| `artifacts_list` | Scope, cursor, page size | Artifact summaries with byte availability and projection state. |
| `records_search` | Scope, text and/or filters, cursor, page size | Matching record summaries, text evidence, scores when applicable, and read receipt. |
| `keys_list` | Scope, field/path restriction, cursor, page size | Observed diagnostic fields and JSON pointers, kinds, record-presence counts, and read receipt. |
| `values_search` | Scope, optional field/path, explicit match operator/value/kind, cursor, page size | Matching value occurrences and parent evidence IDs/spans, with read receipt. |
| `records_context` | Record ID, scope, ordering basis, bounded before/after counts | Retained source-local neighbors, gap information, and read receipt. |
| `artifact_get` | Artifact ID | Metadata and availability, including retained-record-only provenance after parent expiry. |
| `record_get` | Record ID | Record metadata, parsed fields, projection/profile references, and source spans. |
| `bytes_read` | Exactly one artifact/record ID, offset, length, encoding | Exact bounded bytes and next offset. Refuse an expired complete artifact rather than substitute partial retained bytes. |
| `sparql_query` | Query, scope, graph layers, bindings, limits, coverage requirement | Typed native query result, result handle/page, and receipt. |
| `sparql_explain` | Query and scope | Native plan/explanation with applicable limits and scope information. |
| `correlate` | Scope and seed IDs or trace/span/source/time selector | Relationships and their supporting evidence; implemented through prepared queries/retrieval. |
| `result_next` | Result cursor | Next page from the same leased result; an expired handle refuses continuation. |
| `ingest_get` | Import/request ID | Raw acceptance/durability, projection states, source checkpoint, and deletion state. |
| `pins_list` | Store ID, cursor, page size, scan budget | Active named collections and deadlines under one finite captured metadata lease. |
| `pin_get` | Store ID, exactly one pin name or ID, cursor, page size, scan budget | Collection identity, deadline, revision, and captured members; continuations preserve the captured name resolution. |
| `pin` | Request ID, name, concrete targets, days | Durably created/extended collection and actual expiry. |
| `pin_renew` | Request ID, pin ID, days | Durably extended deadline. |
| `pin_release` | Request ID, pin ID | Durable removal of that collection's protection; ordinary retention still applies. |

The query interface supports read forms such as SELECT, ASK, CONSTRUCT, and
DESCRIBE through PurRDF. Preserve RDF term kind, datatype, language, base
direction, and triple-term structure using its result serializers. SPARQL UPDATE
does not provide arbitrary mutation of Sparrow's derived storage; it is refused
by the read surface.

### Core graph vocabulary

Sparrow's vocabulary is `urn:sparrow:vocab:`. Metadata uses `Artifact`, `Record`,
`Source`, and `Stream` classes. Its initial query predicates are:

| Predicate | Value |
| --- | --- |
| `receivedAt`, `expiresAt`, `eventTime` | Typed UTC `xsd:dateTime`; an unknown event time is absent. `expiresAt` is the ordinary retention deadline, not an implicit pin renewal. |
| `source`, `stream`, `artifact` | Entity IRIs linking an occurrence to its origin, generation, or parent. |
| `payload` | Content IRI corresponding to the occurrence's exact payload identity. |
| `projection` | Named graph IRI for a content projection under its recorded profile. |
| `byteLength`, `sourceByteStart`, `sourceByteEnd` | Typed nonnegative integer lengths/half-open offsets. |
| `service`, `host`, `severity`, `traceId`, `spanId` | Normalized diagnostic strings when available; source spelling remains in the original evidence. |

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
| `status` and planned capabilities operation | Operational status and supported profiles/limits. |
| `artifactGet`, `recordGet`, `bytesRead` | Store-scoped metadata and exact bounded ranges. |
| `pin`, `pinRenew`, `pinRelease` | Durable all-or-nothing protection with stable retry identities; renewal optionally checks the current collection revision. |
| `pinRead` | Typed active-list/name/ID selector, finite catalog capture and store-scoped continuation pages. |
| `keysList`, `valuesSearch` and planned text/record/graph operations | Shared native semantics, typed results and query coverage. |
| `watchList`, `watchPut`, `watchRemove` | Durable revision-checked recursive consume administration; removal stops new admission while preserving existing claims. Tail administration remains unfinished. |
| `watchHandoff`, `watchHandoffGet` | Immutable store/request/watch/revision/raw-path/file-generation handoff; durable admission and explicit reserved/claimed/published/removed progress. Stable retries recover the original binding before inspecting the current source path. |

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
in the development daemon. Tail watches, forced-overflow
qualification, and whole-process crash injection at every filesystem boundary
remain unfinished. Ordinary upload continues to preserve every caller source.

## 6. CLI

The planned binary names are `sparrowd` for the daemon and `sparrow` for its
client. Read/analysis commands default to JSON envelopes with the same receipts
as MCP; human formatting is an explicit option. `--raw` is only available on byte
reads. Errors and diagnostics go to stderr; failures use a nonzero exit code.

```bash
sparrowd --config /etc/sparrow/config.toml
sparrow capabilities
sparrow status

sparrow import /srv/dumps/run.jsonl --durability durable
sparrow SOCKET_PATH watch list
sparrow SOCKET_PATH watch put REQUEST_ID 0 completed-watch.toml
sparrow SOCKET_PATH watch put REQUEST_ID CURRENT_REVISION updated-watch.toml
sparrow SOCKET_PATH watch remove REQUEST_ID WATCH_ID CURRENT_REVISION
sparrow SOCKET_PATH watch handoff-prepare REQUEST_ID WATCH_ID CURRENT_REVISION RELATIVE_PATH INTENT.capnp
sparrow SOCKET_PATH watch handoff INTENT.capnp
sparrow SOCKET_PATH watch handoff-status INTENT.capnp
# Save intents outside consumption roots; retry the same intent after uncertain delivery.
# Tail watches remain unfinished.

sparrow records search --service worker --severity ERROR --text timeout
sparrow record get record:42
sparrow bytes read --record record:42 --offset 0 --length 4096 --raw

sparrow pin create --name problem_investigation_1 --for 10d --record record:42
sparrow pin add --name problem_investigation_1 --for 10d --artifact artifact:17
sparrow pin get --name problem_investigation_1
sparrow pin renew pin:8 --for 10d
sparrow pin release pin:8

sparrow sparql --query-file investigation.rq --pin pin:8
sparrow mcp --stdio
```

The CLI generates request IDs for new mutations and accepts an explicit request
ID for reproducible retries. Pin create/add both use the `pin` union-and-extension
operation; an existing name is reported in the receipt, not silently replaced.
The query file is a local client input; its path is not an evidence ID.

## 7. Rust client and logging integration

Plan a separate `sparrow-client` crate so producers do not depend on the database
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

`sparrow_client::TracingLayer::new(delivery.clone())` now composes through
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
The canonical event source currently carries span relationships; a separately
normalized diagnostic RDF relationship view remains unfinished.

Run `cargo run --release -p sparrow-client --example tracing -- SOCKET SOURCE`
for application-owned scoped subscriber composition and an explicit durable
shutdown. The worker and its queue/receipt/retry implementation are shared with
direct producer events. Schema 12 adds explicit buffered/durable direct artifact
and event-batch ingestion plus server WAL fences; the delivery worker retains
its durable acknowledgment law.

### Server WAL acceptance and synchronization

Schema 17 adds typed `Status.pressureStatus`, shared by CLI and MCP status.
It distinguishes physical disk admission from buffered WAL queue pressure.
Its phases are `ready`, `requested`, `awaitingReclamation` and `blocked`;
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

Expose API, store-format, codec/profile, and governor identities in discovery.
Changes to filtering, certainty, retention, identity, or pin semantics require
explicit versioning. Existing raw evidence must remain reconstructible through
supported migrations. Transport adapters share contract fixtures for scope,
pagination, pin receipts, certainty, and errors; CLI and MCP must produce the
same domain result for the same snapshot and operation.
