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
| `pins_list` | Cursor, page size | Active named collections and deadlines. |
| `pin_get` | Exactly one pin name or ID, cursor, page size | Collection identity, deadline, revision, and captured members. |
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

The daemon contract is `crates/sparrow-server/schema/sparrow.capnp`. Generated
client/server types carry requests, binary payloads, instants, counts,
result/error unions and store-scoped references. Mutation requests carry stable
`requestId` values. Uploads, batches, RPC traversal, operation work and concurrency
are bounded. Domain errors remain distinct from a broken RPC connection.

| Typed operation | Contract |
| --- | --- |
| `ingest` and planned streaming import capability | Original bytes, occurrence identity and explicit durability. A partial stream cannot publish a complete artifact. |
| Planned event batch operation | Preserve independent occurrences and client sequence/retry identities. |
| Planned flush and import status operations | Synchronization through a specified watermark; raw, projection, checkpoint and source-deletion state. |
| `status` and planned capabilities operation | Operational status and supported profiles/limits. |
| `artifactGet`, `recordGet`, `bytesRead` | Store-scoped metadata and exact bounded ranges. |
| `pin`, `pinRelease` and planned renew operation | Durable all-or-nothing protection with stable retry identities. |
| `keysList`, `valuesSearch` and planned text/record/graph operations | Shared native semantics, typed results and query coverage. |
| Planned watch administration | Create/list/update/remove and completed handoff without deletion authority over replacement paths. |

Artifact upload metadata accepts `source_name`, an optional existing `source_id`,
`media_type`, `format`, labels, and requested `durability`. Omitted source identity
uses the configured upload source. Format selects the interpretation profile;
it does not authorize changing original bytes or supplying server receipt time.
Event batches use `application/json` with a versioned `sparrow.events/1` envelope
and client instance/sequence identities, or `application/x-ndjson` for a JSONL
stream. Preserve event subspans through the shared JSON parser rather than
round-tripping events through a lossy serialization.

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

No upload or tool-supplied path independently authorizes deleting arbitrary files.
The configured consume watch and its claimed source generation are the deletion
authority. Ordinary file import preserves the caller's file unless it belongs to
that completed-consumption workflow.

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
sparrow watch add /srv/dumps/completed --mode consume --readiness close-or-idle --idle-for 30s
sparrow watch add /srv/logs/active --mode tail --completion explicit

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

## 8. Errors and compatibility

Typed errors carry `code`, a concise `message`, `retryable`, `request_id` when
applicable, and bounded `details`. An error does not include a successful query
prefix after an operational failure. Refusals do not silently select a smaller
scope, shorter pin, altered datatype, or unrelated source generation.

| Code | Meaning |
| --- | --- |
| `INVALID_ARGUMENT` | Invalid IDs, ranges, scopes, durations, or names; 400. |
| `NOT_FOUND` | Unknown or reclaimed evidence/handle; 404. |
| `EVIDENCE_UNAVAILABLE` | Known expired, evicted, or incomplete complete-file representation; 410 for expired/evicted, 409 for incomplete. |
| `PIN_NOT_ACTIVE` | Expired/released collection cannot be renewed or used as an active scope; 410. |
| `CONFLICT` | Retry identity reuse, revision mismatch, or conflicting source generation; 409. |
| `INDEX_NOT_READY` | Required projection coverage is unavailable; 409 with pending/failed diagnostics. |
| `RESULT_EXPIRED` | Bounded snapshot/result lease ended; 410. |
| `RESOURCE_LIMIT` / `RESULT_TOO_LARGE` | Host admission/output refusal; 429 or 413 respectively. Native certified governor outcomes retain their read receipts. |
| `STORAGE_PRESSURE` | Insufficient safe write capacity; 503 with retry state. |
| `STORAGE_FAILURE` | Typed I/O, integrity, or commit failure; 503; retryability follows the actual cause. |
| `UNSUPPORTED` | Unsupported format operation, query capability, or mutation on the read surface; 400. Raw artifact preservation can still succeed independently of projection support. |

Expose API, store-format, codec/profile, and governor identities in discovery.
Changes to filtering, certainty, retention, identity, or pin semantics require
explicit versioning. Existing raw evidence must remain reconstructible through
supported migrations. Transport adapters share contract fixtures for scope,
pagination, pin receipts, certainty, and errors; CLI and MCP must produce the
same domain result for the same snapshot and operation.
