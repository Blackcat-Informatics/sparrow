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
remains source-local and refuses an anchor excluded by the filter.

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
Scoped native views include it without resident-corpus replay. Full graph-layer
selection and correlation/explanation queries remain unfinished.

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
check uses bounded `discover`; explicit operational `status` remains a catalog scan.

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
bounded native JSON page body. Complete scope receipt fields remain unfinished.

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
sparrow-query-capture/2; QueryLimits.queryPathBytes reports that ceiling for
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

A capture owns at most 2 MiB including encoded native JSON metadata/rows and
selector capacity, with 32 KiB per row and 32 KiB metadata ceilings. PurRDF's native
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
creates another handle. Foreign stores refuse STORE_MISMATCH; malformed/non-page
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

Semantic receipts remain frozen across pages; page position and cursor stay in
the existing separate page object. QueryCapture reuses the typed catalog page
receipt on the wire. Capture evidence is admitted with result metadata and
decoded by PurRDF's native JSON codec. Retention, pin changes and profile
declarations do not recompute it. A successful capture has complete execution
and null stop_reason; capacity/scan exhaustion refuses before a page is published.

These operations do not invoke a native query governor, and native_governor is
explicitly absent. Existing incomplete index/scope certainty is preserved;
it is not relabeled as a native certified partial answer. The wider receipt
contract, native governor certification, remaining query surfaces remain first-version requirements.


### Single context capture admission

Schema 33 adds a typed ContextCapture to records_context. Each successful
operation has a fresh snapshot_id and captured as_of, the host admission
profile sparrow-context-capture/1, explicit native governor absence, and
ContextLimits. Context is one complete reply without a continuation or retained
result handle. CLI/MCP receipts report has_more=false and next_cursor=null;
there is no read lease deadline to renew. Original execution, certainty,
directional frontiers, diagnostic coverage and continuity_unknown remain visible.

Chronological visits remain independently bounded per direction. Catalog decoding
now charges the anchor and every visited neighbor, including skipped sources,
against one 32 MiB conservative allowance across both scans. The shared catalog
owner charges encoded catalog bytes at its established expansion factor before
decoding. One 256 KiB response allowance includes the anchor, both directional
outputs and a metadata reserve. Native encoded record rows use the same
conservative typed/agent envelope charge as paged searches. Byte exhaustion is
RESOURCE_LIMIT with no successful result; it never publishes a byte-limited
prefix. Request fewer neighbors or narrow the acceptance interval to reduce work.
These are host admission allowances, not a measured RSS or all-I/O guarantee.

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
| `result_next` | Store ID, result cursor | Next page from the same leased result; an expired handle refuses continuation. |
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
| `discover`, `status` | Bounded compatibility/admission metadata, and separately measured operational status. |
| `artifactGet`, `recordGet`, `bytesRead` | Store-scoped metadata and exact bounded ranges. |
| `pin`, `pinRenew`, `pinRelease` | Durable all-or-nothing protection with stable retry identities; renewal optionally checks the current collection revision. |
| `pinRead` | Typed active-list/name/ID selector, finite catalog capture and store-scoped continuation pages. |
| `keysList`, `valuesSearch` and planned text/record/graph operations | Shared native semantics, typed results and query coverage. |
| `watchList`, `watchPut`, `watchRemove` | Durable revision-checked recursive consume administration; removal stops new admission while preserving existing claims. Tail administration remains unfinished. |
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
without proven matches, and remains identical across continuation.

`total_items` and `known_matching_artifacts` count captured proven rows;
`matching_artifacts` is null and `answer_certainty` is incomplete when diagnostic
membership is unknown. CLI `--require-complete-index` and MCP
`require_complete_index=true` refuse with `INDEX_NOT_READY` on applicable unknown
mappings. No prefix or handle is published on refusal. CLI and MCP share scope
decoding, typed coverage encoding and rendering.

Cursors bind the original membership, selector and limits. Continuation serves
the immutable capture after pin release, collection changes or evidence retirement;
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
not collapse occurrences. Immutable time/diagnostic mapping IDs are optional;
use their existing get operations for definitions. Declared mapping and
configured watch sources remain discoverable with zero retained input.
Watch IDs, revisions and enabled state are captured and sorted within each row.
Producer stream continuity remains `continuity_unknown=true`; this catalog is
not a stream generation or gap ledger. Those operations remain unfinished.

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
does not renew the lease or pin any evidence. Summaries carry original artifact
metadata in an explicit optional union, complete-byte availability at capture
time and exact retained record count. Reclaimed parent catalogs return absent
origin metadata while surviving records retain parent ID/source. Receipt-time
filtering on an applicable unknown origin refuses with `INDEX_NOT_READY`.
Complete-byte availability is an observation at `as_of`; byte reads independently
check current retention. All counts render as decimal strings in agent output.

Status now carries typed artifact catalog handle/byte counts, limits and lease
duration. Artifact and pin pages share both their receipt wire codec and agent
renderer. Schema 27 supersedes this initial scope with membership and diagnostic
filtering. Corpus-scale indexed selection and producer stream discovery remain
unfinished.

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
