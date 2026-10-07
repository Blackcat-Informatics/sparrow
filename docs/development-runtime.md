# Development runtime

Implementation is in progress in `/home/paudley/Active/sparrow`, on
`implementation/helper-census`. Full architecture acceptance remains unmet.

## Transport and present operations

The daemon uses **Cap'n Proto RPC over a private Unix-domain socket**. The HTTP
implementation and its dependency were removed following the transport decision
on 2026-10-06. No JSON operation envelope is sent between clients and the daemon.
The generated typed contract is
[`sparrow.capnp`](../crates/sparrow-server/schema/sparrow.capnp). Domain results
and failures are explicit unions; counts and clock instants are unsigned integer
fields, formats and JSON kinds are enums, and references carry a store identity.

Present RPC methods are `status`, `ingest`, `artifactGet`, `recordGet`,
`bytesRead`, `pin`, `pinRenew`, `pinRelease`, `keysList`, `valuesSearch`, `textSearch`, `recordsContext` and `rdfInspect`. The shared typed
client transport is `sparrow_server::connect`, used within a Tokio `LocalSet`.
The `sparrow` CLI and stdio MCP adapter share generated-client operations and
result rendering. The CLI supplies status, non-destructive buffered file import,
exact byte export, key/value and full-text searches. MCP also supplies artifact/
record metadata and pin/release calls. CLI and MCP both supply stable-ID pin renewal. Remaining CLI commands and query surfaces
are required work.

```sh
CARGO_BUILD_JOBS=4 cargo build --locked --release -p sparrow-server
target/release/sparrowd --config config/development.toml
target/release/sparrow /home/paudley/.local/state/sparrow/development-v5/rpc.sock status
```

## CLI investigations

Every command starts with `sparrow SOCKET_PATH`. These examples use a synthetic
completed file; production sample locations remain read-only references.

```sh
sparrow SOCKET_PATH ingest case-001 synthetic jsonl /tmp/diagnostic-copy.jsonl
sparrow SOCKET_PATH keys --source synthetic --received-from 2026-10-06T00:00:00Z --received-before 2026-10-07T00:00:00Z
sparrow SOCKET_PATH values --source synthetic --path /message --kind string --decoded 'hello world'
sparrow SOCKET_PATH text 'allocation failed' --source synthetic
sparrow SOCKET_PATH bytes STORE_ID ARTIFACT_ID 0 256 /tmp/evidence.bin
sparrow SOCKET_PATH pin-renew renewal-001 STORE_ID PIN_ID 10 1
```

Use a stable request identity for retrying ingestion of the same source, format
and byte sequence. Conflicting reuse is refused. The importer reads at most the
daemon's artifact limit plus one byte, capped locally at 64 MiB plus one byte,
and refuses oversized input without sending ingestion. It does not stream large
files yet, delete inputs or tail active files. Supported explicit formats are
`jsonl`, `log`, `json`, `markdown` and `opaque`; raw preservation is independent
of projection support. Its receipt includes concrete artifact/record IDs and
the daemon's durability acknowledgement.

`bytes` takes store identity, an `artifact:` or `record:` ID, a relative offset,
length (at most 1 MiB) and a new output filename. It writes raw bytes and syncs
the output file; it refuses to overwrite existing files. Its stdout is a JSON
receipt. A local write failure reports that the new output may be incomplete.

`pin-renew` takes an explicit stable request ID, store ID, `pin:` ID, positive
integer days and an optional decimal UInt64 expected revision. MCP's `pin_renew`
uses `request_id`, `store_id`, `pin_id`, integer `days` and optional string
`expected_revision`. Both return the same durable receipt, including direct
members, actual deadline, revision, request identity and durability. Renewal
keeps membership unchanged and uses `max(current_expiry, now + days)`.
Revision mismatches refuse the entire mutation. An identical retry within the
48-hour receipt window returns its original result, including after expiry or
release; that historical receipt does not make the collection active again.
Fresh renewal of an expired/released/unknown ID reports `PIN_NOT_ACTIVE`, and
never follows a replacement that reused its name. Creation/addition and renewal
share one deadline rule, revision increment and synchronized atomic pin/receipt
publication path.
Durations must also remain within the native RFC 3339 clock's representable year
range. UInt64 arithmetic success alone does not authorize an unrenderable
deadline; creation/addition and renewal refuse it before storage publication.

Key/value scope defaults to every retained source and acceptance time, 100 rows
and a 10,000-entry scan budget. `--received-from` and `--received-before` accept
millisecond-exact RFC 3339 instants through the native PurRDF codec. Options
`--limit` and `--scan-budget` select the daemon's bounded work. Value filters
`--path`, `--kind`, `--lexical` and `--decoded` combine as an intersection.
`--decoded` matches exact decoded string equality; it is not token search.
JSON kinds are `string`, `number`, `true`, `false`, `null`, `array` and `object`.
Duplicate/unknown options and invalid intervals are rejected. JSON output keeps
unsigned counts as decimal strings and includes the daemon's completeness and
coverage receipt. Transport/domain failures exit unsuccessfully without a
successful result prefix.

Development configuration selects
`/home/paudley/.local/state/sparrow/development-v5/rpc.sock` and the database in its
parent directory. The directory is mode 0700 and socket mode 0600. The daemon
refuses an existing endpoint rather than unlinking a listener's socket. An owned
dead endpoint must be reconciled before restart after an unclean shutdown.
Connections have a 60-second lease. A configured endpoint is not evidence of a
running server; verify startup and a typed `status` call.

Ingestion acknowledges only after the atomic RocksDB write is synchronized. A
connection failure before acknowledgement leaves delivery unknown; retry with
the same request identity. Ingestion and pin/create/renew/release receipts last 48 hours.
Expired evidence metadata has a one-day tombstone window, except live pinned
records. Retention maintenance runs every 30 seconds.

## Storage and queries

Core storage implements exact-byte content-addressed chunks, independent
occurrences, references, thirty-day acceptance retention, named overlapping
pins, record survival after parent expiry, bounded uploads and durable release
retries. PurRDF runtime dependencies are locked to
`e58974e504b9681597faea1490002ce8ddaf7448`.

JSON search facts come from `purrdf::json::analyze`. They preserve member order,
duplicate names, exact RFC 6901 pointers, native kinds, number spellings and
original spans. Full content digest and native profile identity bind shared
facts. Decoded equality uses PurRDF's lexical decoder. Parser failures keep raw
evidence and appear in query coverage.

The native JSON fact profile caps a record at 16 MiB source bytes, 65,536 values,
depth 128 and 2 MiB aggregate pointer text. Atomic ingestion batches are capped
at 128 MiB. `bytesRead` is capped at 1 MiB. Query limits are 1..1000 rows and
1..100000 catalog/value visits. Scans and response construction are bounded;
limited work is explicitly reported.

`keysList` and `valuesSearch` use temporal catalog scans over stored native
facts. These are not yet field-posting indexes. Scope selects exact source and
a half-open server-acceptance interval in typed Unix milliseconds. Event-time
interpretation remains pending. Value predicates include existence, kind,
exact lexical equality, decoded string equality and an exact pointer; no
wildcard selector is inferred.

## Stdio MCP

Launch the adapter with the existing private daemon socket:

```sh
target/release/sparrow /home/paudley/.local/state/sparrow/development-v5/rpc.sock mcp --stdio
```

A host configuration can use this command and argument array:

```json
{
  "mcpServers": {
    "sparrow": {
      "command": "/home/paudley/Active/sparrow/target/release/sparrow",
      "args": ["/home/paudley/.local/state/sparrow/development-v5/rpc.sock", "mcp", "--stdio"]
    }
  }
}
```

The adapter implements MCP protocol 2025-11-25 initialization, initialized
notification, ping, tools/list, tools/call and cancellation over newline-delimited
stdio. It uses PurRDF's JSON codec and native canonical base64 formatter. The
daemon transport remains typed Cap'n Proto; the adapter never opens RocksDB.
Only protocol messages go to stdout. Each tool opens its own owned RPC connection,
so an idle MCP session does not retain an expired daemon connection lease.

Discovery advertises these implemented tools only: `capabilities`, `status`,
`keys_list`, `values_search`, `text_search`, `records_context`, `artifact_get`, `record_get`,
`bytes_read`, `rdf_inspect`, `pin`, `pin_renew` and `pin_release`. Input schemas reject additional fields;
query arguments use the CLI's names with underscores. Temporal filters remain
explicitly server-acceptance times. Offsets are decimal strings; bounded lengths,
row limits and durations are JSON integers. Byte results use native canonical
base64 by default or explicitly requested UTF-8, which refuses invalid bytes.
Pin/release calls require stable mutation request identities. Read/mutation
annotations accompany tool discovery.

Responses include structured content and the same JSON in a text block. Daemon
domain errors preserve code, message and retryability in structured tool errors;
transport failures have the distinct `RPC_FAILURE` code. Unsupported tools and
malformed protocol requests produce JSON-RPC errors. Undelivered or cancelled
mutation acknowledgement does not prove non-delivery: retry its stable identity.

The adapter bounds requests to 128 KiB, responses to 8 MiB, concurrent calls to
four and each call to 30 seconds. Oversized framing closes the session with an
explicit protocol error; malformed complete JSON frames receive a parse error
and subsequent valid frames can proceed. EOF drains pending calls. Discovery
requires daemon schema version 5, including durable pin renewal receipts, RDF profile identity, record context
and native text/profile identities;
older discovery contracts fail explicitly rather than invent those identities.

The real-daemon acceptance fixture checks CLI/MCP parity for keys, text and context, JSON
value matches, exact base64 bytes, store identity errors, pin/create/renew/release retry and
capabilities before and after daemon crash/reopen. An isolated unresponsive
socket fixture checks pending-call admission, failure during a partial input
frame, cancellation and oversized framing. Integer request IDs beyond binary64
exact precision round-trip without rounding. The full release `make check`
gate passed with these fixtures.

## Retained record context

```bash
target/release/sparrow "$SOCKET" context "$STORE_ID" "$RECORD_ID" \
  --before 10 --after 10 --ordering received_at_record_id
```

The `records_context` MCP tool takes `store_id`, `id`, optional `before`/`after`
counts (default 10 each), `ordering` (only `received_at_record_id`), and the
same source/acceptance-interval/limit/scan-budget arguments as searches. Counts
may be zero. The total including the anchor must fit `limit` (default 100,
maximum 1,000); invalid combinations are refused rather than silently reduced.
An explicitly supplied source or interval must include the anchor. Expired or
evicted anchors fail with `EVIDENCE_UNAVAILABLE`; reclaimed metadata fails with
`NOT_FOUND`. References from another store fail with `STORE_MISMATCH`.

The operation captures membership under the serialized storage owner and scans
the persisted time index outward from the anchor. It returns same-source retained
record summaries, their exact artifact-relative byte bounds and a common receipt.
Both neighbor lists are ascending by `(received_at, record_id)`. This is acceptance
order, not file byte order or inferred event time: all records in a completed
artifact share acceptance time, so their opaque record IDs break ties.

Each direction independently applies `scan_budget` (default 10,000, maximum
100,000 catalog entries). Its result includes `visited`, `retired` and a frontier:
`requested_count`, `scope_boundary` or `scan_budget`. Observed expired/evicted
entries from that source count as retirement gaps; entries from other sources
consume scan visits without appearing as neighbors. Budget exhaustion gives
limited/incomplete execution rather than an assertion that no neighbors exist.
Reaching the requested count is a complete bounded context request.

`continuity_unknown=true` is explicit: tombstones already reclaimed and producer
capture gaps cannot be inferred from the current catalog. Zero observed retired
entries does not prove continuous producer history. Context uses record metadata,
not JSON/text indexes; their projection coverage is not assessed by this operation.
It creates no pin and does not extend retention. A pinned record remains a valid
anchor after its unpinned complete parent artifact expires. Use `bytes_read` and
explicit pins to inspect and preserve the returned evidence.

Core tests compare neighbors against the stored timestamp/ID order and cover
other sources, interval boundaries, zero counts, budgets, reopen, expired anchors,
record pins, observed gaps and corrupted time-index refusal. The real-daemon
fixture checks shared CLI/MCP context results and typed ordering/store errors
before and after crash/reopen.
File/stream order, event-time context, pagination leases and producer continuity
qualification remain required later work.

## Persistent RDF foundation

Store format 4 bound the RDF layout, native JSON profile, native Markdown contract,
and admission limits to a persisted `rdf_profile` identity. Missing identity is a
storage failure; drift refuses reopening with a migration-required error. Earlier
format-3 data is preserved separately; this implementation does not migrate it.

Native JSON analysis supplies both diagnostic field facts and the full native RDF
projection. Markdown uses native slicing and parses its emitted Turtle through
PurRDF. Terms persist as native canonical `ProjectionTerm` JSON with a verified
forward/reverse dictionary and monotonic nonzero 64-bit IDs. Nested triple terms,
blank scopes, literal language/direction and datatype identity survive storage.
Primary, reifier and annotation streams remain separate, each with physical SPOG,
POSG, OSPG, GSPO, GPOS and GOSP indexes. Declared empty named graphs persist.
Native default graph rows are lifted to the content owner's named graph; explicit
native graph names remain intact. Owner IDs distinguish shared graph membership.

Identical content shares its projection graph while each occurrence has its own
record metadata graph. Metadata describes source, artifact, original byte range,
payload identity, acceptance/expiry and projection state. Pin-aware expiry removes
all six indexes and dictionary reference closures only after their final graph
owner expires. Raw evidence survives projection refusal with a diagnostic.

Graph admission caps rows at 32,768, term encoding at 64 KiB and nested depth at
16, dictionary staging at 32 MiB and anticipated RDF batch staging at 64 MiB.
These are admission bounds, not a measured process RSS guarantee. Metadata and
content publication share the raw-evidence atomic write batch.

`sparrow SOCKET_PATH rdf STORE_ID RECORD_ID` and MCP `rdf_inspect` expose retained
record metadata/content graph identities, projection state, diagnostic and exact
counts for each native stream. This inspection does not run SPARQL or reconstruct
a resident dataset. Native stored-view and governor qualification remain required
before semantic graph queries can be advertised.

Qualification compares every persisted JSON quad against native projection in
all six indexes, checks shared content and pinned expiry, exercises Markdown and
oversized/malformed projection refusal with exact-byte preservation, and verifies
RDF 1.2 nested terms, distinct blank scopes, directional literals, reifier and
annotation layers, and declaration-only empty graphs. Profile drift/missing
identity and shared CLI/MCP inspection are covered by the repository gate.

## Bounded stored RDF reads

Store format 5 adds atomic dictionary cardinality, a conservative encoded-term
high-water bound, and graph-declaration postings. Opening a native view does not
scan the dictionary or decode each graph manifest. Missing or inconsistent control
counters are hard storage failures. Format-4 data remains preserved separately
and requires explicit migration before reopening with this implementation.

`Store::with_rdf_records` admits one concrete selection of 1..1000 retained
record IDs. It includes occurrence metadata and complete shared content, validates
the selection atomically, and refuses failed applicable projections. Pin-aware
eligibility uses the same retention law as ordinary reads. Scope preparation,
pin parsing, term access, lazy index scans and native operator scratch share one
read budget; ordinary-age selections do not scan pins. The callback holds the
serialized store owner for its bounded lease. Concurrent snapshot qualification
and the full live/pin/selection query scope interface remain required work.

The default ceilings are 128 MiB live admitted capacity, 100,000 read admissions,
128 MiB admitted maximum read bytes, 100,000 index-row admissions, and a 30-second
lease. A lease may be configured up to 60 seconds. These are host admission
counters, not measurements of engine I/O or process RSS. Native engine caches and
whole-process qualification remain separate. Terms use their persisted encoded
high-water bound times 256 plus 4 KiB as conservative decoded/owned capacity;
lookup inputs are admitted before native conversion can clone them.

Native `DatasetView` and `FallibleDatasetView` read all three physical streams,
preserve declared empty graphs, and use physical prefix probes. Each stream also
exposes its virtual default set union while retaining named graph identities.
Adjacent owner/graph duplicates are suppressed without a resident row set. Typed
guards retain portable native term carriers and resolve datatype/triple links
through the same verified forward/reverse dictionary. Compact IDs remain scoped
to this read; they are not agent evidence references.

All read/lease/admission failures latch the first root cause and stop lazy
iterators. Native before/after checkpoints discard internal prefixes. Tests cover
cross-record native SELECT joins, selected-owner isolation, all bound-axis shapes,
native RDF 1.2 terms/layers, empty graphs, pin survival, exhaustion, missing terms,
malformed index/declaration identities and dictionary reclamation. The required
full SPARQL algebra remains unmet; see
[PurRDF integration gaps](purrdf-integration-gaps.md). No SPARQL tool is advertised.

## Persistent full text

Store format 3 binds native standard analyzer identity at atomic store creation.
The configured `text_lexicon_dir` must contain exactly five standard CBOR
lexicons; native `Analyzer::resolve` verifies their pinned physical identities.
Missing or changed artifacts refuse startup. A changed analyzer identity refuses
reopening without explicit migration. Format 2 stores are preserved and require
migration. The subsequent RDF layout uses format 4 and a separate
`development-v4` directory; format 3 stores also require explicit migration.

Ingestion stores native lexical terms, record token lengths, sparse frequency
postings and separate source-evidence postings in the same synchronized batch as
raw evidence. Expiry removes those postings with the owning record; pins retain
them. Queries never reread or reanalyze retained payloads. The initial record
search profile indexes raw UTF-8 text and decoded JSON string scalars together
as one explicitly named field. Non-UTF-8 and oversized records retain raw bytes
and report failed text coverage; opaque inputs report not applicable.

`text QUERY` uses the same source/time and work-limit options as `keys`. Native
query analysis and prepared BM25 scoring rank records matching any analyzed
query term. A digest identifies captured document membership, token lengths,
store and analysis/ranking laws. Query output includes profile identities,
population and token totals. Do not compare scores across different corpora.
When membership scanning is limited, ranking is over the explicitly incomplete
captured membership. Posting-budget exhaustion refuses ranked publication,
rather than scoring with incomplete document frequencies. This is bounded
stored-fact retrieval, not yet the governed SPARQL text relation or a pagination
lease.

The aligned text profile currently bounds a record at 1 MiB and 16,384 lexical
occurrences across raw and decoded views. Evidence output is at most 64 spans
per returned record, with an explicit `evidence_limited` flag. Raw matches retain
native contributors and highlights; decoded-string matches conservatively cite
the complete original JSON scalar with `coarse=true`. Membership and posting
scans each use the requested scan budget. Document lengths are persisted;
corpus/term populations are measured from the captured scope. Source/time corpus
aggregates and further posting access paths remain performance work.

## Verification and remaining work

The release-profile `make check` gate passed on 2026-10-06, including formatting,
Clippy with warnings denied, workspace tests and the helper-census self-test and
cross check. The real-daemon Cap'n Proto test passed for ingestion, original byte
retrieval, temporal key discovery, decoded string search, malformed JSON
coverage, typed store mismatch, durable occurrence identity after crash/reopen,
durable pin-release retry and rejection of partially valid pin requests. All
eighteen core storage/clock/query/text/context tests passed in the release gate.

The same daemon fixture also exercises the CLI before and after crash/reopen:
stable ingest identity across RPC and CLI, unchanged input bytes, oversized
import refusal, exact byte export and overwrite refusal, time-scoped duplicate
keys, decoded value matches, source exclusion, explicit limited execution and
invalid command rejection.

The latest release workspace build and full `make check` passed. The format-3
development daemon was started and its typed status client succeeded on
2026-10-06 local time (2026-10-07 UTC) with identity
`store:4b5ebc0de33ba66b6bf0892db5168bca`. A synthetic two-line log was durably
imported under `diagnostic-demo`; `text failed --source diagnostic-demo` returned
one record, native BM25 score `0.628834555557`, complete/exact coverage, and the
original `failed` span `[11,17)`. The query returned analyzer and corpus digests.
Directory mode 0700 and socket mode 0600 were verified. The owned format-2
development process was cleanly stopped after confirming its identity and empty
store; its database directory was preserved. These are development runtime
observations, not final architecture acceptance or a guarantee of later liveness.

The daemon was subsequently replaced with the qualified schema-version-2 build
on 2026-10-07 UTC, reopening the same format-3 store and preserving its identity.
Its typed status reported the persisted analyzer identity and text profile. A
live stdio MCP session initialized, listed all ten implemented tools, retrieved
capabilities and repeated the synthetic `failed` search. It returned the same
record, corpus digest and original span `[11,17)`, then exited successfully on
EOF. The private directory/socket modes remained 0700/0600.

The qualified context build then reopened that store with discovery schema
version 3 on 2026-10-07 UTC. Live CLI and stdio MCP context calls returned the
same synthetic anchor and one retained same-source neighbor, with explicit
`received_at_record_id` ordering and unknown producer continuity. MCP listed
eleven implemented tools. The store identity and analyzer/corpus evidence were
preserved, and directory/socket modes remained 0700/0600. No producer data was
consumed or deleted during these runtime checks.

The qualified RDF build started a fresh format-4 store on 2026-10-07 UTC at
`development-v4/rpc.sock`, store ID
`store:c0c19ff0de784da626470803a3274dbc`. The owned format-3 daemon stopped cleanly
and its database remains intact. Live JSONL record
`record:cc036f2592587ba42e4d947bf2c50284` has 80 native primary content rows and
11 metadata rows; Markdown record `record:5dd10b50225185eb43f38181eca8a579` has
41 content rows and 11 metadata rows. CLI and MCP inspection agreed; MCP advertised
twelve tools, drained EOF successfully and produced no diagnostic stdout.
The persisted RDF profile identity is
`1ceeeb7b44784d4db19d7696a8978e50c08961ddebceecd9934f61bd2f9f056f`.

The synthetic two-line log retained all 75 bytes exactly, returned the native
`failed` span [11,17), and exposed its same-source neighbor through context.
The 70,015-byte oversized-term JSON fixture produced an explicit failed RDF
state with no content graph; its original bytes were exported and compared exactly.
Directory mode 0700 and socket mode 0600 were verified. Only synthetic fixtures
were ingested; no production watches or sibling services were changed.
The full repository gate passed, including refusal rollback, native RDF 1.2
identity/layers, all six index permutations, pin-aware reclamation, profile drift,
and daemon/MCP crash-reopen coverage. Stored-view SPARQL remains unimplemented.

The bounded-reader build started a fresh format-5 store on 2026-10-07 UTC at
`development-v5/rpc.sock`, store ID
`store:46c88e949ccec658600b66c7becd5bb7`, RDF profile
`1909d176e2b728f3041d0b2c665600aed5303e4b8525260bc1165850690ad0d5`.
The owned format-4 daemon stopped cleanly and its database remains intact.
Live checks imported five synthetic occurrences. Two identical JSONL records
shared one 80-row content graph while retaining separate 11-row metadata graphs.
The Markdown record retained 41 content rows. All three fixture artifacts exported
their exact original bytes. CLI/MCP RDF inspection agreed, MCP listed twelve tools
and drained EOF without extraneous output, and text/key/context checks passed.
Private directory/socket modes remained 0700/0600. No production inputs were
consumed and no sibling services changed.

The required release gate passed 24 storage scenarios, four core unit tests,
daemon/MCP crash-reopen checks and eleven enforced helper jobs with no duplicate
exceptions. The Rust core's native SELECT join is qualified on persisted fixtures;
the daemon does not expose SPARQL. Native workspace admission for the required
remaining algebra is tracked in [PurRDF integration gaps](purrdf-integration-gaps.md).

Remaining final-server acceptance includes concurrent stored-view qualification,
governed SPARQL, source/artifact metadata graphs, further
text qualification, event-time and combined predicates, context and
correlation, final scope/pagination contracts, streaming imports and recovery
journals, durable completed-file consumption, inotify/tailing, remaining MCP/CLI tools,
syslog, the Rust logging client, pressure eviction/reclamation, codec
verification, bounded buffered durability, fault/performance qualification,
packaging and a verified running-server handoff.

The pin-renewal build reopened the same format-5 store on 2026-10-07 UTC with
discovery schema version 5. The previous owned daemon stopped cleanly; the five
synthetic records, store identity and RDF/text profiles remained unchanged.
Live MCP discovery listed thirteen tools and marked renewal as a mutation.
Fixture pin `pin:6487733f54effd194871768af71c3247` was created over
`record:6b962236ac743284a0598093966dd49a`, renewed with an expected revision,
and released. CLI and MCP returned identical revision-2 durable receipts with
deadline `2026-10-09T06:35:39.756Z`. Retrying renewal after release returned that
same historical receipt while status still reported zero active pins. The MCP
session drained EOF successfully without extraneous stderr. Only this synthetic
pin and the owned development daemon were changed.

The finalized renewal release gate passed 27 storage scenarios, four core unit
tests, the real-daemon/MCP crash-reopen and protocol tests, and twelve helper
jobs with no duplicate exceptions. Renewal coverage includes unchanged members,
non-shortening deadlines, simultaneous revision guards, changed-argument retry
conflicts, post-release/expiry retries, name reuse, the exact expiry/retry
boundaries, reduced membership limits after reopen and native RFC 3339 deadline
refusal before commit. Full server acceptance remains unfinished.

The five standard text lexicon artifacts are available in the sibling PurRDF
checkout under `crates/text/lexicons/artifacts/`. Native `Analyzer::resolve` must
verify their physical identities and bind the fingerprint to persistent text
projections. Do not substitute an empty lexicon or rebuild the retained corpus
as a resident index on each query.
