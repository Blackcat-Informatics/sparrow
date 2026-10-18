# PurRDF integration gaps

These are unfinished first-version requirements, not exclusions from Sparrow's
scope. Sparrow must retain native semantics and bounded reads while resolving them.

## Current integration state

Schema 60 is deployed on format 92 with native SELECT, ASK, CONSTRUCT and
DESCRIBE, typed RPC, CLI/MCP, immutable byte-bounded continuation, retained native
certificates and source/coverage receipts. Metadata includes populated/recordless
artifacts and retained Source/Stream declarations; the live qualification is in
[Development runtime](development-runtime.md#schema-60-live-adoption).
Native text relations, runtime adoption of public measured explanation and
production resource/turnover acceptance remain required. Source now selects
`9b2bf3b71325154137bc1aa817b46356daadfe3a`: upstream PR #609 closed
[PurRDF #605](https://github.com/Blackcat-Informatics/purrdf/issues/605) with
actual-payload materialization, and PR #611 closed
[#600](https://github.com/Blackcat-Informatics/purrdf/issues/600) with governed
request EXPLAIN. The source core adds `Store::sparql_explain` through the same
capture/admission owner as query execution. Signed `126248c` passed the full
release gate. Source schema 61 now adds typed explanation RPC, generated SDK,
shared CLI/MCP rendering and immutable continuation under the existing native
lease family. Three focused release consumer regressions, release Clippy and
the pinned census pass. The normal full gate is required before committing.
The installed image still uses the earlier native revision. The qualification
evidence below records how these surfaces grew.

Source schema 57 shares frozen logical scope receipts across native graph,
context and record/text/key/value captures. Graph receipt ownership uses the
existing native owned-workspace seam; query certainty and coverage retain their
original owners. [Source qualification](development-runtime.md#schema-57-graphcontext-scope-source-qualification)
and [default-image live adoption](development-runtime.md#schema-57-live-adoption)
are separately recorded; the remaining requirements listed above are still open.

Deployed schema 58 adds native correlation through fixed SELECT recipes and typed
substitutions, sharing the same immutable scope, governor, cancellation and
retained-page owners. Selectors cover concrete evidence, diagnostic fields,
source labels and explicit receipt/event intervals. Relationships cover shared
trace ID, span ID, Source entity and co-occurrence within an explicit interval.
Unknown interpretations reduce coverage and strict requests refuse them; native
certainty remains independent. This source implementation does not resolve the
borrowed text producer or operational EXPLAIN gaps below. Default-image private
qualification and live adoption are recorded separately in Development runtime.

Deployed schema 59 adds the normalized tracing observation view through the
existing metadata graph and query paths. It consumes native ordered JSON facts,
preserves producer-instance span identity and occurrence-specific parent/context
evidence, and uses the shared catalog pager/native dictionary owner for backfill.
Invalid claimed observations expose interpretation diagnostics without partial
relationships. Required projection/profile corruption remains a hard failure.
This is independent of the borrowed text producer, governed explanation and
physical reclamation gaps below. Default-image, closed-copy and live qualification
are separately recorded in Development runtime. A supplementary unbound content
export over the nine original producer records was refused by the existing
schema-58 workspace ceiling; complete content-export/resource acceptance is not
claimed from the successful byte or metadata migration checks.

Deployed schema 60 adds explicit plain-log timestamp captures using the public
native XPath engine, sharing timestamp interpretation and byte evidence with
JSON bindings. A direct reference to the already locked `purrdf-core` supplies
that public API without a facade re-export or a second matcher. Source,
default-image and live qualification are recorded separately in Development
runtime. The synthetic control-only profile advances the live writer fence
without new records; original JSON profile identities and recovered generation
receipts remain unchanged. All 253 retained byte targets match their prior hashes.

An earlier 2026-10-18 readback found upstream main at
`9a6055e8a60d34656d9d47c70ba5b85270142a54`, whose OWL causal-dependency changes
left #600 and #601 open at that time. Subsequent PR #611 closed #600. The probes
against `c75cbf94` and preceding source/census selection `7734b013` below remain
historical evidence; current source selection is recorded above.

## Query-scoped stored text producers

[PurRDF #601](https://github.com/Blackcat-Informatics/purrdf/issues/601) requests
the remaining borrowed-producer integration seam. Sparrow's RDF operation borrows
one native RocksDB snapshot and releases its catalog writer before evaluation.
Its stored-text relation must use that same snapshot. On pinned `7734b013` and
by source inspection on main `c75cbf94`, `PropertyFunctionRegistry::register`
requires `Arc<dyn PropertyFunction>` with an implicit `'static` bound, and the
producer's cursor is also returned as an implicitly `'static` trait object.
`NativeFnContext` supplies work/physical/cancellation ownership but no query-local
borrowed source binding. A compile-only probe fails with the exact lifetime
coercion; the otherwise identical owned-registration neighbor compiles.

The requested API supports the original borrowed source through all invocations
and cursor pulls, then retains only owned/admitted result values after query
completion. Native preparation, allocation, governor/cancellation and operational
failure precedence must remain intact. A worker/message bridge is a possible
lifetime workaround, but introduces another per-query ownership and metering
protocol. Sparrow requests the direct scoped seam rather than duplicating that
coordination or replaying the corpus into a resident index. The shipped native
`TextSearchRelation` and `TermOccurrenceRelation` hold a resident `TextIndex`;
Sparrow's record-corpus adapter instead reuses public analyzer and
`PreparedCorpus`/`PreparedQuery` scoring operations over persisted facts.

The source branch now routes ordinary record/text searches through one
`text_index::rank_postings` implementation accepting an explicit immutable
snapshot, captured corpus statistics and membership lookup. Ranking and emitted
posting evidence read the same snapshot. Native PurRDF still owns numerical
scoring; no payload is analyzed or resident text index rebuilt. Borrowed posting
rows reject oversized identity/value encodings before application-owned decoding.
Focused release regressions verify native score equality, frozen postings across
live replacement, corruption, cancellation and scan exhaustion without rows.
The full `make check` gate passes release Clippy with warnings denied, 27 core
unit tests, 183 storage tests, 64 RPC tests, the remaining workspace tests,
census self-tests and 280 pinned ownership jobs with zero exceptions.
The core helper is preparation for the text/SPARQL adapter; no text property
function or new public capability is advertised by this change.

Compile-probe source/results, issue readback and local gate evidence are under
`.stage/native-graph-integration/`: `borrowed_relation_probe.rs`,
`owned_relation_probe.rs`, `{borrowed,owned}-relation-probe.{jsonl,log}`,
`scoped-relation-probe-qualified.json`,
`purrdf-scoped-relations-issue-readback.json` and
`text-snapshot-full-check.log`. Temporary probe examples are removed before the
normal workspace gate. Runtime adoption remains separate from source validation.

## Governed solution materialization

[PurRDF #605](https://github.com/Blackcat-Informatics/purrdf/issues/605) records
the concrete cause of the supplementary content-export admission limitation.
The same nine synthetic producer records expose 2,990 content triples. Counting
them and returning 100 rows succeed on the installed default image; complete
SELECT refuses even without ORDER BY. The installed 128 MiB workspace ceiling
was preserved.

A separately verified 113-file closed backup supplied a private copy, opened
without intake or listeners. Its view reports a 128,256-byte maximum owned-term
certificate. Through the pinned native governed path, complete unsorted SELECT
refuses at private live allowances of 128 MiB, 512 MiB and 1 GiB. The sorted form
also refuses at 128/512 MiB; at 1 GiB it separately reaches the original 100,000
source-request ceiling. These trials are diagnostic admission measurements,
not process RSS or production qualification.

A controlled direct-native comparison with a private 1,536 MiB live allowance
and one million source requests completes both full queries, with 2,990 rows
and no governor trip. Unsorted materialization peaks at 1,152,851,360 admitted
bytes but settles to 1,464,897 retained bytes. Sorted execution peaks at
1,152,852,711 and settles to 1,466,248. Its 111,769 source requests independently
exceed the deployed 100,000 ceiling; this experiment does not certify public
sorted export or authorize silently increasing limits. The same direct calls
at 128 MiB return native Operational refusals without governor trips. Other
finite governors, cumulative I/O and row/lease ceilings are preserved.

The native `materialize_solutions` calls `try_value_of_admitted` for each cell.
For Existing terms, that function retains the whole-view maximum before copying
the term, then final publication settles the actual retained layout. The 8,970
output cells therefore stage 1,150,456,320 bytes of term allowance alone. The
existing native `try_owned_value_of`/`term_value_with_memory` path already admits
actual copied payloads under the original allocation owner. The upstream task
requests reuse of that owner in governed materialization, with unchanged source
failure precedence, cancellation, governors and retained-result ownership.
Sparrow must not write a second materializer or weaken its storage certificate.

The relevant native materialization bodies are unchanged between pinned
`7734b013` and inspected main `9a6055e8`. Main was not executed. Evidence and
probe sources are retained under `.stage/native-graph-integration/graph-workspace-*`
and `purrdf-materialization-issue*`. Failed live reads leave no query handles or
retained byte charge; the live record count and original backup hashes remain
unchanged. Complete content-export acceptance remains NOT MET.

## Retained operational SPARQL integration

[PurRDF #600](https://github.com/Blackcat-Informatics/purrdf/issues/600) is closed.
The selected source now supplies `explain_governed_fallible_view`, accepting the
original native request, options and governors and returning retained explanation
and governor/view receipts. Sparrow core shares the original scope, cancellation,
admission and remote-request policy through one execution boundary. Source
schema 61 adds transport/CLI/MCP integration, preserving structured native
entries under the same page/handle owner. Public consumer regressions pass for
bindings, measured counters, stopped/failure outcomes, replay/release/expiry,
shared admission and oversized-entry refusal. Runtime adoption and default-image
qualification remain required.

The selected-source consumer regressions pass: complete persisted materialization
retains 2,605 rows after source closure under the unchanged 128 MiB workspace;
EXPLAIN preserves substitutions, all native governor limits, frozen scope and
retained node charges, including zero-fuel stops; UPDATE, zero host source-read
admission and cancellation retain their refusals. Release Clippy passes with
warnings denied. Evidence is under
`.stage/native-graph-integration/native-request-adoption-core-explain-final-*`.
These checks do not qualify the installed image or a public explanation adapter.

The prior limitation was observed on 2026-10-18: `7734b013` and then-main
`c75cbf94` exposed retained
fallible measured EXPLAIN, but its public inputs are text/base/options and an
optional stop signal. Typed request substitutions are not accepted; options
document prebinding as inert. The measuring body chooses METERED governors
internally, so callers cannot supply the query's finite governor settings.
Sparrow will not ignore bindings, rewrite query text, copy the private evaluator,
or claim that different measuring limits explain the original governed request.
PR #611 supplies that native request/governor parity through the existing
measuring and publication owners. The historical observations below preserve
source-bound evidence rather than describing the new API.

The preceding runtime selection and helper census reference named committed PurRDF
`7734b013d19b68600ad24c4e8aa5a40955a8e89e`. That revision includes
`a77b743bcc6fad89d88581ef9cb5a626a326834f` (PR #526), which delivered the
bounded operational SPARQL ownership requested in
[issue #508](https://github.com/Blackcat-Informatics/purrdf/issues/508).
This source selection does not update the installed daemon.

Sparrow's persisted view implements the public `reserve_owned_workspace`
capability through its original shared read account. The erased reservation's
control box is admitted before PurRDF's public fallible allocation primitive
constructs it. Its fixed charge survives `resize(0)`. It retains the original
scope reservation, keeping the provider account admitted when a native complete,
partial or explanation result outlives the RocksDB snapshot callback. Payload
resizes use the same checked capacity owner as ordinary term/read guards;
releasing capacity never clears a sticky refusal. No snapshot or evidence pin is
implicitly renewed by retaining a result.

Operational results now carry PurRDF's retained ownership. Callers borrow the
native solution, boolean and graph accessors, or use ownership-carrying
extraction; they cannot move a raw result out and discard its admission. The
storage regressions compare the actual persisted four-form query outputs with a
resident oracle made by PurRDF's own `dataset_from_view`, confined to the test.
They also exercise representative algebra, retained solution extraction and
measured operational EXPLAIN. These checks and the required full gate must pass
before this integration is considered qualified.

Focused local qualification on 2026-10-17 passes four stored-view regression
tests, all sixteen core unit tests, and the long-text-query regression. The
seeded census self-tests include raw module identifier path resolution; the
pinned cross gate reports 260 ownership jobs, zero distinct exceptions and no
unsanctioned duplicate group. Logs are retained locally under
`.stage/native-graph-integration/`. These finite checks do not establish the
unfinished public graph surface or production-volume acceptance.

The former runtime revision `e58974e504b9681597faea1490002ce8ddaf7448`
admitted only projected SELECT/BGP without an explicit dataset. Its refusal of
GRAPH, ASK, CONSTRUCT, DESCRIBE, broader algebra, substitutions and extension
contexts was the upstream blocker; that historical refusal must not remain the
expected success condition of Sparrow's tests.

The core's `with_rdf_scope` entry point captures graph owners from live membership,
concrete record/artifact selections or a revision-guarded pin. It reuses the record
query selection, temporal ordering/integrity checks and diagnostic matcher. The
writer is held through membership capture and native snapshot creation, then
released before the query callback. The result page size never truncates scope;
catalog/scan/read admission exhaustion refuses capture before the callback.
Content readiness follows one shared implementation with explicit-ID reads.
Permissive scope reads expose ready graphs and independent failed/not-applicable
occurrence counts; strict reads refuse failed applicable content. Unknown
diagnostic membership remains unknown, independently of query certainty.

Capture reserves the existing 32 MiB catalog allowance, charging decoded entries
at the shared 64x encoded-byte law. It retains only admitted graph-owner IDs for
execution, with the original owned workspace scope capability. It does not copy
the selected RDF corpus into memory. Cooperative cancellation is checked during
capture and by the native read-account checkpoints throughout execution.
This is record-backed graph membership: it does not yet add independent metadata
graphs for recordless artifacts or expose the RPC/CLI/MCP query operations.

`Store::sparql_query` is the core execution caller. It accepts a native
`SparqlRequest` with typed substitutions and explicit governors, invokes native
governed execution over that captured scope, and retains native result ownership,
certified partial outcomes and diagnostics. Its host envelope records the store,
native sequence, eligibility clock and independent coverage. UPDATE stays on the
native read-query refusal path. Remote requests are disabled; this entry point
does not perform implicit federation reads.

Local qualification of the scoped caller passes all four new regressions:
native read forms and typed substitutions/governor stops/UPDATE refusal;
shared selection and a snapshot surviving pin release/retirement; capture
exhaustion and cancellation without a successful prefix; and independent failed
content/unknown diagnostic coverage. The full `make check` gate passes release
Clippy with warnings denied, all 172 storage tests, the existing 60 RPC tests,
the other workspace tests, seeded census tests and the pinned cross gate
(263 ownership jobs, zero distinct exceptions). Evidence is
`.stage/native-graph-integration/scoped-graph-full-check.log`. Existing RPC tests
do not establish a graph RPC surface; that remains unimplemented.

The core `sparql_read`/`sparql_read_next` path now retains that same native
outcome behind the shared catalog lease owner and precomputes item/byte page
boundaries. It borrows native solutions, ASK values, all three RDF statement
streams, explicit graph declarations and SELECT auxiliary graphs. It preserves
native partial certificates rather than inferring certainty from an empty item
list: zero-fuel COUNT can yield a certified empty lower bound without any count
binding, while an unsettled ASK yields unknown with no items. Captured item
counts are transport counts, not proven-answer cardinalities.

Qualification passes three new storage regressions for byte boundaries and
frozen replay after pin release/retirement within a live lease; certificate and
failure preservation; and handle exhaustion, release and exact expiry. A native
graph fixture separately covers empty declarations, ordinary quads, reifiers and
annotations without an owned graph replay. Full `make check` passes 175 storage
tests, 17 core unit tests, 60 existing RPC tests, the remaining workspace tests,
release Clippy and both census gates (267 jobs, zero distinct exceptions).
Evidence is `.stage/native-graph-integration/native-pages-full-check.log`.
The shared registry accounting now gives status and maintenance the same expiry
path. These core pages still require typed graph RPC, generic `result_next`
dispatch and CLI/MCP integration; the installed daemon cannot create them.

The remaining text surfaces must reuse native prepared bindings,
typed Cap'n Proto publication, retained-page dispatch, cancellation and CLI/MCP
parity. Complete, certain, at-most, unknown and operational failure outcomes must
retain the native distinction. A missing or failed source checkpoint must never
publish a successful result prefix. Measured EXPLAIN must execute through the
same bounded persisted view.

Do not suppress the bounded-view declaration, substitute a no-op reservation,
replay a selected corpus into a resident dataset in production, or copy the
private evaluator or workspace-pricing implementation. The complete SPARQL
CLI/MCP surface must not be advertised until its native admission and egress are
qualified.

Full qualification must compare resident and persisted answers for all four
forms, named/default graph selection, joins/modifiers/paths, RDF 1.2 layers and
terms, empty graphs, selected scope, retention/pins and failures during result
staging. It must separately verify certified governor stops and storage failures.
The dependency migration also propagates native fallible RDF builder operations
through Sparrow's typed error boundary (resource refusals remain RESOURCE_LIMIT)
and converts exact parsed timestamp
integers through a checked integer range. Ranking consumes PurRDF's bounded
score carrier at its full native value width. The removed native 1,024-query-term
ceiling is not reimplemented locally: query input remains limited to 64 KiB and
all existing posting/capture admissions apply. `limits.query_terms` is null
because there is no independent distinct-term ceiling; `query_text_bytes`
continues to report the effective input ceiling. A regression covers more than
1,024 distinct analyzed terms and independently refuses oversized input.

Physical pressure recovery has its separate existing-API qualification in
[native reclamation](native-reclamation.md). The former proposed binding API is
not a prerequisite; the implementation must be evaluated against available
native compaction/blob-GC policy and measured retry admission.
