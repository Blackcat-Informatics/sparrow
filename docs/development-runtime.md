# Development runtime

## Oldest-unpinned logical retirement

`Store::evict_oldest` serializes finite chronological retirement against pin
mutations. It selects records by `(received_at, record_id)` and preserves active
record and whole-artifact pins. Retiring one record makes the unpinned complete
parent unavailable; protected sibling records keep their exact bytes and native
indexes. Shared payload, JSON, RDF and text cleanup uses the existing retirement
owner. Complete source retry receipts retain their original occurrence identity
and do not resurrect evicted bytes.

Each pass limits newly evicted records, time and pin visits, payload ownership
removals, conservative catalog capture and atomic write bytes. Hard capture or batch
refusal commits nothing; a catalog-budget frontier can commit complete retirement
units preceding that boundary. Records retire completely; parent manifest ownership
can be drained over subsequent passes without making a surviving subset
downloadable as a complete file. Reports expose visited/protected counts,
remaining touched-parent manifest references and an explicit traversal frontier.
Logical reference retirement does not certify physical bytes freed or peak RSS.

The daemon now invokes this primitive through the pressure worker below.
Forced native compaction and
sustained physical reclamation qualification remain unfinished.

Temporary-store scenarios cover native RDF/JSON/text and exact-byte survival for
record pins and shared occurrences, stable retries after reopen, whole-artifact
pin protection and expiry, finite manifest cleanup, scan frontiers, atomic
capture/write refusals, and released records whose parent catalog has expired.
The shared corrupted-time-index fixture also checks that a failure after preparing
an earlier retirement commits no successful prefix.

Implementation is in progress in `/home/paudley/Active/sparrow`, on
`implementation/helper-census`. Full architecture acceptance remains unmet.
The current source implements schema 26; the schema sections
below retain their historical qualification evidence.

## Schema 26 artifact membership

Artifact discovery admits the existing store-scoped live/evidence/pin vocabulary
through typed `ArtifactListQuery.selection`, CLI `artifacts --scope-file FILE`
and MCP `artifacts_list.scope`. One reference owner serves record queries and
artifact discovery, preserving whole-artifact and recordless identities alongside
deduplicated records and parents. Source/receipt filters run after full reference
validation. Selected reads seek their metadata instead of scanning unrelated
artifacts and records. Live scans and selected seeks share one metadata collector,
and artifact/source capture and explicit reference expansion share one aggregate
catalog admission law. The existing native pinned-value decoder now admits each
caller before parsing, with one implementation for both bounded lookup paths.

Record-only selection counts retained occurrences within the captured membership;
complete parent availability and original member counts remain separate facts.
An expired whole artifact is refused even when its individual records are pinned.
A retained record can expose a reclaimed parent with unknown original metadata.
Cursors preserve the original membership and counts after pin release/expiry;
fresh reads validate current references. Artifact diagnostic/source-array scope
filters remain unfinished and are explicitly refused rather than ignored. The
artifact tool schema advertises membership only; record query filters retain
their existing shared decoder/encoder and behavior.

Focused core scenarios verify shared record-query membership, recordless parents,
overlap deduplication, selected-reference validation before filters, sparse native
seeks despite unrelated corrupt catalog entries, admission refusal with no prefix,
and frozen pages after pin release and physical catalog retirement. A real test
daemon verifies generated wire selection, CLI/MCP equal replay, scoped counts,
foreign-store/missing/released-reference and changed-membership refusals, filter
rejection and obsolete schema refusal.

Local qualification on 2026-10-07: `make check` passed formatting, release Clippy
with warnings denied, all workspace release tests (108 storage and 31
RPC/daemon/MCP scenarios), seeded census self-tests and the pinned cross-workspace
gate. All 155 ownership jobs hold with zero distinct exceptions. Evidence is
`/tmp/sparrow-artifact-scope-first-check-20261007.log`.

The current client refused status, keys, sources and artifacts against a private
archived schema-25 daemon with `UNSUPPORTED`, exit 1 and zero stdout bytes.
Evidence is `/tmp/sparrow-artifact-scope-schema25-compat.as360qne`; the archive
digest is `51145f2be6894f887324d63f8d1d9c99a2d53b90aef9c3a737647cafefdc336d`.
That owned child exited cleanly under SIGINT and removed its socket. Development
runtime adoption remains to be qualified.

## Schema 25 source discovery

`sourcesList`, CLI `sources STORE_ID` and MCP `sources_list` use one source
capture and one generated client/renderer. The exact intake key is `source_id`;
its separate `rdf_iri` shares the native-hash formatter used by occurrence RDF
publication. Retained complete artifacts, fragment parents and record occurrences
are counted separately. Mapping-only and configured watch-only sources remain
visible with zero retained input. Optional mapping IDs preserve absence;
watch declarations come from the daemon's bounded watch owner. Continuations
preserve captured definitions even after ingestion, evidence retirement or pin
release. Producer continuity is explicitly unknown; stream generations and a
full gap ledger remain unfinished.

The artifact eligibility walker, catalog leases, receipt wire codec, receipt
renderer and CLI catalog options each have one implementation. Source handles
are shared across store clones, bounded to eight and 8 MiB, with a 2 MiB capture,
256 KiB conservative page ceiling and an absolute 60-second lease. Capture scans
pins/artifacts/records and declared profiles under one aggregate scan/time/byte
admission. Typed status exposes source metadata handle use and ceilings.

Focused core scenarios verify exact occurrence counts, native RDF identity and
source scope matching, mapping-only and idle watch sources, fragment survival,
frozen pages after pin release/retirement, corrupt declarations and scan refusal
without a prefix. A real test daemon verifies generated wire replies, runtime
watch ownership, CLI/MCP equal continuation, later-ingestion isolation, optional
profile IDs and explicit store/schema/selector/budget refusals. The additional
source admission scenario verifies the shared eight-handle ceiling, exact
512-byte control-character intake keys, sorted watch revisions and lease expiry.

Local qualification on 2026-10-07: `make check` passed formatting, release Clippy
with warnings denied, all workspace release tests (104 storage and 30
RPC/daemon/MCP scenarios), seeded census self-tests and the pinned cross-workspace
gate. All 152 ownership jobs hold with zero distinct exceptions. Evidence is
`/tmp/sparrow-source-wire-final-check-20261007.log`.

Schema-24 compatibility evidence is
`/tmp/sparrow-source-schema24-compat.hpcbmqzd`: the current client refused status,
keys, sources and artifacts with `UNSUPPORTED`, exit 1 and zero stdout bytes
against a private owned archived daemon. Its archive digest is
`d2fa1260d05c84a67e0ea1fda0a11ec7851a82fdd75593f3045a4301090a50ff`;
the child exited cleanly under SIGINT and removed its socket. There is no fallback
to the old contract.

Signed implementation `f3b9be3` passed its normal full verification hook. The owned
schema-24 development unit exited successfully under SIGINT, with PID zero and
socket removed, before the same unit/config/store reopened under schema 25 at
2026-10-08 02:01:11 UTC (observed PID 1048879). Store identity remains
`store:46c88e949ccec658600b66c7becd5bb7`, format `sparrow.store/16`, with 195 live
records and no pending writes. Directory/socket permissions remain 0700/0600.

Live evidence in `/tmp/sparrow-source-wire-live.kmsu0b54` verifies all 16 source
pages, CLI/MCP equal continuation, mapping IDs matching their get operations,
exact source keys accepted by scoped key discovery and artifact listing, and
store/budget/changed-selector refusal with exit 1 and zero successful stdout.
The diagnostic source has two complete artifacts and three retained records;
the configured development watch source has one complete artifact and one
retained record, plus watch ID `development-completed`, revision 1, enabled true.
All sources explicitly report unknown producer continuity. MCP advertises 23
implemented tools and marks source discovery read-only. The captured source
handles obey the eight-handle ceiling and subsequently expire to zero.

Regression evidence in `/tmp/sparrow-source-diagnostic-regression.m5n3m2lj`
preserves default/strict diagnostic coverage, native text/context behavior,
CLI/MCP equivalence and the exact 104-byte stable retry export.
`/tmp/sparrow-source-raw-regression.jn5gofsa` preserves the descriptor and
completed handoff identities, exact 82/87-byte exports, durable removed phase
and the producer's replacement file. These checks preserve all 195 live records.
Full architecture acceptance, stream generations/gap tracking, broader
live/pin/selection pagination and governed SPARQL surfaces remain unfinished.

## Schema 24 typed artifact pages

`artifactsList` connects the shared catalog reader below to the private Cap'n
Proto daemon. CLI `artifacts STORE_ID` and MCP `artifacts_list` use one generated
client and renderer. Receipt-time selection is an explicit unfiltered/interval
union with typed `Instant` bounds. The request declares the current discovered
schema; obsolete/absent versions and foreign stores are refused before capture.
Source/cursor lengths are checked before owned copies. Status carries typed
artifact handle/byte counts, ceilings and lease duration. Pin/artifact receipt
wire encoding and agent rendering each have one implementation.

Local qualification on 2026-10-07: final `make check` passed formatting, release
Clippy with warnings denied, all workspace release tests (100 storage and 29
RPC/daemon/MCP scenarios), seeded census self-tests and the pinned cross-workspace
gate. All 149 ownership jobs hold with no distinct exceptions. Evidence is
`/tmp/sparrow-artifact-wire-final-check-20261007.log`. The real-daemon scenario
verifies immutable continuation after later ingestion, CLI/MCP equal replay,
explicit unknown origin after parent reclamation, retained record counts,
strict unknown-origin receipt filtering and no successful rows on resource,
store, schema or changed-scope refusal. MCP exposes 22 implemented tools and marks
artifact listing read-only. Existing pin pages pass through the shared codecs.

A private schema 23 daemon captured from the owned development process (SHA256
`57f3a26268b16b811c79c0e68d3e47cba91018a134e3b7891a00002743f988de`)
was refused by the schema 24 CLI for status, keys and artifact listing with
`UNSUPPORTED`, exit 1 and zero stdout bytes. Evidence is
`/tmp/sparrow-artifact-wire-compat.39mlsatd`; the private child stopped cleanly on
SIGINT and removed its socket. No HTTP transport, runtime dependency, store
format or PurRDF pin changed. Full architecture acceptance remains unmet.

Verified development adoption on 2026-10-07: signed functional commit `8cd62a0`
passed the additive pre-commit gate. The owned schema 23 unit stopped cleanly on
SIGINT and removed its socket; the same unit/config/store reopened under schema
24 (observed PID 984327). Store identity remains
`store:46c88e949ccec658600b66c7becd5bb7`, format `sparrow.store/16`, with 195 live
records and no pending writes. Directory/socket permissions remain 0700/0600.

Live evidence in `/tmp/sparrow-artifact-wire-live.thqjmlxm` verifies two immutable
artifact pages for `acceptance-diagnostic-20261007`, CLI/MCP equal continuation,
typed receipt-time selection, exact retained counts (three records), original
104/240-byte lengths and explicit scoped refusals. Typed status reports the one
retained catalog handle and its eight-handle ceiling. Existing default/strict
diagnostic coverage and text/context paths remain passing. Stable artifact,
descriptor and handoff retries preserve their identities and exact 104/82/87
bytes; the handoff producer replacement remains preserved. Full architecture
acceptance, further artifact scopes and source discovery remain unfinished.

## Schema 23 bounded typed discovery

`discover` carries typed store/schema/profile identities, upload/event ceilings
and bounded durability state. SDK connections and agent query admission use it
instead of scanning operational status. Discovery work does not grow with the
retained evidence catalog; lock contention can still delay it. Explicit status
retains measured catalog and physical usage. Discovery is not a health check:
corrupt evidence still fails status and affected queries with `STORAGE_FAILURE`.
The wire change leaves store formats unchanged.

Local qualification on 2026-10-07: `make check` passed formatting, release
Clippy with warnings denied, workspace release tests (95 storage and 28
RPC/daemon/MCP scenarios), census self-tests and the pinned cross-workspace
gate. All 145 ownership jobs hold, with no distinct exceptions. The shared
native catalog corruption fixture lives in one dev-only test support crate;
both dependency edges are dev-only. Store properties and status share their
immutable metadata owner; status and discovery share the durability wire codec.

A private schema 22 daemon was captured from the owned development process
before adoption (SHA256
`3108104465a2ffa04a67b7c7f3446f0780bca3183623f1182700bae1215ab909`).
The current CLI refused `status`, `keys` and `records` with `UNSUPPORTED`,
exit 1 and zero stdout bytes, without falling back to status. Evidence is in
`/tmp/sparrow-discovery-compat.2_glnuku`; the private child exited cleanly on
SIGINT and removed its socket. Full query leases/snapshots/pagination remain
unfinished.

Verified development adoption on 2026-10-07: signed functional commit `0790356`
passed the additive pre-commit gate. The owned schema 22 development unit stopped
cleanly on SIGINT and removed its socket; the same unit/config/store reopened
under schema 23 (observed PID 883173). Store identity remains
`store:46c88e949ccec658600b66c7becd5bb7`, format `sparrow.store/16`, with 195 live
records and no pending writes. Directory/socket modes remain 0700/0600.

Live evidence in `/tmp/sparrow-discovery-live.p0d031bl` verifies CLI/MCP equal
proven rows and unknown coverage, incomplete native text/context coverage,
strict `INDEX_NOT_READY` refusal, and the original 104-byte artifact retry/export.
Descriptor and handoff retries retain their original identities and exact 82/87
bytes; the producer replacement remains preserved. The acceptance harness's
final record-count assertion initially compared its decimal string with an
integer; checking the documented string representation passed without another
ingest or daemon restart. Full architecture acceptance remains unmet.

## Schema 22 diagnostic scope coverage

The shared diagnostic membership owner distinguishes certain matches, certain
exclusions and unknown membership. Default JSON/text/record/context reads return
proven evidence and coverage over visited live source/time members. A known
dimension mismatch proves exclusion despite another unavailable dimension.
Otherwise missing, ambiguous or unprofiled dimensions contribute to unknown
membership. Context anchors remain strict; neighbors share default/strict policy.

Eligible and qualified totals use a typed known/unknown union; agent renderers
preserve unknown as `null` and expose known lower bounds separately. One core
codec serves all coverage receipts, including both context directions. Incomplete
membership is also propagated into event/text corpus coverage. Key counts and
scores describe the covered membership, not unknown records. Complete membership
scans retain exact totals when only output rows are limited.

`require_complete_index` is accepted across all CLI/MCP query surfaces. Applicable
JSON projection failure or undecidable diagnostic membership refuses strict
reads with `INDEX_NOT_READY`; genuine storage failures remain hard errors in
either mode. Existing record-query strict controls feed the same core policy.
No new parser, scoring law, or persisted evidence representation is introduced.

Queries now declare `Scope.schemaVersion`; the daemon refuses older/absent
versions before encoding changed count fields. Every agent query first checks
native discovery. At schema 22 that discovery still scanned status; schema 23
replaces that admission path above. Before the verified
adoption below the development unit ran schema 21.

Local qualification on 2026-10-07: `make check` passed formatting, release Clippy
with warnings denied, all workspace release tests (94 storage and 27 RPC/daemon/
MCP scenarios), seeded census self-tests and the pinned cross-workspace gate.
All 143 ownership jobs hold with no distinct exceptions. Core scenarios exercise
known mismatches with another unknown dimension, default proven rows and unknown
totals across JSON/text/combined/context reads, strict refusal, uncertain context
anchors, pin retention/reopen, complete totals with an output row limit,
incomplete traversal totals and missing captured-profile storage failures.
All five query surfaces exercise identical CLI/MCP coverage and strict refusal.
Generated requests with absent or schema-21 version declarations refuse before
query execution. Native column-family corruption fixtures share one dev-only
open helper instead of repeating the storage setup.

An independently archived schema-21 CLI (SHA-256
`1ee26e7eb898513eeab8cd3cb896a9361610628d510843b89e6a049baa93dea6`)
refused unfiltered key/text reads and a filtered key read against a private
schema-22 daemon: `UNSUPPORTED`, exit 1 and zero stdout bytes. The current CLI
returned one proven member, one unknown member, a null eligible total and a
known lower bound of one. The owned daemon shut down with SIGINT, exit 0 and
socket cleanup. Evidence is under `/tmp/sparrow-coverage-compat.btajhgee`.

The owned development unit adopted signed commit `d319902` after SIGINT shutdown,
MainPID 0, successful result and socket cleanup. PID 850878 now serves schema
22 at the same private socket, directory 0700/socket 0600, on format 16 and store
`store:46c88e949ccec658600b66c7becd5bb7`. One synthetic record with a missing
mapped severity was durably accepted as
`artifact:88c0813c1febf6bd2a103c26f669701d`, retaining all 104 original bytes.
Default combined reads through CLI/MCP agree on the existing proven worker
occurrence, one certainly excluded occurrence and one unknown member. Eligible
and qualified totals are null; their known lower bounds are one. Native text
corpus and context receipts also report incomplete membership. Strict combined
reads refuse with `INDEX_NOT_READY`, exit 1 and zero stdout. Exact-byte export
and stable ingestion retry passed. Status reported 195 live records and no
durability backlog.

Historical descriptor evidence remains unprofiled with an exact unfiltered
count of one. Its original ingestion retry and the immutable handoff retry
retained their artifact identities; independent byte comparisons passed for
all 82 descriptor bytes, all 87 handoff bytes and the replacement producer file.
Evidence is under `/tmp/sparrow-coverage-live.d53na_mw`. This is owned transient
development acceptance. Full SPARQL admission, graph correlation/explanations,
active tailing/syslog, complete read leases/pagination, permanent packaging and
whole-corpus resource/retention qualification remain unfinished.

## Schema 21 diagnostic mappings and filters

Immutable source profiles map service, host, severity and trace/span dimensions
to concrete native JSON pointers. One capture owner uses PurRDF's retained ordered
occurrences and decoded strings; timestamp interpretation shares its unique
mapped-occurrence selector. Native pointer parsing and RDF construction remain
PurRDF functions. No field name, severity case or trace syntax is inferred.
The direct and staged publication paths share the same record preparation owner.
Staged checkpoints bind profile presence or absence before the first record and
retain that decision through reopen and later source definitions.

Known dimensions retain decoded value, native occurrence, immutable profile and
original string-body span. Unknown dimensions retain explicit reasons; duplicate
fields stay ambiguous even if equal. Diagnostic graphs use independent per-record
owners and the existing dictionary/index, scoped view and retirement paths.
Inspection reports their persisted primary rows separately from metadata/content.

All query surfaces AND diagnostic dimensions with source/time/membership and OR
their bounded alternatives. Unknown filtered evidence causes `INDEX_NOT_READY`
with no successful prefix; partial diagnostic-scope coverage/counts-unknown
responses remain unfinished. CLI and MCP share mapping/filter codecs and typed
operations. Schema discovery refuses older endpoints. Source and diagnostic
alternatives now share the concrete `QueryAlternatives` admission owner.

The storage fence advances basic/consumption/watch/handoff formats to 13/14/15/16.
Legacy 9/10/11/12 retain their completed recordless index and store identity while
durably advancing before serving. Historical absent diagnostic snapshots remain
unprofiled; later mappings do not rewrite them. Legacy stores with unfenced
diagnostic registry state refuse startup. Before adoption below the development
unit ran schema 20/format 12. Full governed SPARQL,
graph-layer selection/correlation, tailing/syslog, physical reclamation and the
remaining full architecture qualification are still unfinished.

Local qualification on 2026-10-07: `make check` passed formatting, release Clippy
with warnings denied, all workspace release tests (93 storage and 27 RPC/daemon/
MCP scenarios), seeded census self-tests and the pinned cross-workspace gate.
All 140 ledger jobs hold with zero distinct exceptions. The scenarios cover
native decoded escapes and original body spans, ambiguous equal duplicate
fields, immutable staged-import absence across reopen, diagnostic RDF pin
retention and retirement, all five query surfaces through CLI/MCP, and each
legacy storage authority upgrade. Ranked combined results carry the same
snapshot as chronological results.

An independent private executable check created store
`store:7d736e9327c4ac9cf4a37831e7dc6bbe`, format 15/schema 21. The archived
schema-20 executable (SHA-256
`795b2e312a03075d0bfef5cc8341246647cc38fd8d1d1cb419489f2a7f974d2f`)
refused it with `UNSUPPORTED`, exit 1. The current executable reopened the same
identity and recovered the identical mapping and durable artifact retry receipt.
Both owned current processes shut down with SIGINT, exit 0 and socket cleanup.
Evidence is under `/tmp/sparrow-diagnostic-writer-fence.lbr4q7qy`. This verifies
writer refusal and reopen, not full architecture acceptance.

The owned development unit adopted signed commit `5f429a6` after SIGINT shutdown,
MainPID 0, successful unit result and socket cleanup. PID 802658 now serves schema
21/format 16 at the existing private socket, directory 0700/socket 0600, and store
`store:46c88e949ccec658600b66c7becd5bb7`. Historical descriptor evidence remains
explicitly unprofiled. A new synthetic source defined five mappings before
publishing two JSONL records as `artifact:a35c460d343822b6ef25bd68cb5be04f`.
Combined service/severity/trace filters returned exactly
`record:2b5f999b12f0d4e25a74fe850d832114` through CLI and MCP; native text retrieval
returned the same occurrence and RDF inspection reported 40 diagnostic rows.
The complete 240-byte export matched the input including its escape and CRLF.
Idempotent mapping/ingestion retries preserved their identities. Status reported
194 live records, no durability backlog and the same watch authority.

Original descriptor and immutable handoff retries also retained their artifact
identities. Independent byte comparisons verified all 82 descriptor bytes, all
87 handoff bytes and the preserved replacement producer file. Outputs are under
`/tmp/sparrow-diagnostic-live.nvzlvt27`. This qualifies the owned transient
development deployment; permanent packaging, full architecture acceptance and
the remaining features listed above are unfinished.

## Schema 20 source alternatives

All five query surfaces use one store-owned source predicate: OR across bounded
scope `filter.source_ids`, AND with the outer exact source, receipt interval and
live/selection/pin membership. Key/value discovery and combined/native-text
queries filter before qualification or ranking; context refuses an excluded
anchor and keeps same-source neighbors. Source spelling and occurrence identity
remain unchanged. Present empty or oversized arrays fail rather than widening
scope. The immutable `SourceFilter` type owns admission; native PurRDF remains
the JSON decoder and text/field semantic owner.

This changes the wire contract, not format-12 storage. CLI and MCP require current
native discovery for filtered requests so old daemons cannot ignore the new
field. This currently adds the existing status scan, whose scan time remains
unbounded; no bounded handshake or query snapshot is claimed. Diagnostic
normalization/filter dimensions and the remaining architecture requirements are
still unfinished.

Local qualification on 2026-10-07: `make check` passed formatting, release Clippy
with warnings denied, all workspace release tests (91 storage and 26 RPC/daemon/
MCP scenarios), seeded census self-tests and the pinned cross-workspace gate.
All 131 ledger jobs hold with no distinct exceptions. Source tests exercise OR,
duplicate alternatives, exact-source AND, half-open receipt times, pin-retained
old evidence, unavailable membership before filtering and source-local context.
The daemon integration scenario covers all five surfaces through both CLI and
MCP for live/selection/pin scopes, plus direct generated requests and invalid
array admission. A private archived schema-19 daemon independently refused a
filtered CLI read with `UNSUPPORTED`, exit 1 and zero stdout bytes. Its binary
SHA-256 is `bf9d90750fea6a1cef4ad787625d84677eaccb7936285f9a11110a0d7765fa62`;
evidence is under `/tmp/sparrow-source-filter-old.c_j3gbdi`. That owned private
daemon shut down with SIGINT, exit 0 and socket cleanup.

The owned development unit adopted signed commit `b2d751d` after graceful SIGINT
shutdown, MainPID 0, successful unit result and socket cleanup. PID 727274 now
serves schema 20 on the existing private socket and reopened the same format-12
store with 192 live records. A live scope containing `acceptance-descriptor`, an
absent source and duplicate `acceptance-descriptor` returned exactly the original
descriptor occurrence; adding an outer nonmatching source returned no rows. MCP
returned identical filtered rows to CLI. Descriptor and immutable handoff retries
kept their original identities; independent `cmp` checks passed for all 82
descriptor bytes, 87 handoff bytes and the preserved replacement producer file.
Evidence is under `/tmp/sparrow-source-filter-live.uzLVKl`. This remains transient
development acceptance, not permanent deployment or full architecture completion.

## Streaming evidence catalog reads

Status counts now stream the record and pin catalogs through one native cursor
and one decoded row at a time. They no longer allocate a vector containing all
resident records or all pin envelopes. Eligible record/artifact/pin listings and
pin-name lookup share the same iterator; explicit listing APIs still allocate
their requested result vectors. Pin lookup completes its scan and propagates
later corruption, even after finding a matching name.

The shared decoder validates catalog key/identity agreement before returning
evidence. Pin protection capture uses that same identity owner after its existing
admission checks. Malformed rows and native iterator errors remain hard failures;
filtered or counted reads never turn them into omitted rows or partial success.
Counts remain exact for the captured UTC instant under the writer lock. This
changes no wire schema, storage encoding or lifetime law.

Qualification on 2026-10-07: `make check` passed release Clippy with warnings
denied, all workspace release tests (including 89 storage scenarios and 25
RPC/daemon/MCP scenarios), seeded census self-tests and the pinned cross gate.
All 129 helper ownership jobs hold with no distinct exceptions. New storage
scenarios cover exact counts across retention, pin release and reopen, and hard
failures for corrupted record/artifact/pin identities. This is local source
qualification.

The owned development unit adopted signed commit `bf0260d` after graceful SIGINT
shutdown and socket cleanup. PID 701553 reopened the same store
`store:46c88e949ccec658600b66c7becd5bb7`, format 12/schema 19, with 192 live
records, 193 payload chunks, 188 JSON projections, no pins and no durability
backlog. The socket remained mode 0600 in its mode-0700 directory. The first
connection raced socket creation; the same unit then became ready without a
second start. Descriptor ingestion and immutable handoff retries retained their
original artifact identities and durable receipts. Independent `cmp` checks
verified all 82 descriptor bytes, all 87 handoff bytes including CRLF, and the
preserved replacement producer file. The stdio MCP status adapter also passed
through the typed socket client. Captured outputs are under
`/tmp/sparrow-stream-catalog-live.J3qPAA`. This remains a transient development
unit, not permanent/reboot or corpus-scale qualification.

This removes resident catalog vectors; it does not prove a bounded count-scan
duration, bounded aggregate pin protection, corpus-scale RSS or latency. Those
requirements remain unfinished. Forced physical reclamation also remains unmet
for the concrete safe-binding gaps in
[native reclamation integration](native-reclamation.md).

## Schema 19 recordless artifact eviction

Empty JSONL and log artifacts have no addressable records but retain catalog
and retry metadata. They now publish one native `empty-time:` entry in the same
atomic batch as their artifact and acceptance receipt. Direct and staged upload
paths use one index/key owner. Ordinary metadata expiry deletes this index entry
in the same batch as its artifact; publication does not fabricate a record for
an empty input. A recordless artifact with nonzero bytes or payload references
is a hard ownership failure.

Pressure eviction merges the existing record timeline with this index through
two native peeking cursors, ordered by `(received_at, evidence_id)`. Pins protect
recordless artifacts exactly as other complete artifacts. The traversal cursor
uses the common timestamp/identity suffix; either index's earlier publication
invalidates it, and new publication after exhaustion starts directly at the new
candidate. Record queries and context continue to use their record-only index.
Each new record or recordless artifact consumes one retirement unit. Schema 19
names that frontier `unitLimit`, exposes `recordlessArtifacts` in the typed
last-pass report, and adds the session's cumulative `evictedRecordlessArtifacts`
to pressure status. CLI and stdio MCP use the same renderer.

A retired recordless artifact has no byte dependencies; its catalog and index
are removed atomically, while original acceptance/import retry receipts retain
their stable identities. Retrying the request cannot resurrect its catalog or
bytes. Native disk recovery remains separately measured and required before
further pressure retirement; catalog deletion alone does not prove space freed.

Legacy store formats 5, 6, 7 and 8 upgrade to 9, 10, 11 and 12 respectively,
preserving the existing basic/consumption/watch/handoff writer fences and watch
registry presence checks. Startup fences older writers before building the new
index. Each durable upgrade page inspects at most 512 artifact entries and
admits 32 MiB of raw/decoded catalog capture. Populated record membership is
inspected through PurRDF's native JSON event reader without materializing its
record-ID list. Complete index pages and their checkpoints publish atomically;
reopen resumes the checkpoint. Startup remains unavailable until the required
index is complete. Missing, malformed or unfenced index state fails explicitly;
unsupported writers cannot reopen an upgraded store. Upgrade does not change
artifact IDs, receipt times, content, query projections or watch authority.

Qualification covers mixed record/empty-artifact receipt order, protection and
release, direct and staged retries without resurrection, earlier publication
and publication after exhaustion, admission refusal, corrupt index rollback,
all four legacy writer-fence upgrades, durable page/checkpoint resumption after
reopen, and native duplicate-member validation with a 100,000-member populated
manifest. Real-daemon pressure acceptance retires an empty artifact and an
unpinned record while preserving pinned bytes, and compares cumulative counters
through CLI and stdio MCP. The full gate passed with 87 storage scenarios,
eight core units, three server units, 25 RPC/daemon/MCP scenarios, all remaining
workspace tests and 128 ownership jobs without distinct exceptions or
unsanctioned duplicate groups. Physical reclamation and corpus-scale RSS/latency
acceptance remain unfinished.

A separate owned private-store acceptance archived the pre-upgrade daemon
(schema 18 binary SHA-256
`0a60c01b63aa832ae364d04ea809dc399dd6df841b12c0c20985441d1f3f1d67`).
The new daemon accepted an empty input and stopped gracefully. The archived
daemon refused that store with `UNSUPPORTED` rather than opening it under the
old writer contract. The new daemon then reopened with the same store identity
and original empty-artifact receipt. Config, native store and rejection output
are under `/tmp/sparrow-recordless-writer-fence.gabyjuhl`.

At 2026-10-07 23:08 UTC, the owned development unit was gracefully cut over
to implementation commit `179f386`, main PID 615744. The existing format 8
store upgraded to format 12 and served schema 19 on the same private
`development-v5/rpc.sock`. Its persistent identity remains
`store:46c88e949ccec658600b66c7becd5bb7`; the reopened session is
`session:f0edf1fe3556a7d16580670e6e0eb8f5`. Status retained 192 live records,
193 payload chunks and 188 JSON projections, with no pins or pending durability.
Pressure remained ready with zero retirement counters. The synthetic watch
remains covered at revision 1, with no pending claims; directory/socket modes
remain 0700/0600.

Descriptor and completed-handoff retries returned their original artifact/import
identities. Independent comparisons passed for the 82-byte descriptor and
87-byte CRLF handoff exports; the replacement producer pathname remained intact.
Live stdio MCP reported schema 19, format 12, the same store and 192 live
records. Exports and MCP output are under `/tmp/sparrow-recordless-live.Pua8sL`.
This verifies actual legacy-store cutover and retained-evidence regressions,
without claiming sustained pressure, live producer intake, reboot packaging or
complete architecture acceptance.

## Bounded catalog expiry

Ordinary expiry no longer collects the artifact, record and pin corpus or
constructs one corpus-sized native write batch. `Store::expire_bounded` uses
native ordered seek over artifact, record, pin and request catalogs. Its
session-local cursor resumes complete safe prefixes, and restarts after a
completed traversal or reopen. New entries behind a position and released pins
are revisited on the next traversal. Every pass recaptures current protection
under the same serialized writer lock; progress never grants stale deletion
authority. Eligibility continues to use the current server instant, so logical
expiry does not wait for the cleanup cursor.

The existing retirement admission limits bound visited catalog entries, active
pin capture, dependency units, payload ownership references, conservative decoded
catalog shapes and final atomic write size. Complete artifact manifests drain
across passes. Individual records retire atomically through the same JSON, RDF,
text and payload owner used by pressure eviction and abandoned preparation.
An indivisible record or catalog entry that exceeds a fresh allowance fails
explicitly. Hard decode, ownership and native batch failures publish no prefix
and do not advance the cursor. Empty artifact catalogs are removed after their
ordinary metadata grace period even though they have no records or payloads.
Active consumption and handoff request receipts retain the existing ownership
law rather than being discarded as ordinary expired retries.

The daemon runs one finite pass each second; it does not hold the catalog writer
through a loop draining the corpus. `Store::expire` now invokes the same bounded
implementation with default limits and returns that pass's retirement-unit
count. Callers needing traversal evidence use the explicit frontier and
continuation in `ExpiryReport`. This changes no wire schema or store format.
Native compaction, bounded individual manifest encoding and sustained physical
reclamation qualification remain unfinished.

Qualification covers bounded manifest drainage with shared bytes and a record
pin; release and expiry of protection between pages; empty artifact cleanup;
reopen and fresh-publication traversal; atomic batch/catalog admission refusal;
and a real private-socket daemon draining 70 expired records across multiple
passes while preserving one pinned occurrence and its exact bytes. The full
gate passed with 83 storage scenarios, six core units, three server units,
25 RPC/daemon/MCP scenarios, all other workspace tests and 125 helper ownership
jobs, with no distinct exceptions or unsanctioned duplicate groups.

The initial full gate encountered the previously recorded nonblocking claim
lock failure in parallel recovery acceptance. A local native probe verified that
descriptor close can leave an exclusive `flock` held by an inherited open file
description. Completed-consumption ownership now releases the native lock
explicitly when its guard is dropped, including failures during subsequent root
initialization. Acquisition remains nonblocking and exclusive; release failures
are reported. A deterministic unit retains a duplicated descriptor across owner
release, proves immediate replacement acquisition, and proves that dropping the
old descriptor does not disturb the replacement's ownership. The subsequent
full gate passed. This verifies that lifecycle rule without claiming which
particular child inherited the descriptor in the earlier parallel failure.

At 2026-10-07 22:38 UTC, implementation commit `5b3005d` was gracefully cut
over on the owned development unit. Its main PID is 179150 and the reopened
session is `session:cc93cc2d988d7594ff1a433b132bf3da`. The same private Unix
socket serves schema 18; persistent store identity and format remain
`store:46c88e949ccec658600b66c7becd5bb7` and `sparrow.store/8`. Status reported
192 live records, 193 payload chunks, 188 JSON projections, no pins or pending
durability, ready pressure state and the covered synthetic watch at revision 1.
Directory/socket modes remain 0700/0600.

The saved descriptor and completed-handoff retries returned their original
artifact/import identities. Independent byte comparisons passed for both
82-byte descriptor and 87-byte CRLF handoff exports, and the replacement source
pathname remained intact. Live stdio MCP status reported the same store,
schema 18 and 192 live records. Exports and MCP output are under
`/tmp/sparrow-expiry-live.yV2yG5`. This proves cutover and retained-evidence
regressions; the temporary-store and owned-daemon scenarios above establish
bounded expiry behavior. It does not qualify sustained pressure, arbitrary live
producer intake, reboot packaging or complete architecture acceptance.

## Schema 18 bounded eviction traversal

Eviction retains one session-local position shared by manual core calls and the
daemon's pressure worker. Scan and catalog budgets advance past a complete safe
prefix. A partial parent manifest resumes at the same retired record until its
remaining ownership is drained. Hard storage failures and atomic batch admission
refusal leave the position unchanged. A single required catalog entry that cannot
fit still fails explicitly; it is never skipped to retire younger evidence.

Every pass captures current active pin membership through the existing pin
owner. Changed effective membership, including release and expiry, restarts at
the oldest entry. Renewal preserving the same protection continues the existing
position. Successful time-index publication is observed through RocksDB's native
borrowed write-batch visitor, without decoding a second batch format or copying
payload bodies. An insertion at or before the current position restarts traversal,
so a backwards receipt-clock step cannot hide newly accepted older evidence.
Publication after an exhausted traversal supplies the next candidate directly;
unchanged exhaustion does not repeatedly scan the same protected corpus.

The cursor holds a bounded time key and at most one admitted pin-membership set.
Each current capture remains independently admitted; retained and fresh membership
shapes are each limited by the 32 MiB conservative catalog allowance. This is
internal shape accounting, not a measured RSS guarantee. Reopening discards this
position safely and starts a fresh chronological pass.

Schema 18 exposes a `catalogBudget` logical frontier and a `continuation` boolean
in the typed last-pass report. The pressure worker stays requested when a safe
scan prefix remains, rather than declaring the corpus blocked at its first page.
Physical recovery still gates additional destructive passes after retirement.
The CLI and stdio MCP use the same generated contract and pressure renderer.

Qualification covers pressure progress through a twelve-record protected prefix
with a two-entry scan budget and renewal between passes; release/expiry exposing
an older record; earlier publication; publication after exhaustion; bounded
catalog continuation; native default-family batch observation and invalid-key
refusal; and typed catalog-frontier rendering. The full gate passed with 79
storage scenarios, six core units, two server units, 24 RPC/daemon/MCP scenarios,
all other workspace tests and 123 helper ownership jobs with no distinct
exceptions or unsanctioned duplicate groups.

At 2026-10-07 22:17 UTC, the owned development daemon was gracefully reopened
on implementation commit `e19ce63` and served schema 18 on the same private
`development-v5/rpc.sock`. Store identity remains
`store:46c88e949ccec658600b66c7becd5bb7`, format `sparrow.store/8`, with 192 live
records. The new session is `session:f64e79a8431598645ffb3852bddcb1d2`;
pressure status remained ready, without requested capacity or retirement.
Directory/socket modes remain 0700/0600 and the sole synthetic consume watch
remains covered at revision 1, without pending claims.

The saved descriptor retry returned `artifact:d066e0d527e9e82ec11e13388b386c13`
and its 82-byte export matched the original. The saved completed handoff again
returned its original removed import and `artifact:eec8eedea8b05f3d9512e88ff8ddd61f`;
its 87-byte CRLF export matched the source, and the replacement producer pathname
remained intact. Live stdio MCP status reported schema 18 and the same persistent
store and record count. Acceptance exports and MCP output are under
`/tmp/sparrow-continuation-live.HVXiqO`. This verifies cutover and retained-evidence
regressions, not sustained pressure or reboot/deployment qualification.

## Schema 17 pressure maintenance

Direct record ingestion and staged import writes share one measured physical
admission law. A prepared batch is refused before publication when its estimate
would cross the disk budget minus maintenance reserve. The smallest outstanding
refused estimate requests reclamation; a batch that cannot fit the entire data
allowance returns `RESOURCE_LIMIT` without requesting eviction. Buffered WAL
queue pressure and administrative control-write refusal are separate signals.

The daemon schedules one pressure pass per second, serialized with ingestion and
pin mutations. It captures native flush/compaction and snapshot observations,
uses the shared bounded oldest-unpinned retirement path, and requests nonwaiting
native flushes of both families. Automatic RocksDB compaction and blob GC use
the configured two background jobs. Missing native properties, background errors
and flush scheduling failures are actionable errors, retained in pressure status.
The worker does not manufacture a successful physical reclamation receipt.

After retirement, another destructive pass waits for native work and read
snapshots to finish and for measured physical bytes to fall below its preceding
observation. Writes continue to check their own complete estimate. Status and
pin release remain available under data pressure. A reopened over-budget store
requests maintenance from its current footprint; pressure counters and refused
estimates are session-local observations, not durable producer retry receipts.

Typed status exposes ready/requested/awaiting-reclamation/blocked phase,
requested bytes, pass and retirement totals, native activity and snapshot counts,
the last explicit logical frontier, and any maintenance failure. CLI and stdio
MCP share one renderer. Temporary-store and real-daemon acceptance use owned
ballast to force admission refusal and then remove it to prove capacity-based
retry. That does not qualify forced compaction, sustained physical reclamation,
peak RSS or the complete pressure architecture. A stalled native reclamation
cycle remains visible and backpressures writes instead of repeatedly deleting
additional evidence.

The full workspace gate passed with 75 storage scenarios, 24 real-daemon/RPC/MCP
scenarios, all SDK and other workspace tests, release Clippy with warnings denied,
seeded census self-tests and 122 helper ownership jobs with no distinct exceptions
or unsanctioned duplicate groups. This qualifies the admission/worker/status
integration described here, not the remaining full-architecture acceptance.

On 2026-10-07 at 21:55 UTC, the owned development service was gracefully reopened
on implementation commit `9f320cd`. It serves schema 17 at
`/home/paudley/.local/state/sparrow/development-v5/rpc.sock`, with directory mode
0700 and socket mode 0600. The persistent store remains
`store:46c88e949ccec658600b66c7becd5bb7`, format `sparrow.store/8`, with 192 live
records and no active pins. The opened session is
`session:9dba87291fed3b9bd2d81ed9cf3b6f56`; CLI and live stdio MCP status both
reported ready pressure, zero requested bytes, no evictions and no maintenance
failure. The sole configured synthetic watch remains `development-completed`
revision 1, covered, with no pending claims. This is the existing transient user
unit, not reboot/deployment qualification.

The saved descriptor request `descriptor-acceptance-20261007-01` returned its
original durable artifact `artifact:d066e0d527e9e82ec11e13388b386c13`; an 82-byte
export matched its original source exactly. Both status and admission retry of
the saved completed-file handoff `handoff-acceptance-20261007-01` returned the
original removed import and artifact `artifact:eec8eedea8b05f3d9512e88ff8ddd61f`.
Its 87-byte CRLF export matched the original source, and the replacement producer
pathname still contained `replacement preserved after handoff` plus newline.
Exports and live MCP messages are under `/tmp/sparrow-pressure-live.vQGn2o`.
No pressure ballast or live producer intake was added to this development store.

The initial workspace run hit a transient `WouldBlock` on the existing claim
recovery fixture's coordination lock, alongside the corrected MCP fixture
assertion. The final workspace gate and mandatory commit hook both passed that
recovery fixture. This change does not claim to explain or fix the transient
lock refusal. Logs are `/tmp/sparrow-pressure-check-20261007.log`,
`/tmp/sparrow-pressure-final-check-20261007.log` and
`/tmp/sparrow-pressure-commit-20261007.log`.

## Typed explicit completed handoff

Schema 16 adds `watchHandoff` and read-only `watchHandoffGet`. The Rust SDK and
CLI use the same generated client and receipt certification. The request binds
the original persistent store, stable request ID, registered watch revision,
relative Unix path bytes and exact device/inode/length/mtime/ctime generation.
First admission requires the current enabled finished-file watch and an anchored
regular-file observation, with file and parent synchronization. Explicit producer
handoff bypasses the discovery quiet timer; it does not weaken generation checks.

Admission uses the existing bounded worker control queue and atomic storage
reservation. It does not hold a publication RPC semaphore while awaiting the
worker. Every queued catalog mutation refreshes pending ownership, including
handoffs admitted earlier in that same tick. The same worker then performs
atomic private claim, SDK upload, durable publication and synchronized removal.
An admission receipt says `reserved`; progress can advance through `claimed`,
`published` and `removed`. Admission does not certify publication or deletion.
No separate importer, deletion path, parser or retry transaction is introduced.

The CLI workflow is:

```sh
sparrow SOCKET watch handoff-prepare REQUEST WATCH REVISION RELATIVE_PATH INTENT.capnp
sparrow SOCKET watch handoff INTENT.capnp
sparrow SOCKET watch handoff-status INTENT.capnp
```

Preparation writes one mode-0600, synchronized, create-new Cap'n Proto intent
outside enabled consumption roots. It does not reserve, rename or delete the
source. Intents are bounded to 16 KiB and reject trailing messages, symlinks and
special files. Retry `handoff` with that saved intent; do not run preparation
again against a replacement generation. The SDK supports non-UTF8 source paths.
Stable retries recover their original binding before touching current roots,
definitions or source paths, including after watch removal and daemon restart.
The default stdio MCP search surface receives no destructive handoff authority.

The SDK codec unit module owns one dev-only intent fixture shared by its two
adversarial tests, recorded with the typed-intent ledger job and vectors. The
existing census excludes `cfg(test)` modules, so that fixture is verified by the
unit checks rather than asserted as an independently indexed shipping symbol.
No detector, exception or index rule was relaxed.

The final-source `make check` gate passed with 12 SDK unit tests, 68 storage
scenarios, 23 real-daemon/filesystem/MCP scenarios, all remaining workspace
checks, release Clippy with warnings denied, seeded census self-tests and 114
ledger jobs. The real-daemon handoff scenario covers missing/stale/foreign
authority, generation/path conflicts without admission, immutable CLI preparation
and unsafe output refusal, quiet-timer bypass, exact CRLF bytes, raw non-UTF8
filenames, original retries after replacement/removal/restart, and no duplicate
records. These checks qualify explicit handoff; full architecture acceptance,
active tailing, syslog and the other outstanding query/operational work remain
unfinished.

The owned development daemon was updated from signed implementation `7aea5fc`
and reopened successfully after the live acceptance. It serves schema 16 at
`/home/paudley/.local/state/sparrow/development-v5/rpc.sock`, preserving store
`store:46c88e949ccec658600b66c7becd5bb7` and now fencing writers with
`sparrow.store/8`. Directory/socket modes remain 0700/0600. Captured status shows
192 live records, no pins, no pending durability writes and no watch failure.
The current daemon is the owned transient user unit `sparrow-development.service`;
this is not reboot-persistent packaging or full crash-lifecycle qualification.

The live synthetic watch `development-handoff` was created at revision 5 with a
600,000 ms quiet interval and source `development-handoff`. Its mode-0600 intent
`/tmp/sparrow-handoff-live.b62DhU/intent.capnp` binds request
`handoff-acceptance-20261007-01`. Explicit handoff advanced from durable `reserved`
to `removed` without waiting for that quiet interval, preserving import
`import:10275c3eb8af097970ac07ac615480a416e5242f58488680fdea93de4641b71a`,
artifact `artifact:eec8eedea8b05f3d9512e88ff8ddd61f`, and record
`record:380ba5359ea12540f996eef35cd723ac`. Exact export matched the complete
87-byte CRLF input saved as `/tmp/sparrow-handoff-live.b62DhU/expected.jsonl`.
Value and full-text searches each returned this record with complete coverage.
An event interval from `2026-10-07T21:00:00Z` to `21:01:00Z` returned the one
known event with zero unknown membership, using captured profile
`time-profile:d05caa4278d528afb294b4eee05255daa042b0ab818ba5f451b2f5350a63e2fb`.

The watch was removed at revision 6 after publication. Retrying the original
intent before and after a graceful daemon reopen returned the identical import,
artifact and `removed` phase; a replacement at the original source path remained
intact at
`/home/paudley/.local/state/sparrow/development-handoff-acceptance/finished.jsonl`.
The live count remained 192; no retry allocated another occurrence. The earlier
ordinary descriptor-import retry still returned original artifact
`artifact:d066e0d527e9e82ec11e13388b386c13` and record
`record:23febe08e38e42deea0f58e4880b9358`. Current admission contains only the
original `development-completed` revision-1 synthetic watch. No live producer
directory is configured, and no sibling/model service was modified.

## Explicit handoff storage foundation

Core storage now binds a caller-owned `ConsumptionHandoff` request ID, exact
watch revision, raw relative Unix path and observed device/inode/length/content
clocks to the existing consumption journal. Reservation and retry binding share
one durable write batch. Discovery and explicit admission use one reservation
implementation, path validator and native PurRDF catalog codec. A binding proves
admission only; publication and deletion still require the existing claim,
upload, durable publication and synchronized removal path.

There are at most 128 retained handoff bindings and 16 active journals. Retrying
unchanged arguments recovers original ownership without inspecting a current
source path; conflicting revisions, paths or generations fail. Active bindings
survive ordinary receipt expiry. Removal confirmation atomically starts their
48-hour completed retry window. Missing active ownership is a hard storage
failure, including during expiry. A journal does not pin retained evidence.

First handoff admission requires a durable watch catalog and fences older
writers with `sparrow.store/8`. Subsequent catalog replacements preserve that
marker. The storage API relies on the server to verify the actual registered
revision, enabled state, explicit producer authority and descriptor generation
before first admission. Schema 16 wires those checks through the worker and typed
operations described above.

The storage foundation commit `fad05de` passed the full `make check` gate with
68 storage scenarios, 22 real-daemon,
filesystem and MCP scenarios, all remaining workspace checks, release Clippy
with warnings denied, seeded census self-tests and 109 ledger jobs. The extended
consumption lifecycle covers restart, original publication beyond retry and
retention, removal-window renewal and completed expiry. The admission scenario
covers missing catalog and overflow refusal without orphan reservations,
revision/path/generation conflicts, bounded bindings, reopen and hard failure
after deliberate active-journal corruption. These are storage and regression
checks; they do not qualify a live explicit handoff operation.

## Durable typed watch administration

Schema 15 adds `watchList`, `watchPut`, `watchRemove`, durable definitions/revisions
in status, and explicit CLI administrative commands. `watch put REQUEST_ID 0
DEFINITION.toml` creates a missing definition; the same command with its current
revision updates it. `watch remove REQUEST_ID WATCH_ID CURRENT_REVISION` disables
new source admission. The definition file uses the same strict `ConsumeWatch`
fields as one startup entry, without the `[[consume]]` table wrapper. Its explicit
finished-file producer contract authorizes private-source retirement after the
existing durable publication boundary. Ordinary MCP search tools gain no watch
mutation authority.

The one worker owns mutations through a 16-command typed channel. Administrative
RPC waiting does not hold the publication semaphore, which would prevent the
worker from completing its own typed upload. Store-scoped mutations compare the
current watch revision, carry stable request IDs, and return durable receipts.
The native framed BLAKE3 identity covers operation, exact arguments and expected
revision. A retry returns its original immutable receipt even after later
updates/removal; it does not restore that old authority. Current list/status,
rather than a historical retry acknowledgment, describes current admission.

The server owns one strict version-1 TOML catalog codec. Core storage treats its
bytes as opaque, bounds the complete catalog at 512 KiB, and commits its exact
capture compare-and-set and format marker in one durable write batch. There is
no second configuration parser or filesystem sidecar transaction. The catalog
binds to the persistent store and contains at most eight active/draining roots
and 128 bounded mutation receipts, with eight slots reserved for removal rather
than further put/update requests. Receipt expiry is 48 hours. Capacity refusal
preserves the old catalog; an uncertain persistence or invariant failure is
terminal, so the worker cannot continue with stale source authority.

Initialization atomically fences old writers with `sparrow.store/7`. New writers
accept prior formats; their first registry publication upgrades it. A later
consumption reservation cannot downgrade that marker. A format/catalog mismatch
or malformed stored catalog fails actionably rather than use startup defaults.
Startup configuration is bootstrap-only once the catalog exists: changing or
restoring an old `[[consume]]` block does not undo durable administrative removal.
Structural configuration errors remain errors, while an obsolete bootstrap root
need not exist after its durable definition has been removed.

Root admission reuses the same strict configuration checks, no-symlink
`ClaimRoot`, private lock and generation-bound journal machinery. A new root is
opened before publishing its authority; no claim is admitted before catalog
commit. Updating the same root starts fresh readiness evidence. A different
physical root is refused while any old journal remains. Removal clears discovery
and candidates, retains the disabled definition and held root for recovery, and
automatically retires that definition after all persistent claims finish. It
does not cancel staging, delete unexpected bytes or abandon a blocked claim.
Removed roots with no pending journal stop immediately. The worker rotates root
admission after a processed claim instead of always prioritizing the first root;
sustained multi-root fairness/throughput still needs measurement.

Tests exercise real-daemon create/update/list/remove, explicit authority refusal,
foreign-store and stale-revision refusal, changed retry arguments, ordinary
ingestion after dynamic registration, immutable retries across restart, and stale
bootstrap configuration after removal. A removed watch with a mutated private
claim stays disabled and recoverable across restart, preserves new unclaimed
files and refuses rebinding before touching the other root. Corrupt durable
catalog startup is a hard failure. A real-daemon pressure fixture fills all 120
put receipt slots, verifies refusal, and still disables the watch using reserved
removal capacity. Core coverage proves exact catalog CAS,
capacity refusal, persistent identity/reopen and preservation of the format-7
fence during later source reservation.

The qualification gate includes release Clippy with warnings denied, ten SDK
units, five core units, 67 storage scenarios, 50 census units, five census gate
fixtures, one server unit, 22 RPC/filesystem/MCP scenarios, doctests and the seeded
self-test. The pinned cross gate checks 105 ownership jobs without detector
relaxation or distinct exceptions. The ordinary signed-commit hook runs `make
check` on final source. Live development acceptance is recorded separately.

Explicit completed handoff, active tailing, forced kernel overflow and watch
exhaustion, failure injection at every process/crash boundary, full governed
SPARQL and the remaining full architecture/pressure/memory/deployment acceptance
are still unfinished. Administrative catalog publication does not establish
those results.

The signed implementation `c7adbbc` passed its final ordinary pre-commit gate
on 2026-10-07, including all 22 RPC/filesystem/MCP scenarios and the 105-job
pinned census. The previous owned development PID 2566561 exited successfully
on SIGINT and removed its endpoint. The updated unit is active as PID 3067453
at the same mode-0600 Unix socket, with mode-0700 data and the original store
`store:46c88e949ccec658600b66c7becd5bb7`. Its first administrative catalog commit
upgraded the marker to `sparrow.store/7`; discovery reports schema 15. Existing
190 records were present before this acceptance input.

Live typed administration created the owned synthetic root
`/home/paudley/.local/state/sparrow/development-admin-acceptance` at revision 2,
updated its quiet interval from one to three seconds at revision 3, and later
removed it at revision 4. Its exact 90-byte CRLF JSONL input was consumed as
`artifact:f63d6185e7f7a6abaf49b34f0dc6970c`, record
`record:bd6303bb090e0faf83f4050d3a8a7ba1`. Native byte retrieval compared equal to
the original input; value searching and the declared `/at` RFC3339 event interval
returned one exact row with complete membership and zero unknown events. Its
immutable time profile is
`time-profile:5ed7182c60ad536eea8a18d827ddb592dd263c19af4b0a284d2754d6816fb2eb`.

Retrying `watch-admin-create-20261007-01` with its original definition returned
the original revision-2 receipt after removal, while current list/status still
contained only `development-completed`, revision 1. A new file published after
removal remained untouched beyond the removed watch's quiet interval. The
final observed status showed 191 live records, no pending claims, no watch or
durability failure, and no pending durability writes. Retrying the earlier
ordinary descriptor fixture retained
`artifact:d066e0d527e9e82ec11e13388b386c13` and
`record:23febe08e38e42deea0f58e4880b9358`.
Only owned synthetic roots were administered. No live producer/model directory
or sibling service was changed; permanent deployment and the full architecture
remain unqualified.

## Configured recursive completed consumption (schema 14 qualification)

Schema 14 adds typed `Status.consumeWatches`; the daemon now starts one owned
Linux consumption worker for explicit `[[consume]]` roots. The development
configuration has only the owned synthetic intake described below. An example for a directory that the
operator explicitly owns and authorizes Sparrow to consume is:

```toml
[[consume]]
id = "completed-application-dumps"
root = "/srv/application/completed"
producer_contract = "finished_files"
source = "application-dumps"
format = "jsonl"
quiet_ms = 30000
max_directories = 1024
max_candidates = 4096
```

The contract is required: producers hand off finished regular files and never
write them afterward. Quiet time is an observation within that contract, not
a writer lock. Ordinary imports remain non-destructive. Configuration rejects
overlapping roots, unknown fields, missing contracts, unbounded capacities and
incompatible format names. Root admission rejects symlink components and overlap
with Sparrow's data directory. At most eight roots are admitted, with at least
two RPC connections so the owned SDK connection can coexist with callers.

Each recursive directory is opened with the existing anchored `openat2` owner
and watched before it is scanned. Linux inotify has no dirfd registration API;
the only followed alias is the kernel-owned `/proc/self/fd/<held-directory>`.
Candidate metadata also uses that held-parent alias and `symlink_metadata`,
without opening the candidate or generating a self-induced `IN_OPEN`. The SDK's
`CompletedFileIdentity::capture` remains the sole generation observation owner.
Symlinks, FIFOs, devices and sockets are excluded. Native `statat` handles unknown
directory-entry types without assuming that they are regular files.

Scans advance by 64 entries per 50 ms worker tick. Candidate and rename maps,
directory descriptors and scan queues have explicit capacities. Candidate
saturation preserves excess files, exposes `deferred`, and schedules a new scan
as capacity becomes available. The scanner can finish coverage while candidate
capacity is full, so a full candidate registry does not prevent draining its
admitted prefix. Periodic reconciliation begins after complete scan coverage,
at the larger of 60 seconds or twice the quiet interval plus one second; it
cannot continually restart an unfinished large scan.

Open/write/attribute/creation/close-write/publication events reset quiet time.
Read-only closes are not completion signals. Rename cookies reconcile candidate
movement, with inode identity still checked at the destination. Overflow,
ignored/unmounted directories, directory moves and recursive namespace changes
start a new inotify descriptor epoch and clear stale readiness. An event drain
is bounded at 4096 events; if that limit can discard prefetched Reader bytes,
the entire coverage epoch is invalidated before further claims. No claim is
admitted during reconciliation, and completion of coverage starts fresh quiet
timers. Directory/root/lock coverage failures are actionable terminal daemon
errors; they do not authorize source retirement. The daemon joins the worker,
flushes storage and removes its owned endpoint on an orderly terminal path.

The worker resumes the existing bounded persistent journal registry before
admitting new files. Recovery, SDK upload and private-source unlink share the
previously qualified `ClaimRoot` implementations. RPC discovery has a finite
10-second deadline and claim publication a 45-second deadline. Failure retains
the original journal/request identity and retries after a five-second delay;
it never substitutes a second source occurrence. Pending journals cannot be
silently abandoned by deleting their watch from configuration. Shutdown stops
admission and cancels asynchronous waiting while preserving recoverable claims.
It joins while the server LocalSet still drives its owned Cap'n Proto connection.

Typed status exposes configured ID/root, reconciliation or complete coverage,
held directories, tracked candidates, deferred inputs, pending/completed claims,
coverage epochs/losses and the latest bounded unresolved failure. CLI and stdio
MCP use the same generated reply and renderer; no HTTP interface was added.
The four new acceptance fixtures cover strict configuration, actual recursive
daemon consumption with a one-candidate limit, original-byte queries, writable
reopen deferral, newly created directories, later source generations, confirmed
journal recovery, late-write preservation, directory coverage limits, root
replacement and consumer shutdown. Samples and sibling services remain untouched.

Qualification uses the normal `make check` gate: release Clippy with warnings
denied, ten SDK units, five core units, 66 storage scenarios, 50 census units,
five census gate fixtures, one server unit, 18 RPC/filesystem/MCP scenarios,
doctests and the seeded census self-test. The pinned cross-workspace gate checks
95 helper ownership jobs with zero distinct exceptions or unsanctioned groups.
The signed commit's ordinary pre-commit hook runs the same gate on final source.

At this schema-14 qualification, dynamic typed watch administration, explicit completion RPC, forced kernel
overflow and watch-exhaustion injection, whole-process crash injection at every
claim boundary, fairness across continuously busy roots, active-log tailing and
whole-corpus memory/throughput acceptance remained unfinished. No reboot-persistent
service or production producer handoff has been qualified by this change.

The signed implementation `5126d8b` passed its ordinary final pre-commit gate on
2026-10-07. The previous owned development unit, PID 2481486, exited successfully
on SIGINT and removed its socket. The updated unit is active as PID 2566561,
serving the same private `development-v5/rpc.sock` and persistent store
`store:46c88e949ccec658600b66c7becd5bb7`. Its configuration now authorizes only
the owned mode-0700 synthetic directory
`/home/paudley/.local/state/sparrow/development-completed`, with a 30-second quiet
interval, 16 directory watches and 64 candidates. The excluded private claim
directory is also mode 0700; the RPC socket is mode 0600.

A synthetic 82-byte CRLF JSONL input was atomically published into that root.
The live worker durably consumed it as
`artifact:751286900f498e0c4a3cf272fce7376d`, record
`record:bccb3dfc0bea556833fba8f475b0fb2f`, and retired only its private claim.
CLI byte retrieval compared equal to all original bytes, including the CRLF.
Native value searching returned the exact message; event-time searching for
`2026-10-07T20:00:00Z` through `20:01:00Z` returned its known source instant with
zero unknown events and complete membership. The explicitly defined `/at`
RFC3339 profile is
`time-profile:4923153986ea40fed2adcd0e58d54f57fd04c2fea3e608cb294cef8fab62cdf1`.

The first live consumption reservation upgraded the store marker to
`sparrow.store/6`. Status reported schema 14, 190 live records, one completed
claim, no pending claims, no failure and no pending durability writes. Retrying
the previous independent descriptor fixture retained
`artifact:d066e0d527e9e82ec11e13388b386c13` and
`record:23febe08e38e42deea0f58e4880b9358`. No live application/Brainstem directory
was configured or consumed, and no sibling/model service was changed. This is
live development acceptance; the full architecture and permanent deployment
remain unqualified.

## Transport and present operations

The daemon uses **Cap'n Proto RPC over a private Unix-domain socket**. The HTTP
implementation and its dependency were removed following the transport decision
on 2026-10-06. No JSON operation envelope is sent between clients and the daemon.
The generated typed contract is
[`sparrow.capnp`](../crates/sparrow-client/schema/sparrow.capnp). Domain results
and failures are explicit unions; counts and clock instants are unsigned integer
fields, formats and JSON kinds are enums, and references carry a store identity.

Present RPC methods include the eight import methods documented below,
plus `status`, `flush`, `ingest`, `eventsIngest`, `artifactGet`, `recordGet`,
`bytesRead`, `pin`, `pinRenew`, `pinRelease`, `pinRead`, `keysList`, `valuesSearch`, `textSearch`, `recordsSearch`, `recordsContext`, `rdfInspect`, `sourceTimeDefine` and `sourceTimeGet`. The shared typed
client transport is `sparrow_client::connect`, used within a Tokio `LocalSet`.
The `sparrow` CLI and stdio MCP adapter share generated-client operations and
result rendering. The CLI supplies status, non-destructive resumable file import,
exact byte export, key/value and full-text searches. MCP also supplies artifact/
record metadata and pin/release calls. CLI and MCP both supply stable-ID pin renewal
and paginated pin discovery/member retrieval. Remaining CLI commands and query surfaces
are required work.

## Standalone typed Rust producer client

`crates/sparrow-client` owns the schema compilation, Unix transport, discovery
guard, typed artifact receipts and completed-file upload implementation. The
daemon imports the generated contract from this crate. The CLI uses its public
`import_file` function; there is one hashing, file-identity and resumable upload
path. The existing server exports are reexports of these same generated types.

`sparrow_client::Client::connect` checks discovery before producer mutations.
Within a Tokio `LocalSet`, callers can use `ingest`, `events`, `import_file`, `import_open_file`, `artifact`,
`discovery`, and the generated query capability. Direct ingestion rejects inputs
above the discovered buffered-input limit; file imports hash and upload through
one 64 KiB buffer. Both return owned `ArtifactReceipt` values. Reuse the original
request ID and immutable bytes after a connection failure. Domain errors retain
their exact daemon code, message and retryability; incompatible discovery, I/O,
protocol and transport failures remain explicit.

The producer dependency graph was checked with `cargo tree -p sparrow-client -e
normal`: it contains the Cap'n Proto/Tokio transport, native PurRDF hashing/JSON
and safe Rustix filesystem calls,
without RocksDB, `sparrow-core`, or the semantic engine. Release acceptance now
uses the SDK to recover a partially prepared oversized record after a daemon
crash, verifies the same receipt through CLI retries and SDK inspection, then
checks durable direct ingestion and conflicting request refusal. Discovery tests
refuse old schema versions and missing projection identity. The Rust example in
the crate documentation is compiled as a doctest.

The direct artifact/event-batch client and bounded `Delivery` worker are present.
The worker reconnects with immutable pending requests and returns durable
admitted-input fences through flush/shutdown. The composable `TracingLayer` now
captures bounded typed fields and span callbacks through that same worker.
Schema 12 below adds explicit buffered server acknowledgments and the shared
WAL flush contract. Full server acceptance remains unmet.

### Schema 11: typed producer event batches

`eventsIngest` accepts at most 256 events, a conservative 1 MiB batch expansion,
64 KiB per event, 128 fields per event/span, and 32 context spans. Every string
is at most 16 KiB. Shared SDK admission runs both before encoding and during
daemon decoding; counts and quotas are checked before list/string allocation.
Discovery advertises batch limits. The client refuses older schemas or different
batch bounds before producer mutation.

`ProducerEvent` carries a nonzero increasing client sequence, producer UTC Unix
milliseconds, severity/kind enums, metadata name/target, ordered typed fields,
span context, and optional subject/follows-from identities. Sequence gaps are
allowed and remain visible; this operation does not promise a complete sequence
history. Producers must use a fresh 16-byte client instance for each process and
preserve the exact pending batch and request ID across reconnects. Server retry
identity is the batch request ID, not a global deduplication of span/sequence IDs.

The daemon emits canonical native JSONL using PurRDF's writer and native integer/
floating codecs, then uses the existing atomic record/dependency builder and
durable receipt law. The stored source is this explicit structured-event
representation, not a claim of preserving an unseen textual producer log. Each
event is one independently retained occurrence. Field wrappers retain Rust kind
alongside queryable values. Floating fields always retain all 64 bits, including
negative zero and NaN payloads; nonfinite values have an explicit classification
and no fabricated JSON numeric value. Binary fields retain exact native hex.
Debug/error renderings keep their explicit kinds. Span snapshots/lifecycle and
follows-from references are source evidence; a separate normalized diagnostic
RDF relationship view is still required.

Typed events declare `/event_time` with Unix-millisecond interpretation through
the existing immutable source profile operation before artifact ingestion. An
incompatible existing source profile refuses the batch. A new profile may remain
if subsequent artifact admission fails; no event prefix or acceptance receipt is
published on that failure. Acceptance/expiry clocks remain server clocks.

Release acceptance verifies all field variants, repeated names, escaped text,
span context, exact timestamp evidence spans, temporal/text retrieval, unchanged
receipts after restart, changed NaN-payload conflict, and oversized/invalid input
refusal. The `Delivery` worker below supplies automatic batching/reconnection,
drop counters and durable admitted-input fences. Tracing callbacks are implemented
below; schema 12 also provides explicit buffered server writes and WAL fences.

The owned development daemon was replaced with this qualified schema-11 build
on 2026-10-07. It retained store
`store:46c88e949ccec658600b66c7becd5bb7`, store format `sparrow.store/5`, and the
existing private socket. PID at verification was `1407618`. A live call through
the standalone producer example accepted one 380-byte event as
`artifact:e6c56d2df4de85a31e00c2a01cb4fda9` with record
`record:be55d651c34cd9c68b389dae6f9b0511`; identical arguments returned the same
durable receipt. Temporal/text retrieval found exactly that record at producer
time `2023-11-14T22:13:20.002Z`, boolean value search returned original `true`
source bytes, and a complete byte export reproduced the declared event source.
The source was synthetic; production sample files were untouched.

```sh
CARGO_BUILD_JOBS=4 cargo build --locked --release --workspace --bins --examples
target/release/examples/events /home/paudley/.local/state/sparrow/development-v5/rpc.sock acceptance-schema11-events schema11-typed-event-v1 00112233445566778899aabbccddeeff 1700000000002 'allocation failed typed acceptance'
```

The example takes an explicit instance identity and timestamp so a retry can
reproduce the same input. A new producer process must choose a fresh instance
for new observations; this direct event example remains separate from the
application tracing example below.

### Bounded producer delivery worker

`sparrow_client::Delivery` owns one producer thread, a Tokio LocalSet and the
shared typed client. Producers call `try_send(ProducerEvent)` without waiting for
socket I/O or a queue lock. The worker assigns increasing sequences under queue
serialization. Full, contended, invalid, closed and failed admission each have
visible drop counters. A full-queue refusal can leave an explicit sequence gap;
refusals before sequence assignment remain visible through counters. Event
admission also charges unused string/vector capacity, preventing a tiny value
from retaining an oversized allocation. Caller-owned inputs must already be
constructed; the tracing layer below bounds capture before queue admission.

Defaults are 1024 queued/in-flight events, 4 MiB conservative event charge,
128 events/512 KiB per batch, two-second intended batching, five-second RPC
deadlines and 250 ms retry delay. Setup validates finite upper/lower bounds;
these are configurable. Queue and immutable pending-batch ownership stay charged
until a matching durable receipt. Retryable Unix-socket/transport/daemon errors
preserve exactly the same request ID, instance, sequences, timestamps and values.
RPC timeout means delivery unknown. The first discovered persistent store binds
the worker; reconnecting to another store fails explicitly with pending input
retained. Permanent failures remain inspectable through `stats` and
`pending_batch`, with no manufactured acknowledgement. Source-time registration
pressure now uses the same retryable `STORAGE_PRESSURE` code as ingestion.

`stats` exposes offered/admitted/durable counts, drops by reason, pending count/
charge, in-flight count, retry count, terminal/transient failures, oldest pending
age and flush lag. Charges bound retained work; they do not measure process RSS,
native transport arenas or allocator overhead. The queue's reserved container
capacity remains bounded by setup limits even after it drains.

`flush(timeout)` forces an admitted-sequence fence. `shutdown(timeout)` also
closes admission and waits for the worker/socket to finish. One try-acquired
control slot rejects simultaneous barriers as `Busy`; there is no unbounded
control-message queue. Watermarks carry instance, required/durable sequence,
durable event count, pre-fence drops and the last store-scoped receipt. If any
offer was dropped before the captured fence, the barrier returns `Dropped` with
the admitted-input watermark instead of certifying all offers. A timeout retains
pending work and may be retried. A timed-out shutdown remains closed. Offers
that finish after fence capture belong to a later fence. Dropping the last
handle cancels the owned worker; it does not promise a flush or producer-process
survival. Empty shutdown returns no fabricated receipt.

This is durable delivery for this producer's admitted events. It does not expose
a server-wide flush; schema 12 supplies a separate accepted-storage fence.
Every current event batch receipt must be `durable` before queue retirement.

Release scenarios use real temporary daemons and a typed forwarding relay that
withholds a real durable reply. They verify identical retry bodies/receipt and
one occurrence, count/byte bounds during daemon absence, unchanged pending work
after timeout, explicit drops after recovery, timer-driven delivery without a
flush, fresh process instances, store replacement refusal, reserved-allocation
admission, and cancellation of an unresponsive owned socket before its 60-second
RPC deadline. Production and test relay connections now share the same bounded
server transport constructor. No model/Brainstem service lifecycle is involved.

```sh
CARGO_BUILD_JOBS=4 cargo build --locked --release --workspace --bins --examples
target/release/examples/delivery /home/paudley/.local/state/sparrow/development-v5/rpc.sock delivery-example 1700000000002 'bounded delivery example'
```

Live verification on 2026-10-07 replaced only the owned development daemon with
the qualified build, preserving store identity and schema 11. PID at verification
was `1642510`. Two separate runs of the public delivery example with identical
message/source/time produced fresh client instances
`56799eb3eeea574d8eac6d652e27270b` and `eba3448b332f3ff224a74dfc00386e37`, with
durable sequence-1 receipts for independent artifacts
`artifact:36d39d40186e4eba4efaf62aebff8500` and
`artifact:fb90b17321830cad8aa9527545ba6fa3`. Native temporal/full-text retrieval
returned both 341-byte occurrences at producer time
`2023-11-14T22:13:20.003Z`, with zero unknown time or failed text coverage. Lost-
reply retry qualification remains the deterministic real-daemon relay scenario,
where an unchanged instance/request returned the same occurrence. These synthetic
events do not establish production throughput, RSS or complete tracing acceptance.

```sh
CARGO_BUILD_JOBS=4 cargo build --locked --release -p sparrow-server
target/release/sparrowd --config config/development.toml
target/release/sparrow /home/paudley/.local/state/sparrow/development-v5/rpc.sock status
```

### Composable Rust tracing capture

The standalone client now provides `TracingLayer::new(delivery.clone())` for
application-owned `Registry`/`LookupSpan` subscriber composition. It installs no
global subscriber. All callbacks feed the existing delivery worker and its
immutable retry identities; no second transport, queue or worker is introduced.
Use the production example with a synthetic source:

```sh
cargo run --locked --release -p sparrow-client --example tracing -- \
  /home/paudley/.local/state/sparrow/development-v5/rpc.sock acceptance-tracing-layer
```

Capture preserves all shared primitive field types, including exact float bits,
binary and 128-bit integers. Debug/error formatting has explicit wire variants.
Span new/update/enter/exit/close/follows-from observations include bounded
root-to-subject snapshots, explicit parent bindings and the latest valid fields.
At most 1024 live snapshots are retained, each conservatively charged at 16 KiB;
text/binary/debug/error values refuse above 4 KiB, fields above 128 or 16 KiB,
and context above 32 spans or the shared 64 KiB event admission bound. The layer
walks ancestry leaf-first and charges before cloning to avoid unbounded scope
collection. Closed spans release owned snapshots and admission slots.

Capture failure increments the shared Invalid drop counter, including invalid
clock, unavailable context, formatting/field bounds and span state contention.
A failed span update invalidates future context capture rather than emitting
stale fields. Drops remain explicit errors at flush/shutdown. Subscribers that
remap span IDs are refused visibly. User formatters and the application's own
subscriber internals are outside the layer's allocation/computation control.

The real-daemon scenario exercises all wire field variants, nested/explicit-root
context, updated fields, all lifecycle kinds and store occurrence counts against
the admitted/drop fence. Client scenarios exercise sticky formatting refusal,
failed-update invalidation, reclamation of the 1024-span admission capacity and
unique producer identities across independent subscriber registries sharing one
worker. Direct typed span producers sharing it use `Delivery::next_span_id` too.
These checks do not establish sustained throughput or process RSS. Schema 12
adds buffered/WAL-flush controls; normalized diagnostic RDF relationships remain
unfinished.

The full release-profile `make check` passed: eight client unit tests, 56 core
storage scenarios, eight real-daemon/MCP scenarios, the other workspace checks,
Clippy with warnings denied and the seeded/pinned helper census. All 59 ledger
jobs hold with zero distinct exceptions or unsanctioned isomorphic groups.

Live application-example acceptance on the existing schema-11 development
daemon delivered five durable observations with zero drops, producer instance
`00e9e0542ab8704de7917857bac03658`, artifact
`artifact:b9131864191c576a5782ef44d7b9cc9e` (2014 bytes). Native text search for
`structured`, field value `/fields/1/value` of kind `true`, and the producer-time
interval `[2026-10-07T16:42:42.365Z, 2026-10-07T16:42:42.366Z)` each recovered log
record `record:d04d979eee5329d44da61b9f138c5ca5`, with original field-byte evidence
and zero unknown time or failed text coverage. The daemon remained at the same
store identity and private socket; no sibling producer or service was changed.

### Schema 12: explicit buffered acceptance and WAL fences

Direct SDK artifact/event ingestion can request `Durability::Buffered` through
`ingest_with_durability` / `events_with_durability`; existing default methods and
the tracing delivery worker still require durable acknowledgment. Buffered
acceptance uses the same atomic catalog, payload and native-index publication
with WAL enabled. Its receipt carries a `buffered_through` store/session/sequence
fence. `Client::flush(&fence)`, CLI `flush STORE_ID SESSION_ID SEQUENCE` and MCP
`flush` route through one typed SDK helper. Inspection/retry upgrades durability
without replacing occurrence IDs; mode changes do not change request identity.

The server ledger refuses a new buffered write before publication at 256 pending
writes or 64 MiB of atomic batch byte charge. Repeated requests consume no new
ledger entry. Successful synchronized writes/explicit WAL sync retire earlier
pending charges. Empty control batches use an explicit sync rather than treating
an empty write as a boundary. Write/WAL-sync failure latches uncertainty and
refuses receipt certification until the store is reopened. Native WAL is shared
across families; see [RocksDB WAL](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-(WAL)).

The default intended periodic sync interval is 2000 ms, configurable through
`buffered_flush_interval_ms` from 1 to 60000. It is not a disk latency guarantee.
CLI/MCP status advertises accepted/durable sequences, session, pending writes,
batch bytes, oldest age, interval, lag and failure. Reopening first synchronizes
recovered data and starts a fresh session. Older-session and future fences are
refused; inspect/retry the original artifact to determine recovered state.
Fences do not extend retention or certify producer-side drops.

Storage scenarios cover buffered retry identity, count and byte pressure without
prefix publication, foreign/stale/future fence refusal, durable retries and
completed-file import acknowledgments, empty control batches, queue retirement
and reopen. Real-daemon acceptance exercises SDK/CLI/stdio MCP flush, buffered
artifact/event requests, durable mode upgrade, killed-process durable recovery,
old-session refusal and automatic periodic synchronization. These scenarios
qualify the ordinary native API paths; injected host power loss and real failed
disk/WAL synchronization, peak RSS and sustained pressure remain unqualified.

The full `make check` passed with 60 storage scenarios, nine real-daemon/MCP
scenarios, the other workspace checks, release Clippy with warnings denied and
the seeded/pinned helper census. All 68 ledger jobs hold with zero distinct
exceptions or unsanctioned isomorphic groups.

The previous development process was absent and its owned stale socket refused
connections. After verifying no listener/process remained, that endpoint alone
was removed; the database was preserved. The qualified schema-12 executable now
runs under the owned transient user unit `sparrow-development.service`, PID
1857046 at verification, at the existing `development-v5/rpc.sock` and persistent
store identity `store:46c88e949ccec658600b66c7becd5bb7`. This unit survives tool
session closure; it is development supervision, not installed reboot persistence
or qualified crash-restart automation. Inspect it with:

```sh
systemctl --user status sparrow-development.service
target/release/sparrow /home/paudley/.local/state/sparrow/development-v5/rpc.sock status
```

Live buffered event acceptance under `acceptance-schema12-buffered` returned
`artifact:adccc56dea73a970301e12bea670da6f`, 375 bytes, one record, fence sequence
554975 in session `session:273a5f33ffa9525f8e30471fb856779f`. Immediate status
showed one pending write with 161922 batch bytes and a lower durable sequence.
CLI flush reached 554978; an identical request in durable mode returned the same
artifact. Native temporal/full-text retrieval at
`2023-11-14T22:13:20.004Z` recovered
`record:4663ac26601dd8b25ccf6cd886b7217b`, with zero unknown time or failed text
coverage and original source spans. Directory/socket modes remained 0700/0600.
No producer input or sibling/model service was changed.

### Schema 13: artifact, record and pin query membership

Key/value discovery, native text retrieval, combined same-record queries and
context reads now share explicit membership scopes. Generated `Scope.selection`
defaults to live membership, or selects typed evidence references or a stable
pin with an optional revision guard. CLI `--scope-file FILE` and the MCP `scope`
object share one native PurRDF JSON decoder and generated encoder; field and
scope control files share one bounded reader. No alternate parser, tokenizer,
scoring law, or operation envelope was added.

Every selected reference is validated before filters. Complete artifacts expand
their retained records, overlapping references are deduplicated, and missing or
expired selections fail explicitly. A pinned individual record does not make
its expired complete artifact selectable. Explicit field/text scopes seek their
own chronological candidates; context scans apply the same resolved membership
while retaining their existing directional scan budget. Membership expansion
limits and remaining receipt/paging/I/O limitations are documented in
[Interfaces](interfaces.md).

`make check` passed with 62 storage scenarios, ten real-daemon/MCP scenarios,
eight client unit tests, five core unit tests, one server unit test, all census
unit/integration tests and doctests, release Clippy with warnings denied,
seeded census self-tests and the pinned cross-workspace gate. All 72 ledger jobs
hold, with zero distinct exceptions or unsanctioned isomorphic groups. Storage
scenarios exercise artifact expansion/overlap, record seeks, source intersection,
missing references even outside filters, empty selection, pin revision/deadline,
name reuse, and retained records after parent expiry/reclamation. Real-daemon
acceptance verifies all five CLI/MCP query surfaces and a direct generated
foreign-store reference produces a typed `STORE_MISMATCH` result.

On 2026-10-07 at approximately 17:43 UTC, the previous owned development unit
exited successfully on SIGINT, synchronized storage and removed its socket. The
qualified schema-13 executable now runs under `sparrow-development.service`, PID
2250240 at verification, at the unchanged private socket and persistent store
`store:46c88e949ccec658600b66c7becd5bb7`. Store format remains `sparrow.store/5`,
directory/socket modes 0700/0600, and live record count 187. Reopen starts session
`session:b9a51ba13f0bb6147e92fcf41165cf42`. The transient unit still does not claim
reboot persistence or qualified automatic crash recovery.

Live CLI artifact-plus-record selection recovered the existing buffered event
`record:4663ac26601dd8b25ccf6cd886b7217b` from
`artifact:adccc56dea73a970301e12bea670da6f`: key discovery returned 21 key rows,
`/fields/0/value` returned one scalar row, and native text and combined event-time
search each returned one matching record. Receipts reported exactly one eligible
record and no failed projections or decoding failures. The text corpus contained
one document and 57 tokens. MCP independently recovered the same record through
temporary pin `pin:8a8376d2dc852289eb7e85fd264b88d3`, refused revision 2 with
`CONFLICT` and a foreign store with `STORE_MISMATCH`, then durably released only
that acceptance pin. No producer source or sibling/model service was changed.

Full governed SPARQL, diagnostic normalization/correlation, remaining scope
filters and query snapshots/paging, watchers/syslog, retention/pressure/RSS and
corpus performance qualification, and final deployment acceptance remain unmet.

### Completed descriptor upload and verified acknowledgments

The SDK and CLI now route file uploads through `imports::upload`, with one
opened-file generation guard and one native content hash/read path. The public
`Client::import_open_file` and capability-level helper borrow an
already opened completed regular file without reopening its path. Caller
descriptor ownership and cursor survive success or failure; positional reads
start at byte zero independently of shared descriptor offsets. `CompletedFileIdentity`
supplies the same metadata observation/guard to uploader and future claim
coordination. Path imports use safe Rustix `openat2` with symlink resolution
refused and nonblocking special-file opens; tests reject final/intermediate
symlinks, directories and FIFOs. Unsupported kernels fail explicitly.
Rustix 1.1.5 was already locked for dev tooling and is now a direct producer
filesystem dependency; PurRDF's pinned `purrdf-lex` supplies the native JSON
writer. Neither storage nor the SPARQL engine enters the producer graph.

`ExpectedArtifact::certify` is the shared producer receipt owner. Direct inputs,
event batches and completed files must receive their discovered store/session,
source, format, exact byte length/native BLAKE3 identity, admitted membership
and required durability. A stronger durable reply to a buffered request is
valid. Record identities, retention intervals and payload identity syntax are
validated; inspection also refuses a different requested artifact. Metadata
stability is checked after accepted-retry and final publication replies as well
as hashing/uploading, so a late source rewrite produces no certification.

The existing `sparrow.events/1` source codec moved from the daemon adapter to
`EventBatch::source_bytes` in the SDK. Daemon publication and producer payload
verification now invoke one native JSON implementation, preserving its format
and source profile. The bounded delivery worker still retires only certified
durable batches; permanent contradictions preserve the immutable pending body
and charge for diagnosis rather than certify a prefix.

Focused acceptance exercises opened-file upload after source-path replacement,
nonzero descriptor offsets, stable accepted retries and exact daemon byte reads.
The typed real-daemon relay corrupts direct/event/file-retry and final-publication
acknowledgments, substitutes a foreign staging store, and rewrites a source after
accepted-retry/final-publication replies. Every inconsistent receipt is refused;
late rewrites remain on disk, and the original durable commit remains recoverable.
The producer queue reports zero durable events and retains its pending input
on a receipt mismatch. These checks establish uploader/certification behavior,
not completed-consumption deletion authority or claim-journal recovery.

Run the application-owned example with:

```sh
cargo run --release -p sparrow-client --example descriptor -- SOCKET REQUEST_ID SOURCE FORMAT FILE
```

The full `make check` gate passed with 10 client unit tests, 62 storage scenarios,
12 real-daemon/MCP scenarios, the other workspace tests, release Clippy with
warnings denied, seeded census self-tests and all 75 ledger jobs. No distinct
exceptions or unsanctioned isomorphic groups were added.

On 2026-10-07 the owned development daemon was gracefully drained and replaced
with this release build. It retains schema 13, the same private endpoint and
persistent store identity. Live descriptor acceptance published 82 original
JSONL bytes as `artifact:d066e0d527e9e82ec11e13388b386c13`, record
`record:23febe08e38e42deea0f58e4880b9358`, payload
`blake3:b27c4ccab9c865986c3893a6c8916186ab01f3d86e8a72dbedda94dec2f61389`.
Stable retry returned the identical durable receipt; CLI byte export matched
the preserved source. Native key discovery returned four rows and value lookup
returned the exact message scalar. The typed event example also passed native
source-byte certification and stable retry as
`artifact:6ac189d158e9e25014411efc779ac825` (one record, 374 bytes).
The directory remains mode 0700 and its Unix socket mode 0600. Only synthetic
acceptance inputs and the owned Sparrow unit were used; no production source
was claimed or removed and no sibling/model service was managed.

The durable core journal described below now binds source generation and
original publication identity across lost replies and the ordinary retry horizon.
Qualified filesystem claiming/deletion and inotify reconciliation remain required.
No production sample watch is connected; source removal,
full governed SPARQL, other continuous intake, and full architecture acceptance
remain unfinished.

## Durable completed-input ownership

The core now reserves each completed file generation before filesystem claiming.
`consumption_reserve` binds the watch, original relative Unix path bytes and
device/inode/length/modification/change clocks to one native BLAKE3 request
identity. The path is bounded and excludes traversal, NUL, absolute paths and
`.sparrow-inflight` components; non-UTF-8 filenames remain representable. Source
and format cannot change on retry. At most 16 active journals occupy this owner.
The first reservation atomically changes the store marker from `sparrow.store/5`
to `sparrow.store/6` with the journal. The current writer reads both versions;
older writers refuse version 6 instead of reclaiming managed recovery state.
Stores without consumption reservations keep version 5 and their identity.
`consumption_pending` enumerates the bounded active recovery set through the
same journal reader; completed removal tombstones do not occupy active slots.

`consumption_claimed` records the post-rename descriptor observation after the
coordinator has synchronized affected directories. Rename may change ctime, but
device, inode, size and modification clock must agree with the original
observation. Repeated claim confirmation must agree exactly. These core methods
record an admitted coordinator's assertions; they do not inspect paths, prove
quiet-time readiness, rename files or authorize an unlink by themselves.

The reserved `consume:` request namespace uses the existing durable staged
import implementation exclusively. Missing or unconfirmed ownership refuses
admission. Unpublished managed staging survives the ordinary 48-hour import
lease and reclamation; cancellation cannot discard its source recovery state.
The final publication batch atomically records the original artifact in the
consumption journal alongside catalog/record publication and the original
receipt. Receipt reclamation preserves that publication proof while recovery
ownership remains. A missing or contradictory proof is a hard failure, never
permission to allocate another occurrence.

`consumption_removed` requires the original artifact and exact claimed
observation. It records successful coordinator unlink plus directory sync and
archives the journal in ordinary 48-hour completion retry storage, freeing the
active slot. Expired completed requests cannot start another managed import
without a source reservation. Recovery journals are not pins: ordinary 30-day
retention and physical reference retirement still apply, and retry does not
renew acceptance clocks or reconstruct expired evidence.

Storage scenarios exercise reserved/claimed restart, staging beyond its former
lease, atomic publication with a lost reply, recovery after 40 days and evidence
expiry, replacement-generation refusal, idempotent removal confirmation,
completion cleanup, journal capacity and corrupted/missing publication proof.
The anchored filesystem coordinator below now supplies claim/unlink mechanics.
Watch configuration, inotify reconciliation and whole-daemon crash qualification
are still required before completed consumption is exposed as a daemon operation.
Production sample directories remain untouched.

The full `make check` gate passed with 65 storage scenarios, 12 real-daemon/MCP
scenarios, other workspace tests, release Clippy with warnings denied, seeded
census self-tests and all 80 ledger jobs. No distinct exceptions were added.
The matching release daemon was gracefully installed in the owned development
unit on 2026-10-07. It retains schema 13, its original persistent store identity,
mode-0700 directory and mode-0600 Unix socket. With no configured consumption
coordinator it remains `sparrow.store/5`; version-6 admission/reopen is qualified
in temporary storage scenarios. Live descriptor retry recovered its original
durable artifact. An unreserved `consume:` request failed with `CONFLICT`, and
native key discovery confirmed zero eligible records for its source.

## Anchored filesystem claim coordination

`consumption::ClaimRoot` is the sole filesystem coordinator for the persistent
completed-input journal. Its admitted producer root and private claim directory
remain open as directory descriptors. Child resolution uses safe Rustix Linux
`openat2` with beneath-root, no-symlink and no-mount-crossing flags; special-file
opens are nonblocking. Unsupported kernels fail explicitly. The constructor
rejects ordinary path overlap with the actual store, requires a daemon-owned
mode-0700 `.sparrow-inflight` directory, and holds a mode-0600 single-link
regular-file `flock` for exclusive coordination. A bounded private-directory
audit preserves and reports unknown entries instead of silently cleaning them.
Each transfer also checks that the held directory and coordination lock still
have their original linked inode identities; replacing the lock fences the old
coordinator before it reserves or moves another source.
No unsafe code or new semantic codec is introduced.

The journal now includes device/inode bindings for the root and original parent
directory. Its reserved request hash includes those namespace bindings. Current
version-6 journal records require these fields; missing fields fail rather than
rebind a source. No live consumption journal or production watch was created by
this change. A replaced root with active recovery ownership refuses coordinator
startup before creating another claim directory.

`reserve` requires an exact opened-file identity supplied by a readiness/handoff
scheduler. It checks the regular file and synchronizes it and its original
parent before reserving ownership. `recover` uses `renameat2(RENAME_NOREPLACE)`
into the excluded private directory; it checks the named private file against
the opened descriptor before recording a claim. The claimed file and destination
directory become durable before synchronizing source-parent removal and
committing the claim observation. A changed/missing source or occupied target
is refused and preserved for reconciliation. After that checkpoint, the old
parent is no longer an upload/removal capability: its later replacement cannot
redirect the claimed descriptor or prevent completion of the original claim.

`publish` calls the existing SDK `Client::import_open_file` over Cap'n Proto and
verifies the receipt against the same persistent store/journal. It adds no
second uploader, record preparation or publication law. `remove` recovers the
original durable publication proof, checks both held and named generations,
unlinks only the private claim entry, synchronizes its directory and then
records removal. An error leaves source recovery ownership unresolved. Recovery
of a published absent claim synchronizes that absence and completes only its
original publication; an unpublished missing claim never uploads a replacement
from the original path. The configured producer contract still forbids writes
after handoff; identity checks do not lock arbitrary producer file descriptors.

Acceptance shares one in-process storage owner with the production RPC adapter,
generated Unix client and configuration/open-store factory. Synthetic cases
cover coordinator restart after reservation, rename, publication and unlink;
exact original-byte acceptance and stable retry; replacements of old paths and
parents; unpublished removal refusal; late source rewrite; occupied private
names; missing unpublished claims; symlink escape; exclusive ownership;
store overlap; orphan preservation; and root replacement. These are component
and typed-upload checks, not whole-daemon power-loss or inotify qualification.
The daemon's startup configuration reader moved from `main` to `ServerConfig`,
so the fixture does not copy native analyzer or store setup.
Separate admission checks bound FIFO refusal by a two-second test deadline and
verify that a replaced coordination lock leaves the source untouched.

The full gate uncovered an existing race in physical accounting: native RocksDB
compaction can remove a listed file before its metadata observation. The shared
`Writer::physical_bytes` owner now retries the entire measurement at most three
times for disappearing entries. It never returns the sum accumulated before
the failure. Persistent churn refuses accounting/admission with retryable
`STORAGE_PRESSURE`; the producer keeps its input rather than latch a permanent
protocol failure. Missing
root directories, permission errors and other failures remain hard failures.
An owned sidecar create/remove scenario exercises status under directory churn.
This is a bounded point observation, not an atomic filesystem snapshot or whole
store pressure/physical-reclamation qualification.

This coordinator is not yet connected to a production watch. Inotify admission,
recursive bounded reconciliation, quiet timers, configured root overlap rules,
worker backpressure and observable watch administration remain required to
expose completed consumption. No production samples were renamed or deleted.

The full normal commit hook passed with 66 storage scenarios, 14
RPC/filesystem/MCP scenarios, other workspace tests, release Clippy with warnings
denied, seeded census self-tests and all 89 ledger jobs. There are no distinct
exceptions or unsanctioned isomorphic groups. The matching release binaries were
installed in the owned development unit on 2026-10-07 after successful graceful
shutdown. Live status retained the original store identity, schema 13 and
`sparrow.store/5` (no consumption root is configured), with 189 live records and
no pending durability writes. Descriptor retry returned its original durable
artifact and native scalar lookup returned the original message evidence.
The private directory/socket remain mode 0700/0600. This is live regression
acceptance of existing intake/query paths; source claiming/deletion was exercised
only in synthetic component fixtures.

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
sparrow SOCKET_PATH pins STORE_ID --page-size 200
sparrow SOCKET_PATH pin-get STORE_ID --name problem_investigation_1 --page-size 200
sparrow SOCKET_PATH pin-get STORE_ID --id PIN_ID --page-size 200 --cursor CURSOR
```

Use a stable request identity for retrying ingestion of the same source, format
and byte sequence. Conflicting reuse is refused. The importer hashes and uploads
completed regular files through a 64 KiB buffer, resumes the committed offset,
and verifies file stability before publication. It does not delete inputs or tail
active files. Supported explicit formats are
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
`bytes_read`, `rdf_inspect`, `pins_list`, `pin_get`, `pin`, `pin_renew` and `pin_release`. Input schemas reject additional fields;
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
requires daemon schema version 8, including finite pin catalog captures, durable pin renewal receipts, RDF profile identity, record context
and native text/profile identities;
older discovery contracts fail explicitly rather than invent those identities.

The real-daemon acceptance fixture checks CLI/MCP parity for keys, text and context, JSON
value matches, exact base64 bytes, store identity errors, pin/create/renew/release retry and
capabilities before and after daemon crash/reopen. An isolated unresponsive
socket fixture checks pending-call admission, failure during a partial input
frame, cancellation and oversized framing. Integer request IDs beyond binary64
exact precision round-trip without rounding. The full release `make check`
gate passed with these fixtures.

## Pin catalog pages

The typed `pinRead` selector union supplies active collection listing, stable-ID
lookup and case-sensitive name lookup. CLI `pins STORE_ID` and `pin-get STORE_ID
(--id PIN_ID | --name NAME)` share MCP `pins_list` and `pin_get` operations and
one renderer. MCP requires `store_id`; `pin_get` requires exactly one `pin_id` or
`name`. Both accept optional `cursor`, `page_size` and `scan_budget`. CLI spells
those flags with hyphens. Defaults are 200 requested items and 10,000 catalog
entry admissions; allowed ranges are 1..1000 and 1..100000 respectively.

Fresh reads capture eligible pin metadata at one server clock instant. Collection
rows and member references use stable ID order. Name lookup resolves once to a
stable ID, revision, actual deadline and direct members. Captured pages preserve
that result after add/renew/release, expiry or reuse of the name. The receipt
states `snapshot_id`, `as_of`, `lease_expires_at`, complete/exact execution,
catalog coverage, total items, page position, effective limits and continuation.
Pagination does not downgrade a complete capture into incomplete execution.
A cursor requires its original operation, selector and limits; changed scope is
`CONFLICT`. A retry of the same cursor returns the same domain page through CLI
and MCP. Clients must replay opaque cursors without interpreting their encoding.

These captures retain metadata only. They do not renew pins, preserve original
evidence, keep a RocksDB snapshot open or hold the catalog writer between pages.
A listed member can become unavailable to a subsequent evidence read. The absolute
lease is 60 seconds under both server UTC and a monotonic clock. Pages never extend
it. Expired/unknown/restarted-owner handles refuse with `RESULT_EXPIRED`; fresh
lookup of inactive IDs/names refuses with `PIN_NOT_ACTIVE`. New reads and periodic
maintenance reclaim expired handles. Status reports handle count, admitted
retained capacity, the eight-handle ceiling, 8 MiB capacity ceiling and lease.

One capture admits at most 10,000 items and 2 MiB retained capacity. Catalog entry
decoding is limited to 2 MiB, depth eight and 10,032 native JSON values; one capture
inspects at most 32 MiB of original catalog bytes. Scans, native storage failures,
invalid catalog identities, duplicate membership or conflicting active names
refuse the entire capture without publishing a successful prefix. Capacity growth
is admitted before extending the captured vector. Pages conservatively reserve
2 KiB per item plus 4 KiB envelope space under a 256 KiB structured ceiling, so
the requested item count is an upper bound. These are host admission capacities,
not process RSS or physical RocksDB I/O measurements. Broader query pagination
and concurrent stored-RDF snapshots remain required work.

Blocking storage workers own their concurrency permit until the worker actually
finishes. Cancelling or disconnecting the awaiting RPC does not free admission
early and create an unbounded blocking-work queue. A regression test pauses a
worker, cancels its RPC task, verifies admission remains occupied, then releases
the worker and verifies the permit returns.

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

## Artifact discovery and shared catalog leases

`Store::artifact_read` captures eligible complete artifacts and the parent
provenance of surviving records. Identical payloads retain separate artifact
occurrences. Summaries distinguish complete byte availability, exact eligible
record count and optional original metadata. After parent catalog reclamation,
the source and parent ID remain discoverable through surviving records, but the
original format, receipt time, byte count and original record count are unknown.
Fragment byte counts and record timestamps never substitute for those values.

The initial selector supports an exact source label and a half-open original
artifact receipt interval. A time-filtered capture with an applicable reclaimed
parent refuses with `INDEX_NOT_READY`; a known source mismatch can exclude it
before that check. Whole-artifact pins preserve complete empty inputs as well
as nonempty ones. Record pins preserve surviving parent provenance without
claiming downloadable complete files. The capture uses the existing retention,
protection, event-record validation and native catalog decoding owners.

One `catalog_lease` implementation now serves both pin and artifact captures.
It owns absolute wall/monotonic expiry, page construction, cursor validation,
operation/selector/limit binding and publication admission. The original pin
reader routes through it; its specialized name selects the generic page type,
without retaining a second paging implementation. Capture limits are shared
constants. Each reader has at most eight retained handles and 8 MiB of admitted
metadata, with a 2 MiB per-capture ceiling. The artifact capture inspects at most
32 MiB of catalog bytes, refuses entries over 512 KiB before native decoding,
and admits at most 10,000 summary rows. Each page has a conservative 256 KiB
structured ceiling, which can reduce the requested item count. All pin/artifact/
record visits consume the request's scan budget. Exhaustion or corrupt evidence
publishes no successful prefix or handle.

Pages preserve their captured availability/counts after ingestion or retirement;
`as_of` identifies that observation. A subsequent byte request validates current
availability independently. Captures neither pin bytes nor retain database
versions. Expiry maintenance and status purge expired handles even when clients
stop reading. Core status exposes artifact handle/byte counts and limits.

Schema 24 now connects this reader to typed `artifactsList`, CLI `artifacts
STORE_ID` and MCP `artifacts_list`. CLI and MCP share one native client and
renderer. Pin/artifact receipt encoding and rendering also each have one owner.
Typed status includes artifact handle counts, capacity ceilings and lease
duration. Full live/pin/selection and diagnostic scope support, source discovery
and corpus-scale indexed selection remain unfinished requirements.

Local qualification on 2026-10-07: final `make check` passed formatting,
release Clippy with warnings denied, all workspace release tests (100 storage
and 28 RPC/daemon/MCP scenarios), seeded census self-tests and the pinned
cross-workspace gate. All 147 ownership jobs hold with no distinct exceptions.
Evidence is `/tmp/sparrow-artifact-catalog-final-check-20261007.log`.
Artifact scenarios verify immutable source/time pages after later ingestion,
distinct repeated occurrences, changed-selector/limits and foreign-store cursor
refusal, lease expiry, surviving record parents with unknown origin metadata,
whole-artifact empty pins, frozen observations after release/retirement, no
published prefix on scan/corruption failure, full admitted 512-byte source labels
and the shared-by-clones handle ceiling. Existing pin paging and monotonic expiry
scenarios pass through the shared lease owner. No runtime dependency, store format,
wire schema or PurRDF pin changed. This does not qualify the unfinished typed
artifact-list endpoint or larger-corpus/indexed selection requirements.

## Bounded stored RDF reads

### Concurrent native snapshots

Stored RDF reads now capture one native RocksDB snapshot while validating
selected record membership, projection readiness and active pin protection under
the writer owner. Execution releases that owner. Point reads, forward/reverse
term validation, quad probes and graph declarations all use the same snapshot.
The native sequence number is exposed by `snapshot_sequence_number`; it is
read evidence rather than an occurrence identity or an investigation pin.

The database handle is shared only for lifetime ownership. Snapshot scope does
not retain source records in the current catalog or renew their expiry. A query
may finish reading its captured versions while concurrent pin release and
retention remove current evidence; later reads revalidate current eligibility.
Subsequent ingests are excluded from the captured dictionary and graph state.
The existing bounded lease, sticky resource failure and native checkpoint
boundary still apply. Native snapshot versions can delay physical reclamation;
the pressure worker already observes the native snapshot count. Snapshot release
occurs when the callback exits, including failed execution. A callback that never
returns is not preempted by the storage lease.

One native RDF snapshot per store is admitted across all store clones; another
request returns retryable `RESOURCE_LIMIT` rather than retaining another native
version or waiting in an unbounded queue. This preserves the former single-query
memory admission while allowing ingestion and maintenance to run during it.
The admission slot is released on callback return or a typed error.

The concurrency scenario releases the selected record's protection, retires it
and ingests a new record from another thread while the callback is active. It
requires that writer to finish before the callback continues, verifies unchanged
quads/graph declarations and old-term resolution, refuses lookup of the later
graph, then checks that a new read sees the later sequence and rejects the
retired record. This is native stored-view qualification, not completion of the
CLI/MCP query snapshot, result-handle or pagination contracts. Full SPARQL still
requires the upstream native workspace admission described below.

Local qualification on 2026-10-07: the final `make check` passed formatting,
release Clippy with warnings denied, all workspace release tests (96 storage
and 28 RPC/daemon/MCP scenarios), seeded census self-tests and the pinned
cross-workspace gate. All 145 ownership jobs hold with no distinct exceptions.
Evidence is `/tmp/sparrow-rdf-snapshot-final-check-20261007.log`. The final
concurrency test also verifies shared admission refusal and successful admission
after the first snapshot is released. Existing native semantic, storage-failure
and governor scenarios remain passing. No wire schema, store format, runtime
dependency or PurRDF pin changed. The owned development daemon remained active;
this local stored-view qualification does not claim a live SPARQL RPC endpoint.

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
read budget; ordinary-age selections do not scan pins. Scope capture holds the
serialized store owner; execution reads its immutable snapshot after releasing
the owner. The full live/pin/selection query scope interface remains required work.

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
correlation, final scope/pagination contracts, producer import recovery
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

The pin-catalog build reopened the same development format-5 store on
2026-10-07 UTC with discovery schema version 6. The previous owned daemon stopped
cleanly; store identity, five retained records and RDF/text profiles remained
unchanged. Live MCP discovery listed fifteen tools and marked `pin_get` and
`pins_list` as reads. Fixture pin `pin:829f88b26f505dfb273587d5551e3a81` captured
two direct members under snapshot `pin-read:c244c8654217ed0f6daed1506d4cb5a7`,
as of `2026-10-07T07:03:20.367Z`, with absolute lease deadline
`2026-10-07T07:04:20.367Z`. It was renewed, released and replaced by
`pin:f08d7a4d708309955220018177807f00`. The original cursor still returned its
revision-1 metadata and original deadline while fresh name lookup resolved the
replacement. The collection snapshot likewise retained its original two IDs.
CLI/MCP continuation receipts were identical for both operations. All fixture
pins were released; immediate status reported zero active pins, five records,
two finite handles and 2,714 admitted bytes. EOF drained with no extra stderr.
Private directory/socket modes remained 0700/0600. No production inputs or
sibling services were changed.

The catalog-page gate passed 31 storage scenarios, five core unit tests, the
server worker-cancellation regression, real-daemon/MCP crash-reopen tests and
thirteen enforced helper jobs with no duplicate exceptions. Tests distinguish
pagination from incomplete capture, exercise member/collection stability after
mutations, exact UTC and monotonic expiry, capacity/scan refusal, smaller pages
under the output ceiling, catalog identity faults, source-byte retirement while
metadata pages survive, and cursor invalidation on restart. Full first-version
server acceptance remains unfinished.

## Combined record qualification

`recordsSearch` (discovery schema version 7), MCP `records_search`, and CLI
`records` share one storage operation. Optional native text ranking runs over
records that satisfy every field constraint, rather than joining matches from
separate records in the same artifact. Without text, rows are chronological
acceptance-time summaries and have no fabricated score or ranking corpus.
The existing `textSearch` uses this same operation with no field constraints.

For example, a client-local JSON file containing
`[{"path":"/job","decoded":"fixture-7"},{"path":"/level","decoded":"ERROR"}]`
can be passed to:

```sh
target/release/sparrow SOCKET records --source diagnostic-jsonl \
  --text 'allocation failed' --where-file predicates.json \
  --received-from 2026-10-07T00:00:00Z --received-before 2026-10-08T00:00:00Z
```

MCP accepts the same array as `fields`, with optional `text`, `source`,
`received_from`, `received_before`, `limit`, `scan_budget`, and
`require_complete_index`. The criteria file is read only by the CLI; its path is
never sent as a server read capability. Unknown fields are refused.

Each constraint intersects native `path`, `kind`, `lexical`, and `decoded`
conditions on one occurrence. Lexical equality uses the retained native scalar
span, including JSON escape spelling; native string spans cover the contents
inside the quotes. All constraints qualify the same record. Constraints sharing
a `parent_binding` label must have the same native parent occurrence identity.
An optional `parent_path` chooses an exact ancestor pointer; absent it, the
immediate parent is used. Equal pointers in duplicate members do not make two
parents identical. Root binding uses `parent_path:""`. These are exact concrete
pointers; no additional JSONPath interpreter is introduced.

Rows include one supporting occurrence per predicate, its concrete path/kind,
original artifact byte bounds, and any bound parent identity. All metadata and
witnesses are captured while the storage owner is locked. Text rows, native
scores, ranking corpus identity and evidence are returned separately in the
optional typed `text` result. Raw payloads are not reread or reanalyzed.

A query admits at most 32 constraints and 256 KiB total field criteria, with
64 KiB optional text. Field occurrence/predicate/ancestor work has its own
`scan_budget` ceiling and refuses exhaustion without a result prefix. Membership
and posting work retain their reported existing limits. Chronological rows have
a 2 MiB response ceiling; ranked membership has a conservative 64 MiB capture
ceiling, measured as encoded metadata/witness bytes plus per-record allowance,
not a process RSS guarantee. Applicable failed JSON/text projections are counted;
`require_complete_index:true` refuses them. Non-JSON records are counted as
not applicable when JSON fields are requested. Limits and incomplete coverage
remain visible in the receipt.

The schema-7 operation selected acceptance time only. The explicit source-time
extension below adds event intervals in schema 8. Cursor continuation,
pin/selection query scopes, higher-precision/pre-epoch clock representations,
and the full native governed SPARQL interface remain required work.

Combined-query qualification passed `make check`: 33 storage scenarios, five
core unit tests, the server admission/cancellation unit test, 50 census tooling
unit tests, both real-daemon/MCP scenarios (including crash/reopen), seeded census
self-tests, and all 14 pinned helper-ledger jobs with no distinct exceptions or
unsanctioned isomorphic groups. Real-daemon tests cover CLI/MCP row parity,
field-only absence of text results, root occurrence `0` as a present binding,
strict failed-index refusal, and refusal of unsupported event-time arguments.

The owned development daemon was updated on 2026-10-07 at approximately
07:29 UTC to discovery schema 7, PID **446585**, using the same configuration,
private socket, and `store:46c88e949ccec658600b66c7becd5bb7`. Its prior process
exited cleanly on SIGINT. Status retained five records, four payload chunks, one
JSON projection and no active pins or pin read handles; store format remains
`sparrow.store/5`. No migration, production import or producer lifecycle action
was performed.

Live CLI and stdio MCP combined queries for `diagnostic-jsonl`, text
`allocation failed`, `/job` decoded `fixture-7` and `/level` decoded `ERROR`
returned the two existing synthetic JSONL occurrences with complete/exact
receipts. Both predicates bind root occurrence `0`. Original `/job` evidence is
`[84,93)` and `/level` evidence `[40,45)`. Field-only queries return the same
metadata/witness rows with `text:null`. Native ranking uses two documents and
48 tokens, corpus
`8e28d99ceee6e7073cbef16582bbe985fa97bb420ad62785f5015f5003c2675c`, identical
through CLI and MCP. The MCP adapter exited cleanly on EOF.

## Explicit source-time profiles and event intervals

Discovery schema 8 adds typed `sourceTimeDefine` and `sourceTimeGet` RPCs,
MCP `source_time_define` and `source_time_get`, and CLI
`source-time SOURCE POINTER REPRESENTATION` / `source-time-get SOURCE`.
A definition durably binds one exact source name to a concrete native JSON
pointer and one declared representation. Identical retries return the same
content-addressed profile; conflicting definitions return `CONFLICT` and require
a new logical source name. Define the profile before importing that source.
A later definition never reinterprets older records or changes an ingestion
retry receipt. Profile lookups return explicit absence for undefined sources.
The registry admits at most 1,024 mappings, bounds source names at 512 bytes and
pointers at 4,096 bytes, validates native pointer escapes and catalog identity,
and refuses physical control-admission pressure.

Representations are `rfc3339`, `unix_millis`, and `unix_seconds`. RFC 3339 values
must be native decoded strings with an explicit offset, at or after the Unix
epoch, exact to the existing millisecond clock. Integer epoch representations use
PurRDF's native XSD integer lexical reader and checked unit conversion. Fractional
or exponent-form numeric values are not silently rounded or guessed as another
unit. Out-of-range, pre-epoch, ambiguous-zone and unsupported precision values
remain unknown with `invalid_instant`. Higher-resolution and other producer
clock laws remain required work; these profiles do not claim to cover them.

Every new record durably captures either a known source instant, immutable
profile identity, native occurrence ID and original artifact byte span, or an
explicit unknown reason: `no_profile`, `not_json`, `projection_failed`,
`missing_field`, `ambiguous_field`, `wrong_kind`, or `invalid_instant`. Duplicate
matching timestamp pointers are ambiguous even if their lexical values agree.
Older records without this additive catalog field report `legacy_unprofiled`.
They are never reparsed using a newly registered profile. Missing/broken declared
profile references or source-time evidence outside the record are storage failures.
Known instants additionally appear as typed `eventTime` metadata RDF literals
and link their `sourceTimeProfile`; they do not replace `receivedAt` or `expiresAt`.

For example:

```sh
target/release/sparrow SOCKET source-time copied-events /timestamp rfc3339
target/release/sparrow SOCKET records --source copied-events \
  --event-from 2026-09-27T00:00:00Z --event-before 2026-09-28T00:00:00Z
```

`records_search` accepts paired `event_from` and `event_before` bounds, plus its
existing text/field and acceptance-time filters. CLI uses paired `--event-from`
and `--event-before`. Bounds are UTC half-open intervals. Unknown source times
are excluded, with `event_coverage.unknown` counted among live source/acceptance
scope records. `outside_interval` counts known source times outside the interval;
`evaluated` is their shared population. `membership_complete:false` identifies
an incomplete bounded catalog scan, so these counts must not be treated as exact
store-wide totals. Chronological event queries sort by source instant and stable
record ID before applying the row limit. Optional text ranking runs only over
qualified event-time members, with the existing native corpus receipts.

The response explicitly reports `time_basis:event_time`, each row's known time
and supporting spans, and event coverage. With no event bounds it reports
`received_at` and preserves acceptance ordering. Record metadata and context
also expose the captured event-time state. Expiry and pins remain based on
acceptance/retention deadlines, independently of old, future or skewed source
clocks. Cursor pagination and event-ordered context are not yet implemented.

Source-time qualification passed the full `make check` gate: 38 storage
scenarios, five core unit tests, the server admission/cancellation unit test,
50 census tooling unit tests and five tooling gate scenarios, both real-daemon/
MCP scenarios including crash/reopen, seeded census self-tests, and all 17
helper-ledger jobs. No distinct exceptions or unsanctioned isomorphic groups
were added. Native governed stored-view SELECT verified the new `eventTime`
RDF literal and immutable profile link. Strict event-time index requests also
refuse source-clock capture that depended on a failed applicable JSON projection.

Before cutover, the rebuilt client refused an event-time request against the
still-running schema-7 daemon with `UNSUPPORTED` and no stdout result rows,
because that daemon did not honor the requested time basis. The adapter checks
the returned typed time basis rather than accepting a silently ignored filter.

The owned development daemon was updated at approximately **2026-10-07
08:06 UTC**, PID **595446**, discovery schema **8**, using the same private
socket, configuration and `store:46c88e949ccec658600b66c7becd5bb7`. The previous
owned process exited cleanly on SIGINT. Store format and native RDF/text profile
identities remain unchanged. Directory/socket permissions remain 0700/0600.

Live acceptance used only a copied, complete **962-byte first record** from
`~/Active/lillith_soul/inner-core/corrections/2026-09-27.jsonl`, rather than
claiming the entire producer file. Its device, inode, size and modification time
were unchanged around reading and qualification. CLI export matched every copied
byte, SHA-256
`46aa5ddbc5c4f04e9c4bf96533aa0931628ca41b22222891d36ff502ca378ed1`.
No producer file was renamed, deleted or consumed.

The declared source `copied-correction-source-time-v1` uses `/timestamp` and
`rfc3339`, immutable profile
`time-profile:961b463f0ed6a30f7031df16ba0348e82f61c0848196e8ee0046574f8df4ef2e`.
The copied artifact is `artifact:476f80ab8f332b28801d5383902a9c4f`, record
`record:08a1adcc30816e2f98f4c0db4ebc7da1`. Its source instant is
**2026-09-27T00:13:37.406Z**, native occurrence **5**, exact span **[171,195)**;
its independent acceptance time is **2026-10-07T08:06:59.738Z**. CLI and MCP
event-interval queries returned identical rows and complete coverage with one
evaluated record, zero unknowns and zero outside the interval. Adding an
acceptance-before bound of September 28 excluded the record, demonstrating
that event and acceptance filters intersect independently. MCP record metadata
reported the same native event evidence, and the adapter exited cleanly on EOF.

The development store now retains six records, five payload chunks, two shared
JSON projections, no active pins and no pin read handles. This qualification
covers one copied source record; streaming, watcher/producer lifecycle, whole
corpus coverage, source-clock laws beyond this millisecond profile, pressure/
performance acceptance, and full governed SPARQL remain unfinished.

## Bounded unpublished streaming journals

The core now provides `import_begin`, `import_get`, `import_append`,
`import_read_chunk`, `import_seal`, `import_cancel`, and `import_reclaim` as the
storage foundation for streaming imports. The schema-9 daemon and CLI now expose
this path, as qualified below. **Sealing does not publish an artifact or records,
and never authorizes source deletion.**

Beginning an import binds a request identity to the explicit source, format,
complete length and canonical BLAKE3 payload identity. At most 16 journals occupy
the registry; each catalog row is bounded at 8 KiB, including JSON escaping of
source/request labels. The journal contains counters rather than a growing
chunk array. Its fixed 48-hour recovery lease is not extended by activity.

Append accepts one aligned 64 KiB chunk, except for the declared final remainder.
Each synced WAL batch atomically commits the bytes, a native BLAKE3 chunk digest,
and the next offset. Matching retries return the committed journal; changed
bytes, gaps, partial non-final chunks and changed declaration arguments are
refused. Staging uses private payload keys outside evidence references and
never adds searchable records. Disk admission includes staging batches and
preserves the configured maintenance reserve.

Sealing checks every chunk length/digest and computes the declared whole-input
identity with PurRDF's bounded streaming BLAKE3 engine. It does not buffer the
whole file. Cancellation ends the lease durably. Maintenance reclaims at most
128 expired chunk, layout or preparation units per call and atomically advances each journal's cleanup
cursor; canceled/expired inputs cannot be read or resumed during cleanup.
Physical blob/WAL reclamation still depends on native engine maintenance.

Focused qualification reconstructs CRLF, invalid UTF-8, NUL and a final
unterminated fragment after store reopen; verifies retry/conflict behavior,
complete digest refusal, empty input and the 16-journal ceiling; and stages a
130-chunk input larger than the buffered artifact limit. Cleanup resumes after
reopen from its 128-chunk cursor and removes the remaining two chunks. These
are core store-reopen scenarios. An additional owned test writer is killed with
SIGKILL after acknowledging a synced chunk; reopening recovers its exact offset
and bytes, accepts the final remainder and verifies the whole digest. This is
not daemon streaming or completed-import publication acceptance. The subsequent
publication and schema-9 sections qualify those additional boundaries.

The full `make check` gate passed with 41 storage scenarios, including all three
new streaming journal scenarios, release Clippy with warnings denied, all other
workspace tests, seeded census self-tests and all 19 helper-ledger jobs. No
distinct exceptions or unsanctioned isomorphic groups were introduced.

### Private paged record layouts

`import_prepare_layout` now prepares exact source record spans and canonical
record-byte BLAKE3 identities after sealing. It streams the checked raw chunks
through native hashers and retains at most one 64 KiB chunk and one 128-record
page, including for records larger than the buffered artifact limit. Buffered
ingestion and layout preparation call the same raw delimiter implementation.
CRLF remains intact, delimiters belong to their preceding record, and a
completed final unterminated line becomes a record. Empty JSONL/log inputs
have zero records; empty JSON, Markdown and opaque artifacts have one record.

Each private page is at most 32 KiB, carries a checked native content digest,
and commits atomically with the journal's prepared-record counter. The complete
layout flag is committed only after all byte spans and the complete raw digest
have been checked. Record-count or disk-pressure refusals retain private partial
pages and raw staging. A retry rebuilds the deterministic pages without creating
occurrences and preserves previous cleanup coverage. Old raw-only journals
decode with an explicitly unprepared layout.

`import_layout_page` requires a complete layout and an aligned record ordinal,
returns at most 128 spans plus the complete count and next ordinal, validates
source ranges/ordering and contiguous neighboring pages, and uses the same
fixed recovery lease. Missing/damaged pages are storage failures rather than
empty results. Cancellation/expiry cleanup shares its 128-unit budget between
raw chunks and layout pages and resumes both counters after reopen.

Qualification covers CRLF split across chunks, empty records, a binary final
unterminated record larger than the buffered artifact limit, 130 records across
two pages, native record-digest identity, empty inputs for every format, partial
record-limit refusal followed by reopen/retry, and damaged/missing page refusal.
Cleanup removes both raw and layout pages across a bounded restart boundary.
These layouts remain unpublished core preparation. Native record preparation
and final publication are described below. Cap'n Proto streaming caller wiring
is qualified in the schema-9 section below.

The full `make check` gate passed for the layout change with 44 storage
scenarios, all other workspace tests, release Clippy with warnings denied,
seeded census self-tests and all 21 helper-ledger jobs. No distinct exceptions
or unsanctioned isomorphic groups were added.

### Shared record preparation and staged range reads

Buffered ingestion now calls one internal `RecordBatch` to prepare native
JSON occurrence facts, JSON/Markdown RDF, metadata RDF, persisted text,
captured source time and exact raw chunk ownership. Its finalizer writes the
same shared projection/content reference counters and disk-admission estimate.
Catalog publication and retry receipts remain the caller's atomic commit.
Buffered imports retain record-count preflight before native preparation, using
an allocation-free boundary count rather than an artifact-sized boundary vector.
The former inline native preparation body was removed rather than retained as
a second implementation for the future streaming caller.

`import_bytes` materializes exact ranges of committed staging bytes, including
unsealed inputs, at a maximum of 1 MiB per call. It refuses bytes beyond the
committed offset, overflow, expired/canceled journals and excessive output.
It reads only the intersecting checked staging chunks. Staging and ordinary
evidence byte reads share one exact range assembler; input-specific chunk digest
and lifetime checks remain at their respective storage boundaries. Binary
content, cross-chunk ranges and zero-length reads remain exact after reopen.

The shared builder now also serves private native preparation and final
publication described below. Typed daemon caller wiring is qualified below.

The full `make check` gate passed for shared preparation and staged byte reads:
45 storage scenarios, all other workspace tests, release Clippy with warnings
denied, seeded census self-tests and all 23 helper-ledger jobs. Existing native
projection, ranking, exact-byte, pin/retention and daemon crash/reopen scenarios
continue to pass through the shared production preparation path. No distinct
exceptions or unsanctioned isomorphic groups were added.

### Checkpointed native preparation and complete publication

The core adds `import_preparation_get`, `import_prepare_record`, and
`import_publish`. Preparing one expected record ordinal commits its native
JSON/Markdown, metadata RDF, text and normalized exact-byte dependencies in
the shared builder's synced batch, together with its private record catalog
entry and next preparation ordinal. No ordinary record catalog or temporal
membership is written during preparation. Queries, metadata lookup and pins
cannot treat a prepared prefix as published evidence. Retrying an older
ordinal returns existing progress; skipped ordinals are conflicts. Reopen
preserves progress, reserved artifact identity and all dependency ownership.

The first preparation attempt durably captures one server receipt clock and
immutable source-time profile identity (including explicit absence). All
occurrences use that receipt clock and its 30-day ordinary deadline, independently
of the time spent preparing their dependencies. A newly declared source profile
does not reinterpret the already captured preparation context. The final
availability boundary is the atomic publication commit, rather than a claim
that the captured receipt clock is an exact commit timestamp.

Publication checks every private record's provenance, uniqueness, original
span, captured source-time evidence and metadata RDF presence. It streams the
normalized raw chunks through the shared checked payload reader, verifies each
record's native digest against its sealed layout, and verifies the whole
artifact digest and full byte cover. It then commits the complete artifact,
ordered record catalogs, temporal membership, published staging marker and
retry receipt atomically, after the preceding synced dependency batches.
Final metadata plus receipt is bounded to 128 MiB, additionally constrained
by catalog decoding bounds, current record limit and disk admission. A failed
check/refusal does not publish an artifact or a record prefix.

The final publication receipt lasts 48 hours after publication and can be
retrieved after raw/private staging cleanup. Repeated beginning or publication
of the same declared input returns the published identity; conflicting request
arguments fail. Published evidence cannot be canceled as staging. Its normal
retention/pin rules apply. Expired staging cleanup deletes private record copies
without decrementing transferred published references. Abandoned preparation
releases both reserved artifact and record payload ownership, its projection
references, metadata RDF and text through the same retirement implementation as
ordinary expiry. Cleanup admits at most one prepared record within its shared
128-unit chunk/layout/control budget per call, with durable recovery cursors.

Core qualification publishes a file of **16 MiB plus two bytes**, above the
8 MiB fixture buffered limit, as one complete artifact with two records.
Bounded exports reconstruct every byte and native whole-input identity; its
oversized text projections remain explicit failures without losing raw bytes.
A record pin survives parent expiry and releasing it reclaims all remaining
payload references. Further scenarios cover private-prefix invisibility,
native JSON/RDF/source-time capture, reopen and ordinal retries, publication
retry after staging cleanup, canceled shared dependencies, and missing prepared
payload refusal with no published prefix.

This subsection records core publication qualification; typed RPC/CLI and
daemon crash qualification follow below. This initial core implementation
materialized at most **16 MiB per individual record** and refused larger records.
The schema-10 extension below removes that raw-publication refusal. Measured
RSS/disk-pressure and whole-sample acceptance remain required.

The full `make check` gate passed with 50 storage scenarios, including the five
new native publication/retirement scenarios and cleanup-budget exhaustion across
multiple journals. All other workspace tests, release Clippy with warnings
denied, seeded census self-tests and all 26 helper-ledger jobs passed. No
distinct exceptions or unsanctioned isomorphic groups were added.

### Schema 9: typed resumable file imports

`importBegin`, `importAppend`, `importGet`, `importSeal`, `importLayout`,
`importPrepare`, `importPublish`, and `importCancel` are generated Cap'n Proto
RPC methods over the same private Unix socket. References carry persistent
store identity; wrong-store imports fail before storage mutation. Source/request
labels are checked before copying, and append payloads are limited to 64 KiB
before copying into a storage worker. Every storage operation uses the existing worker admission
permit, including its cancellation ownership law.

Beginning returns an explicit union: unpublished staging progress, or the
original accepted artifact. Buffered and streamed ingestion share the same
request receipt and compare source, format, complete length and native BLAKE3
identity. Existing buffered receipts and earlier publication receipts remain
readable through their recorded artifact fields. A buffered request cannot
create another occurrence while its journal exists. Conflicting request reuse
fails; separate requests with equal bytes remain separate occurrences.

The CLI `ingest REQUEST_ID SOURCE FORMAT FILE` now hashes and uploads through a
single 64 KiB buffer, seeks to the daemon's durable offset on retry, seals,
prepares native records by expected ordinal, and requests final publication.
There is no whole-file `read_to_end` path. It verifies the opened regular file's
device, inode, length, mtime and ctime before upload and before publication,
including nanoseconds. A detected change fails without returning an acceptance
receipt; transport failure preserves staging for the stable request retry.
Files are never renamed or deleted by this command. Schema-8 daemons are refused
before staging; use matching executables.

The real-daemon test kills only its owned children after a committed chunk and
a native record checkpoint. Reopened stores preserve the exact offset and
reserved artifact identity. The production CLI resumes preparation and publishes
one complete artifact; retry returns the identical receipt. Bounded byte reads
reconstruct every source byte. Private preparation is inaccessible through
artifact lookup. Another CLI fixture accepts 8,388,609 bytes despite the daemon's
8 MiB buffered-artifact limit. Core tests cover both directions of retry across
the buffered and streamed paths, conflicting arguments, and prevention of a
second occurrence. The client file guard also rejects a same-length rewrite.

These schema-9 checks did not establish oversized individual record publication
beyond 16 MiB. Producer consumption/watchers, whole-sample throughput or RSS limits,
full governed SPARQL, logging-client delivery, or complete architecture acceptance.

The full `make check` gate passed with 51 storage scenarios, two server unit
checks, three real-daemon/MCP scenarios, the remaining workspace tests, release
Clippy with warnings denied, seeded census self-tests and all 30 ledger jobs.
No distinct exceptions or unsanctioned isomorphic groups were added.

On 2026-10-07 the owned development daemon was replaced with the matching
schema-9 release executable. Its endpoint remains
`/home/paudley/.local/state/sparrow/development-v5/rpc.sock`, mode 0600, inside
the mode-0700 store directory. Persistent store identity remains
`store:46c88e949ccec658600b66c7becd5bb7`.

A temporary, completed copy of
`lillith_soul/inner-core/corrections/2026-09-26.jsonl` was imported through the
production CLI: 167,222 bytes, 171 records, artifact
`artifact:022887776d4345646bdc2ffc5d4dd406`, payload
`blake3:bfb073b4e6367b07412200888a631546ea24512643a517e64b4df490feae9214`.
The repeated request returned the identical durable receipt; full bounded export
matched every byte; source bytes remained unchanged. Native key discovery for
this source returned 41 rows. This is one-file live acceptance, not whole-corpus
or throughput qualification. No producer file was claimed, renamed or deleted,
and no sibling/model service was managed.

### Schema 10: oversized raw records with bounded preparation

Raw publication no longer requires materializing an entire record. Records above
the 16 MiB projection admission boundary prepare at most 128 normalized 64 KiB
chunks per call. Each synced batch commits payload bytes, shared occurrence
reference increments, one checksum-protected chunk-identity page, and the raw
checkpoint together. Chunk boundaries are relative to the record; a record
starting inside an upload chunk is assembled exactly from the checked source
ranges. The persistent reserved record ID survives reopen and is inaccessible
through public record lookup before complete publication.

`ImportPreparation.raw` is an explicit typed optional value carrying the record
ordinal, reserved record ID, prepared chunk count and complete chunk count.
The CLI advances this durable progress, rejects nonadvancing or inconsistent
responses, and publishes only after every record is complete. Repeating a current
record ordinal advances from its saved raw chunk cursor rather than registering
the previous page twice; an older record ordinal only recovers progress.

After all raw pages are durable, final record preparation streams checked
normalized payloads through native BLAKE3 and compares the exact complete record
length and sealed digest. It then uses the existing `RecordBatch` implementation
for projection state, captured source-time profile, text state and native RDF
metadata. JSON refusals use the native profile's public typed source-byte limit.
Markdown materialization is explicitly limited to 16 MiB; text remains limited
to 1 MiB. Opaque content remains not applicable to those projections. Applicable
limits produce persisted failed diagnostics, not empty successful projections.
The source-time reason for an oversized profiled JSON record is
`projection_failed`; receipt-time retention remains independent.

Buffered records and raw checkpoints share `Writer::prepare_payloads` for
content deduplication and two-owner reference increments. Existing normalized
content must agree with its native digest and positive ownership counter.
Final preparation transfers those references to the private complete record;
publication transfers them to ordinary record/parent evidence without another
increment. Abandoned raw checkpoints retire at most one 128-entry page per
cleanup step through `Retirement::payloads`. Shared content belonging to another
accepted occurrence survives. Completed private records use the same retirement
implementation as ordinary expiry. Layout and raw identity pages share one
bounded native-JSON/checksum codec.

The raw checkpoint is bounded at 8 KiB; identity pages at 32 KiB. A raw chunk
batch holds at most 8 MiB of distinct source bytes before batch encoding. Final
record preparation retains ordered chunk identities, not complete source bytes,
and refuses a manifest exceeding the native catalog string budget. Final
artifact metadata remains limited to 128 MiB and its native decoder limits;
disk admission still reserves maintenance capacity. Those resource refusals
retain private retry/cancellation state and do not publish a prefix. Whole-record
and whole-artifact digest checks perform work proportional to the declared input;
latency, peak RSS, physical reclamation and sustained pressure remain unqualified.

Core scenarios verify a valid 17 MiB JSON record starting at byte three,
128/256/273-chunk checkpoints across reopen, complete artifact/record byte reads,
failed JSON/RDF/text coverage, strict index refusal, captured unknown source time,
record-only pin survival and eventual reference reclamation. Separate scenarios
cover canceled partial preparation sharing content with an existing occurrence,
missing normalized payload refusal without publication, and oversized JSON,
Markdown and opaque document behavior. The real-daemon scenario now resumes an
oversized record after a raw ownership checkpoint and preserves its reserved
record identity through the production CLI and exact export.

This qualifies oversized raw ingestion, not producer consumption/watchers,
logging-client delivery, full governed SPARQL, complete query scopes/pagination,
whole-corpus performance, or full architecture acceptance.

The full `make check` gate passed with 55 storage scenarios, two server unit
checks, three real-daemon/MCP scenarios, all other workspace tests, release
Clippy with warnings denied, seeded census self-tests and all 35 ledger jobs.
There are no distinct exceptions or unsanctioned isomorphic groups.

The owned development daemon now runs the matching schema-10 release executable
at the same private socket and persistent store identity. A synthetic completed
JSONL file containing one 17,825,807-byte record was accepted durably as
`artifact:f462b1c1f32180fb4e77c29194c3677d`, record
`record:5bcf538161b77b607f723b761c66333c`, payload
`blake3:9e453ecc3570700ac96f213aa692d46db7a6b2e27d1e6df898075041246ac32f`.
Stable retry returned the identical receipt; consecutive CLI byte ranges of at
most 1 MiB matched the complete original record. Native RDF inspection reported
the explicit failed source-byte projection, and key discovery reported one
eligible record with one failed JSON projection. This exercises live oversized
raw acceptance and coverage reporting, not throughput or memory qualification.
No producer file or sibling/model service was modified.
