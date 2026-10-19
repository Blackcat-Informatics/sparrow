# Sparrow implementation plan

Status: implementation in progress. Helper-census tooling, a RocksDB storage
foundation, the Cap'n Proto daemon, bounded JSON occurrence discovery and persistent
native full-text retrieval and bounded acceptance-time record context through
shared CLI and stdio MCP clients are present. Native JSON/Markdown content and
record metadata now persist through a canonical RDF dictionary and six physical
quad permutations; typed inspection exposes projection state and stream counts.
The native stored view now supports bounded lazy reads and governed
SELECT, ASK, CONSTRUCT and DESCRIBE through the Rust core API, with native typed
substitutions, retained outcomes and scoped membership/coverage. The CLI/MCP and
typed RPC graph surface is implemented and deployed. Recordless
artifact RDF and independent artifact coverage are deployed in schema 52;
native correlation is deployed in schema 58 through the same governed
query and retained-result owners. Text relations and public explanations remain
required. The
integration state is recorded in
[PurRDF integration gaps](purrdf-integration-gaps.md).
Independent Artifact-class metadata for populated artifacts and persisted
Source/Stream class declarations are implemented and deployed in schema 57.
Deployed schema 59 adds occurrence-owned tracing relationships from the existing
native JSON facts, including lifecycle observations, producer-scoped span IDs,
ordered context, declared parent/follows-from evidence and exact context spans.
Its bounded metadata backfill preserves existing authority families and retention.
Runtime adoption and source qualification remain separate evidence surfaces.
The full server acceptance criteria remain unmet. Current executable surfaces
and verification are recorded in [Development runtime](development-runtime.md).
Stable-ID pin renewal is implemented through the core, typed daemon, CLI and MCP,
including durable retry receipts and optional revision guards. Pin discovery
and member retrieval now use finite immutable catalog pages shared by the typed
daemon, CLI and MCP. Artifact and source discovery share the catalog lease owner;
source inventory includes exact intake keys, mapping IDs, retained occurrence
counts and configured watch bindings. Producer continuity remains explicitly
unknown. Source discovery includes finite producer windows and active/recent
terminal file-generation checkpoint IDs, including empty inputs; checkpoint/gap
evidence does not imply complete history.
Artifact discovery now admits live, explicit evidence and revision-guarded pin
membership through the same reference owner as record queries, including
recordless artifacts and frozen pages after pin release. Artifact source-array
and diagnostic filters use the shared record matcher, with frozen coverage,
unknown counts and strict refusal. Record queries, including same-record fields
and native ranked text, now use immutable result pages with frozen receipts and
ranking corpus evidence. Standalone text search shares those captures, native
ranking, operation-bound cursors and aggregate handle admission. Key/value discovery now shares those leases and captures complete native occurrence
rows or key counts before paging. Cursor-only result_next resumes all existing typed page families through their
shared lease and canonical renderers.
Optional artifact grouping uses the same captured record matcher and native
ranking, with frozen per-artifact counts and page-local row references across
typed RPC, CLI and MCP. Its schema-43 live and copied-corpus evidence is recorded
in [Development runtime](development-runtime.md#frozen-artifact-match-groups).
Existing paged query receipts now freeze
capture identity, native profiles and effective host admission ceilings while
preserving original coverage and certainty. Native graph captures retain the
engine's certified governor outcomes. Frozen logical scope receipts are deployed
for record/text/key/value queries in schema 56 and graph/context in schema 57.
Production-scale indexed-selection and resource acceptance remain required.
Key discovery now supports exact native RFC 6901 path restriction through the
same occurrence matcher, typed client, CLI and MCP; paths and complete counts
remain frozen across continuations.
Context now reports single-capture identity and effective directional, catalog
and response admission; byte exhaustion refuses without a successful prefix.
The standalone producer client and schema-11 typed event batch operation are
present. Native event JSONL enters the same occurrence/field/text/time indexes;
the bounded producer queue now reconnects with immutable pending batches and
provides durable admitted-input flush/shutdown fences. The tracing layer, typed
span/context capture, explicit buffered-WAL acknowledgements and flush-through,
and finite producer sequence windows are implemented. Public TCP shares the
generated service and bounded Rust logging worker with Unix. Bounded syslog
listeners and packet interpretation, active-log generations, durable tail
checkpoints, observed rotation/gaps, producer-finish completion and explicit
completed-tail handoff are implemented. Production operational qualification
remains milestone-4 work; source/runtime evidence is recorded separately in
Development runtime.

This plan builds on [Architecture](architecture.md) and [Interfaces](interfaces.md).
It records the initial query priorities and sample discovery supplied on
2026-10-06. The existing milestone dependencies remain: qualify storage and
lifetime first, then import, then query surfaces, then continuous producers.

## Current acceptance boundary

The following gaps preserve the original first-version scope. Installed image
identities and source qualification are recorded separately in
[Development runtime](development-runtime.md). Finite fixtures do not establish
production capacity.

| Required area | Evidence already present | Required work still NOT MET |
| --- | --- | --- |
| Graph queries | Deployed schema 61 supplies SELECT/ASK/CONSTRUCT/DESCRIBE and measured EXPLAIN through typed RPC/CLI/MCP, substitutions, certificates, frozen scope/coverage, correlation and tracing. Production-feature private/live qualification returns the original 2,990 triples in 43 pages under unchanged limits. Source schema 62 adds borrowed stored-text rank/span relations through PR #615; focused core/transport regressions and the full gate pass. | Finish schema-62 production-image qualification and runtime adoption. The landed scoped producer API resolves the prior availability gap; no further upstream API is a prerequisite. [Native integration evidence](purrdf-integration-gaps.md). |
| Reclamation | Shared native tombstone-compaction policy; real-pressure source regression with measured file decrease, successful retry, protected shared bytes and reopen; full release gate at signed 22ec801. The policy is now adopted by the qualified schema-61 daemon. | Sustained physical/resource qualification of the existing native compaction/blob-GC path. No new upstream API is a prerequisite. [Native reclamation](native-reclamation.md). |
| Agent contracts | Shared typed operations and CLI/MCP rendering, capability discovery, temporal/field/text/key/value/context evidence, pins, retained results, measured EXPLAIN and cancellation. Live schema 61 advertises 38 tools, with TCP/Unix/MCP parity and zero leftover handles/bytes after qualification. Working schema 62 carries stored-text corpus/profile/coverage receipts and actual native relation descriptors. | Qualify those new contracts over TCP/Unix CLI/MCP, then complete resource/latency qualification over the deployment corpus. [Interfaces](interfaces.md). |
| Final qualification | Release gate, twelve real completed-file SIGKILL windows, actual owned-kernel queue overflow and configured directory-limit recovery; writable/read-only closes, multiple writers, moved-in files and stale path events; sixty-day synthetic retention/pin cohorts with final logical graph closure. | Measure sustained/burst input, queries and maintenance on a declared deployment envelope and complete physical recovery measurements. Quota-specific checks apply when a strict filesystem ceiling is configured. [Crash/watch checks](interfaces.md#completed-file-process-crash-qualification), [turnover](turnover-qualification.md), [performance](performance-baseline.md). |

The qualified kernel-overflow fixture changes no host limits; configured
directory-budget exhaustion is independent of exhausting the shared kernel
watch quota. The SIGKILL matrix starts from durable reservation and does not
prove power-loss persistence at arbitrary filesystem boundaries. The finite
turnover corpus is 65,642,380 raw bytes per pin mode. None of these results closes
the remaining production acceptance gates. Host reboot and power-loss trials
are not additional first-version gates: the architecture's crash scenarios and
the durability claims actually made determine the required checks.

The initial upstream readback on 2026-10-18 found #600, #601 and #605 open.
Later main landed actual-payload materialization (PR #609, closing #605) and
governed request EXPLAIN (PR #611, closing #600). The preceding source selected
`9b2bf3b71325154137bc1aa817b46356daadfe3a` for both runtime dependencies and
census discovery. Three focused release consumer regressions and release Clippy
pass, covering retained complete materialization and request/governor EXPLAIN
parity, including host refusals. Signed `126248c` passed the full gate for that
adoption. Source schema 61 adds the public EXPLAIN adapters, with three additional
release consumer regressions, release Clippy and pinned census passing. The
normal full gate passed at signed `ffdfbbf`, and production-feature images now
pass private-copy and live adoption with exact-byte, profile and generation
continuity. A later upstream refresh finds #601 completed by PR #615 at
`a47940e1f130bf3739c6d59aaf89643209905ea4`, an ancestor of observed main
`25de0d0de0856c7b88288d0ee65777bd1e95b34b`. Its scoped registry/cursor API is
available. Source dependencies and the census reference now select that exact
landed commit. Source qualification and stored-text consumer implementation
remain separate from the installed schema-61 image, which still uses `9b2bf3b`.
The selected rust-rocksdb upstream remains
`46d7e4f10dd9f8dbb219893592a1cab9bdc109c6`. The earlier inference that Sparrow
required a new file-compaction API was unproven and is withdrawn. Existing native
policy and measured pressure recovery must be evaluated before declaring an
external dependency blocker.

## First useful outcome

An AI agent can import a diagnostic corpus, discover its sources and fields,
search logs and JSONL by text, time, and field values, inspect surrounding
records, retrieve the original bytes, and pin evidence for an investigation.
Every result states its scope, time basis, index coverage, and execution
completeness. Agents can distinguish no matches from missing or unindexed data.

The first query surfaces are full-text log search, temporal and structured-field
log search, full-text JSONL search, key discovery, and value search. An agent
should not need to know a source's schema or write SPARQL before obtaining useful
evidence. SPARQL remains the composable graph query surface.

| Investigation | Operations and required evidence |
| --- | --- |
| What failed during this interval? | Search text and diagnostic fields within an explicit time interval; return occurrence IDs, timestamps, source, and supporting spans. |
| What fields does this producer emit? | List observed JSON pointers and normalized diagnostic fields in the selected source/time scope, with kinds, presence counts, and coverage. |
| Where does this job or request appear? | Exact field-value matching across sources; return matching field occurrences and parent records, preserving independent log occurrences. |
| Which JSONL files contain a match? | Search record-level text, group matching references by artifact, and retain per-record byte offsets; do not replace the complete artifact with matching lines. |
| What happened before and after this event? | Fetch bounded source-local context with an explicit source-order or temporal basis and visible retention/capture gaps. |
| Can I inspect and preserve the evidence? | Read original byte spans and pin concrete record/artifact IDs; subsequent reads do not renew protection. |

## Sample discovery

Read-only discovery used these local locations. The supplied `lillith-soul` path
resolves here under the repository spelling `lillith_soul`.

| Location | Observed evidence and implication |
| --- | --- |
| `/var/log` | Plain and rotated logs, compressed files, and native `.journal` files coexist. Several directories/files are unreadable to the current user. Corpus discovery must report inaccessible inputs rather than claim complete coverage. |
| `~/Active/lillith_soul/inner-core/corrections` and `discards` | JSONL with timestamp, component/producer, process/sequence, nested objects/arrays, strings, integers, booleans, and nulls. Source profiles must map diagnostic fields without limiting discovery to those mappings. |
| `~/Active/lillith_soul/inner-core/private-mentation` | A live JSONL file measured about 153 MB at inspection, with rotated segments. Its sampled records include deep nested content and a line of 153,749 bytes. Size-bounded streaming and oversized-record cases are required. |
| `~/Active/lillith_soul/inner-core/inference-evidence/brainstem-terminal-v2` | Many small artifact bundles. All six inspected `.bin` files were valid UTF-8 JSON, including accounting, allocation, job, and terminal timing fields. Extensions alone cannot establish an artifact's format. |
| `~/Active/lillith_brainstem` | The source/evidence tree contains many JSON files, including large generated analysis artifacts. A repository-wide sample selection must distinguish diagnostic evidence from source and generated tooling data. |

This is preliminary discovery, not a corpus census or benchmark. The inner-core
inventory stopped at 25,000 file entries and does not establish total size or
file counts. Structural JSONL inspection read at most 2 MiB per selected file and
considered at most 100 complete lines: 17 correction records, 8 discard records,
and 100 mentation records parsed successfully. No complete malformed line was
observed in those samples; that does not establish corpus-wide validity.
The inspected `/var/log/pacman.log` prefix contained 870 bracket-prefixed lines
within 64 KiB. Binary inspection read at most 64 KiB per selected `.bin` file.

Production sample bodies, identities, prompts, and outputs stay outside tracked
source. Build synthetic fixtures reflecting observed structure and edge cases.
For performance qualification, make bounded immutable local copies outside source,
record their digests and observation bounds, and measure whole-store costs.
Reading a live file does not establish a stable corpus fingerprint.

Native journal decoding is not yet designed. Preserve opaque journal bytes if
imported, but report text coverage as not applicable until a qualified native
adapter or explicitly identified journal export is available. Likewise, qualify
compressed-input decoding separately from preserving its original compressed
bytes. Do not present either representation as searched plain text without that
adapter. Unknown `.bin` files remain opaque; the six JSON samples do not classify
all files with that extension.

## Query contracts to implement first

### Temporal scope

Each temporal request selects `received_at` or `event_time` explicitly and uses
UTC half-open intervals `[from, to)`. Ordinary expiry always remains based on
server acceptance. Historical imports keep source time independent of acceptance.
Clock skew, out-of-order records, timestamp precision, and unknown source times
must remain visible.

Source profiles declare the field and representation for event time. A JSON
`timestamp`, integer epoch milliseconds, a source filename, and a relative or
monotonic counter are not interchangeable. Do not infer a time zone, unit, or
wall-clock meaning from a field name. Multiple admitted/start/terminal times in a
receipt remain individually queryable; the profile states which, if any, becomes
the diagnostic event time. Ambiguity produces a diagnostic, not a guessed value.

Source schema 60 extends the immutable binding with explicit native whole-record
log captures. JSON and log selectors share timestamp interpretation, byte
evidence, temporal query paths and retention. Native work/storage refusals fail
ingestion rather than publishing a prefix; ordinary absent/invalid source times
remain unknown. See [the contract and limits](interfaces.md#plain-log-timestamp-captures-source-schema-60).

An event-time interval excludes records with unknown event time and reports that
excluded count separately when known. It never substitutes acceptance time.
Chronological pages use the selected timestamp and a stable occurrence-ID
tiebreaker within the captured snapshot. Relevance sorting is an explicit option;
scores identify their ranking corpus and profile.

### Field and value discovery

`keys_list` lists JSON RFC 6901 pointers and diagnostic-field identities in a
selected scope, including observed kinds and record-presence counts. Count each
record once for presence; distinguish that from the number of matching value
occurrences. Preserve duplicate members, arrays, escaped member names, nulls,
empty strings/containers, and absent fields. Repeated paths can have different
occurrence identities and values.

`values_search` searches a specified path/field or all applicable fields and
returns the matched occurrence, kind, record/artifact IDs, and original source
span. Initial operators are existence, kind-qualified exact lexical equality,
decoded string equality where supported, and explicit text matching. Selectors
and match semantics are advertised in capabilities. Numeric value/range predicates
use native exact decimal/XSD facilities under an explicit interpretation profile;
they must not round through floating point or relabel the native JSON graph.

`records_search` combines source/time scope, field predicates, and text predicates
as an intersection. Conditions bind to the same record, not independent records
in a file. Nested constraints that require the same array element bind its native
parent occurrence too. JSON pointers identify concrete paths; a wildcard/recursive
selector is not silently inferred or implemented as a second JSONPath language.

Key discovery and value searches share the same native JSON occurrence model,
scope/filter compilation, indexes, and receipts as other reads. They do not
maintain a separate schema interpretation or filtering engine.

### Full text and context

Use one declared PurRDF analysis law for indexing and queries. Search complete
log record text and applicable JSON scalar text, not just a guessed `message`
field. Allow an explicit path restriction. Match evidence states whether it came
from raw lexical text, decoded text, or a normalized diagnostic field. Exact
identifier lookup is field-value matching; tokenized text search must not be
presented as an exact match for punctuation-bearing job IDs or paths.

The ordered JSON graph retains raw lexical scalar text. A decoded-string search
view is a separate derived interpretation using PurRDF's lexical utilities. Do
not replace escapes or number spellings in the authoritative graph. Until a
native decoding/alignment seam supplies precise character provenance, a decoded
match may cite its complete scalar byte span; it must not fabricate a narrower
byte range. Scalars with unsupported decoding keep their raw bytes/projection
and expose incomplete derived search coverage.

`records_context` captures bounded neighbors of a retained record in a source or
stream, with the requested ordering basis, same read receipt, and gaps. Context
is a concrete query over retained membership, not an implicit pin or a promise of
complete stream history. Multi-source temporal correlation uses the explicit
`correlate` surface rather than inventing a source-local ordering between streams.

## PurRDF integration and ownership

Inspect and compile against an exact selected native dependency revision before
runtime implementation. The current runtime dependencies and census reference
both select `9b2bf3b71325154137bc1aa817b46356daadfe3a`, as recorded separately in
`Cargo.toml`, `Cargo.lock` and `registry/compatibility.toml`. The table below
preserves the original seam audit at
`e58974e504b9681597faea1490002ce8ddaf7448`; later native integration and its
remaining gaps are recorded in [PurRDF integration gaps](purrdf-integration-gaps.md).

| Concern | Native owner / inspected surface | Sparrow work |
| --- | --- | --- |
| JSON occurrence structure | `crates/json/src/lib.rs` and `model.rs`: `analyze`, `Document`, `Value` path/kind/parent/ordinal/span accessors; facade `purrdf::json` | Store/index native occurrences and map record-relative spans into source provenance. |
| Logical JSON interpretation | `crates/lex/src/json.rs`: ordered member preservation, lexical numbers and exact decimal semantics; `json_escape` utilities | Declare additional diagnostic/search interpretations and report refusals without changing raw projection. |
| Full-text analysis and scoring | `crates/text/src/lib.rs`: `Analyzer`, `PreparedCorpus`, `PreparedQuery`, source evidence and substring surfaces; facade `purrdf::text` | Persist bounded postings/statistics and wire storage-backed query producers to native ranking. |
| Ranked composition | `crates/purrdf/src/lib.rs`: facade `purrdf::retrieval` | Use native composition when combining producer rankings; never invent a fusion law. |
| Query evaluation / storage failures | Facade `purrdf::sparql` and persistent `DatasetView` / `FallibleDatasetView` contracts | Implement RocksDB-backed views and common operation compilation, preserving native certainty and operational failure behavior. |

The standard text analyzer requires five caller-supplied baseline lexicon
artifacts. Resolve and fingerprint them as part of query qualification; missing
artifacts are a hard failure. Do not silently select an empty-lexicon profile.
Audit native persistent text/substring facilities before choosing a storage
encoding; do not port resident index internals or rebuild the retention corpus
into a resident index for every query.

The next text implementation uses the public external-store scoring seam:
`PreparedCorpus::new` validates an explicit document population and exact field
token totals; `prepare_query` accepts sorted analyzed terms with document
frequencies; `PreparedQuery::score` accepts stored per-term `FieldInput` values.
`RankingProfile::single_field` supplies the native initial ranking law. Sparrow
must maintain these counts with its occurrence lifetime, including pins, and
bind them to the query's captured corpus. Native `Analyzer::projections` supplies
lexical, surface and punctuation-span projections with original-source
contributors/highlights. Persist their fingerprint and evidence at ingestion;
do not recompute corpus analysis on search. Native analysis and scoring failures
must stay visible. This seam inspection is not completed text-search acceptance.

If an inspected seam cannot serve the actual stored caller, record the precise
upstream integration gap and address it upstream. Do not build a parallel parser,
tokenizer, scorer, or evaluator to keep the local task moving. Extend the helper
ledger as concrete helper ownership is established and run the cross census.

## Implementation slices and exit criteria

### 1. Storage and lifetime foundation — architecture milestone 1

- Select and lock native dependencies/toolchain; verify the required RocksDB
  compression codecs. Define versioned store identity, key encodings, bounded
  configuration, and typed errors.
- Implement serialized atomic catalog writes, exact-byte content/manifests,
  independent occurrences, references, durable pin mutations, injected-clock
  retention, and explicit disk admission/maintenance accounting.
- Qualify byte identity, independent duplicate occurrences, record survival
  after parent expiry, overlapping/reused pins, exact deadline boundaries, and
  pressure without broken pins. Use small synthetic stores and user-space faults.

Exit: milestone-1 acceptance scenarios pass, unsupported formats fail to open,
and storage faults produce explicit failure rather than successful partial writes.
Production disk/RAM limits remain deployment inputs, not guessed constants.

### 2. Non-destructive import and restart recovery — milestone 2 first slice

- Stream copied JSONL, plain logs, JSON documents, and opaque bytes into complete
  manifests with exact delimiters and offsets. Preserve final incomplete lines
  appropriately for completed artifacts versus active producers.
- Select format through explicit hints and bounded validation. Exercise JSON
  receipts named `.bin`, malformed records, long lines, CRLF, and binary data.
- Implement stable retry identities, bounded journals, complete publication,
  durable confirmation, and restart recovery through the daemon's typed layer.

Exit: copied sample-shaped inputs reconstruct exactly after restart; repeated
requests do not duplicate occurrences; raw success and projection failures are
separately observable. Completed-file claim/deletion qualification is still a
required later milestone-2 task, before any destructive producer watch.

### 3. Temporal fields and JSON discovery — milestone 3 first query slice

- Implement native JSON projection and persisted occurrence metadata, dictionary,
  graph probes, time access paths, and the required stored-view semantics.
- Add source profiles, explicit time scope, record discovery, `keys_list`, and
  `values_search`, backed by one typed operation layer and native query machinery.
- Expose matching operations through CLI and MCP with the documented receipt,
  stable pagination, exact bytes, and pin workflow.

Exit: an agent discovers an unfamiliar fixture's keys, finds a job/field value
in a time interval, verifies the matching original bytes, and pins selected
evidence. Native/resident comparisons cover duplicate keys, array-parent binding,
unknown timestamps, lexical versus logical values, expiry, and partial coverage.

### 4. Full-text investigation and context — next milestone 3 slice

- Persist native-analyzed text postings and corpus statistics; declare ranking
  scope, resolve analyzer artifacts, and bind prepared scoring to stored reads.
- Combine text, field, and time constraints; add artifact grouping and bounded
  record context through the same source membership and read leases.
- Qualify ordinary logs, nested JSONL text, escaped strings, large scalar values,
  exact identifier lookup, pagination, and failures during read/scoring.

Exit: the agent investigation scenarios above pass through both CLI and MCP.
Empty answers with incomplete indexing are distinguishable from proved absence;
native certified partial outcomes and genuine storage failures retain their
different contracts. Broad retrieval never requires loading all retained data.

### 5. Remaining first-version contracts and operational qualification

- Finish qualified completed-file consumption and its crash/identity/unlink
  acceptance cases; connect a production consume watch only after that gate.
- Complete the remaining milestone-3 SPARQL forms, stored-view qualification, graph/text
  integration, explanations, and correlation contracts.
- Implement milestone-4 syslog, live tailing, rotation/completion, and the separate
  bounded Rust logging client. Verify client retries, flush/shutdown, and gaps.
- Benchmark immutable representative corpora, concurrent ingestion/queries and
  maintenance, 30-day turnover, pins, and backpressure. Report physical index and
  payload space, RSS, latency distributions, and reclamation lag together.

There is no performance acceptance claim until the corpus, deployment envelope,
configuration, and complete measurement method are recorded. Earlier slices
deliver useful investigations without claiming all first-version gates are met.

## Remaining deployment inputs

These do not prevent foundation work or synthetic query fixtures:

- Deployment host, filesystem/quota, RAM ceiling, disk budget, and maintenance
  reserve. Establish numerical targets after initial whole-store measurement.
- Public Cap'n Proto TCP at `0.0.0.0:518` is authorized, with no ingestion
  authentication requirement (2026-10-08); qualify remote agent access through
  the same typed operations.
- Explicit production watch roots and each producer's completed-file, append,
  rotation, or truncation handoff contract. Sample paths authorize read-only
  inspection, not production consumption or source deletion.
- Any delivery deadline or required sustained/burst ingestion and query latency.

Implementation status must identify which slice and acceptance criteria are
actually satisfied; planning and helper tooling do not establish runtime readiness.
