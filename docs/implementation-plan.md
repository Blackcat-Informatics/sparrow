# Sparrow implementation plan

Status: implementation in progress. Helper-census tooling, a RocksDB storage
foundation, the Cap'n Proto daemon, bounded JSON occurrence discovery and persistent
native full-text retrieval and bounded acceptance-time record context through
shared CLI and stdio MCP clients are present. Native JSON/Markdown content and
record metadata now persist through a canonical RDF dictionary and six physical
quad permutations; typed inspection exposes projection state and stream counts.
The native stored view now supports bounded lazy reads and governed basic-pattern
SELECT through the Rust core API. Full governed SPARQL admission and its CLI/MCP
surface remain required; the concrete native gap is recorded in
[PurRDF integration gaps](purrdf-integration-gaps.md).
The full server acceptance criteria remain unmet. Current executable surfaces
and verification are recorded in [Development runtime](development-runtime.md).
Stable-ID pin renewal is implemented through the core, typed daemon, CLI and MCP,
including durable retry receipts and optional revision guards. Pin discovery
and member retrieval now use finite immutable catalog pages shared by the typed
daemon, CLI and MCP. Artifact and source discovery share the catalog lease owner;
source inventory includes exact intake keys, mapping IDs, retained occurrence
counts and configured watch bindings. Producer continuity remains explicitly
unknown while stream generations and gap tracking are unfinished.
Artifact discovery now admits live, explicit evidence and revision-guarded pin
membership through the same reference owner as record queries, including
recordless artifacts and frozen pages after pin release. Artifact diagnostic
scope filtering and broader record/field/text query pagination remain required.
The standalone producer client and schema-11 typed event batch operation are
present. Native event JSONL enters the same occurrence/field/text/time indexes;
the bounded producer queue now reconnects with immutable pending batches and
provides durable admitted-input flush/shutdown fences. The tracing layer, span
capture and explicit server buffered-WAL acknowledgement/flush contracts remain
milestone-4 work.

This plan builds on [Architecture](architecture.md) and [Interfaces](interfaces.md).
It records the initial query priorities and sample discovery supplied on
2026-10-06. The existing milestone dependencies remain: qualify storage and
lifetime first, then import, then query surfaces, then continuous producers.

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
runtime implementation. The current census reference is
`e58974e504b9681597faea1490002ce8ddaf7448`; Sparrow's runtime dependencies are now
locked to that same revision and have compiled. These seams were inspected at
that revision:

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
- Local versus remote agent access and the remote authentication/TLS arrangement.
- Explicit production watch roots and each producer's completed-file, append,
  rotation, or truncation handoff contract. Sample paths authorize read-only
  inspection, not production consumption or source deletion.
- Any delivery deadline or required sustained/burst ingestion and query latency.

Implementation status must identify which slice and acceptance criteria are
actually satisfied; planning and helper tooling do not establish runtime readiness.
