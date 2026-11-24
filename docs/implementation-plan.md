<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Sparrow implementation plan

Current executable surfaces are specified in [Interfaces](interfaces.md); native integration and its gaps
are in [PurRDF integration gaps](purrdf-integration-gaps.md). This plan states
the query priorities, sample discovery and delivery slices.

This plan builds on [Architecture](architecture.md) and [Interfaces](interfaces.md).
The milestone dependencies are: qualify storage and lifetime first, then
import, then query surfaces, then continuous producers.

## Current acceptance boundary

The following evidence covers the original first-version scope. Finite fixtures do not establish
production capacity.

| Required area | Evidence already present | Qualification boundary |
| --- | --- | --- |
| Graph queries | Typed RPC, CLI and MCP expose native SELECT, ASK, CONSTRUCT, DESCRIBE, measured EXPLAIN and stored-text rank/span relations. | Selected COUNT, ranking and EXPLAIN pass in the synthetic load workload, and whole-store COUNT refuses at catalog admission. [Native integration](purrdf-integration-gaps.md), [load](load-qualification.md#schema-62-native-graph-and-text-follow-up). |
| Reclamation | Native tombstone-compaction policy; real-pressure regression with measured decrease, successful retry, protected shared bytes and reopen; all three current-source 60-day turnover profiles pass. | Finite physical recovery and turnover are qualified. Closed files remain after zero logical ownership; no hard quota is configured and no new upstream API is required. [Native reclamation](native-reclamation.md), [turnover](turnover-qualification.md#schema-62-retention-and-native-compaction-policy). |
| Agent contracts | Typed tools expose text corpus, profile and coverage receipts and native relation descriptors. TCP and Unix, and CLI and MCP, share immutable continuations and exact-byte reads. | Effective limits and typed refusals remain part of the contract. [Interfaces](interfaces.md), [load](load-qualification.md). |
| Final qualification | Release tests cover completed-file process crashes, kernel queue overflow, directory limits, multiple writers, moved-in files and stale events. Synthetic turnover and concurrent load exercise the production store paths. | Production rate, storage density and hard quota recovery require separate measurement. [Crash/watch checks](interfaces.md#completed-file-process-crash-qualification), [turnover](turnover-qualification.md), [performance](performance-baseline.md). |

The qualified kernel-overflow fixture changes no host limits; configured
directory-budget exhaustion is independent of exhausting the shared kernel
watch quota. The SIGKILL matrix starts from durable reservation and does not
prove power-loss persistence at arbitrary filesystem boundaries. The finite
turnover corpus is 65,642,380 raw bytes per pin mode. None of these results closes
production capacity acceptance. Production-scale indexed-selection and resource
acceptance remain required. Host reboot and power-loss trials
are not additional first-version gates: the architecture's crash scenarios and
the durability claims actually made determine the required checks.

The implementation scope is closed by the existing APIs and tests, not by a
hypothetical dependency request. The remaining deployment inputs below determine
future capacity claims; no numerical SLA, strict quota or production consume
root has been inferred. Inaccessible corpus samples remain a coverage limit.

The RocksDB binding is the `rocksdb` crate that `Cargo.toml` declares and
`Cargo.lock` resolves. Evaluate the existing native
compaction policy and measured pressure recovery before declaring an external
dependency blocker.

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

Use bounded immutable samples outside the source tree. Record each selection
and report unreadable inputs alongside the successful observations.

| Input | Required coverage |
| --- | --- |
| Logs | Include plain, rotated and compressed logs and opaque journal files. |
| Application JSONL | Include timestamps, component labels, nested objects and arrays, scalars and nulls. Exercise bounded streaming, oversized records and malformed lines. |
| Artifacts | Exercise exact-byte retention and an explicitly declared format where an extension is ambiguous. |

Keep production sample bodies and identities outside tracked source. Use
synthetic fixtures for tracked qualification and record observation bounds
and whole-store costs for performance runs.

[Journal export imports](interfaces.md#systemd-journal-export-import) produce
searchable JSON Lines records that preserve journal field names and values.
Export native systemd journal files with `journalctl --output=export` before
importing them. Qualify compressed-input decoding separately from preserving
the original compressed bytes. Unknown `.bin` files remain opaque.

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

An event-time interval places records with unknown event time at their receipt
time, marks each with basis `receipt`, and reports their count as
`event_time_fallback` (schema 69). A known source instant is never replaced.
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
runtime implementation. The runtime dependencies are declared in `Cargo.toml` and
locked in `Cargo.lock`; the census reference revision is in
`registry/compatibility.toml`. The table below records the seam audit; native
integration and its remaining gaps are recorded in
[PurRDF integration gaps](purrdf-integration-gaps.md).

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

### 4. Full-text investigation and context — milestone 3 slice

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
- Public Cap'n Proto TCP at `0.0.0.0:518` carries no ingestion authentication
  requirement. Other TCP operations require a shared-secret CHAP session; qualify remote agent access through
  the same typed operations.
- Explicit production watch roots and each producer's completed-file, append,
  rotation, or truncation handoff contract. Sample paths authorize read-only
  inspection, not production consumption or source deletion.
- Any delivery deadline or required sustained/burst ingestion and query latency.

Implementation status must identify which slice and acceptance criteria are
actually satisfied; planning and helper tooling do not establish runtime readiness.
