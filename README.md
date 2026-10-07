# Sparrow

Sparrow is a high-density, temporal diagnostic log and data server. It turns
scattered operational artifacts into retained evidence that agents can discover,
inspect, correlate, and query as a semantic graph.

**Status: implementation in progress.** A RocksDB daemon, Cap'n Proto RPC,
CLI, stdio MCP, JSON field discovery, persistent native full-text retrieval, bounded
record context, durable stable-ID pin renewal and paginated pin discovery,
persistent native JSON/Markdown RDF dictionaries and six-way
quad indexes, and helper-census tooling are implemented. Stored-view SPARQL,
the logging client and
full architecture acceptance
remain unfinished. See [Development runtime](docs/development-runtime.md) for
present commands, verified behavior and limits.

The Rust core also exposes a bounded native stored RDF view. Full SPARQL remains
unfinished because the pinned native engine does not yet price the required
algebra on bounded views; see [PurRDF integration gaps](docs/purrdf-integration-gaps.md).

## Operating model

Sparrow will run as a Rust service on one Linux server with local storage. The
initial workload is under 10 GiB of new, uncompressed data per day, including
large directories of JSONL and `.log` files. Sustained throughput, import bursts,
and total storage density are measurements to establish during implementation.

The default retention window is **up to 30 days from server acceptance**. Source
timestamps remain queryable but do not determine expiry. A configured disk budget
can cause earlier eviction of the oldest unpinned data. When protected data or
unreclaimed storage prevents another write, ingestion applies backpressure.

Agents can retain selected evidence in named, expiring collections. For example:

```text
pin these records for ten days under "problem_investigation_1"
```

A collection can contain complete artifacts and individual records. A record pin
preserves that record's bytes, queryable data, and source provenance without
retaining an entire expired source file. Overlapping pins protect their members
independently. Reads do not extend a pin's deadline.

## Core capabilities

- **Heterogeneous ingestion:** JSON/JSONL, Markdown, structured logs, syslog,
  unformatted text, and opaque diagnostic payloads. Original bytes remain
  available even when a parser rejects an input.
- **Semantic querying:** full JSON and Markdown projections, RDF 1.2 metadata
  and relationships, and SPARQL 1.2 capabilities supplied by
  [PurRDF](https://github.com/Blackcat-Informatics/purrdf).
- **Storage efficiency:** exact-content deduplication, shared projections,
  ordered manifests for large files, and compression of both payloads and
  semantic indexes.
- **Temporal retention:** time-based expiry, explicit disk-pressure eviction,
  named pins, and reference-aware reclamation.
- **Agent access:** typed Cap'n Proto RPC and a stdio MCP adapter, a CLI, bounded evidence
  reads, typed query results, and explicit completeness information.
- **Rust logging integration:** a planned composable `tracing` layer with
  structured events and spans, bounded buffering, drop counters, and explicit
  flush/shutdown.

## Importing existing data

Two directory modes handle different producer lifecycles:

| Mode | Behavior | Source removal |
| --- | --- | --- |
| `consume` | Use Linux inotify writable-close/publication events to discover finished artifacts, followed by a configurable quiet interval; default 30 seconds. Reconcile existing files through scanning. | Remove a claimed file only after its complete byte representation and catalog entry are durable. |
| `tail` | Read appended records and checkpoint file identity and byte offsets. | Retain active files; consume a rotated or explicitly completed file only after completed handoff. |

Inotify `IN_CLOSE_WRITE` supplies a completion candidate, while startup scans and
overflow recovery handle missed events. A close does not prove all writers have
finished or that the file will not be reopened; each watch retains its producer
handoff contract.

Ordinary ingestion may acknowledge buffered data with a configurable two-second
durability interval. Source-file deletion and successful pin mutations require
durable commits. Parsing or indexing failures are visible independently of byte
ingestion; successful raw import is sufficient to consume a completed source.

## Architecture and interfaces

The initial storage substrate is RocksDB, with content-addressed payloads,
integrated blob storage for larger values, and staged compression. Sparrow owns
retention eligibility, named pins, reference counting, and its persistent RDF
backend. PurRDF owns RDF semantics, codecs, JSON/Markdown projection, SPARQL,
text analysis, scoring, and query governors.

- [Architecture](docs/architecture.md): data model, storage decisions, producer
  contracts, retention, failure handling, and implementation milestones.
- [Interfaces](docs/interfaces.md): common types, MCP tools, Cap'n Proto operations,
  CLI examples, named pins, and the Rust client contract.
- [Implementation plan](docs/implementation-plan.md): agent investigation
  priorities, initial sample discovery, query contracts, and delivery slices.
- [Helper census](docs/helper-census.md): PurRDF reuse and local duplication gates.
- [Development runtime](docs/development-runtime.md): implemented surfaces,
  verification evidence and unfinished server acceptance.

The first useful agent query surfaces are full-text log and JSONL searches,
temporal/field filtering, key listing, and value search. Results lead back to
exact source bytes and report coverage so an agent can distinguish no matches
from unindexed or missing evidence.

## Implementation roadmap

1. Storage, exact-byte retrieval, deduplication, retention, and named pins.
2. File/API ingestion, large-file streaming, durable consumption, and recovery.
3. PurRDF-backed projections, persistent graph/text indexes, MCP, and CLI.
4. Live syslog, active-log tailing, and the Rust logging client.

Each milestone has acceptance criteria in the architecture document. Performance
measurements must include graph indexes and maintenance, as well as raw payloads.
Initial read-only sample discovery covers local `/var/log`, Lillith Soul's
`inner-core`, and Brainstem evidence. It is bounded structural inspection, not a
qualified benchmark corpus; sample-based benchmarking remains an implementation
prerequisite for performance claims.

Per-class retention, clustering, embeddings, and a human dashboard are later
extensions. The initial service focuses on agent access to disposable operational
evidence.
