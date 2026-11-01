<p align="center">
  <img src="./docs/sparrow-logo.svg" alt="Sparrow logo: a black cat gently holding a sparrow" width="128" height="128">
</p>

<h1 align="center">Sparrow</h1>

<p align="center">
  <em>Temporal diagnostic evidence, carried gently: retained, pinned and queryable as a graph.</em>
</p>

<p align="center">
  <a href="https://github.com/Blackcat-Informatics/sparrow/actions/workflows/ci.yaml"><img src="https://github.com/Blackcat-Informatics/sparrow/actions/workflows/ci.yaml/badge.svg" alt="CI"></a>
  <a href="./LICENSING.md"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0%20OR%20MulanPSL--2.0-blue.svg" alt="License: MIT OR Apache-2.0 OR MulanPSL-2.0"></a>
  <img src="https://img.shields.io/badge/MSRV-1.98-orange.svg" alt="MSRV 1.98">
  <a href="https://github.com/Blackcat-Informatics/purrdf"><img src="https://img.shields.io/badge/built%20on-PurRDF-111214.svg" alt="Built on PurRDF"></a>
</p>

Sparrow is a high-density, temporal diagnostic log and data server. It turns
scattered operational artifacts into retained evidence that agents can discover,
inspect, correlate, and query as a semantic graph.

**Status: implemented and running; measured limits apply.** A RocksDB daemon, Cap'n Proto RPC,
CLI with resumable typed file imports and bounded oversized-record preparation,
stdio MCP, JSON field discovery, immutable JSON source-time and diagnostic profiles,
mapped service/host/severity/trace/span filters,
explicit partial diagnostic scope coverage with typed unknown totals,
explicit event-time intervals, artifact/record/pin query scopes, combined same-record field/text searches,
persistent native full-text retrieval, bounded
record context, durable stable-ID pin renewal and paginated pin discovery,
persistent native JSON/Markdown RDF dictionaries and six-way
quad indexes, a standalone typed producer client with bounded delivery and a
composable tracing layer, bounded buffered/durable ingestion with session-scoped
WAL flush controls, and helper-census tooling are implemented. Active tailing
with explicit completed-generation retirement, typed public TCP
ingestion, and syslog are implemented. Stored-view SELECT, ASK, CONSTRUCT,
DESCRIBE, measured EXPLAIN and native stored-text rank/span relations are
implemented and deployed in schema 62. See [Development runtime](docs/development-runtime.md) for
present commands, verified behavior and limits.

The installed user service is `sparrow.service`, serving typed Cap'n Proto on
`0.0.0.0:518` and a private Unix socket. Anonymous TCP peers may only discover,
ingest and import; other TCP operations require `--secret-file` CHAP authentication. The client is `~/.local/bin/sparrow`.
Graph queries use PurRDF's native semantics and governors over bounded stored
views, with explicit admission refusals; see
[native integration evidence](docs/purrdf-integration-gaps.md).

## Operating model

Sparrow runs as a Rust service on one Linux server with local storage. The
initial workload is under 10 GiB of new, uncompressed data per day, including
large directories of JSONL and `.log` files. Sustained throughput, import bursts,
and total storage density have finite workload measurements, not a production
capacity guarantee; see the [acceptance boundary](docs/implementation-plan.md#current-acceptance-boundary).

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
- **Rust logging integration:** a composable `tracing` layer with
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

## Build

Sparrow builds on Linux with:

- Rust 1.98 or newer, with rustfmt and Clippy
- `capnp` (the Cap'n Proto compiler) and libclang
- Git 2.54 or newer, and `tar`
- A PurRDF checkout at `../purrdf`, beside this repository, containing the
  revision pinned in `registry/compatibility.toml`. The tests read its text
  lexicons and the helper census archives its source at that revision.

```bash
git clone https://github.com/Blackcat-Informatics/purrdf ../purrdf
git -C ../purrdf checkout "$(sed -n 's/^git_revision = "\(.*\)"/\1/p' registry/compatibility.toml)"

make check          # fmt, release Clippy (warnings denied), release tests, census
make build-runtime  # release server and client binaries
make install-hooks  # run make check before every commit
```

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the gate, the PurRDF reuse rules
and the helper ledger, and [Deployment](docs/deployment.md) for installing the
user service.

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
- [Deployment](docs/deployment.md): persistent Linux user-service installation,
  runtime asset ownership and cutover verification.
- [Performance baseline](docs/performance-baseline.md): copied-corpus disk
  measurements, explicit query-capacity refusals and remaining qualification.

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

## License

Sparrow is licensed under your choice of [MIT](./LICENSE-MIT),
[Apache-2.0](./LICENSE-APACHE) or [MulanPSL-2.0](./LICENSE-MULAN). See
[LICENSING.md](./LICENSING.md) for details, documentation licensing and
contribution terms. Report vulnerabilities as described in
[SECURITY.md](./SECURITY.md).
