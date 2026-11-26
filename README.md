<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

<p align="center">
  <img src="./docs/sparrow-logo.svg" alt="Sparrow logo: a black cat gently holding a sparrow" width="128" height="128">
</p>

<h1 align="center">Sparrow</h1>

<p align="center">
  <em>Temporal diagnostic evidence, carried gently: retained, pinned and queryable as a graph.</em>
</p>

<p align="center">
  <a href="./LICENSING.md"><img src="https://img.shields.io/badge/runtime%20license-MIT%20OR%20Apache--2.0%20OR%20MulanPSL--2.0-blue.svg" alt="Runtime license: MIT OR Apache-2.0 OR MulanPSL-2.0"></a>
  <img src="https://img.shields.io/badge/MSRV-1.98-orange.svg" alt="MSRV 1.98">
  <a href="https://github.com/Blackcat-Informatics/purrdf"><img src="https://img.shields.io/badge/built%20on-PurRDF-111214.svg" alt="Built on PurRDF"></a>
</p>

Sparrow is a high-density, temporal diagnostic log and data server. It turns
scattered operational artifacts into retained evidence that agents can discover,
inspect, correlate, and query as a semantic graph.

Sparrow stores disposable operational logs and artifacts on one Linux server.
It keeps original bytes in RocksDB and uses PurRDF to project JSON and Markdown,
rank text searches, and evaluate governed SPARQL queries. Keep an independent
copy of anything irreplaceable. [Architecture](docs/architecture.md)

`sparrowd` serves typed Cap'n Proto RPC. The `sparrow-access` package supplies
the `sparrow` CLI and MCP adapter. Rust producers can use
[`sparrow-client`](https://crates.io/crates/sparrow-client) and negotiate
`sparrow-producer/1` across compatible wire versions. The current wire schema
is 73. [Interfaces](docs/interfaces.md)

## Quickstart

Build from source on Linux with Rust 1.98 or newer, a C and C++ toolchain, Make,
`capnp` (the Cap'n Proto compiler), libclang, Git, `tar`, and Python 3.11 or
newer. Clone PurRDF beside Sparrow. The lexicon tool reads the revision pinned
in [`registry/compatibility.toml`](registry/compatibility.toml).

```bash
git clone https://github.com/Blackcat-Informatics/purrdf ../purrdf
make build-runtime
```

From the checkout root, create a disposable store and start the daemon in the
foreground. The example binds a private Unix socket and installs no service.

```bash
demo=.stage/quickstart
mkdir -p .stage
mkdir -m 700 "$demo"
lexicons="$(tools/purrdf-lexicons.sh "$PWD" "$demo/bin")/artifacts"
cat > "$demo/server.toml" <<EOF
data_dir = "$demo/store"
socket_path = "$demo/store/rpc.sock"
text_lexicon_dir = "$lexicons"
instance_role = "scratch"
disk_budget = 536870912
maintenance_reserve = 67108864
max_records = 4096
max_pin_members = 1024
max_concurrent_requests = 4
EOF
target/release/sparrowd --config "$demo/server.toml"
```

Once it reports that it is serving, open a second terminal in the checkout
and import one JSONL record.

```bash
demo=.stage/quickstart
printf '%s\n' '{"message":"Sparrow quickstart","level":"info"}' > "$demo/event.jsonl"
target/release/sparrow "$demo/store/rpc.sock" status
target/release/sparrow "$demo/store/rpc.sock" ingest demo-1 quickstart jsonl "$demo/event.jsonl"
target/release/sparrow "$demo/store/rpc.sock" records --source quickstart --limit 10
```

The last command lists the imported record. Stop the foreground daemon with
Ctrl-C. See the [deployment guide](docs/deployment.md) for a long-running
service and the [archive format](docs/archive-format.md) for retention setup.

## Operating model

Rust applications can call `Console::install_auto("build.worker").await?` to start the
[developer console](docs/interfaces.md#developer-console). It displays events locally and
adds structured delivery when a client configuration or local ingest socket is present. The
[console example](crates/sparrow-client/examples/console.rs) shows a print-style
call, typed tracing fields and a `log` record in one span.

For an application that sends logs to the local daemon, start a configured
producer and attach the console once during startup. Printing an observation
queues it in memory. A background worker appends it to a bounded private spool
and replays the spool when the daemon returns.

```rust
use sparrow_client::{Console, Delivery, DeliveryConfig, sparrow_eprintln};
use std::time::Duration;

let delivery = Delivery::start_async(DeliveryConfig::configured_local("build.worker")).await?;
let console = Console::install(Some(delivery.clone()))?;
let targets = 12;
sparrow_eprintln!("Compiled {targets} targets");
console.flush().await?;
delivery.close_local(Duration::from_secs(5)).await?;
```

Use `send_with_token(event, cursor).await?` when advancing an upstream cursor
requires a local disk receipt. Use `shutdown_async(timeout).await?` when the
caller requires a durable receipt from Sparrow. For latency-sensitive diagnostics,
`DeliveryConfig::lossy(endpoint, source)` counts each dropped observation and
returns immediately on queue contention. It performs no disk writes. A producer
that tolerates RAM backpressure can select `DeliveryConfig::lossy_spooled` and
await `offer_spooled(event)` to buffer offline until its disk budget fills.
TCP endpoints, file-backed
configuration, limits and recovery are covered in the
[Rust client contract](docs/interfaces.md#7-rust-client-and-logging-integration).

Sparrow runs as a Rust service on one Linux server with local storage. The
initial workload is under 10 GiB of new, uncompressed data per day, including
large directories of JSONL and `.log` files. Sustained throughput, import bursts,
and total storage density have finite workload measurements, not a production
capacity guarantee; see the [acceptance boundary](docs/implementation-plan.md#current-acceptance-boundary).

An unpinned item is due at `received_at + 30 days` (server acceptance) and is
retired then once the archive holds it. Source timestamps remain queryable but
do not determine expiry. Disk pressure can retire archived, unpinned items
earlier. A pin keeps an item until the pin ends. The deletion gate keeps every
unarchived item past its deadline, so with no archive running nothing retires and ingest is refused with `STORAGE_PRESSURE` at the
budget less the reserve or below the free-space floor. When protected data or unreclaimed storage prevents another write,
ingestion applies backpressure.

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
- **Agent access:** typed Cap'n Proto RPC, a CLI, stdio and [localhost Streamable HTTP MCP](docs/interfaces.md#local-streamable-http), bounded evidence
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

## Contributing

Use Rust 1.98 or newer with rustfmt and Clippy for verification. The gate
uses Git 2.54 or newer, `b3sum`, and the same sibling PurRDF checkout as the
quickstart. Install the helper census and commit hook before running it.

```bash
make install-census
make install-hooks
make check
```

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the gate, the PurRDF reuse rules
and the helper ledger, and [deploy/README.md](deploy/README.md) for installing
the service.

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
- [Deployment](docs/deployment.md): configuration, kept images, unit behavior,
  shipping and backups behind the [`deploy/`](deploy/README.md) procedures.
- [Performance qualification](docs/performance-baseline.md) describes the
  synthetic intake, query and turnover workloads.

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
Initial sample discovery inspected system logs, application logs and
model-inference artifacts. Performance claims require workload measurements.

Per-class retention, clustering, embeddings, and a human dashboard are later
extensions. The initial service focuses on agent access to disposable operational
evidence.

## License

Sparrow is licensed under your choice of [MIT](./LICENSE-MIT),
[Apache-2.0](./LICENSE-APACHE) or [MulanPSL-2.0](./LICENSE-MULAN). See
[LICENSING.md](./LICENSING.md) for details, documentation licensing and
contribution terms. Report vulnerabilities as described in
[SECURITY.md](./SECURITY.md).
