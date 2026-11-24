<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Concurrent typed workload

## Workload and method

The `load` example measures durable ingestion alongside complete record,
text, key and value captures. It uses the same typed operations, generated
client, command parser and cursor dispatcher as the CLI and MCP adapter.

```bash
cargo build --locked --release -p sparrow-access --example load
target/release/examples/load --secret-file SECRET tcp://127.0.0.1:10330 /new/input/directory 16 64 1024 > report.json
```

Run a scratch daemon with an empty store, a loopback TCP listener, a CHAP
secret and no watches or syslog sources. Pass its secret file as `SECRET`.
The example requires an authenticated loopback endpoint above port 1024 and
creates the input directory exclusively. It refuses an existing input
directory, a nonempty store, active pins or configured producer sources.

Each synthetic cohort includes changing JSONL, CRLF logs, Markdown and
random opaque bytes. The example reads each accepted artifact back through
the SDK and verifies its exact bytes. Query captures traverse all pages,
verify their expected counts and explicitly release their leases. The final
status checks the record total and zero retained read handles and bytes.

Ingest timing ends at the durable reply. Whole-run time also includes input
preparation, byte verification and the final queries. Record the filesystem,
resource limits, configuration and external process samples when repeating
the workload.

## Synthetic load results

The recorded runs used 64 JSONL and 64 log records per cohort, with 2,048
padding bytes, one Markdown record and one opaque record. The test stores
used Btrfs, a 2 GiB disk budget, a 128 MiB maintenance reserve, four operation
slots and sixteen connection slots.

| Cohorts | Accepted bytes | Records | Elapsed seconds | Sampled peak RSS (KiB) | Closed file bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| 32 | 8,678,582 | 4,160 | 8.874 | 190,168 | 16,685,248 |
| 64 | 17,358,454 | 8,320 | 23.984 | 233,436 | 33,220,594 |

Both runs completed durable intake, exact-byte readback and full query
traversals. Page admission can return fewer than the requested number of
rows. These samples cover the specified workload and limits.

A single-import run of the release daemon and CLI fixtures imported a
1,048,642-byte synthetic log into a fresh store over its private Unix socket and
published 14,743 records. Immediately after the CLI returned, before shutdown,
seven numbered WAL files totaled 190,945,144 bytes by file length and
227,471,360 bytes in allocated blocks. The store's `OPTIONS-000007` lists the
`default`, `payload` and `rdf_reverse` families with
`max_total_wal_size=268435456` (256 MiB). The pinned-WAL release regression
exercises flushing an idle family after writes cross the configured threshold.

## Schema 62 native graph and text follow-up

A 64-cohort synthetic run also checked the typed native graph APIs. Four
selected artifacts contained 130 records and 129 text documents. SPARQL
COUNT returned 130, and the native rank relation returned the same text
results, scores and corpus identity as ordinary text search. The measured
EXPLAIN completed and the query leases were released. Whole-store COUNT
returned `RESOURCE_LIMIT` at the configured catalog admission boundary.

## Table options and dictionaries

RocksDB table policy is owned by `storage::table_options`. As described in
[the architecture](architecture.md#42-deduplication-and-compression), the
default and payload families trade larger blocks and bottommost Zstd for
fewer stored bytes. Payloads use a bottommost dictionary. The hash-keyed
reverse family uses small blocks and a whole-key Bloom filter so negative
point reads admit less data.

## Write stalls

The storage policy separates a flush slot from compaction work and uses
multiple memtables per family. A full memtable applies pressure while a
flush is pending. Level-zero files and pending compaction bytes provide
separate signals. Measure admission latency and RocksDB stall counters on
a synthetic store with compaction debt when evaluating policy changes.
See [storage architecture](architecture.md#4-storage-decision).
