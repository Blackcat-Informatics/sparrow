<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# PurRDF integration

Sparrow uses the pinned PurRDF release for native RDF, SPARQL and text work.
The [architecture](architecture.md) describes the ownership boundary, and
the [interfaces](interfaces.md) define typed requests and results.

## Native query ownership

The persisted view implements PurRDF's bounded workspace and read
capabilities. Capture freezes eligible catalog owners and a RocksDB
snapshot, then releases the catalog writer before native evaluation.
Membership, content readiness and query certainty remain independent.
Catalog, scan and read admission can refuse before capture publishes a
result.

Sparrow uses native SELECT, ASK, CONSTRUCT, DESCRIBE and measured EXPLAIN.
Native results retain their admission and snapshot evidence. Immutable pages
borrow those results and preserve native completeness and partial-answer
certificates. Cursors share the catalog lease owner and its release and
expiry accounting. Typed RPC, CLI and MCP preserve the same result
contract.

The native evaluator enforces work, source, result and memory governors
under the original read scope. Borrowed term access and the shared
allocation owner charge actual materialized payloads. Broad queries can exceed admission even when only one result page is
requested.

## Stored text and correlation

Stored-text rank and span producers run under the captured snapshot and
share the same stored postings, scoring and source-byte span logic as
ordinary search. Requests name the native relations explicitly. Captured
results retain their text corpus identity, scores, coverage and original
byte spans. See [stored-text graph relations](interfaces.md#stored-text-graph-relations-source-schema-62).

Native correlation uses fixed SELECT recipes and typed substitutions.
Tracing metadata and explicit source-time bindings use the existing
metadata graph and interpretation diagnostics. Plain-log timestamp
bindings compile and match through PurRDF's public XPath engine.

## Qualification

Storage tests compare native persisted answers against a resident
PurRDF oracle. They exercise typed bindings, retained ownership,
cancellation, admission refusals, certificate propagation and frozen
page replay. Transport tests cover the typed RPC, CLI and MCP consumers.

The [synthetic load results](load-qualification.md#schema-62-native-graph-and-text-follow-up)
cover selected graph, text, explanation and whole-store admission cases.
The [synthetic turnover results](turnover-qualification.md#schema-62-retention-and-native-compaction-policy)
cover logical graph closure after retirement. Production-scale query
latency and capacity require separate workloads.
