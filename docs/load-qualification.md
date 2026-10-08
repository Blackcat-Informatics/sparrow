# Concurrent typed workload

The `load` example measures durable ingestion concurrently with complete agent
query captures against an empty private daemon. It uses the generated SDK and
the same production command parser, typed operations, result renderers, cursor
dispatcher and release owner as CLI/MCP. JSON fixture serialization and report
serialization use PurRDF's native codec. No evaluator or text analysis is copied.

```bash
cargo build --locked --release -p sparrow-server --example load
target/release/examples/load tcp://127.0.0.1:10330 /new/input/directory 16 64 1024 > report.json
```

Start an owned private daemon separately with a new store, no watches or syslog,
and a loopback TCP binding. The example refuses non-loopback endpoints, ports
at or below 1024, nonempty stores, active pins and configured producer sources.
The input directory is created exclusively. It never starts or stops a service,
deletes evidence, or opens a competing storage writer. Configuration limits and
storage/capture refusals remain hard failures. Only a successful run emits the
complete report; failure may leave accepted private evidence and saved inputs.

Each cohort contains changing JSONL, CRLF logs, Markdown and OS-random opaque
bytes. JSONL has four keys and one independently addressable record per line;
logs have one record per line; Markdown and opaque artifacts have one each.
Native key discovery includes the root object path as well as the four members.
Every accepted artifact's exact bytes are read back through the SDK. JSONL,
logs and Markdown include a common text term. Random opaque bytes exercise
incompressible input. Saved source bytes, artifact IDs and native payload hashes
bind the report to its actual corpus.

Two independent typed connections run ingestion and record/text/key/value
queries concurrently. Every query traverses its frozen capture completely and
releases it. The final four traversals check independent expected record counts,
text matches, five JSON paths and one exact numeric row-zero occurrence per
JSONL cohort. The value predicate supplies a selective query alongside broad
record and text captures. Final status must
have exact total occurrences and zero record read handles/bytes. An operation
may wait for the serialized catalogue writer; the timings measure that wait too.

Reports retain individual ingest and full capture timings in integer
microsecond strings, completion times, page/row counts and before/after native
status. Ingest timing ends at the durable reply; exact-byte readback is separate.
Whole-run elapsed time includes fixture preparation, verification and final
queries, so raw bytes divided by that time is a measured workload rate, not
isolated peak ingest throughput. No locally invented percentile law is used.

An external collector must record source and binary hashes, configuration,
hardware/filesystem, process RSS, native file and I/O growth, warm/cold state,
and any failed run. The example's internal counters do not measure process RSS
or physical write amplification. This synthetic finite workload does not alone
qualify production samples, sustained rates, 30-day density, hard quota recovery,
reboot behavior or full governed SPARQL.
