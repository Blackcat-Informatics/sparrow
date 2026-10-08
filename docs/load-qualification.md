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

## Qualified runs

Signed source `6900752` passed the full commit gate: release Clippy with warnings
denied, 155 storage tests, 59 RPC integration tests, all remaining workspace
tests, seeded census tests and the pinned cross gate with 243 ledger jobs and
zero distinct exceptions. The selected example was explicitly rebuilt after
the gate and frozen as `~/.local/libexec/sparrow/load-schema48-6900752`, SHA-256
`ecf14355a37e30ec6a0497059933359410ba81f04af40442ada195c9c2ace2ff`.
The private daemon is a capability-free copy of the installed
`sparrowd-schema48-8b56d65`; its separate image hash is recorded in each run.
No runtime source or installed daemon changed for this qualifier.

Both cold stores use Btrfs on the local host, AMD Ryzen AI MAX+ 395 (16 cores,
32 logical CPUs), a 2 GiB disk budget, 128 MiB maintenance reserve, 16 MiB
artifact ceiling, four operation slots and sixteen connection slots. Complete
hardware, kernel, filesystem options, configuration, source identity, both image
hashes, saved inputs, native report, 100 ms RSS/I/O samples and closed file sizes
are retained with each run. Resource samples cover the private daemon, not the
load client's allocation or other host processes. The host was not isolated.

| Cohorts | Evidence directory under `~/.local/state/sparrow/` | Accepted raw bytes | Records | Elapsed seconds | Sampled peak RSS KiB | Closed file bytes |
| --- | --- | --- | --- | --- | --- | --- |
| 32 | `typed-load-qualification-cjlepig8` | 8,678,582 | 4,160 | 8.873771 | 190,168 | 16,685,248 |
| 64 | `typed-load-qualification-gf0bsf51` | 17,358,454 | 8,320 | 23.983617 | 233,436 | 33,220,594 |

Each cohort has 64 JSONL and 64 log records with 2,048 padding bytes, plus
Markdown and random opaque artifacts. The 32-cohort run completes 128 durable
intakes and 20 concurrent captures; the 64-cohort run completes 256 intakes and
28 concurrent captures. Every accepted artifact has exact SDK byte readback.
Final record/text/key/value counts are independently checked; record captures
take 29/57 pages and text captures 40/80 pages. Page byte admission can produce
fewer than the requested 1,000 rows. Both final statuses have zero record
handles/bytes and no pins. Repeating the qualifier against each nonempty store
refuses before creating a new input directory and emits no successful report.
Both private daemons shut down cleanly. The public unit retains PID 1228283,
source `8b56d65`, all 212 records, and its existing listeners and configuration.

| Operation timing | 32 cohorts | 64 cohorts |
| --- | --- | --- |
| Durable intake mean / maximum, ms | 64.129 / 331.958 | 83.999 / 636.051 |
| Concurrent full capture mean / maximum, ms | 422.684 / 1,574.086 | 802.448 / 7,478.830 |
| Final warm record traversal, ms | 65.043 | 146.650 |
| Final warm text traversal, ms | 213.853 | 903.687 |
| Final warm keys traversal, ms | 66.151 | 215.200 |
| Final warm selective value traversal, ms | 59.147 | 193.073 |

These finite observations are not latency guarantees or percentile estimates.
The whole-run rates include fixture generation, readback and final traversals.
Closed file totals include native indexes and control state, not just compressed
payloads, and exclude transient files already reclaimed. Pre-shutdown kernel I/O
counters are preserved; they exclude later shutdown writes and do not establish
native compaction write amplification. RSS sampling is not a true peak bound.

## Refusals and unresolved shutdown observation

The initial development trial `typed-load-qualification-btf4kgbd` failed because
the qualifier expected four key rows. Native readback established the root
object path plus four members; the oracle was corrected. The subsequent
development trial `typed-load-qualification-ex65cpj_` passed before source
qualification. The table above uses only the frozen signed-source image.

Two 128-cohort attempts, `typed-load-qualification-8ma6oqis` and
`typed-load-qualification-ndbq73wh`, explicitly refuse broad record captures at
the existing native host limit: `RESOURCE_LIMIT: record capture exceeds 10000
matches; narrow scope`. The example can admit a proposed corpus up to 50,000
records, but that does not override the runtime's 10,000-row capture limit,
64 MiB staging allowance, resident admission, scan ceiling or lease. These are
failed workloads, not successful measurements or partial-success reports.
No runtime budget was relaxed to obtain either qualified run.

The first failed attempt also exceeded the collector's 120-second SIGINT wait.
Its final exit status was not collected, and its stale Unix socket remained.
The original collector consequently did not retain its in-memory RSS samples.
After authoritative process disappearance, a controlled reopen recovered the
endpoint and preserved 13,064 records; repeating the same query refusal then
shut down with exit zero. The second concurrent 128-cohort attempt refused in
the same way and shut down within the collector's ten-second initial bound.
The probe and logs are preserved in the first failed directory and
`/tmp/sparrow-load-shutdown-probe.log`. The timeout was not reproduced or fixed;
it remains a shutdown-under-failed-load acceptance concern. Later collectors
save samples before waiting and capture stacks if the owned process remains
live after ten seconds. No production process was signalled during these runs.

### Persistent interrupt follow-up

Subsequent signed source `b8a9a6c` fixes a concrete lost-subscription window in
the serving loop: one native SIGINT receiver is registered before storage/worker
startup and retained while maintenance branches await their blocking work.
The original timeout had no stack capture, so its exact cause is not proven
retroactively. [The runtime evidence](development-runtime.md#persistent-interrupt-subscription)
records the real startup comparison, full source gate and public adoption.
Frozen failed-load probes `typed-load-qualification-4zk0g50_` and
`typed-load-qualification-mw88zyrx` retain the same 10,000-match refusal and then
exit zero about 0.114 seconds after SIGINT, removing their sockets. These are
failed workloads with successful teardown observations, not larger successful
load measurements or a general shutdown latency guarantee.
