<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Concurrent typed workload

## Real-corpus first capacity pass

On 2026-10-27 the installed `43e8f2a` images served a private daemon with a fresh
store, the production limits and no watches. The input was 1,095,583,063 bytes
of sealed, deeply nested JSONL production segments, about 17 KiB per record, cut
on line boundaries into 1,149 chunks of at most 1 MiB. One `sparrow ingest` per
chunk ran sequentially over the Unix socket. The host was not idle: its load
average was 37 to 45, 73 GB of swap was in use on the NVMe device that holds the
store, and system I/O pressure (`some`) was 13 to 19 percent.

Only 66 chunks (64,566,545 bytes, 3,230 records) published. Each took 4 to 58
seconds, median 25.7 seconds and 95th percentile 51.4 seconds, with the daemon
using 0.38 cores and its worker blocked on I/O. Fifteen chunks exceeded the
60-second RPC connection lease and were disconnected. Each disconnect left its
import journal occupied, and once 16 were held every later chunk was refused
with `RESOURCE_LIMIT` ("16 import journals are occupied"). The CLI `ingest`
then neither resumed nor canceled its import after a disconnect; since issue #3
it resumes over a new connection and cancels an import that stops advancing.

The store then held 723,926,766 physical bytes for 64.6 MB of input, about 11
times the raw size before any compaction. The abandoned imports account for at
most 16 MB of it. The daemon cgroup peaked at 839 MB, page cache included.

Three findings bound production capacity until they are addressed:

- Throughput for this data on this host is about 30 to 40 KiB per second,
  roughly 3 GB per day, against the stated target of under 10 GiB per day.
- On-disk expansion of deeply nested JSON is about 11 times raw, so 50 GiB holds
  roughly 4.5 GB of such input, not 30 days at the target rate.
- An ingest disconnected by the connection lease leaks its import journal; 16
  leaks stop all further imports until they are reclaimed.

Smaller earlier samples ran faster in isolation: 0.95 MB took 1.5 seconds, 2.2 MB
19 seconds and 4.4 MB 47 seconds, so ingest time grows faster than artifact
size. The causes (fsync cadence, RDF projection of deep JSON, host swap
contention) were not separated; that needs an uncontended host and per-stage
timing.

## Per-record preparation cost (issue #5)

A scratch daemon with the production limits ingested each input through the CLI
on tmpfs and on the store's NVMe device on 2026-10-28, under load average 38 to
54 and I/O pressure (`some`) of 20 to 26 percent. The inputs were 1 MiB of
opaque bytes (one record), 1 MiB of synthetic log lines (5,180 records of about
200 bytes), 4 MiB of the same lines (20,586 records), and one real deep-JSONL
chunk (41 records in 904,586 bytes, about 21.5 KiB each).

Before the change, short records were sync-bound. The 1 MiB log took 9.15
seconds on tmpfs (about 1.8 ms per record) and did not finish within the
60-second connection lease on NVMe. A completed syscall count of a 1,296-record
log import on tmpfs found 1,361 `fdatasync` calls, 13,567 `statx` and 2,670
`getdents64`: preparation committed one synced batch per record and per RPC,
each with its own footprint measurement. Deep JSON costs more per record (about
100 ms on tmpfs) but less per byte; that cost is JSON-specific and stays parked
under the 2026-10-27 ruling.

Preparation now commits up to 256 ordinary records per synced batch, adding
records after the first only while their source stays within 256 KiB. Measured
back to back under the same load:

| Input (NVMe) | Before | After |
| --- | --- | --- |
| 1 MiB opaque | 0.6 s | 0.55 s |
| 1 MiB log | over 60 s (lease cut) | 7.8 s |
| 4 MiB log | over 60 s (lease cut) | 67.5 s (resumed after a lease cut) |
| Deep-JSONL chunk | 5.7 s | 3.0 s |

On tmpfs the same inputs now take 5.8, 27.8 and 2.0 seconds, so a short record
costs about 1.1 ms of processing and 1.5 ms on NVMe.

### Stage timing, store size and footprint (2026-10-29)

A temporary probe drove the store's import calls directly and timed each stage
on a reflink copy of the 4.2 GB production backup and on fresh stores.
Preparation dominates. Every other stage of a small file takes milliseconds.
A 1 MiB log on a fresh store spent 11.0 seconds preparing against 1.7 for
append, 3.7 for layout and 0.9 for publish; on the large store a log's publish
took 2 to 14 seconds. On the large store, preparing one 2 KB JSON record took 2 to 5
seconds and read 77 to 117 MB, against 0.3 seconds on a fresh store. The
projection graph write took 2.06 of 2.09 seconds; JSON facts took 6 ms and text
2 ms.

The cause of the store-size dependence is repeated dictionary reads. The
projection of one new 2 KB record looked up about 14,000 terms, but only 2,237
were distinct, and 95 percent were already stored. Each repeat of a stored term
cost a reverse lookup and a forward verification read, and on a large store
each such read can touch a block on every level. The term writer now remembers
stored terms for the rest of its batch, under its own bound equal to the
staging budget for new terms. Ten files took 4.1 to 4.2 seconds with the cache against 10.0 to 11.8
without it, in both run orders, so the gain is 2.4 to 2.9 times. Bloom filters
were tried and rejected: with filter and index blocks in a 256 MiB cache they
thrashed and read 15 to 56 GB, and without that cache their gain was unproven
while their memory would grow with about 331 million keys.

The same 1 MiB log (14,743 records) on the large store shows the cache's effect
on short records. Before it, preparation took 118.9 seconds with 1,234,857
read calls and 1.84 GB read from the device. With it, three runs on fresh
copies took 72.1 to 103.5 seconds with about 135,000 read calls and 0.55 to
0.73 GB from the device. The spread is host noise under load averages of 25 to
46: the slowest run had the lowest load but one. A
fresh store prepares the same log in 11.0 seconds, so the large store still
costs seven to nine times as much. Skipping the stored-count read for terms
this batch creates was tried and rejected: it left the read calls unchanged,
because those keys sort above every stored count, and its time and device
reads stayed within the same noise.

Counting the dictionary's reads showed where the rest went. Of 61.2 seconds of
preparation, 44.7 went to 58,762 reverse lookups that missed, about 0.76 ms
each; 1,508 hits, their forward verifications and 60,270 reference-count reads
took 1.6 seconds together. Reverse keys are hashes, so each miss lands inside
every level's key range. They now live in their own column family with a
whole-key Bloom filter. A prototype on two fresh copies of the large store
prepared the log in 7.7 and 20.8 seconds against 58.6 for the cache alone,
with 10,049 to 12,096 read calls instead of 134,542 and 24 MB read from the
device instead of 335 MB; a fresh store takes 11.0 seconds. The filter and
index blocks for the store's 6,762,463 reverse keys need 12.5 MB, where a
filter over the whole store would cover about 331 million keys. The prototype
copied those keys into the family in 17 to 21 seconds; the shipped upgrade
moves them once, when the store first opens.

Before either change, a 256 KiB log (3,715 records) on the large store spent
16.5 of 20 seconds in its records' metadata graphs. The thread's own read
counters showed about five reads per record, which suggested CPU-bound work
with no single profile hotspot. The dictionary count above shows those graphs
were waiting on reverse misses. With the reverse family, two 256 KiB logs on a
fresh copy of the large store prepared 3,663 records in 3.63 seconds and 3,714
in 3.06, about 0.8 to 1.0 ms per record.

The footprint was measured by importing 1,048,642 bytes of synthetic log lines
(14,743 records of about 71 bytes) into a fresh store. The store directory held
221 MB, but 190 MB of it was write-ahead log; compressed tables held 31 MB,
about 2.1 KiB per record. The log is a bounded overhead rather than a
per-record cost: the 11.8 GB production store retains 254 MB of it. The earlier
estimate of about 100 MB per 1 MiB log measured small stores, where that
overhead dominates. Uncompressed, the rows came to 181 MB, about 12.0 KiB per
record:

| Rows | Per record | Share |
| --- | ---: | ---: |
| RDF quad index keys (12 quads in 6 orders) | 72 | 31.1% |
| Text evidence | 15 | 26.7% |
| Text postings | 15 | 9.1% |
| Import layout record copy | 1 | 5.2% |
| Record metadata graph | 1 | 5.0% |
| Record catalog entry | 1 | 4.7% |
| RDF dictionary (term, reverse, reference) | 12 | 8.1% |
| Other catalogs | 6 | 10.0% |

Text evidence and postings values are catalog JSON, so shrinking them waits for
the JSON plan. The six quad orders serve SPARQL access paths, and reducing them
is a query design change rather than a fix. What remains open on issue #5 is
the parked JSON work: the catalog codec, the deep-JSON projection cost and its
expansion.

## Table options and dictionaries (issue #19)

On 2026-11-01 a reflink copy of the live store was opened read-only and
sampled in contiguous 32 MiB key slices: 1.18% of the default family, about
half of `payload` and 8% of `rdf_reverse`. Under the former options the sample
compressed 8.98 times, against 8.89 times for the live default family. Each
candidate was loaded into a fresh store with the production family options,
then compacted to the bottommost level. Table sizes repeated exactly across
runs, and compaction CPU within 5%.

| Candidate | default | payload | rdf_reverse | default compaction CPU |
| --- | ---: | ---: | ---: | ---: |
| Former: 4 KiB blocks, ZSTD level 3, no dictionary | 1.000 | 1.000 | 1.000 | 10.4 s |
| 32 KiB blocks, level 3 | 0.795 | 0.833 | 0.950 | 11.4 s |
| 64 KiB blocks, level 3 | 0.775 | 0.773 | 0.946 | 10.2 s |
| 4 KiB blocks, 112 KiB dictionary, level 3 | 1.099 | 0.466 | 0.970 | 14.7 s |
| 32 KiB blocks, level 6 | 0.723 | 0.796 | 0.946 | 12.0 s |
| 32 KiB blocks, level 9 | 0.688 | 0.789 | 0.945 | 18.6 s |
| 32 KiB blocks, level 6, 112 KiB dictionary | 0.724 | 0.435 | 0.951 | 17.1 s |

Ratios are table bytes against the former options for the same family. The
default family held 88% of table bytes. Its quad index keys are already
prefix-delta encoded within each block, so a dictionary is neutral at level 6
and adds 40% compaction CPU. Payload rows halve with a dictionary, but that
family is about 1% of the store. Reverse keys are hashes, within about 5% of
their entropy under every candidate, and 16 to 64 KiB blocks cost 10 to 70
times the point-read CPU of 4 KiB blocks when lookups miss.

The chosen options are owned by `storage::table_options`:

- default and `payload`: 32 KiB blocks, LZ4 above the bottommost level and
  ZSTD level 6 at it;
- `payload` also trains a 112 KiB bottommost dictionary from up to 100 times
  that many sample bytes;
- `rdf_reverse`: 4 KiB blocks, its whole-key Bloom filter and the former
  bottommost level;
- info logs: at most 10 files of 8 MiB.

Applied to the live level sizes, these options would store the 15.74 GB of
tables and blobs in about 12.02 GB, 24% less. This is an extrapolation from the
sample, and the default family is its reliable part. Default compaction costs
about 15% more CPU. ZSTD above the bottommost level would save about 0.67 GB
more at 1.6 times LZ4's CPU on every flush and upper compaction. It is not
enabled. Level 9 saves 0.43 GB more than level 6 but cuts compaction throughput
by 35%, and that throughput is also how fast pressure frees space.

RocksDB rewrites a cold bottommost file only when a compaction from above
overlaps it, and these stores set no periodic compaction, so the old tables do
not convert on their own. The daemon rewrites the default and `payload`
families once through periodic compaction, one file at a time, as described in
[Architecture](architecture.md#42-deduplication-and-compression).

Not measured: read latency under production cache sizes, ingest throughput
under each option set, WAL compression and blob compression level.

## Schema 62 native graph and text follow-up

On 2026-10-19 signed source `bee07bc1690fe7fe3b85e533153ee18fee9349fd`
was measured with the existing 64-cohort workload and unchanged runtime limits.
PurRDF is locked to `a47940e1f130bf3739c6d59aaf89643209905ea4`.
The frozen load image is `load-schema62-bee07bc`, SHA-256
`104d2d08b68d693cc85a090f07d415114148626abb7feadc1eea86acb4ecbd51`.
Private daemon/client copies match the default-feature production images in
[Development runtime](development-runtime.md#schema-62-live-adoption), without
file capabilities. The example enables core `test-support`; the daemon enables
no core features.

Evidence is `~/.local/state/sparrow/typed-load-qualification-v2ilghez`:
collector/images, source identity, configuration, host/filesystem, saved inputs,
native report, 100 ms RSS/I/O samples and closed files. The shared Ryzen/Btrfs
host uses a fresh store, loopback TCP, 2 GiB application budget, 128 MiB reserve,
four operation slots and no watch/syslog intake. No hard quota was configured.

All 256 durable intakes and 28 concurrent complete captures pass, accepting
17,358,454 raw bytes and 8,320 records in 23.336038 seconds, including fixture
generation, exact SDK byte readback and final queries. Mean/maximum durable
intake is 74.137/525.714 ms; mean/maximum concurrent full capture is
792.234/6,481.934 ms. Individual microsecond observations are retained rather
than extrapolated into guarantees. Final warm traversals return:

| Operation | Rows | Pages | Milliseconds |
| --- | ---: | ---: | ---: |
| Records | 8,320 | 57 | 275.882 |
| Text | 8,256 | 80 | 459.919 |
| Keys | 5 | 1 | 192.477 |
| Selective values | 64 | 1 | 165.300 |

The initial collector failed in its later native-check harness because it
assumed 129 ordinary text results fit one page; byte admission returned 101
and a continuation. The passing intake report and failed collector outcome
are both preserved. After correcting the harness to follow continuations,
only the native phase was repeated on the same private store, reopened in a
fresh daemon process with existing filesystem data. The successful report is
`native-v2/report.json`; the original `native/` failure remains unchanged.
The original reopen receipt mislabeled schema 62 as `source`; a separate
`.stage/native-graph-integration/schema62-native-load-qualified.json` records
both schema and full source commit without rewriting that receipt.

The first four artifacts independently contain 130 records and 129 text
documents. Metadata COUNT returns 130; stored native ranking returns all 129
in two pages with exactly the ordinary ordering, decimal scores and corpus
identity. Covered documents contain 900 tokens; the opaque record is not
applicable rather than an index failure. Complete graph COUNT takes 495.775 ms;
ranked text including continuation takes 826.819 ms; EXPLAIN takes 19.743 ms
and returns five entries. Timings include client/rendering/paging overhead.
Source/native certificates and limits are saved. All five read families finish
with zero handles/bytes. Whole-store graph COUNT separately returns typed
`RESOURCE_LIMIT: catalog capture exceeds operation admission` in 83.718 ms;
this is a measured refusal, not a successful broad count or a missing API.

Sampled daemon peak RSS is 243,508 KiB. After the initial clean shutdown, native
file lengths total 34,292,575 bytes and allocated blocks total 34,320,384 bytes.
Last sampled kernel write bytes are 557,989,888. Sampling includes the initial
failed native phase and excludes the reopened native phase; file totals precede
that reopen. These are not true peak bounds, isolated write amplification or
steady-state compression ratios. Both private daemons shut down cleanly.
Public PID 539293 and its 217-record store remain unchanged throughout.

## Workload and method

The `load` example measures durable ingestion concurrently with complete agent
query captures against an empty private daemon. It uses the generated SDK and
the same production command parser, typed operations, result renderers, cursor
dispatcher and release owner as CLI/MCP. JSON fixture serialization and report
serialization use PurRDF's native codec. No evaluator or text analysis is copied.

```bash
cargo build --locked --release -p sparrow-server --example load
target/release/examples/load --secret-file SECRET tcp://127.0.0.1:10330 /new/input/directory 16 64 1024 > report.json
```

Start an owned private daemon separately with a new store, no watches or syslog,
and a loopback TCP binding with its `rpc_tcp_secret_file`; pass that file as
`SECRET`. The example refuses anonymous or non-loopback endpoints, ports
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
