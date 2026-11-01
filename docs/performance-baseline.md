<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Copied-corpus import and query baseline

[Concurrent typed workload](load-qualification.md) supplies a repeatable private
ingestion/query executable through the production SDK and agent renderers.
It complements the copied-corpus and offline retention measurements below;
none establishes full production-rate or density acceptance.

The current schema-62 source has separate
[concurrent load and native graph/text measurements](load-qualification.md#schema-62-native-graph-and-text-follow-up)
and [sixty-day turnover results](turnover-qualification.md#schema-62-retention-and-native-compaction-policy).
Those refresh the changed native integration and reclamation paths; the
representative-corpus and four-importer burst measurements below remain
historical evidence from their identified images. A whole-store graph COUNT
on the new 8,320-record load store is explicitly refused at default catalog
admission. No budget was raised or successful full count inferred from that
refusal. No production throughput or density target has been supplied.

This is measured preliminary qualification, not full performance acceptance.
The initial 2026-10-08 experiment exposed broad chronological capture and
caller-controlled retirement limits. Later installed sources address those
limits; see [current runtime evidence](development-runtime.md) and the mixed
disk-backed paging qualification below. Historical measurements remain tied to
their original images. No budgets were relaxed to obtain successful runs.

## Mixed disk-backed paging during durable intake

Signed daemon source `5ca7eeb`, schema 45, was qualified on a private Btrfs clone
at `~/.local/state/sparrow/disk-query-concurrency-x_bve6d_`. Its `driver.py` and
`report.json` retain image SHA-256 fingerprints, configuration, every CLI attempt,
all native baseline rows, trial durations, input fingerprints, durable receipts,
byte checks, RDF inspection and sampled RSS. The installed public service's PID
was checked before and after and remained unchanged. No production source watch
was created or changed.

The initial corpus has 7,058 records. Six query workers each perform twenty
complete traversals while one sequential importer publishes twenty mixed JSON,
JSONL, Markdown, log and opaque inputs. Malformed JSONL and duplicate members
are included. Durable receipts add 140 distinct record occurrences from 22,044
raw bytes, yielding 7,198 records. Exact import retries preserve their receipts;
exported bytes match each original input fingerprint, and native RDF inspection
is recorded for every added record. Strict discovery over the malformed input
refuses with `INDEX_NOT_READY`; that intentional negative check is separate from
admission refusals.

Queries use the native scope/time boundaries to exclude the new intake, making
every complete result comparable to its frozen pre-load baseline. Broad queries
use an exclusive receipt-time upper bound one millisecond after the final
original receipt. Source, temporal, text, key and value scopes retain their native
counts, scores, ordering and witnesses. All 120 traversals exactly match their
baseline rows and page counts. Each multi-page lease is explicitly released.

| Query family | Rows | Pages | p50 seconds | p95 seconds |
| --- | ---: | ---: | ---: | ---: |
| Broad records | 7,058 | 48 | 0.208189 | 0.893225 |
| Source records | 286 | 2 | 0.130639 | 0.621055 |
| Temporal records | 3,529 | 24 | 0.132395 | 1.303984 |
| Native text | 12 | 1 | 0.086759 | 0.249548 |
| Keys | 51 | 1 | 0.087030 | 0.463866 |
| Values | 286 | 3 | 0.152098 | 0.451372 |

Durations include CLI startup, discovery, all continuation pages and admitted
retry delays; release time is separately retained per trial. p95 is an inclusive
interpolated percentile over twenty observations per family, without confidence
intervals. The concurrent phase lasted 9.134515 seconds. The driver retries only
explicit operation/staging admission refusals, with at most forty attempts and
bounded backoff; disconnects and other unexpected failures fail qualification.
The complete run records 2,459 CLI attempts, 365 explicit `RESOURCE_LIMIT`
operation refusals, zero staging-journal refusals and zero transport disconnects.
These refusal counts include retries and are not a request-level failure rate.
No connection, operation, page, lease, item or disk budget was raised: four
storage operations, sixteen connections, 1 GiB disk with 128 MiB maintenance
reserve, the existing 256 KiB pages and sixty-second leases remain in force.

RSS was sampled every 100 ms during the concurrent phase: eighty-seven samples
with a maximum of 164,896 KiB. This is an observed sampled peak, not an allocation
certificate or a guarantee that shorter peaks were captured. Closed-store file
lengths went from 22,531,779 to 23,062,280 bytes; allocated blocks went from
22,614,016 to 23,142,400 bytes. The final lengths comprise 21,706,917 SST bytes,
170,895 blob bytes, zero WAL bytes and 1,184,468 control/log bytes. Live admission
footprint after load is 24,596,642 bytes; after reopening it is 23,120,835 bytes.
Directory totals omit anonymous staging while it is held, and engine/log files
can change on reopening. These observations do not certify bounded physical
reclamation or steady-state density.

After load and after reopen, pins and result handles/bytes are zero. The reopened
owner preserves all twenty durable receipts, all 7,198 records and every original
query baseline, and closes gracefully. This qualifies concurrent offered load
for this finite warm corpus. The capture writer remains serialized; cancellation
latency, sustained/burst production volume, a deployment quota and native
reclamation admission remain separate unfinished requirements. Later source
`ebea75a` measures abandoned in-progress Unix/TCP captures on this finite corpus:
work finishes after client interruption, and owner maintenance removes its
anonymous staging at the result lease deadline. See
[abandoned query cleanup](development-runtime.md#abandoned-query-capture-cleanup)
for the observed cleanup intervals and preserved corpus. This does not establish
prompt cancellation or sustained abandonment acceptance.

### Four-importer burst

The independent private clone at
`~/.local/state/sparrow/disk-query-burst-is7zc4sc` uses the same qualified images,
budgets, six query families and twenty input formats. Ten workers start together:
six query workers and four durable importers, each assigned five inputs. All
120 complete traversals retain exact baseline rows/page counts, all twenty
durable receipts and original bytes survive retry/reopen, and the reopened store
has 7,198 records with zero pins and result handles/bytes. The public service PID
remains unchanged. The same explicit-refusal retry policy applies; transport
disconnects and unexpected failures are not retried into success.

| Query family | p50 seconds | p95 seconds |
| --- | ---: | ---: |
| Broad records | 0.175798 | 1.130155 |
| Source records | 0.114489 | 0.364287 |
| Temporal records | 0.116889 | 0.797748 |
| Native text | 0.118715 | 0.278884 |
| Keys | 0.056072 | 0.271236 |
| Values | 0.080717 | 0.567337 |

The concurrent phase lasts 8.403313 seconds. The complete run records 2,402 CLI
attempts, 308 explicit operation-admission refusals, zero staging-journal refusals
and zero transport disconnects, plus the intentional strict-index refusal.
Eighty-one 100 ms RSS samples have a maximum of 167,600 KiB. Closed file lengths
are 23,063,383 bytes and allocated blocks 23,142,400 bytes, including 21,708,080
SST, 170,895 blob, zero WAL and 1,184,408 control/log bytes. Admission footprint
is 24,585,668 bytes after load and 23,121,908 after reopen. The same sampled-peak,
anonymous-staging, native-log and physical-reclamation limits apply.

These independent finite runs establish correctness and explicit backpressure
under their recorded offered loads. Their timing difference is not evidence of
four-importer scaling or production-volume acceptance: neither run isolates CPU,
OS cache, native background work or other host workloads, and each family has
only twenty observations.

## Original baseline build, host and method

The installed daemon is signed source `008cd78`, SHA-256
`cd6aca0d425ffb4b57f401bf07538bdaf95517052979548aaf3d482eb834513b`.
The installed CLI is signed source `9406d95`, SHA-256
`7f57ba0de3c036b8e542ddf956345ab1e5c913bbcb229d1686a1f291dcbab532`.
Runtime PurRDF is pinned at `e58974e504b9681597faea1490002ce8ddaf7448`;
RocksDB is 0.25.0 with librocksdb-sys 0.19.0+11.8.1. The daemon uses its
installed, fingerprint-checked native lexicons. The new fixture reports schema
41 and store format 35; it has no completed-tail retirement state requiring 37.

The host is AMD Ryzen AI Max+ 395, 16 cores/32 threads, 133,618,966,528 bytes
of RAM, x86_64 Linux 7.2.7-1-cachyos. The measured directory is on
`/dev/nvme0n1p3`, Btrfs `@home`, with `compress=zstd:1`, SSD and noatime options.
Other workloads remained running. There was no CPU isolation or OS-cache purge.
An initial scratch experiment used tmpfs; those timings are not disk results.
The disk experiment reused byte-identical frozen copies.

Durable sequential file imports use the installed CLI and native SDK path over
a private Unix socket. Timings include CLI process startup, discovery, hashing,
upload, native projection/index publication and durable reply. They do not
measure public TCP throughput or concurrent producer capacity. The fresh-store
pass starts with an empty database, not a promised cold OS page cache. A second
pass creates new independent occurrences of identical inputs. Exact retries of
all eight requests preserve their original receipts and 7,058-record total.

Each query family uses a separate reopened owner of the same fixture, one
warmup and ten sequential trials. Successful durations include all continuation
pages. Published leases remain alive under the real 60-second, eight-handle,
8 MiB registry policy; the experiment does not silently discard them. Refusals
are counted separately from successful latency distributions. p95 uses Python's
inclusive interpolated percentile on the successful observations only; the
small samples provide no confidence interval or sustained-service guarantee.

Configuration is a 1 GiB disk budget, 128 MiB maintenance reserve, 16 MiB maximum
artifact, 100,000 records, 10,000 pin members and four concurrent operations.
No watches, syslog, production source authority or pins were configured.

## Frozen corpus

Each source was read-only. Prefixes were capped at 256 KiB and cut after the last
complete physical newline. Copied bytes, not active original paths, were imported.
No source changed during the bounded capture. These are selected samples, not a
complete corpus inventory; private bodies and identifiers remain outside Git.

| Sample | Original selection | Copied bytes | Records per pass | Coverage |
| --- | --- | ---: | ---: | --- |
| Corrections | `lillith_soul/inner-core/corrections/2026-09-10.jsonl` | 152,659 | 143 | Complete selected file |
| Discards | `lillith_soul/inner-core/discards/2026-07-20.jsonl` | 2,214 | 2 | Complete selected file |
| Plain log | `/var/log/pacman.log` | 262,080 | 3,380 | Prefix only |
| Mentation | `lillith_soul/inner-core/private-mentation/mentation.jsonl` | 161,931 | 4 | Prefix only, including a large record |

The corresponding SHA-256 fingerprints, in table order, are:

```text
e7016002028b58009a00c9009e43a599a8f28fcdb17ec62101db90e5193b35ca
1d49f838f6128a621dd4b80dc47da5efff2d1d8c2cd7591ffa63f7a9fa7aa80e
c1dfe1df6d4a16da49cfd527959d40bc88f45b8ff5253a25e31f463d4f59d7ac
1e4c60841c57663bdde4b0bcdae2c6c94d4867c075568dd099aa540eb4bb0347
```

## Observations

| Durable import pass | Raw bytes | Records | Summed request seconds | Bytes/s | Records/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fresh database | 578,884 | 3,529 | 6.828 | 84,784 | 516.9 |
| New duplicate occurrences | 578,884 | 3,529 | 5.397 | 107,253 | 653.8 |

These are serial CLI observations, not a daemon saturation benchmark. Both
passes retain separate occurrence IDs. Status reports 3,500 unique payload
chunks and 149 unique JSON projections for the 7,058 records. All 298 JSON
occurrences have complete native RDF inspection; 6,760 log occurrences have
content projection not applicable. Inspecting all records in bounded offline
batches finds 149 distinct complete content graphs with 85,400 primary rows,
no reifier/annotation rows, 70,878 occurrence metadata rows and no mapped
diagnostic rows. Inspection uses the signed `65d68b9` native recovery probe;
it does not implement another projector or imply SPARQL qualification.

The sampled daemon RSS peak during imports was 221,748 KiB, sampled every
200 ms. This is process RSS, not an allocation bound or a guarantee that a
shorter peak was observed. Native cache use, write amplification and peak
physical file growth were not separately measured. File-byte totals changed
during native flush/compaction; status during import must not be equated with
the final closed-store footprint. The final component snapshot is:

| Closed-store component | File bytes |
| --- | ---: |
| SSTs | 21,241,084 |
| Blobs | 170,895 |
| WALs | 0 |
| Control/options/engine logs | 947,409 |
| Total file lengths | 22,359,388 |
| Allocated bytes from `st_blocks` | 22,433,792 |

The two passes accepted 1,157,768 raw bytes. The measured closed-store file
lengths are 19.31 times that input, so this small semantic/indexed fixture does
not demonstrate a whole-store density win. This includes inspection/reopen log
growth and is neither a steady-state ratio nor a strict filesystem quota
measurement. Executable and lexicon assets live outside the measured store.
SSTs combine catalog, dictionaries and indexes, so this
experiment does not isolate graph/text index bytes from that family.

| Query | Successful trials / 10 | Complete rows / pages | Successful p50 ms | Successful p95 ms | Refusal |
| --- | ---: | ---: | ---: | ---: | --- |
| All records | 0 | — | — | — | 2 MiB chronological capture ceiling |
| Correction source | 7 | 286 / 2 | 27.56 | 28.82 | Three handle-count refusals |
| First-pass receipt interval | 4 | 3,529 / 24 | 69.28 | 75.75 | Six aggregate snapshot-byte refusals |
| Native text `error`, all sources | 10 | 12 / 1 | 58.06 | 66.46 | None |
| Correction keys | 10 | 51 / 1 | 34.80 | 36.29 | None |
| Correction `/message` values | 10 | 0 / 1 | 35.20 | 36.65 | None; absence probe only |

A separate positive exact decoded-value lookup uses a native-discovered
`/timestamp` value. All ten trials return the two independent occurrences on
one page; p50 is 39.73 ms and p95 40.28 ms. The actual value stays in private
evidence. These queries use receipt-time scope; event-time profiles were not
declared, so this does not benchmark normalized event-time filtering.

The broad-record failure occurs before a page is published and leaves stdout
empty. Lowering CLI page size does not reduce the complete chronological
capture. The source query reaches eight retained replayable handles, and the
interval query reaches the aggregate snapshot-byte ceiling with five handles.
Completed traversal retains earlier cursors for replay until expiry. Single-page
queries publish no retained continuation handle and do not have that throttle.
These are genuine operational limits, not incomplete answers relabeled as passes.

The baseline identified a need for an explicit shared lease-release operation
and a qualified bounded strategy for broad chronological captures. They must preserve
immutable replay until release/expiry and refuse storage errors without a
successful prefix. Raising constants to fit this experiment would not resolve
the required workload behavior.

## Evidence and remaining acceptance

Disk evidence is
`/home/paudley/.local/state/sparrow/qualification-hqh1y0zw`:
`original-source-manifest.json`, `corpus.json`, immutable input copies,
`config.toml`, `run.py`, `run.log`, `report.json`, `matrix.py`, `matrix.log`,
`matrix-report.json`, per-family daemon logs, `rdf-inspection-*.json`,
`rdf-summary.json`, `positive-value-report.json` and `store-physical-final.json`.
The original tmpfs
experiment is `/tmp/sparrow-performance-baseline.8yyvo1pw`; its initial collector
stopped on the broad refusal and then on a lease-capacity refusal. Later matrix
collection records both successes and refusals rather than suppressing them.
Do not rerun the fresh-store driver against its existing exclusive copies/store.
The matrix can rerun on a closed fixture; it explicitly starts/stops only its
isolated owner, never `sparrow.service`.

During the initial baseline, the live server remained PID 1710023, serving
schema 41 on `0.0.0.0:518` with 212 records. It was not stopped, reconfigured or
used for those sample imports. Subsequent schema 42 adoption is recorded in
[Development runtime](development-runtime.md).

## Explicit-release follow-up

Signed source `b53f163` implements explicit result release without changing any
capture, aggregate byte, handle or expiry limit. The same closed 7,058-record
Btrfs fixture was reopened using installed schema 42 daemon and CLI images from
that source. Each family had an independent owner, one warmup and ten sequential
trials. Successful complete traversal was followed by `result-release` using
the first page's snapshot identity. All multi-page release receipts reported
`released=true`. The fixture remains format 35; no new import was performed.

| Query | Successful trials / 10 | Complete rows / pages | Successful p50 ms | Successful p95 ms | Refusal |
| --- | ---: | ---: | ---: | ---: | --- |
| All records | 0 | — | — | — | 2 MiB chronological capture ceiling |
| Correction source | 10 | 286 / 2 | 30.42 | 31.23 | None |
| First-pass receipt interval | 10 | 3,529 / 24 | 62.68 | 67.59 | None |

These timings include CLI startup and complete pagination; the separate release
round trip is recorded as `seconds_with_release`, outside the table timings.
The same inclusive, success-only p95 method applies. This small sequential
experiment establishes removal of retained-lease pressure for these scopes,
not a throughput improvement or concurrent-load qualification. Each family's
final status reports zero retained record handles and bytes, with all 7,058
records present. The broad capture refusal remains actionable work.

Evidence in the same disk directory is `schema42-release-matrix.py`,
`schema42-release-matrix.log`, `schema42-release-matrix-report.json` and
`schema42-release-*-daemon.log`. The driver uses immutable installed images
`sparrowd-schema42-b53f163` and `sparrow-schema42-b53f163`, and verifies that
the production unit remains PID 2062025 throughout this separate qualification.

## Disk crash recovery

A separate Btrfs clone of the closed 7,058-record fixture qualifies two abrupt
process-death boundaries using the installed schema 42 daemon `b53f163` and CLI
`b17f121`. Its persistent identity is inherited from the cloned fixture, not a
new independent production store. No configured producer input or TCP listener
was added. Evidence is
`/home/paudley/.local/state/sparrow/crash-qualification-ci38zu1q`: `driver.py`,
`config.toml`, `report.json`, per-phase daemon logs and exact fetched bytes.
The original fixture remains untouched; the clone ends closed with 7,059 records.

First, the CLI durably protected one record and one artifact, renewed the same
collection to revision 2, and durably imported one synthetic log record. The
collector issued SIGKILL immediately after the successful import reply and
confirmed/reaped that child's `-9` exit. After reopening, the new record and exact
bytes remained, original create/renew/import retries returned their original
receipts, and the active collection retained revision 2. A captured continuation
from the previous owner refused with `RESULT_EXPIRED`.

Second, the CLI released that collection and issued SIGKILL immediately after
the successful release reply. Reopening preserved zero active pins; stable
release replay returned its original receipt. Stable original create replay
also returned its receipt without resurrecting protection. The durable imported
record remained exactly the same occurrence.

After each reopen, all eight original durable imports were replayed with their
original arguments and checked against complete original receipts. Every original
artifact was fetched and compared byte-for-byte with its immutable copied input
(1,157,768 bytes across the eight occurrences). Five complete indexed answer sets
were compared against pre-crash rows, including native text scores and witnesses:

| Query scope | Complete rows / pages |
| --- | ---: |
| Correction records | 286 / 2 |
| First-pass receipt interval | 3,529 / 24 |
| Pacman log text `error` | 12 / 1 |
| Correction keys | 51 / 1 |
| Correction `/timestamp` values | 286 / 3 |

Each successful multi-page traversal explicitly released its capture. The text
corpus is source-scoped so the independent synthetic import cannot legitimately
change its native scoring inputs. These are exact replay/answer comparisons,
not latency measurements. The production unit remained PID 2062025 throughout.
Stale Unix endpoints were removed only after the disposable child was confirmed
dead and reaped; the production socket was never removed.

This establishes recovery after process termination at the two acknowledged
durable publication boundaries. It does not simulate power loss, kernel or device
failure, WAL corruption, interruption inside a storage commit, unknown buffered
delivery, forced reclamation, or every query operator. Those acceptance cases
remain separate.

## Concurrent mixed-format workload and staging retirement

A six-worker trial on another closed Btrfs clone exposed completed imports holding
all 16 staging slots until lease expiry. The initial fixture already contained
eight completed journals. The corrected collector admitted eight new files, then
refused the ninth with `RESOURCE_LIMIT: 16 import journals are occupied`. This
finding is preserved in `concurrent-qualification-bci7r_jj` under the same state
directory. Earlier collector stops are also preserved in
`concurrent-qualification-3ua4xt7v` and `concurrent-qualification-g9a0cysj`
(observed disconnected transport forms), and `concurrent-qualification-u3zrctgq`
(incorrect zero-record expectation for opaque input). Each has `report.json`,
daemon logs and `collector-failure.log`; none is a successful trial.

Signed source `7f01e3e` reclaims completed staging at the existing durable
publication boundary. Durable retry receipts and evidence remain independent.
The 16-journal and 128-unit ceilings are unchanged; still-live unfinished uploads
retain their recovery leases. A regression completes 32 imports at one injected
clock, then verifies retries, bytes and native RDF after reopening. It separately
verifies that 16 unfinished uploads retain their bytes and refuse a seventeenth.
The full hook passed 141 storage scenarios, 51 RPC scenarios, 17 SDK tests and
215 ownership rules with no distinct exceptions.

The successful trial uses installed daemon `sparrowd-schema42-7f01e3e` and CLI
`sparrow-schema42-b17f121` with the original four-operation/four-connection
configuration. It starts five query workers and one sequential importer together
over a private loopback Cap'n Proto TCP endpoint. Every query worker performs
20 complete traversals, checks exact rows against a pre-load baseline and releases
its capture. The import worker accepts 20 inputs, four each of JSON, JSONL,
Markdown, logs and opaque bytes, and repeats every request with identical arguments.
JSONL includes one malformed physical record per input. Non-line formats,
including opaque bytes, have one whole-input record under the existing layout law.

The collector records every failed CLI attempt. It retries only the precise
operation-concurrency, staging-capacity or two observed disconnected transport
outcomes, with unchanged request arguments, at most 40 attempts and bounded
backoff. Other errors or successful stdout on a refusal abort the trial. This
is collector retry behavior, not a new daemon or generic CLI reconnect policy.

All 100 traversals and 20 imports completed. The timed concurrent interval was
8.7265 seconds, including CLI startup, complete pagination, releases, retries and
the final stable ingestion replays. The importer accepted 22,044 bytes and 140
new records; the store grew from 7,058 to 7,198 occurrences. Its 20 first durable
replies had p50 182.51 ms and inclusive p95 538.45 ms, including SDK upload/native
projection and bounded retries but excluding the following explicit stable replay.

| Query | Trials | Complete rows / pages | p50 ms | Inclusive p95 ms |
| --- | ---: | ---: | ---: | ---: |
| Correction records | 20 | 286 / 2 | 132.78 | 649.06 |
| First-pass receipt interval | 20 | 3,529 / 24 | 170.10 | 2,165.56 |
| Pacman log text `error` | 20 | 12 / 1 | 105.23 | 240.17 |
| Correction keys | 20 | 51 / 1 | 48.62 | 166.04 |
| Correction `/timestamp` values | 20 | 286 / 3 | 211.09 | 336.24 |

Query timings exclude the separate release round trip, recorded as
`seconds_with_release`, and include reconnect backoff. There were 894 CLI attempts
during load: 760 successful invocations and 134 disconnected attempts (91 peer
disconnects and 43 TCP resets). No typed RPC-concurrency or staging-capacity
refusal occurred in this repaired trial. The four-connection ceiling deliberately
closes unadmitted connections; it does not give them a typed overload receipt.
Successful workflow counts must not conceal these refusals or claim unlimited
client admission. CLI invocations can contain multiple native RPC calls, so these
are not native RPC request counts.

All 20 artifact byte sequences match their recorded SHA-256 identities. Native
inspection of every new record reports 68 complete projections, 68 not applicable
and four failed malformed JSONL projections. Non-strict key discovery reports
complete execution with incomplete certainty and four failed JSON records;
strict discovery refuses `INDEX_NOT_READY` without successful stdout. Stable
import replay and all five original scoped answer sets also match after reopening.
The final query registry has zero retained handles and bytes.

Sampled load RSS peaks at 168,504 KiB, observed every 100 ms; shorter peaks and
allocation/cache bounds are not established. Closed-store file lengths grow from
22,531,779 to 23,059,920 bytes (SST 21,704,537, blobs 170,895, control/logs
1,184,488, WAL zero); final allocated blocks total 23,142,400 bytes. These totals
include cloned/reopened engine logs and ongoing compaction, not steady-state
index costs or a compression win. The small synthetic workload is not a measured
saturation rate, sustained-load envelope or numerical deployment target.

Successful evidence is
`/home/paudley/.local/state/sparrow/concurrent-qualification-q3wdyj1_`:
`driver.py`, `report.json`, `collector.log`, exact input/output bytes, configuration
and daemon logs. The production unit stayed PID 2222752 during this trial; no
production data or producer input was used. The two disk SIGKILL/reopen boundaries
were repeated with the new daemon and passed, including original receipts/bytes,
pins and query answers. Their new evidence is
`/home/paudley/.local/state/sparrow/crash-qualification-y5vxargh`; the earlier crash
qualification remains historical evidence, not proof about the new binary.

Full performance acceptance remains unmet: complete corpus census and size/
repetition distributions, full JSON/Markdown/malformed/opaque input mix,
representative sustained concurrent ingestion and maintenance, sustained/burst
backpressure, 30-day
turnover, sparse/dense pins, forced reclamation, cache/write-amplification
measurement, and numerical deployment targets remain required. This baseline
does not establish whole-store steady-state compression or capacity for the
intended daily workload.

## Independent connection budget repeat

Signed daemon `608327f` repeats the same copied-corpus workload with four
simultaneous storage operations and an explicit independent 16-connection
ceiling. Evidence is
`~/.local/state/sparrow/connection-budget-qualification-zxoxdknt`: frozen driver,
configuration, per-phase daemon logs, input/output bytes, native RDF inspections
and complete report. It clones the closed 7,058-record Btrfs fixture and opens
only its private Unix/loopback TCP endpoints, with no configured intake. Production
PID 2551939 remains unchanged throughout qualification.

Five query workers complete 20 traversals each while one sequential importer
publishes 20 mixed JSON, JSONL, Markdown, log and opaque files, with exact stable
retries. The timed workload takes 9.014819 seconds, with 1,002 CLI attempts:
760 successful and 242 typed operation concurrency refusals. There are zero
transport disconnects and zero staging-journal refusals in this run. The previous
four-connection run recorded 134 disconnects. This change preserves actionable
operation overload under a finite transport budget; it does not eliminate
backpressure or establish a latency/throughput improvement. The collector retains
its documented bounded retry policy and unchanged mutation arguments. Every
failure is retained in the report; refusal stdout must be empty.

| Query family | Rows / pages per traversal | p50 ms | inclusive p95 ms |
| --- | --- | --- | --- |
| Source records | 286 / 2 | 120.12 | 320.05 |
| Half-open receipt interval | 3,529 / 24 | 181.27 | 2,084.04 |
| Scoped native text | 12 / 1 | 118.32 | 204.34 |
| Source keys | 51 / 1 | 83.80 | 230.75 |
| Source timestamp values | 286 / 3 | 142.89 | 409.67 |

These timings include CLI startup, bounded backoff and complete pagination;
explicit release is recorded separately. The 20 durable imports add 140 records
and 22,044 accepted raw bytes; final count is 7,198. All uploaded original bytes
match their SHA-256 identities, projection inspections retain the known malformed
JSONL failures, strict key discovery refuses `INDEX_NOT_READY`, and reopen keeps
all durable import receipts and five original query answer sets unchanged.
Read handles and retained read bytes return to zero. The complete collector has
1,258 CLI attempts, including post-load inspection, reopen and the one intentional
strict-index refusal; the 242 concurrency refusals are all in the timed load.

Sampled RSS peaks at 168,224 KiB at 100 ms intervals; this is not an allocation or
true peak bound. Closed file bytes grow from 22,531,779 to 23,061,038; final SST
files occupy 21,705,586 bytes, blobs 170,895, controls 1,184,557, and WAL zero.
Allocated space is 23,142,400 bytes. Reopening modifies native engine logs, so this
is a fresh clone baseline rather than the original immutable artifact size.
This small warm workload does not establish sustained turnover, sparse/dense pin
capacity, full-corpus distributions, forced physical recovery or complete
performance acceptance.

The separate [offline turnover qualification](turnover-qualification.md#qualified-run)
now verifies 60 simulated daily cohorts on three new disk stores with no pins,
one sparse record pin, and dense protection of every accepted artifact. It records
exact bytes, native RDF stability, bounded expiry/reopen, high-entropy input,
logical cleanup and physical lag. It does not establish forced reclamation or
production-volume performance acceptance.
