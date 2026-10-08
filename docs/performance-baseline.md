# Copied-corpus import and query baseline

This is measured preliminary qualification, not full performance acceptance.
The 2026-10-08 experiment exposed two query-capacity limits that need further
implementation: broad chronological capture and caller-controlled retirement
of replayable result leases. No budgets were relaxed to obtain successful runs.

## Build, host and method

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

Full performance acceptance remains unmet: complete corpus census and size/
repetition distributions, full JSON/Markdown/malformed/opaque input mix,
concurrent ingestion and maintenance, sustained/burst backpressure, 30-day
turnover, sparse/dense pins, forced reclamation, cache/write-amplification
measurement, and numerical deployment targets remain required. This baseline
does not establish whole-store steady-state compression or capacity for the
intended daily workload.
