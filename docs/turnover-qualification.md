<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Offline turnover qualification

## Schema 62 retention and native compaction policy

On 2026-10-19 the larger profile was rebuilt from signed source
`bee07bc1690fe7fe3b85e533153ee18fee9349fd`, with PurRDF
`a47940e1f130bf3739c6d59aaf89643209905ea4` and the shared native deletion
collector in [Native reclamation](native-reclamation.md). The normal source gate
passes 195 storage, 69 RPC, 32 core and 18 server tests, other workspace tests,
release Clippy and 300 census ownership jobs without distinct exceptions.
The frozen image is `turnover-schema62-bee07bc`, SHA-256
`619729bb367b2f32571a06214d330cf61b295e068f42cd6a9d62d655484eea98`.
Its core feature is `test-support`; default-daemon qualification is separate.

The corpus remains 1,282 records per day, 60 simulated days, repeated and
changing/malformed JSONL, CRLF logs and 1 MiB random opaque input per day:
76,920 records and 65,642,380 raw bytes per mode. The shared Ryzen/Btrfs host,
2 GiB application budget, 128 MiB reserve, 600-second per-mode deadline and
8 GiB host free-space-delta guard remain unchanged. The guard is not a quota.
All daily counts, independent byte checks, exact expiry boundaries, sparse
parent expiry, protected native RDF, release and four reopen points pass.
Final records, pins, payload/JSON references and all five read-handle/byte
families are zero without resurrection. Final format is 86 for these stores
without production watches or receiver definitions.

| Mode | Evidence directory under `~/.local/state/sparrow/` | Records at day 59 | Sampled peak RSS KiB | Maximum daily file bytes | Closed file bytes | Wall seconds | Final expiry passes |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| None | `turnover-v2-qualified-e1djmj65` | 38,460 | 167,696 | 358,536,414 | 13,525,535 | 535.909 | 1,212 |
| Sparse record | `turnover-s62-qualified-fyt6j6kw` | 38,461 | 188,464 | 350,790,925 | 27,417,640 | 130.927 | 1,212 |
| Dense all artifacts | `turnover-s62-qualified-lvglva9g` | 76,920 | 204,668 | 367,297,759 | 40,847,889 | 174.776 | 2,423 |

The original sequential collector completed none mode but stopped sparse mode
when the host free-space delta exceeded its 8 GiB guard after all 60 daily rows
and before final cleanup. Sparse mode in that attempt is not qualified. Owned
store/evidence sizes did not explain the whole filesystem delta; its cause was
not attributed. The failed attempt is retained. Sparse and dense then ran in
independent observation windows with the same numerical guards and workload;
only mode selection changed. Passing none mode was not repeated. Collector
source hashes and the method change are retained in
`.stage/native-graph-integration/schema62-turnover-single-mode-collector-provenance.json`.
Different cache/host activity makes timings unsuitable for causal comparisons.

Final allocated bytes are 13,565,952 / 27,467,776 / 40,890,368. Each mode retains
240 WAL bytes; final blob bytes are 11,553,706 / 22,056,906 / 35,711,128.
Daily file samples and closed component totals expose physical reclamation lag
after zero logical ownership. The separate real-pressure test proves file
decrease and actual retry admission. Last sampled process write bytes are
6,753,640,448 / 5,512,974,336 / 5,942,345,728, including input creation, reports,
native WAL, indexes and maintenance rather than isolated engine amplification.
RSS is sampled every 100 ms and files once per day, not at every allocation.

These results qualify the declared finite turnover workload. They do not set a
production SLA or establish 10 GiB/day density, hard-quota recovery or a hard
native allocation bound. No new upstream API follows from those limits.
Public PID 539293 remains unchanged throughout; no producer root or public
store was used for generated input.

The production RDF view and governed SPARQL API were also run at day 90 on
SHA-256 verified closed copies of all three successful stores. Every copy has
zero dictionary terms and selected records/artifacts, and completes
`ASK { ?s ?p ?o }` with false. Original closed stores remain unopened by this
probe and hash-identical (18/20/20 files). The temporary example was removed
from the source tree after freezing the image; its source, selected build,
copy manifest, reports and `schema62-turnover-graph-qualified.json` are archived
under `.stage/native-graph-integration/`. Image SHA-256 is
`a03cd94e8a8fe7a3ee048b1f1c8de1360ee238eb413d41521a7f8b47a334b38e`.
Source admission uses its defaults; native ceilings are 100,000 fuel, 100 answers,
10,000 intermediate cells, 1 MiB scratch and zero remote requests. This proves
logical graph closure, independently of residual physical file bytes.

## Workload and method

The `turnover` example qualifies retention through production core operations
using an injected UTC clock. It accepts a configuration and one pin mode:

```bash
cargo build --locked --release -p sparrow-server --example turnover
target/release/examples/turnover /absolute/path/config.toml none > report.json
# Optional fixture sizes: repeated/changing JSON rows, log rows, opaque bytes.
target/release/examples/turnover /another/config.toml dense 128 1024 1048576 > report.json
```

The configuration must select a nonexistent data directory with no consume,
syslog or TCP binding. The tool opens no transport, worker or producer source.
Existing stores are refused before opening. Run each mode against its own new
store. Configuration errors, storage failures and failed assertions terminate
with nonzero status; partial output is not a successful report.

By default, sixty simulated daily cohorts each contain eight repeated JSONL records, eight
changing JSONL records plus one deliberately malformed record, 32 repeated CRLF
log records and one 64 KiB OS-random opaque record. Native JSON serialization,
ingestion, projection, hashing, exact bytes, pin protection and dependency
retirement retain their existing owners. Generated inputs are saved beside the
store in `store.inputs`; each accepted artifact identity, native content hash and
byte length is recorded. Optional sizes must be positive and fit configured
artifact/record admission; production storage limits still apply. No tokenizer, projector or retention implementation is
copied into the qualifier.

`none` protects no evidence. `sparse` protects one record from the first JSONL
artifact. `dense` adds all four artifacts from every daily cohort to a named
pin. Each pin mutation requests 90 days and uses its own stable request identity.
The independent count oracle derives records from the fixture specification
(`2 * JSON_ROWS + LOG_ROWS + 2`, default 50), verifies each receipt against that
count, and applies a 30-day ordinary window and the selected protection. It does
not derive expected counts from returned receipts. Every daily traversal uses
finite production expiry passes. The qualification ceiling is twice the total
specified record count plus 2,048 passes; production per-pass limits are unchanged.

Daily verification checks exact bytes for every retained whole artifact,
unavailability of expired parents, sparse record bytes after its parent expires,
and native RDF inspection of the first protected cohort. The report preserves
those native inspection results for comparison across days. Report format
`turnover/2` streams daily rows to `store.inputs/daily.jsonl` and individual native
inspections to `store.inputs/pinned-rdf.jsonl`; the final stdout summary names
both paths and carries `complete: true` only after all checks succeed. A partial
JSONL file is diagnostic evidence, not a successful qualification. Exact-byte
comparison uses 64 KiB file-backed reads. The tool retains occurrence metadata
and first-cohort record IDs, rather than all sixty input bodies or RDF reports.
Stores reopen at days
29, 30 and 59, retaining identity and membership. At day 60 pins are released;
expired first-cohort bytes and attempts to pin them must refuse. Day 90 cleanup
and another reopen must leave zero records, active pins, payload ownership and
read leases without resurrection.

This is a clock-injected correctness and physical-growth workload on real disk,
not 60 days of wall-clock service activity. Daily status measures file bytes;
the external collector records closed SST/blob/control/WAL sizes and sampled
RSS separately. Zero logical ownership does not certify zero physical disk use.
Automatic native compaction is allowed. The installed schema-62 policy marks
deletion-heavy tables using existing APIs; the earlier proposed binding
prerequisite is withdrawn. See [native reclamation](native-reclamation.md) for
measured pressure recovery and its remaining qualification. The historical
turnover reports below predate that policy and must not certify it. This does not qualify
production-volume throughput, steady-state density, quota pressure or full
architecture acceptance.

## Historical default-size qualified run

The evidence below uses the preceding report format, which embeds daily and RDF
rows in the final JSON document. It does not qualify the optional larger profile
or the streaming report implementation.

Signed source `c5e776c` passed the full source gate and normal commit hook:
142 storage scenarios, 52 RPC scenarios, 17 SDK tests, remaining workspace
tests, release Clippy and 219 helper ownership rules with zero distinct
exceptions. Logs are `/tmp/sparrow-turnover-check-complete.log` and
`/tmp/sparrow-turnover-commit.log`. An earlier gate exposed a timing assumption
in the preceding connection test; the test no longer opens a peer immediately
after an unrelated CLI process exits. Its finite disconnect-recovery check
remains. Earlier qualification protected only the first cohort in its dense
mode and is not the dense workload reported here.

After the hook, the selected example was explicitly rebuilt
(`/tmp/sparrow-turnover-selected-build.log`) and exclusively installed as
`~/.local/libexec/sparrow/turnover-schema43-c5e776c`, SHA-256
`bbf6a1237373c84ad7d7c04bcf1f64a00e446cbd2502e6e37e3f638305b280c9`.
This offline tool needs no file capability. The live daemon remains source
`608327f`, PID 2551939; no lifecycle action or production store write was needed.

Evidence is `~/.local/state/sparrow/turnover-qualified-gjuky3s7`: frozen collector,
source/image identity, host, three configurations, native daily reports,
preserved generated inputs, SHA-256 input manifest and closed-store measurement.
Each mode accepts 4,046,020 raw bytes and 3,000 independent records. Repeated
JSONL and log artifacts retain one content identity per format across 60
independent artifact occurrences; each mode has 240 distinct artifact IDs and
60 distinct random opaque content identities. Each mode's 3,932,160 opaque input
bytes compress to 3,933,000 bytes under a separate Zstd level-3 probe, ratio
1.000214. This characterizes those inputs, not the native store's compression
ratio or compaction result.

| Pin mode | Records at day 59 | Sampled peak RSS KiB | Maximum sampled daily file bytes | Closed final file bytes | Elapsed seconds |
| --- | --- | --- | --- | --- | --- |
| None | 1,500 | 131,224 | 42,420,698 | 4,663,261 | 6.309 |
| Sparse record | 1,501 | 127,412 | 42,432,008 | 4,673,402 | 6.956 |
| Dense all artifacts | 3,000 | 138,824 | 29,794,572 | 3,089,467 | 7.307 |

The independent collector verifies every daily count and compares all native
first-cohort pinned RDF inspection fields except the changing membership clock:
one protected record in sparse mode and 50 records in dense mode, including the
explicit malformed JSONL projection failure. None/sparse reach the rolling
30-cohort window; dense keeps every cohort through its active artifact pin.
Source byte checks, exact boundary eligibility, parent loss with sparse-record
survival, four reopen points, release and expired-target refusal all pass.
Final records, pins, payload references, JSON projection references and read
leases are zero; a final reopen does not resurrect them. Rerunning against each
existing store refuses before opening it and emits no successful stdout.

RSS is sampled every 100 ms, not an allocation or true peak bound. Daily file
sizes are status samples, not the maximum native transient allocation. Native
automatic compaction and the different retirement/control write patterns affect
file growth; these three numbers are not comparative compression results.
All final files are SST/control files with no WAL; final allocated bytes are
4,702,208, 4,710,400 and 3,121,152 respectively. Zero logical ownership still
leaves measurable physical bytes. Forced bounded reclamation, quota-pressure
admission and sustained physical recovery remain unqualified.

## Streaming and larger-profile qualification

Signed source `7461947` passed `make check` and its normal pre-commit gate:
158 storage scenarios, 60 RPC integration scenarios, remaining workspace tests,
release Clippy and 247 helper ownership rules with zero distinct exceptions.
After the hook, the selected example was explicitly rebuilt and frozen as
`~/.local/libexec/sparrow/turnover-schema48-7461947`, SHA-256
`2db6f2bc1b14e8785cf8731cdf18ca6b0d959c0df2c817468bc1270a55a04dbd`.
The build used Rust `1.100.0-nightly (4b6d04e70 2026-09-13)`. This offline image
requires no capability and is separate from the installed daemon.

The streaming default-size run is
`~/.local/state/sparrow/turnover-v2-qualified-0jpuil7h`. All three modes passed,
each with 3,000 independently specified records and 4,046,020 accepted raw bytes.
Sampled peak RSS was 134,784 / 129,836 / 132,344 KiB for none / sparse / dense;
elapsed times were 9.729 / 8.292 / 7.555 seconds. This qualifies report format
`turnover/2`; historical evidence above retains its original format.

The larger run is `~/.local/state/sparrow/turnover-v2-qualified-ixqru6f_`.
Its profile is 128 repeated JSONL records, 128 changing JSONL records plus one
malformed record, 1,024 CRLF log records and one 1 MiB random opaque record per
day: 1,282 records per cohort, 76,920 records and 65,642,380 accepted raw bytes
per mode. Each mode used a new Btrfs store with a 2 GiB disk budget and 128 MiB
maintenance reserve. Production admission and expiry pass limits were unchanged.
The collector declared a 600-second deadline per mode and an 8 GiB host
free-space delta ceiling; neither was reached.

| Pin mode | Records at day 59 | Sampled peak RSS KiB | Maximum daily sampled file bytes | Closed final file bytes | Elapsed seconds | Final expiry passes |
| --- | --- | --- | --- | --- | --- | --- |
| None | 38,460 | 171,868 | 317,536,221 | 44,609,148 | 96.324 | 1,212 |
| Sparse record | 38,461 | 182,340 | 319,677,940 | 50,555,601 | 91.662 | 1,212 |
| Dense all artifacts | 76,920 | 208,664 | 323,791,172 | 72,217,653 | 111.440 | 2,423 |

The independent frozen collector validates all 60 daily counts against the
fixture specification and checks receipt record counts, source lengths and
line counts. It records SHA-256 input manifests alongside native BLAKE3 content
identities. Every mode has 240 distinct artifact occurrences, one repeated JSONL
content identity, one repeated log identity, 60 changing JSONL identities and
60 random opaque identities. Native first-cohort RDF results remain identical
except for `membership_at`: 60 inspections for the sparse record and 76,920
inspections for the dense first cohort, including the explicit malformed JSONL
projection failure. Exact-byte checks span multiple 64 KiB reads for each opaque
artifact. Boundary eligibility, expired-parent refusal, sparse-record survival,
four reopens, pin release and expired-target refusal pass. Final records, pins,
payload and JSON projection ownership, and read leases are zero; final reopening
does not resurrect records. Existing stores refuse before opening and produce
no successful stdout. Six invalid/oversized fixture cases refuse before creating
either the store or input directory; evidence is
`~/.local/state/sparrow/turnover-v2-refusals-alzbu_oq`.

Final expiry needed more than the preceding qualifier's fixed 1,024 passes.
The new workload-derived ceiling permits these finite traversals without
increasing a production pass budget. Final stores identify format 42 after
native manifest-retirement fencing. Remaining physical files include unreferenced
blobs and SST/control files, with zero WAL bytes. Final allocated bytes are
44,658,688 / 50,610,176 / 72,273,920. This is direct evidence of physical
reclamation lag after logical ownership reaches zero.

The last sampled process `write_bytes` values are 4,527,529,984 / 4,536,995,840 /
5,209,219,072 bytes. These include native WAL, indexes, retirement, compaction,
generated source files and evidence reports. They are sampled process I/O,
not an isolated engine write-amplification measurement or a bound on steady-state
cost. RSS/HWM is sampled every 100 ms and file sizes once per completed day;
neither proves a true peak or worst-case bound. The clock-injected synthetic
profile does not qualify 10 GiB/day, representative corpus density, concurrent
query/intake bursts, quota pressure, power loss or forced physical reclamation.

Both evidence directories preserve collectors, configurations, source/gate/
selected-build identity, hardware/filesystem evidence, reports and generated
inputs. The live daemon remained source `2f1f8ff`, PID 2129972, during both runs;
a read-only status check reported the same store identity, 212 records and zero
pins. No service lifecycle action or public ingestion was needed.

## Current-source provenance and tracing qualification

On 2026-10-18, the same larger profile and unchanged frozen collector were run
against signed source `72804f3b251328ee103b368c0df2c727fdd0d23c`, selected PurRDF
`7734b013d19b68600ad24c4e8aa5a40955a8e89e` and the current RDF/provenance/tracing
owners. The source's required gate passes 28 core unit, 189 storage and 67 RPC
tests, the other workspace tests, release Clippy, census self-tests and 291
ownership jobs with zero distinct exceptions. This refresh is separate from
the older source/image results above.

The selected offline image is `~/.local/libexec/sparrow/turnover-72804f3`, SHA-256
`cae41183913c55597155e1d27a6a145adaa1ee6f5588720e615b929d9e318ecf`.
Its explicit example build reports core feature `test-support`; this is an
offline core qualification, not a default-daemon image qualification. The
compiler observation is Rust `1.101.0-nightly (21b707e3f 2026-09-30)`.

Evidence is `~/.local/state/sparrow/turnover-v2-qualified-4_3d113n`, with the
frozen collector, compiler/CPU/filesystem observations, source/gate and image
identities, configurations, native reports, saved inputs and digest manifests.
The host reports an AMD RYZEN AI MAX+ 395, 16 cores and 32 logical CPUs. The
filesystem is Btrfs with `compress=zstd:1`. Each new store uses a 2 GiB
application disk budget and 128 MiB maintenance reserve. The original collector's
600-second per-mode deadline and 8 GiB host free-space-delta guard were preserved;
neither was reached. The guard is not a filesystem quota. No hard quota or
production resource ceiling was configured by this run.

Each mode accepts 76,920 records and 65,642,380 raw bytes in 240 independent
artifact occurrences. All sixty daily counts, exact artifact/record bytes,
repeated/changing content identities, 30-day boundaries, sparse parent expiry,
pin release, expired-target refusal and four reopen points pass. Sparse mode
preserves 60 native inspections of its protected record; dense mode preserves
76,920 inspections of its first cohort, including the deliberately failed JSONL
projection. Their native inspection fields stay identical except for membership
time. Final records, active pins, payload/JSON-projection references and all five
read-handle/byte families are zero, with no resurrection after the final reopen.

| Pin mode | Records at day 59 | Sampled peak RSS KiB | Maximum daily file bytes | Closed final file bytes | Wall seconds | Final expiry passes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| None | 38,460 | 168,896 | 393,164,375 | 52,571,341 | 464.108 | 1,212 |
| Sparse record | 38,461 | 159,968 | 352,899,859 | 40,132,578 | 472.418 | 1,212 |
| Dense all artifacts | 76,920 | 159,808 | 379,697,957 | 11,462,260 | 529.498 | 2,423 |

All final stores identify format 86: the current tracing/manifest-retirement
authority family for this workload without watches, receivers or log-clock
definitions. This does not qualify every writer-authority family by itself.
Final allocated bytes are 52,633,600 / 40,181,760 / 11,497,472. None and sparse
each retain 37,811,892 blob-file bytes; dense has no blob files at its final
sample. Every mode has 240 WAL bytes along with SST and control/log files.
Zero logical ownership therefore still leaves measured physical storage.

A separate temporary callsite was compiled against the same locked native core
and archived outside the build tree before the repository gate. It opens only
SHA-256 verified closed copies, using the production captured RDF view and
governed SPARQL API at day 90. All three copies report zero native dictionary
terms and zero selected records/artifacts, and complete `ASK { ?s ?p ?o }` with
false. Source admission retains its defaults; native governors specify 100,000
fuel, 100 answers, 10,000 intermediate cells, 1 MiB scratch and zero remote
requests. The original closed stores were not opened by RocksDB for these
follow-up reads; all 27 / 23 / 16 files remain hash-identical.
`graph-closure-qualified.json` and the archived callsite/image identity bind
these read checks; they certify logical graph closure, not physical reclamation.

RSS/HWM is sampled every 100 ms, and physical sizes once per completed day.
Neither is a true peak/allocation bound. Sequential modes, shared-host activity
(including the graph-probe build), native compaction and different formats make
the elapsed/file-size observations unsuitable for causal comparisons with older
runs. Last sampled process write bytes are 5,311,143,936 / 5,295,796,224 /
5,943,083,008, including inputs, reports, WAL, indexes and maintenance rather than
isolated engine amplification. No production throughput/density, quota pressure,
power-loss or forced bounded reclamation acceptance is claimed. Native text/SPARQL,
governed explanation and materialization integration gaps remain open.

The public service remains PID 4001300 with its original start and store identity,
schema 60, format 92, 217 records and healthy watches over TCP and Unix. No service
lifecycle, public ingestion or producer-file operation was performed. Live
status and immutable source/image/collector checks are retained with the reports.
