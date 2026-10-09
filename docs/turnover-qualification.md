# Offline turnover qualification

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
Automatic native compaction is allowed, but forced bounded reclamation remains
subject to the [native binding gap](native-reclamation.md). This does not qualify
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
