# Offline turnover qualification

The `turnover` example qualifies retention through production core operations
using an injected UTC clock. It accepts a configuration and one pin mode:

```bash
cargo build --locked --release -p sparrow-server --example turnover
target/release/examples/turnover /absolute/path/config.toml none > report.json
```

The configuration must select a nonexistent data directory with no consume,
syslog or TCP binding. The tool opens no transport, worker or producer source.
Existing stores are refused before opening. Run each mode against its own new
store. Configuration errors, storage failures and failed assertions terminate
with nonzero status; partial output is not a successful report.

Sixty simulated daily cohorts each contain eight repeated JSONL records, eight
changing JSONL records plus one deliberately malformed record, 32 repeated CRLF
log records and one 64 KiB OS-random opaque record. Native JSON serialization,
ingestion, projection, hashing, exact bytes, pin protection and dependency
retirement retain their existing owners. Generated inputs are saved beside the
store in `store.inputs`; each accepted artifact identity, native content hash and
byte length is recorded. No tokenizer, projector or retention implementation is
copied into the qualifier.

`none` protects no evidence. `sparse` protects one record from the first JSONL
artifact. `dense` adds all four artifacts from every daily cohort to a named
pin. Each pin mutation requests 90 days and uses its own stable request identity.
The independent count oracle expects 50 records per cohort, a 30-day ordinary
window, and the concrete selected protection. Every daily traversal uses finite
production expiry passes, with a hard 1,024-pass qualification ceiling.

Daily verification checks exact bytes for every retained whole artifact,
unavailability of expired parents, sparse record bytes after its parent expires,
and native RDF inspection of the first protected cohort. The report preserves
those native inspection results for comparison across days. Stores reopen at days
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

## Qualified run

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
