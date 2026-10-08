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
