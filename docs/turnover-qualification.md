<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Offline turnover qualification

## Workload and method

The `turnover` example runs production intake, pinning, retirement, reopen
and read paths using an injected UTC clock. Build it in release mode and
supply a configuration with a nonexistent data directory and no watches,
syslog or TCP listener. Use a new store for each pin mode.

```bash
cargo build --locked --release -p sparrow-server --example turnover
target/release/examples/turnover /absolute/path/config.toml none > report.json
target/release/examples/turnover /another/config.toml dense 128 1024 1048576 > report.json
```

The optional sizes select the number of repeated and changing JSON rows, the
number of log rows and the number of opaque bytes. Each of sixty daily
cohorts contains repeated and changing JSONL, a deliberately malformed
JSONL record, CRLF logs and random opaque bytes. The default produces
3,000 records. The larger profile produces 76,920 records and accepts
65,642,380 raw bytes per store.

The `none` mode protects no records. The `sparse` mode pins a record from
the first JSONL artifact. The `dense` mode protects every artifact. Daily
checks compare membership and exact bytes against the fixture specification.
Checks also cover expiry boundaries, an expired parent with a surviving
pinned record, native RDF on the first protected cohort, and reopen without
resurrection. At day 60 the run releases the pins and at day 90 verifies
logical cleanup after reopening.

Report format `turnover/2` streams daily results and RDF inspections under
`store.inputs`. Final stdout carries `complete: true` only after all checks
pass. Record physical file sizes, process memory and elapsed time separately
from logical bytes and membership.

## Schema 62 retention and native compaction policy

The larger synthetic profile used a 2 GiB store budget and 128 MiB
maintenance reserve. All three modes completed the daily checks, final
retirement and reopen. They finished with zero records, pins, payload and
projection references, and read leases.

| Pin mode | Records at day 59 | Sampled peak RSS (KiB) | Maximum daily file bytes | Closed file bytes | Final expiry passes |
| --- | ---: | ---: | ---: | ---: | ---: |
| None | 38,460 | 167,696 | 358,536,414 | 13,525,535 | 1,212 |
| Sparse | 38,461 | 188,464 | 350,790,925 | 27,417,640 | 1,212 |
| Dense | 76,920 | 204,668 | 367,297,759 | 40,847,889 | 2,423 |

Closed day-90 copies returned zero selected records, artifacts and dictionary
terms, and `ASK { ?s ?p ?o }` returned false under bounded native SPARQL
admission. Files can remain after logical ownership reaches zero. The
[pressure qualification](native-reclamation.md) separately tests physical
reclamation and write retry admission.

## Historical default-size qualified run

All three modes passed with 3,000 records and 4,046,020 raw bytes per run.
The qualification checks daily membership, exact bytes, pin release and
reopen. These synthetic results describe the declared finite workload.
Sustained ingestion, hard quota recovery and production resource bounds
require separate workloads.
