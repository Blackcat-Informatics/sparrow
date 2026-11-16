#!/bin/bash
set -e
perl -0pi -e 's/    println!\(\n        "PROBE ingest total_s/    let kept = latencies.len() - latencies.len() \/ 100;\n    println!(\n        "PROBE ingest trimmed_mean_us {:.1} (fastest 99 percent)",\n        latencies[..kept].iter().map(Duration::as_micros).sum::<u128>() as f64 \/ kept as f64\n    );\n    println!(\n        "PROBE ingest total_s/' probe_payload_index.rs
