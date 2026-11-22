<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Performance qualification

Measure workload shape, durability, admission and resource use together.
Each measurement reports the input sizes and formats, record counts,
query coverage, paging and typed refusals. Record the compiler, build
profile, filesystem, configuration, cache state and resource limits so a
run can be repeated.

## Concurrent intake and queries

The [synthetic load example](load-qualification.md) runs durable mixed-format
intake alongside complete typed query captures. It verifies exact bytes,
frozen answer sets and release of query leases. Intake timing ends at the
durable reply. Whole-run time includes input generation, exact-byte readback
and final queries. Use a scratch store and a new input directory for each
run.

Record successful requests and refusals separately. Measure a full
traversal across all pages and release its lease. A small request page can
still require a large immutable capture. Track storage operation admission,
transport connection limits, capture limits, and disk pressure separately.
See [query streaming](query-streaming.md).

## Simulated turnover qualification

The [offline turnover example](turnover-qualification.md) exercises sixty
simulated daily cohorts with three pin densities. It verifies exact bytes,
RDF inspection, expiry and clean reopen. Record closed file sizes and
sampled process memory alongside logical ownership. Native physical
reclamation follows a separate [pressure workload](native-reclamation.md).

## Remaining acceptance

The synthetic runs establish correctness and measured costs within their
declared limits. Production throughput, steady-state storage density,
recovery under a hard quota and worst-case resource use require separate
qualification.
