<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Helper census

Sparrow adopts PurRDF functionality wherever its public API supplies the required
behavior and keeps each Sparrow-owned helper in one home. The census is the
repository gate for structural duplicates and the ownership rules recorded in
`helpers-ledger.toml`. It is development tooling; no runtime crate may depend on it.

## Running the gate

Use Rust 1.98 or newer with rustfmt and Clippy, plus Git and tar. The sibling
`../purrdf` repository must contain the full commit named in
`registry/compatibility.toml`. No PurRDF code is compiled by the census.

```sh
make check          # formatting, release Clippy/tests, self-tests, cross gate
make install-hooks  # additive pre-commit gate, preserving existing hooks
make census         # target/helper-census/clone-baseline.json
make census-cross   # cross-baseline.json and cross-report.txt in that directory

cargo run --locked --release -p sparrow-helper-census -- --cross --count hex
cargo run --locked --release -p sparrow-helper-census -- --cross --census hex
cargo run --locked --release -p sparrow-helper-census -- --cross --index
cargo run --locked --release -p sparrow-helper-census -- --dump-ledger
```

Hook installation uses Git 2.54's additive `hook.sparrow-verify` configuration in
the checkout's local Git config. It runs `make check` on commits; it does not
replace Stage hooks or change global configuration. Install it in each checkout.

To use another PurRDF checkout, pass `--purrdf-repo /path/to/purrdf` with a cross
mode or `--cross --check`. The tool archives `crates/` and `bindings/` at the
recorded commit into a directory private to the run,
`target/helper-census/purrdf-<revision>.<pid>/`, and removes it once the walk
has read it. It never modifies or scans the sibling's dirty working tree.
Reports identify their reference. Nothing is reused between runs: build
directories are reaped automatically, and a reaped tree that kept its
directories would scan as less of PurRDF than the pin names. Extraction adds
about 1.4 seconds to a run.

`--external-root DIR` is for prepared trees and fixtures. It bypasses the pin and
reports **UNPINNED**. It is not the normal repository gate. A missing reference,
invalid pin, failed archive, unreadable source module, or empty package tree
refuses the operation. A plain `--check` prints **NOT CHECKED** for PurRDF-owned
jobs without a cross scan; `make check` always supplies `--cross`.

## Coverage and ownership

The tool follows Rust modules in packages under `crates/`, including library,
binary, build-script, test, bench, and example targets. Test functions and
test-only modules are excluded from body grouping, while shared support helpers
in test targets are scanned. Generated code and macro-generated bodies are not
expanded. Keep first-party packages under `crates/` to retain this coverage.

It normalizes local names, value adapters, and borrow spellings to group
structurally equivalent bodies and thin forwarders. It also reports shared
constants and hex tables. This is structural evidence, not a semantic equivalence
proof: different algorithms can evade it and unrelated functions can share a
shape. Review contracts and callers before centralizing code.

The gate refuses unsanctioned local body/shim groups, cross-package `#[path]`
source imports, broken ledger homes/entry points/vector/bench references, stale
exceptions/fingerprints, and forbidden copies of enforced helper jobs. The seed
ledger homes hex encoding/decoding, SplitMix64, and the MMIX LCG in PurRDF.
It starts with no Sparrow-specific clone exemptions.

The cross report discovers bodies, shims, and large constants shared with PurRDF.
Only overlap covered by enforced ledger jobs fails the cross gate automatically;
other matches need review and, where appropriate, another ownership row. Overlap
from this deliberately ported census tool is expected and visible in reports.
Duplication solely inside PurRDF is outside Sparrow's gate.

The ledger documents its strict TOML subset and each field. Before adding a helper,
look for an existing PurRDF or Sparrow home. Prefer adoption or shared code. A
necessary variant needs a documented specification, benefit, measured speedup,
or layering reason with a resolvable anchor. Exact-member `[[distinct]]` rows
reopen if a new member appears and fail when stale. Do not import Katamari's
runtime-specific exemptions into Sparrow.

## Provenance and dependency identities

The tool was ported from Katamari commit
`f5290a439388e5b06bd48fd8464551df842dd5c5`, with no local changes in the copied
crate. Its original PurRDF port revision is recorded in the crate's module docs.
Source copyright and AGPL-3.0-only notices are preserved. Sparrow-specific changes
rename the package/report fields, select the `purrdf` registry supplier, and
refuse an empty local census. CLI regression fixtures exercise the port's actual
gate, including local clones and PurRDF-owned helper copies.

The reference starts at PurRDF commit
`e58974e504b9681597faea1490002ce8ddaf7448`, whose workspace declares version 3.0.1
and Rust 1.98. This is a source comparison identity, not a runtime dependency lock
or a replacement for the design document's earlier baseline. `Cargo.lock` locks
the tool's compiled dependencies. Record reference changes and rerun the gate;
select and record runtime dependency revisions when runtime work begins.

This port establishes an implementation guardrail. The daemon, storage engine,
and other product interfaces remain unimplemented.
