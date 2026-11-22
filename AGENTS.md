<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Working on Sparrow

Read [the architecture](docs/architecture.md) and [the interfaces](docs/interfaces.md)
before changing behavior. Read applicable design decisions and local instructions
before changing deployment or repository workflows.

## Scope and ownership

Sparrow stores disposable logs and artifacts on one Linux server. It optimizes
for throughput and storage efficiency. Keep independent copies of irreplaceable
data. Reads preserve the exact bytes of accepted evidence. Retention is based on
receipt time. Pins extend retention, and deletion requires an already verified
archive copy.

Sparrow owns storage, intake, retention and pins. PurRDF owns RDF semantics,
codecs, projections, SPARQL and text search. Use its public facade or native
helpers before introducing another implementation. Request an upstream seam
when no public interface supplies a needed operation.

Use a branch and keep unrelated work intact. Preserve both sides of a merge
conflict by reading their behavior and callers. Keep live daemon, store and
service operations under the operator's explicit control.

## Code

- Keep storage in `sparrow-core` without wire or server dependencies. Keep
  binaries thin. Declare shared dependencies once in the workspace.
- Parse input at the boundary into typed values. Keep visibility narrow.
  Preserve typed errors, their causes and their retry semantics.
- Wire production callers through a single implementation. Share dev-only
  fixtures through the test support crate. Register shared helpers and their
  owners in `helpers-ledger.ttl`. Resolve census findings at the source.
- Add Cap'n Proto fields and methods after the last existing ordinal.
  Preserve all existing ordinals. Agree on any stored-format change before
  implementation and document compatibility, migration and rollback.
- Name and count losses or uncertain acceptance. Use structured tracing at
  meaningful boundaries. Keep credentials and large buffers out of logs.
- Keep tests focused on real behavior and failure boundaries. Use explicit time
  inputs and synchronization when exercising deadlines or concurrent paths.

## Verification

Install the additive pre-commit hook with `make install-hooks` using Git 2.54
or newer. Always commit with the configured verification enabled. Fix a failure
and rerun the normal hook. A change that cannot pass stays uncommitted.

Start with focused checks on affected code and consumers. Run `make check` on
the exact head before merge. It runs formatting, release Clippy with warnings
denied, separate shipped-crate checks, release tests and the pinned helper
census. Report the command, tested scope and its own exit status. A result
obtained through a pipe needs the gate's exit status.

Use the PurRDF revision recorded in `registry/compatibility.toml` for the
lexicons and cross-workspace census. Keep pinned-source checks separate from
Cargo's runtime resolution. Runtime crates have no dependency on the helper
census.

## Documentation

Describe the current technical contract in present tense. Update the governing
document when behavior changes. Keep release history in the changelog and
qualification evidence in the appropriate record. Use generic examples and
omit private deployment addresses, credentials and operator state.

See [Contributing](CONTRIBUTING.md) for the build and contribution workflow.
