<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->
# Contributing to Sparrow

Thanks for your interest in Sparrow. This repository holds the Rust workspace
for the diagnostic evidence service: the storage core, the server with its
Cap'n Proto RPC, CLI and stdio MCP adapter, and the producer client. The
RAF helper census guards against duplicated code. Issues and pull requests
are welcome.

Read [the architecture](docs/architecture.md) and
[the interfaces](docs/interfaces.md) before changing behaviour.

## Ways to contribute

- **Report a bug or request a feature.** Open an issue with a minimal
  reproduction: the input artifact, the command or RPC call, and what Sparrow
  reported.
- **Fix a bug or add a feature.** Open a pull request against `main` from a
  topic branch.
- **Improve the docs.** Corrections to the README and [`docs/`](./docs/) are
  very welcome.

## Design rules that pull requests must respect

- **Reuse PurRDF.** PurRDF owns RDF semantics, codecs, JSON and Markdown
  projection, SPARQL, text analysis, scoring and query governors. Sparrow owns
  storage, retention, pins and ingestion coordination. Do not write a second
  parser, evaluator, projector, tokenizer, scoring law or utility that PurRDF
  already supplies. A missing public PurRDF seam is an upstream task, not
  permission to copy private implementation code.
- **One home per helper.** Give each local helper one implementation and route
  callers through it. Shared test scaffolding belongs in the dev-only
  `sparrow-test-support` crate, not in copied files or cross-package `#[path]`
  modules.
- **Record helpers in the ledger.** `helpers-ledger.ttl` names each helper's
  owner and its reuse rules. Add an ownership job when you introduce or adopt
  a helper. Resolve a duplicate by sharing the code or adopting PurRDF. A
  necessary independent copy needs a narrowly scoped variant or exact-member
  `Distinct` entry with a documented reason. Do not relax detectors or add
  blanket exceptions to pass the gate.
- **Fail loudly.** Missing dependencies and broken invariants must produce
  actionable errors. Do not hide them behind silent fallbacks, swallowed
  errors, stubs or partial success.
- **Keep runtime crates independent of tooling.** No runtime crate may depend
  on `sparrow-test-support`. Install the census separately with
  `make install-census`.

## Development

Prerequisites:

- Rust 1.98 or newer with rustfmt and Clippy, as declared by the workspace
  `rust-version`.
- `capnp` (the Cap'n Proto compiler) and libclang, for the RPC schema and the
  RocksDB bindings
- Git 2.54 or newer, Python 3.11 or newer, `b3sum` and `tar`
- A sibling PurRDF checkout at `../purrdf` that contains the revision pinned
  in [`registry/compatibility.toml`](registry/compatibility.toml). The tests
  read its text lexicons, which `tools/purrdf-lexicons.sh` extracts with
  `git archive` at that revision. The helper census reads the committed objects
  at that revision from the sibling checkout.

```bash
git clone https://github.com/Blackcat-Informatics/purrdf ../purrdf
git -C ../purrdf checkout "$(sed -n 's/^git_revision = "\(.*\)"/\1/p' registry/compatibility.toml)"
```

The main commands:

```bash
make install-census # install the pinned cargo-raf-census
make check          # the gate: tools, fmt, release Clippy, release tests, census
make install-hooks  # run make precommit as an additive pre-commit hook
make fmt            # apply rustfmt
make census         # local duplicate baseline
make census-cross   # report policy findings against the pinned PurRDF source
make build-runtime  # release build of the server and client binaries
```

`make check` is the whole gate. It checks formatting, runs release-profile
Clippy over every target and each shipped crate with warnings denied, runs
the release test suite, and runs the census self-tests and pinned cross-workspace
check. `make all` runs the same gate. Release builds of RocksDB are slow the
first time. Later runs reuse the target directory.

GitHub CI runs the compile and census checks alongside the release tests on
separate runners. The `make check` status passes only when both jobs succeed.

The helper ledger uses canonical RDF 1.2 Turtle. Use RAF's RDF builder to
edit the graph and render it as Turtle. Verify the result with
`cargo-raf-census dump-ledger --format turtle`.
[RAF ledger reference](https://github.com/Blackcat-Informatics/rust-agentic-foundations/blob/v0.8.0/docs/capabilities/census-ledger.md)

### Commit hooks

Run `make install-hooks` once in each checkout. It uses Git 2.54's additive
`hook.<name>` configuration to run `make precommit` (formatting and release Clippy) before every commit, and it
preserves any hooks you already have.

**Never bypass commit verification.** Do not use the commit command's
verification-skipping options, `-n`, `SKIP=`, `HUSKY=0` or any equivalent
escape hatch. If the hook is slow, wait for it. If it fails, fix the code. If
a change cannot pass, do not commit it; say so in the pull request instead.

## Before you open a pull request

- Run `make check` and make sure it is green.
- Add or extend tests only where they protect a named behaviour, regression,
  boundary or invariant. Prefer extending an existing behavioural case.
- Update the docs when the change is user-visible, including
  [the interfaces](docs/interfaces.md) for any RPC, MCP or CLI change.
- Keep `helpers-ledger.ttl` current for any helper you added or adopted.
- Write commit subjects as a plain imperative sentence describing the
  behaviour, for example `Bound the service's CPU and disk use`.

## License

Sparrow is licensed under **MIT OR Apache-2.0 OR MulanPSL-2.0**, at your
option. Unless you explicitly state otherwise, any contribution you submit
for inclusion is licensed under the same three-way choice, without any
additional terms or conditions. New source files carry the SPDX header:

```text
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
```

See [LICENSING.md](./LICENSING.md) for the full terms, including the
contributor relicensing grant.

## Dependency checks

Run `cargo deny --locked check` against the workspace before preparing a
release. The policy checks the locked dependencies for advisories, license
compatibility, wildcard dependencies and unapproved sources.

## Conduct and security

Participation is governed by the [Code of Conduct](./CODE_OF_CONDUCT.md).
Report vulnerabilities privately as described in [SECURITY.md](./SECURITY.md),
not in a public issue.
