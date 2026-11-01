<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->
# Contributing to Sparrow

Thanks for your interest in Sparrow. This repository holds the Rust workspace
for the diagnostic evidence service: the storage core, the server with its
Cap'n Proto RPC, CLI and stdio MCP adapter, the producer client, and the
helper census that guards against duplicated code. Issues and pull requests
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
- **Record helpers in the ledger.** `helpers-ledger.toml` names each helper's
  owner and its reuse rules. Add an ownership row when you introduce or adopt
  a helper. Resolve a duplicate by sharing the code or adopting PurRDF. A
  necessary independent copy needs a narrowly scoped variant or exact-member
  `[[distinct]]` row with a documented reason. Do not relax detectors or add
  blanket exceptions to pass the gate. See
  [the helper census](docs/helper-census.md).
- **Fail loudly.** Missing dependencies and broken invariants must produce
  actionable errors. Do not hide them behind silent fallbacks, swallowed
  errors, stubs or partial success.
- **Keep runtime crates independent of tooling.** No runtime crate may depend
  on `sparrow-helper-census` or `sparrow-test-support`.

## Development

Prerequisites:

- Rust 1.98 or newer (`rust-version` in `Cargo.toml`) with rustfmt and Clippy
- `capnp` (the Cap'n Proto compiler) and libclang, for the RPC schema and the
  RocksDB bindings
- Git 2.54 or newer, and `tar`
- A sibling PurRDF checkout at `../purrdf` that contains the revision pinned
  in [`registry/compatibility.toml`](registry/compatibility.toml). The tests
  read its text lexicons and the census archives its source at that revision.

```bash
git clone https://github.com/Blackcat-Informatics/purrdf ../purrdf
git -C ../purrdf checkout "$(sed -n 's/^git_revision = "\(.*\)"/\1/p' registry/compatibility.toml)"
```

The main commands:

```bash
make check          # the gate: tools, fmt, release Clippy, release tests, census
make install-hooks  # run make check as an additive pre-commit hook
make fmt            # apply rustfmt
make census         # local duplicate baseline
make census-cross   # overlap report against the pinned PurRDF revision
make build-runtime  # release build of the server and client binaries
```

`make check` is the whole gate. It checks formatting, runs release-profile
Clippy over every target with warnings denied, runs the release test suite,
runs the census self-tests, and runs the pinned cross-workspace census check.
`make all` is the same gate. Release builds of RocksDB are slow the first
time; later runs reuse the target directory.

### Commit hooks

Run `make install-hooks` once in each checkout. It uses Git 2.54's additive
`hook.<name>` configuration to run `make check` before every commit, and it
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
- Keep `helpers-ledger.toml` current for any helper you added or adopted.
- Write commit subjects as a plain imperative sentence describing the
  behaviour, for example `Bound the service's CPU and disk use`.

## License

Sparrow is licensed under **MIT OR Apache-2.0 OR MulanPSL-2.0**, at your
option. Unless you explicitly state otherwise, any contribution you submit
for inclusion is licensed under the same three-way choice, without any
additional terms or conditions. New source files carry the SPDX header:

```text
SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
```

See [LICENSING.md](./LICENSING.md) for the full terms, including the
contributor relicensing grant.

## Conduct and security

Participation is governed by the [Code of Conduct](./CODE_OF_CONDUCT.md).
Report vulnerabilities privately as described in [SECURITY.md](./SECURITY.md),
not in a public issue.
