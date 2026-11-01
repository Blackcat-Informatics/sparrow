<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Sparrow working rules

Read [the architecture](docs/architecture.md) and [the interfaces](docs/interfaces.md)
before implementation. Read applicable `.baseline`, `.goals`, and governing ADRs
when present. Preserve unrelated dirty work and sibling worktrees. Keep `main`
protected; implement on an isolated branch/worktree unless Patrick explicitly
authorizes a direct-main exception.

## Reuse and helper ownership

- Use PurRDF functions wherever they supply the required behavior. Inspect its
  public facade and native helper crates before writing a new helper, including
  in tests, benches, and examples. Sparrow owns its storage, retention, pins,
  and ingestion coordination; PurRDF owns RDF semantics, codecs, JSON/Markdown
  projection, SPARQL, text analysis, scoring, and query governors.
- Do not write a second parser, evaluator, projector, tokenizer, scoring law,
  or utility already supplied by PurRDF. A missing public integration seam is an
  upstream task, not permission to copy private implementation code.
- Give each local helper one implementation and route callers through it.
  Shared test scaffolding belongs in one dev-only home rather than copied files
  or cross-package `#[path]` modules.
- `helpers-ledger.toml` names helper owners and enforceable reuse rules. Add
  ownership rows as helpers are introduced or adopted. `make census` reports
  local shapes; `make census-cross` discovers overlap with the recorded PurRDF
  revision; `make census-check` enforces local uniqueness and ledger rules.
- Resolve duplicate findings by sharing the implementation or adopting PurRDF.
  A necessary independent implementation needs a narrowly scoped ledger variant
  or exact-member `[[distinct]]` row with a documented reason. Stale exceptions
  must be removed. Do not relax detectors or add blanket exceptions to pass.
- The census crate is repository tooling only. Runtime crates must not depend
  on it. Its authorized port from Katamari retains source provenance and licensing;
  it is not a second implementation of Sparrow's runtime semantics.
- The cross report is discovery evidence, not proof of semantic equivalence.
  Cross matches outside named ledger jobs require review; they are not all
  automatically gate failures. Inspect contracts and callers before adoption.
- Sibling repositories are read-only references. The census archives the exact
  revision in `registry/compatibility.toml`, never the live PurRDF working tree.
  Record the reason for reference updates and rerun the gate. Runtime dependency
  selection and locking remain separate from this source census reference.

## Evidence and verification

- Read implementations and verify relevant callers before making claims.
  Distinguish observations, inferences, and checks not run.
- Fix failures a change introduces, touches, or depends on. Report unrelated
  pre-existing failures without expanding scope.
- Missing required dependencies and broken invariants must fail with actionable
  errors. Do not use silent fallbacks, swallowed exceptions, stubs, or partial
  success to disguise failure.
- Wire production callers through one intended implementation. Remove obsolete
  alternatives within scope; avoid speculative compatibility layers.
- Prefer the smallest coherent design. Add abstractions and dependencies only
  when the concrete problem warrants them.
- Use focused checks during development and run `make check` before completion.
  It checks formatting, release-profile Clippy with warnings denied, release
  tests, seeded census self-tests, and the pinned cross-workspace gate.
  `make all` is the same gate. Repeat checks when relevant changes or unresolved
  failures justify it; there is no fixed review count.
- Install the additive pre-commit gate with `make install-hooks` in each checkout
  (Git 2.54 or newer). Preserve existing hooks.
- Never bypass commit verification: `git commit --no-verify`, `-n`, `SKIP=`,
  `HUSKY=0`, and every equivalent escape hatch are forbidden without exception.
  Wait for slow hooks; fix failures. If a change cannot pass, do not commit it.
- When evidence contradicts a theory, inspect the computation and use temporary
  probes as needed. Remove diagnostic debris before committing.
- Continue through authorized, actionable next steps. Report real blockers and
  unfinished acceptance criteria precisely.

## Repository integration and durable context

- Resolve merge conflicts by evaluating both sides; do not discard one wholesale.
- Use the Stage skills and `stagectl` for staged issue/PR work, with review effort
  proportional to the change. Every GitHub PR or branch merge uses
  `/home/paudley/stage/root/bin/ghprsq`; resolve unmet prerequisites or report the
  blocker. Ordinary base-to-worktree `git merge` synchronization is allowed.
- Do not add tool branding or agent credits to source, commits, issues, or PRs.
- Use relevant existing memories and verify facts that may have drifted.
  Automatic memory generation and Hindsight are disabled. Do not mutate private
  memory stores. Explicitly requested manual updates may create a note under
  `~/.codex/memories/extensions/ad_hoc/notes/`; never edit generated memory files.
- Record durable project knowledge in repository documentation within authorized
  scope or report it in chat. Do not create another private memory store.

<!-- BEGIN LILLITH BRAINSTEM MANAGED POLICY -->
## Lillith model resource authority

- In `lillith_decodes`, `lillith_draws`, and `lillith_brain`, never load a
  production model or start a model server directly. Use the typed
  `lillith_brainstem` client.
- Never start, stop, restart, supervise, or reconfigure Brainstem unless the
  user explicitly requests that lifecycle action.
- Do not run a GPU, NPU, or model-backed test without an admitted Brainstem
  research profile and exact artifact identity.
- Do not use HTTP, gRPC, TCP, UDP, or a loopback port to communicate with a
  local model backend.
- Preserve sibling experiments and running processes. Diagnose resource state
  read-only before proposing recovery.

<!-- END LILLITH BRAINSTEM MANAGED POLICY -->
