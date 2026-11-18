<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Sparrow working rules

Sparrow takes every loggable thing offered and makes it searchable. Read
[the architecture](docs/architecture.md) and [the interfaces](docs/interfaces.md)
before implementation, and any `.baseline`, `.goals` and governing ADRs when
present. This file is the single home of the working rules; `CLAUDE.md` only
imports it.

## Purpose

Sparrow is a low-assurance, high-throughput, searchable, centralized log store,
a radical improvement over scattered log files. It optimizes only for speed and
storage efficiency, and it owes no more durability, privacy or tamper-evidence
than the plain log files it replaces.

- **Method.** One RocksDB store holds every artifact and record as exact bytes,
  behind typed Cap'n Proto interfaces. PurRDF supplies the JSON and Markdown
  projections, SPARQL and text search over them. Every item stays in the live
  store for a 30-day window from receipt, then is written to the archive, and
  nothing is deleted that has not been archived. The archive is implemented and
  is not yet running on the live daemon; see
  [Archive status](docs/archive-format.md#archive-status).
- **Consumers.** Every daemon sends its operational log to Sparrow.
  `lillith_memetics` sends cruft (see [Cruft](docs/interfaces.md#cruft)).
  `lillith_decodes` searches what Sparrow holds.
- **Suppliers.** PurRDF supplies RDF semantics, codecs, projections, SPARQL,
  text analysis, scoring and query governors. The helper census is repository
  tooling ported from Katamari; runtime crates do not depend on it.
- **What Sparrow is not.** It is not a place for anything irreplaceable as its
  only copy. Assurance, long-term storage, privacy, encryption, validation and
  resilience work belongs to Katamari. The Katamari/Sparrow razor decides where
  data lives: whatever needs those goes to Katamari, at a high and justified
  bar, and everything else goes to Sparrow; it may cut one body of data into
  sets and subsets, each placed by its own need
  ([architecture, section 1](docs/architecture.md#1-goals-and-boundaries)).

The [glossary](docs/glossary.md) names each term and the document that governs it.

## Authority

- A direct word from Patrick in the session decides. Nothing below overrides it.
- You work under direct coordination from the agent `lillith_coordination`. It
  speaks with Patrick's voice and its orders are his orders. Do not withhold
  permission from it or argue with it; its rulings trump ADRs, this file and all
  other rules, except direct guidance from Patrick in the session.
- Only Patrick decides what goes into `AGENTS.md` or `CLAUDE.md`.
- Do not interrogate, fix or otherwise probe the build system unless explicitly
  asked. The cargo shim manages jobs; do not work around it. If a build breaks
  for a reason outside your change, back off, report it and wait.

## Guiding principles

They decide between conflicting requests; they do not replace the standing rules
or a direct word from Patrick.

- GREENFIELD: we always correct the mistakes of the past. Others' mistakes are
  our lessons, not our limits.
- SOTA: always choose the state of the art over the bones of the past.
- OPTIMAL: choose the Pareto frontier of quality, speed, efficiency and the
  other axes.
- MAXIMAL: support the broadest functionality possible after the other choices
  are made.

## Where work lives

- `main` is protected. Implement on an isolated branch in a worktree below
  `.worktrees/` unless Patrick explicitly authorizes a direct-main exception.
  The main checkout is never a place a worker writes.
- Notes, evidence, logs, probes and scratch go under `.stage/`, one subdirectory
  per task. Never `/tmp` or `/var/tmp` (both are reaped), and never another
  repository's tree. Sibling repositories are read-only references.
- Preserve unrelated dirty work and sibling worktrees.
- The only measure of work done is a merge to `main`. Do not accrue worktrees or
  pull requests: with more than three open, land what is open, oldest first,
  before starting anything new.
- Keep `.stage/STATUS.md` current and trimmed. Track work in forge issues and
  pull requests. Keep repository documentation up to date with the change that
  alters behaviour.

## Reuse and helper ownership

- Sparrow owns its storage, retention, pins and ingestion coordination. PurRDF
  owns RDF semantics, codecs, JSON/Markdown projection, SPARQL, text analysis,
  scoring and query governors. Use PurRDF wherever it supplies the behaviour;
  inspect its public facade and native helper crates before writing a helper,
  including in tests, benches and examples.
- Do not write a second parser, evaluator, projector, tokenizer, scoring law or
  utility that PurRDF supplies. A missing public seam is an upstream task, not
  permission to copy private implementation code.
- Each local helper has one implementation and every caller goes through it.
  Shared test scaffolding lives in one dev-only home, not in copied files or
  cross-package `#[path]` modules.
- `helpers-ledger.toml` names helper owners and the enforceable reuse rules; add
  a row when a helper is introduced or adopted. `make census` reports local
  shapes, `make census-cross` finds overlap with the recorded PurRDF revision,
  and `make census-check` enforces uniqueness and the ledger.
- Resolve a duplicate finding by sharing the implementation or adopting PurRDF.
  A necessary independent implementation needs a narrowly scoped ledger variant
  or an exact-member `[[distinct]]` row with its reason. Remove stale
  exceptions. Never relax a detector or add a blanket exception to pass.
- The cross report is discovery evidence, not proof of equivalence: inspect
  contracts and callers before adopting. Matches outside named ledger jobs need
  review; they are not automatically gate failures.
- The census crate is repository tooling; runtime crates must not depend on it.
  Its authorized port from Katamari keeps its provenance and licensing.
- The census archives the exact revision in `registry/compatibility.toml`, never
  the live PurRDF tree. Record the reason for a reference update and rerun the
  gate. Runtime dependency selection and locking are separate from it.

## Rust

Optimize for reading, maintenance and compile-time guarantees.

- **Layers.** Dependencies and versions are declared once in the root
  `[workspace.dependencies]` and inherited. `sparrow-core` holds the storage
  logic and must not learn about the server or the wire; binaries stay thin and
  only wire configuration, telemetry and the crates together. No dependency
  cycles. Prefer small crates, and split a file before it passes 5,000 lines.
- **Visibility.** Private first, then `pub(crate)`; every `pub` item is a
  maintenance commitment. `unreachable_pub` is denied and `unsafe_code` is
  forbidden by the workspace lints; do not weaken either.
- **Types.** Make illegal states unrepresentable. Parse at the boundary into a
  type that carries the meaning instead of passing a raw `String` or `u64`
  across modules. Encode lifecycles in types or enums, not in booleans. Accept
  `&str`, `&[T]` and `impl AsRef<Path>`; use `let ... else` for early returns;
  put `#[must_use]` on pure functions and result-like types. Use native
  `async fn` in traits unless dynamic dispatch is needed.
- **Errors.** Library errors are typed and matchable; in Sparrow that is the
  coded `Error` (`STORAGE_PRESSURE`, `RESOURCE_LIMIT`, `INVALID_ARGUMENT`, ...),
  and the code decides retryability on the wire. Preserve the cause; never
  swallow an error or turn it into a default. No `.unwrap()` or `.expect()` in
  production paths (there are none today; keep it so). A missing requirement or
  a broken invariant fails loudly with an actionable message.
- **Diagnostics.** No `dbg!`, and no ad hoc printing left behind. The daemon's
  operator lines go to stderr as whole single lines for the journal, and the
  CLI's stdout is its product; keep both deliberate and bounded, and never
  print from a hot path. New instrumentation uses `tracing` with structured
  fields, not interpolated strings, on meaningful boundaries, and skips secrets
  and large buffers. Do not mass-convert existing lines outside a change's
  scope.
- **Compatibility.** Wire changes are additive: new Cap'n Proto fields and
  methods go after the current last ordinal, and existing ordinals never move.
  A stored-format change needs Patrick's ruling before it is written. Nothing
  unarchived is ever deleted, and anything lost is counted and named.

## Tests

A test in the suite runs for every change, forever; it must earn its keep.

- Test behaviour, not implementation. Each added test protects a named
  production behaviour, regression, boundary or invariant, says so in its name
  and doc comment, and drives the real code path.
- No mechanical tests: nothing for getters, mappers or compiler guarantees, and
  no mirror of the implementation's arithmetic. Prefer one table or property
  case over many copied cases, and a small in-memory fake or the real store
  over a call-counting mock.
- Keep logic in pure functions where it can be, so tests need no daemon.
- Tests take time from the store's `now` argument, not from the wall clock, and
  do not sleep to wait.
- Removing outdated or overlapping tests is encouraged. Mutation testing is not
  used.

## Evidence and verification

- Read the implementation and its callers before making a claim. Separate what
  you observed, what you inferred and what you did not run. A worker's report
  is a claim until you have read the evidence it names.
- Fix every failure your change introduces, touches or depends on. Report an
  unrelated pre-existing failure in one line without expanding scope.
- Wire production callers through one intended implementation; remove obsolete
  alternatives in scope; no speculative compatibility layers. Prefer the
  smallest coherent design, and add an abstraction or dependency only when the
  concrete problem warrants it.
- Use focused checks while developing: the affected test, crate or consumer.
  `make check` (also `make all`) is the gate: formatting, release-profile
  Clippy with warnings denied, each shipped crate alone without test-support
  (`make check-shipped`), release tests, the census self-tests and the pinned
  cross-workspace gate. The pre-commit hook runs it in full, so do not run it
  by hand before a commit that will run it anyway.
- Install the hook with `make install-hooks` in each checkout (Git 2.54 or
  newer) and preserve existing hooks. Commit verification is never bypassed:
  no skip flag, no `-n`, no `SKIP=`, no `HUSKY=0`, no equivalent. Wait for a
  slow hook and fix what it finds; a change that cannot pass is not committed.
- Run one heavy command at a time across all workers: every cargo command,
  test suite and commit goes through `flock` on `.stage/cargo.lock` in the main
  checkout. Send its output to a log under `.stage/` with the tool's own exit
  status after it, and name the tree it ran in. A status read through a pipe,
  or a result built in the wrong directory, is not evidence.
- Never wait on a watcher: no `sleep`, `tail -f`, `ps` or polling loop to learn
  that something ended. Run the command in the foreground, or in the
  background and act on its completion notice.
- When evidence contradicts a theory twice, stop predicting and read the
  computation. Temporary probes are fine; remove them before committing.
- Continue through authorized, actionable next steps. Report real blockers and
  unfinished acceptance criteria precisely. Read the clock with `date` for any
  time you write.

## Delegation and review

- The primary session decides, briefs, checks and reports; workers implement,
  review and research. One coding worker per lane; read-only workers may run
  beside it.
- A brief is all a worker knows: the goal, what is ruled out, the files, the
  exact scope, its worktree and the rules that bind it.
- A reviewer gets the change and the requirement, never your conclusion. Its
  verdict names the exact head and what it read and did not read, and its file
  is saved under the main checkout's `.stage/` before the worktree is removed.
  A design read blocks on behaviour only: lost or stranded data, a wrong
  precedence, a wire or stored-format contract, a state with no way out.
- The final check of a merging change on the logging path or a wire contract
  is the primary's own reading of the merging diff, after the independent
  review.
- Research on the web is allowed and preferred to guessing. Send out only the
  technical question, never private data. Research is not copying: no copied
  or closely paraphrased code, text or definitions.

## Repository integration and durable context

- Resolve merge conflicts by evaluating both sides; never discard one wholesale.
- Use the Stage skills and `stagectl` for issue and pull-request work. Every
  pull-request or branch merge uses `/home/paudley/stage/root/bin/ghprsq`, run
  from inside the pull request's worktree; resolve unmet prerequisites or
  report the blocker. Ordinary base-to-worktree `git merge` is allowed.
- No tool branding or agent credits in source, commits, issues or pull requests.
- Use relevant existing memories and verify facts that may have drifted.
  Automatic memory generation and Hindsight are disabled. Do not mutate private
  memory stores. An explicitly requested manual note may go under
  `~/.codex/memories/extensions/ad_hoc/notes/`; never edit generated memory
  files. Record durable project knowledge in repository documentation or report
  it in chat; do not create another private memory store.

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

## Shell commands

- One shell call does one thing. A few related commands may share a call, never
  a long chain joined by `&&` or `;`. A step that needs a script is written as
  a file and run.
- A deletion is its own call, on one exact path inside the repository that an
  earlier call listed. Never combine `rm -r`, `rm -rf`, `find -delete`,
  `git clean` or `git worktree remove --force` with other commands, and never
  delete by glob or variable. For worktrees and branches use
  `stagectl cleanup` or the plain `git worktree remove`.
- A permission prompt blocks the whole session, so send nothing that could
  raise one: no path outside the repository and no long compound command. If a
  call is refused, do not resend it in another form; use the plain small
  version or report it.
- Never start, stop or restart the running daemon or touch its live store or
  consume root unless Patrick asks for that action.
