# Review 1: issue #10, group-accessible ingest-only Unix socket

Reviewer: independent delivery reviewer. Worktree
`/home/paudley/Active/sparrow/.worktrees/10-add-a-group-accessible-ingest-only-unix`, branch
`paudley/10-add-a-group-accessible-ingest-only-unix`, base `main` `c11b747`. Change under review:
the uncommitted working-tree diff (31 tracked files) plus the untracked
`crates/sparrow-core/src/storage/intake.rs`, `crates/sparrow-server/src/ingest_gate.rs`,
`crates/sparrow-server/src/ingest_socket.rs` and `packaging/tmpfiles/sparrow-ingest.conf`.
`packaging/` contains only `tmpfiles/sparrow-ingest.conf`; there is no `packaging/llm/`.

## Scope

Authority: `issue-10.md` (verbatim issue, read first) and the single binding ruling in
`plan.md` (lillith_coordination, 2026-11-06 15:25 CST: "#10 first (ingest-only, SO_PEERCRED
provenance, exactly as the issue states, nothing wider)"). `implementation.md` was treated as a
claim only.

Read: every new file in full; the complete diff of `config.rs`, `local_endpoint.rs`, `main.rs`,
`shutdown.rs`, `transport.rs`, `tcp_gate.rs`, `rpc.rs`, `agent/context.rs`, `agent/status.rs`,
`sparrow.capnp`, `sparrow-client/src/lib.rs`, `storage.rs`, `ingest.rs`, `import.rs`,
`publication.rs`, `publication/raw.rs`, `rdf_index.rs`, `writer_format.rs`,
`manifest_retire.rs`, the tail, receiver and test fixture edits, the three docs and
`helpers-ledger.toml`. Also read for context: `archive.rs::open`, `import_cancel`, every caller
of `writer_format::enable_*` and `has_*`, `deploy/systemd/sparrow.service`, the AppArmor
profile, and the live user unit's confinement (none).

Not done, by instruction: no `make check`, no workspace cargo run, no new cargo run of any
kind, no build-system probing.

## Evidence

Logs in `logs/` end with `EXIT=`; every final log post-dates the last source edit (latest
source mtime 17:03:25 on `import.rs`; clippy 17:04:09, fmt 17:04:28, build 17:10:49, core
tests 17:21:30, server lib and bins 17:30:49, rpc 17:38:24).

| Command (from implementation.md) | Log | Result |
| --- | --- | --- |
| `cargo fmt --all --check` | `fmt-check-final.log` | EXIT=0 |
| `cargo clippy --locked --release -p sparrow-core -p sparrow-client -p sparrow-server --all-targets -- -D warnings` | `clippy-2.log` | EXIT=0 |
| `cargo build --locked --release -p sparrow-server --bins` | `build-bins.log` | EXIT=0 |
| `cargo test --locked --release -p sparrow-core --lib --test storage` | `test-core-final.log` | lib 33 passed, storage 212 passed; EXIT=0 |
| `cargo test --locked --release -p sparrow-server --lib --bins` | `test-server-lib-bins-final.log` | lib 28 passed (includes `import_resumed_by_another_uid_is_refused`, `every_schema_operation_has_an_ingest_gate_decision`, `ingest_socket_directory_with_wrong_mode_owner_or_group_fails_startup`); sparrowd bin 6 passed; EXIT=0 |
| `cargo test --locked --release -p sparrow-server --test rpc -- ingest_socket ingest_connections public_tcp capability_documents query_only` | `test-rpc-final.log` | 9 passed, 72 filtered out (includes the other three named tests and both existing public TCP tests); EXIT=0 |

Reviewer checks run: `prose-check` on the changed docs and the tmpfiles entry. The lines
this change adds pass (EXIT=0 on the added lines alone). The whole of `docs/interfaces.md`
reports 9 `UInt32`/`UInt64` joins, all on untouched lines, and the base file has the same
failure (pre-existing, not this change).

The issue's six named tests exist with the named names, and each exercises the behavior the
issue names:

| Test | Where | What it proves |
| --- | --- | --- |
| `ingest_socket_records_peer_credentials_in_provenance` | `tests/rpc.rs` | Real `sparrowd`; `ingest` and `eventsIngest` over the socket; typed `Record.intake`, CLI `context` `anchor.intake` (decimal strings) and SPARQL over the artifact and record metadata graphs all carry the test process's euid, egid and pid as `xsd:nonNegativeInteger`; a private-socket record has no intake; the socket is group `gid`, mode 0660. |
| `ingest_socket_refuses_reads_and_administration` | `tests/rpc.rs` | `status`, `recordsSearch`, `pin`, `watchPut`, `bytesRead` return `PERMISSION_DENIED`; `authenticate` returns `UNSUPPORTED`; the CLI `status` through the socket fails with `PERMISSION_DENIED`. |
| `import_resumed_by_another_uid_is_refused` | `src/ingest_gate.rs` | Two synthetic credentials through the real `IngestGate` and `Rpc`: another uid's `importAppend` and `importPublish` are refused with `PERMISSION_DENIED`; the owner's append succeeds. |
| `ingest_socket_directory_with_wrong_mode_owner_or_group_fails_startup` | `src/ingest_socket.rs` | `check_directory` refuses owner, group, mode (2770 and 0750) and symlink cases; accepts the correct directory. |
| `ingest_connections_have_their_own_ceiling` | `tests/rpc.rs` | With `max_ingest_connections = 1`, a second ingest connection fails while a private-socket CLI `status` succeeds and reports path, gid, max 1, active 1, refused "1". |
| `every_schema_operation_has_an_ingest_gate_decision` | `src/ingest_gate.rs` | Parses `interface Sparrow` from the schema and compares it with the gate's `DECISIONS`, which the same macro that implements the gate generates. |

## Acceptance and wiring (Work section)

- Configuration: `ingest_socket_path`, `ingest_socket_gid` (required together) and
  `max_ingest_connections` (1..256, default 16) in `config.rs`; the path must be absolute,
  free of `..`, and its parent must be neither inside `data_dir` nor an ancestor of it.
- Startup checks: `LocalEndpoint::bind_ingest` runs `check_directory` (lstat, not a symlink, is
  a directory, owner is `geteuid`, group is the gid, mode exactly 2750) before binding, then sets
  0660 and runs `check_socket` (socket type, group, mode); on mismatch it removes the name it just
  bound and fails. `main.rs` routes any error through `shutdown::finish(Err(..))`, so startup
  fails and the private endpoint is cleaned up. Messages name the path, the problem and a remedy.
- Stale-name recovery and cleanup: both sockets use `bind_recovering` (refused-connection
  liveness proof plus device and inode identity) and `shutdown::finish` now removes every
  endpoint through `remove_name`.
- Shared gate: `ingest_gate::decide!` holds the single forward and refuse list and implements
  `sparrow::Server` for `IngestGate<P>`; `AnonymousTcp` keeps `UNAUTHENTICATED` and CHAP as a
  `Policy`; `IngestSocketPolicy` returns `PERMISSION_DENIED` and `UNSUPPORTED` for both CHAP
  methods. The TCP operation lists were removed (no duplicate), and the existing public TCP
  tests still pass.
- Peer credentials: `IngestSocket::admit` reads `peer_cred()` on accept, refuses and logs when
  it fails or the pid is absent or zero, counts the refusal, and then admits under its own
  semaphore with `Access::IngestSocket(credential)`. `transport.rs` wraps
  `rpc.with_peer(credential)`. `with_peer` is `pub(crate)`, so only the transport can set it.
  The private socket still uses `Access::Local` with no intake (out-of-scope item respected).
- Import binding: `Rpc::import_work` runs `Store::import_admit` before append, get, seal,
  layout, prepare and publish; `begin_import` also refuses a resume from another uid. Published
  artifacts and records take `manifest.intake` in both publication paths (`publication.rs:404`,
  `:575`, `raw.rs:218`).
- Provenance: `Artifact.intake` and `Record.intake`, encoded only when present (catalog rows of
  other evidence are unchanged), decoded with a strict endpoint and 32-bit check;
  `rdf_index::intake_metadata` writes `sp:intakeEndpoint` and the three typed literals on the
  artifact graph and each record's metadata graph. `manifest_retire::header` accepts the new
  object member, so retirement does not fail on ingest-socket artifacts.
- Wire, CLI, status: `OptionalIntake` on `Artifact` (`@11`) and `Record` (`@14`),
  `Status.ingestSocket @52`, `SCHEMA_VERSION` 68; `render_record` is shared by CLI `context`
  and MCP `records_context` (`agent/tool.rs:527`); status renders path, gid, max, active and
  refused.
- Packaging and docs: the tmpfiles line matches the issue byte for byte; `deployment.md` covers
  the group, membership, tmpfiles, the three keys and why the daemon need not be in the group;
  `interfaces.md` updates the RPC binding row, the public TCP section, the vocabulary table and
  the section 8 error table.

## Implementer deviations

1. Schema 67 to 68: required. The issue says "This advances the schema version by one."
2. Writer-format family `sparrow.store/110` to `/131` with a lazy fence: the family is required
   ("Fence the store with a new writer-format family"). Two families of 11 are justified:
   archiving is an independent switch, so an archiving store needs its own intake identity to
   keep both fences (`INTAKE_ARCHIVE`), exactly as `stage`, `enable_archive` and
   `enable_intake` now compose; the unit test covers every transition and that older family
   lists know none of the new identities. Every `enable_*` caller is guarded by its `has_*`
   predicate, and the cumulative predicates treat the intake families as above log time, so no
   open-time job tries to lower them. The lazy fence is justified and stays inside the issue's
   wording: it is committed in its own synchronized batch before the first ingest-socket write
   or import begin, so no intake fact can exist in an unfenced store, and stores that never
   serve the socket keep their format (no forced irreversible upgrade). The
   `docs/archive-format.md` line is a necessary consequence.
3. Test fixture `/110` to `/132` in `archive_fence_refuses_unconfigured_open_and_older_writers`:
   justified. The fixture needs an identity no binary knows; `/110` is now known, so the old
   value would stop testing the refusal. Intent preserved.
4. CONFLICT on an inline retry of the same `request_id` from a different uid: not named by the
   issue, but justified as the inline counterpart of the import binding the issue requires.
   Without it a second account would receive the first account's receipt and record IDs (a
   read the ingest socket otherwise refuses) and would believe its submission was recorded
   under its own identity. It changes nothing for the private socket or TCP (both carry no
   intake), and `interfaces.md` documents it. Not scope expansion in substance.
5. Query-only refuses `ingest_socket_path`: justified and minimal. The socket serves only
   writes, and query-only already refuses `rpc_tcp_address`, consume and syslog the same way.
6. `importCancel` not bound to the uid: matches the issue's literal contract ("`importAppend`
   through `importPublish`"). Cancel cannot change provenance, and consumption imports are
   already protected by `import_cancel`. See O3 for the residual exposure.

The binding also covers `importBegin` resume and refuses a `consume:` import from the ingest
socket. Both are small, provenance-preserving consequences of the required binding, not new
features.

## Findings

No REQUIRED findings.

OPTIONAL:

- **O1. Fence and import provenance have no runtime assertion.** The fence call sites
  (`storage.rs` before the first intake write, `import.rs` before committing an intake
  manifest) and the publication paths that copy `manifest.intake` were verified by reading,
  and the `writer_format` unit test covers the transitions, but no test runs the real path
  and asserts that `store_format` lands in the intake family or that a published import's
  artifact and records carry the importBegin caller. Two cheap additions would close this
  without new tests: assert the `status` `store_format` in
  `ingest_socket_records_peer_credentials_in_provenance`, and finish the owner's import in
  `import_resumed_by_another_uid_is_refused` and assert the published artifact's `intake`.
- **O2. The deployment section does not work under the shipped system unit.**
  `deploy/systemd/sparrow.service` has `ProtectSystem=strict` with
  `ReadWritePaths=/SPARROW/store /SPARROW/intake`, and the AppArmor profile grants no
  `/run/sparrow-ingest/` access, so the system unit cannot bind the socket. The live user unit
  has no such confinement, so the issue's UAT is unaffected. Changing the unit or profile would
  exceed the ruling; a one-sentence note in `docs/deployment.md` that the system unit (#22
  cutover) still needs both, or a follow-up issue, keeps the doc accurate.
- **O3. `importCancel` is unbound.** Import IDs are BLAKE3 hashes of the request ID, so a
  group member who knows or guesses another producer's (or the operator's private-socket)
  request ID can cancel its staging. This is a nuisance, not a provenance change, and anonymous
  TCP already allows it. Binding cancel would be a one-line `import_work` change if wanted;
  the issue does not require it.
- **O4. Known gaps, not required by the issue.** `deploy/config/server.toml.example` lacks
  the three keys; it is the system unit's template, so adding them only makes sense with O2.
  The client SDK's `ArtifactReceipt` omits `intake`; the issue requires it only on the typed
  `Artifact` and `Record`, which carry it.
- **O5. Small API and diagnostic nits.** `sparrow_core::admit_import` is re-exported `pub` but has
  no consumer outside sparrow-core. In `bind_ingest`, a failed `remove_name` after a failed
  socket check returns the removal error and drops the check error. The `ingest_with_durability`/`ingest_with_intake`,
  `ingest_producer`/`ingest_producer_with_intake` and `import_start`/`import_start_with_intake`
  pairs are two public entry points each; acceptable to avoid churning every test caller, but
  the repository's one-path rule would prefer a single signature eventually.

## Open gates (not code findings)

- `make check` is part of the issue's acceptance and has not run, including `census-check`
  for the two new ledger jobs and the 72 filtered-out rpc integration tests, which exercise the
  changed transport and TCP gate. The pre-commit hook runs it; the delivery is not complete
  until that hook passes on this tree.
- The UAT needs `sudo`, a deploy and a restart (Patrick's or coordination's action).
- The work is uncommitted and unpushed.

VERDICT: PASS
