# Issue #10 implementation report

Branch `paudley/10-add-a-group-accessible-ingest-only-unix`, based on main `c11b747`.
The change is uncommitted. The working tree has 31 modified tracked files (1004
insertions, 185 deletions) and four new paths. Line numbers below refer to the
working tree at the time this report was written.

## Files changed and why

### sparrow-core
- `src/storage/intake.rs` (new): `INGEST_SOCKET`, `PeerCredential{uid,gid,pid}`,
  `Intake::IngestSocket` (line 25), catalog encode and decode, `admit_import` (line 115),
  and the writer fence `fence` (line 83).
- `src/storage.rs`: `Artifact.intake` and `Record.intake` (line 381), encoded only
  when present. `ingest_with_intake` (line 1534) is the one ingest path;
  `ingest_with_durability` delegates to it. `ingest_producer_with_intake` also
  exists. `ingest_input` fences before the first intake write (line 1656).
- `src/storage/ingest.rs`: if a retried `request_id` arrives with a different intake
  uid, the retry returns CONFLICT (lines 523 and 557).
- `src/storage/import.rs`:
  - `ImportManifest.intake`.
  - `import_start_with_intake`; `begin_import` binds the credential and fences
    before committing the manifest (line 1174).
  - `import_admit` (line 648) checks the caller against the bound credential.
- `src/storage/manifest_retire.rs`: an `intake` header arm (line 242), so retirement
  accepts the new structured member.
- `src/storage/rdf_index.rs`: `intake_metadata` (line 786) writes the four predicates
  onto the artifact graph and onto each record metadata graph.
- `src/storage/writer_format.rs`: new writer-format families `INTAKE`
  (`sparrow.store/110` to `/120`, line 109) and `INTAKE_ARCHIVE` (`/121` to `/131`).
  They interact with the existing archive and log-time families.
- `src/storage/publication.rs`, `publication/raw.rs`, `tail.rs`,
  `receiver_stream.rs`, `query_membership.rs`, `recordless.rs`: carry the new
  `RecordInput.intake` field (`None` for tail and receiver sources).
- `src/lib.rs`: re-exports `INGEST_SOCKET`, `Intake`, `PeerCredential`, `admit_import`.
- `tests/storage.rs`: the archive-fence fixture used `sparrow.store/110` as an
  "unknown future identity". That identity is now the intake family, so the
  fixture uses `/132`.

### sparrow-client
- `schema/sparrow.capnp`:
  - `Artifact.intake @11` and `Record.intake @14`, both `:OptionalIntake` (lines 30 and 90).
  - `enum IntakeEndpoint{ingestSocket}` and `struct Intake{endpoint,uid,gid,pid}`.
  - `Status.ingestSocket @52` (line 246), which reports path, gid, max, active and
    refused connections.
- `src/lib.rs`: `SCHEMA_VERSION` 67 to 68 (line 69).

### sparrow-server
- `src/config.rs`:
  - New keys `ingest_socket_path` (line 59), `ingest_socket_gid` and
    `max_ingest_connections` (line 66; default 16, range 1..256 at line 286).
  - `validate_ingest_socket` (line 285) requires an absolute path whose parent is
    outside `data_dir`, and requires the path and gid together (line 289).
  - Query-only mode refuses the path (line 272).
- `src/ingest_socket.rs` (new):
  - `IngestSocket` has its own semaphore and active and refused counters.
    `admit` (line 56) reads `peer_cred` (line 93); when the read fails it refuses
    the connection and logs it (line 62).
  - `check_directory` (line 116) and `check_socket` (line 163) enforce the
    socket rules; `SOCKET_MODE` is `0660`.
- `src/ingest_gate.rs` (new):
  - One `decide!` table (line 126) holds every `Sparrow` method and its forward or
    refuse decision.
  - A `Policy` trait supplies the refusal, CHAP and session behavior.
    `IngestSocketPolicy` (line 43) refuses with `PERMISSION_DENIED` and answers CHAP
    with `UNSUPPORTED`.
- `src/tcp_gate.rs`: `AnonymousTcp` implements the shared `Policy` (line 42) and
  keeps `UNAUTHENTICATED` and its CHAP upgrade. Its private operation list was
  removed.
- `src/transport.rs`:
  - `Access::IngestSocket(PeerCredential)` (line 38), wired at line 69.
  - `admit_connection` reports whether the connection was admitted.
- `src/rpc.rs`:
  - `with_ingest_socket` (line 216) and `with_peer` (line 222) set `intake`.
  - `import_work` (line 228) calls `Store::import_admit` before import append, get,
    seal, layout, prepare and publish.
  - `ingest`, `eventsIngest` and `importBegin` pass intake through.
  - `intake()` encodes the field (line 907), and status encodes the socket
    (line 2811).
  - `chap_unsupported` is now `pub(crate)`.
- `src/local_endpoint.rs`:
  - `bind_ingest` (line 34) binds the socket, sets mode `0660` and checks group
    and mode.
  - `bind_recovering` (line 57) is the shared stale-name recovery for both sockets.
- `src/shutdown.rs`: `finish` (line 29) cleans up a slice of endpoints, so both
  sockets share one identity-checked removal.
- `src/main.rs`: binds the ingest endpoint (line 104), constructs `IngestSocket`
  (line 107), and adds a separate accept branch (line 239).
- `src/agent/context.rs`: `render_record` emits `intake` (lines 82 and 101) for CLI
  `context` and MCP `records_context`.
- `src/agent/status.rs`: emits `ingest_socket` (line 124).
- `src/lib.rs`: declares the new modules.
- `tests/rpc.rs`: the `configure_ingest_socket` helper (line 1696) and three
  integration tests.

### Packaging, docs and ledger
- `packaging/tmpfiles/sparrow-ingest.conf` (new): `d /run/sparrow-ingest 2750 paudley sparrow-ingest -`.
- `docs/deployment.md`: new section "Ingest-only group socket" (line 144). It covers
  creating the group, adding producers to it, the tmpfiles entry and the three keys,
  and states that the daemon's account does not need to be in the group.
- `docs/interfaces.md`:
  - Ingest socket added to the RPC binding row (line 526) and to "Public TCP and
    syslog packet interpretation" (lines 1826-1829).
  - The four predicates added to "Core graph vocabulary" (line 1539 onward), plus a
    store-format paragraph (around line 1572).
  - `PERMISSION_DENIED` added to the section 8 error table (line 2763).
- `docs/archive-format.md` (line 259): names the `/121` to `/131` archive-plus-intake family.
- `helpers-ledger.toml`: new jobs `producer-only-ingest-gate` and
  `ingest-socket-intake-provenance`, plus updated `owned-local-endpoint` entry points.

## Work bullets

| Work bullet | Where |
| --- | --- |
| Config keys, parent outside `data_dir`, gid required, separate ceiling 1..256 default 16 | `config.rs:59-66`, `:285-289` |
| Directory must be a real directory owned by the daemon uid, with group gid and mode exactly 2750; otherwise an actionable startup error | `ingest_socket.rs:116` (`check_directory`), called from `local_endpoint.rs:34` |
| Socket group inherited, mode 0660 set and both checked; a mismatch fails startup | `local_endpoint.rs:44-46`, `ingest_socket.rs:163-168` |
| Stale-name recovery and shutdown reuse the identity checks | `local_endpoint.rs:57` (`bind_recovering`), `shutdown.rs:29` |
| One shared gate for anonymous TCP and the ingest socket | `ingest_gate.rs:126`, `tcp_gate.rs:42` |
| Ingest socket: `PERMISSION_DENIED` for other operations, `UNSUPPORTED` for CHAP; TCP keeps `UNAUTHENTICATED` and CHAP | `ingest_gate.rs:43`, `tcp_gate.rs:42` |
| `peer_cred` on accept; a failure refuses and logs | `ingest_socket.rs:56-62`, `:93` |
| `Access::IngestSocket(PeerCredential)` carried into every ingest call | `transport.rs:38`, `:69`; `rpc.rs:222` |
| Import bound to the `importBegin` uid; append through publish from another uid gets `PERMISSION_DENIED` | `intake.rs:115`, `import.rs:648`, `rpc.rs:228` |
| Optional `intake` on `Artifact` and `Record`, set only for ingest-socket evidence | `storage.rs:381`, `intake.rs:25` |
| Four predicates as `xsd:nonNegativeInteger` on the artifact and record metadata graphs | `rdf_index.rs:786-800` |
| Writer-format fence | `writer_format.rs:109`, `intake.rs:83`, `storage.rs:1656`, `import.rs:1174` |
| Wire, CLI, MCP and status; schema +1 | `sparrow.capnp:30`, `:90`, `:246`; `sparrow-client/src/lib.rs:69`; `agent/context.rs:82`; `agent/status.rs:124` |
| Packaging and docs | See the previous section |

## Acceptance tests

| Test | Location | Result log |
| --- | --- | --- |
| `ingest_socket_records_peer_credentials_in_provenance` | `crates/sparrow-server/tests/rpc.rs:1723` | `test-rpc-final.log` |
| `ingest_socket_refuses_reads_and_administration` | `crates/sparrow-server/tests/rpc.rs:1861` | `test-rpc-final.log` |
| `import_resumed_by_another_uid_is_refused` | `crates/sparrow-server/src/ingest_gate.rs:198` | `test-server-lib-bins-final.log` |
| `ingest_socket_directory_with_wrong_mode_owner_or_group_fails_startup` | `crates/sparrow-server/src/ingest_socket.rs:184` | `test-server-lib-bins-final.log` |
| `ingest_connections_have_their_own_ceiling` | `crates/sparrow-server/tests/rpc.rs:1908` | `test-rpc-final.log` |
| `every_schema_operation_has_an_ingest_gate_decision` | `crates/sparrow-server/src/ingest_gate.rs:280` | `test-server-lib-bins-final.log` |

What each test checks:

- `ingest_socket_records_peer_credentials_in_provenance`: one `eventsIngest` and one
  `ingest`. It checks the typed `intake` and SPARQL over the metadata graph against
  the test process's uid, gid and pid.
- `ingest_socket_refuses_reads_and_administration`: `status`, `recordsSearch`, `pin`,
  `watchPut` and `bytesRead` return `PERMISSION_DENIED`, and `authenticate` returns
  `UNSUPPORTED`.
- `import_resumed_by_another_uid_is_refused`: two synthetic credentials (uids 951 and
  952). The other uid's append and publish are refused; the owner's append succeeds.
- `ingest_socket_directory_with_wrong_mode_owner_or_group_fails_startup`: one case
  each for mode, owner, group and symlink.
- `ingest_connections_have_their_own_ceiling`: with `max_ingest_connections = 1`, a
  second ingest connection is dropped while a private-socket CLI connection still
  succeeds.
- `every_schema_operation_has_an_ingest_gate_decision`: parses `sparrow.capnp` and
  compares its methods with the gate's `DECISIONS` list.

Tests use temporary directories only. A setgid directory uses the test user's own
gid, found at runtime; no host gid is hardcoded. No fixed TCP ports are used.

## Commands and results

All commands ran in the worktree. Logs are in
`.stage/add-a-group-accessible-ingest-only-unix/logs/`, and each log ends with `EXIT=`.

Final runs, on the current source:

| Command | Log | Result |
| --- | --- | --- |
| `cargo fmt --all --check` | `fmt-check-final.log` | EXIT=0 |
| `cargo build --locked --release -p sparrow-server --bins` | `build-bins.log` | EXIT=0 |
| `cargo test --locked --release -p sparrow-core --lib --test storage` | `test-core-final.log` | lib 33 passed; storage 212 passed, 0 failed; EXIT=0 |
| `cargo test --locked --release -p sparrow-server --lib --bins` | `test-server-lib-bins-final.log` | lib 28 passed; bins 0 and 6 passed; EXIT=0 |
| `cargo test --locked --release -p sparrow-server --test rpc -- ingest_socket ingest_connections public_tcp capability_documents query_only` | `test-rpc-final.log` | 9 passed, 0 failed, 72 filtered out; EXIT=0 |
| `cargo clippy --locked --release -p sparrow-core -p sparrow-client -p sparrow-server --all-targets -- -D warnings` | `clippy-2.log` | EXIT=0 |

Clippy ran before the last test-fixture edit, which only changed a string in
`tests/storage.rs`, and before formatting, which reported no changes.

Earlier runs are kept as history:

- `clippy.log`: EXIT=101. It flagged `too_many_arguments` on `begin_import` and a
  needless borrow; both were fixed.
- `test-rpc-focused.log`: the SPARQL check expected one subject, but an artifact
  selection also returns its record. The test was corrected; see
  `test-rpc-focused-2.log`, 5 passed.
- `test-core-storage.log`: one failure, from the `/110` fixture described above.

## Deviations from the issue

1. **The fence is lazy.** It is committed in its own synchronized batch before the
   first ingest-socket write, not at open. Fencing at open would make every store
   unable to roll back and would change the exact store format that many existing
   tests assert. A store that never served the ingest socket keeps its format.
2. **Inline retries.** An inline `ingest` retry with the same `request_id` from a
   different uid returns CONFLICT. This keeps provenance from changing on retry,
   which is the inline counterpart of the import binding.
3. **Query-only mode** refuses `ingest_socket_path`, because that mode refuses all
   writes.
4. **`importCancel` is not bound to the uid.** The issue binds only `importAppend`
   through `importPublish`.
5. **`import_resumed_by_another_uid_is_refused`** is a unit test in
   `sparrow-server` (`ingest_gate.rs`). It uses two synthetic credentials through
   the real `Rpc` path, not a pure core test.
6. **`docs/archive-format.md`** gained one line for the `/121` to `/131` family,
   which the fence requires.

## Not done or not run

- `make check` was not run, by instruction. It includes the census and
  `census-check` for the new `helpers-ledger.toml` rows, which were therefore not
  run either.
- No workspace-wide test or clippy run. The rpc integration suite ran only the
  filtered subset above, not all 81 tests.
- No commit.
- `deploy/config/server.toml.example` was not updated with the three keys; the
  issue's packaging list does not name it.
- The client SDK's `ArtifactReceipt` does not expose intake. The typed `Artifact`
  and `Record` carry it.
- The UAT on the host was not run. It requires `sudo` and a deployed build.
