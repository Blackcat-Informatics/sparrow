# Issue #10: Add a group-accessible ingest-only Unix socket that records SO_PEERCRED provenance

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Summary

Add a second Unix socket that members of one configured group can reach. It admits only producer operations, and every artifact or record it accepts carries the caller's uid, gid and pid from `SO_PEERCRED` in its provenance. Daemons that run as other users, such as `katamari-stored`, katamari-gitd and the memetics engines, cannot reach Sparrow today. katamari-gitd's sandbox also rules out TCP. With this socket they can log into Sparrow, and every line shows which account sent it.

## Current behavior

Verified on `main` at `ac6277f`.

- `crates/sparrow-server/src/local_endpoint.rs:66` sets the only Unix socket to mode 0600. `crates/sparrow-server/src/config.rs:265` keeps `data_dir` at 0700, and `:210-211` requires `socket_path` to sit directly inside `data_dir`. On this host `data_dir` is `/SPARROW/store`, and `/SPARROW` is also 0700, so no other account can reach the socket.
- `crates/sparrow-server/src/main.rs:178-181` admits every Unix connection with `Access::Local`. `crates/sparrow-server/src/transport.rs:31-36,57` maps `Access::Local` to the complete typed service, including status, queries, pins, watch administration and flush. No code reads peer credentials; a search of the workspace for `peer_cred` or `SO_PEERCRED` finds nothing.
- Anonymous public TCP already has an ingest-only gate. `crates/sparrow-server/src/tcp_gate.rs:100-113` forwards `ingest`, `importBegin` through `importCancel`, `eventsIngest`, `flush` and `discover`, and `:115-154` refuses every other operation with `UNAUTHENTICATED`. The gate also carries the CHAP upgrade (`:156-182`), which a local group socket must not offer.
- Unix and TCP connections share one ceiling, `max_rpc_connections` (1..256, default 16, `config.rs:207-208`). Over the ceiling a stream is dropped with no reply (`transport.rs:50-52`). Every connection closes after the 60-second lease (`transport.rs:39`).
- `crates/sparrow-core/src/storage.rs:253-276`: `Artifact` provenance holds the source label, format and receipt time, with no caller identity.

## Work

**Configuration (`crates/sparrow-server/src/config.rs`).**
- Add `ingest_socket_path`, an absolute path whose parent directory is outside `data_dir`, and `ingest_socket_gid`, a numeric group ID, which is required when the path is set. Add `max_ingest_connections` (1..256, default 16), a ceiling separate from `max_rpc_connections`, so producers cannot starve CLI and MCP connections.
- Startup validates the parent directory and refuses to start, with an actionable message, unless it is a real directory (not a symlink), owned by the daemon's uid, has group `ingest_socket_gid`, and has mode exactly `2750`.
- After binding, the socket must have group `ingest_socket_gid` (inherited from the setgid directory). The daemon sets mode `0660` and checks both; a mismatch is a startup failure.
- Stale-name recovery and shutdown cleanup reuse the identity checks in `local_endpoint.rs`.

**Gate (`crates/sparrow-server/src/tcp_gate.rs`, new `ingest_gate.rs`).**
- Move the forwarded and refused operation lists into one ingest gate shared by anonymous TCP and the ingest socket, so an operation added later needs one admission decision.
- The ingest socket forwards the same producer operations. When #7 adds `producerWindowGet`, it joins this shared list. It refuses every other operation with a new code, `PERMISSION_DENIED`, and refuses `authChallenge` and `authenticate` with `UNSUPPORTED`. Anonymous TCP keeps `UNAUTHENTICATED` and its CHAP upgrade.

**Peer credentials (`crates/sparrow-server/src/main.rs`, `transport.rs`, `rpc.rs`).**
- On accept, read `UnixStream::peer_cred()` (uid, gid, pid). A failure to read it refuses the connection and is logged; the connection is never admitted without credentials.
- Add `Access::IngestSocket(PeerCredential)`. The credential travels with the connection's `Rpc` into every ingest call.
- An import binds the credential of the connection that ran `importBegin`. A later `importAppend` through `importPublish` from a different uid is refused with `PERMISSION_DENIED`, so a resumed upload cannot change its provenance.

**Provenance (`crates/sparrow-core/src/storage.rs`, `artifact_rdf.rs`, `rdf_index.rs`).**
- Add an optional `intake` to `Artifact` and `Record`: endpoint `ingest-socket`, uid, gid and pid. It is set only for evidence accepted on the ingest socket.
- The artifact graph and each record's metadata graph get `sp:intakeEndpoint "ingest-socket"`, `sp:peerUid`, `sp:peerGid` and `sp:peerPid` as `xsd:nonNegativeInteger` literals.
- Fence the store with a new writer-format family so older writers cannot drop these facts.

**Wire and CLI.** Add `intake :OptionalIntake` to the typed `Artifact` and `Record`. CLI `context` and MCP `records_context` render it on each record as `intake`: `null`, or `{"endpoint":"ingest-socket","uid":"N","gid":"N","pid":"N"}` with decimal strings. Status lists the ingest socket path, gid, active connections and refused connections. This advances the schema version by one.

**Packaging and docs.**
- Add `packaging/tmpfiles/sparrow-ingest.conf` with `d /run/sparrow-ingest 2750 paudley sparrow-ingest -`.
- `docs/deployment.md`: create the group, add producer accounts to it, install the tmpfiles entry, and set the three keys. Explain that the daemon's account does not need to be in the group.
- `docs/interfaces.md`:
  - Add the ingest socket to the RPC binding row in section 1 and to "Public TCP and syslog packet interpretation".
  - Add the four predicates to "Core graph vocabulary".
  - Add `PERMISSION_DENIED` to the error table in section 8.

## Out of scope

- Recording peer credentials on the private socket.
- Binding sources to allowed uids.
- Remote producers.

## Depends on

None.

## Automated acceptance

Tests in `crates/sparrow-server` that run as the test user, with a temporary setgid directory owned by one of the user's groups:

- `ingest_socket_records_peer_credentials_in_provenance`: an `eventsIngest` and an `ingest` over the ingest socket produce records whose typed `intake` and metadata graph carry the test process's uid, gid and pid.
- `ingest_socket_refuses_reads_and_administration`: `status`, `recordsSearch`, `pin`, `watchPut` and `bytesRead` return `PERMISSION_DENIED`, and `authenticate` returns `UNSUPPORTED`.
- `import_resumed_by_another_uid_is_refused`: covered by a unit test of the import binding with two synthetic credentials.
- `ingest_socket_directory_with_wrong_mode_owner_or_group_fails_startup`: one case each for mode, owner, group and symlink.
- `ingest_connections_have_their_own_ceiling`: with `max_ingest_connections = 1`, a second ingest connection is dropped while a private-socket CLI connection still succeeds.
- `every_schema_operation_has_an_ingest_gate_decision`: a compile-time or test-time check that the shared gate lists every `Sparrow` method.

`make check` passes.

## UAT

Run on this host after the change is deployed. Step 0 is host administration with `sudo`.

0. Prepare the host and configuration:
   ```bash
   sudo groupadd --system sparrow-ingest
   sudo usermod -aG sparrow-ingest katamari
   sudo install -m 0644 ~/Active/sparrow/packaging/tmpfiles/sparrow-ingest.conf /etc/tmpfiles.d/
   sudo systemd-tmpfiles --create /etc/tmpfiles.d/sparrow-ingest.conf
   stat -c '%a %U:%G' /run/sparrow-ingest
   GID=$(getent group sparrow-ingest | cut -d: -f3)
   printf 'ingest_socket_path = "/run/sparrow-ingest/ingest.sock"\ningest_socket_gid = %s\n' $GID
   ```
   Expected: `2750 paudley:sparrow-ingest`. Add the two printed lines to `~/.config/sparrow/server.toml` above the first table, then `systemctl --user restart sparrow.service`.
1. Confirm the socket:
   ```bash
   SOCK=/SPARROW/store/rpc.sock
   SP="$HOME/.local/bin/sparrow $SOCK"
   STORE=$($SP status | jq -r .store_id)
   systemctl --user is-active sparrow.service
   stat -c '%a %U:%G %F' /run/sparrow-ingest/ingest.sock
   $SP status | jq .ingest_socket
   ```
   Expected: `active`; `660 paudley:sparrow-ingest socket`; status shows the path and gid.
2. Ingest as another account in the group:
   ```bash
   cd ~/Active/sparrow && cargo build --locked --release -p sparrow-client --example events && cargo build --locked --release -p sparrow-server --bin sparrow
   install -m 0755 target/release/examples/events /tmp/sparrow-uat-events
   install -m 0755 target/release/sparrow /tmp/sparrow-uat-cli
   sudo -u katamari /tmp/sparrow-uat-events /run/sparrow-ingest/ingest.sock uat.ingest-socket uat-peer-$(date +%s) $(openssl rand -hex 16) $(date +%s%3N) "peer credential check" durable 1
   ```
   Expected: exit status 0 and a durable receipt.
3. Confirm the caller's identity in provenance:
   ```bash
   RID=$($SP records --source uat.ingest-socket --received-from "$(date -u -d '-10 min' +%FT%TZ)" --page-size 1 | jq -r '.rows[0].record_id')
   $SP context $STORE $RID --before 0 --after 0 | jq -c .anchor.intake
   jq -n --arg s "$STORE" --arg r "$RID" '{store_id: $s, scope: {kind: "selection", store_id: $s, record_ids: [$r]}, layers: {metadata: true, diagnostic: false, content: false}, query: "PREFIX sp: <urn:sparrow:vocab:> SELECT ?uid ?gid WHERE { ?r sp:peerUid ?uid ; sp:peerGid ?gid }"}' > /tmp/sparrow-uat-peer.json
   $SP sparql /tmp/sparrow-uat-peer.json | jq -c '[.rows.native_result.results.bindings[] | {uid: .uid.value, gid: .gid.value}]'
   id -u katamari; id -g katamari
   ```
   Expected: `intake` is `{"endpoint":"ingest-socket","uid":"951","gid":"948","pid":"<nonzero>"}`, matching `id -u katamari` (951) and `id -g katamari` (948); the SPARQL result is `[{"uid":"951","gid":"948"}]`. The query binds `sp:peerUid`, so it reads only ingest-socket rows.
4. The same account cannot read or administer:
   ```bash
   sudo -u katamari /tmp/sparrow-uat-cli /run/sparrow-ingest/ingest.sock status; echo "exit=$?"
   sudo -u katamari /tmp/sparrow-uat-cli /run/sparrow-ingest/ingest.sock records --source uat.ingest-socket; echo "exit=$?"
   sudo -u katamari /tmp/sparrow-uat-cli /run/sparrow-ingest/ingest.sock watch list; echo "exit=$?"
   ```
   Expected: each fails with `PERMISSION_DENIED` and `exit=1`.
5. Accounts outside the group cannot connect, and the private socket stays private:
   ```bash
   sudo -u nobody /tmp/sparrow-uat-cli /run/sparrow-ingest/ingest.sock status; echo "exit=$?"
   sudo -u katamari /tmp/sparrow-uat-cli $SOCK status; echo "exit=$?"
   ```
   Expected: both fail with a permission-denied connection error and `exit=1`.
6. Clean up:
   ```bash
   rm -f /tmp/sparrow-uat-events /tmp/sparrow-uat-cli /tmp/sparrow-uat-peer.json
   ```

Done when every UAT step passes on the live service.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
