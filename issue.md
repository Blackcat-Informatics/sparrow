# Issue #9: Give producers a stable versioned contract independent of the storage schema

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Summary

Define a named producer profile, `sparrow-producer/1`, that the daemon advertises in discovery, and make `sparrow-client` producers check that profile instead of the exact storage schema version. Today every schema bump makes every already-built producer stop delivering for good, so a Sparrow upgrade silently cuts off fleet logging until each daemon is rebuilt. Query clients (CLI and MCP) keep their exact schema check.

## Current behavior

Verified on `main` at `ac6277f`.

- `crates/sparrow-client/src/lib.rs:69`: `pub const SCHEMA_VERSION: u32 = 63;`.
- `crates/sparrow-client/src/receipt.rs:81-101`: `Discovery::decode` refuses unless `schema_version == crate::SCHEMA_VERSION` and the event batch limits equal the client's own constants exactly (`:95-96`, `MAX_EVENT_BATCH_BYTES` 1 MiB and `MAX_EVENT_BATCH_EVENTS` 256 from `events.rs:9-11`). The refusal is `Error::Unsupported("requires the current schema version, matching event batch limits, …; use a matching daemon executable")`.
- `crates/sparrow-client/src/lib.rs:191-209`: `Client::connect_endpoint` always runs that check. Every producer path uses it, including the `Delivery` worker (`delivery.rs:772-773`).
- `crates/sparrow-client/src/error.rs:44-53`: `Error::retryable` returns `false` for `Unsupported`. The worker therefore returns the error (`delivery.rs:795-797`), `fail` latches it, and every later `try_send` is dropped with `DropReason::Failed` (`delivery.rs:369-371`) until the process restarts.
- The daemon side does not need the exact match for producers. `EventBatch` and `Ingest` carry no schema version (`sparrow.capnp:333-340`), and `rpc.rs` checks a request's schema only on query and catalog operations (for example `scope` at `rpc.rs:1177-1186`). `discover` (`rpc.rs:2610-2636`) reports `SCHEMA_VERSION` and the limits, but no producer contract.
- `docs/interfaces.md:1675-1683` says older daemons are refused with `UNSUPPORTED` and that discovery establishes compatibility. No document names a producer contract.

## Work

**Profile definition (`docs/interfaces.md`, new section "Producer contract `sparrow-producer/1`" under section 5).** The profile covers:
- Operations: `discover`, `ingest`, `importBegin` through `importCancel`, `eventsIngest` and `flush`, with their current request and result structs.
- Limits, read from discovery and never hard-coded: `maxArtifactBytes`, `maxRecordsPerArtifact`, `maxEventBatchBytes`, `maxEventBatchEvents` and `ProducerLimits`.
- Retry rules: which codes keep the same request identity (`STORAGE_PRESSURE`, `RESOURCE_LIMIT`, `SHUTTING_DOWN`, transport `Disconnected` and `Overloaded`, and the I/O kinds in `delivery.rs:810-826`), and which codes stop delivery of a batch (`INVALID_ARGUMENT`, `CONFLICT`, `STORE_MISMATCH`).
- Endpoint access: the private Unix socket and anonymous TCP both admit every profile operation (`tcp_gate.rs:100-113`).
- Versioning rule: a profile changes only by additive Cap'n Proto fields and operations. Removing an operation or field, or changing its meaning, needs a new profile name. The daemon advertises every profile it still serves.

**Wire (`crates/sparrow-client/schema/sparrow.capnp`).** Add `producerProfiles :List(Text)` to `Discovery` and to `Status` as new trailing fields. The daemon advertises `["sparrow-producer/1"]` from one constant, `sparrow_client::PRODUCER_PROFILE`. This advances the schema version by one; the producer profile stays `sparrow-producer/1`.

**Client (`crates/sparrow-client/src/receipt.rs`, `lib.rs`, `delivery.rs`).**
- Split discovery decoding. `Discovery::decode` keeps today's exact check for query clients. A new producer decode requires `producerProfiles` to contain `PRODUCER_PROFILE` and accepts any `schema_version`. It treats the daemon's batch limits as ceilings: the client uses the smaller of its own and the daemon's value, and refuses only a daemon ceiling below one event (`MAX_EVENT_BYTES`, 64 KiB).
- Add `Client::connect_producer(endpoint)`, which uses the producer decode. Route the `Delivery` worker and the SDK's producer-only entry points (`ingest_with_durability`, `events_with_durability`, `flush`, `import_file`) through it. CLI and MCP keep `Client::connect_endpoint`.
- A daemon that lacks the profile, including any daemon older than this change (its list decodes as empty), returns `Error::Unsupported` naming `sparrow-producer/1`. In the `Delivery` worker this is not terminal. The worker keeps its pending batch, records the error as `last_transient_failure`, and retries discovery every 30 seconds, so admission continues under the queue's normal bounds. Upgrading the daemon resumes delivery without restarting the producer.

**Server (`crates/sparrow-server/src/rpc.rs`, `agent/status.rs`).** `discover` and `status` fill `producerProfiles`. CLI `status` and `capabilities` render it as `producer_profiles`.

**Docs.** In `docs/interfaces.md` section 8 "Errors and compatibility", state that producer compatibility is by profile and query compatibility is by exact schema. In "Rust client and logging integration", state that the `Delivery` worker keeps retrying discovery every 30 seconds while the profile is missing; no document says so today.

## Out of scope

- The delivery modes (lossless-confirmed and lossy-aspirational) and their spool.
- Publishing `sparrow-client` to a crate registry.
- Relaxing the exact schema check for CLI or MCP query clients.

## Depends on

None.

## Automated acceptance

- `crates/sparrow-client/src/receipt.rs` test `producer_discovery_accepts_a_newer_schema_that_advertises_its_profile`: a discovery message with `schema_version = SCHEMA_VERSION + 1`, a trailing unknown field and `producerProfiles = ["sparrow-producer/1"]` decodes for producers; the query decode refuses it. This protects schema independence for producers and exactness for queries.
- `producer_discovery_refuses_a_daemon_without_the_profile`: an empty profile list gives `Unsupported` naming the profile.
- `producer_discovery_uses_smaller_batch_ceilings`: a daemon advertising 128 events per batch makes the worker build batches of at most 128.
- `crates/sparrow-client/src/delivery.rs` test `missing_producer_profile_backs_off_and_resumes_without_restart`: against a test daemon that first advertises no profile and then advertises it, the worker reports a transient `Unsupported`, never latches `terminal_failure`, and delivers the queued event once the profile appears.
- `crates/sparrow-server` test `discover_and_status_advertise_the_producer_profile`: both report `sparrow-producer/1`, over Unix and anonymous TCP.

`make check` passes.

## UAT

Run on this host after the change is deployed. Step 3 emulates a producer built against an older schema in a scratch worktree below the main checkout.

0. Set up:
   ```bash
   SOCK=/SPARROW/store/rpc.sock
   SP="$HOME/.local/bin/sparrow $SOCK"
   REV=$(git -C ~/Active/sparrow rev-parse --short origin/main)
   ```
1. Confirm the deployment advertises the profile:
   ```bash
   systemctl --user is-active sparrow.service
   $SP status | jq '{schema_version, producer_profiles}'
   ```
   Expected: `active`; `producer_profiles` is `["sparrow-producer/1"]`.
2. A producer built at the deployed revision delivers:
   ```bash
   cd ~/Active/sparrow && cargo build --locked --release -p sparrow-client --example delivery
   ~/Active/sparrow/target/release/examples/delivery $SOCK uat.producer-profile $(date +%s%3N) "profile check current"
   ```
   Expected: one line `<artifact id> Durable sequence=1 events=1 instance=<hex>` and exit status 0.
3. A producer built for an older schema still delivers, while an older query client is refused:
   ```bash
   git -C ~/Active/sparrow worktree add ~/Active/sparrow/.worktrees/uat-old-schema $REV
   cd ~/Active/sparrow/.worktrees/uat-old-schema
   OLD=$(( $(grep -oP 'SCHEMA_VERSION: u32 = \K[0-9]+' crates/sparrow-client/src/lib.rs) - 1 ))
   sed -i "s/SCHEMA_VERSION: u32 = [0-9]*/SCHEMA_VERSION: u32 = $OLD/" crates/sparrow-client/src/lib.rs
   cargo build --release -p sparrow-client --example delivery
   cargo build --release -p sparrow-server --bin sparrow
   target/release/examples/delivery $SOCK uat.producer-profile $(date +%s%3N) "profile check older schema"
   target/release/sparrow $SOCK status; echo "status exit=$?"
   ```
   Expected: the delivery line reports `Durable sequence=1 events=1` with exit status 0. The old `sparrow status` prints `Error: Unsupported("requires the current schema version, …` and `status exit=1`, as any older CLI does against this daemon today.
4. Both events are searchable:
   ```bash
   $SP records --source uat.producer-profile --text "profile check" --received-from "$(date -u -d '-15 min' +%FT%TZ)" --page-size 10 | jq '.rows | length'
   ```
   Expected: `2`.
5. Remove the scratch worktree:
   ```bash
   git -C ~/Active/sparrow worktree remove --force ~/Active/sparrow/.worktrees/uat-old-schema
   ```
   Expected: the worktree is gone from `git -C ~/Active/sparrow worktree list`.

Done when every UAT step passes on the live service.


## Comments (1)

### paudley — 2026-11-21T22:47:56Z

The shared producer-only gate admits discovery, event and artifact ingestion, upload and flush. It denies status on both anonymous TCP and the ingest socket. Verify discovery over those producer endpoints and verify status over the private socket and authenticated TCP. The anonymous status refusal remains part of the access contract.

The logging worker currently constructs a pending request before discovery, and callers can inspect it while the server is unavailable. Apply the advertised byte and event ceilings before first transmission. Preserve the identity and exact data for any send whose outcome is unknown. If a new ceiling cannot accommodate such a request, report a compatibility hold rather than recutting potentially committed events. An oversized single event with no prior transmission can follow the documented input-refusal path. The ordinary query and administrative clients retain strict discovery.


## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
