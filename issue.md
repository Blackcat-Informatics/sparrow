# Issue #8: Make lossy-aspirational delivery an explicit producer mode with exact counted drops

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Summary

Make today's best-effort `sparrow-client` delivery an explicit lossy-aspirational mode. In this mode a producer never blocks and never touches disk, every drop is counted by reason, and every drop shows up in Sparrow as an exact sequence gap. Diagnostic producers that must never stall, such as katamari-gitd and the memetics engines, need this guarantee. Today some drops never reach the server as gaps, one refused batch stops a producer for the rest of its life, and the admission lock is held while the worker builds batches.

## Current behavior

Verified on `main` at `ac6277f`, in `crates/sparrow-client/src/delivery.rs` unless noted.

- `try_send` (`:354-399`) never waits for I/O. It refuses with `DropReason` `Full`, `Contended`, `Invalid`, `Closed`, `Failed` or `CounterExhausted` (`:90-103`). Counters are `DeliveryStats.dropped_by_reason` (`:191`), and a flush with drops returns `FlushError::Dropped` (`:503-507`).
- A sequence is assigned only after the state lock is taken (`:375-380`). `Contended` (`:364-366`), `Invalid` (`:360-363`), `Closed` and `Failed` (`:369-374`) drops consume no sequence, so the server never sees them as gaps. Only `Full` drops (`:381-385`) leave a gap.
- The worker holds the same lock while it assembles a batch and calls `batch.validate()` (`next`, `:592-669`). That makes `Contended` drops likely under load.
- Any non-retryable error ends the worker (`:795-797`). This includes `STORE_MISMATCH` from `bind_store` (`:670-689`), `INVALID_ARGUMENT` and `CONFLICT`. After that, every later offer is dropped with `Failed` (`:369-371`) until the process restarts.
- The constructors `DeliveryConfig::new`, `tcp` and `for_endpoint` (`:44-66`) do not name a delivery mode. Defaults: 1,024 queued events, 4 MiB queued bytes, 128 events and 512 KiB per batch, 2 s flush interval, 5 s RPC timeout, 250 ms retry interval.
- `docs/interfaces.md:2319-2321` describes the logging path as nonblocking, dropping new events when full. No document states which drops become server gaps.

## Work

**Mode selection (`crates/sparrow-client/src/delivery.rs`, `logging.rs`).**
- Replace `DeliveryConfig::new`, `tcp` and `for_endpoint` with `DeliveryConfig::lossy(endpoint, source)`, which keeps today's defaults. Update every caller: `crates/sparrow-client/examples/delivery.rs` and `examples/tracing.rs`, `crates/sparrow-client/src/logging.rs` and the `delivery.rs` tests, `crates/sparrow-server/tests/rpc.rs`, `docs/interfaces.md` and `docs/development-runtime.md`.
- Rename `Delivery::try_send` to `send` and update its callers: `TracingLayer` (`logging.rs:65`), `examples/delivery.rs` and `crates/sparrow-server/tests/rpc.rs`. In this mode `send` never waits for a lock, a disk or the network.

**Exact gaps.**
- Assign each offer's sequence from an atomic counter before any admission check, so every drop leaves an unobserved sequence that the server reports as a gap once a later event is delivered.
- The worker sorts each batch by sequence. An event whose sequence is at or below the last sequence already sent is dropped with the new reason `Late`, which also leaves a gap.
- `CounterExhausted` is the only drop without a sequence, because none can be assigned.

**Short critical section.** The worker takes the queued events out of the shared state with `std::mem::take` (or a bounded split) and assembles and validates the batch outside the lock. `send` holds the lock only to push one event.

**Refusals never end delivery.**
- A non-retryable refusal of a batch (`INVALID_ARGUMENT`, `CONFLICT`) drops that batch's events with the new reason `Refused`, records the code and message in `DeliveryStats.last_refusal`, and continues with the next batch.
- `STORE_MISMATCH` drops batches with `Refused` until the owner calls the new `Delivery::rebind(expected_store_id)`. The worker never rebinds by itself.
- Remove `DropReason::Failed`. A worker panic or invariant break still latches `terminal_failure` and makes `send` return `Closed`.
- The final reasons are `Full`, `Contended`, `Invalid`, `Closed`, `Refused`, `Late` and `CounterExhausted`.

**Stats.** Add `last_refusal`, `store_mismatch` (the observed store, when held) and `dropped_sequences`. `dropped_sequences` equals the sum of every reason except `CounterExhausted`.

**Producer example (`crates/sparrow-client/examples/producer.rs`).** Add an example that UAT and later issues use:
```text
producer SOCKET SOURCE lossy COUNT RATE TAG
```
It sends `COUNT` events at `RATE` per second. Each event's `message` field is `"TAG <sequence>"`. It reports `{"instance","offered","queued","dropped","dropped_total","max_send_micros"}` on stderr every second, where `dropped` is an object keyed by `full`, `contended`, `invalid`, `closed`, `refused`, `late` and `counter_exhausted`, every key present, and counts are JSON numbers. At the end it flushes with a 60 s timeout and prints one JSON line on stdout with the same fields plus `durable_sequence`, `durable_events` and `flush` (`ok`, `dropped`, `timed_out`).

**Docs (`docs/interfaces.md`, "Rust client and logging integration").** Name the mode, list the drop reasons, and state the gap rule: every drop except `CounterExhausted` appears as an unobserved sequence once a later event is delivered, and trailing drops stay in the unknown suffix (`continuity_unknown`). Document `rebind`. No schema or store-format change.

## Out of scope

- The lossless-confirmed mode and its spool.
- Producer-window registry capacity.
- A separate connection pool for producers.

## Depends on

#6, #9

## Automated acceptance

Tests in `crates/sparrow-client/src/delivery.rs` or `crates/sparrow-server` integration tests against a test daemon:

- `lossy_send_never_blocks_while_the_daemon_is_absent`: with a missing socket, 10,000 sends finish in under one second in total, and each returns `Queued` or `Dropped { Full }`.
- `every_lossy_drop_leaves_an_exact_server_gap`: with a small queue and a paused test daemon, force `Full` and `Contended` drops, then resume and deliver more events. The window's `unobserved_sequences` equals the producer's `dropped_sequences`, and the gap intervals cover exactly the dropped sequences.
- `lossy_worker_survives_a_refused_batch`: the test daemon refuses one batch with `INVALID_ARGUMENT`. Its events count as `Refused`, `last_refusal` names the code, and later events are delivered.
- `store_mismatch_drops_until_explicit_rebind`: after the endpoint starts serving another store, offers drop as `Refused` with `store_mismatch` set; after `rebind`, delivery resumes.
- `late_sequence_is_dropped_and_counted`: an event whose sequence is below the last sent sequence is dropped as `Late`.
- The existing `contended_and_oversized_allocations_are_drops_and_empty_shutdown_reports_gaps` is updated so the contended and invalid offers consume sequences.

`make check` passes.

## UAT

Run on this host after the change is deployed. Step 2 stops the live service for 20 seconds, as the nightly backup already does.

0. Set up:
   ```bash
   SOCK=/SPARROW/store/rpc.sock
   SP="$HOME/.local/bin/sparrow $SOCK"
   STORE=$($SP status | jq -r .store_id)
   RUN=$(date +%s); SRC=uat.lossy-$RUN
   cd ~/Active/sparrow && cargo build --locked --release -p sparrow-client --example producer
   PR=~/Active/sparrow/target/release/examples/producer
   ```
1. Confirm the deployment:
   ```bash
   systemctl --user is-active sparrow.service
   ```
   Expected: `active`.
2. Send through an outage. The producer runs for 60 seconds at 200 events per second; Sparrow is down for 20 seconds in the middle:
   ```bash
   $PR $SOCK $SRC lossy 12000 200 uat-lossy > /tmp/uat-lossy.json 2> /tmp/uat-lossy.log &
   sleep 15; systemctl --user stop sparrow.service; sleep 20; systemctl --user start sparrow.service
   wait; cat /tmp/uat-lossy.json | jq .
   ```
   Expected: `max_send_micros` is below `1000` (the target); `dropped.full` is above `0`; `flush` is `dropped`; `offered` is `12000`; `queued + dropped_total` equals `offered`.
3. Check that Sparrow reports the drops as exact gaps:
   ```bash
   W=$($SP producer-source $STORE $SRC | jq '.windows[0]')
   echo "$W" | jq '{accepted_events, unobserved_sequences, last_sequence}'
   ```
   Expected: one window; `accepted_events` equals `durable_events` from step 2; `unobserved_sequences` equals `dropped_total`, because the drops all fall before the events sent after the restart; `accepted_events + unobserved_sequences` equals `last_sequence`.
4. Confirm the delivered events are searchable:
   ```bash
   $SP records --source $SRC --received-from "$(date -u -d '-10 min' +%FT%TZ)" --page-size 1 --scan-budget 100000 | jq -r .page.total_items
   ```
   Expected: equal to `durable_events` from step 2.
5. Confirm the service recovered cleanly:
   ```bash
   systemctl --user is-active sparrow.service; $SP status | jq -r .pressure.phase
   ```
   Expected: `active` and `ready`.

Done when every UAT step passes on the live service.


## Comments (1)

### paudley — 2026-11-21T23:31:04Z

The merged admission contract at `cc938dd` allows a brief in-memory lock wait and preserves admitted batches in quarantine on a permanent refusal. A production caller depending on this behavior should retain it. Move expensive batch construction and validation out of the admission critical section and report capacity refusals and capture failures explicitly. The application-facing performance measurements must include caller latency under contention and the fraction admitted under load.

The producer window starts at its first observed sequence. It can expire, omit earlier gaps or decline an update when the registry is full. The SDK's local counters and fences must therefore account for every refusal independently of what the server can later report. The leading, trailing and missing portions of a window cannot establish an exact historical delivery count.


## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
