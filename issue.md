# Issue #382: Buffered RPC flush test fails when a periodic flush lands between its two calls

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Problem

`buffered_rpc_flush_sdk_cli_mcp_and_recovery_preserve_occurrence_identity` (`crates/sparrow-server/tests/rpc.rs`) failed once in a full `make test` run under load and passes alone and on a rerun. The failing assertion is at `rpc.rs:7886`: the receipt for an artifact sent buffered came back `durability: Durable` with `buffered_through: None`, where the test expects `Buffered` with a watermark (session `session:02416704111b56ec304bdf54a83a5a0d`, sequence 687). The artifact id, payload id and record ids are equal, so the occurrence identity held; only the durability state differed, which is what a periodic buffered flush that ran between the first send and the replay produces.

## Evidence

- Failing run: head e5b6356 (PR #381), `.stage/362-agent/make-check-final-1-flaky.log` lines 1495-1511, `EXIT=2`; the unchanged rerun `make-check-final.log` ends `EXIT=0`.
- `make test` on main at bbac0b2 ended `EXIT=0` with 792 passed (`.stage/375-gate/test.log`).
- #381 changes no server or buffering code.

## Acceptance

- The test does not depend on a buffered entry staying unflushed for a wall-clock interval: the flush is driven or held by the test (the store's `now` argument or an explicit flush control), or the assertion accepts either state where both are correct and still checks the occurrence identity.
- The test passes 50 consecutive runs under `cargo test` with the rest of the `rpc` binary running in parallel.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
