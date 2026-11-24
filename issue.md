# Issue #106: Raise the import slot count and let a progressing connection outlive the 60-second lease

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

- Row C-3 of `docs/open-items.md` (main 03052cf).
- #3 body, "Out of scope": "Raising the lease or the slot count; ingest speed (separate issue)." #3 is closed; no open issue names the lease or the slot count.
- Marked by: the #3 issue drafter, on agent authority. Patrick's 2026-11-07 ruling, relayed by lillith_coordination: "nothing is optional, deferred or a follow-up on your own authority".

## Current behavior

Verified on main at 03052cf.

- `crates/sparrow-server/src/transport.rs:41-42`: `CONNECTION_LEASE` is a fixed 60 seconds. `:76` closes every Unix and TCP connection after it, even while an import is making progress.
- `crates/sparrow-core/src/storage/import.rs:16`: `MAX_IMPORTS = 16` journals for the whole store, shared by the CLI, the consume worker and anonymous TCP producers. `docs/interfaces.md:175` documents the 16 slots.
- `crates/sparrow-core/src/storage.rs:76`: an abandoned journal holds its slot for the 48-hour `RETRY_WINDOW`.
- The #3 body records the effect: on 2026-10-27, 16 held slots made 1,068 later calls fail with `RESOURCE_LIMIT`.

## Work

1. Replace the fixed connection lease with an idle deadline that a connection extends by making progress (bytes received or an RPC completed), with a hard upper bound documented in `docs/interfaces.md`.
2. Raise the import slot count to a value sized from a measurement of journal memory and disk cost, and reserve slots so anonymous TCP producers cannot hold every slot that the private socket and the consume worker need.
3. Advertise both values in `Discovery` and `status`, and update `docs/interfaces.md:175`.

## Done when

- A new test, `seventeen_concurrent_imports_are_admitted`, opens 17 import journals on one store and all 17 succeed. Today the 17th fails with "16 import journals are occupied".
- A new test, `an_import_making_progress_outlives_sixty_seconds`, keeps one connection sending chunks for longer than 60 seconds (with a test clock or a shortened lease constant) and completes the import on that connection. Today the lease closes it.
- A test with every anonymous slot held still admits an import on the private socket.


## Comments (1)

### paudley — 2026-11-24T12:53:35Z

Track the progressing-import connection lease with #91 and share its
idle-deadline implementation, including the documented hard upper
bound and discovery/status acceptance. Keep the import-specific
scenario mapped to this issue until it passes.

Retain the journal work here. The store still caps active journals
at 16. Waiting for a journal to free does not admit a seventeenth
active import or reserve progress for private imports. Measure the
cost of retained journals, size and advertise the new capacity, and
verify 17 simultaneous journals plus private admission while the
anonymous journal share is occupied. #98 retains local status
access under connection saturation.

[Current journal admission](http://lillith:7555/fleet/sparrow/src/commit/80bbc6c/crates/sparrow-core/src/storage/import.rs#L1411)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
