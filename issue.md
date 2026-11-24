# Issue #91: Admit producer connections in their own class with a lease that renews while traffic flows

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

- Row C-8 of `docs/open-items.md`, its first part: "a producer connection pool" (#8 body, Out of scope: "A separate connection pool for producers").
- Row C-31: producer contract G7, "producer sessions inside `max_rpc_connections`" (`SD/coord/decodes-producer-contract.md:43`, where SD is `~/.local/state/sparrow-deliver/`).
- Marked by: agent issue draft (C-8) and agent contract review, "FEATURE", owner Patrick (C-31). No ruling defers either.
- Both rows are the same change, so they share this issue.

## Current behavior

Verified at main 03052cf:

- One semaphore of `max_rpc_connections` permits admits both Unix and public TCP RPC connections (`crates/sparrow-server/src/main.rs:134`, `:237`, `:247`). The default is 16 and the range is 1 to 256 (`crates/sparrow-server/src/config.rs:232-233`).
- `admit_connection` takes a permit with `try_acquire_owned` and drops the stream unanswered when none is free (`crates/sparrow-server/src/transport.rs:55-57`).
- Every admitted connection closes 60 seconds after admission, whatever its traffic (`transport.rs:42`, `:76-79`), so a steady producer reconnects every minute and the journal logs "RPC connection lease expired".
- The local ingest socket from #10 already has its own ceiling, `max_ingest_connections` (`config.rs:63-66`), so Unix producers on that socket are separate. TCP producers are not.

## Work

1. Give producer connections their own admission class, so producer sessions over TCP do not take `max_rpc_connections` permits from CLI and MCP clients. Patrick's 2026-10-27 ruling on connection slots, "No limits", means this must not add a new TCP connection ceiling.
2. Renew a producer connection's lease while its traffic flows, and close it after an idle period instead of a fixed 60 seconds from admission.
3. Document the class and the lease in `docs/interfaces.md` and `docs/deployment.md`.

## Done when

- An RPC test, `tcp_producer_is_admitted_while_rpc_connections_are_full`, holds every `max_rpc_connections` permit with Unix CLI connections and then completes an `eventsIngest` over TCP. Today the TCP stream is dropped unanswered at `transport.rs:55`.
- An RPC test, `active_producer_connection_outlives_the_admission_lease`, sends one `eventsIngest` every few seconds for longer than 60 seconds on one connection and sees no reconnect. Today the lease closes it at 60 seconds.


## Comments (1)

### paudley — 2026-11-24T12:53:35Z

Own the shared transport lease here. The current accept path reserves
a slot before accepting and lets peers wait in the listener backlog
when the ceiling is full. The lease still expires 60 seconds after
admission regardless of progress. The private ingest listener already
has its own admission setting.

Combine #106's progressing import with the progressing producer case
under the same idle-deadline implementation. Preserve #106's request
for a documented hard upper bound and advertised limits. Specify
progress, idle expiry, and the hard bound as separate conditions.

Keep the producer admission test here, including successful TCP
ingest while Unix RPC slots are occupied. #98 retains the inverse
saturation test, idle anonymous expiry, and status access over Unix.
Use one admission design for both tests and preserve the existing
constraint against a new TCP ceiling. #106 retains its separate
per-store journal capacity and private-import acceptance.

[Current transport lease](http://lillith:7555/fleet/sparrow/src/commit/80bbc6c/crates/sparrow-server/src/transport.rs#L62) ·
[Current admission contract](http://lillith:7555/fleet/sparrow/src/commit/80bbc6c/docs/deployment.md#L127)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
