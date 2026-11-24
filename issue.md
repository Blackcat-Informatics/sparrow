# Issue #99: Prove that pipelined calls on a refused CHAP session reach no service

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

- Row D-3 of `docs/open-items.md`.
- `S1/plan.md:44`, where S1 is `.stage/authenticate-the-public-tcp-listener/` in the main checkout: "Pipelined calls on returned capabilities (sessions) are not gated, by design."
- Marked by: the #1 plan, as a Risk. No ruling.

## Current behavior

Verified at main 03052cf:

- `authenticate` returns `Result(TcpSession)`, and `TcpSession.session` is the connection's full `Sparrow` capability (`crates/sparrow-client/schema/sparrow.capnp:725-730`). The gate sets it only on a successful CHAP exchange (`crates/sparrow-server/src/ingest_gate.rs:101-112`).
- A Cap'n Proto client can pipeline calls on `authenticate(...).result.ok.session` before the answer arrives. No test covers what such a call returns when the CHAP response is wrong. The existing test sends a wrong response and checks the refusal (`crates/sparrow-server/tests/rpc.rs:1636-1637`), but pipelines nothing on it.
- The expected outcome, that a pipelined call on the session of a refused `authenticate` fails with no service, is an inference from the union shape and is unverified.

## Work

1. Add the test below, and fix the gate if it shows a pipelined call reaching the service.
2. Replace "not gated, by design" with the proven rule in `docs/interfaces.md`, next to the CHAP description.

## Done when

- An RPC test, `pipelined_calls_on_a_refused_session_reach_no_service`, pipelines `status` and `recordsSearch` on the session of an `authenticate` with a wrong response, and both fail without returning store data. A second case does the same with a correct response, and both succeed. Today no test exercises either path, so the claim stays unproven.
- `docs/interfaces.md` states the pipelining rule and names the test.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
