# Issue #39: Check scoped SPARQL parity on a strict subset scope

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

- Row B-1 of `docs/open-items.md`.
- Review finding, PR #35 (#13), `pr/35:reviews/review-1.md:114-120`, labelled OPTIONAL by the reviewer. Neither the implementer nor the delivery session acted on it. Marked by an agent.

## Current behavior

Verified at main 03052cf.

- `scoped_sparql_answers_match_the_store_wide_plan` (`crates/sparrow-core/tests/storage.rs:18808-18868`) compares a store-wide query with two scoped scopes: `record_page_scope("graphs", 1000)` and a receipt window of 1 to 5 (`:18829-18833`, `:18855`).
- In `owner_graph_fixture`, every record has source `graphs` and the window covers every receipt, so both scopes contain the whole fixture. The assertion at `:18857` therefore compares equal memberships.
- Parity is never checked for a strict subset scope, where a shared or multi-owner graph must drop the rows of owners outside the scope. That path is the owner filter in `QuadIter`.

## Work

1. Add a case to the parity test, or a sibling test, whose scope selects a strict subset of the fixture: for example a one-record `Evidence` scope, or a source or window that excludes at least one owner of a shared graph.
2. Compare its answers with the store-wide query restricted to the same owners, for each layer combination and each query in the existing list.
3. Assert that at least one comparison drops rows that the store-wide plan returns, so the case cannot pass vacuously.

## Done when

- A named test, for example `scoped_sparql_answers_match_the_store_wide_plan_on_a_strict_subset`, exists in `crates/sparrow-core/tests/storage.rs`, and it passes under `make check`.
- The test asserts that the strict-subset scope returns fewer rows than the unscoped plan for at least one query, and equal rows to the store-wide plan filtered to the in-scope owners.
- Breaking the owner filter (for example letting `QuadIter` admit every owner) makes that test fail. No such test exists at 03052cf.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
