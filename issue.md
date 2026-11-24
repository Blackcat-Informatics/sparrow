# Issue #51: Remove or prevent orphan owner-to-graph rows left by a crash and downgrade

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

- Row B-3 of `docs/open-items.md`.
- Review finding, PR #35 (#13), `pr/35:reviews/review-1.md:125-131`, labelled OPTIONAL by the reviewer and not acted on. Marked by an agent.

## Current behavior

Verified at main 03052cf, except where marked.

- Store open runs `tracing_rdf::upgrade` and `event_fallback_rdf::upgrade` (`crates/sparrow-core/src/storage.rs:1422-1423`) before `rdf_index::upgrade_owner_graphs` (`:1424-1426`). Those earlier jobs write through `RdfWrite`, which writes `rdf:o:` rows, before the owner-graph fence moves the store format (`crates/sparrow-core/src/storage/rdf_index.rs:314-331`).
- If the first open crashes in that window, the store format is not yet fenced, so a schema 69 image can open the store. That image retires graphs without deleting their `rdf:o:` mirrors.
- The later owner-graph build (`owner_graph_step`, `rdf_index.rs:337-397`) only copies `rdf:g:` declarations into `rdf:o:`. It never removes `rdf:o:` rows that have no declaration.
- Answers stay correct, because rows still need an in-scope owner, but named-graph enumeration can list an empty graph for an owner whose term is still in scope.
- `docs/interfaces.md:545-558` (owner-to-graph index) has no note about this window.
- Unverified: whether the uncommitted #36 step 2 work, which moves these jobs online, changes the job order.

## Work

1. Close the window: either fence the owner-graph store format before any job that writes `rdf:o:` rows, or make the owner-graph build finish with a subset check that removes every `rdf:o:` row whose `rdf:g:` declaration is absent, in durable pages.
2. Document the chosen law in the owner-to-graph section of `docs/interfaces.md`.

## Done when

- A test builds a store with an `rdf:o:` row that has no matching `rdf:g:` declaration (the state the crash and downgrade leave), runs the owner-graph upgrade or open, and asserts that a `SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }` query in a scope that includes that owner does not list the orphan graph, and that no orphan `rdf:o:` row remains.
- That test fails at 03052cf, where the build leaves the orphan row in place.
- `docs/interfaces.md` states how orphan owner-to-graph rows are prevented or removed.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
