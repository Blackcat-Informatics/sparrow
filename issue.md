# Issue #124: Report exact retained bytes per source in sources

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

- Row C-15 of `docs/open-items.md` (main 03052cf).
- #14 body, "Out of scope": "Per-source byte totals." No open issue names it.
- Marked by: the #14 issue drafter, on agent authority. Patrick's 2026-11-07 ruling, relayed by lillith_coordination: "nothing is optional, deferred or a follow-up on your own authority".

## Current behavior

Verified on main at 03052cf.

- `crates/sparrow-core/src/storage/source_read.rs:81-100`: `SourceSummary` reports complete artifacts, fragment parents, retained records and profile identities per source, but no byte counts.
- An operator therefore cannot tell from `sources` which source consumes the disk budget.

## Work

Depends on #14 (so `sources` answers on a store of any size).

1. Add exact per-source totals of retained payload bytes (and, if cheap to maintain, attributed physical bytes) to `SourceSummary`, the `sourcesList` wire result, the CLI and MCP.
2. Maintain the totals incrementally at ingest and retirement rather than by scanning, so the listing stays bounded.
3. Document the fields in `docs/interfaces.md`.

## Done when

- A new test, `sources_reports_exact_retained_bytes_per_source`, ingests known byte counts into two sources, retires some records, and asserts that `sources` reports each source's exact remaining payload bytes. Today `SourceSummary` has no byte field, so the test cannot pass.


## Comments (1)

### paudley — 2026-11-24T04:44:43Z

The public counterpart now reports exact logical retained payload bytes per
source through RPC, CLI and MCP. Its qualification covers shared content,
partial retirement and opening an older store. Use that implementation as the
integration reference for this Forgejo issue.

[GitHub delivery and verification](https://github.com/Blackcat-Informatics/sparrow/pull/9)


## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
