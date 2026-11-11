# Issue #183: Tail latency of context and records reads during migrations exceeds 30 s at live size (REQUIRED, from #158)

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

PR #169 (#158) live-size measurement at 2.67 million records, `.stage/158/158-measure-2670.log` (EXIT=0), all five migrations pending, 20 samples. Patrick's clarification relayed by coordination 2026-11-10: 30 seconds is the target for the NORMAL query, not a bound every outlier must meet. Decision (sparrow, recorded in PR #169): the build-and-cursor treatment for these two reads is not done in that PR; it is this issue.

## Numbers (p50 / p95 / max, ms)

- `context_rare` during the migrations: 26526.99 / 41902.86 / 83414.87; after: 0.46 / 0.69 / 2.91.
- `records` during the migrations: 27686.61 / 48304.20 / 48735.95; after the migrations: 2475.10 / 3324.31 / 8512.45 (the existing per-source scan, not a migration cost).

The median of both meets the 30 s target; the tail does not. Through the MCP adapter (30 s per tool call, `crates/sparrow-server/src/mcp.rs:229`) the slow tail of calls during the 9 to 14 minute migration window can time out. The measurement ran on a synthetic store of the live record count under the live unit's limits (CPUQuota=100%).

## Work

1. Give `context_rare`-shaped context reads and `records` during a migration the same build-and-cursor page contract as the listings in PR #169 (every page within 5 s; a page with no rows carries the cursor and progress; following the cursor resumes the same capture; an abandoned cursor releases at lease expiry), or bring the tail inside 30 s another way, with a test per behavior.
2. Separately, measure and reduce `records`' 2.5 s median after the migrations (the per-source scan).

## Done when

A re-run of the PR #169 probe at 2.67 million records shows p95 under 30 s for both reads during the migrations, with the log and its EXIT line; or the tail is accepted by coordination with the numbers recorded here.

## Waits on

PR #169 merged. Not a prerequisite of the D147 image unless coordination rules so. Nothing waits on Patrick.


## Comments (1)

### paudley — 2026-11-10T22:14:02Z

Live measurement 2026-11-10 16:13 CST on sparrowd-schema71-a77756a, all four migrations complete (`.stage/image/status-8.json`):
- `sparrow ... records` with no window: failed after 18.04 s with RESOURCE_LIMIT "record capture exhausted candidate scan_budget; narrow scope or raise the budget; no page published" (`.stage/image/first-row-page.json`).
- Same query with a one-hour received window: EXIT=0 in 24.24 s, 1,803 items (`.stage/image/first-row-window.json`). Meets 30 s for the normal case, with little margin.
An unscoped first page returns no rows, which is a speed failure under the mandate.


## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
