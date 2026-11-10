# Issue #173: watch claims --until misses claims whose latest pending attempt is outside the window (REQUIRED, split from #172)

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

Confirming review of PR #170 at 641c76b (`.stage/160/reviews/delivery-review-641c76b.md`, comment 10789 on #170), finding O1, split out of #172 by coordination ruling (2026-11-09): a wrong answer from a query is not optional.

## Defect

`watch claims` with `--until` checks only each claim's LATEST pending attempt against the window (`crates/sparrow-core/src/storage/consumption.rs:1210-1224`). A claim that pends at 04:11, pends again at 04:50 and is retired at 05:10 is missing from `watch claims --since 01:00 --until 04:40`, which then reports `abandoned: 0`. The question this listing exists to answer, "of the claims that went pending in this window, how many are complete, pending or abandoned", gets a wrong answer for any bounded window. Open-ended windows (`--since T` with no `--until`) are exact, and both live acceptance commands in PR #170 use them.

Four short descriptions overstate the rule: `crates/sparrow-client/schema/sparrow.capnp:790-791`, `docs/interfaces.md:2310`, `crates/sparrow-core/src/storage/consumption.rs:1127-1131`, `crates/sparrow-server/src/agent/watch.rs:41-42`. The fuller rule at `docs/interfaces.md:2843-2845` is accurate.

## Work

1. Make a claim qualify when ANY of its pending attempts falls inside the window (or state exactly which rule applies and make the listing match it).
2. Correct the four descriptions in the same change.

## Done when

- A test reproduces the example above (pend 04:11, pend 04:50, retire 05:10, window 01:00 to 04:40) and returns the claim as completed in the window; the same test with the window ending 04:00 returns it as not in the window.
- The four descriptions state the implemented rule.

## Waits on

#170 merged (done, a83294d). Not a prerequisite of the D147 image unless coordination says so. Nothing waits on Patrick.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
