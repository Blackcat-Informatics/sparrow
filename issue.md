# Issue #158: Serve every query and status call completely during online migrations, never INDEX_NOT_READY

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Ruling

Patrick, 2026-11-08 (relayed by lillith_coordination, verbatim): "Note: answering "I'm not ready" is 100% failure for sparrow - might as well just be stopped.  That's "I'm dead" in a different coat."

## Problem

Several read paths refuse with `INDEX_NOT_READY` while an online migration or rebuild runs:
- #149 (ea1a4b2): record context, and artifact listing rows that read the source-context cache, refuse until the `source-context` migration completes.
- Existing: the `source-summary` gate (`source_summary::ready`, used by source and artifact listings), and any other migration gate that refuses reads.

Under the ruling, each of these is a failure. The D147 image does not ship with them.

## Requirement

During any online migration or rebuild, every query and status call gets a correct, complete answer. It is served from the existing index while the new one builds, or from a scan path over the authoritative catalog that yields the same rows. It is never "not ready" and never a partial answer.

- Inventory every refusal that depends on migration or rebuild progress (`INDEX_NOT_READY` and its kin) across sparrow-core and sparrow-server, and give each one a correct serving path.
- A fallback path may be slower; its latency is measured and reported. It must stay within the read governors (bounded pages and scan budgets), returning a resumable cursor rather than a partial result.
- Status never refuses.
- Docs: remove `INDEX_NOT_READY` from every read contract it no longer applies to (interfaces.md refusal table, MCP descriptions).

## Acceptance

- A test runs record context, source and artifact listings, records, and sparql queries throughout a `source-context` rebuild (and a `source-summary` backfill) on a store with intake flowing. Every answer is complete and identical to the same query after the rebuild, and there are zero not-ready replies.
- The answer latency during the rebuild is measured and reported in the PR, against the latency after it.

## Order

Image order: #148, #149, this issue, online #156, then coordination's go (D147).


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
