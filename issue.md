# Issue #180: Refuse overlapping watch roots and shipper sources at configuration load (R106 gaps; REQUIRED)

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

R106 audit (`.stage/r106-watches.md`, commit cf221fa): two missing guards, found by reading.

## Gaps

1. `sparrow-ship` (`deploy/libexec/sparrow-ship`) does not refuse a source directory that overlaps `intake_root` or `state_dir` from its configuration, so a misconfigured source could make the shipper scan its own output or ledger.
2. The daemon refuses a watch root only if it overlaps `data_dir`; it does not refuse a root that holds a file its own output is redirected to (log files), which is the Decodes failure shape.

## Fix

Refuse both at configuration load with an actionable error naming the paths, with a test each.

## Done when

Each overlap is refused with a test; documented in `docs/deployment.md`.

## Waits on

Nothing. Not a prerequisite of the D147 image. Nothing waits on Patrick.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
