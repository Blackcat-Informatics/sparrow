# Issue #365: Show the archive worker's phase, watermark and last failure in status

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Milestone: Before PREFLIGHT. Must land before the archive worker runs on the live daemon.

## Problem

When the archive worker refuses a segment (a record's bytes do not match its link digest, a read-back mismatch, a NAS error), the refused file stays and every later pass stops at it until someone clears it. Today the cause is journaled only. `Status` carries `archiveConfigured`, `archiveHeld` and `archiveHalvings`; it carries no worker phase, watermark, last failure or its age. An operator reading status sees only that deletion is held, not why.

## Acceptance

- `Status` reports the archive worker's phase, its watermark, and the last failure with its code, the segment name and when it began, as additive fields after the current last ordinal.
- A test refuses a forged segment and shows the failure in status, and shows it clear once the refused file is removed and a pass commits.
- `docs/interfaces.md` and `docs/archive-format.md` describe the fields and the operator's way out.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
