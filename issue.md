# Issue #179: Tail watch self-triggers on the daemon's own IN_OPEN after a refused attach (R106 LOOP; REQUIRED)

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

R106 (coordination, Patrick 2026-11-09): "every log write triggered a scan, which triggered a log write" was found in Decodes; Sparrow's watches audited by reading this checkout only (main cf221fa). Report: `.stage/r106-watches.md` (opus, first line names the model). Found by reading, not reproduced.

## Defect

In a tail watch (`producer_contract = "active_files_retain"`) the daemon's own file opens trigger new watch events. `TailRoot::attach` (`crates/sparrow-server/src/tailing.rs:193`) opens a file it does not yet tail BEFORE the store can refuse it. When the store refuses (registry full, `tail.rs:603`; disk pressure, `tail.rs:635`), the open's own `IN_OPEN` event is in the event mask handled at `crates/sparrow-server/src/watches.rs:703-711` (`ReadFlags::OPEN | MODIFY | ATTRIB | CREATE | CLOSE_WRITE | MOVED_TO` calls `self.candidate(relative, true)`), which calls attach again, which opens the file again. This repeats up to 4096 events per pass, then the watch rescans, logs about two journal lines and starts over. Cost: CPU and journal lines; nothing is written into the watched directory (not the Decodes data-write loop), but it is a self-triggered wake loop. Exposure: no template or deployment doc in the checkout uses this watch type; whether a live watch does is UNKNOWN; check `producer_contract` in the live watch catalog (`sparrow SOCKET watch list`).

## Fix

Ignore events that are only `IN_OPEN` for tail watches (the daemon's own opens), or run the store's admission checks before opening the file, with a test that a refused attach does not re-arm itself (count the opens across one pass).

## Done when

A test shows one refused attach produces one open, not a repeat per event; the report's row is re-marked NO LOOP.

## Waits on

Nothing. Not a prerequisite of the D147 image. Nothing waits on Patrick.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
