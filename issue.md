# Issue #168: Hold systemd's own lines about Katamari's unit beside the node's lines (forwarder gap since 16:12:47; REQUIRED)

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

Coordination 2026-11-09 18:4x. The Katamari node was killed by the kernel OOM killer at 18:37:07 CST; the evidence is systemd's own lines about the unit, which Sparrow's forwarder does not hold. Evidence file: `.stage/forwarder/kill-1837.txt` (the six lines, with their command and EXIT=0).

## Gap

At 16:12:47 on 2026-11-09 the Katamari journal forwarder (`~/.local/libexec/sparrow/katamari-journal-spool`, unit `katamari-journal-spool.service`) was switched from the default journal to `journalctl --namespace=katamari --unit=katamari-stored.service` (the node moved to LogNamespace=katamari). The namespace journal holds only the node's own lines. systemd (PID 1) writes its lines about the unit to the DEFAULT journal: "Starting/Started/Stopping/Stopped Katamari store daemon", "Main process exited, code=...", "Failed with result ...", "The kernel OOM killer killed some processes in this unit" and "Consumed ... CPU time ... memory peak". Since 16:12:47 Sparrow holds none of these. The 18:36:36 stop request, the 18:37:07 OOM kill, the 18:37:10 exit by signal 9 and the "20G memory peak" line are in journald only. Before 16:12:47 the old forwarder captured them (about 740 such lines for 06:06 to 16:12, which is why the spool had more lines than `journalctl --namespace`).

## What backfills it

The default journal still holds these lines, so a one-time backfill from `journalctl --unit=katamari-stored.service --since '2026-11-09 06:06:47'` (manager lines only, for 16:12:47 onward) closes the gap, provided the default journal is not rotated first. The old forwarder already spooled 06:06:47 to 16:12:47.

## Work

1. A second forwarder unit that follows the default journal with `--unit=katamari-stored.service` into the same spool directory (or a sibling), under its OWN cursor file (never the namespace forwarder's `katamari-journal-ns.cursor`), tagged so its records are distinguishable from the node's own lines (for example a `source` field or distinct file prefix).
2. Handle the shipper's ledger, which keys files by device, inode, size and mtime: bucket files appended to after shipment are shipped again in full (observed on 2026-11-09 for the 12:00Z bucket). Use separate file names for the manager lines so existing buckets are not rewritten.
3. Repository units and the installer (`deploy/`), `docs/deployment.md` and the user-unit instructions updated together. The unit goes live only with this PR's deploy step, which coordination is told before it is applied.
4. A backfill of 16:12:47 onward from the default journal.

## Done when

- A test or recorded check shows that stopping a unit lands "Stopping ... / Stopped ..." lines in the spool (for example a throwaway user unit stopped under the forwarder), with its log and EXIT line.
- `.stage/forwarder/kill-1837.txt`'s six lines are present in the Sparrow store, queried by exact text from the spool or store.
- The deploy step and its evidence are recorded here.

## Waits on

Nothing. Not a prerequisite of the D147 image. Nothing waits on Patrick.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
