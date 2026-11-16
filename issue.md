# Issue #314: sparrow-ship re-ships every source file after each reboot (ledger keyed on the btrfs device number)

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Problem

`sparrow-ship` keys its per-source ledgers on `device:inode:size:mtime_ns`. On btrfs the device number changes between
boots, so after every reboot every source file looks new and is shipped again. Re-shipped files become new occurrences
(duplicate records); their payload bytes deduplicate physically.

## Evidence

- `~/.local/state/sparrow/ship/lillith-corrections.ledger` has 195 entries: 65 each under devices 36, 85 and 78, one set per boot epoch.
  The source file `inner-core/corrections/2026-11-14.jsonl` is now `dev=78`.
- After the 2026-11-15 reboot the ship queued 64 files per source again, including files dated July, and fed 91 into the intake roots.

## Change

Key the ledger on an identity that survives a mount: inode, size and mtime with the file's path or content digest, not the device number.
Existing ledgers need a one-time reading that treats entries with the same inode, size and mtime as one.

## Acceptance

- A test shows that a changed device number alone does not re-ship a file, and a changed size or mtime still does.
- An existing three-epoch ledger is folded without re-shipping.

## Open question for the operator

A stable identity for catalog and tail declarations across mounts (filesystem UUID, subvolume id, inode) would replace the
device number everywhere. It changes stored declarations, so it needs the operator's ruling before it is written.


## Comments (1)

### paudley — 2026-11-16T00:20:17Z

Measured on 2026-11-15 after the reboot (read-only; script `.stage/step4/ledger-dryrun.py`, state in `~/.local/state/sparrow/ship`).

**Expected behaviour, confirmed.** The re-shipping is expected at the shipper's first run after each boot, because the ledger identity is `device:inode:size:mtime_ns` and btrfs renumbers the device at boot. It did not occur at the daemon start at 18:14:57: that run (18:14:47) fed 0 and the corrections ledger stayed at 195 lines (65 entries each for devices 36, 85 and 78, one set per boot).

**Size of the problem: `lillith-refusals` only.**
- The source directory holds 34,915 files (2,129,693,630 bytes). Its ledger has 34,250 entries under device 36, 192 under 85 and 1,536 under 78 (36 and 78 are the current and first epochs).
- The other four sources are already fully re-shipped under this boot's device (65, 4, 91 and 7 files); each is small.
- Dry run for refusals: 1,600 shipped under this device, 32,653 the same file (inode, size and mtime) under another device, 9 truly new.

**Arithmetic.**
- Scan side: `max_files_per_run = 64` per source, one run per 5 minutes, so 768 files per hour. 32,653 / 768 = 42.5 hours to re-queue the corpus (about 2.1 GB, about 61 KB per file).
- Feed side: `max_pending_bytes` is 16 MiB across all consume roots per run, so at most about 192 MiB per hour. The 2,132 files queued at 18:16 are about 328 MB (refusals 1,700 files, 85 MB). That clears in about 20 runs (under 2 hours), provided the daemon claims each batch within the 5-minute run interval (quiet interval 30 s). The queue is not the bottleneck; the scan is.
- Her fresh refusal files are named by hash, and the shipper scans in name order, so a new file is placed among the 32,653 unqueued old ones and can wait up to about 42 hours. Her inflow (about 100 to 150 per hour in STATUS) is far below 768, so the queue itself keeps up; the delay is the order.
- Cost: every old refusal file is ingested again as new occurrences (the payload bytes deduplicate, the occurrence and index rows do not), at about 47 MB per hour of source bytes for 42 hours.

**Fix proposed.**
1. Code (this issue): key the ledger on `inode:size:mtime_ns` plus the path (device dropped), and read existing entries by that key so the three epochs fold.
2. Immediate and data-only, if sequenced: append `78:`-keyed lines for the 32,653 foldable files to `lillith-refusals.ledger`, under the shipper's lock. It is append-only (undo: remove the appended lines) and changes no store data.

Acceptance as in the issue body, plus: a dry run after the change reports 0 foldable.


## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
