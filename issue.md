# Issue #145: Measure online migrations with live intake and query traffic running

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Problem

The schema 69 switch gate measured the online backfill on a store copy with `sparrowd --no-intake`: no ingest, no watches, no RPC traffic. On the copy it ran 2,614 pages in 7,914 s, about 3.0 s per page, under evening load of about 100 to 176. Live on 2026-11-07, with intake, watches and RPC traffic, the same migration ran at more than 16 s per page. Status, records and sparql were cut off at 60 s, and katamari publications expired. The gate did not test the online design's premise that the store serves while it migrates (coordination finding D109).

## Behavior

`deploy/verify-backup.sh --measure-migration` gains a mode that runs the copy WITH intake replayed and query traffic running:

- **Live intake replayed:** the copy's watches read a recorded or replayed copy of the producers' input, and socket ingest is replayed at its recorded rate. Nothing is read from or written to the live intake.
- **Status and query traffic:** status, records and sparql run at a fixed cadence for the whole backfill.
- **MEASURE lines:**
  - pages per second, overall and per migration;
  - status, records and sparql latency (p50, p95, max) and the count of calls over 60 s;
  - publication deadline expiries;
  - the first status time.

## Done when

1. The mode runs end to end on the newest nightly copy and prints the MEASURE lines above, with an exit status that reflects the gate.
2. The gate's thresholds are written in `docs/deployment.md`: first status within 10 minutes, a pages-per-second floor, no RPC over 60 s and no publication deadline expiry.
3. The next image's switch, the one carrying #142's status fix, the rate fix and the migration control, is measured with this mode before it is scheduled.

Refs #36


## Comments (2)

### paudley — 2026-11-08T05:41:40Z

Rollback finding, 2026-11-07, about 23:33 CST: the schema 69 to 68 rollback restored the pre-upgrade backup. The 2aa56a9 daemon then exited at the end of each startup, about 4.7 minutes in (NRestarts reached 2), with State("unrecognized private claim entry; inspect and reconcile it without discarding its bytes") (2aa56a9 consumption.rs:271-279).

The schema 69 daemon had claimed katamari-stored-20261107T2230.part00006.jsonl at 23:21:43, 43 seconds before the stop. It moved the file into intake/katamari.stored/.sparrow-inflight/ as import:8fbb482a…, and the restored catalog had no journal for that claim. The file had never been published.

Reconciliation: stop the unit, move the entry back to the watch root under its original batch name (same inode, bytes unchanged), then start. The first status came 127 seconds later.

Required here:
- The rollback path in `deploy/user-unit-upgrade.sh --help` and `docs/deployment.md` must include this step: list each consume root's `.sparrow-inflight/` and move every entry whose claim is unknown to the restored catalog back to its root.
- The gate's rollback rehearsal on a copy must exercise it.

### paudley — 2026-11-08T05:52:38Z

Rollback finding 2 (2026-11-07, 23:4x CST): after the restore, the 2aa56a9 daemon reported claim `import:252d1ffe2ddb1cd78ae8d5da96d96f91e93a531b35c765ba0f8f8135728b7eee` (watch katamari-stored) as "unpublished claimed source is missing; restore or reconcile it, never republish a replacement". The claim is in the restored 22:34 catalog. Its file, katamari-stored-20261107T2230.part00007.jsonl (1,048,119 bytes), was gone from the root and from `.sparrow-inflight/`. Most likely the schema 69 daemon published it and removed it after the backup; that is not confirmed from its journal.

**The bytes are recovered.** The ship splits each katamari journal export greedily at 1 MiB or 50,000 lines (`ship.toml`). Re-splitting `katamari-journal/katamari-stored-20261107T2230.jsonl` reproduced every known part size of that batch, and 15 of 15 recomputed parts still in the spool were byte-identical. The recomputed part 7 is 1,048,119 bytes.

**The claim itself is unrecoverable by design.** Recovery requires the private file to match the claimed generation (2aa56a9 consumption.rs, `recover`: `require(&file, claimed)`). That generation includes the inode and ctime, which a recreated file cannot match.

**What was done:** the reconstructed bytes were re-shipped at 23:52 CST as a new source file, `katamari-stored-20261107T2230.part00007.reshipped.jsonl`. The old claim stays pending and logs about every 10 s until it is reconciled.

**Loss:** none of the bytes. The records arrive under a new claim and file name instead of the original.

**Required here:**
- The rollback runbook must handle claims that the newer daemon published after the backup.
- The gate's rollback rehearsal must cover them.
- Reconciling or retiring a pending claim with a missing source needs an admin path. Without one, it logs indefinitely.

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
