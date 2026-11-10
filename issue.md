# Issue #160: Stop write stalls: size RocksDB flush and compaction concurrency and memtables to the live write rate

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Problem

The live daemon spends a third of its life with writes stalled or stopped. RocksDB's own counters for PID 3967348 (schema 68, up since 17:44:24 on 2026-11-08):

- Cumulative write stall 02:27:10, 35.0 percent of the process's life. 10-minute intervals since 21:39 range from 15.2 to 75.1 percent.
- Since 21:39 the store LOG has 182 "Stalling writes because we have N level-0 files" lines (L0 20 to 24, rates as low as 66,926 B/s) and 56 "Stopping writes because we have 2 immutable memtables (waiting for flush)" lines. The stops block intake outright.
- Native counters: l0-file-count-limit-delays 358, memtable-limit-stops 9.
- Estimated pending compaction is 41 to 42.5 GB and growing, on a 37.6 GB store (LSM files L0 3, L4 26, L5 626, L6 360; compaction score 50.72).
- Backpressure reaches producers: since 21:39, 63 katamari-stored consume claims, 43 of them distinct, hit "consume publication deadline expired; retain the original journal and retry".

Evidence: `.stage/r82/stalls-since-2139.txt` (every stall line) and `.stage/r82/write-intervals-since-2139.txt` (per-interval writes, keys, ingest and stall percent).

## Cause (read in code)

`crates/sparrow-core/src/storage/table_options.rs`:

- Line 48, `set_max_background_jobs(2)`. RocksDB splits this into one flush slot and one compaction slot; the live LOG says "flush slots available 1, compaction slots available 1". When it logs "Increasing compaction threads because of estimated pending compaction bytes", it cannot rise above that cap.
- `max_subcompactions` is unset (1), so one single-threaded job compacts the whole store.
- `WRITE_BUFFER_BYTES` is 8 MiB (lines 28 and 65) and `max_write_buffer_number` is unset (2). At about 1 MB/s, a memtable fills in about 8 s; a flush took 2.8 s at 00:49. Two immutable memtables stop writes whenever the single flush slot lags.
- No L0 triggers are set (RocksDB defaults 4, 20 and 36), and there is no rate limiter.

The volume drives it, not the schema: about 9 M keys every 10 minutes, about 160 keys per record. The live daemon still runs the schema 68 image.

## Required change

Size the flush and compaction concurrency, the memtables and, only if measurement requires it, the L0 triggers, so that intake is never stopped and is stalled for a stated, measured fraction of the time.

- The numbers come from measurement. A scratch store driven at the live write rate (about 9 M keys per 10 minutes, on a store of comparable size) measures each option set for stall percent, hard-stop count and pending compaction bytes. The table of sets and results goes on this issue.
- sparrowd shares 32 cores with Katamari and the build system, so it must not take cores by default. Jobs are sized to the measured backlog, and compaction runs at lower scheduling priority than intake.
- If an option changes how new table files are written, `PROFILE` and `REWRITTEN` in table_options.rs follow their documented rule.

## Order and deployment

This change goes first in the D147 image, ahead of #158 and #156, all in the same one-stop switch. Nothing is changed on the running store, and nothing is restarted, before that switch.

## Acceptance

- Measurement table on this issue: each option set's stall percent, hard stops and pending compaction bytes at the live write rate, with the chosen set justified.
- Live, after the switch, over 24 hours:
  - zero hard stops;
  - stall percent at or below the stated target;
  - producers' consume-deadline retries fall to zero;
  - pending compaction bytes decline instead of growing.
- Rulings: P28 (Patrick, verbatim): "no backup may EVER stop lillith, lose data, or drop packets.  All backups MUST be online and perturb no operations." A stopped write path perturbs operations and puts lossy producers' packets at risk. This is also part of C18 criterion 4 (sizing), tracked in #159.

## Added acceptance: per-claim state through the CLI (coordination, 2026-11-09)

The zero-loss check after a stall needs the final state of every consume claim, not an inference from the journal. The status RPC reports only per-watch counts (`pending_claims`, `completed_claims`, `coverage_losses`). Add a read-only per-claim listing to the CLI and RPC, never by opening the store:

- Each claim reports its id, watch, state (pending, published complete, or abandoned), first-seen time, last-attempt time, attempt count and the reason for the last pending state.
- A listing filtered by time returns every claim that went pending in a window, so "of the N claims that went pending since T, how many are complete, how many are pending (with ids and ages) and whether any was abandoned" is one command.
- The 24-hour live acceptance above includes this listing: every claim that went pending is complete, none is abandoned, and the pending ones are named.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
