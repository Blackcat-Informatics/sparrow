# Issue #309: Idle column families pin the WAL: bound WAL size so the store stops growing with no new data

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

The live store's physical size grows about 420 MB an hour while live records rise by about 32 an hour (readings 09:25:27 to 09:58:09 on 2026-11-15). The growth is write-ahead-log files that are kept, not new data.

## Observed (read-only, `.stage/archive/churn/report.md` in the main checkout)

- The store directory at about 09:58 holds 702 `.sst` files (36.46 GB), 10 `.blob` files (0.33 GB, not growing), 53 `.log` WAL files (1.55 GB) and 18 other files (61 MB). That equals `physical_bytes`.
- WAL bytes by file modification hour: 03h 34 MB, 04h 95, 05h 68, 06h 103, 07h 235, 08h 529, 09h 481 (to 09:55). The oldest WAL file dates from 03:13.
- The default column family's SSTs grew only 28.98 GB to 29.20 GB in three hours.
- Compaction is not behind: estimated pending compaction bytes is 0 in every stats dump from 06:55 to 09:55 except two small values.
- The writes began to rise at about 07:05, when one producer's refusal stream rose. The stream eased at about 09:00; the WAL still grows.

## Inferred, not proven

`max_total_wal_size` is 0, and a WAL file is deleted only after every column family has flushed past it. This session the default family flushed 51 times, `rdf_reverse` once and `payload` never. An idle family that never flushes pins every WAL file written after its last flush.

## Acceptance

- Find which column family pins the oldest WAL, from the store's open options and its flush log.
- Bound the WAL: flush idle families when the oldest WAL passes a size or an age, or set a total WAL cap. Take it where the store is opened or in the maintenance loop. Choose the smaller change and say why.
- A test protects the bound on a store with one busy and one idle family.
- Record whether the change needs a daemon restart to take effect.
- Separately, explain the roughly 70,000 keys written per 7.6 KB refusal file and why 41 to 66 MB per ten minutes continue after the refusal volume fell.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
