# Issue #149: Answer status without a synchronous source-context rebuild on every open

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Problem

Every store open rebuilds the source-context cache synchronously before the daemon binds a socket. `Store::open` calls `source_context::rebuild` (`crates/sparrow-core/src/storage.rs:1493` on main; also in the live image 2aa56a9). That function:

1. deletes every key under the cache prefix through a prefix iterator, 512 keys per batch;
2. clears the job checkpoint and seeds the catalog upgrade job;
3. runs the job over the whole catalog.

On the live store (32 GB apparent, more than 1,058,852 records) the reopens after the two EMFILE crashes of 2026-11-08 (#148) took about 25 minutes. RocksDB recovered its 14 WALs in 3 s (17:44:25 to 17:44:27.9). After that, the RocksDB LOG shows flush after flush of `46080 entries, 46080 deletes`: 23 flushes, about 1.06M point tombstones, before any rebuild row is written. Each 512-key batch re-seeks from the start of the prefix and steps over every tombstone not yet compacted, so the clear is quadratic, and each flush adds an L0 table file, which feeds #148's open-file count. The daemon serves nothing during the rebuild: no status, no ingest, no queries.

This is inference from the code and the LOG. Timing each phase is part of this issue.

## Requirements (lillith_coordination, 2026-11-08; the design is open)

- The daemon binds and answers `status` without waiting for a full rebuild.
- The cache is rebuilt only when it is shown to be stale (for example a generation or catalog-version marker checked at open), never by blanket deletion.
- If a clear is ever needed, it must not leave a million point tombstones for later batches to re-seek over (use a range delete, or a new keyspace generation).
- Reads during a rebuild either wait for the cache or are served correctly without it, never wrongly from a partial cache.
- A test shows that reopening a store whose cache is intact does no rebuild.
- The PR reports the reopen time on a copy of a live-size store, timed per phase.

## Ordering

This lands before the #145 measurement, so the measurement covers the open path that will ship. #145's 600 s first-status gate stays as written.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
