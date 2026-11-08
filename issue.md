# Issue #146: Keep status and ingest responsive while an online migration runs

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Problem

On 2026-11-07 the live daemon (image 3a64e08, 562,486 records) ran the `event-fallback-rdf` online migration at about 16.3 s per page. The `--measure-migration` copy had run 3.0 s per page. Live, RPCs were cut off at the 60 s lease and katamari consume publications missed their 45 s deadline. A read-only diagnosis (code references at 3a64e08) found:

1. **Status holds the writer lock while it recounts the store.** `status` holds the global writer lock, a `std::sync::Mutex<Writer>` (storage.rs:482, :1510), while it JSON-decodes every record (storage.rs:2456-2466). It also counts migration keys under the same lock (migration.rs:142-160, :199-205). The maintenance progress line repeats that recount every 64 pages (maintenance.rs:128). Migration pages, ingest and other RPCs wait behind it. d912922 (#142) caches only the totals of completed migrations, so a running migration and `status` still scan under the lock.
2. **Pages do not yield to waiting writers.** Pages run back to back with only a `yield_now` between them (maintenance.rs:173-192, :259), and the mutex is not fair.
3. **The measurement ran under different conditions.** The unit runs at `Nice=10`; the measurement scope sets no Nice (verify-backup.sh:329-330), so the copy ran at nice 0. The copy also had 167,239 records, 3.5 times smaller than live.

## Behavior

- **No full scans under the writer lock.** `status` and the migration report take no full-store scan under the writer lock. Live record counts and each migration's done and total come from counters maintained in the same batches as the writes and checkpoints, read without the writer lock. The progress journal line reports the page's own counters.
- **Waiting writers go first.** A waiting ingest, consume publication or RPC write takes the writer lock ahead of the next migration page. A page defers while writers are pending, and the deferral has a bounded share so that the migration still progresses.
- **The measurement matches the unit.** The measurement scope applies the unit's `Nice`, and `SCOPE_LIMITS` checks it.

## Done when

1. **Status cost.** A test with a running migration and a store of at least 20,000 records shows `status` reading a fixed number of keys, independent of store size, and holding the writer lock for none of them.
2. **Writer priority.** A test shows a consume publication committing within one page duration while a migration pages continuously.
3. **Measurement.** On a copy of the current store, with the #145 gate (live intake and traffic), the new image's MEASURE lines show:
   - `status` p95 under 5 s;
   - no RPC over 60 s;
   - no publication deadline expiry;
   - the recorded pages per second.

Refs #36


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
