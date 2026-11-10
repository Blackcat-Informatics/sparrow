# Issue #176: Keep the source-summary owed completion current in the store against every write (REQUIRED, #158 item g)

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

**Plainly, first:** the wait for a fresh listing's FIRST ROW cannot be removed during the first pass after the schema 68 to 71 switch, under any design. Reason: no exact owed completion exists until the catalog has been read once, and at the switch every migration starts from zero on about 2.67 million records, so the whole catalog is owed. This happens once, on the day of the switch. Measured today (PR #169, `.stage/158/158-measure-2670.log`): a fresh sources listing needs 103 to 174 cursor pages (514.6 to 866.6 s) and artifacts 107 to 139 pages (534.7 to 696.6 s) before the first row, i.e. 9 to 14 minutes. Patrick called 9 to 14 minutes less than acceptable. **Expected wait after the change planned here (source-summary run first after store-counters, plus a build re-base): UNKNOWN until measured.** The probe records only all five migrations together (824.5 s at 2.67M), not source-summary's own duration; the measurement run (about 4.3 hours, about 3 GB of a 99%-full volume) waits for Patrick's answer on the disk (item 125) and for the switch. Option (a) (maintained rows) would not shorten this first wait either (design: `.stage/176/design.md`).

**Restated acceptance (2026-11-10, sparrow, comment 11617):** (1) `source-summary` is the first migration the worker runs after `store-counters` seeding, with a test; (2) a build started during the migration delivers rows no later than one 4,096-entry check after the migration completes, equal to the finished listing's rows, with a test; (3) after completion listings read rows directly (already tested); (4) per-migration timings recorded; the 2.67M run measuring source-summary's own duration waits as above and its numbers are posted here.

---

**ORIGINAL ISSUE TEXT (unchanged, including its original acceptance):**

## Source

#158 / PR #169, coordination 2026-11-09 20:3x: item g of the #158 agent's list, "the store-level owed completion kept current against every write", is filed as its own REQUIRED issue with the live-size numbers as its acceptance input, not left pending in a list. Origin: coordination ruling on the cursor-only page (2026-11-09 ~17:4x): "The store-level owed completion kept current against every write is not built in this branch; if the live-size numbers show a fresh listing needs more than a few minutes of cursor pages, file it REQUIRED with those numbers, and say so either way."

## Problem

While the online `source-summary` migration is pending, each `sources` capture and each live `artifacts` owed window works out from a snapshot what the migration still owes: a pass over every record the migration has not reached (at the live size about 2.67 million records, twice per listing: once per artifact, once for records whose artifact was deleted). No exact row exists until one full pass finishes, so a fresh listing returns cursor-only pages (zero rows plus the build's progress) until the pass ends. PR #169 serves this exactly and within the 30 s page deadline (`mcp.rs:229`), but each fresh listing repeats the pass.

## Work

Keep the owed completion current in the store, written in the same batch as every write (the way `store_counters::apply` and `source_summary::apply` keep their rows exact), so a listing during a migration reads the maintained completion instead of recomputing it. Design with the existing row owners; do not add a second source of truth.

## Done when

- The live-size measurement (2.67 million records, scratch store, schema 71 tree, under the unit's limits, 20 samples each during and after, recorded in PR #169) is the input: if a fresh listing during the migration needs more than a few minutes of cursor pages, this issue is built; if it needs less, the issue closes with the numbers and the reasoning on it.
- If built: a test that a listing during the migration returns its first rows within the page deadline without a full pass, and the maintained completion equals the recomputed one after any mix of ingest, expiry and migration pages.

## Waits on

PR #169 merging and its live-size table. Not a prerequisite of the D147 image unless the numbers say so. Nothing waits on Patrick.


## Comments (2)

### paudley — 2026-11-10T08:15:12Z

Input from PR #169's live-size run (`.stage/158/158-measure-2670.log`, EXIT=0, 2.67 million records): a fresh sources listing needed 103 to 174 cursor pages (514.6 to 866.6 s) before its first row, artifacts 107 to 139 pages (534.7 to 696.6 s). All five migrations together ran 6029 pages in 824.5 s, so source-summary alone ran at most 824.5 s; the probe does not record per-migration timings. Work added here: record each migration's own start, end and `done` counters in the probe output, so source-summary's duration at the live size is measured, not bounded. This is not a decision to close: a fresh listing needs 9 to 14 minutes of cursor pages, over "a few minutes", so this issue is built unless coordination rules otherwise.


### paudley — 2026-11-10T12:02:17Z

Design done (opus, read-only): `.stage/176/design.md` (473 lines). Decision (sparrow, 2026-11-10, inside this repository): build option (b), the migration run-order change (the worker runs `source-summary` directly after `store-counters`; `Migration::ALL` stays the status order; no writer-format fence and no wire change), together with c3, a re-base so a running listing build moves to a fresh snapshot once the migration overtakes it (capping a waiting listing at the migration's own end), and the per-migration timing the earlier comment asked for. Option (a), maintained owed-completion rows, is NOT built: at the switch from schema 68 the owed completion is the whole catalog, so it costs one full pass (514.6 to 866.6 s measured) before any exact row exists, needs a new writer ladder (`sparrow.store/352` to `/406`) that older binaries refuse, and adds reads inside the writer lock on every write, without making the first hour faster.

Restated acceptance (the literal one, "first rows within the page deadline without a full pass", is impossible for any design in the first pass-length after the switch because no exact owed completion exists until the catalog has been read once): (1) `source-summary` is the first migration the worker runs after `store-counters` seeding, with a test; (2) a listing build started during the migration delivers rows no later than one 4,096-entry check after the migration completes, and its rows equal the finished listing's, with a test; (3) after the migration completes, listings read rows directly (already tested); (4) the per-migration timings (start, end, pages, done) are recorded by the probe; the 2.67-million-record run that measures source-summary's own duration (about 4.3 hours last time) is run only with coordination's go-ahead and its numbers are posted here. Coordination: say if you instead require option (a).


## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
