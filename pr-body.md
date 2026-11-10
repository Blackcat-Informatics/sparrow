Closes #158.

Patrick's ruling: "answering "I'm not ready" is 100% failure for sparrow - might as well just be stopped." During any online migration, every query and status call now answers completely and exactly. The answer equals the one the finished migration gives for the same catalog. No source file contains `INDEX_NOT_READY`.

## What changes

- **Status while the store stalls.** The live failure: on 2026-11-08 at 04:41, `sparrow status` (image 2aa56a9, schema 68) failed after about 61 s with RPC_FAILURE "Peer disconnected" while ingest kept flushing. RPC lease expiries continue, the latest at 14:31:08.
  - Cause found: a listing held its read registry lock while it waited for the writer, and status locked every registry to count handles.
  - Fix: each registry now mirrors its handles into a small ledger, and status reads the ledgers (`catalog_lease::read_stats`).
  - With status already off the writer lock (#146, #157), a stalled writer no longer delays status.
  - Acceptance: `status_is_exact_while_counters_seed_and_reads_fixed_keys_without_the_writer_once_seeded` holds the writer with a migration page while an artifact listing and a source listing wait for it, and status must answer inside the hold.
  - Expiry now purges expired read handles itself; status used to do it as a side effect.
- **Status is exact while the store counters seed (R5).** `store_counters::exact_values` adds the keys seeding has not passed, counted from the same snapshot by the same law, in one pass per key prefix.
  - `counts` is always `exact`, and migration totals are always known. The wire fields stay.
  - The closing line's `unique_payload_chunks` is exact too. verify-backup still accepts `pending` from older images. The deployment and recovery docs are updated.
- **Context answers from a snapshot without the writer lock (R1, option a).** Scope, protection and membership are captured under the lock. The anchor and both walks read a snapshot taken with it.
  - The walk checks cancellation every 4,096 entries. CPU is uncapped.
  - The RPC goes through `read_work`, so it can be cancelled.
- **Owed source-summary rows come from a snapshot, outside the writer lock, charged to admission, exact (R2).**
  - `sources` runs the owed pass once per capture and charges its count changes to the capture.
  - A live `artifacts` listing reads owed index rows in windows of 32,768 after its position. It reads a window when it starts and whenever a continuation passes the window's end, always before the page takes the writer.
  - Each page charges the window (64 bytes plus the key per row, at most about 20 MiB) to its 32 MiB admission. A page stops before any row past the window, so no owed row is skipped.
  - A row the migration copied since is listed once. An artifact deleted since is skipped.
- **Scoped SPARQL while the owner index rebuilds (R5).** It reads the copied part of the index plus the declarations past its frontier, filtered to the scope's owners. That walk is not charged to the request budget, checks cancellation, and charges the kept graphs as memory.
  - The reads that serve facts the event-time fallback backfill owes are uncharged too: manifest, subject, forward and reverse term lookups. A scope that fits after the migrations fits during them.
- **A bug in that backfill path, found by the acceptance test.** Owed terms took the ordinal `rdf:next`, which holds the last allocated ordinal. The first owed term collided with the newest stored term, so a graph-count query named the newest artifact's graph `eventTimeFallback`. Owed ordinals now start at `rdf:next + 1`.

## Live-store fact (R2)

The live daemon runs image 2aa56a9, schema 68. Commit 2def69f is not its ancestor, so `source-summary` has never run on the live store. At the D147 switch, every schema 69 and 70 migration starts from zero on about 2.67M live records. The serving paths above carry that load:

- Owed passes run from snapshots without the writer lock.
- Listings never refuse.
- While `store-counters` seeds, status scans the unseeded keys. Seeding is the first migration and its pages only advance a checkpoint, so that window is short. On the live store, though, status may take on the order of 20 s per call until seeding finishes. This is extrapolated linearly from the probe below and was not measured on the live store.

## Latency, during vs after the migrations

Measured with the probe `measure_158` (temporary, not shipped): 300 artifacts of 1,000 records in 10 sources plus a rare source, about 300,000 records. All five migrations were pending, including store-counter seeding. 20 samples each, shared host, p50 / p95 ms.

LATENCY_TABLE

## Kept `EVIDENCE_INCOMPLETE` sites

These sites were `INDEX_NOT_READY` and are recoded to `EVIDENCE_INCOMPLETE`, which is not retryable. Each fires on a per-record state written once at ingest and never revised by a migration, so it does not depend on migration progress. Katamari 2ffc72a6 and brainstem aaf91271 no longer match the old code.

| Site | Why it is independent of migrations |
| --- | --- |
| `storage.rs` strict JSON field read | The record's JSON projection is `failed`, written once at ingest |
| `artifact_read.rs` explicit-scope listing with a receipt filter | A reclaimed parent's origin no longer exists; it refuses the same way after any migration |
| `diagnostic.rs` strict diagnostic filter | The captured diagnostic is `unknown` under an immutable profile |
| `rdf_view/query.rs` strict correlation | Unknown diagnostic interpretations, or an event-time profile that depends on a failed JSON projection |
| `rdf_view/scope.rs` strict diagnostic filter on a recordless artifact | Immutable `record_count == 0`, so no diagnostic record can exist |
| `rdf_view/scope.rs` strict RDF read | The record's RDF projection is `failed`, written at ingest |
| `record_fields.rs` (two sites) | The JSON projection is `failed`, or recorded decoding failures at ingest |
| `text_index.rs` strict text read | The text projection is `incomplete` or `failed`, written at ingest |
| `text_index.rs` strict event interval | The source-time profile depends on a failed JSON projection |

The full 45-row classification is in `.stage/158/` (158-classification.md).

## REQUIRED remainders

None. Not changed, and noted:
- A window recompute in a live artifact listing runs while holding the artifact handle registry, so other artifact listings and releases wait behind it. Status does not.
- The `sources` capture holds its registry through the owed pass.
- The owed fallback fact and term scans are linear in the selected records' owed facts.

## Tested scope

Each log ends with its own EXIT line, under /home/paudley/Active/sparrow/.stage/r82/.

EVIDENCE_TABLE

Independent reviews by delivery-reviewer:
- review-2.md: BLOCKED, F1 to F4 fixed.
- review-3.md: BLOCKED, R1 fixed and R2 evidence recorded.
- review-4.md: REVIEW4_VERDICT.

The reviews are in `.stage/158/`.
