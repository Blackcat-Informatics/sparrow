# Review 1: #146 keep status and ingest responsive while an online migration runs

## Scope

I reviewed the uncommitted worktree
`/home/paudley/Active/sparrow/.worktrees/146-keep-status-and-ingest-responsive-while`
against base `d912922`: the `git diff` of all 20 tracked files plus the untracked
`crates/sparrow-core/src/storage/store_counters.rs` and `writer_queue.rs`.

I read these myself: `issue.md` (verbatim), `plan.md` and its `## User rulings` (D109, plus the
2026-11-07 "nothing is optional, deferred or a follow-up on your own authority" ruling),
`implementation.md`, AGENTS.md, the rust-work skill, and the changed parts of
`docs/interfaces.md`, `docs/architecture.md` and `docs/deployment.md`. I also read these
consumers: `crates/sparrow-server/examples/turnover.rs`, `examples/load.rs`,
`docs/turnover-qualification.md`, `deploy/user-unit-upgrade.sh`, `deploy/libexec/sparrow-backup`,
`retention_metrics.rs`, `expiry.rs`, `retire.rs` and `pressure.rs`.

The stage has no `validation.md`. Execution evidence comes only from the checks table in
`implementation.md`.

## Evidence

My own checks were read-only. I ran no cargo command.

- `bash -n deploy/verify-backup.sh` returned 0. `shellcheck -x deploy/verify-backup.sh` returned 0.
- `prose-check` returned 0 on `docs/architecture.md` and `docs/deployment.md`. On
  `docs/interfaces.md` it returned 1 with 9 `UInt64`/`UInt32` findings, all on lines this diff
  did not touch (pre-existing).
- Code trace (file:line in the worktree) for each finding is given below.
- Implementer-reported checks, which I did not rerun: `cargo test --locked --release -p
  sparrow-core` (exit 0, storage 230/230, lib 36/36), `-p sparrow-server` (exit 0, 28/8/84),
  release clippy `-D warnings` (exit 0), `make census-check` (exit 0, 319 jobs). `make check` was
  not run. Done-when 3 (the live-copy measurement) was not run.

What I confirmed is sound:

- **Counter exactness and crash consistency.** `store_counters::apply`
  (`store_counters.rs:652`) runs inside `Writer::apply` (`durability.rs:144`) under the writer
  lock. It reads pre-batch state from the DB and appends its row changes to the same batch. I
  traced the bound-move arithmetic for forward moves, backward moves, keys inside the moved
  interval and seeding prefix transitions. It is exact: a key in the moved interval nets to its
  post-batch inclusion. Merges on counted keys are refused, and so are row writes from any other
  writer. No production `delete_range` touches a counted prefix. Open-time catalog upgrades run
  before `migration::fence`, and the fence is last in the same synchronized batch. Seeding resume
  is safe because the checkpoint and the counts commit together.
- **Writer format.** `/297` to `/351` continue from `/296` without overlap. The test checks
  that no older family contains them, so an older binary takes the `UNSUPPORTED` branch at open.
  `#142`'s `migration-total:` rows are range-deleted by the fence (`store_counters.rs:563`), and
  nothing reads them any more.
- **Writer queue.** `defer_page` checks `waiting` and waits on the condition variable while
  holding the state mutex. `Arrival::drop` decrements and notifies under the same mutex, so no
  wakeup can be lost. Spurious wakeups are handled by the loop, and nothing busy-waits.
  `Store::lock` is the only path to the writer for everything except migration pages
  (`lock_uncounted`), so ingest, `import_publish` (the consume publication path,
  `sparrow-server/src/consumption.rs:598`), RPC writes, expiry, pressure and flush all register.
- **Done-when 2.** The test drives `import_publish`, the real consume publication entry point.
- **Status no longer detecting a corrupt `record:` row.** This follows from the issue's "no
  full scans" requirement. The record listing and keys still refuse such a row with
  `STORAGE_FAILURE`, and the affected tests give the reason for their changes. I do not count
  this as a weakened guarantee.
- **`verify-backup.sh` Nice.** It reads `Nice` from the loaded user unit `sparrow.service`
  (overridable with `SPARROW_MEASURE_UNIT`), refuses with guidance when the unit is not loaded,
  applies it with `systemd-run --nice`, and checks it against `ps -o ni=` through `SCOPE_LIMITS`.
  That matches the issue. If the unit is absent the script fails with instructions, which is the
  repository's hard-failure rule.

## Findings

### R1 REQUIRED: status is O(store) for as long as the `store-counters` seeding migration runs, which is the first migration the live store will run

`store_counters::values` (`store_counters.rs:544-559`) takes the pending path until seeding
completes. It recounts all 10 counters from the snapshot (`recount`, `:509`): three full
`record:` scans, one of which JSON-decodes every record for `retained-records`, plus two scans
each of `artifact:`, `rdf:g:` and `provenance-rdf:record:`, and full `ref:` and
`projection-ref:` scans. The status RPC calls both `store.status(now)` and `store.migrations()`
(`sparrow-server/src/rpc.rs:2761-2765`), so every status reply during seeding does this twice.
That is more work than the pre-fix status, which decoded each record once. The work is now
outside the writer lock, so ingest is no longer blocked, but the status reply itself still costs
the size of the store. The new test asserts this path on purpose: "The pending path recounts
from a snapshot, so status stays exact while seeding" (`tests/storage.rs:19555`). The fixed-key
assertion only runs after seeding (`:19569-19576`).

Why this blocks:

- Done-when 1 requires "a running migration" with status "reading a fixed number of keys,
  independent of store size". `store-counters` is a running migration, `Migration::ALL[0]`. On
  the 562k-record live store and on the done-when 3 copy it is the migration running right
  after the switch, which is the moment the incident happened.
- The issue's Behavior says "Live record counts and each migration's done and total come from
  counters … read without the writer lock". During seeding they come from a scan instead.
- Done-when 3 (status p95 under 5 s, no RPC over 60 s) is measured on a copy of the current
  store. Its first minutes are this seeding window. Each status poll there does two whole-store
  passes at `Nice=10` with `CPUQuota=100%`.
- Each long recount also holds a RocksDB snapshot for its whole duration. Pressure maintenance
  skips eviction while `rocksdb.num-snapshots != 0` (`pressure.rs`, `maintain_pressure`), so
  frequent status polls during seeding can also hold off reclamation.

What is required: status and the migration report must read a fixed number of keys during
seeding too, and the done-when 1 test must cover a running `store-counters` migration on at
least 20,000 records. The rows are already exact through the seeding watermark, so one
fixed-cost design is this:

- Report the seeding migration's own done and watermark from the rows.
- Report the counter-derived fields as explicitly pending while seeding runs, for example a
  status flag or migration state that says the counts are counting, documented in
  `docs/interfaces.md`. This follows the repository's `INDEX_NOT_READY` precedent for refusing
  partial counts.
- Do not recount.

Whatever design is chosen, it is an in-scope decision for the implementer to record. It must
not leave an O(store) status.

### R2 REQUIRED: the `live_records` contract change was not authorized, breaks the turnover qualification, and is documented inconsistently

- **Not authorized.** At `d912922`, `docs/interfaces.md` stated "Status `live_records` keeps
  its deadline meaning", a contract `#142` kept on purpose. The diff replaces that with "record
  entries stored and not evicted … until expiry deletes its entry, one day after the deadline".
  No user ruling in `plan.md` authorizes changing a CLI/MCP status field's meaning.
- **Existing tests rewritten to the new meaning.** `streaming_status_counts_follow_receipt_retention_pin_release_and_reopen`
  now asserts `live == "5"` at `30 * DAY` after expiry, while `records(30 * DAY).len() == 3`
  (`tests/storage.rs` diff hunk at about `:4136`). The pin-release step no longer changes
  `live_records` until a later expiry pass. `text_relations/tests.rs` dropped its `live_records
  == 0` assertion.
- **Turnover qualification broken.** `turnover.rs:248-253` (documented in
  `docs/turnover-qualification.md:107-112`) checks every day that status `live_records` equals a
  deadline oracle (`day < evidence.day + 30`). Receipts expire at exactly `now + 30 days`
  (`turnover.rs:185`). Expiry retires the evidence at the deadline but deletes the entry only
  when `expires_at + DAY <= now` (`expiry.rs:299`). Retirement does not set `evicted`
  (`retire.rs:44-81`; only `eviction.rs:461` sets it). So on day 30, in `none` and `sparse`
  modes, status counts day-0 records the oracle excludes, and the qualifier fails with "live
  count differs from independent deadline/pin oracle". The example was neither changed nor run.
- **Two meanings of `live_records`.** Retention metrics still compute `live_records` with the
  deadline rule (`retention_metrics.rs:290`).
- **Docs disagree.** `docs/architecture.md` (new text) says "a record past its deadline counts
  until expiry retires it". `docs/interfaces.md` says it counts until expiry deletes the entry a
  day later, "although expiry retires its evidence at the deadline". The code matches
  interfaces.md, so architecture.md is wrong.

What is required: the counter needs a write it can follow, so make it drop when expiry retires
the record (or when the archive gate releases it), not a day later. For example, expiry could
write a persisted retirement marker in the entry it already rewrites in the retirement batch,
with the `Retained` filter excluding marked entries. The remaining deadline-to-pass lag would
then be documented in one consistent text. Otherwise, obtain a recorded ruling for the new
meaning. Either way, update `turnover.rs` and `docs/turnover-qualification.md` so the
qualification passes, and make `architecture.md` and `interfaces.md` agree.

### R3 REQUIRED: "a status reply never waits for a migration page or a write" is false for the pressure and table-rewrite state

`Observed` (`writer_queue.rs:104-131`) puts the pressure and recompression state behind an
`RwLock`. `maintain_pressure` holds `self.pressure.borrow_mut()` across the whole pass,
including `self.commit` and `self.evict_oldest(...)` (`pressure.rs:198` onward).
`recompress_step` holds `recompression.borrow_mut()` across `writer.recompress(...)`
(`recompression.rs:328`). The status RPC calls `pressure_status()` and `recompression_status()`
(`rpc.rs:2763-2764`), and these block on the read lock for the whole eviction pass or rewrite
step. `docs/interfaces.md` (new "Store counters" section) and the `Observed` doc comment ("a
writer never waits behind more than one short status copy") claim otherwise.

What is required: narrow those guards. For example, work on a local copy and publish it in a
short write-guard section, so status waits at most one short copy. Otherwise, correct the docs
to say status can wait for a running pressure pass or rewrite step. The narrowing is the
option consistent with done-when 3's p95.

### R4 REQUIRED (closure condition): done-when 3 is still owed

The plan defers the live-copy measurement until #145 lands. That is a real dependency and it is
disclosed. Under the 2026-11-07 ruling, though, this delivery must not close #146 or describe it
as done. The PR should reference the issue without closing it, and the measurement (status p95,
max RPC, publication-deadline expiries, pages per second) remains open acceptance on #146.

### O1 OPTIONAL: dead test-support helper

`sparrow_test_support::pending_store_counter_migration` (`sparrow-test-support/src/lib.rs`) has
no caller. `implementation.md` still cites it.

### O2 OPTIONAL: the fairness claim is not tested

The docs say the migration "keeps about half of the writer's time under a continuous stream of
writers". The test only shows `pages >= 2` across four sequential publications. Nothing shows
the test fails without the queue, and nothing exercises a continuous writer stream. Either
soften the claim or add a check.

### O3 OPTIONAL: timing margins under load

The priority test allows 1.5 s against a 1 s hold. Two synchronized commits (the page's and the
publication's) must fit in the remaining 0.5 s, which is tight at host load 90-150. The status
test's `!page.is_finished()` check does not prove the page held the writer at the moment status
ran.

### O4 OPTIONAL: deferral delays other maintenance duties

`defer_page` runs on the maintenance worker. Buffered flush, expiry and pressure therefore also
wait up to the previous page's hold during a deferral. This effect has not been measured.

### O5 OPTIONAL: stale contract sentence

`docs/interfaces.md:1253` still says "explicit operational `status` remains a catalog scan".

### O6 OPTIONAL: range deletes are invisible to the observer

`observe_batch` cannot see a `delete_range`, and `source_summary` shares this limit. No
production path issues one on a counted prefix today. A guard or a documented invariant would
stop a future range delete from silently desynchronizing the counters.

VERDICT: BLOCKED
