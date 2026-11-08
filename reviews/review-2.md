# Review 2: #146 recheck of the review 1 rework

## Scope

This recheck covers the reworked areas in the uncommitted worktree
`/home/paudley/Active/sparrow/.worktrees/146-keep-status-and-ingest-responsive-while`
against base `d912922`. Inputs:

- the "Review 1 rework" section of `implementation.md`;
- the plan.md Decisions for 2026-11-08 (R1 option A; `live_records` keeps its deadline meaning);
- the current diff of `store_counters.rs`, `migration.rs`, `storage.rs` (status and Record),
  `expiry.rs`, `writer_queue.rs`, `durability.rs` (`observe_batch`), `maintenance.rs`, the
  capnp schema, `sparrow-client` `SCHEMA_VERSION`, `agent/status.rs`, `agent/tool/catalog.rs`,
  `sparrow-server/src/recompression.rs` and `shutdown.rs`, and the changed tests and docs;
- the status consumers in `deploy/` and `deploy/libexec/sparrow-backup`.

Unchanged history from review 1 is not re-judged.

## Evidence

I ran no cargo command. Everything below comes from reading the source. The rework checks in
`implementation.md` are the implementer's and I did not rerun them:

- core: lib 37/37, storage 230/230;
- server: 28, 8 and 84 tests;
- client: 22 + 1;
- `turnover CONFIG none 16 16 4096` and `sparse`: exit 0 each;
- clippy and fmt: exit 0;
- census-check: exit 0, 320 jobs.

The turnover runs are reported only by exit status, with no saved report. Day 30 runs inside
the daily oracle check (`turnover.rs:248-253`, every day from 0 to 59), so an exit 0 over 60
days implies day 30 passed. I traced the logic and it agrees (see R2 below).

## Recheck of the review 1 findings

### R1: fixed-cost status while seeding. Fixed in core; consumer handling is incomplete (N1).

- **No runtime recount.** `store_counters::recount` is `#[cfg(feature = "test-support")]` and
  only the audit calls it (`store_counters.rs:511`, `:734`). The feature exists only through
  dev-dependencies.
  - `values` reads the 10 rows (`:538`).
  - `status_counted` reads status and the migration report from one snapshot
    (`storage.rs:2521-2537`). The RPC calls `status_report` once (`rpc.rs:2760`).
  - `migrate_step` reads rows only.
  - `unique_payload_chunks` reads the row.
  - The only range scans left are `apply`'s moved-interval counts, which are bounded by one
    page.
- **Fixed-key test while seeding.** `tests/storage.rs:19564-19603` asserts all of these during
  `store-counters` seeding: `counts` is pending, `live_records` is 16,384 (through the
  watermark), every total is `None`, the key count is the same after 5,000 more records and
  under 64, and the key count is identical after seeding. This meets done-when 1 on 20,000
  records with the seeding migration running.
- **Wire, CLI and MCP.**
  - Wire: `countsPending @54` and `totalPending @5`. `encode_migrations` sends `total = 0` with
    `totalPending = true`.
  - CLI and MCP: they render `"total": "counting"` and `"counts": "pending"`
    (`agent/status.rs:175`, `:366`). The MCP `status` description says those fields are not
    totals.
  - `MigrationStatus::complete` treats a counting total as no obstacle. That is right, because
    a migration with no watermark has finished paging.
  - Inside the CLI and MCP, a pending count is never shown as final. Outside them it is; see N1.
- **Schema 70.**
  - `SCHEMA_VERSION` is the single pin. `tools/install-bin.sh:21` derives the installed names
    from it, and tests use the constant.
  - The docs updated: `interfaces.md` (schema 70 flags) and `deployment.md:293` and `:534`.
    The remaining "schema 69" mentions are historical.
  - Compatibility with 69 clients: discovery is an exact match (`sparrow-client/src/receipt.rs:82`),
    and the daemon refuses mismatched requests (`rpc.rs` test at `:2170`). Every
    `sparrow-schema69-*` CLI, and any producer linked at 69, is therefore refused once the
    switch lands.
  - That is the established contract. `user-unit-upgrade.sh` swaps the CLI link.
  - I found no `sparrow-client` Cargo dependency in the katamari or `lillith_*` checkouts
    (depth-limited grep of `Cargo.toml`). Katamari's consume publications reach the daemon
    through consume watches, not through the client.

### R2: retired marker. Fixed.

- **Same batch, crash-consistent.** Expiry sets `record.retired = true` and puts the entry in
  the retirement batch it already writes (`expiry.rs:295-304`).
  - The marker is written only on the `expired && archived` branch.
  - It is written once: the condition is `retired || !record.retired`, so an already-marked
    entry is not rewritten on every pass.
  - `store_counters::apply` sees the put flip `Retained` inclusion in the same batch.
  - If deletion falls in the same batch (more than one day overdue), the delete wins in
    `Changes` (last write wins), and the counter still drops exactly once.
- **Exactness against the deadline oracle.** The `Retained` filter is `!evicted && !retired`
  (`store_counters.rs:464-476`). Expiry retires exactly the records where `!live_record` holds
  at `now` and that are archived. A protected record is live, so it is not retired, and both
  rules count it.
  - So after an expiry pass at the same instant, the count equals the deadline rule.
  - The documented residual windows are real and are the only gaps:
    - between a deadline or pin release and the next expiry pass;
    - while the archive gate holds an expired record.
  - A retired record cannot become live again, because pinning expired evidence is refused.
- **Turnover.** Every simulated day runs expiry passes until `!continuation` before ingesting
  and checking status (`turnover.rs:127-140`). On day 30 the day-0 records are therefore
  already marked, which matches the oracle. Sparse mode's pinned record stays counted, as the
  oracle expects.
- **Reverted expectations.** `streaming_status_counts_follow_…` is back to the base values 5,
  3, 3, 2 and 0. An expiry pass now runs before each assertion that depends on a deadline, which
  matches the documented residual. `text_relations/tests.rs` is back to base.

### R3: Observed views. Fixed for blocking. A poisoned writer is now invisible to status (N2).

- **Publication on every mutation.** The working state is a private `RefCell`. `&mut`
  access exists only through `borrow_mut()`, which returns `Publishing`. Its `Drop` publishes
  `view()`, including on early returns and the error paths (`pressure_maintain`'s
  `inspect_err` publishes the `Blocked` state).
  - No `Cell` or `RefCell` sits inside the durability, pressure or recompression `State`, so no
    mutation can bypass publication.
- **No lock that a pass holds.** Status reads `Published::read`, a short read lock on a copy
  (`durability.rs:339`, `pressure.rs:308`, `recompression.rs:320`). The write side is held
  only for the copy in `Publishing::drop`.
  - `maintain_pressure` and `recompress_step` still hold the `RefCell` across their passes, but
    status never touches the `RefCell`.
- **Stale views.** Views live only in memory and are rebuilt from durable state at open, so a
  crash leaves nothing stale behind.
  - During a pass, status sees the view from before the pass, which is the intended behavior.
  - After a panic, unwinding runs `Publishing::drop` and publishes the partial state. The
    profile has no `panic = "abort"`, so the panic poisons the writer mutex. See N2.

### O1 to O6

- O1, O2, O3, O5: fixed as described.
- **O4: no starvation or harmful reordering.**
  - `defer_migration_page` runs in `spawn_blocking`. `deferred_page` polls `&mut JoinHandle`,
    which is cancel-safe: a duty that wins the unbiased `select!` leaves the wait running, and
    the next poll resumes it.
  - Other duties keep running during a deferral, and they register as writers through `lock()`.
  - On stop, an outstanding deferral returns as soon as no writer waits.
  - Two minor gaps are listed as optional findings below:
    - a deferral that completed before a long duty is not repeated;
    - the first page after a retry tick runs without one.
- **O6: the guard is sound and refuses no legitimate batch in this codebase.**
  - `observe_batch` counts every `put_cf`, `delete_cf` and `merge_cf` callback in every column
    family, and compares the count with `WriteBatch::len()` (`durability.rs:373-416`).
  - Merges are counted, so a merge batch passes the length check. Merges on counted keys are
    still refused on purpose.
  - `put_log_data` does not increase the batch count, so it cannot trip the guard.
  - SingleDelete and wide-column puts would trip it, but no code in `sparrow-core` issues them.
  - The guard applies to every `Writer::apply` batch, because `source_context::update` calls
    `observe_batch` on each one. The open-time fences commit through `catalog_upgrade::commit`
    and are not affected.

## Findings

### N1 REQUIRED: deploy tooling and the closing line present pending counts as final

These consumers are outside the CLI and MCP, and none of them reads `counts`:

1. **`verify-backup.sh --image NEWER` fails on every backup taken before #146.**
   `deploy/RECOVERY.md:277` documents this mode. The query-only copy is fenced at open, but a
   query-only daemon never runs a page. So its status reports `counts: pending`, with
   `unique_payload_chunks` and `live_records` at 0. `crates/sparrow-server/tests/rpc.rs:4373-4377`
   asserts exactly that for a query-only copy.
   - `verify-backup.sh:472-480` compares that 0 with the full count in SOURCE and fails with
     "the copy holds 0 unique payload chunks, but SOURCE recorded N".
   - Round 1's recount returned the exact number here. This is a regression introduced by the
     R1 rework.
2. **The closing line.** `shutdown.rs:58` logs `sparrowd: closed store=… unique_payload_chunks=N`
   from the row with no pending marker.
   - `deploy/libexec/sparrow-backup` records it in SOURCE as the store's count.
   - `verify-backup.sh:542` and `:427` match it as the expected value.
   - A daemon stopped while seeding, or any query-only copy, logs a partial count as final.
3. **`live_records` printed without `counts`.**
   - `sparrow-backup:297` writes `live_records` from status into SOURCE.
   - `user-unit-upgrade.sh:245` prints `live_records=…` in the switch log at the first status
     answer, which is exactly when seeding is running.

Required:

- Each consumer must handle `counts: pending` explicitly. For example:
  - the closing line logs `unique_payload_chunks=pending` (or omits the count) while counts
    are pending;
  - `sparrow-backup` records the count as pending or unrecorded;
  - `verify-backup` reports the payload-chunks check as not verifiable (SKIP / NOT VERIFIED)
    when the copy's counts are pending, instead of failing;
  - `user-unit-upgrade.sh` prints `counts=` beside `live_records`.
- Update `RECOVERY.md` and `deployment.md` to match.

### N2 REQUIRED: a poisoned catalog writer no longer shows in status

At `d912922`, status took the writer lock, so a panic in any writer path turned the next
status into `STORAGE_FAILURE` ("catalog writer poisoned; reopen and inspect the store",
`storage.rs:1560`).

Now `status_report`, `durability_status`, `pressure_status` and `recompression_status` read
the snapshot and the published views, and recover poisoning (`Published::read`). A daemon whose
writer is poisoned refuses every write but reports a clean status: no durability failure and
pressure `ready`. The switch gate and verify-backup treat that status as healthy.

That turns a broken invariant into a silent failure (AGENTS.md: "Missing required dependencies
and broken invariants must fail with actionable errors").

Required:

- Status (and the three observed-status calls) refuses with `STORAGE_FAILURE` when
  `self.inner.is_poisoned()`. That check does not wait for the lock.
- Add a test that poisons the writer and asserts status refuses.

### R4 carried (closure condition, unchanged)

Done-when 3 is still owed until #145 lands. The PR must reference #146 without closing it.

### O7 OPTIONAL: the docs overstate how long an old retired entry counts

The new `docs/interfaces.md` text says "An entry expiry retired before this marker existed
counts until expiry deletes it, at most one day."

The code marks such an entry at its next expiry visit: `retirement.record` returns false, but
`!record.retired` holds, so the entry is rewritten with the marker (`expiry.rs:301-304`). It
stops counting at the next pass, not at deletion.

### O8 OPTIONAL: stale doc comment

The `migrate_step` doc comment (`migration.rs:290-294`) still says the page first yields to
waiting writers. The deferral is now `defer_migration_page`, which the worker runs separately.

### O9 OPTIONAL: deferral gaps

- A deferral that finished before a long duty (a pressure pass, say) is not repeated before
  the page, so writers that arrived in between wait one page.
- The first page after a `migrate.tick()` retry runs without a deferral.

Both are bounded, but neither is documented.

### O10 OPTIONAL: check the switch example's FROM image

`docs/deployment.md:534` now shows `--from sparrowd-schema69-3a64e08`.
`docs/open-items.md` (A-17) says live is schema 68, and the issue says a rollback followed the
2026-11-07 run. I did not inspect the live unit (forbidden), so confirm which image the live
service runs and correct the example if needed. Separately, the 69-client refusal at the switch
could be stated next to that example.

VERDICT: BLOCKED
