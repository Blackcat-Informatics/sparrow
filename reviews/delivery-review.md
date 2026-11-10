# Issue 160 delivery review

## Scope

- Issue: `.stage/stop-write-stalls-size-rocksdb-flush/issue.md`, plus the live Forgejo issue and
  its one comment (coordination, 2026-11-09T10:45:37Z). `issue.md` records "Comments (0)", so
  the stage capture does not include that comment.
- Plan and rulings: `plan.md` (P28, the build-system ruling, and the rule not to touch the live
  store). Validation: `validation.md`. Debt notes: `review-debt.md`.
- Diff: worktree against 3735c1e. The staged index equals `git diff 3735c1e` byte for byte
  (checked at 16:51). There are no commits yet. A commit (hook) appeared to be in progress
  (`.stage/160/commit.log`, 16:48, empty).
- Measurement: `/home/paudley/Active/sparrow/.stage/r82/160-scratch/runs/` (table.md, LOG, .out).
- This was a static review only. I ran no cargo, make or tests. I did not touch /SPARROW.
- Open state, not a finding: origin/main is now 23c3509 (#162), ahead of the 3735c1e base.
  #162 touches `watches.rs`, `watch_admin.rs`, `rpc.rs`, `tests/rpc.rs` and `interfaces.md`.
  The branch needs `stagectl sync` and a recheck of the watch failure branch before ghprsq.

## Evidence (read-only checks)

- Option set C goes through the real open path. `Store::open` (storage.rs:1457) calls
  `table_options::database(...)?`. That is the only production `DB::open*` call; all other
  callers are tests. `family()` sets `max_write_buffer_number(4)` and 32 MiB per family, and
  covers all three families. `database()` sets 6 jobs and 2 subcompactions, plus
  `Env::new()` + `lower_thread_pool_cpu_priority()` + `set_env`.
- I traced the RocksDB 0.25 / 11.8.1 source:
  - `rocksdb_create_default_env` wraps `Env::Default()`.
  - `rocksdb_env_lower_thread_pool_cpu_priority` lowers the LOW (compaction) pool to
    `CpuPriority::kLow` and leaves the HIGH (flush) pool unchanged.
  - `set_env` keeps the Env alive in `outlive`.
  - Subcompaction threads are spawned by the compaction thread, so they inherit its nice value.
  - Nothing lowers I/O priority. This matches the issue and set C.
- The shipped options equal the measured set C. `.stage/160/table_options.with-hook.rs` differs
  from the shipped file only by the throwaway env-var hook, and its `lowcpu` branch makes the
  same Env calls.
- PROFILE/REWRITTEN unchanged: justified. Memtable size, memtable count, job count and
  subcompactions change no block, filter or compression setting. The new constants' doc
  comment states this.
- Journal budget: I recomputed the worst case.
  - A new reservation is at most 8192-128-384-448 = 7232 bytes, history with `first_seen`
    included.
  - History growth is at most about 424 bytes: two 15-digit clocks, the reason field plus 320,
    and attempts digits.
  - Claimed observation: at most 237. Published artifact id: at most 126. Removal clock plus
    completed wrapper: at most 56.
  - Worst case is about 8075 bytes, within 8192, so a schema-71 journal cannot overflow.
- `fit_reason` pushes whole `char`s, so it is UTF-8 safe. Its budget arithmetic is tight
  (`min(remaining, 318) + 2` with the quotes counted once). The unit test pins the encoded
  bound with escape-heavy input.
- Old journals decode: `history` is absent, which gives `ClaimHistory::default()`. Every
  read-modify-write path decodes and re-encodes the full journal: claimed, publish_tail,
  publish_consumption, removed, retire, attempt_pending. None of them builds a journal without
  history.
- `consumption_claims`:
  - It refuses `since > until`, checks for a poisoned writer, then reads one
    `db.snapshot()` for all three prefixes.
  - Pending claims are always listed. Complete claims are listed by `last_pending_at`.
    Abandoned claims are listed by `last_pending_at` or `retired_at`.
  - It refuses the 2049th match with RESOURCE_LIMIT, checks keys against ids, and sorts by
    `(first_seen, import_id)`.
- `watches.rs:1306`: a STORAGE_FAILURE ends `run` the same way the existing `owner.drain()`
  branch does (watches.rs:1140). Other codes are printed and kept in `detector.failure`, so
  nothing is swallowed. The deadline text originates at watches.rs:1297 (45 s), the same path
  the issue's Katamari retries hit.
- Wire:
  - `watchClaims @54` and SCHEMA_VERSION 71 are in place.
  - `rpc.rs:1857` runs `check_store`, then `work()` (spawn_blocking with a request permit),
    then `watch_admin::encode_claims`.
  - The ingest gate lists it under `refuse`, and the anonymous TCP gate reuses that list.
  - `query_only` lists it under reads.
  - The CLI parses `--since`/`--until` pairs and rejects odd counts and unknown flags. USAGE
    and the usage error are both updated.
  - I checked the #158 branch: it takes no schema ordinal.
- Tests:
  - `a_journal_without_history_decodes_with_an_empty_history` covers the switch invariant
    and the reason bound.
  - `claim_listing_reports_each_claims_final_state_and_history` covers listing semantics
    across a reopen.
  - The rpc test extension drives the real CLI for pending and abandoned claims.
  - All three carry rationale comments, test behaviour rather than mirroring the code, and
    stay small.
- `prose-check` on all added doc lines and added code comments: EXIT=0. The earlier
  `.stage/160/prose.log` EXIT=1 hits only older `UInt64`/`UInt32` lines.
- Searches for branding or credits in added lines: none. No PurRDF or local helper duplicated:
  `fit_reason` has no existing equivalent, and the OptionalInstant matches follow the existing
  inline pattern.
- Measurement table vs docs: the five-set table in load-qualification.md matches `table.md`
  (stall %, stops, slowdowns, pending, status max). Pending "start" equals the first dump
  (5.34 GB).

## Findings

1. **REQUIRED: no runtime evidence for the delivered tree** (`validation.md:21`).
   - `validation.md` cites `test-focused2.log`, which does not exist.
   - The only test log, `.stage/160/test-focused.log`, is from 05:17. It predates three
     changes:
     - the sync (14:46);
     - the renumbering to schema 71 / `@54` in `sparrow.capnp`, `ingest_gate.rs`, `rpc.rs`
       and `interfaces.md` (14:47);
     - the switch of `table_options::database` to `Result` with the Env/CPU-priority change,
       in `table_options.rs` and `storage.rs` (15:19).
   - `clippy5.log` (EXIT=0, 16:48) proves compilation only. Nothing has yet opened a store
     with the shipped Env, or run the listing over the renumbered wire.
   - Fix: when the pre-commit `make check` finishes, record its command, exit status and scope
     in validation.md. Alternatively, record a fresh focused release run of the same tests
     with its EXIT line. Correct the log name either way.

2. **REQUIRED: the coordination comment's acceptance is unaddressed, and the listing cannot
   meet it** (`consumption.rs:1181`).
   - The issue comment of 2026-11-09T10:45:37Z (coordination) says: "after the change, the
     per-claim listing must show that every claim that went pending in these windows is
     complete". Those windows are 01:00-04:40, before the switch.
   - The stage capture missed the comment, and the plan's rulings do not record it.
   - The listing shows a complete claim only when `history.last_pending_at` is in the window.
     Every journal written before schema 71 has no history. So no pre-switch claim, including
     the roughly 15 Katamari claims named in that comment, can ever appear as complete.
   - Fix: also list complete claims with no recorded history (`first_seen.is_none()`) whose
     `removed_at` falls in the window, with null pending fields. Document this, together with
     the existing two-day retention limit, in "Per-claim state listing", and cover it in
     `claim_listing_reports_each_claims_final_state_and_history`.
   - Or get a coordination ruling that pre-switch windows are out of scope, and record it
     verbatim under `## User rulings`. Re-capture the comment into the stage either way.

3. **REQUIRED: measurement text overstates status and misattributes the stops**
   (`docs/load-qualification.md:474-484`).
   - "the probe's slowest answer in any run was 378 ms" is false. The x1-baseline run, which
     the same section cites, shows `STATUS ... max=1124.6ms over1s=1`
     (`runs/x1-baseline.out`).
   - The A and B stops did not happen under intake. Both "Stopping writes ... 36 level-0
     files" lines were logged by the opening thread right after `recovery_finished`:
     A and B LOG line 799, before `DB pointer` at line 832. WAL replay into 8 MiB memtables
     pushed level 0 to 36 at open. Only D's stop happened during the run (LOG line 1044,
     16:02:47).
   - So "More compaction jobs alone (B) do not help that compaction; two subcompactions split
     it (C)" credits subcompactions for C's missing stop, when larger memtables during
     recovery plausibly explain it. Only A's open-time stop is disclosed, not B's.
   - Fix: state the slowest probe across all runs (1125 ms, x1) or limit the claim to the
     five sets. Say that the A and B stops were logged at open after WAL recovery. Rest the
     choice of C on the in-run stall percent and D's in-run stop.
   - Then post the corrected table on issue 160. That is an explicit acceptance item, and the
     issue currently has only the coordination comment.

4. **OPTIONAL: `record_pending` does not reserve room for a later publication**
   (`consumption.rs:157`, `consumption.rs:240`).
   - The dynamic budget reserves `COMPLETION_ROOM` (128) for a claimed journal. An
     unpublished claim still grows by its artifact id (up to 126), the removal clock (about
     13) and the completed wrapper (about 43), about 182 in total.
   - Schema-71 journals are safe through the static reservation bound. A pre-71 journal
     reserved under the old 7680 limit could still record a pending reason, then publish, and
     end as a completed record over 8 KiB, which `consumption_admitted` refuses. That needs
     near-maximum, JSON-escaped labels, so it is implausible live.
   - Fix: add publication room when `published_id().is_none()`, and correct the
     `COMPLETION_ROOM` doc comment.

5. **OPTIONAL: listing cost and admission** (`consumption.rs:1132`, `rpc.rs:1871`).
   - The 2048 cap counts matches, not rows. Every completed receipt in the two-day retention
     is still decoded.
   - `work()` uses `try_acquire`, so the listing returns RESOURCE_LIMIT while blocked writers
     hold all request permits, for example during a stop. The text "read without the writer
     lock" could note this.

6. **OPTIONAL: resource notes for the docs** (`docs/architecture.md:154`).
   - The worst-case memtable footprint rises from 48 MiB to 384 MiB (3 families x 4 x
     32 MiB). `sparrow.service` sets MemoryHigh=2G.
   - The scratch runs ran the whole process at nice 10, while `sparrow.service` sets
     Nice=-20, so the live gap between intake and compaction is wider than measured (-20 vs
     19, against 10 vs 19). The live 24-hour acceptance covers this, but stating it would help.

7. **OPTIONAL: sourcing and placement.**
   - `load-qualification.md:450` gives live counts for 04:40-09:00 and 09:00-14:30 with no
     cited evidence file; the issue comment covers only 01:00-04:40.
   - The pointer to `.stage/160/` refers to gitignored local state.
   - In `interfaces.md`, the existing "The default scan budget..." paragraph now sits under the
     new heading. The listing order (by first-seen time, history-less claims first, then id)
     is not documented.

8. **OPTIONAL:** the unit test at `consumption.rs:1245` checks two invariants: legacy decode
   and the reason bound. Splitting it, or naming both in its title, would make a failure
   easier to read.

VERDICT: BLOCKED
