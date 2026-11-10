# Issue 160 delivery review at aa601ed (recheck)

## Scope

- HEAD confirmed: `aa601eded81f9668f08763581e1756472f24f8a9` on
  `paudley/160-stop-write-stalls-size-rocksdb-flush`, two commits over 3735c1e (035b826, aa601ed),
  pushed (`.stage/160/push.log` EXIT=0). Reviewed `git diff 3735c1e aa601ed` (20 files) and the
  review-fix delta `git diff 035b826 aa601ed` (consumption.rs, interfaces.md, load-qualification.md).
- Contract read first: `issue-with-comments.md` (body, "Added acceptance", the coordination comment
  of 2026-11-09T10:45:37Z), then `plan.md` `## User rulings` (P28, build-system ruling, live-store
  ruling), `validation.md`, `.stage/160/pr-body.md`, the posted issue comment
  (`.stage/160/issue-comment.md`, `issue-comment.log` -> issuecomment-10488, EXIT=0).
- Measurement: `/home/paudley/Active/sparrow/.stage/r82/160-scratch/runs/` (table.md, every `.LOG`,
  `.out`, `.err`, `.started`, `.du`, `s4-baseline.hang.txt`, `runall.log`) plus
  `160-scratch/base`, `base-debt`, `debt.out` and `.stage/r82/{stalls,write-intervals}-since-2139.txt`.
- Unit: `systemctl --user cat sparrow` (CPUQuota=100%, IOWeight=20, Nice=10, MemoryHigh=2G).
- Static review only: no cargo, make or tests run; /SPARROW, the live daemon and sockets untouched.
  Read-only checks: git diff/grep/merge-tree, LOG greps, `prose-check` (EXIT=0 on pr-body.md,
  load-qualification.md, architecture.md).
- Open state, not findings: origin/main is 23c3509 (#162), which touches watches.rs, rpc.rs,
  watch_admin.rs, tests/rpc.rs and interfaces.md. `git merge-tree` is clean and main has schema 70
  with no `@54` method, but the branch still needs `stagectl sync` and a recheck before ghprsq.
  A unit-limits measurement (`runs/q-A-baseline-unit-limits`, PID 2546519, started 23:48:43Z,
  after HEAD) is running. If it completes, PR-body point 3 ("did not apply CPUQuota") goes stale.

## Prior findings (delivery-review.md)

1. Runtime evidence: **resolved.** `test-focused2.log` (release; core lib `consumption` 3 tests,
   storage 6, rpc 4; each EXIT=0). `commit2.log` is the full pre-commit `make check` for aa601ed:
   fmt, release clippy with warnings denied, the release tests, and census "321 ledger jobs ... hold".
   It ends `EXIT=0` and runs the new tests by name (lines 49-51, 138, 1046).
2. Pre-71 completions: **resolved.** `listed_in` (consumption.rs:1210-1224) lists a complete claim
   with neither `first_seen` nor `last_pending_at` by `finished_at`. The rule is documented
   (interfaces.md:2816-2819) and has a unit test (consumption.rs:1317).
3. Stop attribution and probe max: **resolved** (see Evidence). New figure errors are in R3.
4-8 (optional): 7 and 8 were addressed (section placement; split unit test). 4 and 5 remain open
   as optional (O4).

## Evidence

- **Sizing reaches the real entry point.** `Store::open` (storage.rs:1457) is the only non-test
  `DB::open*`. Every other open is under `#[cfg(test)]`. `database()` (table_options.rs:60-78)
  sets 6 jobs, 2 subcompactions and a default Env with `lower_thread_pool_cpu_priority` (LOW pool
  only). An Env failure maps to STORAGE_FAILURE with context, so it is not swallowed. `family()`
  sets 32 MiB x 4 for all three `FAMILIES` (storage.rs:1421). PROFILE/REWRITTEN are unchanged and
  correctly so, because no table-file encoding changed.
- **Unit statements.** CPUQuota=100%, IOWeight=20 and Nice=10 match the unit. The PR body's
  "daemon Nice=10, compaction at 19" matches RocksDB `CpuPriority::kLow` (setpriority 19). The
  memtable ceiling of 48 MiB -> 384 MiB (3 x 2 x 8 vs 3 x 4 x 32) under MemoryHigh=2G is correct.
- **Replay-at-open stops (A, B).** "Stopping writes ... 36 level-0 files" is at LOG line 799 in
  both, on the opener thread (3055960 / 3304606, the thread that printed "RocksDB version"). It
  comes after `recovery_finished` (797) and before `DB pointer` (832). D's stop is in-run (line
  1044, 16:02:47, worker thread). C/D replay ended at 34 L0 files and E at 33, slowdown only.
- **Stall figures.** Cumulative stall: A 00:00:52.017 (6.5%), all in the 15:21:43-15:22:43 interval
  (the second minute after open). B 00:01:17.476 (9.2%). D 00:00:33.805 (4.6%), 0.932 s plus
  32.873 s. C 0.933 s and E 0.932 s (0.1%). The ingest max (57.9 s / 81.2 s / 42.6 s) is
  consistent with single blocks.
- **Probe figures.** A `probes=719 max=53.6ms over1s=0`; B 78.3; C 163.5; D 377.7; E 196.2;
  x1 `probes=900 max=1124.6ms over1s=1`. The probe (stall_measure.rs:140-144) makes exactly the
  store reads of `rpc.rs:2904-2913`.
- **Table.** It matches table.md. The d-A baseline has 1 stop line and 1 slowdown line, matching
  coordination point (2). The s4 hang description matches `s4-baseline.hang.txt` and
  `s4-baseline.err`: three STORAGE_PRESSURE panics at line 169, then the main thread's join panic
  at line 181 before the probe stopped. C over E is stated with its reason.
- **Coordination's three required statements** are all in pr-body.md (lines 26, 27, 38-45), with
  the live figures 57/19, 440/141 and 139/51.
- **Journal budget and decode.** Static admission is 8192-128-384-448 (consumption.rs:827-834).
  `record_pending` fits the encoded reason into the remaining room, capped at 320
  (consumption.rs:240-265). It mutates a copy and refuses with RESOURCE_LIMIT before commit. A
  missing `history` decodes as default (consumption.rs:414-417). `optional_clock` and the reason
  decode refuse malformed values with STORAGE_FAILURE.
- **Listing.** Window check, poison check, one `db.snapshot()`, the three prefixes (the retired
  prefix "consumption-retired:" cannot collide with "consumption:"), a size guard, a key/id check,
  refusal of the 2049th match, and a sort by (first_seen, id).
- **Wiring.** `watchClaims @54` and SCHEMA_VERSION 71. rpc.rs:1857 runs check_store, then `work()`
  (spawn_blocking), then `encode_claims`. ingest_gate.rs:182 is in `refuse`; query_only.rs:112 is
  in reads. The CLI parses `--since`/`--until` pairs and rejects odd and unknown flags; USAGE and
  the usage error are updated. The failure branch (watches.rs:1305-1322) ends `run` on
  STORAGE_FAILURE. Any other code is printed and kept in `detector.failure`, so nothing is
  swallowed. Production reserve callers pass `crate::now()?` (sparrow-server consumption.rs:380,
  handoff.rs:358).
- **Tests.** Each new test names its invariant and protects behaviour: legacy decode, the
  reason-bound cut, the pre-71 window rule, listing across a reopen with history fields and window
  exclusion, and the CLI end-to-end for pending -> abandoned. They are small and none mirrors the
  implementation.
- **Credits/branding.** None in the diff, the commit messages or the PR body.
- **Duplication.** Census passed in the hook. See O6.

## Findings

### REQUIRED

**R1. The live acceptance commands in the PR body do not bound the 24-hour window**
(`.stage/160/pr-body.md:40,43`).
- `grep -c 'Stopping writes' /SPARROW/store/LOG*` mixes in pre-switch logs. The D147 restart
  rotates the current LOG, full of stops, into `LOG.old.*`, so the count is non-zero for reasons
  unrelated to the change.
- With `keep_log_file_num` 10 x 8 MiB (table_options.rs:50-51), the oldest post-switch logs may
  already be deleted by hour 24. Then a zero count is a false pass of the P28 criterion.
- The pending-bytes command reads only the current `LOG`, so it cannot show a 24-hour trend.
- Fix: take hard stops from the cumulative native counters in the latest stats dump since open.
  These are the `Write Stall (count): ... l0-file-count-limit-stops, memtable-limit-stops,
  pending-compaction-bytes-stops, total-stops` lines that the issue itself cited. Confirm that the
  daemon has not restarted since the switch.
- For pending compaction bytes, read `Estimated pending compaction bytes` across `LOG` and
  `LOG.old.*` filtered to timestamps after the switch, or sample at intervals shorter than the log
  retention.

**R2. The pre-switch incident-window check is presented without its expiry or the query form it
needs** (`.stage/160/pr-body.md:44`).
- Coordination's acceptance requires the listing to show every claim that went pending in the
  2026-11-09 windows as complete. The PR body says only "Also run it over the pre-switch incident
  windows".
- Completed receipts expire `RETRY_WINDOW` (2 days, storage.rs:86) after completion
  (consumption.rs:1095). So this check is possible only if D147 lands by about 2026-11-11 for each
  window. After that, the completed pre-71 claims are gone.
- Pre-71 claims match by completion time (consumption.rs:1210-1224). A closed
  `--until <window end>` therefore misses a claim that went pending in the window and completed
  after it, for example a 04:11 deadline retry that completed at 04:50.
- Fix: state the deadline in the PR body and in the acceptance list. Prescribe
  `watch claims --since <window start>` with `--until` omitted. State that a "none pending, none
  abandoned" result holds regardless of expiry, but complete counts need the retention.
- This is also a scheduling input for coordination's D147 timing, so it belongs in the PR body.

**R3. Measurement-context figures contradict the evidence**
(`docs/load-qualification.md:453,461,466`; `.stage/160/pr-body.md:12`).
- "The base is 5.4 GB" is unsupported. `160-scratch/base` holds 3,044,013,259 bytes of SST (`du -sb`
  3,050,740,150), and its LOG family sums are about 2.8 GB. No 5.4 GB appears in the evidence.
- "5.3 GB of estimated pending compaction on a 9.7 GB store, about the live ratio" is not supported
  by its own figures. 5.34/9.7 is about 0.55; the live store is 41-42.5 GB pending on 37.6 GB, about
  1.1.
- The 9.7 GB (10,339,012,198 bytes) is 4.31 GB of SST plus 6.02 GB in 30 unreplayed WAL files.
  Every run replays that WAL at open, and the replay is the mechanism behind A's and B's stops.
- "with level 0 at 21 to 25 files on the slowdown lines" contradicts the source it cites. The
  01:00-04:40 comment lists 20 files 26 times (range 20-23), and `stalls-since-2139.txt` has 20-24
  (66 lines at 20).
- Fix:
  - Correct the base size, or cite where 5.4 GB comes from.
  - Drop "about the live ratio", or give both ratios.
  - State the debt store's SST/WAL split.
  - Give the L0 range the evidence shows ("20 to 24"), or cite a source for 25.

### OPTIONAL

- **O1.** "background threads at the lowest CPU priority" (`pr-body.md:5`) should say compaction
  threads. Only the LOW pool is lowered; flush (HIGH) stays at the process nice, as
  table_options.rs:57 states. Note that kLow is nice 19, not SCHED_IDLE.
- **O2.** "Through A's 52-second write block its 719 probes answered within 54 ms"
  (load-qualification.md:500, pr-body.md:30). The 719 are the whole run's probes. Reword to "all
  719 probes of run A, including those during its 52-second block". Also, "status does not wait on
  a stalled write path" holds for the store reads only. The RPC `status` goes through `work()`
  `try_acquire` (rpc.rs:364-379). When blocked ingests hold all `max_concurrent_requests` permits,
  it answers RESOURCE_LIMIT, so name that outcome in the live "status answers" check.
- **O3.** "A one-off four-producer stress run ... stopped after 72 slowdowns"
  (load-qualification.md:506). "Stopped" reads like a write stop. It hung after the driver's
  panic, as the PR body says.
- **O4.** Prior optional points 4 and 5 are still open:
  - `record_pending` reserves no room for the artifact id of an unpublished pre-71 claim
    (consumption.rs:247-252).
  - The listing decodes every retained completed receipt, and returns RESOURCE_LIMIT under a
    full request-permit set.
  - Related edge: the tightened static bound (7232 instead of 7680, consumption.rs:827) now also
    applies when a handoff binds an existing pre-71 journal (`previous` Some, handoff Some). A
    7232-7680-byte journal admitted earlier would then refuse. This is implausible live.
- **O5.** validation.md:24 and pr-body.md:51 list `commit2.log` without its exit status
  (`EXIT=0`). Live rate "about 15,000 keys and 1.35 MB" (load-qualification.md:459) follows the
  issue text. The write-intervals evidence averages about 10,000 keys and 1.22 MB per second.
- **O6.** `clock`/`clock_text` (agent/watch.rs:358-371) re-implement optional-instant rendering
  that other agent modules do inline through `super::instant` (ingest_status.rs:203,
  retention.rs:50, tail_wire.rs:172). Reusing `super::instant` would keep one formatter.

VERDICT: BLOCKED
