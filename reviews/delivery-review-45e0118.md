# Issue 160 delivery review at 45e0118 (recheck of aa601ed)

## Scope

- HEAD confirmed: `git rev-parse HEAD` is `45e0118fcacb2cd28e07b34153541a2fea95acd6` on
  `paudley/160-stop-write-stalls-size-rocksdb-flush`, pushed (`push2.log` EXIT=0). The branch holds
  035b826, aa601ed, the merge d6422c3 of origin/main 23c3509 (#162; `sync2.log` EXIT=0) and 45e0118.
- Read first: `issue-with-comments.md` (body, "Added acceptance", the 2026-11-09T10:45Z comment),
  `plan.md` `## User rulings`, `validation.md`, the prior review `delivery-review-aa601ed.md`,
  `.stage/160/pr-body.md`, `.stage/160/issue-comment2.md`. Rust guidance read: rust-work SKILL.md.
- Diffs read: `git diff origin/main 45e0118` (20 files, 981 insertions: the whole change against
  current main) and `git diff aa601ed 45e0118`. The latter is mostly the #162 merge. Since the merge
  was clean, the origin/main diff for watches.rs, watch_admin.rs, rpc.rs, ingest_gate.rs,
  query_only.rs, consumption.rs and tests/rpc.rs holds only this issue's additions. 45e0118 itself
  touches three files: agent/watch.rs, architecture.md, load-qualification.md.
- Measurement: every figure in both tables was recomputed from `runs/*.LOG` and `*.out`, and from
  `.scope`, `quota-runs.log`, `runall.log`, `s4-baseline.hang.txt`.
- Static review only: no cargo, make or tests run. `/SPARROW`, the live daemon and its sockets were
  not touched. The live counts (57/19, 440/141, 139/51) therefore could not be rechecked here; I
  took them from the issue comment and the coordination brief.
- `prose-check` on pr-body.md, issue-comment2.md, load-qualification.md and architecture.md: EXIT=0.

## Prior REQUIRED items (aa601ed)

- **R1 (acceptance commands do not bound the 24-hour window): resolved.** The PR body now tells the
  operator to confirm `ActiveEnterTimestamp,NRestarts` first, to read cumulative `total-stops` from
  the stats dump, and to sample pending bytes every few hours. It states why not to count
  `Stopping writes` over `LOG*`. I ran the proposed greps on `q-C-...LOG`. The `Write Stall (count)`
  lines carry `total-stops` per family and the value is cumulative. `tail -n 3` yields the three
  families of the last dump: 0, 1, 0, with the 1 on the default family after C's stop. The check
  would therefore catch a stop.
- **R2 (pre-switch window check): resolved.** The PR body prescribes `watch claims --since <window
  start>` with no `--until`, explains the pre-71 completion-time match, and states the two-day
  retention (about 2026-11-11).
- **R3 (context figures): resolved apart from one number.** Base size is now 3.0 GB of table files
  (3,044,013,259 B measured). "About the live ratio" is dropped. The debt store is stated as 4.3 GB
  of table files, 5.3 GB pending and 6.0 GB of unreplayed WAL, 10.3 GB on disk. The L0 range is
  still wrong; see F3.
- Optional O1, O2, O3 and O6 were taken up (compaction-thread wording, "all 719 probes of run A",
  `RESOURCE_LIMIT` caveat, hang wording, `super::instant` reuse in agent/watch.rs).

## Evidence

- **Both tables match the logs.**
  - Unconstrained, from `table.md` and the LOGs:

    | Set | Stall % | Stops | Slowdowns | Pending start / end GB | Status max ms |
    |---|---|---|---|---|---|
    | A | 6.5 | 1 | 1 | 5.34 / 0.27 | 54 |
    | B | 9.2 | 1 | 1 | 5.34 / 0.00 | 78 |
    | C | 0.1 | 0 | 3 | 5.34 / 0.00 | 164 |
    | D | 4.6 | 1 | 4 | 5.34 / 0.00 | 378 |
    | E | 0.1 | 0 | 3 | 5.34 / 0.00 | 196 |

  - Unit limits, recomputed from the LOGs and `.out` files:

    | Set | Stall % | Stops | Slowdowns | Pending start / end GB | Ingest p99 ms | Status max ms |
    |---|---|---|---|---|---|---|
    | A | 37.6 (cumulative 00:05:47.155) | 1 | 39 | 5.34 / 4.07 | 173,878.9 | 2,387.2 |
    | C | 7.0 (00:00:59.157) | 1 | 4 | 5.34 / 0.27 | 32,131.4 | 12,409.7 (2 probes over 10 s, 0 over 45 s) |

  - Keys/s: 11M keys / 923.9 s uptime for A, and 15M / 851.0 s for C.
- **Unit-limits runs.** Both `.scope` files show `CPUQuotaPerSecUSec=1s`, `IOWeight=20`,
  `MemoryHigh=2147483648`, `MemoryMax=4294967296`, nice 10, and the driver inside the sparrow-160
  scope cgroup. `quota-runs.log` has both runs at EXIT=0. The unit file has CPUQuota=100%,
  IOWeight=20, Nice=10, MemoryHigh=2G and MemoryMax=4G, and `deploy/verify-backup.sh:802-803` uses
  the same systemd-run line.
- **Stop attribution.**
  - A's stop (LOG line 799, 17:52:08) is on thread 2546519. That is the opener: it printed
    "RocksDB version" at 17:48:45, and `recovery_finished` is at line 797.
  - C's stop (line 1178, 18:08:49.17) is on worker thread 2814439, 86 s after "DB pointer"
    (18:07:22.8). That is the "about 90 seconds after open" claim. It followed a 959 ms flush.
- **C's stall intervals.** Interval stall is 58.225 s (96.7 percent) at LOG line 1469. Then
  there are exactly nine intervals at 0.0 percent: lines 1570, 1688, 1862, 2044, 2319, 2473, 2640,
  2838 and 2999. This matches the coordination wording. The nine zero intervals ran while 4.9 down
  to 0.27 GB was still pending.
- **A's tail.** Intervals of 84.7, 0.0, 68.3 and 96.7 percent stalled. Pending ended at 4.07 GB.
  It completed 51 ingests against 71 for C.
- **Coordination statements are all present.**
  - `Refs #160` is in the PR body, and "stays open" is stated.
  - Both tables, with the unconstrained one labelled.
  - The plain 7.0 against 37.6 percent statement, "one stop during the debt drain", and "does not
    meet 'no stops, at most 1 percent' until the debt has drained".
  - About 41 GB, "about eight times" (41/5.34 = 7.7), stops likely.
  - #167 owns CPUQuota and IOWeight, and the unit file is not changed. This is stated twice.
  - The baseline's stops began during replay at open.
  - The harness did not reproduce the live stall volume.
  - The live 24-hour acceptance list.
  - The s4-baseline hang cause, which matches `s4-baseline.hang.txt`.
  - Why C over E.
- **Merge with #162.**
  - Failure branch (watches.rs:1461-1484). #162's `detector.failure`, `failed_claim` and `eprintln`
    lines are intact. The new `consumption_attempt_pending` call follows them.
  - A STORAGE_FAILURE from that call ends `run` through `Error::Storage`. Any other code is logged
    and appended to `detector.failure`, so nothing is swallowed.
  - `retry`/`client = None` handling is unchanged after it.
  - `watch_claims` sits in `refuse` (ingest_gate.rs:182) and in query_only `reads` (:112).
  - rpc.rs:1858 runs `check_store`, then `work()` (spawn_blocking, `try_acquire`), then
    `encode_claims`.
  - The edited #162 test (`retire_claim_clears_only_...`) asserts 2 pending with reasons, then
    `--since` shows 2 abandoned and 0 pending. It passed alone in `test-focused3.log`, and in the
    full release workspace run in `commit3.log` (line 850; the core `claim_listing` test at 123).
  - `commit3.log` is the pre-commit `make check` on the final tree: fmt, release clippy with
    warnings denied, workspace release tests, census "321 ledger jobs ... hold". EXIT=0.
  - The schema 71 bump in sparrow.capnp and `SCHEMA_VERSION` agrees with interfaces.md.
- **45e0118.** The agent/watch.rs change removes the duplicate `clock` formatter and reuses
  `super::instant`, with the same output. The architecture.md sentence now matches the quota result.
- **Sizing facts re-checked.** 6 jobs means 1 flush slot and 5 compaction slots (RocksDB
  `max(1, jobs/4)` flush slots). One compaction runs on an idle store, which matches the issue's own
  LOG ("compaction slots available 1"). The statements in architecture.md follow from that.

## Findings

### REQUIRED

**F1. Evidence pointers for the unit-limits runs lead to files that are not there**
(`.stage/160/pr-body.md`, "Evidence (main checkout)" paragraph and "Tested scope"
paragraph; `docs/load-qualification.md:536-537`, the last sentence).
- The PR and the doc say the logs and table are under `.stage/160/` "in the main checkout".
  `/home/paudley/Active/sparrow/.stage/160/scratch-runs/` has 50 files and none is `q-*`. It has no
  `quota-runs.log`, no `.scope` file, and its `table.md` has 7 rows without A or C under the limits.
- `quota-runs.sh` is not in the main checkout's `.stage/160/` either; it exists only in the
  worktree's `.stage/160/`. The main checkout also lacks `commit3.log`, `test-focused3.log`,
  `clippy7.log`, `fmt5.log`, `sync2.log` and `push2.log`, which "Tested scope" places "(worktree and
  main checkout)".
- The unit-limits table is the headline evidence for the coordination rulings. The files exist only
  at `/home/paudley/Active/sparrow/.stage/r82/160-scratch/runs/q-*`. `table.md` there was not
  regenerated either, so Keys/s and p99 are not in any recorded table.
- Fix: copy `q-*`, `quota-runs.log` and `quota-runs.driver.log` into `.stage/160/scratch-runs/`,
  copy `quota-runs.sh` and the new logs into the main checkout's `.stage/160/`, and re-run
  `summarize.py` so `table.md` has the A and C unit-limits rows. Or name the true paths
  (`.stage/r82/160-scratch/runs/`) in the PR body and the doc. In `docs/load-qualification.md`,
  prefer to drop `.stage` paths, since the directory is gitignored.

**F2. validation.md contradicts the delivered evidence**
(`.stage/stop-write-stalls-size-rocksdb-flush/validation.md:22-32`).
- "Not verified" still says "The live unit's `CPUQuota=100%` and `IOWeight=20` were not applied in
  the harness". That is now false: the runs were done, and the PR body says so.
- "Checks" stops at `commit2.log` for aa601ed. It omits `sync2.log`, `fmt5.log`, `clippy7.log`,
  `test-focused3.log`, `commit3.log` (the final tree) and the two unit-limits runs. It also omits
  `quota-runs.log`, and it still names 035b826 and aa601ed as the last commits.
- Fix: update both sections with the logs above, each with its EXIT line, and move the quota item
  from "Not verified" to "Measurement". Keep "the live 37.6 GB store" as not verified.

**F3. The live L0 range "20 to 25" is not backed by the cited evidence**
(`docs/load-qualification.md:453`).
- The prior R3 asked for the range the evidence shows, or a source for 25. The sentence was
  changed from "21 to 25" to "20 to 25" and still has no source.
- `.stage/r82/stalls-since-2139.txt` tops out at 24 (20: 66, 21: 77, 22: 29, 23: 9, 24: 1). The
  01:00 to 04:40 comment tops out at 23.
- Fix: write "20 to 24", or cite the coordination count that has 25.

**F4. The operator runbook still names a schema 70 image for the D147 switch**
(`docs/deployment.md:314, 797, 814, 818-820`).
- This change bumps `SCHEMA_VERSION` to 71. `tools/install-bin.sh` names images from it, so the
  build installs as `sparrowd-schema71-REV` and `sparrow-schema71-REV`.
- deployment.md still says "a schema 70 build installs as `sparrowd-schema70-REV`", tells the
  operator to run `user-unit-upgrade.sh --from sparrowd-schema68-2aa56a9 --to sparrowd-schema70-REV`,
  says "straight to schema 70", and says to switch producers "to the schema 70 pair". Following
  that text after this lands names an image that does not exist.
- The schema 70 mentions that are about the retire capability (`deploy/reconcile-inflight.sh:50`,
  `deploy/user-unit-upgrade.sh:104-105`) are still correct.
- Fix: update the four deployment.md places to schema 71. The refusal sentence in :818-820 should
  say that the daemon refuses schema 70 clients too.

### OPTIONAL

- **O1.** The `Keys/s` figures are overprecise and carry replay time.
  - 11,906 and 17,626 divide RocksDB's rounded counts ("11M", "15M") by uptime. Uptime includes the
    opener's replay: 204 s for A (17:48:45 to 17:52:09) and 131 s for C. The figures are therefore
    good to about two digits, and A's is diluted more than C's.
  - The conclusion "A fell behind" still holds: 51 against 71 ingests, a 96.7 percent last
    interval, and 4.07 GB still pending.
  - Fix: round to "about 12,000 against about 17,600", or cite the ingest counts. "Against about
    18,000" is the unconstrained rate on the same uptime basis, so it is comparable only loosely.
- **O2.** "All of its 59 seconds of stall fall in that one stats interval"
  (`pr-body.md`, `load-qualification.md`). 58.2 s (line 1469) fall in it; 0.9 s were in the replay
  interval (line 1044). Write "58 of its 59 seconds". Optionally add the line numbers 1469 and
  1570..2999 that coordination used.
- **O3.** "The quota is the larger cause" is a harness result. The scope's one-core quota covers the
  whole driver: synthetic producers (the doc says they cost about twice the live per-key CPU), the
  probe and RocksDB. Say that the quota shared one core with synthetic producers, so the result
  may overstate the live effect. #167 is where that is sized.
- **O4.** "Until the debt has drained" is the binding wording, so keep it. The data are slightly
  finer: C was stall-free for nine intervals while 4.9 GB down to 0.27 GB was still pending. The
  architecture.md sentence "met it only once its compaction backlog had drained" is therefore a bit
  stronger than the evidence. Consider "met it in the intervals after the stop" if coordination
  agrees.
- **O5.** The PR body says "By the independent review's finding, the baseline's stops began during
  log replay". Internal review provenance does not belong in a publication body; state the fact
  with the LOG line numbers (799 for A and B).
- **O6.** The ruling set (ship C, `Refs`, both tables, the plain statement, the wording of the
  eight-times point) is not recorded verbatim under `## User rulings` in plan.md. Only the three
  original rulings are there. Append the coordination rulings with their times.
- **O7.** Prior optional O4 and O5 are unchanged (no artifact-id room in `record_pending` for an
  unpublished pre-71 claim; the 7232-byte static bound on a bound pre-71 journal). They remain
  implausible live and need no change here. The "about 15,000 keys" live rate at
  `load-qualification.md` follows the issue text; the write-intervals file averages about 10,000.

## Verdict

The code and the measurement are sound and match the coordination rulings. The merge with #162 is
clean in function, with wiring and tests intact. The four REQUIRED items are documentation and
record defects: evidence pointers, a stale validation record, one unsupported figure, and the
runbook's image name. They are cheap to fix and do not need a new measurement.

VERDICT: BLOCKED
