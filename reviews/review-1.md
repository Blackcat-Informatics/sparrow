# Delivery review 1: #145 Measure online migrations with live intake and query traffic running

## Scope

- Worktree `.worktrees/145-measure-online-migrations-with-live`, branch
  `paudley/145-measure-online-migrations-with-live`. The diff is uncommitted against `main`
  5813c33: 12 modified files (+1290/-84) plus the untracked `deploy/libexec/sparrow-measure-load`
  and `deploy/reconcile-inflight.sh`.
- Read directly: `issue.md` (body and both comments), `plan.md` (`## User rulings`),
  `implementation.md`, the full Rust diff (consumption.rs, tailing.rs, watches.rs,
  watch_admin.rs, config.rs, main.rs, watches/tests.rs), the two scripts and the helper in
  full, the verify-backup.sh, user-unit-upgrade.sh, docs/deployment.md and docs/interfaces.md
  diffs, and the new and changed rpc tests.
- Binding rulings checked: Patrick 2026-11-07 (nothing optional or deferred, every claim of
  done carries its number); Coordination D118 2026-11-08 (measure on
  `/SPARROW/backups/pre-upgrade-sparrowd-schema68-2aa56a9` with a newer schema 70 image via
  `--image`, read and print the copy's record count beside live's 860,209, print each
  threshold's margin, real run after merge).
- Not run here, as instructed: no cargo build or test, no daemon, no live path. The
  real-backup run (Done-when 1 and 3) is sequenced after merge by the D118 ruling and stays
  open until that run's numbers exist.

## Evidence

| Check | Result |
| --- | --- |
| `bash -n` on verify-backup.sh, reconcile-inflight.sh, user-unit-upgrade.sh | exit 0 each |
| `shellcheck -x` on the same three scripts | exit 0 |
| Python `ast.parse` of `deploy/libexec/sparrow-measure-load` | exit 0 |
| `prose-check docs/deployment.md docs/interfaces.md` | exit 1. Only the existing `UInt64`/`UInt32` findings in interfaces.md (lines 70 to 3303). None are in the added text (interfaces.md 528 to 544) or in deployment.md. |
| `ulimit -e; chrt -p $$` (this session) | RLIMIT_NICE 31, policy SCHED_IDLE. This confirms the reason for the `chrt --other 0` change. |
| `systemctl --user show sparrow.service -p CPUSchedulingPolicy -p Nice` (read-only) | `CPUSchedulingPolicy=0`, `Nice=10`; matches the new scope check. |
| Coordinator, item 5 | On main 5813c33 the flaky test passed 10 of 10 alone (`/tmp/s145-mainflake-*.log`). On the branch it failed once inside a multi-test run and passed 5 of 5 alone. |
| Implementer's runs (`implementation.md`, not repeated here) | clippy release `-D warnings` exit 0; fmt exit 0; focused rpc tests 18 of 19 (the one failure is the item 5 test); lib tests 6 passed; mutation "Private treated as Stored" makes the traffic test fail (exit 101). |

## Judgement by question

### 1. Safety boundary: can the copy touch live paths?

Through `deploy/verify-backup.sh`, no. I traced each path:

- `ClaimRoot::from_root` (consumption.rs:234-256). With `Stored`, the filter keeps every
  pending journal of the watch, and `!pending.iter().all(bound_here)` is the same refusal as
  the old `any(!bound_here)`, so the default path is unchanged. With `Private`, the filter
  keeps only claims bound to the private root's device and inode. Foreign claims therefore
  neither refuse the open nor join `known`.
- Every recovery and transfer path calls `scope()` (consumption.rs:331-345, used at 374, 453,
  581, 688) before it opens a file. `scope()` refuses any claim whose root device or inode
  differs from the held root. A Stored-bound claim on a copy reaches `recover()` from the work
  loop (watches.rs:1351). It then fails at `scope()` with "watch root was replaced" before any
  `openat` on the inflight directory, and the descriptor it holds is the private root. A
  claim bound to a live root cannot be recovered against a live root, because the copy never
  opens one.
- Rebinding (watches.rs:58-70, 714, 1146-1165) replaces each catalog definition's root with
  `DIR/basename` before `Detector::open`. It also rebinds each `watchPut` definition. A
  duplicate put returns early without opening anything (watches.rs:1010). `config.consume`,
  `syslog`, `rpc_tcp_address` and `archive` are refused (config.rs:287-311).
  `source_file::root` opens with `RESOLVE_NO_SYMLINKS`, so a symlinked DIR or root fails.
- Tail (tailing.rs:41-72): foreign active generations are skipped at open. `drain` reads only
  held descriptors, which `attach` opens under the private root. Later paths are covered in
  the item 4/#3 ruling below; none of them opens a live file.
- Script: `INTAKE`, `INGEST_DIR` and `WORK` are created under the scratch directory and passed
  through `guard`. The live ingest socket, the shipper's `intake_root`, its `state_dir` and
  every producer `dir` are added to the guarded set. On the first status answer
  `private_violations` fails the run unless every watch root is directly under `INTAKE` and
  the ingest socket is the copy's own. The shipper runs with a scratch configuration
  (`intake_root=INTAKE`, `state_dir=WORK/ship`) whose sources are scratch producer
  directories. Live producer files are only read by `cp --reflink=auto`. The prep copy and
  every rehearsal daemon open scratch copies. Rehearsal roots are under
  `$SCRATCH/rehearsal/intake`, and `reconcile-inflight.sh` runs with `--only-under`.
- The test fixture puts a claim journaled against the "live" root, with its file in the live
  `.sparrow-inflight/`. It asserts that the live root tree, the producer file, the live store
  and the live shipper state are untouched. That is the right invariant.

The daemon itself trusts its caller in the same way `--no-intake` does. See O2.

### 2. Replay honesty, cadence and MEASURE arithmetic

- Socket replay: artifacts received in the window whose source is not a watch source are
  re-sent over the copy's own ingest socket at their `received_at` offsets, looping. This is
  honest for socket and TCP producers. Syslog is skipped because the mode refuses it.
- Watch replay: honest only for `settled` sources. For other rules it can silently replay
  nothing, and no gate notices. This is R1.
- Traffic: three threads, each starting a call every 30 s from the first status answer. Ticks
  covered by a long call are skipped. A call in flight at the end finishes and counts, and a
  call past 600 s is killed and counted failed. This is correct and documented.
- Percentiles use the nearest rank `times[ceil(n*q/100)-1]`, which is correct. `over_60s`
  counts `>= 60`. `rpc_calls_over_limit` covers status, records, sparql and ingest.
- Pages come from `maintenance.rs:151/156`, which count pages per daemon run, so earlier work
  is never counted. The overall rate divides by the seconds from the script's start to the
  first poll that sees every migration complete. That includes the open before the first
  status, so the rate is conservative, and the docs say so. Per-migration seconds are accurate
  to one 15 s poll (documented). Migrations run sequentially (`Migration::ALL` order), so
  timing each one from the previous completion is valid. See O6 for the same-poll edge.
- Expiries: `grep -c 'consume publication deadline expired'` on the daemon log. This matches
  the worker's message (watches.rs:1375), which covers both finished-file and tail
  publications.
- First status: elapsed from the start to the first status answer, gated at 600 s.
- `live_records`: does not meet the D118 ruling. This is R2.

### 3. Gate thresholds

All of these are in docs/deployment.md, enforced by `sparrow-measure-load gates`, and printed
with value, threshold, margin and verdict: first status at most 600 s, at least 0.125 pages/s,
no call at or over 60 s, no failed call, every traffic kind answered, no deadline expiry. Exit
4 on a gate failure. Environment overrides can only tighten a gate (checked, and tested for
RPC). The 0.125 floor is justified from recorded numbers. It is half the failed live rate,
which was below 0.0625. It is about 38 percent of the 0.33 pages/s copy rate without traffic.
At the floor, 2,614 pages take 5.8 hours. The 5.8-hour figure only holds for 2,614 pages,
while a 68 to 70 backfill has its own page count. That does not change the floor, but a
projected duration line would make the margin readable (optional, not listed).

### 4. Rulings on the implementer's open questions

| # | Ruling |
| --- | --- |
| 1 | Not a correction. The D118 ruling sequences the real run after merge with an image built from main. Done-when 1 and 3 stay open until that run prints its numbers. |
| 2 | **Required** (R3). A missing requirement does fail loudly (`die` on LoadState, Nice, policy, chrt, systemd-run). The problem is that the repository's own CI cannot meet the requirement. |
| 3 | Optional (O3). No live-safety impact: no path opens a file under a foreign root. |
| 4 | Accepted. The copy has only the socket, and the docs say so. |
| 5 | Accepted and documented. |
| 6 | Accepted. Exit 3 and the pending-until-70 behavior are documented in the help and the docs. |
| 7 | Optional (O1). `NOT_FOUND` is the structured `code` field of the CLI's `Domain` error, which reaches stderr through `main`'s `Err` (bin/sparrow.rs). The CLI has no other structured error channel. Store identity is checked before any lookup. ingest-get's only `NOT_FOUND` is "unknown or expired ingestion" (ingest_status.rs:220, 278). A false "unknown" still needs the newer store to know the entry, or the run dies. Anchoring the match to `code: "NOT_FOUND"` would tighten it. |
| 8 | Accepted. A cross-version rehearsal would need the older image to open a copy with its watches, and images older than this change lack `--private-intake`, so they would open the live roots. The docs state "the measured image in every role". The exit 3 path for a FROM image older than schema 70 is therefore not rehearsed; that is acceptable under this constraint. |
| 9 | Not caused by the branch (item 5 below). |

### 5. The rpc test `configured_consumer_recovers_confirmed_journal_and_reports_late_write_without_retirement`

The branch cannot produce `live_records` 2 in this test. The test starts sparrowd in normal
mode, so `start_with(..., None)` gives `RootBinding::Stored`:

- `from_root` with Stored: the filter keeps every pending journal of the watch, the refusal is
  equivalent to the old one, and `known` is the same set.
- `Detector::open` passes Stored. `TailRoot` is not used (finished-files contract). The
  `watchPut` rebind arm runs only when `control.private` is `Some`.
- `recover`, `publish`, `remove`, `publication` and the work loop are untouched.
- A double publication of one claim is ruled out: the import ID comes from the reservation's
  request ID, and the confirmed claim's private file is removed after its one publication.

The only other source is the replacement `confirmed.jsonl` that the test writes into the root
before the daemon starts. With `quiet_ms` 10,000 it becomes reservable 10 s after the detector
first observes it (`stable_since`, watches.rs:665-669). `watch_until` requires
`completed_claims == 1`, so the replacement had not finished by then. It must have been claimed
and published after that, during the window that includes the root read and the spawn of the
`sparrow` CLI for `status`. Under concurrent test load, startup and recovery can take about
10 s, which fits. This is an inference and was not reproduced. It fits 10 of 10 alone-passes on
main and the single failure under concurrency. It is a pre-existing timing dependency in the
test. Report it separately; it is outside this issue.

### 6. `chrt --other 0` and `Policy=TS`

Accepted. systemd-run execs chrt, which execs the daemon, so `PID` is still the daemon.
`ps -o cls=` reports `TS` for SCHED_OTHER. The unit check requires
`CPUSchedulingPolicy=0`, which is what this host's unit reports. It also corrects the plain
`--measure-migration` mode, which otherwise inherits this session's SCHED_IDLE, and
docs/deployment.md documents it. Leaving SCHED_IDLE unprivileged needs
`RLIMIT_NICE >= 20 - nice`; this host has 31. Elsewhere chrt fails, and the run fails loudly
with `FAIL open`.

### 7. Tests, docs, scope

- Tests: the two end-to-end tests each protect a named production invariant: private replay
  that never touches live paths and runs both rehearsal findings, and an exit status that
  reflects each gate breach plus the usage-time refusals. The extended config-refusal test
  covers the new mode's refused keys. These are not implementation mirrors. No test covers an
  empty or partial replay (R1).
- Docs: thresholds, MEASURE lines, the runbook and the interfaces contract are present and
  match the code, except for R2's `live_records` sentence and O4.
- Scope: everything maps to the issue, its two comments and the plan. The scheduling change is
  a correctness fix that the measurement needs. No unauthorized expansion.

## Findings

### REQUIRED

**R1. An empty or partial intake replay passes the gate silently.** The mode exists because a
gate measured the backfill without intake (D109). As written, `--with-traffic` can exit 0
having replayed no watch intake at all:

- `prepare_producers` (sparrow-measure-load:75-112) and `prepare_socket` (115-151) accept zero
  rows. They only print the counts.
- Failed `cp` releases and failed `sparrow-ship` runs are only logged (lines 286-309) and
  counted. `ship_failures` is printed but no gate reads it.
- `sealed` sources never ship in the replay. sparrow-ship's `finished()`
  (sparrow-ship:117-119) requires `NAME.manifest.json`. The replay copies only files matching
  `pattern`, and with a pattern such as `*.jsonl` (ship.toml.example) the manifest is never
  copied or renamed to the cycled name.
- `daily` sources lose files: a dated file's mtime is its last append, often after
  `taken_utc`, so it falls outside the window, and the newest name is held back.
- The mapping from source `NAME` to a watch root whose basename is `NAME` is not checked. A
  mismatch ships into a directory no watch consumes.
- The gates (lines 352-364) cover the RPC kinds only.

Required: print per-source replay counts. Fail when a ship.toml source has nothing to replay or
cannot be replayed under its rule (for `sealed`, copy each manifest under its cycled name).
Fail when a shipped name has no private watch root, on any release or ship failure, and when
the copy's watches complete zero claims or socket ingest succeeds zero times while artifacts
were saved. Add a test showing that an empty replay does not exit 0.

**R2. `live_records` is not the copy's record count.** verify-backup.sh:825 prints
`live_records` from the final status, after hours of looped replay have added records, and
docs/deployment.md ("`live_records` is the copy's record count, read from its final status")
calls that the copy's count. The D118 ruling requires the copy's record count, read and
recorded beside live's 860,209, to show that the measured store is comparable in size. A
count inflated by an unbounded amount of replay misleads in exactly the direction the ruling
guards against. Counts are `pending` at the first status on a schema 68 copy, so print the
backup's own count separately from what the replay added. Two ways: count records received
before the run's start, or count the records the replay published and print both numbers.
Then correct the docs.

**R3. The new end-to-end tests make the repository gate fail on its CI host.** The two new rpc
tests run `verify-backup.sh --measure-migration`. It requires a user systemd manager in which
`sparrow.service` reports `LoadState=loaded` (verify-backup.sh:196-210), plus
`systemd-run --user --scope` and `chrt`. `.github/workflows/ci.yaml` runs `make check` on
`ubuntu-latest` (README.md:17 badge), which has neither a user manager nor that unit. Both
tests would fail there. The failure is loud, not a silent skip, but this change introduces it.
That last point is an inference from the workflow and the script: CI was not run. Before merge
the tests must pass in the repository's gate environment. Either let the test supply the unit
and scope inputs it controls, or document and supply the host requirement in the gate. Do not
skip silently.

### OPTIONAL

- **O1** (#7). Match `code: "NOT_FOUND"` instead of any `NOT_FOUND` text in `ingest_get`
  (reconcile-inflight.sh:103). The retire-claim capability check
  (`*unknown*|*usage*|*unimplemented*`, line 190) would turn a schema 70 "unknown watch"
  `NOT_FOUND` into exit 3. Detecting `retire-claim` in the restored CLI's usage text would be
  exact.
- **O2.** `PrivateIntake::new` (watches.rs:48-56) accepts any existing absolute directory.
  `--private-intake` run by hand with the live intake root's parent rebinds to the live roots,
  where Private binding treats live-bound claims as local and recovers them. The script never
  does this (scratch, guarded), and this is the same trust model as `--no-intake`. A
  one-sentence warning in docs/interfaces.md would help.
- **O3** (#3). `TailRoot::reconcile_missing` (tailing.rs:385-402) writes `read_gap`
  discontinuities for foreign active generations into the copy. `attach` (tailing.rs:148-202)
  could adopt a foreign generation on a reused (device, inode) on the same filesystem. Both
  affect the copy only. Filtering foreign generations there, or a unit test, would close the
  gap.
- **O4.** reconcile-inflight.sh `--help` ("one rename") and user-unit-upgrade.sh `--help` ("by
  one rename") describe a link then an unlink (reconcile-inflight.sh:141-142).
  docs/deployment.md has it right.
- **O5.** The script notices a dead load helper only at the end (`wait "$LOAD_PID"`). A
  `kill -0 "$LOAD_PID"` check each poll would fail hours earlier.
- **O6.** Two migrations that complete in the same poll print `seconds=0.000
  pages_per_second=0.0000` for the second one. Print it as sharing the poll instead.
- **O7.** Item 5's pre-existing timing dependency in
  `configured_consumer_recovers_confirmed_journal_and_reports_late_write_without_retirement`
  should be reported as its own issue.

VERDICT: BLOCKED
