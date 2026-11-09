# Delivery review 3: #145 Measure online migrations with live intake and query traffic running

## Scope

- Worktree `.worktrees/145-measure-online-migrations-with-live`. The diff range is
  `ea1a4b2..HEAD`. HEAD is merge 38c3744 of origin/main into branch commit 6bb5a7a. The diff has
  14 files, +2829/-95.
- Read directly: `issue.md` (body and both comments), `plan.md` (`## User rulings`, Behavior,
  Decisions), `validation.md`, `implementation.md`, `reviews/review-1.md` and `review-2.md`. I
  also read the full Rust diff (config.rs, consumption.rs, main.rs, tailing.rs, watch_admin.rs,
  watches.rs, watches/tests.rs) and the full rpc.rs test diff. The scripts read in full were
  `deploy/libexec/sparrow-measure-load` and `deploy/reconcile-inflight.sh`, plus the
  verify-backup.sh and user-unit-upgrade.sh diffs and the docs/deployment.md and
  docs/interfaces.md diffs. For the merge resolution I read
  `git diff 6bb5a7a 38c3744 -- main.rs watches.rs`.
- Binding rulings applied:
  - Patrick, 2026-11-07: nothing is optional or deferred on the implementer's authority.
  - Coordination D118: measure on the schema 68 pre-upgrade backup with a newer `--image`,
    print the backup's own record count and each gate's margin, and do the real run after merge.
  - Orchestrator review 1 and review 2 rulings: all R and O findings are in scope. For review
    2, the full rpc binary twice and the filtered set once.
  - Orchestrator: do not touch `.github`. No CI change is required here.
- Done-when 1 and 3 (the end-to-end run on a real backup, and measuring the next switch before
  it is scheduled) come after merge under D118. They are not findings of this review. They stay
  open, so the PR must say `Refs #145`, not close the issue.
- Not run, as instructed: cargo, any build or test, any daemon, and anything on the live
  service, store, sockets, intake roots, units or backups.

## Evidence

| Check | Result |
| --- | --- |
| `bash -n` on verify-backup.sh, reconcile-inflight.sh, user-unit-upgrade.sh | exit 0 each |
| `shellcheck -x` on the same three scripts | exit 0 |
| Python `ast.parse` of `deploy/libexec/sparrow-measure-load` | exit 0; mode 100755 in the index (also reconcile-inflight.sh) |
| `prose-check docs/deployment.md docs/interfaces.md` | exit 1. All 9 findings are the existing `UInt64`/`UInt32` joins. The same check on the ea1a4b2 versions also reports 9, so none are new. |
| `prose-check` on the `--help` output of reconcile-inflight.sh and user-unit-upgrade.sh | exit 0 |
| `logs/wip-commit.log` (hook on 6bb5a7a) | EXIT=0. The rpc binary passed 89 of 89, including the three traffic tests. The lib tests include the three `private_` unit tests. |
| `logs/sync-fmt.log`, `logs/sync-clippy.log` (merged tree) | fmt log empty. The clippy log ends `Finished release` with no warning or error lines and records no EXIT line. Both are compile-only. |
| `logs/r2-filtered1.log` (merged tree) | Still compiling at review time. No runtime result yet. |
| Merge resolution | main.rs:283-292: the startup line keeps `intake={}` from `intake_label` and adds upstream's `store.open_files()` after it. Upstream's `store_open_ms` line is kept (main.rs:155-167). watches.rs:381-383: `Owner::Tail(Box::new(TailRoot::open(..., binding)))` combines the bound open with upstream's boxed owner. Both checks, `grep 'query_only=false intake=private'` and `'query_only=false intake=false'`, still match as substrings. |

## Recheck of earlier findings

| Finding | State in current code |
| --- | --- |
| R1 | Resolved. `prepare_replay` (helper:98-185) replays from the copy's own catalog and prints per-source counts. It fails on an unrooted or empty shipper source, or an empty window. Release and ship failures are gated. The empty and unrooted test is rpc.rs `verify_backup_traffic_measurement_refuses_an_empty_or_unrooted_replay`. |
| R2 | Resolved. `backup_live_records` comes from the backup's image query-only (verify-backup.sh:550-580), else from the measured image's query-only prep answer when exact, else `pending`. `replay_added` comes from `count-records`. The final `live_records` is labeled as including the replay. |
| R3 | Resolved. `measure_env` (rpc.rs) puts `systemd-run` and `systemctl` doubles on PATH; production keeps the real tools. |
| O1 | Resolved. reconcile-inflight.sh:113 matches `AgentError { code: "NOT_FOUND",`. The retire capability comes from the bare `watch` usage (:164-166). |
| O2 | Resolved. `PrivateIntake::rebind` (watches.rs:68-96) refuses a DIR that is, holds or lies under a configured root. `Catalog::persist` writes the configured roots (watch_admin.rs:451-466), and `restore_root` reverts the record after a refused put. |
| O3 | Resolved. `foreign()` (tailing.rs:423-433) is applied at open, in `attach` and in `reconcile_missing`, under Private binding only. |
| O4 | Resolved. Both `--help` texts say a hard link and an unlink. |
| O5 | Resolved. A `kill -0 "$LOAD_PID"` check at every poll leads to `FAIL load`. |
| O6 | Resolved. `shared_poll=` and `shared_pages=` are printed. |
| O7 | Resolved in the test (rpc.rs: 600 s quiet and `get_candidates() == 1`). Review 2 accepted this, and the orchestrator ruled O1-O7 in scope. |
| R1b | Resolved. `watch_intake` (helper:437-468) adds one gate per watch: completed since the first status must be at least the items released before `claims_at` less the allowance. The allowance is quiet_ms, plus the 45 s `PUBLICATION_DEADLINE` (checked at watches.rs:144), plus the ship interval and the longest run for a shipped watch. `held_back` joins `replay_failures`. A `watch_claims_refused` gate matches the daemon's line at watches.rs:1356. The doc sentence is corrected. The breach case asserts `due=1 completed=0`. |
| R3b | Resolved. A `df` double reports 60 GB or `SPARROW_TEST_DF_AVAIL_BYTES`. The production floors are unchanged (no diff hunk touches them). The disk-floor test now refuses at 49,999,999,999 bytes against the default 50 GB floor. |
| O8 | Partly resolved. The behavior is right: there is no writable-copy fallback, and the source is `backup-image`, `prep-query-only` or `none`. One operator message still names the removed source. See **R-B**. |
| O9 | Resolved. `identity`, `identity_chain` and `aliased` (watches.rs:104-136) are used in `rebind`. The unit test `private_intake_refuses_configured_roots_aliased_by_identity` passed in the hook run. |
| O10 | Resolved. The `measure_env` doc comment carries the RLIMIT_NICE note. |
| O11 | Resolved. plan.md Behavior item 1 describes the catalog replay. |

## Fresh review

- **Default path:** with `RootBinding::Stored`, `ClaimRoot::from_root` keeps every pending
  journal of the watch, and `!pending.iter().all(bound_here)` is the old `any(!bound_here)`, so
  the fatal "configured watch root was replaced" refusal is unchanged. `TailRoot::open`,
  `attach` and `reconcile_missing` filter only under Private. `ConsumeWorker::start` keeps its
  signature and delegates with `None`. Query-only and no-intake still open no root. Every
  existing caller reaches Stored.
- **`--private-intake` refusals:** `arguments()` refuses a second mode, a missing value or a
  repeated `--config`. `check_private_intake` refuses query-only and `consume`, `syslog`,
  `rpc_tcp_address` and `archive`, and it allows the ingest socket. `PrivateIntake::new`
  requires an absolute existing directory, and every check runs before the store opens. The
  rpc config test covers the `consume` and `rpc_tcp_address` refusals.
- **Live-path safety:** the script guards the live ingest socket, the shipper's `intake_root`,
  its `state_dir` and every producer `dir`. The helper reads no producer directory; every byte
  comes from the prep copy's catalog. The shipper runs a scratch configuration whose
  `intake_root` is the private intake. On the first status answer `private_violations` fails the
  run unless every watch root is directly under INTAKE and the ingest socket is the copy's own.
  The rehearsal uses `$SCRATCH/rehearsal/intake` and scratch copies, and runs
  `reconcile-inflight.sh --private-intake`. The traffic test asserts that the live root tree,
  the producer file, the live store and the live shipper state are unchanged.
- **Gates:** each threshold that is meant to be fixed can only be tightened. The value,
  threshold, margin and verdict are printed. Exit 4 means a gate failed and exit 1 means a FAIL
  check. The thresholds the issue requires are in docs/deployment.md: first status 600 s, a
  0.125 pages/s floor, no call at or over 60 s, and no deadline expiry. The one false-pass path
  found is **R-A**.
- **Tests:** each added or changed test protects a named behavior: the private-mode config
  refusals, the default 50 GB disk floor, the private replay with no live path touched plus both
  rehearsal findings, each gate breach giving exit 4, an empty or unrooted replay refused, the
  foreign tail generations left alone, and the configured roots refused and persisted including
  identity aliases. None mirrors the implementation.

## Findings

### REQUIRED

**R-A. The load helper swallows a dead worker thread, so its gates can pass with the load
missing.** `run()` (deploy/libexec/sparrow-measure-load:399-434) starts the `status`,
`records`, `sparql`, watch replay, socket replay and shipper threads as daemon threads. It then
waits only for SIGTERM. An exception in a thread target ends that thread after Python prints a
traceback. `run()` still writes the summary and exits 0.

Example raise points are not hypothetical:
- `release` (:348) calls `subprocess.run(["cp", ...])` with no `OSError` handling. A fork
  failure under the evening load the issue describes raises there.
- `traffic` (:270) calls `request.write_text`.
- `ship` (:378-387) opens `ship.log` and scans the queue.

The gates do not notice:
- If the watch replay thread dies, releases stop. `watch_intake` counts only released items, so
  `due` stops growing and the gate passes with the watches idle. `watch_claims_completed >= 1`
  and `socket_ingests_accepted >= 1` were already satisfied.
- If a traffic thread dies after its first call, `traffic_answered` (calls >= 1) still passes,
  so traffic was not "at a fixed cadence for the whole backfill".

A copy whose intake or queries stopped migrates under less load. That is the D109 false pass
this mode exists to prevent, and it breaks the "no swallowed failures" rule.

Remedy: run each thread target through a wrapper that catches `BaseException`, logs it with the
thread's kind, records it, and sets `stop`. `run()` must then exit non-zero when any thread
failed. Before the backfill completes, verify-backup's per-poll `kill -0` check then fails the
run with `FAIL load`. After it completes, the existing `wait "$LOAD_PID" || fail load` catches
it. A `threading.excepthook` that does the same is an equivalent remedy. State it in the helper
docstring and in docs/deployment.md's `FAIL load` sentence.

**R-B. The O8 fix is incomplete: an operator message still names the removed first-status
source.** deploy/verify-backup.sh:569 prints "the backup image %s is missing or has no
--query-only; its record count comes from the first status". After O8, the fallback is the
measured image's query-only prep answer when exact (`source=prep-query-only`), else `pending`.
The writable copy's first status is never used (verify-backup.sh:541-580, docs/deployment.md
"`backup_live_records` is what..."). The message misstates where the D118 comparison number
comes from, and the orchestrator ruled O8 in scope. Remedy: reword it, for example "...its
record count comes from the measured image's query-only prep answer when exact, else it is
pending".

**R-C. The merged tree has no runtime evidence, and the runs the review 2 ruling requires have
not been recorded.**
- On the merged tree 38c3744, validation.md records only fmt, clippy and the sync. Those are
  compile-only. `logs/r2-filtered1.log` was still compiling during this review, and `r2-full1`
  and `r2-full2` are absent.
- The merge brought in upstream store changes: `open_files`, the status path without a
  source-context rebuild, and migration and store_counters edits. The traffic tests exercise
  these through the measured daemon.
- The orchestrator's review 2 ruling requires the full rpc binary twice and the filtered set
  once. On 6bb5a7a there is one full run, inside the hook.
- implementation.md, "Review 2 rework" (:232), still says the end-to-end tests are NOT yet run.

Remedy: finish the filtered run once and the full rpc binary twice on 38c3744, each redirected
to its log with its `EXIT=` status. Record the pass counts in validation.md, and update the
implementation.md Review 2 section with them. Add the missing `EXIT=` line to the clippy
evidence, or rerun it with the status captured.

### OPTIONAL

- **O-a.** watches.rs:68-96 and watch_admin.rs:417-427: two watches whose roots share a final
  component, such as `/a/x` and `/b/x`, rebind to the same `DIR/x`.
  - `configured` keeps one entry, so a later persist writes the wrong configured root for one
    of them.
  - A finished-files pair then fails startup on the second `flock` with an unnamed
    `EWOULDBLOCK`.
  - A tail pair would read the same private root twice.
  - Only the copy is affected, and the shipped layout (`intake_root/NAME`) keeps basenames
    unique. Refusing a second definition bound to an already-bound private root, with a message
    naming both configured roots, would make this a named hard failure.
- **O-b.** docs/deployment.md:635 documents `watch_intake ... via=ship|direct`, but helper:455
  prints `via=none` for a watch with no releases. Add `none` to the table row.
- **O-c.** deploy/reconcile-inflight.sh:75-79 adds a third JSON-field shell helper beside
  verify-backup.sh:314 `json_field` and lib.sh:306-308. AGENTS.md asks for one implementation
  per helper. Consider a null-aware helper in lib.sh, if lib.sh can be sourced without its
  system-unit defaults, or a note on why it cannot.
- **O-d.** deploy/README.md:35: the `libexec/` row still lists only `sparrow-ship` and
  `sparrow-backup`, and the operator tool `reconcile-inflight.sh` has no row.
- **O-e.** Small text fixes:
  - verify-backup.sh:971 prints "removing $SCRATCH" before `--rehearse-rollback` uses it.
  - rpc.rs:5617: the comment says "Two backup records" for an asserted count of 4. Two come
    from the fixture plus the two window artifacts.
  - rpc.rs:5523 says "the producer's file is only read", but the script no longer reads it.
  - user-unit-upgrade.sh:71 and reconcile-inflight.sh:10 have overlong lines that break the
    surrounding wrap.
  - plan.md:109 Exclusions still mentions reflink copies of producer source files.

Initial verdict (superseded by the recheck below): BLOCKED

## Recheck (uncommitted fixes over 38c3744)

Scope: `git diff HEAD` in the worktree, 12 files, +152/-54. Read in full: the helper, lib.sh,
reconcile-inflight.sh, verify-backup.sh, user-unit-upgrade.sh, README, docs, watch_admin.rs,
watches.rs, watches/tests.rs and rpc.rs hunks. Same constraints: no cargo, no daemon, no live
path.

### Evidence

| Check | Result |
| --- | --- |
| `bash -n` on verify-backup.sh, reconcile-inflight.sh, user-unit-upgrade.sh, lib.sh | exit 0 each |
| `shellcheck -x` on the same four | exit 0 |
| Python `ast.parse` of the helper | exit 0 |
| Helper probe in a scratch directory under `.stage` (removed after): `run` with a missing `--ship` path | EXIT=1. It logs `worker ship failed:` with the traceback, prints the MEASURE lines, writes the summary, then prints `worker(s) ship failed; the run lacks their load`. |
| `reconcile-inflight.sh --help`, and `inflight` with non-executable CLIs (lib.sh sourced, no daemon) | EXIT=0 with the usage text. EXIT=1 with `reconcile-inflight.sh: both CLI images must be executable`. |
| `prose-check` on the docs and README, and on both `--help` outputs | only the existing `UInt` findings; the help texts pass with exit 0 |
| `logs/r2-filtered1.log` (38c3744) | 20 passed, 0 failed (70 filtered out) |
| `logs/r2-full1.log`, `logs/r2-full2.log` (38c3744) | 90 passed, 0 failed, each. Both logs end at the test summary with no `EXIT=` line; the exit status is the orchestrator's report. |
| `logs/r3-unit.log` (fixed tree) | 3 `private_` unit tests passed, `unit EXIT=0` |
| `logs/r3-fmt.log`, `r3-fmt2.log`, `r3-clippy.log` | The first fmt check showed one diff; after formatting the second check is clean. `clippy EXIT=0`. |
| `logs/r3-filtered.log` (fixed tree) | empty: still running |

### Findings

| Finding | State |
| --- | --- |
| R-A | Resolved. `worker()` (helper `run`) wraps every thread target: the three traffic kinds, both replays and the shipper. It catches `BaseException`, records the name, logs the traceback and sets `stop`. `run()` then prints the MEASURE lines and the summary, and exits 1 through `fail()`. Before the backfill completes, verify-backup's per-poll `kill -0` check turns that into `FAIL load`; after it, `wait "$LOAD_PID" \|\| fail load` does. The probe above confirms the exit. The docstring and docs/deployment.md say so. No automated test was added. Injecting the failure end to end needs a test-only seam, and the probe covers the path, so I accept that. |
| R-B | Resolved. verify-backup.sh:563 now names the query-only prep status when exact, otherwise pending. |
| R-C | Resolved for the merge tree: the filtered set passed 20 of 20, and the full rpc binary passed 90 of 90 twice, which meets the review 2 ruling. The fixed tree still needs runtime evidence; see the condition below. |
| O-a | Resolved. `Catalog::rebind_roots` refuses at open when a second configured root maps to the same private root, and the message names both. `admit_private_root` refuses the same for a put of another watch id with `INVALID_ARGUMENT`. The unit test covers the open refusal and checks that both roots are named. interfaces.md documents it. The put branch has no test of its own. That is acceptable: the catalog's overlap validation of rebound roots also stands behind it. |
| O-b | Resolved. The table row lists `via=ship\|direct\|none` and explains `none`. |
| O-c | Resolved. `json_field` now lives only in lib.sh (lib.sh:313). Every verify-backup caller comes after `source lib.sh` (line 139). reconcile-inflight.sh sources lib.sh and uses its `json_field`, `die` and `note`. lib.sh's top level only sets defaults and the scope check, which passes with the defaults. Output now starts `reconcile-inflight.sh:`. The rehearsal's checks (`*"inflight moved=1 kept=0"*`, `*"missing reshipped=1 unretired=0"*`) match substrings, and no doc or test quotes the old prefix. `json_field` now prints `null` instead of an empty line, but its only uses there are `store_id` (never null) and the printed `deletion`. |
| O-d | Resolved. README rows for verify-backup.sh and reconcile-inflight.sh, and `sparrow-measure-load` in the `libexec/` row. |
| O-e | Resolved. verify-backup.sh:965 wording, rpc.rs:5523 and :5617 comments, the user-unit-upgrade.sh help wrap, the reconcile-inflight.sh header wrap, and plan.md Exclusions (:108-109). |

New findings: none REQUIRED.

- OPTIONAL: `rebind_roots` returns its configuration refusal as
  `consumption::Error::Storage` carrying an `INVALID_ARGUMENT` error. It only reaches startup
  output, so this is harmless. A dedicated variant or `State` text would read more accurately.

Condition, not a finding: the fixed tree changes code the end-to-end tests run: the helper,
lib.sh's `json_field`, reconcile-inflight.sh's sourcing of lib.sh, and `rebind_roots`. The
running `logs/r3-filtered.log` and the commit hook's full rpc run must pass and be recorded with
their exit status in validation.md and implementation.md before the PR. If either fails, this
verdict does not stand. Done-when 1 and 3 remain open after merge under D118 (`Refs #145`).

VERDICT: PASS
