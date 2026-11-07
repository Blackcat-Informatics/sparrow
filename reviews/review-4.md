# Delivery review 4 (recheck): issue 36 step 2, online schema 69 migrations

## Scope

- Worktree `/home/paudley/Active/sparrow/.worktrees/36-bound-the-schema-69-first-open`, base `eedb484`, uncommitted (`git diff` plus the untracked `crates/sparrow-core/src/storage/migration.rs`).
- This round covers only what changed after `review-3.md` (written 13:37:22). By modification time, five files changed after it: `crates/sparrow-server/src/main.rs` and `maintenance.rs` (13:49:13), `docs/interfaces.md` and `docs/deployment.md` (13:52:08), and `deploy/verify-backup.sh` (13:52:29). Every other changed file predates review 3 and is not re-judged.
- Read first: all four `## User rulings` in `plan.md`, verbatim. That includes the ~11:5x measurement-mode ruling and the ~11:1x ruling that nothing is optional. Then I read review 3's R-1 and O-a to O-e, and the "Review 4" section of `implementation.md`, and checked each claim against the source.
- I did not run cargo build, test or clippy, per the caller, because a commit hook is running the full suite on this host. I did not run `make check`, did not probe the build system and did not modify the worktree beyond this report and its logs. I did not run `deploy/verify-backup.sh`, and I touched no store, /SPARROW path, backup or live service. Host reads were `systemctl --user show` and `systemctl show` property queries (see Evidence).
- Not judged, per the caller: the acceptance measurement itself, which waits on disk space.

## Evidence

My checks (logs in this directory):

| Command | Log | Exit | Scope |
| --- | --- | --- | --- |
| `bash -n deploy/verify-backup.sh deploy/lib.sh` | `r4-bash-n.log` | 0 | both scripts |
| `shellcheck -x deploy/verify-backup.sh deploy/lib.sh` | `r4-shellcheck.log` | 0 | both scripts |
| `prose-check --stdin` on the added lines of both docs | `r4-prose-docs.log` | 0 | added doc text |
| `prose-check --stdin` on the added script and Rust comments and the script usage text | `r4-prose-comments.log` | 0 | added comments |
| Offline run of the cleanup `sed` expression on sample daemon lines, and of `sort \| paste` on the expected limits in the default and C locales | none (inline) | 0 | the expression keeps both `migration ...` lines and `online migration failed` and drops other duties' failures; both locales give the order in `SCOPE_LIMITS` |

Implementer logs, checked against modification times:

| Log | Written | Covers (latest mtime) | Result |
| --- | --- | --- | --- |
| `r5-bash-n.log` | 13:52:29.326 | `verify-backup.sh` 13:52:29.323 | EXIT=0 |
| `r5-shellcheck.log` | 13:52:33 | the same | EXIT=0 |
| `r5-fmt.log` | 13:52:46 | Rust sources 13:49:13 | EXIT=0 |
| `r5-clippy.log` | 13:53:28 | `sparrow-server`, all targets, release, warnings denied | EXIT=0 |
| `r5-test-maint.log` | 13:54:32 | `--bin sparrowd`: 8 passed, 0 filtered, including `an_ended_maintenance_worker_is_detected_without_intake_workers` | EXIT=0 |
| `r5-test-rpc.log` | 13:56:32 | `--test rpc`: 5 passed, 77 filtered (no-intake copy, the refusals, the three verify-backup tests) | EXIT=0 |

Every log is newer than the files it covers. Coverage gap (not a finding): the full server test run (`r4b-test-server-full.log`, 13:01) predates the `main.rs` change. That change alters the serving loop of every daemon the 77 unselected rpc tests spawn. The selected 5 and all 8 binary tests pass. The full suite runs in the mandatory `make check` hook, which is still owed before this can be called done.

systemd property format, read only (systemd 262): `systemctl --user show` on a nonexistent scope prints `CPUQuotaPerSecUSec=infinity`, `IOWeight=[not set]`, `MemoryHigh=infinity`, `MemoryMax=infinity`. Existing units on this host print memory limits as plain bytes, for example `MemoryMax=3221225472` on a `run-*.scope` and `MemoryMax=51539607552` on `stage-builds.slice`, and `IOWeight` as a plain integer (`IOWeight=100`, `IOWeight=10000`). While I searched for a unit with a CPU quota, my loop also read `sparrow.service`, the live service unit's `CPUQuotaPerSecUSec`. That was a property query only, with no lifecycle action; I disclose it because the caller asked that the live service not be touched. It printed `CPUQuotaPerSecUSec=1s` for the unit's `CPUQuota=100%` (`deploy/systemd/sparrow.service:60`). systemd parses `2G` and `4G` in base 1024, so `2147483648` and `4294967296` are right. The four property names are the ones systemd exports for a scope.

### (1) Review 3 R-1: resolved

- `main.rs`: the health branch is now `_ = worker_health.tick() => match ended_worker(...)` with no `if` guard, so it runs in every mode. `ended_worker` checks `consumer`, then `syslog`, then `maintenance`, each through `is_some_and(is_finished)`. A query-only daemon passes `(None, None, None)` and gets `None`, which costs one no-op every 200 ms.
- Exit trace for `--no-intake`:
  1. `config.query_only` is false, so `maintenance` is `Some(Worker::start(..))`, and `consumer` and `syslog` are `None`.
  2. `maintenance::run` returns `Err` on a latched durability failure, a failed free-space check, a failed job join or a failed status read. Its `JoinHandle::is_finished` then becomes true.
  3. On the next tick (at most 200 ms later), `ended_worker` returns `Some(Ended::Maintenance)`, and `serving` returns `Err("maintenance worker terminated unexpectedly")`.
  4. `shutdown::finish(serving, Workers { .., maintenance: maintenance.take() }, ..)` records `daemon: maintenance worker terminated unexpectedly`. Then `Worker::shutdown` awaits the finished task and records its own error as `maintenance shutdown: store durability uncertain; ...`, so the cause is kept. Admission is drained and `store.flush` runs, recording its error if any. The endpoint names are removed, and `finish` returns `Err(errors.join("; "))`.
  5. `local.run_until` passes that error up, and `main` returns `Err`, which Rust's `Termination` turns into exit status 1.
- In `--measure-migration`, that exit shows up as `kill -0` failing, so the run ends with `FAIL open` plus the log tail and the daemon's migration lines. If the latch is visible first, the run ends with `FAIL durability`.
- Test adequacy: `an_ended_maintenance_worker_is_detected_without_intake_workers` starts a real `Worker` on a real store, in the no-intake shape. It asserts `None` while the worker runs and `Some(Ended::Maintenance)` after the loop ends, without consuming the worker. That protects the decision for the shape that failed. It does not exercise the `select!` wiring, which was where the defect actually was. That wiring is now one unconditional branch, readable at a glance. I judge the test adequate for the contract. There are two ways to cover the wiring end to end. One is to extract the health loop, `worker_health` plus `ended_worker` plus the `Err`, into a function that `main` calls and that an in-process test can drive with an ended `Worker`. That needs no fault seam. The other is to make a spawned `--no-intake` daemon's maintenance loop return `Err`. That needs either a fault-injection seam in the production binary, which is undesirable, or a fault-injecting filesystem, such as a FUSE layer, dm-flakey or an `LD_PRELOAD` shim that fails `fsync`, to latch a durability failure. Forcing a failed free-space check, for example by deleting `data_dir`, does not isolate the path: the serving loop's own `space.tick()` calls `shortfall()?` and would exit first with a different error.

### (2) `verify-backup.sh --measure-migration` changes

- **Durability:** each poll after the first status reads `durability.failure` (`json_field durability failure`) and fails with `FAIL durability` unless the value is `null`. This uses the same accessor as the verify path at line 445.
- **Stall:** `moved=0` and `progress='status=unanswered'` start the loop. The stall comparison only runs after a successful status, and the first answer's `NAME=D/T ...` string always differs from `status=unanswered`, so `moved` is set to `SECONDS` there. The check therefore cannot fire during the open or before the first status, and failed status attempts `continue` before it. After that, `FAIL stall` fires when the combined progress string is unchanged for 600 s or more. The stated reasoning holds as a margin: the step-1 rate gives about 0.9 s per 64-graph page (34 min / 2,360 pages, in the Profile section), and the warm probe's slowest page was 4.0 s at 256 graphs. One correction to the rationale: a failing page sets `failure`, which ends the run with `FAIL migrate` on the next poll. The stall rule therefore actually catches stops that set no failure, such as the daemon pausing writes below its own `min_free_bytes` floor. The script still catches those, and the log tail shows the daemon's pause line.
- **Daemon migration lines:** `cleanup` runs the `sed` whenever `MEASURE_MIGRATION` is set and the log is non-empty, after the daemon is stopped and before `rm -rf`. Every exit after the trap is installed passes through `cleanup`: `fail`, `die`, `exit 0`, and INT or TERM through `exit 130`. The pattern matches the three line shapes `maintenance.rs` emits: `migration NAME progress ...`, `migration NAME complete pages=P`, and `migration NAME progress failed: ...` (lines 124, 135 and 143). It also matches `online migration failed: ...` (line 247, `Job::Migrate.name()`).
- **Scope line:** at the first status, the script runs `systemctl --user show sparrow-verify-XXXXXX.scope -p CPUQuotaPerSecUSec -p IOWeight -p MemoryHigh -p MemoryMax | sort | paste -sd ' '`, with `pipefail` set. It prints the result as a `MEASURE scope=` line and requires an exact match with `CPUQuotaPerSecUSec=1s IOWeight=20 MemoryHigh=2147483648 MemoryMax=4294967296`. The names, formats and sorted order are correct (see Evidence). The scope name matches `--unit="sparrow-${SCRATCH##*/}"` with `.scope` appended. A wrong unit name would print the defaults and fail the comparison, so the check is strict. These are the unit's configured properties. The kernel enforces them only where the controllers are delegated, and review 3 confirmed `cpu io memory` delegation on this host.
- **df failure:** `report_free` returns 2 when `free_bytes` fails. `local free` is declared separately, so the status is not masked. The start path tests `(($? == 1))` inside the `||` group, where `$?` is still `report_free`'s status, and dies with "cannot read the free space ... nothing was copied or started". The per-minute path ends with `FAIL disk` and the read message. In `cleanup`, `report_free end || true` takes no floor.
- **Cleanup:** unchanged apart from the gated `sed`. `trap cleanup EXIT` and `trap 'exit 130' INT TERM` are installed right after `mktemp`. Before that point, nothing exists to remove.

### (3) Docs

- **O6 / O-a:** resolved. The sentence below the reader table is accurate: `fallback_pending` does a `catalog_point` (charged through `budget.request`), a decode reservation and a `term_id_by_value` subject lookup (charged through `budget.reserve`). It is slightly broad, because records at or before the watermark return early and pay nothing (see O-1).
- **O-e:** resolved. The "Online store migrations" paragraph lines now fit the width.
- **No-intake exit:** "If its maintenance worker ends, the daemon exits with an error, as a normal daemon does" matches the trace above. "The startup line ends `query_only=false intake=false`" matches the `eprintln!`.
- **`docs/deployment.md` section:** it matches the script on the disk floors and overrides, the `df` message, the scope read-back, the 15 s poll, the failure bullets present, the daemon-line prefix and the MEASURE table, whose `scope=` row is in sorted order. Its failure list is not exhaustive, though (see O-1).

### (4) Other modes

The verify path (lines 414 to 529) is textually unaffected. The only shared changes are the `cleanup` addition, which runs under `if ((MEASURE_MIGRATION)) && ...`, and `report_free`'s new return value, which only the measurement uses. `STALL_SECONDS` and `SCOPE_LIMITS` are plain assignments. `r5-test-rpc.log` passed the three verify-backup tests, including `verify_backup_fails_on_wrong_store_id`, which starts a query-only daemon, after the script's last modification. The query-only daemon's new always-on health tick finds nothing to check.

## Findings

### REQUIRED

None. Review 3's R-1 is resolved.

### OPTIONAL

- **O-1 (doc and comment precision).**
  - `docs/deployment.md` introduces its failure bullets with "The run fails, removing the copy, when:", which reads as complete, and `implementation.md` claims the section "lists every failure condition". Several exits are missing:
    - `FAIL store-id` (wrong store);
    - `FAIL no-intake` (status or startup line not in no-intake mode, or a watch root open);
    - `FAIL status` "no readable migrations";
    - `FAIL shutdown` (non-zero exit after SIGINT, or no closing-count line).
  - "a page of 64 graphs commits in about a second" appears in the doc and in the script comment as a fact. It is the step-1 average (0.9 s per page), not a measured page time.
  - The O6 sentence could say "each candidate record beyond the backfill watermark".
- **O-2 (test of the wiring).** If the wiring should be tested, extract the health loop into a function that `main` calls, as described under (1), so an in-process test can cover the `select!` branch itself.

VERDICT: PASS
