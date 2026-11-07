# Delivery review 3: issue 36 step 2, online schema 69 migrations

## Scope

- Worktree `/home/paudley/Active/sparrow/.worktrees/36-bound-the-schema-69-first-open`, base `eedb484`. The change is uncommitted: 25 tracked files (1,303 lines added, 257 removed) plus the untracked `crates/sparrow-core/src/storage/migration.rs`.
- Read first: `issue.md` and all four `## User rulings` entries in `plan.md`, verbatim. That includes the ~11:5x ruling on the verify-backup measurement mode, with its disk guard and evidence lines, and the ~11:1x ruling that nothing is optional. Then I read `reviews/review-2.md` and the "Review 2 fixes" and "Review 3" sections of `implementation.md`, and checked each claim against the source.
- Repository rules: AGENTS.md and CLAUDE.md. I did not run `make check` or any workspace gate, did not probe the build system and did not modify the worktree. I ran `deploy/verify-backup.sh` only through the existing harness test, which refuses before it copies anything. Nothing touched /SPARROW, the live service or a backup. The only host reads were `df` and the cgroup delegation files.
- Not judged this round, per the caller: the acceptance measurement itself.

## Evidence

My checks (logs in this directory):

| Command | Log | Exit | Scope |
| --- | --- | --- | --- |
| `bash -n deploy/verify-backup.sh deploy/lib.sh` | `r3-bash-n.log` | 0 | both scripts |
| `shellcheck -x deploy/verify-backup.sh deploy/lib.sh` | `r3-shellcheck.log` | 0 | both scripts |
| `cargo test --release -p sparrow-server --test rpc -- copy_daemons_claim_nothing_query_only_refuses_writes_and_no_intake_migrates query_only_and_no_intake_refuse_conflicting_configuration verify_backup_migration_measurement_refuses_to_start_below_its_disk_floor` | `r3-rpc.log` | 0 | 3 passed, 79 filtered |
| `cargo test --release -p sparrow-server --bin sparrowd -- reads_report_pending_mid_migration_and_the_worker_finishes_it` | `r3-maint.log` | 0 | 1 passed, 6 filtered |
| `cargo test --release -p sparrow-core --lib -- a_failed_page_is_reported_with_its_watermark_held_until_the_next_good_page_clears_it` | `r3-core.log` | 0 | 1 passed, 35 filtered |
| `prose-check` on the added doc lines and on the added script and Rust comments | `r3-prose-docs.log`, `r3-prose-comments.log` | 0, 0 | added text only |

Implementer logs checked against source modification times: `r3-test-core.log` (11:53, lib 36 and storage 220 passed, EXIT=0) is newer than every core source file (latest 11:45). `r4b-test-server-full.log` (13:01, 28 + 7 + 82 passed, EXIT=0) is newer than every server, script and doc file (latest 12:53). `r4b-clippy.log` (12:57) and `r4b-fmt.log` (12:53) both record EXIT=0.

Host observations, read only: `user@1000.service` delegates `cpu cpuset io memory pids`, so `CPUQuota`, `IOWeight` and `MemoryHigh` on a user scope take effect on this host. `/SPARROW` is on `/`, which has 47,319,044,096 bytes (47.3 GB) free. That is below the 50 GB start floor, so the measurement will refuse to start until space is freed. The guard is working as the ruling intends; this is host state, not a finding.

### Review 2 findings, verified

- **R1 (measurement):** the tooling exists (see below). Running it is the parent's step and is not judged here.
- **R2 (duplicate helpers): resolved.** `storage.rs:1294` `any_key` and `storage.rs:1302` `count_prefix(db, prefix, frontier)` are the only implementations. `Writer::count_prefix` (:1136), `catalog_upgrade::seed` (:132), `event_fallback_rdf::fence` (:40) and `counts` (:88), and `rdf_index::fence_owner_graphs` (:321) and `owner_graph_counts` (:355) all route through them. `rdf_index::any_key` is gone. `make census-check` has not run; I confirmed the single-sourcing by grep.
- **R3a (failure path): resolved.** `migration.rs` `a_failed_page_is_reported_with_its_watermark_held_until_the_next_good_page_clears_it` corrupts the next `rdf:graph:record:` key past the committed watermark in an open store and calls `Store::migrate_step`. The test then asserts that the step returns `Err`, that `failure` is set, that `done` and `watermark` are unchanged, and that a retry fails again. After the graph is restored, the next step clears `failure` and advances both `done` and the watermark. I ran it: it passes.
- **R3b (wire pending signal): resolved.** The rpc test runs the CLI `sparql` against the pending query-only copy and asserts `coverage.event_fallback_pending == "1"` and `coverage.complete == false`. After the no-intake daemon finishes, it asserts `"0"`. That covers `index_coverage_complete`, `set_event_fallback_pending` and the agent rendering end to end.
- **R4 (contract text): resolved.** The capnp `MigrationStatus` comment and `MigrationStatus::watermark` both say "`start` before the first page, absent only once complete". `docs/interfaces.md` names the duties that are skipped below the floor and the ones that still run.
- **O1:** resolved ("Each maintenance page examines up to 64 graphs").
- **O2:** resolved. The doc comments on `complete_test_migrations` (`rdf_index.rs:1277`) and `open_test_db` (:1288) are correct again.
- **O3:** resolved. `Migration::index` is used, `State.failures` is sized `Migration::ALL.len()`, and `execute_bounded` no longer exists.
- **O4:** resolved. The journal's progress read runs in `spawn_blocking`.
- **O5:** resolved. The core test is now `..._and_direct_migrate_steps_finish_them`, and its doc says it drives `Store::migrate_step` directly. The maintenance test, `reads_report_pending_mid_migration_and_the_worker_finishes_it`, reads mid-migration after two direct pages and asserts `event_fallback_pending == total - done`. It then starts the real `Worker`, waits for both migrations to complete with no failure, and asserts that the read reports 0. The name matches the behavior.
- **O6:** not resolved. `implementation.md` restates the read-cost observation, but no doc sentence was added (`git diff | grep RESOURCE_LIMIT` finds nothing). It remains optional; see O-a below.
- **O7:** resolved (`r4b-test-server-full.log`).

### `sparrowd --no-intake`

- Argument parsing (`main.rs:23`) accepts exactly `--config P` with one of `--query-only` or `--no-intake`, on either side. Both flags together fall through to `USAGE`. `check_no_intake` refuses `query_only = true` in the configuration. The shared `refuse_intake` names `consume`, `syslog`, `rpc_tcp_address`, `ingest_socket_path` and `archive`, and it runs before the TCP bind at `main.rs:43`.
- Every path that could open intake or a listener: the consume worker starts only in the `else` branch at :81, which no-intake skips. Watches come from `WatchRegistry::stored` with `directories: 0`. The watch controller is attached only when a consumer exists (:180), so watch administration over RPC returns `UNSUPPORTED` (`rpc.rs:260`). The ingest endpoint, the syslog worker and the TCP listener each need a key the mode refuses. `archive` is not used by `sparrowd`. Maintenance duties (`import_reclaim`, expiry, pressure, flush, recompress, migrate) touch only the store. The only socket is the configured Unix socket in the scratch directory.
- Normal and query-only behavior is unchanged apart from the startup line, which gains `intake=`. The only consumers of that line are `verify-backup.sh`, which matches the substring `query_only=true`, and the evidence log.
- Tests: the rpc test runs `--no-intake` on a copy with a stored watch and pending migrations. It asserts that both migrations finish with no failure, that `query_only` is false, that the stored watch reports `directories: 0`, and that the waiting intake file is unchanged. The refusal test covers `consume` and `rpc_tcp_address` under `--no-intake`. The other keys go through the same function.

### `deploy/verify-backup.sh --measure-migration`, checked against the ~11:5x ruling

| Ruling item | Where | Status |
| --- | --- | --- |
| Writable reflink copy under `/SPARROW/verify` | `SCRATCH` from `mktemp` under `VERIFY_ROOT`, `cp -a --reflink=always`, no `query_only` line | met |
| Its own socket | `SOCKET=$SCRATCH/store/verify.sock`, guarded against the live paths | met |
| Ingest, TCP and watches disabled in a scratch config | the config has only socket, data, lexicons, limits and budgets; `--no-intake` refuses the rest; the usage line must name `--no-intake` | met |
| `systemd-run --user --scope`, `CPUQuota=100%`, `IOWeight=20`, `MemoryHigh=2G` (`MemoryMax=4G` matches `deploy/systemd/sparrow.service:59`) | measurement block | met; the scope execs the daemon, so `$!` is the daemon PID |
| Runs until status shows every migration complete with no failure | Python state: `failed` / `complete` / `pending` | met (but see R-1) |
| Scratch removed on success, failure, abort and signals | `trap cleanup EXIT`, `trap 'exit 130' INT TERM`; I tested HUP and TERM against a copy of the trap shape: the EXIT trap runs | met; SIGKILL cannot be handled |
| Refuse under 50 GB at start, before any copy | `report_free start` before `mkdir`/`mktemp`; harness test asserts no scratch | met |
| Abort and remove under 40 GB during the run | `report_free minute "$ABORT_FREE_GB"` then `fail disk`, which goes through cleanup | met, checked each minute, including during the open |
| Free space at start, every minute and at end | `free_space phase=start\|minute\|end` | met |
| Time to first status, total backfill time, image id, start and end clock, record count | `first_status_seconds`, `backfill_seconds`, `image= sha256=`, `start_utc`, `end_utc`, `live_records` | met |
| Existing modes unchanged | `open_watches` and `elapsed` refactors keep the same semantics and format; measurement checks are gated on `MEASURE_MIGRATION` | met |
| `free_bytes` shared | `lib.sh` `free_bytes`, used by `convert_store_no_cow` and `report_free` | met |

Docs: the `docs/deployment.md` section "Measure the online migration" and the `docs/interfaces.md` no-intake paragraph match the code. `backfill_seconds` is measured from daemon start, as the table says, and it is accurate to within one 15-second poll.

## Findings

### REQUIRED

**R-1. A no-intake daemon never notices that its maintenance worker has ended, so it neither exits nor reports, and the measurement can then run forever.** `main.rs:217` checks `maintenance::Worker::is_finished` only inside the branch `_ = consumer_health.tick(), if consumer.is_some() || syslog.is_some()`. In a normal daemon the consumer always exists. In query-only mode there is no maintenance worker. Under `--no-intake`, both `consumer` and `syslog` are `None`, so the branch is disabled and the worker's end is never seen.

`maintenance::run` returns `Err` and stops in three cases: any duty latches a durability failure ("store durability uncertain; exiting so it is reopened", `maintenance.rs:224-229`), the free-space check fails, or a job's `spawn_blocking` join fails. The daemon then keeps serving with no maintenance worker. That contradicts `docs/deployment.md` ("the daemon exits with an error and `Restart=on-failure` reopens the store") and `implementation.md` ("A latched durability failure ends the daemon, as for every duty").

When the latch comes from a flush, pressure, reclaim or expiry write rather than from a page, no migration `failure` is set. The migrations then stop advancing while `status` still answers. The script's loop has no stall bound by design, and it does not read `durability.failure`, so it polls until the 40 GB guard trips or an operator interrupts it.

This is a failure-path defect in the safety-relevant mode. It comes from reading the code; I did not execute it. Fix: include `maintenance.is_some()` in that branch's guard, or give the maintenance check its own branch. Add a focused check that a no-intake daemon whose worker ends exits non-zero. Optionally, also make the script fail on a non-null `durability.failure` in status.

### OPTIONAL

- **O-a.** Review-2 O6 is still open. Add one sentence to the reader table in `docs/interfaces.md`: while the backfill is pending, each candidate record costs an extra manifest read and subject lookup against the request budget, so a large metadata scope can refuse with `RESOURCE_LIMIT` during the migration and succeed after it.
- **O-b.** The script deletes the daemon log with the scratch on success, so the `sparrowd: migration ... progress` and `complete` lines, and any `online migration failed` retry that clears between polls, are lost. Printing the daemon's `sparrowd: migration` lines (or the whole log tail) before cleanup on success would keep page counts with the evidence. The per-minute `progress` MEASURE lines already give an average page rate.
- **O-c.** The script could record the scope's effective limits as a MEASURE line (`systemctl --user show sparrow-verify-XXXXXX.scope -p CPUQuotaPerSecUSec -p IOWeight -p MemoryHigh -p MemoryMax`), so the evidence shows the limits applied as well as requested. They do apply on this host (io is delegated), but nothing in the evidence shows it.
- **O-d.** If `df` fails, `report_free` returns 1, so the caller prints "fell under the abort floor" or "has under the 50 GB free". The run still aborts safely, and `free_bytes` prints its own `die` message first, so this is message accuracy only.
- **O-e.** `docs/interfaces.md` (the "Online store migrations" paragraph, "...retention are; buffered synchronization and the table rewrite still run there. Each page commits its rows and") has one line that was not rewrapped.

Open item for the parent, not a finding this round: the acceptance measurement and the plan Behavior item "find where the remaining ~30 minutes of the 34 go ... and record it" are still to be done. The host's 47.3 GB free on `/` is below the script's 50 GB start floor.

VERDICT: BLOCKED
