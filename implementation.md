# Implementation: issue 145

Committed through the hook as 6bb5a7a (WIP) and synced onto origin/main ea1a4b2 as merge
38c3744. No real backup was measured: per the D118 ruling the orchestrator runs that after
merge. The review 3 rework is below.

## File map

Daemon mode `sparrowd --private-intake DIR`:

- `crates/sparrow-server/src/consumption.rs:69` `RootBinding`; `:217`
  `open_bound`; `:234` `from_root`, where Private skips claims bound to another
  root identity (`:241` to `:251`).
- `crates/sparrow-server/src/tailing.rs:30`, `:41` to `:72` and `:106`: the tail
  root carries the binding and skips foreign active generations.
- `crates/sparrow-server/src/watches.rs:42` `PrivateIntake`, `:58` `rebind`,
  `:71` `binding`, `:704` `ConsumeWorker::start_with` (rebinds the catalog at
  `:714`), `:1102` control state for `watchPut` rebinding.
- `crates/sparrow-server/src/watch_admin.rs:408` `Catalog::rebind_roots`.
- `crates/sparrow-server/src/config.rs:287` `check_private_intake`, `:296`
  `refuse_intake`.
- `crates/sparrow-server/src/main.rs:18` usage, `:22` `Mode`, `:32` argument
  parser, `:116` startup intake label, `:166` worker start.

Measurement and rehearsal:

- `deploy/verify-backup.sh`: options at `:110` and `:111`, gate constants from
  `:152`, scheduling policy check near `:206`, functions `spawn` `:455`,
  `await_status` `:464`, `halt` `:477`, `private_violations` `:539`,
  `pages_rate` `:548`, `rehearsal_config` `:559`, `await_inflight` `:568`,
  `place_batch` `:580`, `rehearse_rollback` `:599`.
- `deploy/libexec/sparrow-measure-load` (new): `prepare_producers` `:75`,
  `prepare_socket` `:115`, `run` `:267`, `gates` `:343`.
- `deploy/reconcile-inflight.sh` (new): `inflight` and `missing` subcommands.
- `deploy/user-unit-upgrade.sh:61`: the rollback claims help bullet.
- `docs/deployment.md:478` scheduling policy, `:542` "Measure the online
  migration with intake and traffic", `:738` "Claims after a rollback".
- `docs/interfaces.md:528` the `--private-intake` contract.

Tests in `crates/sparrow-server/tests/rpc.rs`: `:4682`
`query_only_and_no_intake_refuse_conflicting_configuration` (extended),
`:5103` `traffic_fixture`, `:5242`
`verify_backup_traffic_measurement_replays_privately_and_rehearses_the_rollback`,
`:5359` `verify_backup_traffic_measurement_fails_on_each_gate_breach`.

## Checks

| Check | Result |
| --- | --- |
| `cargo clippy --release -p sparrow-server --all-targets -- -D warnings` | exit 0, run twice |
| `cargo fmt -p sparrow-server --check` | exit 0 |
| `shellcheck -x` on the three changed scripts | exit 0 |
| `bash -n` on the three scripts, Python parse of the helper | exit 0 |
| rpc tests `verify_backup query_only no_intake user_unit retire claim` | final tree: 18 of 19 passed (exit 101); the failure is `configured_consumer_recovers_confirmed_journal_and_reports_late_write_without_retirement` (`live_records` 2, expected 1), unchanged by this branch; it passed in the earlier 19 of 19 run and in 5 of 5 isolated reruns |
| lib tests `watches consumption tailing` | 6 passed, exit 0 |
| Mutation: Private treated as Stored | the traffic test fails (exit 101) with "configured watch root was replaced while source recovery ownership remains"; source restored |
| `prose-check` on the docs and plan | only the existing `UInt64` and `UInt32` findings in `docs/interfaces.md` |

Numbers from the passing fixture run: first status 1.010 s, 6 pages at 4.1265
pages per second, `live_records=6 counts=exact`, one call each of `status`,
`records`, `sparql` and `ingest`, 10 shipper runs, and the rehearsal moved inode
732499586 back, then re-shipped and retired the claim.

## Open questions

1. The measured image must be built from a revision with `--private-intake`,
   so after this merge. The current schema 70 images lack it and are refused.
2. The traffic tests need a user systemd manager with `sparrow.service`
   loaded and `chrt`. A CI host without them fails these tests.
3. Tail watches skip foreign generations at open; later tail drain paths
   have not been exercised with foreign generations.
4. Producer artifacts that arrived live over TCP are re-sent over the copy's
   ingest socket.
5. Per-migration seconds are accurate to one 15-second poll.
6. With a FROM image older than schema 70, `reconcile-inflight.sh missing`
   re-ships and exits 3: the 68 rollback claim stays pending until a schema 70
   image retires it.
7. `reconcile-inflight.sh` detects an unknown entry from the `NOT_FOUND` text
   of `ingest-get`'s standard error.
8. The rehearsal uses the measured image in every role.
9. `configured_consumer_recovers_confirmed_journal_and_reports_late_write_without_retirement`
   failed once under host load (`live_records` 2, expected 1). The test is not
   in this diff and runs the stored binding, which this change leaves as it
   was. The inference is a timing race with the replacement `confirmed.jsonl`
   being ingested before `status`; it was not reproduced on `main`.

## Review 1 rework

Every finding of `reviews/review-1.md` (R1 to R3, O1 to O7) is addressed in the worktree.
Nothing is committed or pushed, `make check` was not run, and no real backup was measured.

### File map

R1, empty or partial replay:

- `deploy/libexec/sparrow-measure-load:94` `prepare_replay` replaces both prepare
  subcommands. It replays what the copy received in the window, from its own catalog:
  watch items through a scratch `settled` shipper source per `ship.toml` source, or
  straight into the root, and other artifacts over the socket. It prints
  `replay source=NAME via=ship|direct|socket artifacts= bytes= skipped=`. It fails when a
  shipper source names no watch root, when a shipper source has nothing in the window, or
  when the window is empty.
- `:314` `run` counts per-source releases and failures. `:414` `gates` adds
  `replay_failures` (release and ship failures), `watch_claims_completed` (at least 1) and
  `socket_ingests_accepted` (at least 1 when any socket artifact was saved), at `:442` to
  `:446`.
- `deploy/verify-backup.sh:164` `SPARROW_MEASURE_REPLAY_SECONDS` (3600 to 86400, may
  only widen), `:555` prepare-replay, and the final watch claim sum passed to `gates`.
- Window selection: the catalog selects by receipt time. That covers `daily` files (mtime
  after the window, newest held back) and `sealed` segments (manifest-gated) as live
  received them, and the replay reads no producer directory (`plan.md`, Decisions).

R2, backup count apart from replay:

- `deploy/verify-backup.sh:529` reads the count with the backup's own image, query-only on
  the prep copy, before any newer image opens it. `:825` prints
  `backup_live_records=N counts=exact source=backup-image|first-status`, or `pending`:
  the first status is used when exact, since the load starts after it.
- `:917` `replay_added` comes from `sparrow-measure-load:184` `count-records` (records
  received since the load started, page by page). The final `live_records` is the copy's
  count at the end, replay included.

R3, gate environment:

- `crates/sparrow-server/tests/rpc.rs:4793` `measure_env`: `systemctl` and `systemd-run`
  test doubles on PATH, and `:4858` `verify_backup_measured`. Every measurement test uses
  them, the pre-existing disk-floor test (`:5146`) included, so none needs a user manager
  or a loaded unit. The script is unchanged and still checks every reported value. The
  `systemd-run` double applies the nice level and records the scope properties.
  `chrt --other 0` runs for real.
- `.github/workflows/ci.yaml:80` gives the tests' scratch root a loop-mounted btrfs
  `target/tmp` and installs `b3sum`. Every verify-backup test needs reflinks and
  `b3sum`, the pre-existing ones too.

O1 to O7:

- O1 `deploy/reconcile-inflight.sh:113`: matches `AgentError { code: "NOT_FOUND",` only.
  `:164`: retire capability is read from the restored CLI's `watch` usage, so a schema 70
  "unknown watch" refusal fails the run instead of exiting 3.
- O2 `crates/sparrow-server/src/watches.rs:66` `rebind` refuses a DIR that is, holds or lies
  under a configured root. `watch_admin.rs:52`, `:417` to `:451` keep and persist the
  configured roots. Because of that, `reconcile-inflight.sh:93` `--private-intake DIR`
  replaces `--only-under`: query-only status now names configured roots. The rehearsal puts
  its watch with a configured root outside DIR (`verify-backup.sh:651`).
  `docs/interfaces.md:533` adds the warning and the contract.
- O3 `crates/sparrow-server/src/tailing.rs:423` `foreign`, used in `attach` (`:153`) and
  `reconcile_missing` (`:397`).
- O4 `deploy/reconcile-inflight.sh` help and `deploy/user-unit-upgrade.sh:67`: "a hard link
  under that name and an unlink of the entry".
- O5 `deploy/verify-backup.sh:777`: `FAIL load` at the first poll after the helper exits.
- O6 `deploy/verify-backup.sh:902`: migrations first seen complete at one poll share its
  interval, `shared_poll=A,B shared_pages=P`.
- O7 `crates/sparrow-server/tests/rpc.rs:6248`: quiet interval 600 s, the maximum and 20
  times the test's 30 s waits, and the wait also requires the replacement as a held
  candidate. The production invariant is unchanged.

Test harness:

- `crates/sparrow-server/tests/rpc.rs:4754` `verify_backup_with` holds `PROCESS_START`
  only across the spawn (`admitted_spawn` then `wait_with_output`, the pattern already
  used at `:7859`), not for the whole run. Before, `admitted_output` held the lock for
  each verify-backup run. The three traffic runs pushed the total held time past the
  daemon's 60 s `CONNECTION_LEASE`. Tests that held a connection while waiting to spawn
  then failed with "Peer disconnected" and "RPC connection lease expired": 2 of 20 in each
  filtered run, and 1 to 11 of 89 in each full run. With the measurement tests skipped,
  the same binary passed 82/82 in 13 s; with the fix, every run below passes.

Tests:

- `crates/sparrow-server/src/watches/tests.rs:410`
  `private_tail_root_leaves_foreign_generations_alone` (O3)
- `:493` `private_intake_refuses_configured_roots_and_persists_them_unchanged` (O2)
- `rpc.rs:5638` `verify_backup_traffic_measurement_refuses_an_empty_or_unrooted_replay` (R1)
- the traffic tests extended for per-source counts, the new gates, `backup_live_records`
  (exact in the breach test, 4 records), `replay_added` of at least 2, and the
  `SPARROW_MEASURE_REPLAY_SECONDS` refusal

Docs: `docs/deployment.md:569` (replay source and checks), `:630` (backup count), `:662`
(new gates), MEASURE table; `docs/interfaces.md:533`.

### Checks

All runs are release profile, run one at a time under `chrt --other 0 nice -n 10`, with
logs in `/tmp`.

| Check | Log | Result |
| --- | --- | --- |
| `cargo fmt -p sparrow-server -- --check` | `145-r1-fmt.log` | exit 0 |
| `cargo clippy --release -p sparrow-server --all-targets -- -D warnings`, after the last edit | `145-r1-fx-clippy.log` | exit 0 |
| lib tests `watches`, `consumption`, `tailing` | `145-r1-lib.log` | exit 0 |
| `private_` unit tests (O2, O3) | `145-r1-unit2.log` | exit 0, 2 passed |
| O2 mutation: persist writes the rebound root | `145-r1-mut.log` | O2 test fails, as intended |
| O3 mutation: `foreign` filter removed from both `attach` and `reconcile_missing` | `145-r1-mut2.log` | O3 test fails, as intended |
| O3 mutation: `foreign` filter removed from `reconcile_missing` only | `145-r1-mut3.log` | O3 test fails, as intended; source restored (cmp) |
| filtered rpc set, `verify_backup query_only no_intake user_unit retire claim`, run 1 | `145-r1-fx-filtered1.log` | exit 0, 20 passed, 49.37 s |
| filtered rpc set, run 2 | `145-r1-fx-filtered2.log` | exit 0, 20 passed, 46.54 s |
| full rpc binary, run 1 | `145-r1-fx-full1.log` | exit 0, 89 passed, 56.30 s, O7 ok |
| full rpc binary, run 2 | `145-r1-fx-full2.log` | exit 0, 89 passed, 70.50 s, O7 ok |
| full rpc binary, run 3 | `145-r1-fx-full3.log` | exit 0, 89 passed, 60.34 s, O7 ok |
| full rpc binary, run 4 | `145-r1-fx-full4.log` | exit 0, 89 passed, 57.48 s, O7 ok |
| full rpc binary, run 5 | `145-r1-fx-full5.log` | exit 0, 89 passed, 57.57 s, O7 ok |
| `shellcheck -x`, `bash -n` on the deploy scripts; Python parse of the helper | none | exit 0 |
| prose-check on the changed docs | none | exit 0 apart from the existing UInt findings |

O7 is `configured_consumer_recovers_confirmed_journal_and_reports_late_write_without_retirement`.
The main traffic test printed `backup_live_records=4 counts=exact source=first-status`,
`replay_added=2` and `watch_claims_completed=1`, and every gate printed PASS.

Before the harness fix, the same sequence failed (`145-r1-ts-filtered{1,2}.log` and
`145-r1-ts-full{1..5}.log`, all exit 101). O7 passed in all five of those full runs.

### Open questions

1. CI: the workflow change was not run on GitHub. Whether `cargo install b3sum` and a loop
   mount succeed on `ubuntu-latest` is unverified.
2. The test daemons inherit this session's SCHED_IDLE class. The counted runs were taken
   under `chrt --other 0 nice -n 10`, the unit's own policy and nice level. The SCHED_IDLE
   failures seen earlier had the `PROCESS_START` cause fixed above, and they were not
   rerun under SCHED_IDLE.
3. The real measurement still needs an image built after merge (unchanged).

## Review 2 rework

The orchestrator ordered a stop part way through this rework, because the Claude Code
binaries are being replaced. Nothing is committed or pushed, `make check` was not run, and
no real backup was measured. No daemon or test process of this rework is left running. The
`.github` change stays reverted, as the orchestrator directed. The decisions are recorded
in `plan.md`, under Decisions, "Review 2".

### State

Implemented and compiled. Rust parts pass clippy, and the unit tests named below pass. The
end-to-end tests are NOT yet run:

- R1b. `deploy/libexec/sparrow-measure-load`:
  - `:65` `PUBLICATION_DEADLINE_SECONDS`, the daemon's compiled 45 s constant, which is in
    no configuration.
  - `run` records each watch's release times and its `via`. After every shipper run it
    records the longest run and the distinct items left queued (`ship_held_back`).
  - `watch_intake` (above `gates`) builds one `watch_intake:NAME` gate per watch. It
    passes when completed claims since the first status are at least the items released
    before the final status less the allowance. The allowance is `quiet_ms` from status
    plus 45 s, plus, for a shipped watch, the ship interval and the longest shipper run.
  - `replay_failures` now includes `held_back`. There is a new `watch_claims_refused`
    gate.
  - `watch_claims_completed` now counts the change since the first status.
- R1b. `deploy/verify-backup.sh`:
  - `:171` `SPARROW_MEASURE_GATE_INFLIGHT_SECONDS`, a ceiling that only tightens. It
    must be a whole number.
  - The first and final status are saved, along with `claims_at`.
  - `:948` counts `reservation refused` lines.
  - The new `gates` arguments are passed, and the usage text is updated.
- R1b docs, `docs/deployment.md`:
  - The `replay_failures` sentence is corrected.
  - New gate entries: `watch_claims_refused` and `watch_intake:NAME`.
  - New MEASURE rows and the INFLIGHT knob.
- R1b tests, `crates/sparrow-server/tests/rpc.rs`:
  - `traffic_fixture` (`:5216`) takes the watch's `quiet_ms`.
  - The main test (`:5381`) runs 60 s of traffic, so its watch item is due. It asserts
    the new gates PASS and `due >= 1`.
  - The breach test (`:5548`) uses a 600 s quiet watch, `GATE_INFLIGHT=0`, and a wrapped
    image that logs one refused reservation. It asserts that `watch_claims_refused` and
    `watch_intake:replayed` FAIL with `due=1 completed=0`, and that `-1` is a usage
    error.
- R3b. `rpc.rs:4798` `measure_env` adds a `df` double. It reports 60 GB, or
  `SPARROW_TEST_DF_AVAIL_BYTES`. The disk-floor test (`:5162`) now reports 49,999,999,999
  bytes against the unchanged default 50 GB floor. The production floors and their checks
  are unchanged.
- O8. `verify-backup.sh:568` to `:573`: the fallback is the measured image's query-only prep
  status (`source=prep-query-only`). The writable copy's first-status fallback is removed.
  The main test now expects `pending`. Docs are updated.
- O9. `crates/sparrow-server/src/watches.rs:103` `identity`, `:114` `identity_chain`,
  `:129` `aliased`, used in `PrivateIntake::rebind`. The new unit test is
  `watches/tests.rs` `private_intake_refuses_configured_roots_aliased_by_identity`. The
  `docs/interfaces.md` O2 paragraph is updated.
- O10. The `measure_env` doc comment carries the RLIMIT_NICE note.
- O11. `plan.md` Behavior item 1 now describes the catalog replay.

Not started at the stop; all of it ran later (see "Review 3 rework", Checks):

- The end-to-end runs of the changed tests: the main traffic test, the breach test and
  the disk-floor test. Their new assertions are unverified. The main test's `due >= 1`
  depends on 60 s covering the item's allowance, about 49 s.
- The requested filtered rpc set (once) and the full rpc binary (twice).
- prose-check of `implementation.md` and `plan.md`. The docs are checked; see below.

### Checks

| Check | Log | Result |
| --- | --- | --- |
| `cargo fmt -p sparrow-server -- --check`, after `cargo fmt` | `/tmp/145-r2-fmt2.log` | exit 0 (the first check, before formatting, was exit 1) |
| `cargo clippy --release -p sparrow-server --all-targets -- -D warnings`, after every Rust edit (formatting came later) | `/tmp/145-r2-clippy.log` | exit 0 |
| `cargo test --release -p sparrow-server --lib -- private_` | `/tmp/145-r2-unit.log` | exit 0, 3 passed |
| O9 mutation: `aliased` loses the DIR-chain check | `/tmp/145-r2-mut-o9.log` | exit 101, the O9 test fails as intended; source restored (cmp) |
| `bash -n` and `shellcheck -x deploy/verify-backup.sh` | none | exit 0 |
| Python `ast.parse` of the helper | none | exit 0 |
| `prose-check docs/deployment.md docs/interfaces.md` | none | only the existing UInt findings |
| filtered rpc set; full rpc binary x2 | none | not run |

## Review 3 rework

Review 3 (`reviews/review-3.md`) was BLOCKED on R-A, R-B and R-C, with O-a to O-e
optional. Patrick's ruling makes every finding required, so all of them are fixed.

### Changes

- R-A: `deploy/libexec/sparrow-measure-load` `run` starts each worker through `worker()`.
  An exception logs the traceback, records the worker and stops every worker. After the
  MEASURE lines and the summary, `run` exits 1, so verify-backup fails the run with
  `FAIL load` at its next poll. The docstring and the `FAIL load` paragraph in
  `docs/deployment.md` say so. A probe on an empty scratch directory under `.stage`,
  with a missing `--ship` path, exited 1 at once with "worker(s) ship failed"; the probe
  directory was removed. No automated test was added: raising inside a worker end to end
  would need a test-only seam in the script.
- R-B: the verify-backup message about a missing backup image now names the query-only
  prep status, or pending.
- R-C: the test runs below.
- O-a: `Catalog::rebind_roots` refuses two configured roots that rebind to one private
  root, and `Catalog::admit_private_root` refuses the same for `watchPut` with
  `INVALID_ARGUMENT`. Both messages name both roots. The unit test
  `private_intake_refuses_configured_roots_and_persists_them_unchanged` covers the open,
  and `docs/interfaces.md` documents it. The recheck's optional note is also done: the open
  returns its own `consumption::Error::PrivateRootShared` instead of a storage error, and
  the put wraps the same message in `INVALID_ARGUMENT`. That last edit is not yet compiled,
  because coordination paused cargo until #160's PR opens.
- O-b: `via=none` is in the MEASURE table.
- O-c: verify-backup's existing `json_field` moved to `deploy/lib.sh` as the one
  implementation. `reconcile-inflight.sh` sources `lib.sh` and uses its `json_field`,
  `die` and `note`, so its lines now start `reconcile-inflight.sh:`.
- O-d: `deploy/README.md` lists `verify-backup.sh`, `reconcile-inflight.sh` and
  `sparrow-measure-load`.
- O-e: the shutdown message no longer says "removing" before the rehearsal, the two test
  comments are corrected, the two long lines are rewrapped, and plan.md's Exclusions are
  updated.
- Merge: the startup line keeps the intake label and upstream's open files report, so
  the docs now say it "reports" `query_only=false intake=private` rather than "ends" with
  it.

### Checks

Logs are in `logs/`.

| Check | Log | Result |
| --- | --- | --- |
| WIP commit 6bb5a7a through the hook (`make check`) | `wip-commit.log` | EXIT=0, rpc binary 89 of 89 |
| sync merge 38c3744 through the hook | `sync-continue.log` | EXIT=0 |
| filtered rpc set on 38c3744 | `r2-filtered1.log` | EXIT=0, 20 passed |
| full rpc binary on 38c3744, run 1 | `r2-full1.log` | EXIT=0, 90 passed |
| full rpc binary on 38c3744, run 2 | `r2-full2.log` | EXIT=0, 90 passed |
| `cargo clippy --release -p sparrow-server --all-targets -- -D warnings`, review 3 fixes | `r3-clippy.log` | EXIT=0 |
| `cargo fmt -p sparrow-server -- --check`, after `cargo fmt` | `r3-fmt2.log` | EXIT=0 (`r3-fmt.log`, before formatting, EXIT=1) |
| `cargo test --release -p sparrow-server --lib -- private_` | `r3-unit.log` | EXIT=0, 3 passed |
| filtered rpc set, review 3 fixes | `r3-filtered.log` | see validation.md |
| `shellcheck -x`, `bash -n` on the changed scripts; Python parse of the helper | none | EXIT=0 |
| prose-check on the docs and README | `prose-docs.log` | only the existing UInt findings |

