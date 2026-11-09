# Delivery review 2: #145 Measure online migrations with live intake and query traffic running

## Scope

- Recheck of review-1's R1 to R3 and O1 to O7, plus everything else the rework changed. Same
  worktree, still uncommitted against `main` 5813c33. Map: `implementation.md`, section
  "Review 1 rework".
- Orchestrator decisions taken as given. The `.github/workflows/ci.yaml` step was reverted;
  `git status` shows no change under `.github`. For R3, judge only what this branch adds
  beyond the requirements of the existing verify-backup tests: btrfs reflink and `b3sum`
  were already required on main (main's verify-backup.sh:318 and :539).
- Also judged: R2's exactness argument, whether R1's gates can pass without the watches
  carrying load, the invariant behind the O7 test fix, the new O2 refusal, and the
  `PROCESS_START` harness change.
- Not run here, as instructed: no cargo, no daemon, no live path. The implementer reports,
  all exit 0: fmt; release clippy on sparrow-server; the filtered rpc set 20 of 20, twice;
  the full rpc binary 89 of 89, five runs in a row.

## Evidence

| Check | Result |
| --- | --- |
| `bash -n` on verify-backup.sh, reconcile-inflight.sh, user-unit-upgrade.sh | exit 0 |
| `shellcheck -x` on the same three scripts | exit 0 |
| Python `ast.parse` of `deploy/libexec/sparrow-measure-load` | exit 0 |
| `prose-check docs/deployment.md docs/interfaces.md` | exit 1, and only the existing `UInt64`/`UInt32` findings (output filtered with `grep -v UInt` was empty) |
| `git status --short .github` | empty: the CI change is reverted |
| Read | helper (full), reconcile-inflight.sh (full), the reworked verify-backup.sh sections (prep 495-560, rehearsal 618-700, loop and gates 760-945), `PrivateIntake` (watches.rs 38-95), the watch_admin.rs diff, the tailing.rs `foreign` filter and its call sites, the two new unit tests, `measure_env` and `verify_backup_with` (rpc.rs 4743-4870), the R1 test, the O7 hunk, docs/deployment.md 565-690, docs/interfaces.md 528-545, deploy/lib.sh `free_bytes` |

## Rechecked findings

### R1. Replay that delivers no intake: partly resolved; one required gap remains (R1b)

What is resolved:

- `prepare_replay` (helper:94-181) now replays what the copy itself received in the window,
  read from its own catalog. Watch artifacts go into their watch's private root, through a
  scratch `settled` shipper source when ship.toml names one and directly otherwise. Every
  other artifact is re-sent over the copy's ingest socket.
- This removes the `daily` and `sealed` blind spots and the reads of producer directories.
  It is a faithful "recorded copy of the producers' input" in the issue's sense: the parts
  are replayed as live received them, at their receipt offsets, and a shipper interval
  batches them as live did.
- The run fails before the measured open (`FAIL prep`) when a ship.toml source names no watch
  root, when a ship.toml source has nothing in the window, or when the window is empty.
  Per-source counts are printed. The new test covers the empty and the unrooted cases.
- Release failures and shipper exit status are counted and gated (`replay_failures`).
- Socket replay is fully gated. Each ingest is a timed call, so every failure fails
  `rpc_calls_failed` and every slow call fails `rpc_calls_over_limit`.

What is not resolved: see R1b under Findings. Watch intake is gated only by
`watch_claims_completed >= 1`.

### R2. Backup count apart from replay: resolved

The exactness argument holds:

- The primary path (verify-backup.sh:529-560) opens the prep copy query-only with the image
  BACKUP/SOURCE records. A query-only daemon runs no maintenance worker, so no expiry,
  eviction or migration (interfaces.md:457-460). Its `live_records` is the count as the
  backup was taken, which is the same quantity as live's 860,209. Images older than schema 70
  report no `counts` field, and the existing `status_counts` treats them as exact.
- When the backup's image is missing or reports pending, the fallback is the measured copy's
  first status, used only if that is exact. Otherwise the line says `pending`, never a
  partial count. That is honest.
- `replay_added` pages through every record received since the load started
  (helper:184-197, releasing its snapshot). No backup record was received after
  `taken_utc`, so this count is the replay's alone.
- The final `live_records` is labeled as including the replay.

One caveat is listed as O8.

### R3. Gate environment: partly resolved; one new requirement remains (R3b)

- `measure_env` (rpc.rs:4793-4855) puts `systemd-run` and `systemctl` test doubles on PATH,
  so the new tests need no user manager and no loaded unit.
- The doubles record and report the requested properties. That makes the scope-limits
  comparison tautological in tests, but `ps` still reads the nice level and the class for
  real, and production keeps the real tools. This is acceptable for an external service
  dependency.
- `chrt` is util-linux and Python 3.11 is already required by main's tests (sparrow-ship's
  `tomllib`), so neither is new.
- R3b (below) is a requirement the new tests add that the existing tests do not have.

### O1 to O7

| Finding | State |
| --- | --- |
| O1 | Resolved. `ingest_get` matches `AgentError { code: "NOT_FOUND",` (reconcile-inflight.sh:113). `AgentError` derives `Debug` (agent/error.rs:7) and is the error the CLI's `main` returns. The retire capability now comes from the restored CLI's bare `watch` usage containing `watch retire-claim`, so a schema 70 refusal fails the run instead of exiting 3. |
| O2 | Resolved; correctness judged below. |
| O3 | Resolved. `foreign()` (tailing.rs:423-433) is applied at open, in `attach` (:158) and in `reconcile_missing` (:403), only under Private binding, so the Stored path is unchanged. `private_tail_root_leaves_foreign_generations_alone` protects it. The implementer's mutation runs show the test fails without each filter. |
| O4 | Resolved. Both `--help` texts now say a hard link and an unlink. |
| O5 | Resolved. verify-backup.sh:772-777 checks `kill -0 "$LOAD_PID"` at every poll. |
| O6 | Resolved. Migrations first seen complete at one poll share its interval and are printed with `shared_poll=` and `shared_pages=`. |
| O7 | Resolved; invariant judged below. |

### O2 refusal

Correct for the misuse it targets:

- `PrivateIntake::new` holds DIR canonicalized. `rebind` canonicalizes the configured root
  when it exists, or compares it as written when it does not (a copy from another host). It
  then refuses when either path is a component-wise prefix of the other.
- A hand run with DIR set to a live intake root's parent, or to the root itself, or to a
  directory inside it, is therefore refused at startup (catalog rebinding) or at `watchPut`
  (`INVALID_ARGUMENT`). The unit test covers all three cases.
- Persistence now writes the configured roots back (`Catalog::configured`, watch_admin.rs
  417-466, receipts included), so a private daemon never rewrites where a copy's catalog
  consumes. `restore_root` undoes the record after a refused put.
- This requires `reconcile-inflight.sh --private-intake DIR` to map each status root to
  `DIR/basename`. That is safer than the old `--only-under`, since no configured root is ever
  touched. The rehearsal puts its watch with a configured root outside DIR. The docs warn
  that the refusal covers configured roots only.
- Gap: the check compares paths, not identities. A bind mount or another alias that makes
  `DIR/NAME` the same directory as a live root would pass. This is optional (O9) because
  `source_file::root` already refuses symlinks.

### O7 test fix

The invariant is preserved and stated more precisely. The test protects consumption.rs
`recover`: "a replacement at its old path is never uploaded or removed on old ownership."
With `quiet_ms` at 600,000 (the validated maximum, config.rs:429) the replacement cannot
become claimable inside the test's waits. The wait now also requires `get_candidates() == 1`,
which shows the replacement is held as an independent new candidate rather than adopted by
the old claim. `live_records == 1` is now deterministic instead of racing a 10 s interval.
Production code is unchanged.

### Harness change

Holding `PROCESS_START` only across `admitted_spawn()` and then calling
`wait_with_output()` matches the existing pattern at rpc.rs:7859. Holding it for whole
verify-backup runs had stretched other tests' waits past the 60 s `CONNECTION_LEASE`, which
explains the "Peer disconnected" and lease failures in the implementer's logs. Each run uses
its own tempdir and scope name, so nothing depended on that serialization. Accepted.

## Findings

### REQUIRED

**R1b. The gates can pass while the watches carry almost none of the replayed intake.**
Watch intake is gated only by `watch_claims_completed >= 1` (helper:444-445). One completed
claim satisfies it, even if the watch then falls hours behind. A watch that completes its
first claim and is then refused (record limit, storage pressure, retries), or that publishes
each part in 30 to 44 s while parts arrive faster, records no deadline expiry and no replay
failure. The shipper also stops feeding a root once its budget is held, so released items pile
up in the scratch producer directories without any failure being counted. A copy whose intake
stalls carries less write load, so the pages and RPC gates also become easier to pass. That is
the false pass this mode exists to prevent.

docs/deployment.md's line "`replay_failures` ... so the run carried all the intake it
replayed" claims what the gate does not check: release and shipper exit status do not show
that the watches consumed anything.

Review-1's remedy listed "zero completed claims" as one failure condition, and that alone was
not enough. Required: gate on how much of the released watch intake the copy's watches
completed. For example, require completed claims at least equal to the watch items released
before `end - (ship interval + quiet + 45 s publication deadline)`. That bound is the
intake that could legitimately still be in flight. Print value, threshold and margin like the
other gates, correct the doc sentence, and add a breach case to the gate test. Shipped parts
are already within the shipper's split limits and direct items are one file each, so
released items map to claims about one to one. Tail watches may need their own counter.

**R3b. The new end-to-end tests need 50 GB free on the test scratch filesystem.**
`--measure-migration` refuses to start below 50 GB free and aborts below 40 GB
(verify-backup.sh:136, :398-403). The floor cannot be lowered. The three new runs
(the traffic test, the breach test, and the empty or unrooted test, whose prep refusal comes
after the disk guard) set no `SPARROW_MEASURE_MIN_FREE_GB`, so they need at least 50 GB free
where `CARGO_TARGET_TMPDIR` lives. No test on main passes the disk guard: the disk-floor test
exercises only its refusals. This is a requirement the existing verify-backup tests do not
have, which the orchestrator's criterion makes required. The guard reads `df` (lib.sh:440), so
a `df` double in `measure_env`, like the systemd doubles, would remove it without changing
production.

### OPTIONAL

- **O8.** The first-status fallback for `backup_live_records` reads a writable copy whose
  maintenance worker may already have run an expiry pass before the first answer. Records
  whose deadline passed after `taken_utc` would then be missing from a count labeled exact.
  The primary path through the backup's image is unaffected. Either note this in the docs or
  label the fallback's source accordingly.
- **O9.** The O2 refusal compares paths. Comparing the device and inode of `DIR/NAME` with the
  configured root's at open, when the root exists, would also catch bind-mount aliases.
- **O10.** A test runner under SCHED_IDLE needs `RLIMIT_NICE >= 20 - nice` for
  `chrt --other 0` (this host has 31). Otherwise the measured daemon fails to start. That
  only affects runners started from an idle-class session; one line in the test comment
  would save a diagnosis.
- **O11.** plan.md Behavior item 1 still describes replay from producer files. The Decisions
  section records the change, but the Behavior text is stale.

VERDICT: BLOCKED
