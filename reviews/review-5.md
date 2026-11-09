# Review 5: #145 at the pushed head 62ee39e

## Scope

Worktree `.worktrees/145-measure-online-migrations-with-live`, branch
`paudley/145-measure-online-migrations-with-live`, HEAD 62ee39e. It equals
`origin/paudley/145-measure-online-migrations-with-live`, and its parents are 682a7a0 (branch)
and 3735c1e (origin/main, #161). Outside `.stage/` the tracked tree is clean
(`git diff --quiet 62ee39e`, EXIT=0).

1. Changes since review 4: `git diff 682a7a0 62ee39e` on `src/backup.rs`, `src/rpc.rs`,
   `src/main.rs`, `src/config.rs` and `docs/interfaces.md`. This is O4-4: `Backup::Copy`,
   `without_backup_for_copy`, the `copy` flag in main.rs and the merged refusal-table row.
2. The whole delivery against main: `git diff 3735c1e 62ee39e` (18 files). I read the server
   core diffs, all of `deploy/verify-backup.sh`, `docs/deployment.md`, `deploy/lib.sh`,
   `deploy/user-unit-upgrade.sh` and `deploy/README.md`, and the lines `tests/rpc.rs` removes.
3. `pr-body.md` and `validation.md` checked against the code and the logs.

Rulings checked: coordination 2026-11-09, verbatim in plan.md ("(a). Port --with-traffic to
online backup sets ... a closed-store backup is forbidden ..."), and Patrick 2026-11-07 ("nothing
is optional ..."). O4-3 was accepted by coordination, so I did not reopen it.

This review was read-only. I ran no cargo command, no build and nothing against the live
service, store, sockets, intake or NAS.

## Evidence

Checks I ran myself:

| Check | Result |
| --- | --- |
| `bash -n` verify-backup.sh, reconcile-inflight.sh, user-unit-upgrade.sh, lib.sh | EXIT=0 each |
| `shellcheck -x` on the same four | EXIT=0, no output |
| Python `ast.parse` of `deploy/libexec/sparrow-measure-load` | EXIT=0 |
| `prose-check pr-body.md` | EXIT=0 |
| `git diff --stat 3735c1e 62ee39e` | sparrow-core, `libexec/sparrow-backup`, `pre-upgrade-backup.sh`, systemd units, agent/ and capnp schema are not touched |
| `find crates deploy docs -newer logs/r8-backup.log` | empty: no source edit after the r8 runs |

Logs I read (EXIT is in each file):

- r8-fmt.log EXIT=0. r8-clippy.log (release, sparrow-server) EXIT=0.
- r8-backup.log EXIT=0, 10 passed and 81 filtered out. The 10 include #161's
  `daemon_backup_refuses_a_local_root_and_reports_it_in_status` and
  `backup_client_refuses_a_local_backup_root_before_asking_the_daemon`, plus
  `query_only_and_no_intake_refuse_conflicting_configuration`, the disk-floor test and all three
  traffic tests. All r8 logs are newer than the last source edit (15:03:34).
- sync3-continue.log: "Sync: completed", EXIT=0. The sync journal shows the commit started
  21:15:29Z and completed 21:26:12Z with `validation_records: []`. The roughly 11-minute commit
  fits a hook `make check`, but the log holds no hook output and no test counts (inference, not
  checked).
- make-check-62ee39e.log holds only the SHA; it is still running.

O4-4 against the code:

- `copy = matches!(mode, Mode::NoIntake | Mode::PrivateIntake(_))` (main.rs:124) is computed
  before `private` moves. The gate (main.rs:300-309) calls `without_backup_for_copy()` for both
  copy modes. Otherwise it attaches the configured target only when not query-only. For a normal
  daemon this is the same as #161's `!intake` branch; for query-only and no-intake it is
  unchanged.
- `Backup::Copy` (backup.rs:17, :30-33) returns `UNSUPPORTED` with text naming both modes, and
  `encode` reports it as `unconfigured`. interfaces.md:2853-2855 and the merged row at :2868
  agree with that.
- No old name is left (`git grep` for `without_backup_for_no_intake`, `Backup::NoIntake`,
  `Self::NoIntake`: none).
- `check_private_intake` still refuses `backup_root` before the store opens (config.rs:315-324).

#161 intact, no closed-store path:

- The branch changes #161's code only by the rename and the message above. Every #161 file
  outside sparrow-server is untouched. The one #161 test the branch edits is the disk-floor test,
  moved onto the `df` double and still asserting #161's messages (review 4).
- In traffic mode the measured copy is `--restore-backup "$BACKUP_ROOT" "$BACKUP_ID" "$DATA"`
  (verify-backup.sh:493) and the prep copy a second restore (:562). `ship.toml` comes from
  `BACKUP/config/sparrow/` (:280), the record count from SOURCE `image`/`cli` and the set's
  `images/` (:332-352), and the free-space check counts two restores (:486-490). No line reads
  `$BACKUP/store`. The helper's only copy is the replay's `cp --reflink=auto` of the prep's
  exported artifacts into scratch (sparrow-measure-load:352). The rehearsal's reflink of the
  stopped scratch store (verify-backup.sh:716) is the stand-in review 4 accepted; it is not the
  measurement target.
- `json_field` moved from verify-backup.sh into lib.sh with identical code, and verify-backup.sh
  sources lib.sh (:144).

## Findings

### REQUIRED

**R5-1. The delivered rollback runbook still says the nightly backup waits for a pending claim,
which #161 made false.** docs/deployment.md:902-903 (new text in this branch): "retries it every
few seconds to 15 minutes, and every nightly backup waits for it". deploy/user-unit-upgrade.sh:97-99
(a paragraph this branch rewrote): "and every nightly backup waits for them". #161's online
backup does not look at claims at all. `deploy/libexec/sparrow-backup` asks the daemon for a
backup and polls `status.backup` (sparrow-backup:202-208), and #161 itself removed the same
claim from interfaces.md ("`sparrow-backup`, which waits for zero pending claims, fails every
night"). An operator following the rollback steps would be misled about backup state after a
restore.
Remedy: drop the clause in both places, or replace it with what holds now, for example "and the
watch reports that failure until the claim is retired". Run prose-check on both files.

**R5-2. The published PR body points to evidence that is not recorded, and validation.md is stale
at the final head.** pr-body.md:47-49 says the merge's hook run "with EXIT=0. Its log is
`sync3-continue.log` and its counts are in `validation.md`. It is the evidence for #161's own
backup tests on the merged tree." Problems:

- validation.md has no `sync3-continue.log` row and no counts.
- sync3-continue.log itself holds only "Sync: completed" and EXIT=0, with no hook output.
- pr-body.md was written at 15:14:39, before the commit completed at 15:26:12, so the claim was
  made before it was true.
- The Checks section (pr-body.md:50-54) cites only the r6 logs. They predate the R4-1 and O4-4
  code; the logs for the shipped code are r7 and r8.
- validation.md:3 still reads "VERDICT: BLOCKED ... the hook commit is held until #160's hook
  commit finishes", and its Scope (:7-8) names `ea1a4b2..HEAD` as origin/main to the branch.
  origin/main is now 3735c1e.

Remedy: when make-check-62ee39e.log finishes, record its EXIT and its test counts (the rpc binary
and sparrow-core backup tests). Add rows for that run and for sync3-continue.log (EXIT=0, commit
62ee39e, no counts in the log). Update validation.md's header and Scope (`3735c1e..62ee39e`).
In the PR body, cite the r8 logs (fmt, clippy, the 10 backup, copy-mode and traffic tests) as
the evidence for the final code. Make the hook and #161-test claim name make-check-62ee39e.log
and its counts, not validation.md. Then rerun prose-check and republish.

### OPTIONAL

- **O5-1.** pr-body.md:21-22 lists the private-intake mode's refused keys as "`consume`,
  `syslog`, `rpc_tcp_address` and `archive`" and leaves out `backup_root`, which
  config.rs:321-324 refuses. The Review section does mention it. Add `backup_root` to the list so
  the description matches interfaces.md and deployment.md.
- **O5-2.** No test covers the `Backup::Copy` refusal. A `backup` call on a `--no-intake` daemon
  should get `UNSUPPORTED` with `phase` `unconfigured`. The private-intake path cannot reach it,
  because startup refuses `backup_root`. Main did not test this either, and the change is a
  rename plus a message, so the contract does not need it.

VERDICT: BLOCKED

## Recheck at 1274cf1

### Scope

HEAD 1274cf1, parent 62ee39e. It equals `origin/paudley/145-measure-online-migrations-with-live`,
and outside `.stage/` the tracked tree is clean (`git diff --quiet HEAD`, EXIT=0).
`git diff 62ee39e 1274cf1` touches only `deploy/user-unit-upgrade.sh` (+4 -4) and
`docs/deployment.md` (+3 -2), so the code reviewed above is unchanged. I also read the
republished `pr-body.md`, `validation.md` and the new logs. As before, this was read-only: no
cargo command and nothing live.

### Evidence

Checks I ran myself:

| Check | Result |
| --- | --- |
| `grep 'nightly backup waits\|backup waits for'` in docs/deployment.md and deploy/user-unit-upgrade.sh | no match (EXIT=1) |
| `bash -n deploy/user-unit-upgrade.sh` | EXIT=0 |
| `shellcheck -x deploy/user-unit-upgrade.sh` | EXIT=0, no output |
| `prose-check docs/deployment.md` | EXIT=0 |
| `prose-check pr-body.md validation.md` | EXIT=0 |

Logs I read:

- make-check-62ee39e.log: the first line is the 62ee39e SHA. It runs fmt, release clippy with
  `-D warnings` on the whole workspace, release tests and both census gates, and ends `EXIT=0`.
  Counts: sparrow-client 22, sparrow-core 40, storage 236, census 50, gate 5, sparrow-server
  lib 32, sparrowd 8, rpc 91 of 91, test-support 1, with none failing. The census cross gate
  reports "OK: 321 ledger jobs". Every #161 test that validation.md names has an `... ok` line:
  `online_backup_runs_beside_continuous_ingest_and_restores_a_consistent_store`,
  `daemon_backup_refuses_a_local_root_and_reports_it_in_status`,
  `backup_client_refuses_a_local_backup_root_before_asking_the_daemon`,
  `backup_to_a_local_disk_fails_before_creating_anything_and_ingest_continues`,
  `storage::backup::tests::backup_root_must_be_a_network_filesystem_apart_from_the_store` and
  `storage::backup::tests::files_held_for_a_backup_do_not_count_and_the_guard_releases_them_near_the_floor`.
- r9-commit.log: the same gate sequence with the same counts (rpc 91 of 91, both sparrow-core
  backup tests ok, census cross OK). It ends with the `[... 1274cf1]` commit line and `EXIT=0`.
- pr-edit2.log EXIT=0 (16:12:48). It was written after pr-body.md (16:12:09), and its rendered
  body contains the Review 5 section, the r9-commit citation and `backup_root` in the
  refused-key list. prose-pr4.log EXIT=0.
- r9-prose-shell.log holds only `EXIT=0`, with no command or output, so the log cannot show
  its scope. I reran its three checks myself (above), and they pass.

### Findings

- **R5-1: resolved.** deployment.md:900-905 now says "and the watch reports that failure until
  the claim is retired". user-unit-upgrade.sh:97-101 now says "and it keeps reporting that until
  the claim is retired". Both agree with #161's backup, which never waits for claims. No other
  copy of the old clause is left in the docs or deploy scripts. `deploy/evidence/lifecycle.md`:1218
  is a dated evidence record from main and is not runbook text.
- **R5-2: resolved.**
  - The PR body's Checks (pr-body.md:47-60) now cite make-check-62ee39e.log and r9-commit.log
    with counts that match the logs, plus the r8 logs and r7-filtered.log for the post-review-4
    code.
  - validation.md's Scope reads `3735c1e..1274cf1`, and it has new rows for sync3-continue (with
    a note that its log has no hook output), make-check-62ee39e, push2/pr-edit, r9-prose-shell
    and r9-commit.
  - The header still reads BLOCKED pending this recheck, which is accurate.
- **O5-1: resolved** (pr-body.md:21). **O5-2: not adopted**, which is acceptable for the reason
  given above.

New, OPTIONAL:

- **O5-3.** r9-prose-shell.log records only an exit status. To make a log that can carry
  evidence on its own, write the commands and their output into it, as the r8 and r9-commit
  logs do. My reruns show the claim holds, so this is not blocking.

VERDICT: PASS
