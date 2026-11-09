# Validation: issue 145

VERDICT: PASS (review 5 recheck at 1274cf1; the real measurement on an online NAS set runs after merge, so #145 stays open)

## Scope

Branch `paudley/145-measure-online-migrations-with-live`. The diff range is
`3735c1e..1274cf1` (origin/main to the branch head). It covers the `sparrowd --private-intake DIR`
mode, `deploy/verify-backup.sh --measure-migration --with-traffic [--rehearse-rollback]`,
`deploy/libexec/sparrow-measure-load`, `deploy/reconcile-inflight.sh`, the rollback help
in `deploy/user-unit-upgrade.sh`, docs, and tests. The real measurement on the backup copy
is excluded: per the D118 ruling the orchestrator runs it after merge.

`deploy/reconcile-inflight.sh` belongs to this issue. It is the shared rollback tool of
plan Behavior items 3 and 4, required by the issue's two rollback-finding comments.

## Evidence

All logs are under `.stage/measure-online-migrations-with-live/logs/`.

| Check | Log | Result |
| --- | --- | --- |
| WIP commit 6bb5a7a through the pre-commit hook (`make check`: fmt, release clippy, release tests, census) | `wip-commit.log` | EXIT=0; rpc binary 89 of 89 passed, including the traffic, breach, empty-replay and disk-floor tests |
| `stagectl sync --issue 145` onto origin/main ea1a4b2 | `sync.log` | two conflicts (main.rs startup line, watches.rs tail owner), both sides kept |
| `cargo fmt -p sparrow-server -- --check` after the merge | `sync-fmt.log` | UNVERIFIED: EXIT=0 was printed by the wrapping shell, not written into the log |
| `cargo clippy --release -p sparrow-server --all-targets -- -D warnings` after the merge | `sync-clippy.log` | UNVERIFIED: EXIT=0 was printed by the wrapping shell, not written into the log |
| `stagectl sync --continue`, merge commit 38c3744 through the hook | `sync-continue.log` | EXIT=0 |
| prose-check `implementation.md plan.md` | `prose-notes.log` | UNVERIFIED as a log: EXIT=0 was printed by the wrapping shell; to be rerun with EXIT in the file |
| filtered rpc set, merged tree 38c3744 | `r2-filtered1.log` | UNVERIFIED: 20 passed, but EXIT=0 was printed by the wrapping shell, not written into the log |
| full rpc binary run 1, merged tree 38c3744 | `r2-full1.log` | UNVERIFIED: 90 passed, but EXIT=0 was printed by the wrapping shell, not written into the log |
| full rpc binary run 2, merged tree 38c3744 | `r2-full2.log` | UNVERIFIED: 90 passed, but EXIT=0 was printed by the wrapping shell, not written into the log |
| release clippy, review 3 fixes (before the error variant) | `r3-clippy.log` | EXIT=0 |
| `cargo fmt --check` after `cargo fmt`, review 3 fixes (before the error variant) | `r3-fmt2.log` | UNVERIFIED: EXIT=0 was printed by the wrapping shell, not written into the log; superseded by the fmt rerun on resume |
| `cargo test --release -p sparrow-server --lib -- private_`, review 3 fixes (before the error variant) | `r3-unit.log` | EXIT=0, 3 passed |
| filtered rpc set, review 3 fixes; its compile began about 05:19:06, before the error-variant edit at 05:19:35 | `r3-filtered.log` | EXIT=0, 20 passed |
| R-A probe: helper `run` with a missing `--ship` | none (scratch removed) | exit 1 at once, "worker(s) ship failed" |
| shellcheck -x, bash -n, helper parse; prose-check of docs and notes | `prose-docs.log` | prose-check EXIT=1 for the existing UInt findings only (shell-reported); shellcheck, bash -n and the parse EXIT=0 (shell-reported, no log) |
| PrivateRootShared variant (the reviewer's optional note): fmt, clippy, `private_` unit tests, hook | none | NOT RUN: coordination paused cargo until #160's PR opens |

Coordination rule (2026-11-09): a gate log must carry its EXIT inside the file. Only
`wip-commit.log`, `sync-continue.log`, `r3-clippy.log`, `r3-unit.log` and
`r3-filtered.log` do. The rows marked UNVERIFIED are cited as unverified in the PR body;
the hook's full rpc run on the final commit is the binding evidence.

| fmt check, final code | `r5-fmt.log` | EXIT=0 in file |
| release clippy, final code (`r4-clippy.log` EXIT=101 for missing field docs, fixed) | `r5-clippy.log` | EXIT=0 in file |
| `--lib private_` unit tests, final code | `r5-unit.log` | EXIT=0 in file, 3 passed |
| commit 682a7a0 through the hook (`make check`) | `r5-commit.log` | EXIT=0 in file, rpc 90 of 90 |
| sync onto 3735c1e | `sync2.log` | EXIT=1 in file: six conflicts; aborted (`sync2-abort.log` EXIT=0) |
| push; PR #162 | `push.log`, `pr-create.log` | EXIT=0 in file |

| port onto 3735c1e (merge staged): `cargo fmt --check` | `r6-fmt.log` | EXIT=0 in file |
| release clippy, sparrow-server all targets | `r6-clippy.log` | EXIT=0 in file |
| `--lib private_` unit tests | `r6-unit.log` | EXIT=0 in file, 3 passed |
| filtered rpc set (19: #161 removed the closed-store chunk test and renamed the linked-store test; `r3-names.txt` vs `r6-names.txt`) | `r6-filtered.log` | EXIT=0 in file, 19 passed, all three traffic tests and the disk floor |
| D118 target comment on #145 | `issue-comment.log` | EXIT=0 in file (issuecomment-9979) |
| prose-check of the PR body and the comment | `prose-pr2.log`, `prose-comment.log` | EXIT=0 in file |

| review 4 fixes (R4-1 backup_root refusal, O4-1, O4-2 two-restore test): fmt | `r7-fmt.log` | EXIT=0 in file |
| release clippy | `r7-clippy.log` | EXIT=0 in file |
| filtered rpc set | `r7-filtered.log` | EXIT=0 in file, 19 passed |
| shellcheck -x verify-backup.sh | `shellcheck2.log` | EXIT=0 in file |
| prose-check docs and README | `prose-docs2.log` | EXIT=1 in file, the existing UInt findings only |

| O4-4 (both copy modes use `Backup::Copy`): fmt | `r8-fmt.log` | EXIT=0 in file |
| release clippy | `r8-clippy.log` | EXIT=0 in file |
| rpc tests `backup no_intake query_only private` | `r8-backup.log` | EXIT=0 in file, 10 passed |

| `stagectl sync --issue 145 --continue`: merge 62ee39e of 3735c1e through the hook | `sync3-continue.log` | EXIT=0 in file; the hook's output is not in the log, so `make-check-62ee39e.log` supplies the counts |
| `make check` on 62ee39e (the log's first line is the SHA) | `make-check-62ee39e.log` | EXIT=0 in file. sparrow-client 22, sparrow-core 40 and storage 236, census 50 and gate 5, sparrow-server lib 32, sparrowd 8, rpc 91, test-support 1; 0 failed; census cross gate OK. #161's backup tests pass, including the daemon-level `online_backup_runs_beside_continuous_ingest_and_restores_a_consistent_store`, `daemon_backup_refuses_a_local_root_and_reports_it_in_status`, `backup_client_refuses_a_local_backup_root_before_asking_the_daemon`, `backup_to_a_local_disk_fails_before_creating_anything_and_ingest_continues`, and sparrow-core's `backup_root_must_be_a_network_filesystem_apart_from_the_store` and `files_held_for_a_backup_do_not_count_and_the_guard_releases_them_near_the_floor` |
| push 62ee39e; PR body republished | `push2.log`, `pr-edit.log` | EXIT=0 in file |
| review 5 R5-1 (rollback text no longer says the backup waits for claims): shellcheck -x, bash -n, prose-check docs/deployment.md | `r9-prose-shell.log` | EXIT=0 in file |
| commit 1274cf1 through the hook (`make check`) | `r9-commit.log` | EXIT=0 in file; the same counts as on 62ee39e, rpc 91 of 91, census cross gate OK |

## Findings

Review 3 (`reviews/review-3.md`): BLOCKED (R-A, R-B, R-C, O-a to O-e), all fixed. The
recheck is `VERDICT: PASS`, conditional on the fixed tree's filtered run (passed) and the
commit hook's full rpc run (pending). Its one new optional note, a dedicated error variant
for the shared-root refusal, is implemented (`consumption.rs` `Error::PrivateRootShared`)
and not yet compiled.

Review 4 (`reviews/review-4.md`): BLOCKED on R4-1, a private-intake copy could take online
backups. Fixed: `check_private_intake` refuses `backup_root`, with a test case and docs.
O4-1 and O4-2 are fixed. O4-3, running the consume worker during `backup_start`, was not
adopted: it would make the set's record count nondeterministic, which the exact
`backup_live_records=4` assertion (the R2 fix) depends on, and a worker owning the catalog
would recover the fixture's deliberately pending claim. The reviewer judged that the order is met as
written; this is reported to coordination for a ruling.

Review 4 recheck: `VERDICT: PASS`. Its new optional O4-4 (the wrong operator text for a backup
call on a private-intake daemon) is fixed: both copy modes use `Backup::Copy`
(`without_backup_for_copy`), whose message names both modes.


Review 5 (`reviews/review-5.md`) at 62ee39e: BLOCKED. O4-4 and #161's code are confirmed
intact. R5-1, the rollback text in `docs/deployment.md` and `deploy/user-unit-upgrade.sh`
saying every nightly backup waits for a pending claim, is fixed in 1274cf1. R5-2, the PR
body and this file citing hook counts that were never recorded, is fixed by the rows above
and the republished PR body. O5-1 is fixed: the PR body lists `backup_root` among the
refused keys. O5-2, a test that a `--no-intake` daemon refuses `backup`, is not added:
main had none and the contract does not require one.

Review 5 recheck at 1274cf1: `VERDICT: PASS`. Its optional O5-3, that `r9-prose-shell.log` did not show its
commands, is fixed: the log was rerun under `bash -x`.
