# Issue #349: Flaky test: cancelled_rpc_keeps_worker_admission_until_storage_work_finishes failed once with RecvError

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Failed once in the commit hook (`make check`) of the merge of main into PR #346 (#324), under a host load average of 60 to 90. Unrelated to that change. It passed alone on an unchanged rerun (`.stage/evidence-keep/324/rpc-test.log`, EXIT=0, 5.8 s) and the full hook then passed.

Failure (`.stage/evidence-keep/324/sync-continue.log` lines 636 and 720-731): `rpc::tests::cancelled_rpc_keeps_worker_admission_until_storage_work_finishes` panicked at `crates/sparrow-server/src/rpc.rs:4497`, `called Result::unwrap() on an Err value: RecvError(())`, i.e. the `release.send(())`/channel peer was gone before the test released it, in the loop that aborts a task or drops its connection guard.

Read the test and `query_work` for a real race (a worker that returns, or a cancelled task that drops the `paused` receiver, before `release.send`) instead of calling it load. Acceptance: the cause is shown from the code, and the test or the code is corrected.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
