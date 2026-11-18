# Issue #348: Flaky test: expiry_below_the_floor_hands_its_retirement_to_the_pressure_wait failed once with ENOLCK on a temp store

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Failed once in the commit hook (`make check`) of the merge of main into PR #346 (#324), while the host load average was 60 to 90. Unrelated to that change. It passed alone on an unchanged rerun (`.stage/evidence-keep/324/floor-test.log`, EXIT=0, 17.8 s) and the full hook then passed.

Failure (`.stage/evidence-keep/324/` first sync log was not kept; message as read): `called Result::unwrap() on an Err value: Error { message: "IO error: lock hold by current process, acquire time ... acquiring thread ...: /tmp/.tmpXXXXXX/LOCK: No locks available" }` in `expiry_below_the_floor_hands_its_retirement_to_the_pressure_wait` (`crates/sparrow-core/tests/storage.rs`).

"No locks available" is ENOLCK from a RocksDB LOCK file on a temporary directory. Read the test and the store-open path for a real cause (a temp directory on a filesystem without POSIX locks, a reused path, a second open of the same directory, a leaked handle), not only load. Acceptance: the cause is shown from the code, and the test or the code is corrected so the failure cannot recur, or the issue records why it is the host's fault with evidence.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
