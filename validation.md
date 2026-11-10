# #158 validation

VERDICT: PENDING_REVIEW (review-5 to review-9 were BLOCKED at earlier heads; the findings of each are addressed or listed; no review has passed yet)

## Scope

Branch `paudley/158-serve-every-query-and-status-call`, PR #169 (WIP). Head a9f1fe9. Earlier head 0f2857c, which
contains the merge f5788dd of origin/main f1219ee (schema 71). Supersedes every earlier
version of this file, including the BLOCKED verdict at R1/R2/R5, which described a
tree that no longer exists. The temporary probe `measure_158.rs` is outside the worktree.

## State of the earlier findings

- review-5 (BLOCKED) R1 (a finished build lost if its page is not delivered), R2 (a build
  thread panic leaves the build running), R3 (no test that `read` advances): fixed in b3162ae.
  review-6 (opus, BLOCKED at 0f2857c) found that the retry check in 0f2857c cancelled before the build was reached. It is replaced by a dev-only delivery-failure hook, for sources and live artifacts. review-7 (opus, BLOCKED at ce095e2) found the hook fired before the pre-fix retirement point; it now fires after the page is built and before the build is retired (158-focused7.log). A call that starts a build and fails now retires it.
- R4 (stale measurements, validation and PR body): this file, `pr-body.md` and the PR body.
- Optional O5 to O8: docs updated (build limit applies to window continuations; a page with
  `build` has no rows; the memory-charging wording; "counter seeding" corrected). O5 (the missing abandoned-artifact-build test) is still open. O6 (a build thread keeps a store handle until its lease ends) is a documented limit.

## Evidence (each log ends with its own EXIT line; all under /home/paudley/Active/sparrow/.stage/158/)

| Check | Log | Result |
| --- | --- | --- |
| Commit hook, 8ba701d | 158-commit2.log | EXIT=0 |
| Clippy after review-5 fixes | 158-clippy5.log | EXIT=0 |
| Listing-build and status tests | 158-focused4.log | EXIT=0, 3 passed |
| Commit hook, b3162ae | 158-commit4.log | EXIT=0 |
| Sync onto f1219ee | 158-sync1.log, 158-sync2.log | EXIT=1 (review required), EXIT=0 (completed) |
| Clippy on the merged tree | 158-clippy7.log | EXIT=0 |
| Listing-build tests with the retry check (superseded) | 158-focused5.log | EXIT=0, 2 passed |
| Listing-build tests with the delivery-failure hook, sources and artifacts (earlier placement) | 158-focused6.log | EXIT=0, 2 passed |
| Commit hook, ce095e2 | 158-commit7.log | EXIT=0 |
| Same tests, hook after the page is built | 158-focused7.log | EXIT=0, 2 passed |
| Commit hook, 950a741 | 158-commit8.log | EXIT=0 |
| Commit hook, a9f1fe9 | 158-commit9.log | EXIT=0 |
| Commit hook, 0f2857c | 158-commit6.log | EXIT=0 (158-commit5.log EXIT=1: fmt-check) |
| Full rpc target, merged tree | 158-rpc4.log | EXIT=0, 91 passed |
| Probe 300k, schema 71 | 158-measure-300.log | EXIT=0 |
| Probe 2.67M, schema 71 | 158-measure-2670.log | EXIT=0, 15321.65 s |

Not logged: the hook output of the merge commit f5788dd itself (stagectl's continue reported
completion only). `158-clippy6-wrongdir.log` ran in the main checkout and is not evidence.
Superseded runs kept for the record: `158-measure-300-schema70.log`,
`158-measure-2670-schema70-stopped.log` (stopped at the schema 71 merge, not a result),
`158-measure-300-restarted-for-notification.log`.

## Latency at the live size (2.67M records, schema 71, all five migrations pending)

p50 / p95 / max ms, 20 samples each, scope CPUQuota=100% IOWeight=20 MemoryHigh=2G
MemoryMax=4G, nice 10.

| Read | During | After |
| --- | --- | --- |
| status | 0.78 / 94 / 199 | 0.69 / 0.92 / 173 |
| status, writer stalled | 1.01 / 52 / 52 | |
| context, dense | 1.3 / 18.9 / 24 | 0.80 / 1.07 / 5.97 |
| context, rare | 26527 / 41903 / 83415 | 0.46 / 0.69 / 2.9 |
| sources page | 5000 / 5073 / 5099 | 0.09 / 0.14 / 0.50 |
| artifacts page | 5000 / 5069 / 5085 | 15.0 / 38.1 / 122 |
| records, one source | 27687 / 48304 / 48736 | 2475 / 3324 / 8512 |
| SPARQL | 82 / 889 / 1459 | 1.51 / 29.2 / 178 |

Migrations: 6029 pages, 824.5 s in all (300k: 682 pages, 39.4 s). Source-summary alone is
not timed by the probe.

Cursor pages to a fresh listing: sources 103 to 174 pages, 514.6 to 866.6 s; artifacts 107
to 139 pages, 534.7 to 696.6 s; slowest page 6.47 s. This is the input for REQUIRED #176.

## Open

- Median `context_rare` and `records` meet 30 s during the migrations; their p95 and max do
  not. The build-and-cursor treatment is deferred to a separate REQUIRED issue (coordinator
  ruling), not in this PR.
- Independent re-review at the final head: pending.
