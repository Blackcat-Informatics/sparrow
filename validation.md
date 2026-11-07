# Validation

VERDICT: PASS

## Scope
Step 1 of issue 36 at commit 9c91a97: paged event-fallback job, skip rule, shared pager hook.

## Evidence
- Review: reviews/review-1.md found no code defects; its one required finding, the measurement on a nightly copy under unit-like limits, is recorded below.
- Commit hook (full make check) passed, log /tmp/commit-36.log, EXIT=0.
- Measurement: image 9c91a97, copy 20261107T100413Z, scope CPUQuota=100%, IOWeight=20, MemoryHigh=2G, MemoryMax=4G, live disk budget. The open took between 30 and 34 minutes (07:49:12 to 08:23:37 CST, 2065 s including shutdown), down from about 76 minutes. Log /tmp/vb-1107-9c91a97.log.
- Synthetic store: 151,000 graphs in 87 to 105 s, unconstrained.

## Findings
The acceptance bar of 10 minutes is not met. Step 2, the online duty, is required and stays under issue 36. This delivery is a partial gain and does not close the issue.
