VERDICT: PASS

## Scope
Issue 20: receipt-time fallback for event time, commit 6442d32.

## Evidence
- Focused release builds, tests, clippy and fmt: logs/ in this stage dir (r1 and r4 logs), all EXIT=0 after fixes.
- Full pre-commit gate (make check) passed on the commit: /tmp/commit-20.log, EXIT=0.
- Independent review: reviews/review-1.md BLOCKED, review-2.md BLOCKED, review-3.md PASS.
- Not run: the on-host UAT (needs a deploy of schema 69 and a restart).

## Findings
None required. Optional O7 and O8 from the reviews are not taken.
