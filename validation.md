# Validation: query-only (#25)

VERDICT: PASS

## Scope
Branch query-only through 9e1e99e: the work reviewed in rounds 1 to 4 (1984bc2), plus a merge of
main (#24, schema 66) that moves this branch's markers to schema 67.

## Evidence
- Review round 4: `VERDICT: PASS` (review-r4.md) on cfe090e..1984bc2.
- Merge 9e1e99e: SCHEMA_VERSION is 67. The query-only status field and the READ_ONLY refusal are
  documented as schema 67. Per review-r4, the predates-the-line ranges now read "66 and older".
  The commit hook (`make check`) passed with 432 tests and census OK.
- The deploy/evidence transcripts keep schema 66 as historical records of the rehearsal images.

## Findings
- O1 (optional): "exited 0" in the comments is slightly stronger than `Result=success`, which
  systemd also reports for SIGINT or SIGTERM. This is safe, because no closing line is logged then.
- O2 (optional): rehearse the journalctl read under the system unit at the #22 cutover.
- Open, not merge-blocking: the issue's UAT steps 0 to 2 need #25 deployed live and a nightly
  backup taken afterwards. Step 3 (the #19 baseline) runs after the merge.
