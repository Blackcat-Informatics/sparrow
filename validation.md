# #176 validation

VERDICT: PASS

Final pushed head 4354a4a4b1ef: `review-7.md` VERDICT: PASS, `hook7.log` EXIT=0.

## Scope

PR #190, branch `paudley/176-keep-the-source-summary-owed-completion`, base bce6adae2d56.
Run order (`Migration::RUN_ORDER`), listing build re-base (`listing_build::Worker`,
`source_summary::{Base, Pass}`, `source_read::source_capture`,
`artifact_read::owed_index_rows`), per-migration journal timing (`sparrow-server`
maintenance), docs (interfaces, architecture section 7, deployment) and helpers ledger.

## Evidence

Logs are under `/home/paudley/Active/sparrow/.stage/176/`, each with its exit status inside.

- `clippy1.log`: focused release clippy, core and server, all targets, warnings denied,
  first revision, EXIT=0.
- `test1.log`: ten selected release `storage` tests, EXIT=101 (eight passed; the two new
  re-base tests failed on fixture limits, data budget and handle count). `test3.log`:
  the two after the fixture fixes, EXIT=0.
- `hook1.log`: commit hook (`make check`: fmt, release workspace clippy with warnings
  denied, release workspace tests, census self-test and pinned cross check) for
  955948bd7f3b, EXIT=0, 12:27:40Z to 12:37:09Z (about 9.5 minutes, so one commit per
  review round). `storage` 246 passed. `push1.log` EXIT=0; `pr-create.log` PR #190 EXIT=0.
- `review-1.md` at 955948bd7f3b: VERDICT: PASS, four optional findings, fixed in
  155f4b43fefd (checks clocked by catalog entries read, journal wait wording, "third
  online migration" wording, `RUN_ORDER` crate-only).
- `round2.log`: focused clippy plus ten `storage` and five `maintenance` tests on the
  fixes, EXIT=0. `round2b.log`: the two re-base tests with the source bound tightened to
  four entries, EXIT=0.
- `hook2.log`: hook for 155f4b43fefd, EXIT=0, 12:58:43Z to 13:07:03Z. `push2.log` EXIT=0.
- `review-2.md` at 155f4b43fefd: VERDICT: PASS, two optional comment findings, fixed in
  c6622845fe9c. `hook3.log` EXIT=0 (13:18:55Z to 13:31:58Z), `push3.log` EXIT=0.
- `review-3.md` at c6622845fe9c: VERDICT: PASS, two optional comment findings, fixed in
  8bd913cce111. `hook4.log` EXIT=0 (13:43:24Z to 13:53:15Z), `push4.log` EXIT=0.
- `review-4.md` at 8bd913cce111: VERDICT: PASS, one optional docs reflow finding, fixed in
  3b996eb41200 together with a misplaced paragraph found while reflowing. `hook5.log`
  EXIT=0, `push5.log` EXIT=0.
- `review-5.md` at 3b996eb41200: VERDICT: BLOCKED, one required docs finding (the fair
  selection sentence read as describing `status`) and four optional wording findings plus
  this file's missing hook5/push5 entries; fixed in 3d40401ebca9, except the
  architecture half of the "what it owes" wording, finished in 4354a4a4b1ef. `hook6.log` EXIT=0
  (14:27:30Z to 14:33:30Z, `storage` 246 passed), `push6.log` EXIT=0.
- `review-6.md` at 3d40401ebca9: VERDICT: PASS, one optional architecture sentence finding
  (fixed in 4354a4a4b1ef) and optional record findings in this file and `review-debt.md`
  (fixed). `hook7.log` EXIT=0 (14:44:27Z to 14:48:18Z, `storage` 246 passed), `push7.log`
  EXIT=0.
- `review-7.md` at 4354a4a4b1ef: VERDICT: PASS, two optional record findings in this file
  and `review-debt.md`, fixed there.
- `prose.log`: prose-check on the edited docs reports only pre-existing `UInt64`/`UInt32`
  lines outside the edited sections. Later prose-check runs on each round's edits, commit
  messages and squash notes were interactive and kept no log; review-6 reran them and
  recorded the same result.

## Findings

None open. The 2.67-million-record measurement has not been run; it waits for
coordination's go-ahead.
