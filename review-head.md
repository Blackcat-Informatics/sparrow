# Independent recheck: #314 at a39f536 (delta since d100b7f)

- Head reviewed: `a39f5362fa6eab87279d45034f76e80b5e70d5a5`, branch `paudley/314-sparrow-ship-re-ships-every-source-file`. `git rev-parse HEAD` checked at the start and again at 2026-11-15 19:22:07 -0600. Its parent is `d100b7f`, and the worktree is clean.
- Read-only: no cargo or build was run, and nothing in the worktree was modified. The prior review at `d100b7f` is `.stage/314/review.md`; this recheck judges only the delta and the findings that review left open.

## Scope

Read:
- `git diff d100b7f..a39f536` in full (3 files, +14/-5).
- `deploy/libexec/sparrow-ship`: docstring 28-37, `ledger_keys` 108-126, ledger read 307-310 and write path 314-331 (unchanged since d100b7f; still open with `"a"` and the default newline handling, so `"\n"` is written as `"\n"` and a `\r` in a name is written raw).
- `crates/sparrow-server/tests/rpc.rs` 7147-7203 (the updated test).
- `docs/deployment.md` 395-404 and 446-452.
- `.stage/314/focused2.log` (header and failure, lines 1-27), `focused3.log` (whole), `commit2.log` (head, test-result lines, tail), and the branch reflog.
- `git grep` over `docs` and `deploy` for any remaining statement that the ledger key includes the device.

Not read:
- The verbatim #314 issue body. The Forgejo remote is not reachable with `gh`, and there is no `issue.md`, `plan.md` or `validation.md` under `.stage/314`.
- Unchanged code outside the lines above, which was read at `d100b7f`.

## Evidence

- Reviewer probe (not cargo, outside the worktree): `.stage/314/review-probe2.sh` ran the worktree script at `a39f536` against scratch trees under `.stage/314/review-probe2/`; log `.stage/314/review-probe2.log`, EXIT=0. Observed:
  - A missing ledger queues 1 and a rerun queues 0. An empty ledger queues 1.
  - A ledger whose last byte is not a newline (the trailing `\n` was stripped) reruns with queued 0, so the last line is still read.
  - Names holding `\r`, `\f` and U+2028 ship (queued 4), and the rerun queues 0. `od -c` shows each written as one ledger line with the separator kept inside the name.
  - A blank line inside the ledger still fails loudly at `s.ledger:2`.
  - A CRLF-terminated line keeps its `\r` in the name column, and the key still parses.
- `focused2.log`: worktree, header `d100b7f` (delta uncommitted), the updated test run against the earlier `splitlines()` reader. It FAILED at rpc.rs:7179 with `reports.ledger:3: ledger key 'name.json'`, EXIT=101. That shows the new case fails on the previous behaviour.
- `focused3.log`: same tree, the final code (only `.stage` logs differ from the commit), 3 shipper tests ok, `EXIT=0`, closed at 18:58:39.
- `commit2.log`: worktree, header `d100b7f`, the full `make check` output with the three shipper tests ok (lines 1069-1100) and the census cross gate OK, `EXIT=0`, closed at 19:17:58. The reflog records `a39f536` at 18:58:45, which is the start time git caches for its dates before the pre-commit hook runs. That is consistent with this log being the hook's verification of `a39f536`. The log itself does not print the new hash.
- `prose-check` on the changed docstring, `ledger_keys` comment and both `deployment.md` passages: EXIT=0.

## Checks

1. `newline=""`: Python performs no newline translation on read, so a `\r` inside a name stays in its line. `split("\n")` then splits only on the writer's terminator. A trailing `""` from the final `\n` is popped, so an empty file gives no lines, a ledger without a final newline keeps its last line, and a blank line in the middle is still reported with its 1-based number. The missing-ledger guard (`exists()`, 309) is unchanged. The probe confirmed all of these. The encoding is the locale default, as before (`read_text()`), and it matches the writer.
2. Docs: `deployment.md:449` now says "inode, size and modification time". The remaining grep hits are correct: sparrow-ship:31, sparrow-ship:124 and deployment.md:400 describe the legacy four-field form, katamari-manager-spool:16 has the new key, and development-runtime.md:5442 and :6368 describe the daemon's consumption guard and a historical acceptance, not this ledger. The crash sentences now name their steps ("between queueing and recording the identity") in the docstring (34) and `deployment.md:402-403`.
3. Test: it still drives the real script through `ship()`, with no sleeps. The new file `odd\rname.json` raises the first run to `queued 3`, read from the script's stdout. On the `d100b7f` reader the second run exits 1 (the `report` closure asserts success), as `focused2.log` shows. On the pre-#314 script the `4242:` five-field line never matches, so `queued 0` fails, as reasoned at `d100b7f`. Rust's `str::lines` only drops a `\r` at the end of a line, so the rewrite keeps the embedded `\r` intact.

## Findings

The REQUIRED finding at `d100b7f` (`deployment.md:448`) is resolved. OPTIONAL 1 (line-break characters in names) is resolved except for a literal `\n` in a name, which the reader cannot tell apart from a line end. That edge is contrived and still fails loudly with the line number. OPTIONAL 5 (the crash sentences) is resolved.

**OPTIONAL:** the reworded docstring line, sparrow-ship:34, is 102 characters and was not reflowed. This is cosmetic; other lines in the file already exceed 100.

OPTIONAL 2, 3 and 4 from `review.md` are unchanged. None of them blocks.

Findings: no REQUIRED findings.

VERDICT: PASS at a39f5362fa6eab87279d45034f76e80b5e70d5a5
