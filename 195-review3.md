Reviewer model: claude-opus-5-5 (opus)

# Delivery re-review: Sparrow PR #196 (issue #195), head 983cfb811a37462fd4bf7bf4031753038f4974fd

## Scope

- This recheck covers one new commit, 983cfb8 (dated 2026-11-10 16:44:31 -0600), and the finding left
  open by `.stage/195-review2.md`. `git diff b8f3d92 983cfb8` touches two files, +9 -6:
  `deploy/user-unit-upgrade.sh` (two `--help` text lines) and `tools/user-unit-upgrade-timers-check.sh`
  (its header comment and the position of `echo PASS`).
- Also read: `.stage/195-shell-check-2.log` in full, and the head, tail, failure search, `EXIT` line and
  mtime of `.stage/195-commit-3.log`.
- Static only. I did not run the harness, the gate or the deploy scripts.

## Evidence

1. **`echo PASS` is now last.** The diff removes it after case 3 and adds it after case 4's `ok`
   line, at the end of the file. A failing case now exits 1 through `fail` before `PASS` prints, so the
   header line "Prints "PASS" and exits 0, or names the failed assertion and exits 1" is true.
   `195-shell-check-2.log` shows four `ok` lines, then `PASS`, then `EXIT=0`.
2. **The header is accurate.** It now names the #195 case: without `--no-backup` the script exports
   `SPARROW_PREFIX` and passes the service's arguments. Case 4 asserts exactly that. The header says
   "runs the whole script four times", and there are four `run` calls. It also says to "run it after
   any change to the script's timer or backup handling". "It needs python3" still holds, because the
   script's phase read uses `python3`.
3. **The `--help` text is correct.**
   - The precondition list now ends "...and, without --no-backup, the running daemon reports an online
     backup configured (...), and the intake, ship and ship-watches directories exist". That matches
     the guard at lines 259-261, which runs only in the backup branch and before anything changes.
   - The `SPARROW_STATE` row now places its default `~/.local/state/sparrow` in the same column as the
     `SPARROW_INTAKE` and `SPARROW_BIN` defaults.
4. **Both logs end in their own `EXIT=0`.**
   - `195-shell-check-2.log` ends `EXIT=0`. Its mtime, 16:44:31, matches the commit date.
   - `195-commit-3.log` ends `EXIT=0` at line 1198. It holds the full `make check` sequence: fmt, release
     Clippy with `-D warnings`, 14 `test result: ok`, no `FAILED` or `error` lines, the census `OK`. Its
     mtime is 16:48:03, about 3.5 min after the commit date. That matches the hook timing model in
     review 2 (the commit is dated, then the pre-commit `make check` runs).
5. **Nothing else changed.** The delta contains only the text and harness lines above. There is no
   change to script logic, the backup call, the guard, `verify-backup.sh` or the README. Every
   behavioral finding from reviews 1 and 2 therefore stands as closed.

## Findings

**OPTIONAL 1.** The edited precondition line in `--help`, line 35 (`unconfigured), and the intake, ship
and ship-watches directories exist. Then, ...`), is about 145 characters long, while the text around it
wraps near 96. This is cosmetic and does not affect accuracy.

The REQUIRED finding from review 2 (`PASS` printed before case 4, and the stale header) is fixed. No
REQUIRED finding remains.

**Open state (not a PR finding).** The live run without `--no-backup`, which must write a complete
`sets/N` and then switch, is still unproven, as declared. `Refs #195` keeps the issue open for it. The
PR body is still stale, and comment #12008 is the accepted record (review 2, item 7).

VERDICT: PASS
