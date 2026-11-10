Reviewer model: claude-opus-5-5 (opus)

# Delivery re-review: Sparrow PR #196 (issue #195), head b8f3d9274e5b3584b57c3691495805379b639d2e

## Scope

- This recheck covers the new commit b8f3d92 (`git diff 06a1bc0 b8f3d92`: README +1 -1,
  `deploy/user-unit-upgrade.sh` +5 -1, `tools/deploy-scripts-check.sh` deleted,
  `tools/user-unit-upgrade-timers-check.sh` +26 -3) and the four findings left open by
  `.stage/195-review.md`. The whole PR is now `git diff main...b8f3d92`, four files, +38 -6.
  `deploy/verify-backup.sh` is unchanged since the first review, which found it correct.
- Read: the incremental diff; `tools/user-unit-upgrade-timers-check.sh` in full;
  `.stage/195-shell-check.log`; the head and tail of `.stage/195-commit-2.log` plus a search for
  failures in it; the head, mtime and commit dates; the PR comment
  (`/api/v1/repos/fleet/sparrow/issues/comments/12008`, created 2026-11-10T22:40:42Z); the current PR
  body and head (`/pulls/196`, head b8f3d92, open); the `install-hooks` target in the Makefile.
- Not run: cargo, the gate, the harness, the deploy scripts. Nothing outside the worktree and `.stage`
  was read, apart from the two Forgejo API reads for the PR record.

## Evidence

1. **Help text (old REQUIRED 1): fixed.** Line 71 now gives the `SPARROW_INTAKE` default as
   `~/.local/state/sparrow/intake`. Line 70 adds `SPARROW_STATE` with default `~/.local/state/sparrow`.
   Both match the code at lines 149-150.
2. **README (old REQUIRED 2): fixed.** `deploy/README.md:438` now reads `... server.toml
   %h/.local/state/sparrow/intake %h/.config/sparrow %h/.local/state/sparrow/ship
   %h/.local/state/sparrow/ship-watches`. That is the order and set of paths the script passes at
   lines 263-264. It also matches the pre-switch user unit. No other user-unit statement in README or
   RECOVERY.md names `/SPARROW/intake` as the user default. RECOVERY.md:276 only names the variable.
3. **Case 4 (old REQUIRED 3): a real behavioral check; the mirror file is deleted.**
   - Case 4 runs the real `user-unit-upgrade.sh` under `env -i`, without `--no-backup`, in a fake
     HOME. `SPARROW_PREFIX`, `SPARROW_STATE` and `SPARROW_INTAKE` are all absent from that
     environment, so every path comes from the script's own defaults.
   - The stub CLI's status now carries `"backup":{"phase":"idle"}`, which gets the run past the
     absent/unconfigured refusal.
   - The stub `sparrow-backup` is `/bin/sh` and records `$SPARROW_PREFIX`, which it can see only if
     the script exports it, plus each argv entry. It then writes `sets/1/SOURCE` with the `image=`
     and `store_id=` lines the script greps for.
   - The case asserts an exact multi-line match of the prefix and the five arguments, and that the
     `pre-upgrade-sparrowd-schema68-aaa` link exists. It then completes the switch with exit 0.
   - Removing the export, changing a default, or reordering, dropping or adding an argument each
     fails the case. It protects both regressions in the issue on the real entry point.
   - Cases 1-3 still pass `--no-backup` through `script_args`. Dropping `SPARROW_INTAKE` from
     `run()` does not affect them, because the variable is read only in the backup branch.
   - `.stage/195-shell-check.log` shows four `ok` lines and `EXIT=0`, written at 16:34:53.
4. **Path guard (old OPTIONAL 5): no regression found.** The loop at lines 259-261 sits inside the
   backup branch, after the phase check and before `log "pre-upgrade backup"`. It refuses with
   `die` before the backup starts and before anything stops, so `--no-backup` never reaches it.
   It tests `-d`. The intake tree and the shipper's `state_dir` (`ship`) are directories, and
   `.config/wt.toml:33` lists `ship-watches/` as a directory. The installed unit's successful sets/6
   run (first review, item 3) shows all three exist on the host. Case 4 creates all three and passes,
   so the guard admits the valid layout. No case exercises the refusal; the issue does not require
   one.
5. **No other behavior change.** Apart from the items above, the incremental diff changes only the
   harness. The full PR diff against main contains nothing beyond the fixes reviewed here and in the
   first review.
6. **Hook logs: my old REQUIRED 4 was wrong and is withdrawn.** `195-shell-check.log` finished at
   16:34:53, and b8f3d92 is dated 16:35:04, 11 s later. The `make check` pre-commit hook (Makefile:
   `hook.sparrow-verify.event pre-commit`, `command 'make check'`) cannot run fmt, release Clippy,
   release tests and the census in 11 s. So git must have stamped the commit before the hook ran,
   not after, which is the reverse of my assumption in the first review. On that reading both logs
   fit their commits:
   - `195-commit.log` ran for about 15.5 min after the 16:12:59 stamp, including a 1m42s Clippy and
     a 9m50s test build.
   - `195-commit-2.log` ran for 5 min after the 16:35:04 stamp, mostly cached.

   `195-commit-2.log` itself contains `cargo fmt --all --check`, release Clippy with `-D warnings`,
   14 `test result: ok`, no `FAILED` or `error`, the census `OK`, and a final `EXIT=0` at line 1203.
   The comment states the scope correctly: the hook proves the Rust gate only, and the shell
   behavior rests on `195-shell-check.log`.
7. **PR body and comment.** The PR body is still the first version: it describes the deleted
   `tools/deploy-scripts-check.sh` and cites `195-commit.log`. Comment #12008 replaces it point by
   point. It covers the guard, the help and README fixes, the gate's actual scope, case 4 with its
   log, the untested one-line `rm`, and the unproven live run, and it keeps `Refs #195`.
   `prose-check` was not rerun on the comment; it is plain declarative text. Merged commit a77756a
   (#194) does not carry its PR body verbatim, so the stale body does not appear to become the merge
   record on its own. I accept the comment as the record.

## Findings

**REQUIRED 1: the harness prints `PASS` before case 4 runs, and its header is stale.**
`tools/user-unit-upgrade-timers-check.sh` still has `echo PASS` at the old end, line 142. Case 4 was
appended after it, so `195-shell-check.log` reads `PASS` and then the `ok   backup` line. If case 4
fails, the run prints `PASS` and then `FAIL: ...` and exits 1. That breaks the script's own stated
contract (line 18: "Prints "PASS" and exits 0, or names the failed assertion and exits 1"). It also
makes a grep for `PASS` or a tailed summary report a pass for a failed run. The header (lines 5-15)
also still says it checks only timer handling and "runs the whole script three times". Fix: move
`echo PASS` after case 4, and update the header to name the #195 case and four runs. These are
two-line changes, and the harness should be rerun afterwards.

**OPTIONAL 2.** The `--help` precondition list (lines 25-35) does not name the new refusal (a
missing intake, `ship` or `ship-watches`). The script's comment promises that every check that can
refuse runs before anything changes, and that still holds; only the help list is incomplete. The
new `SPARROW_STATE` row on line 70 also has its default one column out of alignment.

**OPTIONAL 3.** If Forgejo's API or UI can edit the PR body, which stagectl's limitation does not
rule out, replace the stale body with the comment's text so the PR's front page does not describe a
deleted file.

**Open state (not a PR finding).** The live run without `--no-backup` remains unproven, as declared.
`Refs #195` keeps the issue open for it.

VERDICT: BLOCKED
