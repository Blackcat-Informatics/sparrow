Reviewer model: claude-opus-5-5 (opus)

# Delivery review 1: Sparrow #175 (PR #177), head ba90491

## Scope

- Diff `f1219ee..ba90491` (one commit, ba90491d5d3f123f57ffc858af0815620a0a0534). Files: `deploy/user-unit-upgrade.sh`, `deploy/README.md`, `docs/deployment.md`, `tools/user-unit-upgrade-timers-check.sh` (new, mode 100755).
- Contract: verbatim `issue.md` (Work 1-3 and Done when) and the coordination ruling recorded in `plan.md` `## User rulings` (fix it in the script through an issue and PR, not by hand). The plan's exclusions (lib.sh and other scripts unchanged, live host untouched) hold: the diff touches only the four files.
- This was a static review. I did not run the upgrade script or the check, and I touched no unit, timer, store or daemon. I ran only read-only checks: `git diff`, greps, `shellcheck -x`, `bash -n` and `prose-check`.

## Evidence

- `/home/paudley/Active/sparrow/.stage/175/check-run2.log`: three `ok` lines, `PASS`, `EXIT=0`. The recorded stub call logs in `.stage/175/check-run2/*/state/calls` agree with it:
  - `success` ends `start sparrow.service`, `start sparrow-ship.timer`, then the two `is-active` probes. No start of the backup timer.
  - `abort-both` contains `start sparrow-ship.timer sparrow-backup.timer`. That call can only come from the restore at line 285, so the run did reach the abort path.
  - `abort-ship-only` contains `start sparrow-ship.timer` only.
- `shellcheck.log`: `EXIT=0`, `bash-n EXIT=0`. I reran both myself: `shellcheck -x` on both scripts gave SC_EXIT=0, and `bash -n` on both gave EXIT 0.
- `commit.log` ends with the hook's census lines and `EXIT=0`.
- `prose-check` on `.stage/175/added-prose.txt` (the added text): EXIT=0, no findings.
- Done-when grep: `grep -n 'sparrow-backup.timer' deploy/user-unit-upgrade.sh` shows these code uses:
  - 230: the FAILED report's `is-active`.
  - 272: the was-active probe.
  - 274: the stop.
  - 284: `restore+=(sparrow-backup.timer)` guarded by `((backup_timer_was_active))`. This is the stated was-active restore on the abort path.
  - 354: `is-active` in the log.
  - 359 and 362: printed text.
  - The final start at 352 names `sparrow-ship.timer` only. No other path starts the backup timer.

### Checks against the caller's points

1. **End of switch and abort path.** Line 352 starts only the ship timer. Lines 359-362 print the steps: install the online lines, take the first online backup, run `deploy/verify-backup.sh SET` (SET = `$SPARROW_BACKUPS/sets/ID`), and only after exit 0 run `systemctl --user start sparrow-backup.timer`. Lines 270-272 record per-timer flags before the stop at 274. The abort path (282-286) starts only the flagged timers and names them in the `die` message.
   - **set -e:** a failing `is-active --quiet ... && flag=1` and `((0)) && restore+=(...)` at top level do not trigger errexit, because only the last command of an `&&` list counts. `((${#restore[@]} == 0)) || start ...` never expands an empty array. `${restore[*]:-none}` is safe under `set -u`. At line 354, `is-active` returns 3 inside the arguments of `log`, which errexit ignores. The success case confirms this at runtime: the backup timer is inactive there and the run still exits 0.
   - `verify-backup.sh` takes exactly one positional BACKUP set (`(($# == 1))`, `realpath -e -- "$1"`), so the printed step uses the real interface.
2. **Schema wording.** Line 8 now reads "68 to 71". The remaining mentions of 70 (lines 113, 116, 117, 332) describe what `retire-claim` and `counts` can do from schema 70 on. None names a target. The examples that name real kept images (21, 55, 11) are unchanged.
3. **Docs and --help.** `--help` (header, `--no-backup` note, and all three changed recovery bullets) and `deploy/README.md` "Interim user unit" agree with the behavior. The new paragraph in `docs/deployment.md` (928-936) agrees too, but older lines in the same section were not updated. See finding R1.
4. **Stubbed check.** The check runs the real `deploy/user-unit-upgrade.sh` under `env -i` with a fake HOME, and puts stub `systemctl`, `getcap`, `lsattr`, `ps` and `sleep` first on PATH.
   - **Stubs are faithful.** `show -p X --value UNIT` puts the property at `$3` and the unit at `$5`. `is-active --quiet` is handled. `start` and `stop` change state files. `daemon-reload`, `status` and `LimitNOFILESoft` answer. The CLI images answer `status` with their own schema, so the switch moves the link from 68 to 71 and the schema gate passes. lib.sh calls nothing by absolute path, and the stub logs every call. The recorded calls show the whole run went through the stub.
   - **Writes stay in scratch.** `install -D`, `swap_link` and the state files all land under `SCRATCH/NAME`. `XDG_CONFIG_HOME` is cleared by `env -i`. The world refuses to reuse an existing directory.
   - **The assertions bite.** If the script started both timers at the end (the old behavior), cases 1 and 3 would fail. If the abort path restored nothing, case 2 would fail. If it restored both unconditionally, case 3 would fail.

## Findings

**R1 (REQUIRED).** `docs/deployment.md` still contradicts the new behavior in the section this PR edited. The issue's Done-when says "Docs agree", and the PR body says so.
- Line 954: "...the watches and the ingest socket, starts the timers, and exits 0." The script now starts only `sparrow-ship.timer`.
- Line 963: the after-start recovery says "confirm the store identity and schema, then start the timers". `--help` (lines 86-88) now says start `sparrow-ship.timer`, and start `sparrow-backup.timer` only after `deploy/verify-backup.sh SET` exits 0.
- Line 1019, "Claims after a rollback" step 6: "Stop the newer query-only daemon and start both timers." The changed `--help` rollback note (lines 95-96) now says start `sparrow.service` and `sparrow-ship.timer`, and start the backup timer only after the restored store's first online backup verifies.

An operator following `docs/deployment.md` would start the backup timer that this issue exists to keep stopped. The fix is to reword those three places to match `--help`.

**O1 (OPTIONAL).** With `--no-backup`, the end of the run prints the "take the first online backup" command twice: once in the `NO_BACKUP` block (355-358) and again as step 1 (360). The `NO_BACKUP` block could keep only its warning line.

**O2 (OPTIONAL).** The printed step 2 is the bare `deploy/verify-backup.sh SET`, as the issue words it. `verify-backup.sh` defaults `SPARROW_CONFIG` to `/etc/sparrow/server.toml` and dies if it cannot read it (lines 263-264). On the interim user-unit host the configuration is `~/.config/sparrow/server.toml`. Printing `SPARROW_CONFIG=$SPARROW_CONFIG deploy/verify-backup.sh SET` would avoid a first-try failure. The current form is not dangerous: it fails loudly with an actionable message and never reports exit 0. I inferred this from the source; I did not check whether `/etc/sparrow/server.toml` exists on the live host.

**O3 (OPTIONAL).** Abort cases 2 and 3 do not assert that the abort was reached. They check only `rc != 0` and the final timer states, so a precondition failure before "stop timers" would pass them without testing anything. Today case 1 builds the same world and must succeed first, and the recorded call logs show the restore ran. Asserting the `die` text or the `stop sparrow-ship.timer sparrow-backup.timer` call would make the cases stand on their own.

VERDICT: BLOCKED
