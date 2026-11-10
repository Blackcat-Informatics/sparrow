Reviewer model: claude-opus-5-5 (opus)

# Delivery review 2 (recheck): Sparrow #175 (PR #177), head bd9b4cb

## Scope

- This recheck covers the fix commit `ba90491..bd9b4cb` (`deploy/user-unit-upgrade.sh` +3 -2, `docs/deployment.md` +6 -3). It also checks the open finding R1 from review 1, adopted finding O2, and the rest of `docs/deployment.md` (plus `deploy/README.md` and `--help`) for leftover wording that has this script start "the timers" or "both timers". Work that did not change since review 1 is not judged again.
- The local branch and `origin/paudley/175-user-unit-upgrade-sh-restarts-sparrow` both resolve to bd9b4cba5c7217cf3826d7a3ff5104b65835e21f.
- This was a static review. I did not run the upgrade script or the check, and I touched no unit, timer or store. I ran read-only checks only: `git diff`, greps, `shellcheck -x`, `bash -n` and `prose-check`.

## Evidence

- **R1, `docs/deployment.md`.** All three places now match the script and `--help`:
  - 954: "starts `sparrow-ship.timer` (not the backup timer, below), and exits 0".
  - 963-964: start `sparrow-ship.timer`; start `sparrow-backup.timer` only after a first online backup verifies. This matches `--help` lines 86-88.
  - 1020-1022, "Claims after a rollback" step 6: start `sparrow-ship.timer`, and start the backup timer only after a first online backup of the restored store verifies (`deploy/verify-backup.sh` exits 0). This matches `--help` lines 95-97.
- **Leftover wording.** I ran `grep -n 'starts the timers\|start the timers\|start both timers\|both timers'` over the script, `deploy/README.md` and `docs/deployment.md`:
  - The remaining hits about this script describe stopping, not starting: the script's lines 38 and 84, and `docs/deployment.md` 924, 961 and 992 ("stop both timers", "both timers stopped", "Stop `sparrow.service` and both timers").
  - `deploy/README.md` 256 ("enables the service and both timers") and 317 ("### 7. Apply watches, start the timers, verify") belong to the system-unit install, not this script.
  - None remains that has this script, or its recovery, start the backup timer.
- **O2.** Line 362 now prints `2. SPARROW_CONFIG=<$SPARROW_CONFIG> deploy/verify-backup.sh SET (SET is the new set, <$SPARROW_BACKUPS>/sets/ID)`. `SPARROW_CONFIG` is the user-unit config path the script itself uses (line 146), and `verify-backup.sh` reads that variable (lines 263-264). The printed line still contains `deploy/verify-backup.sh SET`, which the check's assertion (line 117) looks for.
- **Re-wrapped `--help` line.** Lines 96-97 are a text-only re-wrap of the rollback note.
- **Logs.**
  - `/home/paudley/Active/sparrow/.stage/175/check-run3.log`: three `ok` lines, `PASS`, `EXIT=0`, with the same call records as before (success starts only `sparrow-ship.timer`).
  - `shellcheck2.log`: `EXIT=0`, `bash-n EXIT=0`. `commit2.log` ends with `EXIT=0` (hook passed).
- **My own reruns.** `shellcheck -x deploy/user-unit-upgrade.sh` gave SC=0, and `bash -n` gave 0. `prose-check` gave EXIT 0 on `docs/deployment.md`, on `deploy/user-unit-upgrade.sh` and on `.stage/175/added-prose2.txt`.

## Findings

- **R1 (review 1, REQUIRED): resolved.** The three places in `docs/deployment.md` now agree with the behavior and `--help`.
- **O2 (review 1, OPTIONAL): adopted** as described above.
- **O1 and O3 (review 1, OPTIONAL):** still open, and neither blocks.

New, OPTIONAL:

**O4 (OPTIONAL).** `docs/deployment.md` line 954 says "(not the backup timer, below)". The paragraph that explains the backup timer is above it (lines 928-936), so "above" is the correct pointer. The same line is also not re-wrapped to the file's line width.

**O5 (OPTIONAL).** `validation.md` still cites `check-run2.log` and `shellcheck.log`. The current evidence for bd9b4cb is `check-run3.log`, `shellcheck2.log` and `commit2.log`.

Findings: no REQUIRED findings remain.

VERDICT: PASS
