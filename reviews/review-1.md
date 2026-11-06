# Delivery review 1: #31, record verifiable SOURCE in nightlies under the interim user unit

## Scope

- Issue: `.stage/record-verifiable-source-in-nightlies/issue-31.md`. It has no comments.
- Plan and rulings: `plan.md`. The single ruling (2026-11-06, lillith_coordination as Patrick) authorizes fixing the nightly SOURCE in this repository, with no hand copy and no NOT VERIFIED exit.
- Change: the staged diff in the worktree against `main` (13af50d). It is byte-identical to `change.diff` apart from file order. Files: `deploy/libexec/sparrow-backup` (opt-in variable and guard condition) and `deploy/MANUAL.md` (one paragraph).
- Read for context: `deploy/lib.sh`, `deploy/pre-upgrade-backup.sh`, `deploy/test-instance.sh`, `deploy/verify-backup.sh`, `deploy/systemd/sparrow-backup.service`, `deploy/evidence/lifecycle.md` (O10), the live user units under `~/.config/systemd/user/` (read only), the installed helper at `~/.local/libexec/sparrow/sparrow-backup` (read only), and the `~/.local/libexec/sparrow` layout.
- Not done, per instructions: no build, cargo or make; the helper was not run against any unit; no systemctl call; nothing under `/SPARROW` was opened.

## Evidence

1. **Guard behavior.** I loaded the helper with `runpy` (run name `probe`, so `main()` never ran) under `env -i` and called `systemctl_command()` for each environment below. Results:
   - Refused: `SCOPE=user` with the default unit; `SCOPE=user UNIT=sparrow`; `SCOPE=user` with the opt-in set to `0`, `true` or the empty string.
   - Allowed: `SCOPE=user` with `SPARROW_INTERIM_USER_PRODUCTION=1`, for both `sparrow.service` and `sparrow`; `SCOPE=user UNIT=sparrow-deploytest.service`, with or without the opt-in; system scope, with or without the opt-in.

   Only the exact value `1` opens the guard, and every other user-scope run naming `sparrow.service` is still refused.
2. **The guard still covers test-instance runs.** `deploy/lib.sh:50-51` still refuses user scope with `sparrow.service`, with no opt-in, at source time. So `pre-upgrade-backup.sh` and every other lifecycle script stop before they reach the helper, even if the variable leaks into an operator shell. `test-instance.sh env` exports `SPARROW_UNIT=sparrow-deploytest.service`, which the opt-in does not affect. The variable is referenced nowhere else in the repository, so nothing else sets it.
3. **The helper under the user unit.** I checked each dependency read-only:
   - `LIBEXEC`: with `SPARROW_PREFIX=%h/.local`, `~/.local/libexec/sparrow/sparrowd` links to `sparrowd-schema67-13af50d`, and its CLI sibling `sparrow-schema67-13af50d` exists. Both are copied into `images/`, which is the layout `verify-backup.sh:146-151` expects.
   - Closing-count line: the linked image contains `sparrowd: closed store=` (`grep -c -a` printed 1), so `counted` is true and the helper records `unique_payload_chunks`.
   - Running image: the running daemon (pid 3775859, started 04:13 CST by the last nightly) was launched through the `sparrowd` link, which has named schema67-13af50d since 01:45. So the schema check (`sparrow-backup:286-289`) should agree. This is an inference: I did not query daemon status, because that means using the `/SPARROW` socket.
   - Journal lookup: systemd is 262, which meets the 257 floor for `--invocation`. `journalctl --user -u sparrow.service --invocation=1d8bf8c2e1e1418a9f58d07786318ab7 -o cat` returns the live invocation's lines, and entries carry `_SYSTEMD_USER_UNIT=sparrow.service` and `_SYSTEMD_INVOCATION_ID`. So the lookup at `sparrow-backup:167-173` works for the user unit as paudley.
   - CLI on PATH: the live unit sets `PATH=%h/.local/bin:/usr/bin:/bin`, and `~/.local/bin/sparrow` links to `sparrow-schema67-13af50d`, which `daemon_status` runs. `findmnt`, `lsattr`, `chattr`, `cp`, `sync`, `systemctl` and `journalctl` are in `/usr/bin`.
   - `copy_tree`: the backup root `~/.local/share/sparrow/backups` is btrfs (`/dev/nvme0n1p3[/@home]`). `/SPARROW` is not a separate mount in `/proc/self/mounts`, so it is on the same btrfs device under `/`, and reflinks are possible. `chattr ±C` runs only on directories that paudley creates inside the backup, so no privilege is needed.
   - Lexicon bundle: `text_lexicon_dir` resolves to `~/.local/share/sparrow/lexicons-e58974e5…/artifacts`. Its parent exists and holds the five native CBOR lexicons that `verify-backup.sh:170-171` requires.
   - Extra paths: `~/.local/state/sparrow/{intake,ship,ship-watches}` and `ship/lock` exist.
   - `RUNTIME_DIRECTORY`: with `RuntimeDirectory=sparrow-backup` in a user unit, it is `$XDG_RUNTIME_DIR/sparrow-backup` (`%t/sparrow-backup`). The helper writes the marker as `stopped` inside it, which matches the planned `ExecStopPost` check of `%t/sparrow-backup/stopped`. systemd removes the runtime directory only after `ExecStopPost` runs.
4. **Install state now.** The live `~/.config/systemd/user/sparrow-backup.service` still runs the old helper. It has no `RuntimeDirectory=`, and its marker is `%t/sparrow-backup.stopped`. The install has not happened yet. The main checkout is on `main` at 13af50d and is clean.
5. **Prose.** `prose-check` on the MANUAL paragraph and the new code comment exited 0 with no findings.
6. **No `validation.md`** exists in the stage directory, and the acceptance run (a nightly that passes `verify-backup.sh` with exit 0) has not happened.

## Findings

No REQUIRED findings against the change. The diff does what the issue's first behavior asks: an explicit opt-in that only the interim production backup unit sets, while every other user-scope `sparrow.service` run is still refused. Evidence 3 shows the repository helper's dependencies are present for the user unit. The MANUAL text is accurate.

- **OPTIONAL O1: the guard comment no longer tells the whole story.** `sparrow-backup:70` still says "both spellings are refused" right above a condition that now allows them under the opt-in. Suggested: "...are refused unless the interim production backup unit opts in."
- **OPTIONAL O2: the MANUAL paragraph lists only the environment variables.** It does not mention the unit wiring the helper now depends on: `RuntimeDirectory=sparrow-backup`, and `ExecStopPost` checking `%t/sparrow-backup/stopped` instead of the old `%t/sparrow-backup.stopped`. Without the runtime directory the helper fails with a clear message. With a stale `ExecStopPost`, however, a SIGKILL or timeout mid-backup leaves production stopped, because the marker paths no longer match. One more sentence, or the unit lines, would make the hand install reproducible.
- **OPTIONAL O3: install hygiene.**
  - Point `ExecStart` at the main checkout (`/home/paudley/Active/sparrow/deploy/libexec/sparrow-backup`), never the worktree, which is removed after merge.
  - Fast-forward the main checkout before the next 04:00 timer. A pre-merge `main` still refuses the opt-in. That failure is safe (it refuses before any stop), but the night produces no backup.
  - Consider adding `UMask=0077` to the user unit, as the system unit has. The live user unit has none, so the new `images/`, `lexicons/`, `config/` directories and `SOURCE` are created world-readable inside the 0755 backup root. The old helper had the same exposure, so this is not a regression.
  - The stale `~/.local/libexec/sparrow/sparrow-backup` copy can be removed once the unit no longer references it.
- **OPTIONAL O4: no regression test.** Nothing automated checks the guard's truth table. The repository has no test harness for this helper, and the change is two lines, so Evidence 1 is reasonable proof for this delivery.

## Open acceptance (not a defect in the change)

The issue's acceptance can only be shown after merge: install the user unit, then "the first nightly after the install passes verify-backup with exit 0, including the payload-chunks check". None of that has happened (Evidence 4 and 6). This review approves the change for merge only. #31 is not delivered until:

1. the user unit is installed as planned;
2. a nightly, or an operator-started `systemctl --user start sparrow-backup.service`, writes a backup whose SOURCE contains `unique_payload_chunks=`;
3. `deploy/verify-backup.sh` on that backup, with an unedited SOURCE, exits 0, and the command, its exit status and the backup stamp are recorded in `validation.md`.

VERDICT: PASS
