<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Rehearsal (specification F, item 2)

On 2026-11-02 a fresh agent session with no prior context followed these
documents from a clone of the pushed `deploy` branch (commit `c894b19`), as an
operator recovering the service onto a fresh host. It had no root: it staged
the root-only steps with `SPARROW_DESTDIR` and ran the lifecycle on a
user-scope test unit. Its tools were limited to reading files and a list of
shell commands; it could not edit, build, use `sudo` or address
`sparrow.service`. It used the images `make install-bin` had already built.
`REHEARSAL` is `.stage/rehearsal` in the worktree that ran it, `DEPLOY` that
worktree.

## Resolution

Every gap was fixed in the following commit and checked on a new test
instance, except where noted.

| Gap | Fix |
| --- | --- |
| G1 | `lib.sh` `require_socket_length`: `test-instance.sh setup` and `start.sh` refuse a socket path over 107 bytes before anything is created (checked: a 140-byte path is refused and nothing is made). README "Test instance" and RECOVERY "Unit start-limit hit" name the limit. |
| G2 | `install.sh` installs `evidence/` with the documents (checked on the staged tree). |
| G3 | README intro and step 2: the backups do not hold images; rebuild (with the measured time) or copy the old `./bin`; the Forgejo and PurRDF sources; the recovery probe stays in `./bin`. |
| G4 | `install.sh` prints that the slice and old user unit checks are skipped while staging. |
| G5 | `test-instance.sh` writes its configuration before `install.sh`, which keeps it, so no secret warning (checked). |
| G6 | README "Test instance" gives the commands verbatim, with `SPARROW_BIN` and `SPARROW_MAX_EXPOSURE`. |
| G7 | `check.sh` in user scope prints `NOT CHECKED` for AppArmor, the timers and the slice and exits 2 (checked). |
| G8 | README step 7: run as the operator in a new login session; where `sparrow` on `PATH` comes from. The CLI commands themselves were checked by earlier runs (lifecycle.md), not by the rehearsal. |
| G9 | docs/deployment.md: `verify` reports the missing image until `install.sh` has run. |
| G10 | docs/deployment.md: the measured cold and seeded build times. |
| G11 | RECOVERY "Restore from a backup": restore `/etc/sparrow` on a fresh host, then check the secret and `text_lexicon_dir`. |
| G12 | Issues #10 and #22 and the Katamari repository named with their Forgejo location; `REV` and `PURRDF_REV` distinguished in the layout; Git 2.54 in step 1; the unit comments name the fleet specification. |
| G13 | lifecycle.md records the rehearsal's run of the restart-loop stop. |

## The report, verbatim

Rehearsed from `REHEARSAL/sparrow` (branch `deploy`, HEAD `c894b19`) with the kept images at `DEPLOY/bin` (`sparrowd-schema63-ba6ebe7`, `sparrow-schema63-ba6ebe7`, `sparrow-recovery-schema63-ba6ebe7`, `lexicons-a47940e1f130bf3739c6d59aaf89643209905ea4`). Root-only steps were staged with `SPARROW_DESTDIR`; lifecycle steps ran against the user unit `sparrow-deploytest.service`. The live `sparrow.service` was never touched.

One constraint on this run: my shell permission scope allowed the `deploy/*.sh` scripts, `journalctl --user` on the test unit, `systemd-analyze`, `readlink`, `git check-ignore` and file reading, but denied direct execution of the `sparrow` CLI and images, file creation, `ls`, `find`, `df`, `stat`, `python3` and `command -v`. Steps I therefore could not verify are listed as such rather than assumed. The `eval "$(... env)"` prefix was also denied by that scope, so I passed the exact variables that `test-instance.sh env` printed as an explicit environment prefix instead. That substitution is equivalent and is not a documentation gap.

### Gaps

#### G1. Test instance fails to start when the checkout path is deep (SUN_LEN)

- **Document**: README.md "Test instance"; `deploy/test-instance.sh` header; RECOVERY.md "Unit start-limit hit".
- **Tried**: `SPARROW_BIN=... deploy/test-instance.sh setup` with the default scratch directory, then `deploy/start.sh`.
- **Happened**: the unit went straight into a restart loop:
  ```
  start.sh: starting sparrow-deploytest.service on sparrowd-schema63-ba6ebe7
  ● sparrow-deploytest.service - Sparrow deploy test instance (deploy/test-instance.sh)
       Active: activating (auto-restart) (Result: exit-code) since Mon 2026-11-02 02:20:05 CST; 97ms ago
      Process: 87674 ExecStart=.../.stage/test-instance/root/usr/local/libexec/sparrow/sparrowd --config ... (code=exited, status=1/FAILURE)
  start.sh: sparrow-deploytest.service is not active
  ```
  Journal:
  ```
  sparrowd[87674]: sparrowd: watch=deploytest reconciling reason=startup
  sparrowd[87674]: Error: "daemon: path must be shorter than SUN_LEN"
  ```
  The default socket path was 113 characters. Neither `test-instance.sh` nor `start.sh`'s `check_config` checks the Unix socket length limit (107 bytes). The daemon had already created the store (`LOCK`, `IDENTITY`, `000009.sst`) and the claim directory `.sparrow-inflight` before refusing the socket, leaving a half-initialized scratch store behind, and `Restart=on-failure` would have spent the start limit within a minute. `stop.sh` did handle the loop correctly (see Worked).
- **Should say**: README "Test instance" and the script usage should state the socket length limit and tell the operator to pass a short `SCRATCH` argument (`deploy/test-instance.sh setup /short/path`). `start.sh` should refuse a `socket_path` longer than 107 bytes before starting the unit, and `test-instance.sh setup` should refuse to render one. RECOVERY.md "Unit start-limit hit" should list `path must be shorter than SUN_LEN` among the common causes. Workaround used: re-setup at `.stage/rehearsal/ti`, which started cleanly.

#### G2. The installed documentation links to evidence files that install.sh does not install

- **Document**: README.md "Layout" (`/usr/local/share/doc/sparrow/deploy/`, "Copies of these documents"); README "Permissions", "Test instance", RECOVERY.md "Rollback" and "Exposure" all link `evidence/*.md`.
- **Tried**: staged `install.sh` and listed the staged tree.
- **Happened**: `usr/local/share/doc/sparrow/deploy/` holds only `MANUAL.md`, `README.md`, `RECOVERY.md`. `install.sh` step 9 copies `deploy/*.md` only. On a host without the repository (the case README's intro and the `Documentation=file://` line in the unit are written for), every `evidence/...` link is dangling.
- **Should say**: either install `deploy/evidence/` too, or state in the Layout row that the evidence files stay in the repository.

#### G3. "This directory and a clone are enough" is not true for a fresh host

- **Document**: README.md intro and step 2; RECOVERY.md "Where state lives" ("Images and lexicons: rebuilt from the repository and `./bin`") and "Restore from a backup" ("On a fresh host, follow README.md steps 1 to 3 first").
- **Tried**: reading step 2 as if run, and the Cargo lockfile.
- **Happened**: `./bin` is gitignored and not copied by `sparrow-backup` (it copies store, intake, `/etc/sparrow`, `/SPARROW/ship`), so on a fresh host there are no kept images: step 2 must run `make install-bin`, which needs the full toolchain of step 1 plus network access to `https://github.com/Blackcat-Informatics/purrdf` (the locked PurRDF source) and, per docs/deployment.md, a release build that on 2026-11-01 ran for 6267 s and then failed to link. The clone URL in step 2 is `http://127.0.0.1:7555/fleet/sparrow.git`, which only resolves on the current host; a fresh host has no loopback Forgejo. The recovery probe `bin/sparrow-recovery-*` is likewise only in `./bin` and never installed.
- **Should say**: name the reachable remotes for Sparrow and PurRDF, say that the kept images must be rebuilt (with the expected time) or copied from the old host's `./bin` or a backup that includes it, and either back up `./bin` or add it to the restore procedure. Consider installing the recovery probe alongside the images.

#### G4. Staged install silently skips two refusal gates

- **Document**: README.md step 1 ("`install.sh` refuses to enable the units without it") and step 3; `install.sh` header ("skips each step that changes the running host ... printing it").
- **Tried**: `SPARROW_DESTDIR=... SPARROW_BIN=... deploy/install.sh` (without `--no-enable`).
- **Happened**: output listed `staged, not run: systemctl enable ...` but said nothing about the `lillith.slice` check or the old-user-unit check, both of which are inside `if [[ -z $DEST ]]`. A staged rehearsal cannot tell whether those gates would pass on the real host, and the "printing it" claim is not met for them.
- **Should say**: print `skipped while staging: lillith.slice check` and `skipped while staging: old user unit check`, or run the read-only checks even when staging. I could not run `systemctl show -p FragmentPath --value lillith.slice` under this rehearsal's limits, so step 1's prerequisite is unverified here.

#### G5. Test instance prints a misleading "MISSING tcp.secret" warning

- **Document**: README.md "Test instance"; `test-instance.sh setup` output.
- **Tried**: `deploy/test-instance.sh setup`.
- **Happened**:
  ```
  install.sh: MISSING .../ti/root/etc/sparrow/tcp.secret: the public listener needs it (README.md, step 4); the unit will not start without it
  ```
  followed by the script overwriting `server.toml` with a configuration that has no TCP listener. The unit does start. A new operator will chase step 4 needlessly.
- **Should say**: write the test configuration before calling `install.sh`, or have `test-instance.sh` note that the warning does not apply to the test instance.

#### G6. README does not show how to point the scripts at the test instance

- **Document**: README.md "Test instance" ("It prints the environment that points `start.sh`, ... at it").
- **Tried**: following the README alone.
- **Happened**: the README never shows the `eval "$(deploy/test-instance.sh env)"` line, nor that `SPARROW_BIN` must be set when `./bin` lives elsewhere, nor that `check.sh` needs `SPARROW_MAX_EXPOSURE=3.5` for the test unit. Those three facts are only in `evidence/lifecycle.md`, which G2 shows is not installed.
- **Should say**: give the two commands verbatim:
  ```
  eval "$(SPARROW_BIN=PATH/bin deploy/test-instance.sh env)"
  SPARROW_MAX_EXPOSURE=3.5 deploy/check.sh
  ```
  and explain that the user unit scores 3.5 because the root-only hardening is dropped.

#### G7. check.sh in user scope omits checks without saying so

- **Document**: README.md step 7 ("`check.sh` exits 0 when every check passes: ... AppArmor confinement with no denials, both timers and the slice. Exit 2 means a check needed root").
- **Tried**: `check.sh` on the running test unit.
- **Happened**: exit 0 with nine PASS lines and no AppArmor, timer or slice lines at all. In user scope `check.sh` skips that whole block silently rather than printing `NOT CHECKED`, so exit 0 overstates coverage on a test instance.
- **Should say**: print `NOT CHECKED  AppArmor, timers and slice (user scope)` and exit 2, matching the documented contract.

#### G8. Step 7's CLI commands could not be verified, and the CLI link is only on PATH after re-login

- **Document**: README.md step 7 (`sparrow /SPARROW/store/rpc.sock watch put ...`), RECOVERY.md "Verify health" (`sparrow ... status`) and "Watch root missing" (`watch list`), README "Secrets" (`sparrow --secret-file PATH tcp://ADDRESS:518 COMMAND`).
- **Tried**: `ti/root/usr/local/bin/sparrow SOCKET status` and `... watch list`.
- **Happened**: denied by my permission scope, so the `watch put` argument order, `watch list`, the `status` field names (`pressure.phase`, `durability.failure`, `consume_watches[].root`) and the `--secret-file` flag are unverified by me. `check.sh` and the receipt do read `store_id`, `schema_version`, `pressure.phase`, `durability.failure` and `consume_watches` successfully, which covers the `status` fields indirectly. Also unverified: dropping a file into the intake root and watching it be consumed (no file-creation permission).
- **Should say**: nothing new for the commands, but step 7 should remind that `sparrow` is on `PATH` only via the `/usr/local/bin/sparrow` link created by `install.sh`, and that the group membership from step 3 requires a fresh login before the socket admits the operator.

#### G9. `systemd-analyze verify` fails before install, and the document does not say so

- **Document**: docs/deployment.md "Configuration" ("Validate the unit files ... without loading them").
- **Tried**: `systemd-analyze verify deploy/systemd/sparrow.service`.
- **Happened**:
  ```
  sparrow.service: Command /usr/local/libexec/sparrow/sparrowd is not executable: No such file or directory
  ```
  `systemd-analyze security --offline=yes` worked and scored `1.6 OK`, matching RECOVERY.md "Exposure".
- **Should say**: that `verify` reports the missing image until `install.sh` has run, and that only the exposure score is meaningful beforehand.

#### G10. docs/deployment.md ships an unfinished editorial paragraph

- **Document**: docs/deployment.md "Kept images and seeded builds", build-time table.
- **Happened**: the table has two "not measured" rows and the text says "Repeat both runs once `/opt` has unallocated space again ... and replace this paragraph with the figures". An operator cannot tell how long step 2 takes.
- **Should say**: either the measured figures or a plain statement that a release build takes hours on this host and should be done before an emergency, not during one.

#### G11. Restore on a fresh host skips the configuration it needs

- **Document**: RECOVERY.md "Restore from a backup".
- **Happened**: it says "Restore `/etc/sparrow` from `BACKUP/config/sparrow` only if it was lost" and "On a fresh host, follow README.md steps 1 to 3 first". On a fresh host `/etc/sparrow` holds only the templates from step 3, so the secret, the edited `server.toml` and the watch definitions are lost by definition, yet the command block has no line restoring `config/sparrow`, and step 4's `text_lexicon_dir` edit is not mentioned. The backup script's layout (`config/server.toml`, `config/sparrow/`, `config/ship/`) does match the document.
- **Should say**: add `sudo cp -a --reflink=auto /SPARROW/backups/STAMP/config/sparrow/. /etc/sparrow/` to the fresh-host path, and note that the restored `text_lexicon_dir` must name a bundle that `install.sh` installed on the new host (same PurRDF revision, or the open is refused per "Refused open").

#### G12. Unexplained references

- **Document**: README.md "Rulings", "Permissions", "Moving a running service"; MANUAL.md; evidence files.
- **Happened**: "issue #10", "the tracking issue's acceptance steps", "PRODUCTION-LAUNCH section B" (in the unit comments) and "Katamari" appear with no link, number or path. The `REV` suffix for lexicons is a 40-character PurRDF hash while image `REV` is a 7-character Sparrow commit; the Layout table uses `REV` for both. `make install-hooks` needs Git 2.54 or newer (AGENTS.md) but step 1 lists `git` with no version.
- **Should say**: link the issues, name the Katamari path, distinguish `PURRDF_REV` from `REV` as docs/deployment.md already does, and add the Git version.

#### G13. evidence/lifecycle.md "Not run" is now stale

- **Document**: deploy/evidence/lifecycle.md "Not run" ("`stop.sh` on a unit caught between restarts: the loop above had reached `failed` before the fix was exercised").
- **Happened**: this run exercised it (G1): `stop.sh: sparrow-deploytest.service was activating (no drain: the daemon was not serving); stopped`.
- **Should say**: record the run and drop the item.

### Steps that worked as written

- **install.sh staged** (README step 3): created every file in the Layout table under the destdir with the expected names, image links `sparrowd -> sparrowd-schema63-ba6ebe7`, `sparrow -> sparrow-schema63-ba6ebe7`, `bin/sparrow -> libexec/sparrow/sparrow`, the lexicon bundle and the three documents, and printed each skipped host step. Re-run printed `install.sh: nothing changed` and kept the existing configuration and links, as documented.
- **systemd-analyze security --offline** (docs/deployment.md): `Overall exposure level for sparrow.service: 1.6 OK`, matching RECOVERY.md.
- **test-instance.sh setup** (at a short path): rendered the unit into `/run/user/1000/systemd/user/` with the documented deletions (`User`, `Group`, `LoadCredential`, `AppArmorProfile`, capabilities, `Slice`, `Nice`, `OOMScoreAdjust`, `RequiresMountsFor`) and `ProtectHome=read-only`.
- **start.sh** (README step 6): receipt as documented, `state=active`, `schema=63`, `pressure=ready`, `durability_failure=None`, `watches=1`. Second run on the running unit: `already runs sparrowd-schema63-ba6ebe7` plus the receipt, exit 0.
- **check.sh** with `SPARROW_MAX_EXPOSURE=3.5`: all nine checks PASS, including socket `0660` and store `0710`.
- **restart.sh**: `already on sparrowd-schema63-ba6ebe7; restarting it`, drain stop, start, store identity unchanged. `restart.sh --rollback` with no previous pair: `no previous image to roll back to`, exit 1, nothing stopped.
- **stop.sh**: `stopped through its drain (pid 142279); socket removed`; the socket was gone afterwards. On the restart-looping unit it stopped it without hanging.
- **check.sh on the stopped unit**: exit 1 with clear FAIL lines for the unit, socket and status.
- **teardown**: removed the unit and kept the scratch directory, as documented. `.stage/` is gitignored, so the leftover scratch store under the checkout is not visible to Git.
- **MANUAL.md "The helpers by hand"**: the `sparrow-backup` argument order matches the script's usage line and the unit's `ExecStart` (not run).
- **systemd version**: 262, satisfying step 1's "256 or newer". AppArmor enablement, cgroup v2, `aa-enabled`, free space and the tool list in step 1 were not checkable under this rehearsal's limits.
