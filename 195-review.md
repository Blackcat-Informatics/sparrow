Reviewer model: claude-opus-5-5 (opus)

# Delivery review: Sparrow PR #196 (issue #195), head 06a1bc0f45e070515fb2690028dad9aac684947e

## Scope

- Static review of `git diff main...06a1bc0` in
  `/home/paudley/Active/sparrow/.worktrees/195-user-unit-upgrade-sh-pre-upgrade-backup`. The diff changes
  three files: `deploy/user-unit-upgrade.sh` (+5 -1), `deploy/verify-backup.sh` (+1) and the new
  `tools/deploy-scripts-check.sh` (+25).
- Inputs read: `.stage/script-issue.md`, `.stage/195-pr-body.md`, `.stage/195-commit.log`,
  `.stage/image/switch2.log`, `switch2b.log`, `switch2c.log`, `verify-6.log`, `backup-set-2.log`,
  `stage-4-backup-unit.log`, `.stage/forwarder/sparrow-backup.service.pre-switch`, and in the worktree
  `deploy/libexec/sparrow-backup` (whole file), `deploy/lib.sh`, `deploy/user-unit-upgrade.sh` lines 1-300,
  `deploy/verify-backup.sh` lines 1030-1110, `deploy/README.md` lines 395-455,
  `deploy/systemd/sparrow-backup.service`, `tools/user-unit-upgrade-timers-check.sh`, `Makefile`.
- Not run: cargo, builds, the deploy scripts, both check scripts. I did not read the installed unit
  `~/.config/systemd/user/sparrow-backup.service` (a home dotfile, out of bounds), so its exact
  ExecStart is inferred below, not observed.
- `prose-check .stage/195-pr-body.md`: exit 0, no findings.

## Evidence

1. **SPARROW_PREFIX export.** Line 147 exports it. `sparrow-backup` line 45 reads `SPARROW_PREFIX` for
   `LIBEXEC`, and line 233 (`os.readlink(LIBEXEC / "sparrowd")`) is the crash in switch2.log. The fix
   is correct. The export also reaches the other children (the CLI, systemctl, getcap, ps). None of
   them reads the variable: the only Rust uses are two test harness `.env()` calls in
   `crates/sparrow-server/tests/rpc.rs`.
2. **Arguments.** `sparrow-backup` main() takes `SERVER_CONFIG INTAKE [EXTRA...]`. It copies each
   extra to `config/NAME`, and `copy()` fails on a missing path (an rsync exit other than 24 for a
   directory, any nonzero exit for a file). The new call is `server.toml`, `$SPARROW_INTAKE`
   (default `$SPARROW_STATE/intake`, with `SPARROW_STATE` defaulting to `~/.local/state/sparrow`),
   `$CONFIG_DIR`, `$SPARROW_STATE/ship` and `$SPARROW_STATE/ship-watches`. The basenames are
   distinct (`sparrow`, `ship`, `ship-watches`), so nothing collides under `config/`. Dropping the
   separate `ship.toml` argument loses nothing because `ship.toml` is inside `$CONFIG_DIR`, so the
   line 216 precondition ("the backup keeps it") still holds. The defaults are set before
   `source lib.sh`, and lib.sh only applies `${SPARROW_INTAKE:-/SPARROW/intake}`, so the user default
   wins. lib.sh, `pre-upgrade-backup.sh` and the system unit are unchanged, so the system path is not
   affected.
3. **Which form the installed unit uses.** switch2b.log shows `link_stat "/SPARROW/intake" failed: No
   such file or directory`. backup-set-2.log shows the installed `sparrow-backup.service` completing
   sets/6 at 13:06 on the same host, so the installed unit cannot be passing `/SPARROW/intake`. The
   pre-switch user unit (`.stage/forwarder/sparrow-backup.service.pre-switch`) passed
   `%h/.local/state/sparrow/intake %h/.config/sparrow %h/.local/state/sparrow/ship
   %h/.local/state/sparrow/ship-watches`. The new call matches that, which supports the PR's claim.
   I could not confirm the installed unit itself (see Scope).
4. **--no-backup path.** The new variables are used only inside the `else` branch (line 260). The
   export changes no consumer (item 1). No behavior change on `--no-backup`. The existing
   `tools/user-unit-upgrade-timers-check.sh` runs `--no-backup` with `SPARROW_INTAKE` set and should
   be unaffected (not run).
5. **verify-backup.sh loop (lines 1074-1081).** `offset` steps from 0 by 1048576 while
   `offset < length`. `size` is `min(length - offset, 1048576)`, so the last chunk is the short
   remainder. `rm -f -- "$chunk"` now runs before every `bytes` read, including the first. Each chunk
   is appended to `artifact.bytes`, which is truncated once before the loop. A zero-length artifact
   skips the loop and hashes an empty file. The chunk lives in the `mktemp -d` SCRATCH. The fix is
   correct and complete for the `File exists` failure in verify-6.log (offset 1048576).
   `create_new(true)` in `crates/sparrow-server/src/agent/read.rs:56` is consistent with the refusal.
6. **tools/deploy-scripts-check.sh.** It runs `bash -n` on `deploy/*.sh`, then makes four
   `grep -qx/-qF` matches against the exact patched lines and one `sed` check that the line before
   the `bytes` call is the `rm`. It never runs either script and never observes the argv or
   environment the client receives. It is not wired into `make check` (the Makefile has no shell
   gate). That matches the existing `tools/user-unit-upgrade-timers-check.sh` precedent, and the PR
   body says so.
7. **Commit and PR text.** The subject ends `(#195)`, the commit has no body and no `Closes`. The PR
   body says `Refs #195` and states "Not yet proven live: a full run without --no-backup", which is
   accurate.
8. **Hook log.** `.stage/195-commit.log` (1275 lines) is the full `make check` sequence: fmt,
   release Clippy, release tests (14 `test result: ok`, no failures), census self-test and cross
   check. It ends `EXIT=0`. The log's mtime is 2026-11-10 16:28:24 -0600, but the commit's author and
   committer dates are both 16:12:59 -0600. Git sets those dates after the pre-commit hook returns.
   The log has no start or end stamp of its own. The gate covers only Rust and the census, so it says
   nothing about the three changed shell files.

## Findings

**REQUIRED 1: `--help` now misstates the intake default.** `deploy/user-unit-upgrade.sh:70` still
documents `SPARROW_INTAKE ... /SPARROW/intake`. This diff changed the default to
`~/.local/state/sparrow/intake`, and it added `SPARROW_STATE` (which also locates `ship` and
`ship-watches`) without listing it under "Environment". The help text is the operator's recovery
document that every FAILED line points to. Fix: correct the default on line 70 and add a
`SPARROW_STATE` row.

**REQUIRED 2: the repository's documented user unit contradicts the script it must match.**
`deploy/README.md:438` ("Interim user unit", which says the unit "must carry these lines") gives
`ExecStart=... server.toml /SPARROW/intake %h/.config/sparrow /SPARROW/ship`. Evidence item 3 shows
this form fails on this host (no `/SPARROW/intake`) and is not what the installed unit runs. The new
script comment ("as sparrow-backup.service passes it (%h/.local/state/sparrow/...)") and the guard's
message ("differ from the user service's") claim parity with a unit the repository describes
differently. An operator who installs the README form gets a nightly backup that fails in rsync and
diverges from the script. The issue's contract is that the script passes the arguments the user unit
uses, so the repository's own statement of those arguments must agree. Fix (one line): replace the
README ExecStart with the installed unit's actual arguments, after confirming them from the installed
unit. Doing so also records the evidence that the PR claim currently rests on.

**REQUIRED 3: the acceptance test is a text mirror of the diff, not a check of the client's
arguments.** The acceptance asks for "A test or a dry-run mode that checks the client's arguments
before any backup". `tools/deploy-scripts-check.sh` greps for the literal patched lines. It would
pass with a wrong `SPARROW_STATE` default, a wrong `PATH` prefix, or arguments that name paths that do
not exist. It fails on harmless reformatting, such as rewrapping the call or moving the `rm`
comment. It protects no runtime behavior, which the test-value rules treat as a finding. A cheap
behavioral check fits the existing harness. Add one case to `tools/user-unit-upgrade-timers-check.sh`
that runs the real script without `--no-backup`:
- the stub CLI's status carries `"backup":{"phase":"idle"}`;
- a stub `$lib/sparrow-backup` records `$SPARROW_PREFIX` and `"$@"` and writes
  `sets/1/SOURCE` with `image=sparrowd-schema68-aaa` and `store_id=s1`;
- the case asserts the recorded prefix is `$HOME/.local` and the argv is the user-unit form.

Then delete the grep guards so there is one check. Keeping `bash -n` there or elsewhere is harmless.
The verify-backup `sed` adjacency check also goes: the one-line fix needs no mirror.

**REQUIRED 4: the PR body's hook claim does not match the artifact's timing.** The PR body says
"Commit hook (full gate): .stage/195-commit.log ends EXIT=0". That log finished at 16:28:24, 15 min
25 s after the commit object was created at 16:12:59. Because git dates a commit after its
pre-commit hook returns, this file cannot be the hook run that admitted 06a1bc0, unless its mtime
was refreshed afterwards (for example by a copy). Fix: point the claim at the actual hook output, or
reword it to say what this log is (a full `make check` that finished at 16:28:24 on this tree). I am
not alleging a bypass. The audit claim just has to be accurate.

**OPTIONAL 5.** Before asking the daemon for a backup, check that `$SPARROW_INTAKE`,
`$SPARROW_STATE/ship` and `$SPARROW_STATE/ship-watches` exist. This would turn the switch2b failure
class (a 7 to 18 minute backup wasted on a bad path) into an immediate refusal. The issue does not
require it.

**OPTIONAL 6 (out of scope, pre-existing).** `deploy/pre-upgrade-backup.sh:50` (system unit) passes
config, intake and config dir. The system `sparrow-backup.service` ExecStart also passes
`/SPARROW/ship`. This is the same class of mismatch for the system path. It deserves a separate
issue, not a change in this PR.

**Open state (not a PR finding).** Acceptance criterion 1 (a live run without `--no-backup` that
writes a complete `sets/N` and then switches) is unproven, as the PR body says. `Refs #195` correctly
leaves the issue open until it is shown.

VERDICT: BLOCKED
