# Issue #175: user-unit-upgrade.sh restarts sparrow-backup.timer before the first verified backup, and names the wrong target schema (REQUIRED; prerequisite of the D147 switch)

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

Sparrow's re-read of `deploy/user-unit-upgrade.sh` for the D147 switch (2026-11-09 20:14 CST) and coordination ruling 20:2x: "the unconditional restart of sparrow-backup.timer at line 331 against 'the timer restarts only after a verify exits 0' is a defect in the script: fix it in the script through an issue and PR before the switch, not by stopping the timer by hand afterwards."

## Defect

`deploy/user-unit-upgrade.sh:331` ends the run with `systemctl --user start sparrow-ship.timer sparrow-backup.timer`, and it does the same at line 264 on the early-abort path. `sparrow-backup.timer` therefore restarts as soon as the switch finishes, before the first online backup has been taken and verified. #156's acceptance and the D147 ruling say the backup timer restarts only after a first online backup has been verified (`deploy/verify-backup.sh` exits 0). On the interim user unit the installed `sparrow-backup.service` is the old closed-store form until the operator installs the online lines (`deploy/README.md` from line 417), so a timer started by this script can run the wrong backup.

Related text defect: `deploy/user-unit-upgrade.sh:8` says "the schema 68 to 70 switch"; the next image is schema 71 (PR #170, `SCHEMA_VERSION` 71). Fix the wording ("68 to 71") and anywhere else in the script's header and `--help` that names schema 70 as the target, without changing the examples that name real kept images.

## Work

1. The script starts only `sparrow-ship.timer` at the end of a successful switch (and on the early-abort path, restarts only the timers it stopped that were running, not the backup timer unless it was running before). It leaves `sparrow-backup.timer` stopped and prints the exact instruction: take the first online backup, run `deploy/verify-backup.sh SET`, and only after exit 0 `systemctl --user start sparrow-backup.timer`.
2. Update `deploy/README.md` "Interim user unit", `docs/deployment.md` and the script's `--help` to say so.
3. A test or recorded check that the script no longer starts the backup timer: the script can be exercised against stubs (see how `deploy/test-instance.sh` and the rpc tests stage `systemctl`), or by a documented dry check that greps the starts; record the log with its EXIT line.

## Done when

- `grep -n 'sparrow-backup.timer' deploy/user-unit-upgrade.sh` shows no `start` of it on any path except an explicit "was active before" restore on the abort path (state which), with the test or recorded check.
- Docs agree. `bash -n` and `shellcheck` on the script pass.

## Waits on

Nothing. It is a prerequisite of the D147 switch. Nothing waits on Patrick.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
