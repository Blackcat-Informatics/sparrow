Reviewer model: claude-opus-5-5 (opus)

# Delivery review: verify-backup restore margin (c446e2f, Refs #183)

## Scope

Commit c446e2f on `paudley/183-tail-latency-of-context-and-records`, the only commit above
origin/main (`git diff origin/main`): deploy/verify-backup.sh, deploy/RECOVERY.md,
docs/deployment.md and crates/sparrow-server/tests/rpc.rs, 4 files, +28/-10.

Contract (coordinator ruling, 2026-11-11): deploy/verify-backup.sh must refuse a restore unless
a stated margin of free space remains on the scratch filesystem after the copy, and must print
the figure it used.

This was a static review. I ran no cargo, build, daemon or test, and touched nothing under
/SPARROW or /tmp. I ran three small read-only `bash -c` arithmetic probes, plus `prose-check`.

## Evidence

- Guard (deploy/verify-backup.sh:488-502): `need` is SOURCE `engine_bytes` (checked numeric at
  :489). `copies` is 2 under `--with-traffic` (:492-493). `margin=$((RESTORE_MARGIN_GB * GB))`
  with `GB=1000000000` (:149, :498). The check is `free > copies*need + margin` (:501). The
  boundary is strict, so exactly 40 GB left is refused. That is conservative and acceptable.
- Order: the figures line (:499-500) prints before the check, so it appears on both pass and
  refusal. The check runs before `mkdir -- "$DATA"` (:503) and before `--restore-backup` (:505).
  It runs after `mktemp -d` creates the empty scratch directory (:431) and after the full NAS
  manifest read (:472-484). That order is the same as before this commit (the old 10 GiB check
  sat at the same place), and cleanup removes the scratch directory. The second (prep) restore
  under `--with-traffic` is covered by `copies=2` in this one check.
- Figures line: it prints free bytes, `copies*need`, the margin in GB and `free - copies*need`.
  All of these are the values the check uses, except in the leading-zero case below.
- Validation (:151-155): unset means 40. The value must match `^[0-9]+$` and not be under 40,
  or the script exits 2 with a message on stderr. This runs right after option parsing, before
  any filesystem work. The usage text (:52-56: "default 40, may only be raised", "Exit 2 is a
  usage error"), deploy/RECOVERY.md:272-274 and docs/deployment.md:696 ("default and minimum
  40 GB", "after both copies") match the code.
- Tests (rpc.rs:5263-5309): `engine_bytes=11000000000` with `--with-traffic` against the df
  double's 60000000000 gives 22e9 + 40e9 = 62e9 > 60e9, so the run is refused. The test checks
  exit 1, the stderr reason, the exact figures line on stdout (60000000000 / 22000000000 / 40 /
  38000000000, which matches the arithmetic), that nothing was restored, and that the scratch
  directory was removed. These checks protect the named behaviour.
- Routing plain `verify_backup` through `verify_backup_measured` (rpc.rs:4841-4842) is sound.
  The systemctl and systemd-run doubles are only reached under `--measure-migration`
  (verify-backup.sh:225-246, :814, :903). `SPARROW_MEASURE_UNIT` is only read in that mode. The
  df double returns 60 GB, which clears every plain fixture's small `engine_bytes` plus 40 GB.
  The doubles are written under `verify.parent()`, which is the test's own tempdir at every call
  site (rpc.rs:5106, 5180, 5221, 5485, 5652, 5783). No test calls `verify_backup_with` directly,
  so no restore path depends on the host's real free space.
- Execution evidence:
  - .stage/183/test-verify-margin.log: the focused release run has 3 tests passing, among them
    `verify_backup_migration_measurement_refuses_to_start_below_its_disk_floor` and
    `verify_backup_fails_on_wrong_store_id`. The second covers the exit-0 pass path through the
    new check. EXIT=0.
  - .stage/183/commit-margin.log: the commit hook run has all 6 `verify_backup_*` tests and the
    rpc target (93 passed) passing, plus the census gates. EXIT=0.
- Prose and attribution: `prose-check` exits 0 on deploy/RECOVERY.md and docs/deployment.md, on
  the diff's added lines (`--stdin`), and on the commit message. `git diff origin/main` and the
  commit message contain no agent attribution, co-author trailer or tool branding.

## Findings

1. **REQUIRED**: deploy/verify-backup.sh:152 and :498. The `SPARROW_VERIFY_MARGIN_GB`
   validation accepts values that break "may only raise 40" and "print the figure it used".
   Bash arithmetic reads the value as an expression, and I confirmed both failures with
   read-only `bash -c` probes:
   - Overflow: `SPARROW_VERIFY_MARGIN_GB=9300000000` passes `^[0-9]+$` and `>= 40`, but
     `$((R * GB))` wraps to -9146744073709551616. The check then passes for any copy size, so
     the margin is turned off. The figures line still prints `margin 9300000000 GB`.
   - Leading zero: `0100` is read as octal 64. The script enforces 64 GB while it prints
     `margin 0100 GB`, so the printed figure is not the one used. `08`/`080` fail with "value
     too great for base". That error surfaces at :498 as exit 1, after the scratch directory
     and the manifest read, not as the documented exit 2 usage error.

   Fix: narrow the check to the whole numbers the arithmetic can hold, e.g.
   `[[ $RESTORE_MARGIN_GB =~ ^[1-9][0-9]{1,8}$ ]] && ((RESTORE_MARGIN_GB >= 40))`, or
   normalise with `10#`, and keep exit 2. The `SPARROW_MEASURE_*_GB` checks (:228-229) have the
   same pattern, but they are older and outside this contract.
2. **OPTIONAL**: deploy/verify-backup.sh:431-494. The margin refusal comes after `mktemp -d`
   and after the full NAS `b3sum` read of the set. A run that is bound to be refused first
   creates an empty scratch directory and reads the whole backup, which can take a long time
   for a 38 GB set. Nothing is restored, so the contract holds. Checking `free_root` before
   `mkdir -p`/`mktemp` (as `report_free` does at :410-429) would refuse earlier. The current
   position has one advantage: it measures free space right before the restore.
3. **OPTIONAL**: deploy/verify-backup.sh:499. Under `--with-traffic`, "copy needs %s bytes"
   reports both copies. "copies need" or "restore needs" would read more accurately. The
   stderr reason already says "2 time(s)".
4. **OPTIONAL**: deploy/RECOVERY.md:272-274 and docs/deployment.md:696. The new sentences are
   not rewrapped to the surrounding width (RECOVERY.md:274 and deployment.md:696 run well past
   the paragraph's line length). This is cosmetic only.
5. **OPTIONAL**: rpc.rs:5263. The comment says one restore "fits", but the test never runs the
   one-copy case at 11 GB. The pass path of the check is covered elsewhere (the
   `verify_backup_fails_on_wrong_store_id` exit 0 run). No test exercises the exit 2 rejection
   of `SPARROW_VERIFY_MARGIN_GB`. Adding one is optional because the ruling's contract (refuse
   and print) is already covered.

REQUIRED findings: deploy/verify-backup.sh:152 and :498 (the margin check accepts values that overflow or start with a zero).

VERDICT: BLOCKED

## Recheck at head 958ccd4

Reviewer model: claude-opus-5-5 (opus)

Scope: `git diff c446e2f 958ccd4`, one commit, `deploy/verify-backup.sh` (+2/-1) and
`crates/sparrow-server/tests/rpc.rs` (+15). This was a static review again: no cargo, no build,
no /tmp, no search outside the checkout.

Finding 1 (REQUIRED) is closed:

- deploy/verify-backup.sh:153 now requires `^[1-9][0-9]{1,8}$` and then `>= 40`.
- Octal: a leading zero no longer matches the pattern. The pattern check fails first, and the
  `||` stops there, so `((RESTORE_MARGIN_GB < 40))` is never evaluated for `0100`, `08` or
  `080`. Every value the arithmetic does see is plain decimal with no leading zero, so it is
  read as base 10.
- Overflow: the pattern allows at most nine digits, so the largest margin is
  999999999 * 1e9 = 9.99999999e17 bytes. That is under the 64-bit limit of about 9.22e18 and
  leaves room for `copies * need` (2 * `engine_bytes`) at any realistic set size. A ten-digit
  value such as `9300000000` is refused before any arithmetic runs.
- Each refusal goes through the existing `printf ... >&2; exit 2`. That runs right after option
  parsing, before any directory or manifest work, so it is the documented usage exit. Valid
  values from 40 to 999999999 are still accepted. The usage text says "default 40, may only be
  raised" and is still accurate. The new comment at :152 is accurate.

Test (rpc.rs:5263-5277): the new loop in
`verify_backup_migration_measurement_refuses_to_start_below_its_disk_floor` runs `39` (below
the floor), `0100` (octal raise), `08` (invalid octal) and `9300000000` (overflow). For each
one it checks exit 2 and the margin message on stderr. Under c446e2f, `0100` and `9300000000`
passed validation and `08` ended in an arithmetic error with exit 1, so this loop fails on the
old code and protects the fix. It reuses the test's existing `backup`, `verify`, `live` and
`args`, which are defined at rpc.rs:5219-5226. The refusal comes before `mkdir -p`, so `verify`
still does not exist for the later part of the test.

Execution evidence:

- .stage/183/test-verify-margin2.log: the focused run has 1 test,
  `verify_backup_migration_measurement_refuses_to_start_below_its_disk_floor ... ok`, with 92
  filtered out. EXIT=0.
- .stage/183/commit-margin2.log: the commit hook run has the rpc target at 93 passed and
  0 failed, the same test ok, and no FAILED or panicked lines. EXIT=0.

Prose and attribution: `prose-check --stdin` exits 0 on the commit message and on the added
lines. The diff has no co-author trailer and no agent attribution.

The open OPTIONAL findings 2 to 5 are unchanged and do not block.

Findings: none REQUIRED at 958ccd4.

VERDICT: PASS
