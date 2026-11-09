# Review 4: #145 port of `--with-traffic` onto online backup sets

## Scope

The merge in progress in `.worktrees/145-measure-online-migrations-with-live`: HEAD 682a7a0
(the branch), MERGE_HEAD 3735c1e (#161, online backup sets), with every resolution staged.
Reviewed `git diff --cached 3735c1e` (the branch's delivery on top of new main, 16 files) and
`git diff --cached HEAD` (what #161 brings in). I read the six conflicted files in full where
they changed: `crates/sparrow-server/src/main.rs`, `crates/sparrow-server/tests/rpc.rs`,
`deploy/README.md`, `deploy/user-unit-upgrade.sh`, `deploy/verify-backup.sh` and
`docs/deployment.md`. I also read `config.rs`, `deploy/lib.sh`, `docs/interfaces.md`,
`deploy/libexec/sparrow-backup` (the set layout and SOURCE keys), `crates/sparrow-server/src/backup.rs`,
`rpc.rs` (backup gating) and the stage inputs (issue, plan with `## User rulings`, validation,
PR body and issue-comment draft).

Binding rulings I checked against the code: coordination 2026-11-09 "(a). Port --with-traffic to
online backup sets ...", the orders from the caller (keep #161's behavior intact, no closed-store
path, the prep copy, measured copy, record count and ship.toml all come from an online set, the
fixture builds its set through `backup_start`), and Patrick's "nothing is optional".

This review was read-only. I ran no cargo command and no build.

## Evidence

Checks I ran myself:

| Check | Result |
| --- | --- |
| `bash -n` on verify-backup.sh, reconcile-inflight.sh, user-unit-upgrade.sh, lib.sh | EXIT=0 for each |
| `shellcheck -x` on verify-backup.sh, reconcile-inflight.sh, user-unit-upgrade.sh | EXIT=0, no findings |
| Python parse of `deploy/libexec/sparrow-measure-load` | EXIT=0 |
| `prose-check` on pr-body.md and issue-comment-target.md | EXIT=0 |
| `prose-check` on docs/deployment.md and deploy/README.md | EXIT=0 |
| `git diff --cached 3735c1e --stat` on sparrow-core, sparrow-backup, server backup.rs, rpc.rs, agent/ | empty: the branch changes none of #161's backup code |

The implementer's logs, read in the files: r6-fmt EXIT=0; r6-clippy (release, sparrow-server)
EXIT=0; r6-unit EXIT=0, 3 `private_` tests passed; r6-filtered EXIT=0, 19 passed and 72
filtered out. The drop from 20 to 19 matches `r3-names.txt` against `r6-names.txt`: #161
removed `verify_backup_reports_pending_counts_as_not_verifiable` and renamed the linked-store
test to `verify_backup_refuses_an_image_without_query_only`.

How the port checks against the ruling:

- **No closed-store path in the traffic mode.** The measured copy is restored with
  `"$daemon" --restore-backup "$BACKUP_ROOT" "$BACKUP_ID" "$DATA"` (verify-backup.sh:493). The
  prep copy is a second restore of the same set (:559-563). The old
  `cp -a --reflink=always "$BACKUP/store/."` lines are gone from both paths (merge diff). The
  record count comes from the set's `images/` via SOURCE `image` and `cli` (:577-597).
  sparrow-backup writes both keys (sparrow-backup:255). `ship.toml` is read from
  `BACKUP/config/sparrow/ship.toml` (:280). That matches sparrow-backup's `config/NAME` layout
  for the `/etc/sparrow` EXTRA (sparrow-backup:248-250, sparrow-backup.service:29).
- **Free space.** `copies=2` under `--with-traffic` (:486-490). Both restores exist together:
  DATA is restored at :493 and PREP at :562, and PREP is removed at :611. The rehearsal's
  scratch store is a reflink of DATA with matched attributes, so it shares extents. The
  docs (deployment.md "twice the set's `engine_bytes` plus 10 GiB") agree.
- **Fixture.** `backup_fixture_with` takes the set with `store.backup_start` on an open store
  (rpc.rs:4954-4967). `traffic_fixture` runs before that backup: a stored watch catalog, a
  journaled pending claim in `.sparrow-inflight/` and two artifacts in the window
  (rpc.rs:5197-5262). It then adds `images/` and `config/sparrow/ship.toml` and rewrites the
  manifest (:5265-5296). The manifest stays valid: ship.toml is a regular file that both the
  writer (`find -type f`) and verify-backup's set diff (:469-472) cover. The `images/` entries
  are symlinks, which neither side lists, so the fixture's images are not checksummed, unlike
  a production set where rsync copies real files. This is only a fixture difference; both
  sides agree.
- **USAGE contract.** The line now holds `--query-only`, `--no-intake`, `--private-intake DIR`
  and `--restore-backup` (main.rs:18). verify-backup greps all four (:360, :364, :367, :371),
  and the rpc test's old-image double (rpc.rs:5603) checks the `--private-intake` refusal.
- **Argument handling.** The two `--restore-backup` arms are matched first and return
  (main.rs:112-116). Anything else goes to `arguments()`, which refuses a repeated `--config`,
  a second mode, a missing value and any unknown token with USAGE (main.rs:31-46). A
  `--restore-backup` with the wrong arity falls through to `arguments()` and gets USAGE, as on
  main. The 4-argument restore runs in the r6 traffic tests. The 3-argument form has no rpc
  test on either side.
- **Startup line.** `intake_label` prints `true` or `false` exactly as the old boolean did for
  the normal, no-intake and query-only modes, and `private` for the new mode (main.rs:138-142).
- **Rehearsal stand-in.** The rehearsal's "restored backup" is a reflink of daemon A's stopped
  scratch store (verify-backup.sh:713-716). I judge this acceptable under the ruling. The
  ruling and the orders name the measurement target and its inputs (measured copy, prep copy,
  record count, ship.toml), and all of those come from the online set. The stand-in is no
  backup of a live or production store, and it is not the measurement target. It fixes a
  point-in-time claim journal for a disposable scratch store, and this script cannot make that
  as an online set: the daemon refuses a non-network `backup_root` (core backup.rs:205-226),
  and the only local seam is `BackupTarget::local_for_test`. Writing to the real NAS root is
  guarded and would purge live sets past `backup_keep`. The stand-in is documented as a
  stand-in in the script comment (:677-679), the help, deployment.md and the PR body. It existed
  before the merge, so the port did not add it.
- **#161's tests.** Source unchanged except the disk-floor test, which this branch had already
  moved onto the `df` double (review 2, R3b) and which still asserts #161's messages. Not run on
  the merged tree: #161's daemon-level tests `daemon_backup_refuses_a_local_root_and_reports_it_in_status`
  and `backup_client_refuses_a_local_backup_root_before_asking_the_daemon`, and the
  sparrow-core backup tests. `main.rs` changed on the backup-gating path in this merge, so the
  hook's full run (pending, per validation.md) is the evidence those still pass.

## Findings

### REQUIRED

**R4-1. A `--private-intake` daemon takes online backups, which breaks #161's rule that a
copy never backs up.** main.rs:122 sets `let intake = !matches!(mode, Mode::NoIntake);`, so
`intake` is true under `--private-intake`. #161's gate at main.rs:298-306 then reads "Only the
daemon serving the live intake backs up ... a no-intake one serves a copy": it refuses backups
only when `!intake`, and otherwise calls `rpc.with_backup(target)` whenever the configuration
sets `backup_root`. `check_private_intake` (config.rs:315-320) does not refuse `backup_root`. A
private-intake daemon is a disposable copy with the same `store_id` as the live store, so
`backup(storeId)` passes the `STORE_MISMATCH` check. If its configuration names the live
`backup_root`, it backs up into the live NAS root and purges the oldest backups past
`backup_keep` (interfaces.md "Online backup"). The set's own `config/server.toml` names
`backup_root = "/net/synology/archives/sparrow"` when the live server.toml does
(server.toml.example:38), so a hand run from a set's configuration would do this. #161's own
text says why this matters: "backing it up would mix another store into the live backup root"
(backup.rs:15-16). The verify-backup scratch configuration writes no `backup_root`, so the
script path is safe today. The daemon mode's contract is not, and the merge produced the gap.
The interfaces.md refusal table (`--no-intake` row, :2865) and the `unconfigured` phase
sentence (:2851-2852) also leave out the new mode.
Remedy: treat private intake as a copy at the backup gate. Compute a `copy` flag from `mode`
before `private` is moved into `start_with` (`!matches!(mode, Mode::Normal | Mode::QueryOnly)`),
and use `if copy { rpc.without_backup_for_no_intake() }`. Word `Backup::NoIntake`'s message
and doc for both copy modes. Alternatively, refuse `backup_root` in `check_private_intake`
beside `archive`. Then add `--private-intake` to the interfaces.md refusal table and phase
sentence, and to the deployment.md mode bullet that lists refused keys (:624). Add one
behavioral check: either `sparrow SOCKET backup` on a private-intake daemon configured with a
`backup_root` returns `UNSUPPORTED` with status phase `unconfigured`, or, for the refusal
variant, a `refused(..., &["--private-intake", private], "backup_root")` case in
`query_only_and_no_intake_refuse_conflicting_configuration` (rpc.rs:4683).

### OPTIONAL

- **O4-1.** docs/deployment.md:790 says "The `--to` image must be built from a revision with
  `--private-intake` and `--restore-backup`", but the command below it passes the image with
  `--image`. Say "The `--image` image (the `--to` image of the switch)" so the flag matches.
- **O4-2.** The two-restore free-space rule (verify-backup.sh:486-490) has no test. The `df`
  double can protect it: report 60 GB with a SOURCE `engine_bytes` of about 26 GB (manifest
  rewritten). Then one restore fits, and `--with-traffic` must refuse with "2 time(s)". The
  contract does not need it; it would protect the boundary this port changed.
- **O4-3.** The fixture takes its set after the consume worker has stopped (rpc.rs:5231-5240,
  then `backup_start` at :4955). So the set holds intake state, a catalog, a pending claim and
  window artifacts, but no intake ran during the backup. That meets the order as written
  ("through the online path ... from a store with live intake"). Starting `backup_start` while
  the worker is still running would match the ruling's "with live intake" more closely, at
  some cost in determinism.

VERDICT: BLOCKED

## Recheck

Scope: the unstaged edits on top of the staged merge (`git diff`): config.rs, main.rs,
tests/rpc.rs, docs/interfaces.md and docs/deployment.md. Read-only, as before.

Evidence:

- r7-fmt.log EXIT=0; r7-clippy.log (release, sparrow-server) EXIT=0; r7-filtered.log EXIT=0,
  19 passed and 72 filtered out. The filtered run includes
  `query_only_and_no_intake_refuse_conflicting_configuration` and
  `verify_backup_migration_measurement_refuses_to_start_below_its_disk_floor`. Every log is
  newer than the edited files: the last source edit (rpc.rs) is at 14:50:36, fmt at 14:50:39,
  clippy at 14:50:48 and the filtered run at 14:56:45.
- shellcheck2.log EXIT=0. I reran `bash -n deploy/verify-backup.sh` myself: EXIT=0.
- prose-docs2.log EXIT=1, with nine `UInt64`/`UInt32` findings in docs/interfaces.md only.
  3735c1e has the same nine `UInt` mentions, so they are not new. No new prose finding.

R4-1 (REQUIRED): resolved.

- `check_private_intake` (config.rs:316-324) refuses `backup_root` before anything starts, and
  the error names the key. main.rs:133 calls it before the store opens, so the backup gate at
  main.rs:298-306 never sees a private-intake daemon with a target.
- The new `refused(..., &["--private-intake", private], "backup_root")` case
  (rpc.rs:4743-4749) checks the message prefix `--private-intake refuses the configured
  \`backup_root\`` that the helper requires, and that the store was not opened. The appended key
  is top-level, like the passing `rpc_tcp_address` case, and the path sits outside `data_dir`,
  so `ServerConfig::read` accepts it and the refusal comes from the mode check.
- interfaces.md now lists `backup_root` among the mode's refused keys, says why, gives the
  `unconfigured` phase and adds a refusal-table row. deployment.md:624-626 gives the key list.

O4-1: resolved. deployment.md:791 now reads "The `--image` image, the `--to` image of the
switch".

O4-2: resolved. The added case (rpc.rs:5180-5222) sets `engine_bytes=26000000000` and rewrites
the manifest, so the manifest check passes and the refusal comes from the space check. Under the
60 GB `df` double, one restore (26 GB plus 10 GiB) fits and two do not. The test asserts exit 1,
the exact text "2 time(s) needs 52000000000 bytes plus 10 GiB", no `PASS restore:` and an empty
verify root. If the rule went back to one copy, the run would get past the check and the test
would fail.

O4-3: not adopted, which is acceptable. The reason holds: the exact `backup_live_records=4`
assertion depends on a set with a fixed count, and a running worker would recover the
deliberately pending claim. Coordination makes the call. I said in the report that the order is
met as written.

New, OPTIONAL:

- **O4-4.** With `backup_root` refused at startup, a `backup` call on a private-intake daemon
  reaches `Backup::Unconfigured`. Its message says "configure backup_root on the NAS archive and
  restart" (backup.rs:26-29), but this mode refuses exactly that. The new interfaces.md row says
  `UNSUPPORTED`, which is correct. The operator text points the wrong way, but only on a call
  nobody should make. Remedy if wanted: call `rpc.without_backup_for_no_intake()` for the
  private mode as well (main.rs:300), and word `Backup::NoIntake`'s message for both copy modes.

Still open, as before: #161's daemon-level backup tests and the sparrow-core backup tests have
not run on the merged tree. The hook's full run is the evidence for them.

VERDICT: PASS
