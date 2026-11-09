Refs #145

## Measurement target (D118, coordination ruling 2026-11-09)

Under P28 and D146 a closed-store backup is forbidden, so the measurement target is an
online backup set:

- Before the D147 switch: a set taken by the online path from a test store with live
  intake. This is the measurement's own fixture, which takes its set through
  `backup_start`.
- After the D147 switch: the first real online set on the NAS
  (`/net/synology/archives/sparrow/sets/ID`).

The real measurement on a NAS set runs after merge, so Done-when 1 and 3 stay open.

## What it adds

- `sparrowd --config FILE --private-intake DIR`: a writable daemon with full intake for a
  disposable copy. Every watch root is rebound to `DIR/NAME`. A configured root that is,
  holds or lies under `DIR`, or aliases it by device and inode, is refused. So are two
  roots with one final name. `consume`, `syslog`, `rpc_tcp_address`, `archive` and `backup_root` are
  refused.
- `deploy/verify-backup.sh --measure-migration --with-traffic [--rehearse-rollback]
  BACKUP_ROOT/sets/ID`, on top of #161's online backup sets:
  - The measured copy and the query-only prep copy are each restored from the set with
    `sparrowd --restore-backup`. The free-space check counts both restores.
  - `ship.toml` comes from the set's `config/sparrow/`, and the backup's own record count
    from its `images/`. No closed-store path remains.
  - It replays what the copy received in the hour before the backup: watch items go
    through the repository's shipper or straight into the private roots, and other
    artifacts go over the copy's ingest socket.
  - Status, records and sparql run every 30 s. The run prints MEASURE lines and one gate
    line per threshold with its margin.
  - Exit 0 means every gate passed and 4 means a gate failed. A crashed load worker fails
    the run with `FAIL load`.
- `deploy/reconcile-inflight.sh`: the rollback reconciliation for both of the issue's
  rollback findings. `--rehearse-rollback` and `deploy/user-unit-upgrade.sh --help` use
  it. The rehearsal's "restored backup" is a scratch copy of a stopped store. It stands
  in for a restore, because the daemon backs up only to a network `backup_root`.
- The gate thresholds are in `docs/deployment.md`.

## Checks

Every log cited here carries its EXIT inside the file. The logs are in the run
directory's `logs/`.

- `make check` on the merge 62ee39e (`make-check-62ee39e.log`, EXIT=0) and the hook on the
  head 1274cf1 (`r9-commit.log`, EXIT=0): fmt, release clippy, release tests and the census
  gates. sparrow-core has 40 unit and 236 storage tests, sparrow-server 32 lib and 8
  sparrowd tests, and the rpc binary 91 of 91, with none failing. That includes #161's
  backup tests on the merged tree: the online backup beside continuous ingest, the
  local-root refusals by the daemon and the client, and sparrow-core's network-filesystem
  and file-floor tests.
- After the review 4 fixes: fmt (`r8-fmt.log`), release clippy (`r8-clippy.log`) and the
  rpc tests matching `backup no_intake query_only private` (`r8-backup.log`, 10 passed),
  each EXIT=0. The filtered rpc set (`r7-filtered.log`, 19 passed, including all three
  traffic tests and the disk-floor test) also passed with EXIT=0. The set is 19 tests, not
  20, because #161 removed the closed-store chunk-count test.
- After review 5: shellcheck, bash -n and prose-check of the rollback text
  (`r9-prose-shell.log`, EXIT=0).

Unverified: the filtered set (20 passed) and the two full rpc binary runs (90 passed
each) on 38c3744. Their EXIT was printed by the wrapping shell, not written into the
logs.

## Review

Independent reviews: 1 to 3 on the delivery, 4 on the port, and a fifth at the pushed
head. Every finding of reviews 1 to 3 was fixed. Review 4 was BLOCKED on R4-1 (a
private-intake copy could take online backups into a configured `backup_root`), and its
recheck is `VERDICT: PASS`:

- R4-1, fixed: `--private-intake` refuses `backup_root` at startup.
- O4-1 and O4-2, fixed: the `--image` wording in the docs, and a test of the two-restore
  free-space rule.
- O4-4, fixed: both copy modes refuse backups with one message (`Backup::Copy`).
- O4-3 was not adopted. It would take the fixture's set while its consume worker runs.
  That makes the set's record count nondeterministic, and the exact
  `backup_live_records=4` check that protects the R2 fix depends on a fixed count. A
  worker owning the catalog would also recover the fixture's deliberately pending claim.
  The reviewer found the order met as written, and coordination accepted it.

Review 5 at 62ee39e confirmed O4-4 and found #161's code intact. It was BLOCKED on two
text and evidence findings, both fixed:

- R5-1: the rollback text said every nightly backup waits for a pending claim. Since #161
  the backup never waits for claims; the text in `docs/deployment.md` and
  `deploy/user-unit-upgrade.sh` now says the watch reports the failure until the claim is
  retired (1274cf1).
- R5-2: this body cited hook counts that no log recorded. The Checks above now cite
  `make-check-62ee39e.log` and `r9-commit.log`, which carry the counts and their EXIT.
- O5-1, fixed: `backup_root` is listed among the refused keys. O5-2, a `--no-intake`
  backup refusal test, is not added: main had none and the contract does not need it.
