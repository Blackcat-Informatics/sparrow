Closes #156.

## Behavior

Backups are online and go only to the NAS archive `/net/synology/archives/sparrow`. Patrick's
rulings: "Backup may not: stop the server, pause the server, or in any way perturb the ingestion.",
"There is no "closed store window"", and "Sparrow and katamaria are FORBIDDEN from backing up to
local disks."

- The running daemon takes the backup. `sparrow SOCKET backup` (RPC `backup`) starts RocksDB's
  BackupEngine on one thread at nice 19, over the database handle the readers already share. It
  takes no writer lock and flushes nothing, and nothing is stopped, paused or drained. `status`
  reports `backup` (phase, times, id, files, bytes, failure). One backup runs at a time; a second
  request is refused with `CONFLICT`.
- `backup_root` must be on nfs, nfs4, cifs or smb3 and not on the store's filesystem. Each
  backup checks this in `/proc/self/mountinfo`, before it creates anything and again on the
  canonical path. Startup checks only the path, so a daemon whose NAS is down still serves.
- Files RocksDB holds for the copy do not count toward the disk budget or eviction: table and blob
  files count only the bytes RocksDB versions reference, and logs below
  `min-log-number-to-keep` do not count. A guard thread at normal priority re-enables file
  deletions once free space falls below twice `min_free_bytes`, and the backup then fails. A
  backup therefore never refuses a write, evicts evidence or brings ingest to the floor, even one
  stalled on the NAS.
- `deploy/libexec/sparrow-backup` asks the daemon for the backup and copies the image pair,
  lexicons, configuration and intake into `sets/ID.partial` on the NAS. It syncs, writes a
  `MANIFEST.b3` read back from the NAS, renames the set into place and prunes sets whose engine
  backup was purged. Nothing is staged locally.
- `pre-upgrade-backup.sh` and `user-unit-upgrade.sh` take the pre-upgrade backup online on the
  NAS. `--no-backup REASON` covers a running image without the operation (schema 68 to 70 at
  D147).
- `verify-backup.sh` checks a NAS set: b3sum of every manifest entry, the file list both ways, a
  restore with `sparrowd --restore-backup` (checksummed), then query-only checks.
- Units, the AppArmor profile (backup root and `mountinfo`), the seccomp allowance for
  `setpriority`, tmpfiles, install, uninstall, test-instance and docs (deployment.md, interfaces.md,
  README.md, RECOVERY.md) change together. The wire addition stays within schema 70, as #36 did
  for schema 69; there is no store format change.

## Measured intake impact

`online_backup_runs_beside_continuous_ingest_and_restores_a_consistent_store` takes a backup of
a 32 MiB store while a writer ingests continuously:

- No ingest was refused.
- Ingest latency before: p50 528 us, p95 772 us.
- During the backup (63 writes): p50 541 us, p95 636 us, max 842 us.
- After: p95 867 us.

The restored store holds every record committed before the backup started.

## Tested scope

Every log below carries its own EXIT line. Logs are under
`.stage/back-up-sparrow-only-to-the-nas-archive/logs/` in the worktree.

| Check | Log | EXIT |
| --- | --- | --- |
| `make check` through the pre-commit hook (fmt, release Clippy `-D warnings`, release workspace tests, census) | commit-hook.log | 0 |
| sparrow-core lib (40) and the whole storage test target (236), release | test-core-all-3.log | 0 |
| sparrow-server rpc backup tests (6, including the daemon-level backup test), release | test-server-backup-4.log | 0 |
| Online backup beside ingest (latency figures above) | test-core-backup.log | 0 |
| shellcheck of the deploy scripts, `bash -n`, Python parse of sparrow-backup | static-checks.log | 0 |

Independent review: review 1 BLOCKED with six required corrections, all fixed; review 2 `VERDICT: PASS`.

## Not yet exercised (D147 window, after merge)

The client's success path against a real NFS mount, the system-unit sandbox and the first NAS
backup with its verify run after deployment. Install the user-unit script and unit, add
`backup_root` and `backup_keep` to `~/.config/sparrow/server.toml` immediately before the switch,
then:

```bash
deploy/user-unit-upgrade.sh --from sparrowd-schema68-REV --to sparrowd-schema70-REV --no-backup "schema 68 has no online backup (D147)"
~/.local/libexec/sparrow/sparrow-backup ~/.config/sparrow/server.toml /SPARROW/intake ~/.config/sparrow /SPARROW/ship
SPARROW_CONFIG=~/.config/sparrow/server.toml deploy/verify-backup.sh /net/synology/archives/sparrow/sets/ID
```

Restart `sparrow-backup.timer` only after this merges and the verify exits 0.
