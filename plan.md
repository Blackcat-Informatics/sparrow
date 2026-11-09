# Plan: #156 Back up Sparrow only to the NAS archive, never to a local disk

## User rulings

Relayed by lillith_coordination in the implementer brief (2026-11-09); these override the issue body.

- Patrick, verbatim: "Backup may not: stop the server, pause the server, or in any way perturb the ingestion."
- Patrick, verbatim: "There is no "closed store window""
- Patrick, verbatim: "That's the POINT OF SPARROW"
- Patrick, P28, verbatim: "no backup may EVER stop lillith, lose data, or drop packets.  All backups MUST be online and perturb no operations."
- Patrick, 2026-11-08 ~19:45 CST, verbatim: "Sparrow and katamaria are FORBIDDEN from backing up to local disks."
- Location: /net/synology/archives/sparrow.
- Coordinator (2026-11-09): landing order #160, then #158, then #156; stop before ghprsq and report.
- Coordinator (2026-11-09, verbatim excerpt): "issue no NEW cargo command (build, test, clippy, fmt, or a commit that runs the hook) until #160's scratch measurement, hook and review have run. Any command you already have running or queued behind the lock may finish."
- Coordinator (2026-11-09, verbatim excerpt): "every gate log must carry its EXIT inside the file itself (redirect to the log, then echo "EXIT=$?" into it). An exit code printed by a wrapping shell is not evidence. Your PR body must cite logs that contain their own EXIT line."
- Patrick (CLAUDE.md), verbatim: "DO NOT mess with, interogate, attemp to fix, or otherise probe the build system unless you are explictly asked to.  If build errors occur, backoff and wait."

## Behavior

- The running daemon takes an online backup on request (`backup` RPC, CLI `sparrow SOCKET backup`) with
  RocksDB BackupEngine on one nice-19 thread over the shared DB handle; no writer lock, no flush, no stop.
- The backup root must be a network filesystem apart from the store's, judged at every backup
  (mountinfo, before creating anything and again on the canonical path); startup checks only paths.
- `deploy/libexec/sparrow-backup` asks the daemon, polls status, copies images, lexicons, config and
  intake into `sets/ID.partial` on the NAS, syncs, writes MANIFEST.b3 read back from the NAS, renames
  into place, prunes sets whose engine backup was purged. No local staging.
- `pre-upgrade-backup.sh` and `user-unit-upgrade.sh` take the pre-upgrade backup online to the NAS;
  `--no-backup REASON` for a daemon without the operation (schema 68 to 70 switch).
- `verify-backup.sh` verifies a NAS set: manifest b3sum, restore with checksum, query-only checks.
- Offline restore `sparrowd --restore-backup ROOT [ID] EMPTY_DIR`.
- Units, apparmor, tmpfiles, install/uninstall, docs updated together.

## Dependencies

- origin/main ea1a4b2 (synced 2026-11-09). Landing after #160 and #158; resync before commit.

## Exclusions

- No change to the live store, daemon, units, timers or the NAS. Installing the user-unit script and
  unit, the first NAS backup and its verify happen in the D147 window after merge.

## Acceptance

- Online backup completes beside continuous ingest with no refusal, and measured ingest latency.
- Local backup root fails before creating anything; client refuses before asking the daemon.
- verify-backup.sh verifies a set and fails on a manifest mismatch or another store.
- `make check` through the pre-commit hook.
- Post-deploy (not in this PR): a NAS backup and its verify-backup.sh pass.

## Risk

- Wire addition kept within schema 70 (precedent: schema 70 features added in place; no store format
  change). Decision recorded; reviewer to assess.
- A daemon exiting during a backup abandons it; engine discards partial files on next open.
