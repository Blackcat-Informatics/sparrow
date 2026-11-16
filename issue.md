# Issue #330: Remove the online store backup (#156) and the upgrade script's dependence on it; guard the archive mount

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Operator's ruling (relayed by coordination, 2026-11-16 00:20): Sparrow backs up nothing except the age-out to the NAS in GTS files. The online store backup (#156) goes. The nightly backup timer is already disabled on the live host (23:44).

## Remove
- `sparrow-backup` (client and `deploy/libexec/sparrow-backup`), the RocksDB BackupEngine path (`crates/sparrow-core/src/storage/backup.rs`, `Store::backup_start`, the server's backup RPC and its worker), the offline `backup::restore`, `deploy/verify-backup.sh`, `deploy/pre-upgrade-backup.sh`, `deploy/systemd/sparrow-backup.service` and `.timer`, and the documents that describe them.
- `backup_root` / `backup_keep` from the server configuration. If the config refuses unknown keys today, an old file naming them is refused with a named error; say which it is.
- Wire: the backup RPC methods are removed only if that can be done without moving an ordinal (additive-only rule): if not, they stay as refused stubs with a named `UNSUPPORTED`, documented.

## Change
- `deploy/user-unit-upgrade.sh` (and `install.sh`, `check.sh`, `restart.sh` if they rely on it) no longer require a verified backup. Say plainly in its text what an upgrade then relies on: the kept previous image and link swap; a format-changing upgrade has no undo.
- Add to the archive worker (small, same change): it refuses to write, with a named error, when the archive directory is not on the expected mount (compare the directory's device with its parent's, or a marker file); filling the root disk with archives because a mount is absent is the failure a log store cannot shrug off. Mechanism is the implementer's choice; state it.

## Not in this change
Deleting `/net/synology/archives/sparrow` (42 GB, the BackupEngine tree): after this merges, by exact path, on coordination's word.

## Acceptance
- Nothing in the repository, deploy files, docs or helpers ledger refers to the removed feature except a short note of its removal where history matters; `make check` passes (it runs census).
- A test for the mount guard (real directories; the device comparison is a pure function).
- One independent review.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
