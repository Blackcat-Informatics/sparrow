# Delivery review 2: #156 Back up Sparrow only to the NAS archive (recheck)

Reviewer: the same independent delivery reviewer. Read-only. No cargo, make or build-system
probing, and nothing live touched.

## Scope

This recheck covers review 1's required findings R1 to R6 and the code and text the fixes
changed, judged from the interdiff between `review.patch` and `review-2.patch` (568 lines). Two
files are new in the diff: `crates/sparrow-core/src/storage/space.rs` (a `min_free()` getter)
and `deploy/test-instance.sh` (header). Unchanged history is not re-reviewed.

Checked against current source:

- `deploy/systemd/sparrow.service:123-127`
- `crates/sparrow-core/src/storage/space.rs` (`shortfall_after` semantics)
- `crates/sparrow-core/src/storage/backup.rs` (guard)
- `deploy/libexec/sparrow-backup:205-241`
- `deploy/user-unit-upgrade.sh:194-227`
- vendored RocksDB 11.8.1: `DBImpl::EnableFileDeletions` (`db_impl_files.cc:169-197`) and
  `FindObsoleteFiles`

## Evidence

| Check | Log | Exit | Scope |
| --- | --- | --- | --- |
| sparrow-core lib (40) and the whole storage test target (236) | logs/test-core-all-3.log | 0 | Includes `files_held_for_a_backup_do_not_count_and_the_guard_releases_them_near_the_floor` and every storage test that reads `physical_bytes`, budget or eviction |
| sparrow-server rpc backup tests (6) | logs/test-server-backup-4.log | 0 | Includes `daemon_backup_refuses_a_local_root_and_reports_it_in_status`, `user_unit_upgrade_refuses_before_changing_anything` and `verify_backup_fails_on_wrong_store_id` |
| Rerun of the earlier server tests | logs/test-server-backup-2.log | 0 (recorded) | 5 passed |
| `make check` | none | pending | The mandatory pre-commit hook. It runs the rest of the sparrow-server suite, which the `linked_physical_bytes` change can affect, plus release Clippy and fmt. |

`prose-check` on the changed docs, scripts, unit, profile and sources exited 1. Its nine hits
are all `UInt64`, `UInt32` or `binary64` on unchanged lines; the changed text adds none.

## Recheck of the required findings

**R1. Resolved.** `SystemCallFilter=setpriority` follows `~@privileged @resources`
(`sparrow.service:127`). In an allow-list filter, systemd merges later entries, so the line adds
`setpriority` back. The comment states the reason. The `rocksdb::Env` and backup engine options
in use do not lower thread priority (`CreateBackupOptions` defaults), so nothing else in this
path calls `@resources`. validation.md records a read-only check of the installed user unit: no
`SystemCallFilter`, `Nice=10`.

**R2. Resolved.** `@{PROC}/@{pid}/mountinfo r,` is granted, with its reason, beside the other
`/proc` entries.

**R3. Resolved.**

- **Budget and eviction.** `linked_physical_bytes` now counts `.sst` and `.blob` bytes at most
  the per-family sum of `rocksdb.total-sst-files-size` and `rocksdb.total-blob-file-size`, which
  RocksDB computes over unique files of live versions. It skips `N.log` with
  `N < rocksdb.min-log-number-to-keep`. Files held only by `DisableFileDeletions` therefore count
  toward neither `admit_data_write` nor `maintain_pressure`.
- **Free-space floor.** A guard thread starts before the backup thread lowers its priority and
  never touches the root. Every 250 ms it calls `shortfall_after(min_free)`, which reports a
  shortfall when free space is below twice the floor, and on a shortfall or a failed measurement
  it calls `enable_file_deletions()`.
  - In RocksDB 11.8.1 the counter is floored at 0 (`db_impl_files.cc:176-178`).
  - `CreateCustomCheckpoint` takes no extra disable, and nothing else in Sparrow disables
    deletions (grep), so a release before or after the backup's own window is harmless.
  - The guard result is combined into the backup's failure message, and a guard-spawn failure
    marks the slot failed.
- **Test.** The unit test checks the invariant directly: held bytes are excluded, the guard does
  nothing above the threshold and releases below it, and every file counts once nothing is held.
  The 236 storage tests passed with the new accounting.
- **Docs.** RECOVERY, deployment.md and interfaces.md now state the effect correctly.

**R4. Resolved.** The daemon-level test runs the real `sparrowd` and `sparrow` binaries:

- Without `backup_root`, the phase is `unconfigured` and `backup` is refused with
  `UNSUPPORTED`.
- With a local root, `backup` returns `running` with `root`, `keep` and `started_at`, then
  `status.backup` turns `failed` with matching `started_at`, a set `finished_at`, `backup_id` 0
  and the not-a-network-filesystem failure. The root is never created.
- It asserts the `status` fields the client reads.

The exit statuses are recorded.

**R5. Resolved.** `user-unit-upgrade.sh` now requires `backup_root == SPARROW_BACKUPS` in both
modes, so TO always starts with its backup configured. The error message names the order: add
the keys immediately before the run, remove them before a rollback. README "Interim user unit",
deployment.md and the script's `--help` say the same.

**R6. Resolved.**

- The template comment now says startup checks only the path, and warns that older images refuse
  the keys.
- README step 3 says the template sets the keys and that the NAS directory must be created.
- RECOVERY now reads "Effect: none on ingestion", with an accurate account of the held files.

Optional items done: O1 (the rollback window is stated), O2 (the client refuses when the link's
schema differs from `status.schema_version`; `status` is the last poll's), O3 (ledger summary
and vectors), O7 (test-instance header), O8 (BFQ qualification). These were checked and are
correct. O4, O5, O6 and O9 are declined with reasons in validation.md; they stay optional.

## Findings

### N1. OPTIONAL: avoid repeated full scans under the DB mutex

Once the counter is 0, every further guard call during a shortfall makes
`EnableFileDeletions` run `FindObsoleteFiles(force full scan)`, and its `GetChildren` runs with
`mutex_` held (`db_impl_files.cc:181`, `FindObsoleteFiles` asserting the mutex). While free space
stays below twice the floor for the rest of a backup, that is a directory scan under the DB mutex
four times a second. It is small and happens only near the floor, but it is avoidable: call
`enable_file_deletions()` only while `rocksdb.is-file-deletions-enabled` reads 0.

### N2. OPTIONAL: the `physical_bytes` description is slightly incomplete

`min(tables, referenced)` also leaves out table and blob files that are not yet referenced (flush
and compaction outputs still being written), not only obsolete ones. These used to count, so a
large compaction can now briefly exceed `disk_budget` on disk; the free-space floor still guards
the disk. `docs/interfaces.md` says only "except the files RocksDB no longer references". Add
"or does not yet reference", or note it beside `disk_budget`.

### N3. OPTIONAL: the new `--no-backup` refusal has no test

The check that `--no-backup` without `backup_root` is refused is new behavior with no test. The
existing refusal test passes because its earlier refusals fire first. One more case in
`user_unit_upgrade_refuses_before_changing_anything` would cover it.

Open state, not a finding: the change is uncommitted, and `make check` runs in the pre-commit
hook. Per plan, the client's success path, the system-unit sandbox and the first NAS backup with
its verify are first exercised in the D147 window.

VERDICT: PASS
