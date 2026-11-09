# Validation index: #156

VERDICT: PASS (committed 31a026c, pushed, PR #161; not landed: waits for #160 and #158 and the coordinator)

## State

- Inherited WIP preserved: worktree tree a97cdd2 (ref refs/wip/156-worktree), index tree 5e7da01
  (ref refs/wip/156-index), plus wip/staged.patch, wip/unstaged.patch, wip/untracked.tar.
  Unpublished local refs only.
- Synced onto origin/main ea1a4b2 with `git merge-tree` (conflicts in
  crates/sparrow-core/src/storage.rs and docs/deployment.md, resolved keeping both sides);
  branch HEAD reset to ea1a4b2 with the merged result uncommitted in the worktree.
- Review 1 (reviews/delivery-review-1.md): BLOCKED, R1-R6 required. Fixes applied:
  - R1: deploy/systemd/sparrow.service re-allows `setpriority` after `~@resources`. Installed user
    unit (read-only check) has no SystemCallFilter, Nice=10.
  - R2: deploy/apparmor/sparrow grants `@{PROC}/@{pid}/mountinfo r`.
  - R3: `linked_physical_bytes` counts table/blob files at most the bytes RocksDB versions
    reference (per-family total-sst-files-size + total-blob-file-size) and skips WALs below
    rocksdb.min-log-number-to-keep, so files held for a backup never count toward budget or
    eviction. A guard thread (normal priority, spawned before the backup thread lowers itself,
    never touches NFS) checks free space every 250 ms and re-enables file deletions when free
    space < 2 x min_free_bytes; the backup then fails if it still needed a file. Unit test
    `files_held_for_a_backup_do_not_count_and_the_guard_releases_them_near_the_floor`.
    No unsafe: rust-rocksdb 0.25 does not expose StopBackup; RocksDB's EnableFileDeletions
    counter is floor-guarded at 0 and nothing else in Sparrow disables deletions.
  - R4: daemon-level test `daemon_backup_refuses_a_local_root_and_reports_it_in_status`.
  - R5: user-unit-upgrade.sh requires backup_root == SPARROW_BACKUPS in both modes; README and
    deployment.md state the key order (add immediately before the switch, remove before rollback).
  - R6: server.toml.example and README step 3 corrected; RECOVERY effect paragraph corrected.
  - Optional done: O1 (rollback window stated), O2 (client schema/link check), O3 (ledger
    wording/vectors), O7 (test-instance header), O8 (BFQ qualification). Not done: O4 (accepted;
    client success path first runs at D147), O5 (lifecycle lock on NAS), O6 (NFS squash mapping,
    deployment-time), O9 (backup authorization scope).
- Post-sync fixes: verify_backup_fails_on_wrong_store_id rewrites the manifest after each
  deliberate SOURCE edit; stale "closed-store backup" links fixed in
  docs/development-runtime.md and docs/load-qualification.md.

## Scope and evidence

| Check | Log | EXIT |
| --- | --- | --- |
| cargo build --locked --release -p sparrow-server --bins (pre-review) | logs/build-release.log | 0 |
| sparrow-core lib+storage tests filtered `backup` (pre-review) | logs/test-core-backup.log | 0 |
| sparrow-server rpc backup tests (first run) | logs/test-server-backup.log | 101 (fixture fixed) |
| same, rerun (pre-review) | logs/test-server-backup-2.log | 0 (5 passed) |
| shellcheck -x deploy scripts (incl. test-instance.sh), bash -n, Python parse of sparrow-backup | logs/static-checks.log | 0 |
| cargo fmt --all --check (pre-review) | logs/fmt-check.log | 1, fixed by cargo fmt |
| sparrow-core lib + whole storage test target (post-review, first) | logs/test-core-all-2.log | 101 (2 lib tests: raw test DBs lack families; fixed) |
| sparrow-core lib (40) + whole storage target (236) | logs/test-core-all-3.log | 0 |
| sparrow-server rpc backup tests incl. daemon_backup (first) | logs/test-server-backup-3.log | 101 (fixture dir; fixed) |
| sparrow-server rpc backup tests, 6 incl. daemon_backup | logs/test-server-backup-4.log | 0 |
| make check via pre-commit hook (git commit 31a026c, 1158 s) | logs/commit-hook.log | 0 |

Measured ingest latency beside an online backup (test-core-backup.log):
before p50=528us p95=772us; during p50=541us p95=636us max=842us n=63; after p95=867us.

## Findings

- Wire addition kept within schema 70 (reviewer 1: acceptable; precedent 4b108a9).

- Review 2 (reviews/delivery-review-2.md): VERDICT: PASS. Optional N1 (guard skips enable when
  deletions already enabled) and N2 (interfaces note on unreferenced flush/compaction output) applied
  after review; covered by the commit hook. N3 (test for --no-backup without backup_root) not added.
- Coordinator order (2026-11-09): no new cargo command until #160's measurement, hook and review
  have run; already-queued commands may finish.

## Evidence rule (coordinator, 2026-11-09)

Only logs that carry their own EXIT line count as evidence. build-release.log, fmt-check.log,
fmt.log, shellcheck.log and test-server-backup-2.log carry none and are superseded: compilation and
tests by test-core-all-3.log and test-server-backup-4.log (cargo test builds the binaries), static
checks by static-checks.log, and formatting by the commit hook's `make check` (commit-hook.log).
