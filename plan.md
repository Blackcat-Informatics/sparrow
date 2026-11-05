# Plan: copy-on-write off for the store (Patrick's 2026-10-15 ruling, Katamari's mechanism)

## Behavior
- deploy/lib.sh gains `switch_off_copy_on_write` (Katamari's pattern: chattr +C on a directory, btrfs only, never a failure, skipped when the directory already holds copy-on-write files), `match_copy_on_write` (an empty copy directory takes its source's attribute) and `convert_store_no_cow` (rewrite a closed store by copy into a +C sibling, compare its file list, sizes, modes and bytes, swap, and keep the original as `store.cow-STAMP`).
- deploy/copy-on-write.sh exposes the three functions as `off`, `match` and `convert`.
- install.sh switches copy-on-write off for /SPARROW/store and /SPARROW/backups. restart.sh converts the closed store when any file still has copy-on-write. verify-backup.sh, sparrow-backup's copy_tree and the RECOVERY restore steps match the source's attribute before a reflink copy.

## Dependencies
None.

## Exclusions
The live conversion itself runs at the combined restart, which uses its own user-unit script.

## Acceptance
- Rehearsal on a reflink copy of the 2026-11-04 nightly (rehearsal.log): the conversion left 336 files, 0 with copy-on-write, and kept the original. verify-backup passed every check it could run on the converted copy with image 6737fac. It ended NOT VERIFIED (exit 1) only because the old helper's SOURCE records no payload count.
- Shellcheck is clean, and the commit hook (make check) passes.

## Risk
Backup and restore tooling. A reflink across attributes is refused by btrfs, and --reflink=auto then copies every byte.

## User rulings
- 2026-11-04, Patrick via coordination, verbatim: "Make sure that sparrow is using katamari's COW btrfs trick on db."
- 2026-11-04, coordination approved: "set chattr +C on store, backup and verify directories in deploy/restart.sh and install.sh per Katamari's pattern (directory only, inherited, never a failure); at the restart, after the named pre-upgrade backup (which stays COW as the way back to schema 62), convert the closed store by copy into a +C sibling with file list and sizes verified, swap, lsattr every file, and keep the old COW copy until the first nightly passes; verify-backup and restore use plain copies where a reflink would cross a COW and a +C tree."
- Decision recorded: instead of plain copies, a copy's directory takes its source's attribute, so the reflink never crosses attributes and no copy duplicates every byte. Conversion is measured at 425 s for 13 GB (330 s copy, 92 s byte comparison), and is kept simple rather than pre-copied.

- Review round 1 BLOCKED (review.md): B1, a stale socket broke the comparison, so sockets are now excluded from the copy and the comparisons (a socket is not data, and the daemon creates its own); directory sizes are not compared; the copy is synced before the swap. S1, the restart script's preconditions, was fixed in that script.
