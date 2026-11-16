Closes #332.

## Change

- **Identity file written by the worker.** On its first pass the archive worker writes `STORE_ID` (the store id plus a newline) into the archive directory, but only when the directory is on its own mount and empty. The write goes through `publish` (temp file, fsync, rename, directory fsync), which `write_segment` now shares. A matching `STORE_ID` is accepted without a rewrite; another store's file is refused and never overwritten; a directory with segments but no `STORE_ID` is refused, not adopted. A leftover `STORE_ID.partial` from a crash does not block the first write.
- **Mount guard.** `require_own_mount` (pure over device and filesystem type) replaces the device-only check. The directory's device must differ from `/`'s, and its filesystem type must be a network type (nfs, smb, cifs, ceph, afs, 9p, lustre, ncp, coda) or differ from `/`'s. A `statfs` failure is a loud error. The refusal names the device or the type found.

## What this does not stop

- An empty mount point inside a local filesystem of a type other than `/`'s passes both rules and is adopted; segments then go to the local disk. The configured `dir` must therefore be a directory inside the share, never the mount point itself (`/net/synology/archives/sparrow-archive` as deployed). An unmounted share leaves no such subdirectory.
- FUSE is not treated as a network filesystem.

## Known limit

Two writers racing on one empty directory could both pass the read and the rename could replace a `STORE_ID` created in between. One archive thread per daemon and the store lock make this unreachable in the supported setup; an atomic create (`link(2)`) was judged not worth the change.

## Docs

`deploy/README.md`, `deploy/RECOVERY.md`, `docs/deployment.md`, `docs/interfaces.md` and `docs/architecture.md` 4.4 now describe the worker-written file, the two mount rules and the mount-point caveat. The operator no longer writes `STORE_ID` by hand.

## Tests

- `the_first_pass_writes_store_id_into_an_empty_directory_and_the_second_accepts_it` (inode comparison proves no rewrite)
- `a_leftover_store_id_partial_does_not_block_the_first_write`
- `the_watermark_stays_when_the_root_is_not_this_stores` (another store's file untouched; segments without STORE_ID refused)
- `the_mount_rules_refuse_the_roots_device_and_its_local_filesystem_type` (table test)

Not run: any real NFS or CIFS mount (the network types are covered only by the pure table test). The daemon integration tests in `rpc.rs` use a /dev/shm archive directory and fail loudly on a refusal.

## Evidence

Main checkout `.stage/evidence-keep/332/`: hook `commit3.log` at 96a30f6 (EXIT=0); the first commit's hook at 14c7925 (`commit.log`, EXIT=0); the failed attempt (`commit2.log`, EXIT=1, a conflict marker from the merge, since resolved). Review `.stage/archive/review-332.md`: BLOCKED at 14c7925 on two false doc claims; fixed in 96a30f6 and read by the primary. The merge of main 17f4cfe is in 96a30f6.
