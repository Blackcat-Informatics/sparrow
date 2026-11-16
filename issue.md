# Issue #315: Design: stop persisting the device number as identity (works on any filesystem; operator ruling needed)

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Problem

Sparrow's daemon and shipper treat the file system's device number (`st_dev`) as part of a root's or file's identity. The device
number is the only part of `(device, inode)` that is not stable across a reboot on every kind of mount: btrfs subvolumes, but
also network and overlay mounts and removable or re-enumerated disks, get a different number after a boot. After the forced
reboot of 2026-11-15 this stopped the daemon from starting (#313) and made the shipper re-ship every source file (#314).

(An earlier version of this design used a filesystem UUID and subvolume id. The operator denied it: it assumes btrfs. This version
assumes no particular filesystem.)

## Where the device number is part of identity (to be confirmed by the design)

- Stored tail declarations: `root_device` and `root_inode` (`crates/sparrow-server/src/tailing.rs`, `TailRoot::open`).
- Pending consumption journals: `bound_here` in `ClaimRoot::from_root` (`crates/sparrow-server/src/consumption.rs`) compares the stored `root_device`.
- Completed-file identity: `CompletedFileIdentity` carries device, inode, length, mtime and ctime (`crates/sparrow-client/src/imports.rs`),
  used by claims, finish intents and tail discontinuity checks.
- Held-root binding: `source_file.rs` compares `dev()` of the held root with the path's.
- `sparrow-ship` ledgers: `device:inode:size:mtime_ns` (#314).

## Proposal (design only): do not store or compare a device number across a restart

What identifies a thing on any filesystem and survives a reboot, with nothing but what the filesystem itself reports:

1. A root: its inode number, plus the fact that the daemon holds an open descriptor to it while it runs. Within one run the held
   descriptor already fixes the object, so the device number adds nothing there. Across a restart the daemon re-resolves the configured path and
   compares the inode only.
2. A file: inode, size and mtime. Within one run the held descriptor fixes it. Across a restart, a stored checkpoint resumes only if
   the file's current size is at least the checkpointed offset and its inode is equal; a smaller size, or a different inode, takes
   the existing `identity_changed` or `truncated` discontinuity.
3. The device number may still be read for same-boot checks, but is never persisted as identity and never compared with a persisted value.

The known weakness is inode reuse after a delete and create: a new file may get the old inode and be larger than the old offset.
The device number never detected this either (the same device is reused), so removing it loses no protection. Closing the gap
would need a prefix digest of the checkpointed bytes in the checkpoint, which is a stored-format change and is not proposed here.

## Needs the operator's ruling before anything is written

- Dropping `root_device` from the comparison is not a format change. Removing the field from stored declarations is, and is not proposed: the stored field stays and is ignored.
- Decide: does #313's discontinuity path stay as the permanent handling of a device-only mismatch, or become the plain "match" under this design?
- Decide the ship ledger key for #314: inode, size, mtime (device dropped), folding existing three-epoch ledgers.

## Acceptance of this issue

A written design the operator can rule on. No code until he rules. If he rules the simplest alternative, the work is: stop comparing
the persisted device number in the places listed, with a test that a device-only change leaves identity equal and an inode, size or mtime change does not.

Related: #313 (startup fix, required and stands), #314 (shipper ledger, required and stands).


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
