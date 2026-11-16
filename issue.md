# Issue #313: Start fails after a reboot: tail checkpoint root device number changes on btrfs

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Problem

After the forced host reboot on 2026-11-15, `sparrow.service` could not start. Every start ended with
`State("active file checkpoints require their original watch revision, root and interpretation; restore that definition before recovery")`,
and the unit hit its start limit. lillith's intake has been down since the reboot.

## Evidence

- `TailRoot::open` (`crates/sparrow-server/src/tailing.rs`) compares each incomplete, ungapped tail generation's
  declared `root_device` and `root_inode` with the root's current identity. It runs for every catalog watch with
  `active_files_retain`, enabled or not.
- The stored declarations of watch `development-tail-8a45378` (revision 7) say `root_device=56, root_inode=55520248`.
  After the reboot the same directory reports `dev=55, ino=55520248`. `/home` is btrfs (`@home`); btrfs hands out
  an anonymous `st_dev` per mount, so the device number is not stable across boots.
- No startup mode or RPC clears the refusal on the live store: `--no-intake` has no watch controller, `watch_put` and
  `watch_remove` refuse a tail watch with active generations, no RPC gap-marks a generation, and disabling does not skip the open.

## Change

When a generation matches on inode, watch revision, source and format and only the root device differs, record the
existing `identity_changed` discontinuity (`Store::tail_discontinue`) instead of refusing, write one operator line naming
the watch and both device numbers, and continue. Every other mismatch still refuses. No stored-format change.

## Acceptance

- A test shows both arms: a device-only change discontinues the generation and the open succeeds; a changed inode,
  revision, source or format still refuses.
- The operator line names the watch id and both device numbers.
- Committed generations' records are untouched.

## Not in scope

A stable identity across mounts (filesystem UUID, subvolume, inode) is a stored-format question for the operator; filed separately.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
