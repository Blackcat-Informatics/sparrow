# Issue #156: Back up Sparrow only to the NAS archive, never to a local disk

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Ruling

Patrick, 2026-11-08 ~19:45 CST (relayed by lillith_coordination, verbatim): "Sparrow and katamaria are FORBIDDEN from backing up to local disks." "Have them remove those immediately if they have been." "SPARROW has a network backup localtion already specified."

The specified location is `/net/synology/archives/sparrow` (`deploy/systemd/sparrow-backup.service:11`, `docs/deployment.md:449`, `deploy/README.md:60`). At 20:19 CST on 2026-11-08 every local backup was deleted (D143). Sparrow has no backup until this lands, and `sparrow-backup.timer` stays stopped until it is merged.

## Change

- `sparrow-backup` writes its closed-store backup only to `/net/synology/archives/sparrow` (created if absent), and never to a local disk. It refuses to start when the target is not a network filesystem (NFS mount check) or is on the store's filesystem.
- The closed-store window stays short. The NAS cannot take a reflink, so the copy must not hold the daemon stopped for a full network copy. The design keeps the stop to a bounded window, for example a reflink snapshot on the local filesystem used only as a transient staging step, copied to the NAS and removed in the same run, or a filesystem snapshot. Any transient local staging is removed before the run reports success, and the run fails loudly if it cannot be removed.
- A copy is renamed into place only after every file is written and synchronized on the NAS.
- Pruning, SOURCE and the images, lexicons and config kept with each backup move to the NAS too.
- The installed user-unit script and unit, and the repository's script and units, are updated together. The installed script's drift from main is removed.
- `deploy/user-unit-upgrade.sh` writes its pre-upgrade backup to the NAS as well.
- Docs updated: deployment.md, README.md, RECOVERY.md.

## Acceptance

- A completed backup exists under `/net/synology/archives/sparrow/`, verified against the closed store by a file list plus `b3sum` of every file.
- No Sparrow backup bytes remain on a local disk after the run.
- `verify-backup.sh` verifies a NAS backup.
- `sparrow-backup.timer` is restarted only after this merges.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
