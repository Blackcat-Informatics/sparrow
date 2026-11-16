# Issue #332: Archive worker writes STORE_ID on its first pass; mount guard checks the filesystem type

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

Follow-up to #330 (coordination, 2026-11-16).

1. The archive worker writes `STORE_ID` itself on its first pass when the archive directory is on the mount and empty, so a hand-written identity file is not a step that gets forgotten. It still refuses a directory that already holds a different store's `STORE_ID`, and never overwrites one. (Today the worker requires `STORE_ID` and nothing writes it since the backup engine was removed.)
2. Close the mount guard's gap: the #330 guard compares the archive directory's device with `/`'s, so an unmounted share whose parent is another local btrfs subvolume (for example under `/home`) passes. Require that the archive directory's filesystem type is not the root's, or that it is a network filesystem, whichever is simpler and testable without a mount (a pure function over the type, as the device check is).

Acceptance: tests for first-pass write, refusal of another store's id, the filesystem-type refusal; docs (`deployment.md`, `interfaces.md`, README) updated; one review.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
