# Issue #31: Record verifiable SOURCE in nightlies taken under the interim user unit

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

The nightly backups (sparrow-backup.timer under the interim user unit) run an older hand-installed copy of `deploy/libexec/sparrow-backup` (2026-10-27). Its SOURCE lacks `store_id`, `image`, `schema_version`, `store_format`, `live_records` and `unique_payload_chunks`. So `deploy/verify-backup.sh` refuses a nightly as taken, and with `store_id` added by hand it still exits 1 with NOT VERIFIED (20261106T101316Z, 2026-11-06).

The repository helper records all of those, but refuses `SPARROW_SCOPE=user` with `sparrow.service`. That guard keeps test-instance runs off the real unit (lifecycle review O10). Until #22 moves production to the system unit, the user unit is production.

Behavior:
- The repository helper accepts the interim production user unit only on an explicit opt-in set by its own backup unit, and keeps refusing every other `SPARROW_SCOPE=user` run naming `sparrow.service`.
- The installed user backup unit runs the repository helper, so a nightly verifies as taken: `deploy/verify-backup.sh` exits 0 with no SOURCE edit.

Acceptance: the first nightly after the install passes verify-backup with exit 0, including the payload-chunks check.

#22 removes the opt-in together with the user unit.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
