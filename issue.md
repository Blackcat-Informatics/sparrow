# Issue #148: Bound the store's open files below the unit's nofile limit (live EMFILE crashes)

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Problem

The live daemon (the old user unit, `~/.config/systemd/user/sparrow.service`) crashed twice on 2026-11-08 with `EMFILE`:

- 16:43:52 CST: `maintenance worker terminated unexpectedly; maintenance shutdown: store durability uncertain; exiting so it is reopened: IO error: While open a file for random read: /SPARROW/store/106568.sst: Too many open files`. systemd restarted it (NRestarts=1). The store took about 25 minutes to answer again (startup line at 17:08:47).
- 17:44:14 CST: the same failure on `/SPARROW/store/107799.sst`, 35 minutes after the reopen (NRestarts=2).

## Cause

- The user unit sets no `LimitNOFILE`, so the daemon inherits the session's soft limit of 1024. The hard limit is 1048576.
- The repository's system unit `deploy/systemd/sparrow.service` sets `LimitNOFILE=65536`, but live does not run that unit.
- The store opens with RocksDB's default `max_open_files=-1` (`OPTIONS-107792`), so every table file stays open. `crates/sparrow-core/src/storage/table_options.rs` `database()` does not bound it.
- At 32 GB apparent there are 917 `.sst` and 9 `.blob` files. Together with sockets, intake files and logs, that passes 1024.

## Interim (operator action, done)

At 17:51:15 CST, `prlimit --pid 3967348 --nofile=65536:1048576` raised the soft limit from 1024 to 65536, without a restart. It holds until the next restart.

## Fix

1. Bound `max_open_files` in `table_options::database()` below the process limit, with headroom for sockets, intake files and logs. Derive it from `RLIMIT_NOFILE` at open and refuse to start when the soft limit is too small for the configured floor, rather than letting EMFILE crash a running store.
2. The store's startup line reports both numbers (the soft nofile limit and `max_open_files`).
3. The unit live runs gets `LimitNOFILE=65536`. The fix must reach the deployed user unit, not only `deploy/systemd/sparrow.service`. Either the deploy tree carries it, or the user-unit path writes it as a drop-in.
4. Patrick, 2026-11-08 10:36 CST (verbatim): "When we come back I want us to add fsacl permissions to all systemd processes as well." Add filesystem ACLs on the units' runtime, state and socket paths for the processes that read or watch them. This goes in the same PR.
5. Explain the 25-minute reopen (investigation in progress) and record it here.

## Acceptance

- A store whose table count exceeds the old soft limit opens and runs under the unit's limit, and the daemon never reaches EMFILE from table files.
- A soft limit below the required floor refuses at startup with an actionable message naming `LimitNOFILE`.
- The startup line shows `nofile=` and `max_open_files=`.
- The installed user unit (or its drop-in) carries `LimitNOFILE=65536`, and a restart picks it up within coordination's window.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
