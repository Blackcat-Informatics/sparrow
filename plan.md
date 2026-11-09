# Plan: #145 Measure online migrations with live intake and query traffic running

Worktree `.worktrees/145-measure-online-migrations-with-live`, branch
`paudley/145-measure-online-migrations-with-live`, base `main` 5813c33.

## User rulings

- 2026-11-09 (lillith_coordination, verbatim): "(a). Port --with-traffic to online backup sets. Under P28 and D146 a closed-store backup is forbidden, so the closed-store pre-upgrade target is obsolete as a measurement target; D118's target becomes an online set: for the measurement before the D147 switch, a set taken by the new online path from a test store with live intake (the measurement's own fixture), and after the switch the first real online set from the NAS. State the new target in #145 and #162's body. The agent resumes on that, resyncs onto 3735c1e, hook, review, PR."
  Consequence: this supersedes the D118 copy ruling's target below; the traffic mode reads
  BACKUP_ROOT/sets/ID and restores with `sparrowd --restore-backup`, with no closed-store path.
- 2026-11-08 10:36 CST (Patrick, relayed by lillith_coordination, verbatim): "When we come back I want us to add fsacl permissions to all systemd processes as well." (Handled in the R82 unit PR, not in #145.)
- 2026-11-08 ~17:50 (lillith_coordination): "go, with R82 first"; then #145 continues as proposed. The 70 percent rule is coordination's D118 decision, recorded in `.stage/STATUS.md` (main checkout), not a Patrick ruling.
- 2026-11-08 10:33 CST (Patrick, relayed by lillith_coordination, verbatim): "Have all the agents update their docs and state - we are about to roll the claude code binaries.  All agents are to find a safe spot to stop working.  Including you."

- Patrick, 2026-11-07: "nothing is optional, deferred or a follow-up on your own
  authority; requests to cut count as failure and evasion; every claim of done carries
  the number that proves it"
- 2026-11-08 07:5x CST (lillith_coordination, D118 copy ruling): "Coordination, D118: (b).
  Measure #145 on the pre-upgrade-sparrowd-schema68-2aa56a9 backup (closed, schema 68,
  20 GB apparent against live's 23 GB; size is what failed the last gate and this is
  within 15 percent of it) with the record count read and recorded beside live's
  860,209. No downtime now. (a) stays reserved: if (b)'s numbers sit within the gate's
  margin, say so and I set a 3-minute window for the stop, reflink copy and restart under
  your procedure. (c) is refused as the size that misled the last gate. Your sweep is at
  zero, so the #145 code in worktree 145 proceeds: review, hook, PR, ghprsq, then the run;
  send the PR number and the gate numbers with the command."
  Consequences: the target is `/SPARROW/backups/pre-upgrade-sparrowd-schema68-2aa56a9`,
  whose SOURCE has no `live_records`, so the mode reads the copy's record count and
  prints it; the newer image comes in through `--image`; every gate line prints its
  margin. The orchestrator runs the real measurement after merge.
- Orchestrator, 2026-11-08 (review 1 BLOCKED): "Fix everything in the worktree. Per
  Patrick's ruling nothing is optional, so O1-O7 are in scope too." R1 (empty or partial
  replay must fail, per-source counts, empty-replay test), R2 (backup_live_records and
  replay_added, exact or honestly pending), R3 (new end-to-end tests pass in the gate
  environment without a loaded sparrow.service or user manager, never by skipping), O1 to
  O7 as listed in reviews/review-1.md. Same constraints; report a "Review 1 rework"
  section in implementation.md, the filtered rpc set run at least twice, and the O7 test
  five times inside full rpc binary runs.
- Orchestrator, 2026-11-08 (review 2 BLOCKED): "Fix all of these; nothing is optional."
  R1b (completed claims must keep up with the watch items released before the end less
  ship interval + quiet + the 45 s publication deadline, derived from the copy's real
  config; a gate with a margin; shipper hold-backs because a root was full and refused
  claims count as failures; correct the replay_failures sentence; a breach case in the gate
  test), R3b (a df double in measure_env; production's disk floor unchanged and not
  lowerable), O8 (the backup count from the query-only prep, else pending), O9 (device and
  inode as well as the canonical path), O10 (nice-limit note in the test comment), O11
  (plan Behavior item 1). The `.github/workflows/ci.yaml` change was reverted by the
  orchestrator: "Don't touch .github again." Same constraints; a "Review 2 rework" section
  in implementation.md, the full rpc binary twice and the filtered set once.
- Orchestrator brief: no commit, no push, no `make check`; never touch the live service,
  store, sockets, intake roots or backup contents; start sparrowd only through
  `deploy/verify-backup.sh`; one cargo job at a time.

## Behavior

1. `deploy/verify-backup.sh --measure-migration --with-traffic --image DAEMON BACKUP`
   opens a writable reflinked copy with `sparrowd --private-intake DIR` under the same
   user scope, Nice and limits as `--measure-migration` (kept), and runs, from the first
   status answer until every migration is complete:
   - replay: every artifact the copy itself received in the window (the hour before the
     backup's `taken_utc`, `SPARROW_MEASURE_REPLAY_SECONDS` may widen it to a day), read
     once before the run from a separate query-only prep copy's catalog and released at
     its recorded receipt offset, looped (each cycle a new name): an artifact of a watch's
     source into that watch's private root, through the repository's own
     `deploy/libexec/sparrow-ship` (a scratch `settled` source per `ship.toml` source,
     live budgets, every 300 s after each completed run as `sparrow-ship.timer` does) when
     `ship.toml` names a source after the root, and placed directly otherwise; any other
     artifact re-sent over the copy's own ingest socket;
   - traffic: `status`, `records` (the last hour) and `sparql` (scoped to the last hour)
     every 30 s each, timed;
   - MEASURE lines: pages per second overall and per migration (from the daemon's
     `sparrowd: migration` page counters), latency p50/p95/max, call count, failures
     and calls over 60 s for status, records, sparql and ingest, publication deadline
     expiries (daemon log), first status seconds, the copy's `live_records`, and one
     `gate` line per threshold with value, threshold, margin and verdict;
   - exit 0 when every gate passes, 4 when the run completed and a gate failed, 1 for the
     existing FAIL checks.
2. Gates (docs/deployment.md and the script): first status within 600 s; overall
   migration pages per second at least 0.125; no RPC call of 60 s or more; no failed RPC
   call; no publication deadline expiry; recorded watch input not exhausted is not a gate
   (it loops).
3. `--rehearse-rollback` (with `--with-traffic`, after the measurement, on the copy
   only) builds both rollback findings on scratch copies and reconciles them with the
   shared tool `deploy/reconcile-inflight.sh`:
   - finding 1: an entry in a root's `.sparrow-inflight/` claimed after the "backup" is
     moved back to its root under its original batch name (same inode); the restored
     daemon first refuses to start with "unrecognized private claim entry" and starts
     after reconciliation;
   - finding 2: a claim the restored catalog holds, published by the newer daemon after
     the backup, has its bytes re-shipped from the newer store as `NAME.reshipped.EXT`
     and is retired with `watch retire-claim`.
4. `deploy/user-unit-upgrade.sh --help` (extend the #147 bullet) and docs/deployment.md
   give that rollback procedure using `deploy/reconcile-inflight.sh`.
5. `sparrowd --config FILE --private-intake DIR`: a writable daemon with full intake for a
   disposable copy; every watch definition it opens or accepts is rebound to
   `DIR/<basename of its root>`; it refuses `consume`, `syslog`, `rpc_tcp_address` and
   `archive` (the ingest socket is allowed); pending claims bound to another root
   identity no longer refuse startup in this mode and are never recovered here (scope
   check refuses each, reported as the watch failure). Startup line ends
   `query_only=false intake=private`.

## Dependencies

- #147 (schema 70, retire-claim, writer-first pages) in base.
- `deploy/libexec/sparrow-ship` reused unchanged as the watch replay shipper.
- The measured image must carry `--private-intake`, so it is an image built from main
  after this change; older images are refused by usage line.

## Exclusions

- No run on a real backup (orchestrator runs it after merge).
- Live paths are never read or written. The replay reads the copy's own catalog, never a
  producer directory or live intake root (Decisions, "Review 1, replay selection").

## Acceptance

- rpc tests: gate fails per threshold (first status, pages floor, RPC over 60 s, deadline
  expiry), no live path touched (live roots and producer dirs unchanged, every watch
  root reported under scratch), rehearsal moves the unknown entry back and retires the
  published-after-backup claim; existing verify-backup tests pass.
- shellcheck -x, bash -n, prose-check, cargo fmt, clippy release -D warnings, focused tests.

## Risk

- Rust change to watch opening: limited to the new mode; default and other modes keep
  the fatal root-replaced refusal.
- Pages/s timing granularity is the 15 s poll; documented.

## Decisions

- Mount namespaces were rejected: unprivileged `mount --bind` is refused on this host
  (tested: "must be superuser"), and a bind mount would not reproduce a pending claim's
  root identity anyway. Pending claims bind root device and inode
  (`consumption.rs` from_root), so a copy needs a daemon seam: `--private-intake`.
- Private root = DIR/basename(stored root), so the shipper's `intake_root/NAME` layout
  feeds the same roots unchanged.
- Replay window: the hour before `taken_utc` (deterministic, present in the copy and in
  the producer spool), looped for the whole backfill.
- Socket replay set: direct producer artifacts (not from a watch source); the copy has
  only an ingest socket, so TCP-ingested producer artifacts are re-sent over it too.
- Pages floor 0.125 pages/s (8 s per page): rejects the failed live run (over 16 s per
  page, 0.0625), is about 38 percent of the no-traffic copy rate (2,614 pages in
  7,914 s, 0.33) leaving the writer-first share to traffic, and finishes a
  2,614-page backfill in under 5.8 hours, inside one night before the 04:00 backup.
- Rehearsal over-limit trick: a scratch `max_records` below a file's record count keeps
  its publication refused, so its claim stays pending and in `.sparrow-inflight/`
  deterministically; raising `max_records` lets the "newer" daemon publish it.
- `--private-intake DIR` refuses `consume`, `syslog`, `rpc_tcp_address` and `archive`,
  allows the copy's own ingest socket, and excludes query-only. Claims bound to another
  root identity never refuse the open in this mode and are never recovered.
- Review 1, replay selection: the replay reads what the copy received in the window from
  its own catalog, not producer files by modification time. That covers `daily` files
  (mtime after the window, newest held back) and `sealed` segments (manifest-gated) as live
  received them, and reads no producer directory. Watch items go through a scratch
  `settled` shipper source per `ship.toml` source (live budgets and splitting), or straight
  into the root for a watch with no shipper source; other artifacts go over the socket.
  A shipper source with no watch root, or with nothing in the window, fails before the
  measured open; `SPARROW_MEASURE_REPLAY_SECONDS` may widen the window to a day.
- Review 1, backup count: read by the backup's own image query-only on the prep copy before
  any newer image opens it (exact, nothing migrates or expires); else the measured first
  status if exact (the load starts after it); else `pending`. `replay_added` pages the
  records received since the load started (none of the backup's were received that late).
- Review 1, CI: the measurement tests run `systemctl` and `systemd-run` test doubles on
  PATH; the script is unchanged and still checks every value. The CI workflow gains a
  loop-mounted btrfs `target/tmp` and `b3sum`, which every verify-backup test (also the
  pre-existing ones) needs; not run on GitHub here.
- Review 1, O2: the daemon refuses a private-intake DIR that is, holds or lies under a
  configured root, which needs the catalog to keep configured roots, so persistence writes
  them back; `reconcile-inflight.sh --private-intake DIR` replaces `--only-under`, mapping
  query-only status roots to the private roots.
- Review 1, O3: a reused file identity under a private root is refused by the store's own
  one-active-generation rule; the filter keeps the foreign generation from being adopted or
  given a gap.
- Review 1, O7: the test's quiet interval is the 600 s maximum, 20 times its 30 s waits, and
  it waits on the replacement being a held candidate.
- The daemon starts through `chrt --other 0`: this agent session runs under SCHED_IDLE,
  which a user scope inherits (observed `FAIL scope` and class IDL). The unit's
  `CPUSchedulingPolicy` must be 0 and the scope check requires `Policy=TS`, matching the
  live PID's `TS` class.
- Review 2, R1b: per watch, completed claims since the first status (pending journals
  from the backup cannot complete, since their sources are not in the empty private root)
  must reach the watch items released before the final status less the in-flight
  allowance: the watch's `quiet_ms` from status, plus the daemon's publication deadline
  (45 s, a compiled constant, watches.rs `PUBLICATION_DEADLINE`, not in any config), plus
  for shipped items the run's ship interval and the longest observed shipper run. Tail
  watches count too: a tail claim also increments completed_claims. The allowance may
  only be lowered (`SPARROW_MEASURE_GATE_INFLIGHT_SECONDS`), which the breach test uses
  with a 600 s quiet watch. Distinct items left in the shipper queue after any run (a root
  over budget) join `replay_failures`; `reservation refused` daemon lines make a
  `watch_claims_refused` gate.
- Review 2, R3b: `measure_env` adds a `df` double (default 60 GB available, overridable
  by the double's own variable); production's floors are unchanged.
- Review 2, O8: backup count from the backup's image, else the measured image's query-only
  prep status when exact, else pending; the writable copy's first status is no longer used.
- Review 2, O9: `rebind` also refuses when the configured root (existing) shares device and
  inode with DIR, an ancestor of DIR, or DIR/NAME, or DIR does with an ancestor of the root.

## Stop point, 2026-11-08 10:3x CST (Claude Code binary roll)

- #145: branch paudley/145-measure-online-migrations-with-live, head 5813c33 (the same as main). Nothing is committed or pushed, and there is no PR. 14 files are uncommitted: 12 modified (config.rs, consumption.rs, main.rs, tailing.rs, watch_admin.rs, watches.rs, watches/tests.rs, tests/rpc.rs, user-unit-upgrade.sh, verify-backup.sh, deployment.md, interfaces.md) and 2 new (deploy/libexec/sparrow-measure-load, deploy/reconcile-inflight.sh).
- Review 1 BLOCKED: R1 (an empty replay passed), R2 (the record count was mixed with the replay), R3 (tests needed a user manager), plus O1 to O7. All were fixed in the review 1 rework; the full rpc binary passed 89 of 89 five times.
- Review 2 BLOCKED: R1b (watch intake was gated only by at least 1 completed claim; it must gate claims against released items less the in-flight allowance), R3b (the new end-to-end tests need 50 GB free; add a df double), plus O8 to O11. The implementer was mid-rework at the stop; its state is in implementation.md, "Review 2 rework".
- The orchestrator reverted the implementer's change to .github/workflows/ci.yaml (CLAUDE.md forbids build-system changes). Pre-existing: the verify-backup tests need btrfs and b3sum, which GitHub's ubuntu-latest CI lacks. Reported, not fixed here.
- Resume: finish the review 2 rework, review round 3, the hook commit, the PR (Refs #145) and ghprsq. Then build the image from merged main and run the measurement.
- D118 as amended: the measurement runs on /SPARROW/backups/pre-upgrade-sparrowd-schema68-2aa56a9 with --image (schema 70 from merged main). It prints the backup's own record count beside live's (860,209 at 07:11; 1,058,852 at 09:11) and each threshold's margin. No restart of live until coordination's window. Option (a), the 3-minute live stop and copy, is reserved if the numbers sit within the gate's margin. Coordination's stop message also names a "70 percent rule". Its exact text is not in this session's record and must be retrieved from coordination before it is applied.
- R77: no backup since 04:34. The next nightly fails Mon 2026-11-09 04:11 CST unless the retire command is deployed (the schema 70 restart) and import:252d1ffe… is retired first.
- Live: sparrow.service PID 510914, schema 68, NRestarts 0, untouched.
