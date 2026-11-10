# Issue 160 plan

## Behavior
1. RocksDB flush/compaction concurrency, memtables and (only if measured necessary) L0 triggers sized
   from a scratch measurement at the live write rate; compaction threads at lower CPU priority than
   intake; no cores taken by default (RocksDB raises compaction parallelism only under backlog).
2. Read-only per-claim listing: RPC `watchClaims`, CLI `watch claims`, from durable claim history
   (first seen, attempts, last attempt, last pending time and reason) kept in each claim journal.

## Dependencies
- Scratch store under /home/paudley/Active/sparrow/.stage/r82/160-scratch (btrfs reflink copies).
- Builds serialized with flock on /home/paudley/Active/sparrow/.stage/cargo.lock.

## Exclusions
- No change to the live store, daemon, sockets, units. Live 24 h acceptance runs after D147.

## Acceptance
- Measurement table (stall percent, hard stops, pending compaction bytes per set) on issue 160.
- Claim listing answers "of N claims that went pending since T: complete / pending (ids, ages) /
  abandoned" in one command; docs/interfaces.md updated.
- Commit through the hook (make check), PR via stagectl pr-create, independent review PASS.

## Risk
- Measurement shares the host with the live daemon and builds: scratch runs at nice 10, ionice
  best-effort 7.

## User rulings
- (coordination brief, 2026-11-09) P28, Patrick verbatim: "no backup may EVER stop lillith, lose
  data, or drop packets.  All backups MUST be online and perturb no operations."
- (coordination brief, 2026-11-09) Patrick verbatim: "DO NOT mess with, interogate, attemp to fix,
  or otherwise probe the build system unless you are explictly asked to. If build errors occur,
  backoff and wait."
- (coordination brief, 2026-11-09) Never touch the live store, live daemon (PID 3967348), its
  sockets, intake roots or units; serialize cargo and git commit with flock; stop before ghprsq.

Coordination rulings relayed by lillith_coordination, 2026-11-09 (verbatim excerpts):

- "choose C unless the table shows a reason for E; state the reason in the PR body either way."
- "Coordination decision for #160: option (a), with (c) as its own issue, now #167 (REQUIRED, "Size the unit's CPUQuota and IOWeight from measurement"). Ship set C now."
- "The PR says `Refs #160`, NOT `Closes`; #160 stays open until the live LOG shows the target after the drain."
- "No one-core tuning run (b) before the PR."
- "That is NINE intervals after the stop's interval, not ten; fix the wording in the docs and PR body to nine (or "every later interval")."
- "open the #160 PR NOW at head 45e0118 with `stagectl pr-create` ... and stop before ghprsq."
