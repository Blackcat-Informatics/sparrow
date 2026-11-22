# Issue #449: Fatal I/O error on a WAL write ends the daemon: design an in-process store rebuild or a named alternative

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Problem
A non-no-space I/O error on a write-ahead-log write (EIO and similar) leaves RocksDB's handle stopped; it classes that error as fatal and documents reopening as the only recovery (`db/error_handler.cc`, kWriteCallback IOError fatal; no auto-recovery). Sparrow ends the daemon and relies on its supervisor to reopen the store. Under the rule that only a restart must not clear a failure state, this is a remaining row. Raised from #444 and #447 (comment 22152). Ranked after #445 to #448.

## What to determine
- What an in-process store rebuild takes: `Writer` state keyed to sequence numbers (eviction and expiry cursors, archive state, pressure state, durability ledger), open read leases and iterators that hold the old handle, and the directory LOCK that stays held until every handle is dropped.
- What clients meet during an in-process rebuild against what they meet across the exit today (reads refused or served, the write refusals and how long, held batches, the start waiting for space).
- Whether a real device error on the store's disk leaves anything sensible to do but stop: a failing disk may fail the reopen too. If not, say what the daemon should do (refuse writes retryably, keep serving reads, name the state) rather than exit.

## Acceptance
- A written design with the three answers above, from the code.
- A decision: rebuild in process, or a bounded and named alternative, with its client-visible behaviour.
- If built: tests that fail on main and a fault-injection harness for the fatal class.


## Comments (6)

### paudley — 2026-11-21T16:42:44Z

## Finding: a flush at the instant space returns wedges the database's recovery

Harness: an unprivileged tmpfs (`unshare -rm`, 160 MiB), filled until a RocksDB WAL append fails with ENOSPC, then the filler removed. Sparrow durability latches with a retryable STORAGE_PRESSURE. RocksDB 11.8.1 through rust-rocksdb 0.25.0.

| Arm | Runs | Store calls after space returned | RocksDB resumed | Latch cleared |
|---|---|---|---|---|
| Proofs from 0 s, every 0.5 s (a proof flushes every column family) | 3 (the original run and A0-1, A0-2) | from the instant of freeing | never, 120 s | never |
| Proofs from 1 s, every 1 s | 2 (A1, A2) | from 1 s | yes | yes, 10 s and 6 s |
| Silent, one proof at 120 s | 2 (B1, B2) | none until the end | yes, 9 s and 8 s | yes |
| The shipped maintenance worker (15 s grace, proofs 30 s then 60 s apart) | 1 clean run | none for 30 s while space was low | yes | yes, 16 s after space returned |

In the wedged runs the RocksDB LOG shows the recovery logging "Cannot clear hard error" once at 0 bytes free, then nothing until shutdown ("Failed to resume DB [Shutdown in progress]"), with 240 manual flushes in between. The recovery's resume path waits for background work, and flush jobs keep being re-armed. That mechanism is read from the source (`.stage/449/recovery-source.md`), not shown by running. The exact trigger is not narrowed: one flush at the instant space returns versus repeated flushes was not run.

Logs: `.stage/447/enospc-a0-summary.txt`, `enospc-pair-A0-{1,2}.log`, `LOG-enospc-pair-A0-{1,2}`, `enospc-final5.log` and `LOG` (the original run), `enospc-pair-summary.txt`, `enospc-pair-{A1,A2,B1,B2}.log`, `enospc-noflush.log`, `enospc-worker.log`.

Consequences, read from the code:
- The shipped worker avoids the instant only by schedule: `Proofs::step` (maintenance.rs:447) waits until space has been adequate for 15 s (`Pace::DAEMON`, :390), then proofs are 30 s and 60 s apart. RocksDB's poll fires within 5 s of the crossing, and the resume took under 1 s in the silent runs. A resume that took longer than 15 s is not covered.
- Every Sparrow write is refused at the latch before reaching RocksDB (`Writer::apply`, durability.rs:208), and `Store::flush` and `flush_through` call `synchronize()` first (storage.rs:3034, durability.rs:336). The pressure flush runs only after a retirement commit. Forced compaction (`compact`, forced_compaction.rs:338) has no latch check; it does not flush memtables. RocksDB's own background work is outside our control. Each caller's handling of the refusal was not traced.
- A wedged recovery ends in exit: the latch stands for 120 s after space is adequate, `maintenance::run` returns an error, and the daemon exits with status 1; systemd restarts it and the start waits for space.
- Therefore the in-process reopen (#449) is required before the switch image.

Not tested: an I/O error such as EIO, an explicit `Resume()` (the Rust binding has none), a real (non-tmpfs) filesystem near full. An ext4 image cannot be mounted unprivileged here.


### paudley — 2026-11-21T16:46:23Z

## Addendum: a single proof at 0 s does not wedge the recovery

One run, same harness: one proof at the instant the filler was removed, then silence. RocksDB resumed after 8 s and the final proof cleared the latch (`.stage/447/enospc-pair-S0.log`). So one flush at the instant space returns is not enough to wedge it. The wedge appeared only in runs with repeated proofs from 0 s (every 0.5 s, 240 flushes in the first run's LOG); proofs from 1 s at 1 s cadence did not wedge it. The trigger is therefore narrower than "a flush at the instant": repeated flushes beginning at the instant space returns fit all runs, and the exact threshold (count or interval) is not isolated. The single proof at 0.5 s was not run.

The conclusions above stand with that wording: the shipped worker avoids the window by its 15 s grace and its spacing, and a wedged recovery ends in exit after 120 s, so the in-process reopen (#449) is required before the switch image. This addendum replaces "a flush at the instant space returns wedges the recovery" in the earlier comment.


### paudley — 2026-11-22T06:39:28Z

The maintenance worker still returns an error when a durability proof has remained unsuccessful for 120 seconds after free space became adequate. Two A0 native logs at `.stage/447/LOG-enospc-pair-A0-1` and `.stage/447/LOG-enospc-pair-A0-2` show wedged handles reaching `Shutdown complete` within about 6 ms of cancelling the native resume. They establish close under the exercised ENOSPC fault. [Fault evidence](http://lillith:7555/fleet/sparrow/issues/449#issuecomment-22391)

A store generation must own all state derived from that open. Reuse the store-opening and worker-start paths to rebuild the writer and durability session, watch detectors and pending journals, maintenance, RPC capabilities and read leases. Keep the process-owned Unix and TCP listeners bound. The standard ingest socket matters for new SDK processes because a missing socket selects standalone mode for that process.

- Return a typed reopening outcome from maintenance. Refuse or disconnect new RPCs so clients retry, stop admitting old work, finish or cancel admitted diagnostic and normal work, and expire old capabilities. The current operation drain excludes diagnostic permits. Idle RPC connections hold store clones.
- Close with a single tracked owner after native listing builds, snapshots and compaction release their handles. Keep the archive's detached network writes independent of store ownership, and fence stale archive commits by generation. Publish a named recovery state and retry a repairable open failure with bounded backoff.
- A failed WAL sync can leave accepted buffered batches only in native memory. The existing API already requires stable retries or inspection across a new session. Preserve durable acknowledgements, identify any uncertain buffered batches that replay failed to recover, and reject old-session fences as stale. Confirm any policy that would deliberately discard an accepted batch beyond that existing contract.
- Route durability proofs through one paced owner. A poisoned writer now attempts a native proof from each caller that requests revalidation, including durability status. An error from that status path can stop maintenance before its proof cadence. Passive observation with gated recovery is an independently deliverable prerequisite.

Qualify the next-generation path with the existing scratch ENOSPC harness. Record the same PID and endpoints, new durability session, byte-exact readback of confirmed writes, stale old fences, exact retry outcomes and recovery with an active read. Also name the outcomes of a native close still in progress, an unavailable archive and a failed reopen. Fatal EIO has separate fault-injection acceptance. No same-process reopen ran in this assessment.


### paudley — 2026-11-22T06:59:15Z

#485 owns the poisoned-writer proof schedule. Normal callers and status currently reach the underlying whole-store proof through writer revalidation. Moving known latch clearance to the paced maintenance owner can ship independently and does not require reopening the store. #449 retains the storage-session rebuild, buffered acceptance recovery, active worker teardown and fatal I/O qualification.


### paudley — 2026-11-22T07:54:43Z

#490 owns the mount boundary. The archive thread currently holds a `Store` while its filesystem call blocks. The watchdog marks an overrun, and commit checks that mark after the call returns. Releasing the store while the call is still blocked requires a file job that owns only segment data. Keep a single job active across session changes and keep prepare and commit under the session.

#487 delivered paced proof ownership under writer poison. This issue retains the full session transition, including native handle drain, buffered acceptance and fatal WAL I/O qualification. [Archive slice](http://lillith:7555/fleet/sparrow/issues/490) · [Proof ownership](http://lillith:7555/fleet/sparrow/pulls/487)


### paudley — 2026-11-22T08:35:13Z

#495 owns the admission drain required before releasing a store session. The ordinary operation budget excludes the separate status budget. A diagnostic native task can still own the old `Store` after its caller disconnects, and the final fence currently waits only on ordinary operations.

The drain must cover both budgets. This issue retains cancellation of old connections, process-owned endpoints, session rebuilding and the durability contract. [Admission drain](http://lillith:7555/fleet/sparrow/issues/495)


## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
