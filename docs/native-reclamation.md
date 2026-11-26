<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Native reclamation

Sparrow must demonstrate physical recovery after logical retirement, preserve
pinned/shared bytes, expose native failures and admit the refused write again.
A new bounded file-compaction API is not a prerequisite, and this contract requires no new
upstream RocksDB work.

## Existing implementation and native controls

The selected `rocksdb 0.25.0` / `librocksdb-sys 0.19.0+11.8.1` already supplies:

- Error-returning asynchronous column-family flush scheduling. The pressure
  worker uses it after a durable, bounded retirement pass.
- Native automatic compaction and blob garbage collection. The store already
  enables blob GC and limits background jobs to two, with 8 MiB write buffers.
- `Options::add_compact_on_deletion_collector_factory`, configured through one
  helper for every column family: a 1,024-entry window, 128-deletion trigger and
  0.25 whole-table deletion ratio. This marks tombstone-heavy tables for native
  compaction even when level-size triggers are quiet.
- `Options::set_blob_gc_force_threshold`, which can schedule targeted
  compactions when eligible old blob files contain enough garbage. Its native
  default is 1.0; merely enabling blob GC does not enable that trigger.
- `DB::wait_for_compact` with a finite timeout and native error return. This is
  an available observation primitive, not evidence that disk capacity recovered.
- Background-error, pending/running compaction and snapshot observations used by
  the existing pressure owner, plus independent filesystem-size measurement.

The production path must use these owners rather than invent a second deletion
engine. Scheduling or finishing a native job is insufficient: pressure remains
until the measured footprint allows the outstanding write estimate. Failed or
stalled native recovery must not authorize endlessly deleting more evidence.
Status and pin-release controls remain usable during backpressure.

`compact_range_cf_opt` has a unit return and its C wrapper discards native
status. A caller must not relabel that return as success. This limitation alone
does not establish that Sparrow needs selected-file compaction, paginated file
metadata or a new binding. No alternative binding has been selected.

## Forced compaction ends a stalled wait

Native controls alone cannot end one state: a retirement whose flush is no
smaller than what it freed queues no native compaction, so linked bytes never fall
and the wait never ends. The pressure worker forces one range compaction of each
family per wait, once nothing native is pending and linked bytes have not fallen. It
runs on its own thread, outside the writer lock. The binding discards the native
status, so only the measured linked bytes judge it; if they still have not fallen the
wait is released and the next pass retires more. The pass needs the maintenance reserve
free on the filesystem and otherwise does nothing that writes; its retirement and wait-clear commits are admitted against that reserve, not the free-space floor, which ordinary writes keep. The interface
document has the status fields and the states that cannot end in process.

## A floor refusal requests reclamation

A write refused at the free-space floor requests a pressure pass once per episode, and
the pass measures the floor itself. Without that request a store inside its budget would
return at once and the status would read `ready` while writes were refused.

The deficit is a free-space quantity and stays out of `requested_bytes`, which is compared
with the disk budget: put there it would make `physical + requested <= limit` true on any
under-budget store, so the pass would declare itself ready and retire nothing, and a stored
marker would restore it as a budget request at the next open. The pass keeps its own floor
fields instead, and the two branches that would declare the pass ready or stalled on
reservations are guarded while the floor is the reason.

Retirement for the floor is the budget path's own: oldest first, archived and unpinned
entries only, stopping at the first unarchived entry. It applies only where an archive
policy exists, and is bounded per episode by the first measured deficit plus the
maintenance reserve, and by 8 retirement batches, because freed bytes lag retirement
until native compaction runs. The judgment that the floor alone is the reason uses linked
bytes, as the reservation stall does, so query reservations never skip these limits. A store
with no archive policy retires nothing early, and its status
says so. The durable wait marker of a floor-only retirement carries the fixed 65536 bytes a
reopened over-allowance store already uses, never the deficit, so a reopen restores a wait
whose first pass clears it through the ready branch. When the measured deficit reaches zero
the episode ends and the ordinary ready path removes the marker.

Ageing out is the first avenue of a floor episode and runs below the floor.
A floor refusal wakes it with the pass. It runs through `Store::expire_below_floor`: the
same bounded pass as ordinary expiry, but its commits are admitted against the maintenance
reserve like the pass's own, it does not run import reclamation, and when free space is
under the reserve it is refused before it reads anything. That refusal is not ageing out's
turn: the episode does not count it, and ageing out is asked again by the floor's resume or
by the pressure pass when it observes that free space has reached the reserve.
A recovery that stops short of the floor can still wake expiry. The pressure
pass wakes it again if free space falls below the reserve and recovers. It never deletes unarchived or
pinned evidence (the archive gate and the pins are the same code as the pass's). It has no
per-episode cap, unlike early retirement: early retirement of archived evidence is a policy
choice that must be bounded, while evidence past retention is not, and a cap would leave
aged-out data on disk and writes refused while an honest avenue remains. The reserve
headroom the pass's own flush and compaction need is protected by the batch size (the
ordinary 32 units) and by the reserve admission of every batch, which refuses when free
space is under the reserve, so no further cap is needed. A backlog continues by the
duty's continuation flag, an event, until nothing is due or the reserve refuses; a refusal
parks it until the pass finds that free space has reached the reserve.
Retired evidence returns its bytes only after native work, so when ageing out retires
evidence for an episode that asked for reclamation it opens an in-memory wait, with no
stored marker. Nothing was stored, so ending such a wait needs no commit; a commit would be
one more write admitted against the reserve, which could be refused for no reason the wait
has. The pass measures it as it measures its own, forcing one compaction when linked bytes
do not fall. The pass retires archived evidence early only after ageing out has had its
turn, so retention that was only delayed goes before the policy-sensitive avenue. On a store with no archive policy the
pass still measures and ends ageing out's wait, then says nothing is retired early. The
lease purge writes nothing and also runs below the floor: the pass stalls on open read
snapshots, which only the purge ends, and it wakes the pass. Import reclamation and online
migration stay skipped below the floor.

With no archive policy and nothing expired, a store at the floor has no avenue left and
refuses writes until space returns. `floorPause.writesRefused`, the pressure status
`floorRefusedWrites` and the `floor*` fields show it. A client that does not retry loses
what it was refused, and the store cannot count that loss; it counts only the refusals.

## A failed native compaction does not latch the pressure state

The binding's compaction call is `void` all the way down: the C function
`rocksdb_compact_range_cf_opt` drops the status RocksDB returns, and no other binding
entry point returns it. A failed compaction shows only as a rise in
`rocksdb.background-errors`, which RocksDB raises for a compaction error through its
error handler. That count is cumulative since open and cannot be cleared in place. Treating
any nonzero count as fatal would fail every later pass after one failed compaction on a
filling disk, keep the wait marker, and leave intake refused until a restart. The worker
therefore keeps a baseline of the summed count, taken at open, and fails a pass
only on an error new since the last pass, as the table rewrite already does. That pass
reports the error once (`failure`, a journal line, the `nativeErrors` counter), ends the
wait in memory and on disk, and the next pass retires what the archive gate allows. The
forced thread samples the count around its compaction, so a failed one is told apart from
a no-op and ends the wait the same way (`forcedFailures`); a panic of the thread does
too. A store too damaged to take even the small write that ends the wait fails the pass with
a plain message ("the store cannot take a write, so the pressure wait could not be
ended"); the error baseline then stays put, so the pass reports it again for as long as it
lasts. A new RocksDB error is journaled at once, with its reason, not at the bounded rate.
Policy: intake and its recovery come before an old error. The error stays visible in
the counters; nothing not yet archived is removed to make room, because the archive
gate still decides what retirement may take.

Any ended wait now deletes its persisted marker, so a release that retires nothing no
longer leaves a marker that restores a wait nobody holds at the next open. A pass that
retires evidence stages a new marker with its own batch. A crash between the two
reopens above the data allowance, which requests maintenance by itself.

The forced compaction times itself and reads each family's table and blob file bytes
(`rocksdb.total-sst-files-size` plus `rocksdb.total-blob-file-size`, the two the linked
footprint counts) before and after. `pressureStatus` carries
`forcedCompactionMs` and `forcedCompactionFamilies`, so the first real run on a store
measures its own duration and per-family effect. These are measurements of that run,
not estimates.

## Resource and failure boundary

Application admission accounts for expected write space and a maintenance
reserve. Native worker count, buffer settings and compaction policy constrain
work; target-file sizing or a preferred compaction byte count is not a hard
total output guarantee. A filesystem/volume quota provides the hard outer disk
ceiling. Quota/native errors remain visible storage failures. This follows the
existing architecture boundary rather than requiring a new total-output API.

## Qualification

The real-pressure regression
`pressure_recovers_real_native_bytes_and_retries_without_ballast_removal`
fills an owned temporary store with incompressible bytes until native files
cause `STORAGE_PRESSURE`. It keeps a pinned payload with an independently
retirable shared occurrence, drives the production pressure worker, measures
file-size recovery, retries the original refused write and checks exact bytes
through reopen. The store reclaims its own obsolete files while the fixture
keeps external ballast in place.

The unchanged production-default baseline retired 32 records and 1,024 payload
references, but remained backpressured at its 45-second fixture deadline. Its
file footprint grew from 63,136,303 to 63,324,660 bytes, with no pending flush,
compaction or snapshot. This demonstrated missing scheduling after retirement,
not a missing safe binding API.

With the shared native deletion collector, the same production-default fixture
passed in 1.71 seconds: file bytes fell from 63,141,099 to 53,583,692 after 32
record retirements, the original refused request was accepted, and exact pinned
and newly accepted bytes survived reopen. No external ballast was removed, no
binding/dependency changed, and the 96 MiB budget, 32 MiB maintenance reserve and
45-second fixture deadline were preserved. This measures the linked native-file
footprint and actual retry admission; it is not a hard-quota or allocated-block
measurement.

This finite test does not establish sustained production turnover, a hard-quota
run, recovery of an already-stalled store created before this policy,
or snapshot-held recovery. These are limits of that fixture's evidence, not
additional acceptance gates for unencountered historical states or every possible
native failure. The sixty-day synthetic workload passes all three pin modes.
[The measured results](turnover-qualification.md#schema-62-retention-and-native-compaction-policy)
include residual physical files after zero logical ownership. Sustained production
capacity has no established target in this deployment;
quota-specific qualification applies when a strict filesystem ceiling is
configured. None implies that a hypothetical upstream API is mandatory.
