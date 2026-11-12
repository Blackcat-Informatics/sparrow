<!--
SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
-->

# Native reclamation

Sparrow must demonstrate physical recovery after logical retirement, preserve
pinned/shared bytes, expose native failures and admit the refused write again.
The earlier proposal for a new bounded file-compaction API is withdrawn as an
unproven prerequisite. No new upstream RocksDB work is required by this contract.

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

## Forced compaction ends a stalled wait (#211)

Native controls alone left one state with no end: a retirement whose flush is no
smaller than what it freed queues no native compaction, so linked bytes never fall
and the wait never ends. The pressure worker now forces one range compaction of each
family per wait, once nothing native is pending and linked bytes have not fallen. It
runs on its own thread, outside the writer lock. The binding discards the native
status, so only the measured linked bytes judge it; if they still have not fallen the
wait is released and the next pass retires more. The pass needs the maintenance reserve
free on the filesystem and otherwise does nothing that writes; its retirement and wait-clear commits are admitted against that reserve, not the free-space floor, which ordinary writes keep. The interface
document has the status fields and the states that cannot end in process.

## A failed native compaction does not latch the pressure state (#218)

The binding's compaction call is `void` all the way down: the C function
`rocksdb_compact_range_cf_opt` drops the status RocksDB returns, and no other binding
entry point returns it. A failed compaction shows only as a rise in
`rocksdb.background-errors`, which RocksDB raises for a compaction error through its
error handler. That count is cumulative since open and cannot be cleared in place. The
pressure worker used to treat any nonzero count as fatal, so one failed compaction on a
filling disk failed every later pass, kept the wait marker, and left intake refused
until a restart.

Now the worker keeps a baseline of the summed count, taken at open, and fails a pass
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
through reopen. It does not remove external ballast to simulate reclamation.
Evidence is retained under
`.stage/native-graph-integration/reclamation-existing-apis-*`.

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
native failure. The existing sixty-day workload now passes all three pin modes
at the installed source; [the measured results](turnover-qualification.md#schema-62-retention-and-native-compaction-policy)
include residual physical files after zero logical ownership. Sustained production
capacity has no established target in this deployment;
quota-specific qualification applies when a strict filesystem ceiling is
configured. None implies that a hypothetical upstream API is mandatory.
