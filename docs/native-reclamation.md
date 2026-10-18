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
  helper for both column families: a 1,024-entry window, 128-deletion trigger and
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
snapshot-held recovery or every native storage-failure scenario. Those need
separate evidence; none implies that a hypothetical upstream API is mandatory.
