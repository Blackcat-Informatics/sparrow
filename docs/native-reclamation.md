# Native reclamation integration

Forced bounded physical reclamation remains NOT MET. Logical retirement, native
flush scheduling and automatic compaction/blob GC are implemented, but they do
not establish sustained measured disk recovery.

The current lockfile selects `rocksdb 0.25.0` and
`librocksdb-sys 0.19.0+11.8.1`. Inspection of their implementations identified
the following required seams:

- `rocksdb::DB::compact_range_cf_opt` returns `()`. Its C entry point
  `rocksdb_compact_range_cf_opt` in `rocksdb/db/c.cc` discards the native
  `CompactRange` status. A wrapper cannot recover that discarded failure reason
  by pretending the return is a successful `Result`.
- The C API provides `rocksdb_compact_files_cf`, which captures the native
  `CompactFiles` status through `SaveError(errptr, s)`. The safe Rust facade
  currently exposes no corresponding file-selection operation.
- `DB::live_files` materializes metadata for every table file. A bounded worker
  needs admitted/paged metadata rather than an unrestricted catalog allocation.
- The safe `CompactOptions` facade exposes bottommost-level policy, exclusivity,
  level changes and timestamp history. It does not expose the native bounded-file
  compaction output options or `CompactRangeOptions`' explicit blob-GC policy.

A key-range estimate is not an input/output byte bound: selected key ranges can
intersect entire table files and referenced blob files. Merely splitting the
hash key space or using `get_approximate_sizes_cf` would not prove admission
against the maintenance reserve. Automatic native activity, background-error
properties and a returned range-compaction call cannot certify the native
request's successful completion.

The dependency integration task is to expose safe, error-returning bounded file
compaction and bounded metadata capture, including the required blob-GC and
output controls. It must preserve the workspace's `unsafe_code = "forbid"`
policy; Sparrow must not copy private dependency code or introduce an unchecked
FFI path. No dependency or sibling repository was modified for this inspection.

After that seam exists, the production worker must admit selected input, expected
output and control space; keep at most one owned reclamation job; release the
catalog writer before native disk I/O; report exact native failures; and join
its job during shutdown. Logical retirement must remain gated while reclamation
is pending. Native completion still requires independent physical measurement
and successful retry admission before pressure can be declared recovered.
Qualification must include shared/pinned bytes, snapshots, worker shutdown,
native failure, restart, and sustained pressure over the intended corpus.

This gap does not authorize synthetic progress writes, silent retries, premature
reclamation receipts or more evidence deletion while physical recovery stalls.

## Alternative binding inspection

On 2026-10-08, the maintained
[zaidoon1 binding fork](https://docs.rs/crate/rust-rocksdb/0.53.0) was inspected as
an alternative to the selected binding. Release `v0.53.0` names tag object
`c8b2e2c3a6939ea2cd35ff073c10b1e431615d88`; its native package declares
`rust-librocksdb-sys 0.48.1+11.8.1`. This is discovery, not a selected dependency
or qualified store migration.

Its safe `compact_files`/`compact_files_cf` APIs return a typed native result or
error and expose compaction output names/job information. `CompactionOptions`
supplies per-output-file size, subcompaction, trivial-move and cancellation
controls. Those are useful seams, but a per-file output size is not a total job
input/output or blob-space bound. They do not establish pressure recovery.

The release's `ColumnFamilyMetaDataOptions` filters by level and overlapping
start/end keys. It does not supply a maximum admitted file count or metadata byte
budget before collection. `get_column_family_metadata_with_options` returns all
selected levels/files in its owned metadata result. The level and key filters
do not prove bounded metadata capture or bounded compaction closure; overlapping
file/blob input and total output still require admission. Range compaction also
retains the unit-returning API. These observations are from
[release metadata source](https://github.com/zaidoon1/rust-rocksdb/blob/v0.53.0/src/metadata.rs)
and [release compaction source](https://github.com/zaidoon1/rust-rocksdb/blob/v0.53.0/src/compaction.rs).

No dependency, sibling checkout, daemon or live store was changed for this
inspection. A candidate binding still needs the missing bounded collection and
resource contracts, followed by full source/copy-store/failure qualification
before runtime adoption. The original forced-reclamation requirement remains
unmet.

On 2026-10-18 the first-party
[release index](https://github.com/zaidoon1/rust-rocksdb/releases) still identified
v0.53.0 as latest. A fresh inspection of the current
[metadata options](https://raw.githubusercontent.com/zaidoon1/rust-rocksdb/master/src/metadata.rs)
and [compaction options](https://raw.githubusercontent.com/zaidoon1/rust-rocksdb/master/src/compaction.rs)
found the same relevant limits: metadata selection filters levels/key ranges,
and file compaction exposes per-output-file sizing and cancellation. These
inspected types still supply neither admitted metadata pagination nor a total
job output-byte ceiling. This refresh does not establish the missing bounded
reclamation contract or justify changing the selected dependency.
