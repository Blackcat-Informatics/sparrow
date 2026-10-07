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
