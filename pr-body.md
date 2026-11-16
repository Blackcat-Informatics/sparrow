Closes #341.

## Change

An archive segment also closes on graph quads. Selection stops adding records when the segment's quad count would pass `max_segment_quads` (default 3,000,000, configurable under `[archive]`). A record whose own count is over the budget goes alone in its segment. The closing rule is skipped for a rebuild from an intent, which covers the range the intent names. The rule limits what one segment carries, not what is stored.

## Budget

The measured transient is 220 to 390 bytes per quad (#336). 3,000,000 × 390 B = 1.17 GB. Adding the daemon's ~100 MB baseline gives 1.27 GB, about 59% of the 2 GiB MemoryHigh, leaving room for the 64 MiB payload target and allocator slack. It also matches the measured log-shaped peak (2.8M quads, about 1.2 GB).

## How the count is known before encoding

- Exact: twice the manifest rows of each owning graph (metadata, projection, diagnostic, artifact graph the first time the segment meets it), plus the 2 link quads. The manifest row count is read without decoding terms; no stored field is added.
- Upper bound: 4 fallback quads for a record without a known event time (encoding writes `eventTimeFallback` and `eventTimeBasis` in the named and default graphs, and no manifest holds them).
- Not counted: an artifact graph the post-scan loop adds at the boundary (tens of rows).

## Tests

`a_segment_closes_on_its_quads_and_an_oversized_record_goes_alone` (tests/archive_segment.rs): one table on the real `archive_prepare` path with limits scaled to 40 records and 15,000 quads. Log-shaped records close at the record limit; JSON-heavy records close early on quads; an oversized record goes alone.

## Re-run (JSON-heavy: 100 keys, 10 records per artifact, 2,500 records; separate ingest and archive scopes, CPUQuota=100%, MemoryHigh=2G, MemoryMax=4G; NAS output)

| Segment | Records | Quads | Bytes |
|---|---|---|---|
| 1 | 839 | 2,997,482 | 66.6 MB |
| 2 | 411 | 1,469,486 | 31.7 MB |
| 3 | 839 | 2,997,482 | 66.6 MB |
| 4 | 411 | 1,469,522 | 31.7 MB |

Largest segment 2,997,482 quads, against the 3,000,000 budget. Before the fallback count each 840-record segment held 3,001,052. The archive process peaked at 1.25 GB HWM with memory.peak 1.46 GB; memory.events high 0, max 0, oom 0, oom_kill 0. The 2,500-record single segment that reached 2.15 GB in #336 is now split.

## Known limits

- The 2 quads per stored row is measured on two shapes only (log and 100-key JSON). Markdown, reifier and annotation rows could differ; the review asked for the Markdown and concordance cost, which is not measured.
- The GTS writer keeps its 0.6 to 0.7 GB between segments: the allocator holds freed pages, and returning them needs `malloc_trim`, which the workspace's unsafe-code ban rules out without a dependency.

## Evidence

Worktree `.stage/archive-segments-close-on-graph-rows-as/`: `commit.log` (d16966a, EXIT=0), `commit2.log` (3a22d49, EXIT=0), the re-run logs. Review `.stage/archive/review-341.md`: BLOCKED at d16966a on the under-count of event-time fallback quads (fixed at 3a22d49, read by the primary); the memory result and the selection logic were checked and passed.
