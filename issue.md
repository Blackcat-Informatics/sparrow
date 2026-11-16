# Issue #304: Archive prepare read cost grows with segment size; scope limit of 1000 ids

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

`archive_prepare` (merged in 5421dbe, `crates/sparrow-core/src/storage/archive_encode.rs`) costs more per record as a segment grows. This blocks the archive install. A real segment holds up to 50,000 records, and at this growth it would never finish.

## Measured (head f5585f3 on 5421dbe, daemon expiry test, 19-byte lines, one record each)

Source: `.stage/archive/lane2w/prepare-cost.md` and `probe-*.log` in the main checkout. A temporary probe read the view budget after `dataset_from_view` and has been removed.

| records | requests | row_visits | admitted_bytes | ms | requests/record |
|---:|---:|---:|---:|---:|---:|
| 5 | 14,161 | 3,964 | 1,858,336 | 309 | 2,832 |
| 20 | 147,253 | 46,654 | 18,714,687 | 2,587 | 7,363 |
| 70 | 1,604,903 | 526,954 | 203,343,570 | 22,320 | 22,927 |

That is about n^1.8 for 1,330 bytes of evidence. It passes today only because `ARCHIVE_READ` allows `1 << 32` requests. Against 82eec56 (ceiling 100,000) the 70-record case failed with `RESOURCE_LIMIT requests=100001/100000`.

## Second limit on the same path

`Store::with_rdf_scope_ids` (`rdf_view.rs`) refuses more than 1,000 record ids or 1,000 artifact ids with `INVALID_ARGUMENT`. The segment cap allows 50,000 records, so `archive_prepare` must not pass a larger selection through one call. Found by reading the code; not yet run.

## Acceptance

- The read cost of `archive_prepare` per record is bounded by a constant that does not depend on the segment's record count. Measured at 70, 700 and more than 1,000 records, with the numbers recorded.
- A segment of more than 1,000 records prepares and commits.
- A test protects the per-record bound at a size where the current code would exceed it.
- The segment's bytes and graph are unchanged in meaning: same quads, same blobs and links. The existing archive segment tests and the lane 3 recovery test still pass.
- The read ceilings in `ARCHIVE_READ` come back down to bounds that would have caught this defect.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
