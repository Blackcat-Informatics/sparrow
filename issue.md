# Issue #83: Test splitting a single line longer than the raw-record ceiling

State: open   Repo: fleet/sparrow   Forge: forgejo

## Body

## Source

- Row B-54 of `docs/open-items.md` (main 03052cf).
- Artifact-limit review, OPTIONAL finding 4 (carried): `~/.local/state/sparrow-deliver/reviews/artifact-limit-review.md:165-166`. Issue 21's "Out of scope" section.
- Marked by an agent: "carried".

## Current behavior

- `Store::import_prepare_layout` (`crates/sparrow-core/src/storage/import.rs:846-890`) splits each record at the raw-record ceiling (`raw_record_bytes`) using `record_ends` per format.
- The split tests (`split_document`, `crates/sparrow-core/tests/storage.rs:7260`, and `split_raw_document_reads_back_exactly_and_both_expires_and_evicts`, `:7521`) cover a single JSON document. `oversized_jsonl_fixture` (`:7162`) has a 17 MiB line, below the smallest test ceiling of 32 MiB plus one chunk, so it is never split.
- No test splits one line of a line-delimited format (JSONL or text) that is longer than the ceiling and sits between other lines. Grep found none; that the path works is unverified.

## Work

Add a test that lays out a JSONL (or text) artifact with a short line, one line longer than the ceiling, and another short line, under `split_raw_records_above`.

## Done when

`cargo test --release -p sparrow-core --test storage oversized_line_splits` runs a new test that asserts:

- the long line becomes consecutive raw records, each no larger than the ceiling;
- the short lines keep their own records and byte bounds;
- an artifact byte read returns the input byte-exact.

No such test exists today.


## Comments (0)

## Coverage

- forgejo: no blockedBy/subIssues/closedByPullRequests fields; link crawl uses body regex only
