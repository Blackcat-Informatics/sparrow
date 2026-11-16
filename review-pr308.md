# Review: PR #308 (issue #304, with #305 item 1)

Reviewed 2026-11-15 10:02 CST. Head ff88ab3891201ed4f971bf86e1fcc686e88b32fd (equal to
origin/paudley/304-archive-prepare-read-cost-grows-with at review time) on base
5f6202b174a1a6da27acc593b71e4ac7dc703dd9. I judged the patch. An amend that leaves the patch
identical does not change this verdict.

## Scope

What I read:
- `git diff 5f6202b ff88ab3`: `archive_encode.rs`, `archive_segment.rs`, `rdf_view.rs`, and
  `tests/archive_segment.rs`.
- The requirements: `.stage/archive/followups/prepare-cost-issue.md` and item 1 of
  `worker-notes-issue.md`.
- Head source:
  - `with_rdf_scope_ids` and `with_rdf_record_layers`, plus the `rows`, `physical_rows` and
    `resolve` cost accounting and the `QuadIter` per-row charge (`rdf_view.rs`).
  - The lease check in `rdf_view/budget.rs`.
  - `select` and `archive_prepare` with its halving loop (`archive_segment.rs`).
  - `ArchivePolicy` validation (`archive.rs`) and the `MAX_ROWS` (32,768) graph bound in
    `rdf_index.rs`.
  - The JSON and Markdown projection paths in `ingest.rs` and `projection.rs`.
  - The worker's `drain` and `archive_segment` (`sparrow-server/src/archive.rs` at 5f6202b).
- PurRDF 4.0.2 from the cargo registry:
  - `dataset_from_view` and `reconstruct` (`ir/pack/certify.rs`).
  - `DatasetImporter::try_new` and `try_append` (`ir/import.rs`).
  - Union `assign_scopes` and `rewrite` (`ir/union/mod.rs`).
  - `ScopeBinding` (`ir/composite.rs`).
  - The `RdfDatasetBuilder` set and dedupe contract (`ir/builder.rs`).
- Evidence in `.stage/archive/lane304/`: `prepare-cost-after.md`, `probe-after-{70,700,1500}.log`,
  `focused.log`, `focused-archive.log`, `hook.log` and `hook2.log`.
- `prose-check` on the added doc comments returned exit 0.

What I did not read or run:
- I ran no cargo, tests or hook, as instructed.
- I did not read the GTS writer, so whole-file determinism across processes is inferred, not
  proved.
- I did not read the union's row-deduplication internals beyond the scope assignment.
- I did not read `include_record` or the owner-graph enumeration in depth.
- I did not read `archive_commit` cost at 50,000 records.
- I did not check `sparrow-archive` beyond grepping it for blank or scope handling (none found).

## Evidence

- `focused.log`: `tests/archive_segment.rs`, debug, 6 passed, EXIT=0. This includes the new test.
- `focused-archive.log`: `archived_segment_reimports_byte_exact`, 1 passed and 1 filtered out,
  EXIT=0.
- `hook.log`: the full `make check` sequence, release. All 19 `test result` lines are ok, the
  census holds, and it ends EXIT=0.
- `hook2.log` is a partial later run with no result lines. It is not evidence either way.
- The probes are debug-profile measurements on 19-byte `log` lines:

  | records | requests | requests per record | rows | time |
  |---:|---:|---:|---:|---:|
  | 70 | 18,146 | 259 | 2,268 | 180 ms |
  | 700 | 178,796 | 255 | 22,428 | 1.9 s |
  | 1,500 | 382,796 | 255 | 48,028 | 4.6 s |

  Each probe exited 0.
- Rebase: all of the evidence ran on the pre-rebase tree d3ff08c. Between d3ff08c and 5f6202b,
  only these files changed: `sparrow-server/src/archive.rs`, `config.rs`, `main.rs`,
  `tests/rpc.rs`, `deploy/README.md` and `docs/interfaces.md`. The PR touches only
  `sparrow-core`, so no file overlaps.
- Behaviour across the rebase: the worker's new stall and `io_timeout` flight begins at
  `start_pass` inside `archive_segment`, after `archive_prepare` returns. A longer prepare
  therefore neither trips the stall watchdog nor blocks a commit. The hunks do not interact.
  The evidence still describes the pre-rebase tree, not ff88ab3.

## What holds

- **Segment meaning.** `dataset_from_view` is `DatasetImporter::try_new(...).try_append()` on a
  fresh builder, followed by `freeze`. The new code runs the same import, then pushes the link
  quads into the same builder.
  - De-duplication: the builder dedupes quads on push, because the dataset is a set (C0.5), and
    `freeze` re-sorts deterministically. Identical quads collapse as the union's did.
  - Graph naming: stored graphs import by value, and the link graph IRI and predicates are
    unchanged. Declaration-only named graphs are still replayed.
  - Blank nodes: `Independent` binding made the union renumber the stored view's blank scopes
    densely and rewrite CDT literal labels to match. The new path keeps the stored
    `(label, scope)` pairs, CDT labels included. The link side has no blank nodes. Both
    mappings are injective, so the blank partition, and with it the RDF meaning, is identical.
  - Bytes: the GTS bytes of a segment are not identical to what the old code would have
    written, but meaning was the contract.
- **Query bound.** `with_rdf_scope_ids` is `pub(super)`. Its only callers at head are
  `archive_encode::encode` and `with_rdf_record_layers`, which re-imposes 1..1000 with the
  same code and message. `with_rdf_records` routes through it. `sparrow-server` does not call
  any of the three. Query, MCP and RPC cannot reach the loosened bound.
- **Formula arithmetic.** At 50,000 entries the ceilings are 25.6M requests and 3.2M row
  visits. Overflow would need about 3.6e16 entries, which the 256 MiB segment cap makes
  unreachable. `live_bytes` (2 GiB) and the lease are unchanged. Scope live bytes at 50,000
  ids come to about 100 MiB plus four times the catalog bytes, inside 2 GiB.
- **No-way-out cases for many entries.** A failure on a multi-entry segment (RESOURCE_LIMIT,
  RESULT_EXPIRED) halves the cap and loses nothing.
- **Byte-equal retry.** The new test prepares twice and compares the file bytes. The second
  prepare takes the in-flight intent rebuild path, which is the path the worker takes after a
  crash. The test drives `archive_prepare` and `archive_commit` on a real store. It would fail
  on the old union path, because that path exceeds the new ceilings and halves below 1,100
  records.

## Findings

1. **REQUIRED: the read ceiling counts entries, not the stored graph a segment carries, so a
   legitimate record with a larger content graph can never be archived. The archive then
   sticks behind it with no way out.**

   Mechanism:
   - `archive_read(entries)` allows 10,000 + 512n requests and 2,000 + 64n row visits.
   - The scoped view charges one request and one row visit per quad row read
     (`QuadIter::next`, `budget.request(128, true)`), plus a request per resolved term.
   - Cost therefore scales with the quads in the records' graphs, not with the record count.
   - The allowance was calibrated only on 19-byte `log` lines, at about 32 rows per record.

   Consequences:
   - The store admits content graphs up to `MAX_ROWS` = 32,768 rows (`rdf_index.rs`) from
     `json`, `jsonl` and `markdown` projections.
   - A single record whose graphs exceed about 2,064 rows, or about 10,512 requests, is refused
     even alone. A Markdown document or JSON object of a few thousand triples is ordinary.
   - With `entries == 1` there is no halving, so `archive_prepare` returns
     `archive cannot encode <entry>: RESOURCE_LIMIT ...`. The worker's `drain` fails on every
     pass.
   - The watermark never passes that record, and nothing after it is archived. Because nothing
     unarchived is deleted, the store fills under retention.
   - Before this PR, the 2^32 ceilings admitted such a record.

   A milder form of the same defect: a segment of structured records averaging more than 64
   rows each (for example, `jsonl` objects of about 40 fields or more) exceeds the ceiling at
   full size.
   - Each pass re-reads and halves down to roughly 2,000 / (rows − 64) entries.
   - The pass wastes up to about twice the first ceiling, or the 60 s lease, in refused reads
     before each small segment.
   - That is per-record waste far beyond the defect this issue removes.

   Status: an inference from reading the accounting code. It was not run, and no existing test
   covers a record with a large content graph. The archive tests use `log` lines, and the
   recovery test uses small `jsonl`.

   Needed:
   - A ceiling that bounds cost per stored row as well as per entry. One option is a per-row
     allowance over the rows the scope actually owns, plus the per-entry fixed cost. Another is
     an allowance that at least covers the store's own per-graph bound for each entry's graphs,
     provided it still catches the old growth.
   - A test in which a `json` or `markdown` record with a graph larger than about 2,100 rows
     prepares, and, if cheap, a segment of such records past 1,000 entries.

2. **OPTIONAL: performance measured only at 1,500 records, in debug.**
   - At about 3 ms per record (debug), a 50,000-record segment takes about 150 s. A release
     build may or may not finish inside the 60 s lease.
   - The halving loop is a safe way out, but it would spend a full lease on every pass before
     halving.
   - `with_rdf_scope_ids` also holds the writer lock (`self.lock()`) for the whole capture:
     catalog point read, decode, validate and include, for every id. The capture now grows
     from at most 1,000 ids to up to 50,000, so ingestion pauses for it once per segment.
   - Fix: a release-profile measurement at 50,000 records, covering total time and the
     lock-held capture time.

3. **OPTIONAL: the new test's doc overstates when the old read failed.** It says the old union
   read exceeded the allowance "from about 20 records on". By the issue's own numbers, 5
   records cost 14,161 requests against an allowance of 12,560, so it already failed at 5.

4. **OPTIONAL: cross-process byte equality is not proved.** The byte-equal check runs in one
   process. A fresh builder per prepare and a sorting `freeze` make per-process hash order
   irrelevant, but nothing proves equality across processes. The issue asked for an in-process
   test, which this is.

VERDICT: BLOCKED
