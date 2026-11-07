# Issue 36, step 1: page the schema 69 event-fallback job

Worktree `.worktrees/36-bound-the-schema-69-first-open`, branch
`paudley/36-bound-the-schema-69-first-open`, base `main` 758b41a. Uncommitted; no
commit, no `make check`, live service and stores untouched.

## What changed

### Shared pager: page-scoped publication (`crates/sparrow-core/src/storage/catalog_upgrade.rs`)

- `:24` `Job<P = Publish>` is now generic in its publisher. The default parameter keeps
  every existing `Job` literal (artifact, all-artifact, provenance, tracing, recordless,
  source context, tail ranges, the pager test) unchanged in source and behavior.
- `:38` new trait `Publication`. It has `open` (page state before the first entry),
  `publish`, `full` (whether the page must end; the default is never) and `finish` (writes
  the page state into the batch before the checkpoint).
- `:57` `impl Publication for Publish`: a plain entry function has no page state, `full`
  is always false and `finish` does nothing, so the other jobs page exactly as before.
- `step`: opens the page state at `:189`, ends the page once it holds an entry and `full`
  says so at `:204`, and finishes the page into the batch at `:243`. The checkpoint `put`,
  the admission hook (`artifact_rdf::admit`) and the synchronized `commit` still run once
  per page, after every entry.
- `run`, `seed` and `step` take `&Job<P>`.

### RDF writer and stored graph decoding (`crates/sparrow-core/src/storage/rdf_index.rs`)

- `:443` `RdfWrite::known` returns the identity of a term the batch staged or the
  dictionary holds, without staging it. `:447` `lookup` is the existing staged, cached,
  reverse and forward lookup, moved out of `intern`. `intern` now calls `lookup` and
  stages only on a miss, so there is still one lookup implementation.
- `:812`, `:824` `stored_metadata` takes the caller's `&mut RdfWrite` instead of creating
  one, and `StoredMetadata` no longer carries a writer. Graphs in one page must share one
  writer: `RdfWrite::new` reads `rdf:next`, reference counts and the term count from the
  committed database, so two writers in one uncommitted batch would allocate the same
  term identities and lose reference counts. This is why `artifact_rdf::JOB` documents
  `page_entries: 1`.
- `:949` `date_time` is `pub(super)` so the new test reuses it.
- `provenance_rdf.rs:64` creates its writer and passes it in. It still uses one graph per
  page, so its behavior is unchanged.

### Event-fallback job (`crates/sparrow-core/src/storage/event_fallback_rdf.rs`)

- Bounds (`:16`-`:22`): `PAGE_ENTRIES = 4096` graphs; `PAGE_INPUT = 16 MiB` of stored
  manifest bytes (the pager's `page_bytes`, with the charge now the input length);
  `PAGE_BATCH = 16 MiB` of page batch (the `full` check). A page ends at whichever bound
  comes first. With a 16 MiB page, a following graph of the maximum size (32,768 rows: an
  anticipated 19 MiB of index writes plus about 9 MiB of deletes) still fits under the
  writer's 64 MiB `MAX_BATCH` admission. The admission therefore cannot fail just because
  pages grew.
- Decode allowance: the old per-entry `json_charge(bytes, 32 MiB)` with
  `PAGE_BYTES - charge` passed to `stored_metadata`. This only worked with one entry per
  page, because the page's cumulative charge budget equalled the per-entry allowance.
  Summing 65x JSON charges across a multi-entry page would have capped pages at about
  250 graphs and bounded nothing real, since each graph's decoded terms are dropped
  before the next. Now `publish` applies `json_charge(bytes, GRAPH_BYTES)` to each graph
  and passes `GRAPH_BYTES - charge` as before. What the page keeps is bounded separately
  by `PAGE_BATCH`, `PAGE_INPUT` and `PAGE_ENTRIES`.
- `:61`/`:63` `Fallback` implements `Publication`. The page state is `(db, RdfWrite)`.
  `finish` calls `RdfWrite::finish` once per page, so the dictionary counters, new terms
  and reference counts of all the page's graphs are written in the same batch as the
  checkpoint.
- Skip without decoding (`:105`-`:119`): the job parses the graph's own manifest, which
  the pager has already read, with `manifest_rows`. It resolves `vocab:eventTime` and
  `vocab:eventTimeFallback` with `RdfWrite::known`, which the writer caches after the first
  lookup in a page. A primary row with either predicate triggers one lookup of the record
  subject's identity. The graph is skipped when a row has that subject and one of those
  predicates. No term is decoded and nothing is written. Because the dictionary is a
  verified bijection, this is the same test the decoded loop made. The job still reads
  only the graph, so graphs prepared by an unfinished staged import, which have no
  catalog record, are still covered.
- `:136` the decoded loop keeps a hard failure. If a decoded graph still shows
  `eventTime`/`eventTimeFallback` on its subject, the dictionary and the manifest disagree,
  and the job refuses rather than writing contradictory facts.
- The test fixture `remove_event_fallback_rdf_for_legacy_fixture` passes its own writer
  to `stored_metadata`.

### Documentation

- `docs/interfaces.md` (the schema 69 backfill paragraph, about lines 531-538) now
  describes the page bounds, the one synchronized write per page, resumption after the
  last committed page, and the skip rule.

### Test (`event_fallback_rdf.rs:242`)

`pages_share_one_writer_skip_source_times_and_resume_after_a_failed_page` uses six record
graphs with no catalog record. Two of them carry `eventTime`.
- A three-graph page rebuilds two graphs through one shared writer in one commit, and the
  checkpoint moves to the third graph.
- The next page would rebuild `record:e` and then refuses a malformed `record:f`. Nothing
  from that page is published: `record:e` is byte-identical and the checkpoint has not
  moved.
- After a reopen, `upgrade` resumes and completes. The source-time graphs are
  byte-identical to their original form, and the others each gain exactly the two
  fallback rows.
- Removing every graph afterwards returns `rdf:term-count` to 0. This proves the shared
  writer's reference counts are exact across multi-graph pages.

## Expected pages and commits for 151,000 records

- Count bound: at least ceil(151,000 / 4,096) = 37 pages. The synthetic store measured
  exactly 37.
- Real graphs are larger than the synthetic ones, so `PAGE_BATCH` may end pages sooner.
  A rebuilt graph of about 20 rows writes roughly 15-25 KB of index puts and deletes, so
  a page of rebuilt graphs holds about 700-1,000 of them. Worst case: about 150-220 pages
  for 151,000 records. Graphs that already have a source time add nothing to the batch.
- Synchronized commits: one per page, plus one for the format fence and seed. That is
  about 38 to about 220, against about 151,001 before. `artifact_rdf::admit` walks the
  database directory (`linked_physical_bytes`) when a batch owns RDF metadata, which every
  rebuilt page does. It also now runs once per page instead of once per record.

## Measured local timings

Probe: a temporary ignored unit test, now removed. It built N synthetic record graphs of
6-8 quads each, half with `eventTime`, using `open_test_db` on the real disk (not tmpfs,
so `fsync` counts), and then ran the job. Release profile, unconstrained, on this
workstation.

| Records | Before (one graph per page) | After |
|---|---|---|
| 10,000 | 198.2 s (about 19.8 ms per graph) | 3.0 s |
| 60,000 | not run | 13.1 s (15 pages) |
| 151,000 | not run; about 50 min extrapolated from 10k | 105.4 s; rerun 87.3 s (37 pages) |

The cost per graph rises with N, probably because the test database uses RocksDB's
default options and stalls on compaction (an inference; I did not profile the binary
symbolically). The 76-minute open observed on the nightly copy matches the old cost of
about 20-30 ms per record.

## #13 owner graph build (`rdf_index::upgrade_owner_graphs`)

No change. Each page copies up to 65,536 `rdf:g:` declarations into `rdf:o:` rows (a
batch of about 1.5 MB), then runs one `admit` and one synchronized commit. At about
500,000 declarations that is 8 pages. Per page there is one `linked_physical_bytes`
walk, and only if the batch owns RDF metadata, which an `rdf:o:` batch does not. The
per-page cost does not matter at that scale.

## Checks run (logs in `.stage/bound-the-schema-69-first-open/logs/`)

- `cargo fmt --all --check`: exit 0 (`fmt-check-final.log`).
- `cargo clippy --locked --release -p sparrow-core --all-targets --features test-support -- -D warnings`:
  exit 0 (`clippy-final.log`).
- `cargo test --locked --release -p sparrow-core --lib storage::`: exit 0, 35 passed,
  including the pager, recordless, provenance and owner graph tests and the new test
  (`unit-final.log`).
- `cargo test --locked --release -p sparrow-core --test storage`: exit 0, 220 passed,
  including the #20 integration test
  `tracing_relationships_keep_instance_and_occurrence_evidence_through_backfill_and_retirement`
  (`it-storage-final.log`).
- `cargo test --locked --release -p sparrow-server --test rpc` on the 5 tests that open
  legacy stores (`configured_inotify_...`, `explicit_handoff_...`, `log_timestamp_profiles_...`,
  `typed_tail_handoff_...`, `typed_watch_administration_...`): exit 0, 5 passed
  (`it-rpc.log`). This run predates the final small edit (test helper reuse and docs),
  which does not touch any production path they exercise.
- `prose-check docs/interfaces.md` and the changed sources: the only findings are
  existing `UInt64`/`UInt32` lines elsewhere in `interfaces.md`, none in the edited
  paragraph.

## Not done

- No measurement on a copy of the 20261107T100413Z nightly under unit-like limits
  (`CPUQuota=100%`, `IOWeight=20`, `MemoryHigh=2G`). The brief excluded backups and
  stores, so the issue's acceptance measurement is still open. That run decides whether
  step 2 (online duty) is needed.
- `make check`, including `census-check` and the workspace-wide lint and tests: not run,
  per the brief. It runs in the commit hook.
- No commit or push.
