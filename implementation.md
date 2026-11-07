# Implementation: issue 13, bound SPARQL work by the request's scope

Branch `paudley/13-bound-sparql-work-by-the-request-s` from `main` 8e7a191. Uncommitted, as
instructed. Paths are relative to the worktree root.

## Work items

### Owner-to-graph index (`crates/sparrow-core/src/storage/rdf_index.rs`)

- `rdf:o:<owner><graph>` key: `OWNER_GRAPH_PREFIX` at `rdf_index.rs:295`,
  `owner_graph_key` at `rdf_index.rs:299`.
- Written beside every `rdf:g:` declaration in `RdfWrite::graph_inner`
  (`rdf_index.rs:640`) and deleted beside it in `RdfWrite::remove` (`rdf_index.rs:712`),
  in the same batch through the same owner. These are the only two places that write or
  delete `rdf:g:` rows, so ingest, metadata replacement, retirement, pin expiry and the
  other startup migrations all keep the two sets equal.
- Open-time build `upgrade_owner_graphs` (`rdf_index.rs:314`) beside `upgrade_reverse`,
  paged by `owner_graph_step` (`rdf_index.rs:337`). It first commits the fence and a
  `[1]` checkpoint in one synced batch, then copies `rdf:g:<graph><owner>` keys into
  `rdf:o:` in pages of 65,536 declarations. Each page commits its rows and its frontier
  (`[1]` plus the last copied 16-byte suffix) together through
  `catalog_upgrade::commit` after `artifact_rdf::admit` disk admission; `[0]` marks
  completion. A malformed or zero-identity declaration refuses with `STORAGE_FAILURE`
  and publishes nothing from its page. A fenced store whose checkpoint is missing
  refuses.
- It reads only declaration keys, never catalog records or manifests, so graphs that a
  staged import prepared before publication (the #20 lesson) are covered and cannot make
  it fail.
- Called from `Store::open` after `event_fallback_rdf::upgrade` (`storage.rs:1424`).

### Writer-format fence (`crates/sparrow-core/src/storage/writer_format.rs`)

- Five new families `OWNER_GRAPH_TRACING` .. `OWNER_GRAPH_INTAKE_ARCHIVE`
  (`writer_format.rs:218` onward, grouped as `OWNER_GRAPH` at `writer_format.rs:285`):
  `sparrow.store/187` to `/241`. Each lifts one event fallback family by 55, in the same
  order, exactly as the fallback families lift the tracing, log time, archive and intake
  families. No existing family was renumbered.
- `lower` (`:307`) now strips the owner graph lift, then the fallback lift; `relift`
  (`:315`) reapplies both, so `enable_log_time`, `enable_archive`, `enable_intake` and
  `stage` keep the fence. `has_event_fallback` is true for owner graph identities and
  `enable_event_fallback` is idempotent on them. New `has_owner_graphs` (`:332`) and
  `enable_owner_graphs` (`:337`); `families()` and `read()` include the new identities so
  authority checks still resolve to the original base family.
- The unit test `every_authority_transition_preserves_the_artifact_writer_fence` now also
  stages every base transition from each owner graph family and checks lifting,
  idempotence, switch preservation and that no older family contains the new identities
  (`writer_format.rs:731` onward).

### Scope-bounded evaluation (`rdf_view.rs`, `rdf_view/sparse.rs`, `rdf_view/scope.rs`)

- `QueryScope::restricts_membership` (`crates/sparrow-core/src/query.rs:138`): a
  selection or pin, or a live scope with `source`, `sources`, diagnostics, or a receipt
  bound other than `[0, u64::MAX)`.
- `StoredRdfView.scoped` (`rdf_view.rs:117`) is set from it in `capture_rdf_scope`
  (`rdf_view/scope.rs:342`) and always for explicit record lists in
  `with_rdf_record_layers` (`rdf_view.rs:934`). The whole live store keeps `false`, so
  its plan is unchanged.
- Plan selection in `rows` (`rdf_view.rs:404`): named-graph patterns keep the indexed
  plan; unbound patterns keep using `ScopedRows` with plan 0 (as before); in a scoped view
  every bound default or any-graph pattern now also goes through `ScopedRows` with the
  store-wide plan the engine chose.
- `ScopedRows` (`rdf_view/sparse.rs:21` onward) takes the plan and pattern. For a
  store-wide plan `P` in `SPOG`/`POSG`/`OSPG` each in-scope graph is sought in `P + 3`
  (`GSPO`/`GPOS`/`GOSP`, `sparse.rs:94`) with the graph then the bound terms as prefix.
  That twin's tail after the graph is `P`'s order, so each cursor is sorted by `P`, and
  the existing heap keys rows by `P`'s axes (`sparse.rs:166`). This keeps one global
  order identical to the store-wide plan, and equal union-default rows from different
  graphs pop consecutively, so the existing `previous` deduplication still removes them.
  For the cases the issue names this is GSPO when the subject is bound, GPOS when the
  predicate is bound, GOSP when only the object is bound and GSPO when none is.
- `graph_rows` (`rdf_view.rs:470`): a scoped view enumerates graphs with `owner_graphs`
  (`rdf_view.rs:486`), one `rdf:o:<owner>` prefix walk per captured owner, collected into
  an admitted ordered set (64 bytes per graph reserved), emitted lazily through
  `OwnedGraphs` (`rdf_view.rs:622`) with the same `verified_forward` check as before.
  `named_graphs` and `ScopedRows` initialisation both use it, so unbound patterns no
  longer read every declaration in the store.
- Charging is unchanged: one request and row visit per index row advance, including the
  terminal probe (charged before each advance in the owner walk, as `GraphIter` does),
  128 bytes per quad row, 64 per declaration row, two point reads per verified term,
  4,096 bytes reserved per cursor. Only rows of in-scope graphs are read.

### Docs and ledger

- `docs/architecture.md:528`: owner-to-graph index, per-graph evaluation, ordering and
  dedup, charging, whole-store plan, migration.
- `docs/interfaces.md:539`: new "Owner-to-graph index (store format)" section (families,
  job, resumability, first-open cost, no wire change). `docs/interfaces.md:940`: SPARQL
  request cost follows the scope's graphs and rows, not the store's size.
- `docs/archive-format.md:259`: the archive families under the new fence.
- `helpers-ledger.toml:159`: job `owner-graph-index`.

## Named tests (`crates/sparrow-core/tests/storage.rs`)

| Test | Line | What it protects |
| --- | --- | --- |
| `owner_graph_index_tracks_graph_declarations` | 18776 | `rdf:o:` equals swapped `rdf:g:` after ingest (metadata, diagnostic, shared content, jsonl, log), after retirement with one pinned record, and after pin expiry; declaration count strictly falls at each step. |
| `scoped_sparql_answers_match_the_store_wide_plan` | 18808 | 11 queries (unbound, `GRAPH ?g`, group count, predicate-bound, predicate+object, subject-bound, subject+object, object-only, named-graph IRI) in four layer selections; a source-filtered and a receipt-window scope (scoped plan) return rows identical in content and order to the unfiltered whole-store plan, all as complete (exact) results; at least 30 comparisons are nonempty; union-default `?s a sp:Source` returns fewer rows than its `GRAPH ?g` form. |
| `scoped_sparql_cost_is_independent_of_store_size` | 18873 | A one-record selection (with its single-record artifact) answers `?s ?p ?o LIMIT 5`, `?r sp:byteLength ?n` and a `GRAPH ?g` count with identical rows and identical request counts before and after 20,000 more records are ingested, each below 1,000 requests; an all-layer `GRAPH ?g` count is also identical before and after. |
| `owner_graph_index_migration_builds_the_index_and_fences_older_writers` | 18937 | A store reset to event fallback family `/132` with no index and no checkpoint, holding a prepared unpublished import, reopens as `/187` with `rdf:o:` equal to `rdf:g:` and the same scoped answers; a fenced restart with checkpoint `[1]` and no rows rebuilds; a fenced store missing its checkpoint refuses with `STORAGE_FAILURE`; publication afterwards keeps the sets equal. |

Supporting helpers: `scoped_rows` (18686), `owner_graph_fixture` (18718),
`owner_graph_rows` (18749). Unit test
`owner_graph_build_resumes_after_reopen_and_refuses_malformed_declarations`
(`rdf_index.rs:1284`) covers page-by-page resumption across a database reopen and the
malformed-declaration refusal. The existing `rdf12` view test now also asserts no `rdf:o:`
rows remain after removal.

Existing tests updated for the new identities (+55): `storage.rs` lines 35, 221, 226,
290, 591, 1275, 1456, 1759, 1886, 4716, 4794, 5373, 5411, 5418, 5425, 5976, 5991,
6039, 6064, 6099, 6116, 6394, 11822; computed offsets at 2425, 2925, 3268, 3912, 4431;
archive range at 18391, 18392, 18403; the unknown-identity probe at 18435 moved from
`/187` to `/242`. `crates/sparrow-server/tests/rpc.rs` lines 276, 628, 3869, 4086, 4872.

## Checks (logs under `.stage/bound-sparql-work-by-the-request-s/logs/`)

All release profile, `--locked`.

| Check | Log | Exit |
| --- | --- | --- |
| `cargo fmt --all -- --check` | `fmt-check-7.log` | 0 |
| `cargo clippy -p sparrow-core --all-targets -- -D warnings` | `clippy-core-7.log` | 0 |
| `cargo test -p sparrow-core --lib` (34 tests) | `test-core-lib-7.log` | 0 |
| `cargo test -p sparrow-core --test storage` (all 220 tests) | `test-storage-full-6.log` | 0 |
| `cargo test -p sparrow-server --test rpc` filtered to the five tests whose format assertions changed | `test-rpc-7.log` | 0 |

Earlier failing runs are kept for the record: `clippy-core-1.log` (dead field, fixed),
`test-named-1.log` (one 10,000-record artifact exceeds RDF write staging; split into
1,000-record artifacts), `test-cost-3.log` (cost numbers below), `test-named-4.log`
(whole-store capture refusal, see deviations), `test-storage-full-5.log` (computed
format offsets not yet updated). `make check` and workspace-wide commands were not run,
per the task limits; clippy on `sparrow-server` was not run (only its test literals
changed). The rpc filter `sparql` matched no additional tests.

## First-open cost and resumability

The first open of an existing store, after the other startup jobs, makes one forward pass
over the `rdf:g:` key range (16-byte suffix, empty values; no catalog reads, no JSON) and
writes one 22-byte `rdf:o:` key per declaration, in synced pages of 65,536. A record
contributes its metadata graph, its diagnostic graph when profiled, and a share of its
content projection's graphs; artifacts add one each. For about 136,000 records that is an
estimated few hundred thousand declarations, so on the order of 5 to 10 pages and tens of
megabytes of keys: sequential key I/O expected to take seconds, not a rebuild of the
13 GB store. This estimate was not measured on the live store. The job is resumable across
restarts: the fence and checkpoint commit before any page, every page commits with its
frontier, and a restart resumes after the last committed declaration. The service stays
unavailable until the build finishes, as with the #20 job.

## Deviations from the issue

- Graph-leading order choice: the issue lists GSPO/GPOS/GOSP by which term is bound. The
  implementation uses the graph-leading twin of the store-wide plan (`P + 3`). That
  equals the issue's list for every case it names and, when subject and object are both
  bound, picks GOSP (the store-wide plan there is OSPG), because the cursors must be
  sorted by the store-wide order for the heap to keep one global order.
- `scoped_sparql_cost_is_independent_of_store_size` selects the record together with its
  single-record artifact. `sp:byteLength` is written only on artifact metadata
  (`rdf_index.rs` `artifact_metadata`), so over a record alone the query returns no rows
  (measured: 0 rows, 36 requests). The `?s ?p ?o LIMIT 5` query over the record alone
  cost 248 requests.
- The `< 1,000` bound is asserted for the metadata layer. With all three layers the
  one-record `GRAPH ?g` count cost 1,458 requests on the record-only fixture: identical
  in the 1-record and 20,001-record stores, but above 1,000, because each emitted row
  still charges one row visit plus two point reads for each of its four terms (the
  unchanged charging law) and content graphs add rows. The test asserts the all-layer
  count is unchanged by store size instead.
- "Where today's plan exceeds 100,000": the old plan no longer runs for scoped requests,
  so the test proves independence (equal counts at 1 and 20,001 records) rather than
  running the old plan. A whole-live-store query over the 20,001-record store is refused
  earlier by catalog capture admission (`catalog capture exceeds operation admission`,
  scan budget 10,000), so it was not used as the comparison.
- Source admission profile `sparrow-rdf-source-v4` is unchanged: the charging law is the
  same, only the rows read differ.

## Not done / for the caller

- Not committed, not pushed; `make check` (the hook gate) not run.
- UAT not run (requires deployment). UAT step 2 as written (`?r a sp:Record ;
  sp:byteLength ?n`) will return zero rows, not `N`: records carry no `byteLength`, only
  artifacts do. The step needs a different predicate (for example the record's own
  metadata) or an artifact-based query; this change does not alter the vocabulary.
- Large filtered live scopes: per-graph evaluation holds one cursor (4 KiB admitted) per
  in-scope graph, so a filtered live scope with many tens of thousands of graphs fails
  with `RESOURCE_LIMIT` on live bytes rather than requests. Such a scope already exceeded
  the request ceiling under the old plan.
