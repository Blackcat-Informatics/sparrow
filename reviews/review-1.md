# Delivery review 1: issue 13, bound SPARQL work by the request's scope

## Scope

- Worktree `/home/paudley/Active/sparrow/.worktrees/13-bound-sparql-work-by-the-request-s`, uncommitted
  `git diff` against `main` 8e7a191 (13 files, +877/-62).
- Read verbatim: `issue.md` (body plus the #10 UAT comment), `plan.md` with `## User rulings`
  (two coordination rulings, ordering and closure only; no scope cuts), and `implementation.md`. I
  treated the implementation report as claims to check.
- Reviewed: `rdf_index.rs` (key, writer, remover, open-time job, unit test), `storage.rs` (open
  order), `writer_format.rs` (families, lower/relift, enable/has, `stage`, `read`, unit test),
  `query.rs` (`restricts_membership`), `rdf_view.rs` (`rows`, `physical_rows`, `QuadIter`,
  `graph_rows`, `owner_graphs`, `GraphIter`), `rdf_view/sparse.rs` (`ScopedRows`),
  `rdf_view/scope.rs`, `rdf_view/budget.rs` (failure stickiness), the four named integration tests
  and their helpers, the +55 identity edits in `tests/storage.rs` and `sparrow-server/tests/rpc.rs`,
  the docs and the ledger row.

## Evidence

Commands I ran, all read-only:

- `cargo test --release --locked -p sparrow-core --test storage -- owner_graph scoped_sparql`
  exited 0, with 5 passed and 215 filtered out. The run covered the four named tests plus
  `scoped_sparql_uses_native_read_forms_bindings_and_governor_outcomes`.
- `cargo test --release --locked -p sparrow-core --lib -- owner_graph every_authority_transition`
  exited 0, with 2 passed: `owner_graph_build_resumes_after_reopen_and_refuses_malformed_declarations`
  and `every_authority_transition_preserves_the_artifact_writer_fence`.
- I ran `prose-check` on the added doc lines (stdin), and it exited 0. On the whole files it reports
  only existing `UInt64`/`UInt32` lines outside this diff.
- `grep` over `crates/sparrow-core/src` found that `graph_key` and `rdf:g:` are written in
  `RdfWrite::graph_inner` and deleted in `RdfWrite::remove`, and nowhere else. No `delete_range`
  is used. Every retirement, replacement and upgrade caller goes through `RdfWrite`.

Implementer logs: the timestamps of `test-storage-full-6.log` (220 passed, EXIT=0, release) and the
`-7` logs (clippy `-D warnings`, fmt, lib 34, rpc 5 filtered, all EXIT=0) are later than the last
source edit, so they apply to the reviewed tree. They did not run `make check`, the census gate or
Clippy on `sparrow-server`. The pre-commit hook still has to run them.

## Assessment by focus area

1. **`rdf:o` beside `rdf:g`.** Verified. `graph_inner` puts both keys for every graph in the same
   batch. `remove` deletes both, using the owner read from the manifest. `replace_metadata` is
   remove followed by `graph_inner` in one batch. Ingest, retirement (`retire.rs`), pin expiry,
   recordless cleanup and the startup jobs all reach these two functions. The
   `owner_graph_index_tracks_graph_declarations` test checks equality after ingest, retirement and
   pin expiry, with the declaration count falling strictly at each step.
2. **Open-time job.** Ordering is correct. One synced batch commits the fence and the `[1]`
   checkpoint before any page. Each page commits its rows and frontier together after disk
   admission, so a crash leaves either the fence with a resumable checkpoint or the old format
   untouched. Resume restarts at the frontier key and skips it exactly. Page-boundary handling
   (an exact multiple, an empty store, a fresh `/5` store lifted on first open) ends in `[0]`.
   Repeating a page is safe because the puts are idempotent. The job reads only `rdf:g:` keys,
   never catalog records, so prepared graphs from an unpublished import are covered (the #20
   round 4 lesson). The migration test includes a prepared import and checks that publication
   afterwards keeps the two sets equal. A fenced store with no checkpoint refuses with
   `STORAGE_FAILURE`. On an already-migrated store the job does one point read.
3. **Writer families 187..241.** These follow the #20 pattern: five 11-identity families, each
   lifting one fallback family by 55 in the same order. No existing identity was renumbered.
   `lower` strips the owner-graph lift and then the fallback lift. `relift` reapplies both, so
   `stage`, `enable_log_time`, `enable_archive` and `enable_intake` keep the fence (covered by the
   unit test). `families()` and `read()` include the new identities, so authority checks resolve
   to the base family. Older writers are fenced: on `main` the unknown-identity probe at
   `tests/storage.rs:18435` used `sparrow.store/187` and expected `STORAGE_FAILURE`, which shows
   the schema-69 binary refuses `/187`. This diff moves that probe to `/242`.
4. **Scoped equals store-wide.** For a store-wide plan `P` in 0..3, the twin `P + 3` has `P`'s
   order as its tail after the graph. Each per-graph cursor is therefore sorted by `P`, and the heap
   key `ORDERS[P].1` applied to `[s,p,o,g]` reproduces the store-wide global order. With union
   default (`g` = None → 0), equal rows from different graphs tie and pop consecutively, so the
   existing `previous` check removes duplicates. `probe_plan` returns `< 3` for every default or
   any-graph pattern, so the `plan >= 3` fallback to `Indexed` is never taken for bound,
   non-named patterns. Named-graph patterns keep the indexed graph-leading plan. Charging is
   unchanged: one request and row visit per index advance including the terminal probe, 64 bytes
   per declaration row, 4 KiB per cursor, and two point reads per verified term. Every failure in
   `owner_graphs` and `next_owned` records the error on the sticky budget before returning `None`,
   so a truncated graph list cannot appear as a complete answer. In the whole-live-store plan
   (`scoped == false`), bound patterns stay `Indexed` and unbound patterns stay
   `ScopedRows(plan 0)` over the `rdf:g:` walk, which is byte-for-byte the old behavior (plan
   `0 + 3` = GSPO, identity heap key). SPARQL, explain and correlation all reach this code through
   `with_native_result` → `capture_rdf_scope`.
5. **The four departures from the issue.**
   - *`sp:byteLength` selection includes the artifact.* Acceptable. `byteLength` is written only by
     `artifact_metadata` (`rdf_index.rs:1087`; no record-level writer exists), so a record-only
     selection has no `byteLength` rows to return.
   - *All-layer `GRAPH ?g` count is 1,458 requests.* Acceptable. The issue's test asks for the three
     queries below 1,000 without naming a layer, and the metadata-layer assertions satisfy it. The
     all-layer count is asserted equal at 1 and 20,001 records, and UAT step 3 sets no request
     threshold.
   - *Independence instead of "old plan exceeds 100,000".* Acceptable. The equality assertion
     fails under the old plan: the unbound `rdf:g:` walk and the store-wide `byteLength` scan both
     grow with the 20,000 bulk records. It therefore protects the scope-bounded-cost regression.
     It does not show the old plan crossing 100,000 at this fixture size; the issue states that
     as its premise, and the test does not check it.
   - *Graph-first twin of the store-wide plan.* Acceptable, and needed to keep the issue's "one
     global order" requirement. It matches the issue's list for every case the issue names. For
     subject+object it picks GOSP, a longer prefix than GSPO.
6. **Docs.** The `architecture.md` section 7 paragraph, the `interfaces.md` store-format section
   and SPARQL cost paragraph, and the `archive-format.md` ranges (209–219, 231–241) all match the
   code. The added prose passes `prose-check`.
7. **Test value.** Each named test protects a stated invariant: mirror equality, scoped/store-wide
   row and order parity with exact certainty across four layer sets and 11 pattern shapes (at
   least 30 nonempty comparisons, plus a check that union-default deduplication happens),
   size-independent cost, and migration with fencing and resume. The unit test covers mid-page
   resume across a reopen. The +55 identity edits are mechanical updates of existing assertions.

## Findings

- **OPTIONAL (must reach the issue owner before closure; not a code correction).** UAT step 2 as
  written (`?r a sp:Record ; sp:byteLength ?n`, expecting `n == N`) cannot pass. Records carry no
  `sp:byteLength`; only artifacts do (`rdf_index.rs:1087`). The implementer reported this too.
  This change does not alter the vocabulary, and the issue's scope does not authorize doing so.
  Ask Patrick or coordination to amend the step, for example to an artifact-subject query or a
  record predicate such as `sp:receivedAt`, before the UAT is judged. The #10 `sp:peerUid` UAT is
  not affected: `intake_metadata` writes on both records and artifacts (`rdf_index.rs:1101,1251`).
- **OPTIONAL.** `scoped_sparql_answers_match_the_store_wide_plan` uses two scoped scopes whose
  membership equals the whole fixture: every record has source `graphs` and the window 1..5 covers
  every receipt. Parity is therefore never checked for a strict subset, where a shared or
  multi-owner graph must drop out-of-scope owners' rows. The code path is the unchanged owner
  filter in `QuadIter`, and the cost test's equal rows before and after growth partly cover it. A
  one-record `Evidence` scope compared with a store-wide query filtered to the same record would
  close the gap.
- **OPTIONAL.** In the unit test, the malformed declaration `graph_key(0, 5)` sorts before every
  valid row, so the error fires on the first row. "Publishes nothing from its page" therefore holds
  trivially. A malformed key sorting after valid rows in the same page would test the batch
  atomicity the comment claims.
- **OPTIONAL.** Earlier startup jobs (`tracing_rdf`, `event_fallback_rdf`) now write `rdf:o:` rows
  through `RdfWrite` before the owner-graph fence commits. Suppose the first new open crashes in
  that window and a schema-69 binary then runs. That binary can retire graphs without deleting
  their mirrors, and the build later copies `rdf:g:` but never removes stale `rdf:o:` rows. Answers
  stay correct, since in-scope rows still require an in-scope owner, but named-graph enumeration
  could list an empty graph for an owner whose term is still in scope. This is a narrow
  crash-plus-downgrade window. A note in the interfaces section, or a final subset check in the
  job, would cover it.
- **OPTIONAL (evidence).** `make check`, including `census-check` for the new ledger row and its
  forbidden names, has not run. The commit hook must pass it before this is committed. The
  first-open cost on the 13 GB live store is an unmeasured estimate.

No REQUIRED findings.

VERDICT: PASS
