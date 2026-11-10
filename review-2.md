# #158 delivery re-review (review 2)

## Scope

This review covers the uncommitted worktree
`/home/paudley/Active/sparrow/.worktrees/158-serve-every-query-and-status-call`
(branch `paudley/158-serve-every-query-and-status-call`), compared with `main` at
ea1a4b2 (`git diff main`, 32 files). I read the issue, the plan's `## User rulings`
(Patrick's "not ready" ruling, plus coordinator rulings R1, R2 and R5 and the
stalled-status order), validation.md and its post-ruling section, and the Rust skill.
I did not review the temporary probe `crates/sparrow-core/tests/measure_158.rs`.

This was a code-only review. I ran no cargo, make, build or test command because of
the build freeze, and I touched no live store, daemon or unit. The only commands I
ran were `git diff`, `grep`, reads of the stage logs, and `prose-check` on the four
changed docs.

## Evidence

- Last build attempt on this tree: `/home/paudley/Active/sparrow/.stage/r82/158-check12.log`
  (05:19) ended with `EXIT=101`. It reported three Clippy/dead-code errors:
  `validate_diagnostic_record` unused, `walk` with too many arguments, and
  `read % 4096`. Sources were edited afterwards, between 05:19 and 05:28
  (`source_context.rs`, `context.rs`, `diagnostic.rs`, `artifact_read.rs`,
  `source_summary.rs`, `source_read.rs`, `rdf_view.rs`, `migration.rs`). No log shows
  the current tree compiling or passing a test.
- The latency table (`158-measure5.log`, 04:31) comes from the pre-ruling tree. It
  does not measure the snapshot context path, the owed pass outside the lock, the
  owner-pending declaration walk, or status during `store-counters` seeding with
  `exact_values`.
- `prose-check` on interfaces.md, architecture.md, deployment.md and RECOVERY.md
  exited 1. Every hit is the existing `UInt64`/`UInt32` identifier pattern, and none
  is in the changed text.

I verified the following by reading the code:

- Ledger mirror (`catalog_lease.rs:291-441`). `snapshots` is now private. Every
  mutation path calls `mirror()`: publish insert, publish removal after cancellation,
  release, and purge. `Store::open` clones each ledger `Arc` before moving its
  registry into the `Mutex` (`storage.rs:1575-1625`), and no path replaces a
  registry. `read_stats` applies the same liveness test as `Snapshot::alive`.
  Status (`storage.rs:2656-2675`) no longer locks a registry. The status RPC's other
  calls (durability, pressure and recompression copies, consumption retired) take no
  writer or registry lock.
- `exact_values` (`store_counters.rs:596-616`). Rows hold keys up to
  `min(follows, seed)`. The added `count(low = bound, high = follows)` covers exactly
  the gap. `count` returns 0 when `high <= low` or `low = Everything`. `report` uses
  rows for `store-counters` `done` and exact values everywhere else, and the journal
  keeps `total = None` while seeding.
- Context (`context.rs:146-262`). The protected set, membership, scope receipt,
  `cached` and the snapshot are captured under the writer lock, and the lock is
  dropped before any walk. Every catalog write goes through the writer, so
  `cached(&writer.db)` equals the snapshot's state. `Timeline::walk` positions
  `(received_at, after)` the same way in both key families. Reverse iteration from
  `…{t}:` starts before `t`, and the `after` key is skipped. The receipt bounds stop
  the walk before a record outside the interval is decoded.
- Artifact listing merge (`artifact_read.rs:909-942`, `:860-867`). For equal keys
  the stored row wins and `after` moves past both, so a copied row is listed once. A
  retired owed artifact folds to `summary == None` through the missing-entry branch
  (`Consumed::Indexed`) and is skipped. An artifact unlisted since then is dropped by
  `finish_row`. Artifacts created after the start snapshot get stored rows from the
  live observer (`source_summary.rs:627-633`, the `created` case). Owed rows computed
  at any snapshot taken no later than the page are therefore exact.
- `Completion::owed` (`source_summary.rs:778-814`) runs the same
  `artifact_page`/`record_page` through the `Rows` sink. The single unbounded record
  pass counts a deleted parent's records together, which matches the cross-page
  `Partial` accumulation.
- Scoped SPARQL, owner pending (`rdf_view.rs:774-889`). Graphs are the `rdf:o:`
  mirror rows plus the declarations from the frontier onward, filtered to the
  owners, deduplicated by the set and memoized once per view. The walk runs during
  `operation`, after `drop(writer)` (`rdf_view/scope.rs`). It checks the budget's
  checkpoint every 4,096 rows. The view does not certify probe order, so appending
  owed rows to `rows()` is allowed. The owed graph semantics for `Any`, `Default`
  and `Named` match the stored union law.
- Kept `EVIDENCE_INCOMPLETE` sites (`diagnostic.rs:595`, `text_index.rs:58`/`:566`,
  `record_fields.rs:113`/`:130`, `storage.rs:2104`, `rdf_view/scope.rs:187`/`:281`,
  `query.rs:208`, `artifact_read.rs:1364`) all test per-record state written at
  ingest. None depends on migration progress. No `INDEX_NOT_READY` remains in Rust
  sources. What remains is in dated run records (`docs/development-runtime.md`,
  `docs/performance-baseline.md`).

## Findings

### F1 REQUIRED: the live artifact listing still refuses because a migration is unfinished

`artifact_read.rs:432-457` (`owed_index_rows`) returns `RESOURCE_LIMIT` when the
owed index rows for the listing's selection exceed 32 MiB (64 bytes plus the key,
about 200 bytes per artifact, which is roughly 165,000 owed artifacts). The refusal
exists only while `source-summary` is unfinished. At D147 that migration starts from
zero on the live store (rulings). An unfiltered or wide live `artifacts` listing
over that many artifacts therefore refuses until the migration advances.

- This is "not ready" under another code, which Patrick's ruling forbids. The issue
  requires "returning a resumable cursor rather than a partial result."
- The docs contradict themselves. `docs/interfaces.md:824-827` says the set
  "refuses with `RESOURCE_LIMIT`" and then "No reader refuses because a migration is
  pending." `migration.rs:23-24` and `docs/architecture.md:563` claim no reader
  refuses either.

Fix: window the owed set instead of refusing.

- During the `Completion` pass, keep only the smallest owed keys above the listing
  position, in index order, that fit a fixed share of the page admission (a bounded
  max-heap keyed by index key).
- Record in `Owed` the window's last key and whether the window was truncated.
- In `next_listed_row`, when a truncated window is exhausted and the merge passes
  its last key, recompute the next window from a fresh snapshot, keeping keys above
  `position.after`. Do this before the page takes the writer (the continuation is
  already outside it in `artifact_read_cancellable`), or drop to a new continuation
  page.

Exactness holds for any snapshot taken no later than the page, as verified above.
This keeps the answer exact, bounds memory, and never refuses, which satisfies both
"charged to the page's admission" and the no-refusal ruling. Then remove the
`over()` refusal and correct the three doc passages.

### F2 REQUIRED: the "uncharged" event-fallback lookups are charged on every dictionary hit

`rdf_view.rs:615-657`: `lookup_in(value, charged = false)` skips the charge only for
the reverse probe. On a hit it calls `verified_forward` (`:660-681`), which charges
`point(...)` (a charged `point_in`) and `reverse_point(.., true)`. During the
backfill, the following lookups normally hit, so each pending selected record spends
several charged requests and about `max_encoded` (up to 65,536) admitted bytes per
hit:

- `fallback_pending` (subject)
- `owe_fallback` (graph IRI, subject)
- `owe_term` (predicates and the basis IRI)

Two consequences:

- `docs/interfaces.md:669-672` ("None of these reads is charged to the request
  budget, so a metadata scope near its budget answers during the migration as after
  it") and the validation.md R5a claim are false.
- A large strict or permissive metadata scope can still hit `RESOURCE_LIMIT` only
  while the backfill is pending. Before #158 that was the documented behavior; the
  new contract removes it.

Fix:

- Thread `charged` into a `verified_forward_in(id, charged)` used by `lookup_in`,
  keeping the memory reservation.
- Extend the existing schema 69 scoped SPARQL test
  (`crates/sparrow-core/tests/storage.rs`, the event-fallback owed-facts case near
  :19296) with a request budget that the post-migration answer fits, and assert the
  same scope answers while the backfill is pending.

### F3 REQUIRED: no execution evidence on the current tree, and the issue's latency comparison is missing for the new paths

The last compile failed (see Evidence), and the tree changed afterwards. fmt,
Clippy, the RPC tests and the storage tests have not run since the post-ruling
changes. `source_context.rs:238-291` is not indented inside `impl Timeline`, so the
fmt gate would rewrite it.

The issue requires "the answer latency during the rebuild is measured and reported
… against the latency after it." The coordinator also ordered that a status call
returning while writes are stalled be recorded with its latency. Neither exists for
the current code:

- Status during `store-counters` seeding now costs a scan of every unseeded key, per
  call (`store_counters.rs:596-616`). That is up to four passes over `record:`
  (Records, RetainedRecords with a full JSON parse per record, SourceContextDone,
  SourceSummaryDone), plus `payload`, graph, record-graph, projection and artifact
  keys. At D147, seeding starts from zero on about 1.7 million records, and
  `deploy/user-unit-upgrade.sh:264-276` takes its first status inside that window.
  The live incident was a status call dying at about 61 s. The 0.30 ms "during"
  figure in validation.md predates this code.
- Live artifact page and source listing: the owed pass is now outside the lock.
- Scoped SPARQL: the owner-pending declaration walk.
- Context: the snapshot path.

Fix:

- When the freeze lifts, run focused tests:
  - `every_read_answers_completely_while_online_migrations_run_and_intake_flows`
  - `status_is_exact_while_counters_seed_and_reads_fixed_keys_without_the_writer_once_seeded`
  - `source_summary_migration_serves_at_once_and_listings_answer_finished_counts`
  - the schema 69 SPARQL and context tests
  - the sparrow-server RPC target (`verify_backup_reports_pending_counts_as_not_verifiable`
    now expects a pass)
  - then fmt, Clippy and the hook.
- Rerun the probe on the current tree with `store-counters` pending, and record
  status latency during seeding against after, and during a held writer.
- If seeding-time status is seconds at probe scale, make `exact_values` count each
  prefix in one pass that evaluates every term, rather than one scan per term, and
  report the live-scale estimate in the PR.

### F4 REQUIRED: the merge behaviors the docs promise for one listing are not exercised

`docs/interfaces.md:354-359` and validation.md claim two behaviors: "A row the
migration has copied since is listed once, and an artifact retired since is
skipped." The acceptance loop (`storage.rs:20596-20615`, with `online_answers` from
:20435) reads each listing to the end before the next `migrate_step`, and no
retirement happens inside a listing. So no test drives the equal-key dedupe
(`artifact_read.rs:928-933`) or the owed-retired skip (`:863-867`). The code reads
correctly, but this is the core of R2's "exact answer" across continuation pages,
and a regression would show up as duplicated or phantom rows.

Fix: in the acceptance test or the source-summary listing test, open a live listing
with a small `page_size` while `source-summary` is pending. Between continuation
pages, run enough `migrate_step` pages to copy owed rows past the cursor, and expire
one owed artifact ahead of the cursor. Then assert that the concatenated listing
equals the finished-migration listing.

### O1 OPTIONAL: stale cost comment on `status_counted`

`storage.rs:2653-2655` still says status reads "a fixed number plus one per pin row
… whether or not the counters are seeded." That is false while seeding. Say "once
seeded," as interfaces.md now does.

### O2 OPTIONAL: owed rows sit outside the registry memory bound

`Owed` (`artifact_read.rs:173-181`) is shared by `Arc` across a listing's handles,
but it is excluded from `Snapshot::capacity`. Up to `MAX_HANDLES` (8) listings can
each keep 32 MiB resident for a lease, outside the 8 MiB `HANDLE_BYTES` registry
budget. Windowing (F1) bounds this. Otherwise, charge each distinct owed set to the
registry capacity.

### O3 OPTIONAL: owed fact scans are linear, as in the earlier review's O3

`owed_rows` (`rdf_view.rs:467-494`) filters every owed quad on every pattern cursor,
and a scoped view opens one per graph, so the cost is O(graphs × owed quads).
`lookup_in`'s owed-term search is linear too. Index owed quads by graph and owed
terms by canonical bytes.

### O4 OPTIONAL: the stalled-status test proves only the source registry case

In `status_is_exact_while_counters_seed…` (`storage.rs:19885-19898`), the artifact
listing computes its owed pass before it locks its registry. It may still be in that
pass when status runs, so only the source listing deterministically holds its
registry. That is sufficient to catch the regression, but the doc comment's claim
that "listings … keep their read registries locked" overstates it. Adjust the
comment, or hold the artifact listing at its registry with a test pause.

### O5 OPTIONAL: the source listing holds its registry for the whole owed pass

`source_read.rs:318-374` takes `source_reads` before the writer and keeps it through
`Completion::owed`, which takes seconds while pending. Concurrent `sources` captures
and cursor replays therefore serialize, each holding an RPC permit, and `Rpc::work`
refuses status with `RESOURCE_LIMIT` once permits run out (that mechanism already
existed; this change amplifies it). Capture the snapshot under the writer, compute
the owed pass, and lock the registry only to publish.

VERDICT: BLOCKED
