# Issue 36 step 2: run the schema 69 migrations online

Branch `paudley/36-bound-the-schema-69-first-open`, base `eedb484`. Nothing is
committed. The worktree holds the complete change; `make check` was not run (by
instruction).

## Design

`Store::open` no longer runs the #20 event-fallback page loop or the #13
owner-graph build. It applies both writer-format fences and seeds both
checkpoints in one synchronized batch, validates both checkpoints, and returns.
A store with no record graph or no graph declaration seeds that migration as
complete, so a new store reports both complete. The joined maintenance worker
runs the pages as one duty, one page per admission, owner index first. A page
commits through `Writer::commit`, the ordinary write path, so it shares the
durability latch, the free-space floor, the data-write admission and the
pressure request. Its rows and its checkpoint are in one synchronized batch, so
a restart resumes after the last committed page. The page builders are the
existing ones, split from their commit: `catalog_upgrade::page` (used by the old
`step`) and `rdf_index::owner_graph_page`. There is one page implementation per
job, and both the old open-time loops are gone.

Writer-format families are not renumbered. The fence is the same
`enable_event_fallback` then `enable_owner_graphs` lift as before, so older
writers still refuse the store. A fenced store whose checkpoint is missing
still refuses to open.

## Changes (file:line in the working tree)

- `crates/sparrow-core/src/storage/migration.rs` (new). `fence` :95,
  `Writer::pending_migration` :116, `Writer::migration_page` :127,
  `Store::migrate_step` :176 (latches or clears the per-migration failure),
  `Store::migrations` :196. Page bounds: owner index 65,536 declarations (:20),
  event fallback 64 graphs (:23, below the job's own 4,096-graph and 16 MiB
  bounds, to keep the writer hold short; see the profile).
- `crates/sparrow-core/src/storage.rs:1426`: open calls `migration::fence`
  instead of the two loops; `Writer.migrations` :509, initialized :1462.
- `crates/sparrow-core/src/storage/catalog_upgrade.rs`: `progress` :169 and
  `page` :190 split out of `step`/`run`; behavior of the other jobs unchanged.
- `crates/sparrow-core/src/storage/rdf_index.rs`: `fence_owner_graphs` :313
  (seeds `[0]` when no declaration exists), `any_key` :328,
  `owner_graph_progress` :337, `owner_graph_counts` :358, `owner_graph_page`
  :382; `upgrade_owner_graphs` and `owner_graph_step` removed;
  `complete_test_migrations` :1299 (test only).
- `crates/sparrow-core/src/storage/event_fallback_rdf.rs`: `CHECKPOINT` :24,
  `fence` :38 (seeds complete when no record graph exists), `frontier` :55,
  `decode_frontier` :60, `page` :76, `counts` :86 (from the empty
  `provenance-rdf:record:` markers, one per record graph), `carries_event_time`
  :109, now shared by the page's skip rule and the reader; `upgrade` removed.
- `crates/sparrow-core/src/storage/rdf_view.rs`: `owner_index` and `fallback`
  view state :120 and `FallbackPending` :132, read from the view's own snapshot
  in `migration_state` :304; `fallback_pending` :335; record selections use
  the owner index only when complete :1011.
- `crates/sparrow-core/src/storage/rdf_view/scope.rs`:
  `RdfScopeCoverage.event_fallback_pending` :120, counted (and refused under
  `require_complete_index` with `INDEX_NOT_READY`) in `include_record` :243;
  scoped plan only with a complete index :358; correlation captures do not
  count it :361.
- `crates/sparrow-core/src/storage/rdf_view/query.rs:88`: raw SPARQL
  `index_coverage_complete` is false while any selected graph is pending.
- `crates/sparrow-core/src/lib.rs`: exports `Migration`, `MigrationPage`,
  `MigrationStatus`.
- `crates/sparrow-client/schema/sparrow.capnp`: `Status.migrations @53
  :List(MigrationStatus)` :253, `struct MigrationStatus` :258,
  `SparqlCoverage.eventFallbackPending @14` :794.
- `crates/sparrow-server/src/rpc.rs:2775` and
  `crates/sparrow-server/src/recompression.rs:25` encode status migrations;
  `crates/sparrow-server/src/agent/status.rs:150,346` render CLI/MCP
  `migrations`; `crates/sparrow-server/src/rpc/sparql.rs:532` and
  `crates/sparrow-server/src/agent/sparql.rs:801` carry
  `coverage.event_fallback_pending`.
- `crates/sparrow-server/src/maintenance.rs`: `Job::Migrate` :69, executed via
  `Store::migrate_step` :87; `MigrationJournal` :122 logs
  `sparrowd: migration NAME progress pages=P done=D total=T watermark=W` every
  64 pages (:23) and `sparrowd: migration NAME complete pages=P`. Selection
  :197-198: the first page on an immediate tick, then a `yield_now`
  continuation while pages succeed, fair with the other duties; skipped below
  the free-space floor like the other write duties; a failed page logs
  `sparrowd: online migration failed: ...`, ends the continuation and retries
  on the 60 s tick (:21, :257). A latched durability failure ends the daemon,
  as for every duty.
- `crates/sparrow-test-support/src/lib.rs:179`: `pending_schema_69_migrations`,
  one fixture for the fenced-but-unmigrated state, used by core and server
  tests.
- Docs: `docs/interfaces.md` (query-only paragraph :484-490, fallback and
  owner-index sections now online, new "Online store migrations (schema 69)"
  :565 with status fields, journal lines and the reader table),
  `docs/architecture.md:548`, `docs/deployment.md:288`.

## Reader behavior

| Reader | Migration pending | Migration complete |
| --- | --- | --- |
| Scoped SPARQL (selection, pin, source, receipt-time or diagnostic filter) | Whole-store plan (walk every `rdf:g:` declaration, filtered to the scope's owners): the pre-#13 plan, same rows | Owner index (`rdf:o:`), cost follows the scope |
| Whole-live-store SPARQL | Whole-store plan | Whole-store plan |
| Record metadata graph in a raw SPARQL scope | Record without a source event time, graph beyond the backfill frontier, and no `eventTimeFallback` row: counted in `event_fallback_pending`; answer `complete` false; `require_complete_index` refuses `INDEX_NOT_READY` | 0, no refusal |
| Correlation | Not counted: its recipe reads only `eventTime` and `receivedAt` | Same |
| Writes during the migration | Ingest writes `rdf:o:` rows and fallback facts in the same batch (tested) | Same |
| Catalog event time (intervals, sorting, event clock, `event_time_fallback`) | Derived from the record catalog: `EventTime::placement` (`source_time.rs:266`), used by `text_index.rs:570-633`; the correlation event clock is `COALESCE(eventTime, receivedAt)` (`correlation.rs:222`). Neither reads the fallback triples | Same |

Both migration states are read from the query's own snapshot, so a page that
commits during a query cannot split it between plans.

## Decisions (reversible; flag for Patrick)

1. Wire schema stays 69 (`SCHEMA_VERSION` unchanged). The repository's rule is
   that a schema advances when fields are added, and endpoints check the exact
   schema. Schema 69 has never been deployed (live stays on 68 by the
   2026-11-07 ruling), so no schema 69 endpoint without these fields exists to
   disagree with. If Patrick would rather any field addition after an image
   was built advance the schema, it moves to 70 with no other change.
2. Query-only open: it still applies the fence and seeds (one small
   synchronized batch, like every other open-time catalog upgrade a query-only
   open already applies, documented), but it never runs a page: no maintenance
   worker starts. It serves through the pending read paths, and `status`
   reports the migrations where the copy left them. The verify-backup checks
   (store id, `pressure.phase = ready`, durability failure null, unchanged
   `unique_payload_chunks`, records window, artifact bytes, shutdown line)
   still hold: the fence touches no evidence or `ref:` row. Tested by the
   extended query-only RPC test. The open itself is now about 2.5 s for 50,000
   records instead of the backfill's duration.
3. Strict reads (`require_complete_index`) refuse while selected graphs are
   pending, as they refuse failed content projections. This is the explicit
   reader rule; clients that set it see `INDEX_NOT_READY` until the backfill
   passes their records.
4. `done`/`total` are counted at status time (record graphs by their
   provenance markers, owner index by declarations), so they follow ingest and
   retirement. Status already counts the whole catalog on every call; the two
   counts measured 34 ms at 50,000 records.

## Checks

All release profile, logs in `.stage/bound-the-schema-69-first-open/logs/`.

| Check | Log | Exit |
| --- | --- | --- |
| `cargo fmt --all --check` | `final-fmt.log` | 0 |
| `cargo clippy --release -p sparrow-core -p sparrow-server -p sparrow-test-support -p sparrow-client --all-targets -- -D warnings` | `final-clippy.log` | 0 |
| `cargo test --release --no-fail-fast -p sparrow-core` (lib 35, storage 220 passed) | `final-test-core.log` | 0 |
| `cargo test --release -p sparrow-server --bin sparrowd -- maintenance` (4 passed, incl. the new duty test) | `final-test-server-maint.log` | 0 |
| `cargo test --release -p sparrow-server --test rpc -- query_only_daemon_serves_reads_and_refuses_writes` (1 passed) | `final-test-server-qo.log` | 0 |
| Earlier: full `cargo test --release --no-fail-fast -p sparrow-server` (lib 28, sparrowd 7, rpc 81 passed), before the fallback page bound moved from 256 to 64 | `test-server1.log` | 0 |

Tests (named for behavior):
- `open_serves_before_the_schema_69_migrations_and_direct_migrate_steps_finish_them`
  (core; renamed in review 3): open serves with both migrations pending (done 0 of total, watermark
  `start`); scoped answers equal the whole-live-store answer while pending;
  coverage counts pending graphs and a strict read refuses; an ingest during
  the migration has its facts and index rows at once; one page completes the
  index, a restart resumes with the backfill pending; after the duty finishes,
  answers equal the fully migrated store (as row sets, since rebuilt graphs
  take new dictionary identities) and the one-record scope's request count
  falls, showing the owner-index plan; a staged import published afterwards
  carries the facts; a fenced store without its checkpoint refuses to open.
  It replaces the open-time owner-index migration test.
- `reads_report_pending_mid_migration_and_the_worker_finishes_it`
  (server maintenance; replaced in review 3): a scoped SPARQL read part-way
  through the backfill is served and reports pending equal to total minus
  done; the joined worker finishes both migrations and the read then reports 0.
- `copy_daemons_claim_nothing_query_only_refuses_writes_and_no_intake_migrates`
  (formerly `query_only_daemon_serves_reads_and_refuses_writes`, extended): a query-only
  daemon on a copy with pending migrations serves and reports them pending,
  and runs no page.
- Existing: the scoped parity and cost tests, the catalog pager and
  event-fallback page tests (page-level resume after a committed page and a
  failed page publishing nothing), the owner page unit test, and the legacy
  `/66` fixture test, which now drives the duty with `migrate`.

## Profile (synthetic store; no live data)

A temporary ignored probe test (removed; no debris left) built a store of
50,000 JSON records (50 artifacts of 1,000 lines, 5 sources, no source time
profile, so every record needs the fallback facts), reset it to the pending
state, and timed the open and every page. Host: this workstation, no cgroup
limits, warm page cache. Log: `logs/probe-50k.log`.

Measured:
- Record graph manifests: 50,000, each 716 bytes (13 quads); 100,050 graph
  declarations.
- Open with both migrations pending: 2.45 s.
- `Store::migrations`: 34 ms; full `status`: 280 ms first, then about 430 ms
  per call.
- Owner index: 2 pages, 0.06 s total.
- Event fallback at 256 graphs per page: 196 pages, 64 s total, median page
  0.25 s, slowest page 4.0 s (about 1.3 ms per rebuilt graph). The page bound
  is now 64 graphs, so a median page should hold the writer for about 60 ms
  under these conditions (inferred, not rerun).
- An empty synchronized commit: about 7 ms (50 in 0.34 s).
- The fixture that removes the facts commits one graph per synchronized write
  with its own RDF writer, the pre-step-1 shape: 965 s for 50,000 graphs, about
  19 ms per graph, 15 times the batched cost.

Inferred (not measured; the copy under unit limits was not rerun):
- Warm and unlimited, 151,000 graphs would take about 195 s of pages, in line
  with step 1's two minutes. The live open took 30 to 34 minutes, about ten
  times longer.
- A rebuilt graph costs one dictionary lookup per term on decode
  (`rdf:term:` point reads), one reference-count read per term on removal, a
  reverse-family lookup per term on intern, and six index deletes plus six puts
  per quad. That is roughly 60 to 80 random point reads per graph, so about 10
  million for 151,000 graphs, spread over the RDF tables of the whole store.
- The verify copy is a fresh reflink, whose files have new inodes and so an
  empty page cache, and `MemoryHigh=2G` charges page cache to the unit, so a
  store larger than that cannot stay cached. Under `IOWeight=20` each miss is a
  throttled random read. At 0.2 ms average per read, 10 million reads take
  about 33 minutes, which matches the observation. The 4 s slowest page in the
  warm run points at compaction write stalls from the index churn, which would
  also grow under `IOWeight=20`.
- The admission directory walk per page is small: 24 directory entries here,
  and it no longer runs per graph since step 1. `fsync` costs about 7 ms per
  page, 4 s for 600 pages, so it is not the cause.
- Estimate for the online backfill on the live store under the unit's limits:
  of the order of the 30 minutes observed, now in the background, with each
  64-graph page holding the writer for well under a second at the observed
  rate (34 minutes / 2,360 pages is about 0.9 s per page). That hold time is
  the remaining online cost to watch. Ingest waits for at most one page.

Not done: no run under `CPUQuota=100%`, `IOWeight=20`, `MemoryHigh=2G` and no
cold-cache measurement (no root to drop caches; no nightly copy used), so the
30-minute attribution remains an inference. The acceptance run on a copy of
the nightly (status within 10 minutes, backfill finishing in the background)
is Patrick's or the coordinator's to schedule.

## Not done / open

- Nothing committed or pushed; `make check` not run (by instruction).
- The acceptance measurement on a nightly copy under unit-like limits.
- `with_rdf_records` (test-only helper surface) refuses with `INDEX_NOT_READY`
  while selected graphs are pending, because it is always strict; no
  production caller uses it.

## Review 2 fixes

Source: `reviews/review-2.md` (BLOCKED). Nothing committed; `make check` not
run (by instruction).

Required:
- One helper per check. `storage.rs:1294` `any_key` and `storage.rs:1302`
  `count_prefix` (done and total from an optional frontier) are the single
  implementations. `Writer::count_prefix` (`storage.rs:1136`),
  `catalog_upgrade::seed`, `event_fallback_rdf::counts` and `fence`, and
  `rdf_index::owner_graph_counts` and `fence_owner_graphs` route through them;
  `rdf_index::any_key` is removed. `make census-check` was not run; the code
  is single-sourced by inspection.
- Failing-page test: `migration.rs:217`
  `a_failed_page_is_reported_with_its_watermark_held_until_the_next_good_page_clears_it`
  sends a corrupt graph through `Store::migrate_step` in an open store. The
  status reports the failure with done and watermark unchanged, a retry fails
  on the same page, and after repair the next page clears the failure and
  advances the watermark.
- End to end: `query_only_daemon_serves_reads_and_refuses_writes`
  (`crates/sparrow-server/tests/rpc.rs`, about line 4380) now runs one CLI
  `sparql` call on the pending copy and asserts
  `coverage.event_fallback_pending == "1"` and `coverage.complete == false`
  at the client.
- Contradictions: the capnp `MigrationStatus` comment and the
  `MigrationStatus::watermark` doc say `start` before the first page and
  absent only once complete. `docs/interfaces.md` says pressure maintenance,
  import reclamation and retention are skipped below the free-space floor,
  while buffered synchronization and the table rewrite still run.

Optional:
- Done: the stale "4,096 graphs" text now says 64 graphs per page; the swapped
  doc comments on `complete_test_migrations` and `open_test_db` are restored;
  `Migration::index` replaces the duplicate slot mapping and the dead
  `execute_bounded` arm is gone (`Job::execute` is one match); the journal's
  progress read runs on the blocking pool (`spawn_blocking`).
- Read cost while pending: each pending-candidate record costs one extra
  manifest point read and a subject lookup against the request budget, so a
  large scope can refuse with `RESOURCE_LIMIT` during the backfill and succeed
  after it.
- Test-name overstatement: done in review 3 (see below).
- The full `sparrow-server` suite: rerun in review 3 (see below).

Checks (logs under `logs/`):

| Check | Log | Result |
| --- | --- | --- |
| `cargo fmt --all --check` | `r2fix-fmt.log` | EXIT=0 |
| `cargo clippy --release` on sparrow-core, sparrow-server, sparrow-test-support, sparrow-client, `--all-targets -D warnings` | `r2fix-clippy.log` | EXIT=0 |
| `cargo test --release --no-fail-fast -p sparrow-core` | `r2fix-test-core.log` | lib 36, storage 220 passed; EXIT=0 |
| `cargo test --release -p sparrow-server --bin sparrowd -- maintenance` | `r2fix-test-server-maint.log` | 4 passed; EXIT=0 |
| `cargo test --release -p sparrow-server --test rpc -- query_only_daemon_serves_reads_and_refuses_writes` | `r2fix-test-server-qo.log` | 1 passed; EXIT=0 |

## Review 3: test names, mid-migration read, full server run, migration measurement mode

Nothing committed; `make check` not run (by instruction). The measurement mode
was not run against any backup or store.

- Core test renamed to
  `open_serves_before_the_schema_69_migrations_and_direct_migrate_steps_finish_them`;
  its doc says it drives `Store::migrate_step` one page at a time. It stays
  direct because it needs a restart between exactly one owner-index page and
  the backfill; the worker path is the maintenance test's.
- Maintenance test replaced by
  `reads_report_pending_mid_migration_and_the_worker_finishes_it`: two direct
  pages put the store part-way through the backfill, a scoped SPARQL read is
  served and reports `event_fallback_pending` equal to total minus done, the
  joined worker then finishes both migrations, and the same read reports 0.
- Measurement mode (ruling of about 11:5x CST, `plan.md`):
  `deploy/verify-backup.sh --measure-migration [--image DAEMON] BACKUP`.
  It reuses the verify mode's guards, image selection, lexicon and budget
  derivation, reflink copy and cleanup trap, and `lib.sh` (`die`,
  `config_value`, `require_socket_length`, `match_copy_on_write`, and a new
  `free_bytes`, which `convert_store_no_cow` now also uses). The daemon runs
  under `systemd-run --user --scope` with `CPUQuota=100%`, `IOWeight=20`,
  `MemoryHigh=2G` and `MemoryMax=4G`. The disk guard refuses below 50 GB free
  and aborts below 40 GB; the overrides may only raise those floors. The
  `MEASURE` lines are documented in `docs/deployment.md`, "Measure the online
  migration".
- Daemon seam this needed: `sparrowd --no-intake`. A writable daemon loads
  the store's durable watch catalog (`watch_admin.rs` `Catalog::load`)
  whatever the configuration's `[[consume]]` says, so a scratch config cannot
  keep a copy from consuming the live intake its catalog names. The flag
  starts the maintenance worker but no consume worker; it reports stored
  watches as query-only does, refuses `consume`, `syslog`, `rpc_tcp_address`,
  `ingest_socket_path` and `archive` with the key named, excludes query-only
  mode, and the startup line now ends `intake=...`. The flag is documented in
  `docs/interfaces.md`, and the usage line names it.
- Tests for the seam and the script:
  - `copy_daemons_claim_nothing_query_only_refuses_writes_and_no_intake_migrates`
    (formerly the query-only test, extended): `--no-intake` on the copy
    finishes both migrations with no failure, opens no watch root, claims no
    file, and its SPARQL reports pending 0.
  - `query_only_and_no_intake_refuse_conflicting_configuration` adds the
    no-intake refusals.
  - `verify_backup_migration_measurement_refuses_to_start_below_its_disk_floor`
    is the dry harness test: the start floor refuses, and an override that
    lowers a floor is refused, before any scratch exists.
  - The systemd scope path itself is not exercised by any test.

| Check | Log | Result |
| --- | --- | --- |
| `cargo fmt --all --check` | `r4b-fmt.log` | EXIT=0 |
| `cargo clippy --release` four crates `--all-targets -D warnings` (after the last core change) | `r4-clippy.log` | EXIT=0 |
| `cargo clippy --release -p sparrow-server --all-targets -D warnings` (after the last edit) | `r4b-clippy.log` | EXIT=0 |
| `cargo test --release --no-fail-fast -p sparrow-core` (after the rename) | `r3-test-core.log` | lib 36, storage 220 passed; EXIT=0 |
| `cargo test --release --no-fail-fast -p sparrow-server` (full suite) | `r4b-test-server-full.log` | 28 + 7 + 82 passed; EXIT=0 |
| `bash -n` on `deploy/verify-backup.sh` and `deploy/lib.sh` | `r4-bash-n.log` | EXIT=0 |
| `shellcheck -x` on both scripts | `r4-shellcheck.log` | EXIT=0 |

Earlier runs in this round: `r3b-test-server-full.log` (full suite before the
measurement mode; 28 + 7 + 81 passed, EXIT=0). `r4-test-server-full.log`
EXIT=101 failed on one assertion in the new test (`directories` is a JSON
number); the assertion was fixed, and the run above passes.

## Review 4: review-3 R-1 and every optional item

Nothing committed; `make check` not run. The measurement mode was not run
against any backup or store.

- R-1. The worker-health tick in `main.rs` is now unconditional. Its decision
  is one function, `ended_worker(consumer, syslog, maintenance)`, which covers
  every mode. A no-intake daemon whose maintenance worker ends now returns
  "maintenance worker terminated unexpectedly", and `shutdown::finish`
  propagates it as the exit status.
  - Test: `maintenance::tests::an_ended_maintenance_worker_is_detected_without_intake_workers`
    starts a real `Worker` and checks the no-intake shape (no consume or
    syslog worker). `ended_worker` reports nothing while the worker runs, and
    `Ended::Maintenance` once its loop has ended.
  - The test is in process, not a spawned daemon. Ending the maintenance loop
    in a spawned daemon needs a write or fsync failure, and no
    fault-injection seam exists in the production binary. I did not add one.
    The tested function is the only decision the serving loop makes; the
    `--no-intake` startup itself is covered by the rpc copy test.
- `--measure-migration` changes:
  - It fails on a non-null `durability.failure` (`FAIL durability`).
  - It fails with `FAIL stall` when done/total has not moved for 600 s after
    the first status. Why 600 s: ten of the worker's 60 s retry intervals
    against the live step-1 average of about 0.9 s per 64-graph page (34
    minutes over 2,360 pages; corrected in review 5 from "about a second"),
    and also the switch gate's whole 10-minute budget. This is documented in the script and in
    `docs/deployment.md`.
  - It prints the daemon's `sparrowd: migration ...` and
    `sparrowd: online migration failed: ...` lines as
    `verify-backup: daemon:` before the log is removed, however the run ends.
  - It emits `MEASURE scope=UNIT CPUQuotaPerSecUSec=... IOWeight=...
    MemoryHigh=... MemoryMax=...` from `systemctl --user show` and fails
    unless that line is exactly
    `CPUQuotaPerSecUSec=1s IOWeight=20 MemoryHigh=2147483648 MemoryMax=4294967296`.
    The property format was checked read-only against a nonexistent scope on
    this host. The applied values themselves are unobserved until the first
    real run.
  - An unreadable `df` now has its own message at start and per minute
    (`report_free` returns 2).
- Docs:
  - The O6 read-cost sentence is now under the reader table in
    `docs/interfaces.md`.
  - The migrations paragraph is rewrapped.
  - The no-intake paragraph now states the exit on a maintenance-worker end.
  - The `docs/deployment.md` section lists the scope line and the new failure
    conditions. Correction (review 5): that list omitted `FAIL store-id`,
    `FAIL no-intake`, `FAIL scope`, `FAIL disk`, `FAIL shutdown` and the
    "no readable migrations" case of `FAIL status`; it is now complete.
  - The script's usage text is updated.

| Check | Log | Result |
| --- | --- | --- |
| `bash -n` both scripts | `r5-bash-n.log` | EXIT=0 |
| `shellcheck -x` both scripts | `r5-shellcheck.log` | EXIT=0 |
| `cargo fmt --all --check` | `r5-fmt.log` | EXIT=0 |
| `cargo clippy --release -p sparrow-server --all-targets -D warnings` | `r5-clippy.log` | EXIT=0 |
| `cargo test --release -p sparrow-server --bin sparrowd` | `r5-test-maint.log` | 8 passed; EXIT=0 |
| `cargo test --release -p sparrow-server --test rpc -- copy_daemons_claim_nothing query_only_and_no_intake_refuse verify_backup` | `r5-test-rpc.log` | 5 passed; EXIT=0 |

## Review 5: review-4 optional items

Nothing committed; the full suite was not run (the commit hook runs it).

- `docs/deployment.md` now lists every `FAIL` of `--measure-migration`: open,
  store-id, no-intake, scope, durability, status (three missed answers, or no
  readable migrations), migrate, stall, disk and shutdown. The review-4 claim
  above is corrected.
- The page-time wording: the doc and the script comment now say the live
  step-1 open averaged about 0.9 s per 64-graph page (34 minutes over 2,360
  pages). That figure comes from the live open's total, not from the
  synthetic store; the synthetic probe measured 0.25 s median per 256-graph
  page, warm.
- The read-cost sentence in `docs/interfaces.md` now limits the cost to
  candidates beyond the backfill watermark; records at or before it cost
  nothing extra.
- The serving loop's worker-health check is now `async fn worker_health` in
  `main.rs`, the future the serving loop pins once and selects on
  (`failure = &mut health => return Err(failure)`). It replaces
  `ended_worker` and `Ended`. The test
  `maintenance::tests::the_health_loop_fails_a_no_intake_daemon_whose_maintenance_worker_ends`
  replaces the review-4 test. It drives that function in the no-intake shape:
  it stays pending for five health intervals while a real `Worker` runs, and
  returns "maintenance worker terminated unexpectedly" once the worker's loop
  ends.

| Check | Log | Result |
| --- | --- | --- |
| `cargo fmt --all --check` | `r6-fmt.log` | EXIT=0 |
| `cargo clippy --release -p sparrow-server --all-targets -D warnings` | `r6-clippy.log` | EXIT=0 |
| `cargo test --release -p sparrow-server --bin sparrowd` | `r6-test-bin.log` | 8 passed; EXIT=0 |
| `bash -n` both scripts | `r6-bash-n.log` | EXIT=0 |
| `shellcheck -x` both scripts | `r6-shellcheck.log` | EXIT=0 |
| `prose-check docs/deployment.md docs/interfaces.md` | `r6-prose.log` | EXIT=1; only the 9 `UInt32`/`UInt64` findings, which the base revision of `interfaces.md` also has (9) |
