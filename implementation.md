# Implementation: issue 20, receipt-time fallback for event time

Worktree `/home/paudley/Active/sparrow/.worktrees/20-fall-back-to-receipt-time-marked-as`,
base main 2aa56a9. Nothing is committed. Wire schema 68 -> 69. Stored records are
unchanged. Since review 1, the store format gains the receipt-fallback fence families
(`sparrow.store/132`-`/186`) and an RDF backfill job (R1).

## Work items

1. **Basis on every event-time read result, derived at read time.**
   - `EventTimeBasis { Source, Receipt }`, `EventPlacement { millis, basis }` and
     `EventTime::placement(received_at)`:
     `crates/sparrow-core/src/storage/source_time.rs:212`, `:232`, `:256`. This is the one
     place the law is implemented. The stored `EventTime::Unknown { reason, profile_id }` is
     unchanged.
   - Exported from `crates/sparrow-core/src/lib.rs:99`.
   - Ledger row `event-time-receipt-fallback`: `helpers-ledger.toml:89`.
2. **Intervals, sorting, correlation and the fallback count.**
   - Records reads (`text_index.rs`):
     - The interval test uses the placement: `crates/sparrow-core/src/storage/text_index.rs:570`.
     - Disk-stage order and in-memory chronological order use it too: `:604` and `:633`.
     - The fallback counter is at `:510` and `:592`. It counts qualified records placed by
       receipt time and is 0 for a receipt-time read.
     - It is emitted as top-level `event_time_fallback` at `:738` (chronological) and
       `:928` (text-ranked).
   - Correlation:
     - The event clock gets its fallback at read time inside the SPARQL recipe:
       `OPTIONAL eventTime`, then `COALESCE(source, receivedAt)` and
       `IF(BOUND, "source", "receipt")`, at `crates/sparrow-core/src/storage/correlation.rs:217`.
     - It projects `?seed_window_basis ?related_window_basis ?selected_basis`.
     - The profile is now `sparrow-correlation/2` (`:15`).
     - Because every record now has a placement, event time no longer makes `complete()`
       false (`:93`). The strict refusal message in
       `crates/sparrow-core/src/storage/rdf_view/query.rs:208` now names only diagnostic
       interpretations.
   - Graph replies:
     - Agent `interpretations.event_time.fallback` is set at
       `crates/sparrow-server/src/agent/sparql.rs:760`.
     - It equals the unknown count, because every unknown is placed by receipt time.
3. **Record output with basis and reason.**
   - Wire:
     - `UnknownEventTime.fallback @2 :Instant` (`crates/sparrow-client/schema/sparrow.capnp:195`).
     - `RecordsResults.eventTimeFallback @10` (`:567`).
     - `SCHEMA_VERSION = 69` (`crates/sparrow-client/src/lib.rs:69`).
   - RPC:
     - `event_time(out, value, received_at)` (`crates/sparrow-server/src/rpc.rs:1090`) sets
       the fallback instant at `:1113`.
     - It is used by `record`/context and by `RecordMatch` rows.
     - The records count is set at `:491`.
   - Agent (CLI and MCP):
     - `event_time.basis` is `source`, or `receipt` plus `fallback_instant` next to the
       original `state`/`reason`/`profile_id`: `crates/sparrow-server/src/agent/source_time.rs:191`.
       There is one renderer, shared by `records`, `record` and `context`.
     - Top-level `event_time_fallback` is at `crates/sparrow-server/src/agent/records.rs:269`.
4. **RDF.**
   - `eventTime` is written only for a Known source time. An Unknown source time gets
     `eventTimeFallback` (a typed `xsd:dateTime`) and `eventTimeBasis "receipt"`:
     `crates/sparrow-core/src/storage/rdf_index.rs:1009`.
5. **Docs.**
   - `docs/interfaces.md`:
     - New section "Receipt-time event fallback (schema 69)" at `:500`.
     - Evidence records (`:553`) and temporal reads (`:636`), which replace the old
       "never substitutes receipt time" text.
     - Log-time section (`:677`).
     - Correlation, which replaces the old "never use receipt time as a fallback" text
       (`:1308`, `:1325`), and interpretations (`:1335`).
     - RDF vocabulary table (`:1564`).
   - `docs/architecture.md:105` and `:503`.
   - Also `docs/implementation-plan.md:222`, which stated the old contract.

## Named tests

All in `crates/sparrow-core/tests/storage.rs`, using a shared fixture: a plain log with no
profile received at 5000, and a JSON record with source time 1000 received at 5000.

- `event_interval_includes_receipt_fallback` (`:13575`).
- `source_event_time_is_never_marked_fallback` (`:13619`).
- `correlation_event_clock_uses_fallback_marked` (`:13649`). It runs in strict mode and
  checks window times and the `source`/`receipt` labels.
- `sparql_distinguishes_fallback_time` (`:13702`).

## Existing tests updated to the new contract (none deleted)

- `native_log_time_profiles_preserve_byte_ranges_event_order_and_immutable_history`
  (`storage.rs:149`). Strict event-clock correlation now succeeds with 1 row; it used to
  return `INDEX_NOT_READY`.
- Renamed `event_intervals_exclude_unknowns_...` to
  `event_intervals_place_unknowns_at_receipt_sort_source_time_and_keep_retention_on_acceptance`
  (`storage.rs:13397`). `outside_interval` goes from 1 to 8, and it now asserts
  `event_time_fallback` is 0.
- `crates/sparrow-server/tests/rpc.rs` (`mcp_exercise`):
  - `outside_interval` goes from 1 to 3 (`:9318`).
  - Asserts the source row has `basis = source`.
  - New check at `:9338`: an interval around the current time returns the two unknown
    records with `basis = receipt`, `fallback_instant = received_at`, `instant` absent, the
    reason kept, and `event_time_fallback = 2`.

## Checks (all release profile; logs under `.stage/fall-back-to-receipt-time-marked-as/logs/`)

| Check | Log | Exit |
| --- | --- | --- |
| `cargo build --release -p sparrow-server --lib` | `build-server-lib.log` | 0 |
| `cargo test --release -p sparrow-core --test storage -- fallback event_interval native_correlation native_log_time source_time_profiles event_intervals_place` (9 passed) | `core-storage-focused.log` | 0 |
| `cargo test --release -p sparrow-server --test rpc -- log_timestamp_profiles_share... remote_agent_adapters_share... correlation_cli_mcp... typed_queries_and_exact_bytes_survive_daemon_crash` (4 passed) | `server-rpc-focused.log` | 0 |
| `cargo clippy --release -p sparrow-core -p sparrow-client -p sparrow-server --lib --tests -- -D warnings` | `clippy.log` | 0 |
| `cargo fmt -p sparrow-core -p sparrow-server -p sparrow-client --check` (after applying fmt) | `fmt-check.log` (`fmt-check-1.log` is the pre-fmt run) | 0 |

Not run, as instructed: `make check`, workspace test or clippy, the census gates and the
full test files. `make check`, which the pre-commit hook runs, is still required before
commit.

## Deviations and decisions

- **RDF fallback triples are written at index time, not derived at read time.**
  - SPARQL reads go directly over the physical RocksDB quad indexes (`rdf_view.rs`). There
    is no seam for adding virtual quads at read time.
  - So new graphs get the triples at publication. Graphs published before schema 69 get
    them from the backfill (R1 below). Stored records do not change.
  - Correlation does not depend on the stored triples. It derives the fallback from
    `receivedAt` inside the query.
- **Wire shape.**
  - Basis is implied by the union variant: `known` is source, `unknown` is receipt. The
    variant carries the fallback instant, so a redundant basis field cannot contradict it.
  - Agent JSON renders `basis` explicitly.
  - The fallback instant uses the key `fallback_instant`, never `instant`. This mirrors the
    separate RDF predicate, so a naive reader of `event_time.instant` cannot mistake a
    receipt time for a source time.
- **`event_time_fallback` placement and meaning.**
  - It is top-level on records results, beside `known_qualified_records`, as the UAT
    expects (`jq .event_time_fallback`).
  - It counts qualified (matched) records placed by receipt time.
  - Graph and correlation replies report `interpretations.event_time.fallback` instead.
- **`eventTimeBasis`** is emitted only for fallbacks (`"receipt"`), as the issue specifies.
  Known records carry `eventTime` without a basis triple.

## Review 1 corrections

### R1: backfill of pre-69 metadata graphs (decided in plan.md)

- Fence: new writer-format families in `crates/sparrow-core/src/storage/writer_format.rs`:
  - `FALLBACK_TRACING` `/132`-`/142` (`:139`), with `FALLBACK_LOG_TIME`, `FALLBACK_ARCHIVE`,
    `FALLBACK_INTAKE` and `FALLBACK_INTAKE_ARCHIVE` following, up to `/186`.
  - Each is the tracing-or-later family shifted by +55 (`FALLBACK` at `:207`).
  - `lower` (`:228`) and `relift` (`:233`) keep the existing archive, intake and log-time
    switches working on fenced identities. `has_event_fallback` and `enable_event_fallback`
    are at `:240` and `:245`; the latter refuses pre-tracing formats.
  - `stage()` relifts the staged identity, so later authority transitions keep the fence.
  - Older binaries refuse `/132` and above as unknown identities, as they did for earlier
    families.
- Job: new module `crates/sparrow-core/src/storage/event_fallback_rdf.rs`.
  - It follows the `provenance_rdf.rs`/`tracing_rdf.rs` precedent: one `catalog_upgrade::Job`
    (`:13`), with checkpoint `event-fallback-rdf-upgrade`, over `rdf:graph:record:`, using
    `artifact_rdf::admit` page admission.
  - The fence is written before the job runs.
  - `publish` (`:52`) reads only the graph. It was reworked under R4 (see "Review 2
    corrections").
  - It is called from Store open after `tracing_rdf::upgrade`
    (`crates/sparrow-core/src/storage.rs:1423`).
- Shared helpers in `crates/sparrow-core/src/storage/rdf_index.rs`:
  - `stored_metadata` (`:701`) is extracted from `provenance_rdf::publish`, which now uses
    it, so there is one stored-graph loader.
  - `event_time_metadata` (`:784`) writes the event-time triples at publication. Its
    receipt branch and the backfill share `fallback_metadata` (`:806`).
- Test-only: `remove_event_fallback_rdf_for_legacy_fixture` (`event_fallback_rdf.rs:111`,
  behind `test-support`) produces pre-69 graphs. `sparrow-test-support`'s
  `remove_artifact_rdf_upgrade` also deletes the new checkpoint.
- Ledger: job `event-fallback-rdf-backfill` (`helpers-ledger.toml:145`).
- Docs:
  - `docs/interfaces.md` (`:526`) replaces "until rebuilt" with the backfill and its fence.
  - `docs/archive-format.md:259` lists the fallback archive identities.
- Test: extended
  `tracing_relationships_keep_instance_and_occurrence_evidence_through_backfill_and_retirement`
  (`crates/sparrow-core/tests/storage.rs:379`). A plain-log record with Unknown time has its
  fallback triples removed (the manifest loses 2 rows). After reopen:
  - the store format is `/132`;
  - SPARQL finds `eventTimeFallback` `1970-01-01T00:00:00.003Z` with basis `receipt`;
  - it finds no `eventTime`.
- Existing `store_format` assertions shifted by +55 for fenced stores, in `storage.rs` and
  `crates/sparrow-server/tests/rpc.rs`. This includes `archive_fence_refuses_...`, whose
  range is now `154..=164` and whose unknown identity is now `/187`.

### R2: strict correlation refuses failed event-time projections

- `RdfScopeCoverage.event_time_projection_failed` counts `projection_failed` event times
  (`crates/sparrow-core/src/storage/rdf_view/scope.rs:113`, `:224`).
- `complete()` requires it to be 0 whenever a Time selector or Time relation uses the event
  clock (`crates/sparrow-core/src/storage/correlation.rs:105`).
- The refusal message names ready event-time projections again
  (`crates/sparrow-core/src/storage/rdf_view/query.rs:208`).
- Docs: the projection_failed refusal sentences are restored in the `docs/interfaces.md`
  log-time and correlation sections.
- Test: extended `native_log_time_profiles_preserve_byte_ranges_event_order_and_immutable_history`
  (`storage.rs:162`-`:177`). A `log-clock-json` record whose projection failed:
  - strict event-clock correlation returns `INDEX_NOT_READY`;
  - strict received-clock correlation succeeds.

### R3: wire and JSON shapes described separately

- The `docs/interfaces.md` schema 69 section (`:500`-`:520`) now describes two shapes:
  - Cap'n Proto: the `EventTime` union variant is the basis, `UnknownEventTime.fallback` is
    the instant, and `RecordsResults.eventTimeFallback` is the count.
  - CLI/MCP JSON: `event_time.basis`, `fallback_instant`, and top-level
    `event_time_fallback`.

### O4

- The `docs/development-runtime.md:6311` paragraph no longer says unknown-time records are
  excluded. It describes schema 69 receipt placement.

### O1: not taken

- The core `records_query` rows have no basis field. Asserting the basis through that read
  result would mean changing the row shape, which is outside this fix.
- The basis is already asserted through real read surfaces:
  - `sparql_distinguishes_fallback_time`, over published RDF;
  - the RPC `mcp_exercise` CLI check, over agent JSON.
- The named test keeps `placement()` together with read-result checks (unknown state,
  `event_time_fallback = 1`).

### Review-1 checks (release; logs under `.stage/fall-back-to-receipt-time-marked-as/logs/`)

| Check | Log | Exit |
| --- | --- | --- |
| `cargo build --release -p sparrow-core --features test-support --lib` | `r1-core-build.log` | 0 (after fixing one `mut`; earlier runs failed to compile and were fixed) |
| `cargo test --release -p sparrow-core --lib -- writer_format rdf_index tracing_rdf recordless catalog_upgrade provenance` (11 passed) | `r1-core-lib.log` | 0 |
| `cargo test --release -p sparrow-core --test storage -- fallback event_interval native_correlation native_log_time source_time_profiles event_intervals_place tracing_relationships upgrade legacy fence archive_fence ... syslog_format` (30 passed, 1 failed: `archive_fence_...` stale range, then fixed) | `r1-core-storage.log` | 101 |
| `cargo test --release -p sparrow-core --test storage -- archive_fence` (1 passed, after fix) | `r1-core-archive-fence.log` | 0 |
| `cargo test --release -p sparrow-server --test rpc -- log_timestamp_profiles... remote_agent_adapters... correlation_cli_mcp... typed_queries_and_exact_bytes... configured_inotify_consumption... explicit_handoff_consumes... typed_tail_handoff... typed_watch_administration... fallback event_time` (8 passed) | `r1-server-rpc.log` | 0 |
| `cargo fmt -p sparrow-core -p sparrow-client -p sparrow-server -p sparrow-test-support --check` | `r1-fmt-check.log` | 0 |
| `cargo clippy --release -p sparrow-core -p sparrow-client -p sparrow-server -p sparrow-test-support --lib --tests -- -D warnings` | `r1-clippy.log` | 0 (a first run failed on `cloned_ref_to_slice_refs` in the R1 test, which is now fixed) |
| `cargo test --release -p sparrow-core --test storage -- tracing_relationships` (1 passed, after the clippy fix to that test) | `r1-core-tracing.log` | 0 |

## Review 2 corrections

### R4: backfill covers privately prepared record graphs

- Problem: the backfill required a public catalog row for every `rdf:graph:record:*` graph.
  - A staged import writes each record graph before publication, and that preparation
    survives a restart.
  - An unfinished import therefore failed the first schema 69 open.
- Fix: follow the provenance backfill precedent and derive the facts from the graph alone.
  `event_fallback_rdf::publish` (`crates/sparrow-core/src/storage/event_fallback_rdf.rs:52`)
  no longer reads the catalog:
  - A graph with `eventTime` has a source instant, and is skipped.
    - Pre-69 writers emitted `eventTime` exactly for Known source times (checked on main:
      `metadata()` wrote it only in the `EventTime::Known` branch).
  - A graph with `eventTimeFallback` is already current, and is skipped.
  - Otherwise the subject's single `receivedAt` `xsd:dateTime` is parsed with
    `crate::parse_instant`.
    - The value is placed through `EventPlacement::receipt`
      (`crates/sparrow-core/src/storage/source_time.rs:241`). `EventTime::placement` now uses
      the same constructor for unknown times.
    - The triples are written by `rdf_index::fallback_metadata`
      (`crates/sparrow-core/src/storage/rdf_index.rs:806`), which `event_time_metadata`
      also uses for its receipt branch.
  - A missing, untyped or repeated `receivedAt` fails with `STORAGE_FAILURE`.
- `rdf_index::date_time` (`:827`) is now the single builder of `xsd:dateTime` literals for
  `receivedAt`, `expiresAt`, `eventTime` and `eventTimeFallback`.
- Test: extended
  `tracing_relationships_keep_instance_and_occurrence_evidence_through_backfill_and_retirement`
  (`crates/sparrow-core/tests/storage.rs:449` onward):
  - A staged `log` import is prepared at 4 and not published. Its graph is the only record
    graph without a public row.
  - The legacy fixture strips 2 rows from it.
  - After reopen and the backfill, `import_publish` succeeds. The published record has
    `eventTimeFallback` `1970-01-01T00:00:00.004Z` with basis `receipt` and no `eventTime`.
- Ledger and docs:
  - `helpers-ledger.toml`: the backfill job now names `fallback_metadata`. The
    receipt-fallback job adds `fallback_metadata` and `EventPlacement::receipt`.
  - The `docs/interfaces.md` backfill paragraph states that the job reads only the graph and
    covers prepared graphs.

### O5

- `docs/interfaces.md:486`: the schema 62 store keeps `/92` only under schema 68. A schema
  69 open, query-only included, lifts it to `/147` and runs the backfill.
- `docs/interfaces.md:716`: on a schema 69 store, the first log definition moves 132..142 to
  143..153.

### O6

- `deploy/RECOVERY.md` "Refused open" lists
  `STORAGE_FAILURE: log time interpretation law and writer fence disagree`. Under
  "Recover", it explains that this is the older-image case on a log time store, recovered
  the same way.

### O7

- Skipped, as instructed.

### Review-2 checks (release; logs under `.stage/fall-back-to-receipt-time-marked-as/logs/`)

| Check | Log | Exit |
| --- | --- | --- |
| `cargo test --release -p sparrow-core --test storage -- tracing_relationships fallback event_interval native_log_time prepared_import staged_diagnostics recordless archive_fence` (21 passed) | `r4-core-storage.log` | 0 |
| `cargo test --release -p sparrow-core --lib -- rdf_index writer_format provenance catalog_upgrade source_time` (6 passed) | `r4-core-lib.log` | 0 |
| `cargo test --release -p sparrow-server --test rpc -- log_timestamp_profiles... remote_agent_adapters... correlation_cli_mcp...` (3 passed) | `r4-server-rpc.log` | 0 |
| `cargo fmt -p sparrow-core -p sparrow-client -p sparrow-server -p sparrow-test-support --check` | `r4-fmt-check.log` | 0 |
| `cargo clippy --release -p sparrow-core -p sparrow-client -p sparrow-server -p sparrow-test-support --lib --tests -- -D warnings` | `r4-clippy.log` | 0 |

## Not done

- `make check`, full test files, census gates and the commit, all deliberately left for the
  caller. The pre-commit hook runs `make check`.
- UAT on the live service, which needs a deploy.
