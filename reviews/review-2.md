# Delivery review 2 (recheck): issue #20, receipt-time fallback for event time

## Scope

- This recheck covers only the work changed since `review-1.md`:
  - R1: the RDF backfill, the writer-format fence and the shared RDF helpers.
  - R2: strict correlation over `projection_failed` source times.
  - R3: the wire and JSON shapes in the docs.
  - O4: `docs/development-runtime.md`.
  - The stated reason for skipping O1.
- Work tree: uncommitted, base `main` 2aa56a9. 25 files are modified, and
  `crates/sparrow-core/src/storage/event_fallback_rdf.rs` is new.
- `plan.md ## Decisions` now records the backfill decision, with its reasoning. That
  resolves the recording conflict noted in review 1.
- `## User rulings` is unchanged; its rulings still cover only queue order.

## Evidence

Execution evidence (read from logs; I did not rerun anything):

- `logs/r1-core-lib.log`: 11 core unit tests passed, exit 0. They include
  `writer_format::tests::every_authority_transition_preserves_the_artifact_writer_fence`,
  `tracing_fence_...` and `provenance_upgrade_covers_private_owners_...`.
- `logs/r1-core-storage.log`: 30 storage tests passed and 1 failed,
  `archive_fence_...` (stale range).
  - The fixed test passed alone in `logs/r1-core-archive-fence.log`.
  - The tracing test was edited for Clippy at 20:50 and passed again in
    `logs/r1-core-tracing.log`.
- `logs/r1-server-rpc.log`: 8 tests passed.
- `logs/r1-clippy.log` and `logs/r1-fmt-check.log`: both exit 0.
- Freshness:
  - The last production source edit is `rdf_index.rs` at 20:07:56.
  - The storage, lib and rpc logs (20:19 to 20:49) are newer than that.
  - Only `tests/storage.rs` changed afterwards, and its affected test was rerun.
- Not run: `make check`, the full storage and rpc test files, and the census gates.

Source checks (read-only):

- **There is no renumbering.** Identities `/33` to `/131` keep their numbers.
  - `writer_format.rs` appends five new families, `/132` to `/186`.
  - Each new identity is its lifted family's identity plus 55.
  - Test literals moved by +55 only because every store opened by the schema 69 binary
    is fenced at open.
- **Fence consistency:**
  - `families()` and `read()` chain `FALLBACK`, so `index % 11` still yields the base
    authority.
  - Every `has_*` predicate lowers first, and `has_provenance`, `has_all_artifact_rdf`
    and `has_artifact_rdf` lower through `has_tracing`.
  - `enable_log_time`, `enable_archive`, `enable_intake` and `stage()` relift.
  - `enable_tracing` and `enable_provenance` are only reached on unfenced formats:
    `tracing_rdf::upgrade` skips a store that already has tracing, and `stage()` passes
    a base identity.
  - `enable_event_fallback` refuses pre-tracing formats. That is safe because
    `tracing_rdf::upgrade` runs immediately before it in `Store::open`
    (`storage.rs:1422-1423`).
- **Callers:**
  - Every production caller uses the `writer_format::` predicates and transitions:
    `source_time.rs:20/458/535`, `intake.rs:88/95`, `archive.rs:237/255`,
    `durability.rs`, `consumption.rs`, `handoff.rs`, `watch_registry.rs`,
    `pressure.rs`, `manifest_retire.rs`, `receiver_stream.rs` and `diagnostic.rs`.
  - Hard-coded identities elsewhere are base or legacy identities (`/5`-`/43`), which
    are compared after `read()` lowers to the base. None compares a tracing-onward
    literal.
- **Test literals:**
  - A repo-wide search for tracing-onward literals (`/77`-`/131`), excluding the
    writer-format table, history and evidence files, finds two left.
  - Both are deliberate `/77` assertions where the tracing upgrade runs and the fallback
    upgrade has not: `tracing_rdf.rs:415`, and `storage.rs:562`, where open fails before
    the fallback job.
  - The computed offsets are all +55: `legacy + 107`, `current + 99`, `version + 123`.
  - The rpc test literals are `/134`, `/135`, `/136` and `/145`.
  - The changed archive test now checks `154..=164`, `/176`-`/186` is documented, and
    `/187` is unknown. The added assertion
    `number(fenced) - 154 == (number(unfenced) - 33) % 11` keeps the base-authority
    check.
- **`docs/archive-format.md:259`** names `/154`-`/164` and `/176`-`/186` correctly.
- **Older-writer refusal:**
  - A schema-68 `read()` does not know `/132`+, so the format check refuses it.
  - The writer-format unit test asserts that no fallback identity appears in any
    earlier family.
- **Shared helpers:**
  - `rdf_index::stored_metadata` is a straight extraction of the former
    `provenance_rdf::publish` body: the same manifest, ownership, dictionary, closure
    and admission checks. Its primary quads are now pushed after verification instead
    of inside the field loop, which gives the same result.
  - `provenance_rdf::publish` passes the same `PAGE_BYTES - charge` bound.
  - `rdf_index::event_time_metadata` is the single writer of the event-time triples,
    for both publication (`metadata`) and the backfill.
- **R2:**
  - `RdfScopeCoverage.event_time_projection_failed` is counted in `include_record`
    (`scope.rs:221-227`).
  - `complete()` requires it to be 0 when the selector or the relation uses the event
    clock (`correlation.rs:102-117`).
  - The test extension (`storage.rs:158-205`) ingests invalid JSON under a time profile
    and asserts the reason is `projection_failed`. Strict event-clock correlation then
    returns `INDEX_NOT_READY`, and the strict received clock succeeds.
  - The docs restore the refusal sentences.
- **R3:** `docs/interfaces.md:509-520` now describes the Cap'n Proto shape (union
  variant, `UnknownEventTime.fallback`, `RecordsResults.eventTimeFallback`) separately
  from the CLI/MCP JSON shape.
- **O4:** the development-runtime paragraph now states the schema 69 placement.

## Findings

### REQUIRED

**R4. The backfill fails store open whenever a privately prepared record graph exists.**

- What the code does:
  - `event_fallback_rdf::publish` iterates every `rdf:graph:record:*` owner.
  - It requires a public catalog row:
    `db.get_pinned(id)?.ok_or_else(|| failure("record metadata graph has no catalog record"))`
    (`event_fallback_rdf.rs:60-62`).
- Why that breaks:
  - Staged-import preparation writes each prepared record's metadata graph before
    publication. `publication.rs:493-517` requires `rdf:graph:<record.id>` to exist and
    the public row `db.get(&record.id)` to be absent. The record itself lives under
    `record_key(id, index)`.
  - Private preparation is durable across restarts.
  - The repository already knows these owners exist: the provenance backfill handles
    them and tests them
    (`rdf_index.rs:1174-1226`, `provenance_upgrade_covers_private_owners_...`,
    "private graph has no public catalog row").
- Consequence:
  - A store stopped with an unfinished or abandoned staged import fails its first
    schema 69 open with `STORAGE_FAILURE`, and the daemon cannot start.
  - Skipping such graphs is not a fix either. Publication reuses the prepared graph
    (`publication.rs:513`), so those records would go live without the fallback facts.
- The R1 test covers published records only.
- Correction:
  - Make the backfill cover private owners. Either resolve the private record through
    its publication progress, or derive the facts from the graph itself: the absence of
    `eventTime`, plus the graph's `receivedAt` routed through the shared placement law.
  - Add a case with a prepared, unpublished record to an existing upgrade test, after
    the model of the provenance private-owner test.

### OPTIONAL

- **O5.** Two docs statements are stale under schema 69:
  - `docs/interfaces.md:712` gives the log-time fence as 88..98. A schema 69 store's
    first log definition moves `/132+k` to `/143+k`. The schema 69 section states the
    general lift, so a one-line pointer would do.
  - `docs/interfaces.md:485` ("Opening a store last written by the schema 62 image left
    its format, `sparrow.store/92`, unchanged") was true when written. A schema 69 open,
    including a query-only open by `verify-backup.sh`, now moves that store to `/147`
    and runs the backfill.
- **O6.** An older binary on a log-time store refuses with "log time interpretation law
  and writer fence disagree", because its `validate_log_fence` does not know `/143`+.
  - It does not refuse with the `UNSUPPORTED` format message that `deploy/RECOVERY.md`
    "Refused open" lists.
  - The archive and intake families already behave this way, and the archive test
    deletes the law to reach the format refusal. So this is pre-existing, but the live
    store is a log-time store.
  - A RECOVERY.md line naming this message as the "image older than the store" case
    would help a rollback operator.
- **O7.** The idempotent branch (graph already has `eventTimeFallback`) and the refusal
  branch (stored `eventTime` on an unknown-time record) have no test.
  - Both are defensive. Per-step atomicity makes the first unreachable after a crash.
  - No test is needed for either, but O7 records that only the main path is exercised.

### Review-1 findings

- **R1:** decision recorded, and the backfill and fence are consistent across every
  checked caller, test literal and doc range. It is still open on R4 for private
  owners.
- **R2:** resolved.
- **R3:** resolved.
- **O1 (skipped):** the reason is valid.
  - Core `records_query` rows carry no basis by design; the basis is added at the
    RPC/agent boundary.
  - The basis is protected through the surfaces that carry it: the RPC `mcp_exercise`
    CLI rows, and `sparql_distinguishes_fallback_time` over published RDF.
  - Adding a basis to core rows only to test it would widen scope.
- **O4:** done.

### Is the fence justified?

- Yes. An older binary only refuses a store through an identity it does not know.
  - A marker key would be ignored by a schema-68 binary.
  - Re-scanning every graph at each open would cost a full pass per start.
  - An image-level guard (`restart.sh` refusing a lower schema) is not a store-level
    invariant.
- The five lifted families follow the existing pattern of one family per switch
  combination: archive, intake and intake+archive.
- No smaller store-level change would keep an older binary from publishing graphs that
  lack the fallback facts.

## Open state (not findings)

- `make check` is not yet run; the commit hook runs it.
- UAT needs a deploy.
- Nothing is committed.

VERDICT: BLOCKED
