# Delivery review 1: issue #20, receipt-time fallback for event time

## Scope

- Worktree `/home/paudley/Active/sparrow/.worktrees/20-fall-back-to-receipt-time-marked-as`,
  branch `paudley/20-fall-back-to-receipt-time-marked-as`, base `main` 2aa56a9 (schema 68).
- Reviewed the uncommitted working-tree `git diff`: 18 files, no untracked files.
- Read first: `issue.md` (verbatim issue, no comments). Then `plan.md`, including
  `## User rulings` and `## Decisions`. Then `implementation.md` and `logs/`.
- The rulings only set queue order (#20 before #13 and #14). No ruling covers scope, RDF
  backfill or strict-mode semantics.
- Checked against Work items 1 to 5, the four named tests, the production callers, the
  failure paths, the schema-version rules, docs accuracy and test updates.

## Evidence

Execution evidence (read from logs; I did not rerun anything):

- `logs/core-storage-focused.log`: `cargo test --release -p sparrow-core --test storage`
  with filters.
  - Exit 0: 9 passed, 207 filtered out.
  - Covers the four named tests plus five existing log-time, source-time and correlation
    tests.
  - The log (19:13) is newer than every edited source file (latest 19:00:06).
- `logs/server-rpc-focused.log`: 4 rpc tests passed, 77 filtered out. These include
  `remote_agent_adapters_share_all_query_and_mutation_paths...`, which runs `mcp_exercise`
  with the new CLI fallback assertions.
- `logs/clippy.log`: release Clippy over the three crates (`--lib --tests -D warnings`),
  exit 0.
- `logs/fmt-check.log`: empty after fmt.
- `logs/build-server-lib.log`: release build, exit 0.
- Not run: `make check` (issue acceptance item 5), the census gates, the full storage and
  rpc test files, and UAT.
  - Most fixture records have unknown source times, and each now carries two extra
    metadata triples.
  - I found no remaining test that asserts exact metadata row or quad counts:
    `metadata_primary_rows` is only compared CLI against MCP, and `primary_rows` covers
    content graphs.
  - So the commit hook's `make check` remains the gate for the unrun cases.

Source checks (read-only):

- Single law: `EventTime::placement` (`source_time.rs:256`).
  - Used by records interval, disk-stage order and in-memory order (`text_index.rs:570`,
    `:604`, `:633`).
  - Fallback counter at `:588`, emitted at `:738` and `:928`.
  - Also used by RPC rendering (`rpc.rs:1113`) and RDF metadata (`rdf_index.rs:994`).
  - Correlation restates the law in SPARQL with `COALESCE(eventTime, receivedAt)`
    (`correlation.rs:218`), as the ledger row records.
- Wiring:
  - `records_page` sets `eventTimeFallback` (`rpc.rs:491`), and the agent emits it
    top-level (`agent/records.rs:269`).
  - `record_get` and `context` render through `context::render_record`, which calls
    `source_time::event`.
  - RPC `record()` passes `received_at` (`rpc.rs:1030`).
  - Captured continuation pages reuse the stored metadata object
    (`record_read.rs:349`), so `event_time_fallback` survives `result_next`.
- Missed consumers:
  - `text_index` keeps `event_coverage.unknown` and adds `event_time_fallback` beside it.
  - `record_fields.rs:237` still emits the stored shape, and basis is added at the
    RPC/agent boundary from the same `received_at`. Every external record surface
    (CLI, MCP, RPC records, `record`, `context`) carries the basis.
  - `--event-from/--event-before` is the only event-interval entry point.
  - I found no other event-time ordering consumer.
- Schema:
  - `SCHEMA_VERSION` is 69, and the two capnp fields are added at new ordinals (`@2`,
    `@10`).
  - The docs section follows the existing "Schema N adds ..." pattern with exact
    discovery refusal.
  - `CORRELATION_PROFILE` moved to `sparrow-correlation/2`, and `correlation_wire.rs`
    checks it on descriptor decode.
- Tests:
  - The two existing tests were updated to the new contract, not deleted:
    - `native_log_time_profiles...`: `INDEX_NOT_READY` becomes 1 row.
    - `event_intervals_place_unknowns...`: `outside_interval` goes from 1 to 8.
  - `mcp_exercise` adds a CLI fallback interval check: basis `receipt`,
    `fallback_instant == received_at`, no `instant`, reason kept, count 2.
- `prose-check` on the added doc/schema/ledger lines found one false positive: the capnp
  type `:UInt64` on a code line. Its other hits are on unchanged lines of
  `docs/interfaces.md`.

## Findings

### REQUIRED

**R1. RDF fallback triples are missing for every record published before schema 69. This
is an unratified scope cut to item 4, and the docs misstate how long the gap lasts.**

- Item 4 requires `sparrow:eventTimeBasis "receipt"` and `sparrow:eventTimeFallback` for
  fallback records. The implementation writes them only when a metadata graph is built
  (`rdf_index.rs:1009`).
- Is the decision consistent with the issue?
  - "No stored record changes, so no migration is needed" (item 1) explains why read-time
    derivation needs no migration. It does not forbid one.
  - The out-of-scope item "Changing stored records" covers records. Metadata graphs are
    derived index rows.
  - The repository already backfills new RDF vocabulary over retained records through
    `catalog_upgrade` jobs (`provenance_rdf.rs`, `tracing_rdf.rs`). So a backfill is not
    excluded by the issue.
- Does item 4's guarantee hold? Its non-confusion half does: Unknown event times never
  wrote `eventTime`, so an old graph cannot present a fallback as a source time.
- What is the gap?
  - On the live store, a SPARQL query over `eventTimeFallback` silently omits every
    pre-69 fallback record. No coverage signal tells the reader.
  - The gap is not transient for pinned records, which can outlive the 30-day retention.
  - `docs/interfaces.md:518-519` says those graphs "lack them until rebuilt". No operator
    or automatic rebuild path exists for ordinary record metadata: the tracing and
    provenance upgrades are one-time format jobs.
- The plan and the report disagree:
  - `implementation.md` calls this "a decision for Patrick" and lists it under "Not done".
  - `plan.md ## Decisions` records it as already decided, with no ruling behind it.
- Correction, either of:
  - (a) Obtain Patrick's (or `lillith_coordination`'s) explicit acceptance of the gap,
    record it verbatim under `## User rulings`, and correct the doc to say pre-69 graphs
    lack the triples for their remaining retention, indefinitely while pinned.
  - (b) Backfill through the existing `catalog_upgrade` mechanism.

**R2. Strict event-clock correlation no longer refuses records whose source-time profile
depends on a failed JSON projection. Strict records reads still refuse them.**

- In a records read, strict mode with an event interval still returns `INDEX_NOT_READY`
  for an Unknown event time with reason `projection_failed`: "selected source-time
  profile depends on a failed JSON projection" (`text_index.rs:561-568`). That is an
  index-readiness failure, not a producer without a timestamp.
- Before this change, strict correlation refused any unknown event time. That set
  included `projection_failed`.
- The change removes the event clock from `CorrelationCapture::complete`
  (`correlation.rs:93-111`) wholesale.
- `RdfScopeCoverage::complete()` does not catch this case either. Correlation selects
  `content: false`, so `include_record` returns before it counts `content_failed`
  (`rdf_view/scope.rs:210-235`).
- Result: strict event-clock correlation now places such a record at receipt time and
  answers. A strict records read over the same record refuses.
- What stays correct: dropping the refusal for ordinary unknowns (`no_profile`,
  `missing_field`, `log_no_match`, ...) matches item 2, and moving to
  `sparrow-correlation/2` is right because the recipe text and projected columns changed.
- Correction:
  - Count selected records whose event time is `Unknown { reason: "projection_failed" }`
    in the correlation coverage.
  - Make `complete()` require zero of them when either clock is `Event`, mirroring the
    records-read law.
  - Restore the matching sentence in `docs/interfaces.md` (correlation coverage and the
    log-time section).
  - Extend an existing strict-correlation case to protect the refusal.

**R3. `docs/interfaces.md:510-514` attributes agent JSON keys to the RPC.**

- The doc says record output on "MCP and RPC renders `event_time.basis`" and that a
  receipt basis "adds `fallback_instant`".
- The Cap'n Proto wire has neither key. The basis is the union variant (`known` is
  source, `unknown` is receipt), and the instant is `UnknownEventTime.fallback`.
- The implementer's own report states this design. Only the doc is wrong.
- Correction: state the RPC shape separately from the CLI/MCP JSON shape.

### OPTIONAL

- **O1.** `event_interval_includes_receipt_fallback` asserts basis `receipt` through
  `placement()` directly. The core read rows do not carry a basis.
  - The read-result basis is protected only by the RPC `mcp_exercise` CLI check.
  - That is adequate coverage, but the named test checks less through the read path than
    its name implies.
- **O2.** `interpretations.event_time.fallback` (`agent/sparql.rs:760`) always equals
  `unknown`, so it adds no information.
  - It also appears on raw SPARQL replies, where `eventTime` does not place unknowns and
    pre-69 graphs lack the fallback triple.
  - Consider limiting it to event-clock correlation replies, or documenting that it is a
    scope count rather than a placement count.
- **O3.** For an event-clock time selector, the OPTIONAL source variable is
  `?selected_source` (`correlation.rs:218`). The Source selector's substitution uses the
  same name.
  - The two selectors are mutually exclusive, so nothing breaks today.
  - A distinct suffix (`_event`) would avoid a future collision.
- **O4.** `docs/development-runtime.md:6309-6313` still says unknown source times "are
  excluded". It is a dated development log, so leaving it as history is defensible.

## Open state (not findings)

- Issue acceptance `make check` has no execution evidence yet. It must pass through the
  commit hook before delivery is claimed.
- UAT requires deployment.
- Nothing is committed or pushed.

VERDICT: BLOCKED
