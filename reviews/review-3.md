# Delivery review 3 (recheck): issue #20, receipt-time fallback for event time

## Scope

- This recheck covers only the work changed since `review-2.md`:
  - R4: the backfill now reads only the metadata graph.
  - The `fallback_metadata` and `date_time` helpers in `rdf_index.rs`.
  - `EventPlacement::receipt` in `source_time.rs`.
  - The extended backfill test.
  - The O5 doc items in `docs/interfaces.md` and the O6 item in `deploy/RECOVERY.md`.
  - The `helpers-ledger.toml` rows.
- O7 was skipped, as allowed.
- Work tree: uncommitted, base `main` 2aa56a9.

## Evidence

Execution evidence (read from logs; I did not rerun anything):

- `logs/r4-core-storage.log` (21:21): 21 storage tests passed, 195 filtered out. They
  include:
  - `tracing_relationships_keep_instance_and_occurrence_evidence_through_backfill_and_retirement`;
  - `prepared_import_publishes_native_records_atomically_and_retries_after_cleanup`;
  - `archive_fence_...`, `staged_diagnostics_...`, `legacy_recordless_...`;
  - the four named issue tests.
- `logs/r4-core-lib.log`: 6 lib tests passed, including the writer-format and
  provenance private-owner tests.
- `logs/r4-server-rpc.log`: 3 tests passed.
- `logs/r4-clippy.log` and `logs/r4-fmt-check.log`: both exit 0.
- Freshness: every edited source, test, doc and ledger file is dated 21:06 to 21:09,
  older than the r4 logs.
- Not run: `make check`, the full test files and the census gates.

Source checks (read-only):

1. **A stored `eventTime` reliably marks a source instant.**
   - On `main`, the only `eventTime` writer in `crates/sparrow-core/src` is
     `rdf_index::metadata`, inside
     `if let crate::EventTime::Known { millis, profile_id, .. }` (`main:rdf_index.rs:994-996`).
   - `git log -S eventTime` shows the predicate entered in a6dfa81. That commit
     introduced it inside that same `Known` branch, and the only later change is the
     correlation reader (4aadf2c).
   - The diagnostic vocabulary has no time predicate (`service`, `host`, `severity`,
     `traceId`, `spanId`), and diagnostic graphs live under `diagnostic:record:*` owners,
     outside the job's `rdf:graph:record:` source.
   - So every pre-69 record graph carries `eventTime` exactly when its record had a known
     source time. Skipping graphs that have `eventTime` or `eventTimeFallback` is
     therefore correct.
2. **The `date_time` cleanup changes no literal.**
   - `date_time(millis)` is
     `RdfLiteral::typed(crate::format_instant(millis)?, "http://www.w3.org/2001/XMLSchema#dateTime")`.
     That is byte-for-byte the expression it replaces for `receivedAt` and `expiresAt`
     (`occurrence_metadata`) and for `eventTime`.
   - For a known record, `eventTime` takes `placement.millis`, which equals the stored
     `millis`.
   - Lexical form, datatype and term identity are unchanged. Existing graphs, dictionary
     term keys and published hashes are unaffected.
3. **The test proves the R4 path.**
   - `stage_fixture` plus `import_prepare_record(.., 4)` creates a prepared graph, and
     the test asserts it is the only record graph without a public catalog row
     (`current_private.len() == 1`).
   - The legacy fixture strips 2 rows from both the plain graph and the prepared graph,
     and the test asserts it.
   - After reopen, the fenced open succeeds. Under the review-2 code it would have
     failed: "record metadata graph has no catalog record".
   - `import_publish` then succeeds.
   - Publication does not rebuild record graphs. `publication.rs` only checks that
     `rdf:graph:<record.id>` exists (`:513`), then writes the catalog rows plus artifact
     RDF (`artifact_rdf::publish` stages the artifact owner only).
   - So the `eventTimeFallback` `...00.004Z` with basis `receipt` and no `eventTime`,
     found on the published record, can only come from the backfill of the private graph.
4. **One law.**
   - `EventPlacement::receipt` is the only constructor of a receipt placement. Both
     `EventTime::placement` (`source_time.rs:272`) and the backfill
     (`event_fallback_rdf.rs:101`) use it.
   - `fallback_metadata` is the only writer of the fallback triples. Both
     `event_time_metadata` and the backfill use it, and it refuses a non-receipt
     placement.
   - The ledger rows name the new entry points.
5. **Failure paths.** A missing, untyped or repeated `receivedAt` on the subject fails
   with `STORAGE_FAILURE`.
6. **Docs.**
   - `docs/interfaces.md` now states the `/92` to `/147` lift. That matches
     `FALLBACK_LOG_TIME[4]`, because `/92` is `LOG_TIME[4]`.
   - It also states the 132..142 to 143..153 log-time move, and that the backfill reads
     only the graph and covers prepared graphs.
   - `deploy/RECOVERY.md` lists the log-time law/fence refusal and its recovery.

## Findings

### REQUIRED

None.

### OPTIONAL

- **O8.** A malformed stored `receivedAt` fails with `INVALID_ARGUMENT` and no
  "event fallback RDF:" context. The code is
  `crate::parse_instant(received)?` (`event_fallback_rdf.rs:101`), and `parse_instant`
  reports `INVALID_ARGUMENT`.
  - The open still refuses, but the code and the message misdescribe stored-data
    corruption.
  - `map_err` into `failure(...)` would match the job's other refusals.
- **O7 (skipped)** still stands as recorded in review 2: there is no test of the
  "already current" branch. That is acceptable.

### Earlier findings

- **R4:** resolved.
- **O5 and O6:** resolved.
- **R1, R2 and R3:** remain resolved; nothing in this change touches their verified
  behavior.

## Open state (not findings)

- `make check` has not run; the commit hook must pass before delivery is claimed.
- UAT needs a deploy.
- Nothing is committed or pushed.

VERDICT: PASS
