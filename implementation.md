# #146 implementation notes

## Diagnosis verified against d912922

- Writer lock: `Store.inner: Arc<Mutex<Writer>>` (crates/sparrow-core/src/storage.rs:485), `fn lock` (:1516).
- `status` takes the writer lock and decodes every `record:` row, counts `ref:`,
  `projection-ref:` and `pin:` under it (storage.rs:2454-2537).
- `migrations()` takes the writer lock (migration.rs:289-295); a running migration counts
  its source keys (`migration_counts`, migration.rs:179-211); #142 caches only completed
  totals (`migration-total:`), and a migration completed before #142 is counted once per
  session under the lock (migration.rs:239-249).
- Maintenance progress line calls `store.migrations()` every 64 pages
  (crates/sparrow-server/src/maintenance.rs:118-145).
- Pages run back to back with only `yield_now` (maintenance.rs:181-182, :259); the std
  mutex is not fair.
- verify-backup.sh measurement scope: see deploy/verify-backup.sh (Nice not applied).

## Design (decided)

1. `storage/store_counters.rs`: one owner `apply` runs in `Writer::apply` after
   `source_summary::apply` and maintains, in the same batch, fixed counter rows
   `store-counter:<name>` (u64 BE):
   population counters `records`, `retained-records` (not evicted), `artifacts`,
   `graph-declarations` (`rdf:g:`), `record-graphs` (`provenance-rdf:record:`),
   `payload-chunks` (`ref:`), `json-projections` (`projection-ref:`); and one `done`
   row per data migration (keys at or before that migration's checkpoint bound).
   Every counter is a sum of terms (prefix, filter, bound). A term's bound is the
   minimum of the counter seeding watermark and (for done rows) the migration's
   checkpoint. A batch changing a key under a term adjusts by existence/filter
   before vs after when the key is within the post-batch bound; a batch moving a
   checkpoint adds the signed count of the keys between the old and new bound, read
   from the pre-batch database. Exact under any mix; crash-consistent (same batch).
   Underflow or a missing row: STORAGE_FAILURE, never clamped.
2. Seeding: new first online migration `store-counters` (Migration::ALL[0]); its pages
   only advance its checkpoint across up to 16,384 keys of one prefix at a time; the
   observer does the counting. Fence at open: new writer-format family
   (lifts SOURCE_SUMMARY by 55, /297-/351), zero rows, checkpoint start (complete for an
   empty store), discards stale `migration-total:` rows. No open-time scan.
   (Review 1 replaced the following: no runtime recount; see "Review 1 rework".)
   While it seeds, status and migration progress read the explicit pending path: an
   exact recount from a RocksDB snapshot, without the writer lock.
3. status/migrations: read from a snapshot via `Store`-level `Arc<DB>`; no writer lock.
   Running and completed migrations both report done = done row, total = population
   counter(s). #142's recorded `migration-total:` rows are removed (one path).
4. Writer priority: `Store::lock` registers waiting writers; a migration page defers
   while writers wait, at most the longer of the previous page's hold and 100 ms
   (Condvar wait, no busy loop).
5. MigrationPage carries the page's own done/total/watermark; maintenance journal prints it.
6. deploy/verify-backup.sh: measurement scope sets the unit's Nice read from the unit
   file and SCOPE_LIMITS checks it.

## Changes (file:line)

- Store counters, one owner: crates/sparrow-core/src/storage/store_counters.rs
  (`Seed` :247, test-only audit `recount` :511, rows-only `values` :538, `fence` :563, seeding
  `page` :592, batch observer `apply` :652, legacy fixture lowering :717, test audit
  :740). Wired in `Writer::apply` at crates/sparrow-core/src/storage/durability.rs:144,
  right after `source_summary::apply`. Shares `source_summary::failure`
  (source_summary.rs:93).
- Migrations: crates/sparrow-core/src/storage/migration.rs (`StoreCounters` first in
  `Migration::ALL` :57, `MigrationPage` with its own done/total/watermark :125, `fence`
  :146, `report` from counter rows :177, `migrate_step` defers to writers and reports
  from a snapshot :302, lock-free `migrations` :355). The counters fence discards #142's
  `migration-total:` rows.
- Writer priority and lock-free observed state:
  crates/sparrow-core/src/storage/writer_queue.rs (`arrive` :65, `defer_page` :76,
  `page_held` :94, `Observed` :104). `Store::lock` registers writers
  (storage.rs:1534); migration pages take `lock_uncounted` (storage.rs:1543).
- Status without the writer lock: crates/sparrow-core/src/storage.rs (`Unlocked`, the
  lock-free state behind one `Arc`, :486 and :508; `decode_evidence_entry` :617;
  `store_properties` :849; `properties` :2445; `unique_payload_chunks` :2482; `status`
  :2492; `status_counted` :2499; test hooks :2600 and :2611). Durability, pressure and
  recompression status read `Observed` state: durability.rs:312, pressure.rs:300,
  recompression.rs:312.
- Writer format: crates/sparrow-core/src/storage/writer_format.rs (`STORE_COUNTERS_*`
  families `sparrow.store/297` to `/351` from :375; `lower_store_counters`,
  `has_store_counters`, `enable_store_counters` follow).
- Removed duplicate counting paths: `Writer::count_catalog`, the free `count_prefix`,
  `source_summary::migration_counts`, `event_fallback_rdf::counts` and
  `rdf_index::owner_graph_counts`.
- Maintenance journal prints the page's own progress:
  crates/sparrow-server/src/maintenance.rs:120 (no `migrations()` recount every 64
  pages).
- Test support: crates/sparrow-test-support/src/lib.rs:181 and :206 (the legacy fixtures
  remove the store counters).
- Measurement scope Nice: deploy/verify-backup.sh:99 (`MEASURE_UNIT`), :114 (reads the
  loaded user unit's Nice), :117 (`SCOPE_LIMITS` checks it), :346 (`--nice`), :394
  (applied Nice from `ps`).
- Docs: docs/interfaces.md:593-620 (online migrations), :650 ("Store counters (store
  format)"), :665 (`live_records` follows the catalog: entries stored and not evicted);
  docs/architecture.md:556-591; docs/deployment.md:446-452 and :509 (scope Nice);
  helpers-ledger.toml:173 (`store-counters`), :187
  (`writer-priority-over-migration-pages`), :2695 and :2906 (homes moved to
  `decode_evidence_entry` and `store_properties`).
- Tests (crates/sparrow-core/tests/storage.rs): `expire_through` :19215,
  `assert_counts_match_recount` :19227 (counter rows equal a recount),
  `source_counters_match_a_full_recount_after_every_lifecycle_step` :19370,
  `negative_or_missing_store_counter_is_a_storage_failure` :19462,
  `status_reads_fixed_keys_without_the_writer_while_a_migration_runs` :19538,
  `a_waiting_publication_commits_within_one_page_while_a_migration_pages` :19606.
  Updated for the counted `live_records` and the status that reads no record:
  :3291, :4115, :4174, :4868, :16262, text_relations/tests.rs, and
  crates/sparrow-server/tests/rpc.rs:3122. Format offsets shifted by 55; the
  maintenance test at maintenance.rs:380 drains the new first migration.

## Checks

All run in the worktree, one cargo invocation at a time, release profile.

| Check | Exit |
| --- | --- |
| `cargo fmt --check -p sparrow-core -p sparrow-server -p sparrow-test-support` | 0 |
| `cargo clippy --locked --release -p sparrow-core -p sparrow-server -p sparrow-test-support --all-targets -- -D warnings` | 0 (final run) |
| `cargo test --locked --release --no-fail-fast -p sparrow-core` | 0: lib 36/36, storage 230/230 |
| `cargo test --locked --release --no-fail-fast -p sparrow-server` | 0: 28, 8 and 84 passed |
| `cargo test --locked --release -p sparrow-test-support` | 0: 1/1 |
| `cargo test ... -p sparrow-core` focused rerun after the final refactor (status, pins, expiry, eviction, counters) | 0: 72 + 3 passed |
| `make census-check` | 0: 319 ledger jobs and 1 distinct row hold |
| `bash -n deploy/verify-backup.sh`, `shellcheck -x deploy/verify-backup.sh` | 0, 0 |
| `prose-check` on deployment.md, architecture.md, this file | 0 |
| `prose-check docs/interfaces.md` | 1: 9 `UInt64`/`UInt32` findings, all present at d912922, none in added lines |

Not run: `make check` (excluded by instruction), the live measurement (done-when 3,
after #145).

## Review 1 rework

Decisions:

- 2026-11-08 parent decision: option A (B misleads percentage readers; C blanks progress during done-when 3).
  Status gains `countsPending @54 :Bool`, MigrationStatus gains `totalPending @5 :Bool`, with
  a schema bump; JSON, CLI and MCP report `"counts": "pending"|"exact"` and a pending
  migration's `"total": "counting"`. While pending, the counted fields are exact through
  the seeding watermark and never read as a final total.

Findings (file:line in the worktree):

- R1, fixed-cost status while seeding. `store_counters::values` returns the rows only
  (crates/sparrow-core/src/storage/store_counters.rs:538); the recount is test-support
  only (:511, used by the audit). `migration::report` gives each migration's done from
  rows and `total: None` while seeding (migration.rs:179; `MigrationStatus.total` is
  `Option<u64>` at :107, `complete` at :119 treats a counting total as no obstacle).
  `Store::status_report` reads status and the report from one snapshot
  (storage.rs:2512, :2522; `counts` at :2622); the status RPC calls it once
  (crates/sparrow-server/src/rpc.rs:2760) and sets `countsPending` (:2846). Wire:
  `countsPending @54`, `totalPending @5` (crates/sparrow-client/schema/sparrow.capnp:257,
  :267), `SCHEMA_VERSION` 70 (crates/sparrow-client/src/lib.rs:69); encode
  (crates/sparrow-server/src/recompression.rs:35); CLI/MCP `counts` and `total:
  counting` (crates/sparrow-server/src/agent/status.rs:175, :366); MCP description
  (agent/tool/catalog.rs:115). Docs: interfaces.md:618 (schema 70 flags), :694, :1275;
  deployment.md:293 (schema 70 images and naming from `SCHEMA_VERSION`, example switch
  69 to 70). Installed names come only from `SCHEMA_VERSION` (tools/install-bin.sh:21);
  no compatibility registry pins the client schema (registry/compatibility.toml pins
  only PurRDF). Test: tests/storage.rs:19546 reads the same key count at 20,000 and
  25,000 records while `store-counters` seeds (`counts` pending, every total `None`,
  `live_records` 16,384 through the watermark) and after seeding (`counts` exact,
  25,000); rpc.rs:4376/:4442 cover CLI `pending`/`counting` on a query-only copy and
  `exact` after a no-intake daemon seeds.
- R2, deadline meaning kept. Record entries carry `retired`, written only when set
  (storage.rs:436, :731, :791); expiry sets it in the retirement batch it already writes
  (expiry.rs:302); `retained-records` excludes evicted or retired entries
  (store_counters.rs:464-476). Test expectations reverted to the documented values,
  with an expiry pass where a deadline passes (tests/storage.rs:4115, :4873;
  text_relations/tests.rs is back to base); `expire_through` (:19223) drains a large
  retirement. Docs agree: interfaces.md:676 and architecture.md:571 state the deadline
  meaning and the expiry-pass lag; `retention_metrics` keeps the deadline rule, the
  oracle the count equals after expiry. The turnover qualifier passes day 30 in none and
  sparse modes (checks below).
- R3, status never waits on a pass. `Observed` keeps working state writer-local and
  publishes an `Observe::View` copy when a mutable borrow ends; status reads
  `Published::read` (writer_queue.rs:101, :108, :114, :179). Views: durability.rs:83
  (counts and oldest acceptance, not the ledger), pressure.rs:79, recompression.rs:130.
  Docs: interfaces.md:694, architecture.md:577.
- R4: the PR must reference #146 without closing it; done-when 3 remains open.
- O1: `pending_store_counter_migration` removed; this file corrected.
- O2: the "about half" claim is removed (interfaces.md, writer_queue.rs module doc).
- O3: status test holds the page 6 s, waits until the writer is held, and asserts it is
  still held after status answers (tests/storage.rs:19546; `writer_busy_for_tests`,
  storage.rs:2646); priority test uses 4 s pages, 2 s slack, and starts after a page holds
  the writer (:19647).
- O4: `migrate_step` no longer defers (migration.rs:295); `Store::defer_migration_page`
  (:336) runs in a blocking task the maintenance worker polls with its other duties
  (crates/sparrow-server/src/maintenance.rs:108, :202, :258), so only the page waits.
- O5: interfaces.md:1275.
- O6: runtime range deletes on counted prefixes: none. The only `delete_range` calls are
  the open-time fences (source_summary.rs:380, store_counters.rs:552) and a test fixture
  (store_counters.rs:703). `observe_batch` now refuses any batch whose iterated
  operations fall short of `WriteBatch::len()`, which counts range deletes
  (durability.rs:411), for every observer in `Writer::apply`; unit test
  durability.rs:440.

Rework checks (release profile, one cargo invocation at a time):

| Check | Exit |
| --- | --- |
| `cargo fmt --check` (core, server, test-support, client) | 0 |
| `cargo clippy --locked --release` on the four crates, `--all-targets -- -D warnings` | 0 |
| `cargo test --locked --release --no-fail-fast -p sparrow-core` | 0: lib 37/37, storage 230/230 |
| `cargo test --locked --release --no-fail-fast -p sparrow-server` | 0: 28, 8 and 84 passed |
| `cargo test --locked --release -p sparrow-client` | 0: 22 + 1 passed |
| `turnover CONFIG none 16 16 4096` and `sparse` (60 days, fresh /tmp stores, removed after) | 0, 0 |
| `make census-check` | 0: 320 ledger jobs hold |
| `bash -n` and `shellcheck -x` on deploy/verify-backup.sh | 0, 0 |
| `prose-check` on architecture.md, deployment.md, this file | 0 |
| `prose-check docs/interfaces.md` | 1: the same 9 pre-existing `UInt64`/`UInt32` findings |

Not run: `make check` (excluded), the done-when 3 live measurement (after #145).

## Review 2 rework

Every finding of `reviews/review-2.md`, mapped to its change (worktree line numbers).

| Finding | Change |
| --- | --- |
| N1 closing line | `crates/sparrow-core/src/storage.rs:2510` `unique_payload_chunks` returns `None` while the counters seed; `crates/sparrow-server/src/shutdown.rs:64` logs `unique_payload_chunks=pending` |
| N1 sparrow-backup | `deploy/libexec/sparrow-backup:147` accepts `pending` in the closing line; `:196` `recorded_count` writes `live_records=pending` when status reports `counts=pending` (`:313`), refuses any other `counts` value; docstring updated |
| N1 verify-backup | `deploy/verify-backup.sh:180` `status_counts`; `:482-499` a pending copy or a `pending` SOURCE prints `NOT VERIFIABLE payload-chunks: not verifiable (counts pending)` and never compares; a non-numeric, non-pending SOURCE value fails; `:563` expects `pending` in the closing line of a pending copy; `:572` ends `verified except payload-chunks: not verifiable (counts pending)`, exit 0; `:424` MEASURE prints `counts=` |
| N1 user-unit-upgrade | `deploy/user-unit-upgrade.sh:247` prints `counts=` beside `live_records` |
| N1 docs | `deploy/RECOVERY.md:243`, `:246`, `:273-286`, `:309`; `docs/deployment.md:295-303`, `:387`, `:444`, `:521` |
| N1 test | `crates/sparrow-server/tests/rpc.rs:4676` `verify_backup_reports_pending_counts_as_not_verifiable`: SOURCE pending on an exact copy, then a pre-#146 copy (counters removed) the image opens pending; both exit 0 with NOT VERIFIABLE, no `PASS payload-chunks`, every other check PASS, closing line exact or `pending` |
| N2 | `storage.rs:556` `writer_poisoned` (one error), `:1571` `refuse_poisoned` (`Mutex::is_poisoned`, no wait), called by `status_counted` `:2549`, `unique_payload_chunks` `:2511`, `durability.rs:339`, `pressure.rs:308`, `recompression.rs:320`; `storage.rs:2679` dev-only `poison_writer_for_tests`; test `crates/sparrow-core/tests/storage.rs:19646` `status_refuses_once_the_writer_is_poisoned`; `docs/interfaces.md:697-700` |
| O7 | `docs/interfaces.md:686-688`: an entry retired before the marker is marked at expiry's next visit and stops counting at the next pass |
| O8 | `crates/sparrow-core/src/storage/migration.rs:291` `migrate_step` no longer claims to yield; `:335` `defer_migration_page` names both callers |
| O9 | `crates/sparrow-server/src/maintenance.rs:72` `Job::Migrate { defer }` waits inline before the page; `:214` the retry tick sets `defer: true`; `:260-265` a background deferral that finished while another duty ran marks the next page `stale`, so it waits again immediately before the page; `docs/interfaces.md:609-613` |
| O10 | `docs/deployment.md:542` `--from sparrowd-schema68-2aa56a9`; `:545-548` live runs schema 68 since the rollback, schema 69 clients are refused after the switch. `docs/open-items.md` A-17 already says live is schema 68; unchanged |

### Review 2 checks

| Check | Exit |
| --- | --- |
| `cargo fmt --check` (core, server, test-support, client) | 0 |
| `cargo clippy --locked --release` on the four crates, `--all-targets -- -D warnings` | 0 |
| `cargo test --locked --release --no-fail-fast -p sparrow-core` | 0: lib 37/37, storage 231/231 |
| `cargo test --locked --release --no-fail-fast -p sparrow-server` | lib 28/28, sparrow 0, sparrowd 8/8 (first run hit my 590 s timeout during `rpc`); `--test rpc` rerun 0: 85/85 |
| `make census-check` | 0: 320 ledger jobs hold |
| `bash -n`, `shellcheck -x` on verify-backup.sh and user-unit-upgrade.sh | 0, 0, 0, 0 |
| `python3 -I -m py_compile deploy/libexec/sparrow-backup` (cache removed) and a probe of `CLOSED` and `recorded_count` | 0 |
| `prose-check` deployment.md, RECOVERY.md, architecture.md | 0 |
| `prose-check docs/interfaces.md` | 1: the same 9 pre-existing `UInt64`/`UInt32` findings |

## D118: retire an unrecoverable consume claim (coordination ruling, 2026-11-08)

Context: the live claim `import:252d1ffe…` is claimed, unpublished, and its private source is
missing. `ClaimRoot::recover` requires the claimed generation, so it can never complete, and
`sparrow-backup` waits for zero pending claims.

| Requirement | Change |
| --- | --- |
| Durable record (time, caller, reason) | `crates/sparrow-core/src/storage/consumption.rs:178` `RetiredConsumption`, `:783` `Store::consumption_retire`. It refuses an empty/multi-line caller or reason, a journal that is already retired, unknown, removal-confirmed, on another watch, published (`published_id`), or never claimed. One batch puts `consumption-retired:ID` (journal plus retirement) and deletes `consumption:ID`, so `consumption_pending` and its 16-slot bound drop it. Bound: `:155` 64 retired claims. Readers: `:858` `consumption_retired` (one iterator, no writer lock) and `:517` `consumption_retired_admitted` |
| Source must be missing | `crates/sparrow-server/src/consumption.rs:531` `ClaimRoot::retire`: checks the watch scope and private namespace, refuses while `.sparrow-inflight/ID` opens, and never touches a path |
| Admin RPC | `crates/sparrow-client/schema/sparrow.capnp:756` `watchClaimRetire @52`, `:348` `WatchClaimRetire`, `:323` `RetiredClaim`; `crates/sparrow-server/src/rpc.rs:1821` handler. It refuses with `PERMISSION_DENIED` when the connection has no identified administrator |
| Admin authentication | `ingest_gate.rs:180` refuses it for anonymous TCP and the ingest socket; `query_only.rs:133` classes it as a write. Caller: `rpc.rs:90` `admin`, `transport.rs:34` `Access::Local(String)` from `transport.rs:45` `local_caller` (`unix uid=U pid=P`, `main.rs:277`; an unreadable peer is dropped and logged), `tcp_gate.rs:78` `tcp-chap peer=ADDR` after CHAP |
| Worker path, one journal line | `watch_admin.rs:77` `Command::RetireClaim`, `:244` `retire_claim`; `watches.rs:1086` dispatch (drops the claim's retry entry), `:821` `retire_claim` (clears the watch failure, logs `sparrowd: watch=W claim=ID retired-unrecoverable caller=… reason=… path=…` once) |
| Status and watch listings | `sparrow.capnp:318` `ConsumeWatchStatus.retiredClaims @19`, `:340` `WatchInfo.retiredClaims @4`; `watch_admin.rs:199` `encode_retired`, `:218` `encode_retired_claim`; `rpc.rs:2890` status. JSON: `agent/watch.rs:322` `retired_claim` (`state: retired-unrecoverable`), `agent/status.rs:113` |
| ingest_get | `ingest_status.rs:89` `IngestDeletion::Retired`, `:284`; `sparrow.capnp:638` `retired @5`; `agent/ingest_status.rs:230` `retired_unrecoverable` |
| CLI and MCP | `agent/watch.rs:95` `watch retire-claim WATCH_ID CLAIM_ID REASON`; `agent/tool.rs:160` and `agent/tool/catalog.rs:38`, `:286`, `:685-700` MCP `watch_claim_retire` (not read-only, destructive, not idempotent) |
| Docs | `docs/interfaces.md:2256`, `:2759`, `:2766` section "Retiring an unrecoverable claim", gate list `:476`, CLI example; `docs/deployment.md:413-420`; `deploy/user-unit-upgrade.sh:61` `--help` bullet "Claims after a rollback or restore" (private claim entry reconciliation from #145, then retirement) |
| Tests | `crates/sparrow-core/tests/storage.rs:6047` `consumption_retire_moves_only_an_unpublished_claim_out_of_pending`: refuses unclaimed, wrong watch, blank reason, published; a retirement leaves pending 2 to 1, survives reopen, reads `Retired`, and a second retirement is refused. `crates/sparrow-server/tests/rpc.rs:4098` `retire_claim_clears_only_an_unrecoverable_claim_and_lets_the_drain_reach_zero`: daemon with two unrecoverable claims; CLI and MCP refuse the one whose private source exists (`CONFLICT`, "still exists"); retiring the missing one drops pending 2 to 1 and lists it in status and `watch list` with a `unix uid=` caller; a second retirement is refused; after the replaced entry is set aside and retired, pending is 0 with no waiting file (the sparrow-backup drain), the watch failure clears, and the daemon log holds exactly 2 `retired-unrecoverable` lines. The MCP tool count in `mcp_exercise` is now 39 |

Not done here: the live retirement of `import:252d1ffe…` (a lifecycle action on the live service, outside this worktree's authority). It needs the schema 70 image.

### D118 checks

| Check | Exit |
| --- | --- |
| `cargo fmt --check` (core, server, test-support, client) | 0 |
| `cargo clippy --locked --release` on the four crates, `--all-targets -- -D warnings` | 0 (after the test edits too) |
| `cargo test --locked --release --no-fail-fast -p sparrow-core` | 0: lib 37/37, storage 232/232 |
| `cargo test --locked --release --no-fail-fast -p sparrow-server` (before the MCP count fix) | lib 28/28, sparrowd 8/8, rpc 84/86: the two `mcp_exercise` callers expected 38 tools, now 39; fixed |
| `cargo test --locked --release --no-fail-fast -p sparrow-server --test rpc` after the fix and the added MCP refusal | 0: 86/86 |
| `cargo test --locked --release -p sparrow-client` | 0: 22 + 1 |
| `make census-check` | 0: 320 ledger jobs hold |
| `bash -n`, `shellcheck -x` deploy/user-unit-upgrade.sh | 0, 0 |
| `prose-check` deployment.md | 0; interfaces.md 1, the same 9 pre-existing findings |

## Review 3 rework

Every review-3 finding is in scope (nothing optional).

| Finding | Change |
| --- | --- |
| D1 | `consumption_retire` (crates/sparrow-core/src/storage/consumption.rs:860) calls `Writer::end_retired_consumption_import` (crates/sparrow-core/src/storage/import.rs:1052) in the retirement batch: if the claim's staged import began, its lease ends at `now` (the manifest must be the unpublished consumption staging, else `STORAGE_FAILURE`). `import_reclaim` (import.rs:996-1000) keeps an unpublished consumption import only while its claim is not retired (`consumption::retired`, consumption.rs:164), so the existing expired-lease removal frees the staged chunks, private preparation and slot. No second removal path. Test `retiring_a_claim_with_a_begun_import_frees_its_slot_and_staged_chunks` (crates/sparrow-core/tests/storage.rs:6154): 16 begun imports refuse a 17th, reclaim keeps the claim's import before retirement, retirement then reclaim frees it, the import is gone and a new begin succeeds. Docs: interfaces.md "Retiring an unrecoverable claim", deployment.md drain paragraph. |
| O11 | deploy/verify-backup.sh exits 3 for "verified except payload-chunks: not verifiable (counts pending)" (line 576); `--help` lists exits 0/1/2/3 (line 35). RECOVERY.md (lines 248, 288-289) and deployment.md (line 455) say exit 3 is not verified. rpc `verify_backup_reports_pending_counts_as_not_verifiable` asserts `Some(3)` for both runs (rpc.rs:4943, 4966). |
| O12 | `Detector.failed_claim` (crates/sparrow-server/src/watches.rs:265) records the claim whose recovery set the failure (watches.rs:1300, failure text now `claim ID: error`); every other failure and success resets it. `retire_claim` clears the failure only when `failed_claim` is the retired claim (watches.rs:860). The rpc retire test retires the non-causing claim first and asserts the failure, naming the cause, is kept. |
| O13 | rpc `retire_claim_clears_only_an_unrecoverable_claim_and_lets_the_drain_reach_zero` (rpc.rs:4117) is now deterministic: both claims are made unrecoverable, the stable failure identifies the cause, the other is retired (failure kept), a file is written while the cause is still pending (drain nonzero), then the cause is retired, the failure clears, pending reaches 0, the file is consumed, two retired claims are listed and two journal lines written. Refusal tests: anonymous TCP `watch retire-claim` gives `UNAUTHENTICATED` (rpc.rs:1617); the ingest socket's `watchClaimRetire` gives `PERMISSION_DENIED` (rpc.rs:1888). |

### Review 3 checks

| Check | Result |
| --- | --- |
| `cargo fmt --check` (core, server, test-support, client) | 0 |
| `cargo clippy --locked --release -p sparrow-core -p sparrow-server -p sparrow-test-support -p sparrow-client --all-targets -- -D warnings` | 0 |
| `cargo test --locked --release -p sparrow-core` | 0: 37 + 233 |
| `bash -n`, `shellcheck -x` deploy/verify-backup.sh | 0, 0 |
| `prose-check` deployment.md, RECOVERY.md | 0, 0; interfaces.md 1, the same 9 pre-existing findings |
| `cargo test --locked --release -p sparrow-server` | 0: lib 28, sparrowd 8, rpc 86 (the four changed tests pass) |
| `make census-check` | 0: 320 ledger jobs and 1 distinct row hold |

## Review 4 rework

| Finding | Change |
| --- | --- |
| T1 | The rule that retiring a claim keeps the failure another claim or discovery caused is now tested where its inputs are controlled: `watches::tests::retiring_a_claim_clears_only_the_failure_that_claim_caused` (crates/sparrow-server/src/watches/tests.rs:325) opens a real `Detector`, claims three sources through its own claim owner, removes their private copies, sets the failure and `failed_claim`, and calls the production `retire_claim`. Retiring another claim keeps the failure and its claim; retiring the cause clears both; a discovery failure (no claim) survives a retirement. The rpc test (crates/sparrow-server/tests/rpc.rs:4116) no longer reads which claim caused the failure: it keeps the CLI and MCP CONFLICT refusals, retires one claim (fields, status, watch list, second refusal), writes a waiting file while the other claim is stuck (pending at least 1), retires the last claim, and waits on the observable condition pending 0 and completed at least 1, then asserts the failure is null (only retired claims ever failed). No sleeps or stable-read heuristics remain. |
| O14 | The core D1 test (crates/sparrow-core/tests/storage.rs:6154) begins the 15 ordinary imports at 50 days, after the claim's own lease expired and while theirs are live. Reclaim at that instant returns 0 and a 17th begin is still refused; after retirement, reclaim frees the one chunk and the import at that same instant, so the final begin succeeds only through the retired import's freed slot. |

### Review 4 checks

| Check | Result |
| --- | --- |
| `cargo fmt --check` (core, server, test-support, client) | 0 |
| `cargo clippy --locked --release -p sparrow-core -p sparrow-server -p sparrow-test-support -p sparrow-client --all-targets -- -D warnings` | 0 |
| `cargo test --locked --release -p sparrow-core --test storage` | 0: 233 |
| `cargo test --locked --release -p sparrow-server --lib` | 0: 29 (the new unit test included) |
| `cargo test --locked --release -p sparrow-server --test rpc` | 0: 86 |
| rpc retire test, `--exact`, five consecutive runs | 0, 0, 0, 0, 0 |
| `make census-check` | 0: 320 ledger jobs and 1 distinct row hold |
