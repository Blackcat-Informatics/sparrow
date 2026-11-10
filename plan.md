# #158 plan: serve every query and status call during online migrations

## Behavior

Every read that refused because an online migration or rebuild was unfinished now answers
completely, with the rows the finished migration gives:

- Record context and reclaimed-parent folds in the live artifact listing walk one source
  timeline, `source_context::walk`: the cache once its rebuild is complete, otherwise the
  primary `time:` index filtered to the source, bounded by the caller's receipt interval.
- The source listing and the live artifact listing add what the `source-summary` migration
  still owes (`source_summary::Completion::owed`, the migration's own counting passes run to
  the end over the reader's view without writing).
- Scoped SPARQL during the event-fallback backfill serves the facts the backfill owes the
  selected record graphs (`StoredRdfView::owe_fallback`), so strict reads answer.
- Scoped views enumerate their owner graphs once per view (memo), so graph-enumerating
  queries do not re-walk the owner index per cursor.
- Kept refusals fire only on per-record states written once at ingest and are recoded from
  `INDEX_NOT_READY` to `EVIDENCE_INCOMPLETE` (classification:
  /home/paudley/Active/sparrow/.stage/r82/158-classification.md).
- Status never refused before or after; the acceptance test calls it at every step.

## Dependencies

#160 lands first; sync before commit.

## Exclusions

None authorized. The owner-graph-index migration keeps the whole-store plan while pending
(it answers; no refusal). A declaration-walk enumeration was tried by the previous session
and reverted because it is not needed for a complete answer.

## Acceptance

- `every_read_answers_completely_while_online_migrations_run_and_intake_flows` (storage.rs).
- Latency during vs after rebuild, measured with the temporary probe
  `crates/sparrow-core/tests/measure_158.rs` (removed before commit; numbers go in the PR).

## Risk

The fallback timeline walk skips other sources' records without charging the visit budget
(bounded only by the receipt interval), under the writer lock. The owed source-summary pass
reads every unreached catalog entry per listing call. Both are measured and reported.

## User rulings

- Patrick, 2026-11-08 (relayed by lillith_coordination, verbatim): "answering "I'm not ready" is 100% failure for sparrow - might as well just be stopped.  That's "I'm dead" in a different coat."
- P28, Patrick (verbatim): "no backup may EVER stop lillith, lose data, or drop packets.  All backups MUST be online and perturb no operations."
- Patrick (verbatim): "DO NOT mess with, interogate, attemp to fix, or otherwise probe the build system unless you are explictly asked to. If build errors occur, backoff and wait."
- lillith_coordination, 2026-11-09: stop before ghprsq and report; land only on its word.
- lillith_coordination, 2026-11-09 (speaks for Patrick), ruling on review findings, verbatim excerpts:
  "R1, option (a). Answer exactly from a snapshot without the writer lock, with cancellation checks. CPU stays uncapped. Option (b), a budget frontier that can differ from the answer after the rebuild, is refused as "not ready in another coat"."
  "R2, confirmed. Compute once per listing from a snapshot, outside the writer lock, charged to the page's admission, with an exact answer." Live store: source-summary has never run; live daemon runs image 2aa56a9 (schema 68), 2def69f is not its ancestor, so every schema 69/70 migration starts from zero at D147 on ~1.7M live records (STATUS 2026-11-08 18:14). Record in PR body.
  "R5, in scope." RESOURCE_LIMIT on scoped SPARQL while the owner index rebuilds, and status "pending" counts while counters seed. Fix here, or file a REQUIRED issue named in the PR body with the reason.
  EVIDENCE_INCOMPLETE: keep only at migration-independent sites; PR body states what each kept site means. Do not run ghprsq (consumers katamari/brainstem must answer first).
  Gates: fmt, clippy, sparrow-server RPC tests, commit through hook; independent re-review of R1/R2/R5; fetch/sync before commit; push; stagectl pr-create; STOP and report.
- lillith_coordination, 2026-11-09: consumers (katamari 2ffc72a6, brainstem aaf91271) cleared the EVIDENCE_INCOMPLETE rename; merge-hold lifted. Still stop after opening the PR; coordination runs landing.

- Coordinator latency ruling (2026-11-09, verbatim excerpt): "`status` is constant-time during counter seeding: it answers from the seeding checkpoint, reports the counts as pending with the progress, and never scans the unseeded keys per call" ... "each page returns within the client's default deadline at the live size, by paging if need be" ... "Measure at the live size on a scratch store, NOT by extrapolation from 300,000."
- Coordinator acceptance of the plan (2026-11-09, in substance): status reverts R5b to checkpoint-only; keep the ledger fix. The cursor-only page is ACCEPTED on conditions, each with a test: the first call registers the capture and starts the build at once; every page returns within its 5 second budget; a page with no rows carries the continuation cursor and the build's progress (entries read of the owed total); following the cursor resumes the same capture and never restarts the build; rows that arrive are exact against the one snapshot; an abandoned cursor releases its build and snapshot at lease expiry. The PR states how long source-summary itself runs at the live size; records and context get the same treatment if over 30 s at live size; the store-level kept-current completion is not built here, filed REQUIRED if a fresh listing needs more than a few minutes of cursor pages (say so either way). Deadline 30 s (mcp.rs:229), 60 s lease (transport.rs:57) noted. Scratch matches record count 2.67M; compile probe under the lock, run the compiled binary OUTSIDE the lock; 20 samples per kind, during and after, both sizes. Order: #160 ahead; edits now; cargo step 1 only after #160's follow-up commit and hook ran (check lslocks and git -C <160 worktree> log). Evidence under /home/paudley/Active/sparrow/.stage/158/ with EXIT in every log. Name model use (sub-agents and models) in the next report.

- Coordinator deferral ruling (2026-11-10, relayed in the session, verbatim): "Decision: the branch as measured meets the target for the normal case, so the build-and-cursor treatment for context_rare and records is NOT done in PR #169; it becomes a separate REQUIRED issue (I file it) for the tail and for records' 2.5 s after the migrations." The clarification behind it, from Patrick: 30 s is the target for the NORMAL query, not a bound every outlier must meet.
- Coordinator ruling (2026-11-10, verbatim): "Go. ... proceed in this branch" on the build-and-cursor treatment, then superseded the same day by the deferral ruling above.

- Coordinator decision (2026-11-10, relayed in the session, verbatim): "(2) Pages over 5 s: ACCEPT your reading. The 5 s is the build WAIT per call, added to page assembly; rows-free pages take 5000 to 5100 ms and the slowest page with rows was 6.47 s, against the 30 s deadline (`mcp.rs:229`); do not lower the wait and do not re-run the 300k probe for this."
- Coordinator decision (2026-11-10, relayed in the session, verbatim): "(3) Source-summary alone at the live size: NO new 4.3 hour run. State in the PR the upper bound the existing log gives: all five migrations together took 6029 pages in 824.5 s at 2.67M (`158-measure-2670.log`), so source-summary alone took at most 824.5 s; the per-migration timing is not recorded and is added to the probe's output as part of the #176 work."
- Coordinator decision (2026-11-10, verbatim): "(4) The deferral issue number is #183".

## Work plan after the 2026-11-09 ruling (resume point)

State: uncommitted; acceptance test strengthened and passing (158-focused10.log); latency table in validation.md (158-measure5.log); first review BLOCKED (findings R1-R7, O1-O4 recorded in validation.md).

- R1 (context off-lock): records_context takes the writer lock only to capture protected set, membership, scope receipt, store id, `source_context::cached` and one RocksDB snapshot S (all writes go through the writer lock, so state under the lock equals S). Drop the lock; anchor read, anchor checks and both neighbor walks read S through `store_counters::Reader::snapshot`. Make the read helpers Reader-based, with the Writer methods delegating (one implementation): time_record, time_profile, diagnostic_profile, validate_diagnostic_record, validate_event_record, matches_diagnostic_scope. `source_context::walk(reader, cached, hash, position, bounds, direction, visit)` with a bidirectional Reader iterate. Add `records_context_cancellable(.., &QueryCancellation)`, checkpoints every N walk entries; RPC uses read_work.
- R2 sources: owed pass already runs once per capture on the snapshot; charge the Completion's memory (deltas) to the capture admission.
- R2 artifacts: the first live page takes a snapshot under the lock, drops the lock, computes the full owed index-row set for the selector on the snapshot (uncapped CPU, memory charged), stores it in the lease snapshot body; continuations reuse it. Page-time validation under the lock: an owed key is listed only if the stored index lacks it and the artifact still qualifies now (count_artifact through a capturing Rows sink on the live db; a missing entry is a reclaimed parent folded through walk at its receipt instant).
- R5a: while the owner index is pending, scoped views stay scoped: owner graphs = the owners' `rdf:o:` rows (complete through the frontier and for new graphs) plus a walk of `rdf:g:` declarations after the frontier filtered to the owners, uncharged to the request budget (CPU uncapped) with cancellation; memo once per view.
- R5b: status during counter seeding: for each counter add `count(reader, term, capped bound, uncapped bound)` (the same `count` apply uses) on the status snapshot; totals and `counts` become exact. Keep the wire fields (`counts`, `totalPending`) and report exact/false; doc that they are always exact.
- Then: tests (acceptance covers status exactness during seeding too), fmt, clippy, rpc tests, re-review, fetch/sync, commit via hook, push, stagectl pr-create, STOP.
- lillith_coordination, 2026-11-09: live `sparrow status` (schema 68) failed after ~61 s (RPC_FAILURE Peer disconnected 04:41:34; lease expired 04:42:35) while ingest flushed. Acceptance must include a status call that returns while writes are stalled or stopped, with its latency recorded. Same R1/R2/R5 rulings; no other scope change.

## Latency rework (after the plan acceptance)

Step 1 DONE (uncompiled): status, migrations(), unique_payload_chunks back to the ea1a4b2 checkpoint-only answer; exact_values and Positions::follows removed; report() lost its exact param; shutdown.rs, maintenance.rs, deploy/RECOVERY.md, deploy/verify-backup.sh, deploy/user-unit-upgrade.sh, docs/deployment.md checked out from ea1a4b2; status/schema-69/source-index test asserts, rpc.rs copy_daemons and verify-backup scenario 2, interfaces/architecture status text and helpers-ledger store-counters row reverted. Ledger fix kept.

Step 2 design (listing builds):
- New module storage/listing_build.rs: Builds<C, T> registry (cap MAX_HANDLES, purge(now) drops expired builds), Build{context C, expires_at, shared Arc<Shared<T>>}, Drop cancels. Shared{state Mutex<Option<Result<T>>>, Condvar, read AtomicU64, total Option<u64>, cancellation QueryCancellation, polled Mutex<Instant>}. Thread (std::thread::spawn) owns Store clone, takes its own snapshot, ticks progress, exits on cancel or own lease deadline; running counter for tests.
- Wait budget BUILD_WAIT = 5 s (test-support override). Cursor-only page: CatalogReadPage.build = Some(BuildProgress{read, total: Option}), next_cursor "<build id>/0", zero rows. Wire: CatalogReadReceipt.build @10 :CatalogBuild {read @0, total @1, totalPending @2}; rpc.rs catalog_receipt; agent/catalog.rs receipt renders "build".
- Progress units = source-summary migration units: artifact entries scanned (artifact phase) + record entries scanned (record phase); total = (Artifacts+Records) - SourceSummaryDone from the capture snapshot; None while counters seed. Add Rows::entry() hook (no-op for WriteBatch) so Completion ticks per entry and checks cancellation every 4,096 (the record phase had no cancellation check inside its single usize::MAX page).
- sources: when source-summary incomplete, the whole capture (writer-lock registry reads + snapshot + owed pass + rows) runs in the build thread; capture `started` set after the owed pass; on publish from a poll, expires_at rebased to poll_now + LEASE_MS, as_of stays. Registry lock never held while waiting.
- artifacts: owed window computation (owed_index_rows) runs as a build, fresh and on window crossing; context {selector, page_size, scan_budget, start Position, offset}; artifact_next split so the reads lock is dropped while waiting; result_read.rs:121 also calls artifact_next.
- Tests (each condition): build starts at first call; page within budget; cursor-only page carries cursor + progress; following cursor resumes same build (no restart: build id/spawn count); rows exact vs snapshot (equal to finished-migration listing); abandoned cursor: purge at expiry cancels thread and drops snapshot (running counter 0).

Step 2 WRITTEN (uncompiled, 2026-11-09 ~17:40): storage/listing_build.rs (Builds/Build/Worker/Waiter, BUILD_WAIT 5 s, census, release); source_summary Rows-free `entry` hook on artifact_page/record_page + owed_entries(); source_read: source_capture (thread body), source_build_poll/page, publish_sources, BUILD "source-build"; artifact_read: Pending/Next, artifact_window/build_poll/build_page/pending_page, owed_index_rows(worker), BUILD "artifact-build"; result_read: build cursors + Next::Window + release of builds; storage.rs: source_builds/artifact_builds fields, purge at expiry, test hooks set_listing_build_wait_for_tests/listing_builds_for_tests/purge_read_leases_for_tests; CatalogReadPage.build; capnp CatalogBuild + rpc.rs encoder + agent/catalog.rs render (only when present); MCP tool texts; interfaces "Listing builds" section, architecture; helpers-ledger job listing-build. Tests: a_source_listing_during_the_source_summary_migration_is_a_build_its_cursor_resumes, a_live_artifact_listing_during_the_source_summary_migration_resumes_its_build (helper hold_listing_builds). Probe: root .stage/r82/158-scratch/<artifacts>, max_records 4M, sources_page/artifacts_page released, TO_ROWS lines. Waiting on #160 (task b8dnm7f3n) before cargo step 1.

Commit 8ba701d (hook 158-commit2.log EXIT=0), pushed, WIP PR #169. Probe binary compiled under lock (158-probe-build.log EXIT=0), copied to .stage/r82/158-scratch/measure_158.bin; run.sh runs 300 then 2670 outside the lock (bg task bng64tfpc). 158-measure-300.log EXIT=0: during status 0.30/2.25 ms, sources_page 5000/5000, artifacts_page 5000/5000, context_rare 5216/9762, records 4994/8415; TO_ROWS sources 17.7-19.5 s 4 pages, artifacts 13.2-19.7 s; after status 0.43, context_rare 0.79, sources 0.17, artifacts 19.6, records 3483/5157 (records slow even after: pre-existing scan cost); MIGRATE 682 pages 66.3 s. Reviewer (delivery-reviewer, inherited model) running on 8ba701d -> review-5.md.
