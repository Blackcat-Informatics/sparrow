## Summary

Implements the #176 decision: option (b) with c3 and per-migration timing. Option (a), maintained owed-completion rows, is not built.

- **Run order.** The maintenance worker runs `source-summary` directly after `store-counters` (`Migration::RUN_ORDER`). `Migration::ALL` stays the `status` and report order. There is no writer-format fence and no wire or schema change. Each migration resumes from its own checkpoint, so a store left part way through under either order resumes correctly.
- **Listing build re-base.** At each 4,096-entry check a `sources` capture build or a live `artifacts` window build reads the migration's checkpoint and `done` counter. If the migration has overtaken the build, the build drops its snapshot and partial pass, takes a fresh snapshot and reads only what is still owed there. "Overtaken" means the migration is complete, or its checkpoint has moved and it has passed more entries than the build has read plus one check interval. The build keeps its identity, cursor and lease. A build started during the migration therefore delivers rows no later than its next check after the migration completes. `build.read` and `total` restart from the fresh snapshot.
- **Per-migration timing.** The daemon journal now logs `sparrowd: migration NAME start pages=1 done=D total=T` at each migration's first committed page of the session. Progress and completion lines carry `elapsed_seconds`, timed from the start of that first page, so the `complete` line gives each migration's own duration, pages and final `done`. The probe `measure_158.rs` is not in the tree, so the timing lives in these lines; `deploy/verify-backup.sh` already reprints them.
- Docs: `docs/interfaces.md` ("Online store migrations", "Per-source counters and artifact index", "Listing builds"), `docs/architecture.md` section 7, `docs/deployment.md`. The helpers ledger records the re-base entry points.

## Tests

- `the_worker_runs_the_source_summary_migration_directly_after_the_store_counters`
- `a_store_part_way_through_its_migrations_under_either_order_resumes_to_the_finished_rows`
- `a_source_listing_build_the_migration_overtakes_delivers_the_finished_rows_at_its_next_check`
- `a_live_artifact_window_build_re_bases_onto_a_newer_snapshot_mid_migration`
- Updated for the new order: `reads_report_pending_mid_migration_and_the_worker_finishes_it` (sparrow-server).

## Not done here

The 2.67-million-record measurement of source-summary's own duration has not been run. It needs coordination's go-ahead, so this PR refs #176 and does not close it.

Refs #176
