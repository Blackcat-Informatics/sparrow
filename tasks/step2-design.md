# Step 2 working design (agent notes)
- New core module storage/migration.rs: open-time `fence(db)` (one sync batch: enable_event_fallback + seed fallback checkpoint; enable_owner_graphs + seed owner checkpoint [1]); `Store::migrate_step(now)` one page under writer lock, commit via Writer::commit (durability latch, space floor, admit_data_write); `Store::migrations()` -> Vec<MigrationStatus{name,done,total,watermark,failure}>; failure latched in Writer RefCell, cleared by next success.
- catalog_upgrade: split `page(db, job, progress, max) -> (WriteBatch, complete)`; step = page+admit+commit. rdf_index: split owner_graph page builder similarly.
- Online page bounds smaller (writer lock held per page); measure.
- done/total: count provenance-rdf:record: markers (empty values) vs watermark; owner: count rdf:g: keys vs frontier.
- Readers: view.scoped = restricts_membership && owner checkpoint == [0] in the view snapshot. #20: RdfScopeCoverage.event_fallback_pending (records, metadata layer, event_time Unknown, graph key > frontier and manifest rows lack eventTimeFallback); complete() false when >0; strict -> INDEX_NOT_READY. Wire SparqlCoverage.eventFallbackPending @14, agent render.
- Status wire: Status.migrations @53 :List(MigrationStatus); schema stays 69 (69 never deployed; live on 68). Agent status render `migrations`.
- Maintenance: Job::Migrate; continuation via yield while pages remain; failure -> retry on 60 s tick; eprintln progress every 64 pages + completion.
- Query-only: fence+seed at open (like all open upgrades), no pages (no worker); status shows pending.
## Progress (agent)
- Done: migration.rs, catalog_upgrade::page/progress, rdf_index owner page split + any_key, event_fallback fence/page/counts/carries_event_time, rdf_view reader paths (owner_index, FallbackPending), coverage.event_fallback_pending (correlation excluded; raw SPARQL complete=false), capnp Status.migrations @53 + SparqlCoverage.eventFallbackPending @14, rpc/agent render, maintenance Job::Migrate + journal, test-support pending_schema_69_migrations, core test open_serves_before..., maintenance test, rpc query-only extension, docs interfaces/architecture/deployment.
- Fresh stores seed migrations complete when source empty.
- Temporary probe: crates/sparrow-core/tests/zz_profile_probe.rs (DELETE before handoff).
- Remaining: core test rerun (logs/test-core3.log), server tests, fmt, clippy, profile run, implementation.md.
