// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One joined, serial maintenance owner; RPC retains its own request budget.

use sparrow_core::{
    Error, EvictionLimits, Migration, MigrationPage, RECOMPRESSION_SETTLE_SECONDS, Store,
};
use std::time::Duration;
use tokio::sync::watch;

/// Pause after each complete expiry traversal. Expiry affects reads immediately,
/// so this only delays physical retirement. One traversal parses the whole
/// catalog under the writer lock; restarting it every second starved ingest.
const EXPIRY_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// Pause between table rewrite steps. A step only observes progress and turns
/// native periodic compaction on or off, so it never holds the loop for long.
const RECOMPRESSION_INTERVAL: Duration = Duration::from_secs(5);
/// Retry interval of an online migration whose last page failed. While pages succeed the
/// next one is admitted as a continuation, fairly with the other duties.
const MIGRATION_RETRY_INTERVAL: Duration = Duration::from_secs(60);
/// Pages between journal progress lines of one online migration.
const MIGRATION_PROGRESS_PAGES: u64 = 64;

pub(super) struct Worker {
    stop: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<Result<(), String>>>,
    /// Closes when the task ends, by return, panic or cancellation.
    ended: tokio::sync::oneshot::Receiver<()>,
}

impl Worker {
    pub(super) fn start(store: Store, flush: Duration) -> Self {
        let (stop, stopped) = watch::channel(false);
        let (alive, ended) = tokio::sync::oneshot::channel::<()>();
        Self {
            stop,
            task: Some(tokio::spawn(async move {
                let _alive = alive;
                run(store, stopped, flush).await
            })),
            ended,
        }
    }

    /// Resolve when the task ends. Await it at most once.
    pub(super) async fn ended(&mut self) {
        // The task never sends: its sender closes as it ends, the only outcome.
        let _ = (&mut self.ended).await;
    }

    pub(super) async fn shutdown(mut self) -> Result<(), String> {
        self.stop.send_replace(true);
        self.task
            .take()
            .ok_or("maintenance join owner missing")?
            .await
            .map_err(|error| format!("maintenance join failed: {error}"))?
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Never abort a native write; the joined loop stops after its completion.
        self.stop.send_replace(true);
    }
}

#[derive(Clone, Copy)]
enum Job {
    Flush,
    Pressure,
    Reclaim,
    Expiry,
    Recompress,
    /// One migration page. With `defer`, the page first waits inline for writers waiting on
    /// the writer lock: the first page after a retry tick, and a page whose background
    /// deferral finished while another duty ran, so writers may have arrived since.
    Migrate {
        defer: bool,
    },
}

impl Job {
    fn name(self) -> &'static str {
        match self {
            Self::Flush => "buffered WAL synchronization",
            Self::Pressure => "pressure maintenance",
            Self::Reclaim => "import journal reclamation",
            Self::Expiry => "retention maintenance",
            Self::Recompress => "table rewrite",
            Self::Migrate { .. } => "online migration",
        }
    }

    /// Only expiry and online migration change their continuation; a migration reports
    /// its page. Other duties cannot lose that work.
    fn execute(self, store: &Store) -> Result<Outcome, Error> {
        let done = Outcome::Continue(None);
        match self {
            Self::Flush => store.flush_buffered().map(|_| done),
            Self::Pressure => store
                .pressure_maintain(sparrow_server::now()?, EvictionLimits::default())
                .map(|_| done),
            // Published journals hold one of 16 import slots until reclaimed.
            Self::Reclaim => store.import_reclaim(sparrow_server::now()?).map(|_| done),
            Self::Expiry => store
                .expire_bounded(sparrow_server::now()?, EvictionLimits::default())
                .map(|report| Outcome::Continue(Some(report.continuation))),
            Self::Recompress => store
                .recompress_step(sparrow_server::now()?, RECOMPRESSION_SETTLE_SECONDS)
                .map(|_| done),
            Self::Migrate { defer } => {
                // The page's time includes its wait for writers, as the migration's does.
                let began = std::time::Instant::now();
                if defer {
                    store.defer_migration_page();
                }
                store
                    .migrate_step()
                    .map(|page| Outcome::Migrated(page, began))
            }
        }
    }
}

/// Wait for the deferral before the next page. Polling it is cancel-safe: a duty that wins
/// the select leaves the deferral running, and the next poll resumes the same wait.
async fn deferred_page(
    deferral: &mut Option<tokio::task::JoinHandle<std::time::Duration>>,
) -> Result<(), String> {
    if let Some(handle) = deferral.as_mut() {
        handle
            .await
            .map_err(|error| format!("migration page deferral join failed: {error}"))?;
        *deferral = None;
    }
    Ok(())
}

enum Outcome {
    Continue(Option<bool>),
    /// The page, and when its job began.
    Migrated(Option<MigrationPage>, std::time::Instant),
}

/// Journal lines of the online migrations: a start line at each migration's first committed
/// page of the session, progress every few pages, and completion, each timed from the start
/// of that first page (#176).
#[derive(Default)]
struct MigrationJournal {
    pages: [u64; Migration::ALL.len()],
    started: [Option<std::time::Instant>; Migration::ALL.len()],
}

impl MigrationJournal {
    /// Journal one committed page from the counters its own batch updated; it reads
    /// nothing more and never waits for the writer. `began` is when the page's job began.
    fn page(&mut self, page: &MigrationPage, began: std::time::Instant) {
        let index = page.migration.index();
        self.pages[index] += 1;
        let pages = self.pages[index];
        let name = page.migration.name();
        let total = || {
            page.total
                .map_or_else(|| "counting".to_owned(), |total| total.to_string())
        };
        let started = *self.started[index].get_or_insert(began);
        if pages == 1 {
            eprintln!(
                "sparrowd: migration {name} start pages=1 done={} total={}",
                page.done,
                total()
            );
        }
        let elapsed = started.elapsed().as_secs_f64();
        if let Some(failure) = &page.failure {
            // The migration stops on a latched failure, so this logs once.
            eprintln!(
                "sparrowd: migration {name} failed pages={pages} elapsed_seconds={elapsed:.3}: {failure}"
            );
        } else if page.complete {
            eprintln!(
                "sparrowd: migration {name} complete pages={pages} done={} elapsed_seconds={elapsed:.3}",
                page.done
            );
        } else if pages.is_multiple_of(MIGRATION_PROGRESS_PAGES) {
            eprintln!(
                "sparrowd: migration {name} progress pages={pages} done={} total={} watermark={} elapsed_seconds={elapsed:.3}",
                page.done,
                total(),
                page.watermark.as_deref().unwrap_or("none")
            );
        }
    }
}

async fn run(
    store: Store,
    mut stopped: watch::Receiver<bool>,
    flush_interval: Duration,
) -> Result<(), String> {
    let mut flush = tokio::time::interval(flush_interval);
    let mut pressure = tokio::time::interval(Duration::from_secs(1));
    let mut reclaim = tokio::time::interval(Duration::from_secs(1));
    let mut expiry = tokio::time::interval(EXPIRY_SWEEP_INTERVAL);
    let mut recompress = tokio::time::interval(RECOMPRESSION_INTERVAL);
    let mut migrate = tokio::time::interval(MIGRATION_RETRY_INTERVAL);
    for interval in [
        &mut flush,
        &mut pressure,
        &mut reclaim,
        &mut expiry,
        &mut recompress,
        &mut migrate,
    ] {
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    }
    // Buffered synchronization retains its configured initial delay.
    flush.tick().await;
    let mut continuation = false;
    // Pages continue back to back once the first succeeds, until every migration is
    // complete; a failed page waits for the retry tick, never a busy loop. Immediately
    // before every page the worker waits for writers waiting on the writer lock (#146).
    // After a page that wait runs in a blocking task it polls with its other duties, so it
    // delays only the page. If that task finished while another duty ran, writers may have
    // arrived since, so the page waits again inline, as the first page after a retry does.
    let mut migrating = false;
    let mut deferral: Option<tokio::task::JoinHandle<std::time::Duration>> = None;
    let mut stale = false;
    let mut journal = MigrationJournal::default();
    loop {
        let job = tokio::select! {
            biased;
            _ = stopped.changed() => return Ok(()),
            job = async {
                // Keep duties fair even when a flush interval is shorter than
                // native work. Only stop admission has strict priority.
                tokio::select! {
                    _ = flush.tick() => Ok(Job::Flush),
                    _ = pressure.tick() => Ok(Job::Pressure),
                    _ = reclaim.tick() => Ok(Job::Reclaim),
                    _ = recompress.tick() => Ok(Job::Recompress),
                    _ = expiry.tick(), if !continuation => Ok(Job::Expiry),
                    _ = tokio::task::yield_now(), if continuation => Ok(Job::Expiry),
                    _ = migrate.tick(), if !migrating => Ok(Job::Migrate { defer: true }),
                    deferred = deferred_page(&mut deferral), if migrating => {
                        deferred.map(|()| Job::Migrate { defer: stale })
                    }
                }
            } => job,
        };
        let job = job?;
        // Below the free-space floor every write is refused. Synchronizing
        // accepted writes is still allowed and still needed, and the table
        // rewrite step must run to turn native rewriting off.
        if !matches!(job, Job::Flush | Job::Recompress)
            && store
                .space_floor()
                .shortfall()
                .map_err(|error| format!("free-space check failed: {error}"))?
                .is_some()
        {
            continuation = false;
            migrating = false;
            deferral = None;
            continue;
        }
        let owner = store.clone();
        // Await this complete bounded native job before admitting another duty.
        let (result, latched) = tokio::task::spawn_blocking(move || {
            let result = job.execute(&owner);
            let latched = match &result {
                Ok(_) => Ok(None),
                Err(_) => owner.durability_status().map(|status| status.failure),
            };
            (result, latched)
        })
        .await
        .map_err(|error| format!("{} join failed: {error}", job.name()))?;
        // A failed write leaves durability uncertain until the store is reopened.
        // Ending the daemon lets its supervisor reopen it instead of refusing every write.
        if let Some(failure) =
            latched.map_err(|error| format!("{} status failed: {error}", job.name()))?
        {
            return Err(format!(
                "store durability uncertain; exiting so it is reopened: {failure}"
            ));
        }
        // A deferral that finished while this duty ran no longer reflects the writers
        // waiting now; the page it admits waits again first.
        if !matches!(job, Job::Migrate { .. })
            && deferral
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            stale = true;
        }
        match result {
            Ok(Outcome::Continue(Some(more))) => {
                // The pause runs from the end of a traversal, however long it took.
                if !more {
                    expiry.reset();
                }
                continuation = more;
            }
            Ok(Outcome::Continue(None)) => {}
            Ok(Outcome::Migrated(page, began)) => {
                migrating = page.is_some();
                if migrating {
                    let owner = store.clone();
                    deferral = Some(tokio::task::spawn_blocking(move || {
                        owner.defer_migration_page()
                    }));
                    stale = false;
                }
                if let Some(page) = &page {
                    journal.page(page, began);
                }
            }
            Err(error) => {
                eprintln!("sparrowd: {} failed: {error}", job.name());
                // Retry a refused pass or page on the ordinary tick, never a busy loop.
                match job {
                    Job::Expiry => continuation = false,
                    Job::Migrate { .. } => {
                        migrating = false;
                        migrate.reset();
                    }
                    _ => {}
                }
            }
        }
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn backlog_drains_between_ticks_with_pins_and_buffered_flushes() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let path =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let config = sparrow_server::ServerConfig::read(&path).unwrap();
        let store = config.open_store().unwrap();
        let now = sparrow_server::now().unwrap();
        let old = now - 31 * 86_400_000;
        let input = (0..1024)
            .map(|i| format!("expired record {i}\n"))
            .collect::<String>();
        let artifact = store
            .ingest("backlog", "source", "log", input.as_bytes(), old)
            .unwrap();
        store
            .pin("backlog-pin", "case", &artifact.record_ids[..1], 40, old)
            .unwrap();
        let fresh = store
            .ingest_with_durability(
                "buffered",
                "source",
                "opaque",
                b"buffered",
                now,
                sparrow_core::Durability::Buffered,
            )
            .unwrap();
        assert_eq!(store.durability_status().unwrap().pending_writes, 1);
        assert_eq!(
            store.status(now).unwrap()["unique_payload_chunks"].as_str(),
            Some("1025")
        );
        let worker = Worker::start(store.clone(), Duration::from_millis(1));
        // Thirty-two default passes would take over thirty seconds under the
        // former once-per-second driver. This allows ample host scheduling slack.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if store.status(sparrow_server::now().unwrap()).unwrap()["unique_payload_chunks"]
                    .as_str()
                    == Some("2")
                    && store.durability_status().unwrap().pending_writes == 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        worker.shutdown().await.unwrap();
        assert_eq!(
            store.bytes(&artifact.record_ids[0], 0, 17, now).unwrap(),
            b"expired record 0\n"
        );
        assert_eq!(store.bytes(&fresh.id, 0, 8, now).unwrap(), b"buffered");
        assert_eq!(store.artifact(&artifact.id).unwrap_err().code, "NOT_FOUND");
    }

    #[tokio::test]
    async fn import_journals_are_reclaimed_between_expiry_sweeps() {
        // INVARIANT: a finished import frees its slot within seconds even though
        // complete expiry traversals pause for a minute.
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let path =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let store = sparrow_server::ServerConfig::read(&path)
            .unwrap()
            .open_store()
            .unwrap();
        let worker = Worker::start(store.clone(), Duration::from_millis(1));
        // Let the first traversal finish so the next one is a minute away.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let input = b"cancelled import";
        let payload = format!(
            "blake3:{}",
            purrdf_hash::hex::encode(purrdf_hash::blake3::hash(input).as_bytes())
        );
        let now = sparrow_server::now().unwrap();
        let manifest = store
            .import_begin(
                "cancelled",
                "source",
                "opaque",
                input.len() as u64,
                &payload,
                now,
            )
            .unwrap();
        store.import_cancel(&manifest.id, now).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            // A canceled journal reads as IMPORT_EXPIRED until it is reclaimed.
            while store
                .import_get(&manifest.id, sparrow_server::now().unwrap())
                .err()
                .is_none_or(|error| error.code != "NOT_FOUND")
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            store
                .import_get(&manifest.id, sparrow_server::now().unwrap())
                .unwrap_err()
                .code,
            "NOT_FOUND"
        );
        worker.shutdown().await.unwrap();
    }

    /// INVARIANT: a store opened with both schema 69 migrations, the source summary
    /// migration and a source timeline rebuild pending serves reads while the backfill is part done, and those reads
    /// report the record graphs still pending; the joined worker's pages then finish every
    /// migration, after which the same read reports nothing pending and the per-source
    /// counters equal a full recount.
    #[tokio::test]
    async fn reads_report_pending_mid_migration_and_the_worker_finishes_it() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let path =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let config = sparrow_server::ServerConfig::read(&path).unwrap();
        let store = config.open_store().unwrap();
        let now = sparrow_server::now().unwrap();
        let input = (0..300).map(|i| format!("line {i}\n")).collect::<String>();
        store
            .ingest("migrate", "source", "log", input.as_bytes(), now)
            .unwrap();
        drop(store);
        let db = sparrow_test_support::catalog_db(&data).unwrap();
        sparrow_test_support::pending_schema_69_migrations(&db).unwrap();
        sparrow_test_support::pending_source_summary_migration(&db).unwrap();
        sparrow_test_support::stale_source_context_cache(&db).unwrap();
        drop(db);
        let store = config.open_store().unwrap();
        let pending = store.migrations().unwrap();
        assert!(
            pending.iter().all(|m| !m.complete() && m.total.is_none()),
            "{pending:?}"
        );
        // Commit the worker's pages in its run order (the counters, the source summary, the
        // owner index) through the first backfill page, so the read below lands part-way
        // through the backfill, not before or after it.
        while store.migrate_step().unwrap().unwrap().migration != Migration::EventFallbackRdf {}
        let part = store.migrations().unwrap();
        let (index, fallback) = (
            &part[Migration::OwnerGraphIndex.index()],
            &part[Migration::EventFallbackRdf.index()],
        );
        assert!(index.complete() && !fallback.complete(), "{part:?}");
        assert!(fallback.done > 0, "{part:?}");
        use sparrow_core::QueryScope;
        let scope = sparrow_test_support::record_scope!(QueryScope, "source", 1000);
        let read = |store: &sparrow_core::Store| {
            let governors = purrdf::sparql::QueryGovernors::METERED.with_fuel(10_000_000);
            let result = store
                .sparql_query(
                    &sparrow_core::SparqlRead {
                        text_relations: None,
                        scope: &scope,
                        layers: sparrow_core::RdfGraphLayers::ALL,
                        request: purrdf::SparqlRequest {
                            query: "SELECT * WHERE { ?s ?p ?o } LIMIT 1",
                            base_iri: None,
                            substitutions: &[],
                        },
                        governors: &governors,
                        limits: sparrow_core::RdfReadLimits::default(),
                    },
                    sparrow_server::now().unwrap(),
                    &sparrow_core::QueryCancellation::default(),
                )
                .unwrap();
            assert!(result.outcome.is_ok(), "the read is served");
            result.coverage.event_fallback_pending
        };
        let waiting = read(&store);
        assert_eq!(waiting, fallback.total.unwrap() - fallback.done, "{part:?}");
        let worker = Worker::start(store.clone(), Duration::from_millis(1));
        tokio::time::timeout(Duration::from_secs(30), async {
            while !store
                .migrations()
                .unwrap()
                .iter()
                .all(sparrow_core::MigrationStatus::complete)
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        worker.shutdown().await.unwrap();
        let done = store.migrations().unwrap();
        assert!(
            done.iter()
                .all(|m| m.failure.is_none() && m.total == Some(m.done)),
            "{done:?}"
        );
        assert_eq!(read(&store), 0);
        let (stored, recounted) = store.source_count_audit_for_tests().unwrap();
        assert_eq!(stored, recounted);
        assert_eq!(stored["source"], [300, 1, 0]);
    }

    /// INVARIANT: in the no-intake shape (maintenance, but no consume or syslog worker) the
    /// daemon's health loop, the branch its serving loop exits on, keeps waiting while the
    /// maintenance worker runs and returns the maintenance failure once that loop has ended,
    /// so the daemon exits with an error instead of serving on without maintenance.
    #[tokio::test]
    async fn the_health_loop_fails_a_no_intake_daemon_whose_maintenance_worker_ends() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let path =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let store = sparrow_server::ServerConfig::read(&path)
            .unwrap()
            .open_store()
            .unwrap();
        let mut worker = Worker::start(store, Duration::from_millis(1));
        let (mut consumer, mut syslog) = (None, None);
        // A running worker: a second passes without a failure.
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                crate::worker_health(&mut consumer, &mut syslog, Some(&mut worker)),
            )
            .await
            .is_err()
        );
        // End the loop without consuming the worker, as any unexpected end would.
        worker.stop.send_replace(true);
        let failure = tokio::time::timeout(
            Duration::from_secs(10),
            crate::worker_health(&mut consumer, &mut syslog, Some(&mut worker)),
        )
        .await
        .unwrap();
        assert_eq!(
            failure.to_string(),
            "maintenance worker terminated unexpectedly"
        );
    }

    #[tokio::test]
    async fn shutdown_before_start_preserves_unflushed_work_for_final_fence() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let path =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let config = sparrow_server::ServerConfig::read(&path).unwrap();
        let store = config.open_store().unwrap();
        store
            .ingest_with_durability(
                "stopped-buffer",
                "source",
                "opaque",
                b"buffered",
                sparrow_server::now().unwrap(),
                sparrow_core::Durability::Buffered,
            )
            .unwrap();
        let worker = Worker::start(store.clone(), Duration::from_millis(1));
        tokio::time::timeout(Duration::from_secs(2), worker.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(store.durability_status().unwrap().pending_writes, 1);
        store.flush().unwrap();
        assert_eq!(store.durability_status().unwrap().pending_writes, 0);
    }
}
