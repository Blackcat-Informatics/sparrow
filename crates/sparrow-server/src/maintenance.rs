// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One joined, serial maintenance owner; RPC retains its own request budget.

use crate::pressure_duty;
use sparrow_core::{
    EXPIRY_CEILING_MS, Error, EvictionLimits, ExpiryReport, Migration, MigrationPage,
    PressureStatus, RECOMPRESSION_SETTLE_SECONDS, Store,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, watch};

/// Shortest and longest sleep before the next expiry pass. Expiry affects reads immediately,
/// so the sleep only delays physical retirement. A pass reads only the due prefix of the
/// time index, so the worker sleeps until the next deadline the pass reported; the ceiling
/// protects against a wall clock that stepped forward, since the sleep itself is monotonic,
/// and paces the request-row scan.
const EXPIRY_FLOOR: Duration = Duration::from_secs(1);
const EXPIRY_CEILING: Duration = Duration::from_millis(EXPIRY_CEILING_MS);
/// Retry interval of an expiry pass that failed. The failure's cause has no event: the pass
/// ran and the native layer refused it, and nothing signals that it would now succeed. Its
/// observer is this loop's expiry arm; 60 seconds keeps a persistent failure to one journal
/// line a minute. A pass skipped below the free-space floor does not use it: the floor's
/// resume edge wakes the pass (#247).
const EXPIRY_RETRY: Duration = Duration::from_secs(60);
/// Interval of the read-lease purge: expired read handles hold snapshots until it runs.
const LEASE_INTERVAL: Duration = Duration::from_secs(60);
/// Pause between table rewrite steps. A step only observes progress and turns
/// native periodic compaction on or off, so it never holds the loop for long.
const RECOMPRESSION_INTERVAL: Duration = Duration::from_secs(5);
/// Retry interval of an online migration whose last page failed, with the same reasoning as
/// [`EXPIRY_RETRY`]: a failed page has no event, and this loop's migration arm observes it
/// at this cadence. A page skipped below the free-space floor does not wait for it: the
/// floor's resume edge wakes the migration (#247). While pages succeed the next one is
/// admitted as a continuation, fairly with the other duties.
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
    /// `flush` is the buffered synchronization interval. The only production caller passes
    /// `buffered_flush_interval_ms`, which storage configuration validation bounds to
    /// 1000..60000 ms, so no production timer here fires more than once a second. No floor is
    /// enforced at this entry because this module's tests pass 1 ms to reach the flush duty
    /// without waiting whole seconds; they are the only exception.
    pub(super) fn start(
        store: Store,
        flush: Duration,
        held: sparrow_server::pressure_wake::PressureWake,
    ) -> Self {
        let (stop, stopped) = watch::channel(false);
        let (alive, ended) = tokio::sync::oneshot::channel::<()>();
        Self {
            stop,
            task: Some(tokio::spawn(async move {
                let _alive = alive;
                let result = run(&store, stopped, flush, held).await;
                store.set_retention_waker(None);
                store.set_pressure_request_waker(None);
                store.space_floor().set_resume_waker(None);
                result
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
    Leases,
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
            Self::Leases => "read lease purge",
            Self::Recompress => "table rewrite",
            Self::Migrate { .. } => "online migration",
        }
    }

    /// Only expiry and online migration change their schedule; a migration reports
    /// its page. Other duties cannot lose that work.
    fn execute(self, store: &Store) -> Result<Outcome, Error> {
        let done = Outcome::Continue;
        match self {
            Self::Flush => store.flush_buffered().map(|_| done),
            Self::Pressure => store
                .pressure_maintain(sparrow_server::now()?, EvictionLimits::default())
                .map(|status| Outcome::Pressured(Box::new(status))),
            // Published journals hold one of 16 import slots until reclaimed.
            Self::Reclaim => store.import_reclaim(sparrow_server::now()?).map(|_| done),
            Self::Expiry => {
                let now = sparrow_server::now()?;
                store
                    .expire_bounded(now, EvictionLimits::default())
                    .map(|report| Outcome::Expired(report, now))
            }
            Self::Leases => store
                .purge_read_leases(sparrow_server::now()?)
                .map(|()| done),
            Self::Recompress => store
                .recompress_step(sparrow_server::now()?, RECOMPRESSION_SETTLE_SECONDS)
                .map(|_| done),
            Self::Migrate { defer } => {
                // The page's time includes the wait for writers this job runs inline; a wait
                // that ran in the background beside other duties is not part of it.
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

/// Duties that still run below the free-space floor. Synchronization accepts nothing new,
/// the table rewrite step only turns native rewriting off, and pressure maintenance is what
/// frees space. Every other duty writes more than it frees, so it waits for the floor.
const fn runs_below_floor(job: Job) -> bool {
    matches!(job, Job::Flush | Job::Recompress | Job::Pressure)
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
    Continue,
    /// A pressure pass's status, which sets the next pass and what the daemon journals.
    Pressured(Box<PressureStatus>),
    /// The pass report and the clock reading it ran at.
    Expired(ExpiryReport, u64),
    /// The page, and when its job began.
    Migrated(Option<MigrationPage>, std::time::Instant),
}

/// Journal lines of the online migrations: a start line at each migration's first committed
/// page of the session, then progress every few pages and completion, which carry the time
/// since the start of that first page (#176).
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

/// How long to sleep after an expiry pass that left no due backlog.
fn expiry_delay(report: &ExpiryReport, now: u64) -> Duration {
    report
        .next_due
        .map_or(EXPIRY_CEILING, |due| {
            Duration::from_millis(due.saturating_sub(now))
        })
        .clamp(EXPIRY_FLOOR, EXPIRY_CEILING)
}

/// Account for a failed pressure pass and return the delay to the next one. The store shows
/// `Blocked` until the next pass; the duty journals a new native error at once and a repeat at
/// a bounded rate, and retries on its backoff, never a busy loop. Held frames are woken: a
/// failed pass is an ending they evaluate again, as they do after one that succeeded (#247).
///
/// The backoff (1 s doubling to 30 s) is this loop's observer of native `RocksDB` flush and
/// compaction state, which has no completion hook in the store: the pass reads the pending
/// counts, and waiting less cannot change what native work does. Every event the daemon does
/// signal (pin release or commit, archive commit, forced compaction end, floor resume, a
/// refused write requesting a pass) wakes the pass at once instead.
fn pressure_failed(
    store: &Store,
    duty: &mut pressure_duty::Duty,
    held: &sparrow_server::pressure_wake::PressureWake,
    error: &Error,
) -> Duration {
    held.wake();
    // A status that cannot be read is journaled and counted by the duty.
    let status = store.pressure_status().map_err(|error| error.to_string());
    let verdict = duty.failed(
        sparrow_server::now().unwrap_or_default(),
        &error.to_string(),
        status.as_ref().map_err(String::as_str),
    );
    for line in &verdict.lines {
        eprintln!("{line}");
    }
    verdict.delay
}

/// Observe the free-space floor for `job` and say whether it is paused. The floor's resume
/// edge wakes held frames itself, in whichever observation consumes it (#247). This is also
/// the one observer of the two endings that have no event: free space returning because
/// another process freed it, and native flush or compaction work completing, which the store
/// has no listener for. Its reclaim tick, one a second, is the cadence the daemon measures
/// the floor at; while any frame is held it wakes them all to evaluate again, and no frame
/// owns a timer. It runs before the below-floor skip, because reclamation is skipped exactly
/// while the floor is paused, which is when held frames need the tick.
fn observe_floor(
    store: &Store,
    job: Job,
    held: &sparrow_server::pressure_wake::PressureWake,
) -> Result<bool, String> {
    let paused = store
        .space_floor()
        .observe(sparrow_server::now().map_err(|error| format!("clock failed: {error}"))?)
        .map_err(|error| format!("free-space check failed: {error}"))?
        .is_some();
    if matches!(job, Job::Reclaim) {
        held.observe_tick();
    }
    Ok(paused)
}

/// Skipped-duty counts of the test running on this thread, so a test waits for the skip it
/// exercises as an event instead of sleeping.
#[cfg(test)]
mod skips {
    use super::Job;
    use std::cell::Cell;

    thread_local! {
        static EXPIRY: Cell<u64> = const { Cell::new(0) };
        static MIGRATE: Cell<u64> = const { Cell::new(0) };
    }

    pub(super) fn note(job: Job) {
        match job {
            Job::Expiry => EXPIRY.with(|count| count.set(count.get() + 1)),
            Job::Migrate { .. } => MIGRATE.with(|count| count.set(count.get() + 1)),
            _ => {}
        }
    }

    pub(super) fn expiry() -> u64 {
        EXPIRY.with(Cell::get)
    }

    pub(super) fn migrate() -> u64 {
        MIGRATE.with(Cell::get)
    }
}

async fn run(
    store: &Store,
    mut stopped: watch::Receiver<bool>,
    flush_interval: Duration,
    held: sparrow_server::pressure_wake::PressureWake,
) -> Result<(), String> {
    // A pin release or commit and an archive commit change what expiry may retire; the store
    // notifies this wake without blocking, and a pending wake survives a busy duty.
    // The same events, and the end of a forced compaction, change what a pressure pass can
    // do, so they wake it too instead of leaving it to its backoff (#211).
    let wake = Arc::new(Notify::new());
    let pressure_wake = Arc::new(Notify::new());
    // The floor's resume edge wakes the duties that skipped below it, so none waits out a
    // retry clock for it, and the held frames (#247). The edge is signalled by the floor's
    // own observation, so it reaches this worker whichever loop consumes it. A refused write
    // that requests a pressure pass wakes the pass the same way instead of waiting for its
    // next scheduled time.
    let migrate_wake = Arc::new(Notify::new());
    store
        .space_floor()
        .set_resume_waker(Some(held.resume_waker({
            let wake = Arc::clone(&wake);
            let pressure_wake = Arc::clone(&pressure_wake);
            let migrate_wake = Arc::clone(&migrate_wake);
            move || {
                wake.notify_one();
                pressure_wake.notify_one();
                migrate_wake.notify_one();
            }
        })));
    store.set_pressure_request_waker(Some(Arc::new({
        let pressure_wake = Arc::clone(&pressure_wake);
        move || pressure_wake.notify_one()
    })));
    store.set_retention_waker(Some(Arc::new({
        let wake = Arc::clone(&wake);
        let pressure_wake = Arc::clone(&pressure_wake);
        let held = held.clone();
        move || {
            wake.notify_one();
            pressure_wake.notify_one();
            held.wake();
        }
    })));
    let mut flush = tokio::time::interval(flush_interval);
    let mut reclaim = tokio::time::interval(Duration::from_secs(1));
    let mut leases = tokio::time::interval(LEASE_INTERVAL);
    let mut recompress = tokio::time::interval(RECOMPRESSION_INTERVAL);
    let mut migrate = tokio::time::interval(MIGRATION_RETRY_INTERVAL);
    for interval in [
        &mut flush,
        &mut reclaim,
        &mut leases,
        &mut recompress,
        &mut migrate,
    ] {
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    }
    // Buffered synchronization retains its configured initial delay.
    flush.tick().await;
    let mut continuation = false;
    let mut pressure_duty = pressure_duty::Duty::default();
    let mut pressure_at = tokio::time::Instant::now();
    // The first pass runs at once; each later one at the deadline the previous one reported.
    let mut expiry_at = tokio::time::Instant::now();
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
                    _ = tokio::time::sleep_until(pressure_at) => Ok(Job::Pressure),
                    _ = pressure_wake.notified() => Ok(Job::Pressure),
                    _ = reclaim.tick() => Ok(Job::Reclaim),
                    _ = recompress.tick() => Ok(Job::Recompress),
                    _ = leases.tick() => Ok(Job::Leases),
                    _ = tokio::time::sleep_until(expiry_at), if !continuation => Ok(Job::Expiry),
                    _ = wake.notified(), if !continuation => Ok(Job::Expiry),
                    _ = tokio::task::yield_now(), if continuation => Ok(Job::Expiry),
                    _ = migrate.tick(), if !migrating => Ok(Job::Migrate { defer: true }),
                    _ = migrate_wake.notified(), if !migrating => Ok(Job::Migrate { defer: true }),
                    deferred = deferred_page(&mut deferral), if migrating => {
                        deferred.map(|()| Job::Migrate { defer: stale })
                    }
                }
            } => job,
        };
        let job = job?;
        // Below the free-space floor every write is refused. Synchronizing accepted writes is
        // still allowed and still needed, and the table rewrite step must run to turn native
        // rewriting off. Pressure maintenance runs too: it is what frees space, and it keeps
        // its own check that the maintenance reserve is free (#211). The floor is observed
        // on every duty so a pause and its end are counted in status.
        let paused = observe_floor(store, job, &held)?;
        if paused && !runs_below_floor(job) {
            continuation = false;
            migrating = false;
            deferral = None;
            // Expiry waits for the floor's resume edge, which wakes it; the ceiling is only the
            // wall-clock bound every idle expiry sleep already has.
            if matches!(job, Job::Expiry) {
                expiry_at = tokio::time::Instant::now() + EXPIRY_CEILING;
            }
            #[cfg(test)]
            skips::note(job);
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
            Ok(Outcome::Expired(report, now)) => {
                // Only real due backlog continues without sleeping; otherwise sleep until
                // the next deadline, however long the pass took.
                if report.anomalies != 0 {
                    eprintln!(
                        "sparrowd: retention saw {} entries whose own deadline differs from receipt plus retention; honoured at their own deadline",
                        report.anomalies
                    );
                }
                continuation = report.continuation;
                expiry_at = tokio::time::Instant::now()
                    + if continuation {
                        Duration::ZERO
                    } else {
                        expiry_delay(&report, now)
                    };
            }
            Ok(Outcome::Continue) => {}
            Ok(Outcome::Pressured(status)) => {
                held.wake();
                let verdict = pressure_duty.observe(&status);
                for line in &verdict.lines {
                    eprintln!("{line}");
                }
                pressure_at = tokio::time::Instant::now() + verdict.delay;
            }
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
            Err(error) if matches!(job, Job::Pressure) => {
                pressure_at = tokio::time::Instant::now()
                    + pressure_failed(store, &mut pressure_duty, &held, &error);
            }
            Err(error) => {
                eprintln!("sparrowd: {} failed: {error}", job.name());
                // Retry a refused pass or page on the ordinary tick, never a busy loop.
                match job {
                    Job::Expiry => {
                        continuation = false;
                        expiry_at = tokio::time::Instant::now() + EXPIRY_RETRY;
                    }
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

    // Value: below the free-space floor the daemon used to freeze the whole pressure state
    // machine exactly when the disk was nearly full (#211). Protects: pressure maintenance
    // is a duty that runs below the floor, beside the two that never add data, while
    // every duty that writes more than it frees (expiry, import reclamation, leases,
    // online migration) still waits for the floor.
    #[test]
    fn only_duties_that_free_space_or_add_nothing_run_below_the_floor() {
        assert!(runs_below_floor(Job::Pressure));
        assert!(runs_below_floor(Job::Flush));
        assert!(runs_below_floor(Job::Recompress));
        for job in [
            Job::Reclaim,
            Job::Expiry,
            Job::Leases,
            Job::Migrate { defer: false },
            Job::Migrate { defer: true },
        ] {
            assert!(!runs_below_floor(job), "{}", job.name());
        }
    }

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
        let worker = Worker::start(store.clone(), Duration::from_millis(1), Default::default());
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
        let worker = Worker::start(store.clone(), Duration::from_millis(1), Default::default());
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

    /// INVARIANT (#183): after a pass that leaves nothing due the worker sleeps until the next
    /// reported deadline, up to an hour, yet a pin release wakes it at once, so evidence a pin
    /// held retires within seconds of the release and not an hour later.
    #[tokio::test]
    async fn a_pin_release_wakes_the_retention_duty_from_its_long_sleep() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let path =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let store = sparrow_server::ServerConfig::read(&path)
            .unwrap()
            .open_store()
            .unwrap();
        let now = sparrow_server::now().unwrap();
        let old = now - 31 * 86_400_000;
        let artifact = store
            .ingest("held-until-release", "source", "log", b"held\n", old)
            .unwrap();
        let pin = store
            .pin(
                "held-pin",
                "case",
                std::slice::from_ref(&artifact.id),
                40,
                old,
            )
            .unwrap();
        let worker = Worker::start(store.clone(), Duration::from_millis(1), Default::default());
        // Let the first pass finish: it holds the entry and schedules the next pass far away.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(store.artifact(&artifact.id).is_ok(), "a pinned entry stays");
        store.release(&pin.id).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while store.artifact(&artifact.id).is_ok() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        worker.shutdown().await.unwrap();
    }

    /// A store holding 300 records whose owner index, source summary and source timeline
    /// migrations are pending, reopened so the worker's pages must run them.
    fn store_with_pending_migrations(dir: &std::path::Path) -> Store {
        let data = dir.join("store");
        let path = sparrow_test_support::write_server_config(dir, &data, &data.join("rpc.sock"));
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
        config.open_store().unwrap()
    }

    /// INVARIANT: a store opened with the owner index, the source summary migration and a
    /// source timeline rebuild pending serves reads mid-migration, and those reads derive and
    /// count the record graph's receipt-time fallback facts; the joined worker's pages then
    /// finish every migration, after which the same read answers the same and the per-source
    /// counters equal a full recount.
    #[tokio::test]
    async fn reads_report_pending_mid_migration_and_the_worker_finishes_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_with_pending_migrations(dir.path());
        let pending = store.migrations().unwrap();
        assert!(
            pending.iter().all(|m| !m.complete() && m.total.is_none()),
            "{pending:?}"
        );
        // Commit the worker's pages in its run order (the counters, the source summary, the
        // owner index) through the owner index, so the read below lands before the source
        // timeline rebuild.
        while store.migrate_step().unwrap().unwrap().migration != Migration::OwnerGraphIndex {}
        let part = store.migrations().unwrap();
        let index = &part[Migration::OwnerGraphIndex.index()];
        assert!(index.complete(), "{part:?}");
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
        // The reader derives the fallback facts for each of the 300 record graphs whichever
        // migration is pending, and counts them.
        assert_eq!(read(&store), 300, "{part:?}");
        let worker = Worker::start(store.clone(), Duration::from_millis(1), Default::default());
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
        assert_eq!(read(&store), 300);
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
        let mut worker = Worker::start(store, Duration::from_millis(1), Default::default());
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

    /// Value (#247): below the free-space floor expiry used to retry on a 60 second clock,
    /// and the floor's resume was seen only by whichever loop observed it first; the daemon's
    /// main loop does the same observation every second, so the resume could pass unseen by
    /// maintenance. Protects: the pause is observed through the real measured free space,
    /// the edge is consumed by an observer other than the worker (as the main loop does), and
    /// expiry still runs within seconds of it, from the edge itself.
    #[tokio::test]
    async fn the_floor_resume_wakes_expiry_even_when_another_loop_consumes_the_edge() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let path =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let store = sparrow_server::ServerConfig::read(&path)
            .unwrap()
            .open_store()
            .unwrap();
        let now = sparrow_server::now().unwrap();
        let old = now - 31 * 86_400_000;
        let artifact = store
            .ingest("expired-below-floor", "source", "log", b"old\n", old)
            .unwrap();
        let floor = store.space_floor();
        floor.set_free_for_tests(Some(0));
        assert!(floor.observe(now).unwrap().is_some());
        let worker = Worker::start(store.clone(), Duration::from_millis(1), Default::default());
        // The first expiry pass runs at once and is skipped below the floor.
        tokio::time::timeout(Duration::from_secs(30), async {
            while skips::expiry() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("expiry was never skipped below the floor");
        assert!(
            store.artifact(&artifact.id).is_ok(),
            "expiry ran below the floor"
        );
        floor.set_free_for_tests(None);
        // This thread plays the main loop: it consumes the edge before the worker can.
        assert!(
            floor
                .observe(sparrow_server::now().unwrap())
                .unwrap()
                .is_none()
        );
        tokio::time::timeout(Duration::from_secs(10), async {
            while store.artifact(&artifact.id).is_ok() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("expiry waited out a retry clock after the floor resumed");
        worker.shutdown().await.unwrap();
    }

    /// Value (#247): an online migration page skipped below the floor waited out the 60 second
    /// retry tick. Protects: the floor's resume edge, consumed by another observer, runs the
    /// pending migrations to completion within seconds.
    #[tokio::test]
    async fn the_floor_resume_wakes_a_migration_skipped_below_the_floor() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_with_pending_migrations(dir.path());
        let floor = store.space_floor();
        floor.set_free_for_tests(Some(0));
        assert!(
            floor
                .observe(sparrow_server::now().unwrap())
                .unwrap()
                .is_some()
        );
        let worker = Worker::start(store.clone(), Duration::from_millis(1), Default::default());
        // The first migration tick runs at once and is skipped below the floor.
        tokio::time::timeout(Duration::from_secs(30), async {
            while skips::migrate() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("migration was never skipped below the floor");
        assert!(
            store.migrations().unwrap().iter().all(|m| !m.complete()),
            "a migration ran below the floor"
        );
        floor.set_free_for_tests(None);
        assert!(
            floor
                .observe(sparrow_server::now().unwrap())
                .unwrap()
                .is_none()
        );
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
        .expect("migration waited out its retry tick after the floor resumed");
        worker.shutdown().await.unwrap();
    }

    /// Value (#247): the held frames' only wake for endings with no event (free space
    /// returning, native work completing) is the reclaim observation. Protects, counting the
    /// observation in isolation from the pressure pass's own wake: a reclaim observation while
    /// the floor is paused and no event fired wakes a held frame; with none held it sends
    /// nothing; and no other duty ticks.
    #[test]
    fn the_reclaim_observation_wakes_held_frames_below_the_floor_and_only_while_one_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let path =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let store = sparrow_server::ServerConfig::read(&path)
            .unwrap()
            .open_store()
            .unwrap();
        store.space_floor().set_free_for_tests(Some(0));
        let held = sparrow_server::pressure_wake::PressureWake::default();
        assert!(observe_floor(&store, Job::Reclaim, &held).unwrap());
        assert_eq!(held.sent(), 0, "nothing is held");
        let waiter = held.subscribe();
        let frame = waiter.hold();
        assert!(observe_floor(&store, Job::Flush, &held).unwrap());
        assert_eq!(held.sent(), 0, "only the reclaim observation ticks");
        assert!(observe_floor(&store, Job::Reclaim, &held).unwrap());
        assert_eq!(held.sent(), 1, "the tick wakes a held frame while paused");
        drop(frame);
        assert!(observe_floor(&store, Job::Reclaim, &held).unwrap());
        assert_eq!(held.sent(), 1, "the frame was released");
    }

    /// Value (#247): a failed pressure pass is an ending a held syslog frame must evaluate
    /// again; it used to set the backoff only, leaving the frame to its own clock. Protects:
    /// the failure wakes the held frames and still returns the duty's backoff delay.
    #[test]
    fn a_failed_pressure_pass_wakes_the_held_frames_and_backs_off() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let path =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let store = sparrow_server::ServerConfig::read(&path)
            .unwrap()
            .open_store()
            .unwrap();
        let held = sparrow_server::pressure_wake::PressureWake::default();
        let delay = pressure_failed(
            &store,
            &mut pressure_duty::Duty::default(),
            &held,
            &Error::new("STORAGE_FAILURE", "native pass failure"),
        );
        assert_eq!(held.sent(), 1);
        assert!(delay >= pressure_duty::BASE_DELAY && delay <= pressure_duty::BACKOFF_CEILING);
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
        let worker = Worker::start(store.clone(), Duration::from_millis(1), Default::default());
        tokio::time::timeout(Duration::from_secs(2), worker.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(store.durability_status().unwrap().pending_writes, 1);
        store.flush().unwrap();
        assert_eq!(store.durability_status().unwrap().pending_writes, 0);
    }
}
