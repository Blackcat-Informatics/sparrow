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
/// line a minute. A pass refused below the maintenance reserve does not use it: the floor's
/// resume edge wakes the pass (#247, #267).
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
        Self::start_paced(store, flush, held, Pace::DAEMON)
    }

    /// [`Worker::start`] with the pacing of a standing durability latch named.
    pub(super) fn start_paced(
        store: Store,
        flush: Duration,
        held: sparrow_server::pressure_wake::PressureWake,
        pace: Pace,
    ) -> Self {
        let (stop, stopped) = watch::channel(false);
        let (alive, ended) = tokio::sync::oneshot::channel::<()>();
        Self {
            stop,
            task: Some(tokio::spawn(async move {
                let _alive = alive;
                let result = run(&store, stopped, flush, held, pace).await;
                store.set_retention_waker(None);
                store.set_pressure_request_waker(None);
                store.set_import_reclaim_waker(None);
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
    /// its page. Other duties cannot lose that work. `paused` says the free-space floor is
    /// crossed: expiry then runs under the maintenance reserve and without import
    /// reclamation (#267).
    fn execute(self, store: &Store, paused: bool) -> Result<Outcome, Error> {
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
                let limits = EvictionLimits::default();
                if paused {
                    store.expire_below_floor(now, limits)
                } else {
                    store.expire_bounded(now, limits)
                }
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
/// the table rewrite step only turns native rewriting off, pressure maintenance is what
/// frees space, expiry (ageing out) retires evidence past retention in the same small bounded
/// batches under the maintenance reserve, and the lease purge writes nothing and ends the read
/// snapshots a pressure pass stalls on (#267). Import reclamation and online migration still
/// wait for the floor: a migration page rewrites data and writes more than it frees, and
/// import reclamation is not scoped into this change.
const fn runs_below_floor(job: Job) -> bool {
    matches!(
        job,
        Job::Flush | Job::Recompress | Job::Pressure | Job::Expiry | Job::Leases
    )
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

/// When a failed expiry pass is tried again, and whether the failure is journaled.
struct ExpiryRetry {
    after: Duration,
    journal: bool,
}

/// What follows a failed expiry pass. Below the free-space floor, a refusal typed as the
/// maintenance reserve's ([`Error::is_reserve_refusal`]; a budget `STORAGE_PRESSURE` from an
/// inner commit is not one) is free space under the reserve: not a failure, and it has
/// events: the floor's resume edge, and the pressure pass asking again when free space clears
/// the reserve. It takes no retry clock and no journal line per attempt (the pressure duty's
/// stall text reports the reserve); `EXPIRY_CEILING` is only the wall-clock bound every idle
/// expiry sleep already has. Any other failure has no event and retries on [`EXPIRY_RETRY`],
/// journaled.
fn expiry_retry(paused: bool, error: &Error) -> ExpiryRetry {
    if paused && error.is_reserve_refusal() {
        ExpiryRetry {
            after: EXPIRY_CEILING,
            journal: false,
        }
    } else {
        ExpiryRetry {
            after: EXPIRY_RETRY,
            journal: true,
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
        #[cfg(test)]
        skips::observed_reclaim();
        held.observe_tick();
    }
    Ok(paused)
}

/// When the worker proves a standing durability latch, and when it gives up and exits.
///
/// A proof writes every memtable to a table file, which competes with the background work
/// RocksDB runs to resume a handle a full disk stopped; repeated proofs through that window
/// were observed to keep it from resuming. The worker therefore leaves the store alone while
/// free space is below RocksDB's recovery buffer, then waits a few of RocksDB's 5 s polls,
/// proves once, and spaces further proofs by a growing interval.
#[derive(Clone, Copy, Debug)]
pub(super) struct Pace {
    /// How long free space must stay adequate before the first proof: three polls of
    /// RocksDB's 5 s file manager.
    pub(super) grace: Duration,
    /// The wait after the first failed proof; it doubles after each failure.
    pub(super) first: Duration,
    /// The longest wait between proofs.
    pub(super) longest: Duration,
    /// How long the latch may stand with free space adequate before the daemon exits so the
    /// store is reopened. A resume takes one to two polls once space is back, so 120 s (twenty
    /// four polls, with the proofs at 15 s, 45 s and 105 s) leaves a working resume ample time,
    /// and a handle that does not resume by then never will.
    pub(super) bound: Duration,
}

impl Pace {
    /// The daemon's pacing.
    pub(super) const DAEMON: Self = Self {
        grace: Duration::from_secs(15),
        first: Duration::from_secs(30),
        longest: Duration::from_secs(60),
        bound: Duration::from_secs(120),
    };

    /// The wait after the `failures`-th failed proof.
    fn after(&self, failures: u32) -> Duration {
        self.first
            .saturating_mul(1_u32 << failures.saturating_sub(1).min(16))
            .min(self.longest)
    }
}

/// What the worker does next about a standing durability latch.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    /// Leave the store alone.
    Wait,
    /// Prove every accepted write durable now.
    Prove,
    /// End the daemon so its supervisor reopens the store.
    Exit,
}

/// The schedule of proofs of one standing latch.
#[derive(Default)]
struct Proofs {
    /// When free space last became adequate, `None` while it is low.
    adequate_since: Option<tokio::time::Instant>,
    failures: u32,
    last: Option<tokio::time::Instant>,
    /// Whether a latch has been seen standing, so every later duty reads it again.
    active: bool,
}

impl Proofs {
    /// Whether the latch must be read after a duty. A failed duty may have met a new latch,
    /// and while one stands every duty reads it again: a successful duty says nothing about
    /// it, and taking it for a cleared latch would restart the schedule of proofs on every
    /// success.
    fn must_read(&self, duty_failed: bool) -> bool {
        duty_failed || self.active
    }

    /// Record what reading the latch found. Not latched forgets the schedule.
    fn read(&mut self, latched: bool) {
        if latched {
            self.active = true;
        } else {
            *self = Self::default();
        }
    }

    /// The step at `now`, given whether measured free space is below RocksDB's recovery
    /// buffer. Free space falling low again restarts the wait.
    fn step(&mut self, pace: &Pace, now: tokio::time::Instant, space_low: bool) -> Step {
        if space_low {
            *self = Self::default();
            return Step::Wait;
        }
        let elapsed = now.saturating_duration_since(*self.adequate_since.get_or_insert(now));
        if elapsed >= pace.bound {
            return Step::Exit;
        }
        if elapsed < pace.grace {
            return Step::Wait;
        }
        match self.last {
            Some(last) if now.saturating_duration_since(last) < pace.after(self.failures) => {
                Step::Wait
            }
            _ => Step::Prove,
        }
    }

    /// Record a proof that failed at `now`.
    fn failed(&mut self, now: tokio::time::Instant) {
        self.failures += 1;
        self.last = Some(now);
    }
}

/// Why a proof of durability fails, named for the journal and the exit line.
#[derive(Debug, PartialEq, Eq)]
enum Cause {
    /// The filesystem is out of space.
    NoSpace,
    /// Any other failure.
    Other,
}

impl Cause {
    fn name(&self) -> &'static str {
        match self {
            Self::NoSpace => "no space left on the store's filesystem",
            Self::Other => "an I/O error",
        }
    }
}

/// Name the cause of a latch. Measured free space below RocksDB's recovery buffer is
/// no-space whatever RocksDB's message says; a message naming ENOSPC (either wording) is
/// no-space as well. The name only labels the journal and the exit line.
fn cause(message: &str, space_low: bool) -> Cause {
    let lower = message.to_ascii_lowercase();
    if space_low || lower.contains("no space") || lower.contains("enospc") {
        Cause::NoSpace
    } else {
        Cause::Other
    }
}

/// Whether durability is latched, read without taking the writer.
fn latched(store: &Store) -> Result<bool, String> {
    store
        .durability_status()
        .map(|status| status.failure.is_some())
        .map_err(|error| error.to_string())
}

/// Skipped-duty counts of the test running on this thread, so a test waits for the skip it
/// exercises as an event instead of sleeping.
/// One structured line for an expiry pass that retired or held evidence. The counts are the
/// pass's own; `archive_waiting` is the due entry the archive gate held back.
fn journal_expiry(report: &ExpiryReport, floor_paused: bool) {
    if report.records == 0
        && report.artifacts == 0
        && report.deleted == 0
        && report.archive_waiting == 0
    {
        return;
    }
    eprintln!(
        "sparrowd: expiry pass records={} artifacts={} payload_refs={} deleted={} archive_waiting={} floor_paused={floor_paused}",
        report.records,
        report.artifacts,
        report.payload_refs,
        report.deleted,
        report.archive_waiting
    );
    tracing::info!(
        records = report.records,
        artifacts = report.artifacts,
        payload_refs = report.payload_refs,
        deleted = report.deleted,
        archive_waiting = report.archive_waiting,
        floor_paused,
        "expiry pass"
    );
}

/// One structured line per eviction pass that retired evidence. The status repeats its last
/// pass until another runs, so `seen` holds the pass count already journaled.
fn journal_eviction(status: &PressureStatus, seen: &mut u64) {
    if status.passes == *seen {
        return;
    }
    *seen = status.passes;
    let Some(pass) = &status.last_pass else {
        return;
    };
    eprintln!(
        "sparrowd: eviction pass={} records={} artifacts={} recordless_artifacts={} payload_refs={} protected={} frontier={:?} evicted_records_total={}",
        status.passes,
        pass.records.len(),
        pass.artifacts.len(),
        pass.recordless_artifacts,
        pass.payload_refs,
        pass.protected,
        pass.frontier,
        status.evicted_records
    );
    tracing::info!(
        pass = status.passes,
        records = pass.records.len(),
        artifacts = pass.artifacts.len(),
        recordless_artifacts = pass.recordless_artifacts,
        payload_refs = pass.payload_refs,
        protected = pass.protected,
        frontier = ?pass.frontier,
        evicted_records_total = status.evicted_records,
        "eviction pass"
    );
}

#[cfg(test)]
mod skips {
    use super::Job;
    use std::cell::Cell;
    use std::rc::Rc;
    use tokio::sync::Notify;

    thread_local! {
        static EXPIRY: Cell<u64> = const { Cell::new(0) };
        static MIGRATE: Cell<u64> = const { Cell::new(0) };
        static EXPIRED: Cell<u64> = const { Cell::new(0) };
        static PASS: Rc<Notify> = Rc::new(Notify::new());
        static MIGRATE_TICKS: Cell<u64> = const { Cell::new(0) };
        static MIGRATE_WAKES: Cell<u64> = const { Cell::new(0) };
        static RECLAIM_TICKS: Cell<u64> = const { Cell::new(0) };
        static RECLAIM_WAKES: Cell<u64> = const { Cell::new(0) };
        static RECLAIM: Cell<u64> = const { Cell::new(0) };
        static RECLAIM_OBSERVED: Cell<u64> = const { Cell::new(0) };
    }

    /// The reclaim observation ran (inside `observe_floor`).
    pub(super) fn observed_reclaim() {
        RECLAIM_OBSERVED.with(|count| count.set(count.get() + 1));
    }

    pub(super) fn reclaim_observed() -> u64 {
        RECLAIM_OBSERVED.with(Cell::get)
    }

    /// A migration page was started by the retry tick (`false`) or by a wake (`true`).
    pub(super) fn migrate_started(woken: bool) {
        let counter = if woken {
            &MIGRATE_WAKES
        } else {
            &MIGRATE_TICKS
        };
        counter.with(|count| count.set(count.get() + 1));
    }

    pub(super) fn migrate_wakes() -> u64 {
        MIGRATE_WAKES.with(Cell::get)
    }

    /// A reclaim pass was started by the interval tick (`false`) or by a wake (`true`).
    pub(super) fn reclaim_started(woken: bool) {
        let counter = if woken {
            &RECLAIM_WAKES
        } else {
            &RECLAIM_TICKS
        };
        counter.with(|count| count.set(count.get() + 1));
    }

    pub(super) fn reclaim_ticks() -> u64 {
        RECLAIM_TICKS.with(Cell::get)
    }

    pub(super) fn reclaim_wakes() -> u64 {
        RECLAIM_WAKES.with(Cell::get)
    }

    /// The worker's loop finished one turn (a duty ran, or was skipped below the floor). Tests
    /// that wait for the store to change block on this event instead of polling it.
    pub(super) fn pass_done() {
        PASS.with(|pass| pass.notify_one());
    }

    pub(super) fn passes() -> Rc<Notify> {
        PASS.with(Rc::clone)
    }

    /// An expiry pass finished and scheduled the next one.
    pub(super) fn expired_pass() {
        EXPIRED.with(|count| count.set(count.get() + 1));
    }

    pub(super) fn expired() -> u64 {
        EXPIRED.with(Cell::get)
    }

    pub(super) fn reclaim() -> u64 {
        RECLAIM.with(Cell::get)
    }

    pub(super) fn note(job: Job) {
        pass_done();
        match job {
            Job::Reclaim => RECLAIM.with(|count| count.set(count.get() + 1)),
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
    pace: Pace,
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
    // An explicit import cancel, a publication and the retirement of a claim with a begun
    // import each make a journal reclaimable, and a pass that left a journal unfinished asks for
    // the next one; the store wakes this duty for each (#249). The duty's one-second interval
    // stays only as the observer of a lease that merely expires, which has no event, and the
    // floor's resume edge below wakes a pass that was skipped below the floor.
    let reclaim_wake = Arc::new(Notify::new());
    store.set_import_reclaim_waker(Some(Arc::new({
        let reclaim_wake = Arc::clone(&reclaim_wake);
        move || reclaim_wake.notify_one()
    })));
    // Unsubscribes when this duty loop returns.
    let _resume = store.space_floor().subscribe_resume(held.resume_waker({
        let wake = Arc::clone(&wake);
        let pressure_wake = Arc::clone(&pressure_wake);
        let migrate_wake = Arc::clone(&migrate_wake);
        let reclaim_wake = Arc::clone(&reclaim_wake);
        move || {
            reclaim_wake.notify_one();
            wake.notify_one();
            pressure_wake.notify_one();
            migrate_wake.notify_one();
        }
    }));
    // A floor refusal also asks ageing out to run: it is the first avenue of a floor episode
    // (#267), so the refusal wakes expiry as well as the pass.
    store.set_pressure_request_waker(Some(Arc::new({
        let pressure_wake = Arc::clone(&pressure_wake);
        let wake = Arc::clone(&wake);
        move || {
            wake.notify_one();
            pressure_wake.notify_one();
        }
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
    let mut journaled_eviction_passes = 0_u64;
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
    // The proofs of the standing latch and the last failure they met.
    let mut proofs = Proofs::default();
    let mut proof_failure: Option<String> = None;
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
                    _ = reclaim.tick() => {
                        #[cfg(test)]
                        skips::reclaim_started(false);
                        Ok(Job::Reclaim)
                    }
                    _ = reclaim_wake.notified() => {
                        #[cfg(test)]
                        skips::reclaim_started(true);
                        Ok(Job::Reclaim)
                    }
                    _ = recompress.tick() => Ok(Job::Recompress),
                    _ = leases.tick() => Ok(Job::Leases),
                    _ = tokio::time::sleep_until(expiry_at), if !continuation => Ok(Job::Expiry),
                    _ = wake.notified(), if !continuation => Ok(Job::Expiry),
                    _ = tokio::task::yield_now(), if continuation => Ok(Job::Expiry),
                    _ = migrate.tick(), if !migrating => {
                        #[cfg(test)]
                        skips::migrate_started(false);
                        Ok(Job::Migrate { defer: true })
                    }
                    _ = migrate_wake.notified(), if !migrating => {
                        #[cfg(test)]
                        skips::migrate_started(true);
                        Ok(Job::Migrate { defer: true })
                    }
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
            #[cfg(test)]
            skips::note(job);
            continue;
        }
        let owner = store.clone();
        // Await this complete bounded native job before admitting another duty.
        let joined = tokio::task::spawn_blocking(move || job.execute(&owner, paused)).await;
        // A duty that panicked ends only itself: a writer it held is recovered by the next
        // operation that takes the writer, and the duty runs again at its next turn.
        let result = match joined {
            Ok(outcome) => outcome,
            Err(error) => {
                eprintln!(
                    "sparrowd: {} panicked; the next duty recovers the writer: {error}",
                    job.name()
                );
                continuation = false;
                migrating = false;
                deferral = None;
                continue;
            }
        };
        let latched = if proofs.must_read(result.is_err()) {
            let owner = store.clone();
            tokio::task::spawn_blocking(move || latched(&owner))
                .await
                .map_err(|error| format!("{} latch read join failed: {error}", job.name()))?
                .map_err(|error| format!("{}: {error}", job.name()))?
        } else {
            false
        };
        let first_sight = latched && !proofs.active;
        proofs.read(latched);
        if !latched {
            proof_failure = None;
        } else {
            let space_low = store.free_space_is_low().unwrap_or(false);
            let named = cause(proof_failure.as_deref().unwrap_or_default(), space_low);
            if first_sight {
                eprintln!(
                    "sparrowd: store durability latched ({}); writes are refused, the store is left alone until free space is adequate and RocksDB has had time to resume, then the proof runs",
                    named.name()
                );
            }
            match proofs.step(&pace, tokio::time::Instant::now(), space_low) {
                Step::Wait => {}
                Step::Exit => {
                    return Err(format!(
                        "store durability uncertain and not re-established within {:?} of adequate free space ({}); RocksDB clears this only by reopening, so exiting for the supervisor to reopen it: {}",
                        pace.bound,
                        named.name(),
                        proof_failure.as_deref().unwrap_or("no proof ran")
                    ));
                }
                Step::Prove => {
                    let owner = store.clone();
                    let proof = tokio::task::spawn_blocking(move || owner.reestablish_durability())
                        .await
                        .map_err(|error| format!("durability proof join failed: {error}"))?;
                    match proof {
                        Ok(Some(cleared)) => {
                            proofs.read(false);
                            proof_failure = None;
                            eprintln!(
                                "sparrowd: store durability re-established in place after: {cleared}"
                            );
                            // Writers held on the latch's retryable refusal evaluate again now.
                            held.wake();
                            wake.notify_one();
                            pressure_wake.notify_one();
                            migrate_wake.notify_one();
                        }
                        Ok(None) => proofs.read(false),
                        Err(error) => {
                            proofs.failed(tokio::time::Instant::now());
                            proof_failure = Some(error.message);
                        }
                    }
                }
            }
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
                journal_expiry(&report, paused);
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
                if paused {
                    // The floor episode waits for ageing out before it retires archived
                    // evidence early, and measures what ageing out retired.
                    pressure_wake.notify_one();
                }
                #[cfg(test)]
                skips::expired_pass();
            }
            Ok(Outcome::Continue) => {
                if paused && matches!(job, Job::Leases) {
                    // Ended lease snapshots are what a pressure pass stalled on open
                    // snapshots waits for.
                    pressure_wake.notify_one();
                }
            }
            Ok(Outcome::Pressured(status)) => {
                held.wake();
                journal_eviction(&status, &mut journaled_eviction_passes);
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
                let retry = expiry_retry(paused, &error);
                if !matches!(job, Job::Expiry) || retry.journal {
                    eprintln!("sparrowd: {} failed: {error}", job.name());
                }
                // Retry a refused pass or page on the ordinary tick, never a busy loop.
                match job {
                    Job::Expiry => {
                        continuation = false;
                        expiry_at = tokio::time::Instant::now() + retry.after;
                        if paused {
                            // The floor episode waits for ageing out to have had its turn.
                            pressure_wake.notify_one();
                        }
                        #[cfg(test)]
                        if !retry.journal {
                            skips::note(job);
                        }
                    }
                    Job::Migrate { .. } => {
                        migrating = false;
                        migrate.reset();
                    }
                    _ => {}
                }
            }
        }
        #[cfg(test)]
        skips::pass_done();
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The daemon's store configuration, except that evidence may retire without an archive:
    /// these tests exercise retention itself, and the daemon's own store holds it.
    fn open_retiring(
        config: &sparrow_server::ServerConfig,
    ) -> Result<Store, Box<dyn std::error::Error>> {
        Ok(Store::open(
            &config.data_dir,
            sparrow_core::Config {
                buffered_flush_interval_ms: config.buffered_flush_interval_ms,
                disk_budget: config.disk_budget,
                maintenance_reserve: config.maintenance_reserve,
                min_free_bytes: config.min_free_bytes,
                max_records: config.max_records,
                max_pin_members: config.max_pin_members,
                text_analyzer: sparrow_core::resolve_text_analyzer(&config.text_lexicon_dir)?,
                archive: None,
                retire_without_archive: true,
                instance_role: config.instance_role,
            },
        )?)
    }

    /// Wait until `done` holds, blocking on the worker's pass event (a loop turn ended) and
    /// re-checking each time. The 30 s deadline covers the whole wait, not each turn (the
    /// worker ends a turn at least every second): a condition that never holds fails naming
    /// `what`. It ends no wait that succeeds.
    async fn until(what: &str, done: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(30), async {
            while !done() {
                skips::passes().notified().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("did not happen within 30 s: {what}"));
    }

    // Value: below the free-space floor the daemon used to freeze the whole pressure state
    // machine exactly when the disk was nearly full (#211), and later skipped ageing out and
    // the lease purge too (#267). Protects: pressure maintenance, ageing out (under the
    // maintenance reserve) and the lease purge (which writes nothing) are duties that run below
    // the floor, beside the two that never add data, while the duties that write more than
    // they free (import reclamation, online migration) still wait for the floor.
    #[test]
    fn only_duties_that_free_space_or_add_nothing_run_below_the_floor() {
        for job in [
            Job::Pressure,
            Job::Flush,
            Job::Recompress,
            Job::Expiry,
            Job::Leases,
        ] {
            assert!(runs_below_floor(job), "{}", job.name());
        }
        for job in [
            Job::Reclaim,
            Job::Migrate { defer: false },
            Job::Migrate { defer: true },
        ] {
            assert!(!runs_below_floor(job), "{}", job.name());
        }
    }

    // Value (#267): a full disk used to put expiry on a 60 second retry clock with a journal
    // line per attempt, though its refusal below the reserve ends by an event (free space
    // returning, which wakes expiry). Protects: below the floor, a reserve refusal waits for the
    // wall-clock ceiling and is not journaled; any other failure, any failure with the
    // floor not crossed, and a budget `STORAGE_PRESSURE` raised by an inner commit (which has
    // no resume event) keep the retry clock and the journal line, told apart by the typed
    // reserve label and not by the code alone.
    #[test]
    fn an_expiry_refusal_at_the_reserve_has_no_retry_clock_and_no_journal_line() {
        let refused = Error::new(
            "STORAGE_PRESSURE",
            format!("{}only 1 bytes free", sparrow_core::RESERVE_REFUSAL_PREFIX),
        );
        assert!(refused.is_reserve_refusal());
        let quiet = expiry_retry(true, &refused);
        assert_eq!((quiet.after, quiet.journal), (EXPIRY_CEILING, false));
        let floor_clear = expiry_retry(false, &refused);
        assert_eq!(
            (floor_clear.after, floor_clear.journal),
            (EXPIRY_RETRY, true)
        );
        let failed = expiry_retry(true, &Error::new("STORAGE_FAILURE", "native failure"));
        assert_eq!((failed.after, failed.journal), (EXPIRY_RETRY, true));
        for other in [
            Error::new(
                "STORAGE_PRESSURE",
                "write would cross the maintenance reserve; bounded pressure maintenance requested",
            ),
            Error::new(
                "STORAGE_PRESSURE",
                format!("{}only 1 bytes free", sparrow_core::FLOOR_REFUSAL_PREFIX),
            ),
        ] {
            let retry = expiry_retry(true, &other);
            assert_eq!(
                (retry.after, retry.journal),
                (EXPIRY_RETRY, true),
                "{other}"
            );
        }
    }

    /// A daemon-config store whose floor (32 MiB) is above its maintenance reserve (8 MiB), so
    /// free space moved with the override lands between them: ageing out and the pressure pass
    /// can write, ordinary writes cannot.
    fn floor_store(dir: &std::path::Path) -> Store {
        let data = dir.join("store");
        let path = sparrow_test_support::write_server_config(dir, &data, &data.join("rpc.sock"));
        let text = std::fs::read_to_string(&path).unwrap();
        let raised = text.replace("min_free_bytes = 1048576", "min_free_bytes = 33554432");
        assert_ne!(raised, text);
        std::fs::write(&path, raised).unwrap();
        open_retiring(&sparrow_server::ServerConfig::read(&path).unwrap()).unwrap()
    }

    /// Free space above the 8 MiB reserve and below the 32 MiB floor.
    const BETWEEN_RESERVE_AND_FLOOR: u64 = 16 * 1024 * 1024;

    // Value (#267): expiry was skipped below the floor, so a store under its budget kept
    // evidence past retention while it refused every write. Protects: with free space between
    // the reserve and the floor, the worker ages out evidence past retention while the floor
    // stays crossed (it never resumes in this test), and an ordinary write is still refused.
    #[tokio::test]
    async fn expiry_below_the_floor_retires_aged_out_evidence_under_the_reserve() {
        let dir = tempfile::tempdir().unwrap();
        let store = floor_store(dir.path());
        let now = sparrow_server::now().unwrap();
        let old = now - 31 * 86_400_000;
        let artifact = store
            .ingest("aged-below-floor", "source", "log", b"old\n", old)
            .unwrap();
        let floor = store.space_floor();
        floor.set_free_for_tests(Some(BETWEEN_RESERVE_AND_FLOOR));
        assert!(floor.observe(now).unwrap().is_some());
        let worker = Worker::start(store.clone(), Duration::from_millis(1), Default::default());
        until("aged out evidence to retire below the floor", || {
            store.artifact(&artifact.id).is_err()
        })
        .await;
        assert!(
            floor
                .observe(sparrow_server::now().unwrap())
                .unwrap()
                .is_some(),
            "the floor must still be crossed"
        );
        assert!(
            store
                .ingest("refused-below-floor", "source", "log", b"new\n", now)
                .unwrap_err()
                .is_free_space_floor()
        );
        worker.shutdown().await.unwrap();
    }

    // Value (#267): the lease purge writes nothing, but it was skipped below the floor, and a
    // pressure pass stalls on read snapshots that only the purge ends. Protects: with the floor
    // crossed, the worker purges an expired read lease and its query reservation is released.
    #[tokio::test]
    async fn lease_purge_runs_below_the_floor_and_releases_an_expired_lease() {
        use sparrow_core::{QueryScope, RecordOperation, RecordQuery, RecordRead};
        let dir = tempfile::tempdir().unwrap();
        let store = floor_store(dir.path());
        store
            .ingest(
                "leased-below-floor",
                "pages",
                "jsonl",
                b"{}\n".repeat(8).as_slice(),
                1,
            )
            .unwrap();
        let footprint = || {
            store.status(2).unwrap()["physical_bytes"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
        };
        let before = footprint();
        // A lease taken at clock 2 has long ended by the worker's real clock.
        let page = store
            .records_read(
                &RecordRead {
                    operation: RecordOperation::Records,
                    scope: sparrow_test_support::record_scope!(QueryScope, "pages", 2),
                    query: RecordQuery::default(),
                    cursor: None,
                },
                2,
            )
            .unwrap();
        assert!(page.next_cursor.is_some());
        let charge = footprint() - before;
        assert!(charge > 0, "a leased page must charge the footprint");
        let floor = store.space_floor();
        floor.set_free_for_tests(Some(BETWEEN_RESERVE_AND_FLOOR));
        assert!(
            floor
                .observe(sparrow_server::now().unwrap())
                .unwrap()
                .is_some()
        );
        let worker = Worker::start(store.clone(), Duration::from_millis(1), Default::default());
        until("the expired lease to be purged below the floor", || {
            footprint() < before + charge / 2
        })
        .await;
        worker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn backlog_drains_between_ticks_with_pins_and_buffered_flushes() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let path =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let config = sparrow_server::ServerConfig::read(&path).unwrap();
        let store = open_retiring(&config).unwrap();
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
        until("the backlog to drain", || {
            store.status(sparrow_server::now().unwrap()).unwrap()["unique_payload_chunks"].as_str()
                == Some("2")
                && store.durability_status().unwrap().pending_writes == 0
        })
        .await;
        worker.shutdown().await.unwrap();
        assert_eq!(
            store.bytes(&artifact.record_ids[0], 0, 17, now).unwrap(),
            b"expired record 0\n"
        );
        assert_eq!(store.bytes(&fresh.id, 0, 8, now).unwrap(), b"buffered");
        assert_eq!(store.artifact(&artifact.id).unwrap_err().code, "NOT_FOUND");
    }

    /// INVARIANT (#447): a standing latch is left alone while free space is below RocksDB's
    /// recovery buffer, however long, and the free space returning restarts the wait; once
    /// space is adequate the first proof waits out the grace, further proofs are spaced by a
    /// doubling interval that stops at the longest, and the daemon exits only when the latch
    /// has stood the whole bound with space adequate. The cause is named by measured free
    /// space first and by either ENOSPC wording second, never by free text alone.
    #[test]
    fn a_standing_latch_is_proved_sparingly_and_exits_only_after_the_bound() {
        let pace = Pace::DAEMON;
        let origin = tokio::time::Instant::now();
        let at = |seconds: u64| origin + Duration::from_secs(seconds);
        let mut proofs = Proofs::default();
        // Low free space waits indefinitely, far past the bound.
        for seconds in [0, 10, 300, 3600] {
            assert_eq!(proofs.step(&pace, at(seconds), true), Step::Wait);
        }
        // Space returns at 4000 s: wait out the grace, then prove once.
        assert_eq!(proofs.step(&pace, at(4000), false), Step::Wait);
        assert_eq!(proofs.step(&pace, at(4014), false), Step::Wait);
        assert_eq!(proofs.step(&pace, at(4015), false), Step::Prove);
        // A failed proof is followed by 30 s, then 60 s, then 60 s of silence.
        proofs.failed(at(4015));
        assert_eq!(proofs.step(&pace, at(4044), false), Step::Wait);
        assert_eq!(proofs.step(&pace, at(4045), false), Step::Prove);
        proofs.failed(at(4045));
        assert_eq!(proofs.step(&pace, at(4104), false), Step::Wait);
        assert_eq!(proofs.step(&pace, at(4105), false), Step::Prove);
        proofs.failed(at(4105));
        // The bound counts from when space became adequate.
        assert_eq!(proofs.step(&pace, at(4119), false), Step::Wait);
        assert_eq!(proofs.step(&pace, at(4120), false), Step::Exit);
        // Space falling low again restarts the whole wait.
        assert_eq!(proofs.step(&pace, at(4121), true), Step::Wait);
        assert_eq!(proofs.step(&pace, at(4200), false), Step::Wait);

        for (message, space_low, expected) in [
            ("Input/output error", true, Cause::NoSpace),
            ("garbled text", true, Cause::NoSpace),
            ("IO error: No space left on device", false, Cause::NoSpace),
            ("IO error: no space left", false, Cause::NoSpace),
            ("write failed: ENOSPC", false, Cause::NoSpace),
            ("IO error: Input/output error", false, Cause::Other),
            ("something unforeseen", false, Cause::Other),
        ] {
            assert_eq!(cause(message, space_low), expected, "{message} {space_low}");
        }
    }

    /// INVARIANT (#447): other duties succeeding while a latch stands do not restart its
    /// schedule of proofs. A new latch is read only after a failed duty, a standing one after
    /// every duty, and a duty that succeeds with the latch still standing leaves the grace
    /// running, so the first proof comes when the grace ends instead of never. Fails on a
    /// worker that takes a successful duty for a cleared latch: its schedule restarts every
    /// second and no proof ever runs.
    #[test]
    fn a_successful_duty_does_not_restart_the_proofs_of_a_standing_latch() {
        let pace = Pace::DAEMON;
        let origin = tokio::time::Instant::now();
        let mut proofs = Proofs::default();
        assert!(!proofs.must_read(false), "no latch was seen");
        assert!(proofs.must_read(true), "a failed duty may have met one");
        proofs.read(true);
        let mut proved_at = None;
        for second in 0..=20_u64 {
            // Every second a different duty succeeds and the latch is read again.
            assert!(
                proofs.must_read(false),
                "a standing latch is read after every duty"
            );
            proofs.read(true);
            if proofs.step(&pace, origin + Duration::from_secs(second), false) == Step::Prove {
                proved_at = Some(second);
                break;
            }
        }
        assert_eq!(proved_at, Some(15));
        proofs.read(false);
        assert!(!proofs.must_read(false), "a cleared latch is forgotten");
    }

    /// INVARIANT (#447): a durability latch the store can clear no longer ends the daemon.
    /// The worker's next failed duty proves every accepted write durable in place: the latch
    /// clears, the buffered write it left uncertain is certified, writes are accepted again,
    /// and the worker is still running to shut down cleanly. Fails on the worker that exits on
    /// every latch: the latch never clears and the worker has ended.
    #[tokio::test]
    async fn a_durability_latch_clears_in_place_and_the_worker_keeps_running() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let path =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let store = sparrow_server::ServerConfig::read(&path)
            .unwrap()
            .open_store()
            .unwrap();
        let now = sparrow_server::now().unwrap();
        store
            .ingest_with_durability(
                "buffered",
                "source",
                "opaque",
                b"buffered",
                now,
                sparrow_core::Durability::Buffered,
            )
            .unwrap();
        store
            .latch_durability_for_tests("injected WAL sync failure")
            .unwrap();
        let worker = Worker::start_paced(
            store.clone(),
            Duration::from_millis(1),
            Default::default(),
            Pace {
                grace: Duration::ZERO,
                first: Duration::from_millis(10),
                longest: Duration::from_millis(10),
                bound: Duration::from_secs(25),
            },
        );
        until("the latch to clear", || {
            let status = store.durability_status().unwrap();
            status.failure.is_none() && status.pending_writes == 0
        })
        .await;
        store
            .ingest("after", "source", "log", b"accepted\n", now)
            .unwrap();
        worker.shutdown().await.unwrap();
    }

    /// INVARIANT (#447): under a real full filesystem the daemon's own maintenance worker
    /// leaves a latched store alone while free space is low, proves durability after space
    /// returns and RocksDB has had time to resume, clears the latch in place, and is still
    /// running. The count of manual flushes in RocksDB's own LOG shows whether the store was
    /// touched while space was low.
    ///
    /// Needs real time (the worker waits out RocksDB's 5 s polls) and a filesystem it can fill,
    /// so it is ignored in the gate: run `tools/enospc-check.sh LOG worker`, which mounts a
    /// 160 MiB tmpfs in a user and mount namespace (`unshare -rm`) and names it in
    /// `SPARROW_ENOSPC_DIR`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "runs under tools/enospc-check.sh on a 160 MiB tmpfs and needs about two minutes"]
    async fn a_full_disk_latch_is_proved_once_by_the_worker_after_space_returns() {
        use sparrow_core::Config;
        use std::io::Write;
        let dir = std::path::PathBuf::from(
            std::env::var_os("SPARROW_ENOSPC_DIR")
                .expect("SPARROW_ENOSPC_DIR names the small filesystem; run tools/enospc-check.sh"),
        );
        let data = dir.join("store");
        let store = Store::open(
            &data,
            Config {
                disk_budget: 4 * 1024 * 1024 * 1024,
                maintenance_reserve: 1024 * 1024,
                min_free_bytes: 4 * 1024,
                ..sparrow_test_support::storage_config!(Config)
            },
        )
        .unwrap();
        let flushes = || {
            std::fs::read_to_string(data.join("LOG"))
                .unwrap_or_default()
                .lines()
                .filter(|line| line.contains("Manual flush start"))
                .count()
        };
        let worker = Worker::start_paced(
            store.clone(),
            Duration::from_secs(1),
            Default::default(),
            Pace::DAEMON,
        );
        store
            .ingest("before", "source", "log", b"kept\n", 1)
            .unwrap();
        let filler = dir.join("filler");
        let mut file = std::fs::File::create(&filler).unwrap();
        let chunk = vec![0_u8; 64 * 1024];
        while file.write_all(&chunk).is_ok() {}
        drop(file);
        let length = std::fs::metadata(&filler).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&filler)
            .unwrap()
            .set_len(length - 16 * 1024)
            .unwrap();
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        let mut failed = None;
        for attempt in 0..480 {
            let payload: Vec<u8> = (0..256 * 1024)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    seed.to_le_bytes()[0]
                })
                .collect();
            if let Err(error) = store.ingest_with_durability(
                &format!("full-{attempt}"),
                "source",
                "opaque",
                &payload,
                2,
                sparrow_core::Durability::Durable,
            ) {
                failed = Some(error);
                break;
            }
        }
        eprintln!("first failure: {failed:?}");
        assert!(store.durability_status().unwrap().failure.is_some());

        // Space stays low: the worker must leave the store alone.
        tokio::time::sleep(Duration::from_secs(5)).await;
        let at_latch = flushes();
        tokio::time::sleep(Duration::from_secs(30)).await;
        eprintln!(
            "manual flushes: {at_latch} at the latch, {} after 30 s with space low",
            flushes()
        );
        assert_eq!(
            flushes(),
            at_latch,
            "the worker touched the store while space was low"
        );
        assert!(store.durability_status().unwrap().failure.is_some());

        std::fs::remove_file(&filler).unwrap();
        let freed = std::time::Instant::now();
        let cleared = loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if store.durability_status().unwrap().failure.is_none() {
                break freed.elapsed();
            }
            assert!(
                freed.elapsed() < Duration::from_secs(150),
                "the latch did not clear"
            );
        };
        eprintln!("RESULT cleared_after={cleared:?}");
        store
            .ingest("after", "source", "log", b"after\n", 3)
            .unwrap();
        worker
            .shutdown()
            .await
            .expect("the worker was still running");
    }

    /// INVARIANT (#183, #249): an import's slot is freed by an event, not by the one-second
    /// interval. Three events make a journal reclaimable and each must reach the worker's
    /// reclaim arm: an explicit cancel, a publication, and the free-space floor's resume edge
    /// for a pass the pause skipped. The test waits on the event the daemon's begin-import
    /// waiters wait on (the reclamation commit signals the import slot waker), and reads which
    /// source started each reclaim pass: the interval's tick or the wake. A freed slot with no
    /// wake-started pass means the interval did the work, so removing the worker's wake arm,
    /// the waker's install, the callback, or the resume edge's notification fails it.
    #[tokio::test]
    async fn an_import_slot_is_freed_by_a_wake_and_not_by_the_interval() {
        for case in ["cancel", "publish", "resume", "large"] {
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("store");
            let path = sparrow_test_support::write_server_config(
                dir.path(),
                &data,
                &data.join("rpc.sock"),
            );
            let store = open_retiring(&sparrow_server::ServerConfig::read(&path).unwrap()).unwrap();
            let freed = Arc::new(Notify::new());
            store.set_import_slot_waker(Some(Arc::new({
                let freed = Arc::clone(&freed);
                move || freed.notify_one()
            })));
            // The large import has 130 chunks: one reclaim pass frees 128, so it needs a second.
            let input: Vec<u8> = if case == "large" {
                vec![b'a'; 130 * 64 * 1024]
            } else {
                b"one\n".to_vec()
            };
            let digest = format!(
                "blake3:{}",
                purrdf_hash::hex::encode(purrdf_hash::blake3::hash(&input).as_bytes())
            );
            let now = sparrow_server::now().unwrap();
            let staged = store
                .import_begin(case, "source", "log", input.len() as u64, &digest, now)
                .unwrap();
            let floor = store.space_floor();
            if case == "resume" {
                // Ended before the worker exists, so no wake is sent; the pause then skips the
                // worker's first reclaim pass.
                store.import_cancel(&staged.id, now).unwrap();
                floor.set_free_for_tests(Some(0));
                assert!(floor.observe(now).unwrap().is_some());
            }
            if case == "large" {
                // Staged before the worker exists, so the 130 appends cannot delay a turn and
                // let an overdue interval tick race the cancel wake.
                for (index, chunk) in input.chunks(64 * 1024).enumerate() {
                    store
                        .import_append(&staged.id, (index * 64 * 1024) as u64, chunk, now)
                        .unwrap();
                }
            }
            let (ticks, skipped, expired, skipped_expiry) = (
                skips::reclaim_ticks(),
                skips::reclaim(),
                skips::expired(),
                skips::expiry(),
            );
            let worker =
                Worker::start(store.clone(), Duration::from_secs(3600), Default::default());
            // The first reclaim tick and the first expiry pass (which also reclaims imports)
            // are ready at once and have run; after them only an event or the interval, a
            // second away, can start a reclaim pass. A pass the pause skipped counts too.
            let first_passes_ran = || {
                if case == "resume" {
                    skips::reclaim() > skipped && skips::expiry() > skipped_expiry
                } else {
                    skips::reclaim_ticks() > ticks && skips::expired() > expired
                }
            };
            until("the first passes", first_passes_ran).await;
            let wakes = skips::reclaim_wakes();
            match case {
                "cancel" | "large" => store.import_cancel(&staged.id, now).unwrap(),
                "publish" => {
                    store.import_append(&staged.id, 0, &input, now).unwrap();
                    store.import_seal(&staged.id, now).unwrap();
                    store.import_prepare_layout(&staged.id, now).unwrap();
                    store.import_prepare_record(&staged.id, 0, now).unwrap();
                    store.import_publish(&staged.id, now).unwrap();
                }
                _ => {
                    floor.set_free_for_tests(None);
                    assert!(floor.observe(now).unwrap().is_none());
                }
            }
            // The bounds only report a slot never freed or a reclaim pass no wake started; they
            // end no wait that succeeds. On the resume edge the expiry pass is woken beside the
            // reclaim pass and may free the slot first, so the wake-started pass is awaited too.
            tokio::time::timeout(Duration::from_secs(30), freed.notified())
                .await
                .unwrap_or_else(|_| panic!("{case}: the import slot was never freed"));
            if case == "resume" {
                until("a wake-started reclaim pass", || {
                    skips::reclaim_wakes() > wakes
                })
                .await;
            } else {
                // The counter rises when the pass starts, before it frees anything, so it is
                // already visible here; a regression fails after the interval, not the bound.
                let expected = wakes + if case == "large" { 2 } else { 1 };
                assert!(
                    skips::reclaim_wakes() >= expected,
                    "{case}: the slot was freed with {} wake-started passes, expected {expected}",
                    skips::reclaim_wakes()
                );
            }
            worker.shutdown().await.unwrap();
        }
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
        let store = open_retiring(&sparrow_server::ServerConfig::read(&path).unwrap()).unwrap();
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
        // The finished pass is the event; the bound only reports a pass that never finished.
        until("the first expiry pass", || skips::expired() > 0).await;
        assert!(store.artifact(&artifact.id).is_ok(), "a pinned entry stays");
        store.release(&pin.id).unwrap();
        until("the released entry to retire", || {
            store.artifact(&artifact.id).is_err()
        })
        .await;
        worker.shutdown().await.unwrap();
    }

    /// A store holding 300 records whose owner index, source summary and source timeline
    /// migrations are pending, reopened so the worker's pages must run them.
    fn store_with_pending_migrations(dir: &std::path::Path) -> Store {
        let data = dir.join("store");
        let path = sparrow_test_support::write_server_config(dir, &data, &data.join("rpc.sock"));
        let config = sparrow_server::ServerConfig::read(&path).unwrap();
        let store = open_retiring(&config).unwrap();
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
        open_retiring(&config).unwrap()
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
        until("the migrations to complete", || {
            store
                .migrations()
                .unwrap()
                .iter()
                .all(sparrow_core::MigrationStatus::complete)
        })
        .await;
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
        let store = open_retiring(&sparrow_server::ServerConfig::read(&path).unwrap()).unwrap();
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
        let store = open_retiring(&sparrow_server::ServerConfig::read(&path).unwrap()).unwrap();
        let now = sparrow_server::now().unwrap();
        let old = now - 31 * 86_400_000;
        let artifact = store
            .ingest("expired-below-floor", "source", "log", b"old\n", old)
            .unwrap();
        let floor = store.space_floor();
        floor.set_free_for_tests(Some(0));
        assert!(floor.observe(now).unwrap().is_some());
        let worker = Worker::start(store.clone(), Duration::from_millis(1), Default::default());
        // The first expiry pass runs at once; free space of 0 is below the maintenance reserve,
        // so it is refused (and counted by the skip counter) with nothing retired.
        until("the first expiry pass to be refused", || {
            skips::expiry() > 0
        })
        .await;
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
        until("expiry to run after the floor resumed", || {
            store.artifact(&artifact.id).is_err()
        })
        .await;
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
        until("the first migrate pass to be skipped", || {
            skips::migrate() > 0
        })
        .await;
        assert!(
            store.migrations().unwrap().iter().all(|m| !m.complete()),
            "a migration ran below the floor"
        );
        let wakes = skips::migrate_wakes();
        floor.set_free_for_tests(None);
        assert!(
            floor
                .observe(sparrow_server::now().unwrap())
                .unwrap()
                .is_none()
        );
        until("the migrations to complete after the resume", || {
            store
                .migrations()
                .unwrap()
                .iter()
                .all(sparrow_core::MigrationStatus::complete)
        })
        .await;
        // The resume edge started the first page; the 60 s retry tick did not.
        assert!(
            skips::migrate_wakes() > wakes,
            "no migration page was started by a wake"
        );
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
        let store = open_retiring(&sparrow_server::ServerConfig::read(&path).unwrap()).unwrap();
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

    /// Value (#251): the reclaim observation is the held frames' only wake for endings with no
    /// event, and it must run before the below-floor skip, because reclamation is skipped
    /// exactly while the floor is paused, which is when frames are held. The unit test above
    /// proves `observe_floor` ticks; this one drives the real worker loop. Protects: with the
    /// floor paused and a frame held, every reclaim pass the loop skips was observed first;
    /// moving the `observe_floor` call after the skip leaves skipped passes unobserved.
    #[tokio::test]
    async fn the_worker_observes_the_floor_before_skipping_reclaim_below_it() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let path =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let store = open_retiring(&sparrow_server::ServerConfig::read(&path).unwrap()).unwrap();
        store.space_floor().set_free_for_tests(Some(0));
        let held = sparrow_server::pressure_wake::PressureWake::default();
        let waiter = held.subscribe();
        let frame = waiter.hold();
        // The reclaim interval's first tick is ready at once, so no time needs to pass.
        let worker = Worker::start(store.clone(), Duration::from_secs(3600), held.clone());
        until("a skipped reclaim pass", || skips::reclaim() > 0).await;
        assert!(
            skips::reclaim_observed() >= skips::reclaim(),
            "a reclaim pass was skipped below the floor without its observation"
        );
        drop(frame);
        worker.shutdown().await.unwrap();
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
        let store = open_retiring(&sparrow_server::ServerConfig::read(&path).unwrap()).unwrap();
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
        let store = open_retiring(&config).unwrap();
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
