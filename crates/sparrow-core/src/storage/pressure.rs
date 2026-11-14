// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One physical admission signal and bounded, pin-safe pressure worker.

use super::{
    Error, Store, Writer,
    eviction::{EvictionLimits, EvictionReport},
    forced_compaction::{FamilySize, Slot},
    space::FloorStatus,
};
use rocksdb::FlushOptions;

/// Ask the native picker to revisit tombstone-heavy tables even when level-size
/// triggers are quiet. In particular, a flushed retirement batch must not wait
/// for more ingestion before its obsolete payload references can be collected.
pub(super) fn native_compaction_options(options: &mut rocksdb::Options) {
    options.add_compact_on_deletion_collector_factory(1024, 128, 0.25);
}

pub(super) const WAIT_KEY: &str = "pressure-reclamation-wait";
pub(super) const FORMAT: &[u8] = b"sparrow.store/38";
pub(super) const WATCH_FORMAT: &[u8] = b"sparrow.store/39";

pub(super) fn stage_wait(batch: &mut rocksdb::WriteBatch, physical: u64, requested: u64) {
    let value = purrdf_lex::json::Object::new()
        .with("physical_before", physical.to_string())
        .with("requested_bytes", requested.to_string());
    batch.put(WAIT_KEY, purrdf_lex::json::write_compact(&value.into()));
}

/// Pressure worker state, independent of buffered WAL queue pressure.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PressurePhase {
    /// No outstanding refused physical-space request.
    #[default]
    Ready,
    /// A write was refused and maintenance has been requested.
    Requested,
    /// Retirement is durable; wait for native work and measured physical recovery.
    AwaitingReclamation,
    /// The bounded pass could make no progress, or encountered a hard failure.
    Blocked,
    /// Retirement stopped at the archive watermark; writes stay refused until archiving catches up.
    AwaitingArchive,
}

/// Durable phase/request recovery with session-local maintenance counters and observations.
#[derive(Clone, Debug, Default)]
pub struct PressureStatus {
    /// Current maintenance phase.
    pub phase: PressurePhase,
    /// Smallest outstanding refused write estimate; zero when ready.
    pub requested_bytes: u64,
    /// Number of attempted logical retirement passes.
    pub passes: u64,
    /// Total record occurrences evicted by this session's pressure worker.
    pub evicted_records: u64,
    /// Recordless artifact catalogs reclaimed by this session's pressure worker.
    pub evicted_recordless_artifacts: u64,
    /// Payload ownership references retired by this session's pressure worker.
    pub payload_refs_retired: u64,
    /// Native flush pending/running observations across every family.
    pub pending_flushes: u64,
    /// Native compaction pending/running observations across every family.
    pub pending_compactions: u64,
    /// Native snapshot count at the last maintenance observation.
    pub snapshots: u64,
    /// Last successful bounded retirement report, including its explicit frontier.
    pub last_pass: Option<EvictionReport>,
    /// The last actionable maintenance failure. It stays while the phase is Ready (a surfaced
    /// native-error text or a collected forced-compaction failure) until the next episode ends
    /// or a retiring pass overwrites it; `native_errors` and `forced_failures` say how many.
    pub failure: Option<String>,
    /// Clock reading (Unix milliseconds) of the last pass. Every age below is measured to it.
    pub observed_at: u64,
    /// Linked store bytes the last pass measured (see `linked_physical_bytes`).
    pub linked_bytes: u64,
    /// Unix milliseconds intake first stayed refused in this episode; zero while ready.
    /// Session-local: a reopened store restarts the clock at its first pass.
    pub episode_since: u64,
    /// Unix milliseconds the current non-ready phase began; zero while ready.
    pub phase_since: u64,
    /// Unix milliseconds the current wait for native reclamation began; zero when none.
    pub wait_since: u64,
    /// Linked bytes when that wait began; zero when none. The wait ends when linked bytes
    /// fall below it, or when a forced compaction leaves them there.
    pub wait_before_bytes: u64,
    /// Manual compactions forced because a wait saw no drop in linked bytes.
    pub forced_compactions: u64,
    /// Waits ended because a forced compaction left linked bytes unchanged, so the next
    /// pass retired more evidence.
    pub waits_released: u64,
    /// Passes that ended without retiring evidence or moving the wait on.
    pub stalled_passes: u64,
    /// Data writes this session answered `STORAGE_PRESSURE` at the physical admission check.
    pub refused_writes: u64,
    /// Why the last pass could do no more, when it could not.
    pub waiting_on: Option<String>,
    /// The free-space floor's pauses and refusals this session.
    pub floor: FloorStatus,
    /// Wall time of the last finished forced compaction, in milliseconds; zero before one.
    pub forced_compaction_ms: u64,
    /// Table and blob file bytes of each family before and after that compaction; empty
    /// before one.
    pub forced_compaction_families: Vec<FamilySize>,
    /// Forced compactions that ended in a failure (a panic or a raised native error).
    pub forced_failures: u64,
    /// New RocksDB background errors this session's pressure passes saw. Each is reported
    /// once as a failed pass and then counted here; none latches the worker.
    pub native_errors: u64,
    /// Free-space-floor episodes in which a refused write asked for reclamation (#267).
    pub floor_requests: u64,
    /// Bytes of free space missing below the floor at the last pass of an episode; zero
    /// when no floor episode is open or no pass has measured it yet.
    pub floor_requested_bytes: u64,
    /// Retirement passes run while the floor, not the disk budget, was the reason.
    pub floor_passes: u64,
    /// Linked store bytes the current or last floor episode saw leave; kept until the next
    /// episode starts.
    pub floor_freed_bytes: u64,
    /// The current floor episode freed as much as its bound allows (the first measured
    /// deficit plus the maintenance reserve) and retires no more. The daemon journals the
    /// edge; the status text says it in words.
    pub floor_bound_reached: bool,
}

impl PressureStatus {
    /// How long the current wait for native reclamation has lasted at the last pass.
    #[must_use]
    pub fn wait_age_ms(&self) -> u64 {
        if self.wait_since == 0 {
            0
        } else {
            self.observed_at.saturating_sub(self.wait_since)
        }
    }

    /// How long intake has stayed refused in this episode at the last pass.
    #[must_use]
    pub fn episode_age_ms(&self) -> u64 {
        if self.episode_since == 0 {
            0
        } else {
            self.observed_at.saturating_sub(self.episode_since)
        }
    }
}

/// The wait after a durable retirement: it ends when linked bytes fall below `before`.
#[derive(Clone, Copy, Debug)]
struct Wait {
    before: u64,
    step: Step,
    /// A retirement pass staged a durable marker for this wait. Ageing out below the floor
    /// (#267) opens a wait with none. Nothing was stored, so ending it needs no commit: a
    /// commit would be one more write admitted against the maintenance reserve, which could
    /// be refused for no reason the wait has.
    durable: bool,
}

/// How far the one forced compaction of a wait has come.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    /// Native work alone is trusted so far.
    Native,
    /// A forced compaction was started.
    Forced,
    /// It ended; linked bytes decide whether the wait ends.
    Measured,
}

/// What a free-space-floor episode keeps between passes. Session-local: a reopened store
/// starts with none, and nothing of it is durable.
#[derive(Default)]
struct FloorEpisode {
    /// A write refused at the floor asked for reclamation in this episode.
    asked: bool,
    /// Bytes below the floor at the last pass of the episode; zero when the floor is not
    /// the reason for this pass.
    deficit: u64,
    /// Retirement passes this episode has run.
    passes: u64,
    /// Retirement batches this episode has made durable.
    batches: u64,
    /// Set by the first pass of the episode.
    start: Option<FloorStart>,
    /// Ageing out ran (or failed) since this episode asked. A refusal
    /// at the maintenance reserve is not a run: nothing was attempted. Early retirement of
    /// archived evidence waits for it: ageing out is normal retention that was only delayed,
    /// early retirement is the policy-sensitive avenue.
    expiry_tried: bool,
    /// Ageing out has been asked to run and has not been refused at the reserve since. The
    /// refusal clears it, and the first pass that finds free space back above the reserve
    /// asks again, so a rise that stops short of the floor still runs ageing out, by an
    /// event and without a clock.
    expiry_asked: bool,
}

/// The measures a floor episode is bounded by.
#[derive(Clone, Copy)]
struct FloorStart {
    /// Linked store bytes at the first pass.
    linked: u64,
    /// Most linked bytes the episode may retire: the first deficit plus the maintenance
    /// reserve.
    bound: u64,
}

/// Most retirement batches one floor episode may make durable, whatever bytes they free.
/// The byte bound counts measured linked-byte reduction, which lags retirement until native
/// compaction runs, so batches that free nothing yet must be capped too. A batch retires at
/// most `EvictionLimits::max_records` units (32 by default), so the cap is 256 units there.
const FLOOR_EPISODE_BATCHES: u64 = 8;

/// Durable wait marker value of a floor-only retirement. It is the value a reopened store
/// already above its allowance uses, never the deficit: a deficit is a free-space measure and
/// the marker's request is compared with the disk budget.
const FLOOR_MARKER_BYTES: u64 = 65536;

#[derive(Default)]
pub(super) struct State {
    pub(super) status: PressureStatus,
    wait: Option<Wait>,
    floor: FloorEpisode,
    /// The cumulative native background-error count already reported; only a higher count
    /// fails a pass (the same baseline as the table rewrite's).
    background_errors: u64,
    /// The phase `phase_since` was stamped for.
    phase_seen: Option<PressurePhase>,
}

impl super::writer_queue::Observe for State {
    type View = PressureStatus;
    fn view(&self) -> PressureStatus {
        self.status.clone()
    }
}

impl State {
    pub(super) fn load(db: &rocksdb::DB) -> Result<Self, Error> {
        let background_errors = super::forced_compaction::native_errors(db)?;
        let Some(bytes) = db.get_pinned(WAIT_KEY)? else {
            return Ok(Self {
                background_errors,
                ..Self::default()
            });
        };
        if !matches!(
            super::writer_format::read(db)?.as_deref(),
            Some(
                FORMAT
                    | WATCH_FORMAT
                    | super::receiver_stream::FORMAT
                    | super::receiver_stream::WATCH_FORMAT
                    | super::manifest_retire::FORMAT
                    | super::manifest_retire::WATCH_FORMAT
            )
        ) {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "pressure recovery marker is missing its writer fence",
            ));
        }
        if bytes.len() > 512 {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "pressure recovery marker exceeds its bounded encoding",
            ));
        }
        let value = super::parse(&bytes)?;
        let physical = super::integer(&value, "physical_before")?;
        let requested = super::integer(&value, "requested_bytes")?;
        if physical == 0 || requested == 0 {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "pressure recovery marker requires positive physical and request bounds",
            ));
        }
        Ok(Self {
            status: PressureStatus {
                phase: PressurePhase::AwaitingReclamation,
                requested_bytes: requested,
                wait_before_bytes: physical,
                ..PressureStatus::default()
            },
            wait: Some(Wait {
                before: physical,
                step: Step::Native,
                durable: true,
            }),
            floor: FloorEpisode::default(),
            background_errors,
            phase_seen: None,
        })
    }

    /// Move to `phase`, stamping when a non-ready phase began.
    fn enter(&mut self, phase: PressurePhase, now: u64) {
        self.status.phase = phase;
        self.stamp(now);
    }

    /// Fill the start times of an episode and phase begun between passes (a refusal
    /// or a reopen), which have no clock of their own.
    fn stamp(&mut self, now: u64) {
        if self.status.phase == PressurePhase::Ready {
            self.status.phase_since = 0;
            return;
        }
        let stamp = now.max(1);
        if self.status.episode_since == 0 {
            self.status.episode_since = stamp;
        }
        if self.phase_seen != Some(self.status.phase) {
            self.status.phase_since = stamp;
        }
        self.phase_seen = Some(self.status.phase);
        if self.wait.is_some() && self.status.wait_since == 0 {
            self.status.wait_since = stamp;
        }
    }

    /// Ready again: the episode, its wait and its reason end together.
    fn ready(&mut self) {
        self.status.requested_bytes = 0;
        self.status.phase = PressurePhase::Ready;
        self.status.failure = None;
        self.status.waiting_on = None;
        self.status.episode_since = 0;
        self.status.phase_since = 0;
        self.status.wait_since = 0;
        self.status.wait_before_bytes = 0;
        self.phase_seen = None;
        self.wait = None;
        self.end_floor_episode();
    }

    /// The measured deficit reached zero (or the store is ready): the floor episode ends.
    /// What it freed and how many passes it ran stay in the status until the next one.
    fn end_floor_episode(&mut self) {
        self.floor = FloorEpisode::default();
        self.status.floor_requested_bytes = 0;
        self.status.floor_bound_reached = false;
    }

    /// End the wait without ending the episode.
    fn end_wait(&mut self) {
        self.wait = None;
        self.status.wait_since = 0;
        self.status.wait_before_bytes = 0;
    }

    /// A pass that could do nothing more, and why.
    /// While the floor is the reason for the pass, the text leads with how far below it the
    /// store's filesystem is.
    fn stalled(&mut self, reason: impl Into<String>) {
        self.status.stalled_passes = self.status.stalled_passes.saturating_add(1);
        let reason = reason.into();
        self.status.waiting_on = Some(self.floor_text(&reason));
    }

    /// `reason`, led by the floor deficit when the floor is the reason for this pass.
    fn floor_text(&self, reason: &str) -> String {
        match self.floor.deficit {
            0 => reason.to_owned(),
            deficit => format!("free space is below the floor by {deficit} bytes; {reason}"),
        }
    }

    /// The reason a floor pass retired nothing more, saying what the episode has done.
    fn floor_tried(&self, reason: &str) -> String {
        if self.floor.deficit == 0 {
            return reason.to_owned();
        }
        format!(
            "reclamation tried ({} passes, {} bytes freed): {reason}",
            self.floor.passes, self.status.floor_freed_bytes
        )
    }
}

/// The existing conservative batch output/WAL estimate, shared by boot and live writes.
pub(super) fn data_batch_estimate(batch: &rocksdb::WriteBatch) -> u64 {
    (batch.size_in_bytes() as u64)
        .saturating_mul(4)
        .saturating_add(65536)
}

const WRITE_CEILING_MESSAGE: &str = "estimated atomic write cannot fit the configured data budget; increase the disk budget or reduce the operation";

/// Why admission refused a data write. The one cause a caller acts on is a type, not a
/// message: a write whose estimate exceeds the whole data allowance can never fit, whatever
/// the store holds, so no retry of it can succeed. Every other refusal keeps its own error.
#[derive(Debug)]
pub(super) enum WriteRefusal {
    /// The estimate exceeds the whole data allowance (independent of the store's content).
    Ceiling,
    Other(Error),
}

impl From<Error> for WriteRefusal {
    fn from(error: Error) -> Self {
        Self::Other(error)
    }
}

impl From<rocksdb::Error> for WriteRefusal {
    fn from(error: rocksdb::Error) -> Self {
        Self::Other(error.into())
    }
}

impl From<WriteRefusal> for Error {
    fn from(refusal: WriteRefusal) -> Self {
        match refusal {
            WriteRefusal::Ceiling => Error::new("RESOURCE_LIMIT", WRITE_CEILING_MESSAGE),
            WriteRefusal::Other(error) => error,
        }
    }
}

/// Largest measured footprint that still leaves the admitted data-write allowance.
pub(super) fn data_write_ceiling(
    disk_budget: u64,
    reserve: u64,
    estimate: u64,
) -> Result<u64, WriteRefusal> {
    if estimate == 0 {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "data write requires a positive capacity estimate",
        )
        .into());
    }
    let limit = disk_budget.checked_sub(reserve).ok_or_else(|| {
        Error::new(
            "INVALID_ARGUMENT",
            "disk budget must exceed maintenance reserve",
        )
    })?;
    limit.checked_sub(estimate).ok_or(WriteRefusal::Ceiling)
}

impl Writer {
    /// Called before publishing a prepared data batch; never mutates its dependencies.
    pub(super) fn admit_data_write(&self, estimate: u64) -> Result<(), WriteRefusal> {
        let ceiling = data_write_ceiling(
            self.config.disk_budget,
            self.config.maintenance_reserve,
            estimate,
        )?;
        if self.physical_bytes()? <= ceiling {
            return Ok(());
        }
        let mut pressure = self.pressure.borrow_mut();
        // The first request of a budget episode is the one with nothing requested yet. The
        // phase cannot tell: a floor episode may already hold it at Requested (#267).
        let first = pressure.status.requested_bytes == 0;
        let requested = &mut pressure.status.requested_bytes;
        *requested = if *requested == 0 {
            estimate
        } else {
            (*requested).min(estimate)
        };
        if first {
            pressure.status.phase = PressurePhase::Requested;
        }
        pressure.status.refused_writes = pressure.status.refused_writes.saturating_add(1);
        drop(pressure);
        if first {
            self.wake_pressure_pass();
        }
        Err(Error::new(
            "STORAGE_PRESSURE",
            "write would cross the maintenance reserve; bounded pressure maintenance requested; retry after measured physical recovery",
        )
        .into())
    }

    /// Run reclamation commits (the retirement batch and the wait clear, nothing else)
    /// under the maintenance reserve instead of the free-space floor: they are what
    /// frees space, and ordinary writes keep the floor.
    pub(super) fn reclaiming<T>(
        &self,
        work: impl FnOnce() -> Result<T, Error>,
    ) -> Result<T, Error> {
        struct Reset<'a>(&'a std::cell::Cell<bool>);
        impl Drop for Reset<'_> {
            fn drop(&mut self) {
                self.0.set(false);
            }
        }
        self.reclaiming.set(true);
        let _reset = Reset(&self.reclaiming);
        work()
    }

    /// The admission check of a commit: the floor for ordinary writes, the maintenance
    /// reserve for reclamation.
    ///
    /// A refusal at the floor also asks for reclamation (#267): the refusal itself stays
    /// immediate and typed, and it now always leaves a pass in motion.
    pub(super) fn admit_space(&self) -> Result<(), Error> {
        if !self.reclaiming.get() {
            return self.space.check().inspect_err(|error| {
                if error.is_free_space_floor() {
                    self.request_floor_reclaim();
                }
            });
        }
        match self
            .space
            .shortfall_below(self.config.maintenance_reserve)?
        {
            Some(shortfall) => Err(super::space::reserve_refusal(&shortfall)),
            None => Ok(()),
        }
    }

    /// Record that a write was refused at the floor and wake the pass once per episode.
    /// Every later refusal of the episode costs one borrow and a flag read. The pass
    /// measures the deficit itself; this only says that someone was turned away.
    fn request_floor_reclaim(&self) {
        let first = {
            let mut state = self.pressure.borrow_mut();
            let first = !state.floor.asked;
            state.floor.asked = true;
            if first {
                state.status.floor_requests = state.status.floor_requests.saturating_add(1);
                state.status.floor_freed_bytes = 0;
                state.floor.start = None;
                state.floor.passes = 0;
                state.floor.batches = 0;
                state.floor.expiry_tried = false;
                // The wake below asks ageing out to run.
                state.floor.expiry_asked = true;
            }
            if state.status.phase == PressurePhase::Ready {
                state.status.phase = PressurePhase::Requested;
                state.status.waiting_on = Some(
                    "free space is below the floor and a write was refused; reclamation requested, not yet attempted"
                        .to_owned(),
                );
            }
            first
        };
        if first {
            self.wake_pressure_pass();
        }
    }

    /// Tell the maintenance worker a pass should run now instead of at its scheduled time.
    fn wake_pressure_pass(&self) {
        let waker = self
            .pressure_request_waker
            .lock()
            .ok()
            .and_then(|slot| slot.clone());
        if let Some(waker) = waker {
            waker();
        }
    }

    pub(super) fn native_property(&self, family: &str, name: &str) -> Result<u64, Error> {
        let cf = self
            .db
            .cf_handle(family)
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "native maintenance family missing"))?;
        self.db.property_int_value_cf(cf, name)?.ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                format!("required native maintenance property unavailable: {family}/{name}"),
            )
        })
    }

    /// End the wait in memory and on disk: a marker left behind would restore it at the
    /// next open although nothing is waiting. A pass that retires evidence stages a new one
    /// with that batch; a crash between the two reopens above the allowance, which asks
    /// for maintenance by itself.
    ///
    /// A commit that fails means the store cannot take even this small write; the error
    /// says so plainly, and the caller leaves its baseline alone so the next pass reports
    /// it again for as long as it lasts.
    fn end_wait(&self, state: &mut State) -> Result<(), Error> {
        if state.wait.is_some_and(|wait| !wait.durable) {
            // Ageing out opened this wait in memory only; there is no marker to delete.
            state.end_wait();
            return Ok(());
        }
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete(WAIT_KEY);
        #[cfg(feature = "test-support")]
        let refused = self.forced.wait_end_refused();
        #[cfg(not(feature = "test-support"))]
        let refused = false;
        let committed = if refused {
            Err(Error::new("STORAGE_FAILURE", "commit refused by a test"))
        } else {
            self.reclaiming(|| self.commit(batch))
        };
        committed.map_err(|error| {
            Error::new(
                error.code,
                format!(
                    "the store cannot take a write, so the pressure wait could not be ended; intake stays refused until it can: {error}"
                ),
            )
        })?;
        state.end_wait();
        Ok(())
    }

    /// Take a finished forced compaction's measures into the status. One that failed
    /// (a panic or a raised native error) is counted and reported, and ends the wait so the
    /// pass retires more evidence instead of waiting on a compaction that freed nothing.
    fn collect_forced(&self, state: &mut State) -> Result<Option<String>, Error> {
        let Slot::Finished(outcome) = self.forced.poll() else {
            return Ok(None);
        };
        state.status.forced_compaction_ms = outcome.elapsed_ms;
        state.status.forced_compaction_families = outcome.families;
        let failure = outcome
            .failure
            .map(|failure| format!("the forced compaction failed: {failure}"));
        if failure.is_some() {
            state.status.forced_failures = state.status.forced_failures.saturating_add(1);
            state.status.failure.clone_from(&failure);
        }
        // Acknowledge first: a failed wait-ending commit below must not leave the outcome
        // in the slot to be counted again by the next pass.
        self.forced.acknowledge();
        if let Some(forced) = failure.as_deref()
            && state.wait.is_some()
        {
            // The caller's error replaces the status failure and the duty marks the forced
            // failure reported, so its own reason must ride in the error or it is lost.
            self.end_wait(state)
                .map_err(|error| Error::new(error.code, format!("{error}; {forced}")))?;
        }
        Ok(failure)
    }

    /// Count a RocksDB background error raised while no pass is retiring evidence, and say
    /// so in the status. The pass does not fail: nothing is waiting on native work, so the
    /// error holds nothing and the baseline moves at once (an old error never holds intake).
    ///
    /// `forced` is a forced-compaction failure collected in this same pass: its reason leads
    /// the text, since the error it raised is the one counted here and no wait was involved.
    fn surface_native_errors(&self, state: &mut State, forced: Option<&str>) -> Result<(), Error> {
        let errors = self.forced.native_errors(&self.db)?;
        if errors > state.background_errors {
            let new = errors - state.background_errors;
            state.background_errors = errors;
            state.status.native_errors = state.status.native_errors.saturating_add(new);
            state.status.failure = Some(match forced {
                Some(forced) => format!(
                    "{forced}; RocksDB reported {new} new background errors ({} this session)",
                    state.status.native_errors
                ),
                None => format!(
                    "RocksDB reported {new} new background errors ({} this session) while no pressure pass was waiting on native work",
                    state.status.native_errors
                ),
            });
        }
        Ok(())
    }

    /// The floor episode of a store with no archive policy: nothing is retired early, and the
    /// status says so. Ageing out is the only avenue there.
    fn no_archive_floor(
        &self,
        state: &mut State,
        now: u64,
        forced_failure: Option<&str>,
    ) -> Result<PressureStatus, Error> {
        state.enter(PressurePhase::Blocked, now);
        state.stalled("this store has no archive policy, so nothing is retired early");
        self.surface_native_errors(state, forced_failure)?;
        Ok(state.status.clone())
    }

    /// Open the wait that follows a retirement: it ends when linked bytes fall below `linked`,
    /// and a forced compaction is started if they do not. `durable` says a marker was staged
    /// with the retirement batch. Native flushes are scheduled without holding the writer
    /// through disk I/O; automatic compaction and blob GC stay with the background jobs.
    fn begin_wait(
        &self,
        state: &mut State,
        linked: u64,
        now: u64,
        durable: bool,
        progress: &str,
    ) -> Result<(), Error> {
        state.wait = Some(Wait {
            before: linked,
            step: Step::Native,
            durable,
        });
        state.status.wait_before_bytes = linked;
        state.status.wait_since = now.max(1);
        state.status.waiting_on = None;
        state.enter(PressurePhase::AwaitingReclamation, now);
        if state.floor.asked {
            let text = state.floor_text(progress);
            state.status.waiting_on = Some(text);
        }
        let mut options = FlushOptions::default();
        options.set_wait(false);
        self.db
            .flush_cfs_opt(&super::family_handles(&self.db)?, &options)?;
        Ok(())
    }

    /// Ageing out was refused because free space is under the maintenance reserve: nothing
    /// was attempted, so the episode's early retirement still waits for it. It is asked
    /// again when free space next clears the reserve (see `ask_expiry`).
    pub(super) fn expiry_refused_at_reserve(&self) {
        self.pressure.borrow_mut().floor.expiry_asked = false;
    }

    /// Wake ageing out the first time free space clears the maintenance reserve after a
    /// refusal at it (the ask is outstanding until a refusal clears it): the daemon's resume
    /// edge fires only at the floor, and a rise that stops between the reserve and the floor
    /// is the range ageing out is admitted in. This holds whether or not an earlier pass of
    /// the episode ran, because a backlog can be refused part way. The pass is the observer
    /// (it runs on its own backoff and is itself woken by every event that matters); no
    /// timer is added.
    fn ask_expiry(&self, state: &mut State) -> Result<(), Error> {
        if state.floor.expiry_asked {
            return Ok(());
        }
        if self
            .space
            .shortfall_below(self.config.maintenance_reserve)?
            .is_some()
        {
            return Ok(());
        }
        state.floor.expiry_asked = true;
        self.wake_pressure_pass();
        Ok(())
    }

    /// Ageing out ran below the floor (#267), or failed: the floor
    /// episode may go on to early retirement. When it retired evidence for a floor episode
    /// that asked, open an in-memory wait (nothing is stored, so ending it needs no commit)
    /// so the pass measures whether linked bytes fell and forces a compaction if they did
    /// not. `linked_before` is the linked store bytes measured before the retirement; `None`
    /// means nothing retired.
    pub(super) fn expiry_ran_below_floor(
        &self,
        now: u64,
        linked_before: Option<u64>,
    ) -> Result<(), Error> {
        let mut state = self.pressure.borrow_mut();
        state.floor.expiry_tried = true;
        if let Some(linked) = linked_before
            && state.floor.asked
            && state.wait.is_none()
        {
            self.begin_wait(
                &mut state,
                linked,
                now,
                false,
                "ageing out retired evidence and the store waits for native work to return its bytes",
            )?;
        }
        Ok(())
    }

    fn maintain_pressure(&self, now: u64, limits: EvictionLimits) -> Result<PressureStatus, Error> {
        let physical = self.physical_bytes()?;
        let linked = self.linked_physical_bytes()?;
        let limit = self.config.disk_budget - self.config.maintenance_reserve;
        // The floor is measured here, in the pass, and never put into `requested_bytes`: that
        // field is a store-footprint quantity compared with the disk budget, a deficit is a
        // free-space quantity of the filesystem (#267).
        let deficit = self.space.deficit()?;
        let mut state = self.pressure.borrow_mut();
        state.status.observed_at = now;
        state.status.linked_bytes = linked;
        let floor_ended = state.floor.asked && deficit == 0;
        if floor_ended {
            state.end_floor_episode();
        }
        let floor_on = state.floor.asked && deficit > 0;
        state.floor.deficit = if floor_on { deficit } else { 0 };
        if floor_on {
            state.status.floor_requested_bytes = deficit;
            let start = *state.floor.start.get_or_insert(FloorStart {
                linked,
                bound: deficit.saturating_add(self.config.maintenance_reserve),
            });
            state.status.floor_freed_bytes = start.linked.saturating_sub(linked);
            if state.status.phase == PressurePhase::Ready {
                state.status.phase = PressurePhase::Requested;
            }
        }
        if state.status.requested_bytes == 0
            && physical <= limit
            && !floor_on
            && !floor_ended
            && state.wait.is_none()
        {
            // No episode is active, but a RocksDB background error is still news: surface it
            // now instead of at the next episode's first pass.
            self.surface_native_errors(&mut state, None)?;
            return Ok(state.status.clone());
        }
        // A reopened store already above its data allowance needs maintenance too.
        if state.status.requested_bytes == 0 && physical > limit {
            state.status.requested_bytes = 65536.min(limit);
            state.status.phase = PressurePhase::Requested;
        }
        state.stamp(now);
        let forced_failure = self.collect_forced(&mut state)?;
        let reason = forced_failure.clone();
        // The duty treats a failed pass's error as carrying a collected forced failure's
        // reason, so any later error in this pass must carry it too.
        self.retire_pass(
            &mut state,
            now,
            limits,
            (physical, linked, limit),
            forced_failure,
        )
        .map_err(|error| match reason {
            Some(forced) if !error.message.contains(&forced) => {
                Error::new(error.code, format!("{error}; {forced}"))
            }
            _ => error,
        })
    }

    /// The rest of a pressure pass once a finished forced compaction has been collected.
    fn retire_pass(
        &self,
        state: &mut State,
        now: u64,
        limits: EvictionLimits,
        (physical, linked, limit): (u64, u64, u64),
        forced_failure: Option<String>,
    ) -> Result<PressureStatus, Error> {
        let floor_on = state.floor.deficit > 0;
        if physical.saturating_add(state.status.requested_bytes) <= limit && !floor_on {
            if state.wait.is_some_and(|wait| wait.durable) {
                let mut batch = rocksdb::WriteBatch::default();
                batch.delete(WAIT_KEY);
                self.reclaiming(|| self.commit(batch))?;
            }
            // A finished compaction of this wait must not stand in for the next one's.
            self.forced.acknowledge();
            state.ready();
            // Ready clears the failure text; a collected forced failure keeps its reason.
            state.status.failure.clone_from(&forced_failure);
            self.eviction.borrow_mut().reset();
            self.surface_native_errors(state, forced_failure.as_deref())?;
            return Ok(state.status.clone());
        }
        if linked.saturating_add(state.status.requested_bytes) <= limit && !floor_on {
            // Temporary query reservations alone are not authority to retire evidence.
            state.stalled("only temporary query reservations hold the data allowance");
            self.surface_native_errors(state, forced_failure.as_deref())?;
            return Ok(state.status.clone());
        }
        // The floor alone (the store's own linked footprint is inside its budget) is authority
        // to retire only evidence an archive holds a copy of. A store with no archive policy
        // has no copy of anything, so the floor retires nothing early there and the status
        // says so. This is judged on linked bytes, as the reservation stall above is: query
        // reservations are in `physical` but are no authority, so judging on `physical` would
        // let a floor episode with reservations skip the guard and the bound below. Every path
        // to the retirement with the floor on and no budget authority passes through both.
        let floor_only = floor_on && linked.saturating_add(state.status.requested_bytes) <= limit;
        if floor_only {
            self.ask_expiry(state)?;
        }
        // With no archive policy ageing out is the only avenue, and its retirement can leave a
        // wait open that only this pass measures and ends (forced compaction included), so a
        // store with such a wait goes on through the gates and refuses after them.
        let no_archive = floor_only && self.archive.is_none();
        if no_archive && state.wait.is_none() {
            return self.no_archive_floor(state, now, forced_failure.as_deref());
        }
        state.status.pending_flushes = 0;
        state.status.pending_compactions = 0;
        // Only an error new since the last pass fails this one. The native count is
        // cumulative since open and cannot be cleared in place, so an old error must never
        // keep failing every pass: intake would stay refused with nothing working to end it.
        // The failed wait ends here, so the next pass retires what the archive gate allows.
        let errors = self.forced.native_errors(&self.db)?;
        if errors > state.background_errors {
            let new = errors - state.background_errors;
            // The wait may already have ended with a failed forced compaction.
            if state.wait.is_some() {
                // The baseline moves only once the wait has ended, so a store that cannot
                // take the write is reported again on every pass.
                self.end_wait(state)?;
            }
            state.background_errors = errors;
            state.status.native_errors = state.status.native_errors.saturating_add(new);
            self.forced.acknowledge();
            let forced = forced_failure
                .map(|failure| format!("; {failure}"))
                .unwrap_or_default();
            return Err(Error::new(
                "STORAGE_FAILURE",
                format!(
                    "RocksDB reported {new} new background errors ({} this session); the pressure wait ended and the next pass retires more evidence{forced}",
                    state.status.native_errors
                ),
            ));
        }
        for family in super::FAMILIES {
            state.status.pending_flushes += self
                .native_property(family, "rocksdb.mem-table-flush-pending")?
                + self.native_property(family, "rocksdb.num-running-flushes")?;
            state.status.pending_compactions += self
                .native_property(family, "rocksdb.compaction-pending")?
                + self.native_property(family, "rocksdb.num-running-compactions")?;
        }
        state.status.snapshots = self.native_property("default", "rocksdb.num-snapshots")?;
        if state.status.pending_flushes != 0
            || state.status.pending_compactions != 0
            || state.status.snapshots != 0
        {
            let reason = if state.status.pending_flushes != 0 {
                "a native flush"
            } else if state.status.pending_compactions != 0 {
                "a native compaction"
            } else {
                "open read snapshots"
            };
            state.stalled(format!("waiting for {reason} to finish"));
            return Ok(state.status.clone());
        }
        // Everything below writes: a retirement batch, its flush and a compaction. Each
        // is small beside the maintenance reserve the budget withholds for them, but not
        // beside a filesystem with less than that free. The free-space floor itself does
        // not stop them: they are what frees space, and refusing intake already stops
        // the rest of the writers.
        if let Some(shortfall) = self
            .space
            .shortfall_below(self.config.maintenance_reserve)?
        {
            state.stalled(shortfall);
            return Ok(state.status.clone());
        }
        if let Some(wait) = state.wait {
            if linked < wait.before {
                self.forced.acknowledge();
                self.end_wait(state)?;
            } else {
                // Linked bytes have not fallen and no native work is pending or running:
                // nothing is working to end the wait, so force one compaction.
                match wait.step {
                    Step::Native => {
                        // A compaction that ended after its own wait was cleared (the
                        // Ready branch cannot acknowledge one still running) belongs to
                        // no wait; only a running one blocks this wait's.
                        self.forced.acknowledge();
                        state.enter(PressurePhase::AwaitingReclamation, now);
                        if self.forced.start(std::sync::Arc::clone(&self.db))? {
                            state.wait = Some(Wait {
                                step: Step::Forced,
                                ..wait
                            });
                            state.status.forced_compactions =
                                state.status.forced_compactions.saturating_add(1);
                            state.stalled("forced a compaction: linked bytes had not fallen");
                        } else {
                            // An earlier compaction is still running: it is native work.
                            state.stalled("an earlier forced compaction is still running");
                        }
                        return Ok(state.status.clone());
                    }
                    Step::Forced => {
                        match self.forced.poll() {
                            Slot::Running => {
                                state.stalled("a forced compaction is running");
                                return Ok(state.status.clone());
                            }
                            Slot::Finished(_) | Slot::Idle => {}
                        }
                        self.forced.acknowledge();
                        state.wait = Some(Wait {
                            step: Step::Measured,
                            ..wait
                        });
                        // The compaction's own deletions land after it returns; measure
                        // on the next pass.
                        state.stalled("a forced compaction ended; measuring linked bytes");
                        return Ok(state.status.clone());
                    }
                    Step::Measured => {
                        // The retirement is durable and a compaction left the footprint
                        // where it was, so waiting longer protects nothing: retire more.
                        self.end_wait(state)?;
                        state.status.waits_released = state.status.waits_released.saturating_add(1);
                    }
                }
            }
        }
        if no_archive {
            return self.no_archive_floor(state, now, forced_failure.as_deref());
        }
        if floor_only && !state.floor.expiry_tried {
            // Ageing out is normal retention that was only delayed; early retirement of
            // archived evidence is the policy-sensitive avenue and goes second. Expiry's
            // arrival (success, refusal or failure) wakes the pass.
            state.enter(PressurePhase::Requested, now);
            state.stalled("waiting for ageing out to run first");
            self.surface_native_errors(state, forced_failure.as_deref())?;
            return Ok(state.status.clone());
        }
        if floor_only && let Some(start) = state.floor.start {
            let freed = start.linked.saturating_sub(linked);
            let batches = state.floor.batches;
            if freed >= start.bound || batches >= FLOOR_EPISODE_BATCHES {
                // The episode has retired as much as it may for an outside cause: the
                // rest of the shortfall is held outside Sparrow. Nothing more is retired
                // until the deficit ends the episode or a new one begins.
                state.status.floor_bound_reached = true;
                state.enter(PressurePhase::Blocked, now);
                let reason = if freed >= start.bound {
                    format!(
                        "the store freed {freed} bytes for this floor episode (bound {}) and free space is still short; the rest is held outside Sparrow",
                        start.bound
                    )
                } else {
                    format!(
                        "the store retired {batches} batches for this floor episode (cap {FLOOR_EPISODE_BATCHES}) and free space is still short; the rest is held outside Sparrow"
                    )
                };
                state.stalled(reason);
                self.surface_native_errors(state, forced_failure.as_deref())?;
                return Ok(state.status.clone());
            }
        }
        state.status.passes = state.status.passes.saturating_add(1);
        if floor_on {
            state.status.floor_passes = state.status.floor_passes.saturating_add(1);
            state.floor.passes = state.floor.passes.saturating_add(1);
        }
        #[cfg(feature = "test-support")]
        if self.forced.retirement_refused() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "retirement refused by a test",
            ));
        }
        // The durable wait marker carries a budget request, never a deficit: a floor-only
        // retirement writes the value a store reopened above its allowance already uses.
        let marker = match state.status.requested_bytes {
            0 => FLOOR_MARKER_BYTES,
            requested => requested,
        };
        let report = self.reclaiming(|| self.evict_oldest(now, limits, Some((linked, marker))))?;
        state.status.evicted_records = state
            .status
            .evicted_records
            .saturating_add(report.records.len() as u64);
        state.status.evicted_recordless_artifacts = state
            .status
            .evicted_recordless_artifacts
            .saturating_add(report.recordless_artifacts as u64);
        state.status.payload_refs_retired = state
            .status
            .payload_refs_retired
            .saturating_add(report.payload_refs as u64);
        let changed =
            !report.records.is_empty() || !report.artifacts.is_empty() || report.payload_refs != 0;
        let continuing = report.continuation;
        let archive_lag = report.frontier == super::eviction::EvictionFrontier::ArchiveLag;
        let protected = report.protected;
        state.status.last_pass = Some(report);
        // A forced compaction's failure stays visible for the pass that ended its wait.
        state.status.failure = forced_failure;
        if changed {
            if floor_on {
                state.floor.batches = state.floor.batches.saturating_add(1);
            }
            self.begin_wait(
                state,
                linked,
                now,
                true,
                "reclamation in progress: evidence was retired and the store waits for native work to return its bytes",
            )?;
        } else if archive_lag {
            state.enter(PressurePhase::AwaitingArchive, now);
            let reason =
                state.floor_tried("the archive watermark has not reached the oldest evidence");
            state.stalled(reason);
        } else if continuing {
            state.status.waiting_on = None;
            state.enter(PressurePhase::Requested, now);
            if floor_on {
                let text =
                    state.floor_text("reclamation in progress: more evidence remains to retire");
                state.status.waiting_on = Some(text);
            }
        } else {
            state.enter(PressurePhase::Blocked, now);
            let reason = state.floor_tried(&if protected == 0 {
                "no evidence is left to retire".to_owned()
            } else {
                format!("no evidence could be retired: {protected} entries are protected by a pin")
            });
            state.stalled(reason);
        }
        Ok(state.status.clone())
    }
}

impl Store {
    /// Session-local pressure observations; reads do not request retirement.
    /// Reads the observed state without the writer lock, and refuses once it is poisoned.
    pub fn pressure_status(&self) -> Result<PressureStatus, Error> {
        self.refuse_poisoned()?;
        let mut status = self.unlocked.pressure.read();
        status.floor = self.space.status();
        Ok(status)
    }

    /// Dev-only: arm a fault for the next forced compaction.
    #[cfg(feature = "test-support")]
    pub fn arm_forced_compaction_fault(&self, fault: super::forced_compaction::Fault) {
        self.unlocked.forced.arm(fault);
    }

    /// Dev-only: make the commit that ends a pressure wait fail, as a store that cannot
    /// take a write would.
    #[cfg(feature = "test-support")]
    pub fn refuse_pressure_wait_end(&self, refused: bool) {
        self.unlocked.forced.refuse_wait_end(refused);
    }

    /// Dev-only: make the retirement of each pass fail, as a store that cannot retire would.
    #[cfg(feature = "test-support")]
    pub fn refuse_pressure_retirement(&self, refused: bool) {
        self.unlocked.forced.refuse_retirement(refused);
    }

    /// Dev-only: count native background errors a real failing compaction would raise.
    #[cfg(feature = "test-support")]
    pub fn simulate_native_errors(&self, count: u64) {
        self.unlocked.forced.simulate_native_errors(count);
    }

    /// Dev-only: leave a finished, unacknowledged forced compaction in the slot.
    #[cfg(feature = "test-support")]
    pub fn leave_forced_compaction_finished(&self) {
        self.unlocked.forced.leave_finished();
    }

    /// Block until no forced compaction is running and RocksDB has no flush or compaction
    /// queued or running, so a test observes the end of native work by event, never by time.
    #[cfg(feature = "test-support")]
    pub fn await_native_quiet(&self) -> Result<(), Error> {
        self.unlocked.forced.wait_finished();
        self.unlocked
            .db
            .wait_for_compact(&rocksdb::WaitForCompactOptions::default())?;
        Ok(())
    }

    /// One bounded pressure pass, serialized with pins and all ingestion writers.
    pub fn pressure_maintain(
        &self,
        now: u64,
        limits: EvictionLimits,
    ) -> Result<PressureStatus, Error> {
        let writer = self.lock()?;
        writer
            .maintain_pressure(now, limits)
            .inspect_err(|error| {
                let mut state = writer.pressure.borrow_mut();
                state.enter(PressurePhase::Blocked, now);
                state.status.failure = Some(error.to_string().chars().take(1024).collect());
                state.status.stalled_passes = state.status.stalled_passes.saturating_add(1);
            })
            .map(|mut status| {
                status.floor = self.space.status();
                status
            })
    }
}
