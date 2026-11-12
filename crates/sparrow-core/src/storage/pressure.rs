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
    /// Latched actionable maintenance failure; cleared by a successful pass.
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
    /// Table bytes of each family before and after that compaction; empty before one.
    pub forced_compaction_families: Vec<FamilySize>,
    /// Forced compactions that ended in a failure (a panic or a raised native error).
    pub forced_failures: u64,
    /// New RocksDB background errors this session's pressure passes saw. Each is reported
    /// once as a failed pass and then counted here; none latches the worker.
    pub native_errors: u64,
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

#[derive(Default)]
pub(super) struct State {
    pub(super) status: PressureStatus,
    wait: Option<Wait>,
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
            }),
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
    }

    /// End the wait without ending the episode.
    fn end_wait(&mut self) {
        self.wait = None;
        self.status.wait_since = 0;
        self.status.wait_before_bytes = 0;
    }

    /// A pass that could do nothing more, and why.
    fn stalled(&mut self, reason: impl Into<String>) {
        self.status.stalled_passes = self.status.stalled_passes.saturating_add(1);
        self.status.waiting_on = Some(reason.into());
    }
}

/// The existing conservative batch output/WAL estimate, shared by boot and live writes.
pub(super) fn data_batch_estimate(batch: &rocksdb::WriteBatch) -> u64 {
    (batch.size_in_bytes() as u64)
        .saturating_mul(4)
        .saturating_add(65536)
}

/// Largest measured footprint that still leaves the admitted data-write allowance.
pub(super) fn data_write_ceiling(
    disk_budget: u64,
    reserve: u64,
    estimate: u64,
) -> Result<u64, Error> {
    if estimate == 0 {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "data write requires a positive capacity estimate",
        ));
    }
    let limit = disk_budget.checked_sub(reserve).ok_or_else(|| {
        Error::new(
            "INVALID_ARGUMENT",
            "disk budget must exceed maintenance reserve",
        )
    })?;
    limit.checked_sub(estimate).ok_or_else(||Error::new("RESOURCE_LIMIT","estimated atomic write cannot fit the configured data budget; increase the disk budget or reduce the operation"))
}

impl Writer {
    /// Called before publishing a prepared data batch; never mutates its dependencies.
    pub(super) fn admit_data_write(&self, estimate: u64) -> Result<(), Error> {
        let ceiling = data_write_ceiling(
            self.config.disk_budget,
            self.config.maintenance_reserve,
            estimate,
        )?;
        if self.physical_bytes()? <= ceiling {
            return Ok(());
        }
        let mut pressure = self.pressure.borrow_mut();
        let requested = &mut pressure.status.requested_bytes;
        *requested = if *requested == 0 {
            estimate
        } else {
            (*requested).min(estimate)
        };
        if pressure.status.phase == PressurePhase::Ready {
            pressure.status.phase = PressurePhase::Requested;
        }
        pressure.status.refused_writes = pressure.status.refused_writes.saturating_add(1);
        Err(Error::new(
            "STORAGE_PRESSURE",
            "write would cross the maintenance reserve; bounded pressure maintenance requested; retry after measured physical recovery",
        ))
    }

    /// Run reclamation commits (the retirement batch and the wait clear, nothing else)
    /// under the maintenance reserve instead of the free-space floor: they are what
    /// frees space, and ordinary writes keep the floor.
    fn reclaiming<T>(&self, work: impl FnOnce() -> Result<T, Error>) -> Result<T, Error> {
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
    pub(super) fn admit_space(&self) -> Result<(), Error> {
        if !self.reclaiming.get() {
            return self.space.check();
        }
        match self
            .space
            .shortfall_below(self.config.maintenance_reserve)?
        {
            Some(shortfall) => Err(Error::new("STORAGE_PRESSURE", shortfall)),
            None => Ok(()),
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
    fn end_wait(&self, state: &mut State) -> Result<(), Error> {
        let mut batch = rocksdb::WriteBatch::default();
        batch.delete(WAIT_KEY);
        self.reclaiming(|| self.commit(batch))?;
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
            if state.wait.is_some() {
                self.end_wait(state)?;
            }
        }
        self.forced.acknowledge();
        Ok(failure)
    }

    fn maintain_pressure(&self, now: u64, limits: EvictionLimits) -> Result<PressureStatus, Error> {
        let physical = self.physical_bytes()?;
        let linked = self.linked_physical_bytes()?;
        let limit = self.config.disk_budget - self.config.maintenance_reserve;
        let mut state = self.pressure.borrow_mut();
        state.status.observed_at = now;
        state.status.linked_bytes = linked;
        if state.status.requested_bytes == 0 && physical <= limit {
            return Ok(state.status.clone());
        }
        // A reopened store already above its data allowance needs maintenance too.
        if state.status.requested_bytes == 0 {
            state.status.requested_bytes = 65536.min(limit);
            state.status.phase = PressurePhase::Requested;
        }
        state.stamp(now);
        let forced_failure = self.collect_forced(&mut state)?;
        if physical.saturating_add(state.status.requested_bytes) <= limit {
            if state.wait.is_some() {
                let mut batch = rocksdb::WriteBatch::default();
                batch.delete(WAIT_KEY);
                self.reclaiming(|| self.commit(batch))?;
            }
            // A finished compaction of this wait must not stand in for the next one's.
            self.forced.acknowledge();
            state.ready();
            self.eviction.borrow_mut().reset();
            return Ok(state.status.clone());
        }
        if linked.saturating_add(state.status.requested_bytes) <= limit {
            // Temporary query reservations alone are not authority to retire evidence.
            state.stalled("only temporary query reservations hold the data allowance");
            return Ok(state.status.clone());
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
            state.background_errors = errors;
            state.status.native_errors = state.status.native_errors.saturating_add(new);
            self.end_wait(&mut state)?;
            self.forced.acknowledge();
            return Err(Error::new(
                "STORAGE_FAILURE",
                format!(
                    "RocksDB reported {new} new background errors ({} this session); the pressure wait ended and the next pass retires more evidence",
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
                self.end_wait(&mut state)?;
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
                        self.end_wait(&mut state)?;
                        state.status.waits_released = state.status.waits_released.saturating_add(1);
                    }
                }
            }
        }
        state.status.passes = state.status.passes.saturating_add(1);
        let requested = state.status.requested_bytes;
        let report =
            self.reclaiming(|| self.evict_oldest(now, limits, Some((linked, requested))))?;
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
            state.wait = Some(Wait {
                before: linked,
                step: Step::Native,
            });
            state.status.wait_before_bytes = linked;
            state.status.wait_since = now.max(1);
            state.status.waiting_on = None;
            state.enter(PressurePhase::AwaitingReclamation, now);
            // Schedule native flushes without holding the writer through disk I/O.
            // Automatic native compaction/blob GC remains limited to the background jobs.
            let mut options = FlushOptions::default();
            options.set_wait(false);
            self.db
                .flush_cfs_opt(&super::family_handles(&self.db)?, &options)?;
        } else if archive_lag {
            state.enter(PressurePhase::AwaitingArchive, now);
            state.stalled("the archive watermark has not reached the oldest evidence");
        } else if continuing {
            state.status.waiting_on = None;
            state.enter(PressurePhase::Requested, now);
        } else {
            state.enter(PressurePhase::Blocked, now);
            state.stalled(format!(
                "no evidence could be retired: {protected} entries are protected by a pin"
            ));
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
