// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One physical admission signal and bounded, pin-safe pressure worker.

use super::{
    Error, Store, Writer,
    eviction::{EvictionLimits, EvictionReport},
};
use rocksdb::FlushOptions;

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
    /// Native flush pending/running observations across both families.
    pub pending_flushes: u64,
    /// Native compaction pending/running observations across both families.
    pub pending_compactions: u64,
    /// Native snapshot count at the last maintenance observation.
    pub snapshots: u64,
    /// Last successful bounded retirement report, including its explicit frontier.
    pub last_pass: Option<EvictionReport>,
    /// Latched actionable maintenance failure; cleared by a successful pass.
    pub failure: Option<String>,
}

#[derive(Default)]
pub(super) struct State {
    status: PressureStatus,
    awaiting_physical: Option<u64>,
}

impl State {
    pub(super) fn load(db: &rocksdb::DB) -> Result<Self, Error> {
        let Some(bytes) = db.get_pinned(WAIT_KEY)? else {
            return Ok(Self::default());
        };
        if !matches!(
            db.get("store_format")?.as_deref(),
            Some(FORMAT | WATCH_FORMAT)
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
                ..PressureStatus::default()
            },
            awaiting_physical: Some(physical),
        })
    }
}

impl Writer {
    /// Called before publishing a prepared data batch; never mutates its dependencies.
    pub(super) fn admit_data_write(&self, estimate: u64) -> Result<(), Error> {
        if estimate == 0 {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "data write requires a positive capacity estimate",
            ));
        }
        let limit = self.config.disk_budget - self.config.maintenance_reserve;
        if estimate > limit {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "estimated atomic write cannot fit the configured data budget; increase the disk budget or reduce the operation",
            ));
        }
        if self.physical_bytes()?.saturating_add(estimate) <= limit {
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
        Err(Error::new(
            "STORAGE_PRESSURE",
            "write would cross the maintenance reserve; bounded pressure maintenance requested; retry after measured physical recovery",
        ))
    }

    fn native_property(&self, family: &str, name: &str) -> Result<u64, Error> {
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

    fn maintain_pressure(&self, now: u64, limits: EvictionLimits) -> Result<PressureStatus, Error> {
        let physical = self.physical_bytes()?;
        let linked = self.linked_physical_bytes()?;
        let limit = self.config.disk_budget - self.config.maintenance_reserve;
        let mut state = self.pressure.borrow_mut();
        if state.status.requested_bytes == 0 && physical <= limit {
            return Ok(state.status.clone());
        }
        // A reopened store already above its data allowance needs maintenance too.
        if state.status.requested_bytes == 0 {
            state.status.requested_bytes = 65536.min(limit);
            state.status.phase = PressurePhase::Requested;
        }
        if physical.saturating_add(state.status.requested_bytes) <= limit {
            if state.awaiting_physical.is_some() {
                let mut batch = rocksdb::WriteBatch::default();
                batch.delete(WAIT_KEY);
                self.commit(batch)?;
            }
            state.status.requested_bytes = 0;
            state.status.phase = PressurePhase::Ready;
            state.status.failure = None;
            state.awaiting_physical = None;
            self.eviction.borrow_mut().reset();
            return Ok(state.status.clone());
        }
        if linked.saturating_add(state.status.requested_bytes) <= limit {
            // Temporary query reservations alone are not authority to retire evidence.
            return Ok(state.status.clone());
        }
        state.status.pending_flushes = 0;
        state.status.pending_compactions = 0;
        for family in ["default", "payload"] {
            if self.native_property(family, "rocksdb.background-errors")? != 0 {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "RocksDB reports background errors; reopen and inspect storage before reclaiming evidence",
                ));
            }
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
            return Ok(state.status.clone());
        }
        if state
            .awaiting_physical
            .is_some_and(|before| linked >= before)
        {
            // Do not repeatedly retire more evidence while native reclamation lags.
            state.status.phase = PressurePhase::AwaitingReclamation;
            return Ok(state.status.clone());
        }
        state.awaiting_physical = None;
        state.status.passes = state.status.passes.saturating_add(1);
        let report =
            self.evict_oldest(now, limits, Some((linked, state.status.requested_bytes)))?;
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
        state.status.last_pass = Some(report);
        state.status.failure = None;
        if changed {
            state.awaiting_physical = Some(linked);
            state.status.phase = PressurePhase::AwaitingReclamation;
            // Schedule native flushes without holding the writer through disk I/O.
            // Automatic native compaction/blob GC remains limited to two background jobs.
            let mut options = FlushOptions::default();
            options.set_wait(false);
            let default = self
                .db
                .cf_handle("default")
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "default family missing"))?;
            let payload = self
                .db
                .cf_handle("payload")
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "payload family missing"))?;
            self.db.flush_cfs_opt(&[&default, &payload], &options)?;
        } else if continuing {
            state.status.phase = PressurePhase::Requested;
        } else {
            state.status.phase = PressurePhase::Blocked;
        }
        Ok(state.status.clone())
    }
}

impl Store {
    /// Session-local pressure observations; reads do not request retirement.
    pub fn pressure_status(&self) -> Result<PressureStatus, Error> {
        Ok(self.lock()?.pressure.borrow().status.clone())
    }

    /// One bounded pressure pass, serialized with pins and all ingestion writers.
    pub fn pressure_maintain(
        &self,
        now: u64,
        limits: EvictionLimits,
    ) -> Result<PressureStatus, Error> {
        let writer = self.lock()?;
        writer.maintain_pressure(now, limits).inspect_err(|error| {
            let mut state = writer.pressure.borrow_mut();
            state.status.phase = PressurePhase::Blocked;
            state.status.failure = Some(error.to_string().chars().take(1024).collect());
        })
    }
}
