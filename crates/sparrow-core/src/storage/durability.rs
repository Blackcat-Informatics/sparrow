// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One serialized WAL boundary for durable control and bounded buffered intake.

use super::{Artifact, Error, Store, Writer};
use rocksdb::{WriteBatch, WriteOptions};
use std::{collections::BTreeMap, time::Instant};

/// Maximum accepted writes awaiting an explicit synchronization boundary.
pub const MAX_BUFFERED_WRITES: usize = 256;
/// Maximum admitted atomic batch bytes awaiting synchronization.
pub const MAX_BUFFERED_BYTES: usize = 64 * 1024 * 1024;
/// Default intended periodic synchronization interval.
pub const DEFAULT_FLUSH_INTERVAL_MS: u32 = 2000;

/// A synchronization fence, scoped to a persistent store and one opened session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurabilityWatermark {
    /// Persistent store identity.
    pub store_id: String,
    /// Fresh identity for this database-open session; old-session fences are refused.
    pub session_id: String,
    /// RocksDB sequence through which synchronization is requested/reached.
    pub sequence: u64,
}

/// Requested acknowledgment boundary; buffered acceptance does not promise host-crash survival.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// Atomic visible publication with WAL enabled but no synchronization promise.
    Buffered,
    /// All dependencies cross the synchronized WAL boundary before acknowledgment.
    Durable,
}

/// Bounded unsynchronized-write telemetry, independent of retention lifetime.
#[derive(Clone, Debug)]
pub struct DurabilityStatus {
    /// Intended configurable periodic synchronization interval.
    pub flush_interval_ms: u32,
    /// Age beyond that intended interval; this is not a synchronization guarantee.
    pub flush_lag_ms: u64,
    /// Current opened-session identity.
    pub session_id: String,
    /// Latest accepted database sequence, including control mutations.
    pub accepted_through: u64,
    /// Highest sequence certified by a successful synchronization.
    pub durable_through: u64,
    /// Buffered atomic writes still owned by the durability ledger.
    pub pending_writes: usize,
    /// Their conservative atomic batch byte charge.
    pub pending_bytes: usize,
    /// Oldest buffered acceptance age in milliseconds; zero when empty.
    pub oldest_pending_ms: u64,
    /// Latched storage uncertainty; reopening is required before receipt certification.
    pub failure: Option<String>,
}

struct Pending {
    sequence: u64,
    at: Instant,
}
pub(super) struct State {
    session_id: String,
    durable_through: u64,
    pending: BTreeMap<String, Pending>,
    bytes: usize,
    failure: Option<String>,
}
impl State {
    pub(super) fn new(session_id: String, sequence: u64) -> Self {
        Self {
            session_id,
            durable_through: sequence,
            pending: BTreeMap::new(),
            bytes: 0,
            failure: None,
        }
    }
    fn check(&self) -> Result<(), Error> {
        if let Some(failure) = &self.failure {
            return Err(Error::new(
                "STORAGE_FAILURE",
                format!("durability uncertain; reopen and inspect the store: {failure}"),
            ));
        }
        Ok(())
    }
    fn reached(&mut self, sequence: u64) {
        self.durable_through = sequence;
        self.pending.clear();
        self.bytes = 0;
    }
}

impl Writer {
    fn watermark(&self, sequence: u64) -> DurabilityWatermark {
        DurabilityWatermark {
            store_id: self.id.clone(),
            session_id: self.durability.borrow().session_id.clone(),
            sequence,
        }
    }
    pub(super) fn receipt(
        &self,
        mut artifact: Artifact,
        mode: Durability,
    ) -> Result<Artifact, Error> {
        self.durability.borrow().check()?;
        if mode == Durability::Durable
            && self.durability.borrow().pending.contains_key(&artifact.id)
        {
            self.synchronize()?;
        }
        artifact.buffered = self
            .durability
            .borrow()
            .pending
            .get(&artifact.id)
            .map(|pending| self.watermark(pending.sequence));
        Ok(artifact)
    }

    /// Every write uses WAL. Only complete intake batches may request buffered acknowledgment.
    pub(super) fn apply(&self, batch: WriteBatch, buffered_id: Option<&str>) -> Result<(), Error> {
        let bytes = batch.size_in_bytes();
        {
            let state = self.durability.borrow();
            state.check()?;
            if let Some(id) = buffered_id {
                if state.pending.contains_key(id) {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "buffered occurrence identity already occupied",
                    ));
                }
                if state.pending.len() == MAX_BUFFERED_WRITES
                    || bytes > MAX_BUFFERED_BYTES - state.bytes
                {
                    return Err(Error::new(
                        "STORAGE_PRESSURE",
                        "buffered durability ledger is full; synchronize before retrying",
                    ));
                }
            }
        }
        if buffered_id.is_none() && batch.is_empty() {
            self.synchronize()?;
            return Ok(());
        }
        let mut options = WriteOptions::default();
        options.disable_wal(false);
        options.set_sync(buffered_id.is_none());
        if let Err(error) = self.db.write_opt(batch, &options) {
            self.durability.borrow_mut().failure = Some(error.to_string());
            return Err(error.into());
        }
        let sequence = self.db.latest_sequence_number();
        let mut state = self.durability.borrow_mut();
        if let Some(id) = buffered_id {
            state.bytes += bytes;
            state.pending.insert(
                id.to_owned(),
                Pending {
                    sequence,
                    at: Instant::now(),
                },
            );
        } else {
            // A synchronized write covers earlier writes in the shared WAL.
            state.reached(sequence);
        }
        Ok(())
    }

    pub(super) fn synchronize(&self) -> Result<DurabilityWatermark, Error> {
        self.durability.borrow().check()?;
        if let Err(error) = self.db.flush_wal(true) {
            self.durability.borrow_mut().failure = Some(error.to_string());
            return Err(error.into());
        }
        let sequence = self.db.latest_sequence_number();
        self.durability.borrow_mut().reached(sequence);
        Ok(self.watermark(sequence))
    }
}

impl Store {
    /// Synchronize through a specified current-session fence, refusing future/stale tokens.
    /// The serialized writer can also cover later accepted writes in this session.
    pub fn flush_through(
        &self,
        required: &DurabilityWatermark,
    ) -> Result<DurabilityWatermark, Error> {
        let writer = self.lock()?;
        if required.store_id != writer.id {
            return Err(Error::new(
                "STORE_MISMATCH",
                "durability fence belongs to a different store",
            ));
        }
        if required.session_id != writer.durability.borrow().session_id {
            return Err(Error::new(
                "STALE_WATERMARK",
                "durability fence belongs to an earlier opened session; inspect the original artifact or retry its request",
            ));
        }
        if required.sequence > writer.db.latest_sequence_number() {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "durability fence names a future sequence",
            ));
        }
        writer.synchronize()
    }

    /// Intended periodic boundary; does no I/O when there is no pending buffered intake.
    pub fn flush_buffered(&self) -> Result<(), Error> {
        let writer = self.lock()?;
        writer.durability.borrow().check()?;
        if !writer.durability.borrow().pending.is_empty() {
            writer.synchronize()?;
        }
        Ok(())
    }

    /// Inspect current sequence, queue charge, age and any latched uncertainty.
    pub fn durability_status(&self) -> Result<DurabilityStatus, Error> {
        let writer = self.lock()?;
        let state = writer.durability.borrow();
        let oldest_pending_ms = state
            .pending
            .values()
            .map(|pending| pending.at.elapsed())
            .max()
            .map(|age| u64::try_from(age.as_millis()))
            .transpose()
            .map_err(|_| Error::new("STORAGE_FAILURE", "buffered age exceeds millisecond range"))?
            .unwrap_or(0);
        Ok(DurabilityStatus {
            flush_interval_ms: writer.config.buffered_flush_interval_ms,
            flush_lag_ms: oldest_pending_ms
                .saturating_sub(u64::from(writer.config.buffered_flush_interval_ms)),
            session_id: state.session_id.clone(),
            accepted_through: writer.db.latest_sequence_number(),
            durable_through: state.durable_through,
            pending_writes: state.pending.len(),
            pending_bytes: state.bytes,
            oldest_pending_ms,
            failure: state.failure.clone(),
        })
    }
}
