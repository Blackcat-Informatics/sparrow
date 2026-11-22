// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One serialized WAL boundary for durable control and bounded buffered intake.

use super::{Artifact, Error, Store, Writer};
use rocksdb::{WriteBatch, WriteBatchIteratorCf, WriteOptions};
use std::{collections::BTreeMap, time::Instant};

/// Maximum accepted writes awaiting an explicit synchronization boundary.
pub const MAX_BUFFERED_WRITES: usize = 256;
/// Maximum admitted atomic batch bytes awaiting synchronization.
pub const MAX_BUFFERED_BYTES: usize = 64 * 1024 * 1024;
/// Default intended periodic synchronization interval.
pub const DEFAULT_FLUSH_INTERVAL_MS: u32 = 2000;
/// Shortest accepted periodic synchronization interval. A flush that fires more than once
/// a second adds fsync pressure for no durability a one second bound does not already give.
pub const MIN_FLUSH_INTERVAL_MS: u32 = 1000;
/// Longest accepted periodic synchronization interval.
pub const MAX_FLUSH_INTERVAL_MS: u32 = 60_000;

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
    /// Latched storage uncertainty: writes and receipt certification are refused, retryably,
    /// until [`Store::reestablish_durability`] proves everything accepted durable.
    pub failure: Option<String>,
    /// Times durability latched uncertain since the store opened.
    pub latches: u64,
    /// Times a latch cleared in place since the store opened.
    pub recoveries: u64,
    /// Times the catalog writer was recovered after a writer panicked holding it.
    pub writer_recoveries: u64,
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
    latches: u64,
    recoveries: u64,
}
/// What durability status reads (#146): the ledger's counts and oldest acceptance, not the
/// ledger itself.
#[derive(Clone)]
pub(super) struct View {
    session_id: String,
    durable_through: u64,
    pending_writes: usize,
    pending_bytes: usize,
    oldest: Option<Instant>,
    failure: Option<String>,
    latches: u64,
    recoveries: u64,
}

impl super::writer_queue::Observe for State {
    type View = View;
    fn view(&self) -> View {
        View {
            session_id: self.session_id.clone(),
            durable_through: self.durable_through,
            pending_writes: self.pending.len(),
            pending_bytes: self.bytes,
            oldest: self.pending.values().map(|pending| pending.at).min(),
            failure: self.failure.clone(),
            latches: self.latches,
            recoveries: self.recoveries,
        }
    }
}

/// The free space RocksDB's file manager requires before it resumes a stopped handle (its
/// default reserved disk buffer).
const ROCKSDB_RECOVERY_BUFFER_BYTES: u64 = 32 * 1024 * 1024;

/// The refusal while durability is latched uncertain by `failure`. It is the retryable
/// `STORAGE_PRESSURE`, the code every writer already holds and retries on: the latch clears in
/// place once [`Writer::reestablish`] succeeds, and otherwise the daemon reopens the store, so
/// a retry of the same request can succeed either way.
fn uncertain(failure: &str) -> Error {
    Error::new(
        "STORAGE_PRESSURE",
        format!("durability uncertain; writes resume once the store re-establishes it: {failure}"),
    )
}

impl State {
    pub(super) fn new(session_id: String, sequence: u64) -> Self {
        Self {
            session_id,
            durable_through: sequence,
            pending: BTreeMap::new(),
            bytes: 0,
            failure: None,
            latches: 0,
            recoveries: 0,
        }
    }
    /// Refuse while durability is latched uncertain (see [`uncertain`]).
    pub(super) fn check(&self) -> Result<(), Error> {
        match &self.failure {
            Some(failure) => Err(uncertain(failure)),
            None => Ok(()),
        }
    }
    /// Latch a failed write or synchronization and return the refusal it now causes.
    fn latch(&mut self, cause: String) -> Error {
        if self.failure.is_none() {
            self.latches += 1;
        }
        let refusal = uncertain(&cause);
        self.failure = Some(cause);
        refusal
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
        // No latch check here: a read of an artifact is served while durability is latched and
        // reports its still-pending fence, which certifies nothing. A durable receipt that must
        // synchronize is refused by `synchronize`, which checks the latch.
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
    /// A refusal for the write ceiling stays typed ([`super::pressure::WriteRefusal`]); every
    /// other caller converts it to [`Error`] through `?`.
    pub(super) fn apply(
        &self,
        mut batch: WriteBatch,
        buffered_id: Option<&str>,
        record_estimate: Option<u64>,
    ) -> Result<(), super::pressure::WriteRefusal> {
        if buffered_id.is_none() && batch.is_empty() {
            return Ok(self.synchronize().map(|_| ())?);
        }
        // A refused write leaves the session certifiable; a failed one does not.
        self.durability.borrow().check()?;
        self.admit_space()?;
        let mut publication = super::eviction::Publication::default();
        batch.iterate_cf(&mut publication);
        publication.check()?;
        self.admit_archive_publication(publication.earliest.as_deref())?;
        super::source_context::update(self, &mut batch)?;
        super::source_summary::apply(self, &mut batch)?;
        super::payload_index::apply(&self.db, &mut batch)?;
        super::store_counters::apply(&self.db, &mut batch)?;
        let manifest_features = super::manifest_retire::features(&self.db)?;
        if publication.manifest_retirement || manifest_features.is_some() {
            let receiver_present = self.db.get(super::receiver_stream::KEY)?.is_some();
            if manifest_features == Some(true)
                && !receiver_present
                && !publication.receiver_registry
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "manifest store lost its required receiver registry",
                )
                .into());
            }
            let receiver = publication.receiver_registry
                || receiver_present
                || manifest_features == Some(true);
            let watch = publication.watch_registry || self.db.get("watch_registry")?.is_some();
            batch.put(super::manifest_retire::FEATURES, [u8::from(receiver)]);
            super::writer_format::stage(
                &self.db,
                &mut batch,
                if watch {
                    super::manifest_retire::WATCH_FORMAT
                } else {
                    super::manifest_retire::FORMAT
                },
            )?;
        } else if publication.receiver_registry
            || self.db.get(super::receiver_stream::KEY)?.is_some()
        {
            let watch = publication.watch_registry || self.db.get("watch_registry")?.is_some();
            super::writer_format::stage(
                &self.db,
                &mut batch,
                if watch {
                    super::receiver_stream::WATCH_FORMAT
                } else {
                    super::receiver_stream::FORMAT
                },
            )?;
        } else if publication.pressure_wait
            || matches!(
                super::writer_format::read(&self.db)?.as_deref(),
                Some(super::pressure::FORMAT | super::pressure::WATCH_FORMAT)
            )
        {
            let watch = publication.watch_registry || self.db.get("watch_registry")?.is_some();
            super::writer_format::stage(
                &self.db,
                &mut batch,
                if watch {
                    super::pressure::WATCH_FORMAT
                } else {
                    super::pressure::FORMAT
                },
            )?;
        }
        if publication.owned_rdf_metadata || record_estimate.is_some() {
            // Admit once after source context and authority metadata have joined
            // the exact batch. A smaller preliminary refusal cannot certify a retry.
            self.admit_data_write(
                record_estimate
                    .unwrap_or(0)
                    .max(super::pressure::data_batch_estimate(&batch)),
            )?;
        }
        let bytes = batch.size_in_bytes();
        {
            let state = self.durability.borrow();
            state.check()?;
            if let Some(id) = buffered_id {
                if state.pending.contains_key(id) {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "buffered occurrence identity already occupied",
                    )
                    .into());
                }
                if state.pending.len() == MAX_BUFFERED_WRITES
                    || bytes > MAX_BUFFERED_BYTES - state.bytes
                {
                    return Err(Error::new(
                        "STORAGE_PRESSURE",
                        "buffered durability ledger is full; synchronize before retrying",
                    )
                    .into());
                }
            }
        }
        let mut options = WriteOptions::default();
        options.disable_wal(false);
        options.set_sync(buffered_id.is_none());
        if let Err(error) = self.db.write_opt(batch, &options) {
            return Err(self.durability.borrow_mut().latch(error.to_string()).into());
        }
        if let Some(earliest) = publication.earliest {
            self.eviction.borrow_mut().published(&earliest);
            self.expiry.borrow_mut().published(&earliest);
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
            return Err(self.durability.borrow_mut().latch(error.to_string()));
        }
        let sequence = self.db.latest_sequence_number();
        self.durability.borrow_mut().reached(sequence);
        Ok(self.watermark(sequence))
    }

    /// Clear a durability latch in place once everything accepted is proven durable; `None`
    /// when nothing was latched, otherwise the cause that cleared.
    ///
    /// Every write that returned success is in a memtable or an earlier table file. A WAL
    /// write or sync that failed may have lost WAL bytes the page cache held, so a later WAL
    /// sync alone certifies nothing. Writing every memtable to a synchronized table file does
    /// not depend on the WAL, and the WAL sync after it proves the log accepts writes again.
    /// Both succeed only when RocksDB itself still accepts writes on this handle; a handle
    /// RocksDB stopped (a hard or fatal background error) fails here, and the latch stands.
    pub(super) fn reestablish(&self) -> Result<Option<String>, Error> {
        let Some(cause) = self.durability.borrow().failure.clone() else {
            return Ok(None);
        };
        let proven = self
            .db
            .flush_cfs_opt(
                &super::family_handles(&self.db)?,
                &rocksdb::FlushOptions::default(),
            )
            .and_then(|()| self.db.flush_wal(true));
        if let Err(error) = proven {
            return Err(Error::new(
                "STORAGE_FAILURE",
                format!("durability not re-established: {error}; latched by: {cause}"),
            ));
        }
        let sequence = self.db.latest_sequence_number();
        let mut state = self.durability.borrow_mut();
        state.reached(sequence);
        state.failure = None;
        state.recoveries += 1;
        Ok(Some(cause))
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

    /// Clear a durability latch in the running store once every accepted write is proven
    /// durable (see [`Writer::reestablish`]); `Ok(None)` when nothing was latched,
    /// `Ok(Some(cause))` when the latch cleared. The daemon's maintenance worker paces
    /// calls to this operation. A poisoned writer remains poisoned until both its
    /// catalog state has been revalidated and the proof succeeds.
    ///
    /// # Errors
    /// The catalog validation or durability proof failed. The latch keeps its cause.
    pub fn reestablish_durability(&self) -> Result<Option<String>, Error> {
        let arrival = self.unlocked.writers.arrive()?;
        let locked = self.inner.lock();
        drop(arrival);
        let writer = match locked {
            Ok(writer) => return writer.reestablish(),
            Err(poisoned) => poisoned.into_inner(),
        };
        if writer.durability.borrow().failure.is_none() {
            self.recover_unlatched_writer(&writer)?;
            return Ok(None);
        }
        writer.revalidate().map_err(|error| {
            error.with_context("validate the poisoned writer before proving durability")
        })?;
        let cleared = writer.reestablish()?;
        if cleared.is_some() {
            self.note_writer_recovered();
        }
        Ok(cleared)
    }

    /// Read the last published durability failure without acquiring or recovering the writer.
    #[must_use]
    pub fn durability_failure(&self) -> Option<String> {
        self.unlocked.durability.read().failure
    }

    /// Whether free space on the filesystem holding the store is below what RocksDB needs to
    /// resume a handle a full disk stopped: its 32 MiB recovery buffer, the maintenance
    /// reserve or the free-space floor, whichever is largest. Measured, not read from an
    /// error message.
    ///
    /// # Errors
    /// A failed free-space measurement.
    pub fn free_space_is_low(&self) -> Result<bool, Error> {
        let needed = self
            .unlocked
            .config
            .maintenance_reserve
            .max(self.unlocked.config.min_free_bytes)
            .max(ROCKSDB_RECOVERY_BUFFER_BYTES);
        Ok(self.space_floor().shortfall_below(needed)?.is_some())
    }

    /// Dev-only: latch durability uncertain with `cause`, as a failed write or WAL sync does,
    /// while RocksDB itself still accepts writes on this handle.
    #[cfg(feature = "test-support")]
    pub fn latch_durability_for_tests(&self, cause: &str) -> Result<(), Error> {
        let writer = self.lock()?;
        drop(writer.durability.borrow_mut().latch(cause.to_owned()));
        Ok(())
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
    /// Reads the observed state without the writer lock, and recovers an unlatched poisoned
    /// writer first.
    pub fn durability_status(&self) -> Result<DurabilityStatus, Error> {
        self.recover_poisoned()?;
        let state = self.unlocked.durability.read();
        let oldest_pending_ms = state
            .oldest
            .map(|at| u64::try_from(at.elapsed().as_millis()))
            .transpose()
            .map_err(|_| Error::new("STORAGE_FAILURE", "buffered age exceeds millisecond range"))?
            .unwrap_or(0);
        Ok(DurabilityStatus {
            flush_interval_ms: self.unlocked.config.buffered_flush_interval_ms,
            flush_lag_ms: oldest_pending_ms
                .saturating_sub(u64::from(self.unlocked.config.buffered_flush_interval_ms)),
            session_id: state.session_id,
            accepted_through: self.unlocked.db.latest_sequence_number(),
            durable_through: state.durable_through,
            pending_writes: state.pending_writes,
            pending_bytes: state.pending_bytes,
            oldest_pending_ms,
            failure: state.failure,
            latches: state.latches,
            recoveries: state.recoveries,
            writer_recoveries: self.writer_recoveries(),
        })
    }
}

/// A catalog write observer run by [`Writer::apply`]: it folds each default-family put and
/// delete of one batch, and refuses merges on the keys it reads.
pub(super) trait BatchObserver {
    /// Fold one put (`Some` value) or delete (`None`) of `name`.
    fn observe(&mut self, name: &[u8], value: Option<&[u8]>) -> Result<(), Error>;
    /// The refusal of a merge on `name`, when the observer reads that key.
    fn refuse_merge(&self, name: &[u8]) -> Option<Error>;
}

/// Fold every default-family put, delete and merge of `batch` into `observer`, in batch
/// order, stopping at its first failure. A batch holding an operation the iterator does not
/// report, a range delete, is refused: the observers' rows could not follow it (#146).
pub(super) fn observe_batch(
    batch: &WriteBatch,
    observer: &mut impl BatchObserver,
) -> Result<(), Error> {
    struct Fold<'o, O> {
        observer: &'o mut O,
        failure: Option<Error>,
        seen: usize,
    }
    impl<O: BatchObserver> WriteBatchIteratorCf for Fold<'_, O> {
        fn merge_cf(&mut self, cf: u32, name: &[u8], _value: &[u8]) {
            self.seen += 1;
            if cf == 0 && self.failure.is_none() {
                self.failure = self.observer.refuse_merge(name);
            }
        }
        fn put_cf(&mut self, cf: u32, name: &[u8], value: &[u8]) {
            self.seen += 1;
            if cf == 0 && self.failure.is_none() {
                self.failure = self.observer.observe(name, Some(value)).err();
            }
        }
        fn delete_cf(&mut self, cf: u32, name: &[u8]) {
            self.seen += 1;
            if cf == 0 && self.failure.is_none() {
                self.failure = self.observer.observe(name, None).err();
            }
        }
    }
    let mut fold = Fold {
        observer,
        failure: None,
        seen: 0,
    };
    batch.iterate_cf(&mut fold);
    if let Some(failure) = fold.failure {
        return Err(failure);
    }
    if fold.seen != batch.len() {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "a catalog write holds a range delete, which the derived counters cannot follow",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod observe_tests {
    use super::*;

    struct Every(usize);

    impl BatchObserver for Every {
        fn observe(&mut self, _: &[u8], _: Option<&[u8]>) -> Result<(), Error> {
            self.0 += 1;
            Ok(())
        }
        fn refuse_merge(&self, _: &[u8]) -> Option<Error> {
            None
        }
    }

    /// INVARIANT (#146): a catalog write whose batch holds a range delete is refused before
    /// any derived counter follows it, because the batch iterator never reports the range;
    /// the same batch without it is observed in full.
    #[test]
    fn a_range_delete_in_a_catalog_write_is_refused() {
        let mut batch = WriteBatch::default();
        batch.put(b"record:a", b"{}");
        batch.delete(b"record:b");
        let mut every = Every(0);
        observe_batch(&batch, &mut every).unwrap();
        assert_eq!(every.0, 2);
        batch.delete_range(b"record:".as_slice(), b"record;".as_slice());
        let refused = observe_batch(&batch, &mut Every(0)).unwrap_err();
        assert_eq!(refused.code, "STORAGE_FAILURE");
        assert!(refused.message.contains("range delete"), "{refused:?}");
    }
}
