// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Finite catalog expiry, sharing all dependency retirement with eviction.

use super::eviction::EvictionLimits;
use super::{
    Artifact, Catalog, DAY, Error, Pin, Record, Store, Writer, integer, parse, put, retire,
};
use rocksdb::{Direction, IteratorMode};

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum Phase {
    #[default]
    Artifacts,
    Records,
    Pins,
    Requests,
}

impl Phase {
    fn prefix(self) -> &'static [u8] {
        match self {
            Self::Artifacts => b"artifact:",
            Self::Records => b"record:",
            Self::Pins => b"pin:",
            Self::Requests => b"request:",
        }
    }

    fn following(self) -> Option<Self> {
        match self {
            Self::Artifacts => Some(Self::Records),
            Self::Records => Some(Self::Pins),
            Self::Pins => Some(Self::Requests),
            Self::Requests => None,
        }
    }
}

/// Ephemeral native seek position; durable evidence never depends on this cursor.
#[derive(Clone, Default, PartialEq, Eq)]
pub(super) struct Cursor {
    phase: Phase,
    next: Option<(Vec<u8>, bool)>,
}

/// Why a finite expiry pass stopped.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExpiryFrontier {
    /// This catalog traversal completed; the next pass starts a fresh traversal.
    #[default]
    Exhausted,
    /// Maximum catalog entries inspected.
    ScanBudget,
    /// Maximum record/artifact dependency retirement units.
    UnitLimit,
    /// The next complete record cannot fit remaining payload ownership admission.
    PayloadBudget,
    /// Catalog capture stopped after a complete safe prefix.
    CatalogBudget,
}

/// Durable logical expiry observations, independent of native physical reclamation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExpiryReport {
    /// Catalog entries inspected, separately from bounded pin protection capture.
    pub visited: usize,
    /// Records whose dependency closure was retired.
    pub records: u64,
    /// Artifacts whose complete manifest ownership was partially or fully retired.
    pub artifacts: u64,
    /// Payload ownership references retired through the shared owner.
    pub payload_refs: usize,
    /// Expired catalog and retry rows removed.
    pub deleted: usize,
    /// Explicit bounded frontier.
    pub frontier: ExpiryFrontier,
    /// More work remains in this session's catalog traversal.
    pub continuation: bool,
}

impl Store {
    /// Perform one finite expiry pass; never collect the resident corpus.
    /// `max_records` bounds total record/artifact retirement units in this operation.
    /// Current protection is captured under the writer lock on every pass.
    /// Hard failures commit nothing and do not advance the catalog cursor.
    pub fn expire_bounded(&self, now: u64, limits: EvictionLimits) -> Result<ExpiryReport, Error> {
        limits.validate()?;
        self.import_reclaim(now)?;
        self.pin_read_stats(now)?;
        self.artifact_read_stats(now)?;
        self.source_read_stats(now)?;
        self.record_read_stats(now)?;
        self.lock()?.expire_bounded(now, limits)
    }

    /// Default bounded expiry pass; returns dependency retirement units, not freed bytes.
    /// Callers requiring complete traversal inspect `expire_bounded` continuation.
    pub fn expire(&self, now: u64) -> Result<u64, Error> {
        let report = self.expire_bounded(now, EvictionLimits::default())?;
        Ok(report.records + report.artifacts)
    }
}

impl Writer {
    fn expire_bounded(&self, now: u64, limits: EvictionLimits) -> Result<ExpiryReport, Error> {
        let mut catalog = limits.max_catalog_bytes;
        let mut pin_visits = 0;
        let protected = self.protected_matching(
            now,
            |_| true,
            || {
                pin_visits += 1;
                if pin_visits > limits.scan_budget + 1 {
                    Err(Error::new(
                        "RESOURCE_LIMIT",
                        "pin capture exceeds expiry scan budget",
                    ))
                } else {
                    Ok(())
                }
            },
            None,
            Some(&mut catalog),
        )?;
        let mut saved = self.expiry.borrow_mut();
        let start = saved.clone();
        let mut position = start.clone();
        let mut report = ExpiryReport::default();
        let mut retirement = retire::Retirement::new(self)?;
        'phases: loop {
            let prefix = position.phase.prefix();
            let begin = position.next.clone();
            let key = begin.as_ref().map_or(prefix, |(key, _)| key.as_slice());
            for entry in self
                .db
                .iterator(IteratorMode::From(key, Direction::Forward))
            {
                let (key, bytes) = entry?;
                if !key.starts_with(prefix) {
                    break;
                }
                if begin.as_ref().is_some_and(|(key_before, inclusive)| {
                    !inclusive && key.as_ref() == key_before.as_slice()
                }) {
                    continue;
                }
                if report.visited == limits.scan_budget {
                    report.frontier = ExpiryFrontier::ScanBudget;
                    break 'phases;
                }
                if key.len() > 1024 {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "expiry catalog key exceeds its bounded identity contract",
                    ));
                }
                match Self::charge_catalog_shape(&mut catalog, &bytes) {
                    Ok(()) => {}
                    Err(error) if error.code == "RESOURCE_LIMIT" && report.visited != 0 => {
                        report.frontier = ExpiryFrontier::CatalogBudget;
                        break 'phases;
                    }
                    Err(error) => return Err(error),
                }
                let value = parse(&bytes)?;
                report.visited += 1;
                match position.phase {
                    Phase::Artifacts => {
                        let mut artifact = Artifact::decode(&value)?;
                        if key.as_ref() != artifact.id.as_bytes() {
                            return Err(Error::new(
                                "STORAGE_FAILURE",
                                "expiry artifact catalog identity mismatch",
                            ));
                        }
                        if !self.live_artifact(&artifact, now, &protected) {
                            if !artifact.chunks.is_empty() {
                                if report.records + report.artifacts == limits.max_records as u64 {
                                    report.frontier = ExpiryFrontier::UnitLimit;
                                    break 'phases;
                                }
                                let remaining = limits.max_payload_refs - report.payload_refs;
                                if remaining == 0 {
                                    report.frontier = ExpiryFrontier::PayloadBudget;
                                    break 'phases;
                                }
                                report.payload_refs +=
                                    retirement.artifact(&mut artifact, remaining)?;
                                report.artifacts += 1;
                                put(&mut retirement.batch, &artifact.id, &artifact);
                                if !artifact.chunks.is_empty() {
                                    position.next = Some((key.to_vec(), true));
                                    report.frontier = ExpiryFrontier::PayloadBudget;
                                    break 'phases;
                                }
                            }
                            if artifact.expires_at.saturating_add(DAY) <= now {
                                retirement.batch.delete(&key);
                                retirement.batch.delete(super::recordless::key(&artifact));
                                report.deleted += 1;
                            }
                        }
                    }
                    Phase::Records => {
                        let mut record = Record::decode(&value)?;
                        if key.as_ref() != record.id.as_bytes() {
                            return Err(Error::new(
                                "STORAGE_FAILURE",
                                "expiry record catalog identity mismatch",
                            ));
                        }
                        if !self.live_record(&record, now, &protected) {
                            if report.records + report.artifacts == limits.max_records as u64 {
                                report.frontier = ExpiryFrontier::UnitLimit;
                                break 'phases;
                            }
                            if record.chunks.len() > limits.max_payload_refs {
                                return Err(Error::new(
                                    "RESOURCE_LIMIT",
                                    "complete expired record exceeds payload ownership admission; increase the pass limit",
                                ));
                            }
                            if record.chunks.len() > limits.max_payload_refs - report.payload_refs {
                                report.frontier = ExpiryFrontier::PayloadBudget;
                                break 'phases;
                            }
                            report.payload_refs += record.chunks.len();
                            if retirement.record(&mut record, 1)? {
                                report.records += 1;
                                put(&mut retirement.batch, &record.id, &record);
                            }
                            if record.expires_at.saturating_add(DAY) <= now {
                                retirement.batch.delete(&key);
                                retirement.batch.delete(format!(
                                    "time:{:020}:{}",
                                    record.received_at, record.id
                                ));
                                report.deleted += 1;
                            }
                        }
                    }
                    Phase::Pins => {
                        let pin = Pin::decode(&value)?;
                        if key.as_ref() != pin.id.as_bytes() {
                            return Err(Error::new(
                                "STORAGE_FAILURE",
                                "expiry pin catalog identity mismatch",
                            ));
                        }
                        if pin.expires_at <= now {
                            retirement.batch.delete(&key);
                            report.deleted += 1;
                        }
                    }
                    Phase::Requests => {
                        if !self.managed_consumption_request(&key, &value)?
                            && integer(&value, "expires_at")? <= now
                        {
                            retirement.batch.delete(&key);
                            report.deleted += 1;
                        }
                    }
                }
                position.next = Some((key.to_vec(), false));
            }
            let Some(phase) = position.phase.following() else {
                break;
            };
            position.phase = phase;
            position.next = None;
        }
        let batch = retirement.finish()?;
        if batch.size_in_bytes() > limits.max_batch_bytes {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "expiry batch exceeds native write admission",
            ));
        }
        self.commit(batch)?;
        report.continuation = report.frontier != ExpiryFrontier::Exhausted;
        *saved = if report.continuation {
            position
        } else {
            Cursor::default()
        };
        Ok(report)
    }
}
