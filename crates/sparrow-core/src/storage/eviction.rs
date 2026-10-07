// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Serialized oldest-unpinned logical retirement. Physical reclamation is separate.

use super::{Artifact, DAY, Error, Store, Writer, put, retire};
use rocksdb::{Direction, IteratorMode, WriteBatchIteratorCf};
use std::collections::{BTreeMap, BTreeSet};

/// One session-local position, valid only while the captured protection is unchanged.
#[derive(Default)]
pub(super) struct Cursor {
    protected: BTreeSet<String>,
    next: Option<(Vec<u8>, bool)>,
    exhausted: bool,
}

impl Cursor {
    pub(super) fn reset(&mut self) {
        self.protected.clear();
        self.next = None;
        self.exhausted = false;
    }

    pub(super) fn published(&mut self, key: &[u8]) {
        if self.exhausted {
            self.next = Some((key.to_vec(), true));
            self.exhausted = false;
            return;
        }
        if self
            .next
            .as_ref()
            .is_some_and(|(next, _)| key <= next.as_slice())
        {
            self.reset();
        }
    }
}

/// Observe native publication keys without copying payloads or interpreting a batch codec.
#[derive(Default)]
pub(super) struct Publication {
    pub(super) earliest: Option<Vec<u8>>,
    invalid: bool,
}

impl Publication {
    pub(super) fn check(&self) -> Result<(), Error> {
        if self.invalid {
            Err(Error::new(
                "STORAGE_FAILURE",
                "time publication key exceeds its bounded identity contract",
            ))
        } else {
            Ok(())
        }
    }
}

impl WriteBatchIteratorCf for Publication {
    fn put_cf(&mut self, cf_id: u32, key: &[u8], _value: &[u8]) {
        if cf_id == 0 && key.starts_with(b"time:") {
            if key.len() > b"time:".len() + 20 + 1 + 128 {
                self.invalid = true;
            } else if self
                .earliest
                .as_ref()
                .is_none_or(|earliest| key < earliest.as_slice())
            {
                self.earliest = Some(key.to_vec());
            }
        }
    }

    fn delete_cf(&mut self, _cf_id: u32, _key: &[u8]) {
        // Deletion introduces no earlier candidate; native seek handles absent bookmarks.
    }

    fn merge_cf(&mut self, cf_id: u32, key: &[u8], value: &[u8]) {
        self.put_cf(cf_id, key, value);
    }
}

/// Finite per-pass admission. Catalog charge includes decoded and retained shapes.
#[derive(Clone, Copy, Debug)]
pub struct EvictionLimits {
    /// Maximum newly evicted records (1..=128).
    pub max_records: usize,
    /// Maximum time entries and, independently, pin entries (1..=100000).
    pub scan_budget: usize,
    /// Maximum payload ownership references retired (1..=65536).
    pub max_payload_refs: usize,
    /// Conservative catalog capture charge (1..=32 MiB).
    pub max_catalog_bytes: usize,
    /// Maximum atomic native batch bytes (1..=128 MiB).
    pub max_batch_bytes: usize,
}

impl Default for EvictionLimits {
    fn default() -> Self {
        Self {
            max_records: 32,
            scan_budget: 10000,
            max_payload_refs: 4096,
            max_catalog_bytes: 32 * 1024 * 1024,
            max_batch_bytes: 128 * 1024 * 1024,
        }
    }
}

/// Why this pass stopped; a bounded frontier never certifies corpus exhaustion.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EvictionFrontier {
    /// All time entries were visited.
    #[default]
    Exhausted,
    /// Maximum newly retired record count reached.
    RecordLimit,
    /// Maximum visited time entries reached.
    ScanBudget,
    /// Next complete record cannot fit the remaining reference allowance.
    PayloadBudget,
    /// Catalog capture reached admission after a complete safe prefix.
    CatalogBudget,
}

/// Durable logical changes, not a claim about physical disk bytes freed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EvictionReport {
    /// Record occurrences made unavailable in oldest acceptance order.
    pub records: Vec<String>,
    /// Complete parent representations made unavailable by this pass.
    pub artifacts: Vec<String>,
    /// Time entries inspected, including protected and retired evidence.
    pub visited: usize,
    /// Eligible records skipped because a record or parent pin protects them.
    pub protected: usize,
    /// Payload ownership references retired, including parent manifests.
    pub payload_refs: usize,
    /// Remaining manifest references in parents touched by this pass.
    pub pending_manifest_refs: usize,
    /// Explicit traversal frontier.
    pub frontier: EvictionFrontier,
    /// A bounded traversal position remains for the next pass in this session.
    pub continuation: bool,
}

impl Store {
    /// Durably retire oldest unpinned records and invalidate their complete parents.
    /// Pins are captured under the same writer lock as the atomic retirement.
    /// Records retire in full; parent manifest cleanup can continue in later passes.
    /// Hard failures commit nothing; bounded frontiers publish only complete retirement units.
    /// The session-local traversal resumes safely and does not run native compaction.
    pub fn evict_oldest(&self, now: u64, limits: EvictionLimits) -> Result<EvictionReport, Error> {
        self.lock()?.evict_oldest(now, limits)
    }
}

impl Writer {
    pub(super) fn evict_oldest(
        &self,
        now: u64,
        limits: EvictionLimits,
    ) -> Result<EvictionReport, Error> {
        for (value, maximum) in [
            (limits.max_records, 128),
            (limits.scan_budget, 100000),
            (limits.max_payload_refs, 65536),
            (limits.max_catalog_bytes, 32 * 1024 * 1024),
            (limits.max_batch_bytes, 128 * 1024 * 1024),
        ] {
            if value == 0 || value > maximum {
                return Err(Error::new(
                    "INVALID_ARGUMENT",
                    "eviction limit outside supported bounds",
                ));
            }
        }
        let writer = self;
        let mut catalog = limits.max_catalog_bytes;
        let mut pins = 0;
        let protected = writer.protected_matching(
            now,
            |_| true,
            || {
                pins += 1;
                if pins > limits.scan_budget + 1 {
                    Err(Error::new(
                        "RESOURCE_LIMIT",
                        "pin capture exceeds eviction scan budget",
                    ))
                } else {
                    Ok(())
                }
            },
            None,
            Some(&mut catalog),
        )?;
        let mut cursor = self.eviction.borrow_mut();
        if cursor.protected == protected && cursor.exhausted {
            return Ok(EvictionReport::default());
        }
        let start = if cursor.protected == protected {
            cursor.next.clone()
        } else {
            None
        };
        let mut next = start.clone();
        let mut retirement = retire::Retirement::new(writer)?;
        let mut parents = BTreeMap::<String, Artifact>::new();
        let mut report = EvictionReport::default();
        let start_key = start
            .as_ref()
            .map_or(b"time:".as_slice(), |(key, _)| key.as_slice());
        for entry in writer
            .db
            .iterator(IteratorMode::From(start_key, Direction::Forward))
        {
            let (key, value) = entry?;
            if !key.starts_with(b"time:") {
                break;
            }
            if start.as_ref().is_some_and(|(previous, inclusive)| {
                !inclusive && key.as_ref() == previous.as_slice()
            }) {
                continue;
            }
            if report.visited == limits.scan_budget {
                report.frontier = EvictionFrontier::ScanBudget;
                break;
            }
            let mut record = match writer.time_record(&key, &value, &mut catalog) {
                Ok(record) => record,
                Err(error) if error.code == "RESOURCE_LIMIT" && next != start => {
                    report.frontier = EvictionFrontier::CatalogBudget;
                    break;
                }
                Err(error) => return Err(error),
            };
            report.visited += 1;
            if protected.contains(&record.id) || protected.contains(&record.artifact_id) {
                report.protected += 1;
                next = Some((key.to_vec(), false));
                continue;
            }
            if !parents.contains_key(&record.artifact_id) {
                match writer.get_limited::<Artifact>(&record.artifact_id, &mut catalog) {
                    Ok(parent) => {
                        if parent.id != record.artifact_id {
                            return Err(Error::new(
                                "STORAGE_FAILURE",
                                "eviction parent catalog identity mismatch",
                            ));
                        }
                        parents.insert(parent.id.clone(), parent);
                    }
                    Err(error)
                        if error.code == "NOT_FOUND"
                            && record.expires_at.saturating_add(DAY) <= now => {}
                    Err(error) if error.code == "RESOURCE_LIMIT" && next != start => {
                        report.frontier = EvictionFrontier::CatalogBudget;
                        break;
                    }
                    Err(error) => return Err(error),
                }
            }
            if parents
                .get(&record.artifact_id)
                .is_some_and(|parent| !parent.record_ids.contains(&record.id))
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "eviction record missing from parent manifest",
                ));
            }
            if !record.evicted {
                if report.records.len() == limits.max_records {
                    report.frontier = EvictionFrontier::RecordLimit;
                    break;
                }
                if record.chunks.len() > limits.max_payload_refs - report.payload_refs {
                    report.frontier = EvictionFrontier::PayloadBudget;
                    break;
                }
                report.payload_refs += record.chunks.len();
                retirement.record(&mut record, 1)?;
                record.evicted = true;
                put(&mut retirement.batch, &record.id, &record);
                report.records.push(record.id);
                if let Some(parent) = parents.get_mut(&record.artifact_id)
                    && !parent.evicted
                {
                    parent.evicted = true;
                    report.artifacts.push(parent.id.clone());
                }
            }
            if let Some(parent) = parents
                .get_mut(&record.artifact_id)
                .filter(|parent| parent.evicted)
            {
                report.payload_refs +=
                    retirement.artifact(parent, limits.max_payload_refs - report.payload_refs)?;
                if !parent.chunks.is_empty() {
                    next = Some((key.to_vec(), true));
                    report.frontier = if report.records.len() == limits.max_records {
                        EvictionFrontier::RecordLimit
                    } else {
                        EvictionFrontier::PayloadBudget
                    };
                    break;
                }
            }
            next = Some((key.to_vec(), false));
        }
        for parent in parents.values().filter(|parent| parent.evicted) {
            report.pending_manifest_refs += parent.chunks.len();
            put(&mut retirement.batch, &parent.id, parent);
        }
        let batch = retirement.finish()?;
        if batch.size_in_bytes() > limits.max_batch_bytes {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "eviction batch exceeds native write admission",
            ));
        }
        writer.commit(batch)?;
        if report.frontier == EvictionFrontier::Exhausted {
            next = None;
        }
        report.continuation = next.is_some();
        cursor.next = next;
        cursor.protected = protected;
        cursor.exhausted = report.frontier == EvictionFrontier::Exhausted;
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_batch_observation_tracks_only_bounded_default_time_publication() {
        let mut batch = rocksdb::WriteBatch::default();
        batch.put("time:00000000000000000002:record:later", "record:later");
        batch.put("pin:control", "control");
        batch.put("time:00000000000000000001:record:earlier", "record:earlier");
        let mut observation = Publication::default();
        batch.iterate_cf(&mut observation);
        observation.check().unwrap();
        assert_eq!(
            observation.earliest.as_deref(),
            Some(b"time:00000000000000000001:record:earlier".as_slice())
        );
        observation.put_cf(
            1,
            b"time:00000000000000000000:record:payload-family",
            b"ignored",
        );
        assert_eq!(
            observation.earliest.as_deref(),
            Some(b"time:00000000000000000001:record:earlier".as_slice())
        );
        let mut invalid = rocksdb::WriteBatch::default();
        invalid.put(format!("time:{}", "x".repeat(200)), "record:invalid");
        invalid.iterate_cf(&mut observation);
        assert_eq!(observation.check().unwrap_err().code, "STORAGE_FAILURE");
    }
}
