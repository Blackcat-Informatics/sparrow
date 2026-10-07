// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Serialized oldest-unpinned logical retirement. Physical reclamation is separate.

use super::{Artifact, DAY, Error, Store, put, retire};
use std::collections::BTreeMap;

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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvictionFrontier {
    /// All time entries were visited.
    Exhausted,
    /// Maximum newly retired record count reached.
    RecordLimit,
    /// Maximum visited time entries reached.
    ScanBudget,
    /// Next complete record cannot fit the remaining reference allowance.
    PayloadBudget,
}

/// Durable logical changes, not a claim about physical disk bytes freed.
#[derive(Clone, Debug, PartialEq, Eq)]
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
}

impl Store {
    /// Durably retire oldest unpinned records and invalidate their complete parents.
    /// Pins are captured under the same writer lock as the atomic retirement.
    /// Records retire in full; parent manifest cleanup can continue in later passes.
    /// Admission failure commits nothing. This does not run native compaction.
    pub fn evict_oldest(&self, now: u64, limits: EvictionLimits) -> Result<EvictionReport, Error> {
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
        let writer = self.lock()?;
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
        let mut retirement = retire::Retirement::new(&writer)?;
        let mut parents = BTreeMap::<String, Artifact>::new();
        let mut report = EvictionReport {
            records: Vec::new(),
            artifacts: Vec::new(),
            visited: 0,
            protected: 0,
            payload_refs: 0,
            pending_manifest_refs: 0,
            frontier: EvictionFrontier::Exhausted,
        };
        for entry in writer.db.prefix_iterator("time:") {
            let (key, value) = entry?;
            if !key.starts_with(b"time:") {
                break;
            }
            if report.visited == limits.scan_budget {
                report.frontier = EvictionFrontier::ScanBudget;
                break;
            }
            let mut record = writer.time_record(&key, &value, &mut catalog)?;
            report.visited += 1;
            if protected.contains(&record.id) || protected.contains(&record.artifact_id) {
                report.protected += 1;
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
            }
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
        Ok(report)
    }
}
