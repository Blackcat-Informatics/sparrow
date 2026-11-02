// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Serialized oldest-unpinned logical retirement. Physical reclamation is separate.

use super::{DAY, Error, Store, Writer, put, recordless, retire};
use rocksdb::{Direction, IteratorMode, WriteBatchIteratorCf};
use std::collections::{BTreeMap, BTreeSet};

type IndexEntry = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>;

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
    pub(super) pressure_wait: bool,
    pub(super) watch_registry: bool,
    pub(super) receiver_registry: bool,
    pub(super) manifest_retirement: bool,
    pub(super) owned_rdf_metadata: bool,
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
        if cf_id == 0 {
            self.owned_rdf_metadata |=
                key.starts_with(b"artifact-rdf:") || key.starts_with(b"provenance-rdf:");
            self.pressure_wait |= key == super::pressure::WAIT_KEY.as_bytes();
            self.watch_registry |= key == b"watch_registry";
            self.receiver_registry |= key == super::receiver_stream::KEY.as_bytes();
            self.manifest_retirement |= key.starts_with(super::manifest_retire::PREFIX);
        }
        if cf_id == 0
            && let Some(key) = key
                .strip_prefix(b"time:")
                .or_else(|| key.strip_prefix(recordless::PREFIX))
        {
            if key.len() > 20 + 1 + 128 {
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
    /// Maximum record/recordless-artifact retirement units (1..=128).
    pub max_records: usize,
    /// Maximum traversal entries and, independently, pin entries (1..=100000).
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
        Self::DEFAULT
    }
}

impl EvictionLimits {
    /// The limits production expiry and pressure passes run with.
    pub(super) const DEFAULT: Self = Self {
        max_records: 32,
        scan_budget: 10000,
        max_payload_refs: 4096,
        max_catalog_bytes: 32 * 1024 * 1024,
        max_batch_bytes: 128 * 1024 * 1024,
    };

    pub(super) fn validate(self) -> Result<(), Error> {
        for (value, maximum) in [
            (self.max_records, 128),
            (self.scan_budget, 100000),
            (self.max_payload_refs, 65536),
            (self.max_catalog_bytes, 32 * 1024 * 1024),
            (self.max_batch_bytes, 128 * 1024 * 1024),
        ] {
            if value == 0 || value > maximum {
                return Err(Error::new(
                    "INVALID_ARGUMENT",
                    "retirement limit outside supported bounds",
                ));
            }
        }
        Ok(())
    }
}

/// Why this pass stopped; a bounded frontier never certifies corpus exhaustion.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EvictionFrontier {
    /// All time entries were visited.
    #[default]
    Exhausted,
    /// Maximum newly retired record/recordless-artifact unit count reached.
    UnitLimit,
    /// Maximum visited time entries reached.
    ScanBudget,
    /// Next complete record cannot fit the remaining reference allowance.
    PayloadBudget,
    /// Catalog capture reached admission after a complete safe prefix.
    CatalogBudget,
    /// The next unprotected entry is not yet covered by a committed archive segment.
    ArchiveLag,
}

/// Durable logical changes, not a claim about physical disk bytes freed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EvictionReport {
    /// Record occurrences made unavailable in oldest acceptance order.
    pub records: Vec<String>,
    /// Complete parent representations made unavailable by this pass.
    pub artifacts: Vec<String>,
    /// Recordless artifact catalogs reclaimed by this pass, also included in `artifacts`.
    pub recordless_artifacts: usize,
    /// Time entries inspected, including protected and retired evidence.
    pub visited: usize,
    /// Entries skipped because an active record or artifact pin protects them.
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
    /// Durably retire oldest unpinned records and recordless artifacts.
    /// Pins are captured under the same writer lock as the atomic retirement.
    /// Records retire in full; parent manifest cleanup can continue in later passes.
    /// Hard failures commit nothing; bounded frontiers publish only complete retirement units.
    /// The session-local traversal resumes safely and does not run native compaction.
    pub fn evict_oldest(&self, now: u64, limits: EvictionLimits) -> Result<EvictionReport, Error> {
        self.lock()?.evict_oldest(now, limits, None)
    }
}

impl Writer {
    pub(super) fn evict_oldest(
        &self,
        now: u64,
        limits: EvictionLimits,
        pressure: Option<(u64, u64)>,
    ) -> Result<EvictionReport, Error> {
        limits.validate()?;
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
        let mut parents = BTreeMap::<String, super::manifest_retire::Envelope>::new();
        let mut report = EvictionReport::default();
        for entry in writer.eviction_entries(start.as_ref().map(|(key, _)| key.as_slice())) {
            let (key, value) = entry?;
            let recordless = key.starts_with(recordless::PREFIX);
            let position = &key[if recordless {
                recordless::PREFIX.len()
            } else {
                b"time:".len()
            }..];
            if start
                .as_ref()
                .is_some_and(|(previous, inclusive)| !inclusive && position == previous.as_slice())
            {
                continue;
            }
            if report.visited == limits.scan_budget {
                report.frontier = EvictionFrontier::ScanBudget;
                break;
            }
            if recordless {
                let artifact = match writer.indexed_recordless(&key, &value, &mut catalog) {
                    Ok(artifact) => artifact,
                    Err(error) if error.code == "RESOURCE_LIMIT" && next != start => {
                        report.frontier = EvictionFrontier::CatalogBudget;
                        break;
                    }
                    Err(error) => return Err(error),
                };
                report.visited += 1;
                if protected.contains(&artifact.id) {
                    report.protected += 1;
                    next = Some((position.to_vec(), false));
                    continue;
                }
                if !writer.archived(artifact.received_at, &artifact.id) {
                    next = Some((position.to_vec(), true));
                    report.frontier = EvictionFrontier::ArchiveLag;
                    break;
                }
                if report.records.len() + report.recordless_artifacts == limits.max_records {
                    report.frontier = EvictionFrontier::UnitLimit;
                    break;
                }
                retirement.artifact_graph(&artifact.id, artifact.received_at)?;
                retirement.batch.delete(&artifact.id);
                super::manifest_retire::remove(&mut retirement.batch, &artifact.id);
                retirement.batch.delete(&key);
                report.artifacts.push(artifact.id);
                report.recordless_artifacts += 1;
                next = Some((position.to_vec(), false));
                continue;
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
                next = Some((position.to_vec(), false));
                continue;
            }
            if !parents.contains_key(&record.artifact_id) {
                match writer.read_catalog(&record.artifact_id, |bytes| {
                    super::manifest_retire::Envelope::read(
                        writer,
                        &record.artifact_id,
                        bytes,
                        limits.max_payload_refs,
                        &mut catalog,
                    )
                }) {
                    Ok(parent) => {
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
            if let Some(parent) = parents.get(&record.artifact_id)
                && !parent.contains(&record.id, &mut catalog)?
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "eviction record missing from parent manifest",
                ));
            }
            // Oldest-first retirement stops at the archive watermark, never past it.
            if !writer.archived(record.received_at, &record.id)
                || parents
                    .get(&record.artifact_id)
                    .is_some_and(|parent| !writer.archived(parent.received_at, &parent.id))
            {
                next = Some((position.to_vec(), true));
                report.frontier = EvictionFrontier::ArchiveLag;
                break;
            }
            if !record.evicted {
                if report.records.len() + report.recordless_artifacts == limits.max_records {
                    report.frontier = EvictionFrontier::UnitLimit;
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
                    && !parent.evicted()
                {
                    retirement.artifact_graph(&parent.id, parent.received_at)?;
                    parent.evict();
                    report.artifacts.push(parent.id.clone());
                }
            }
            if let Some(parent) = parents
                .get_mut(&record.artifact_id)
                .filter(|parent| parent.evicted())
            {
                report.payload_refs +=
                    retirement.artifact(parent, limits.max_payload_refs - report.payload_refs)?;
                if parent.pending() != 0 {
                    next = Some((position.to_vec(), true));
                    report.frontier = if report.records.len() + report.recordless_artifacts
                        == limits.max_records
                    {
                        EvictionFrontier::UnitLimit
                    } else {
                        EvictionFrontier::PayloadBudget
                    };
                    break;
                }
            }
            next = Some((position.to_vec(), false));
        }
        for parent in parents.values().filter(|parent| parent.evicted()) {
            report.pending_manifest_refs += parent.pending() as usize;
            parent.stage(&mut retirement.batch);
        }
        let mut batch = retirement.finish()?;
        if let Some((physical, requested)) = pressure
            && (!report.records.is_empty()
                || !report.artifacts.is_empty()
                || report.payload_refs != 0)
        {
            super::pressure::stage_wait(&mut batch, physical, requested);
        }
        // The shared publication owner may add the bounded pressure format fence.
        if batch.size_in_bytes().saturating_add(256) > limits.max_batch_bytes {
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

    /// Merge two native ordered cursors by their identical timestamp/identity suffix.
    /// Only one entry from each index is peeked; no resident corpus is captured.
    pub(super) fn eviction_entries(
        &self,
        after: Option<&[u8]>,
    ) -> impl Iterator<Item = IndexEntry> + '_ {
        let mut record_start = b"time:".to_vec();
        let mut empty_start = recordless::PREFIX.to_vec();
        if let Some(after) = after {
            record_start.extend_from_slice(after);
            empty_start.extend_from_slice(after);
        }
        let mut records = self
            .db
            .iterator(IteratorMode::From(&record_start, Direction::Forward))
            .peekable();
        let mut empty = self
            .db
            .iterator(IteratorMode::From(&empty_start, Direction::Forward))
            .peekable();
        std::iter::from_fn(move || {
            if records.peek().is_some_and(Result::is_err) {
                return records.next();
            }
            if empty.peek().is_some_and(Result::is_err) {
                return empty.next();
            }
            let record = records
                .peek()
                .and_then(|row| row.as_ref().ok())
                .filter(|(key, _)| key.starts_with(b"time:"));
            let artifact = empty
                .peek()
                .and_then(|row| row.as_ref().ok())
                .filter(|(key, _)| key.starts_with(recordless::PREFIX));
            match (record, artifact) {
                (None, None) => None,
                (Some(_), None) => records.next(),
                (None, Some(_)) => empty.next(),
                (Some((record, _)), Some((artifact, _))) => {
                    if record[b"time:".len()..] < artifact[recordless::PREFIX.len()..] {
                        records.next()
                    } else {
                        empty.next()
                    }
                }
            }
        })
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
            Some(b"00000000000000000001:record:earlier".as_slice())
        );
        observation.put_cf(
            1,
            b"time:00000000000000000000:record:payload-family",
            b"ignored",
        );
        assert_eq!(
            observation.earliest.as_deref(),
            Some(b"00000000000000000001:record:earlier".as_slice())
        );
        let mut invalid = rocksdb::WriteBatch::default();
        invalid.put(format!("time:{}", "x".repeat(200)), "record:invalid");
        invalid.iterate_cf(&mut observation);
        assert_eq!(observation.check().unwrap_err().code, "STORAGE_FAILURE");
    }
}
