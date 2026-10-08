// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Pin-aware artifact discovery, including parents known only through retained records.

use super::{Artifact, Catalog, Error, Record, Store, catalog_lease, random_id};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use super::catalog_lease::{CAPTURE_BYTES, MAX_ITEMS, SCAN_BYTES};
pub(super) use super::catalog_lease::{HANDLE_BYTES, MAX_HANDLES};
const ENTRY_BYTES: usize = 512 * 1024;

/// Artifact metadata filter, bound into every continuation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactSelector {
    /// Exact source label, or all sources.
    pub source: Option<String>,
    /// Inclusive original artifact receipt time, in Unix milliseconds.
    pub received_from: u64,
    /// Exclusive original artifact receipt time.
    pub received_before: u64,
}

impl Default for ArtifactSelector {
    fn default() -> Self {
        Self {
            source: None,
            received_from: 0,
            received_before: u64::MAX,
        }
    }
}

impl catalog_lease::Selector for ArtifactSelector {
    fn capacity(&self) -> usize {
        self.source.as_ref().map_or(0, String::capacity)
    }
}

/// Bounded immutable artifact catalog capture or continuation.
#[derive(Clone, Debug)]
pub struct ArtifactRead {
    /// Original operation selector.
    pub selector: ArtifactSelector,
    /// Previous opaque continuation, or a fresh capture.
    pub cursor: Option<String>,
    /// Item upper bound, 1..1000; byte admission can reduce it.
    pub page_size: usize,
    /// Total pin/artifact/record catalog admission, 1..100000.
    pub scan_budget: usize,
}

impl Default for ArtifactRead {
    fn default() -> Self {
        Self {
            selector: ArtifactSelector::default(),
            cursor: None,
            page_size: 200,
            scan_budget: 10_000,
        }
    }
}

/// Original artifact metadata, absent after the parent catalog was reclaimed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactOrigin {
    /// Original interpretation hint.
    pub format: String,
    /// Original receipt time, never inferred from surviving record timestamps.
    pub received_at: u64,
    /// Ordinary deadline, independent of investigation protection.
    pub expires_at: u64,
    /// Complete original byte count, never a sum of surviving fragments.
    pub byte_length: u64,
    /// Original number of addressable records.
    pub original_records: usize,
}

/// Discovery metadata, which is not a capability to download expired complete bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactSummary {
    /// Stable artifact occurrence identity.
    pub id: String,
    /// Original source label retained in occurrence provenance.
    pub source: String,
    /// Original parent metadata, or explicit absence.
    pub origin: Option<ArtifactOrigin>,
    /// Whether the complete artifact was eligible at capture time.
    pub complete_bytes_available: bool,
    /// Exact number of eligible records at capture time.
    pub retained_records: usize,
}

impl ArtifactSummary {
    fn capacity(&self) -> usize {
        512 + self.id.capacity()
            + self.source.capacity()
            + self
                .origin
                .as_ref()
                .map_or(0, |origin| origin.format.capacity())
    }
}

/// Immutable artifact summaries in stable artifact-ID order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactPageBody {
    /// Page rows. Missing parents retain identity/source and explicit unknown metadata.
    pub artifacts: Vec<ArtifactSummary>,
}

impl catalog_lease::PageBody for ArtifactPageBody {
    const PAGE_ITEMS: usize = (catalog_lease::PAGE_BYTES - 4096) / 8192;
    fn len(&self) -> usize {
        self.artifacts.len()
    }
    fn capacity(&self) -> usize {
        self.artifacts.capacity() * std::mem::size_of::<ArtifactSummary>()
            + self
                .artifacts
                .iter()
                .map(ArtifactSummary::capacity)
                .sum::<usize>()
    }
    fn page(&self, start: usize, end: usize) -> Self {
        Self {
            artifacts: self.artifacts[start..end].to_vec(),
        }
    }
}

/// Complete artifact capture receipt and bounded page; reads do not pin evidence.
pub type ArtifactReadPage = catalog_lease::CatalogReadPage<ArtifactPageBody>;
pub(super) type Reads = catalog_lease::Reads<ArtifactSelector, ArtifactPageBody>;

impl Store {
    /// Discover eligible complete artifacts and surviving records' parent provenance.
    /// No partial capture is published on catalog, resource or coverage failure.
    pub fn artifact_read(&self, query: &ArtifactRead, now: u64) -> Result<ArtifactReadPage, Error> {
        if !(1..=1000).contains(&query.page_size)
            || !(1..=100000).contains(&query.scan_budget)
            || query.selector.received_from >= query.selector.received_before
            || query
                .selector
                .source
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.len() > 512)
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "artifact reads require a bounded source, increasing receipt interval, page_size 1..1000 and scan_budget 1..100000",
            ));
        }
        let mut reads = self
            .artifact_reads
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "artifact read registry poisoned"))?;
        reads.purge(now);
        if let Some(cursor) = &query.cursor {
            return reads.resume(
                &query.selector,
                cursor,
                query.page_size,
                query.scan_budget,
                now,
            );
        }
        let started = Instant::now();
        let expires_at = now
            .checked_add(catalog_lease::LEASE_MS)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "artifact read deadline overflow"))?;
        crate::format_instant(now)?;
        crate::format_instant(expires_at)?;
        let writer = self.lock()?;
        let mut scanned = 0;
        let mut remaining = SCAN_BYTES;
        let protected = writer.protected_matching(
            now,
            |_| true,
            || {
                if scanned == query.scan_budget
                    || started.elapsed() >= Duration::from_millis(catalog_lease::LEASE_MS)
                {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "artifact pin capture exhausted scan/time admission",
                    ));
                }
                scanned += 1;
                Ok(())
            },
            Some(ENTRY_BYTES),
            Some(&mut remaining),
        )?;
        let mut rows = BTreeMap::<String, ArtifactSummary>::new();
        let mut capacity = 4096;
        for prefix in [b"artifact:".as_slice(), b"record:".as_slice()] {
            let mut iterator = writer.db.raw_iterator();
            iterator.seek(prefix);
            while iterator.valid() {
                let key = iterator.key().ok_or_else(|| {
                    Error::new("STORAGE_FAILURE", "artifact discovery iterator key missing")
                })?;
                if !key.starts_with(prefix) {
                    break;
                }
                let bytes = iterator.value().ok_or_else(|| {
                    Error::new(
                        "STORAGE_FAILURE",
                        "artifact discovery iterator value missing",
                    )
                })?;
                if scanned == query.scan_budget
                    || bytes.len() > ENTRY_BYTES
                    || started.elapsed() >= Duration::from_millis(catalog_lease::LEASE_MS)
                {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "artifact capture exhausted entry/scan/time admission; no result published",
                    ));
                }
                scanned += 1;
                super::Writer::charge_catalog(&mut remaining, bytes.len())?;
                let value = super::parse(bytes)?;
                let (summary, retained) = if prefix == b"artifact:" {
                    let artifact = Artifact::decode(&value)?;
                    if artifact.id.as_bytes() != key {
                        return Err(Error::new(
                            "STORAGE_FAILURE",
                            "artifact discovery catalog identity mismatch",
                        ));
                    }
                    super::validate_ingest_labels(&artifact.id, &artifact.source, &artifact.format)
                        .map_err(|error| Error::new("STORAGE_FAILURE", error.message))?;
                    let complete = writer.live_artifact(&artifact, now, &protected);
                    (
                        ArtifactSummary {
                            id: artifact.id,
                            source: artifact.source,
                            origin: Some(ArtifactOrigin {
                                format: artifact.format,
                                received_at: artifact.received_at,
                                expires_at: artifact.expires_at,
                                byte_length: artifact.byte_length,
                                original_records: artifact.record_ids.len(),
                            }),
                            complete_bytes_available: complete,
                            retained_records: 0,
                        },
                        false,
                    )
                } else {
                    let record = Record::decode(&value)?;
                    if record.id.as_bytes() != key {
                        return Err(Error::new(
                            "STORAGE_FAILURE",
                            "artifact discovery record identity mismatch",
                        ));
                    }
                    writer.validate_event_record(&record)?;
                    if !writer.live_record(&record, now, &protected) {
                        iterator.next();
                        continue;
                    }
                    (
                        ArtifactSummary {
                            id: record.artifact_id,
                            source: record.source,
                            origin: None,
                            complete_bytes_available: false,
                            retained_records: 0,
                        },
                        true,
                    )
                };
                if !summary.id.starts_with("artifact:")
                    || summary.id.len() > 128
                    || summary.source.is_empty()
                    || summary.source.len() > 512
                {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "artifact discovery identity or source provenance is invalid",
                    ));
                }
                if summary.capacity() > 4096 {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "artifact summary exceeds row admission",
                    ));
                }
                if let Some(existing) = rows.get_mut(&summary.id) {
                    if existing.source != summary.source {
                        return Err(Error::new(
                            "STORAGE_FAILURE",
                            "artifact and record source provenance disagree",
                        ));
                    }
                    if retained {
                        existing.retained_records += 1;
                    }
                } else {
                    if rows.len() == MAX_ITEMS || capacity + summary.capacity() > CAPTURE_BYTES {
                        return Err(Error::new(
                            "RESOURCE_LIMIT",
                            "artifact capture exceeds item/capacity admission",
                        ));
                    }
                    capacity += summary.capacity();
                    let mut summary = summary;
                    summary.retained_records = usize::from(retained);
                    rows.insert(summary.id.clone(), summary);
                }
                iterator.next();
            }
            iterator.status()?;
        }
        let mut artifacts = Vec::new();
        for (_, summary) in rows {
            if !summary.complete_bytes_available && summary.retained_records == 0 {
                continue;
            }
            if query
                .selector
                .source
                .as_ref()
                .is_some_and(|source| source != &summary.source)
            {
                continue;
            }
            if let Some(origin) = &summary.origin {
                if summary.retained_records > origin.original_records {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "retained record count exceeds original artifact membership",
                    ));
                }
                crate::format_instant(origin.received_at)
                    .and_then(|_| crate::format_instant(origin.expires_at))
                    .map_err(|error| Error::new("STORAGE_FAILURE", error.message))?;
                if origin.received_at < query.selector.received_from
                    || origin.received_at >= query.selector.received_before
                {
                    continue;
                }
            } else if query.selector.received_from != 0
                || query.selector.received_before != u64::MAX
            {
                return Err(Error::new(
                    "INDEX_NOT_READY",
                    "original artifact receipt time is unknown after parent reclamation; omit the receipt filter or select retained records",
                ));
            }
            artifacts.push(summary);
        }
        let snapshot = catalog_lease::Snapshot {
            bytes: 0,
            id: random_id("artifact-read")?,
            store_id: writer.id.clone(),
            selector: query.selector.clone(),
            as_of: now,
            expires_at,
            started,
            page_size: query.page_size,
            scan_budget: query.scan_budget,
            scanned,
            body: ArtifactPageBody { artifacts },
        };
        drop(writer);
        reads.publish(snapshot, now, CAPTURE_BYTES, HANDLE_BYTES, MAX_HANDLES)
    }

    pub(super) fn artifact_read_stats(&self, now: u64) -> Result<(usize, usize), Error> {
        let mut reads = self
            .artifact_reads
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "artifact read registry poisoned"))?;
        reads.purge(now);
        Ok((reads.snapshots.len(), reads.capacity()))
    }
}
