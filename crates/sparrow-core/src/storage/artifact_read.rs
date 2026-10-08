// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Pin-aware artifact discovery, including parents known only through retained records.

use super::{Artifact, Catalog, Error, Record, Store, Writer, catalog_lease, random_id};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use super::catalog_lease::{CAPTURE_BYTES, CaptureAdmission, ENTRY_BYTES, MAX_ITEMS, SCAN_BYTES};
pub(super) use super::catalog_lease::{HANDLE_BYTES, MAX_HANDLES};

/// Artifact metadata filter, bound into every continuation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactSelector {
    /// Eligible live evidence, a concrete selection, or an active stable pin.
    pub selection: crate::QuerySelection,
    /// Exact source label, or all sources.
    pub source: Option<String>,
    /// OR alternatives intersected with the outer exact source.
    pub sources: Option<crate::QueryAlternatives>,
    /// AND dimensions and OR alternatives, using the shared record matcher.
    pub diagnostics: BTreeMap<crate::DiagnosticDimension, crate::QueryAlternatives>,
    /// Refuse unknown diagnostic membership.
    pub require_complete_index: bool,
    /// Inclusive original artifact receipt time, in Unix milliseconds.
    pub received_from: u64,
    /// Exclusive original artifact receipt time.
    pub received_before: u64,
}

impl Default for ArtifactSelector {
    fn default() -> Self {
        Self {
            selection: crate::QuerySelection::Live,
            source: None,
            sources: None,
            diagnostics: BTreeMap::new(),
            require_complete_index: false,
            received_from: 0,
            received_before: u64::MAX,
        }
    }
}

impl ArtifactSelector {
    fn scope(&self) -> crate::QueryScope {
        crate::QueryScope {
            selection: self.selection.clone(),
            source: self.source.clone(),
            sources: self.sources.clone(),
            diagnostics: self.diagnostics.clone(),
            require_complete_index: self.require_complete_index,
            received_from: self.received_from,
            received_before: self.received_before,
            limit: 1000,
            scan_budget: 100000,
        }
    }
}

impl catalog_lease::Selector for ArtifactSelector {
    fn capacity(&self) -> usize {
        self.source.as_ref().map_or(0, String::capacity)
            + self
                .sources
                .iter()
                .chain(self.diagnostics.values())
                .map(crate::QueryAlternatives::capacity)
                .sum::<usize>()
            + usize::from(!self.diagnostics.is_empty()) * 1024
            + self.selection.capacity()
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
    /// Exact number of retained records within the captured membership.
    pub retained_records: usize,
    /// Matching retained records; unknown mappings remain explicit.
    pub diagnostic_coverage: crate::DiagnosticCoverage,
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
    /// Coverage across all eligible records, including parents with no proven match.
    pub diagnostic_coverage: crate::DiagnosticCoverage,
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
            diagnostic_coverage: self.diagnostic_coverage,
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
        query.selector.selection.validate()?;
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
        let artifacts = writer.artifact_capture(
            &query.selector,
            now,
            started,
            query.scan_budget,
            &mut scanned,
            &mut remaining,
        )?;
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
            body: artifacts,
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

/// One collector owns live and selected artifact/record metadata folding.
struct ArtifactCapture {
    rows: BTreeMap<String, ArtifactSummary>,
    capacity: usize,
    scope: crate::QueryScope,
    diagnostic_coverage: crate::DiagnosticCoverage,
}
impl ArtifactCapture {
    /// Fold one admitted native catalog entry without retaining its decoded body.
    fn push(
        &mut self,
        writer: &Writer,
        key: &[u8],
        bytes: &[u8],
        now: u64,
        protected: &std::collections::BTreeSet<String>,
        admission: &mut CaptureAdmission<'_>,
    ) -> Result<(), Error> {
        admission.entry(bytes.len())?;
        let value = super::parse(bytes)?;
        let (summary, retained) = if key.starts_with(b"artifact:") {
            let artifact = Artifact::decode(&value)?;
            if artifact.id.as_bytes() != key {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "artifact discovery catalog identity mismatch",
                ));
            }
            super::validate_ingest_labels(&artifact.id, &artifact.source, &artifact.format)
                .map_err(|error| Error::new("STORAGE_FAILURE", error.message))?;
            let complete = writer.live_artifact(&artifact, now, protected);
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
                    diagnostic_coverage: crate::DiagnosticCoverage::default(),
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
            if !writer.live_record(&record, now, protected) {
                return Ok(());
            }
            let mut diagnostic_coverage = crate::DiagnosticCoverage::default();
            let parent = self.rows.get(&record.artifact_id);
            let origin = parent.and_then(|row| row.origin.as_ref());
            if artifact_eligible(&self.scope, &record.source, origin)? {
                writer.matches_diagnostic_scope(
                    &self.scope,
                    &record,
                    &mut diagnostic_coverage,
                    self.scope.require_complete_index,
                )?;
                self.diagnostic_coverage.merge(diagnostic_coverage);
            }
            (
                ArtifactSummary {
                    id: record.artifact_id,
                    source: record.source,
                    origin: None,
                    complete_bytes_available: false,
                    retained_records: 0,
                    diagnostic_coverage,
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
        if let Some(existing) = self.rows.get_mut(&summary.id) {
            if existing.source != summary.source {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "artifact and record source provenance disagree",
                ));
            }
            if retained {
                existing.retained_records += 1;
                existing
                    .diagnostic_coverage
                    .merge(summary.diagnostic_coverage);
            }
        } else {
            if self.rows.len() == MAX_ITEMS || self.capacity + summary.capacity() > CAPTURE_BYTES {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "artifact capture exceeds item/capacity admission",
                ));
            }
            self.capacity += summary.capacity();
            let mut summary = summary;
            summary.retained_records = usize::from(retained);
            self.rows.insert(summary.id.clone(), summary);
        }
        Ok(())
    }
}

impl Writer {
    pub(super) fn artifact_capture(
        &self,
        selector: &ArtifactSelector,
        now: u64,
        started: Instant,
        scan_budget: usize,
        scanned: &mut usize,
        remaining: &mut usize,
    ) -> Result<ArtifactPageBody, Error> {
        let protected = self.protected_matching(
            now,
            |_| true,
            || {
                if *scanned == scan_budget
                    || started.elapsed() >= Duration::from_millis(catalog_lease::LEASE_MS)
                {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "artifact pin capture exhausted scan/time admission",
                    ));
                }
                *scanned += 1;
                Ok(())
            },
            Some(ENTRY_BYTES),
            Some(remaining),
        )?;
        let mut admission = CaptureAdmission {
            started,
            scan_budget,
            scanned,
            remaining,
        };
        let selection = self.query_selection_membership(
            &selector.selection,
            now,
            &protected,
            Some(&mut admission),
        )?;
        let mut capture = ArtifactCapture {
            rows: BTreeMap::new(),
            capacity: 4096,
            scope: selector.scope(),
            diagnostic_coverage: crate::DiagnosticCoverage::default(),
        };
        if let Some(selection) = selection {
            for id in &selection.parents {
                if let Some(bytes) = self.db.get_pinned(id)? {
                    capture.push(self, id.as_bytes(), &bytes, now, &protected, &mut admission)?;
                } else if selection.artifacts.contains(id) {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "resolved complete artifact disappeared during capture",
                    ));
                }
            }
            for id in selection.records.keys() {
                let bytes = self.db.get_pinned(id)?.ok_or_else(|| {
                    Error::new(
                        "STORAGE_FAILURE",
                        "resolved selected record disappeared during capture",
                    )
                })?;
                capture.push(self, id.as_bytes(), &bytes, now, &protected, &mut admission)?;
            }
        } else {
            for prefix in [b"artifact:".as_slice(), b"record:".as_slice()] {
                let mut iterator = self.db.raw_iterator();
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
                    capture.push(self, key, bytes, now, &protected, &mut admission)?;
                    iterator.next();
                }
                iterator.status()?;
            }
        }
        let mut artifacts = Vec::new();
        for (_, summary) in capture.rows {
            if !summary.complete_bytes_available && summary.retained_records == 0 {
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
            }
            if !artifact_eligible(&capture.scope, &summary.source, summary.origin.as_ref())?
                || !selector.diagnostics.is_empty() && summary.diagnostic_coverage.matched == 0
            {
                continue;
            }
            artifacts.push(summary);
        }
        Ok(ArtifactPageBody {
            artifacts,
            diagnostic_coverage: capture.diagnostic_coverage,
        })
    }
}

// Original artifact receipt time is independent of record acceptance time.
fn artifact_eligible(
    scope: &crate::QueryScope,
    source: &str,
    origin: Option<&ArtifactOrigin>,
) -> Result<bool, Error> {
    if !scope.matches_source(source) {
        return Ok(false);
    }
    match origin {
        Some(origin) => {
            Ok(origin.received_at >= scope.received_from
                && origin.received_at < scope.received_before)
        }
        None if scope.received_from == 0 && scope.received_before == u64::MAX => Ok(true),
        None => Err(Error::new(
            "INDEX_NOT_READY",
            "original artifact receipt time is unknown after parent reclamation; omit the receipt filter or select retained records",
        )),
    }
}
