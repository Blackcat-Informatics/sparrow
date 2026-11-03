// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Pin-aware artifact discovery, including parents known only through retained records.
//!
//! The live listing reads the artifact and record receipt indexes in receipt
//! order and stops at its page, so its charge follows the page rather than the
//! store. Explicit selections remain one bounded immutable capture.

use super::{
    Artifact, Catalog, Error, Record, Store, Writer, catalog_lease, random_id,
    temporal_index::{TemporalRows, entry_time},
};
use std::{
    collections::{BTreeMap, BTreeSet},
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

/// Artifact summaries: selections in artifact-ID order, live pages in receipt then ID order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactPageBody {
    /// Page rows. Missing parents retain identity/source and explicit unknown metadata.
    pub artifacts: Vec<ArtifactSummary>,
    /// Coverage across the eligible records this capture or live page read,
    /// including parents with no proven match.
    pub diagnostic_coverage: crate::DiagnosticCoverage,
    /// Rows delivered by earlier pages of the same live listing.
    offset: usize,
    /// Receipt position after this page's rows, while the live listing continues.
    resume: Option<Position>,
}

/// Exclusive live listing position: receipt time, and the last artifact read at it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Position {
    received_at: u64,
    after: Option<String>,
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
            + self
                .resume
                .as_ref()
                .and_then(|position| position.after.as_ref())
                .map_or(0, String::capacity)
    }
    fn page(&self, start: usize, end: usize) -> Result<Self, Error> {
        Ok(Self {
            artifacts: self.artifacts[start..end].to_vec(),
            diagnostic_coverage: self.diagnostic_coverage,
            offset: self.offset,
            resume: self.resume.clone(),
        })
    }
    fn offset(&self) -> usize {
        self.offset
    }
    fn continues(&self) -> bool {
        self.resume.is_some()
    }
}

/// Complete artifact capture receipt and bounded page; reads do not pin evidence.
pub type ArtifactReadPage = catalog_lease::CatalogReadPage<ArtifactPageBody>;
pub(super) type Reads = catalog_lease::Reads<ArtifactSelector, ArtifactPageBody>;

impl Store {
    /// Discover eligible complete artifacts and surviving records' parent provenance.
    /// No partial capture is published on catalog, resource or coverage failure.
    pub fn artifact_read(&self, query: &ArtifactRead, now: u64) -> Result<ArtifactReadPage, Error> {
        self.artifact_read_cancellable(query, now, &crate::QueryCancellation::default())
    }

    /// Use the same capture owner with cooperative caller cancellation.
    pub fn artifact_read_cancellable(
        &self,
        query: &ArtifactRead,
        now: u64,
        cancellation: &crate::QueryCancellation,
    ) -> Result<ArtifactReadPage, Error> {
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
        cancellation.checkpoint()?;
        let mut reads = self
            .artifact_reads
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "artifact read registry poisoned"))?;
        reads.purge(now);
        cancellation.checkpoint()?;
        if let Some(cursor) = &query.cursor {
            let parsed = catalog_lease::Cursor::parse(cursor)?;
            let binding = (&query.selector, query.page_size, query.scan_budget);
            if let Some(page) =
                self.artifact_next(&mut reads, &parsed, Some(binding), now, cancellation)?
            {
                return Ok(page);
            }
            return reads.resume(
                &query.selector,
                cursor,
                query.page_size,
                query.scan_budget,
                now,
            );
        }
        if query.selector.selection == crate::QuerySelection::Live {
            let start = Position {
                received_at: query.selector.received_from,
                after: None,
            };
            let page = Continued {
                selector: &query.selector,
                page_size: query.page_size,
                scan_budget: query.scan_budget,
                start,
                offset: 0,
                predecessor: None,
            };
            return self.artifact_page(&mut reads, page, now, cancellation);
        }
        let started = Instant::now();
        let expires_at = now
            .checked_add(catalog_lease::LEASE_MS)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "artifact read deadline overflow"))?;
        crate::format_instant(now)?;
        crate::format_instant(expires_at)?;
        let writer = self.lock()?;
        cancellation.checkpoint()?;
        let mut scanned = 0;
        let mut remaining = SCAN_BYTES;
        let mut admission = CaptureAdmission {
            cancellation,
            started,
            scan_budget: query.scan_budget,
            scanned: &mut scanned,
            remaining: &mut remaining,
        };
        let artifacts = writer.artifact_capture(&query.selector, now, &mut admission)?;
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
        reads.publish(
            snapshot,
            now,
            CAPTURE_BYTES,
            HANDLE_BYTES,
            MAX_HANDLES,
            cancellation,
        )
    }
}

/// One live listing page request, fresh or continuing a previous page's handle.
struct Continued<'a> {
    selector: &'a ArtifactSelector,
    page_size: usize,
    scan_budget: usize,
    start: Position,
    offset: usize,
    predecessor: Option<String>,
}

impl Store {
    /// Read the next live page when `cursor` continues past its handle's rows.
    /// Other cursors are replayed by the shared lease owner, so this returns `None`.
    pub(super) fn artifact_next(
        &self,
        reads: &mut Reads,
        cursor: &catalog_lease::Cursor<'_>,
        binding: Option<(&ArtifactSelector, usize, usize)>,
        now: u64,
        cancellation: &crate::QueryCancellation,
    ) -> Result<Option<ArtifactReadPage>, Error> {
        let Some(snapshot) = reads.continuation(cursor, now)? else {
            return Ok(None);
        };
        if let Some((selector, page_size, scan_budget)) = binding {
            snapshot.bind(selector, page_size, scan_budget)?;
        }
        let selector = snapshot.selector.clone();
        let start = snapshot.body.resume.clone().ok_or_else(|| {
            Error::new("STORAGE_FAILURE", "artifact continuation lost its position")
        })?;
        let page = Continued {
            selector: &selector,
            page_size: snapshot.page_size,
            scan_budget: snapshot.scan_budget,
            start,
            offset: snapshot.body.offset + snapshot.body.artifacts.len(),
            predecessor: Some(snapshot.id.clone()),
        };
        self.artifact_page(reads, page, now, cancellation).map(Some)
    }

    /// Read and publish one live page. A continued page replaces its predecessor's
    /// handle only once it is published, so a failed continuation can be retried.
    fn artifact_page(
        &self,
        reads: &mut Reads,
        page: Continued<'_>,
        now: u64,
        cancellation: &crate::QueryCancellation,
    ) -> Result<ArtifactReadPage, Error> {
        let started = Instant::now();
        let expires_at = now
            .checked_add(catalog_lease::LEASE_MS)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "artifact read deadline overflow"))?;
        crate::format_instant(now)?;
        crate::format_instant(expires_at)?;
        let writer = self.lock()?;
        cancellation.checkpoint()?;
        let mut scanned = 0;
        let mut remaining = SCAN_BYTES;
        let mut admission = CaptureAdmission {
            cancellation,
            started,
            scan_budget: page.scan_budget,
            scanned: &mut scanned,
            remaining: &mut remaining,
        };
        let mut body = writer.artifact_receipt_page(
            page.selector,
            &page.start,
            page.page_size
                .min(<ArtifactPageBody as catalog_lease::PageBody>::PAGE_ITEMS),
            now,
            &mut admission,
        )?;
        body.offset = page.offset;
        let snapshot = catalog_lease::Snapshot {
            bytes: 0,
            id: random_id("artifact-read")?,
            store_id: writer.id.clone(),
            selector: page.selector.clone(),
            as_of: now,
            expires_at,
            started,
            page_size: page.page_size,
            scan_budget: page.scan_budget,
            scanned,
            body,
        };
        drop(writer);
        let held = page.predecessor.and_then(|id| reads.snapshots.remove(&id));
        let published = reads.publish(
            snapshot,
            now,
            CAPTURE_BYTES,
            HANDLE_BYTES,
            MAX_HANDLES,
            cancellation,
        );
        if published.is_err()
            && let Some(held) = held
        {
            reads.snapshots.insert(held.id.clone(), held);
        }
        published
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
    fn new(scope: crate::QueryScope) -> Self {
        Self {
            rows: BTreeMap::new(),
            capacity: 4096,
            scope,
            diagnostic_coverage: crate::DiagnosticCoverage::default(),
        }
    }

    /// Fold one admitted native catalog entry without retaining its decoded body.
    /// `receipt` is the index time it was reached through, which must agree.
    #[allow(clippy::too_many_arguments)]
    fn push(
        &mut self,
        writer: &Writer,
        key: &[u8],
        bytes: &[u8],
        now: u64,
        protected: &BTreeSet<String>,
        admission: &mut CaptureAdmission<'_>,
        receipt: Option<u64>,
    ) -> Result<(), Error> {
        admission.entry(bytes.len())?;
        let value = super::parse(bytes)?;
        let (summary, retained) = if key.starts_with(b"artifact:") {
            let artifact = Artifact::decode(&value)?.stored(writer)?;
            if artifact.id.as_bytes() != key
                || receipt.is_some_and(|time| time != artifact.received_at)
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "artifact discovery catalog identity or receipt mismatch",
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
            if record.id.as_bytes() != key || receipt.is_some_and(|time| time != record.received_at)
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "artifact discovery record identity or receipt mismatch",
                ));
            }
            writer.validate_event_record(&record)?;
            if !writer.live_record(&record, now, protected) {
                return Ok(());
            }
            // A record shares its parent's receipt. A parent without a receipt
            // index row (an evicted catalog) is folded here, before its records.
            if !self.rows.contains_key(&record.artifact_id)
                && let Some(parent) = writer.db.get_pinned(&record.artifact_id)?
            {
                self.push(
                    writer,
                    record.artifact_id.as_bytes(),
                    &parent,
                    now,
                    protected,
                    admission,
                    Some(record.received_at),
                )?;
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
        admission: &mut CaptureAdmission<'_>,
    ) -> Result<ArtifactPageBody, Error> {
        let protected = self.artifact_protected(now, admission)?;
        let selection =
            self.query_selection_membership(&selector.selection, now, &protected, Some(admission))?;
        let mut capture = ArtifactCapture::new(selector.scope());
        if let Some(selection) = selection {
            for id in &selection.parents {
                if let Some(bytes) = self.db.get_pinned(id)? {
                    capture.push(
                        self,
                        id.as_bytes(),
                        &bytes,
                        now,
                        &protected,
                        admission,
                        None,
                    )?;
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
                capture.push(
                    self,
                    id.as_bytes(),
                    &bytes,
                    now,
                    &protected,
                    admission,
                    None,
                )?;
            }
        } else {
            let mut receipts =
                Receipts::new(self, selector.received_from, selector.received_before);
            while let Some(time) = receipts.next_time()? {
                receipts.fold(self, &mut capture, time, now, &protected, admission)?;
            }
        }
        let mut artifacts = Vec::new();
        for (_, summary) in capture.rows {
            admission.cancellation.checkpoint()?;
            if let Some(summary) = finish_row(&capture.scope, selector, summary)? {
                artifacts.push(summary);
            }
        }
        Ok(ArtifactPageBody {
            artifacts,
            diagnostic_coverage: capture.diagnostic_coverage,
            offset: 0,
            resume: None,
        })
    }

    /// Read one live page in receipt order from `start`, stopping at `limit` rows.
    /// The page's own admission bounds it. Exhausting that admission after a row
    /// ends the page with a continuation; exhausting it first refuses by name.
    fn artifact_receipt_page(
        &self,
        selector: &ArtifactSelector,
        start: &Position,
        limit: usize,
        now: u64,
        admission: &mut CaptureAdmission<'_>,
    ) -> Result<ArtifactPageBody, Error> {
        let (page_size, scan_budget) = (limit, admission.scan_budget);
        let refuse = |error: Error| {
            if error.code != "RESOURCE_LIMIT" {
                return error;
            }
            Error::new(
                "RESOURCE_LIMIT",
                format!(
                    "an artifact listing page of page_size {page_size} cannot fit one receipt row within its bound of {} catalog entries (scan_budget) and {SCAN_BYTES} charged catalog bytes: {}",
                    scan_budget, error.message
                ),
            )
        };
        let protected = self.artifact_protected(now, admission).map_err(&refuse)?;
        let scope = selector.scope();
        let mut receipts = Receipts::new(self, start.received_at, selector.received_before);
        let mut artifacts = Vec::new();
        let mut diagnostic_coverage = crate::DiagnosticCoverage::default();
        let mut read_any = false;
        let mut resume = None;
        'groups: while let Some(time) = receipts.next_time()? {
            let after = start.after.as_deref().filter(|_| time == start.received_at);
            if artifacts.len() == limit {
                resume = Some(Position {
                    received_at: time,
                    after: after.map(str::to_owned),
                });
                break;
            }
            let mut capture = ArtifactCapture::new(scope.clone());
            match receipts.fold(self, &mut capture, time, now, &protected, admission) {
                Ok(()) => {}
                Err(error) if error.code == "RESOURCE_LIMIT" && read_any => {
                    resume = Some(Position {
                        received_at: time,
                        after: after.map(str::to_owned),
                    });
                    break;
                }
                Err(error) => return Err(refuse(error)),
            }
            let rows = capture
                .rows
                .into_iter()
                .filter(|(id, _)| after.is_none_or(|after| id.as_str() > after));
            let mut last = after.map(str::to_owned);
            for (id, summary) in rows {
                admission.cancellation.checkpoint()?;
                if artifacts.len() == limit {
                    resume = Some(Position {
                        received_at: time,
                        after: last,
                    });
                    break 'groups;
                }
                read_any = true;
                last = Some(id);
                diagnostic_coverage.merge(summary.diagnostic_coverage);
                if let Some(summary) = finish_row(&scope, selector, summary)? {
                    artifacts.push(summary);
                }
            }
        }
        Ok(ArtifactPageBody {
            artifacts,
            diagnostic_coverage,
            offset: 0,
            resume,
        })
    }

    /// Live pins protect evidence for every artifact capture, charged to its admission.
    fn artifact_protected(
        &self,
        now: u64,
        admission: &mut CaptureAdmission<'_>,
    ) -> Result<BTreeSet<String>, Error> {
        let CaptureAdmission {
            cancellation,
            started,
            scan_budget,
            scanned,
            remaining,
        } = admission;
        self.protected_matching(
            now,
            |_| true,
            || {
                cancellation.checkpoint()?;
                if **scanned == *scan_budget
                    || started.elapsed() >= Duration::from_millis(catalog_lease::LEASE_MS)
                {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "artifact pin capture exhausted scan/time admission",
                    ));
                }
                **scanned += 1;
                Ok(())
            },
            Some(ENTRY_BYTES),
            Some(&mut **remaining),
        )
    }
}

/// Artifact and record receipt indexes over one half-open receipt interval.
/// Every record shares its parent's receipt, so one millisecond is one fold group.
struct Receipts<'a> {
    artifacts: TemporalRows<'a>,
    records: TemporalRows<'a>,
}

impl<'a> Receipts<'a> {
    fn new(writer: &'a Writer, from: u64, before: u64) -> Self {
        let scan = |options: rocksdb::ReadOptions| writer.db.raw_iterator_opt(options);
        Self {
            artifacts: TemporalRows::new(super::artifact_rdf::TIME, from, before, scan),
            records: TemporalRows::new("time:", from, before, scan),
        }
    }

    /// The earliest receipt time either index holds next.
    fn next_time(&mut self) -> Result<Option<u64>, Error> {
        let artifact = head_time(&mut self.artifacts, super::artifact_rdf::TIME)?;
        let record = head_time(&mut self.records, "time:")?;
        Ok(artifact.into_iter().chain(record).min())
    }

    /// Fold every artifact, then every record, received at `time`.
    fn fold(
        &mut self,
        writer: &Writer,
        capture: &mut ArtifactCapture,
        time: u64,
        now: u64,
        protected: &BTreeSet<String>,
        admission: &mut CaptureAdmission<'_>,
    ) -> Result<(), Error> {
        for (rows, prefix, kind) in [
            (&mut self.artifacts, super::artifact_rdf::TIME, "artifact:"),
            (&mut self.records, "time:", "record:"),
        ] {
            while head_time(rows, prefix)? == Some(time) {
                let folded = rows.read_next(|_, id| {
                    let id = std::str::from_utf8(id)
                        .ok()
                        .filter(|id| id.starts_with(kind) && id.len() > kind.len())
                        .ok_or_else(|| {
                            Error::new(
                                "STORAGE_FAILURE",
                                "receipt index requires a bounded catalog identity",
                            )
                        })?;
                    let bytes = writer.db.get_pinned(id)?.ok_or_else(|| {
                        Error::new(
                            "STORAGE_FAILURE",
                            "receipt index references a missing catalog entry",
                        )
                    })?;
                    capture.push(
                        writer,
                        id.as_bytes(),
                        &bytes,
                        now,
                        protected,
                        admission,
                        Some(time),
                    )
                });
                folded.transpose()?;
            }
        }
        Ok(())
    }
}

fn head_time(rows: &mut TemporalRows<'_>, prefix: &str) -> Result<Option<u64>, Error> {
    rows.peek()
        .map(|entry| entry.and_then(|(key, value)| entry_time(prefix, key, value)))
        .transpose()
}

/// Keep a summary only when it is available and matches the outer filters.
fn finish_row(
    scope: &crate::QueryScope,
    selector: &ArtifactSelector,
    summary: ArtifactSummary,
) -> Result<Option<ArtifactSummary>, Error> {
    if !summary.complete_bytes_available && summary.retained_records == 0 {
        return Ok(None);
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
    if !artifact_eligible(scope, &summary.source, summary.origin.as_ref())?
        || !selector.diagnostics.is_empty() && summary.diagnostic_coverage.matched == 0
    {
        return Ok(None);
    }
    Ok(Some(summary))
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
