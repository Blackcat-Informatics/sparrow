// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Pin-aware artifact discovery, including parents known only through retained records.
//!
//! The live listing walks the per-source artifact index (`source-artifact:`), source by
//! source in source-key order and by receipt within a source, reads each listed
//! artifact's entry and its records, and stops at its page, so its charge follows the
//! page rather than the store. Explicit selections remain one bounded immutable capture.

use super::{
    Catalog, Error, Record, Store, Writer, catalog_lease, listing_build, random_id, source_summary,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use super::catalog_lease::{CAPTURE_BYTES, CaptureAdmission, ENTRY_BYTES, MAX_ITEMS, SCAN_BYTES};

/// Charged bytes of one owed index row beyond its key: its share of the ordered set.
const OWED_ROW_BYTES: usize = 64;
/// Owed index rows one window holds. Keys are at most a few hundred bytes, so a full window
/// is charged well inside the page admission.
const OWED_WINDOW: usize = 32_768;
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

/// Artifact summaries: selections in artifact-ID order, live pages in source-key, then
/// receipt, then ID order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactPageBody {
    /// Page rows. Missing parents retain identity/source and explicit unknown metadata.
    pub artifacts: Vec<ArtifactSummary>,
    /// Coverage across the eligible records of a capture's rows, or of the rows
    /// a live page delivered or excluded, including parents with no proven match.
    pub diagnostic_coverage: crate::DiagnosticCoverage,
    /// Whether this is a live listing page, read from the per-source index page by page,
    /// whose counts cover only the rows delivered so far while the listing continues.
    receipt_walk: bool,
    /// Rows delivered by earlier pages of the same live listing.
    offset: usize,
    /// Index position after this page's rows, while the live listing continues.
    resume: Option<Position>,
    /// The index rows the source-summary migration owed when the live listing began.
    owed: Owed,
}

/// A window of the per-source index rows the unfinished source-summary migration owes within
/// a live listing's selection: the first [`OWED_WINDOW`] after the listing's position, read
/// from a snapshot without the writer lock, and their charged bytes. The pages of a listing
/// share one window and charge it to their admission; it is not handle bytes, which bound
/// one page's rows. `through` is the window's last row when more owed rows follow it: a page
/// stops before any row after it, and the next page reads the next window.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Owed {
    rows: std::sync::Arc<BTreeSet<String>>,
    bytes: usize,
    through: Option<String>,
}

impl ArtifactPageBody {
    /// A live listing page rather than one immutable capture.
    #[must_use]
    pub fn receipt_walk(&self) -> bool {
        self.receipt_walk
    }
}

/// Live listing position: the last index row whose artifact was folded completely, and the
/// row being folded when a page's admission ran out inside it, so an artifact with more
/// records than one page admits spans pages. The partial row is boxed so a page result
/// carries one pointer for it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Position {
    after: Option<String>,
    partial: Option<Box<Partial>>,
}

/// One artifact folded part way: its index key, the records consumed so far and the summary
/// folded from them, absent until the first catalog entry is read.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Partial {
    key: String,
    received_at: u64,
    id: String,
    /// Listed from the owed rows rather than the stored index: an artifact retired since
    /// the listing began is then absent from the finished index too, and is skipped.
    owed: bool,
    consumed: Consumed,
    summary: Option<ArtifactSummary>,
}

/// The next row a live page lists.
enum Listed {
    /// An index row: its key, receipt time, artifact identity and whether it is owed.
    Row(String, u64, String, bool),
    /// The next row lies after the owed window's end.
    Window,
    /// No row follows.
    End,
}

/// Records consumed by a partial fold.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Consumed {
    /// The artifact entry has not been read.
    Start,
    /// This many identities of the artifact entry's record manifest.
    Listed(u64),
    /// A deleted parent's records, read from the source's receipt timeline at the parent's
    /// receipt; the ID of the last record consumed.
    Indexed(Option<String>),
}

impl Partial {
    fn capacity(&self) -> usize {
        self.key.capacity()
            + self.id.capacity()
            + match &self.consumed {
                Consumed::Indexed(Some(after)) => after.capacity(),
                _ => 0,
            }
            + self.summary.as_ref().map_or(0, ArtifactSummary::capacity)
    }
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
            + self.resume.as_ref().map_or(0, |position| {
                position.after.as_ref().map_or(0, String::capacity)
                    + position.partial.as_ref().map_or(0, |partial| {
                        std::mem::size_of::<Partial>() + partial.capacity()
                    })
            })
    }
    fn page(&self, start: usize, end: usize) -> Result<Self, Error> {
        Ok(Self {
            artifacts: self.artifacts[start..end].to_vec(),
            diagnostic_coverage: self.diagnostic_coverage,
            receipt_walk: self.receipt_walk,
            offset: self.offset,
            resume: self.resume.clone(),
            owed: self.owed.clone(),
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
        let binding = (&query.selector, query.page_size, query.scan_budget);
        if let Some(cursor) = &query.cursor {
            let parsed = catalog_lease::Cursor::parse(cursor)?;
            if listing_build::is_build(&parsed, BUILD) {
                return self.artifact_build_poll(&parsed, Some(binding), now, cancellation);
            }
            let mut reads = self.artifact_reads()?;
            reads.purge(now);
            cancellation.checkpoint()?;
            match self.artifact_next(&mut reads, &parsed, Some(binding), now, cancellation)? {
                Some(Next::Page(page)) => return Ok(page),
                Some(Next::Window(pending)) => {
                    drop(reads);
                    return self.artifact_window(pending, now, cancellation);
                }
                None => {}
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
            let pending = Pending {
                selector: query.selector.clone(),
                page_size: query.page_size,
                scan_budget: query.scan_budget,
                start: Position::default(),
                offset: 0,
            };
            return self.artifact_window(pending, now, cancellation);
        }
        let mut reads = self.artifact_reads()?;
        reads.purge(now);
        cancellation.checkpoint()?;
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

/// A live listing page that must first read a window of the rows the source-summary
/// migration owes: where it starts and the rows earlier pages delivered. It is the context a
/// listing build keeps for the page it serves once it finishes.
#[derive(Clone)]
pub(super) struct Pending {
    selector: ArtifactSelector,
    page_size: usize,
    scan_budget: usize,
    start: Position,
    offset: usize,
}

/// What a continuation cursor leads to.
pub(super) enum Next {
    /// The next page, read.
    Page(ArtifactReadPage),
    /// A page that first needs the next window of owed rows.
    Window(Pending),
}

/// Artifact listing builds, each computing one window of owed index rows.
pub(super) type Builds = listing_build::Builds<Pending, Owed>;
/// Identity kind of an artifact listing build.
pub(super) const BUILD: &str = "artifact-build";

/// One live listing page request, fresh or continuing a previous page's handle.
struct Continued<'a> {
    selector: &'a ArtifactSelector,
    page_size: usize,
    scan_budget: usize,
    start: Position,
    offset: usize,
    owed: Owed,
}

impl Store {
    /// The next window of index rows the unfinished source-summary migration owes within
    /// `selector`'s sources and receipt bounds after `after`, in key order; empty once it is
    /// complete. A live listing reads a window from a snapshot without the writer lock, when
    /// it starts and whenever its pages pass the window's end, and merges it with the stored
    /// rows, so it lists exactly the artifacts the finished index will. Rows outside the
    /// window cannot be missed: a row the migration copies is stored, and an artifact written
    /// meanwhile is indexed by its own write. The pass runs to the end of what the migration
    /// owes, so its time follows the unmigrated remainder; CPU is not capped. It runs as a
    /// listing build: `worker` counts each catalog entry it reads and stops it once the
    /// build is cancelled or its lease ends. Once the running migration overtakes the pass,
    /// it starts again from a newer snapshot, which owes less (#176); any snapshot taken
    /// before the page reads the stored rows gives the same listing.
    fn owed_index_rows(
        &self,
        selector: &ArtifactSelector,
        after: Option<&str>,
        worker: &listing_build::Worker<Owed>,
    ) -> Result<Owed, Error> {
        let prefixes = selected_prefixes(selector);
        let (from, before) = (selector.received_from, selector.received_before);
        let live = super::store_counters::Reader::db(&self.unlocked.db);
        let owed = loop {
            let snapshot = self.unlocked.db.snapshot();
            let reader = super::store_counters::Reader::snapshot(&snapshot);
            let progress = source_summary::progress(|key| reader.get(key.as_bytes()))?;
            let base = source_summary::Base::read(&reader, &progress)?;
            worker.total(source_summary::owed_entries(&reader, &progress)?);
            let (prefixes, after) = (prefixes.clone(), after.map(str::to_owned));
            let keep = move |key: &str| {
                let prefix = key.get(..source_summary::INDEX.len() + 65).unwrap_or(key);
                after.as_deref().is_none_or(|after| key > after)
                    && prefixes
                        .as_ref()
                        .is_none_or(|list| list.iter().any(|wanted| wanted == prefix))
                    && source_summary::index_entry(prefix.as_bytes(), key.as_bytes())
                        .is_ok_and(|(time, _)| time >= from && time < before)
            };
            match source_summary::Completion::owed(
                &reader,
                &progress,
                (self.owed_window(), Box::new(keep)),
                &mut || worker.entry(|read| base.overtaken(&live, read)),
            )? {
                source_summary::Pass::Complete => return Ok(Owed::default()),
                source_summary::Pass::Owed(owed) => break owed,
                source_summary::Pass::Overtaken => worker.rebase(),
            }
        };
        let bytes = owed
            .index
            .iter()
            .map(|key| OWED_ROW_BYTES + key.len())
            .sum();
        let through = owed.truncated.then(|| owed.index.last().cloned()).flatten();
        Ok(Owed {
            rows: std::sync::Arc::new(owed.index),
            bytes,
            through,
        })
    }

    /// Owed index rows one listing window holds.
    fn owed_window(&self) -> usize {
        #[cfg(feature = "test-support")]
        if let rows @ 1.. = self.owed_window.load(std::sync::atomic::Ordering::SeqCst) {
            return rows;
        }
        OWED_WINDOW
    }

    /// Read the next live page when `cursor` continues past its handle's rows.
    /// Other cursors are replayed by the shared lease owner, so this returns `None`.
    /// The continued handle stays live: its owner retires it once the new page
    /// has been delivered, so an abandoned or failed continuation keeps the cursor. A page
    /// past its window's end returns [`Next::Window`], so the caller can release the
    /// registry before it reads the next window.
    pub(super) fn artifact_next(
        &self,
        reads: &mut Reads,
        cursor: &catalog_lease::Cursor<'_>,
        binding: Option<(&ArtifactSelector, usize, usize)>,
        now: u64,
        cancellation: &crate::QueryCancellation,
    ) -> Result<Option<Next>, Error> {
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
        let (page_size, scan_budget) = (snapshot.page_size, snapshot.scan_budget);
        let offset = snapshot.body.offset + snapshot.body.artifacts.len();
        let owed = snapshot.body.owed.clone();
        // The previous page passed its window's end: the next window is read before the
        // page takes the writer.
        if let Some(through) = &owed.through
            && start.partial.is_none()
            && start.after.as_deref() >= Some(through.as_str())
        {
            return Ok(Some(Next::Window(Pending {
                selector,
                page_size,
                scan_budget,
                start,
                offset,
            })));
        }
        let page = Continued {
            selector: &selector,
            page_size,
            scan_budget,
            start,
            offset,
            owed,
        };
        self.artifact_page(reads, page, now, cancellation)
            .map(|page| Some(Next::Page(page)))
    }

    pub(super) fn artifact_reads(&self) -> Result<std::sync::MutexGuard<'_, Reads>, Error> {
        self.artifact_reads
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "artifact read registry poisoned"))
    }

    pub(super) fn artifact_builds(&self) -> Result<std::sync::MutexGuard<'_, Builds>, Error> {
        self.unlocked
            .artifact_builds
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "artifact listing builds poisoned"))
    }

    /// Serve a live page that needs the window of owed rows after its position. Once the
    /// source-summary migration is complete nothing is owed and the page is read at once;
    /// otherwise the window is read by a listing build (#158).
    pub(super) fn artifact_window(
        &self,
        pending: Pending,
        now: u64,
        cancellation: &crate::QueryCancellation,
    ) -> Result<ArtifactReadPage, Error> {
        let complete = source_summary::progress(|key| {
            self.unlocked.db.get(key.as_bytes()).map_err(Error::from)
        })? == source_summary::Progress::Complete;
        if complete {
            return self.artifact_pending_page(pending, Owed::default(), now, cancellation);
        }
        let (id, waiter, wait) = {
            let store = self.clone();
            let (selector, after) = (pending.selector.clone(), pending.start.after.clone());
            let mut builds = self.artifact_builds()?;
            let (id, waiter) = builds.start(BUILD, pending, now, move |worker| {
                store.owed_index_rows(&selector, after.as_deref(), worker)
            })?;
            (id, waiter, builds.wait())
        };
        let page = self.artifact_build_page(&id, &waiter, wait, now, cancellation);
        if page.is_err() {
            // The caller never learns this build's cursor: nothing can reach it.
            self.artifact_builds()?.remove(&id);
        }
        page
    }

    /// Follow a build cursor, binding it to the caller's selector and limits when given.
    pub(super) fn artifact_build_poll(
        &self,
        cursor: &catalog_lease::Cursor<'_>,
        binding: Option<(&ArtifactSelector, usize, usize)>,
        now: u64,
        cancellation: &crate::QueryCancellation,
    ) -> Result<ArtifactReadPage, Error> {
        if cursor.start() != 0 {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "cursor is not a listing build position",
            ));
        }
        let (waiter, wait) = {
            let mut builds = self.artifact_builds()?;
            let (pending, waiter) = builds.poll(cursor.id(), now)?;
            if let Some((selector, page_size, scan_budget)) = binding
                && (&pending.selector != selector
                    || pending.page_size != page_size
                    || pending.scan_budget != scan_budget)
            {
                return Err(Error::new(
                    "CONFLICT",
                    "cursor requires its original operation, selector and limits",
                ));
            }
            (waiter, builds.wait())
        };
        self.artifact_build_page(cursor.id(), &waiter, wait, now, cancellation)
    }

    /// Wait for an owed-window build, at most `wait`: the page it serves once finished,
    /// otherwise a page with no rows, its cursor and its progress.
    fn artifact_build_page(
        &self,
        id: &str,
        waiter: &listing_build::Waiter<Owed>,
        wait: std::time::Duration,
        now: u64,
        cancellation: &crate::QueryCancellation,
    ) -> Result<ArtifactReadPage, Error> {
        let finished = waiter.wait(wait, cancellation)?;
        let mut builds = self.artifact_builds()?;
        let Some(result) = finished else {
            let (Some(pending), Some(lease_expires_at)) =
                (builds.context(id), builds.expires_at(id))
            else {
                return Err(Error::new("RESULT_EXPIRED", "listing build lease ended"));
            };
            return Ok(catalog_lease::CatalogReadPage {
                store_id: self.id.to_string(),
                snapshot_id: id.to_owned(),
                as_of: now,
                lease_expires_at,
                total_items: pending.offset,
                page_start: pending.offset,
                next_cursor: Some(format!("{id}/0")),
                page_size: pending.page_size,
                scan_budget: pending.scan_budget,
                scanned: 0,
                build: Some(waiter.progress()),
                body: ArtifactPageBody {
                    artifacts: Vec::new(),
                    diagnostic_coverage: crate::DiagnosticCoverage::default(),
                    receipt_walk: true,
                    offset: pending.offset,
                    resume: None,
                    owed: Owed::default(),
                },
            });
        };
        let owed = match result {
            Ok(owed) => owed,
            Err(error) => {
                builds.remove(id);
                return Err(error);
            }
        };
        let pending = builds.context(id).cloned().ok_or_else(|| {
            Error::new(
                "RESULT_EXPIRED",
                "listing build lease ended before its page",
            )
        })?;
        drop(builds);
        // The build is retired only once its page exists, so a failed or cancelled call
        // leaves the finished window for the next call on the same cursor.
        let page = self.artifact_pending_page(pending, owed, now, cancellation)?;
        #[cfg(feature = "test-support")]
        if self.artifact_builds()?.take_delivery_failure_for_tests() {
            return Err(Error::new("CANCELLED", "test: the page was not delivered"));
        }
        self.artifact_builds()?.remove(id);
        Ok(page)
    }

    fn artifact_pending_page(
        &self,
        pending: Pending,
        owed: Owed,
        now: u64,
        cancellation: &crate::QueryCancellation,
    ) -> Result<ArtifactReadPage, Error> {
        let mut reads = self.artifact_reads()?;
        reads.purge(now);
        cancellation.checkpoint()?;
        let page = Continued {
            selector: &pending.selector,
            page_size: pending.page_size,
            scan_budget: pending.scan_budget,
            start: pending.start,
            offset: pending.offset,
            owed,
        };
        self.artifact_page(&mut reads, page, now, cancellation)
    }

    /// Read and publish one live page under its own lease.
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
        // The owed window stays resident while the page reads, so it is charged to it. A
        // window is bounded well inside the admission.
        let mut remaining = SCAN_BYTES.checked_sub(page.owed.bytes).ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "artifact listing owed window exceeds its admission",
            )
        })?;
        let writer = self.lock()?;
        cancellation.checkpoint()?;
        let mut scanned = 0;
        let mut admission = CaptureAdmission {
            cancellation,
            started,
            scan_budget: page.scan_budget,
            scanned: &mut scanned,
            remaining: &mut remaining,
        };
        let mut body = writer.artifact_index_page(
            page.selector,
            page.start,
            page.page_size,
            now,
            &page.owed,
            &mut admission,
        )?;
        body.offset = page.offset;
        body.owed = page.owed;
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
        let (summary, retained) = if key.starts_with(b"artifact:") {
            // Validate the manifest arrays without owning them, so the charge
            // follows the scalar metadata rather than the artifact's record count.
            admission.visit()?;
            let id = std::str::from_utf8(key)
                .map_err(|_| Error::new("STORAGE_FAILURE", "invalid artifact catalog key"))?;
            let header = super::artifact_rdf::catalog(&writer.db, id, bytes, admission.remaining)?;
            let original_records = usize::try_from(header.record_count)
                .map_err(|_| Error::new("STORAGE_FAILURE", "artifact record count overflow"))?;
            let artifact = header.artifact;
            if let Some(time) = receipt.filter(|time| *time != artifact.received_at) {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    format!(
                        "{id} is indexed at receipt {time} but its catalog was received at {}",
                        artifact.received_at
                    ),
                ));
            }
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
                        original_records,
                    }),
                    complete_bytes_available: complete,
                    retained_records: 0,
                    diagnostic_coverage: crate::DiagnosticCoverage::default(),
                },
                false,
            )
        } else {
            admission.entry(bytes.len())?;
            let record = Record::decode(&super::parse(bytes)?)?;
            if record.id.as_bytes() != key {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "artifact discovery record identity mismatch",
                ));
            }
            if let Some(time) = receipt.filter(|time| *time != record.received_at) {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    format!(
                        "{} is indexed at receipt {time} but was received at {}; its parent is {}",
                        record.id, record.received_at, record.artifact_id
                    ),
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
            return Err(Error::new(
                "STORAGE_FAILURE",
                "a live artifact listing reads the per-source index page by page, not as one capture",
            ));
        }
        let mut artifacts = Vec::new();
        for (_, summary) in capture.rows {
            admission.cancellation.checkpoint()?;
            if let Some(summary) = finish_row(&capture.scope, selector, summary, false)? {
                artifacts.push(summary);
            }
        }
        Ok(ArtifactPageBody {
            artifacts,
            diagnostic_coverage: capture.diagnostic_coverage,
            receipt_walk: false,
            offset: 0,
            resume: None,
            owed: Owed::default(),
        })
    }

    /// Read one live page from the per-source artifact index from `start`. Stop at the page
    /// size, or when the page's admission runs out after it consumed a catalog entry, and
    /// return the position to continue from. A page that cannot consume one catalog entry
    /// refuses by naming the row. While the per-source index is being seeded, the listing's
    /// `owed` window is merged with the stored rows, and the page stops before any row after
    /// the window's end.
    fn artifact_index_page(
        &self,
        selector: &ArtifactSelector,
        start: Position,
        page_size: usize,
        now: u64,
        owed: &Owed,
        admission: &mut CaptureAdmission<'_>,
    ) -> Result<ArtifactPageBody, Error> {
        let limit = page_size.min(<ArtifactPageBody as catalog_lease::PageBody>::PAGE_ITEMS);
        let prefixes = selected_prefixes(selector);
        let protected = self.artifact_protected(now, admission)?;
        let scope = selector.scope();
        let mut position = start;
        let mut artifacts = Vec::new();
        let mut diagnostic_coverage = crate::DiagnosticCoverage::default();
        let mut progressed = false;
        let resume = loop {
            if artifacts.len() == limit {
                let more = position.partial.is_some()
                    || !matches!(
                        self.next_listed_row(
                            prefixes.as_deref(),
                            position.after.as_deref(),
                            selector,
                            owed,
                        )?,
                        Listed::End
                    );
                break more.then_some(position);
            }
            let mut row = match position.partial.take() {
                Some(row) => *row,
                None => {
                    let (key, received_at, id, owed) = match self.next_listed_row(
                        prefixes.as_deref(),
                        position.after.as_deref(),
                        selector,
                        owed,
                    )? {
                        Listed::Row(key, received_at, id, owed) => (key, received_at, id, owed),
                        // The next page reads the next window.
                        Listed::Window => break Some(position),
                        Listed::End => break None,
                    };
                    Partial {
                        key,
                        received_at,
                        id,
                        owed,
                        consumed: Consumed::Start,
                        summary: None,
                    }
                }
            };
            match self.fold_index_row(&mut row, &scope, now, &protected, admission) {
                Ok(()) => {
                    progressed = true;
                    if row.owed && row.summary.is_none() {
                        // Retired since the listing began: the finished index omits it.
                        position.after = Some(row.key);
                        continue;
                    }
                    let summary = row.summary.ok_or_else(|| {
                        Error::new(
                            "STORAGE_FAILURE",
                            format!(
                                "the per-source index lists {} without its entry or a retained record",
                                row.id
                            ),
                        )
                    })?;
                    position.after = Some(row.key);
                    diagnostic_coverage.merge(summary.diagnostic_coverage);
                    if let Some(summary) = finish_row(&scope, selector, summary, true)? {
                        artifacts.push(summary);
                    }
                }
                Err(error) if error.code == "RESOURCE_LIMIT" => {
                    if progressed || row.consumed != Consumed::Start {
                        position.partial = Some(Box::new(row));
                        break Some(position);
                    }
                    return Err(refuse_row(
                        selector,
                        page_size,
                        admission.scan_budget,
                        &row,
                        &error,
                    ));
                }
                Err(error) => return Err(error),
            }
        };
        Ok(ArtifactPageBody {
            artifacts,
            diagnostic_coverage,
            receipt_walk: true,
            offset: 0,
            resume,
            owed: Owed::default(),
        })
    }

    /// The next listed row after `after`: the lesser of the next stored index row and the
    /// next owed one in the window, and whether it is owed. A row the migration copied since
    /// the window was read is stored, so it is listed once. A row after the window's end
    /// waits for the next window, since an owed row may precede it.
    fn next_listed_row(
        &self,
        prefixes: Option<&[String]>,
        after: Option<&str>,
        selector: &ArtifactSelector,
        window: &Owed,
    ) -> Result<Listed, Error> {
        let next = self.next_window_row(prefixes, after, selector, &window.rows)?;
        Ok(match (next, &window.through) {
            (Some(row), Some(through)) if row.0 > *through => Listed::Window,
            (None, Some(_)) => Listed::Window,
            (Some((key, time, id, owed)), _) => Listed::Row(key, time, id, owed),
            (None, None) => Listed::End,
        })
    }

    /// The lesser of the next stored index row after `after` and the next row of `owed`.
    fn next_window_row(
        &self,
        prefixes: Option<&[String]>,
        after: Option<&str>,
        selector: &ArtifactSelector,
        owed: &BTreeSet<String>,
    ) -> Result<Option<(String, u64, String, bool)>, Error> {
        let stored = self
            .next_index_row(prefixes, after, selector)?
            .map(|(key, time, id)| (key, time, id, false));
        let pending = match after {
            Some(after) => owed
                .range::<str, _>((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                .next(),
            None => owed.first(),
        };
        let Some(pending) = pending.filter(|key| {
            stored
                .as_ref()
                .is_none_or(|(stored, _, _, _)| key.as_str() < stored.as_str())
        }) else {
            return Ok(stored);
        };
        let prefix = pending
            .get(..source_summary::INDEX.len() + 65)
            .ok_or_else(|| {
                Error::new("STORAGE_FAILURE", "invalid per-source artifact index key")
            })?;
        let (time, id) = source_summary::index_entry(prefix.as_bytes(), pending.as_bytes())?;
        Ok(Some((pending.clone(), time, id.to_owned(), true)))
    }

    /// The next per-source index row after `after` within the selected sources and the
    /// receipt bounds: its key, receipt time and artifact identity. Rows outside the bounds
    /// are skipped by seeking, never read.
    fn next_index_row(
        &self,
        prefixes: Option<&[String]>,
        after: Option<&str>,
        selector: &ArtifactSelector,
    ) -> Result<Option<(String, u64, String)>, Error> {
        let from = format!("{:016x}", selector.received_from);
        let following = |prefix: &str| {
            prefixes.map(|list| list.iter().find(|next| next.as_str() > prefix).cloned())
        };
        let mut iterator = self.db.raw_iterator();
        match (after, prefixes) {
            (Some(after), _) => {
                iterator.seek(after);
                if iterator.valid() && iterator.key() == Some(after.as_bytes()) {
                    iterator.next();
                }
            }
            (None, Some(list)) => match list.first() {
                Some(prefix) => iterator.seek(format!("{prefix}{from}")),
                None => return Ok(None),
            },
            (None, None) => iterator.seek(source_summary::INDEX),
        }
        loop {
            if !iterator.valid() {
                iterator.status()?;
                return Ok(None);
            }
            let key = iterator
                .key()
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "artifact index key missing"))?
                .to_vec();
            if !key.starts_with(source_summary::INDEX.as_bytes()) {
                return Ok(None);
            }
            let prefix = key
                .get(..source_summary::INDEX.len() + 65)
                .and_then(|prefix| std::str::from_utf8(prefix).ok())
                .filter(|prefix| prefix.ends_with(':'))
                .ok_or_else(|| {
                    Error::new("STORAGE_FAILURE", "invalid per-source artifact index key")
                })?
                .to_owned();
            if let Some(list) = prefixes
                && !list.contains(&prefix)
            {
                match following(&prefix).flatten() {
                    Some(next) => {
                        iterator.seek(format!("{next}{from}"));
                        continue;
                    }
                    None => return Ok(None),
                }
            }
            let (time, id) = source_summary::index_entry(prefix.as_bytes(), &key)?;
            if time < selector.received_from {
                iterator.seek(format!("{prefix}{from}"));
                continue;
            }
            if time >= selector.received_before {
                match following(&prefix) {
                    Some(Some(next)) => iterator.seek(format!("{next}{from}")),
                    Some(None) => return Ok(None),
                    // `;` follows `:`, so this seeks past every row of this source.
                    None => iterator.seek(format!("{};", &prefix[..prefix.len() - 1])),
                }
                continue;
            }
            let id = id.to_owned();
            let key = String::from_utf8(key).map_err(|_| {
                Error::new("STORAGE_FAILURE", "invalid per-source artifact index key")
            })?;
            return Ok(Some((key, time, id)));
        }
    }

    /// Fold one listed artifact: its entry, then its records through the entry's record
    /// manifest, or, for a parent whose entry was deleted, through the source's record
    /// receipt index at the parent's receipt. `row` records each consumed entry, so a
    /// refusal part way leaves the position to resume from.
    fn fold_index_row(
        &self,
        row: &mut Partial,
        scope: &crate::QueryScope,
        now: u64,
        protected: &BTreeSet<String>,
        admission: &mut CaptureAdmission<'_>,
    ) -> Result<(), Error> {
        let hash = row
            .key
            .get(source_summary::INDEX.len()..source_summary::INDEX.len() + 64)
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "invalid per-source artifact index key"))?
            .to_owned();
        if row.consumed == Consumed::Start {
            match self.db.get_pinned(&row.id)? {
                Some(bytes) => {
                    admission.visit()?;
                    let header = super::artifact_rdf::catalog(
                        &self.db,
                        &row.id,
                        &bytes,
                        admission.remaining,
                    )?;
                    let original_records = usize::try_from(header.record_count).map_err(|_| {
                        Error::new("STORAGE_FAILURE", "artifact record count overflow")
                    })?;
                    let artifact = header.artifact;
                    if artifact.received_at != row.received_at
                        || source_summary::source_hash(&artifact.source) != hash
                    {
                        return Err(Error::new(
                            "STORAGE_FAILURE",
                            format!(
                                "the per-source index row {} disagrees with its artifact's source or receipt",
                                row.key
                            ),
                        ));
                    }
                    let summary = ArtifactSummary {
                        complete_bytes_available: self.live_artifact(&artifact, now, protected),
                        id: artifact.id,
                        source: artifact.source,
                        origin: Some(ArtifactOrigin {
                            format: artifact.format,
                            received_at: artifact.received_at,
                            expires_at: artifact.expires_at,
                            byte_length: artifact.byte_length,
                            original_records,
                        }),
                        retained_records: 0,
                        diagnostic_coverage: crate::DiagnosticCoverage::default(),
                    };
                    if summary.capacity() > 4096 {
                        return Err(Error::new(
                            "RESOURCE_LIMIT",
                            "artifact summary exceeds row admission",
                        ));
                    }
                    row.summary = Some(summary);
                    row.consumed = Consumed::Listed(0);
                }
                None => row.consumed = Consumed::Indexed(None),
            }
        }
        loop {
            match row.consumed.clone() {
                Consumed::Start => {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "artifact listing fold lost its start",
                    ));
                }
                Consumed::Listed(consumed) => {
                    let bytes = self.db.get_pinned(&row.id)?.ok_or_else(|| {
                        Error::new(
                            "STORAGE_FAILURE",
                            format!("{} disappeared during its listing fold", row.id),
                        )
                    })?;
                    let mut scratch = usize::MAX;
                    let header = super::manifest_retire::header(&bytes, &mut scratch, 0, 0)?;
                    let ids = header.record_ids(&bytes, admission.remaining, consumed, 1024)?;
                    if ids.is_empty() {
                        return Ok(());
                    }
                    for (index, id) in ids.iter().enumerate() {
                        self.fold_index_record(
                            row, &hash, id, true, scope, now, protected, admission,
                        )?;
                        row.consumed = Consumed::Listed(consumed + index as u64 + 1);
                    }
                }
                Consumed::Indexed(after) => {
                    let mut next = None;
                    // One receipt instant: the walk is bounded by the records accepted
                    // then, so it runs under the writer like the rest of the page.
                    let reader = super::store_counters::Reader::db(&self.db);
                    let timeline = super::source_context::Timeline {
                        reader: &reader,
                        cached: super::source_context::cached(&self.db)?,
                        cancellation: admission.cancellation,
                    };
                    timeline.walk(
                        &hash,
                        (row.received_at, after.as_deref()),
                        row.received_at..=row.received_at,
                        rocksdb::Direction::Forward,
                        |_, id| {
                            next = Some(id.to_owned());
                            Ok(false)
                        },
                    )?;
                    let Some(id) = next else {
                        return Ok(());
                    };
                    self.fold_index_record(
                        row, &hash, &id, false, scope, now, protected, admission,
                    )?;
                    row.consumed = Consumed::Indexed(Some(id));
                }
            }
        }
    }

    /// Fold one record of a listed artifact. `listed` records come from the artifact's own
    /// manifest and must name it; a receipt index row of another artifact is skipped.
    #[allow(clippy::too_many_arguments)]
    fn fold_index_record(
        &self,
        row: &mut Partial,
        hash: &str,
        id: &str,
        listed: bool,
        scope: &crate::QueryScope,
        now: u64,
        protected: &BTreeSet<String>,
        admission: &mut CaptureAdmission<'_>,
    ) -> Result<(), Error> {
        // A listed record deleted after its deadline leaves its identity in the manifest.
        let Some(bytes) = self.db.get_pinned(id)? else {
            return Ok(());
        };
        admission.entry(bytes.len())?;
        let record = Record::decode(&super::parse(&bytes)?)?;
        if record.id != id {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "artifact discovery record identity mismatch",
            ));
        }
        if record.artifact_id != row.id {
            if listed {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    format!(
                        "{id} is listed by {} but names parent {}",
                        row.id, record.artifact_id
                    ),
                ));
            }
            return Ok(());
        }
        if record.received_at != row.received_at
            || source_summary::source_hash(&record.source) != hash
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                format!(
                    "{id} disagrees with its parent {}'s per-source index row on source or receipt",
                    row.id
                ),
            ));
        }
        self.validate_event_record(&record)?;
        let summary = row.summary.get_or_insert_with(|| ArtifactSummary {
            id: row.id.clone(),
            source: record.source.clone(),
            origin: None,
            complete_bytes_available: false,
            retained_records: 0,
            diagnostic_coverage: crate::DiagnosticCoverage::default(),
        });
        if summary.source != record.source {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "artifact and record source provenance disagree",
            ));
        }
        if !self.live_record(&record, now, protected) {
            return Ok(());
        }
        summary.retained_records += 1;
        if scope.matches_source(&record.source) {
            let mut coverage = crate::DiagnosticCoverage::default();
            self.matches_diagnostic_scope(
                scope,
                &record,
                &mut coverage,
                scope.require_complete_index,
            )?;
            summary.diagnostic_coverage.merge(coverage);
        }
        Ok(())
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

/// The index prefixes a listing walks, in key order, or `None` for every source.
fn selected_prefixes(selector: &ArtifactSelector) -> Option<Vec<String>> {
    let sources: Vec<&str> = match (&selector.source, &selector.sources) {
        (None, None) => return None,
        (Some(source), None) => vec![source.as_str()],
        (Some(source), Some(alternatives)) => alternatives
            .ids()
            .iter()
            .filter(|id| *id == source)
            .map(String::as_str)
            .take(1)
            .collect(),
        (None, Some(alternatives)) => alternatives.ids().iter().map(String::as_str).collect(),
    };
    let mut prefixes = sources
        .into_iter()
        .map(source_summary::index_prefix)
        .collect::<Vec<_>>();
    prefixes.sort();
    prefixes.dedup();
    Some(prefixes)
}

/// Name the page bound and the row that cannot fit within one fresh page.
fn refuse_row(
    selector: &ArtifactSelector,
    page_size: usize,
    scan_budget: usize,
    row: &Partial,
    error: &Error,
) -> Error {
    let received =
        crate::format_instant(row.received_at).unwrap_or_else(|_| row.received_at.to_string());
    let source = selector
        .source
        .as_deref()
        .map_or_else(String::new, |source| format!(" of source {source:?}"));
    Error::new(
        "RESOURCE_LIMIT",
        format!(
            "an artifact listing page of page_size {page_size} cannot fit one catalog row of artifact {}{source} received at {received} within its bound of {scan_budget} catalog entries (scan_budget) and {SCAN_BYTES} charged catalog bytes; raise scan_budget or start the receipt interval after it: {}",
            row.id, error.message
        ),
    )
}

/// Keep a summary only when it is available and matches the outer filters.
/// `indexed` rows were reached through the per-source index between the receipt bounds,
/// which proves a reclaimed parent's receipt without its metadata.
fn finish_row(
    scope: &crate::QueryScope,
    selector: &ArtifactSelector,
    summary: ArtifactSummary,
    indexed: bool,
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
    let eligible = if indexed {
        scope.matches_source(&summary.source)
    } else {
        artifact_eligible(scope, &summary.source, summary.origin.as_ref())?
    };
    if !eligible || !selector.diagnostics.is_empty() && summary.diagnostic_coverage.matched == 0 {
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
            "EVIDENCE_INCOMPLETE",
            "original artifact receipt time is unknown after parent reclamation; omit the receipt filter or select retained records",
        )),
    }
}
