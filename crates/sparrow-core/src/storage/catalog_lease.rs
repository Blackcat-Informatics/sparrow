// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One bounded metadata lease and cursor implementation for catalog readers.

use super::Error;
use std::{
    collections::BTreeMap,
    mem::size_of,
    time::{Duration, Instant},
};

pub(super) const LEASE_MS: u64 = 60_000;
/// Conservative structured output ceiling shared by immutable catalog pages.
pub const PAGE_BYTES: usize = 256 * 1024;
pub(super) const MAX_HANDLES: usize = 8;
pub(super) const HANDLE_BYTES: usize = 8 * 1024 * 1024;
pub(super) const CAPTURE_BYTES: usize = 2 * 1024 * 1024;
pub(super) const SCAN_BYTES: usize = 32 * 1024 * 1024;
pub(super) const MAX_ITEMS: usize = 10_000;
pub(super) const ENTRY_BYTES: usize = 512 * 1024;

/// Conservative charge for one native-encoded row and its typed/agent envelopes.
pub(super) fn response_row_bytes(encoded: usize) -> usize {
    encoded.saturating_mul(2).saturating_add(1024)
}

/// One aggregate catalog entry/byte/time admission across capture and reference expansion.
pub(super) struct CaptureAdmission<'a> {
    pub cancellation: &'a super::record_read::QueryCancellation,
    pub started: Instant,
    pub scan_budget: usize,
    pub scanned: &'a mut usize,
    pub remaining: &'a mut usize,
}

impl CaptureAdmission<'_> {
    pub(super) fn entry(&mut self, bytes: usize) -> Result<(), Error> {
        if bytes > ENTRY_BYTES {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "catalog capture exhausted entry/scan/time admission; no result published",
            ));
        }
        self.visit()?;
        super::Writer::charge_catalog(self.remaining, bytes)
    }

    /// Count one catalog entry against the scan budget and lease time only; the
    /// caller charges its bytes through a streaming decoder's own admission.
    pub(super) fn visit(&mut self) -> Result<(), Error> {
        self.cancellation.checkpoint()?;
        if *self.scanned == self.scan_budget
            || self.started.elapsed() >= Duration::from_millis(LEASE_MS)
        {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "catalog capture exhausted entry/scan/time admission; no result published",
            ));
        }
        *self.scanned += 1;
        Ok(())
    }
}

pub(super) trait Selector: Clone + PartialEq {
    fn capacity(&self) -> usize;
}

pub(super) trait PageBody: Sized {
    const PAGE_ITEMS: usize;
    fn len(&self) -> usize;
    fn capacity(&self) -> usize;
    fn page(&self, start: usize, end: usize) -> Result<Self, Error>;
    fn page_end(&self, start: usize, page_size: usize) -> Result<usize, Error> {
        Ok(self.len().min(start + page_size.min(Self::PAGE_ITEMS)))
    }
    fn is_boundary(&self, start: usize, page_size: usize) -> Result<bool, Error> {
        Ok(start.is_multiple_of(page_size.min(Self::PAGE_ITEMS)))
    }
    /// Rows delivered by earlier pages of a continued read.
    fn offset(&self) -> usize {
        0
    }
    /// Whether the reader continues past this body's last row.
    fn continues(&self) -> bool {
        false
    }
}

/// Complete immutable metadata capture with bounded pagination.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogReadPage<B> {
    /// Persistent store identity.
    pub store_id: String,
    /// Opaque identity of this capture.
    pub snapshot_id: String,
    /// Eligibility clock, in Unix milliseconds.
    pub as_of: u64,
    /// Absolute deadline, never renewed by reading pages.
    pub lease_expires_at: u64,
    /// Full captured result size; for a continued read, rows delivered through this page.
    pub total_items: usize,
    /// Zero-based first returned position.
    pub page_start: usize,
    /// Continuation bound to the original operation, selector and limits.
    pub next_cursor: Option<String>,
    /// Requested item ceiling.
    pub page_size: usize,
    /// Original capture admission ceiling.
    pub scan_budget: usize,
    /// Catalog entries inspected during capture.
    pub scanned: usize,
    /// Set on a page that answers before its capture's build has finished: the page has no
    /// rows, `next_cursor` resumes the same build, and this is how far it has read (#158).
    pub build: Option<super::listing_build::BuildProgress>,
    /// Captured rows and metadata.
    pub body: B,
}

impl<B> CatalogReadPage<B> {
    /// Transform only the body, preserving the original immutable lease receipt.
    pub(crate) fn try_map_body<C>(
        self,
        map: impl FnOnce(B) -> Result<C, Error>,
    ) -> Result<CatalogReadPage<C>, Error> {
        Ok(CatalogReadPage {
            store_id: self.store_id,
            snapshot_id: self.snapshot_id,
            as_of: self.as_of,
            lease_expires_at: self.lease_expires_at,
            total_items: self.total_items,
            page_start: self.page_start,
            next_cursor: self.next_cursor,
            page_size: self.page_size,
            scan_budget: self.scan_budget,
            scanned: self.scanned,
            build: self.build,
            body: map(self.body)?,
        })
    }
}

#[derive(Clone)]
pub(super) struct Snapshot<S, B> {
    pub bytes: usize,
    pub id: String,
    pub store_id: String,
    pub selector: S,
    pub as_of: u64,
    pub expires_at: u64,
    pub started: Instant,
    pub page_size: usize,
    pub scan_budget: usize,
    pub scanned: usize,
    pub body: B,
}

impl<S: Selector, B: PageBody> Snapshot<S, B> {
    pub(super) fn capacity(&self) -> usize {
        512 + size_of::<Self>()
            + self.id.capacity() * 2
            + self.store_id.capacity()
            + self.selector.capacity()
            + self.body.capacity()
    }
    /// Refuse a cursor presented with another operation, selector or limits.
    pub(super) fn bind(
        &self,
        selector: &S,
        page_size: usize,
        scan_budget: usize,
    ) -> Result<(), Error> {
        if &self.selector != selector
            || self.page_size != page_size
            || self.scan_budget != scan_budget
        {
            return Err(Error::new(
                "CONFLICT",
                "cursor requires its original operation, selector and limits",
            ));
        }
        Ok(())
    }
    pub(super) fn alive(&self, now: u64) -> bool {
        now < self.expires_at && self.started.elapsed() < Duration::from_millis(LEASE_MS)
    }
    fn resume(&self, start: usize, now: u64) -> Result<CatalogReadPage<B>, Error> {
        if !self.alive(now) {
            return Err(Error::new(
                "RESULT_EXPIRED",
                "catalog read lease ended before page staging",
            ));
        }
        if start == 0
            || start >= self.body.len()
            || !self.body.is_boundary(start, self.page_size)?
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "cursor is not a captured page boundary",
            ));
        }
        let page = self.page(start)?;
        if !self.alive(now) {
            return Err(Error::new(
                "RESULT_EXPIRED",
                "catalog read lease ended during page staging",
            ));
        }
        Ok(page)
    }
    fn page(&self, start: usize) -> Result<CatalogReadPage<B>, Error> {
        let end = self.body.page_end(start, self.page_size)?;
        Ok(CatalogReadPage {
            store_id: self.store_id.clone(),
            snapshot_id: self.id.clone(),
            as_of: self.as_of,
            lease_expires_at: self.expires_at,
            total_items: self.body.offset() + self.body.len(),
            page_start: self.body.offset() + start,
            next_cursor: (end < self.body.len() || self.body.continues())
                .then(|| format!("{}/{end}", self.id)),
            page_size: self.page_size,
            scan_budget: self.scan_budget,
            scanned: self.scanned,
            build: None,
            body: self.body.page(start, end)?,
        })
    }
}

/// Live handles and their bytes at `now`, from a registry's ledger. The ledger's lock is
/// held only to copy a few rows, never while a page reads, so `status` answers while a
/// listing waits for a stalled writer with its registry locked (#158).
pub(super) fn read_stats(ledger: &Ledger, now: u64) -> (usize, usize) {
    let rows = lock_whole(ledger);
    let lease = Duration::from_millis(LEASE_MS);
    rows.values()
        .filter(|row| now < row.expires_at && row.started.elapsed() < lease)
        .fold((0, 0), |(handles, bytes), row| {
            (handles + 1, bytes + row.bytes)
        })
}

/// Lock a value every change replaces whole, such as a ledger, which each change rewrites
/// from its registry, or a listing build's state: a panic part-way through one leaves
/// nothing the next change does not replace, so poison carries no meaning.
pub(super) fn lock_whole<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One published handle's lease and size, as [`read_stats`] counts it.
pub(super) struct LedgerRow {
    expires_at: u64,
    started: Instant,
    bytes: usize,
}

/// A registry's handles, mirrored under their own short-held lock.
pub(super) type Ledger = std::sync::Arc<std::sync::Mutex<BTreeMap<String, LedgerRow>>>;

pub(super) struct Cursor<'a> {
    id: &'a str,
    start: usize,
}
impl<'a> Cursor<'a> {
    pub(super) fn parse(cursor: &'a str) -> Result<Self, Error> {
        let (id, index) = cursor
            .split_once('/')
            .filter(|_| cursor.len() <= 160)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "invalid catalog page cursor"))?;
        if index.is_empty() || !index.as_bytes().iter().all(u8::is_ascii_digit) {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "invalid catalog page position",
            ));
        }
        let start: usize = index
            .parse()
            .map_err(|_| Error::new("INVALID_ARGUMENT", "invalid catalog page position"))?;
        Ok(Self { id, start })
    }

    /// The handle or build the cursor names.
    pub(super) fn id(&self) -> &'a str {
        self.id
    }

    /// The position the cursor continues from.
    pub(super) fn start(&self) -> usize {
        self.start
    }
}

pub(super) struct Reads<S, B> {
    snapshots: BTreeMap<String, Snapshot<S, B>>,
    ledger: Ledger,
}

impl<S, B> Default for Reads<S, B> {
    fn default() -> Self {
        Self {
            snapshots: BTreeMap::new(),
            ledger: Ledger::default(),
        }
    }
}

impl<S, B> Reads<S, B> {
    /// The ledger `status` reads instead of locking this registry.
    pub(super) fn ledger(&self) -> Ledger {
        std::sync::Arc::clone(&self.ledger)
    }

    /// Mirror the handle set into the ledger after a change.
    fn mirror(&self) {
        let mut rows = lock_whole(&self.ledger);
        rows.clear();
        rows.extend(self.snapshots.iter().map(|(id, snapshot)| {
            (
                id.clone(),
                LedgerRow {
                    expires_at: snapshot.expires_at,
                    started: snapshot.started,
                    bytes: snapshot.bytes,
                },
            )
        }));
    }
}

impl<S: Selector, B: PageBody> Reads<S, B> {
    pub(super) fn release(&mut self, id: &str, now: u64) -> bool {
        self.purge(now);
        let released = self.snapshots.remove(id).is_some();
        self.mirror();
        released
    }

    pub(super) fn purge(&mut self, now: u64) {
        self.snapshots.retain(|_, value| value.alive(now));
        self.mirror();
    }

    #[cfg(test)]
    pub(super) fn insert_for_tests(&mut self, snapshot: Snapshot<S, B>) {
        self.snapshots.insert(snapshot.id.clone(), snapshot);
        self.mirror();
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }
    pub(super) fn capacity(&self) -> usize {
        self.snapshots.values().map(|snapshot| snapshot.bytes).sum()
    }
    pub(super) fn resume(
        &self,
        selector: &S,
        cursor: &str,
        page_size: usize,
        scan_budget: usize,
        now: u64,
    ) -> Result<CatalogReadPage<B>, Error> {
        let cursor = Cursor::parse(cursor)?;
        let snapshot = self.snapshot(&cursor)?;
        snapshot.bind(selector, page_size, scan_budget)?;
        snapshot.resume(cursor.start, now)
    }

    /// The live handle a cursor continues past, when its reader reads further rows.
    pub(super) fn continuation(
        &self,
        cursor: &Cursor<'_>,
        now: u64,
    ) -> Result<Option<&Snapshot<S, B>>, Error> {
        let Some(snapshot) = self.snapshots.get(cursor.id) else {
            return Ok(None);
        };
        if cursor.start != snapshot.body.len() || !snapshot.body.continues() {
            return Ok(None);
        }
        if !snapshot.alive(now) {
            return Err(Error::new(
                "RESULT_EXPIRED",
                "catalog read lease ended before its continuation",
            ));
        }
        Ok(Some(snapshot))
    }
    fn snapshot(&self, cursor: &Cursor<'_>) -> Result<&Snapshot<S, B>, Error> {
        self.snapshots.get(cursor.id).ok_or_else(|| {
            Error::new(
                "RESULT_EXPIRED",
                "catalog read handle is expired, unknown, or from a previous store owner",
            )
        })
    }
    pub(super) fn resume_captured(
        &self,
        cursor: &Cursor<'_>,
        now: u64,
    ) -> Result<Option<(&S, CatalogReadPage<B>)>, Error> {
        self.snapshots
            .get(cursor.id)
            .map(|snapshot| Ok((&snapshot.selector, snapshot.resume(cursor.start, now)?)))
            .transpose()
    }
    pub(super) fn publish(
        &mut self,
        mut snapshot: Snapshot<S, B>,
        now: u64,
        max_capture_bytes: usize,
        max_handle_bytes: usize,
        max_handles: usize,
        cancellation: &super::record_read::QueryCancellation,
    ) -> Result<CatalogReadPage<B>, Error> {
        cancellation.checkpoint()?;
        snapshot.bytes = snapshot.capacity();
        if !snapshot.alive(now) {
            return Err(Error::new(
                "RESULT_EXPIRED",
                "catalog capture lease ended before publication",
            ));
        }
        if snapshot.bytes > max_capture_bytes || self.capacity() + snapshot.bytes > max_handle_bytes
        {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "catalog metadata snapshot capacity is exhausted",
            ));
        }
        let page = snapshot.page(0)?;
        cancellation.checkpoint()?;
        if !snapshot.alive(now) {
            return Err(Error::new(
                "RESULT_EXPIRED",
                "catalog read lease ended during page staging",
            ));
        }
        if page.next_cursor.is_some() {
            if self.snapshots.len() >= max_handles {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "catalog metadata handle count is exhausted",
                ));
            }
            self.snapshots.insert(snapshot.id.clone(), snapshot);
            self.mirror();
        }
        if let Err(error) = cancellation.checkpoint() {
            self.snapshots.remove(&page.snapshot_id);
            self.mirror();
            return Err(error);
        }
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct CancelDuringPage(super::super::record_read::QueryCancellation);
    impl PageBody for CancelDuringPage {
        const PAGE_ITEMS: usize = 1;
        fn len(&self) -> usize {
            2
        }
        fn capacity(&self) -> usize {
            64
        }
        fn page(&self, _: usize, _: usize) -> Result<Self, Error> {
            self.0.cancel();
            Ok(Self(self.0.clone()))
        }
    }

    #[test]
    fn cancellation_during_page_staging_does_not_publish_a_catalogue_lease() {
        let cancellation = super::super::record_read::QueryCancellation::default();
        let mut reads =
            Reads::<super::super::source_read::SourceSelector, CancelDuringPage>::default();
        let snapshot = Snapshot {
            bytes: 0,
            id: "source-read:staging".into(),
            store_id: "store:test".into(),
            selector: Default::default(),
            as_of: 1,
            expires_at: LEASE_MS + 1,
            started: Instant::now(),
            page_size: 1,
            scan_budget: 10,
            scanned: 2,
            body: CancelDuringPage(cancellation.clone()),
        };
        let error = reads
            .publish(
                snapshot,
                1,
                CAPTURE_BYTES,
                HANDLE_BYTES,
                MAX_HANDLES,
                &cancellation,
            )
            .err()
            .unwrap();
        assert_eq!(error.code, "CANCELLED");
        assert_eq!(reads.capacity(), 0);
        assert!(reads.is_empty());
    }
}
