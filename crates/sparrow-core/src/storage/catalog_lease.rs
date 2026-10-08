// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

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
    pub started: Instant,
    pub scan_budget: usize,
    pub scanned: &'a mut usize,
    pub remaining: &'a mut usize,
}

impl CaptureAdmission<'_> {
    pub(super) fn entry(&mut self, bytes: usize) -> Result<(), Error> {
        if *self.scanned == self.scan_budget
            || bytes > ENTRY_BYTES
            || self.started.elapsed() >= Duration::from_millis(LEASE_MS)
        {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "catalog capture exhausted entry/scan/time admission; no result published",
            ));
        }
        *self.scanned += 1;
        super::Writer::charge_catalog(self.remaining, bytes)
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
    fn page_end(&self, start: usize, page_size: usize) -> usize {
        self.len().min(start + page_size.min(Self::PAGE_ITEMS))
    }
    fn is_boundary(&self, start: usize, page_size: usize) -> bool {
        start.is_multiple_of(page_size.min(Self::PAGE_ITEMS))
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
    /// Full captured result size.
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
    /// Captured rows and metadata.
    pub body: B,
}

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
        if start == 0 || start >= self.body.len() || !self.body.is_boundary(start, self.page_size) {
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
        let end = self.body.page_end(start, self.page_size);
        Ok(CatalogReadPage {
            store_id: self.store_id.clone(),
            snapshot_id: self.id.clone(),
            as_of: self.as_of,
            lease_expires_at: self.expires_at,
            total_items: self.body.len(),
            page_start: start,
            next_cursor: (end < self.body.len()).then(|| format!("{}/{end}", self.id)),
            page_size: self.page_size,
            scan_budget: self.scan_budget,
            scanned: self.scanned,
            body: self.body.page(start, end)?,
        })
    }
}

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
}

pub(super) struct Reads<S, B> {
    pub snapshots: BTreeMap<String, Snapshot<S, B>>,
}

impl<S, B> Default for Reads<S, B> {
    fn default() -> Self {
        Self {
            snapshots: BTreeMap::new(),
        }
    }
}

impl<S: Selector, B: PageBody> Reads<S, B> {
    pub(super) fn release(&mut self, id: &str, now: u64) -> bool {
        self.purge(now);
        self.snapshots.remove(id).is_some()
    }

    pub(super) fn purge(&mut self, now: u64) {
        self.snapshots.retain(|_, value| value.alive(now));
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
        if &snapshot.selector != selector
            || snapshot.page_size != page_size
            || snapshot.scan_budget != scan_budget
        {
            return Err(Error::new(
                "CONFLICT",
                "cursor requires its original operation, selector and limits",
            ));
        }
        snapshot.resume(cursor.start, now)
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
    ) -> Result<CatalogReadPage<B>, Error> {
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
        }
        Ok(page)
    }
}
