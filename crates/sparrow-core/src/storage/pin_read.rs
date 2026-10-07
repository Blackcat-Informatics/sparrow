// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Bounded immutable catalog captures. They retain pin metadata, never evidence.

use super::{Catalog, Error, Pin, Store, random_id};
use purrdf_lex::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
    time::{Duration, Instant},
};

/// Maximum live pin metadata handles shared by every clone of a store owner.
pub(super) const MAX_HANDLES: usize = 8;
/// Maximum admitted retained catalog capacity across all pin read handles.
pub(super) const HANDLE_BYTES: usize = 8 * 1024 * 1024;
/// Maximum admitted capacity of one captured pin result.
const SNAPSHOT_BYTES: usize = 2 * 1024 * 1024;
/// Absolute catalog read lease; page consumption never extends it.
pub(super) const LEASE_MS: u64 = 60_000;
/// Maximum original catalog bytes inspected by one capture.
const SCAN_BYTES: usize = 32 * 1024 * 1024;
/// Maximum items in a captured result or members in one readable collection.
const MAX_ITEMS: usize = 10_000;
/// Conservative structured output ceiling used to bound page admission.
pub const PAGE_BYTES: usize = 256 * 1024;
const PAGE_ITEMS: usize = (PAGE_BYTES - 4096) / 2048;

/// A store-local catalog read scope; names resolve once at capture time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PinSelector {
    /// All collections active at the captured clock instant.
    Active,
    /// One stable collection identity.
    Id(String),
    /// One case-sensitive active collection name.
    Name(String),
}

/// Immutable-page request; continuations must keep its selector and limits.
#[derive(Clone, Debug)]
pub struct PinRead {
    /// Captured operation and selector.
    pub selector: PinSelector,
    /// Opaque continuation from this same store owner, or a fresh capture.
    pub cursor: Option<String>,
    /// Requested upper bound, 1..=1000; serialized capacity can reduce it.
    pub page_size: usize,
    /// Catalog entry admission ceiling, 1..=100000; exhaustion refuses capture.
    pub scan_budget: usize,
}

impl Default for PinRead {
    fn default() -> Self {
        Self {
            selector: PinSelector::Active,
            cursor: None,
            page_size: 200,
            scan_budget: 10_000,
        }
    }
}

/// Collection metadata at the captured instant, independent of later mutations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PinSummary {
    /// Stable store-local collection identity.
    pub id: String,
    /// Case-sensitive collection name.
    pub name: String,
    /// Actual collection deadline in Unix milliseconds.
    pub expires_at: u64,
    /// Captured mutation revision.
    pub revision: u64,
    /// Number of direct distinct members, without artifact expansion.
    pub direct_member_count: usize,
}

impl PinSummary {
    fn of(pin: &Pin) -> Self {
        Self {
            id: pin.id.clone(),
            name: pin.name.clone(),
            expires_at: pin.expires_at,
            revision: pin.revision,
            direct_member_count: pin.members.len(),
        }
    }
    fn capacity(&self) -> usize {
        self.id.capacity() + self.name.capacity()
    }
}

/// A page of collection summaries or one collection's direct member references.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PinPageBody {
    /// Active collections in stable pin-ID order.
    Collections(Vec<PinSummary>),
    /// Direct member references in stable evidence-ID order.
    Members {
        /// The same captured collection summary on every page.
        pin: PinSummary,
        /// The selected page of member IDs.
        members: Vec<String>,
    },
}

impl PinPageBody {
    fn len(&self) -> usize {
        match self {
            Self::Collections(rows) => rows.len(),
            Self::Members { members, .. } => members.len(),
        }
    }
    fn capacity(&self) -> usize {
        match self {
            Self::Collections(rows) => {
                rows.capacity() * size_of::<PinSummary>()
                    + rows.iter().map(PinSummary::capacity).sum::<usize>()
            }
            Self::Members { pin, members } => {
                size_of::<PinSummary>()
                    + pin.capacity()
                    + members.capacity() * size_of::<String>()
                    + members.iter().map(String::capacity).sum::<usize>()
            }
        }
    }
    fn page(&self, start: usize, end: usize) -> Self {
        match self {
            Self::Collections(rows) => Self::Collections(rows[start..end].to_vec()),
            Self::Members { pin, members } => Self::Members {
                pin: pin.clone(),
                members: members[start..end].to_vec(),
            },
        }
    }
}

/// Exact complete catalog capture with bounded pagination, not a live pin scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PinReadPage {
    /// Persistent store identity.
    pub store_id: String,
    /// Opaque identity of this immutable metadata capture.
    pub snapshot_id: String,
    /// Eligibility clock captured once, in Unix milliseconds.
    pub as_of: u64,
    /// Absolute page lease deadline; never renewed by a read.
    pub lease_expires_at: u64,
    /// Full captured result size; pagination does not imply incomplete execution.
    pub total_items: usize,
    /// Zero-based position of the first returned item.
    pub page_start: usize,
    /// Opaque next-page cursor, absent when the captured result is exhausted.
    pub next_cursor: Option<String>,
    /// Requested item ceiling bound into the cursor.
    pub page_size: usize,
    /// Effective catalog scan admission ceiling bound into the cursor.
    pub scan_budget: usize,
    /// Number of catalog entries inspected to establish the complete capture.
    pub scanned: usize,
    /// Captured rows and metadata.
    pub body: PinPageBody,
}

struct Snapshot {
    bytes: usize,
    id: String,
    store_id: String,
    selector: PinSelector,
    as_of: u64,
    expires_at: u64,
    started: Instant,
    page_size: usize,
    scan_budget: usize,
    scanned: usize,
    body: PinPageBody,
}

impl Snapshot {
    fn capacity(&self) -> usize {
        // Include the map key, node allowance and selector as well as owned rows.
        512 + size_of::<Self>()
            + self.id.capacity() * 2
            + self.store_id.capacity()
            + match &self.selector {
                PinSelector::Active => 0,
                PinSelector::Id(s) | PinSelector::Name(s) => s.capacity(),
            }
            + self.body.capacity()
    }
    fn alive(&self, now: u64) -> bool {
        now < self.expires_at && self.started.elapsed() < Duration::from_millis(LEASE_MS)
    }
    fn page(&self, start: usize) -> PinReadPage {
        let end = self.body.len().min(start + self.page_size.min(PAGE_ITEMS));
        PinReadPage {
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
            body: self.body.page(start, end),
        }
    }
}

#[derive(Default)]
pub(super) struct Reads {
    snapshots: BTreeMap<String, Snapshot>,
}

impl Reads {
    fn purge(&mut self, now: u64) {
        self.snapshots.retain(|_, value| value.alive(now));
    }
    fn capacity(&self) -> usize {
        self.snapshots.values().map(|snapshot| snapshot.bytes).sum()
    }
}

fn validate(query: &PinRead) -> Result<(), Error> {
    if !(1..=1000).contains(&query.page_size) || !(1..=100000).contains(&query.scan_budget) {
        return Err(Error::new(
            "INVALID_ARGUMENT",
            "pin reads require page_size 1..1000 and scan_budget 1..100000",
        ));
    }
    match &query.selector {
        PinSelector::Active => {}
        PinSelector::Id(id) if id.starts_with("pin:") && id.len() > 4 && id.len() <= 128 => {}
        PinSelector::Name(name)
            if !name.is_empty() && name.len() <= 128 && !name.chars().any(char::is_control) => {}
        _ => {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "pin selector requires a bounded stable ID or case-sensitive name",
            ));
        }
    }
    Ok(())
}

fn decode(key: &[u8], bytes: &[u8]) -> Result<Pin, Error> {
    if bytes.len() > SNAPSHOT_BYTES {
        return Err(Error::new(
            "RESOURCE_LIMIT",
            "pin catalog entry exceeds bounded read capacity",
        ));
    }
    let value = json::read_slice(
        bytes,
        json::Limits {
            max_depth: 8,
            max_values: (MAX_ITEMS + 32) as u64,
            max_string_bytes: SNAPSHOT_BYTES,
            unique_members: true,
        },
    )
    .map_err(|error| Error::new("STORAGE_FAILURE", format!("pin catalog JSON: {error}")))?;
    let pin = Pin::decode(&value)?;
    if pin.id.as_bytes() != key
        || !pin.id.starts_with("pin:")
        || pin.id.len() <= 4
        || pin.id.len() > 128
        || pin.name.is_empty()
        || pin.name.len() > 128
        || pin.name.chars().any(char::is_control)
        || pin.revision == 0
        || pin.members.is_empty()
        || value["members"].as_array().map(|members| members.len()) != Some(pin.members.len())
        || pin.members.iter().any(|id| {
            id.len() > 128
                || id.chars().any(char::is_control)
                || !(id.starts_with("artifact:") || id.starts_with("record:"))
        })
    {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "pin catalog identity or membership invariant failed",
        ));
    }
    if pin.members.len() > MAX_ITEMS {
        return Err(Error::new(
            "RESOURCE_LIMIT",
            "pin membership exceeds bounded read capacity",
        ));
    }
    crate::format_instant(pin.expires_at)
        .map_err(|error| Error::new("STORAGE_FAILURE", format!("pin catalog deadline: {error}")))?;
    Ok(pin)
}

impl Store {
    /// Capture or continue immutable pin metadata under a finite shared handle budget.
    /// Reads neither renew protection nor hold database versions/evidence bytes alive.
    pub fn pin_read(&self, query: &PinRead, now: u64) -> Result<PinReadPage, Error> {
        validate(query)?;
        let mut reads = self
            .pin_reads
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "pin read registry poisoned"))?;
        reads.purge(now);
        if let Some(cursor) = &query.cursor {
            let (id, index) = cursor
                .split_once('/')
                .filter(|_| cursor.len() <= 160)
                .ok_or_else(|| Error::new("INVALID_ARGUMENT", "invalid pin page cursor"))?;
            if !index.is_empty() && index.as_bytes().iter().all(u8::is_ascii_digit) {
                let start: usize = index
                    .parse()
                    .map_err(|_| Error::new("INVALID_ARGUMENT", "invalid pin page position"))?;
                let snapshot = reads.snapshots.get(id).ok_or_else(|| {
                    Error::new(
                        "RESULT_EXPIRED",
                        "pin read handle is expired, unknown, or from a previous store owner",
                    )
                })?;
                if snapshot.selector != query.selector
                    || snapshot.page_size != query.page_size
                    || snapshot.scan_budget != query.scan_budget
                {
                    return Err(Error::new(
                        "CONFLICT",
                        "cursor requires its original operation, selector and limits",
                    ));
                }
                if start == 0
                    || start >= snapshot.body.len()
                    || !start.is_multiple_of(snapshot.page_size.min(PAGE_ITEMS))
                {
                    return Err(Error::new(
                        "INVALID_ARGUMENT",
                        "cursor is not a captured page boundary",
                    ));
                }
                let page = snapshot.page(start);
                if !snapshot.alive(now) {
                    return Err(Error::new(
                        "RESULT_EXPIRED",
                        "pin read lease ended during page staging",
                    ));
                }
                return Ok(page);
            }
            return Err(Error::new("INVALID_ARGUMENT", "invalid pin page position"));
        }
        let started = Instant::now();
        let expires_at = now
            .checked_add(LEASE_MS)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "pin read deadline overflow"))?;
        crate::format_instant(now)?;
        crate::format_instant(expires_at)?;
        let writer = self.lock()?;
        let mut scanned = 0;
        let mut inspected_bytes = 0_usize;
        let mut selected = None;
        let mut active_names = BTreeSet::new();
        let mut summaries = Vec::new();
        let mut summary_capacity = 4096;
        let mut admit = |key: &[u8], bytes: &[u8]| -> Result<Pin, Error> {
            if scanned >= query.scan_budget || started.elapsed() >= Duration::from_millis(LEASE_MS)
            {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "pin catalog capture exhausted its scan or time admission; no result published",
                ));
            }
            scanned += 1;
            inspected_bytes = inspected_bytes
                .checked_add(bytes.len())
                .ok_or_else(|| Error::new("RESOURCE_LIMIT", "pin capture byte overflow"))?;
            if inspected_bytes > SCAN_BYTES {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "pin catalog capture exceeded its original-byte admission; no result published",
                ));
            }
            decode(key, bytes)
        };
        if let PinSelector::Id(id) = &query.selector {
            let bytes = writer.db.get_pinned(id)?.ok_or_else(|| {
                Error::new(
                    "PIN_NOT_ACTIVE",
                    "collection is unknown, released, or reclaimed",
                )
            })?;
            selected = Some(admit(id.as_bytes(), &bytes)?);
        } else {
            let mut iterator = writer.db.raw_iterator();
            iterator.seek(b"pin:");
            while iterator.valid() {
                let key = iterator
                    .key()
                    .ok_or_else(|| Error::new("STORAGE_FAILURE", "pin iterator key missing"))?;
                if !key.starts_with(b"pin:") {
                    break;
                }
                let bytes = iterator
                    .value()
                    .ok_or_else(|| Error::new("STORAGE_FAILURE", "pin iterator value missing"))?;
                let pin = admit(key, bytes)?;
                iterator.next();
                if pin.expires_at <= now {
                    continue;
                }
                match &query.selector {
                    PinSelector::Active => {
                        if summaries.len() >= MAX_ITEMS {
                            return Err(Error::new(
                                "RESOURCE_LIMIT",
                                "pin result exceeds captured item limit",
                            ));
                        }
                        let summary = PinSummary::of(&pin);
                        if !active_names.insert(pin.name.clone()) {
                            return Err(Error::new(
                                "STORAGE_FAILURE",
                                "multiple active pin IDs share one name",
                            ));
                        }
                        if summaries.len() == summaries.capacity() {
                            let old_capacity = summaries.capacity();
                            let target = (old_capacity * 2).clamp(8, MAX_ITEMS);
                            let planned = summary_capacity
                                + (target - old_capacity) * size_of::<PinSummary>()
                                + summary.capacity();
                            if planned > SNAPSHOT_BYTES {
                                return Err(Error::new(
                                    "RESOURCE_LIMIT",
                                    "pin result exceeds captured capacity",
                                ));
                            }
                            summaries
                                .try_reserve_exact(target - summaries.len())
                                .map_err(|error| {
                                    Error::new(
                                        "RESOURCE_LIMIT",
                                        format!("pin result capacity admission: {error}"),
                                    )
                                })?;
                            summary_capacity +=
                                (summaries.capacity() - old_capacity) * size_of::<PinSummary>();
                        }
                        summary_capacity += summary.capacity();
                        if summary_capacity > SNAPSHOT_BYTES {
                            return Err(Error::new(
                                "RESOURCE_LIMIT",
                                "pin result exceeds captured capacity",
                            ));
                        }
                        summaries.push(summary);
                    }
                    PinSelector::Name(name) if pin.name == *name => {
                        if selected.is_some() {
                            return Err(Error::new(
                                "STORAGE_FAILURE",
                                "multiple active pin IDs share one name",
                            ));
                        }
                        selected = Some(pin);
                    }
                    _ => {}
                }
            }
            iterator.status()?;
        }
        let body = if query.selector == PinSelector::Active {
            PinPageBody::Collections(summaries)
        } else {
            let pin = selected.filter(|pin| pin.expires_at > now).ok_or_else(|| {
                Error::new(
                    "PIN_NOT_ACTIVE",
                    "collection is not active at the captured instant",
                )
            })?;
            let summary = PinSummary::of(&pin);
            PinPageBody::Members {
                pin: summary,
                members: pin.members.into_iter().collect(),
            }
        };
        let mut snapshot = Snapshot {
            bytes: 0,
            id: random_id("pin-read")?,
            store_id: writer.id.clone(),
            selector: query.selector.clone(),
            as_of: now,
            expires_at,
            started,
            page_size: query.page_size,
            scan_budget: query.scan_budget,
            scanned,
            body,
        };
        drop(writer);
        snapshot.bytes = snapshot.capacity();
        if !snapshot.alive(now) {
            return Err(Error::new(
                "RESULT_EXPIRED",
                "pin catalog capture lease ended before publication",
            ));
        }
        if snapshot.bytes > SNAPSHOT_BYTES || reads.capacity() + snapshot.bytes > HANDLE_BYTES {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "pin metadata snapshot capacity is exhausted",
            ));
        }
        let page = snapshot.page(0);
        if !snapshot.alive(now) {
            return Err(Error::new(
                "RESULT_EXPIRED",
                "pin read lease ended during page staging",
            ));
        }
        if page.next_cursor.is_some() {
            if reads.snapshots.len() >= MAX_HANDLES {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "pin metadata handle count is exhausted",
                ));
            }
            reads.snapshots.insert(snapshot.id.clone(), snapshot);
        }
        Ok(page)
    }

    pub(super) fn pin_read_stats(&self, now: u64) -> Result<(usize, usize), Error> {
        let mut reads = self
            .pin_reads
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "pin read registry poisoned"))?;
        reads.purge(now);
        Ok((reads.snapshots.len(), reads.capacity()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_expiry_reclaims_a_handle_despite_a_backward_wall_clock() {
        let mut snapshot = Snapshot {
            bytes: 0,
            id: "pin-read:test".into(),
            store_id: "store:test".into(),
            selector: PinSelector::Active,
            as_of: 1000,
            expires_at: 61000,
            started: Instant::now() - Duration::from_millis(LEASE_MS + 1),
            page_size: 1,
            scan_budget: 100,
            scanned: 0,
            body: PinPageBody::Collections(Vec::new()),
        };
        snapshot.bytes = snapshot.capacity();
        assert!(!snapshot.alive(0));
        let mut reads = Reads::default();
        reads.snapshots.insert(snapshot.id.clone(), snapshot);
        assert!(reads.capacity() > 0);
        reads.purge(0);
        assert!(reads.snapshots.is_empty());
        assert_eq!(reads.capacity(), 0);
    }
}
