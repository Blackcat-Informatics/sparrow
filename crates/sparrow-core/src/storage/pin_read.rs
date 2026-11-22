// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Bounded immutable catalog captures. They retain pin metadata, never evidence.

use super::{Catalog, Error, Pin, Store, random_id};
use purrdf_lex::json;
use std::{
    collections::BTreeSet,
    mem::size_of,
    time::{Duration, Instant},
};

/// Conservative structured output ceiling used to bound page admission.
pub use super::catalog_lease::PAGE_BYTES;
use super::catalog_lease::{CAPTURE_BYTES as SNAPSHOT_BYTES, MAX_ITEMS, SCAN_BYTES};
pub(super) use super::catalog_lease::{HANDLE_BYTES, LEASE_MS, MAX_HANDLES};
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

impl super::catalog_lease::PageBody for PinPageBody {
    const PAGE_ITEMS: usize = PAGE_ITEMS;
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
    fn page(&self, start: usize, end: usize) -> Result<Self, Error> {
        Ok(match self {
            Self::Collections(rows) => Self::Collections(rows[start..end].to_vec()),
            Self::Members { pin, members } => Self::Members {
                pin: pin.clone(),
                members: members[start..end].to_vec(),
            },
        })
    }
}

/// Exact complete pin metadata capture with bounded pagination.
pub type PinReadPage = super::catalog_lease::CatalogReadPage<PinPageBody>;
type Snapshot = super::catalog_lease::Snapshot<PinSelector, PinPageBody>;
pub(super) type Reads = super::catalog_lease::Reads<PinSelector, PinPageBody>;

impl super::catalog_lease::Selector for PinSelector {
    fn capacity(&self) -> usize {
        match self {
            Self::Active => 0,
            Self::Id(s) | Self::Name(s) => s.capacity(),
        }
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
        self.pin_read_cancellable(query, now, &crate::QueryCancellation::default())
    }

    /// Resolve and page a collection with the same capture owner and caller lifetime.
    pub fn pin_read_cancellable(
        &self,
        query: &PinRead,
        now: u64,
        cancellation: &crate::QueryCancellation,
    ) -> Result<PinReadPage, Error> {
        validate(query)?;
        cancellation.checkpoint()?;
        let mut reads = self
            .pin_reads
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "pin read registry poisoned"))?;
        reads.purge(now);
        cancellation.checkpoint()?;
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
            .checked_add(LEASE_MS)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "pin read deadline overflow"))?;
        crate::format_instant(now)?;
        crate::format_instant(expires_at)?;
        let writer = self.lock()?;
        cancellation.checkpoint()?;
        let mut scanned = 0;
        let mut inspected_bytes = 0_usize;
        let mut selected = None;
        let mut active_names = BTreeSet::new();
        let mut summaries = Vec::new();
        let mut summary_capacity = 4096;
        let mut admit = |key: &[u8], bytes: &[u8]| -> Result<Pin, Error> {
            cancellation.checkpoint()?;
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
                members: pin
                    .members
                    .into_iter()
                    .map(|member| {
                        cancellation.checkpoint()?;
                        Ok(member)
                    })
                    .collect::<Result<_, Error>>()?,
            }
        };
        let snapshot = Snapshot {
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
        reads.publish(
            snapshot,
            now,
            SNAPSHOT_BYTES,
            HANDLE_BYTES,
            MAX_HANDLES,
            cancellation,
        )
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
        reads.insert_for_tests(snapshot);
        assert!(reads.capacity() > 0);
        reads.purge(0);
        assert!(reads.is_empty());
        assert_eq!(reads.capacity(), 0);
    }
}
