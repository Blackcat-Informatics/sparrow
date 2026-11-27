// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Admitted logical retention accounting; no physical-recovery inference.

use super::record_read::QueryCancellation;
use super::store_counters::{self, Counter, Positions, Reader};
use super::{Error, Record, Store, Writer, hex, manifest_retire, owner_summary};
use std::collections::{BTreeMap, BTreeSet};

/// Limits for one all-or-error catalog accounting operation.
#[derive(Clone, Copy, Debug)]
pub struct RetentionReadLimits {
    /// Catalog rows and payload ownership references visited, at most 100,000. While the
    /// owner summary is complete a visit is one owner summary row or one distinct digest an
    /// owner holds; until then it is one owner entry or one chunk occurrence.
    pub max_visits: usize,
    /// Aggregate catalog/map construction admission, at most 32 MiB.
    pub catalog_bytes: usize,
    /// Aggregate raw payload bytes verified, at most 1 GiB.
    pub payload_bytes: u64,
}

impl Default for RetentionReadLimits {
    fn default() -> Self {
        Self {
            max_visits: 10_000,
            catalog_bytes: 32 * 1024 * 1024,
            payload_bytes: 64 * 1024 * 1024,
        }
    }
}

/// A complete bounded logical account at one serialized membership instant.
/// Payload byte fields count unique raw chunks; indexes and physical files are excluded.
#[derive(Clone, Debug, Default)]
pub struct RetentionMetrics {
    /// Persistent store identity.
    pub store_id: String,
    /// Server UTC milliseconds used for eligibility.
    pub as_of: u64,
    /// Eligible record occurrences, including records retained beyond ordinary expiry by pins.
    pub live_records: u64,
    /// Eligible complete artifact occurrences, including empty inputs.
    pub live_artifacts: u64,
    /// Eligible record occurrences covered by active pins.
    pub pinned_records: u64,
    /// Eligible complete artifact occurrences covered by whole-artifact pins.
    pub pinned_artifacts: u64,
    /// Oldest eligible occurrence receipt clock, absent exactly when the live catalogs are empty.
    pub oldest_received_at: Option<u64>,
    /// Elapsed milliseconds from the oldest eligible receipt to as_of; zero for an empty store.
    pub retained_window_ms: u64,
    /// Total unique raw bytes with native ownership references, including private preparation.
    pub owned_payload_bytes: u64,
    /// Unique raw bytes referenced by any eligible artifact or record.
    pub live_payload_bytes: u64,
    /// Unique raw bytes referenced by any actively pinned eligible artifact or record.
    /// This overlaps live_payload_bytes, even when ordinary retention also applies.
    pub pin_held_payload_bytes: u64,
    /// Unique raw bytes exclusively owned by ineligible published artifacts/records.
    /// Logical eligibility for retirement does not certify physical disk reclamation.
    pub retirable_payload_bytes: u64,
    /// Unique raw bytes with references outside the published artifact/record catalogs.
    /// Private preparation can share these chunks with live evidence.
    pub unclassified_payload_bytes: u64,
    /// Unique referenced payload chunks whose native content was verified.
    pub payload_chunks: u64,
    /// Catalog rows, ownership references and admitted terminal pin seeks.
    pub visits: u64,
}

#[derive(Default)]
struct Ownership {
    published: u64,
    live: bool,
    pinned: bool,
}

/// Catalog bytes charged for each distinct digest the entry walk's ownership map holds: a
/// worst-case bound, not a typical size. The map is a `BTreeMap<String, Ownership>` whose keys
/// are 64-byte digests. Each non-root B-tree node holds at least 5 keys, and an internal node is
/// 552 B (allocated as 560 B), so a key costs at most 112 B of node; its heap key is a 64-byte
/// request, 80 B under glibc malloc. That is 192 B. The charge rounds it up to 256 B to cover
/// allocator rounding. The bound holds for a map of seven or more entries. A root node is
/// allocated whole, so a map of one or two entries can exceed its charge by up to 288 B in total.
/// That moves the refusal point only, never the answer.
const OWNER_ENTRY_CHARGE: usize = 256;

/// The admission state of one account: its visit count, catalog budget, payload budget and
/// cancellation. The frozen reference walk shares it, so visits are counted by one body.
pub(super) struct Admission<'a> {
    pub(super) limits: RetentionReadLimits,
    pub(super) visits: u64,
    pub(super) remaining: usize,
    pub(super) payload: u64,
    pub(super) cancellation: &'a QueryCancellation,
}

impl Admission<'_> {
    /// Count one catalog row or ownership reference, or fail on cancellation or `max_visits`.
    pub(super) fn visit(&mut self) -> Result<(), Error> {
        self.cancellation.checkpoint()?;
        if self.visits >= self.limits.max_visits as u64 {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "retention metrics exceed max_visits; no account published",
            ));
        }
        self.visits += 1;
        Ok(())
    }

    /// Fold one occurrence of `digest` into the ownership map: one visit, the canonical
    /// identity check, the charge for a new digest, then the owner's counts and flags.
    fn owner(
        &mut self,
        map: &mut BTreeMap<String, Ownership>,
        digest: &str,
        live: bool,
        pinned: bool,
    ) -> Result<(), Error> {
        self.visit()?;
        if hex::decode_32_canonical(digest).is_none() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "retention ownership has a noncanonical payload identity",
            ));
        }
        if !map.contains_key(digest) {
            self.remaining = self
                .remaining
                .checked_sub(OWNER_ENTRY_CHARGE)
                .ok_or_else(|| {
                    Error::new(
                        "RESOURCE_LIMIT",
                        "retention ownership map exceeds catalog_bytes",
                    )
                })?;
        }
        let owner = map.entry(digest.to_owned()).or_default();
        owner.published = add(owner.published, 1)?;
        owner.live |= live;
        owner.pinned |= pinned;
        Ok(())
    }

    fn owners(
        &mut self,
        map: &mut BTreeMap<String, Ownership>,
        chunks: &[String],
        live: bool,
        pinned: bool,
    ) -> Result<(), Error> {
        for digest in chunks {
            self.owner(map, digest, live, pinned)?;
        }
        Ok(())
    }
}

impl Admission<'_> {
    /// Charge one owner summary row's decode against the catalog allowance and return the
    /// charge, which the caller refunds once the decoded row is dropped.
    fn summary_row(&mut self, bytes: &[u8]) -> Result<usize, Error> {
        let charge = bytes.len().saturating_mul(4).saturating_add(1024);
        self.remaining = self.remaining.checked_sub(charge).ok_or_else(|| {
            Error::new(
                "RESOURCE_LIMIT",
                "retention owner summary exceeds catalog_bytes",
            )
        })?;
        Ok(charge)
    }

    /// Add one owner's remaining occurrences of each distinct digest to the ownership map,
    /// visiting each distinct digest once.
    fn occurrences(
        &mut self,
        map: &mut BTreeMap<String, Ownership>,
        chunks: &BTreeMap<[u8; 32], u64>,
        live: bool,
        pinned: bool,
    ) -> Result<(), Error> {
        for (digest, count) in chunks {
            self.visit()?;
            let digest = hex::encode(digest);
            if !map.contains_key(&digest) {
                self.remaining = self.remaining.checked_sub(512).ok_or_else(|| {
                    Error::new(
                        "RESOURCE_LIMIT",
                        "retention ownership map exceeds catalog_bytes",
                    )
                })?;
            }
            let owner = map.entry(digest).or_default();
            owner.published = add(owner.published, *count)?;
            owner.live |= live;
            owner.pinned |= pinned;
        }
        Ok(())
    }
}

/// Whether the owner summary rows may answer: the summary migration is complete and the
/// store counters are seeded, so the rows can be checked against the entry counts.
fn summaries_ready(writer: &Writer) -> Result<bool, Error> {
    let reader = Reader::db(&writer.db);
    let positions = Positions::read(|key| reader.get(key.as_bytes()))?;
    Ok(positions.seeded() && owner_summary::complete(|key| reader.get(key.as_bytes()))?)
}

/// Checked accounting addition; overflow is a storage failure, never a wrapped total.
pub(super) fn add(left: u64, right: u64) -> Result<u64, Error> {
    left.checked_add(right)
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "retention accounting overflow"))
}

impl RetentionMetrics {
    /// Fold one eligible receipt into the oldest receipt; a receipt later than `as_of` fails.
    pub(super) fn observe(&mut self, received: u64) -> Result<(), Error> {
        if received > self.as_of {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "retained receipt clock is later than the accounting clock",
            ));
        }
        self.oldest_received_at = Some(
            self.oldest_received_at
                .map_or(received, |oldest| oldest.min(received)),
        );
        Ok(())
    }
}

/// Account every owner from its summary row: one visit for the row and one for each distinct
/// digest it owns, checked against the entry counts the store counters hold.
fn summarised_owners(
    writer: &Writer,
    now: u64,
    protected: &BTreeSet<String>,
    admission: &mut Admission<'_>,
    owners: &mut BTreeMap<String, Ownership>,
    report: &mut RetentionMetrics,
) -> Result<(), Error> {
    let mut rows = [0_u64; 2];
    for (kind, prefix) in [owner_summary::ARTIFACT_PREFIX, owner_summary::RECORD_PREFIX]
        .into_iter()
        .enumerate()
    {
        let mut cursor = writer.db.raw_iterator();
        cursor.seek(prefix);
        while cursor.valid() {
            let (Some(key), Some(bytes)) = (cursor.key(), cursor.value()) else {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "retention summary cursor lost its row",
                ));
            };
            if !key.starts_with(prefix) {
                break;
            }
            admission.visit()?;
            let charge = admission.summary_row(bytes)?;
            let summary = owner_summary::Summary::decode(key, bytes)?;
            rows[kind] += 1;
            let id = std::str::from_utf8(&key[owner_summary::PREFIX.len()..])
                .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
            if summary.expires_at <= summary.received_at {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "retention owner deadline contradicts its receipt",
                ));
            }
            let (live, pinned) = if kind == 0 {
                (
                    Writer::artifact_eligible(
                        id,
                        summary.evicted,
                        summary.expires_at,
                        now,
                        protected,
                    ),
                    protected.contains(id),
                )
            } else {
                (
                    super::record_eligible(
                        id,
                        &summary.parent,
                        summary.evicted,
                        summary.expires_at,
                        now,
                        protected,
                    ),
                    protected.contains(id) || protected.contains(&summary.parent),
                )
            };
            if live {
                let (total, held) = if kind == 0 {
                    (&mut report.live_artifacts, &mut report.pinned_artifacts)
                } else {
                    (&mut report.live_records, &mut report.pinned_records)
                };
                *total = add(*total, 1)?;
                *held = add(*held, u64::from(pinned))?;
                report.observe(summary.received_at)?;
            }
            admission.occurrences(owners, &summary.chunks, live, live && pinned)?;
            // The decoded row is dropped here; the ownership entries added above keep theirs.
            admission.remaining += charge;
            cursor.next();
        }
        cursor.status()?;
    }
    let reader = Reader::db(&writer.db);
    for (counter, rows) in [(Counter::Artifacts, rows[0]), (Counter::Records, rows[1])] {
        if store_counters::stored(&reader, counter)? != rows {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "retention owner summary rows disagree with the catalog entry counts",
            ));
        }
    }
    Ok(())
}

impl Store {
    /// Compute complete logical retention metrics without advancing intake or maintenance.
    /// Real capacity, cancellation and integrity failures publish no partial account.
    pub fn retention_metrics(
        &self,
        now: u64,
        limits: RetentionReadLimits,
    ) -> Result<RetentionMetrics, Error> {
        self.retention_metrics_cancellable(now, limits, &QueryCancellation::default())
    }

    /// The same accounting with cooperative cancellation before every bounded work unit.
    pub fn retention_metrics_cancellable(
        &self,
        now: u64,
        limits: RetentionReadLimits,
        cancellation: &QueryCancellation,
    ) -> Result<RetentionMetrics, Error> {
        if limits.max_visits == 0
            || limits.max_visits > 100_000
            || limits.catalog_bytes == 0
            || limits.catalog_bytes > 32 * 1024 * 1024
            || limits.payload_bytes == 0
            || limits.payload_bytes > 1024 * 1024 * 1024
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "retention metrics require max_visits 1..100000, catalog_bytes 1..33554432 and payload_bytes 1..1073741824",
            ));
        }
        crate::format_instant(now)?;
        cancellation.checkpoint()?;
        let writer = self.lock()?;
        let mut admission = Admission {
            limits,
            visits: 0,
            remaining: limits.catalog_bytes,
            payload: limits.payload_bytes,
            cancellation,
        };
        let mut pin_visits = 0_u64;
        let protected = writer.protected_matching(
            now,
            |_| true,
            || {
                cancellation.checkpoint()?;
                if pin_visits >= limits.max_visits as u64 {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "retention pin scan exceeds max_visits",
                    ));
                }
                pin_visits += 1;
                Ok(())
            },
            Some(1024 * 1024),
            Some(&mut admission.remaining),
        )?;
        // The protection owner's admission callback counts each pin row read.
        admission.visits = pin_visits;
        let mut report = RetentionMetrics {
            store_id: writer.id.clone(),
            as_of: now,
            ..Default::default()
        };
        let mut owners = BTreeMap::new();
        // Until the owner summary is complete, every owner entry is read: a summary that is
        // still being built is never read as if it were complete.
        let prefixes: &[&[u8]] = if summaries_ready(&writer)? {
            summarised_owners(
                &writer,
                now,
                &protected,
                &mut admission,
                &mut owners,
                &mut report,
            )?;
            &[b"ref:"]
        } else {
            &[b"artifact:", b"record:", b"ref:"]
        };
        for prefix in prefixes.iter().copied() {
            let mut cursor = writer.db.raw_iterator();
            cursor.seek(prefix);
            while cursor.valid() {
                let key = cursor.key().ok_or_else(|| {
                    Error::new("STORAGE_FAILURE", "retention catalog cursor lost its key")
                })?;
                if !key.starts_with(prefix) {
                    break;
                }
                if prefix != b"ref:" && (key.len() <= prefix.len() || key.len() > 128) {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "retention catalog requires a bounded evidence identity",
                    ));
                }
                admission.visit()?;
                let bytes = cursor.value().ok_or_else(|| {
                    Error::new("STORAGE_FAILURE", "retention catalog cursor lost its value")
                })?;
                let id = std::str::from_utf8(key)
                    .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
                match prefix {
                    b"artifact:" => {
                        let before_row = admission.remaining;
                        let artifact = manifest_retire::Row::read(
                            &writer.db,
                            id,
                            bytes,
                            &mut admission.remaining,
                            |_, _, _| Ok(()),
                        )?;
                        let pinned = protected.contains(&artifact.id);
                        if artifact.expires_at <= artifact.received_at {
                            return Err(Error::new(
                                "STORAGE_FAILURE",
                                "retention artifact deadline contradicts its receipt",
                            ));
                        }
                        let live = Writer::artifact_eligible(
                            &artifact.id,
                            artifact.evicted(),
                            artifact.expires_at,
                            now,
                            &protected,
                        );
                        if live {
                            report.live_artifacts = add(report.live_artifacts, 1)?;
                            report.pinned_artifacts =
                                add(report.pinned_artifacts, u64::from(pinned))?;
                            report.observe(artifact.received_at)?;
                        }
                        let row_charge = before_row - admission.remaining;
                        // The chunks are folded in a second pass over the same row: its scalars
                        // may follow its chunk array, so `live` is known only after the first.
                        artifact.fold(bytes, |digest, _| {
                            admission.owner(&mut owners, digest, live, live && pinned)
                        })?;
                        // The row is dropped here. Its read cost bounds the peak while it is
                        // held, so it is refunded; the ownership entries added above keep
                        // their own charge.
                        admission.remaining += row_charge;
                    }
                    b"record:" => {
                        let shape_before = admission.remaining;
                        Writer::charge_catalog_shape(&mut admission.remaining, bytes)?;
                        let record: Record = writer.decode_evidence(key, bytes)?;
                        // The transient parse charge, measured before ownership entries are added.
                        let shape_charge = shape_before - admission.remaining;
                        writer.validate_event_record(&record)?;
                        if record.expires_at <= record.received_at
                            || record.byte_end < record.byte_start
                            || !record.artifact_id.starts_with("artifact:")
                            || record.artifact_id.len() > 128
                        {
                            return Err(Error::new(
                                "STORAGE_FAILURE",
                                "retention record has invalid clocks, range or parent identity",
                            ));
                        }
                        let live = writer.live_record(&record, now, &protected);
                        let pinned = protected.contains(&record.id)
                            || protected.contains(&record.artifact_id);
                        if live {
                            report.live_records = add(report.live_records, 1)?;
                            report.pinned_records = add(report.pinned_records, u64::from(pinned))?;
                            report.observe(record.received_at)?;
                        }
                        admission.owners(&mut owners, &record.chunks, live, live && pinned)?;
                        // The decoded record is dropped here, so only its shape charge is released.
                        admission.remaining += shape_charge;
                    }
                    _ => {
                        let digest = id.strip_prefix("ref:").ok_or_else(|| {
                            Error::new("STORAGE_FAILURE", "retention reference prefix disagrees")
                        })?;
                        if hex::decode_32_canonical(digest).is_none() {
                            return Err(Error::new(
                                "STORAGE_FAILURE",
                                "retention reference has a noncanonical payload identity",
                            ));
                        }
                        let references = u64::from_be_bytes(bytes.try_into().map_err(|_| {
                            Error::new(
                                "STORAGE_FAILURE",
                                "invalid retention payload reference counter",
                            )
                        })?);
                        let owner = owners.remove(digest).unwrap_or_default();
                        if references == 0 || references < owner.published {
                            return Err(Error::new(
                                "STORAGE_FAILURE",
                                "retention payload ownership contradicts its native reference counter",
                            ));
                        }
                        cancellation.checkpoint()?;
                        // The one native chunk reader admits borrowed input before hashing
                        // and validates size/content before any caller-owned copy.
                        let payload =
                            writer.payload_chunk_view(digest, Some(&mut admission.payload))?;
                        let length = payload.len() as u64;
                        report.payload_chunks = add(report.payload_chunks, 1)?;
                        report.owned_payload_bytes = add(report.owned_payload_bytes, length)?;
                        if owner.live {
                            report.live_payload_bytes = add(report.live_payload_bytes, length)?;
                        }
                        if owner.pinned {
                            report.pin_held_payload_bytes =
                                add(report.pin_held_payload_bytes, length)?;
                        }
                        if references == owner.published && !owner.live {
                            report.retirable_payload_bytes =
                                add(report.retirable_payload_bytes, length)?;
                        }
                        if references > owner.published {
                            report.unclassified_payload_bytes =
                                add(report.unclassified_payload_bytes, length)?;
                        }
                    }
                }
                cursor.next();
            }
            cursor.status()?;
        }
        if !owners.is_empty() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "retention catalog references payloads without native ownership counters",
            ));
        }
        cancellation.checkpoint()?;
        report.retained_window_ms = report.oldest_received_at.map_or(0, |oldest| now - oldest);
        report.visits = admission.visits;
        Ok(report)
    }
}
