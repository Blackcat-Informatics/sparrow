// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Admitted logical retention accounting; no physical-recovery inference.

use super::record_read::QueryCancellation;
use super::{Error, Record, Store, Writer, hex, manifest_retire};
use std::collections::BTreeMap;

/// Limits for one all-or-error catalog accounting operation.
#[derive(Clone, Copy, Debug)]
pub struct RetentionReadLimits {
    /// Catalog rows and payload ownership references visited, at most 100,000.
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

struct Admission<'a> {
    limits: RetentionReadLimits,
    visits: u64,
    remaining: usize,
    payload: u64,
    cancellation: &'a QueryCancellation,
}

impl Admission<'_> {
    fn visit(&mut self) -> Result<(), Error> {
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

    fn owners(
        &mut self,
        map: &mut BTreeMap<String, Ownership>,
        chunks: &[String],
        live: bool,
        pinned: bool,
    ) -> Result<(), Error> {
        for digest in chunks {
            self.visit()?;
            if hex::decode_32_canonical(digest).is_none() {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "retention ownership has a noncanonical payload identity",
                ));
            }
            if !map.contains_key(digest) {
                self.remaining = self.remaining.checked_sub(512).ok_or_else(|| {
                    Error::new(
                        "RESOURCE_LIMIT",
                        "retention ownership map exceeds catalog_bytes",
                    )
                })?;
            }
            let owner = map.entry(digest.clone()).or_default();
            owner.published = add(owner.published, 1)?;
            owner.live |= live;
            owner.pinned |= pinned;
        }
        Ok(())
    }
}

fn add(left: u64, right: u64) -> Result<u64, Error> {
    left.checked_add(right)
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "retention accounting overflow"))
}

impl RetentionMetrics {
    fn observe(&mut self, received: u64) -> Result<(), Error> {
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
        // The protection owner's admission callback also checks the terminal seek.
        admission.visits = pin_visits;
        let mut report = RetentionMetrics {
            store_id: writer.id.clone(),
            as_of: now,
            ..Default::default()
        };
        let mut owners = BTreeMap::new();
        for prefix in [b"artifact:".as_slice(), b"record:", b"ref:"] {
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
                        let artifact = manifest_retire::Envelope::read(
                            &writer,
                            id,
                            bytes,
                            limits.max_visits,
                            &mut admission.remaining,
                        )?;
                        if artifact.pending() != artifact.prefix.len() as u64 {
                            return Err(Error::new(
                                "RESOURCE_LIMIT",
                                "retention manifest ownership exceeds max_visits",
                            ));
                        }
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
                        admission.owners(&mut owners, &artifact.prefix, live, live && pinned)?;
                    }
                    b"record:" => {
                        Writer::charge_catalog_shape(&mut admission.remaining, bytes)?;
                        let record: Record = writer.decode_evidence(key, bytes)?;
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
