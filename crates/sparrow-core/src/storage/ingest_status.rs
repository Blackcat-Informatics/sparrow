// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One bounded read of ingestion acceptance, projection and source ownership.

use super::{
    Artifact, Catalog, Error, Record, Store, Writer, field, hex, import, publication, read_field,
};

/// Explicit request namespaces prevent collisions between upload and handoff IDs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngestSelector {
    /// Original upload or direct-ingest request ID.
    Request(String),
    /// Opaque import ID returned by native upload or consumption admission.
    Import(String),
    /// Original producer handoff request ID.
    Handoff(String),
}

/// Work admission for one status read; failures publish no partial summary.
#[derive(Clone, Copy, Debug)]
pub struct IngestReadLimits {
    /// Maximum records and pin collections visited, at most 100,000.
    pub scan_budget: usize,
    /// Aggregate native catalog construction admission, at most 32 MiB.
    pub catalog_bytes: usize,
}

impl Default for IngestReadLimits {
    fn default() -> Self {
        Self {
            scan_budget: 10_000,
            catalog_bytes: 32 * 1024 * 1024,
        }
    }
}

/// Counts refer to record occurrences associated with this publication attempt.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IngestIndexCounts {
    /// Successfully prepared native projections.
    pub complete: u64,
    /// Native projection failures.
    pub failed: u64,
    /// Explicitly incomplete native text preparation.
    pub incomplete: u64,
    /// This index does not apply to the record's declared representation.
    pub not_applicable: u64,
    /// Accepted records whose ordinary evidence lifetime ended.
    pub unavailable: u64,
    /// Known layout records not yet completely prepared.
    pub pending: u64,
}

impl IngestIndexCounts {
    fn observe(&mut self, state: &str, text: bool) -> Result<(), Error> {
        match state {
            "complete" => self.complete += 1,
            "failed" => self.failed += 1,
            "incomplete" if text => self.incomplete += 1,
            "not_applicable" if text => self.not_applicable += 1,
            _ => {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "unknown native ingestion projection state",
                ));
            }
        }
        Ok(())
    }
}

/// Preparation and acceptance are separate from synchronized filesystem deletion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngestDeletion {
    /// Ordinary uploads carry no source-deletion authority.
    NotManaged,
    /// A durable source claim was reserved, not acknowledged as claimed.
    Reserved,
    /// The original source was claimed, but publication is unfinished.
    Claimed,
    /// Original artifact or terminal tail checkpoint was published; removal is unconfirmed.
    Published,
    /// The ownership journal confirms directory-synchronized source removal.
    Removed,
    /// An administrator retired the claim as unrecoverable: claimed, its private source
    /// missing, never published. Nothing will publish or remove it.
    Retired,
}

/// One catalog membership instant; inspecting it never flushes or advances intake.
#[derive(Clone, Debug)]
pub struct IngestStatus {
    /// Original explicit namespace and selector, echoed independently of managed request resolution.
    pub selector: IngestSelector,
    /// Persistent store identity.
    pub store_id: String,
    /// Server status clock in Unix milliseconds.
    pub as_of: u64,
    /// Resolved native ingestion request ID (managed imports use their reserved namespace).
    pub request_id: String,
    /// Original admitted native import identity; direct ingestion has none.
    pub import_id: Option<String>,
    /// Raw upload checkpoint, absent for direct intake and tail-only completion.
    pub staging: Option<import::ImportManifest>,
    /// Native private preparation progress, absent outside unpublished preparation.
    pub preparation: Option<publication::ImportPreparation>,
    /// Original acceptance receipt with current-session buffered/durable state.
    pub accepted: Option<Artifact>,
    /// Original terminal tail checkpoint; acknowledging it creates no new records.
    pub tail: Option<super::tail::TailGeneration>,
    /// Authoritative source-retirement phase, separate from evidence acceptance.
    pub deletion: IngestDeletion,
    /// Original synchronized source-removal clock, only in the Removed phase.
    pub removed_at: Option<u64>,
    /// Original bounded source-ownership journal, absent for ordinary intake.
    pub consumption: Option<super::consumption::ConsumptionJournal>,
    /// Exact publication-attempt count once layout completes; unknown before then.
    pub total_records: Option<u64>,
    /// Records whose native state was inspected, including unavailable accepted records.
    pub inspected_records: u64,
    /// Native JSON search projection coverage.
    pub json: IngestIndexCounts,
    /// Native RDF content projection coverage.
    pub rdf: IngestIndexCounts,
    /// Native raw/decoded text projection coverage.
    pub text: IngestIndexCounts,
}

impl Store {
    /// Read original request state and bounded index coverage under one catalog lock.
    /// No append, publication, source removal, retention renewal or synchronization occurs.
    pub fn ingest_status(
        &self,
        selector: &IngestSelector,
        limits: IngestReadLimits,
        now: u64,
    ) -> Result<IngestStatus, Error> {
        if limits.scan_budget == 0
            || limits.scan_budget > 100_000
            || limits.catalog_bytes == 0
            || limits.catalog_bytes > 32 * 1024 * 1024
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "ingestion status requires scan_budget 1..100000 and catalog_bytes 1..33554432",
            ));
        }
        crate::format_instant(now)?;
        let writer = self.lock()?;
        let mut remaining = limits.catalog_bytes;
        let import_id = match selector {
            IngestSelector::Import(id) => {
                if id
                    .strip_prefix("import:")
                    .and_then(hex::decode_32_canonical)
                    .is_none()
                {
                    return Err(Error::new(
                        "INVALID_ARGUMENT",
                        "require a canonical native import ID",
                    ));
                }
                id.clone()
            }
            IngestSelector::Request(request) | IngestSelector::Handoff(request) => {
                if request.is_empty() || request.len() > 128 {
                    return Err(Error::new(
                        "INVALID_ARGUMENT",
                        "require a request ID of 1..128 bytes",
                    ));
                }
                if matches!(selector, IngestSelector::Handoff(_)) {
                    writer
                        .handoff_status_import(request, now, &mut remaining)?
                        .ok_or_else(|| {
                            Error::new("NOT_FOUND", "unknown or expired handoff request")
                        })?
                } else {
                    import::import_id(request)
                }
            }
        };
        let mut ownership = writer.consumption_admitted(&import_id, Some(&mut remaining))?;
        let mut retired = false;
        if ownership.is_none()
            && let Some(claim) = writer.consumption_retired_admitted(&import_id, &mut remaining)?
        {
            ownership = Some(claim.journal);
            retired = true;
        }
        let published =
            publication::published_admitted(&writer, &import_id, now, Some(&mut remaining))?;
        let mut staging = published.as_ref().map(|(manifest, _)| manifest.clone());
        if staging.is_none() {
            staging = writer
                .optional_catalog_value(&import_id, &mut remaining)?
                .as_ref()
                .map(import::ImportManifest::decode)
                .transpose()?;
            if let Some(manifest) = &staging {
                manifest.require_live(now)?;
            }
        }
        if let Some(manifest) = &staging
            && manifest.id != import_id
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "ingestion status import identity mismatch",
            ));
        }
        let request_id = match selector {
            IngestSelector::Request(request) => request.clone(),
            _ => staging
                .as_ref()
                .map(|m| m.request_id.clone())
                .or_else(|| ownership.as_ref().map(|j| j.request_id.clone()))
                .ok_or_else(|| Error::new("NOT_FOUND", "unknown or expired ingestion"))?,
        };
        if staging.as_ref().is_some_and(|m| m.request_id != request_id)
            || ownership
                .as_ref()
                .is_some_and(|j| j.request_id != request_id)
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "ingestion status request identity mismatch",
            ));
        }
        let direct = writer
            .retry_admitted(
                &format!("request:ingest:{request_id}"),
                now,
                Some(&mut remaining),
            )?
            .as_ref()
            .map(|saved| Artifact::decode(field(saved, "receipt")?))
            .transpose()?;
        if let (Some(direct), Some((_, staged))) = (&direct, &published)
            && direct.encode() != staged.encode()
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "ingestion status found conflicting original acceptance receipts",
            ));
        }
        let accepted = direct
            .or_else(|| published.map(|(_, artifact)| artifact))
            .map(|a| writer.receipt(a, super::durability::Durability::Buffered))
            .transpose()?;
        if let (Some(manifest), Some(artifact)) = (&staging, &accepted)
            && (manifest.published_artifact.as_ref() != Some(&artifact.id)
                || manifest.source != artifact.source
                || manifest.format != artifact.format
                || manifest.byte_length != artifact.byte_length
                || manifest.payload_id != artifact.payload_id
                || manifest.layout_records != artifact.record_ids.len() as u64)
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "ingestion acceptance contradicts original staging",
            ));
        }
        let preparation = match &staging {
            Some(manifest) if accepted.is_none() => {
                publication::preparation_admitted(&writer, manifest, Some(&mut remaining))?
            }
            _ => None,
        };
        let tail = ownership
            .as_ref()
            .filter(|j| j.tail_published)
            .and_then(|j| j.tail.clone());
        if accepted.is_none() && staging.is_none() && ownership.is_none() {
            return Err(Error::new(
                "NOT_FOUND",
                "unknown or expired ingestion request",
            ));
        }
        let deletion = match &ownership {
            None => IngestDeletion::NotManaged,
            Some(_) if retired => IngestDeletion::Retired,
            Some(j) if j.removed_at.is_some() => IngestDeletion::Removed,
            Some(j) if j.published_artifact.is_some() || j.tail_published => {
                IngestDeletion::Published
            }
            Some(j) if j.claimed.is_some() => IngestDeletion::Claimed,
            Some(_) => IngestDeletion::Reserved,
        };
        let total_records = accepted
            .as_ref()
            .map(|a| a.record_ids.len() as u64)
            .or_else(|| {
                staging
                    .as_ref()
                    .filter(|m| m.layout_complete)
                    .map(|m| m.layout_records)
            })
            .or_else(|| tail.as_ref().map(|_| 0));
        let pending = total_records
            .unwrap_or(0)
            .checked_sub(accepted.as_ref().map_or_else(
                || preparation.as_ref().map_or(0, |p| p.prepared_records),
                |a| a.record_ids.len() as u64,
            ))
            .ok_or_else(|| {
                Error::new(
                    "STORAGE_FAILURE",
                    "ingestion preparation exceeds its declared record count",
                )
            })?;
        let mut status = IngestStatus {
            selector: selector.clone(),
            store_id: writer.id.clone(),
            as_of: now,
            request_id,
            import_id: (staging.is_some() || ownership.is_some()).then_some(import_id),
            staging,
            preparation,
            accepted,
            tail,
            deletion,
            removed_at: ownership.as_ref().and_then(|j| j.removed_at),
            consumption: ownership,
            total_records,
            inspected_records: 0,
            json: IngestIndexCounts {
                pending,
                ..Default::default()
            },
            rdf: IngestIndexCounts {
                pending,
                ..Default::default()
            },
            text: IngestIndexCounts {
                pending,
                ..Default::default()
            },
        };
        let count = status.accepted.as_ref().map_or_else(
            || {
                status
                    .preparation
                    .as_ref()
                    .map_or(0, |p| p.prepared_records)
            },
            |a| a.record_ids.len() as u64,
        );
        if count > limits.scan_budget as u64 {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "ingestion status exceeds record scan_budget; raise it up to 100000",
            ));
        }
        let mut pin_visits = 0;
        let protected = if status.accepted.is_some() {
            writer.protected_matching(
                now,
                |_| true,
                || {
                    pin_visits += 1;
                    if pin_visits > limits.scan_budget {
                        Err(Error::new(
                            "RESOURCE_LIMIT",
                            "ingestion status exceeds pin scan_budget",
                        ))
                    } else {
                        Ok(())
                    }
                },
                None,
                Some(&mut remaining),
            )?
        } else {
            Default::default()
        };
        let mut ids = std::collections::BTreeSet::new();
        for index in 0..count {
            let key = match &status.accepted {
                Some(a) => a.record_ids[index as usize].clone(),
                None => publication::record_key(
                    status.import_id.as_deref().ok_or_else(|| {
                        Error::new(
                            "STORAGE_FAILURE",
                            "private preparation has no admitted import",
                        )
                    })?,
                    index,
                ),
            };
            let value = writer.optional_catalog_value(&key, &mut remaining)?;
            let Some(value) = value else {
                if status
                    .accepted
                    .as_ref()
                    .is_some_and(|a| now >= a.expires_at)
                    && !protected.contains(&key)
                {
                    status.unavailable();
                    continue;
                }
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "ingestion status publication record is missing",
                ));
            };
            let record = Record::decode(&value)?;
            writer.validate_event_record(&record)?;
            if !record.id.starts_with("record:") || record.id.len() > 128 {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "ingestion status requires a bounded record identity",
                ));
            }
            remaining = remaining.checked_sub(512).ok_or_else(|| {
                Error::new(
                    "RESOURCE_LIMIT",
                    "ingestion status identity admission exhausted",
                )
            })?;
            if !ids.insert(record.id.clone()) || (status.accepted.is_some() && record.id != key) {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "ingestion status record identity mismatch or duplicate",
                ));
            }
            let owner = status
                .accepted
                .as_ref()
                .map(|a| a.id.as_str())
                .or_else(|| status.preparation.as_ref().map(|p| p.artifact_id.as_str()));
            if Some(record.artifact_id.as_str()) != owner {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "ingestion status record belongs to another publication",
                ));
            }
            let envelope = status
                .accepted
                .as_ref()
                .map(|a| {
                    (
                        a.source.as_str(),
                        a.received_at,
                        a.expires_at,
                        a.byte_length,
                    )
                })
                .or_else(|| {
                    status
                        .preparation
                        .as_ref()
                        .zip(status.staging.as_ref())
                        .map(|(p, m)| {
                            (
                                m.source.as_str(),
                                p.received_at,
                                p.expires_at,
                                m.byte_length,
                            )
                        })
                });
            if !envelope.is_some_and(|(source, received, expires, bytes)| {
                record.source == source
                    && record.received_at == received
                    && record.expires_at == expires
                    && record.byte_start <= record.byte_end
                    && record.byte_end <= bytes
            }) {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "ingestion status record contradicts publication provenance",
                ));
            }
            if status.accepted.is_some() && !writer.live_record(&record, now, &protected) {
                status.unavailable();
                continue;
            }
            status.inspect(&writer, &record, &mut remaining)?;
        }
        Ok(status)
    }
}

impl IngestStatus {
    fn unavailable(&mut self) {
        self.inspected_records += 1;
        self.json.unavailable += 1;
        self.rdf.unavailable += 1;
        self.text.unavailable += 1;
    }

    fn inspect(
        &mut self,
        writer: &Writer,
        record: &Record,
        remaining: &mut usize,
    ) -> Result<(), Error> {
        for (key, counts, text) in [
            (record.projection_id.clone(), &mut self.json, false),
            (
                if record.rdf_projection_id.is_empty() {
                    String::new()
                } else {
                    format!("rdf:state:{}", record.rdf_projection_id)
                },
                &mut self.rdf,
                false,
            ),
            (format!("text-record:{}", record.id), &mut self.text, true),
        ] {
            if key.is_empty() {
                counts.not_applicable += 1;
                continue;
            }
            let value = writer
                .optional_catalog_value(&key, remaining)?
                .ok_or_else(|| {
                    Error::new(
                        "STORAGE_FAILURE",
                        "ingestion status native projection is missing",
                    )
                })?;
            counts.observe(&read_field::<String>(&value, "state")?, text)?;
        }
        self.inspected_records += 1;
        Ok(())
    }
}
