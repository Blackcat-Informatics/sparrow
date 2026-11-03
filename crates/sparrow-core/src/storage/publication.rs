// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Recoverable native preparation and atomic complete-artifact publication.

use super::import::{IMPORT_CHUNK_BYTES, IMPORT_LAYOUT_PAGE_RECORDS, ImportManifest};
use super::{
    Artifact, Catalog, Error, RETENTION_MS, RETRY_WINDOW, Record, Store, Writer, blake3, field,
    hex, ingest, integer, json, parse, put, random_id, read_chunk_range, read_field, retire,
};
use purrdf_lex::json::{Object, Value};
use rocksdb::WriteBatch;

mod raw;
pub use raw::RawPreparation;
pub(super) const PUBLICATION_BYTES: usize = 128 * 1024 * 1024;
/// One preparation checkpoint covers at most this many ordinary records, and
/// adds further records only while their source stays within the byte bound; a
/// first record larger than the byte bound forms its own checkpoint. Each
/// checkpoint costs one synced commit and one footprint measurement, so a record
/// per checkpoint made small-record files sync-bound.
const PREPARE_RECORDS: u64 = 256;
const PREPARE_SOURCE_BYTES: usize = 256 * 1024;

/// Durable private preparation progress. This is not an evidence receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportPreparation {
    /// Store-local staging identity.
    pub import_id: String,
    /// Reserved artifact identity, inaccessible until final publication.
    pub artifact_id: String,
    /// Number of durably prepared records in original order.
    pub prepared_records: u64,
    /// Complete source record count.
    pub total_records: u64,
    /// Server receipt clock captured for this publication attempt.
    pub received_at: u64,
    /// Ordinary deadline for all artifact/record occurrences.
    pub expires_at: u64,
    /// Immutable source-time profile captured before native preparation.
    pub source_time_profile: Option<String>,
    /// Immutable diagnostic interpretation captured before any record preparation.
    pub diagnostic_profile: Option<String>,
    /// Bounded raw chunk preparation of the current oversized record, if any.
    pub raw: Option<RawPreparation>,
    reclaimed_records: u64,
}

impl Catalog for ImportPreparation {
    fn encode(&self) -> Value {
        Object::new()
            .with("import_id", self.import_id.clone())
            .with("artifact_id", self.artifact_id.clone())
            .with("prepared_records", self.prepared_records.to_string())
            .with("total_records", self.total_records.to_string())
            .with("received_at", self.received_at.to_string())
            .with("expires_at", self.expires_at.to_string())
            .with("source_time_profile", self.source_time_profile.clone())
            .with("diagnostic_profile", self.diagnostic_profile.clone())
            .with(
                "raw",
                self.raw
                    .as_ref()
                    .map(Catalog::encode)
                    .unwrap_or(Value::Null),
            )
            .with("reclaimed_records", self.reclaimed_records.to_string())
            .into()
    }
    fn decode(value: &Value) -> Result<Self, Error> {
        let progress = Self {
            import_id: read_field(value, "import_id")?,
            artifact_id: read_field(value, "artifact_id")?,
            prepared_records: integer(value, "prepared_records")?,
            total_records: integer(value, "total_records")?,
            received_at: integer(value, "received_at")?,
            expires_at: integer(value, "expires_at")?,
            source_time_profile: read_field(value, "source_time_profile")?,
            diagnostic_profile: value
                .get("diagnostic_profile")
                .map(purrdf_lex::json::record::FromJson::from_json)
                .transpose()?
                .unwrap_or(None),
            raw: match value.get("raw") {
                None | Some(Value::Null) => None,
                Some(value) => Some(RawPreparation::decode(value)?),
            },
            reclaimed_records: integer(value, "reclaimed_records")?,
        };
        if !progress.artifact_id.starts_with("artifact:")
            || progress.prepared_records > progress.total_records
            || progress.reclaimed_records > progress.prepared_records
            || progress.received_at.checked_add(RETENTION_MS) != Some(progress.expires_at)
            || progress.raw.as_ref().is_some_and(|raw| {
                raw.record_index != progress.prepared_records
                    || progress.prepared_records == progress.total_records
            })
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "inconsistent import preparation checkpoint",
            ));
        }
        crate::format_instant(progress.expires_at)
            .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
        Ok(progress)
    }
}

impl ImportPreparation {
    fn captured_diagnostic_profile(
        &self,
        writer: &Writer,
        source: &str,
    ) -> Result<Option<super::diagnostic::DiagnosticProfile>, Error> {
        match &self.diagnostic_profile {
            None => Ok(None),
            Some(id) => writer
                .diagnostic_profile(source)?
                .filter(|profile| profile.id == *id)
                .map(Some)
                .ok_or_else(|| {
                    Error::new(
                        "STORAGE_FAILURE",
                        "captured diagnostic profile is missing or changed",
                    )
                }),
        }
    }

    fn captured_profile(
        &self,
        writer: &Writer,
        source: &str,
    ) -> Result<Option<super::source_time::SourceTimeProfile>, Error> {
        match &self.source_time_profile {
            None => Ok(None),
            Some(id) => writer
                .time_profile(source)?
                .filter(|profile| profile.id == *id)
                .map(Some)
                .ok_or_else(|| {
                    Error::new(
                        "STORAGE_FAILURE",
                        "captured source-time profile is missing or changed",
                    )
                }),
        }
    }
}

fn preparation_key(id: &str) -> String {
    format!("import-preparation:{id}")
}
pub(super) fn record_key(id: &str, index: u64) -> String {
    format!("import-record:{id}:{index:020}")
}
fn receipt_key(id: &str) -> String {
    format!("request:import-publication:{id}")
}

fn preparation(
    writer: &Writer,
    manifest: &ImportManifest,
) -> Result<Option<ImportPreparation>, Error> {
    preparation_admitted(writer, manifest, None)
}

pub(super) fn preparation_admitted(
    writer: &Writer,
    manifest: &ImportManifest,
    remaining: Option<&mut usize>,
) -> Result<Option<ImportPreparation>, Error> {
    let Some(bytes) = writer.db.get(preparation_key(&manifest.id))? else {
        return Ok(None);
    };
    if bytes.len() > 8192 {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "import preparation checkpoint exceeds its bound",
        ));
    }
    if let Some(remaining) = remaining {
        Writer::charge_catalog_shape(remaining, &bytes)?;
    }
    let progress = ImportPreparation::decode(&parse(&bytes)?)?;
    if progress.import_id != manifest.id || progress.total_records != manifest.layout_records {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "import preparation does not match its sealed layout",
        ));
    }
    Ok(Some(progress))
}

pub(super) fn published(
    writer: &Writer,
    id: &str,
    now: u64,
) -> Result<Option<(ImportManifest, Artifact)>, Error> {
    published_admitted(writer, id, now, None)
}

pub(super) fn published_admitted(
    writer: &Writer,
    id: &str,
    now: u64,
    mut remaining: Option<&mut usize>,
) -> Result<Option<(ImportManifest, Artifact)>, Error> {
    let managed = writer.consumption_admitted(id, remaining.as_deref_mut())?;
    let saved = if managed
        .as_ref()
        .is_some_and(|journal| journal.published_artifact.is_some())
    {
        let bytes = writer.db.get_pinned(receipt_key(id))?.ok_or_else(|| {
            Error::new("STORAGE_FAILURE", "consumption publication receipt missing")
        })?;
        if let Some(remaining) = remaining.as_deref_mut() {
            Writer::charge_catalog_shape(remaining, &bytes)?;
        }
        Some(parse(&bytes)?)
    } else {
        writer.retry_admitted(&receipt_key(id), now, remaining.as_deref_mut())?
    };
    let Some(saved) = saved else {
        return Ok(None);
    };
    let manifest = ImportManifest::decode(field(&saved, "manifest")?)?;
    let artifact = Artifact::decode(field(&saved, "receipt")?)?;
    integer(&saved, "expires_at")?;
    if manifest.consumption {
        writer
            .require_consumption_admitted(
                &manifest.request_id,
                &manifest.source,
                &manifest.format,
                manifest.byte_length,
                remaining,
            )
            .map_err(|e| {
                if e.code == "RESOURCE_LIMIT" {
                    e
                } else {
                    Error::new("STORAGE_FAILURE", e.to_string())
                }
            })?;
    }
    if manifest.id != id
        || (manifest.consumption
            && managed
                .as_ref()
                .and_then(|journal| journal.published_artifact.as_ref())
                != Some(&artifact.id))
        || manifest.published_artifact.as_ref() != Some(&artifact.id)
        || manifest.payload_id != artifact.payload_id
        || manifest.byte_length != artifact.byte_length
        || manifest.source != artifact.source
        || manifest.format != artifact.format
    {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "published import receipt contradicts its declared input",
        ));
    }
    Ok(Some((manifest, artifact)))
}

impl Store {
    /// Recover private preparation progress; explicit absence precedes preparation.
    pub fn import_preparation_get(
        &self,
        id: &str,
        now: u64,
    ) -> Result<Option<ImportPreparation>, Error> {
        let writer = self.lock()?;
        let manifest: ImportManifest = writer.get(id)?;
        manifest.require_live(now)?;
        preparation(&writer, &manifest)
    }

    /// Prepare a bounded run of ordinary records starting at the expected ordinal
    /// and checkpoint their native dependencies in one durable batch. An oversized
    /// record advances alone through raw chunks. Retrying an older ordinal returns
    /// progress without another occurrence.
    pub fn import_prepare_record(
        &self,
        id: &str,
        expected_index: u64,
        now: u64,
    ) -> Result<ImportPreparation, Error> {
        let writer = self.lock()?;
        let manifest: ImportManifest = writer.get(id)?;
        manifest.require_live(now)?;
        if !manifest.layout_complete || manifest.published_artifact.is_some() {
            return Err(Error::new(
                "CONFLICT",
                "require a complete, unpublished record layout",
            ));
        }
        if manifest.layout_records > writer.config.max_records as u64 {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "sealed layout exceeds the current configured record limit",
            ));
        }
        let mut progress = match preparation(&writer, &manifest)? {
            Some(progress) => progress,
            None => {
                if expected_index != 0 {
                    return Err(Error::new(
                        "CONFLICT",
                        "preparation begins at record ordinal zero",
                    ));
                }
                let expires_at = now.checked_add(RETENTION_MS).ok_or_else(|| {
                    Error::new("INVALID_ARGUMENT", "publication retention overflow")
                })?;
                crate::format_instant(expires_at)
                    .map_err(|e| Error::new("INVALID_ARGUMENT", e.to_string()))?;
                let progress = ImportPreparation {
                    import_id: id.into(),
                    artifact_id: random_id("artifact")?,
                    prepared_records: 0,
                    total_records: manifest.layout_records,
                    received_at: now,
                    expires_at,
                    source_time_profile: writer
                        .time_profile(&manifest.source)?
                        .map(|profile| profile.id),
                    diagnostic_profile: writer
                        .diagnostic_profile(&manifest.source)?
                        .map(|profile| profile.id),
                    raw: None,
                    reclaimed_records: 0,
                };
                let mut batch = WriteBatch::default();
                put(&mut batch, &preparation_key(id), &progress);
                writer.admit_import_batch(&batch)?;
                writer.commit(batch)?;
                progress
            }
        };
        if expected_index > progress.prepared_records {
            return Err(Error::new(
                "CONFLICT",
                "expected record ordinal skips committed preparation",
            ));
        }
        if expected_index < progress.prepared_records
            || progress.prepared_records == progress.total_records
        {
            return Ok(progress);
        }
        let profile = progress.captured_profile(&writer, &manifest.source)?;
        let diagnostic_profile = progress.captured_diagnostic_profile(&writer, &manifest.source)?;
        let mut builder = ingest::RecordBatch::new(&writer)?;
        let mut records = Vec::new();
        let mut source_bytes = 0;
        let mut loaded = None;
        let mut rows = Vec::new();
        let mut index = expected_index;
        while index < progress.total_records && index - expected_index < PREPARE_RECORDS {
            let page_index =
                index / IMPORT_LAYOUT_PAGE_RECORDS as u64 * IMPORT_LAYOUT_PAGE_RECORDS as u64;
            if loaded != Some(page_index) {
                rows = writer.import_layout_rows(&manifest, page_index)?;
                loaded = Some(page_index);
            }
            let span = &rows[(index - page_index) as usize];
            let length = usize::try_from(span.byte_end - span.byte_start).map_err(|_| {
                Error::new(
                    "RESOURCE_LIMIT",
                    "record length exceeds addressable native preparation",
                )
            })?;
            if length as u64 > crate::projection::RECORD_BYTES {
                if records.is_empty() {
                    return raw::advance(&writer, &manifest, progress, span);
                }
                break;
            }
            if !records.is_empty() && source_bytes + length > PREPARE_SOURCE_BYTES {
                break;
            }
            let first = span.byte_start / IMPORT_CHUNK_BYTES as u64;
            let after = span.byte_end.div_ceil(IMPORT_CHUNK_BYTES as u64);
            let bytes = read_chunk_range(
                (first..after)
                    .map(|chunk| writer.import_chunk(&manifest, chunk * IMPORT_CHUNK_BYTES as u64)),
                span.byte_start % IMPORT_CHUNK_BYTES as u64,
                length,
            )?;
            if format!("blake3:{}", hex::encode(blake3::hash(&bytes).as_bytes())) != span.payload_id
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "prepared source record disagrees with its sealed byte identity",
                ));
            }
            records.push(builder.record(ingest::RecordInput {
                producer: None,
                receiver: None,
                tail: None,
                id: random_id("record")?,
                artifact_id: &progress.artifact_id,
                source: &manifest.source,
                format: &manifest.format,
                byte_start: span.byte_start,
                payload: ingest::RecordPayload::Buffered(&bytes),
                received_at: progress.received_at,
                expires_at: progress.expires_at,
                source_time_profile: &profile,
                diagnostic_profile: diagnostic_profile.as_ref(),
            })?);
            source_bytes += length;
            index += 1;
        }
        let ingest::PreparedRecords {
            mut batch,
            estimate,
            ..
        } = builder.prepare()?;
        for (ordinal, record) in (expected_index..).zip(&records) {
            put(&mut batch, &record_key(id, ordinal), record);
        }
        progress.prepared_records = index;
        put(&mut batch, &preparation_key(id), &progress);
        if batch.size_in_bytes() > PUBLICATION_BYTES {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "prepared record dependencies exceed 128 MiB",
            ));
        }
        writer.apply(batch, None, Some(estimate))?;
        Ok(progress)
    }

    /// Atomically publish the complete artifact, ordered records and temporal
    /// membership after all native dependencies have crossed synced checkpoints.
    pub fn import_publish(&self, id: &str, now: u64) -> Result<Artifact, Error> {
        let writer = self.lock()?;
        if let Some((_, artifact)) = published(&writer, id, now)? {
            return Ok(artifact);
        }
        let mut manifest: ImportManifest = writer.get(id)?;
        manifest.require_live(now)?;
        if manifest.published_artifact.is_some() {
            return Err(Error::new(
                "CONFLICT",
                "publication retry window ended; import cannot create a replacement occurrence",
            ));
        }
        let progress = preparation(&writer, &manifest)?.ok_or_else(|| {
            Error::new(
                "CONFLICT",
                "prepare all native record dependencies before publication",
            )
        })?;
        if !manifest.layout_complete
            || progress.prepared_records != manifest.layout_records
            || progress.reclaimed_records != 0
        {
            return Err(Error::new(
                "CONFLICT",
                "cannot publish partial or retired preparation",
            ));
        }
        if manifest.layout_records > writer.config.max_records as u64 {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "publication exceeds the current configured record limit",
            ));
        }
        let mut batch = WriteBatch::default();
        let mut record_ids = Vec::new();
        let mut chunks = Vec::new();
        let mut next_byte = 0;
        let mut input_hash = blake3::Hasher::new();
        let mut ids = std::collections::BTreeSet::new();
        let mut layout = Vec::new();
        if writer.db.get(&progress.artifact_id)?.is_some() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "reserved artifact identity already exists outside its publication receipt",
            ));
        }
        for index in 0..progress.prepared_records {
            if index.is_multiple_of(IMPORT_LAYOUT_PAGE_RECORDS as u64) {
                layout = writer.import_layout_rows(&manifest, index)?;
            }
            let span = &layout[(index % IMPORT_LAYOUT_PAGE_RECORDS as u64) as usize];
            let record: Record = writer.get(&record_key(id, index))?;
            if record.artifact_id != progress.artifact_id
                || record.source != manifest.source
                || record.received_at != progress.received_at
                || record.expires_at != progress.expires_at
                || record.byte_start != next_byte
                || record.byte_end > manifest.byte_length
                || record.evicted
                || !record.id.starts_with("record:")
                || !ids.insert(record.id.clone())
                || writer.db.get(&record.id)?.is_some()
                || record.byte_start != span.byte_start
                || record.byte_end != span.byte_end
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "private record does not match complete publication provenance",
                ));
            }
            writer.validate_event_record(&record)?;
            if writer.db.get(format!("rdf:graph:{}", record.id))?.is_none() {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "prepared record metadata RDF is missing",
                ));
            }
            let mut record_hash = blake3::Hasher::new();
            let mut record_length = 0_u64;
            for digest in &record.chunks {
                let payload = writer.payload_chunk(digest)?;
                record_length =
                    record_length
                        .checked_add(payload.len() as u64)
                        .ok_or_else(|| {
                            Error::new("STORAGE_FAILURE", "prepared record byte count overflow")
                        })?;
                input_hash.update(&payload);
                record_hash.update(&payload);
            }
            if record_length != span.byte_end - span.byte_start
                || format!("blake3:{}", hex::encode(record_hash.finalize().as_bytes()))
                    != span.payload_id
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "prepared record payload does not match its exact sealed span",
                ));
            }
            next_byte = record.byte_end;
            chunks.extend(record.chunks.iter().cloned());
            put(&mut batch, &record.id, &record);
            batch.put(
                format!("time:{:020}:{}", record.received_at, record.id),
                record.id.as_bytes(),
            );
            record_ids.push(record.id);
            if batch.size_in_bytes() > PUBLICATION_BYTES {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "complete publication metadata exceeds 128 MiB",
                ));
            }
        }
        if next_byte != manifest.byte_length
            || format!("blake3:{}", hex::encode(input_hash.finalize().as_bytes()))
                != manifest.payload_id
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "prepared records do not cover the complete artifact",
            ));
        }
        let artifact = Artifact {
            buffered: None,
            id: progress.artifact_id,
            source: manifest.source.clone(),
            format: manifest.format.clone(),
            received_at: progress.received_at,
            expires_at: progress.expires_at,
            byte_length: manifest.byte_length,
            payload_id: manifest.payload_id.clone(),
            record_ids,
            chunks,
            evicted: false,
        };
        writer.require_retirable(artifact.chunks.len(), artifact.record_ids.len())?;
        parse(json::write_compact(&artifact.encode()).as_bytes())?;
        put(&mut batch, &artifact.id, &artifact);
        super::artifact_rdf::publish(&writer.db, &mut batch, &artifact)?;
        super::recordless::publish(&mut batch, &artifact)?;
        manifest.published_artifact = Some(artifact.id.clone());
        writer.publish_consumption(&mut batch, &manifest, &artifact)?;
        put(&mut batch, id, &manifest);
        let saved: Value = Object::new()
            .with("manifest", manifest.encode())
            .with("receipt", artifact.encode())
            .with(
                "expires_at",
                now.checked_add(RETRY_WINDOW)
                    .ok_or_else(|| {
                        Error::new("INVALID_ARGUMENT", "publication receipt deadline overflow")
                    })?
                    .to_string(),
            )
            .into();
        batch.put(receipt_key(id), json::write_compact(&saved));
        writer.ingest_receipt(
            &mut batch,
            &manifest.request_id,
            &artifact,
            ingest::Origin::Ordinary,
            now,
        )?;
        if batch.size_in_bytes() > PUBLICATION_BYTES {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "complete publication and receipt exceed 128 MiB",
            ));
        }
        writer.admit_import_batch(&batch)?;
        writer.commit(batch)?;
        Ok(artifact)
    }
}

/// Reclaim at most one private prepared record, or the supplied finite share of
/// published catalog copies. Transferred dependencies are never decremented.
pub(super) fn reclaim_prepared(
    writer: &Writer,
    manifest: &ImportManifest,
    limit: u64,
) -> Result<(bool, u64), Error> {
    let Some(mut progress) = preparation(writer, manifest)? else {
        return Ok((true, 0));
    };
    if manifest.published_artifact.is_some() {
        if progress.raw.is_some() || limit == 0 {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "invalid published staging reclamation state",
            ));
        }
        let start = progress.reclaimed_records;
        let end = progress.prepared_records.min(start.saturating_add(limit));
        let mut batch = WriteBatch::default();
        for index in start..end {
            batch.delete(record_key(&manifest.id, index));
        }
        progress.reclaimed_records = end;
        let done = end == progress.prepared_records;
        if done {
            batch.delete(preparation_key(&manifest.id));
        } else {
            put(&mut batch, &preparation_key(&manifest.id), &progress);
        }
        writer.commit(batch)?;
        return Ok((done, (end - start).max(1)));
    }
    if progress.raw.is_some() {
        if manifest.published_artifact.is_some() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "published import has unfinished raw preparation",
            ));
        }
        raw::reclaim(writer, manifest, &mut progress)?;
        return Ok((false, 1));
    }
    let mut batch = if progress.reclaimed_records < progress.prepared_records {
        let key = record_key(&manifest.id, progress.reclaimed_records);
        let mut record: Record = writer.get(&key)?;
        let mut batch = if manifest.published_artifact.is_none() {
            let mut retirement = retire::Retirement::private(writer)?;
            retirement.record(&mut record, 2)?;
            retirement.finish()?
        } else {
            WriteBatch::default()
        };
        batch.delete(key);
        progress.reclaimed_records += 1;
        batch
    } else {
        WriteBatch::default()
    };
    let done = progress.reclaimed_records == progress.prepared_records;
    if done {
        batch.delete(preparation_key(&manifest.id));
    } else {
        put(&mut batch, &preparation_key(&manifest.id), &progress);
    }
    writer.commit(batch)?;
    Ok((done, 1))
}
