// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Bounded durable payload ownership for records larger than projection admission.

use super::super::import::{
    IMPORT_CHUNK_BYTES, ImportManifest, ImportRecordSpan, decode_page, encode_page,
};
use super::super::{
    Catalog, Error, Record, Writer, blake3, hex, ingest, integer, parse, put, random_id,
    read_chunk_range, read_field, retire,
};
use super::{ImportPreparation, preparation_key, record_key};
use purrdf_lex::json::{Object, Value};
use rocksdb::WriteBatch;
use std::collections::BTreeMap;

const PAGE_CHUNKS: u64 = 128;

/// Durable private raw preparation; no record or artifact is publicly accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawPreparation {
    /// Original record ordinal currently being prepared.
    pub record_index: u64,
    /// Stable reserved record identity across checkpoint retries.
    pub record_id: String,
    /// Number of normalized 64 KiB chunks durably owned by the private record/parent.
    pub prepared_chunks: u64,
    /// Complete record chunk count, including its final shorter chunk.
    pub total_chunks: u64,
    reclaimed_chunks: u64,
}

impl Catalog for RawPreparation {
    fn encode(&self) -> Value {
        Object::new()
            .with("record_index", self.record_index.to_string())
            .with("record_id", self.record_id.clone())
            .with("prepared_chunks", self.prepared_chunks.to_string())
            .with("total_chunks", self.total_chunks.to_string())
            .with("reclaimed_chunks", self.reclaimed_chunks.to_string())
            .into()
    }
    fn decode(value: &Value) -> Result<Self, Error> {
        let raw = Self {
            record_index: integer(value, "record_index")?,
            record_id: read_field(value, "record_id")?,
            prepared_chunks: integer(value, "prepared_chunks")?,
            total_chunks: integer(value, "total_chunks")?,
            reclaimed_chunks: integer(value, "reclaimed_chunks")?,
        };
        if !raw.record_id.starts_with("record:")
            || raw.record_id.len() != 39
            || raw.total_chunks <= crate::projection::RECORD_BYTES / IMPORT_CHUNK_BYTES as u64
            || raw.prepared_chunks == 0
            || raw.prepared_chunks > raw.total_chunks
            || raw.reclaimed_chunks > raw.prepared_chunks
            || (raw.prepared_chunks != raw.total_chunks
                && !raw.prepared_chunks.is_multiple_of(PAGE_CHUNKS))
            || (raw.reclaimed_chunks != raw.prepared_chunks
                && !raw.reclaimed_chunks.is_multiple_of(PAGE_CHUNKS))
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "inconsistent raw record preparation checkpoint",
            ));
        }
        Ok(raw)
    }
}

fn page_key(import: &str, record: u64, page: u64) -> String {
    format!("import-record-chunks:{import}:{record:020}:{page:020}")
}

fn page(
    writer: &Writer,
    import: &str,
    raw: &RawPreparation,
    start: u64,
) -> Result<Vec<String>, Error> {
    if !start.is_multiple_of(PAGE_CHUNKS) || start >= raw.prepared_chunks {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "invalid raw chunk page ordinal",
        ));
    }
    let bytes = writer
        .db
        .get(page_key(import, raw.record_index, start / PAGE_CHUNKS))?
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "prepared raw chunk page missing"))?;
    let value = decode_page(&bytes)?;
    let rows = value
        .as_array()
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "raw chunk page must contain an array"))?;
    if rows.len() as u64 != (raw.prepared_chunks - start).min(PAGE_CHUNKS) {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "raw chunk page count mismatch",
        ));
    }
    rows.iter()
        .map(|row| {
            let digest = row
                .as_str()
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "raw chunk identity must be text"))?;
            if digest.len() != 64
                || hex::decode(digest).map_or(true, |bytes| {
                    bytes.len() != 32 || hex::encode(&bytes) != digest
                })
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "noncanonical raw chunk identity",
                ));
            }
            Ok(digest.to_owned())
        })
        .collect()
}

pub(super) fn advance(
    writer: &Writer,
    manifest: &ImportManifest,
    mut progress: ImportPreparation,
    span: &ImportRecordSpan,
) -> Result<ImportPreparation, Error> {
    let length = span.byte_end - span.byte_start;
    let total = length.div_ceil(IMPORT_CHUNK_BYTES as u64);
    // Native catalog decoding budgets all strings, including ordered chunk identities.
    if total > (super::super::CATALOG_STRING_BYTES as u64 - 8192) / 64 {
        return Err(Error::new(
            "RESOURCE_LIMIT",
            "raw record chunk manifest exceeds native catalog string budget",
        ));
    }
    let mut raw = match progress.raw.take() {
        Some(raw) => raw,
        None => RawPreparation {
            record_index: span.index,
            record_id: random_id("record")?,
            prepared_chunks: 0,
            total_chunks: total,
            reclaimed_chunks: 0,
        },
    };
    if raw.record_index != span.index || raw.total_chunks != total || raw.reclaimed_chunks != 0 {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "raw preparation does not match its sealed record span",
        ));
    }
    if raw.prepared_chunks < total {
        let end = total.min(raw.prepared_chunks + PAGE_CHUNKS);
        let mut content = BTreeMap::new();
        let mut chunks = Vec::new();
        for index in raw.prepared_chunks..end {
            let relative = index * IMPORT_CHUNK_BYTES as u64;
            let offset = span.byte_start + relative;
            let count = (length - relative).min(IMPORT_CHUNK_BYTES as u64) as usize;
            let first = offset / IMPORT_CHUNK_BYTES as u64;
            let after = (offset + count as u64).div_ceil(IMPORT_CHUNK_BYTES as u64);
            let bytes = read_chunk_range(
                (first..after)
                    .map(|chunk| writer.import_chunk(manifest, chunk * IMPORT_CHUNK_BYTES as u64)),
                offset % IMPORT_CHUNK_BYTES as u64,
                count,
            )?;
            let digest = hex::encode(blake3::hash(&bytes).as_bytes());
            chunks.push(digest.clone());
            content.entry(digest).or_insert(bytes);
        }
        let mut batch = WriteBatch::default();
        writer.prepare_payloads(&mut batch, content, &chunks)?;
        batch.put(
            page_key(
                &manifest.id,
                raw.record_index,
                raw.prepared_chunks / PAGE_CHUNKS,
            ),
            encode_page(&Value::array(
                chunks.into_iter().map(Value::from).collect::<Vec<_>>(),
            ))?,
        );
        raw.prepared_chunks = end;
        progress.raw = Some(raw);
        put(&mut batch, &preparation_key(&manifest.id), &progress);
        writer.admit_import_batch(&batch)?;
        writer.commit(batch)?;
        return Ok(progress);
    }
    let mut chunks = Vec::with_capacity(total as usize);
    let mut digest = blake3::Hasher::new();
    let mut received = 0_u64;
    for start in (0..total).step_by(PAGE_CHUNKS as usize) {
        for id in page(writer, &manifest.id, &raw, start)? {
            let bytes = writer.payload_chunk(&id)?;
            let expected = (length - received).min(IMPORT_CHUNK_BYTES as u64);
            if bytes.len() as u64 != expected {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "prepared raw chunk length disagrees with its sealed record",
                ));
            }
            received += bytes.len() as u64;
            digest.update(&bytes);
            chunks.push(id);
        }
    }
    if received != length
        || format!("blake3:{}", hex::encode(digest.finalize().as_bytes())) != span.payload_id
    {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "prepared raw record disagrees with its complete sealed digest",
        ));
    }
    let profile = progress.captured_profile(writer, &manifest.source)?;
    let diagnostic_profile = progress.captured_diagnostic_profile(writer, &manifest.source)?;
    let mut builder = ingest::RecordBatch::new(writer)?;
    let record: Record = builder.record(ingest::RecordInput {
        producer: None,
        id: raw.record_id,
        artifact_id: &progress.artifact_id,
        source: &manifest.source,
        format: &manifest.format,
        byte_start: span.byte_start,
        payload: ingest::RecordPayload::Prepared {
            byte_length: length,
            digest: &span.payload_id[7..],
            chunks,
        },
        received_at: progress.received_at,
        expires_at: progress.expires_at,
        source_time_profile: &profile,
        diagnostic_profile: diagnostic_profile.as_ref(),
    })?;
    parse(super::super::json::write_compact(&record.encode()).as_bytes())?;
    let ingest::PreparedRecords { mut batch, .. } = builder.finish()?;
    put(&mut batch, &record_key(&manifest.id, span.index), &record);
    for index in 0..total.div_ceil(PAGE_CHUNKS) {
        batch.delete(page_key(&manifest.id, span.index, index));
    }
    progress.prepared_records += 1;
    put(&mut batch, &preparation_key(&manifest.id), &progress);
    writer.admit_import_batch(&batch)?;
    writer.commit(batch)?;
    Ok(progress)
}

/// Release one raw page through the same ownership retirement law, then checkpoint.
pub(super) fn reclaim(
    writer: &Writer,
    manifest: &ImportManifest,
    progress: &mut ImportPreparation,
) -> Result<(), Error> {
    let mut raw = progress
        .raw
        .take()
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "raw cleanup checkpoint missing"))?;
    let mut retirement = retire::Retirement::new(writer)?;
    let chunks = page(writer, &manifest.id, &raw, raw.reclaimed_chunks)?;
    retirement.payloads(&chunks, 2)?;
    let mut batch = retirement.finish()?;
    batch.delete(page_key(
        &manifest.id,
        raw.record_index,
        raw.reclaimed_chunks / PAGE_CHUNKS,
    ));
    raw.reclaimed_chunks += chunks.len() as u64;
    if raw.reclaimed_chunks < raw.prepared_chunks {
        progress.raw = Some(raw);
    }
    put(&mut batch, &preparation_key(&manifest.id), progress);
    writer.commit(batch)
}
