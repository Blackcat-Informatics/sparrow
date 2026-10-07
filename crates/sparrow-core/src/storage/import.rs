// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Durable raw staging, separate from accepted evidence and derived indexes.

use super::{
    CHUNK, Catalog, Error, RETRY_WINDOW, Store, Writer, blake3, hex, integer, json, parse, put,
    read_field, record_ends, validate_ingest_labels,
};
use purrdf_lex::json::{Object, Value};
use rocksdb::WriteBatch;

/// Fixed raw staging unit; only the final unit may be shorter.
pub const IMPORT_CHUNK_BYTES: usize = CHUNK;
const MAX_IMPORTS: usize = 16;
const MANIFEST_BYTES: usize = 8192;
const RECLAIM_CHUNKS: u64 = 128;
/// Maximum record spans in one immutable layout page.
pub const IMPORT_LAYOUT_PAGE_RECORDS: usize = 128;
const LAYOUT_BYTES: usize = 32 * 1024;

/// A recoverable unpublished input. Sealing verifies bytes, but does not accept
/// evidence or authorize producer deletion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportManifest {
    /// Store-local staging identity bound to a request, not an evidence ID.
    pub id: String,
    /// Stable client retry identity.
    pub request_id: String,
    /// Explicit logical source.
    pub source: String,
    /// Explicit interpretation hint.
    pub format: String,
    /// Declared complete length.
    pub byte_length: u64,
    /// Canonical BLAKE3 identity declared before uploading.
    pub payload_id: String,
    /// Next append offset, durably committed with preceding chunks.
    pub received_bytes: u64,
    /// Fixed recovery deadline; activity does not extend it.
    pub expires_at: u64,
    /// Complete bytes have been verified against the declared identity.
    pub sealed: bool,
    /// Number of record spans durably prepared so far.
    pub layout_records: u64,
    /// All record spans and digests are committed; evidence remains unpublished.
    pub layout_complete: bool,
    reclaimed_chunks: u64,
    reclaimed_layout_pages: u64,
}

impl Catalog for ImportManifest {
    fn encode(&self) -> Value {
        Object::new()
            .with("id", self.id.clone())
            .with("request_id", self.request_id.clone())
            .with("source", self.source.clone())
            .with("format", self.format.clone())
            .with("byte_length", self.byte_length.to_string())
            .with("payload_id", self.payload_id.clone())
            .with("received_bytes", self.received_bytes.to_string())
            .with("expires_at", self.expires_at.to_string())
            .with("sealed", self.sealed)
            .with("layout_records", self.layout_records.to_string())
            .with("layout_complete", self.layout_complete)
            .with("reclaimed_chunks", self.reclaimed_chunks.to_string())
            .with(
                "reclaimed_layout_pages",
                self.reclaimed_layout_pages.to_string(),
            )
            .into()
    }

    fn decode(value: &Value) -> Result<Self, Error> {
        let manifest = Self {
            id: read_field(value, "id")?,
            request_id: read_field(value, "request_id")?,
            source: read_field(value, "source")?,
            format: read_field(value, "format")?,
            byte_length: integer(value, "byte_length")?,
            payload_id: read_field(value, "payload_id")?,
            received_bytes: integer(value, "received_bytes")?,
            expires_at: integer(value, "expires_at")?,
            sealed: read_field(value, "sealed")?,
            reclaimed_chunks: integer(value, "reclaimed_chunks")?,
            layout_records: value
                .get("layout_records")
                .map(|_| integer(value, "layout_records"))
                .transpose()?
                .unwrap_or(0),
            layout_complete: value
                .get("layout_complete")
                .map(|_| read_field(value, "layout_complete"))
                .transpose()?
                .unwrap_or(false),
            reclaimed_layout_pages: value
                .get("reclaimed_layout_pages")
                .map(|_| integer(value, "reclaimed_layout_pages"))
                .transpose()?
                .unwrap_or(0),
        };
        validate_ingest_labels(&manifest.request_id, &manifest.source, &manifest.format)
            .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
        let lines = ["jsonl", "log"].contains(&manifest.format.as_str());
        if manifest.id != import_id(&manifest.request_id)
            || !valid_payload_id(&manifest.payload_id)
            || manifest.received_bytes > manifest.byte_length
            || (manifest.received_bytes != manifest.byte_length
                && !manifest.received_bytes.is_multiple_of(CHUNK as u64))
            || manifest.reclaimed_chunks > manifest.chunk_count()
            || (manifest.sealed && manifest.received_bytes != manifest.byte_length)
            || (manifest.layout_complete && !manifest.sealed)
            || (manifest.layout_records != 0 && !manifest.sealed)
            || manifest.reclaimed_layout_pages > manifest.layout_pages()
            || (!manifest.layout_complete
                && !manifest
                    .layout_records
                    .is_multiple_of(IMPORT_LAYOUT_PAGE_RECORDS as u64))
            || (lines && manifest.layout_records > manifest.byte_length)
            || (manifest.layout_complete
                && lines
                && (manifest.layout_records == 0) != (manifest.byte_length == 0))
            || (!lines && manifest.layout_records > 1)
            || (manifest.layout_complete && !lines && manifest.layout_records != 1)
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "inconsistent import manifest",
            ));
        }
        crate::format_instant(manifest.expires_at)
            .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
        Ok(manifest)
    }
}

impl ImportManifest {
    fn chunk_count(&self) -> u64 {
        self.received_bytes.div_ceil(CHUNK as u64)
    }
    fn layout_pages(&self) -> u64 {
        self.layout_records
            .div_ceil(IMPORT_LAYOUT_PAGE_RECORDS as u64)
    }
    fn require_live(&self, now: u64) -> Result<(), Error> {
        if now >= self.expires_at || self.reclaimed_chunks != 0 || self.reclaimed_layout_pages != 0
        {
            return Err(Error::new(
                "IMPORT_EXPIRED",
                "staging lease ended; complete bounded reclamation before starting a new import",
            ));
        }
        Ok(())
    }
}

fn valid_payload_id(id: &str) -> bool {
    id.strip_prefix("blake3:")
        .and_then(hex::decode_32_canonical)
        .is_some()
}

fn import_id(request: &str) -> String {
    format!(
        "import:{}",
        hex::encode(blake3::hash(request.as_bytes()).as_bytes())
    )
}

fn chunk_key(id: &str, offset: u64) -> String {
    format!("import-chunk:{id}:{offset:020}")
}

fn layout_key(id: &str, page: u64) -> String {
    format!("import-layout:{id}:{page:020}")
}

/// Exact staged record boundary and native complete record-byte identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportRecordSpan {
    /// Zero-based source record ordinal.
    pub index: u64,
    /// Inclusive original artifact offset.
    pub byte_start: u64,
    /// Exclusive original offset, including any line delimiter.
    pub byte_end: u64,
    /// Canonical native BLAKE3 of the complete record bytes.
    pub payload_id: String,
}

impl Catalog for ImportRecordSpan {
    fn encode(&self) -> Value {
        Object::new()
            .with("index", self.index.to_string())
            .with("byte_start", self.byte_start.to_string())
            .with("byte_end", self.byte_end.to_string())
            .with("payload_id", self.payload_id.clone())
            .into()
    }
    fn decode(value: &Value) -> Result<Self, Error> {
        let span = Self {
            index: integer(value, "index")?,
            byte_start: integer(value, "byte_start")?,
            byte_end: integer(value, "byte_end")?,
            payload_id: read_field(value, "payload_id")?,
        };
        if span.byte_start > span.byte_end || !valid_payload_id(&span.payload_id) {
            return Err(Error::new("STORAGE_FAILURE", "invalid import record span"));
        }
        Ok(span)
    }
}

/// One bounded immutable layout page under the import's fixed staging lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportLayoutPage {
    /// Record spans in original order, at most 128 rows.
    pub records: Vec<ImportRecordSpan>,
    /// Complete input record count; no evidence has been published.
    pub total_records: u64,
    /// Next aligned page start, or absence at the final page.
    pub next_index: Option<u64>,
}

impl Writer {
    fn imports(&self) -> Result<Vec<ImportManifest>, Error> {
        let mut rows = Vec::new();
        for entry in self.db.prefix_iterator("import:") {
            let (key, bytes) = entry?;
            if !key.starts_with(b"import:") {
                break;
            }
            if rows.len() >= MAX_IMPORTS || bytes.len() > MANIFEST_BYTES {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "import registry exceeds its admitted bounds",
                ));
            }
            let manifest = ImportManifest::decode(&parse(&bytes)?)?;
            if key.as_ref() != manifest.id.as_bytes() {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "import registry key mismatch",
                ));
            }
            rows.push(manifest);
        }
        Ok(rows)
    }

    fn import_chunk(&self, manifest: &ImportManifest, offset: u64) -> Result<Vec<u8>, Error> {
        if !offset.is_multiple_of(CHUNK as u64) || offset >= manifest.received_bytes {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "require an existing aligned staging offset",
            ));
        }
        let cf = self
            .db
            .cf_handle("payload")
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "payload family missing"))?;
        let bytes = self
            .db
            .get_cf(cf, chunk_key(&manifest.id, offset))?
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "staged chunk missing"))?;
        let expected = (manifest.received_bytes - offset).min(CHUNK as u64) as usize;
        if bytes.len() != expected + 32 || blake3::hash(&bytes[32..]).as_bytes() != &bytes[..32] {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "staged chunk length or digest mismatch",
            ));
        }
        Ok(bytes[32..].to_vec())
    }

    fn admit_import_batch(&self, batch: &WriteBatch) -> Result<(), Error> {
        let estimate = (batch.size_in_bytes() as u64)
            .saturating_mul(4)
            .saturating_add(65536);
        if self.physical_bytes()?.saturating_add(estimate)
            > self.config.disk_budget - self.config.maintenance_reserve
        {
            return Err(Error::new(
                "STORAGE_PRESSURE",
                "staging would cross the disk maintenance reserve",
            ));
        }
        Ok(())
    }

    fn commit_layout_page(
        &self,
        manifest: &mut ImportManifest,
        spans: &[ImportRecordSpan],
        complete: bool,
    ) -> Result<(), Error> {
        let mut batch = WriteBatch::default();
        if let Some(first) = spans.first() {
            if spans.len() > IMPORT_LAYOUT_PAGE_RECORDS
                || !first
                    .index
                    .is_multiple_of(IMPORT_LAYOUT_PAGE_RECORDS as u64)
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "invalid prepared layout page size or alignment",
                ));
            }
            let body = json::write_compact(&Value::array(
                spans.iter().map(Catalog::encode).collect::<Vec<_>>(),
            ));
            if body.len() + 32 > LAYOUT_BYTES {
                return Err(Error::new("RESOURCE_LIMIT", "layout page exceeds 32 KiB"));
            }
            let mut encoded = Vec::with_capacity(32 + body.len());
            encoded.extend_from_slice(blake3::hash(body.as_bytes()).as_bytes());
            encoded.extend_from_slice(body.as_bytes());
            batch.put(
                layout_key(
                    &manifest.id,
                    first.index / IMPORT_LAYOUT_PAGE_RECORDS as u64,
                ),
                encoded,
            );
            // Rebuilding after a failed attempt cannot lose the cleanup coverage
            // of later pages that were already committed on the previous attempt.
            manifest.layout_records = manifest
                .layout_records
                .max(first.index + spans.len() as u64);
        }
        manifest.layout_complete = complete;
        put(&mut batch, &manifest.id, manifest);
        self.admit_import_batch(&batch)?;
        self.commit(batch)
    }

    fn import_layout_rows(
        &self,
        manifest: &ImportManifest,
        index: u64,
    ) -> Result<Vec<ImportRecordSpan>, Error> {
        let bytes = self
            .db
            .get(layout_key(
                &manifest.id,
                index / IMPORT_LAYOUT_PAGE_RECORDS as u64,
            ))?
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "import layout page missing"))?;
        if !(32..=LAYOUT_BYTES).contains(&bytes.len())
            || blake3::hash(&bytes[32..]).as_bytes() != &bytes[..32]
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "import layout page size or digest mismatch",
            ));
        }
        let value = parse(&bytes[32..])?;
        let values = value
            .as_array()
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "layout page must contain an array"))?;
        let count =
            (manifest.layout_records - index).min(IMPORT_LAYOUT_PAGE_RECORDS as u64) as usize;
        if values.len() != count {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "import layout page count mismatch",
            ));
        }
        let mut rows = Vec::with_capacity(count);
        for (offset, value) in values.iter().enumerate() {
            let row = ImportRecordSpan::decode(value)?;
            if row.index != index + offset as u64
                || row.byte_end > manifest.byte_length
                || (rows
                    .last()
                    .is_some_and(|previous: &ImportRecordSpan| previous.byte_end != row.byte_start))
                || (row.index == 0 && row.byte_start != 0)
                || (row.index + 1 == manifest.layout_records
                    && row.byte_end != manifest.byte_length)
                || (row.byte_start == row.byte_end
                    && !(manifest.byte_length == 0
                        && !["log", "jsonl"].contains(&manifest.format.as_str())
                        && manifest.layout_records == 1))
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "import record layout does not cover exact source ranges",
                ));
            }
            rows.push(row);
        }
        Ok(rows)
    }
}

impl Store {
    /// Create or resume a bounded durable journal for a declared complete input.
    /// This does not create an artifact, records, or a deletion receipt.
    pub fn import_begin(
        &self,
        request_id: &str,
        source: &str,
        format: &str,
        byte_length: u64,
        payload_id: &str,
        now: u64,
    ) -> Result<ImportManifest, Error> {
        validate_ingest_labels(request_id, source, format)?;
        if !valid_payload_id(payload_id) {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "require canonical complete blake3 payload identity",
            ));
        }
        let writer = self.lock()?;
        let id = import_id(request_id);
        if let Some(bytes) = writer.db.get(&id)? {
            let previous = ImportManifest::decode(&parse(&bytes)?)?;
            if previous.request_id != request_id
                || previous.source != source
                || previous.format != format
                || previous.byte_length != byte_length
                || previous.payload_id != payload_id
            {
                return Err(Error::new(
                    "CONFLICT",
                    "import request identity has different declared input",
                ));
            }
            previous.require_live(now)?;
            return Ok(previous);
        }
        if byte_length > writer.config.disk_budget - writer.config.maintenance_reserve {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "declared input exceeds the entire available disk budget",
            ));
        }
        if writer.imports()?.len() >= MAX_IMPORTS {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "16 import journals are occupied; resume, cancel, or reclaim them",
            ));
        }
        let expires_at = now
            .checked_add(RETRY_WINDOW)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "import lease overflow"))?;
        crate::format_instant(expires_at)
            .map_err(|e| Error::new("INVALID_ARGUMENT", e.to_string()))?;
        let manifest = ImportManifest {
            id,
            request_id: request_id.into(),
            source: source.into(),
            format: format.into(),
            byte_length,
            payload_id: payload_id.into(),
            received_bytes: 0,
            expires_at,
            sealed: false,
            reclaimed_chunks: 0,
            layout_records: 0,
            layout_complete: false,
            reclaimed_layout_pages: 0,
        };
        let mut batch = WriteBatch::default();
        put(&mut batch, &manifest.id, &manifest);
        writer.admit_import_batch(&batch)?;
        writer.commit(batch)?;
        Ok(manifest)
    }

    /// Recover the committed offset and fixed lease after a client/daemon restart.
    pub fn import_get(&self, id: &str, now: u64) -> Result<ImportManifest, Error> {
        if !id.starts_with("import:") {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "require an import staging ID",
            ));
        }
        let manifest: ImportManifest = self.lock()?.get(id)?;
        manifest.require_live(now)?;
        Ok(manifest)
    }

    /// Append exactly one fixed-size chunk (or the declared final remainder).
    /// Identical committed-offset retries succeed; overlaps and changed bytes fail.
    pub fn import_append(
        &self,
        id: &str,
        offset: u64,
        bytes: &[u8],
        now: u64,
    ) -> Result<ImportManifest, Error> {
        if bytes.is_empty() || bytes.len() > CHUNK || !offset.is_multiple_of(CHUNK as u64) {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "require a nonempty chunk of at most 64 KiB at an aligned offset",
            ));
        }
        let writer = self.lock()?;
        let mut manifest: ImportManifest = writer.get(id)?;
        manifest.require_live(now)?;
        if offset >= manifest.byte_length
            || bytes.len() as u64 != (manifest.byte_length - offset).min(CHUNK as u64)
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "chunk must match its declared complete-input range",
            ));
        }
        if offset < manifest.received_bytes {
            if writer.import_chunk(&manifest, offset)? != bytes {
                return Err(Error::new(
                    "CONFLICT",
                    "committed import offset contains different bytes",
                ));
            }
            return Ok(manifest);
        }
        if offset != manifest.received_bytes || manifest.sealed {
            return Err(Error::new(
                "CONFLICT",
                "append must follow the committed offset of an unsealed import",
            ));
        }
        let mut encoded = Vec::with_capacity(32 + bytes.len());
        encoded.extend_from_slice(blake3::hash(bytes).as_bytes());
        encoded.extend_from_slice(bytes);
        let cf = writer
            .db
            .cf_handle("payload")
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "payload family missing"))?;
        let mut batch = WriteBatch::default();
        batch.put_cf(cf, chunk_key(id, offset), encoded);
        manifest.received_bytes += bytes.len() as u64;
        put(&mut batch, id, &manifest);
        writer.admit_import_batch(&batch)?;
        writer.commit(batch)?;
        Ok(manifest)
    }

    /// Read one checked staging chunk for recovery/publication, never as evidence.
    pub fn import_read_chunk(&self, id: &str, offset: u64, now: u64) -> Result<Vec<u8>, Error> {
        let writer = self.lock()?;
        let manifest: ImportManifest = writer.get(id)?;
        manifest.require_live(now)?;
        writer.import_chunk(&manifest, offset)
    }

    /// Verify every committed chunk and the complete digest with native bounded
    /// streaming BLAKE3. Sealing is not artifact publication or deletion authority.
    pub fn import_seal(&self, id: &str, now: u64) -> Result<ImportManifest, Error> {
        let writer = self.lock()?;
        let mut manifest: ImportManifest = writer.get(id)?;
        manifest.require_live(now)?;
        if manifest.received_bytes != manifest.byte_length {
            return Err(Error::new("CONFLICT", "cannot seal an incomplete import"));
        }
        let mut hash = blake3::Hasher::new();
        for chunk in 0..manifest.chunk_count() {
            hash.update(&writer.import_chunk(&manifest, chunk * CHUNK as u64)?);
        }
        if format!("blake3:{}", hex::encode(hash.finalize().as_bytes())) != manifest.payload_id {
            return Err(Error::new(
                "CONFLICT",
                "complete staged digest differs from declared payload; cancel and correct the input",
            ));
        }
        if !manifest.sealed {
            manifest.sealed = true;
            let mut batch = WriteBatch::default();
            put(&mut batch, id, &manifest);
            writer.admit_import_batch(&batch)?;
            writer.commit(batch)?;
        }
        Ok(manifest)
    }

    /// End a staging lease durably. Physical cleanup is bounded and resumable.
    pub fn import_cancel(&self, id: &str, now: u64) -> Result<(), Error> {
        let writer = self.lock()?;
        let mut manifest: ImportManifest = writer.get(id)?;
        manifest.expires_at = manifest.expires_at.min(now);
        let mut batch = WriteBatch::default();
        put(&mut batch, id, &manifest);
        writer.commit(batch)
    }

    /// Durably prepare exact record spans with bounded memory. Repeated or
    /// interrupted attempts rebuild deterministic private pages, never occurrences.
    /// This is preparation for publication, not an acceptance/deletion receipt.
    pub fn import_prepare_layout(&self, id: &str, now: u64) -> Result<ImportManifest, Error> {
        let writer = self.lock()?;
        let mut manifest: ImportManifest = writer.get(id)?;
        manifest.require_live(now)?;
        if !manifest.sealed {
            return Err(Error::new(
                "CONFLICT",
                "seal all raw bytes before preparing their record layout",
            ));
        }
        if manifest.layout_complete {
            return Ok(manifest);
        }
        let mut rows = Vec::with_capacity(IMPORT_LAYOUT_PAGE_RECORDS);
        let mut record_hash = blake3::Hasher::new();
        let mut input_hash = blake3::Hasher::new();
        let mut record_start = 0;
        let mut next_index = 0;
        let count = manifest.chunk_count().max(1);
        for chunk in 0..count {
            let bytes = if manifest.byte_length == 0 {
                Vec::new()
            } else {
                writer.import_chunk(&manifest, chunk * CHUNK as u64)?
            };
            input_hash.update(&bytes);
            let mut cursor = 0;
            for end in record_ends(&manifest.format, &bytes, chunk + 1 == count) {
                if next_index >= writer.config.max_records as u64 {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "import layout exceeds configured record count; raw staging remains available for cancellation",
                    ));
                }
                record_hash.update(&bytes[cursor..end]);
                let byte_end = chunk * CHUNK as u64 + end as u64;
                rows.push(ImportRecordSpan {
                    index: next_index,
                    byte_start: record_start,
                    byte_end,
                    payload_id: format!(
                        "blake3:{}",
                        hex::encode(record_hash.finalize().as_bytes())
                    ),
                });
                record_hash = blake3::Hasher::new();
                record_start = byte_end;
                cursor = end;
                next_index += 1;
                if rows.len() == IMPORT_LAYOUT_PAGE_RECORDS {
                    writer.commit_layout_page(&mut manifest, &rows, false)?;
                    rows.clear();
                }
            }
            record_hash.update(&bytes[cursor..]);
        }
        if next_index < manifest.layout_records
            || record_start != manifest.byte_length
            || format!("blake3:{}", hex::encode(input_hash.finalize().as_bytes()))
                != manifest.payload_id
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "prepared record layout does not match the complete sealed input",
            ));
        }
        writer.commit_layout_page(&mut manifest, &rows, true)?;
        Ok(manifest)
    }

    /// Retrieve one checked, complete layout page at an aligned record ordinal.
    pub fn import_layout_page(
        &self,
        id: &str,
        index: u64,
        now: u64,
    ) -> Result<ImportLayoutPage, Error> {
        if !index.is_multiple_of(IMPORT_LAYOUT_PAGE_RECORDS as u64) {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "layout page ordinal must be aligned to 128 records",
            ));
        }
        let writer = self.lock()?;
        let manifest: ImportManifest = writer.get(id)?;
        manifest.require_live(now)?;
        if !manifest.layout_complete {
            return Err(Error::new(
                "CONFLICT",
                "complete record layout has not been committed",
            ));
        }
        if index >= manifest.layout_records {
            if index == 0 && manifest.layout_records == 0 {
                return Ok(ImportLayoutPage {
                    records: Vec::new(),
                    total_records: 0,
                    next_index: None,
                });
            }
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "layout page ordinal is outside the complete layout",
            ));
        }
        let records = writer.import_layout_rows(&manifest, index)?;
        if index != 0 {
            let previous =
                writer.import_layout_rows(&manifest, index - IMPORT_LAYOUT_PAGE_RECORDS as u64)?;
            if previous.last().map(|row| row.byte_end) != records.first().map(|row| row.byte_start)
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "neighboring layout pages do not cover contiguous bytes",
                ));
            }
        }
        let next = index + records.len() as u64;
        Ok(ImportLayoutPage {
            records,
            total_records: manifest.layout_records,
            next_index: (next < manifest.layout_records).then_some(next),
        })
    }

    /// Reclaim at most 128 expired chunks/layout pages and 16 journals per call.
    /// Each synced batch atomically advances its persistent cleanup cursor.
    pub fn import_reclaim(&self, now: u64) -> Result<u64, Error> {
        let writer = self.lock()?;
        let cf = writer
            .db
            .cf_handle("payload")
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "payload family missing"))?;
        let mut batch = WriteBatch::default();
        let mut removed = 0;
        for mut manifest in writer.imports()? {
            if manifest.expires_at > now {
                continue;
            }
            let end = manifest
                .chunk_count()
                .min(manifest.reclaimed_chunks + RECLAIM_CHUNKS - removed);
            for chunk in manifest.reclaimed_chunks..end {
                batch.delete_cf(cf, chunk_key(&manifest.id, chunk * CHUNK as u64));
                removed += 1;
            }
            manifest.reclaimed_chunks = end;
            let layout_end = manifest
                .layout_pages()
                .min(manifest.reclaimed_layout_pages + RECLAIM_CHUNKS - removed);
            for page in manifest.reclaimed_layout_pages..layout_end {
                batch.delete(layout_key(&manifest.id, page));
                removed += 1;
            }
            manifest.reclaimed_layout_pages = layout_end;
            if end == manifest.chunk_count() && layout_end == manifest.layout_pages() {
                batch.delete(&manifest.id);
            } else {
                put(&mut batch, &manifest.id, &manifest);
            }
        }
        if batch.size_in_bytes() != 0 {
            writer.commit(batch)?;
        }
        Ok(removed)
    }
}
