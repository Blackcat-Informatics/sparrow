// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Durable raw staging, separate from accepted evidence and derived indexes.

use super::{
    BYTE_READ_BYTES, CHUNK, Catalog, Error, RETRY_WINDOW, Store, Writer, blake3, hex, integer,
    json, parse, put, read_chunk_range, read_field, record_ends, validate_ingest_labels,
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
/// Longest record a layout keeps whole: 2048 chunks (128 MiB). A longer record is
/// laid out as consecutive raw records, so each chunk manifest fits one retirement
/// pass and one catalog read while the artifact still reads back byte-exact.
pub const RAW_RECORD_BYTES: u64 = 2048 * CHUNK as u64;
/// Largest input one atomic publication batch can carry. `import_publish` writes
/// every chunk identity four times (record row, artifact row, saved receipt and
/// ingest receipt) as a 67-byte JSON string, within 128 MiB less 16 MiB for the
/// fixed rows: 438,210 chunks, about 26.7 GiB. The v2 store format (#5) replaces
/// this single batch.
pub const MAX_IMPORT_BYTES: u64 =
    (super::publication::PUBLICATION_BYTES as u64 - 16 * 1024 * 1024) / (4 * 67) * CHUNK as u64;

/// Starting an upload either resumes private staging or returns accepted evidence.
/// An accepted retry never allocates another staging journal or occurrence.
#[derive(Clone, Debug)]
pub enum ImportStart {
    /// Private, resumable input; this is not an evidence receipt.
    Staging(ImportManifest),
    /// Original durable receipt from buffered or streamed ingestion.
    Accepted(super::Artifact),
}

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
    /// Complete published artifact identity, absent during private preparation.
    pub published_artifact: Option<String>,
    /// Source ownership persists until its consumption journal is resolved.
    pub consumption: bool,
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
            .with("published_artifact", self.published_artifact.clone())
            .with("consumption", self.consumption)
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
            published_artifact: value
                .get("published_artifact")
                .map(|_| read_field(value, "published_artifact"))
                .transpose()?
                .unwrap_or(None),
            consumption: value
                .get("consumption")
                .map(|_| read_field(value, "consumption"))
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
            || manifest.consumption != manifest.request_id.starts_with("consume:")
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
            // A document is one record unless layout split it into raw records,
            // each longer than the 16 MiB projected record bound.
            || (!lines
                && manifest.layout_records
                    > manifest
                        .byte_length
                        .div_ceil(crate::projection::RECORD_BYTES)
                        .max(1))
            || (manifest.layout_complete && !lines && manifest.layout_records == 0)
            || (manifest.published_artifact.is_some() && !manifest.layout_complete)
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
    pub(super) fn require_live(&self, now: u64) -> Result<(), Error> {
        if (!self.consumption && now >= self.expires_at)
            || self.reclaimed_chunks != 0
            || self.reclaimed_layout_pages != 0
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

pub(super) fn import_id(request: &str) -> String {
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

    pub(super) fn import_chunk(
        &self,
        manifest: &ImportManifest,
        offset: u64,
    ) -> Result<Vec<u8>, Error> {
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

    pub(super) fn admit_import_batch(&self, batch: &WriteBatch) -> Result<(), Error> {
        self.admit_data_write(super::pressure::data_batch_estimate(batch))
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
            let encoded = encode_page(&Value::array(
                spans.iter().map(Catalog::encode).collect::<Vec<_>>(),
            ))?;
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

    pub(super) fn import_layout_rows(
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
        let value = decode_page(&bytes)?;
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
        self.lock()?
            .begin_import(request_id, source, format, byte_length, payload_id, now)
    }

    /// Lower this open store's whole-record ceiling for import layouts so a test can
    /// split a document without a 128 MiB input. Pieces must stay raw, so the ceiling
    /// is a whole number of chunks above twice the projected record bound.
    /// This entry point is absent unless the dev-only test-support feature is selected.
    #[cfg(feature = "test-support")]
    pub fn split_raw_records_above(&self, bytes: u64) -> Result<(), Error> {
        let floor = 2 * crate::projection::RECORD_BYTES + CHUNK as u64;
        if !bytes.is_multiple_of(CHUNK as u64) || !(floor..=RAW_RECORD_BYTES).contains(&bytes) {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "raw record ceiling must be whole chunks above twice the projected record bound",
            ));
        }
        self.lock()?.raw_record_bytes.set(bytes);
        Ok(())
    }

    /// Atomically recover a shared ingestion receipt or create/resume private staging.
    pub fn import_start(
        &self,
        request_id: &str,
        source: &str,
        format: &str,
        byte_length: u64,
        payload_id: &str,
        now: u64,
    ) -> Result<ImportStart, Error> {
        validate_ingest_labels(request_id, source, format)?;
        if !valid_payload_id(payload_id) {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "require canonical complete blake3 payload identity",
            ));
        }
        let writer = self.lock()?;
        writer.require_consumption(request_id, source, format, byte_length)?;
        if let Some(receipt) = writer.accepted_ingest(
            request_id,
            source,
            format,
            byte_length,
            payload_id,
            super::ingest::Origin::Ordinary,
            now,
        )? {
            return Ok(ImportStart::Accepted(
                writer.receipt(receipt, super::durability::Durability::Durable)?,
            ));
        }
        writer
            .begin_import(request_id, source, format, byte_length, payload_id, now)
            .map(ImportStart::Staging)
    }

    /// Recover the committed offset and fixed lease after a client/daemon restart.
    pub fn import_get(&self, id: &str, now: u64) -> Result<ImportManifest, Error> {
        if !id.starts_with("import:") {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "require an import staging ID",
            ));
        }
        let writer = self.lock()?;
        if let Some((manifest, _)) = super::publication::published(&writer, id, now)? {
            return Ok(manifest);
        }
        let manifest: ImportManifest = writer.get(id)?;
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

    /// Materialize an exact bounded raw staging range for projection/recovery.
    /// The range must be committed, but need not have been sealed or published.
    pub fn import_bytes(
        &self,
        id: &str,
        offset: u64,
        length: usize,
        now: u64,
    ) -> Result<Vec<u8>, Error> {
        if length > BYTE_READ_BYTES {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "staging byte reads are bounded to 1 MiB",
            ));
        }
        let writer = self.lock()?;
        let manifest: ImportManifest = writer.get(id)?;
        manifest.require_live(now)?;
        let end = offset
            .checked_add(length as u64)
            .filter(|end| *end <= manifest.received_bytes)
            .ok_or_else(|| {
                Error::new(
                    "INVALID_ARGUMENT",
                    "byte range is outside committed staging",
                )
            })?;
        let first = offset / CHUNK as u64;
        let after = end.div_ceil(CHUNK as u64);
        read_chunk_range(
            (first..after).map(|chunk| writer.import_chunk(&manifest, chunk * CHUNK as u64)),
            offset % CHUNK as u64,
            length,
        )
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
        if super::publication::published(&writer, id, now)?.is_some() {
            return Err(Error::new(
                "CONFLICT",
                "published evidence cannot be canceled as staging",
            ));
        }
        let mut manifest: ImportManifest = writer.get(id)?;
        if manifest.consumption {
            return Err(Error::new(
                "CONFLICT",
                "consumption staging owns a claimed source; reconcile its journal instead of canceling its recovery state",
            ));
        }
        if manifest.published_artifact.is_some() {
            return Err(Error::new(
                "CONFLICT",
                "published evidence cannot be canceled as staging",
            ));
        }
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
        let split = writer.raw_record_bytes.get();
        let mut layout = Layout {
            writer: &writer,
            rows: Vec::with_capacity(IMPORT_LAYOUT_PAGE_RECORDS),
            next_index: 0,
            record_start: 0,
            record_hash: blake3::Hasher::new(),
            pending: None,
            split,
            piece: split - crate::projection::RECORD_BYTES,
        };
        let mut input_hash = blake3::Hasher::new();
        let count = manifest.chunk_count().max(1);
        for chunk in 0..count {
            let bytes = if manifest.byte_length == 0 {
                Vec::new()
            } else {
                writer.import_chunk(&manifest, chunk * CHUNK as u64)?
            };
            input_hash.update(&bytes);
            let base = chunk * CHUNK as u64;
            let mut cursor = 0;
            for end in record_ends(&manifest.format, &bytes, chunk + 1 == count) {
                layout.feed(&mut manifest, base + cursor as u64, &bytes[cursor..end])?;
                layout.finish_record(&mut manifest, base + end as u64)?;
                cursor = end;
            }
            layout.feed(&mut manifest, base + cursor as u64, &bytes[cursor..])?;
        }
        let Layout {
            rows,
            next_index,
            record_start,
            ..
        } = layout;
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

    /// Reclaim at most 128 published or expired staging units per call,
    /// including at most one private prepared record, and 16 journals.
    /// Each synced batch atomically advances its persistent cleanup cursor.
    pub fn import_reclaim(&self, now: u64) -> Result<u64, Error> {
        let writer = self.lock()?;
        let mut prepared_reclaimed = false;
        let cf = writer
            .db
            .cf_handle("payload")
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "payload family missing"))?;
        let mut batch = WriteBatch::default();
        let mut removed = 0;
        let manifests = writer.imports()?;
        let mut remaining_manifests = manifests.len() as u64;
        for mut manifest in manifests {
            let share = (RECLAIM_CHUNKS - removed) / remaining_manifests;
            remaining_manifests -= 1;
            let published = manifest.published_artifact.is_some();
            let journal_limit = if published {
                removed + share
            } else {
                RECLAIM_CHUNKS
            };
            if (!published && manifest.expires_at > now)
                || (manifest.consumption && manifest.published_artifact.is_none())
            {
                continue;
            }
            // Published preparation contains only transferred catalog copies.
            // Give every journal a finite share so a large completed file cannot
            // prevent small completed journals from releasing admission slots.
            let preparation_done = if (published || !prepared_reclaimed) && share != 0 {
                let (done, reclaimed) =
                    super::publication::reclaim_prepared(&writer, &manifest, share)?;
                prepared_reclaimed |= !published && reclaimed != 0;
                removed += reclaimed;
                done
            } else {
                false
            };
            let end = manifest
                .chunk_count()
                .min(manifest.reclaimed_chunks + journal_limit - removed);
            for chunk in manifest.reclaimed_chunks..end {
                batch.delete_cf(cf, chunk_key(&manifest.id, chunk * CHUNK as u64));
                removed += 1;
            }
            manifest.reclaimed_chunks = end;
            let layout_end = manifest
                .layout_pages()
                .min(manifest.reclaimed_layout_pages + journal_limit - removed);
            for page in manifest.reclaimed_layout_pages..layout_end {
                batch.delete(layout_key(&manifest.id, page));
                removed += 1;
            }
            manifest.reclaimed_layout_pages = layout_end;
            if end == manifest.chunk_count()
                && layout_end == manifest.layout_pages()
                && preparation_done
            {
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

impl Writer {
    fn begin_import(
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
        let writer = self;
        let consumption = writer.require_consumption(request_id, source, format, byte_length)?;
        let id = import_id(request_id);
        if let Some(previous) = super::publication::published(writer, &id, now)? {
            let saved = previous.0;
            if saved.request_id != request_id
                || saved.source != source
                || saved.format != format
                || saved.byte_length != byte_length
                || saved.payload_id != payload_id
            {
                return Err(Error::new(
                    "CONFLICT",
                    "published import request has different declared input",
                ));
            }
            return Ok(saved);
        }
        if writer
            .accepted_ingest(
                request_id,
                source,
                format,
                byte_length,
                payload_id,
                super::ingest::Origin::Ordinary,
                now,
            )?
            .is_some()
        {
            return Err(Error::new(
                "CONFLICT",
                "request already accepted; use import_start to retrieve its receipt",
            ));
        }
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
        if byte_length > MAX_IMPORT_BYTES {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                format!(
                    "declared input of {byte_length} bytes exceeds {MAX_IMPORT_BYTES} bytes (about 26.7 GiB), the most one atomic publication can carry; split the input and import the parts"
                ),
            ));
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
            published_artifact: None,
            consumption,
            reclaimed_layout_pages: 0,
        };
        let mut batch = WriteBatch::default();
        put(&mut batch, &manifest.id, &manifest);
        writer.admit_import_batch(&batch)?;
        writer.commit(batch)?;
        Ok(manifest)
    }
}

/// Streaming record layout. A record longer than `split` bytes is emitted as
/// consecutive raw records: each full piece is `piece` bytes, a whole number of
/// chunks, and the remainder keeps more than `RECORD_BYTES`, so every piece stays
/// raw and none exceeds `split`.
struct Layout<'a> {
    writer: &'a Writer,
    rows: Vec<ImportRecordSpan>,
    next_index: u64,
    record_start: u64,
    /// Digest of the bytes since `record_start`.
    record_hash: blake3::Hasher,
    /// Candidate cut at `record_start + piece`, the digest of the bytes before it,
    /// and the digest of the bytes after it.
    pending: Option<(u64, String, blake3::Hasher)>,
    split: u64,
    piece: u64,
}

fn payload_id(hash: &blake3::Hasher) -> String {
    format!("blake3:{}", hex::encode(hash.finalize().as_bytes()))
}

impl Layout<'_> {
    /// Hash the bytes at artifact offset `at`, all inside the current record.
    fn feed(&mut self, manifest: &mut ImportManifest, at: u64, bytes: &[u8]) -> Result<(), Error> {
        let end = at + bytes.len() as u64;
        let cut = self.record_start + self.piece;
        match &mut self.pending {
            None if at <= cut && cut < end => {
                let (before, after) = bytes.split_at((cut - at) as usize);
                self.record_hash.update(before);
                let id = payload_id(&self.record_hash);
                self.record_hash.update(after);
                let mut tail = blake3::Hasher::new();
                tail.update(after);
                self.pending = Some((cut, id, tail));
            }
            None => {
                self.record_hash.update(bytes);
            }
            Some((_, _, tail)) => {
                self.record_hash.update(bytes);
                tail.update(bytes);
            }
        }
        if end - self.record_start > self.split {
            let (cut, id, tail) = self
                .pending
                .take()
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "raw record split lost its cut"))?;
            self.emit(manifest, cut, id)?;
            self.record_hash = tail;
        }
        Ok(())
    }

    /// Close the current record, which `feed` has kept within `split` bytes.
    fn finish_record(&mut self, manifest: &mut ImportManifest, byte_end: u64) -> Result<(), Error> {
        self.pending = None;
        let id = payload_id(&self.record_hash);
        self.emit(manifest, byte_end, id)?;
        self.record_hash = blake3::Hasher::new();
        Ok(())
    }

    fn emit(
        &mut self,
        manifest: &mut ImportManifest,
        byte_end: u64,
        payload_id: String,
    ) -> Result<(), Error> {
        if self.next_index >= self.writer.config.max_records as u64 {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "import layout exceeds configured record count; raw staging remains available for cancellation",
            ));
        }
        self.rows.push(ImportRecordSpan {
            index: self.next_index,
            byte_start: self.record_start,
            byte_end,
            payload_id,
        });
        self.record_start = byte_end;
        self.next_index += 1;
        if self.rows.len() == IMPORT_LAYOUT_PAGE_RECORDS {
            self.writer
                .commit_layout_page(manifest, &self.rows, false)?;
            self.rows.clear();
        }
        Ok(())
    }
}

/// Shared immutable JSON page codec for record layouts and prepared raw payloads.
pub(super) fn encode_page(value: &Value) -> Result<Vec<u8>, Error> {
    let body = json::write_compact(value);
    if body.len() + 32 > LAYOUT_BYTES {
        return Err(Error::new("RESOURCE_LIMIT", "import page exceeds 32 KiB"));
    }
    let mut encoded = Vec::with_capacity(32 + body.len());
    encoded.extend_from_slice(blake3::hash(body.as_bytes()).as_bytes());
    encoded.extend_from_slice(body.as_bytes());
    Ok(encoded)
}

pub(super) fn decode_page(bytes: &[u8]) -> Result<Value, Error> {
    if !(32..=LAYOUT_BYTES).contains(&bytes.len())
        || blake3::hash(&bytes[32..]).as_bytes() != &bytes[..32]
    {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "import page size or digest mismatch",
        ));
    }
    parse(&bytes[32..])
}
