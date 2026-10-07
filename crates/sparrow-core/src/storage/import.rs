// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Durable raw staging, separate from accepted evidence and derived indexes.

use super::{
    CHUNK, Catalog, Error, RETRY_WINDOW, Store, Writer, blake3, hex, integer, parse, put,
    read_field, validate_ingest_labels,
};
use purrdf_lex::json::{Object, Value};
use rocksdb::WriteBatch;

/// Fixed raw staging unit; only the final unit may be shorter.
pub const IMPORT_CHUNK_BYTES: usize = CHUNK;
const MAX_IMPORTS: usize = 16;
const MANIFEST_BYTES: usize = 8192;
const RECLAIM_CHUNKS: u64 = 128;

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
    reclaimed_chunks: u64,
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
            .with("reclaimed_chunks", self.reclaimed_chunks.to_string())
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
        };
        validate_ingest_labels(&manifest.request_id, &manifest.source, &manifest.format)
            .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
        if manifest.id != import_id(&manifest.request_id)
            || !valid_payload_id(&manifest.payload_id)
            || manifest.received_bytes > manifest.byte_length
            || (manifest.received_bytes != manifest.byte_length
                && !manifest.received_bytes.is_multiple_of(CHUNK as u64))
            || manifest.reclaimed_chunks > manifest.chunk_count()
            || (manifest.sealed && manifest.received_bytes != manifest.byte_length)
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
    fn require_live(&self, now: u64) -> Result<(), Error> {
        if now >= self.expires_at || self.reclaimed_chunks != 0 {
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

    /// Reclaim at most 128 expired chunks and 16 journal entries per call.
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
            if end == manifest.chunk_count() {
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
