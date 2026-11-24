// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Durable raw staging, separate from accepted evidence and derived indexes.

use super::eviction::RetirementShare;
use super::{
    BYTE_READ_BYTES, CHUNK, Catalog, Error, RETRY_WINDOW, Store, Writer, blake3, hex, integer,
    json, parse, put, read_chunk_range, read_field, record_ends, validate_ingest_labels,
};
use purrdf_lex::json::{Object, Value};
use rocksdb::{DB, WriteBatch};
use sparrow_limits::MAX_PUBLICATION_RECORDS;

/// Fixed raw staging unit; only the final unit may be shorter.
pub const IMPORT_CHUNK_BYTES: usize = CHUNK;
/// Default total and the anonymous TCP limit on starting new journals.
pub const DEFAULT_IMPORT_JOURNALS: usize = 16;
/// Maximum supported total. The writer can reclaim this many rows at either admission setting.
pub const MAX_IMPORT_JOURNALS: usize = 20;
const IMPORT_PREFIX: &[u8] = b"import:";

/// Which part of the import journal registry a new request may use.
#[derive(Clone, Copy)]
pub enum ImportAdmission {
    /// Anonymous TCP may start a journal while total occupancy is below sixteen.
    AnonymousTcp,
    /// An authenticated or local caller may use the configured total.
    WithReserve,
}

/// Validate a configured import journal capacity before opening a store.
pub fn validate_import_journal_limit(limit: usize) -> Result<(), Error> {
    if matches!(limit, DEFAULT_IMPORT_JOURNALS | MAX_IMPORT_JOURNALS) {
        Ok(())
    } else {
        Err(Error::new(
            "INVALID_ARGUMENT",
            "max_import_journals must be 16 or 20",
        ))
    }
}

/// The import bound a store advertises and the over-bound refusals it has counted. One
/// instance is shared by the writer, which enforces the bound, and `Store::properties`,
/// which reports both without taking the writer lock. The count lives for the process (#280).
///
/// It also holds the two values a part cut needs and the layout enforces (#280): the raw
/// split, above which a record is laid out as raw pieces, and the retirement row budget of
/// [`retire_row_budget`]. The layout reads the split here, so what is advertised is what is
/// enforced, test hooks included.
#[derive(Debug)]
pub(super) struct ImportLedger {
    bound: std::sync::atomic::AtomicU64,
    refusals: std::sync::atomic::AtomicU64,
    raw_split: std::sync::atomic::AtomicU64,
    row_budget: std::sync::atomic::AtomicU64,
}

impl Default for ImportLedger {
    fn default() -> Self {
        Self {
            bound: std::sync::atomic::AtomicU64::new(MAX_IMPORT_BYTES),
            refusals: std::sync::atomic::AtomicU64::new(0),
            raw_split: std::sync::atomic::AtomicU64::new(RAW_RECORD_BYTES),
            row_budget: std::sync::atomic::AtomicU64::new(retire_row_budget(
                RetirementShare::DEFAULT,
            ) as u64),
        }
    }
}

impl ImportLedger {
    pub(super) fn bound(&self) -> u64 {
        self.bound.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Longest record a layout keeps whole; a longer one is laid out as raw pieces.
    pub(super) fn raw_split(&self) -> u64 {
        self.raw_split.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// What one artifact's chunk and record charges may sum to; see [`retire_row_budget`].
    pub(super) fn row_budget(&self) -> u64 {
        self.row_budget.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(feature = "test-support")]
    fn set_bound(&self, bound: u64) {
        self.bound
            .store(bound, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(feature = "test-support")]
    fn set_raw_split(&self, bytes: u64) {
        self.raw_split
            .store(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(feature = "test-support")]
    fn set_row_budget(&self, budget: u64) {
        self.row_budget
            .store(budget, std::sync::atomic::Ordering::Relaxed);
    }

    pub(super) fn refusals(&self) -> u64 {
        self.refusals.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Count one over-bound declaration and build its permanent refusal.
    fn refuse(&self, message: String) -> Error {
        self.refusals
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Error::new("INVALID_ARGUMENT", message)
    }
}
/// The `RESOURCE_LIMIT` message of a begin-import that finds every journal slot occupied. The
/// daemon waits for a slot on exactly this refusal (#246); no other refusal is one.
pub const IMPORT_JOURNALS_OCCUPIED: &str =
    "16 import journals are occupied; resume, cancel, or reclaim them";
const EXPANDED_JOURNALS_OCCUPIED: &str =
    "20 import journals are occupied; resume, cancel, or reclaim them";

/// Whether a begin-import refusal can clear when a journal is reclaimed.
pub fn import_journals_occupied(error: &Error) -> bool {
    error.code == "RESOURCE_LIMIT"
        && matches!(
            error.message.as_str(),
            IMPORT_JOURNALS_OCCUPIED | EXPANDED_JOURNALS_OCCUPIED
        )
}
const MANIFEST_BYTES: usize = 8192;
const RECLAIM_CHUNKS: u64 = 128;
/// Maximum record spans in one immutable layout page.
pub const IMPORT_LAYOUT_PAGE_RECORDS: usize = 128;
const LAYOUT_BYTES: usize = 32 * 1024;
/// Longest record a layout keeps whole: 2048 chunks (128 MiB). A longer record is
/// laid out as consecutive raw records, so each chunk manifest fits one retirement
/// pass and one catalog read while the artifact still reads back byte-exact.
pub const RAW_RECORD_BYTES: u64 = 2048 * CHUNK as u64;
/// Chunks one atomic publication batch carries. `import_publish` writes every
/// chunk identity four times (record row, artifact row, saved receipt and ingest
/// receipt) within 128 MiB, less 16 MiB for the fixed rows.
const PUBLICATION_CHUNKS: usize = (super::publication::PUBLICATION_BYTES - 16 * 1024 * 1024)
    / (4 * super::manifest_retire::ROW_CHUNK_BYTES);

/// Chunks whose artifact row one retirement pass under `share` can read, counting
/// a record identity for every chunk, as an artifact of one-chunk records has.
pub(super) const fn envelope_chunks(share: RetirementShare) -> usize {
    use super::manifest_retire::{ROW_CHUNK_BYTES, ROW_RECORD_BYTES, envelope_charge};
    let budget = share.envelope_budget();
    let fixed = envelope_charge(0, 0, share.limits.max_payload_refs);
    if budget <= fixed {
        0
    } else {
        (budget - fixed) / (2 * (ROW_CHUNK_BYTES + ROW_RECORD_BYTES))
    }
}

/// Charge of one chunk identity against [`retire_row_budget`]: the artifact row holds it
/// once and a retirement pass reserves the row twice.
pub(super) const RETIRE_CHUNK_CHARGE: u64 = 2 * super::manifest_retire::ROW_CHUNK_BYTES as u64;
/// Charge of one record identity against [`retire_row_budget`].
pub(super) const RETIRE_RECORD_CHARGE: u64 = 2 * super::manifest_retire::ROW_RECORD_BYTES as u64;

/// What the chunk and record charges of one artifact may sum to under `share`.
/// [`Writer::require_retirable`] refuses exactly when
/// `RETIRE_CHUNK_CHARGE * chunks + RETIRE_RECORD_CHARGE * records` exceeds it, because the
/// envelope charge is affine in both counts; zero when the fixed charge alone fills the
/// envelope, as [`envelope_chunks`] treats it.
pub(super) const fn retire_row_budget(share: RetirementShare) -> usize {
    use super::manifest_retire::envelope_charge;
    let budget = share.envelope_budget();
    let fixed = envelope_charge(0, 0, share.limits.max_payload_refs);
    budget.saturating_sub(fixed)
}

/// Chunks an import may declare: the smaller of the publication batch and the
/// retirement envelope under `share`.
const fn admitted_chunks(share: RetirementShare) -> usize {
    let envelope = envelope_chunks(share);
    if envelope < PUBLICATION_CHUNKS {
        envelope
    } else {
        PUBLICATION_CHUNKS
    }
}

/// Largest input `import_begin` admits. Expiry and eviction read an artifact's
/// whole catalog row in one pass, within the 32 MiB evidence budget less the 4 MiB
/// record row reserve; pin capture has its own budget. That is the binding bound:
/// 129,935 chunks, about 7.93 GiB. The four chunk copies of publication fit at this
/// size. The v2 store format (#5) replaces both the whole-row read and the single
/// publication batch.
pub const MAX_IMPORT_BYTES: u64 = admitted_chunks(RetirementShare::DEFAULT) as u64 * CHUNK as u64;

const _: () = {
    use super::manifest_retire::envelope_charge;
    let share = RetirementShare::DEFAULT;
    let chunks = admitted_chunks(share);
    let take = share.limits.max_payload_refs;
    let budget = share.envelope_budget();
    // The admitted artifact's row fits one pass, and one chunk more would not.
    assert!(envelope_charge(chunks, chunks, take) <= budget);
    assert!(chunks < PUBLICATION_CHUNKS || envelope_charge(chunks + 1, chunks + 1, take) > budget);
    // The envelope and the record row reserve together fit the evidence budget.
    assert!(budget + share.record_reserve <= share.limits.max_catalog_bytes);
    // Its four copies fit the publication batch.
    assert!(chunks <= PUBLICATION_CHUNKS);
    assert!(
        16 * 1024 * 1024 + chunks * 4 * super::manifest_retire::ROW_CHUNK_BYTES
            <= super::publication::PUBLICATION_BYTES
    );
    assert!(RAW_RECORD_BYTES == 2048 * CHUNK as u64);
};

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
    /// Endpoint and caller of the connection that began the import; it binds the import.
    pub intake: Option<super::intake::Intake>,
    reclaimed_chunks: u64,
    reclaimed_layout_pages: u64,
}

impl Catalog for ImportManifest {
    fn encode(&self) -> Value {
        let object = Object::new()
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
            );
        match &self.intake {
            Some(intake) => object.with("intake", intake.encode()).into(),
            None => object.into(),
        }
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
            intake: super::intake::decode_member(value)?,
        };
        validate_ingest_labels(&manifest.request_id, &manifest.source, &manifest.format)
            .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
        let lines = ["jsonl", "log"].contains(&manifest.format.as_str());
        if manifest.id != import_id(&manifest.request_id)
            || manifest.consumption != manifest.request_id.starts_with("consume:")
            || (manifest.consumption && manifest.intake.is_some())
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

pub(super) fn valid_payload_id(id: &str) -> bool {
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

type JournalRow = (Box<[u8]>, Box<[u8]>);

fn import_rows(db: &DB) -> impl Iterator<Item = Result<JournalRow, Error>> + '_ {
    db.prefix_iterator(IMPORT_PREFIX)
        .take_while(|row| match row {
            Ok((key, _)) => key.starts_with(IMPORT_PREFIX),
            Err(_) => true,
        })
        .enumerate()
        .map(|(index, row)| {
            let (key, bytes) = row?;
            if index >= MAX_IMPORT_JOURNALS || bytes.len() > MANIFEST_BYTES {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "import registry exceeds its admitted bounds",
                ));
            }
            Ok((key, bytes))
        })
}

impl Writer {
    fn imports(&self) -> Result<Vec<ImportManifest>, Error> {
        import_rows(&self.db)
            .map(|row| {
                let (key, bytes) = row?;
                let manifest = ImportManifest::decode(&parse(&bytes)?)?;
                if key.as_ref() != manifest.id.as_bytes() {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "import registry key mismatch",
                    ));
                }
                Ok(manifest)
            })
            .collect()
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
            .map_err(Error::from)
    }

    /// Largest declared input: `MAX_IMPORT_BYTES` under production retirement limits.
    fn import_bytes_bound(&self) -> u64 {
        admitted_chunks(self.retirement.get()) as u64 * CHUNK as u64
    }

    /// Refuse a declaration above the import bound, permanently. This is the one place
    /// an over-bound declaration is refused and counted.
    fn require_within_import_bound(&self, byte_length: u64) -> Result<(), Error> {
        let admitted = self.import_bytes_bound();
        if byte_length > admitted {
            return Err(self.import_ledger.refuse(format!(
                "declared input of {byte_length} bytes exceeds the import bound of {admitted} bytes (about {:.2} GiB), the most one retirement pass can read back as an artifact; split the input into parts of at most {admitted} bytes, import each part as its own request, then import the manifest",
                admitted as f64 / (1u64 << 30) as f64
            )));
        }
        if byte_length > self.config.disk_budget - self.config.maintenance_reserve {
            return Err(Error::input_bound(format!(
                "declared input exceeds the entire available disk budget of {} bytes",
                self.config.disk_budget - self.config.maintenance_reserve
            )));
        }
        Ok(())
    }

    /// Refuse an artifact whose catalog row one retirement pass could not read. The
    /// declared-size bound covers documents; this covers many-record artifacts.
    pub(super) fn require_retirable(&self, chunks: usize, records: usize) -> Result<(), Error> {
        let share = self.retirement.get();
        if super::manifest_retire::envelope_charge(chunks, records, share.limits.max_payload_refs)
            > share.envelope_budget()
        {
            return Err(Error::input_bound(format!(
                "an artifact of {chunks} chunks and {records} records exceeds what one retirement pass can read; split the input and import the parts"
            )));
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
    /// Count existing journals with a bounded catalog iterator without taking the writer lock.
    pub fn import_journal_occupancy(&self) -> Result<usize, Error> {
        import_rows(&self.unlocked.db).try_fold(0, |count, row| row.map(|_| count + 1))
    }

    /// Configured ceiling for callers that can use the private reserve.
    pub fn max_import_journals(&self) -> usize {
        self.unlocked.config.max_import_journals
    }

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
        self.lock()?.begin_import(
            request_id,
            source,
            format,
            byte_length,
            payload_id,
            None,
            ImportAdmission::WithReserve,
            now,
        )
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
        // The ledger is what the layout enforces and what the store advertises.
        self.lock()?.import_ledger.set_raw_split(bytes);
        Ok(())
    }

    /// Size this open store's import bound and pin budget from scaled retirement
    /// `limits` and record row reserve, so a test can reach the budgets without
    /// multi-GiB inputs. Expiry and eviction must then run with the same limits.
    /// This entry point is absent unless the dev-only test-support feature is selected.
    #[cfg(feature = "test-support")]
    pub fn scale_retirement_for_tests(
        &self,
        limits: super::eviction::EvictionLimits,
        record_reserve: usize,
    ) -> Result<super::eviction::RetirementBounds, Error> {
        limits.validate()?;
        if record_reserve >= limits.max_catalog_bytes {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "record row reserve must leave part of the evidence budget",
            ));
        }
        let writer = self.lock()?;
        let share = RetirementShare {
            limits,
            record_reserve,
        };
        writer.retirement.set(share);
        writer.import_ledger.set_bound(writer.import_bytes_bound());
        writer
            .import_ledger
            .set_row_budget(retire_row_budget(share) as u64);
        Ok(super::eviction::RetirementBounds {
            import_bytes: writer.import_bytes_bound(),
            pins: share.max_pins(),
            pinned_members: share.max_pinned_members(),
        })
    }

    /// Recover a receipt or start or resume staging with its initiating intake.
    /// Published artifacts and records carry that intake. See [`Store::import_admit`].
    #[expect(
        clippy::too_many_arguments,
        reason = "the declared import, its intake binding and the acceptance clock"
    )]
    pub fn import_start(
        &self,
        request_id: &str,
        source: &str,
        format: &str,
        byte_length: u64,
        payload_id: &str,
        intake: Option<super::intake::Intake>,
        now: u64,
    ) -> Result<ImportStart, Error> {
        self.import_start_with_admission(
            request_id,
            source,
            format,
            byte_length,
            payload_id,
            intake,
            ImportAdmission::WithReserve,
            now,
        )
    }

    /// Recover a receipt or start or resume staging under the caller's journal admission class.
    #[expect(
        clippy::too_many_arguments,
        reason = "the declared import, caller admission, intake binding and acceptance clock"
    )]
    pub fn import_start_with_admission(
        &self,
        request_id: &str,
        source: &str,
        format: &str,
        byte_length: u64,
        payload_id: &str,
        intake: Option<super::intake::Intake>,
        admission: ImportAdmission,
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
            intake.as_ref(),
            now,
        )? {
            return Ok(ImportStart::Accepted(
                writer.receipt(receipt, super::durability::Durability::Durable)?,
            ));
        }
        writer
            .begin_import(
                request_id,
                source,
                format,
                byte_length,
                payload_id,
                intake,
                admission,
                now,
            )
            .map(ImportStart::Staging)
    }

    pub(super) fn import_operation<'a>(
        &'a self,
        id: &'a str,
        now: u64,
    ) -> Result<ImportOperation<'a>, Error> {
        Ok(ImportOperation {
            writer: self.lock()?,
            store: self,
            id,
            now,
        })
    }

    /// Check source and caller admission while holding the operation's writer lock.
    pub fn authorize_import<'a>(
        &'a self,
        id: &'a str,
        caller: Option<&super::intake::Intake>,
        now: u64,
        admit_source: impl FnOnce(&str) -> Result<(), Error>,
    ) -> Result<ImportOperation<'a>, Error> {
        let operation = self.import_operation(id, now)?;
        if id.starts_with("import:") {
            let manifest = match super::publication::published(&operation.writer, id, now)? {
                Some((manifest, _)) => Some(manifest),
                None => operation
                    .writer
                    .db
                    .get(id)?
                    .map(|bytes| ImportManifest::decode(&parse(&bytes)?))
                    .transpose()?,
            };
            if let Some(manifest) = manifest {
                admit_source(&manifest.source)?;
                super::intake::admit_import(manifest.intake.as_ref(), caller)?;
            }
        }
        Ok(operation)
    }

    /// Refuse a caller other than the one that began this import.
    /// Use `authorize_import` to keep the lock through an operation.
    pub fn import_admit(
        &self,
        id: &str,
        caller: Option<&super::intake::Intake>,
        now: u64,
    ) -> Result<(), Error> {
        self.authorize_import(id, caller, now, |_| Ok(())).map(drop)
    }

    /// Recover the committed offset and fixed lease after a client/daemon restart.
    pub fn import_get(&self, id: &str, now: u64) -> Result<ImportManifest, Error> {
        self.import_operation(id, now)?.get()
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
        self.import_operation(id, now)?.append(offset, bytes)
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
            return Err(Error::input_bound(
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
        self.import_operation(id, now)?.seal()
    }

    /// End a staging lease durably. Physical cleanup is bounded and resumable.
    pub fn import_cancel(&self, id: &str, now: u64) -> Result<(), Error> {
        self.import_operation(id, now)?.cancel()
    }

    /// Durably prepare exact record spans with bounded memory. Repeated or
    /// interrupted attempts rebuild deterministic private pages, never occurrences.
    /// This is preparation for publication, not an acceptance/deletion receipt.
    pub fn import_prepare_layout(&self, id: &str, now: u64) -> Result<ImportManifest, Error> {
        self.import_operation(id, now)?.prepare_layout()
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
    /// including at most one private prepared record. A pass examines at most twenty journals.
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
        let mut journals_freed = 0u64;
        // An eligible journal this pass rewrote instead of deleting still has work to free.
        let mut work_left = false;
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
            // An unpublished consumption import is kept while its claim may still publish.
            // Once an administrator retires the claim, the retirement record is the durable
            // mark that releases it, through this same path, as an expired lease (D118).
            // A claim recorded as a part set publishes through its manifest, never through
            // this row, so the row a single-path attempt began is released at once (#280).
            // A claim whose set manifest receipt is gone is not provably a set and keeps it.
            let set_leftover = manifest.consumption
                && !published
                && super::consumption::recorded_as_set(&writer, &manifest.id)?;
            if !set_leftover
                && ((!published && manifest.expires_at > now)
                    || (manifest.consumption
                        && !published
                        && !super::consumption::retired(&writer, &manifest.id)?))
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
                journals_freed += 1;
            } else {
                work_left = true;
                put(&mut batch, &manifest.id, &manifest);
            }
        }
        if batch.size_in_bytes() != 0 {
            writer.commit(batch)?;
        }
        drop(writer);
        // The slot is free only once the commit that deleted its journal has landed.
        if journals_freed != 0 {
            self.wake_import_slot();
        }
        // A pass bounds its work (128 chunks, or one private prepared record of an unpublished
        // import) and rewrites the journal it did not finish: ask for the next pass by event,
        // not by the duty's interval. Only a pass that freed something asks, so a journal
        // that can make no progress never spins; a pass that deleted every eligible journal
        // asks for none.
        if work_left && removed > 0 {
            self.wake_import_reclaim();
        }
        Ok(removed)
    }
}

/// An import operation under an exclusive store writer lock.
pub struct ImportOperation<'a> {
    pub(super) store: &'a Store,
    pub(super) writer: std::sync::MutexGuard<'a, Writer>,
    pub(super) id: &'a str,
    pub(super) now: u64,
}

impl ImportOperation<'_> {
    /// Get under the admitted writer lock.
    pub fn get(self) -> Result<ImportManifest, Error> {
        let Self {
            writer, id, now, ..
        } = self;
        if !id.starts_with("import:") {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "require an import staging ID",
            ));
        }
        if let Some((manifest, _)) = super::publication::published(&writer, id, now)? {
            return Ok(manifest);
        }
        let manifest: ImportManifest = writer.get(id)?;
        manifest.require_live(now)?;
        Ok(manifest)
    }

    /// Append under the admitted writer lock.
    pub fn append(self, offset: u64, bytes: &[u8]) -> Result<ImportManifest, Error> {
        let Self {
            writer, id, now, ..
        } = self;
        if bytes.is_empty() || bytes.len() > CHUNK || !offset.is_multiple_of(CHUNK as u64) {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "require a nonempty chunk of at most 64 KiB at an aligned offset",
            ));
        }
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

    /// Seal under the admitted writer lock.
    pub fn seal(self) -> Result<ImportManifest, Error> {
        let Self {
            writer, id, now, ..
        } = self;
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

    /// Cancel under the admitted writer lock.
    pub fn cancel(self) -> Result<(), Error> {
        let Self {
            store,
            writer,
            id,
            now,
        } = self;
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
        writer.commit(batch)?;
        drop(writer);
        // The lease has ended: the journal is reclaimable now, so tell the reclamation duty.
        store.wake_import_reclaim();
        Ok(())
    }

    /// Prepare layout under the admitted writer lock.
    pub fn prepare_layout(self) -> Result<ImportManifest, Error> {
        let Self {
            writer, id, now, ..
        } = self;
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
        let split = writer.import_ledger.raw_split();
        let mut layout = Layout {
            writer: &writer,
            rows: Vec::with_capacity(IMPORT_LAYOUT_PAGE_RECORDS),
            next_index: 0,
            record_start: 0,
            record_hash: blake3::Hasher::new(),
            pending: None,
            split,
            piece: split - crate::projection::RECORD_BYTES,
            chunks: 0,
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
            chunks,
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
        // Refuse a many-record artifact here, before publication work, rather than
        // only when it is published; the publish-time check remains for layouts that
        // completed earlier.
        let records = usize::try_from(next_index)
            .map_err(|_| Error::new("RESOURCE_LIMIT", "import layout record count overflow"))?;
        writer.require_retirable(chunks, records)?;
        writer.commit_layout_page(&mut manifest, &rows, true)?;
        Ok(manifest)
    }
}

impl Writer {
    /// End the staging lease of a retired claim's unpublished consumption import, if one
    /// began, in the retirement's own batch. [`Store::import_reclaim`] then frees its staged
    /// chunks, private preparation and journal slot as for any expired lease.
    pub(super) fn end_retired_consumption_import(
        &self,
        batch: &mut WriteBatch,
        id: &str,
        now: u64,
    ) -> Result<(), Error> {
        if self.db.get(id)?.is_none() {
            return Ok(());
        }
        let mut manifest: ImportManifest = self.get(id)?;
        if !manifest.consumption || manifest.published_artifact.is_some() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "a retired claim's import is not its unpublished consumption staging",
            ));
        }
        manifest.expires_at = manifest.expires_at.min(now);
        put(batch, id, &manifest);
        Ok(())
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the declared import, caller admission, intake binding and acceptance clock"
    )]
    fn begin_import(
        &self,
        request_id: &str,
        source: &str,
        format: &str,
        byte_length: u64,
        payload_id: &str,
        intake: Option<super::intake::Intake>,
        admission: ImportAdmission,
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
        if consumption && intake.is_some() {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "consumption imports belong to the daemon's watches, not to an ingest-socket caller",
            ));
        }
        // A set part is the daemon's too: a squatted part would block or forge a set (#280).
        if request_id.starts_with(super::consumption::SET_PREFIX) && intake.is_some() {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                super::consumption::SET_INTAKE_REFUSAL,
            ));
        }
        // A claim publishes as one artifact or as a set, never both (#285).
        if let Some(claim) = super::consumption::part_claim_import(request_id)
            && let Some(journal) = writer.consumption(&claim)?
            && journal.published_artifact.is_some()
            && matches!(
                writer.set_manifest(&journal)?,
                super::consumption::SetManifest::Absent
            )
        {
            return Err(Error::new(
                "CONFLICT",
                format!(
                    "claim {} is published as one artifact; set part {request_id} is refused",
                    journal.import_id
                ),
            ));
        }
        let id = import_id(request_id);
        let mut part_gone = false;
        let previous = match super::publication::published_admitted(writer, &id, now, None)? {
            super::publication::Published::Saved(manifest, artifact) => Some((manifest, *artifact)),
            super::publication::Published::PartGone(manifest) => {
                // Later parts start where this one ends, so the same cut is sent again.
                part_gone = true;
                if manifest.request_id != request_id
                    || manifest.source != source
                    || manifest.format != format
                    || manifest.byte_length != byte_length
                    || manifest.payload_id != payload_id
                {
                    return Err(Error::new(
                        "CONFLICT",
                        "published import request has different declared input",
                    ));
                }
                None
            }
            super::publication::Published::None => None,
        };
        if let Some(previous) = previous {
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
            super::intake::admit_import(saved.intake.as_ref(), intake.as_ref())?;
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
                intake.as_ref(),
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
            super::intake::admit_import(previous.intake.as_ref(), intake.as_ref())?;
            if part_gone && previous.published_artifact.is_some() {
                return Err(Error::new(
                    "IMPORT_EXPIRED",
                    format!(
                        "set part {request_id}'s earlier publication row awaits reclamation; the part begins again after it"
                    ),
                ));
            }
            previous.require_live(now)?;
            return Ok(previous);
        }
        writer.require_within_import_bound(byte_length)?;
        let ceiling = match admission {
            ImportAdmission::AnonymousTcp => DEFAULT_IMPORT_JOURNALS,
            ImportAdmission::WithReserve => writer.config.max_import_journals,
        };
        if writer.imports()?.len() >= ceiling {
            let message = if ceiling == DEFAULT_IMPORT_JOURNALS {
                IMPORT_JOURNALS_OCCUPIED
            } else {
                EXPANDED_JOURNALS_OCCUPIED
            };
            return Err(Error::new("RESOURCE_LIMIT", message));
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
            intake,
            reclaimed_layout_pages: 0,
        };
        let mut batch = WriteBatch::default();
        put(&mut batch, &manifest.id, &manifest);
        writer.admit_import_batch(&batch)?;
        if intake.is_some() {
            super::intake::fence(writer)?;
        }
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
    /// Payload chunks of the records emitted so far, as publication will store them.
    chunks: usize,
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
            return Err(Error::input_bound(format!(
                "import layout exceeds the configured max_records of {}; raw staging remains available for cancellation",
                self.writer.config.max_records
            )));
        }
        // The publication's atomic source-context update admits this many distinct records.
        // Refusing here costs the layout read, not the preparation of every record (#397).
        if self.next_index >= MAX_PUBLICATION_RECORDS as u64 {
            return Err(Error::input_bound(format!(
                "import of more than {MAX_PUBLICATION_RECORDS} records in one publication exceeds metadata admission; split the input into parts of at most that many records"
            )));
        }
        let length = usize::try_from(byte_end - self.record_start)
            .map_err(|_| Error::new("RESOURCE_LIMIT", "import record length overflow"))?;
        self.chunks += length.div_ceil(CHUNK);
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
