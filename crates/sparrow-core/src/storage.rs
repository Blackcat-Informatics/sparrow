// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Serialized catalog mutations and reference-aware payload ownership.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex};

use purrdf_hash::{blake3, hex};
use purrdf_lex::json::record::{DecodeError, Record as JsonRecord};
use purrdf_lex::json::{self, Object, Value};
use rocksdb::{
    ColumnFamilyDescriptor, DB, DBCompressionType, IteratorMode, Options, WriteBatch, WriteOptions,
};

pub(crate) mod context;
pub(crate) mod durability;
pub(crate) mod import;
mod ingest;
pub(crate) mod pin_read;
pub(crate) mod publication;
mod query_membership;
mod rdf_index;
pub(crate) mod rdf_view;
mod record_fields;
mod retire;
pub(crate) mod source_time;
mod text_index;

const FORMAT: &[u8] = b"sparrow.store/5";
const DAY: u64 = 86_400_000;
const RETRY_WINDOW: u64 = 2 * DAY;
const CHUNK: usize = 64 * 1024;
const BYTE_READ_BYTES: usize = 1024 * 1024;
const CATALOG_STRING_BYTES: usize = 16 * 1024 * 1024;

fn read_chunk_range(
    chunks: impl IntoIterator<Item = Result<Vec<u8>, Error>>,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, Error> {
    let end = offset
        .checked_add(length as u64)
        .ok_or_else(|| Error::new("INVALID_ARGUMENT", "byte range overflow"))?;
    let mut position = 0_u64;
    let mut result = Vec::with_capacity(length);
    for chunk in chunks {
        let chunk = chunk?;
        let next = position
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "manifest byte count overflow"))?;
        if next > offset && position < end {
            result.extend_from_slice(
                &chunk
                    [offset.saturating_sub(position) as usize..(end.min(next) - position) as usize],
            );
        }
        position = next;
        if position >= end {
            break;
        }
    }
    if result.len() != length {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "manifest does not cover the requested bytes",
        ));
    }
    Ok(result)
}

fn validate_ingest_labels(request_id: &str, source: &str, format: &str) -> Result<(), Error> {
    if request_id.is_empty() || request_id.len() > 128 || source.is_empty() || source.len() > 512 {
        return Err(Error::new(
            "INVALID_ARGUMENT",
            "request_id and source must be bounded nonempty strings",
        ));
    }
    if !["jsonl", "log", "json", "markdown", "opaque"].contains(&format) {
        return Err(Error::new(
            "INVALID_ARGUMENT",
            "format must be jsonl, log, json, markdown, or opaque",
        ));
    }
    Ok(())
}

fn record_ends<'a>(
    format: &str,
    bytes: &'a [u8],
    complete: bool,
) -> impl Iterator<Item = usize> + 'a {
    let lines = ["jsonl", "log"].contains(&format);
    bytes
        .iter()
        .enumerate()
        .filter_map(move |(index, byte)| (lines && *byte == b'\n').then_some(index + 1))
        .chain(
            (complete && (!lines || (!bytes.is_empty() && bytes.last() != Some(&b'\n'))))
                .then_some(bytes.len()),
        )
}

/// Typed failures from admission, evidence lifetime, and storage.
#[derive(Debug)]
pub struct Error {
    /// Stable machine-readable refusal code.
    pub code: &'static str,
    /// Actionable diagnostic.
    pub message: String,
}

impl Error {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for Error {}

impl purrdf_lex::json::record::ToJson for Error {
    fn to_json(&self) -> Value {
        Object::new()
            .with("code", self.code)
            .with("message", self.message.clone())
            .with("retryable", self.code == "STORAGE_PRESSURE")
            .into()
    }
}

impl From<rocksdb::Error> for Error {
    fn from(error: rocksdb::Error) -> Self {
        Self::new("STORAGE_FAILURE", error.to_string())
    }
}

/// Explicit, bounded storage admission configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// Intended WAL synchronization interval, 1..=60000 milliseconds.
    pub buffered_flush_interval_ms: u32,
    /// Physical database budget including maintenance reserve.
    pub disk_budget: u64,
    /// Capacity withheld for compaction and control mutations.
    pub maintenance_reserve: u64,
    /// Maximum bytes in a single bounded ingestion operation.
    pub max_artifact_bytes: usize,
    /// Maximum records published by one ingestion operation.
    pub max_records: usize,
    /// Maximum direct members in one pin collection.
    pub max_pin_members: usize,
    /// Resolved standard native analysis law, shared across index and query callers.
    pub text_analyzer: purrdf::text::Analyzer,
}

impl Config {
    fn validate(&self) -> Result<(), Error> {
        if !(1..=60000).contains(&self.buffered_flush_interval_ms) {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "buffered_flush_interval_ms must be 1..60000",
            ));
        }
        if self.maintenance_reserve < 1024 * 1024
            || self.disk_budget <= self.maintenance_reserve
            || self.max_artifact_bytes == 0
            || self.max_records == 0
            || self.max_pin_members == 0
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "supply positive operation limits and a disk budget exceeding a maintenance reserve of at least 1 MiB",
            ));
        }
        Ok(())
    }
}

/// A complete artifact occurrence and its source provenance.
#[derive(Clone, Debug)]
pub struct Artifact {
    /// Present only while this current-session acceptance is not synchronized.
    /// Catalog encoding excludes this ephemeral receipt state.
    pub buffered: Option<durability::DurabilityWatermark>,
    /// Stable store-local occurrence identity.
    pub id: String,
    /// Logical source label, independent of payload identity.
    pub source: String,
    /// Explicit interpretation hint.
    pub format: String,
    /// Server acceptance in Unix milliseconds.
    pub received_at: u64,
    /// Ordinary retention deadline in Unix milliseconds.
    pub expires_at: u64,
    /// Full original byte count.
    pub byte_length: u64,
    /// Full BLAKE3 digest of the original bytes.
    pub payload_id: String,
    /// Addressable records in source order.
    pub record_ids: Vec<String>,
    chunks: Vec<String>,
    evicted: bool,
}

/// One independent record occurrence, retaining its own bytes after parent expiry.
#[derive(Clone, Debug)]
pub struct Record {
    /// Stable occurrence identity.
    pub id: String,
    /// Parent artifact identity.
    pub artifact_id: String,
    /// Original source label.
    pub source: String,
    /// Server acceptance in Unix milliseconds.
    pub received_at: u64,
    /// Ordinary retention deadline in Unix milliseconds.
    pub expires_at: u64,
    /// Half-open byte start in the original artifact.
    pub byte_start: u64,
    /// Half-open byte end, including the original delimiter.
    pub byte_end: u64,
    /// Shared, versioned native JSON search projection, or absent for other formats.
    pub projection_id: String,
    /// Shared complete native RDF graph/state identity, absent for non-projectable formats.
    pub rdf_projection_id: String,
    /// Declared source instant and evidence, or an explicit unknown diagnostic.
    pub event_time: source_time::EventTime,
    chunks: Vec<String>,
    evicted: bool,
}

/// A named, durably committed collection of concrete evidence identities.
#[derive(Clone, Debug)]
pub struct Pin {
    /// Stable identity; never reused when a name is reused.
    pub id: String,
    /// Case-sensitive UTF-8 label.
    pub name: String,
    /// Finite UTC deadline in Unix milliseconds.
    pub expires_at: u64,
    /// Monotonically increasing collection revision.
    pub revision: u64,
    /// Distinct direct artifact/record targets.
    pub members: BTreeSet<String>,
}

impl Pin {
    fn extend(&mut self, deadline: u64) -> Result<(), Error> {
        let expires_at = self.expires_at.max(deadline);
        crate::format_instant(expires_at).map_err(|error| {
            Error::new(
                "STORAGE_FAILURE",
                format!("stored pin deadline cannot be rendered: {error}"),
            )
        })?;
        self.expires_at = expires_at;
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "pin revision overflow"))?;
        Ok(())
    }
}

fn pin_deadline(days: u64, now: u64) -> Result<u64, Error> {
    if days == 0 {
        return Err(Error::new(
            "INVALID_ARGUMENT",
            "pin duration must be positive integer days",
        ));
    }
    let deadline = days
        .checked_mul(DAY)
        .and_then(|duration| now.checked_add(duration))
        .ok_or_else(|| Error::new("INVALID_ARGUMENT", "pin deadline overflow"))?;
    crate::format_instant(deadline)?;
    Ok(deadline)
}

/// One database owner. Clones share a serialized catalog writer.
#[derive(Clone)]
pub struct Store {
    inner: Arc<Mutex<Writer>>,
    pin_reads: Arc<Mutex<pin_read::Reads>>,
}

struct Writer {
    db: DB,
    id: String,
    config: Config,
    durability: std::cell::RefCell<durability::State>,
}

fn random_id(kind: &str) -> Result<String, Error> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes)
        .map_err(|e| Error::new("STORAGE_FAILURE", format!("identity entropy: {e}")))?;
    Ok(format!("{kind}:{}", hex::encode(&bytes)))
}

fn parse(bytes: &[u8]) -> Result<Value, Error> {
    json::read_slice(
        bytes,
        json::Limits {
            max_depth: 128,
            max_values: 1048576,
            max_string_bytes: CATALOG_STRING_BYTES,
            unique_members: true,
        },
    )
    .map_err(|e| Error::new("STORAGE_FAILURE", format!("corrupt catalog JSON: {e}")))
}

impl From<DecodeError> for Error {
    fn from(error: DecodeError) -> Self {
        Self::new(
            "STORAGE_FAILURE",
            format!("invalid catalog record: {error}"),
        )
    }
}

fn field<'a>(value: &'a Value, name: &'static str) -> Result<&'a Value, Error> {
    JsonRecord::new(value, "catalog record")?
        .raw(name)?
        .ok_or_else(|| DecodeError::missing_field(name).into())
}

fn read_field<T: purrdf_lex::json::record::FromJson>(
    value: &Value,
    name: &'static str,
) -> Result<T, Error> {
    Ok(JsonRecord::new(value, "catalog record")?.required(name)?)
}

fn integer(value: &Value, name: &'static str) -> Result<u64, Error> {
    Ok(
        JsonRecord::new(value, "catalog record")?.required_with(name, |value| {
            value
                .as_str()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| DecodeError::invalid_value(value, "decimal u64 string"))
        })?,
    )
}

trait Catalog: Sized {
    fn encode(&self) -> Value;
    fn decode(value: &Value) -> Result<Self, Error>;
}

impl purrdf_lex::json::record::ToJson for Artifact {
    fn to_json(&self) -> Value {
        self.encode()
    }
}

impl purrdf_lex::json::record::ToJson for Record {
    fn to_json(&self) -> Value {
        self.encode()
    }
}

impl purrdf_lex::json::record::ToJson for Pin {
    fn to_json(&self) -> Value {
        self.encode()
    }
}

impl Catalog for Artifact {
    fn encode(&self) -> Value {
        Object::new()
            .with("id", self.id.clone())
            .with("source", self.source.clone())
            .with("format", self.format.clone())
            .with("received_at", self.received_at.to_string())
            .with("expires_at", self.expires_at.to_string())
            .with("byte_length", self.byte_length.to_string())
            .with("payload_id", self.payload_id.clone())
            .with("record_ids", self.record_ids.clone())
            .with("chunks", self.chunks.clone())
            .with("evicted", self.evicted)
            .into()
    }

    fn decode(v: &Value) -> Result<Self, Error> {
        Ok(Self {
            buffered: None,
            id: read_field::<String>(v, "id")?,
            source: read_field::<String>(v, "source")?,
            format: read_field::<String>(v, "format")?,
            received_at: integer(v, "received_at")?,
            expires_at: integer(v, "expires_at")?,
            byte_length: integer(v, "byte_length")?,
            payload_id: read_field::<String>(v, "payload_id")?,
            record_ids: read_field::<Vec<String>>(v, "record_ids")?,
            chunks: read_field::<Vec<String>>(v, "chunks")?,
            evicted: read_field(v, "evicted")?,
        })
    }
}

impl Catalog for Record {
    fn encode(&self) -> Value {
        Object::new()
            .with("id", self.id.clone())
            .with("artifact_id", self.artifact_id.clone())
            .with("source", self.source.clone())
            .with("received_at", self.received_at.to_string())
            .with("expires_at", self.expires_at.to_string())
            .with("byte_start", self.byte_start.to_string())
            .with("byte_end", self.byte_end.to_string())
            .with("projection_id", self.projection_id.clone())
            .with("rdf_projection_id", self.rdf_projection_id.clone())
            .with("event_time", self.event_time.encode())
            .with("chunks", self.chunks.clone())
            .with("evicted", self.evicted)
            .into()
    }

    fn decode(v: &Value) -> Result<Self, Error> {
        let record = Self {
            id: read_field::<String>(v, "id")?,
            artifact_id: read_field::<String>(v, "artifact_id")?,
            source: read_field::<String>(v, "source")?,
            received_at: integer(v, "received_at")?,
            expires_at: integer(v, "expires_at")?,
            byte_start: integer(v, "byte_start")?,
            byte_end: integer(v, "byte_end")?,
            projection_id: read_field(v, "projection_id")?,
            rdf_projection_id: read_field(v, "rdf_projection_id")?,
            event_time: v
                .get("event_time")
                .map(source_time::EventTime::decode)
                .transpose()?
                .unwrap_or_else(|| source_time::EventTime::unknown("legacy_unprofiled", None)),
            chunks: read_field::<Vec<String>>(v, "chunks")?,
            evicted: read_field(v, "evicted")?,
        };
        if let source_time::EventTime::Known {
            source_start,
            source_end,
            ..
        } = &record.event_time
            && (*source_start < record.byte_start
                || source_start > source_end
                || *source_end > record.byte_end)
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "source time evidence outside record",
            ));
        }
        Ok(record)
    }
}

impl Catalog for Pin {
    fn encode(&self) -> Value {
        Object::new()
            .with("id", self.id.clone())
            .with("name", self.name.clone())
            .with("expires_at", self.expires_at.to_string())
            .with("revision", self.revision.to_string())
            .with("members", self.members.iter().cloned().collect::<Vec<_>>())
            .into()
    }

    fn decode(v: &Value) -> Result<Self, Error> {
        Ok(Self {
            id: read_field::<String>(v, "id")?,
            name: read_field::<String>(v, "name")?,
            expires_at: integer(v, "expires_at")?,
            revision: integer(v, "revision")?,
            members: read_field::<Vec<String>>(v, "members")?
                .into_iter()
                .collect(),
        })
    }
}

impl Writer {
    fn payload_chunk(&self, digest: &str) -> Result<Vec<u8>, Error> {
        let cf = self
            .db
            .cf_handle("payload")
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "payload family missing"))?;
        let bytes = self
            .db
            .get_cf(cf, digest)?
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "referenced payload missing"))?;
        if bytes.len() > CHUNK || hex::encode(blake3::hash(&bytes).as_bytes()) != digest {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "payload size or digest mismatch",
            ));
        }
        Ok(bytes)
    }
    fn commit_pin(&self, key: &str, arguments: Value, pin: &Pin, now: u64) -> Result<(), Error> {
        let receipt: Value = Object::new()
            .with("arguments", arguments)
            .with(
                "expires_at",
                now.checked_add(RETRY_WINDOW)
                    .ok_or_else(|| Error::new("INVALID_ARGUMENT", "retry deadline overflow"))?
                    .to_string(),
            )
            .with("receipt", pin.encode())
            .into();
        let mut batch = WriteBatch::default();
        put(&mut batch, &pin.id, pin);
        batch.put(key, json::write_compact(&receipt));
        self.commit(batch)
    }

    fn commit(&self, batch: WriteBatch) -> Result<(), Error> {
        self.apply(batch, None)
    }

    fn get<T: Catalog>(&self, key: &str) -> Result<T, Error> {
        let bytes = self
            .db
            .get(key)?
            .ok_or_else(|| Error::new("NOT_FOUND", format!("unknown evidence {key}")))?;
        T::decode(&parse(&bytes)?)
    }

    fn retry(&self, key: &str, now: u64) -> Result<Option<Value>, Error> {
        let Some(bytes) = self.db.get(key)? else {
            return Ok(None);
        };
        let value = parse(&bytes)?;
        Ok((integer(&value, "expires_at")? > now).then_some(value))
    }

    fn release_batch(&self, id: &str, batch: &mut WriteBatch) -> Result<(), Error> {
        if !id.starts_with("pin:") {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "release requires stable pin ID",
            ));
        }
        let _: Pin = self.get(id)?;
        batch.delete(id);
        Ok(())
    }

    fn list<T: Catalog>(&self, prefix: &str) -> Result<Vec<T>, Error> {
        let mut rows = Vec::new();
        for entry in self.db.prefix_iterator(prefix) {
            let (key, bytes) = entry?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            rows.push(T::decode(&parse(&bytes)?)?);
        }
        Ok(rows)
    }

    fn count_prefix(&self, prefix: &str) -> Result<u64, Error> {
        let mut count = 0_u64;
        for entry in self.db.prefix_iterator(prefix) {
            let (key, _) = entry?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    fn protected(&self, now: u64) -> Result<BTreeSet<String>, Error> {
        self.protected_matching(now, |_| true, || Ok(()), None)
    }

    fn protected_matching<R>(
        &self,
        now: u64,
        include: impl Fn(&str) -> bool,
        mut admit: impl FnMut() -> Result<R, Error>,
        maximum: Option<usize>,
    ) -> Result<BTreeSet<String>, Error> {
        let mut protected = BTreeSet::new();
        let mut iterator = self.db.prefix_iterator("pin:");
        loop {
            let _admission = admit()?;
            let Some(entry) = iterator.next() else {
                break;
            };
            let (key, bytes) = entry?;
            if !key.starts_with(b"pin:") {
                break;
            }
            if maximum.is_some_and(|maximum| bytes.len() > maximum) {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "pin catalog entry exceeds RDF scope admission",
                ));
            }
            let pin = Pin::decode(&parse(&bytes)?)?;
            if pin.expires_at > now {
                protected.extend(pin.members.into_iter().filter(|member| include(member)));
            }
        }
        Ok(protected)
    }

    fn live_record(&self, record: &Record, now: u64, protected: &BTreeSet<String>) -> bool {
        !record.evicted
            && (now < record.expires_at
                || protected.contains(&record.id)
                || protected.contains(&record.artifact_id))
    }

    fn live_artifact(&self, artifact: &Artifact, now: u64, protected: &BTreeSet<String>) -> bool {
        !artifact.evicted && (now < artifact.expires_at || protected.contains(&artifact.id))
    }

    fn physical_bytes(&self) -> Result<u64, Error> {
        fn directory(path: &Path) -> Result<u64, Error> {
            let mut size = 0_u64;
            for entry in
                std::fs::read_dir(path).map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?
            {
                let entry = entry.map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
                let metadata = entry
                    .metadata()
                    .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
                let bytes = if metadata.is_dir() {
                    directory(&entry.path())?
                } else {
                    metadata.len()
                };
                size = size
                    .checked_add(bytes)
                    .ok_or_else(|| Error::new("STORAGE_FAILURE", "disk accounting overflow"))?;
            }
            Ok(size)
        }
        directory(self.db.path())
    }
}

fn put<T: Catalog>(batch: &mut WriteBatch, key: &str, value: &T) {
    batch.put(key, json::write_compact(&value.encode()));
}

impl Store {
    /// Open a single-writer RocksDB store with an explicit disk budget.
    pub fn open(path: impl AsRef<Path>, config: Config) -> Result<Self, Error> {
        config.validate()?;
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_max_background_jobs(2);
        options.set_write_buffer_size(8 * 1024 * 1024);
        options.set_compression_type(DBCompressionType::Lz4);
        options.set_bottommost_compression_type(DBCompressionType::Zstd);
        let mut payload = Options::default();
        payload.set_compression_type(DBCompressionType::Lz4);
        payload.set_bottommost_compression_type(DBCompressionType::Zstd);
        payload.set_enable_blob_files(true);
        payload.set_min_blob_size(CHUNK as u64);
        payload.set_blob_compression_type(DBCompressionType::Zstd);
        payload.set_enable_blob_gc(true);
        payload.set_write_buffer_size(8 * 1024 * 1024);
        let db = DB::open_cf_descriptors(
            &options,
            path,
            [
                ColumnFamilyDescriptor::new("default", options.clone()),
                ColumnFamilyDescriptor::new("payload", payload),
            ],
        )?;
        let fingerprint = hex::encode(&config.text_analyzer.fingerprint());
        let rdf_fingerprint = rdf_index::profile_identity()?;
        let id = match db.get("store_format")? {
            Some(format) if format == FORMAT => {
                let id = db
                    .get("store_id")?
                    .ok_or_else(|| Error::new("STORAGE_FAILURE", "store identity missing"))?;
                String::from_utf8(id).map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?
            }
            Some(_) => {
                return Err(Error::new(
                    "UNSUPPORTED",
                    "unsupported store format; migration required",
                ));
            }
            None => {
                if db.iterator(IteratorMode::Start).next().is_some() {
                    return Err(Error::new("STORAGE_FAILURE", "nonempty unversioned store"));
                }
                let id = random_id("store")?;
                let mut batch = WriteBatch::default();
                batch.put("store_format", FORMAT);
                batch.put("store_id", &id);
                batch.put("text_analyzer", &fingerprint);
                batch.put("rdf_profile", &rdf_fingerprint);
                batch.put("rdf:term-count", 0_u64.to_be_bytes());
                batch.put("rdf:max-encoded", 0_u64.to_be_bytes());
                batch.put("rdf:next", 0_u64.to_be_bytes());
                let mut write = WriteOptions::default();
                write.set_sync(true);
                db.write_opt(batch, &write)?;
                id
            }
        };
        match db.get("text_analyzer")? {
            Some(stored) if stored != fingerprint.as_bytes() => {
                return Err(Error::new(
                    "UNSUPPORTED",
                    "text analyzer identity changed; explicit index migration required",
                ));
            }
            Some(_) => {}
            None => {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "store text analyzer identity missing; inspect or restore the catalog",
                ));
            }
        }
        let terms = rdf_index::required_counter(&db, "rdf:term-count")?;
        let maximum = rdf_index::required_counter(&db, "rdf:max-encoded")?;
        let next = rdf_index::required_counter(&db, "rdf:next")?;
        if maximum > 65536 || (terms > 0 && maximum == 0) || next < terms {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "RDF read control counters inconsistent",
            ));
        }
        match db.get("rdf_profile")? {
            Some(stored) if stored == rdf_fingerprint.as_bytes() => {}
            Some(_) => {
                return Err(Error::new(
                    "UNSUPPORTED",
                    "RDF profile changed; explicit projection/index migration required",
                ));
            }
            None => {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "store RDF profile identity missing",
                ));
            }
        }
        // Recovered buffered writes are certified only after a real synchronization.
        db.flush_wal(true)?;
        let durability = std::cell::RefCell::new(durability::State::new(
            random_id("session")?,
            db.latest_sequence_number(),
        ));
        Ok(Self {
            inner: Arc::new(Mutex::new(Writer {
                db,
                id,
                config,
                durability,
            })),
            pin_reads: Arc::new(Mutex::new(pin_read::Reads::default())),
        })
    }

    /// Return this persistent store's identity.
    pub fn store_id(&self) -> Result<String, Error> {
        Ok(self.lock()?.id.clone())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Writer>, Error> {
        self.inner.lock().map_err(|_| {
            Error::new(
                "STORAGE_FAILURE",
                "catalog writer poisoned; reopen and inspect the store",
            )
        })
    }

    /// Durably ingest one bounded complete artifact, preserving every original byte.
    /// JSONL and log formats create records including their original line delimiters.
    /// A stable request identity returns its original receipt on retry.
    pub fn ingest(
        &self,
        request_id: &str,
        source: &str,
        format: &str,
        bytes: &[u8],
        now: u64,
    ) -> Result<Artifact, Error> {
        self.ingest_with_durability(
            request_id,
            source,
            format,
            bytes,
            now,
            durability::Durability::Durable,
        )
    }

    /// Accept a complete atomic artifact at the caller's explicit WAL boundary.
    pub fn ingest_with_durability(
        &self,
        request_id: &str,
        source: &str,
        format: &str,
        bytes: &[u8],
        now: u64,
        mode: durability::Durability,
    ) -> Result<Artifact, Error> {
        validate_ingest_labels(request_id, source, format)?;
        let writer = self.lock()?;
        if bytes.len() > writer.config.max_artifact_bytes {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "artifact exceeds the configured bounded ingestion size",
            ));
        }
        let payload_id = format!("blake3:{}", hex::encode(blake3::hash(bytes).as_bytes()));
        if let Some(receipt) = writer.accepted_ingest(
            request_id,
            source,
            format,
            bytes.len() as u64,
            &payload_id,
            now,
        )? {
            return writer.receipt(receipt, mode);
        }
        if writer.db.get(import::import_id(request_id))?.is_some() {
            return Err(Error::new(
                "CONFLICT",
                "request has a staged import; resume it through import_start",
            ));
        }
        let source_time_profile = writer.time_profile(source)?;
        let expires_at = now.checked_add(30 * DAY).ok_or_else(|| {
            Error::new(
                "INVALID_ARGUMENT",
                "acceptance timestamp overflows retention deadline",
            )
        })?;
        if record_ends(format, bytes, true).count() > writer.config.max_records {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "artifact exceeds configured record count",
            ));
        }
        let id = random_id("artifact")?;
        let mut prepared = ingest::RecordBatch::new(&writer)?;
        let mut records = Vec::new();
        let mut start = 0;
        for end in record_ends(format, bytes, true) {
            let record = prepared.record(ingest::RecordInput {
                id: random_id("record")?,
                artifact_id: &id,
                source,
                format,
                byte_start: start as u64,
                payload: ingest::RecordPayload::Buffered(&bytes[start..end]),
                received_at: now,
                expires_at,
                source_time_profile: &source_time_profile,
            })?;
            put(&mut prepared.batch, &record.id, &record);
            prepared.batch.put(
                format!("time:{now:020}:{}", record.id),
                record.id.as_bytes(),
            );
            records.push(record.id);
            start = end;
        }
        let ingest::PreparedRecords { mut batch, chunks } = prepared.finish()?;
        let artifact = Artifact {
            buffered: None,
            id,
            source: source.to_owned(),
            format: format.to_owned(),
            received_at: now,
            expires_at,
            byte_length: bytes.len() as u64,
            payload_id,
            record_ids: records,
            chunks,
            evicted: false,
        };
        put(&mut batch, &artifact.id, &artifact);
        writer.ingest_receipt(&mut batch, request_id, &artifact, now)?;
        if batch.size_in_bytes() > 128 * 1024 * 1024 {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "atomic ingestion batch exceeds 128 MiB; split the import into smaller complete artifacts",
            ));
        }
        writer.apply(
            batch,
            (mode == durability::Durability::Buffered).then_some(artifact.id.as_str()),
        )?;
        writer.receipt(artifact, mode)
    }

    /// Read artifact metadata, including known expired or evicted artifacts.
    pub fn artifact(&self, id: &str) -> Result<Artifact, Error> {
        if !id.starts_with("artifact:") {
            return Err(Error::new("INVALID_ARGUMENT", "wrong evidence kind"));
        }
        let writer = self.lock()?;
        writer.receipt(writer.get(id)?, durability::Durability::Buffered)
    }

    /// Read record metadata, including minimal parent provenance.
    pub fn record(&self, id: &str) -> Result<Record, Error> {
        if !id.starts_with("record:") {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "record metadata requires a record: identity",
            ));
        }
        let writer = self.lock()?;
        let record = writer.get(id)?;
        writer.validate_event_record(&record)?;
        Ok(record)
    }

    /// Capture currently eligible record membership at an explicit server instant.
    pub fn records(&self, now: u64) -> Result<Vec<Record>, Error> {
        let writer = self.lock()?;
        let protected = writer.protected(now)?;
        Ok(writer
            .list::<Record>("record:")?
            .into_iter()
            .filter(|record| writer.live_record(record, now, &protected))
            .collect())
    }

    /// Query persistent native JSON facts in a bounded temporal catalog scan.
    /// The serialized writer gives one eligible membership instant for the read.
    pub fn json_query(
        &self,
        scope: &crate::QueryScope,
        query: &crate::JsonQuery,
        now: u64,
    ) -> Result<Value, Error> {
        scope.validate()?;
        let predicate = query.predicate();
        if let Some(predicate) = &predicate {
            predicate.validate()?;
        }
        let writer = self.lock()?;
        let protected = writer.protected(now)?;
        let mut visited = 0_usize;
        let mut eligible = 0_u64;
        let mut failed = 0_u64;
        let mut not_applicable = 0_u64;
        let mut decoding_failures = 0_u64;
        let mut limited = false;
        let mut rows = Vec::new();
        let mut keys = BTreeMap::<(String, String), (u64, u64, String)>::new();
        let mut value_visits = 0_usize;
        let mut response_bytes = 0_usize;
        for record in writer.query_candidates(scope, now, &protected)? {
            let record = record?;
            if record.received_at >= scope.received_before {
                break;
            }
            if visited >= scope.scan_budget {
                limited = true;
                break;
            }
            visited += 1;
            if !writer.live_record(&record, now, &protected)
                || scope
                    .source
                    .as_ref()
                    .is_some_and(|source| *source != record.source)
            {
                continue;
            }
            eligible += 1;
            if record.projection_id.is_empty() {
                not_applicable += 1;
                continue;
            }
            let bytes = writer
                .db
                .get(&record.projection_id)?
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "live record projection missing"))?;
            let facts = parse(&bytes)?;
            if read_field::<String>(&facts, "state")? != "complete" {
                failed += 1;
                continue;
            }
            decoding_failures += integer(&facts, "decoding_failures")?;
            let values = field(&facts, "values")?.as_array().ok_or_else(|| {
                Error::new("STORAGE_FAILURE", "projection values must be an array")
            })?;
            for fact in values {
                if value_visits == scope.scan_budget {
                    limited = true;
                    break;
                }
                value_visits += 1;
                let path: String = read_field(fact, "path")?;
                let kind: String = read_field(fact, "kind")?;
                match query {
                    crate::JsonQuery::Keys => {
                        let entry = keys.entry((path, kind)).or_default();
                        entry.0 += 1;
                        if entry.2 != record.id {
                            entry.1 += 1;
                            entry.2.clone_from(&record.id);
                        }
                    }
                    crate::JsonQuery::Values { .. } => {
                        if !predicate
                            .as_ref()
                            .ok_or_else(|| {
                                Error::new("STORAGE_FAILURE", "value predicate missing")
                            })?
                            .matches(fact)?
                        {
                            continue;
                        }
                        if rows.len() == scope.limit {
                            limited = true;
                            break;
                        }
                        response_bytes =
                            response_bytes.saturating_add(json::write_compact(fact).len() + 2048);
                        if response_bytes > 2 * 1024 * 1024 {
                            limited = true;
                            break;
                        }
                        rows.push(Value::from(
                            Object::new()
                                .with("record_id", record.id.clone())
                                .with("artifact_id", record.artifact_id.clone())
                                .with("source", record.source.clone())
                                .with("received_at", crate::format_instant(record.received_at)?)
                                .with(
                                    "source_start",
                                    (record.byte_start + integer(fact, "start")?).to_string(),
                                )
                                .with(
                                    "source_end",
                                    (record.byte_start + integer(fact, "end")?).to_string(),
                                )
                                .with("value", fact.clone()),
                        ));
                    }
                }
            }
            if limited {
                break;
            }
        }
        if matches!(query, crate::JsonQuery::Keys) {
            limited |= keys.len() > scope.limit;
            rows = keys
                .into_iter()
                .take(scope.limit)
                .map(|((path, kind), (occurrences, records, _))| {
                    Value::from(
                        Object::new()
                            .with("path", path)
                            .with("kind", kind)
                            .with("occurrences", occurrences.to_string())
                            .with("records", records.to_string()),
                    )
                })
                .collect();
        }
        Ok(Object::new()
            .with("rows", Value::array(rows))
            .with("store_id", writer.id.clone())
            .with("membership_at", crate::format_instant(now)?)
            .with("time_basis", "received_at")
            .with("execution", if limited { "limited" } else { "complete" })
            .with(
                "certainty",
                if limited || failed > 0 || decoding_failures > 0 {
                    "incomplete"
                } else {
                    "exact"
                },
            )
            .with(
                "coverage",
                Value::from(
                    Object::new()
                        .with("visited", visited.to_string())
                        .with("eligible_records", eligible.to_string())
                        .with("failed_json_records", failed.to_string())
                        .with("non_json_records", not_applicable.to_string())
                        .with("decoding_failures", decoding_failures.to_string()),
                ),
            )
            .into())
    }

    /// Return active pin collections at an explicit UTC instant.
    pub fn pins(&self, now: u64) -> Result<Vec<Pin>, Error> {
        Ok(self
            .lock()?
            .list::<Pin>("pin:")?
            .into_iter()
            .filter(|pin| pin.expires_at > now)
            .collect())
    }

    /// Return complete artifacts eligible for exact-byte retrieval at this instant.
    pub fn artifacts(&self, now: u64) -> Result<Vec<Artifact>, Error> {
        let writer = self.lock()?;
        let protected = writer.protected(now)?;
        writer
            .list::<Artifact>("artifact:")?
            .into_iter()
            .filter(|artifact| writer.live_artifact(artifact, now, &protected))
            .map(|artifact| writer.receipt(artifact, durability::Durability::Buffered))
            .collect::<Result<Vec<_>, _>>()
    }

    /// Read an exact bounded range of complete artifact or retained record bytes.
    pub fn bytes(&self, id: &str, offset: u64, length: usize, now: u64) -> Result<Vec<u8>, Error> {
        if length > BYTE_READ_BYTES {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "byte reads are bounded to 1 MiB",
            ));
        }
        let writer = self.lock()?;
        let protected = writer.protected(now)?;
        let (chunks, size) = if id.starts_with("artifact:") {
            let artifact: Artifact = writer.get(id)?;
            if !writer.live_artifact(&artifact, now, &protected) {
                return Err(Error::new(
                    "EVIDENCE_UNAVAILABLE",
                    "complete artifact expired or was evicted",
                ));
            }
            (artifact.chunks, artifact.byte_length)
        } else if id.starts_with("record:") {
            let record: Record = writer.get(id)?;
            if !writer.live_record(&record, now, &protected) {
                return Err(Error::new(
                    "EVIDENCE_UNAVAILABLE",
                    "record expired or was evicted",
                ));
            }
            (record.chunks, record.byte_end - record.byte_start)
        } else {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "bytes target must be an artifact or record",
            ));
        };
        offset
            .checked_add(length as u64)
            .filter(|end| *end <= size)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "byte range is outside evidence"))?;
        let chunks = chunks
            .into_iter()
            .map(|digest| writer.payload_chunk(&digest));
        read_chunk_range(chunks, offset, length)
    }

    /// Create or extend a collection atomically; every target must be live.
    pub fn pin(
        &self,
        request_id: &str,
        name: &str,
        targets: &[String],
        days: u64,
        now: u64,
    ) -> Result<Pin, Error> {
        if request_id.is_empty()
            || request_id.len() > 128
            || name.is_empty()
            || name.len() > 128
            || name.chars().any(char::is_control)
            || targets.is_empty()
            || days == 0
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "pin requires bounded request/name, concrete targets, and positive integer days",
            ));
        }
        let deadline = pin_deadline(days, now)?;
        let writer = self.lock()?;
        let retry_key = format!("request:pin:{request_id}");
        let targets: BTreeSet<_> = targets.iter().cloned().collect();
        let arguments: Value = Object::new()
            .with("name", name)
            .with("targets", targets.iter().cloned().collect::<Vec<_>>())
            .with("days", days.to_string())
            .into();
        if targets.len() > writer.config.max_pin_members {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "pin input exceeds configured membership limit",
            ));
        }
        if let Some(previous) = writer.retry(&retry_key, now)? {
            if field(&previous, "arguments")? != &arguments {
                return Err(Error::new(
                    "CONFLICT",
                    "pin request_id reused with different arguments",
                ));
            }
            return Pin::decode(field(&previous, "receipt")?);
        }
        let protected = writer.protected(now)?;
        for target in &targets {
            let live = if target.starts_with("artifact:") {
                writer.live_artifact(&writer.get::<Artifact>(target)?, now, &protected)
            } else if target.starts_with("record:") {
                writer.live_record(&writer.get::<Record>(target)?, now, &protected)
            } else {
                return Err(Error::new(
                    "INVALID_ARGUMENT",
                    "pin target must be artifact or record",
                ));
            };
            if !live {
                return Err(Error::new(
                    "EVIDENCE_UNAVAILABLE",
                    "pin cannot resurrect expired or evicted evidence",
                ));
            }
        }
        let mut pin = writer
            .list::<Pin>("pin:")?
            .into_iter()
            .find(|pin| pin.name == name && pin.expires_at > now)
            .unwrap_or(Pin {
                id: random_id("pin")?,
                name: name.to_owned(),
                expires_at: deadline,
                revision: 0,
                members: BTreeSet::new(),
            });
        pin.members.extend(targets);
        if pin.members.len() > writer.config.max_pin_members {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "pin exceeds direct membership limit",
            ));
        }
        pin.extend(deadline)?;
        writer.commit_pin(&retry_key, arguments, &pin, now)?;
        Ok(pin)
    }

    /// Durably extend a stable active collection without changing its members.
    /// An identical retry returns its original receipt even after release/expiry.
    pub fn renew_pin(
        &self,
        request_id: &str,
        id: &str,
        days: u64,
        expected_revision: Option<u64>,
        now: u64,
    ) -> Result<Pin, Error> {
        if request_id.is_empty()
            || request_id.len() > 128
            || !id.starts_with("pin:")
            || id.len() <= 4
            || id.len() > 128
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "renewal requires bounded request_id and stable pin ID",
            ));
        }
        let deadline = pin_deadline(days, now)?;
        let writer = self.lock()?;
        let key = format!("request:renew:{request_id}");
        let arguments: Value = Object::new()
            .with("pin_id", id)
            .with("days", days.to_string())
            .with(
                "expected_revision",
                expected_revision
                    .map(|value| Value::from(value.to_string()))
                    .unwrap_or(Value::Null),
            )
            .into();
        if let Some(previous) = writer.retry(&key, now)? {
            if field(&previous, "arguments")? != &arguments {
                return Err(Error::new(
                    "CONFLICT",
                    "renewal request_id reused with different arguments",
                ));
            }
            return Pin::decode(field(&previous, "receipt")?);
        }
        let mut pin = writer.get::<Pin>(id).map_err(|error| {
            if error.code == "NOT_FOUND" {
                Error::new("PIN_NOT_ACTIVE", "pin is unknown, released, or reclaimed")
            } else {
                error
            }
        })?;
        if pin.id != id {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "pin catalog identity does not match its key",
            ));
        }
        if pin.expires_at <= now {
            return Err(Error::new(
                "PIN_NOT_ACTIVE",
                "expired collection cannot be renewed",
            ));
        }
        if expected_revision.is_some_and(|revision| revision != pin.revision) {
            return Err(Error::new(
                "CONFLICT",
                "pin revision does not match expected_revision",
            ));
        }
        if pin.members.len() > writer.config.max_pin_members {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "pin exceeds current direct membership limit",
            ));
        }
        pin.extend(deadline)?;
        writer.commit_pin(&key, arguments, &pin, now)?;
        Ok(pin)
    }

    /// Remove only this stable collection's protection; other pins remain active.
    pub fn release(&self, id: &str) -> Result<(), Error> {
        let writer = self.lock()?;
        let mut batch = WriteBatch::default();
        writer.release_batch(id, &mut batch)?;
        writer.commit(batch)
    }

    /// Durably release a pin with a bounded stable retry receipt.
    pub fn release_retry(&self, request_id: &str, id: &str, now: u64) -> Result<Value, Error> {
        if request_id.is_empty() || request_id.len() > 128 {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "release requires a bounded nonempty request_id",
            ));
        }
        let writer = self.lock()?;
        let key = format!("request:release:{request_id}");
        if let Some(previous) = writer.retry(&key, now)? {
            if read_field::<String>(&previous, "pin_id")? != id {
                return Err(Error::new(
                    "CONFLICT",
                    "release request_id reused with a different pin",
                ));
            }
            return Ok(field(&previous, "receipt")?.clone());
        }
        let receipt: Value = Object::new()
            .with("request_id", request_id)
            .with("pin_id", id)
            .with("released_at", crate::format_instant(now)?)
            .with("durability", "durable")
            .into();
        let saved: Value = Object::new()
            .with("pin_id", id)
            .with("receipt", receipt.clone())
            .with(
                "expires_at",
                now.checked_add(RETRY_WINDOW)
                    .ok_or_else(|| Error::new("INVALID_ARGUMENT", "retry deadline overflow"))?
                    .to_string(),
            )
            .into();
        let mut batch = WriteBatch::default();
        writer.release_batch(id, &mut batch)?;
        batch.put(key, json::write_compact(&saved));
        writer.commit(batch)?;
        Ok(receipt)
    }

    /// Reclaim unreachable payload references; record pins survive parent expiry.
    pub fn expire(&self, now: u64) -> Result<u64, Error> {
        self.import_reclaim(now)?;
        self.pin_read_stats(now)?;
        let writer = self.lock()?;
        let protected = writer.protected(now)?;
        let mut retirement = retire::Retirement::new(&writer)?;
        let mut expired = 0;
        for mut artifact in writer.list::<Artifact>("artifact:")? {
            if !writer.live_artifact(&artifact, now, &protected) && !artifact.chunks.is_empty() {
                for digest in artifact.chunks.drain(..) {
                    *retirement.removals.entry(digest).or_default() += 1;
                }
                put(&mut retirement.batch, &artifact.id, &artifact);
                expired += 1;
            }
        }
        for mut record in writer.list::<Record>("record:")? {
            if !writer.live_record(&record, now, &protected) && retirement.record(&mut record, 1)? {
                put(&mut retirement.batch, &record.id, &record);
                expired += 1;
            }
        }
        let mut batch = retirement.finish()?;
        for pin in writer.list::<Pin>("pin:")? {
            if pin.expires_at <= now {
                batch.delete(pin.id);
            }
        }
        for entry in writer.db.prefix_iterator("request:") {
            let (key, bytes) = entry?;
            if !key.starts_with(b"request:") {
                break;
            }
            if integer(&parse(&bytes)?, "expires_at")? <= now {
                batch.delete(key);
            }
        }
        for record in writer.list::<Record>("record:")? {
            if !writer.live_record(&record, now, &protected)
                && record.expires_at.saturating_add(DAY) <= now
            {
                batch.delete(&record.id);
                batch.delete(format!("time:{:020}:{}", record.received_at, record.id));
            }
        }
        for artifact in writer.list::<Artifact>("artifact:")? {
            if !writer.live_artifact(&artifact, now, &protected)
                && artifact.expires_at.saturating_add(DAY) <= now
            {
                batch.delete(artifact.id);
            }
        }
        writer.commit(batch)?;
        Ok(expired)
    }

    /// Synchronize accepted writes and flush both catalog and payload families.
    pub fn flush(&self) -> Result<(), Error> {
        let writer = self.lock()?;
        writer.synchronize()?;
        writer.db.flush()?;
        writer.db.flush_cf(
            writer
                .db
                .cf_handle("payload")
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "payload family missing"))?,
        )?;
        Ok(())
    }

    /// Report measured database bytes and deduplicated live payload references.
    pub fn status(&self, now: u64) -> Result<Value, Error> {
        let (pin_handles, pin_bytes) = self.pin_read_stats(now)?;
        let writer = self.lock()?;
        let protected = writer.protected(now)?;
        let records = writer.list::<Record>("record:")?;
        let live = records
            .iter()
            .filter(|record| writer.live_record(record, now, &protected))
            .count();
        let unique_chunks = writer.count_prefix("ref:")?;
        Ok(Object::new()
            .with("pin_read_handles", pin_handles.to_string())
            .with("pin_read_bytes", pin_bytes.to_string())
            .with("max_pin_read_handles", pin_read::MAX_HANDLES.to_string())
            .with("max_pin_read_bytes", pin_read::HANDLE_BYTES.to_string())
            .with("pin_read_lease_ms", pin_read::LEASE_MS.to_string())
            .with("store_id", writer.id.clone())
            .with(
                "store_format",
                std::str::from_utf8(FORMAT)
                    .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?,
            )
            .with("rdf_profile", rdf_index::profile_identity()?)
            .with(
                "text_analyzer",
                hex::encode(&writer.config.text_analyzer.fingerprint()),
            )
            .with("text_profile", crate::text::PROFILE)
            .with("retry_window_seconds", (RETRY_WINDOW / 1000).to_string())
            .with(
                "unique_json_projections",
                writer.count_prefix("projection-ref:")?.to_string(),
            )
            .with("physical_bytes", writer.physical_bytes()?.to_string())
            .with("disk_budget", writer.config.disk_budget.to_string())
            .with(
                "max_artifact_bytes",
                writer.config.max_artifact_bytes.to_string(),
            )
            .with(
                "max_records_per_artifact",
                writer.config.max_records.to_string(),
            )
            .with("max_pin_members", writer.config.max_pin_members.to_string())
            .with(
                "maintenance_reserve",
                writer.config.maintenance_reserve.to_string(),
            )
            .with("live_records", live.to_string())
            .with("unique_payload_chunks", unique_chunks.to_string())
            .with(
                "active_pins",
                writer
                    .list::<Pin>("pin:")?
                    .iter()
                    .filter(|pin| pin.expires_at > now)
                    .count()
                    .to_string(),
            )
            .into())
    }
}
