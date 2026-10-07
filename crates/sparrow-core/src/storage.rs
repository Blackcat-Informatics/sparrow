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
mod rdf_index;
pub(crate) mod rdf_view;
mod text_index;

const FORMAT: &[u8] = b"sparrow.store/5";
const DAY: u64 = 86_400_000;
const RETRY_WINDOW: u64 = 2 * DAY;
const CHUNK: usize = 64 * 1024;

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
}

struct Writer {
    db: DB,
    id: String,
    config: Config,
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
            max_string_bytes: 16777216,
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
            .with("chunks", self.chunks.clone())
            .with("evicted", self.evicted)
            .into()
    }

    fn decode(v: &Value) -> Result<Self, Error> {
        Ok(Self {
            id: read_field::<String>(v, "id")?,
            artifact_id: read_field::<String>(v, "artifact_id")?,
            source: read_field::<String>(v, "source")?,
            received_at: integer(v, "received_at")?,
            expires_at: integer(v, "expires_at")?,
            byte_start: integer(v, "byte_start")?,
            byte_end: integer(v, "byte_end")?,
            projection_id: read_field(v, "projection_id")?,
            rdf_projection_id: read_field(v, "rdf_projection_id")?,
            chunks: read_field::<Vec<String>>(v, "chunks")?,
            evicted: read_field(v, "evicted")?,
        })
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
        let mut options = WriteOptions::default();
        options.set_sync(true);
        self.db.write_opt(batch, &options)?;
        Ok(())
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
        Ok(Self {
            inner: Arc::new(Mutex::new(Writer { db, id, config })),
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
        if request_id.is_empty()
            || request_id.len() > 128
            || source.is_empty()
            || source.len() > 512
        {
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
        let writer = self.lock()?;
        if bytes.len() > writer.config.max_artifact_bytes {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "artifact exceeds the configured bounded ingestion size",
            ));
        }
        let retry_key = format!("request:ingest:{request_id}");
        let mut preimage = Vec::new();
        for component in [source.as_bytes(), format.as_bytes(), bytes] {
            purrdf_hash::frame::frame_le(&mut preimage, component);
        }
        let signature = hex::encode(blake3::hash(&preimage).as_bytes());
        if let Some(previous) = writer.retry(&retry_key, now)? {
            if read_field::<String>(&previous, "signature")? != signature {
                return Err(Error::new(
                    "CONFLICT",
                    "request_id was committed with different input",
                ));
            }
            return Artifact::decode(field(&previous, "receipt")?);
        }
        let expires_at = now.checked_add(30 * DAY).ok_or_else(|| {
            Error::new(
                "INVALID_ARGUMENT",
                "acceptance timestamp overflows retention deadline",
            )
        })?;
        let id = random_id("artifact")?;
        let mut batch = WriteBatch::default();
        let cf = writer
            .db
            .cf_handle("payload")
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "payload family missing"))?;
        let mut content = BTreeMap::<String, Vec<u8>>::new();
        let mut chunks = Vec::new();
        let mut records = Vec::new();
        let mut projections = BTreeMap::<String, (Value, u64)>::new();
        let mut rdf_projections = BTreeMap::<String, (Value, u64)>::new();
        let mut rdf = rdf_index::RdfWrite::new(&writer.db)?;
        let profile = crate::projection::json_profile()?;
        let markdown = crate::projection::markdown_profile()?;
        let mut start = 0;
        let boundaries = if ["jsonl", "log"].contains(&format) {
            bytes
                .iter()
                .enumerate()
                .filter(|(_, b)| **b == b'\n')
                .map(|(i, _)| i + 1)
                .chain((!bytes.is_empty() && bytes.last() != Some(&b'\n')).then_some(bytes.len()))
                .collect::<Vec<_>>()
        } else {
            vec![bytes.len()]
        };
        if boundaries.len() > writer.config.max_records {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "artifact exceeds configured record count",
            ));
        }
        for end in boundaries {
            let digest = hex::encode(blake3::hash(&bytes[start..end]).as_bytes());
            let mut native_graph = None;
            let projection_id = if ["json", "jsonl"].contains(&format) {
                let key = format!("projection:json:{}:{digest}", profile.identity().to_hex());
                if let Some((_, count)) = projections.get_mut(&key) {
                    *count += 1;
                } else {
                    let facts = match writer.db.get(&key)? {
                        Some(stored) => parse(&stored)?,
                        None => {
                            let (facts, graph) = crate::projection::json_facts(
                                &bytes[start..end],
                                &digest,
                                &profile,
                            )?;
                            native_graph = Some(graph);
                            facts
                        }
                    };
                    projections.insert(key.clone(), (facts, 1));
                }
                key
            } else {
                String::new()
            };
            let rdf_projection_id = if !projection_id.is_empty() {
                format!("rdf-{projection_id}")
            } else if format == "markdown" {
                format!(
                    "rdf-projection:markdown:{}:{digest}",
                    markdown.contract_id().to_hex()
                )
            } else {
                String::new()
            };
            if !rdf_projection_id.is_empty() {
                if let Some((_, count)) = rdf_projections.get_mut(&rdf_projection_id) {
                    *count += 1;
                } else {
                    let key = format!("rdf:state:{rdf_projection_id}");
                    let state = match writer.db.get(&key)? {
                        Some(stored) => parse(&stored)?,
                        None => {
                            let result = if format == "markdown" {
                                crate::projection::markdown_graph(
                                    &bytes[start..end],
                                    &digest,
                                    &markdown,
                                )
                            } else {
                                native_graph.ok_or_else(|| {
                                    Error::new(
                                        "STORAGE_FAILURE",
                                        "existing JSON facts have no required RDF projection state",
                                    )
                                })?
                            };
                            match result
                                .and_then(|graph| rdf.graph(&mut batch, &rdf_projection_id, &graph))
                            {
                                Ok(()) => Object::new()
                                    .with("state", "complete")
                                    .with("graph", rdf_index::graph_iri(&rdf_projection_id))
                                    .into(),
                                Err(error)
                                    if ["RESOURCE_LIMIT", "PROJECTION_FAILURE"]
                                        .contains(&error.code) =>
                                {
                                    Object::new()
                                        .with("state", "failed")
                                        .with("diagnostic", error.to_string())
                                        .into()
                                }
                                Err(error) => return Err(error),
                            }
                        }
                    };
                    rdf_projections.insert(rdf_projection_id.clone(), (state, 1));
                }
            }
            let mut record_chunks = Vec::new();
            for chunk in bytes[start..end].chunks(CHUNK) {
                let digest = hex::encode(blake3::hash(chunk).as_bytes());
                content
                    .entry(digest.clone())
                    .or_insert_with(|| chunk.to_vec());
                record_chunks.push(digest.clone());
                chunks.push(digest);
            }
            let record = Record {
                id: random_id("record")?,
                artifact_id: id.clone(),
                source: source.to_owned(),
                received_at: now,
                expires_at,
                byte_start: start as u64,
                byte_end: end as u64,
                projection_id,
                rdf_projection_id,
                chunks: record_chunks,
                evicted: false,
            };
            let json = projections
                .get(&record.projection_id)
                .map(|(facts, _)| facts);
            let text = crate::text::facts(
                &writer.config.text_analyzer,
                &bytes[start..end],
                json,
                format == "opaque",
            );
            writer.put_text(&mut batch, &record.id, &text)?;
            let rdf_state = rdf_projections
                .get(&record.rdf_projection_id)
                .map(|(state, _)| state);
            rdf.graph(
                &mut batch,
                &record.id,
                rdf_index::metadata(&record, &digest, rdf_state)?.as_ref(),
            )?;
            put(&mut batch, &record.id, &record);
            batch.put(
                format!("time:{now:020}:{}", record.id),
                record.id.as_bytes(),
            );
            records.push(record.id);
            start = end;
        }
        let mut additions = BTreeMap::<String, u64>::new();
        for digest in &chunks {
            *additions.entry(digest.clone()).or_default() += 2;
        }
        let mut new_bytes = 0_u64;
        for (id, (state, added)) in rdf_projections {
            let key = format!("rdf:projection-ref:{id}");
            let previous = writer
                .db
                .get(&key)?
                .map(|v| {
                    v.as_slice()
                        .try_into()
                        .map(u64::from_be_bytes)
                        .map_err(|_| {
                            Error::new("STORAGE_FAILURE", "invalid RDF projection counter")
                        })
                })
                .transpose()?
                .unwrap_or(0);
            batch.put(
                key,
                previous
                    .checked_add(added)
                    .ok_or_else(|| {
                        Error::new("STORAGE_FAILURE", "RDF projection reference overflow")
                    })?
                    .to_be_bytes(),
            );
            batch.put(format!("rdf:state:{id}"), json::write_compact(&state));
        }
        rdf.finish(&mut batch)?;
        for (key, (facts, added)) in projections {
            let ref_key = format!("projection-ref:{key}");
            let previous = writer
                .db
                .get(&ref_key)?
                .map(|bytes| {
                    bytes
                        .as_slice()
                        .try_into()
                        .map(u64::from_be_bytes)
                        .map_err(|_| {
                            Error::new("STORAGE_FAILURE", "invalid projection reference counter")
                        })
                })
                .transpose()?
                .unwrap_or(0);
            let encoded = json::write_compact(&facts);
            new_bytes = new_bytes.saturating_add(encoded.len() as u64);
            batch.put(&key, encoded);
            batch.put(
                ref_key,
                previous
                    .checked_add(added)
                    .ok_or_else(|| Error::new("STORAGE_FAILURE", "projection reference overflow"))?
                    .to_be_bytes(),
            );
        }
        for (digest, payload) in content {
            let reference_key = format!("ref:{digest}");
            let count = match writer.db.get(&reference_key)? {
                Some(v) => u64::from_be_bytes(v.as_slice().try_into().map_err(|_| {
                    Error::new("STORAGE_FAILURE", "invalid payload reference counter")
                })?),
                None => {
                    new_bytes += payload.len() as u64;
                    batch.put_cf(cf, &digest, &payload);
                    0
                }
            };
            let count = count.checked_add(additions[&digest]).ok_or_else(|| {
                Error::new("STORAGE_FAILURE", "payload reference counter overflow")
            })?;
            batch.put(reference_key, count.to_be_bytes());
        }
        let estimate = (bytes.len() as u64)
            .saturating_add(new_bytes)
            .saturating_add((records.len() as u64).saturating_mul(2048))
            .saturating_add(4096)
            .max(
                (batch.size_in_bytes() as u64)
                    .saturating_mul(2)
                    .saturating_add(4096),
            );
        if writer.physical_bytes()?.saturating_add(estimate)
            > writer.config.disk_budget - writer.config.maintenance_reserve
        {
            return Err(Error::new(
                "STORAGE_PRESSURE",
                "insufficient capacity below maintenance reserve; release pins or reclaim storage before retrying",
            ));
        }
        let artifact = Artifact {
            id,
            source: source.to_owned(),
            format: format.to_owned(),
            received_at: now,
            expires_at,
            byte_length: bytes.len() as u64,
            payload_id: format!("blake3:{}", hex::encode(blake3::hash(bytes).as_bytes())),
            record_ids: records,
            chunks,
            evicted: false,
        };
        put(&mut batch, &artifact.id, &artifact);
        let receipt: Value = Object::new()
            .with("signature", signature)
            .with(
                "expires_at",
                now.checked_add(RETRY_WINDOW)
                    .ok_or_else(|| Error::new("INVALID_ARGUMENT", "retry deadline overflow"))?
                    .to_string(),
            )
            .with("receipt", artifact.encode())
            .into();
        batch.put(retry_key, json::write_compact(&receipt));
        if batch.size_in_bytes() > 128 * 1024 * 1024 {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "atomic ingestion batch exceeds 128 MiB; split the import into smaller complete artifacts",
            ));
        }
        writer.commit(batch)?;
        Ok(artifact)
    }

    /// Read artifact metadata, including known expired or evicted artifacts.
    pub fn artifact(&self, id: &str) -> Result<Artifact, Error> {
        self.evidence(id, "artifact:")
    }

    /// Read record metadata, including minimal parent provenance.
    pub fn record(&self, id: &str) -> Result<Record, Error> {
        self.evidence(id, "record:")
    }

    fn evidence<T: Catalog>(&self, id: &str, prefix: &str) -> Result<T, Error> {
        if !id.starts_with(prefix) {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                format!("metadata requires an identity with prefix {prefix}"),
            ));
        }
        self.lock()?.get(id)
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
        if let crate::JsonQuery::Values {
            kind: Some(kind), ..
        } = query
            && ![
                "string", "number", "true", "false", "null", "array", "object",
            ]
            .contains(&kind.as_str())
        {
            return Err(Error::new("INVALID_ARGUMENT", "unknown native JSON kind"));
        }
        let writer = self.lock()?;
        let protected = writer.protected(now)?;
        let start = format!("time:{:020}:", scope.received_from);
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
        for entry in writer.db.iterator(rocksdb::IteratorMode::From(
            start.as_bytes(),
            rocksdb::Direction::Forward,
        )) {
            let (key, id) = entry?;
            if !key.starts_with(b"time:") {
                break;
            }
            let id = std::str::from_utf8(&id)
                .map_err(|_| Error::new("STORAGE_FAILURE", "invalid temporal index identity"))?;
            let record: Record = writer.get(id)?;
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
                    crate::JsonQuery::Values {
                        path: wanted_path,
                        kind: wanted_kind,
                        lexical,
                        decoded,
                    } => {
                        let mut fields = JsonRecord::new(fact, "native JSON occurrence")?;
                        let actual_lexical: Option<String> = fields.optional("lexical")?;
                        let actual_decoded: Option<String> = fields.optional("decoded")?;
                        if wanted_path.as_ref().is_some_and(|wanted| wanted != &path)
                            || wanted_kind.as_ref().is_some_and(|wanted| wanted != &kind)
                            || lexical
                                .as_ref()
                                .is_some_and(|wanted| actual_lexical.as_ref() != Some(wanted))
                            || decoded
                                .as_ref()
                                .is_some_and(|wanted| actual_decoded.as_ref() != Some(wanted))
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
        Ok(writer
            .list::<Artifact>("artifact:")?
            .into_iter()
            .filter(|artifact| writer.live_artifact(artifact, now, &protected))
            .collect())
    }

    /// Read an exact bounded range of complete artifact or retained record bytes.
    pub fn bytes(&self, id: &str, offset: u64, length: usize, now: u64) -> Result<Vec<u8>, Error> {
        if length > 1024 * 1024 {
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
        let end = offset
            .checked_add(length as u64)
            .filter(|end| *end <= size)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "byte range is outside evidence"))?;
        let cf = writer
            .db
            .cf_handle("payload")
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "payload family missing"))?;
        let mut position = 0_u64;
        let mut result = Vec::with_capacity(length);
        for digest in chunks {
            let chunk = writer
                .db
                .get_cf(cf, &digest)?
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "referenced payload missing"))?;
            if hex::encode(blake3::hash(&chunk).as_bytes()) != digest {
                return Err(Error::new("STORAGE_FAILURE", "payload digest mismatch"));
            }
            let next = position + chunk.len() as u64;
            if next > offset && position < end {
                result.extend_from_slice(
                    &chunk[(offset.saturating_sub(position)) as usize
                        ..((end.min(next)) - position) as usize],
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
        let writer = self.lock()?;
        let protected = writer.protected(now)?;
        let mut batch = WriteBatch::default();
        let mut removals = BTreeMap::<String, u64>::new();
        let mut projection_removals = BTreeMap::<String, u64>::new();
        let mut rdf_removals = BTreeMap::<String, u64>::new();
        let mut rdf = rdf_index::RdfWrite::new(&writer.db)?;
        let mut expired = 0;
        for mut artifact in writer.list::<Artifact>("artifact:")? {
            if !writer.live_artifact(&artifact, now, &protected) && !artifact.chunks.is_empty() {
                for digest in artifact.chunks.drain(..) {
                    *removals.entry(digest).or_default() += 1;
                }
                put(&mut batch, &artifact.id, &artifact);
                expired += 1;
            }
        }
        for mut record in writer.list::<Record>("record:")? {
            if !writer.live_record(&record, now, &protected) {
                writer.remove_text(&mut batch, &record.id)?;
            }
            if !writer.live_record(&record, now, &protected)
                && (!record.chunks.is_empty()
                    || !record.projection_id.is_empty()
                    || !record.rdf_projection_id.is_empty()
                    || writer.db.get(format!("rdf:graph:{}", record.id))?.is_some())
            {
                rdf.remove(&mut batch, &record.id)?;
                if !record.rdf_projection_id.is_empty() {
                    *rdf_removals
                        .entry(std::mem::take(&mut record.rdf_projection_id))
                        .or_default() += 1;
                }
                for digest in record.chunks.drain(..) {
                    *removals.entry(digest).or_default() += 1;
                }
                if !record.projection_id.is_empty() {
                    *projection_removals
                        .entry(std::mem::take(&mut record.projection_id))
                        .or_default() += 1;
                }
                put(&mut batch, &record.id, &record);
                expired += 1;
            }
        }
        for (id, removed) in rdf_removals {
            let key = format!("rdf:projection-ref:{id}");
            let bytes = writer
                .db
                .get(&key)?
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "RDF projection reference missing"))?;
            let count =
                u64::from_be_bytes(bytes.as_slice().try_into().map_err(|_| {
                    Error::new("STORAGE_FAILURE", "invalid RDF projection counter")
                })?);
            let remaining = count.checked_sub(removed).ok_or_else(|| {
                Error::new("STORAGE_FAILURE", "RDF projection reference underflow")
            })?;
            if remaining == 0 {
                let state = writer
                    .db
                    .get(format!("rdf:state:{id}"))?
                    .ok_or_else(|| Error::new("STORAGE_FAILURE", "RDF projection state missing"))?;
                if read_field::<String>(&parse(&state)?, "state")? == "complete" {
                    rdf.remove(&mut batch, &id)?;
                }
                batch.delete(key);
                batch.delete(format!("rdf:state:{id}"));
            } else {
                batch.put(key, remaining.to_be_bytes());
            }
        }
        rdf.finish(&mut batch)?;
        let cf = writer
            .db
            .cf_handle("payload")
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "payload family missing"))?;
        for (projection, removed) in projection_removals {
            let key = format!("projection-ref:{projection}");
            let bytes = writer
                .db
                .get(&key)?
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "projection reference missing"))?;
            let count = u64::from_be_bytes(bytes.as_slice().try_into().map_err(|_| {
                Error::new("STORAGE_FAILURE", "invalid projection reference counter")
            })?);
            let remaining = count
                .checked_sub(removed)
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "projection reference underflow"))?;
            if remaining == 0 {
                batch.delete(key);
                batch.delete(projection);
            } else {
                batch.put(key, remaining.to_be_bytes());
            }
        }
        for (digest, removed) in removals {
            let key = format!("ref:{digest}");
            let bytes = writer.db.get(&key)?.ok_or_else(|| {
                Error::new("STORAGE_FAILURE", "reference counter missing during expiry")
            })?;
            let count = u64::from_be_bytes(
                bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::new("STORAGE_FAILURE", "invalid reference counter"))?,
            );
            let remaining = count
                .checked_sub(removed)
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "reference counter underflow"))?;
            if remaining == 0 {
                batch.delete(&key);
                batch.delete_cf(cf, digest);
            } else {
                batch.put(key, remaining.to_be_bytes());
            }
        }
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
        writer.db.flush_wal(true)?;
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
        let writer = self.lock()?;
        let protected = writer.protected(now)?;
        let records = writer.list::<Record>("record:")?;
        let live = records
            .iter()
            .filter(|record| writer.live_record(record, now, &protected))
            .count();
        let unique_chunks = writer.count_prefix("ref:")?;
        Ok(Object::new()
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
