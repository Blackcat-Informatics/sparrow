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

pub(crate) mod artifact_read;
pub(crate) mod catalog_lease;
mod catalog_upgrade;
pub(crate) mod consumption;
pub(crate) mod context;
pub(crate) mod diagnostic;
pub(crate) mod durability;
pub(crate) mod eviction;
pub(crate) mod expiry;
pub(crate) mod handoff;
pub(crate) mod import;
mod ingest;
pub(crate) mod pin_read;
pub(crate) mod pressure;
pub(crate) mod producer_stream;
pub(crate) mod publication;
mod query_membership;
mod rdf_index;
pub(crate) mod rdf_view;
mod record_fields;
pub(crate) mod record_read;
mod record_spool;
mod recordless;
pub(crate) mod result_read;
mod retire;
pub(crate) mod source_read;
pub(crate) mod source_time;
pub(crate) mod syslog;
pub(crate) mod tail;
mod tail_ranges;
mod text_index;
pub(crate) mod watch_registry;

const FORMAT: &[u8] = b"sparrow.store/33";
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
    if !["jsonl", "log", "json", "markdown", "opaque", "syslog"].contains(&format) {
        return Err(Error::new(
            "INVALID_ARGUMENT",
            "format must be jsonl, log, json, markdown, opaque, or syslog",
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
    /// Construct one machine-readable domain refusal with an actionable message.
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
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
    /// Immutable declared diagnostic values and their native occurrence evidence.
    pub diagnostics: diagnostic::DiagnosticSnapshot,
    /// Immutable accepted position in a producer sequence window, when declared.
    pub producer: Option<producer_stream::ProducerPosition>,
    /// Immutable syslog packet interpretation; raw bytes remain authoritative.
    pub syslog: Option<syslog::SyslogSnapshot>,
    /// Immutable original file-generation byte position, when tailed.
    pub tail: Option<tail::TailPosition>,
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
    rdf_read: Arc<Mutex<()>>,
    artifact_reads: Arc<Mutex<artifact_read::Reads>>,
    source_reads: Arc<Mutex<source_read::Reads>>,
    record_reads: Arc<Mutex<record_read::Reads>>,
}

struct Writer {
    db: Arc<DB>,
    id: String,
    config: Config,
    durability: std::cell::RefCell<durability::State>,
    pressure: std::cell::RefCell<pressure::State>,
    eviction: std::cell::RefCell<eviction::Cursor>,
    expiry: std::cell::RefCell<expiry::Cursor>,
    query_staging: Arc<std::sync::atomic::AtomicU64>,
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
    .map_err(Error::from)
}

impl From<json::Error> for Error {
    fn from(error: json::Error) -> Self {
        Self::new("STORAGE_FAILURE", format!("corrupt catalog JSON: {error}"))
    }
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

trait EvidenceCatalog: Catalog {
    fn identity(&self) -> &str;
}

impl EvidenceCatalog for Artifact {
    fn identity(&self) -> &str {
        &self.id
    }
}

impl EvidenceCatalog for Record {
    fn identity(&self) -> &str {
        &self.id
    }
}

impl EvidenceCatalog for Pin {
    fn identity(&self) -> &str {
        &self.id
    }
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
            .with("diagnostics", self.diagnostics.encode())
            .with("producer", self.producer.as_ref().map(Catalog::encode))
            .with("syslog", self.syslog.as_ref().map(Catalog::encode))
            .with("tail", self.tail.as_ref().map(Catalog::encode))
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
            diagnostics: v
                .get("diagnostics")
                .map(|value| {
                    diagnostic::DiagnosticSnapshot::decode(
                        value,
                        integer(v, "byte_start")?,
                        integer(v, "byte_end")?,
                    )
                })
                .transpose()?
                .unwrap_or_default(),
            producer: v
                .get("producer")
                .filter(|value| !value.is_null())
                .map(producer_stream::ProducerPosition::decode)
                .transpose()?,
            tail: v
                .get("tail")
                .filter(|value| !value.is_null())
                .map(tail::TailPosition::decode)
                .transpose()?,
            chunks: read_field::<Vec<String>>(v, "chunks")?,
            syslog: v
                .get("syslog")
                .filter(|value| !value.is_null())
                .map(syslog::SyslogSnapshot::decode)
                .transpose()?,
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
        if let Some(snapshot) = &record.syslog {
            snapshot.validate_record(&record)?;
        }
        if let Some(position) = &record.tail
            && (record.producer.is_some()
                || record.syslog.is_some()
                || record.source != position.declaration.source
                || position.source_end.checked_sub(position.source_start)
                    != record.byte_end.checked_sub(record.byte_start))
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "file-generation record provenance disagrees with its byte span or intake kind",
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
    fn properties(&self) -> Result<Object, Error> {
        let format = self
            .db
            .get("store_format")?
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "store format missing"))?;
        Ok(Object::new()
            .with("store_id", self.id.clone())
            .with(
                "store_format",
                std::str::from_utf8(&format)
                    .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?,
            )
            .with("rdf_profile", rdf_index::profile_identity()?)
            .with(
                "text_analyzer",
                hex::encode(&self.config.text_analyzer.fingerprint()),
            )
            .with("text_profile", crate::text::PROFILE)
            .with("retry_window_seconds", (RETRY_WINDOW / 1000).to_string())
            .with(
                "max_producer_streams",
                producer_stream::MAX_PRODUCER_STREAMS.to_string(),
            )
            .with(
                "max_producer_gap_intervals",
                producer_stream::MAX_PRODUCER_GAPS.to_string(),
            )
            .with(
                "max_producer_registry_bytes",
                producer_stream::MAX_PRODUCER_REGISTRY_BYTES.to_string(),
            )
            .with(
                "producer_metadata_retention_seconds",
                (30 * DAY / 1000).to_string(),
            )
            .with("disk_budget", self.config.disk_budget.to_string())
            .with(
                "max_artifact_bytes",
                self.config.max_artifact_bytes.to_string(),
            )
            .with(
                "max_records_per_artifact",
                self.config.max_records.to_string(),
            )
            .with("max_pin_members", self.config.max_pin_members.to_string())
            .with(
                "maintenance_reserve",
                self.config.maintenance_reserve.to_string(),
            ))
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
        let mut remaining = usize::MAX;
        self.get_limited(key, &mut remaining)
    }

    fn get_limited<T: Catalog>(&self, key: &str, remaining: &mut usize) -> Result<T, Error> {
        self.get_admitted(key, |bytes| Self::charge_catalog(remaining, bytes))
    }

    /// Decode one pinned catalog value only after the caller's admission succeeds.
    fn get_admitted<T: Catalog>(
        &self,
        key: &str,
        admit: impl FnOnce(usize) -> Result<(), Error>,
    ) -> Result<T, Error> {
        let bytes = self
            .db
            .get_pinned(key)?
            .ok_or_else(|| Error::new("NOT_FOUND", format!("unknown evidence {key}")))?;
        admit(bytes.len())?;
        T::decode(&parse(&bytes)?)
    }

    fn charge_catalog(remaining: &mut usize, bytes: usize) -> Result<(), Error> {
        *remaining = remaining
            .checked_sub(bytes.saturating_mul(64))
            .ok_or_else(|| {
                Error::new(
                    "RESOURCE_LIMIT",
                    "catalog capture exceeds operation admission",
                )
            })?;
        Ok(())
    }

    fn time_record(
        &self,
        key: &[u8],
        value: &[u8],
        remaining: &mut usize,
    ) -> Result<Record, Error> {
        let id = std::str::from_utf8(value)
            .map_err(|e| Error::new("STORAGE_FAILURE", format!("invalid time index: {e}")))?;
        if !id.starts_with("record:") || id.len() <= "record:".len() || id.len() > 128 {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "time index requires a bounded record identity",
            ));
        }
        let record: Record = self.get_limited(id, remaining).map_err(|error| {
            if error.code == "NOT_FOUND" {
                Error::new("STORAGE_FAILURE", "time index references a missing record")
            } else {
                error
            }
        })?;
        if record.id != id
            || key != format!("time:{:020}:{}", record.received_at, record.id).as_bytes()
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "time index does not match its record",
            ));
        }
        Ok(record)
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

    fn catalog<'a, T: EvidenceCatalog + 'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl Iterator<Item = Result<T, Error>> + 'a {
        self.db
            .prefix_iterator(prefix)
            .take_while(move |entry| match entry {
                Ok((key, _)) => key.starts_with(prefix.as_bytes()),
                Err(_) => true,
            })
            .map(|entry| {
                let (key, bytes) = entry?;
                Self::decode_evidence(&key, &bytes)
            })
    }

    fn decode_evidence<T: EvidenceCatalog>(key: &[u8], bytes: &[u8]) -> Result<T, Error> {
        let row = T::decode(&parse(bytes)?)?;
        if row.identity().as_bytes() != key {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "evidence catalog identity disagrees with its key",
            ));
        }
        Ok(row)
    }

    fn count_catalog<T: EvidenceCatalog>(
        &self,
        prefix: &str,
        include: impl Fn(&T) -> bool,
    ) -> Result<u64, Error> {
        self.catalog::<T>(prefix).try_fold(0_u64, |count, row| {
            count
                .checked_add(u64::from(include(&row?)))
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "evidence catalog counter overflow"))
        })
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
        self.protected_matching(now, |_| true, || Ok(()), None, None)
    }

    fn protected_matching<R>(
        &self,
        now: u64,
        include: impl Fn(&str) -> bool,
        mut admit: impl FnMut() -> Result<R, Error>,
        maximum: Option<usize>,
        mut remaining: Option<&mut usize>,
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
                    "pin catalog entry exceeds operation admission",
                ));
            }
            if let Some(remaining) = remaining.as_deref_mut() {
                Self::charge_catalog(remaining, bytes.len())?;
            }
            let pin: Pin = Self::decode_evidence(&key, &bytes)?;
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
        fn failure(error: std::io::Error, path: &Path, root: &Path) -> Error {
            Error::new(
                if error.kind() == std::io::ErrorKind::NotFound && path != root {
                    "ACCOUNTING_CHANGED"
                } else {
                    "STORAGE_FAILURE"
                },
                format!("{}: {error}", path.display()),
            )
        }
        fn directory(path: &Path, root: &Path) -> Result<u64, Error> {
            let mut size = 0_u64;
            for entry in std::fs::read_dir(path).map_err(|e| failure(e, path, root))? {
                let entry = entry.map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
                let metadata = entry
                    .metadata()
                    .map_err(|e| failure(e, &entry.path(), root))?;
                let bytes = if metadata.is_dir() {
                    directory(&entry.path(), root)?
                } else {
                    metadata.len()
                };
                size = size
                    .checked_add(bytes)
                    .ok_or_else(|| Error::new("STORAGE_FAILURE", "disk accounting overflow"))?;
            }
            Ok(size)
        }
        let mut last = String::new();
        for _ in 0..3 {
            match directory(self.db.path(), self.db.path()) {
                Ok(bytes) => {
                    return Ok(bytes.saturating_add(
                        self.query_staging
                            .load(std::sync::atomic::Ordering::Relaxed),
                    ));
                }
                Err(error) if error.code == "ACCOUNTING_CHANGED" => last = error.message,
                Err(error) => return Err(error),
            }
        }
        Err(Error::new(
            "STORAGE_PRESSURE",
            format!(
                "database footprint stayed unstable across 3 complete measurements; retry accounting before admission: {last}"
            ),
        ))
    }
}

fn put<T: Catalog>(batch: &mut WriteBatch, key: &str, value: &T) {
    batch.put(key, json::write_compact(&value.encode()));
}

impl Store {
    /// Actual single-writer database location, for excluding it from intake roots.
    pub fn data_path(&self) -> Result<std::path::PathBuf, Error> {
        Ok(self.lock()?.db.path().to_path_buf())
    }

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
            Some(format)
                if format == FORMAT
                    || format == consumption::FORMAT
                    || format == watch_registry::FORMAT
                    || format == handoff::FORMAT
                    || format == consumption::TAIL_FORMAT
                    || recordless::legacy_format(&format).is_some() =>
            {
                if recordless::requires_watch_registry(&format)
                    != db.get("watch_registry")?.is_some()
                {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "store format and durable watch registry disagree; restore the original catalog and its writer fence",
                    ));
                }
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
                batch.put("store_format", b"sparrow.store/5");
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
        recordless::upgrade(&db)?;
        diagnostic::fence_format(&db)?;
        tail_ranges::upgrade(&db)?;
        if db.get(producer_stream::KEY)?.is_none() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "producer registry missing from fenced store",
            ));
        }
        // Recovered buffered writes are certified only after a real synchronization.
        db.flush_wal(true)?;
        let durability = std::cell::RefCell::new(durability::State::new(
            random_id("session")?,
            db.latest_sequence_number(),
        ));
        let store = Self {
            inner: Arc::new(Mutex::new(Writer {
                db: Arc::new(db),
                id,
                config,
                durability,
                pressure: std::cell::RefCell::new(pressure::State::default()),
                eviction: std::cell::RefCell::new(eviction::Cursor::default()),
                expiry: std::cell::RefCell::new(expiry::Cursor::default()),
                query_staging: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            })),
            pin_reads: Arc::new(Mutex::new(pin_read::Reads::default())),
            rdf_read: Arc::new(Mutex::new(())),
            artifact_reads: Arc::new(Mutex::new(artifact_read::Reads::default())),
            source_reads: Arc::new(Mutex::new(source_read::Reads::default())),
            record_reads: Arc::new(Mutex::new(record_read::Reads::default())),
        };
        let writer = store.lock()?;
        producer_stream::registry(&writer)?;
        tail::registry(&writer)?;
        drop(writer);
        Ok(store)
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
        self.ingest_input(
            request_id,
            source,
            format,
            bytes,
            ingest::Origin::Ordinary,
            now,
            mode,
        )
    }

    /// Accept normalized typed events and their producer positions in one WAL batch.
    pub fn ingest_producer(
        &self,
        request_id: &str,
        source: &str,
        bytes: &[u8],
        producer: &producer_stream::ProducerBatch,
        now: u64,
        mode: durability::Durability,
    ) -> Result<Artifact, Error> {
        producer.validate()?;
        self.ingest_input(
            request_id,
            source,
            "jsonl",
            bytes,
            ingest::Origin::Producer(producer),
            now,
            mode,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one shared artifact publication path with optional producer provenance"
    )]
    fn ingest_input(
        &self,
        request_id: &str,
        source: &str,
        format: &str,
        bytes: &[u8],
        origin: ingest::Origin<'_>,
        now: u64,
        mode: durability::Durability,
    ) -> Result<Artifact, Error> {
        validate_ingest_labels(request_id, source, format)?;
        if request_id.starts_with("consume:") {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "consumption requests require the durable staged import path",
            ));
        }
        let writer = self.lock()?;
        if bytes.len() > writer.config.max_artifact_bytes {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "artifact exceeds the configured bounded ingestion size",
            ));
        }
        if let Some(input) = origin.tail()
            && (input.offset.checked_add(bytes.len() as u64) != Some(input.end)
                || (!input.complete && bytes.last() != Some(&b'\n'))
                || mode != durability::Durability::Durable)
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "tail checkpoint requires an exact complete byte prefix and durable publication",
            ));
        }
        let payload_id = format!("blake3:{}", hex::encode(blake3::hash(bytes).as_bytes()));
        if let Some(receipt) = writer.accepted_ingest(
            request_id,
            source,
            format,
            bytes.len() as u64,
            &payload_id,
            origin,
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
        let diagnostic_profile = writer.diagnostic_profile(source)?;
        let expires_at = now.checked_add(30 * DAY).ok_or_else(|| {
            Error::new(
                "INVALID_ARGUMENT",
                "acceptance timestamp overflows retention deadline",
            )
        })?;
        let record_count = record_ends(format, bytes, true).count();
        let producer = origin.producer();
        if producer.is_some_and(|input| input.sequences.len() != record_count) {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "producer sequence count differs from normalized record count",
            ));
        }
        if record_count > writer.config.max_records {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "artifact exceeds configured record count",
            ));
        }
        let stream = producer
            .map(|input| producer_stream::prepare(&writer, source, input, now))
            .transpose()?;
        let tail_checkpoint = origin
            .tail()
            .map(|input| tail::prepare(&writer, source, format, input, record_count, now))
            .transpose()?;
        let id = random_id("artifact")?;
        let mut prepared = ingest::RecordBatch::new(&writer)?;
        let mut records = Vec::new();
        let mut start = 0;
        for (ordinal, end) in record_ends(format, bytes, true).enumerate() {
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
                diagnostic_profile: diagnostic_profile.as_ref(),
                producer: producer.zip(stream.as_ref()).map(|(input, stream)| {
                    producer_stream::ProducerPosition {
                        stream_id: stream.id.clone(),
                        sequence: input.sequences[ordinal],
                    }
                }),
                tail: tail_checkpoint
                    .as_ref()
                    .map(|checkpoint| tail::TailPosition {
                        generation_id: checkpoint.id.clone(),
                        declaration: checkpoint.declaration.clone(),
                        source_start: checkpoint.offset + start as u64,
                        source_end: checkpoint.offset + end as u64,
                    }),
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
        recordless::publish(&mut batch, &artifact)?;
        writer.ingest_receipt(&mut batch, request_id, &artifact, origin, now)?;
        if let Some(checkpoint) = tail_checkpoint {
            batch.put(tail::KEY, checkpoint.encoded);
        }
        if let Some(stream) = stream {
            stream.publish(&mut batch);
        }
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
        writer
            .catalog::<Record>("record:")
            .filter(|row| match row {
                Ok(record) => writer.live_record(record, now, &protected),
                Err(_) => true,
            })
            .collect()
    }

    /// Query persistent native JSON facts in a bounded temporal catalog scan.
    /// The serialized writer gives one eligible membership instant for the read.
    pub fn json_query(
        &self,
        scope: &crate::QueryScope,
        query: &crate::JsonQuery,
        now: u64,
    ) -> Result<Value, Error> {
        self.json_query_capture(scope, query, now, false)
    }

    pub(super) fn json_query_capture(
        &self,
        scope: &crate::QueryScope,
        query: &crate::JsonQuery,
        now: u64,
        capture: bool,
    ) -> Result<Value, Error> {
        let row_limit = if capture {
            catalog_lease::MAX_ITEMS
        } else {
            scope.limit
        };
        scope.validate()?;
        let predicate = query.predicate()?;
        predicate.validate()?;
        let writer = self.lock()?;
        let protected = writer.protected(now)?;
        let mut visited = 0_usize;
        let mut eligible = 0_u64;
        let mut failed = 0_u64;
        let mut not_applicable = 0_u64;
        let mut decoding_failures = 0_u64;
        let mut diagnostics = diagnostic::DiagnosticCoverage::default();
        let mut limited = false;
        let mut rows = Vec::new();
        let mut keys = BTreeMap::<(String, String), (u64, u64, String)>::new();
        let mut key_bytes = 0_usize;
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
                || !scope.matches_source(&record.source)
                || !writer.matches_diagnostic_scope(
                    scope,
                    &record,
                    &mut diagnostics,
                    scope.require_complete_index,
                )?
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
                if scope.require_complete_index {
                    return Err(Error::new(
                        "INDEX_NOT_READY",
                        "selected JSON projection unavailable",
                    ));
                }
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
                if !predicate.matches(fact)? {
                    continue;
                }
                let path: String = read_field(fact, "path")?;
                let kind: String = read_field(fact, "kind")?;
                match query {
                    crate::JsonQuery::Keys { .. } => {
                        if capture && !keys.contains_key(&(path.clone(), kind.clone())) {
                            key_bytes = key_bytes
                                .saturating_add(path.len() + kind.len() + record.id.len() + 256);
                            if keys.len() >= row_limit || key_bytes > catalog_lease::CAPTURE_BYTES {
                                return Err(Error::new(
                                    "RESOURCE_LIMIT",
                                    "key capture exceeds admission; narrow scope",
                                ));
                            }
                        }
                        let entry = keys.entry((path, kind)).or_default();
                        entry.0 += 1;
                        if entry.2 != record.id {
                            entry.1 += 1;
                            entry.2.clone_from(&record.id);
                        }
                    }
                    crate::JsonQuery::Values { .. } => {
                        if rows.len() == row_limit {
                            limited = true;
                            break;
                        }
                        let row = Value::from(
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
                        );
                        response_bytes = response_bytes.saturating_add(if capture {
                            json::write_compact(&row).len() + std::mem::size_of::<Vec<u8>>()
                        } else {
                            json::write_compact(fact).len() + 2048
                        });
                        if response_bytes > catalog_lease::CAPTURE_BYTES {
                            limited = true;
                            break;
                        }
                        rows.push(row);
                    }
                }
            }
            if limited {
                break;
            }
        }
        if capture && limited {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "JSON capture exhausted scan, occurrence, row or byte admission; narrow scope; no page published",
            ));
        }
        if matches!(query, crate::JsonQuery::Keys { .. }) {
            limited |= keys.len() > row_limit;
            rows = keys
                .into_iter()
                .take(row_limit)
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
                if !diagnostics.complete(limited) || failed > 0 || decoding_failures > 0 {
                    "incomplete"
                } else {
                    "exact"
                },
            )
            .with(
                "coverage",
                diagnostics.encode(
                    visited as u64,
                    eligible,
                    [failed, not_applicable, decoding_failures],
                    limited,
                ),
            )
            .into())
    }

    /// Return active pin collections at an explicit UTC instant.
    pub fn pins(&self, now: u64) -> Result<Vec<Pin>, Error> {
        self.lock()?
            .catalog::<Pin>("pin:")
            .filter(|row| match row {
                Ok(pin) => pin.expires_at > now,
                Err(_) => true,
            })
            .collect()
    }

    /// Return complete artifacts eligible for exact-byte retrieval at this instant.
    pub fn artifacts(&self, now: u64) -> Result<Vec<Artifact>, Error> {
        let writer = self.lock()?;
        let protected = writer.protected(now)?;
        writer
            .catalog::<Artifact>("artifact:")
            .filter(|row| match row {
                Ok(artifact) => writer.live_artifact(artifact, now, &protected),
                Err(_) => true,
            })
            .map(|artifact| writer.receipt(artifact?, durability::Durability::Buffered))
            .collect::<Result<Vec<_>, _>>()
    }

    /// Read an exact bounded range of artifact, record, or complete retained file bytes.
    pub fn bytes(&self, id: &str, offset: u64, length: usize, now: u64) -> Result<Vec<u8>, Error> {
        if length > BYTE_READ_BYTES {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "byte reads are bounded to 1 MiB",
            ));
        }
        let writer = self.lock()?;
        if id.starts_with("file-generation:") {
            return tail_ranges::bytes(&writer, id, offset, length, now);
        }
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
            .catalog::<Pin>("pin:")
            .try_fold(None, |found, pin| -> Result<_, Error> {
                let pin = pin?;
                Ok(found.or_else(|| (pin.name == name && pin.expires_at > now).then_some(pin)))
            })?
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

    /// Read constant-size store and admission properties without scanning evidence.
    /// Does not measure physical bytes or count records, pins, references or projections.
    pub fn properties(&self) -> Result<Value, Error> {
        Ok(self.lock()?.properties()?.into())
    }

    /// Report measured database bytes and deduplicated live payload references.
    pub fn status(&self, now: u64) -> Result<Value, Error> {
        let (pin_handles, pin_bytes) = self.pin_read_stats(now)?;
        let (artifact_handles, artifact_bytes) = self.artifact_read_stats(now)?;
        let (source_handles, source_bytes) = self.source_read_stats(now)?;
        let (record_handles, record_bytes) = self.record_read_stats(now)?;
        let writer = self.lock()?;
        let protected = writer.protected(now)?;
        let live = writer.count_catalog::<Record>("record:", |record| {
            writer.live_record(record, now, &protected)
        })?;
        let unique_chunks = writer.count_prefix("ref:")?;
        Ok(writer
            .properties()?
            .with("source_read_handles", source_handles.to_string())
            .with("source_read_bytes", source_bytes.to_string())
            .with(
                "max_source_read_handles",
                catalog_lease::MAX_HANDLES.to_string(),
            )
            .with(
                "max_source_read_bytes",
                catalog_lease::HANDLE_BYTES.to_string(),
            )
            .with("source_read_lease_ms", catalog_lease::LEASE_MS.to_string())
            .with("record_read_handles", record_handles.to_string())
            .with("record_read_bytes", record_bytes.to_string())
            .with(
                "max_record_read_handles",
                catalog_lease::MAX_HANDLES.to_string(),
            )
            .with(
                "max_record_read_bytes",
                catalog_lease::HANDLE_BYTES.to_string(),
            )
            .with("record_read_lease_ms", catalog_lease::LEASE_MS.to_string())
            .with("pin_read_handles", pin_handles.to_string())
            .with("pin_read_bytes", pin_bytes.to_string())
            .with("max_pin_read_handles", pin_read::MAX_HANDLES.to_string())
            .with("max_pin_read_bytes", pin_read::HANDLE_BYTES.to_string())
            .with("pin_read_lease_ms", pin_read::LEASE_MS.to_string())
            .with("artifact_read_handles", artifact_handles.to_string())
            .with("artifact_read_bytes", artifact_bytes.to_string())
            .with(
                "max_artifact_read_handles",
                artifact_read::MAX_HANDLES.to_string(),
            )
            .with(
                "max_artifact_read_bytes",
                artifact_read::HANDLE_BYTES.to_string(),
            )
            .with(
                "artifact_read_lease_ms",
                catalog_lease::LEASE_MS.to_string(),
            )
            .with(
                "unique_json_projections",
                writer.count_prefix("projection-ref:")?.to_string(),
            )
            .with("physical_bytes", writer.physical_bytes()?.to_string())
            .with("live_records", live.to_string())
            .with("unique_payload_chunks", unique_chunks.to_string())
            .with(
                "active_pins",
                writer
                    .count_catalog::<Pin>("pin:", |pin| pin.expires_at > now)?
                    .to_string(),
            )
            .into())
    }
}
