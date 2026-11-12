// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Serialized catalog mutations and reference-aware payload ownership.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex};

use purrdf_hash::{blake3, hex};
use purrdf_lex::json::record::{DecodeError, Record as JsonRecord};
use purrdf_lex::json::{self, Object, Value};
use rocksdb::{DB, IteratorMode, WriteBatch, WriteOptions};

pub(crate) mod archive;
mod artifact_rdf;
pub(crate) mod backup;
mod event_fallback_rdf;
pub(crate) mod forced_compaction;
#[cfg(feature = "test-support")]
pub use event_fallback_rdf::store_event_fallback_rdf_for_legacy_fixture;
mod provenance_rdf;
mod tracing_rdf;
#[cfg(feature = "test-support")]
pub use artifact_rdf::retire_artifact_rdf_for_legacy_fixture;
#[cfg(feature = "test-support")]
pub use source_summary::{SourceCountAudit, remove_source_summary_for_legacy_fixture};
#[cfg(feature = "test-support")]
pub use store_counters::{StoreCounterAudit, remove_store_counters_for_legacy_fixture};
pub(crate) mod artifact_read;
pub(crate) mod catalog_lease;
mod catalog_upgrade;
pub(crate) mod consumption;
pub(crate) mod context;
pub(crate) mod correlation;
pub(crate) mod diagnostic;
pub(crate) mod durability;
pub(crate) mod eviction;
pub(crate) mod expiry;
pub(crate) mod handoff;
pub(crate) mod import;
mod ingest;
pub(crate) mod ingest_status;
pub(crate) mod intake;
pub(crate) mod listing_build;
pub(crate) mod log_time;
mod manifest_retire;
pub(crate) mod migration;
pub(crate) mod pin_read;
pub(crate) mod pressure;
pub(crate) mod producer_stream;
pub(crate) mod publication;
mod query_membership;
mod rdf_index;
pub(crate) mod rdf_view;
pub(crate) mod receiver_stream;
pub(crate) mod recompression;
mod record_fields;
pub(crate) mod record_read;
mod record_spool;
mod scope_receipt;
pub(crate) mod space;
pub use scope_receipt::{QUERY_SCOPE_PROFILE, QUERY_SCOPE_WORKSPACE_BYTES};
pub(crate) mod open_files;
mod recordless;
pub(crate) mod result_read;
pub(crate) mod retention_metrics;
mod retire;
mod sequence_window;
mod source_context;
pub(crate) mod source_read;
mod source_summary;
pub(crate) mod source_time;
mod store_counters;
pub(crate) mod syslog;
mod table_options;
pub(crate) mod tail;
mod tail_ranges;
mod temporal_index;
mod text_index;
pub(crate) mod watch_registry;
mod writer_format;
mod writer_queue;

const FORMAT: &[u8] = b"sparrow.store/33";
const DAY: u64 = 86_400_000;
const RETRY_WINDOW: u64 = 2 * DAY;
/// How long an accepted artifact and its records live, in milliseconds, unless pinned.
pub const RETENTION_MS: u64 = 30 * DAY;
const CHUNK: usize = 64 * 1024;
const BYTE_READ_BYTES: usize = 1024 * 1024;
/// Largest artifact accepted as one in-memory batch. Larger artifacts stream
/// through the staged import, within the disk budget and `MAX_IMPORT_BYTES`.
pub const INLINE_ARTIFACT_BYTES: usize = 16 * 1024 * 1024;
const CATALOG_STRING_BYTES: usize = 16 * 1024 * 1024;
const CATALOG_LIMITS: json::Limits = json::Limits {
    max_depth: 128,
    max_values: 1048576,
    max_string_bytes: CATALOG_STRING_BYTES,
    unique_members: true,
};

fn in_range(offset: u64, length: usize, size: u64) -> Result<(), Error> {
    offset
        .checked_add(length as u64)
        .filter(|end| *end <= size)
        .map(|_| ())
        .ok_or_else(|| Error::new("INVALID_ARGUMENT", "byte range is outside evidence"))
}

/// Read one record's bytes, or those of an artifact holding at most one record.
/// A record is chunked from its own start in full CHUNK pieces, so the read seeks
/// straight to the chunk holding `offset` and costs the requested range at any size.
/// Only the visited chunks are checked to be full; the skipped ones are trusted.
fn record_bytes(
    writer: &Writer,
    chunks: Vec<String>,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, Error> {
    let skip = offset / CHUNK as u64;
    let last = chunks.len().saturating_sub(1);
    let chunks = chunks
        .into_iter()
        .enumerate()
        .skip(skip as usize)
        .map(|(index, digest)| {
            let payload = writer.payload_chunk(&digest)?;
            if index != last && payload.len() != CHUNK {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "record manifest holds a short chunk before its end",
                ));
            }
            Ok(payload)
        });
    read_chunk_range(chunks, offset - skip * CHUNK as u64, length)
}

/// Read a multi-record artifact through its ordered, contiguous record rows. A
/// binary search on `byte_start` finds the record holding `offset`; each record
/// then seeks like `record_bytes`. Beyond the requested chunks, a read decodes the
/// whole artifact row once and a logarithmic number of record rows.
fn artifact_bytes(
    writer: &Writer,
    ids: &[String],
    offset: u64,
    length: usize,
    now: u64,
    protected: &BTreeSet<String>,
) -> Result<Vec<u8>, Error> {
    let record = |index: usize| -> Result<Record, Error> {
        let record: Record = writer.get(&ids[index])?;
        if !writer.live_record(&record, now, protected) || record.byte_end < record.byte_start {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "live artifact references an unreadable record",
            ));
        }
        Ok(record)
    };
    let (mut low, mut high) = (0, ids.len());
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        if record(middle)?.byte_start <= offset {
            low = middle;
        } else {
            high = middle;
        }
    }
    let mut result = Vec::with_capacity(length);
    let mut position = offset;
    for index in low..ids.len() {
        if result.len() == length {
            break;
        }
        let record = record(index)?;
        let gap = || {
            Error::new(
                "STORAGE_FAILURE",
                "artifact records do not cover its bytes contiguously",
            )
        };
        if index != low && record.byte_start != position {
            return Err(gap());
        }
        let start = position.checked_sub(record.byte_start).ok_or_else(gap)?;
        let rest = record.byte_end.checked_sub(position).ok_or_else(gap)?;
        let take = rest.min((length - result.len()) as u64) as usize;
        if take == 0 {
            continue;
        }
        result.extend(record_bytes(writer, record.chunks, start, take)?);
        position += take as u64;
    }
    if result.len() != length {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "artifact records do not cover the requested bytes",
        ));
    }
    Ok(result)
}

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

impl From<purrdf_lex::allocation::StorageError> for Error {
    fn from(error: purrdf_lex::allocation::StorageError) -> Self {
        let code = match error {
            purrdf_lex::allocation::StorageError::FormattingFailed => "STORAGE_FAILURE",
            _ => "RESOURCE_LIMIT",
        };
        Self::new(code, error.to_string())
    }
}

/// Explicit, bounded storage admission configuration.
#[derive(Clone, Debug)]
pub struct Config {
    /// Intended WAL synchronization interval, 1000..=60000 milliseconds.
    pub buffered_flush_interval_ms: u32,
    /// Physical database budget including maintenance reserve.
    pub disk_budget: u64,
    /// Capacity withheld for compaction and control mutations.
    pub maintenance_reserve: u64,
    /// Filesystem free space that must remain before any store write.
    pub min_free_bytes: u64,
    /// Maximum records published by one ingestion operation.
    pub max_records: usize,
    /// Maximum direct members in one pin collection.
    pub max_pin_members: usize,
    /// Resolved standard native analysis law, shared across index and query callers.
    pub text_analyzer: purrdf::text::Analyzer,
    /// Archive-before-delete policy. Once a store is fenced for archiving it requires one.
    pub archive: Option<archive::ArchivePolicy>,
}

impl Config {
    fn validate(&self) -> Result<(), Error> {
        if !(durability::MIN_FLUSH_INTERVAL_MS..=durability::MAX_FLUSH_INTERVAL_MS)
            .contains(&self.buffered_flush_interval_ms)
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "buffered_flush_interval_ms must be 1000..60000",
            ));
        }
        if self.maintenance_reserve < 1024 * 1024
            || self.disk_budget <= self.maintenance_reserve
            || self.max_records == 0
            || self.max_pin_members == 0
            || self.min_free_bytes == 0
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "supply positive operation limits, a positive free-space floor and a disk budget exceeding a maintenance reserve of at least 1 MiB",
            ));
        }
        if let Some(policy) = &self.archive {
            policy.validate()?;
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
    /// Endpoint and caller identity, present only for evidence accepted on the ingest socket.
    pub intake: Option<intake::Intake>,
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
    /// Immutable durable syslog receiver observation, never a sender sequence.
    pub receiver: Option<receiver_stream::ReceiverPosition>,
    /// Immutable syslog packet interpretation; raw bytes remain authoritative.
    pub syslog: Option<syslog::SyslogSnapshot>,
    /// Immutable original file-generation byte position, when tailed.
    pub tail: Option<tail::TailPosition>,
    /// Endpoint and caller identity, present only for evidence accepted on the ingest socket.
    pub intake: Option<intake::Intake>,
    chunks: Vec<String>,
    evicted: bool,
    /// Expiry retired the record's evidence (#146). The entry stays until a day past the
    /// deadline; status `live_records` stops counting it here, in expiry's own batch.
    retired: bool,
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

/// What a [`Store`] reads or coordinates without the writer lock (#146), shared by its
/// clones behind one `Arc`.
struct Unlocked {
    /// The writer's database, for reads that take no writer lock: `status`, migration
    /// progress and constant properties read counter rows from a snapshot of it.
    db: Arc<DB>,
    config: Config,
    query_staging: Arc<std::sync::atomic::AtomicU64>,
    /// Writers waiting for the writer lock, ahead of migration pages.
    writers: writer_queue::WriterQueue,
    migration_failures: migration::Failures,
    /// The writer's durability, pressure and table rewrite state, which status reads
    /// without the writer lock.
    durability: writer_queue::Published<durability::View>,
    pressure: writer_queue::Published<pressure::PressureStatus>,
    recompression: writer_queue::Published<recompression::RecompressionStatus>,
    /// Source and artifact listings still reading what the source-summary migration owes.
    source_builds: Mutex<source_read::Builds>,
    artifact_builds: Mutex<artifact_read::Builds>,
    /// The one online backup slot (#156); a backup never takes the writer lock.
    backup: backup::Slot,
    /// Wakes the maintenance worker's retention duty after an event that changes what
    /// expiry may retire: a pin release or commit and an archive commit.
    retention_waker: Arc<Mutex<Option<RetentionWaker>>>,
    /// The one compaction a stalled pressure wait may force.
    forced: Arc<forced_compaction::Forced>,
}

/// A non-blocking callback that asks the retention duty to run a pass now.
pub type RetentionWaker = Arc<dyn Fn() + Send + Sync>;

/// One database owner. Clones share a serialized catalog writer.
#[derive(Clone)]
pub struct Store {
    id: Arc<str>,
    open_files: open_files::OpenFiles,
    space: space::SpaceFloor,
    inner: Arc<Mutex<Writer>>,
    unlocked: Arc<Unlocked>,
    pin_reads: Arc<Mutex<pin_read::Reads>>,
    rdf_read: Arc<Mutex<()>>,
    artifact_reads: Arc<Mutex<artifact_read::Reads>>,
    source_reads: Arc<Mutex<source_read::Reads>>,
    record_reads: Arc<Mutex<record_read::Reads>>,
    sparql_reads: Arc<Mutex<rdf_view::pages::Reads>>,
    /// Each read registry's handle ledger, in pin, artifact, source, record, SPARQL order.
    /// `status` counts handles here, so it never waits on a registry a listing holds while
    /// it waits for the writer.
    lease_ledgers: Arc<[catalog_lease::Ledger; 5]>,
    /// Runs while a source listing reads its snapshot without the writer lock.
    #[cfg(feature = "test-support")]
    source_read_pause: Arc<Mutex<Option<source_read::Pause>>>,
    /// Owed index rows one artifact listing window holds, when a test lowers it.
    #[cfg(feature = "test-support")]
    owed_window: Arc<std::sync::atomic::AtomicUsize>,
    open_phases: Arc<[OpenPhase]>,
}

/// Wall time of one phase of [`Store::open`], in the order the open ran them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenPhase {
    /// Stable phase name.
    pub name: &'static str,
    /// Wall time the phase took.
    pub elapsed: std::time::Duration,
}

/// Records consecutive open phases: each lap ends the previous phase.
struct OpenTimer {
    phases: Vec<OpenPhase>,
    mark: std::time::Instant,
}

impl OpenTimer {
    fn start() -> Self {
        Self {
            phases: Vec::with_capacity(8),
            mark: std::time::Instant::now(),
        }
    }

    fn lap(&mut self, name: &'static str) {
        let now = std::time::Instant::now();
        self.phases.push(OpenPhase {
            name,
            elapsed: now - self.mark,
        });
        self.mark = now;
    }
}

struct ReadLeaseStats {
    pin: (usize, usize),
    artifact: (usize, usize),
    source: (usize, usize),
    record: (usize, usize),
    sparql: (usize, usize),
}

struct Writer {
    db: Arc<DB>,
    id: String,
    config: Config,
    space: space::SpaceFloor,
    forced: Arc<forced_compaction::Forced>,
    /// Set while a pressure pass commits reclamation, which the floor does not gate.
    reclaiming: std::cell::Cell<bool>,
    durability: writer_queue::Observed<durability::State>,
    pressure: writer_queue::Observed<pressure::State>,
    eviction: std::cell::RefCell<eviction::Cursor>,
    expiry: std::cell::RefCell<expiry::Cursor>,
    recompression: writer_queue::Observed<recompression::State>,
    /// Dev-only: how long each migration page holds the writer before it commits.
    #[cfg(feature = "test-support")]
    page_hold: std::cell::Cell<Option<std::time::Duration>>,
    /// Present exactly when the store archives before deleting.
    archive: Option<std::cell::RefCell<archive::ArchiveState>>,
    query_staging: Arc<std::sync::atomic::AtomicU64>,
    /// Longest record an import layout keeps whole; `import::RAW_RECORD_BYTES`
    /// except where the dev-only test hook lowers it for one open store.
    raw_record_bytes: std::cell::Cell<u64>,
    /// Retirement share the import bound and pin caps derive from: production
    /// defaults except where the dev-only test hook scales them for one open store.
    retirement: std::cell::Cell<eviction::RetirementShare>,
}

/// The refusal every operation reports once a writer panicked while holding the writer lock.
fn writer_poisoned() -> Error {
    Error::new(
        "STORAGE_FAILURE",
        "catalog writer poisoned; reopen and inspect the store",
    )
}

fn random_id(kind: &str) -> Result<String, Error> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes)
        .map_err(|e| Error::new("STORAGE_FAILURE", format!("identity entropy: {e}")))?;
    Ok(format!("{kind}:{}", hex::encode(&bytes)))
}

fn parse(bytes: &[u8]) -> Result<Value, Error> {
    json::read_slice(bytes, CATALOG_LIMITS).map_err(Error::from)
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

/// Whether `record` is retained at `now`: not evicted, and unexpired or protected.
fn live_record(record: &Record, now: u64, protected: &BTreeSet<String>) -> bool {
    !record.evicted
        && (now < record.expires_at
            || protected.contains(&record.id)
            || protected.contains(&record.artifact_id))
}

/// One record catalog entry by ID from `reader`, admitted against `remaining` before it is
/// decoded; `NOT_FOUND` when absent.
fn record_entry(
    reader: &store_counters::Reader<'_>,
    id: &str,
    remaining: &mut usize,
) -> Result<Record, Error> {
    let bytes = reader
        .get(id.as_bytes())?
        .ok_or_else(|| Error::new("NOT_FOUND", format!("unknown evidence {id}")))?;
    Writer::charge_catalog_shape(remaining, &bytes)?;
    Record::decode(&parse(&bytes)?)
}

/// The record a primary `time:` index entry names, read from `reader` and checked against
/// the entry.
fn time_record(
    reader: &store_counters::Reader<'_>,
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
    let record = record_entry(reader, id, remaining).map_err(|error| {
        if error.code == "NOT_FOUND" {
            Error::new("STORAGE_FAILURE", "time index references a missing record")
        } else {
            error
        }
    })?;
    if record.id != id || key != format!("time:{:020}:{}", record.received_at, record.id).as_bytes()
    {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "time index does not match its record",
        ));
    }
    Ok(record)
}

trait Catalog: Sized {
    fn encode(&self) -> Value;
    fn decode(value: &Value) -> Result<Self, Error>;
    fn stored(self, _writer: &Writer) -> Result<Self, Error> {
        Ok(self)
    }
}

trait EvidenceCatalog: Catalog {
    fn identity(&self) -> &str;
}

/// Decode one evidence catalog entry and check its identity against its key. Needs no
/// writer, so `status` decodes pin rows from a snapshot with it (#146); the writer's
/// [`Writer::decode_evidence`] adds the stored form.
fn decode_evidence_entry<T: EvidenceCatalog>(key: &[u8], bytes: &[u8]) -> Result<T, Error> {
    let row = T::decode(&parse(bytes)?)?;
    if row.identity().as_bytes() != key {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "evidence catalog identity disagrees with its key",
        ));
    }
    Ok(row)
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
        let object = Object::new()
            .with("id", self.id.clone())
            .with("source", self.source.clone())
            .with("format", self.format.clone())
            .with("received_at", self.received_at.to_string())
            .with("expires_at", self.expires_at.to_string())
            .with("byte_length", self.byte_length.to_string())
            .with("payload_id", self.payload_id.clone())
            .with("record_ids", self.record_ids.clone())
            .with("chunks", self.chunks.clone())
            .with("evicted", self.evicted);
        // Absent intake keeps the catalog row of evidence from every other endpoint unchanged.
        match &self.intake {
            Some(intake) => object.with("intake", intake.encode()).into(),
            None => object.into(),
        }
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
            intake: intake::decode_member(v)?,
            chunks: read_field::<Vec<String>>(v, "chunks")?,
            evicted: read_field(v, "evicted")?,
        })
    }
    fn stored(mut self, writer: &Writer) -> Result<Self, Error> {
        manifest_retire::resolve(writer, &mut self)?;
        Ok(self)
    }
}

impl Catalog for Record {
    fn encode(&self) -> Value {
        let object = Object::new()
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
            .with("receiver", self.receiver.as_ref().map(Catalog::encode))
            .with("syslog", self.syslog.as_ref().map(Catalog::encode))
            .with("tail", self.tail.as_ref().map(Catalog::encode))
            .with("chunks", self.chunks.clone())
            .with("evicted", self.evicted);
        // Written only once set, so an entry expiry has not retired keeps its encoding.
        let object = if self.retired {
            object.with("retired", true)
        } else {
            object
        };
        match &self.intake {
            Some(intake) => object.with("intake", intake.encode()).into(),
            None => object.into(),
        }
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
            receiver: v
                .get("receiver")
                .filter(|value| !value.is_null())
                .map(receiver_stream::ReceiverPosition::decode)
                .transpose()?,
            tail: v
                .get("tail")
                .filter(|value| !value.is_null())
                .map(tail::TailPosition::decode)
                .transpose()?,
            intake: intake::decode_member(v)?,
            chunks: read_field::<Vec<String>>(v, "chunks")?,
            syslog: v
                .get("syslog")
                .filter(|value| !value.is_null())
                .map(syslog::SyslogSnapshot::decode)
                .transpose()?,
            evicted: read_field(v, "evicted")?,
            retired: match v.get("retired") {
                None => false,
                Some(_) => read_field(v, "retired")?,
            },
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
        if record.receiver.is_some()
            && (record.syslog.is_none() || record.producer.is_some() || record.tail.is_some())
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "receiver position disagrees with record intake kind",
            ));
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

/// Constant-size store and admission properties from the stored `store_format`.
fn store_properties(format: Option<Vec<u8>>, id: &str, config: &Config) -> Result<Object, Error> {
    let format = format.ok_or_else(|| Error::new("STORAGE_FAILURE", "store format missing"))?;
    Ok(Object::new()
        .with("store_id", id.to_owned())
        .with(
            "store_format",
            std::str::from_utf8(&format)
                .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?,
        )
        .with("rdf_profile", rdf_index::profile_identity()?)
        .with(
            "text_analyzer",
            hex::encode(&config.text_analyzer.fingerprint()),
        )
        .with("text_profile", crate::text::PROFILE)
        .with("retry_window_seconds", (RETRY_WINDOW / 1000).to_string())
        .with("retention_seconds", (RETENTION_MS / 1000).to_string())
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
        .with("disk_budget", config.disk_budget.to_string())
        .with(
            "max_inline_artifact_bytes",
            INLINE_ARTIFACT_BYTES.to_string(),
        )
        .with("max_records_per_artifact", config.max_records.to_string())
        .with("max_pin_members", config.max_pin_members.to_string())
        .with(
            "maintenance_reserve",
            config.maintenance_reserve.to_string(),
        ))
}

impl Writer {
    fn payload_chunk(&self, digest: &str) -> Result<Vec<u8>, Error> {
        Ok(self.payload_chunk_view(digest, None)?.to_vec())
    }

    fn payload_chunk_view(
        &self,
        digest: &str,
        remaining: Option<&mut u64>,
    ) -> Result<rocksdb::DBPinnableSlice<'_>, Error> {
        let cf = self
            .db
            .cf_handle("payload")
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "payload family missing"))?;
        let bytes = self
            .db
            .get_pinned_cf(cf, digest)?
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "referenced payload missing"))?;
        if bytes.is_empty() || bytes.len() > CHUNK {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "invalid native payload chunk size",
            ));
        }
        if let Some(remaining) = remaining {
            *remaining = remaining.checked_sub(bytes.len() as u64).ok_or_else(|| {
                Error::new(
                    "RESOURCE_LIMIT",
                    "payload verification exceeds operation admission",
                )
            })?;
        }
        if hex::encode(blake3::hash(&bytes).as_bytes()) != digest {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "payload size or digest mismatch",
            ));
        }
        Ok(bytes)
    }
    /// Refuse a pin creation or extension that would take the store past its pin
    /// caps. Every expiry and eviction pass charges all pin rows, expired ones
    /// included until expiry deletes them, against the pin capture budget, and the
    /// caps keep that charge inside it. Release never passes through here.
    fn admit_pins(
        &self,
        pins: usize,
        members: usize,
        current_pins: usize,
        current_members: usize,
    ) -> Result<(), Error> {
        let share = self.retirement.get();
        let (max_pins, max_members) = (share.max_pins(), share.max_pinned_members());
        if pins > max_pins || members > max_members {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                format!(
                    "pins are capped at {max_pins} rows and {max_members} members store-wide, and the store holds {current_pins} rows with {current_members} members, expired rows included until expiry deletes them; release pins or wait for expiry"
                ),
            ));
        }
        Ok(())
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
        self.apply(batch, None, None)
    }

    fn get<T: Catalog>(&self, key: &str) -> Result<T, Error> {
        let mut remaining = usize::MAX;
        self.get_limited(key, &mut remaining)
    }

    fn get_limited<T: Catalog>(&self, key: &str, remaining: &mut usize) -> Result<T, Error> {
        self.get_admitted(key, |bytes| Self::charge_catalog_shape(remaining, bytes))
    }

    /// Decode one pinned catalog value only after the caller's admission succeeds.
    fn get_admitted<T: Catalog>(
        &self,
        key: &str,
        admit: impl FnOnce(&[u8]) -> Result<(), Error>,
    ) -> Result<T, Error> {
        self.read_catalog(key, |bytes| {
            admit(bytes)?;
            T::decode(&parse(bytes)?)?.stored(self)
        })
    }

    fn read_catalog<T>(
        &self,
        key: &str,
        read: impl FnOnce(&[u8]) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let bytes = self
            .db
            .get_pinned(key)?
            .ok_or_else(|| Error::new("NOT_FOUND", format!("unknown evidence {key}")))?;
        read(&bytes)
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

    /// Reserve native tree construction, typed decoding and retirement encoding
    /// before any owned tree is built. Native events borrow the encoded input.
    /// Aggregate query capture retains its separate opaque-row admission law.
    fn charge_catalog_shape(remaining: &mut usize, bytes: &[u8]) -> Result<(), Error> {
        let refusal = || {
            Error::new(
                "RESOURCE_LIMIT",
                "catalog capture exceeds operation admission",
            )
        };
        // Eight copies of encoded bytes cover decoded strings/numbers, typed
        // clones and re-encoding. Per-event charge covers Value/member/vector
        // and uniqueness-set scaffolding, including simultaneous parse/decode
        // owners. The fixed allowance covers the bounded native reader stack.
        let strings = bytes
            .len()
            .checked_mul(8)
            .and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(refusal)?;
        let mut available = remaining.checked_sub(strings).ok_or_else(refusal)?;
        let mut reader = json::Reader::from_slice(bytes, CATALOG_LIMITS)?;
        loop {
            match reader.next_event()? {
                json::Event::End => break,
                json::Event::EndObject { .. } | json::Event::EndArray { .. } => {}
                _ => available = available.checked_sub(1024).ok_or_else(refusal)?,
            }
        }
        *remaining = available;
        Ok(())
    }

    fn time_record(
        &self,
        key: &[u8],
        value: &[u8],
        remaining: &mut usize,
    ) -> Result<Record, Error> {
        time_record(&store_counters::Reader::db(&self.db), key, value, remaining)
    }

    fn retry(&self, key: &str, now: u64) -> Result<Option<Value>, Error> {
        self.retry_admitted(key, now, None)
    }

    fn retry_admitted(
        &self,
        key: &str,
        now: u64,
        remaining: Option<&mut usize>,
    ) -> Result<Option<Value>, Error> {
        let Some(bytes) = self.db.get_pinned(key)? else {
            return Ok(None);
        };
        if let Some(remaining) = remaining {
            Self::charge_catalog_shape(remaining, &bytes)?;
        }
        let value = parse(&bytes)?;
        Ok((integer(&value, "expires_at")? > now).then_some(value))
    }

    fn optional_catalog_value(
        &self,
        key: &str,
        remaining: &mut usize,
    ) -> Result<Option<Value>, Error> {
        self.db
            .get_pinned(key)?
            .map(|bytes| {
                Self::charge_catalog_shape(remaining, &bytes)?;
                parse(&bytes)
            })
            .transpose()
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
                self.decode_evidence(&key, &bytes)
            })
    }

    fn decode_evidence<T: EvidenceCatalog>(&self, key: &[u8], bytes: &[u8]) -> Result<T, Error> {
        decode_evidence_entry::<T>(key, bytes)?.stored(self)
    }

    fn count_prefix(&self, prefix: &str) -> Result<u64, Error> {
        let mut total = 0;
        for row in self.db.prefix_iterator(prefix) {
            if !row?.0.starts_with(prefix.as_bytes()) {
                break;
            }
            total += 1;
        }
        Ok(total)
    }

    fn protected(&self, now: u64) -> Result<BTreeSet<String>, Error> {
        self.protected_matching(now, |_| true, || Ok(()), None, None)
    }

    fn protected_matching<R>(
        &self,
        now: u64,
        include: impl Fn(&str) -> bool,
        admit: impl FnMut() -> Result<R, Error>,
        maximum: Option<usize>,
        remaining: Option<&mut usize>,
    ) -> Result<BTreeSet<String>, Error> {
        self.capture_pins(now, include, admit, maximum, remaining, None)
    }

    /// Capture live pin members, optionally collecting the keys of expired pin rows
    /// and the earliest live deadline so expiry can delete and schedule them.
    fn capture_pins<R>(
        &self,
        now: u64,
        include: impl Fn(&str) -> bool,
        mut admit: impl FnMut() -> Result<R, Error>,
        maximum: Option<usize>,
        mut remaining: Option<&mut usize>,
        mut sweep: Option<&mut expiry::PinSweep>,
    ) -> Result<BTreeSet<String>, Error> {
        let mut protected = BTreeSet::new();
        for entry in self.db.prefix_iterator("pin:") {
            let (key, bytes) = entry?;
            if !key.starts_with(b"pin:") {
                break;
            }
            // Admission is charged for each pin row read, never for the probe that ends
            // the range.
            let _admission = admit()?;
            if maximum.is_some_and(|maximum| bytes.len() > maximum) {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "pin catalog entry exceeds operation admission",
                ));
            }
            if let Some(remaining) = remaining.as_deref_mut() {
                Self::charge_catalog_shape(remaining, &bytes)?;
            }
            let pin: Pin = self.decode_evidence(&key, &bytes)?;
            if pin.expires_at > now {
                if let Some(sweep) = sweep.as_deref_mut() {
                    sweep.live(pin.expires_at);
                }
                protected.extend(pin.members.into_iter().filter(|member| include(member)));
            } else if let Some(sweep) = sweep.as_deref_mut() {
                sweep.expired.push(key);
            }
        }
        Ok(protected)
    }

    fn live_record(&self, record: &Record, now: u64, protected: &BTreeSet<String>) -> bool {
        live_record(record, now, protected)
    }

    fn live_artifact(&self, artifact: &Artifact, now: u64, protected: &BTreeSet<String>) -> bool {
        Self::artifact_eligible(
            &artifact.id,
            artifact.evicted,
            artifact.expires_at,
            now,
            protected,
        )
    }

    fn artifact_eligible(
        id: &str,
        evicted: bool,
        expires_at: u64,
        now: u64,
        protected: &BTreeSet<String>,
    ) -> bool {
        !evicted && (now < expires_at || protected.contains(id))
    }

    fn physical_bytes(&self) -> Result<u64, Error> {
        Ok(self.linked_physical_bytes()?.saturating_add(
            self.query_staging
                .load(std::sync::atomic::Ordering::Relaxed),
        ))
    }

    fn linked_physical_bytes(&self) -> Result<u64, Error> {
        linked_physical_bytes(&self.db)
    }
}

/// The bytes the store occupies: every file under the database directory except
/// the ones RocksDB no longer references. Obsolete table and blob files and
/// write-ahead logs normally vanish as soon as they become obsolete; an online
/// backup holds them until its copy ends (#156), and they must not count against
/// the disk budget, or a backup would refuse writes and evict evidence. Table and
/// blob files therefore count at most the bytes every family's versions still
/// reference, and logs below the oldest one recovery needs do not count. The
/// filesystem's free-space floor, which sees the held files, is guarded by the
/// backup itself (backup::start).
pub(super) fn linked_physical_bytes(db: &DB) -> Result<u64, Error> {
    /// RocksDB deletes obsolete WAL and SST files while the walk runs. A file
    /// removed between listing and inspection no longer occupies space, so it
    /// counts as zero; only a missing database directory is a failure.
    fn entries(path: &Path, root: &Path) -> Result<Vec<(std::ffi::OsString, u64)>, Error> {
        let listing = match std::fs::read_dir(path) {
            Ok(listing) => listing,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && path != root => {
                return Ok(Vec::new());
            }
            Err(error) => {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    format!("{}: {error}", path.display()),
                ));
            }
        };
        let mut sizes = Vec::new();
        for entry in listing {
            let entry = entry.map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        format!("{}: {error}", entry.path().display()),
                    ));
                }
            };
            let bytes = if metadata.is_dir() {
                total(&entries(&entry.path(), root)?)?
            } else {
                metadata.len()
            };
            sizes.push((entry.file_name(), bytes));
        }
        Ok(sizes)
    }
    fn total(sizes: &[(std::ffi::OsString, u64)]) -> Result<u64, Error> {
        sizes.iter().try_fold(0_u64, |sum, (_, bytes)| {
            sum.checked_add(*bytes)
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "disk accounting overflow"))
        })
    }
    let property = |family: &rocksdb::ColumnFamily, name: &str| {
        db.property_int_value_cf(family, name)?.ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                format!("RocksDB reports no {name} property"),
            )
        })
    };
    let mut referenced = 0_u64;
    // Every family the database has: a family it lacks holds no table to count.
    for family in FAMILIES.iter().filter_map(|name| db.cf_handle(name)) {
        for name in [
            "rocksdb.total-sst-files-size",
            "rocksdb.total-blob-file-size",
        ] {
            referenced = referenced.saturating_add(property(family, name)?);
        }
    }
    let oldest_log = db
        .property_int_value("rocksdb.min-log-number-to-keep")?
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "RocksDB reports no rocksdb.min-log-number-to-keep property",
            )
        })?;
    let mut tables = 0_u64;
    let mut other = Vec::new();
    for (name, bytes) in entries(db.path(), db.path())? {
        let text = name.to_string_lossy();
        if text.ends_with(".sst") || text.ends_with(".blob") {
            tables = tables.saturating_add(bytes);
        } else if !text
            .strip_suffix(".log")
            .and_then(|number| number.parse::<u64>().ok())
            .is_some_and(|number| number < oldest_log)
        {
            // Everything but an obsolete write-ahead log, which recovery no longer reads.
            other.push((name, bytes));
        }
    }
    total(&other)?
        .checked_add(tables.min(referenced))
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "disk accounting overflow"))
}

/// Every column family. Flushes and native maintenance checks cover all of them:
/// a log file is released only after every family with data in it flushes.
pub(super) const FAMILIES: [&str; 3] = ["default", "payload", rdf_index::REVERSE_FAMILY];

pub(super) fn family_handles(db: &DB) -> Result<Vec<&rocksdb::ColumnFamily>, Error> {
    FAMILIES
        .iter()
        .map(|name| {
            db.cf_handle(name)
                .ok_or_else(|| Error::new("STORAGE_FAILURE", format!("{name} family missing")))
        })
        .collect()
}

/// Whether any key starts with `prefix`.
fn any_key(db: &DB, prefix: &[u8]) -> Result<bool, Error> {
    match db.prefix_iterator(prefix).next() {
        Some(row) => Ok(row?.0.starts_with(prefix)),
        None => Ok(false),
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
        let mut timer = OpenTimer::start();
        config.validate()?;
        let space = space::SpaceFloor::new(path.as_ref(), config.min_free_bytes);
        let open_files = open_files::OpenFiles::from_process()?;
        let db = DB::open_cf_descriptors(
            &table_options::database(open_files.max_open_files)?,
            path,
            table_options::descriptors(),
        )?;
        timer.lap("rocksdb");
        source_time::validate_log_fence(&db)?;
        let fingerprint = hex::encode(&config.text_analyzer.fingerprint());
        let rdf_fingerprint = rdf_index::profile_identity()?;
        let id = match writer_format::read(&db)? {
            Some(format)
                if format == FORMAT
                    || format == consumption::FORMAT
                    || format == watch_registry::FORMAT
                    || format == handoff::FORMAT
                    || format == consumption::TAIL_FORMAT
                    || format == pressure::FORMAT
                    || format == pressure::WATCH_FORMAT
                    || format == receiver_stream::FORMAT
                    || format == receiver_stream::WATCH_FORMAT
                    || format == manifest_retire::FORMAT
                    || format == manifest_retire::WATCH_FORMAT
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
                // A new store writes every table under the current options.
                batch.put(recompression::PROFILE_KEY, table_options::PROFILE);
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
        let advance_rdf_profile = match db.get("rdf_profile")? {
            Some(stored) if stored == rdf_fingerprint.as_bytes() => false,
            Some(stored) if rdf_index::previous_profile_advances(&db, &stored)? => true,
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
        };
        timer.lap("identity");
        rdf_index::upgrade_reverse(&db)?;
        recordless::upgrade(&db)?;
        diagnostic::fence_format(&db)?;
        tail_ranges::upgrade(&db)?;
        artifact_rdf::upgrade(&db, config.disk_budget, config.maintenance_reserve)?;
        provenance_rdf::upgrade(&db, config.disk_budget, config.maintenance_reserve)?;
        tracing_rdf::upgrade(&db, config.disk_budget, config.maintenance_reserve)?;
        timer.lap("upgrades");
        // Schema 69's backfills and a stale source-context cache's rebuild run online, one
        // page per maintenance admission.
        migration::fence(&db)?;
        timer.lap("migration_fence");
        let archive = archive::open(&db, config.archive.as_ref())?;
        if db.get(producer_stream::KEY)?.is_none() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "producer registry missing from fenced store",
            ));
        }
        if advance_rdf_profile {
            // After the upgrades and fences, so their refusals leave the profile unchanged; the
            // synchronization below makes it durable.
            db.put("rdf_profile", &rdf_fingerprint)?;
        }
        timer.lap("archive");
        // Recovered buffered writes are certified only after a real synchronization.
        db.flush_wal(true)?;
        timer.lap("wal_sync");
        let durability = writer_queue::Observed::new(durability::State::new(
            random_id("session")?,
            db.latest_sequence_number(),
        ));
        manifest_retire::features(&db)?;
        let pressure = writer_queue::Observed::new(pressure::State::load(&db)?);
        let recompression = writer_queue::Observed::new(recompression::State::load(&db)?);
        timer.lap("state");
        let db = Arc::new(db);
        let query_staging = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (pin_reads, artifact_reads, source_reads, record_reads, sparql_reads) = (
            pin_read::Reads::default(),
            artifact_read::Reads::default(),
            source_read::Reads::default(),
            record_read::Reads::default(),
            rdf_view::pages::Reads::default(),
        );
        let retention_waker = Arc::new(Mutex::new(None));
        let forced = forced_compaction::Forced::new(Arc::clone(&retention_waker));
        let mut store = Self {
            id: Arc::from(id.as_str()),
            open_files,
            space: space.clone(),
            unlocked: Arc::new(Unlocked {
                db: Arc::clone(&db),
                config: config.clone(),
                query_staging: Arc::clone(&query_staging),
                writers: writer_queue::WriterQueue::default(),
                migration_failures: migration::Failures::default(),
                durability: durability.published(),
                pressure: pressure.published(),
                recompression: recompression.published(),
                source_builds: Mutex::default(),
                artifact_builds: Mutex::default(),
                backup: backup::Slot::default(),
                retention_waker,
                forced: Arc::clone(&forced),
            }),
            inner: Arc::new(Mutex::new(Writer {
                db,
                id,
                config,
                space,
                forced,
                reclaiming: std::cell::Cell::new(false),
                durability,
                pressure,
                eviction: std::cell::RefCell::new(eviction::Cursor::default()),
                expiry: std::cell::RefCell::new(expiry::Cursor::default()),
                recompression,
                #[cfg(feature = "test-support")]
                page_hold: std::cell::Cell::new(None),
                archive: archive.map(std::cell::RefCell::new),
                query_staging,
                raw_record_bytes: std::cell::Cell::new(import::RAW_RECORD_BYTES),
                retirement: std::cell::Cell::new(eviction::RetirementShare::DEFAULT),
            })),
            lease_ledgers: Arc::new([
                pin_reads.ledger(),
                artifact_reads.ledger(),
                source_reads.ledger(),
                record_reads.ledger(),
                sparql_reads.ledger(),
            ]),
            pin_reads: Arc::new(Mutex::new(pin_reads)),
            rdf_read: Arc::new(Mutex::new(())),
            artifact_reads: Arc::new(Mutex::new(artifact_reads)),
            source_reads: Arc::new(Mutex::new(source_reads)),
            record_reads: Arc::new(Mutex::new(record_reads)),
            sparql_reads: Arc::new(Mutex::new(sparql_reads)),
            #[cfg(feature = "test-support")]
            source_read_pause: Arc::new(Mutex::new(None)),
            #[cfg(feature = "test-support")]
            owed_window: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            open_phases: Arc::new([]),
        };
        let writer = store.lock()?;
        producer_stream::registry(&writer)?;
        receiver_stream::registry(&writer)?;
        tail::registry(&writer)?;
        drop(writer);
        timer.lap("registries");
        store.open_phases = timer.phases.into();
        Ok(store)
    }

    /// Wall time of each phase of the open that produced this store, in order. A stale
    /// source-context cache is reset inside `migration_fence` and rebuilt online afterwards.
    #[must_use]
    pub fn open_phases(&self) -> &[OpenPhase] {
        &self.open_phases
    }

    /// Start an online backup into `target` on its own thread and return at once (#156). It
    /// clones the database handle the unlocked readers share and never takes the writer lock,
    /// so ingest, maintenance and reads continue throughout. A request while a backup runs is
    /// refused with `CONFLICT`.
    pub fn backup_start(
        &self,
        target: &backup::BackupTarget,
        now: u64,
    ) -> Result<backup::BackupStatus, Error> {
        backup::start(
            &self.unlocked.backup,
            Arc::clone(&self.unlocked.db),
            self.space.clone(),
            self.id.to_string(),
            target,
            now,
        )
    }

    /// The latest online backup since the store opened, without the writer lock.
    #[must_use]
    pub fn backup_status(&self) -> backup::BackupStatus {
        self.unlocked.backup.status()
    }

    /// Return the immutable identity validated at open, without waiting for a writer.
    pub fn store_id(&self) -> Result<String, Error> {
        Ok(self.id.to_string())
    }

    /// The descriptor limit at open and the table cache bound derived from it.
    #[must_use]
    pub fn open_files(&self) -> open_files::OpenFiles {
        self.open_files
    }

    /// The free-space floor every write checks, readable without the writer lock.
    #[must_use]
    pub fn space_floor(&self) -> &space::SpaceFloor {
        &self.space
    }

    /// Take the writer lock as a writer: while it waits, migration pages defer to it.
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Writer>, Error> {
        let arrival = self.unlocked.writers.arrive()?;
        let writer = self.lock_uncounted();
        drop(arrival);
        writer
    }

    /// Take the writer lock without registering as a waiting writer: migration pages,
    /// which defer to registered writers before they ask.
    fn lock_uncounted(&self) -> Result<std::sync::MutexGuard<'_, Writer>, Error> {
        self.inner.lock().map_err(|_| writer_poisoned())
    }

    /// Refuse, without waiting for the writer lock, once a writer panicked while holding it.
    /// Reads that never take the lock (status and the observed states) call this so they
    /// report the poisoned writer as every locked operation does, rather than stale state.
    pub(crate) fn refuse_poisoned(&self) -> Result<(), Error> {
        if self.inner.is_poisoned() {
            return Err(writer_poisoned());
        }
        Ok(())
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
        self.ingest_with_intake(request_id, source, format, bytes, None, now, mode)
    }

    /// Accept a complete atomic artifact, recording the endpoint and caller that sent it.
    #[expect(
        clippy::too_many_arguments,
        reason = "the ingest request, its intake provenance and its WAL boundary"
    )]
    pub fn ingest_with_intake(
        &self,
        request_id: &str,
        source: &str,
        format: &str,
        bytes: &[u8],
        intake: Option<intake::Intake>,
        now: u64,
        mode: durability::Durability,
    ) -> Result<Artifact, Error> {
        self.ingest_input(
            request_id,
            source,
            format,
            bytes,
            ingest::Origin::Ordinary,
            intake,
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
        self.ingest_producer_with_intake(request_id, source, bytes, producer, None, now, mode)
    }

    /// Accept typed events, recording the endpoint and caller that sent them.
    #[expect(
        clippy::too_many_arguments,
        reason = "the event batch, its intake provenance and its WAL boundary"
    )]
    pub fn ingest_producer_with_intake(
        &self,
        request_id: &str,
        source: &str,
        bytes: &[u8],
        producer: &producer_stream::ProducerBatch,
        intake: Option<intake::Intake>,
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
            intake,
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
        intake: Option<intake::Intake>,
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
        if bytes.len() > INLINE_ARTIFACT_BYTES {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "inline ingestion carries at most 16 MiB; stage a larger artifact through import_begin",
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
            intake.as_ref(),
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
        if intake.is_some() {
            intake::fence(&writer)?;
        }
        let source_time_profile = writer.time_profile(source)?;
        let diagnostic_profile = writer.diagnostic_profile(source)?;
        let expires_at = now.checked_add(RETENTION_MS).ok_or_else(|| {
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
        let receiver = origin
            .receiver()
            .map(|input| {
                if format != "syslog"
                    || record_count != 1
                    || mode != durability::Durability::Durable
                {
                    return Err(Error::new(
                        "INVALID_ARGUMENT",
                        "receiver observation requires one durably published syslog packet",
                    ));
                }
                receiver_stream::prepare(&writer, source, input, None, now)
            })
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
                receiver: receiver.as_ref().map(|prepared| prepared.position.clone()),
                tail: tail_checkpoint
                    .as_ref()
                    .map(|checkpoint| tail::TailPosition {
                        generation_id: checkpoint.id.clone(),
                        declaration: checkpoint.declaration.clone(),
                        source_start: checkpoint.offset + start as u64,
                        source_end: checkpoint.offset + end as u64,
                    }),
                intake,
            })?;
            put(&mut prepared.batch, &record.id, &record);
            prepared.batch.put(
                format!("time:{now:020}:{}", record.id),
                record.id.as_bytes(),
            );
            records.push(record.id);
            start = end;
        }
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
            intake,
            chunks: Vec::new(),
            evicted: false,
        };
        let (mut batch, artifact, estimate) = prepared.finish_artifact(artifact)?;
        put(&mut batch, &artifact.id, &artifact);
        recordless::publish(&mut batch, &artifact)?;
        writer.ingest_receipt(&mut batch, request_id, &artifact, origin, now)?;
        if let Some(checkpoint) = tail_checkpoint {
            batch.put(tail::KEY, checkpoint.encoded);
        }
        if let Some(stream) = stream {
            stream.publish(&mut batch);
        }
        if let Some(receiver) = receiver {
            receiver.publish(&mut batch);
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
            Some(estimate),
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
        self.json_query_stage(scope, query, now, None)
    }

    fn json_query_stage(
        &self,
        scope: &crate::QueryScope,
        query: &crate::JsonQuery,
        now: u64,
        mut stage: Option<&mut record_spool::Builder>,
    ) -> Result<Value, Error> {
        let capture = stage.is_some();
        if let Some(stage) = stage.as_deref() {
            stage.checkpoint()?;
        }
        let row_limit = if capture {
            record_read::disk_capture_items(None, scope.scan_budget)
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
        let mut limit = None;
        let mut rows = Vec::new();
        let mut keys = BTreeMap::<(String, String), (u64, u64, String)>::new();
        let mut key_bytes = 0_usize;
        let mut value_visits = 0_usize;
        let mut response_bytes = 0_usize;
        let mut matched_values = 0_usize;
        let captured = writer.query_candidates(scope, now, &protected)?;
        if let Some(stage) = stage.as_deref_mut() {
            stage.scope = Some(captured.scope);
        }
        for record in captured.records {
            if let Some(stage) = stage.as_deref() {
                stage.checkpoint()?;
            }
            let record = record?;
            if record.received_at >= scope.received_before {
                break;
            }
            if visited >= scope.scan_budget {
                limit = Some((
                    "record_visit_budget",
                    scope.scan_budget,
                    "narrow source/time/evidence scope or raise scan_budget (at most 100000)",
                ));
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
                .get_pinned(&record.projection_id)?
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "live record projection missing"))?;
            let facts = parse(&bytes)?;
            if read_field::<String>(&facts, "state")? != "complete" {
                failed += 1;
                if scope.require_complete_index {
                    return Err(Error::new(
                        "EVIDENCE_INCOMPLETE",
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
                if let Some(stage) = stage.as_deref() {
                    stage.checkpoint()?;
                }
                if value_visits == scope.scan_budget {
                    limit = Some((
                        "json_occurrence_budget",
                        scope.scan_budget,
                        "narrow source/time/evidence scope or raise scan_budget (at most 100000)",
                    ));
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
                        if !keys.contains_key(&(path.clone(), kind.clone())) {
                            key_bytes = key_bytes
                                .saturating_add(path.len() + kind.len() + record.id.len() + 256);
                            if (capture && keys.len() >= row_limit)
                                || key_bytes > catalog_lease::SCAN_BYTES
                            {
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
                        if matched_values == row_limit {
                            limit = Some((
                                "result_item_budget",
                                row_limit,
                                "narrow source/time/evidence scope",
                            ));
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
                        if let Some(stage) = stage.as_deref_mut() {
                            stage.push(row, None)?;
                            matched_values += 1;
                            continue;
                        }
                        response_bytes =
                            response_bytes.saturating_add(json::write_compact(fact).len() + 2048);
                        if response_bytes > catalog_lease::CAPTURE_BYTES {
                            limit = Some((
                                "response_byte_budget",
                                catalog_lease::CAPTURE_BYTES,
                                "narrow source/time/evidence scope",
                            ));
                            break;
                        }
                        rows.push(row);
                        matched_values += 1;
                    }
                }
            }
            if limit.is_some() {
                break;
            }
        }
        if let Some((reason, ceiling, action)) = limit.filter(|_| capture) {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                format!("JSON capture exhausted {reason}={ceiling}; {action}; no page published"),
            ));
        }
        let mut limited = limit.is_some();
        if matches!(query, crate::JsonQuery::Keys { .. }) {
            limited |= keys.len() > row_limit;
            for row in
                keys.into_iter()
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
            {
                if let Some(stage) = stage.as_deref_mut() {
                    stage.push(row, None)?;
                } else {
                    rows.push(row);
                }
            }
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
        if id.starts_with("artifact:") {
            let artifact: Artifact = writer.get(id)?;
            if !writer.live_artifact(&artifact, now, &protected) {
                return Err(Error::new(
                    "EVIDENCE_UNAVAILABLE",
                    "complete artifact expired or was evicted",
                ));
            }
            in_range(offset, length, artifact.byte_length)?;
            if artifact.record_ids.len() <= 1 {
                return record_bytes(&writer, artifact.chunks, offset, length);
            }
            return artifact_bytes(
                &writer,
                &artifact.record_ids,
                offset,
                length,
                now,
                &protected,
            );
        }
        if !id.starts_with("record:") {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "bytes target must be an artifact or record",
            ));
        }
        let record: Record = writer.get(id)?;
        if !writer.live_record(&record, now, &protected) {
            return Err(Error::new(
                "EVIDENCE_UNAVAILABLE",
                "record expired or was evicted",
            ));
        }
        in_range(offset, length, record.byte_end - record.byte_start)?;
        record_bytes(&writer, record.chunks, offset, length)
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
        let (found, pins, members) = writer.catalog::<Pin>("pin:").try_fold(
            (None, 0_usize, 0_usize),
            |(found, pins, members), pin| -> Result<_, Error> {
                let pin = pin?;
                let total = members + pin.members.len();
                let found =
                    found.or_else(|| (pin.name == name && pin.expires_at > now).then_some(pin));
                Ok((found, pins + 1, total))
            },
        )?;
        let extended = found.is_some();
        let mut pin = match found {
            Some(pin) => pin,
            None => Pin {
                id: random_id("pin")?,
                name: name.to_owned(),
                expires_at: deadline,
                revision: 0,
                members: BTreeSet::new(),
            },
        };
        let before = pin.members.len();
        pin.members.extend(targets);
        if pin.members.len() > writer.config.max_pin_members {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "pin exceeds direct membership limit",
            ));
        }
        writer.admit_pins(
            pins + usize::from(!extended),
            members - before + pin.members.len(),
            pins,
            members,
        )?;
        pin.extend(deadline)?;
        writer.commit_pin(&retry_key, arguments, &pin, now)?;
        self.wake_retention();
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
        self.wake_retention();
        Ok(pin)
    }

    /// Remove only this stable collection's protection; other pins remain active.
    pub fn release(&self, id: &str) -> Result<(), Error> {
        let writer = self.lock()?;
        let mut batch = WriteBatch::default();
        writer.release_batch(id, &mut batch)?;
        writer.commit(batch)?;
        self.wake_retention();
        Ok(())
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
        self.wake_retention();
        Ok(receipt)
    }

    /// Synchronize accepted writes and flush every column family.
    pub fn flush(&self) -> Result<(), Error> {
        let writer = self.lock()?;
        writer.synchronize()?;
        writer.db.flush_cfs_opt(
            &family_handles(&writer.db)?,
            &rocksdb::FlushOptions::default(),
        )?;
        Ok(())
    }

    /// Read constant-size store and admission properties without scanning evidence or
    /// taking the writer lock. Does not measure physical bytes or count records, pins,
    /// references or projections.
    pub fn properties(&self) -> Result<Value, Error> {
        Ok(store_properties(
            self.unlocked.db.get("store_format")?,
            &self.id,
            &self.unlocked.config,
        )?
        .into())
    }

    /// Install, or with `None` remove, the callback that wakes the retention duty. The
    /// callback runs on the thread that released a pin, committed a pin or committed an
    /// archive segment, so it must not block.
    pub fn set_retention_waker(&self, waker: Option<RetentionWaker>) {
        if let Ok(mut slot) = self.unlocked.retention_waker.lock() {
            *slot = waker;
        }
    }

    fn wake_retention(&self) {
        let waker = self
            .unlocked
            .retention_waker
            .lock()
            .ok()
            .and_then(|slot| slot.clone());
        if let Some(waker) = waker {
            waker();
        }
    }

    /// Drop every expired read handle, so a lease duty frees their snapshots.
    pub fn purge_read_leases(&self, now: u64) -> Result<(), Error> {
        fn purge<S: catalog_lease::Selector, B: catalog_lease::PageBody>(
            registry: &Mutex<catalog_lease::Reads<S, B>>,
            now: u64,
        ) -> Result<(), Error> {
            registry
                .lock()
                .map_err(|_| Error::new("STORAGE_FAILURE", "read registry poisoned"))?
                .purge(now);
            Ok(())
        }
        purge(&self.pin_reads, now)?;
        purge(&self.artifact_reads, now)?;
        purge(&self.source_reads, now)?;
        purge(&self.record_reads, now)?;
        purge(&self.sparql_reads, now)?;
        // An abandoned listing build ends here at the latest: dropping it cancels its thread,
        // which then drops its snapshot.
        self.unlocked
            .source_builds
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "source listing builds poisoned"))?
            .purge(now);
        self.unlocked
            .artifact_builds
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "artifact listing builds poisoned"))?
            .purge(now);
        Ok(())
    }

    fn read_lease_stats(&self, now: u64) -> ReadLeaseStats {
        let [pin, artifact, source, record, sparql] = &*self.lease_ledgers;
        ReadLeaseStats {
            pin: catalog_lease::read_stats(pin, now),
            artifact: catalog_lease::read_stats(artifact, now),
            source: catalog_lease::read_stats(source, now),
            record: catalog_lease::read_stats(record, now),
            sparql: catalog_lease::read_stats(sparql, now),
        }
    }

    /// Count stored payload chunks, the `unique_payload_chunks` of status, from its counter
    /// row without the writer lock. It changes only when evidence is ingested or retired.
    /// `None` while the store counters seed (status `counts` is `pending`): the counter row
    /// then holds only the chunks below the seeding watermark, which is not the total.
    pub fn unique_payload_chunks(&self) -> Result<Option<u64>, Error> {
        self.refuse_poisoned()?;
        let snapshot = self.unlocked.db.snapshot();
        let reader = store_counters::Reader::snapshot(&snapshot);
        let positions = store_counters::Positions::read(|key| reader.get(key.as_bytes()))?;
        if !positions.seeded() {
            return Ok(None);
        }
        Ok(Some(
            store_counters::values(&reader)?[&store_counters::Counter::PayloadChunks],
        ))
    }

    /// Report measured database bytes, the store counters and active pins. It never takes
    /// the writer lock: every count comes from one snapshot, so a migration page or a write
    /// in progress does not delay it, and it refuses once that lock is poisoned. `counts` is
    /// `pending` while the store counters seed;
    /// `live_records`, `unique_payload_chunks` and `unique_json_projections` are then exact
    /// only through the seeding watermark, not totals. Its cost does not grow with the store.
    pub fn status(&self, now: u64) -> Result<Value, Error> {
        self.status_report(now).map(|(status, _)| status)
    }

    /// [`Store::status`] and [`Store::migrations`] from one snapshot, as the status RPC
    /// reports them.
    pub fn status_report(
        &self,
        now: u64,
    ) -> Result<(Value, Vec<migration::MigrationStatus>), Error> {
        self.status_counted(now).map(|(report, _)| report)
    }

    /// [`Store::status_report`] and the number of keys it read from its snapshot: a fixed
    /// number plus one per pin row, which the pin caps bound, whether or not the counters
    /// are seeded.
    fn status_counted(
        &self,
        now: u64,
    ) -> Result<((Value, Vec<migration::MigrationStatus>), u64), Error> {
        self.refuse_poisoned()?;
        let ReadLeaseStats {
            pin: (pin_handles, pin_bytes),
            artifact: (artifact_handles, artifact_bytes),
            source: (source_handles, source_bytes),
            record: (record_handles, record_bytes),
            sparql: (sparql_handles, sparql_bytes),
        } = self.read_lease_stats(now);
        let snapshot = self.unlocked.db.snapshot();
        let reader = store_counters::Reader::snapshot(&snapshot);
        let positions = store_counters::Positions::read(|key| reader.get(key.as_bytes()))?;
        let counters = store_counters::values(&reader)?;
        let failures = self.failures()?.clone();
        let migrations = migration::report(&reader, &positions, &counters, &failures)?;
        let mut active_pins = 0_u64;
        reader.scan(b"pin:", None, |key, bytes| {
            let pin = decode_evidence_entry::<Pin>(key, bytes)?;
            active_pins += u64::from(pin.expires_at > now);
            Ok(true)
        })?;
        let physical_bytes = linked_physical_bytes(&self.unlocked.db)?.saturating_add(
            self.unlocked
                .query_staging
                .load(std::sync::atomic::Ordering::Relaxed),
        );
        let status = store_properties(
            reader.get(b"store_format")?,
            &self.id,
            &self.unlocked.config,
        )?
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
        .with("sparql_read_handles", sparql_handles.to_string())
        .with("sparql_read_bytes", sparql_bytes.to_string())
        .with(
            "max_sparql_read_handles",
            catalog_lease::MAX_HANDLES.to_string(),
        )
        .with(
            "max_sparql_read_bytes",
            catalog_lease::HANDLE_BYTES.to_string(),
        )
        .with("sparql_read_lease_ms", catalog_lease::LEASE_MS.to_string())
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
            counters[&store_counters::Counter::JsonProjections].to_string(),
        )
        .with("physical_bytes", physical_bytes.to_string())
        .with(
            "live_records",
            counters[&store_counters::Counter::RetainedRecords].to_string(),
        )
        .with(
            "unique_payload_chunks",
            counters[&store_counters::Counter::PayloadChunks].to_string(),
        )
        .with("active_pins", active_pins.to_string())
        .with(
            "counts",
            if positions.seeded() {
                "exact"
            } else {
                "pending"
            },
        )
        .into();
        Ok(((status, migrations), reader.keys()))
    }

    /// Dev-only: what the status RPC reads, [`Store::status_report`], with the number of
    /// keys it read, so tests can show its cost is fixed.
    #[cfg(feature = "test-support")]
    pub fn status_with_key_reads_for_tests(
        &self,
        now: u64,
    ) -> Result<(Value, Vec<migration::MigrationStatus>, u64), Error> {
        let ((status, migrations), keys) = self.status_counted(now)?;
        Ok((status, migrations, keys))
    }

    /// Dev-only: whether some thread holds the writer lock now.
    #[cfg(feature = "test-support")]
    pub fn writer_busy_for_tests(&self) -> bool {
        matches!(
            self.inner.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        )
    }

    /// Dev-only: poison the writer lock, as a writer that panicked while holding it does.
    #[cfg(feature = "test-support")]
    pub fn poison_writer_for_tests(&self) {
        let joined = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let _writer = self.inner.lock();
                    panic!("a writer panics while it holds the catalog writer");
                })
                .join()
        });
        assert!(joined.is_err() && self.inner.is_poisoned());
    }

    /// Dev-only: hold every later listing build's thread with `hold` once it has read `entry`
    /// entries, so a test can observe a running build's progress.
    #[cfg(feature = "test-support")]
    pub fn hold_listing_builds_for_tests(
        &self,
        entry: u64,
        hold: impl Fn() + Send + Sync + 'static,
    ) -> Result<(), Error> {
        let poisoned = || Error::new("STORAGE_FAILURE", "listing builds poisoned");
        let hold: listing_build::Hold = (entry, std::sync::Arc::new(hold));
        self.unlocked
            .source_builds
            .lock()
            .map_err(|_| poisoned())?
            .hold_for_tests(hold.clone());
        self.unlocked
            .artifact_builds
            .lock()
            .map_err(|_| poisoned())?
            .hold_for_tests(hold);
        Ok(())
    }

    /// Dev-only: make the next source or artifact listing call that finds its build finished
    /// fail after its page is built and before the build is retired, as a failed delivery does.
    #[cfg(feature = "test-support")]
    pub fn fail_next_listing_delivery_for_tests(&self) -> Result<(), Error> {
        let poisoned = || Error::new("STORAGE_FAILURE", "listing builds poisoned");
        self.unlocked
            .source_builds
            .lock()
            .map_err(|_| poisoned())?
            .fail_next_delivery_for_tests();
        self.unlocked
            .artifact_builds
            .lock()
            .map_err(|_| poisoned())?
            .fail_next_delivery_for_tests();
        Ok(())
    }

    /// Dev-only: make every source and artifact listing call wait at most `wait` for its
    /// build before it answers with the build's cursor.
    #[cfg(feature = "test-support")]
    pub fn set_listing_build_wait_for_tests(&self, wait: std::time::Duration) -> Result<(), Error> {
        let poisoned = || Error::new("STORAGE_FAILURE", "listing builds poisoned");
        self.unlocked
            .source_builds
            .lock()
            .map_err(|_| poisoned())?
            .set_wait_for_tests(wait);
        self.unlocked
            .artifact_builds
            .lock()
            .map_err(|_| poisoned())?
            .set_wait_for_tests(wait);
        Ok(())
    }

    /// Dev-only: source then artifact listing builds, each as registered builds, threads
    /// still running and builds ever started.
    #[cfg(feature = "test-support")]
    pub fn listing_builds_for_tests(&self) -> Result<[(usize, usize, u64); 2], Error> {
        let poisoned = || Error::new("STORAGE_FAILURE", "listing builds poisoned");
        Ok([
            self.unlocked
                .source_builds
                .lock()
                .map_err(|_| poisoned())?
                .census(),
            self.unlocked
                .artifact_builds
                .lock()
                .map_err(|_| poisoned())?
                .census(),
        ])
    }

    /// Dev-only: catalog entries every finished source, then artifact, listing build has
    /// read, across its re-bases.
    #[cfg(feature = "test-support")]
    pub fn listing_build_entries_for_tests(&self) -> Result<[u64; 2], Error> {
        let poisoned = || Error::new("STORAGE_FAILURE", "listing builds poisoned");
        Ok([
            self.unlocked
                .source_builds
                .lock()
                .map_err(|_| poisoned())?
                .entries(),
            self.unlocked
                .artifact_builds
                .lock()
                .map_err(|_| poisoned())?
                .entries(),
        ])
    }

    /// Dev-only: drop every read handle and listing build whose lease ended by `now`, as
    /// the maintenance worker's lease duty does.
    #[cfg(feature = "test-support")]
    pub fn purge_read_leases_for_tests(&self, now: u64) -> Result<(), Error> {
        self.purge_read_leases(now)
    }

    /// Dev-only: hold at most `rows` owed index rows in one artifact listing window, so a
    /// small store crosses window ends; 0 restores the production window.
    #[cfg(feature = "test-support")]
    pub fn set_owed_window_for_tests(&self, rows: usize) {
        self.owed_window
            .store(rows, std::sync::atomic::Ordering::SeqCst);
    }

    /// Dev-only: make every migration page hold the writer this long before it commits.
    #[cfg(feature = "test-support")]
    pub fn hold_migration_pages_for_tests(
        &self,
        hold: Option<std::time::Duration>,
    ) -> Result<(), Error> {
        self.lock()?.page_hold.set(hold);
        Ok(())
    }
}

#[cfg(test)]
mod catalog_admission_tests {
    #[test]
    fn dense_shape_refusal_and_malformed_input_do_not_spend_admission() {
        let dense = super::json::write_compact(&super::Value::array(vec![0_u64; 1000]));
        let mut remaining = 64 * 1024;
        assert_eq!(
            super::Writer::charge_catalog_shape(&mut remaining, dense.as_bytes())
                .unwrap_err()
                .code,
            "RESOURCE_LIMIT"
        );
        assert_eq!(remaining, 64 * 1024);
        assert_eq!(
            super::Writer::charge_catalog_shape(&mut remaining, b"[null,]")
                .unwrap_err()
                .code,
            "STORAGE_FAILURE"
        );
        assert_eq!(remaining, 64 * 1024);
    }
}
