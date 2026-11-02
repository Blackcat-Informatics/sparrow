// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Durable file-generation checkpoints, published with the shared record pipeline.

use super::{
    Artifact, Catalog, DAY, Error, Store, Writer, integer, json, parse, random_id, read_field,
};
use purrdf_lex::json::{Object, Value};
use rocksdb::WriteBatch;
use std::collections::BTreeSet;

/// Maximum retained active or recent terminal file generations.
pub const MAX_TAIL_GENERATIONS: usize = 64;
/// Hard pre-decode and publication ceiling for checkpoint metadata.
pub const MAX_TAIL_REGISTRY_BYTES: usize = 512 * 1024;
pub(super) const KEY: &str = "tail-generation-registry";

/// Explicit observer declaration; the filesystem coordinator owns descriptor validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TailStart {
    /// Stable retry identity for this generation declaration.
    pub request_id: String,
    /// Configured watch identity.
    pub watch: String,
    /// Exact configured watch revision observed at generation declaration.
    pub watch_revision: u64,
    /// Original namespace directory identities, using the shared filesystem codec.
    pub directories: super::consumption::ConsumptionDirectories,
    /// Original relative Unix path, without UTF-8 conversion.
    pub path: Vec<u8>,
    /// Logical source label.
    pub source: String,
    /// One of the shared line formats, log or jsonl.
    pub format: String,
    /// Opened regular file's device identity.
    pub device: u64,
    /// Opened regular file's inode identity.
    pub inode: u64,
}

impl Catalog for TailStart {
    fn encode(&self) -> Value {
        Object::new()
            .with("request_id", self.request_id.clone())
            .with("watch", self.watch.clone())
            .with("watch_revision", self.watch_revision.to_string())
            .with("directories", self.directories.encode())
            .with("path", super::hex::encode(&self.path))
            .with("source", self.source.clone())
            .with("format", self.format.clone())
            .with("device", self.device.to_string())
            .with("inode", self.inode.to_string())
            .into()
    }
    fn decode(value: &Value) -> Result<Self, Error> {
        let start = Self {
            request_id: read_field(value, "request_id")?,
            watch: read_field(value, "watch")?,
            watch_revision: integer(value, "watch_revision")?,
            directories: super::consumption::ConsumptionDirectories::decode(super::field(
                value,
                "directories",
            )?)?,
            path: super::hex::decode_canonical(&read_field::<String>(value, "path")?)
                .map_err(|error| Error::new("STORAGE_FAILURE", error.to_string()))?,
            source: read_field(value, "source")?,
            format: read_field(value, "format")?,
            device: integer(value, "device")?,
            inode: integer(value, "inode")?,
        };
        start
            .validate()
            .map_err(|error| Error::new("STORAGE_FAILURE", error.message))?;
        Ok(start)
    }
}
impl TailStart {
    /// Allocate a bounded declaration identity using the storage identity owner.
    pub fn fresh_request_id() -> Result<String, Error> {
        random_id("tail-request")
    }
    fn validate(&self) -> Result<(), Error> {
        super::validate_ingest_labels(&self.request_id, &self.source, &self.format)?;
        super::consumption::validate_consumption_path(&self.watch, &self.path)?;
        if !["log", "jsonl"].contains(&self.format.as_str())
            || self.inode == 0
            || self.watch_revision == 0
            || self.directories.root_inode == 0
            || self.directories.parent_inode == 0
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "tail requires a line format and an opened regular-file inode",
            ));
        }
        Ok(())
    }
}

/// Immutable record byte provenance, independent of checkpoint metadata retention.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TailPosition {
    /// Opaque file-generation identity.
    pub generation_id: String,
    /// Original watch/path/opened identity and interpretation retained with a pinned record.
    pub declaration: TailStart,
    /// Inclusive original file offset.
    pub source_start: u64,
    /// Exclusive original file offset, including the original line delimiter.
    pub source_end: u64,
}
impl Catalog for TailPosition {
    fn encode(&self) -> Value {
        Object::new()
            .with("generation_id", self.generation_id.clone())
            .with("declaration", self.declaration.encode())
            .with("source_start", self.source_start.to_string())
            .with("source_end", self.source_end.to_string())
            .into()
    }
    fn decode(value: &Value) -> Result<Self, Error> {
        let position = Self {
            generation_id: read_field(value, "generation_id")?,
            declaration: TailStart::decode(super::field(value, "declaration")?)?,
            source_start: integer(value, "source_start")?,
            source_end: integer(value, "source_end")?,
        };
        if !position.generation_id.starts_with("file-generation:")
            || position.generation_id.len() > 128
            || position.source_start >= position.source_end
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "invalid immutable file-generation position",
            ));
        }
        Ok(position)
    }
}

/// Bounded durable checkpoint; reading an unfinished line does not advance offset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TailGeneration {
    /// Opaque generation identity, never derived from an inode alone.
    pub id: String,
    /// Original explicit declaration.
    pub declaration: TailStart,
    /// First acceptance instant.
    pub started_at: u64,
    /// Durably published complete byte prefix.
    pub offset: u64,
    /// Independently accepted records, including a finalized unterminated line.
    pub records: u64,
    /// Explicit completed handoff, never inferred from close or idle time.
    pub complete: bool,
    /// Observer-declared discontinuity; its missing bytes are not reconstructed.
    pub gap: Option<String>,
    /// Terminal metadata expiry; active generations remain subject to the fixed registry quota.
    pub expires_at: Option<u64>,
    /// Original durable completion intent and terminal publication clock, when declared.
    pub finish: Option<TailFinish>,
}
impl Catalog for TailGeneration {
    fn encode(&self) -> Value {
        Object::new()
            .with("finish", self.finish.as_ref().map(Catalog::encode))
            .with("id", self.id.clone())
            .with("declaration", self.declaration.encode())
            .with("started_at", self.started_at.to_string())
            .with("offset", self.offset.to_string())
            .with("records", self.records.to_string())
            .with("complete", self.complete)
            .with("gap", self.gap.clone())
            .with("expires_at", self.expires_at.map(|value| value.to_string()))
            .into()
    }
    fn decode(value: &Value) -> Result<Self, Error> {
        let generation = Self {
            finish: value
                .get("finish")
                .filter(|value| !value.is_null())
                .map(TailFinish::decode)
                .transpose()?,
            id: read_field(value, "id")?,
            declaration: TailStart::decode(super::field(value, "declaration")?)?,
            started_at: integer(value, "started_at")?,
            offset: integer(value, "offset")?,
            records: integer(value, "records")?,
            complete: read_field(value, "complete")?,
            gap: read_field(value, "gap")?,
            expires_at: read_field::<Option<String>>(value, "expires_at")?
                .map(|value| {
                    value.parse::<u64>().map_err(|_| {
                        Error::new("STORAGE_FAILURE", "invalid tail metadata deadline")
                    })
                })
                .transpose()?,
        };
        if !generation.id.starts_with("file-generation:")
            || generation.id.len() > 128
            || generation.records > generation.offset
            || (generation.complete && generation.gap.is_some())
            || (generation.complete || generation.gap.is_some()) != generation.expires_at.is_some()
            || generation
                .expires_at
                .is_some_and(|deadline| deadline < generation.started_at)
            || generation.gap.as_ref().is_some_and(|gap| {
                !["truncated", "identity_changed", "read_gap"].contains(&gap.as_str())
            })
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "invalid file-generation checkpoint state",
            ));
        }
        if let Some(finish) = &generation.finish {
            let observed = &finish.intent.observed;
            if finish.intent.generation_id != generation.id
                || finish.intent.watch_revision != generation.declaration.watch_revision
                || (observed.device, observed.inode)
                    != (generation.declaration.device, generation.declaration.inode)
                || generation.offset > observed.byte_length
                || finish.finished_at.is_some() != generation.complete
                || (generation.complete && generation.offset != observed.byte_length)
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "tail finish contradicts its generation checkpoint",
                ));
            }
        }
        Ok(generation)
    }
}

/// Immutable producer assertion that one opened generation has finished writing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TailFinishIntent {
    /// Stable completion retry identity, separate from declaration admission.
    pub request_id: String,
    /// Original opaque file generation.
    pub generation_id: String,
    /// Original watch revision authorizing this generation.
    pub watch_revision: u64,
    /// Exact final descriptor observation, using the shared file identity codec.
    pub observed: super::consumption::ConsumptionGeneration,
}
impl TailFinishIntent {
    fn validate(&self) -> Result<(), Error> {
        if self.request_id.is_empty()
            || self.request_id.len() > 128
            || !self.generation_id.starts_with("file-generation:")
            || self.generation_id.len() > 128
            || self.watch_revision == 0
            || self.observed.inode == 0
            || self.observed.modified.1 >= 1_000_000_000
            || self.observed.changed.1 >= 1_000_000_000
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "tail finish requires bounded request/generation identities, a revision and a valid final file observation",
            ));
        }
        Ok(())
    }
}
impl Catalog for TailFinishIntent {
    fn encode(&self) -> Value {
        Object::new()
            .with("request_id", self.request_id.clone())
            .with("generation_id", self.generation_id.clone())
            .with("watch_revision", self.watch_revision.to_string())
            .with("observed", self.observed.encode())
            .into()
    }
    fn decode(value: &Value) -> Result<Self, Error> {
        let intent = Self {
            request_id: read_field(value, "request_id")?,
            generation_id: read_field(value, "generation_id")?,
            watch_revision: integer(value, "watch_revision")?,
            observed: super::consumption::ConsumptionGeneration::decode(super::field(
                value, "observed",
            )?)?,
        };
        intent
            .validate()
            .map_err(|error| Error::new("STORAGE_FAILURE", error.message))?;
        Ok(intent)
    }
}

/// Durable completion admission and the original terminal publication instant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TailFinish {
    /// Immutable original producer intent.
    pub intent: TailFinishIntent,
    /// First durable completion admission.
    pub admitted_at: u64,
    /// Set atomically with the final checkpoint; retries never refresh it.
    pub finished_at: Option<u64>,
}
impl Catalog for TailFinish {
    fn encode(&self) -> Value {
        Object::new()
            .with("intent", self.intent.encode())
            .with("admitted_at", self.admitted_at.to_string())
            .with("finished_at", self.finished_at.map(|at| at.to_string()))
            .into()
    }
    fn decode(value: &Value) -> Result<Self, Error> {
        Ok(Self {
            intent: TailFinishIntent::decode(super::field(value, "intent")?)?,
            admitted_at: integer(value, "admitted_at")?,
            finished_at: read_field::<Option<String>>(value, "finished_at")?
                .map(|value| {
                    value
                        .parse()
                        .map_err(|_| Error::new("STORAGE_FAILURE", "invalid tail finish instant"))
                })
                .transpose()?,
        })
    }
}

fn finished_request<'a>(
    rows: &'a [TailGeneration],
    intent: &TailFinishIntent,
) -> Result<Option<&'a TailGeneration>, Error> {
    let original = rows.iter().find(|row| {
        row.finish
            .as_ref()
            .is_some_and(|finish| finish.intent.request_id == intent.request_id)
    });
    if original.is_some_and(|row| {
        row.finish
            .as_ref()
            .is_none_or(|finish| finish.intent != *intent)
    }) {
        return Err(Error::new(
            "CONFLICT",
            "tail finish request already binds a different final generation",
        ));
    }
    Ok(original)
}

/// One accepted byte prefix; any pending suffix remains at the source checkpoint.
#[derive(Clone, Debug)]
pub struct TailPublication {
    /// Original accepted generation.
    pub generation_id: String,
    /// Prefix start at the immutable publication boundary.
    pub source_start: u64,
    /// Prefix end at the immutable publication boundary.
    pub source_end: u64,
    /// Number of observed suffix bytes not accepted as a completed record.
    pub pending_bytes: usize,
    /// Accepted complete batch representation, not a claim of complete-file capture.
    pub artifact: Option<Artifact>,
    /// Explicit generation completion.
    pub complete: bool,
}

#[derive(Clone, Copy)]
pub(super) struct Append<'a> {
    pub id: &'a str,
    pub offset: u64,
    pub end: u64,
    pub complete: bool,
}
impl Append<'_> {
    pub(super) fn encode(&self) -> Value {
        Object::new()
            .with("generation_id", self.id)
            .with("offset", self.offset.to_string())
            .with("end", self.end.to_string())
            .with("complete", self.complete)
            .into()
    }
}

pub(super) fn registry(writer: &Writer) -> Result<Vec<TailGeneration>, Error> {
    let raw = writer.db.get(KEY)?.ok_or_else(|| {
        Error::new(
            "STORAGE_FAILURE",
            "file-generation checkpoint registry missing",
        )
    })?;
    decode_registry(&raw)
}

pub(super) fn decode_registry(raw: &[u8]) -> Result<Vec<TailGeneration>, Error> {
    if raw.len() > MAX_TAIL_REGISTRY_BYTES {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "file-generation registry exceeds decode ceiling",
        ));
    }
    let value = parse(raw)?;
    let rows = value.as_array().ok_or_else(|| {
        Error::new(
            "STORAGE_FAILURE",
            "file-generation registry is not an array",
        )
    })?;
    if rows.len() > MAX_TAIL_GENERATIONS {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "file-generation registry exceeds count ceiling",
        ));
    }
    let mut ids = BTreeSet::new();
    let mut requests = BTreeSet::new();
    let mut finishes = BTreeSet::new();
    let generations = rows
        .iter()
        .map(TailGeneration::decode)
        .collect::<Result<Vec<_>, _>>()?;
    if generations.iter().any(|row| {
        !ids.insert(&row.id)
            || !requests.insert(&row.declaration.request_id)
            || row
                .finish
                .as_ref()
                .is_some_and(|finish| !finishes.insert(&finish.intent.request_id))
    }) {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "duplicate file-generation registry identity",
        ));
    }
    Ok(generations)
}

fn bytes(generations: &[TailGeneration]) -> Result<Vec<u8>, Error> {
    let encoded = json::write_compact(&Value::array(generations.iter().map(Catalog::encode)));
    if encoded.len() > MAX_TAIL_REGISTRY_BYTES {
        return Err(Error::new(
            "RESOURCE_LIMIT",
            "file-generation metadata exceeds its publication quota",
        ));
    }
    Ok(encoded.into_bytes())
}

pub(super) struct Prepared {
    pub encoded: Vec<u8>,
    pub id: String,
    pub declaration: TailStart,
    pub offset: u64,
}

pub(super) fn prepare(
    writer: &Writer,
    source: &str,
    format: &str,
    input: Append<'_>,
    count: usize,
    now: u64,
) -> Result<Prepared, Error> {
    let mut generations = registry(writer)?;
    let row = generations
        .iter_mut()
        .find(|row| row.id == input.id)
        .ok_or_else(|| Error::new("NOT_FOUND", "file generation absent"))?;
    if row.complete
        || row.gap.is_some()
        || row.offset != input.offset
        || row.declaration.source != source
        || row.declaration.format != format
    {
        return Err(Error::new(
            "CONFLICT",
            "tail checkpoint, source, format or lifecycle changed before publication",
        ));
    }
    if input.end < input.offset {
        return Err(Error::new(
            "INVALID_ARGUMENT",
            "tail checkpoint cannot move backwards",
        ));
    }
    if let Some(finish) = &row.finish
        && (input.end > finish.intent.observed.byte_length
            || (input.complete && input.end != finish.intent.observed.byte_length))
    {
        return Err(Error::new(
            "CONFLICT",
            "tail publication exceeds or incompletely finalizes its immutable finish boundary",
        ));
    }
    row.records = row
        .records
        .checked_add(count as u64)
        .ok_or_else(|| Error::new("RESOURCE_LIMIT", "tail record count overflow"))?;
    row.offset = input.end;
    row.complete = input.complete;
    if input.complete {
        if let Some(finish) = &mut row.finish {
            finish.finished_at = Some(now);
        }
        row.expires_at =
            Some(now.checked_add(30 * DAY).ok_or_else(|| {
                Error::new("INVALID_ARGUMENT", "tail terminal deadline overflow")
            })?);
    }
    let id = row.id.clone();
    let declaration = row.declaration.clone();
    Ok(Prepared {
        encoded: bytes(&generations)?,
        id,
        declaration,
        offset: input.offset,
    })
}

impl Store {
    /// Capture finite checkpoint metadata for one configured watch, including terminal rows.
    pub fn tail_generations(&self, watch: &str) -> Result<Vec<TailGeneration>, Error> {
        if watch.is_empty() || watch.len() > 128 {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "tail watch requires 1..128 bytes",
            ));
        }
        let writer = self.lock()?;
        Ok(registry(&writer)?
            .into_iter()
            .filter(|row| row.declaration.watch == watch)
            .collect())
    }
    /// Read original completion admission before probing today's watch or source descriptor.
    pub fn tail_finish_get(
        &self,
        intent: &TailFinishIntent,
    ) -> Result<Option<TailGeneration>, Error> {
        intent.validate()?;
        let writer = self.lock()?;
        Ok(finished_request(&registry(&writer)?, intent)?.cloned())
    }

    /// Durably bind a validated final observation; the coordinator owns descriptor certification.
    pub fn tail_finish_start(
        &self,
        intent: &TailFinishIntent,
        now: u64,
    ) -> Result<TailGeneration, Error> {
        intent.validate()?;
        let writer = self.lock()?;
        let mut rows = registry(&writer)?;
        if let Some(original) = finished_request(&rows, intent)? {
            return Ok(original.clone());
        }
        let row = rows
            .iter_mut()
            .find(|row| row.id == intent.generation_id)
            .ok_or_else(|| Error::new("NOT_FOUND", "file generation absent"))?;
        if row.complete
            || row.gap.is_some()
            || row.finish.is_some()
            || row.declaration.watch_revision != intent.watch_revision
            || (row.declaration.device, row.declaration.inode)
                != (intent.observed.device, intent.observed.inode)
            || row.offset > intent.observed.byte_length
        {
            return Err(Error::new(
                "CONFLICT",
                "tail finish conflicts with current generation identity, revision or checkpoint",
            ));
        }
        row.finish = Some(TailFinish {
            intent: intent.clone(),
            admitted_at: now,
            finished_at: None,
        });
        let result = row.clone();
        let mut batch = WriteBatch::default();
        batch.put(KEY, bytes(&rows)?);
        writer.admit_import_batch(&batch)?;
        writer.commit(batch)?;
        Ok(result)
    }

    /// Declare a generation once; exact declaration retries preserve its current checkpoint.
    pub fn tail_start(&self, declaration: &TailStart, now: u64) -> Result<TailGeneration, Error> {
        declaration.validate()?;
        let writer = self.lock()?;
        let mut generations = registry(&writer)?;
        if let Some(row) = generations
            .iter()
            .find(|row| row.declaration.request_id == declaration.request_id)
        {
            if &row.declaration != declaration {
                return Err(Error::new(
                    "CONFLICT",
                    "tail declaration retry changed identity or interpretation",
                ));
            }
            return Ok(row.clone());
        }
        generations.retain(|row| row.expires_at.is_none_or(|deadline| deadline > now));
        if generations.len() >= MAX_TAIL_GENERATIONS {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "file-generation checkpoint registry is full; resolve active generations or await terminal expiry",
            ));
        }
        if generations.iter().any(|row| {
            !row.complete
                && row.gap.is_none()
                && row.declaration.watch == declaration.watch
                && row.declaration.device == declaration.device
                && row.declaration.inode == declaration.inode
        }) {
            return Err(Error::new(
                "CONFLICT",
                "opened identity already belongs to an active generation; declare its discontinuity before replacing it",
            ));
        }
        let row = TailGeneration {
            id: random_id("file-generation")?,
            declaration: declaration.clone(),
            started_at: now,
            offset: 0,
            records: 0,
            complete: false,
            gap: None,
            expires_at: None,
            finish: None,
        };
        generations.push(row.clone());
        let mut batch = WriteBatch::default();
        batch.put(KEY, bytes(&generations)?);
        writer.admit_import_batch(&batch)?;
        writer.commit(batch)?;
        Ok(row)
    }

    /// Read finite checkpoint metadata; it does not retain expired record bytes.
    pub fn tail_generation(&self, id: &str) -> Result<TailGeneration, Error> {
        let writer = self.lock()?;
        registry(&writer)?
            .into_iter()
            .find(|row| row.id == id)
            .ok_or_else(|| Error::new("NOT_FOUND", "file generation absent"))
    }

    /// Observe a discontinuity without inventing a replacement checkpoint or lost bytes.
    pub fn tail_discontinue(
        &self,
        id: &str,
        offset: u64,
        reason: &str,
        now: u64,
    ) -> Result<TailGeneration, Error> {
        if !["truncated", "identity_changed", "read_gap"].contains(&reason) {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "unsupported tail discontinuity reason",
            ));
        }
        let writer = self.lock()?;
        let mut generations = registry(&writer)?;
        let row = generations
            .iter_mut()
            .find(|row| row.id == id)
            .ok_or_else(|| Error::new("NOT_FOUND", "file generation absent"))?;
        if row.offset != offset
            || row.complete
            || row.gap.as_deref().is_some_and(|gap| gap != reason)
        {
            return Err(Error::new(
                "CONFLICT",
                "tail discontinuity conflicts with the current checkpoint",
            ));
        }
        if row.gap.is_none() {
            row.gap = Some(reason.into());
            row.expires_at = Some(now.checked_add(30 * DAY).ok_or_else(|| {
                Error::new("INVALID_ARGUMENT", "tail terminal deadline overflow")
            })?);
        }
        let result = row.clone();
        let mut batch = WriteBatch::default();
        batch.put(KEY, bytes(&generations)?);
        writer.admit_import_batch(&batch)?;
        writer.commit(batch)?;
        Ok(result)
    }

    /// Publish only complete line bytes and their checkpoint in the same durable batch.
    pub fn tail_append(
        &self,
        id: &str,
        offset: u64,
        input: &[u8],
        complete: bool,
        now: u64,
    ) -> Result<TailPublication, Error> {
        let generation = self.tail_generation(id)?;
        let length = super::record_ends(&generation.declaration.format, input, complete)
            .last()
            .unwrap_or(0);
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "tail byte offset overflow"))?;
        let append = Append {
            id,
            offset,
            end,
            complete,
        };
        let artifact = if length == 0 {
            let writer = self.lock()?;
            if !(complete && generation.complete && generation.offset == offset) {
                let encoded = prepare(
                    &writer,
                    &generation.declaration.source,
                    &generation.declaration.format,
                    append,
                    0,
                    now,
                )?;
                if complete {
                    let mut batch = WriteBatch::default();
                    batch.put(KEY, encoded.encoded);
                    writer.admit_import_batch(&batch)?;
                    writer.commit(batch)?;
                }
            }
            None
        } else {
            let request = format!(
                "tail:{}:{offset}:{end}:{}",
                id.strip_prefix("file-generation:")
                    .ok_or_else(|| Error::new(
                        "INVALID_ARGUMENT",
                        "wrong file-generation identity"
                    ))?,
                u8::from(complete)
            );
            Some(self.ingest_input(
                &request,
                &generation.declaration.source,
                &generation.declaration.format,
                &input[..length],
                super::ingest::Origin::Tail(append),
                now,
                super::durability::Durability::Durable,
            )?)
        };
        Ok(TailPublication {
            generation_id: id.into(),
            source_start: offset,
            source_end: end,
            pending_bytes: input.len() - length,
            artifact,
            complete,
        })
    }
}
