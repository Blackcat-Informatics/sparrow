// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Frozen reference for the entry walk: the retention accounting as it stood before the
//! streamed walk, kept for the oracle. A copy is deliberate. The oracle compares the production
//! answer with this one, so the reference must not share the parse or charge code the change
//! rewrote. The retirement state record, the catalog codec, the writer's pin, record and payload
//! reads are shared and unchanged by the change. Test support only; it is not in production.

use super::super::record_read::QueryCancellation;
use super::super::retention_metrics::{Admission, RetentionMetrics, RetentionReadLimits, add};
use super::super::{Artifact, CATALOG_LIMITS, Catalog, Error, Record, Store, Writer, hex, json};
use super::{State, failure, reserve};
use std::collections::BTreeMap;
use std::ops::Range;

/// Catalog bytes the frozen walk charges for each distinct digest its ownership map holds.
const OWNER_CHARGE: usize = 512;

#[derive(Default)]
struct Ownership {
    published: u64,
    live: bool,
    pinned: bool,
}

/// Fold each occurrence of `chunks` into the frozen ownership map at the pre-change charge of
/// 512 catalog bytes per new digest. The visit and the checked addition are the production ones.
fn legacy_owners(
    admission: &mut Admission<'_>,
    map: &mut BTreeMap<String, Ownership>,
    chunks: &[String],
    live: bool,
    pinned: bool,
) -> Result<(), Error> {
    for digest in chunks {
        admission.visit()?;
        if hex::decode_32_canonical(digest).is_none() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "retention ownership has a noncanonical payload identity",
            ));
        }
        if !map.contains_key(digest) {
            admission.remaining =
                admission
                    .remaining
                    .checked_sub(OWNER_CHARGE)
                    .ok_or_else(|| {
                        Error::new(
                            "RESOURCE_LIMIT",
                            "retention ownership map exceeds catalog_bytes",
                        )
                    })?;
        }
        let owner = map.entry(digest.clone()).or_default();
        owner.published = add(owner.published, 1)?;
        owner.live |= live;
        owner.pinned |= pinned;
    }
    Ok(())
}

struct LegacyArray {
    range: Range<usize>,
    count: u64,
    prefix: Vec<String>,
    found: bool,
}

/// The pre-change chunk and record array reader: keeps `take` elements after `skip`.
fn legacy_strings(
    reader: &mut json::Reader<'_>,
    remaining: &mut usize,
    skip: u64,
    take: usize,
    target: Option<&str>,
) -> Result<LegacyArray, Error> {
    let json::Event::BeginArray { at } = reader.next_event()? else {
        return Err(failure("artifact manifest member must be an array"));
    };
    let mut result = LegacyArray {
        range: at..at,
        count: 0,
        prefix: Vec::new(),
        found: false,
    };
    loop {
        match reader.next_event()? {
            json::Event::String(raw) => {
                if raw.has_escapes() && raw.raw().len() > *remaining {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "manifest escape decoding exceeds admission",
                    ));
                }
                let decoded = raw.decode()?;
                if decoded.len() > CATALOG_LIMITS.max_string_bytes {
                    return Err(failure("manifest string exceeds native catalog limit"));
                }
                result.found |= target.is_some_and(|target| decoded == target);
                if result.count >= skip && result.prefix.len() < take {
                    reserve(
                        remaining,
                        raw.raw()
                            .len()
                            .saturating_mul(2)
                            .saturating_add(2 * std::mem::size_of::<String>()),
                    )?;
                    result.prefix.push(decoded.into_owned());
                }
                result.count += 1;
            }
            json::Event::EndArray { at } => {
                result.range.end = at + 1;
                return Ok(result);
            }
            _ => {
                return Err(failure(
                    "artifact manifest array contains a non-string value",
                ));
            }
        }
    }
}

/// The pre-change artifact header reader, with the same members, charges and checks.
fn legacy_header(
    bytes: &[u8],
    remaining: &mut usize,
    skip: u64,
    take: usize,
) -> Result<(Artifact, u64, LegacyArray), Error> {
    let mut reader = json::Reader::from_slice(bytes, CATALOG_LIMITS)?;
    if !matches!(reader.next_event()?, json::Event::BeginObject { .. }) {
        return Err(failure("artifact catalog must be an object"));
    }
    let mut fields = json::Object::new();
    let mut records = None;
    let mut chunks = None;
    loop {
        let raw = match reader.next_event()? {
            json::Event::Key(raw) => raw,
            json::Event::EndObject { .. } => break,
            _ => return Err(failure("artifact catalog member expected")),
        };
        reserve(
            remaining,
            raw.raw().len().saturating_mul(64).saturating_add(1024),
        )?;
        let name = raw.decode()?.into_owned();
        if fields.contains_key(&name) {
            return Err(failure("duplicate artifact catalog member"));
        }
        let value = match name.as_str() {
            "record_ids" => {
                records = Some(legacy_strings(&mut reader, remaining, 0, 0, None)?);
                json::Value::array(Vec::<String>::new())
            }
            "chunks" => {
                chunks = Some(legacy_strings(&mut reader, remaining, skip, take, None)?);
                json::Value::array(Vec::<String>::new())
            }
            "intake" => {
                let slice = &bytes[reader.skip_value()?];
                reserve(
                    remaining,
                    slice.len().saturating_mul(64).saturating_add(1024),
                )?;
                json::read_slice(slice, CATALOG_LIMITS)?
            }
            _ => {
                let start = match reader.next_event()? {
                    json::Event::String(raw) => raw.span().start - 1,
                    json::Event::Number { at, .. }
                    | json::Event::Bool { at, .. }
                    | json::Event::Null { at } => at,
                    _ => return Err(failure("unsupported complex artifact catalog member")),
                };
                let slice = &bytes[start..reader.offset()];
                reserve(
                    remaining,
                    slice.len().saturating_mul(64).saturating_add(1024),
                )?;
                json::read_slice(slice, CATALOG_LIMITS)?
            }
        };
        fields.push(name, value);
    }
    if !matches!(reader.next_event()?, json::Event::End) {
        return Err(failure("artifact catalog has trailing data"));
    }
    let artifact = Artifact::decode(&fields.into())?;
    let records = records.ok_or_else(|| failure("missing record manifest"))?;
    let chunks = chunks.ok_or_else(|| failure("missing payload manifest"))?;
    if records.count == 0 && (artifact.byte_length != 0 || chunks.count != 0) {
        return Err(failure(
            "recordless artifact has unaccounted original bytes",
        ));
    }
    Ok((artifact, records.count, chunks))
}

/// The pre-change envelope: the row's identity, deadlines, retirement state and the first
/// `take` chunk identities after the retired position.
struct LegacyEnvelope {
    id: String,
    expires_at: u64,
    received_at: u64,
    root_evicted: bool,
    prefix: Vec<String>,
    state: State,
}

impl LegacyEnvelope {
    fn evicted(&self) -> bool {
        self.root_evicted || self.state.evicted
    }

    fn pending(&self) -> u64 {
        self.state.total - self.state.retired
    }
}

fn legacy_envelope(
    db: &rocksdb::DB,
    id: &str,
    bytes: &[u8],
    take: usize,
    remaining: &mut usize,
) -> Result<LegacyEnvelope, Error> {
    let mut available = *remaining;
    reserve(
        &mut available,
        bytes.len().saturating_mul(2).saturating_add(8192),
    )?;
    let saved = State::read(db, id)?;
    let (artifact, _records, chunks) = legacy_header(
        bytes,
        &mut available,
        saved.map_or(0, |state| state.retired),
        take,
    )?;
    if artifact.id != id {
        return Err(failure("artifact catalog identity disagrees with its key"));
    }
    let state = saved.unwrap_or(State {
        total: chunks.count,
        ..State::default()
    });
    if state.total != chunks.count {
        return Err(failure(
            "manifest retirement count disagrees with stored artifact",
        ));
    }
    let envelope = LegacyEnvelope {
        id: artifact.id,
        expires_at: artifact.expires_at,
        received_at: artifact.received_at,
        root_evicted: artifact.evicted,
        prefix: chunks.prefix,
        state,
    };
    *remaining = available;
    Ok(envelope)
}

impl Store {
    /// The pre-change entry walk: every owner entry is read, whatever the owner summary holds.
    /// Refusal codes and totals are those of the walk before the streamed walk.
    #[doc(hidden)]
    pub fn retention_metrics_entry_walk_reference_for_tests(
        &self,
        now: u64,
        limits: RetentionReadLimits,
    ) -> Result<RetentionMetrics, Error> {
        let cancellation = QueryCancellation::default();
        if limits.max_visits == 0
            || limits.max_visits > 100_000
            || limits.catalog_bytes == 0
            || limits.catalog_bytes > 32 * 1024 * 1024
            || limits.payload_bytes == 0
            || limits.payload_bytes > 1024 * 1024 * 1024
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "retention metrics require max_visits 1..100000, catalog_bytes 1..33554432 and payload_bytes 1..1073741824",
            ));
        }
        crate::format_instant(now)?;
        cancellation.checkpoint()?;
        let writer = self.lock()?;
        let mut admission = Admission {
            limits,
            visits: 0,
            remaining: limits.catalog_bytes,
            payload: limits.payload_bytes,
            cancellation: &cancellation,
        };
        let mut pin_visits = 0_u64;
        let protected = writer.protected_matching(
            now,
            |_| true,
            || {
                cancellation.checkpoint()?;
                if pin_visits >= limits.max_visits as u64 {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "retention pin scan exceeds max_visits",
                    ));
                }
                pin_visits += 1;
                Ok(())
            },
            Some(1024 * 1024),
            Some(&mut admission.remaining),
        )?;
        admission.visits = pin_visits;
        let mut report = RetentionMetrics {
            store_id: writer.id.clone(),
            as_of: now,
            ..Default::default()
        };
        let mut owners = BTreeMap::new();
        for prefix in [&b"artifact:"[..], &b"record:"[..], &b"ref:"[..]] {
            let mut cursor = writer.db.raw_iterator();
            cursor.seek(prefix);
            while cursor.valid() {
                let key_bytes = cursor.key().ok_or_else(|| {
                    Error::new("STORAGE_FAILURE", "retention catalog cursor lost its key")
                })?;
                if !key_bytes.starts_with(prefix) {
                    break;
                }
                if prefix != b"ref:" && (key_bytes.len() <= prefix.len() || key_bytes.len() > 128) {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "retention catalog requires a bounded evidence identity",
                    ));
                }
                admission.visit()?;
                let bytes = cursor.value().ok_or_else(|| {
                    Error::new("STORAGE_FAILURE", "retention catalog cursor lost its value")
                })?;
                let id = std::str::from_utf8(key_bytes)
                    .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
                match prefix {
                    b"artifact:" => {
                        let before_envelope = admission.remaining;
                        let artifact = legacy_envelope(
                            &writer.db,
                            id,
                            bytes,
                            limits.max_visits,
                            &mut admission.remaining,
                        )?;
                        if artifact.pending() != artifact.prefix.len() as u64 {
                            return Err(Error::new(
                                "RESOURCE_LIMIT",
                                "retention manifest ownership exceeds max_visits",
                            ));
                        }
                        let pinned = protected.contains(&artifact.id);
                        if artifact.expires_at <= artifact.received_at {
                            return Err(Error::new(
                                "STORAGE_FAILURE",
                                "retention artifact deadline contradicts its receipt",
                            ));
                        }
                        let live = Writer::artifact_eligible(
                            &artifact.id,
                            artifact.evicted(),
                            artifact.expires_at,
                            now,
                            &protected,
                        );
                        if live {
                            report.live_artifacts = add(report.live_artifacts, 1)?;
                            report.pinned_artifacts =
                                add(report.pinned_artifacts, u64::from(pinned))?;
                            report.observe(artifact.received_at)?;
                        }
                        let envelope_charge = before_envelope - admission.remaining;
                        legacy_owners(
                            &mut admission,
                            &mut owners,
                            &artifact.prefix,
                            live,
                            live && pinned,
                        )?;
                        admission.remaining += envelope_charge;
                    }
                    b"record:" => {
                        let shape_before = admission.remaining;
                        Writer::charge_catalog_shape(&mut admission.remaining, bytes)?;
                        let record: Record = writer.decode_evidence(key_bytes, bytes)?;
                        let shape_charge = shape_before - admission.remaining;
                        writer.validate_event_record(&record)?;
                        if record.expires_at <= record.received_at
                            || record.byte_end < record.byte_start
                            || !record.artifact_id.starts_with("artifact:")
                            || record.artifact_id.len() > 128
                        {
                            return Err(Error::new(
                                "STORAGE_FAILURE",
                                "retention record has invalid clocks, range or parent identity",
                            ));
                        }
                        let live = writer.live_record(&record, now, &protected);
                        let pinned = protected.contains(&record.id)
                            || protected.contains(&record.artifact_id);
                        if live {
                            report.live_records = add(report.live_records, 1)?;
                            report.pinned_records = add(report.pinned_records, u64::from(pinned))?;
                            report.observe(record.received_at)?;
                        }
                        legacy_owners(
                            &mut admission,
                            &mut owners,
                            &record.chunks,
                            live,
                            live && pinned,
                        )?;
                        admission.remaining += shape_charge;
                    }
                    _ => {
                        let digest = id.strip_prefix("ref:").ok_or_else(|| {
                            Error::new("STORAGE_FAILURE", "retention reference prefix disagrees")
                        })?;
                        if hex::decode_32_canonical(digest).is_none() {
                            return Err(Error::new(
                                "STORAGE_FAILURE",
                                "retention reference has a noncanonical payload identity",
                            ));
                        }
                        let references = u64::from_be_bytes(bytes.try_into().map_err(|_| {
                            Error::new(
                                "STORAGE_FAILURE",
                                "invalid retention payload reference counter",
                            )
                        })?);
                        let owner = owners.remove(digest).unwrap_or_default();
                        if references == 0 || references < owner.published {
                            return Err(Error::new(
                                "STORAGE_FAILURE",
                                "retention payload ownership contradicts its native reference counter",
                            ));
                        }
                        cancellation.checkpoint()?;
                        let payload =
                            writer.payload_chunk_view(digest, Some(&mut admission.payload))?;
                        let length = payload.len() as u64;
                        report.payload_chunks = add(report.payload_chunks, 1)?;
                        report.owned_payload_bytes = add(report.owned_payload_bytes, length)?;
                        if owner.live {
                            report.live_payload_bytes = add(report.live_payload_bytes, length)?;
                        }
                        if owner.pinned {
                            report.pin_held_payload_bytes =
                                add(report.pin_held_payload_bytes, length)?;
                        }
                        if references == owner.published && !owner.live {
                            report.retirable_payload_bytes =
                                add(report.retirable_payload_bytes, length)?;
                        }
                        if references > owner.published {
                            report.unclassified_payload_bytes =
                                add(report.unclassified_payload_bytes, length)?;
                        }
                    }
                }
                cursor.next();
            }
            cursor.status()?;
        }
        if !owners.is_empty() {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "retention catalog references payloads without native ownership counters",
            ));
        }
        cancellation.checkpoint()?;
        report.retained_window_ms = report.oldest_received_at.map_or(0, |oldest| now - oldest);
        report.visits = admission.visits;
        Ok(report)
    }
}
