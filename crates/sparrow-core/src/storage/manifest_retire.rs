// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Bounded manifest retirement over unchanged, native-encoded artifact catalogs.

use super::{Artifact, CATALOG_LIMITS, Catalog, Error, Writer, json};
use std::ops::Range;

pub(super) const FORMAT: &[u8] = b"sparrow.store/42";
pub(super) const WATCH_FORMAT: &[u8] = b"sparrow.store/43";
pub(super) const PREFIX: &[u8] = b"artifact-retirement:";
pub(super) const FEATURES: &str = "manifest-retirement-features";

pub(super) fn features(db: &rocksdb::DB) -> Result<Option<bool>, Error> {
    let fenced = matches!(
        super::writer_format::read(db)?.as_deref(),
        Some(FORMAT | WATCH_FORMAT)
    );
    match (fenced, db.get_pinned(FEATURES)?) {
        (false, None) => Ok(None),
        (true, Some(bytes)) if bytes.as_ref() == [0] || bytes.as_ref() == [1] => {
            Ok(Some(bytes[0] == 1))
        }
        _ => Err(failure(
            "manifest retirement features and writer fence disagree",
        )),
    }
}

fn failure(message: &str) -> Error {
    Error::new("STORAGE_FAILURE", message)
}
fn reserve(remaining: &mut usize, bytes: usize) -> Result<(), Error> {
    *remaining = remaining.checked_sub(bytes).ok_or_else(|| {
        Error::new(
            "RESOURCE_LIMIT",
            "manifest retirement exceeds catalog admission",
        )
    })?;
    Ok(())
}
fn key(id: &str) -> String {
    format!("artifact-retirement:{id}")
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct State {
    total: u64,
    retired: u64,
    evicted: bool,
}

impl State {
    fn read(db: &rocksdb::DB, id: &str) -> Result<Option<Self>, Error> {
        let Some(bytes) = db.get_pinned(key(id))? else {
            return Ok(None);
        };
        if bytes.len() != 17
            || bytes[16] > 1
            || !matches!(
                super::writer_format::read(db)?.as_deref(),
                Some(FORMAT | WATCH_FORMAT)
            )
        {
            return Err(failure(
                "invalid manifest retirement state or missing writer fence",
            ));
        }
        let state = Self {
            total: u64::from_be_bytes(
                bytes[..8]
                    .try_into()
                    .map_err(|_| failure("manifest count width"))?,
            ),
            retired: u64::from_be_bytes(
                bytes[8..16]
                    .try_into()
                    .map_err(|_| failure("manifest position width"))?,
            ),
            evicted: bytes[16] == 1,
        };
        if state.retired > state.total {
            return Err(failure(
                "manifest retirement position exceeds its original count",
            ));
        }
        Ok(Some(state))
    }
}

/// Apply effective ownership only to stored catalogs, never to original retry receipts.
pub(super) fn resolve(writer: &Writer, artifact: &mut Artifact) -> Result<(), Error> {
    if let Some(state) = State::read(&writer.db, &artifact.id)? {
        if state.total != artifact.chunks.len() as u64 {
            return Err(failure(
                "manifest retirement count disagrees with stored artifact",
            ));
        }
        artifact.chunks.drain(
            ..usize::try_from(state.retired)
                .map_err(|_| failure("manifest position exceeds address space"))?,
        );
        artifact.evicted |= state.evicted;
    }
    Ok(())
}

pub(super) fn remove(batch: &mut rocksdb::WriteBatch, id: &str) {
    batch.delete(key(id));
}

struct Array {
    range: Range<usize>,
    count: u64,
    prefix: Vec<String>,
    found: bool,
}

/// The native reader owns grammar and escape decoding; only domain string arrays are accepted.
fn strings(
    reader: &mut json::Reader<'_>,
    remaining: &mut usize,
    skip: u64,
    take: usize,
    target: Option<&str>,
) -> Result<Array, Error> {
    let json::Event::BeginArray { at } = reader.next_event()? else {
        return Err(failure("artifact manifest member must be an array"));
    };
    let mut result = Array {
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

pub(super) struct Header {
    /// Scalars only. Native array traversal validates membership without owning it.
    pub artifact: Artifact,
    pub record_count: u64,
    records: Range<usize>,
    chunks: Array,
}

/// Validate the existing artifact codec's scalar fields without owning its large arrays.
pub(super) fn header(
    bytes: &[u8],
    remaining: &mut usize,
    skip: u64,
    take: usize,
) -> Result<Header, Error> {
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
                records = Some(strings(&mut reader, remaining, 0, 0, None)?);
                json::Value::array(Vec::<String>::new())
            }
            "chunks" => {
                chunks = Some(strings(&mut reader, remaining, skip, take, None)?);
                json::Value::array(Vec::<String>::new())
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
    // The arrays were individually type/grammar checked. The existing codec owns
    // every scalar shape, required field and integer conversion.
    let artifact = Artifact::decode(&fields.into())?;
    let records = records.ok_or_else(|| failure("missing record manifest"))?;
    let chunks = chunks.ok_or_else(|| failure("missing payload manifest"))?;
    if records.count == 0 && (artifact.byte_length != 0 || chunks.count != 0) {
        return Err(failure(
            "recordless artifact has unaccounted original bytes",
        ));
    }
    Ok(Header {
        artifact,
        record_count: records.count,
        records: records.range,
        chunks,
    })
}

impl Header {
    /// Return scalar metadata only; the arrays are validated but intentionally absent.
    pub(super) fn stored(mut self, db: &rocksdb::DB, id: &str) -> Result<Self, Error> {
        if self.artifact.id != id {
            return Err(failure("artifact catalog identity disagrees with its key"));
        }
        if !id.starts_with("artifact:") || id.len() <= 9 {
            return Err(failure(
                "artifact metadata requires a bounded artifact identity",
            ));
        }
        super::validate_ingest_labels(id, &self.artifact.source, &self.artifact.format)
            .map_err(|error| failure(&error.message))?;
        if let Some(state) = State::read(db, id)? {
            if state.total != self.chunks.count {
                return Err(failure(
                    "manifest retirement count disagrees with stored artifact",
                ));
            }
            self.artifact.evicted |= state.evicted;
        }
        Ok(self)
    }
}

pub(super) struct Envelope {
    raw: Vec<u8>,
    pub id: String,
    pub expires_at: u64,
    pub received_at: u64,
    root_evicted: bool,
    records: Range<usize>,
    pub prefix: Vec<String>,
    before: State,
    state: State,
}

impl Envelope {
    pub(super) fn read(
        writer: &Writer,
        id: &str,
        bytes: &[u8],
        take: usize,
        remaining: &mut usize,
    ) -> Result<Self, Error> {
        let mut available = *remaining;
        reserve(
            &mut available,
            bytes.len().saturating_mul(2).saturating_add(8192),
        )?;
        let saved = State::read(&writer.db, id)?;
        let header = header(
            bytes,
            &mut available,
            saved.map_or(0, |state| state.retired),
            take,
        )?;
        if header.artifact.id != id {
            return Err(failure("artifact catalog identity disagrees with its key"));
        }
        let state = saved.unwrap_or(State {
            total: header.chunks.count,
            ..State::default()
        });
        if state.total != header.chunks.count {
            return Err(failure(
                "manifest retirement count disagrees with stored artifact",
            ));
        }
        let envelope = Self {
            raw: bytes.to_vec(),
            id: header.artifact.id,
            received_at: header.artifact.received_at,
            expires_at: header.artifact.expires_at,
            root_evicted: header.artifact.evicted,
            records: header.records,
            prefix: header.chunks.prefix,
            before: state,
            state,
        };
        *remaining = available;
        Ok(envelope)
    }
    pub(super) fn evicted(&self) -> bool {
        self.root_evicted || self.state.evicted
    }
    pub(super) fn evict(&mut self) {
        self.state.evicted = true;
    }
    pub(super) fn pending(&self) -> u64 {
        self.state.total - self.state.retired
    }
    pub(super) fn contains(&self, id: &str, remaining: &mut usize) -> Result<bool, Error> {
        let mut reader = json::Reader::from_slice(&self.raw[self.records.clone()], CATALOG_LIMITS)?;
        Ok(strings(&mut reader, remaining, 0, 0, Some(id))?.found)
    }
    pub(super) fn advance(&mut self, count: usize) -> Result<(), Error> {
        if count > self.prefix.len() || count as u64 > self.pending() {
            return Err(failure("manifest retirement exceeds its admitted prefix"));
        }
        self.state.retired += count as u64;
        self.prefix.drain(..count);
        Ok(())
    }
    pub(super) fn stage(&self, batch: &mut rocksdb::WriteBatch) {
        if self.state != self.before {
            let mut bytes = Vec::with_capacity(17);
            bytes.extend_from_slice(&self.state.total.to_be_bytes());
            bytes.extend_from_slice(&self.state.retired.to_be_bytes());
            bytes.push(u8::from(self.state.evicted));
            batch.put(key(&self.id), bytes);
        }
    }
}

/// Detect a native artifact receipt without building an owned tree.
fn artifact_receipt(bytes: &[u8]) -> Result<bool, Error> {
    let mut reader = json::Reader::from_slice(bytes, CATALOG_LIMITS)?;
    if !matches!(reader.next_event()?, json::Event::BeginObject { .. }) {
        return Ok(false);
    }
    loop {
        match reader.next_event()? {
            json::Event::Key(raw) => {
                if raw.decode()? == "format" {
                    return Ok(true);
                }
                reader.skip_value()?;
            }
            json::Event::EndObject { .. } => return Ok(false),
            _ => return Err(failure("request receipt member expected")),
        }
    }
}

/// Expiry consumes only the bounded control envelope, never an agent retry response.
pub(super) fn request_expiry(
    writer: &Writer,
    key: &[u8],
    bytes: &[u8],
    remaining: &mut usize,
) -> Result<(u64, bool), Error> {
    let mut available = *remaining;
    reserve(
        &mut available,
        bytes.len().saturating_mul(2).saturating_add(8192),
    )?;
    let mut reader = json::Reader::from_slice(bytes, CATALOG_LIMITS)?;
    if !matches!(reader.next_event()?, json::Event::BeginObject { .. }) {
        return Err(failure("request catalog must be an object"));
    }
    let mut fields = json::Object::new();
    loop {
        let raw = match reader.next_event()? {
            json::Event::Key(raw) => raw,
            json::Event::EndObject { .. } => break,
            _ => return Err(failure("request catalog member expected")),
        };
        reserve(
            &mut available,
            raw.raw().len().saturating_mul(64).saturating_add(1024),
        )?;
        let name = raw.decode()?.into_owned();
        if fields.contains_key(&name) {
            return Err(failure("duplicate request catalog member"));
        }
        let span = reader.skip_value()?;
        let slice = &bytes[span];
        let value = if name == "receipt" && artifact_receipt(slice)? {
            header(slice, &mut available, 0, 0)?;
            json::Value::Null
        } else {
            Writer::charge_catalog_shape(&mut available, slice)?;
            super::parse(slice)?
        };
        fields.push(name, value);
    }
    if !matches!(reader.next_event()?, json::Event::End) {
        return Err(failure("request catalog has trailing data"));
    }
    let value = json::Value::from(fields);
    let result = (
        super::integer(&value, "expires_at")?,
        writer.managed_consumption_request(key, &value)?,
    );
    *remaining = available;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_manifest_events_preserve_escapes_and_reject_invalid_domain_arrays() {
        let input = r#"{"chunks":["first","sec\u006fnd"],"record_ids":["record:\u0078"],"evicted":false,"payload_id":"blake3:x","byte_length":"1","expires_at":"2","received_at":"0","format":"log","source":"source","id":"artifact:x"}"#;
        let mut remaining = usize::MAX;
        let parsed = header(input.as_bytes(), &mut remaining, 1, 1).unwrap();
        assert_eq!(parsed.artifact.id, "artifact:x");
        assert_eq!(parsed.record_count, 1);
        assert_eq!(parsed.chunks.count, 2);
        assert_eq!(parsed.chunks.prefix, ["second"]);
        let mut reader =
            json::Reader::from_slice(&input.as_bytes()[parsed.records], CATALOG_LIMITS).unwrap();
        assert!(
            strings(&mut reader, &mut remaining, 0, 0, Some("record:x"))
                .unwrap()
                .found
        );
        for invalid in [
            input.replace("\"first\"", "null"),
            input.replace("\"record:\\u0078\"", "{}"),
            input.replace(
                "\"id\":\"artifact:x\"",
                "\"id\":\"artifact:x\",\"id\":\"artifact:y\"",
            ),
            input.replace("\"expires_at\":\"2\"", "\"expires_at\":2"),
            format!("{input} false"),
        ] {
            assert_eq!(
                header(invalid.as_bytes(), &mut remaining, 0, 1)
                    .err()
                    .unwrap()
                    .code,
                "STORAGE_FAILURE"
            );
        }
        assert_eq!(
            header(input.as_bytes(), &mut 1, 0, 1).err().unwrap().code,
            "RESOURCE_LIMIT"
        );
    }
}
