// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Bounded manifest retirement over unchanged, native-encoded artifact catalogs.

use super::{Artifact, CATALOG_LIMITS, Catalog, Error, Writer, json};
use std::ops::Range;

/// The pre-change entry walk, kept for the oracle. Test support only.
#[cfg(feature = "test-support")]
mod reference;

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

/// One chunk identity in an artifact row: 64 hex digits, quotes and a separator.
pub(super) const ROW_CHUNK_BYTES: usize = 67;
/// One record identity in an artifact row: `record:` and 32 hex digits, quoted,
/// with a separator.
pub(super) const ROW_RECORD_BYTES: usize = 42;
/// An encoded artifact row outside its two arrays, with the longest source label
/// escaped six bytes per byte, the identities and three 20-digit integers.
const ROW_FIXED_BYTES: usize = 4096;
/// `header` charges each member name and scalar 64 times its encoded length plus
/// 1024; ten names and eight scalars together encode within `ROW_FIXED_BYTES`.
const HEADER_FIXED_CHARGE: usize = 64 * ROW_FIXED_BYTES + 18 * 1024;

/// Upper bound of what `Envelope::read` reserves from a pass's catalog budget for
/// an artifact row of `chunks` chunk and `records` record identities while taking
/// `take` chunk identities: twice the row, 8 KiB, the header's scalar charge and
/// the retained prefix.
pub(super) const fn envelope_charge(chunks: usize, records: usize, take: usize) -> usize {
    let row = ROW_FIXED_BYTES + chunks * ROW_CHUNK_BYTES + records * ROW_RECORD_BYTES;
    2 * row + 8192 + HEADER_FIXED_CHARGE + take * (2 * 66 + 2 * std::mem::size_of::<String>())
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
        if !matches!(
            super::writer_format::read(db)?.as_deref(),
            Some(FORMAT | WATCH_FORMAT)
        ) {
            return Err(failure(
                "invalid manifest retirement state or missing writer fence",
            ));
        }
        Self::decode(&bytes).map(Some)
    }

    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() != 17 || bytes[16] > 1 {
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
        Ok(state)
    }
}

/// Whether an encoded manifest retirement state retires its complete artifact.
pub(super) fn evicted_flag(bytes: &[u8]) -> Result<bool, Error> {
    State::decode(bytes).map(|state| state.evicted)
}

/// The original chunk count, the retired position and the evicted flag an encoded manifest
/// retirement state holds.
pub(super) fn position(bytes: &[u8]) -> Result<(u64, u64, bool), Error> {
    State::decode(bytes).map(|state| (state.total, state.retired, state.evicted))
}

/// Whether an encoded manifest retirement state has released any of its artifact's bytes: the
/// complete artifact is evicted, or expiry or eviction has already retired chunks (their
/// references are decremented and unreferenced payload deleted) while the entry stays.
pub(super) fn releases_bytes(bytes: &[u8]) -> Result<bool, Error> {
    State::decode(bytes).map(|state| state.evicted || state.retired > 0)
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
    found: bool,
}

/// The sink that keeps the first `take` chunk identities a caller asks for, charging each one
/// it keeps at the rate the native reader has always charged a retained identity. The sink is
/// given the decoded identity, its raw length and the admission budget the reader charges from.
fn retain(
    prefix: &mut Vec<String>,
    take: usize,
) -> impl FnMut(&str, usize, &mut usize) -> Result<(), Error> + '_ {
    move |chunk, raw_len, remaining| {
        if prefix.len() < take {
            reserve(
                remaining,
                raw_len
                    .saturating_mul(2)
                    .saturating_add(2 * std::mem::size_of::<String>()),
            )?;
            prefix.push(chunk.to_owned());
        }
        Ok(())
    }
}

/// The native reader owns grammar and escape decoding; only domain string arrays are accepted.
/// Each element at or after `skip` is passed to `each` in array order and is not retained.
fn strings<F>(
    reader: &mut json::Reader<'_>,
    remaining: &mut usize,
    skip: u64,
    target: Option<&str>,
    mut each: F,
) -> Result<Array, Error>
where
    F: FnMut(&str, usize, &mut usize) -> Result<(), Error>,
{
    let json::Event::BeginArray { at } = reader.next_event()? else {
        return Err(failure("artifact manifest member must be an array"));
    };
    let mut result = Array {
        range: at..at,
        count: 0,
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
                if result.count >= skip {
                    each(&decoded, raw.raw().len(), remaining)?;
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
    chunks: u64,
    /// The chunk identities `header` retained; empty when the header was read with `header_with`.
    prefix: Vec<String>,
}

/// Validate an artifact row's scalar fields and its arrays in one pass. Each chunk identity at
/// or after the first `skip` is passed to `chunk` in manifest order and is not retained.
fn header_with<F>(
    bytes: &[u8],
    remaining: &mut usize,
    skip: u64,
    mut chunk: F,
) -> Result<Header, Error>
where
    F: FnMut(&str, usize, &mut usize) -> Result<(), Error>,
{
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
                records = Some(strings(&mut reader, remaining, 0, None, |_, _, _| Ok(()))?);
                json::Value::array(Vec::<String>::new())
            }
            "chunks" => {
                chunks = Some(strings(&mut reader, remaining, skip, None, &mut chunk)?);
                json::Value::array(Vec::<String>::new())
            }
            // The one bounded object member; the artifact codec checks its shape.
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
        chunks: chunks.count,
        prefix: Vec::new(),
    })
}

/// Validate an artifact row and retain up to `take` chunk identities after the first `skip`.
pub(super) fn header(
    bytes: &[u8],
    remaining: &mut usize,
    skip: u64,
    take: usize,
) -> Result<Header, Error> {
    let mut prefix = Vec::new();
    let mut header = header_with(bytes, remaining, skip, retain(&mut prefix, take))?;
    header.prefix = prefix;
    Ok(header)
}

impl Header {
    /// The number of chunk identities the artifact row holds.
    pub(super) fn chunk_count(&self) -> u64 {
        self.chunks
    }

    /// The chunk identities retained after the requested skip, up to the requested take.
    pub(super) fn into_chunks(self) -> Vec<String> {
        self.prefix
    }

    /// Up to `take` of the artifact's record identities after the first `skip`, read from
    /// the same encoded row the header was read from.
    pub(super) fn record_ids(
        &self,
        bytes: &[u8],
        remaining: &mut usize,
        skip: u64,
        take: usize,
    ) -> Result<Vec<String>, Error> {
        let row = bytes
            .get(self.records.clone())
            .ok_or_else(|| failure("record manifest lies outside its artifact row"))?;
        let mut reader = json::Reader::from_slice(row, CATALOG_LIMITS)?;
        let mut ids = Vec::new();
        strings(&mut reader, remaining, skip, None, retain(&mut ids, take))?;
        Ok(ids)
    }

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
            if state.total != self.chunks {
                return Err(failure(
                    "manifest retirement count disagrees with stored artifact",
                ));
            }
            self.artifact.evicted |= state.evicted;
        }
        Ok(self)
    }
}

/// One artifact row's identity, deadlines and retirement state. It holds no chunk identities:
/// `read` validates the row in one pass, and `fold` passes its chunk identities to a caller in a
/// second pass over the same bytes.
pub(super) struct Row {
    pub id: String,
    pub expires_at: u64,
    pub received_at: u64,
    root_evicted: bool,
    records: Range<usize>,
    state: State,
}

impl Row {
    /// Read the row `bytes` of artifact `id`, charging `remaining` once for the row the caller
    /// holds and for the header's scalars. Each chunk identity after the retired position is
    /// passed to `chunk`. Fails when the identity disagrees with its key, when the retirement
    /// state's count disagrees with the row, or when the state or row is malformed.
    pub(super) fn read<F>(
        db: &rocksdb::DB,
        id: &str,
        bytes: &[u8],
        remaining: &mut usize,
        chunk: F,
    ) -> Result<Self, Error>
    where
        F: FnMut(&str, usize, &mut usize) -> Result<(), Error>,
    {
        reserve(remaining, bytes.len().saturating_add(8192))?;
        let saved = State::read(db, id)?;
        let header = header_with(
            bytes,
            remaining,
            saved.map_or(0, |state| state.retired),
            chunk,
        )?;
        if header.artifact.id != id {
            return Err(failure("artifact catalog identity disagrees with its key"));
        }
        let state = saved.unwrap_or(State {
            total: header.chunks,
            ..State::default()
        });
        if state.total != header.chunks {
            return Err(failure(
                "manifest retirement count disagrees with stored artifact",
            ));
        }
        Ok(Self {
            id: header.artifact.id,
            expires_at: header.artifact.expires_at,
            received_at: header.artifact.received_at,
            root_evicted: header.artifact.evicted,
            records: header.records,
            state,
        })
    }

    /// Pass every chunk identity after the retired position to `chunk`, in manifest order, by
    /// reading `bytes` again: the same row `read` accepted. Fails unless exactly `pending()`
    /// identities were passed. The row's scalars may follow its chunk array, so a caller that
    /// needs them before folding the chunks reads the row once with `read` and folds it here.
    pub(super) fn fold<F>(&self, bytes: &[u8], mut chunk: F) -> Result<(), Error>
    where
        F: FnMut(&str, usize) -> Result<(), Error>,
    {
        let mut passed = 0_u64;
        let mut scalars = usize::MAX;
        header_with(
            bytes,
            &mut scalars,
            self.state.retired,
            |digest, raw_len, _| {
                passed += 1;
                chunk(digest, raw_len)
            },
        )?;
        if passed != self.pending() {
            return Err(failure("manifest retirement passes chunks the row lacks"));
        }
        Ok(())
    }

    /// Whether the artifact is evicted by its row or by its retirement state.
    pub(super) fn evicted(&self) -> bool {
        self.root_evicted || self.state.evicted
    }

    /// The chunk identities the retirement state has not yet retired.
    pub(super) fn pending(&self) -> u64 {
        self.state.total - self.state.retired
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
    /// Read an artifact row and keep up to `take` of its unretired chunk identities, with an
    /// owned copy of the row that `contains` reads.
    pub(super) fn read(
        writer: &Writer,
        id: &str,
        bytes: &[u8],
        take: usize,
        remaining: &mut usize,
    ) -> Result<Self, Error> {
        let mut available = *remaining;
        // The owned copy of the row is charged on top of the row the reader charges.
        reserve(&mut available, bytes.len())?;
        let mut prefix = Vec::new();
        let row = Row::read(
            &writer.db,
            id,
            bytes,
            &mut available,
            retain(&mut prefix, take),
        )?;
        let state = row.state;
        let envelope = Self {
            raw: bytes.to_vec(),
            id: row.id,
            received_at: row.received_at,
            expires_at: row.expires_at,
            root_evicted: row.root_evicted,
            records: row.records,
            prefix,
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
        Ok(strings(&mut reader, remaining, 0, Some(id), |_, _, _| Ok(()))?.found)
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

/// The native header of a saved request's `receipt` member, and that member's bytes, read
/// without building any tree or owning the record and chunk arrays (#285).
pub(super) fn saved_receipt<'a>(
    bytes: &'a [u8],
    remaining: &mut usize,
) -> Result<(Header, &'a [u8]), Error> {
    let mut reader = json::Reader::from_slice(bytes, CATALOG_LIMITS)?;
    if !matches!(reader.next_event()?, json::Event::BeginObject { .. }) {
        return Err(failure("request catalog must be an object"));
    }
    loop {
        match reader.next_event()? {
            json::Event::Key(raw) => {
                let receipt = raw.decode()? == "receipt";
                let slice = &bytes[reader.skip_value()?];
                if receipt {
                    return Ok((header(slice, remaining, 0, 0)?, slice));
                }
            }
            json::Event::EndObject { .. } => return Err(failure("request has no receipt")),
            _ => return Err(failure("request catalog member expected")),
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
        let records = parsed.records.clone();
        assert_eq!(parsed.chunk_count(), 2);
        assert_eq!(parsed.into_chunks(), ["second"]);
        let mut reader =
            json::Reader::from_slice(&input.as_bytes()[records], CATALOG_LIMITS).unwrap();
        assert!(
            strings(
                &mut reader,
                &mut remaining,
                0,
                Some("record:x"),
                |_, _, _| Ok(())
            )
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
