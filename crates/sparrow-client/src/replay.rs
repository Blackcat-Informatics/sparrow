// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Replay of one client quarantine file: the stored batch is parsed back into the exact
//! [`EventBatch`] the producer held and resent under its own request identity, and the file
//! is removed only once the store acknowledges it. A refusal that recurs leaves the file in
//! place with the new refusal recorded in its header, so an operator sees why it is still there.

use crate::{
    ArtifactReceipt, Client, Error, EventBatch, EventField, EventKind, EventLevel, EventSpan,
    FieldValue, MAX_EVENT_BATCH_EVENTS, ProducerEvent,
};
use purrdf_lex::json::{self, Object, Value};
use std::{
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

/// Largest quarantine file replay reads. A producer batch is bounded well below this by the
/// delivery limits; a larger file is not one this client wrote.
const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;

/// The outcome of a replay the store acknowledged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayReceipt {
    /// The quarantine file that was resent and removed.
    pub path: PathBuf,
    /// Events the file held; all are now durable in the store.
    pub events: usize,
    /// The store's certified receipt for the batch.
    pub receipt: ArtifactReceipt,
}

/// Resend the batch held in the quarantine file `path` over `client` and remove the file once
/// the store acknowledges it durably. The batch is sent under a replay identity derived from
/// the file's own (a request identity and producer instance that depend only on the stored
/// request and instance): the store refuses a batch whose sequences lie behind positions it
/// already accepted from the original producer instance, so a live producer's later batches
/// would block every replay of an earlier one. A retry of one replay, including after a lost
/// reply, repeats the same identity and is answered idempotently. A file written for held
/// events at a local failure may already be stored under the original identity; replaying it
/// then stores those events a second time under the replay instance.
///
/// A daemon refusal returns the refusal, keeps the file and records it in the file's header
/// (`replay_attempts`, `last_replay_code`, `last_replay_reason`) so a refusal that recurs is
/// visible where the file lies. A transport failure returns its error and changes nothing:
/// replay again when the daemon is reachable.
///
/// # Errors
/// [`Error::Io`] when the file cannot be read or is not a quarantine file written by this
/// client; any [`Client::events`] error.
pub async fn replay_quarantine(client: &Client, path: &Path) -> Result<ReplayReceipt, Error> {
    let file = read_file(path)?;
    let parsed = parse(&file)?;
    match client.events(&replay_batch(&parsed.batch)).await {
        Ok(receipt) => {
            remove(path)?;
            Ok(ReplayReceipt {
                path: path.to_owned(),
                events: parsed.batch.events.len(),
                receipt,
            })
        }
        Err(error) => {
            if let Error::Domain { code, message, .. } = &error {
                // The refusal is the answer; failing to write it down must not mask it, but it
                // must not vanish either, so a write failure is appended to the error text.
                if let Err(write) = record_refusal(path, &parsed, code, message) {
                    return Err(Error::Io(std::io::Error::other(format!(
                        "{error}; the refusal could not be recorded in {}: {write}",
                        path.display()
                    ))));
                }
            }
            Err(error)
        }
    }
}

/// The batch as replayed: its events unchanged, under a request identity and producer instance
/// derived from the original's.
fn replay_batch(batch: &EventBatch) -> EventBatch {
    let mut material = batch.client_instance.to_vec();
    material.extend_from_slice(batch.request_id.as_bytes());
    let key = purrdf_hash::blake3::derive_key("sparrow.client.quarantine.replay/1", &material);
    let mut client_instance = [0; 16];
    client_instance.copy_from_slice(&key[..16]);
    EventBatch {
        request_id: format!("replay:{}", batch.request_id),
        source: batch.source.clone(),
        client_instance,
        events: batch.events.clone(),
    }
}

fn corrupt(message: impl std::fmt::Display) -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("not a sparrow client quarantine file: {message}"),
    ))
}

fn read_file(path: &Path) -> Result<Vec<u8>, Error> {
    use std::io::Read;
    let file = std::fs::File::open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.len() > MAX_FILE_BYTES {
        return Err(corrupt("not a regular file within the replay size bound"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_FILE_BYTES + 1).read_to_end(&mut bytes)?;
    Ok(bytes)
}

struct Parsed {
    batch: EventBatch,
    header: Object,
    payload: Vec<u8>,
}

/// Parse a quarantine file into its batch. The batch is accepted only when encoding it again
/// reproduces the stored event lines byte for byte, so replay can never send something other
/// than what the producer held.
fn parse(file: &[u8]) -> Result<Parsed, Error> {
    let end = file
        .iter()
        .position(|byte| *byte == b'\n')
        .ok_or_else(|| corrupt("no header line"))?;
    let (header, payload) = (&file[..end], &file[end + 1..]);
    let limits = json::Limits {
        max_depth: 64,
        max_values: 1 << 20,
        max_string_bytes: 1 << 20,
        unique_members: true,
    };
    let header = json::read_slice(header, limits).map_err(corrupt)?;
    let header = header
        .as_object()
        .cloned()
        .ok_or_else(|| corrupt("header is not an object"))?;
    if header.get("profile").and_then(Value::as_str) != Some("sparrow.client.quarantine/1") {
        return Err(corrupt("unknown profile"));
    }
    let text = |name: &str| {
        header
            .get(name)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| corrupt(format!("header lacks {name}")))
    };
    let client_instance: [u8; 16] = purrdf_hash::hex::decode_canonical(&text("client_instance")?)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| corrupt("client_instance is not 16 hex bytes"))?;
    let mut events = Vec::new();
    for line in payload.split_inclusive(|byte| *byte == b'\n') {
        if events.len() == MAX_EVENT_BATCH_EVENTS * 64 {
            return Err(corrupt("too many event lines"));
        }
        let value = json::read_slice(line, limits).map_err(corrupt)?;
        events.push(event(&value)?);
    }
    let batch = EventBatch {
        request_id: text("request_id")?,
        source: text("source")?,
        client_instance,
        events,
    };
    if batch.quarantine_bytes() != payload {
        return Err(corrupt("event lines do not round-trip exactly"));
    }
    if header.get("events").and_then(Value::as_u64) != Some(batch.events.len() as u64)
        || header.get("payload_bytes").and_then(Value::as_u64) != Some(payload.len() as u64)
    {
        return Err(corrupt("header counts disagree with the event lines"));
    }
    let sequences = (
        batch.events.first().map(|event| event.sequence),
        batch.events.last().map(|event| event.sequence),
    );
    if sequences.0.is_none()
        || header.get("first_sequence").and_then(Value::as_u64) != sequences.0
        || header.get("last_sequence").and_then(Value::as_u64) != sequences.1
    {
        return Err(corrupt("header sequences disagree with the event lines"));
    }
    Ok(Parsed {
        batch,
        header,
        payload: payload.to_vec(),
    })
}

fn member<'a>(value: &'a Value, name: &str) -> Result<&'a Value, Error> {
    value
        .get(name)
        .ok_or_else(|| corrupt(format!("event lacks {name}")))
}
fn string(value: &Value, name: &str) -> Result<String, Error> {
    member(value, name)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| corrupt(format!("{name} is not text")))
}
fn unsigned(value: &Value, name: &str) -> Result<u64, Error> {
    member(value, name)?
        .as_u64()
        .ok_or_else(|| corrupt(format!("{name} is not an unsigned integer")))
}
fn optional(value: &Value, name: &str) -> Result<Option<u64>, Error> {
    match member(value, name)? {
        Value::Null => Ok(None),
        other => other
            .as_u64()
            .map(Some)
            .ok_or_else(|| corrupt(format!("{name} is not an unsigned integer or null"))),
    }
}
fn items<'a>(value: &'a Value, name: &str) -> Result<&'a [Value], Error> {
    member(value, name)?
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| corrupt(format!("{name} is not an array")))
}

fn event(value: &Value) -> Result<ProducerEvent, Error> {
    let kind = match string(value, "kind")?.as_str() {
        "log" => EventKind::Log,
        "span_new" => EventKind::SpanNew,
        "span_record" => EventKind::SpanRecord,
        "span_enter" => EventKind::SpanEnter,
        "span_exit" => EventKind::SpanExit,
        "span_close" => EventKind::SpanClose,
        "span_follows_from" => EventKind::SpanFollowsFrom,
        other => return Err(corrupt(format!("unknown event kind {other}"))),
    };
    let level = match string(value, "level")?.as_str() {
        "error" => EventLevel::Error,
        "warn" => EventLevel::Warn,
        "info" => EventLevel::Info,
        "debug" => EventLevel::Debug,
        "trace" => EventLevel::Trace,
        other => return Err(corrupt(format!("unknown event level {other}"))),
    };
    Ok(ProducerEvent {
        sequence: unsigned(value, "sequence")?,
        event_time: unsigned(value, "event_time")?,
        kind,
        level,
        name: string(value, "name")?,
        target: string(value, "target")?,
        fields: items(value, "fields")?
            .iter()
            .map(field)
            .collect::<Result<_, _>>()?,
        context: items(value, "context")?
            .iter()
            .map(span)
            .collect::<Result<_, _>>()?,
        span: optional(value, "span")?,
        follows_from: optional(value, "follows_from")?,
    })
}

fn span(value: &Value) -> Result<EventSpan, Error> {
    Ok(EventSpan {
        id: unsigned(value, "id")?,
        parent: optional(value, "parent")?,
        name: string(value, "name")?,
        target: string(value, "target")?,
        fields: items(value, "fields")?
            .iter()
            .map(field)
            .collect::<Result<_, _>>()?,
    })
}

fn field(value: &Value) -> Result<EventField, Error> {
    let name = string(value, "name")?;
    let kind = string(value, "kind")?;
    let lexeme = |value: &Value| {
        value
            .as_number()
            .map(|number| number.lexeme().to_owned())
            .ok_or_else(|| corrupt("field value is not a number"))
    };
    let number_error = |_| corrupt("field integer out of range");
    let value = match kind.as_str() {
        "f64" => FieldValue::Float64Bits(unsigned(value, "bits")?),
        "text" => FieldValue::Text(string(value, "value")?),
        "debug" => FieldValue::Debug(string(value, "value")?),
        "error" => FieldValue::Error(string(value, "value")?),
        "bool" => FieldValue::Boolean(
            member(value, "value")?
                .as_bool()
                .ok_or_else(|| corrupt("bool field is not a boolean"))?,
        ),
        "i64" => FieldValue::Signed(
            lexeme(member(value, "value")?)?
                .parse()
                .map_err(number_error)?,
        ),
        "u64" => FieldValue::Unsigned(
            lexeme(member(value, "value")?)?
                .parse()
                .map_err(number_error)?,
        ),
        "i128" => FieldValue::Signed128(
            lexeme(member(value, "value")?)?
                .parse()
                .map_err(number_error)?,
        ),
        "u128" => FieldValue::Unsigned128(
            lexeme(member(value, "value")?)?
                .parse()
                .map_err(number_error)?,
        ),
        "bytes_hex" => FieldValue::Bytes(
            purrdf_hash::hex::decode_canonical(&string(value, "value")?).map_err(corrupt)?,
        ),
        other => return Err(corrupt(format!("unknown field kind {other}"))),
    };
    Ok(EventField { name, value })
}

/// Remove the file once its batch is acknowledged, then make the removal durable.
fn remove(path: &Path) -> Result<(), Error> {
    std::fs::remove_file(path)?;
    if let Some(dir) = path.parent() {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

/// Rewrite the file with the recurring refusal in its header. The payload bytes are copied
/// unchanged; the new file replaces the old one by rename, so the batch is on disk at every
/// instant.
fn record_refusal(path: &Path, parsed: &Parsed, code: &str, reason: &str) -> std::io::Result<()> {
    let attempts = parsed
        .header
        .get("replay_attempts")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        .saturating_add(1);
    let mut header = parsed.header.clone();
    header.insert("replay_attempts", attempts);
    header.insert("last_replay_code", code);
    header.insert("last_replay_reason", reason);
    let mut bytes = String::new();
    json::write_into(&mut bytes, &header.into(), json::Format::COMPACT);
    bytes.push('\n');
    let mut bytes = bytes.into_bytes();
    bytes.extend_from_slice(&parsed.payload);
    let name = path
        .file_name()
        .ok_or_else(|| std::io::Error::other("quarantine path has no file name"))?
        .to_string_lossy();
    let scratch = path.with_file_name(format!(".{name}.{}.replay", std::process::id()));
    match std::fs::remove_file(&scratch) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&scratch)?;
    let written = file.write_all(&bytes).and_then(|()| file.sync_all());
    drop(file);
    let renamed = written.and_then(|()| std::fs::rename(&scratch, path));
    if renamed.is_err() {
        let _ = std::fs::remove_file(&scratch);
    }
    renamed?;
    if let Some(dir) = path.parent() {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rich_batch() -> EventBatch {
        let fields = [
            FieldValue::Text("t".into()),
            FieldValue::Boolean(true),
            FieldValue::Signed(-5),
            FieldValue::Unsigned(u64::MAX),
            FieldValue::Signed128(i128::MIN),
            FieldValue::Unsigned128(u128::MAX),
            FieldValue::Float64Bits(f64::NAN.to_bits()),
            FieldValue::Float64Bits((-0.0_f64).to_bits()),
            FieldValue::Bytes(vec![0, 255]),
            FieldValue::Debug("d".into()),
            FieldValue::Error("e".into()),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, value)| EventField {
            name: format!("f{index}"),
            value,
        })
        .collect::<Vec<_>>();
        EventBatch {
            request_id: "events:abc:00000000000000000001".into(),
            source: "app".into(),
            client_instance: [7; 16],
            events: vec![ProducerEvent {
                sequence: 4,
                event_time: 1000,
                kind: EventKind::SpanRecord,
                level: EventLevel::Warn,
                name: "n".into(),
                target: "t".into(),
                fields: fields.clone(),
                context: vec![EventSpan {
                    id: 3,
                    parent: Some(2),
                    name: "s".into(),
                    target: "t".into(),
                    fields,
                }],
                span: Some(3),
                follows_from: None,
            }],
        }
    }

    /// INVARIANT: replay resends exactly what the producer held. A saved file parses back to
    /// an equal batch for every field type (including NaN payloads, signed zero and 128-bit
    /// extremes), and a file whose event lines were altered is refused instead of being sent
    /// as something the producer never held.
    #[test]
    fn a_quarantine_file_parses_back_to_the_exact_batch_and_an_altered_one_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let batch = rich_batch();
        let (path, _) = crate::quarantine::save(dir.path(), &batch, "CONFLICT", "r").unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(parse(&bytes).unwrap().batch, batch);
        let altered = String::from_utf8(bytes)
            .unwrap()
            .replace("\"sequence\":4", "\"sequence\":5");
        assert!(parse(altered.as_bytes()).is_err());
        assert!(parse(b"not a quarantine file").is_err());
    }
}
