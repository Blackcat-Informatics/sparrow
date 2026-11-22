// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Reconcile an immutable quarantine batch with the store. An uncertain original
//! needs its exact retained receipt. Never-submitted parts use crash-checkpointed,
//! deadline-bound identities. Removal follows certification of the whole file.

use crate::{
    ArtifactReceipt, Client, Error, EventBatch, EventField, EventKind, EventLevel, EventSpan,
    FieldValue, MAX_EVENT_BATCH_EVENTS, ProducerEvent,
};
use purrdf_lex::json::{self, Object, Value};
use std::path::{Path, PathBuf};
mod journal;

/// Largest original quarantine payload replay reads. A producer batch is bounded well below this by the
/// delivery limits (a full queue at the largest configuration is under this); a larger file
/// is not one this client wrote.
const MAX_FILE_BYTES: u64 = 512 * 1024 * 1024;
// Reserve space for one attempt and one receipt per event at the smallest partition.
const MAX_JOURNALED_BYTES: u64 = MAX_FILE_BYTES + 96 * 1024 * 1024;
const INPUT_LIMITS: json::Limits = json::Limits {
    max_depth: 64,
    max_values: 1 << 20,
    max_string_bytes: 1 << 20,
    unique_members: true,
};

/// The outcome of a replay the store acknowledged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayReceipt {
    /// The quarantine file that was resent and removed.
    pub path: PathBuf,
    /// Events the file held; all are now durable in the store.
    pub events: usize,
    /// The store's certified receipt for the last part.
    pub receipt: ArtifactReceipt,
}

/// Reconcile one quarantine file against this store. Historical and uncertain original
/// requests can only recover an exact retained receipt. Fresh derived parts persist their
/// identity and deadline before conditional publication, and their certified receipt before
/// the next part. Removal is authorized only after every part has a durable receipt.
pub async fn replay_quarantine(client: &Client, path: &Path) -> Result<ReplayReceipt, Error> {
    let absolute = std::path::absolute(path)?;
    let (journal, mut parsed) =
        tokio::task::spawn_blocking(move || journal::Journal::open(absolute))
            .await
            .map_err(std::io::Error::other)??;
    let receipt = if parsed.header.get("profile").and_then(Value::as_str)
        == Some("sparrow.client.quarantine/1")
    {
        historical(client, &parsed.batch).await?
    } else {
        match journal.reconcile(client, &mut parsed).await {
            Ok(receipt) => receipt,
            Err(error) => {
                if let Error::Domain { code, message, .. } = &error {
                    let attempts = parsed
                        .header
                        .get("replay_attempts")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    parsed
                        .header
                        .insert("replay_attempts", attempts.saturating_add(1));
                    parsed.header.insert("last_replay_code", code.clone());
                    parsed.header.insert("last_replay_reason", message.clone());
                    journal.save(&parsed).await.map_err(|write| {
                        std::io::Error::other(format!(
                            "{error}; recording the replay refusal failed: {write}"
                        ))
                    })?;
                }
                return Err(error);
            }
        }
    };
    journal.remove().await?;
    Ok(ReplayReceipt {
        path: path.to_owned(),
        events: parsed.batch.events.len(),
        receipt,
    })
}

async fn historical(client: &Client, batch: &EventBatch) -> Result<ArtifactReceipt, Error> {
    // v1 could publish an original RPC or any prefix of historical derived parts.
    // Zero never authorizes first publication, even after receipt retirement or a clock rollback.
    if batch.validate().is_ok() {
        match client
            .submit_events(batch, crate::Durability::Durable, Some(0))
            .await
        {
            Ok(receipt) => return Ok(receipt),
            Err(Error::Domain { code, .. })
                if matches!(code.as_str(), "UNCERTAIN_DELIVERY" | "CONFLICT") => {}
            Err(Error::Unsupported(crate::delivery::IMMUTABLE_BATCH_LIMIT)) => (),
            Err(error) => return Err(error),
        }
    }
    let mut last = None;
    for part in replay_parts(batch, MAX_EVENT_BATCH_EVENTS, crate::MAX_EVENT_BATCH_BYTES)? {
        last = Some(
            client
                .submit_events(&part, crate::Durability::Durable, Some(0))
                .await?,
        );
    }
    last.ok_or_else(|| corrupt("no replayable events"))
}

/// The batches as replayed, in order: the events unchanged, under a request identity and
/// producer instance derived from the original's. A file may hold more than one batch's worth
/// (the queued events saved at a local failure are one file), so it is cut with the limits the
/// worker applies to a batch; the identity of each part is its index, so a retry of one part
/// repeats it.
fn replay_parts(
    batch: &EventBatch,
    event_limit: usize,
    byte_limit: usize,
) -> Result<Vec<EventBatch>, Error> {
    let mut material = batch.client_instance.to_vec();
    material.extend_from_slice(batch.request_id.as_bytes());
    let key = purrdf_hash::blake3::derive_key("sparrow.client.quarantine.replay/1", &material);
    let mut client_instance = [0; 16];
    client_instance.copy_from_slice(&key[..16]);
    let part = |index: usize, events: Vec<ProducerEvent>| EventBatch {
        request_id: format!("replay:{}:{index}", batch.request_id),
        source: batch.source.clone(),
        client_instance,
        events,
    };
    let overhead = 1024 + 6 * (batch.request_id.len() + 16 + batch.source.len());
    let mut parts = Vec::new();
    let (mut events, mut bytes) = (Vec::new(), 0_usize);
    for event in &batch.events {
        let charge = event.admission_bytes()?;
        if !events.is_empty()
            && (events.len() == event_limit || bytes + charge + overhead > byte_limit)
        {
            parts.push(part(parts.len(), std::mem::take(&mut events)));
            bytes = 0;
        }
        bytes += charge;
        events.push(event.clone());
    }
    if !events.is_empty() {
        parts.push(part(parts.len(), events));
    }
    Ok(parts)
}

fn checkpoint_digest(change: &Value) -> String {
    purrdf_hash::hex::encode(
        purrdf_hash::blake3::hash(json::write_compact(change).as_bytes()).as_bytes(),
    )
}

fn corrupt(message: impl std::fmt::Display) -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("not a sparrow client quarantine file: {message}"),
    ))
}

fn read_file(path: &Path) -> Result<(Vec<u8>, std::fs::Metadata), Error> {
    use std::io::Read;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.nlink() > 2 || meta.len() > MAX_JOURNALED_BYTES {
        return Err(corrupt(
            "expected a regular quarantine file within the size and link bounds",
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_JOURNALED_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_JOURNALED_BYTES {
        return Err(corrupt("file exceeded the replay size bound while reading"));
    }
    Ok((bytes, meta))
}

struct Parsed {
    batch: EventBatch,
    header: Object,
    payload: std::sync::Arc<[u8]>,
    certificates: Vec<Value>,
    valid_bytes: usize,
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
    let header = json::read_slice(header, INPUT_LIMITS).map_err(corrupt)?;
    let mut header = header
        .as_object()
        .cloned()
        .ok_or_else(|| corrupt("header is not an object"))?;
    if !matches!(
        header.get("profile").and_then(Value::as_str),
        Some("sparrow.client.quarantine/1" | "sparrow.client.quarantine/2")
    ) {
        return Err(corrupt("unknown profile"));
    }
    let size = header
        .get("payload_bytes")
        .and_then(Value::as_u64)
        .filter(|size| *size <= MAX_FILE_BYTES)
        .and_then(|size| usize::try_from(size).ok())
        .ok_or_else(|| corrupt("invalid payload length"))?;
    let (payload, tail) = payload
        .split_at_checked(size)
        .ok_or_else(|| corrupt("incomplete event payload"))?;
    if header.get("profile").and_then(Value::as_str) == Some("sparrow.client.quarantine/1")
        && !tail.is_empty()
    {
        return Err(corrupt("unexpected data after historical payload"));
    }
    let mut certificates = match header.remove("certificates").as_mut() {
        None => Vec::new(),
        Some(Value::Array(list)) => std::mem::take(list),
        _ => return Err(corrupt("invalid certified prefix")),
    };
    let mut valid_bytes = end + 1 + size;
    for line in tail.split_inclusive(|b| *b == b'\n') {
        if !line.ends_with(b"\n") {
            // Publication starts only after a complete synchronized attempt. A torn
            // certificate leaves its preceding guarded attempt available for recovery.
            break;
        }
        let frame = json::read_slice(line, INPUT_LIMITS).map_err(corrupt)?;
        let entry = member(&frame, "change")?;
        if frame.get("digest").and_then(Value::as_str) != Some(&checkpoint_digest(entry)) {
            return Err(corrupt("replay checkpoint checksum mismatch"));
        }
        if entry.get("part").and_then(Value::as_u64) != Some(certificates.len() as u64) {
            return Err(corrupt("checkpoint is out of order"));
        }
        match (
            entry.get("attempt"),
            entry.get("receipt"),
            header.get("replay_attempt"),
        ) {
            (Some(attempt), None, None) => {
                header.insert("replay_attempt", attempt.clone());
            }
            (None, Some(receipt), Some(_)) => {
                certificates.push(receipt.clone());
                header.remove("replay_attempt");
            }
            _ => return Err(corrupt("checkpoint does not follow its attempt")),
        }
        valid_bytes += line.len();
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
        if events.len() == 16384 {
            return Err(corrupt("too many event lines"));
        }
        let value = json::read_slice(line, INPUT_LIMITS).map_err(corrupt)?;
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
        payload: payload.into(),
        certificates,
        valid_bytes,
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
        let (path, _) = crate::quarantine::save(
            dir.path(),
            &batch,
            "CONFLICT",
            "r",
            crate::quarantine::Original::Unsent,
            None,
        )
        .unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(parse(&bytes).unwrap().batch, batch);
        let mut corrupt_frame = bytes.clone();
        corrupt_frame.extend_from_slice(
            b"{\"digest\":\"invalid\",\"change\":{\"part\":0,\"attempt\":{}}}\n{",
        );
        assert!(
            parse(&corrupt_frame).is_err(),
            "a torn tail cannot hide an invalid interior checkpoint"
        );
        let altered = String::from_utf8(bytes)
            .unwrap()
            .replace("\"sequence\":4", "\"sequence\":5");
        assert!(parse(altered.as_bytes()).is_err());
        assert!(parse(b"not a quarantine file").is_err());
    }

    /// INVARIANT: a file larger than one batch (the queued events saved when a local failure
    /// ended the worker are one file of up to the whole queue) replays as several batches that
    /// each pass the daemon's batch limits, in order, none repeated or lost; each part keeps a
    /// stable identity. Before, the whole file went out as one batch the daemon would refuse.
    #[test]
    fn a_file_over_one_batch_is_replayed_in_parts_that_each_fit_the_batch_limits() {
        let base = rich_batch();
        let template = base.events[0].clone();
        let big = EventBatch {
            events: (1..=700)
                .map(|sequence| ProducerEvent {
                    sequence,
                    ..template.clone()
                })
                .collect(),
            ..base
        };
        let parts =
            replay_parts(&big, MAX_EVENT_BATCH_EVENTS, crate::MAX_EVENT_BATCH_BYTES).unwrap();
        assert!(parts.len() > 2);
        let mut next = 1;
        for (index, part) in parts.iter().enumerate() {
            part.validate().unwrap();
            assert!(part.events.len() <= MAX_EVENT_BATCH_EVENTS);
            assert!(part.request_id.ends_with(&format!(":{index}")));
            for event in &part.events {
                assert_eq!(event.sequence, next);
                next += 1;
            }
        }
        assert_eq!(next, 701);
        assert_eq!(
            replay_parts(&big, MAX_EVENT_BATCH_EVENTS, crate::MAX_EVENT_BATCH_BYTES).unwrap(),
            parts
        );
    }
}
