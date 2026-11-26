// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Parses systemd's journal export stream and maps each entry to a producer event.
//!
//! The parser accepts input in arbitrary chunks. An entry is complete at the blank line that
//! ends it, so the result does not depend on where the reads split the stream.

use sparrow_client::{
    EventField, EventKind, EventLevel, FieldValue, MAX_EVENT_FIELDS, ProducerEvent,
};
use std::fmt;

/// Largest byte count of one journal entry's fields, measured before mapping.
pub const MAX_ENTRY_BYTES: usize = 1 << 20;
/// Metadata name of every event the forwarder produces.
pub const EVENT_NAME: &str = "journal_entry";
/// Prefix of the forwarder's own unit instances, whose entries are never forwarded.
const OWN_UNIT_PREFIX: &str = "sparrow-journal@";

/// One field of a journal entry. `binary` records the export encoding, not the content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalField {
    name: String,
    value: Vec<u8>,
    binary: bool,
}

impl JournalField {
    /// Journal field name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Exact field content.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.value
    }

    /// Content as text when it was exported as text and is valid UTF-8.
    fn text(&self) -> Option<&str> {
        if self.binary {
            None
        } else {
            std::str::from_utf8(&self.value).ok()
        }
    }

    /// Typed value: text when the field is text and valid UTF-8, exact bytes otherwise.
    fn value(&self) -> FieldValue {
        match self.text() {
            Some(text) => FieldValue::Text(text.to_owned()),
            None => FieldValue::Bytes(self.value.clone()),
        }
    }
}

/// One complete journal entry in stream order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalEntry {
    fields: Vec<JournalField>,
    cursor: String,
}

impl JournalEntry {
    /// The entry's `__CURSOR`, which names it in the journal.
    #[must_use]
    pub fn cursor(&self) -> &str {
        &self.cursor
    }

    /// Fields in export order.
    #[must_use]
    pub fn fields(&self) -> &[JournalField] {
        &self.fields
    }

    /// The first field with this name.
    fn get(&self, name: &str) -> Option<&JournalField> {
        self.fields.iter().find(|field| field.name == name)
    }
}

/// A refused export stream. Both variants name the last complete entry before the fault, so
/// the operator can restart from that cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// A field name, binary length or terminator does not match the export format.
    Malformed {
        /// Cursor of the last complete entry, when one has been read.
        cursor_before: Option<String>,
        /// Which part of the format was violated.
        reason: &'static str,
    },
    /// The entry being read exceeds [`MAX_ENTRY_BYTES`].
    Oversized {
        /// Cursor of the last complete entry, when one has been read.
        cursor_before: Option<String>,
    },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let before = |cursor: &Option<String>| {
            cursor.as_deref().map_or_else(
                || "at the start of the stream".to_owned(),
                |c| format!("after cursor {c}"),
            )
        };
        match self {
            Self::Malformed {
                cursor_before,
                reason,
            } => write!(
                f,
                "journal export is malformed {}: {reason}",
                before(cursor_before)
            ),
            Self::Oversized { cursor_before } => write!(
                f,
                "journal entry {} exceeds {MAX_ENTRY_BYTES} bytes",
                before(cursor_before)
            ),
        }
    }
}

impl std::error::Error for ParseError {}

/// Incremental parser for journal export text. Feed it chunks in order.
#[derive(Debug, Default)]
pub struct ExportParser {
    pending: Vec<u8>,
    fields: Vec<JournalField>,
    entry_bytes: usize,
    last_cursor: Option<String>,
}

impl ExportParser {
    /// An empty parser at the start of an export stream.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append `input` and return every entry it completes, in stream order.
    ///
    /// # Errors
    ///
    /// Returns [`ParseError`] at the first malformed or oversized entry. The entries completed
    /// earlier in the same call are not returned; the caller restarts from the last cursor it
    /// delivered, so they are read again rather than lost.
    pub fn push(&mut self, input: &[u8]) -> Result<Vec<JournalEntry>, ParseError> {
        self.pending.extend_from_slice(input);
        let pending = std::mem::take(&mut self.pending);
        let mut entries = Vec::new();
        let mut pos = 0;
        let outcome = self.scan(&pending, &mut pos, &mut entries);
        self.pending = pending[pos..].to_vec();
        outcome.map(|()| entries)
    }

    /// Consume complete fields from `pending` starting at `pos`.
    fn scan(
        &mut self,
        pending: &[u8],
        pos: &mut usize,
        out: &mut Vec<JournalEntry>,
    ) -> Result<(), ParseError> {
        loop {
            let rest = &pending[*pos..];
            let Some(&first) = rest.first() else {
                return Ok(());
            };
            if first == b'\n' {
                *pos += 1;
                if !self.fields.is_empty() {
                    out.push(self.finish()?);
                }
                continue;
            }
            let Some(line_end) = rest.iter().position(|&byte| byte == b'\n') else {
                if rest.len() > MAX_ENTRY_BYTES {
                    return Err(self.oversized());
                }
                return Ok(());
            };
            let line = &rest[..line_end];
            let consumed = if let Some(eq) = line.iter().position(|&b| b == b'=') {
                self.text_field(&line[..eq], &line[eq + 1..])?;
                line_end + 1
            } else {
                match self.binary_field(rest, line_end)? {
                    Some(consumed) => consumed,
                    None => return Ok(()),
                }
            };
            *pos += consumed;
        }
    }

    /// Admit a `KEY=value` line.
    fn text_field(&mut self, name: &[u8], value: &[u8]) -> Result<(), ParseError> {
        let name = self.field_name(name)?;
        self.admit(name.len() + value.len())?;
        self.fields.push(JournalField {
            name,
            value: value.to_vec(),
            binary: false,
        });
        Ok(())
    }

    /// Admit a `KEY` line followed by a 64-bit length and that many bytes. Returns the bytes
    /// consumed, or `None` when the rest of the field has not arrived yet.
    fn binary_field(&mut self, rest: &[u8], line_end: usize) -> Result<Option<usize>, ParseError> {
        let name = self.field_name(&rest[..line_end])?;
        let header = line_end + 1;
        let Some(length_bytes) = rest.get(header..header + 8) else {
            return Ok(None);
        };
        let mut length = [0_u8; 8];
        length.copy_from_slice(length_bytes);
        let length = usize::try_from(u64::from_le_bytes(length)).unwrap_or(usize::MAX);
        if !self.fits(name.len().saturating_add(length)) {
            return Err(self.oversized());
        }
        let data = header + 8;
        let Some(&terminator) = rest.get(data.saturating_add(length)) else {
            return Ok(None);
        };
        if terminator != b'\n' {
            return Err(self.malformed("binary field is not followed by a newline"));
        }
        self.admit(name.len() + length)?;
        self.fields.push(JournalField {
            name,
            value: rest[data..data + length].to_vec(),
            binary: true,
        });
        Ok(Some(data + length + 1))
    }

    /// A journal field name: uppercase ASCII letters, digits and underscores, not starting
    /// with a digit.
    fn field_name(&self, raw: &[u8]) -> Result<String, ParseError> {
        let valid = !raw.is_empty()
            && !raw[0].is_ascii_digit()
            && raw
                .iter()
                .all(|&b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_');
        if !valid {
            return Err(self.malformed("field name is not a journal key"));
        }
        Ok(String::from_utf8_lossy(raw).into_owned())
    }

    fn fits(&self, bytes: usize) -> bool {
        self.entry_bytes.saturating_add(bytes) <= MAX_ENTRY_BYTES
    }

    fn admit(&mut self, bytes: usize) -> Result<(), ParseError> {
        if !self.fits(bytes) {
            return Err(self.oversized());
        }
        self.entry_bytes += bytes;
        Ok(())
    }

    /// Close the entry being read. It must carry a `__CURSOR` field.
    fn finish(&mut self) -> Result<JournalEntry, ParseError> {
        let fields = std::mem::take(&mut self.fields);
        self.entry_bytes = 0;
        let cursor = fields
            .iter()
            .find(|field| field.name == "__CURSOR")
            .and_then(|field| field.text().map(str::to_owned));
        let Some(cursor) = cursor else {
            return Err(self.malformed("entry has no text __CURSOR field"));
        };
        self.last_cursor = Some(cursor.clone());
        Ok(JournalEntry { fields, cursor })
    }

    fn malformed(&self, reason: &'static str) -> ParseError {
        ParseError::Malformed {
            cursor_before: self.last_cursor.clone(),
            reason,
        }
    }

    fn oversized(&self) -> ParseError {
        ParseError::Oversized {
            cursor_before: self.last_cursor.clone(),
        }
    }
}

/// Why an entry cannot become a producer event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MapError {
    /// `__REALTIME_TIMESTAMP` is absent or is not a microsecond count.
    Timestamp,
    /// `PRIORITY` is not a syslog level from 0 to 7.
    Priority,
    /// The fixed fields alone exceed the producer event bound.
    Oversized,
}

impl fmt::Display for MapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Timestamp => "__REALTIME_TIMESTAMP is missing or not a microsecond count",
            Self::Priority => "PRIORITY is not a syslog level from 0 to 7",
            Self::Oversized => "the fixed fields of an entry exceed the producer event bound",
        })
    }
}

impl std::error::Error for MapError {}

/// A journal entry that becomes a producer event.
#[derive(Debug, Clone, PartialEq)]
pub struct Forwarded {
    /// The producer event to send. Its sequence is assigned by the delivery handle.
    pub event: ProducerEvent,
    /// Whether the message was cut to fit the event bound.
    pub truncated: bool,
    /// Fields that did not fit the field count or the event bound and were left out.
    pub omitted_fields: u64,
}

/// The mapping decision for one entry.
#[derive(Debug, Clone, PartialEq)]
pub enum Mapping {
    /// The entry becomes this event.
    Event(Box<Forwarded>),
    /// The entry belongs to one of the forwarder's own units and is never forwarded.
    OwnUnit,
}

/// Maps a complete entry to a producer event for the journal `source`.
///
/// Fields 0 to 4 are fixed so diagnostic mappings can name them: `service`, `host`,
/// `severity`, `message` and `journal_cursor`. The other fields follow in export order,
/// without `MESSAGE` and `__CURSOR`, which the fixed fields carry.
///
/// # Errors
///
/// Returns [`MapError`] when the timestamp or priority is unusable, or the fixed fields do not
/// fit the event bound.
pub fn map_entry(entry: &JournalEntry, source: &str) -> Result<Mapping, MapError> {
    if is_own_unit(entry) {
        return Ok(Mapping::OwnUnit);
    }
    let event_time = entry
        .get("__REALTIME_TIMESTAMP")
        .and_then(JournalField::text)
        .and_then(|text| text.parse::<u64>().ok())
        .ok_or(MapError::Timestamp)?
        / 1000;
    let (level, severity) = priority(entry)?;
    let service = service_name(entry);
    let host = entry.get("_HOSTNAME").map_or_else(String::new, lossy);
    let message = entry.get("MESSAGE").map(JournalField::value);
    let message_bytes = entry.get("MESSAGE").map_or(&[][..], JournalField::bytes);
    let fixed = [
        text_field("service", service),
        text_field("host", host),
        text_field("severity", severity.to_owned()),
    ];
    let cursor = text_field("journal_cursor", entry.cursor().to_owned());
    let others: Vec<EventField> = entry
        .fields()
        .iter()
        .filter(|field| field.name() != "MESSAGE" && field.name() != "__CURSOR")
        .map(|field| EventField {
            name: field.name().to_owned(),
            value: field.value(),
        })
        .collect();
    let parts = Parts {
        event_time,
        level,
        source,
        head: fixed,
        cursor,
    };
    let message = message.unwrap_or(FieldValue::Text(String::new()));
    parts.build(message, message_bytes, others)
}

/// Fields shared by every candidate event built from one entry.
struct Parts<'a> {
    event_time: u64,
    level: EventLevel,
    source: &'a str,
    head: [EventField; 3],
    cursor: EventField,
}

impl Parts<'_> {
    fn event(
        &self,
        message: FieldValue,
        extra: &[EventField],
        others: &[EventField],
    ) -> ProducerEvent {
        let mut fields = Vec::with_capacity(5 + extra.len() + others.len());
        fields.extend(self.head.iter().cloned());
        fields.push(EventField {
            name: "message".to_owned(),
            value: message,
        });
        fields.push(self.cursor.clone());
        fields.extend(extra.iter().cloned());
        fields.extend(others.iter().cloned());
        ProducerEvent {
            sequence: 0,
            event_time: self.event_time,
            kind: EventKind::Log,
            level: self.level,
            name: EVENT_NAME.to_owned(),
            target: self.source.to_owned(),
            fields,
            context: Vec::new(),
            span: None,
            follows_from: None,
        }
    }

    /// Keep the whole message when the event fits; otherwise cut the message to the longest
    /// prefix that fits, then keep the other fields that still fit.
    fn build(
        &self,
        message: FieldValue,
        message_bytes: &[u8],
        others: Vec<EventField>,
    ) -> Result<Mapping, MapError> {
        let (kept, over) = split_count(&others, MAX_EVENT_FIELDS - 5);
        let whole = self.event(message.clone(), &[], kept);
        if whole.admission_bytes().is_ok() {
            return Ok(Mapping::Event(Box::new(Forwarded {
                event: whole,
                truncated: false,
                omitted_fields: over,
            })));
        }
        let total = u64::try_from(message_bytes.len()).unwrap_or(u64::MAX);
        let digest = purrdf_hash::hex::encode(purrdf_hash::blake3::hash(message_bytes).as_bytes());
        let extra = [
            EventField {
                name: "message_truncated".to_owned(),
                value: FieldValue::Boolean(true),
            },
            EventField {
                name: "message_bytes".to_owned(),
                value: FieldValue::Unsigned(total),
            },
            EventField {
                name: "message_blake3".to_owned(),
                value: FieldValue::Text(digest),
            },
        ];
        let cut = self.longest_fitting_prefix(&message, message_bytes, &extra)?;
        let (kept, more) = split_count(&others, MAX_EVENT_FIELDS - 5 - extra.len());
        let mut kept_fields: Vec<EventField> = Vec::new();
        let mut omitted = over + more;
        for field in kept {
            kept_fields.push(field.clone());
            let candidate = self.event(cut_value(&message, cut), &extra, &kept_fields);
            if candidate.admission_bytes().is_err() {
                kept_fields.pop();
                omitted += 1;
            }
        }
        Ok(Mapping::Event(Box::new(Forwarded {
            event: self.event(cut_value(&message, cut), &extra, &kept_fields),
            truncated: true,
            omitted_fields: omitted,
        })))
    }

    /// Binary search over byte offsets for the largest prefix that fits with `extra`.
    fn longest_fitting_prefix(
        &self,
        message: &FieldValue,
        bytes: &[u8],
        extra: &[EventField],
    ) -> Result<usize, MapError> {
        let fits = |cut: usize| {
            self.event(cut_value(message, cut), extra, &[])
                .admission_bytes()
                .is_ok()
        };
        if !fits(0) {
            return Err(MapError::Oversized);
        }
        let (mut low, mut high) = (0, bytes.len());
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            if fits(floor_boundary(message, middle)) {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        Ok(floor_boundary(message, low))
    }
}

/// The first `limit` fields and the number of fields beyond it.
fn split_count(fields: &[EventField], limit: usize) -> (&[EventField], u64) {
    let kept = fields.len().min(limit);
    (&fields[..kept], (fields.len() - kept) as u64)
}

/// Largest byte offset at or below `cut` that keeps text valid UTF-8.
fn floor_boundary(message: &FieldValue, cut: usize) -> usize {
    match message {
        FieldValue::Text(text) => {
            let mut cut = cut.min(text.len());
            while !text.is_char_boundary(cut) {
                cut -= 1;
            }
            cut
        }
        _ => cut,
    }
}

/// The message value cut at `cut` bytes, keeping its text or binary form.
fn cut_value(message: &FieldValue, cut: usize) -> FieldValue {
    match message {
        FieldValue::Text(text) => FieldValue::Text(text[..floor_boundary(message, cut)].to_owned()),
        FieldValue::Bytes(bytes) => FieldValue::Bytes(bytes[..cut.min(bytes.len())].to_vec()),
        other => other.clone(),
    }
}

fn text_field(name: &str, value: String) -> EventField {
    EventField {
        name: name.to_owned(),
        value: FieldValue::Text(value),
    }
}

fn lossy(field: &JournalField) -> String {
    String::from_utf8_lossy(field.bytes()).into_owned()
}

/// Service name: the user unit, then the system unit, then the syslog identifier, then the
/// command name. A user unit comes first because journald gives its entries
/// `_SYSTEMD_UNIT=user@<uid>.service`.
fn service_name(entry: &JournalEntry) -> String {
    [
        "_SYSTEMD_USER_UNIT",
        "_SYSTEMD_UNIT",
        "SYSLOG_IDENTIFIER",
        "_COMM",
    ]
    .iter()
    .find_map(|name| entry.get(name).map(lossy))
    .unwrap_or_default()
}

/// The event level and the syslog severity name. An entry without `PRIORITY` is informational.
fn priority(entry: &JournalEntry) -> Result<(EventLevel, &'static str), MapError> {
    let value = match entry.get("PRIORITY") {
        None => 6,
        Some(field) => field
            .text()
            .and_then(|text| text.parse::<u8>().ok())
            .ok_or(MapError::Priority)?,
    };
    Ok(match value {
        0 => (EventLevel::Error, "emerg"),
        1 => (EventLevel::Error, "alert"),
        2 => (EventLevel::Error, "crit"),
        3 => (EventLevel::Error, "err"),
        4 => (EventLevel::Warn, "warning"),
        5 => (EventLevel::Info, "notice"),
        6 => (EventLevel::Info, "info"),
        7 => (EventLevel::Debug, "debug"),
        _ => return Err(MapError::Priority),
    })
}

/// Whether the entry came from one of the forwarder's own unit instances.
fn is_own_unit(entry: &JournalEntry) -> bool {
    ["_SYSTEMD_USER_UNIT", "_SYSTEMD_UNIT"].iter().any(|name| {
        entry
            .get(name)
            .and_then(JournalField::text)
            .is_some_and(|unit| unit.starts_with(OWN_UNIT_PREFIX))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(name: &str, value: &str) -> Vec<u8> {
        format!("{name}={value}\n").into_bytes()
    }

    fn binary(name: &str, value: &[u8]) -> Vec<u8> {
        let mut out = format!("{name}\n").into_bytes();
        out.extend_from_slice(&(value.len() as u64).to_le_bytes());
        out.extend_from_slice(value);
        out.push(b'\n');
        out
    }

    /// Two entries: one text-only, one whose MESSAGE is binary and multi-line.
    fn fixture() -> Vec<u8> {
        let mut stream = Vec::new();
        for field in [
            text("__CURSOR", "s=one;i=1"),
            text("__REALTIME_TIMESTAMP", "1700000000000001"),
            text("PRIORITY", "3"),
            text("_SYSTEMD_UNIT", "nginx.service"),
            text("MESSAGE", "plain"),
            b"\n".to_vec(),
        ] {
            stream.extend(field);
        }
        for field in [
            text("__CURSOR", "s=one;i=2"),
            text("__REALTIME_TIMESTAMP", "1700000000000002"),
            text("PRIORITY", "6"),
            binary("MESSAGE", b"line one\nline two"),
            text("_HOSTNAME", "host-a"),
            b"\n".to_vec(),
        ] {
            stream.extend(field);
        }
        stream
    }

    /// INVARIANT: a chunk boundary anywhere in the stream yields the same entries as one push.
    #[test]
    fn journal_export_parser_reads_text_and_binary_fields() {
        let stream = fixture();
        let whole = ExportParser::new().push(&stream).expect("valid stream");
        assert_eq!(whole.len(), 2);
        assert_eq!(whole[1].cursor(), "s=one;i=2");
        assert_eq!(
            whole[1].get("MESSAGE").map(JournalField::bytes),
            Some(&b"line one\nline two"[..])
        );
        for split in 1..stream.len() {
            let mut parser = ExportParser::new();
            let mut entries = parser.push(&stream[..split]).expect("prefix");
            entries.extend(parser.push(&stream[split..]).expect("suffix"));
            assert_eq!(entries, whole, "split at {split}");
        }
    }

    /// INVARIANT: a malformed or oversized entry stops the parser and names the cursor of the last
    /// complete entry before it, so a restart resumes from there.
    #[test]
    fn journal_export_parser_refuses_malformed_and_oversized_entries() {
        let mut stream = fixture();
        stream.extend(b"lower=case\n\n");
        let error = ExportParser::new().push(&stream).expect_err("bad key");
        assert_eq!(
            error,
            ParseError::Malformed {
                cursor_before: Some("s=one;i=2".to_owned()),
                reason: "field name is not a journal key",
            }
        );

        // INVARIANT: an entry over the parser bound is refused, not buffered without limit.
        let mut parser = ExportParser::new();
        let big = vec![b'x'; MAX_ENTRY_BYTES + 1];
        let mut stream = b"MESSAGE=".to_vec();
        stream.extend(big);
        let error = parser
            .push(&stream)
            .expect_err("no newline inside the bound");
        assert_eq!(
            error,
            ParseError::Oversized {
                cursor_before: None
            }
        );
    }

    fn entry(fields: &[(&str, &str)]) -> JournalEntry {
        let mut stream = Vec::new();
        for (name, value) in fields {
            stream.extend(text(name, value));
        }
        stream.push(b'\n');
        ExportParser::new()
            .push(&stream)
            .expect("valid entry")
            .remove(0)
    }

    fn event_of(mapping: Mapping) -> Forwarded {
        match mapping {
            Mapping::Event(forwarded) => *forwarded,
            Mapping::OwnUnit => panic!("expected an event"),
        }
    }

    /// INVARIANT: the fixed fields occupy positions 0 to 4 with the documented service order,
    /// and the priority maps to its level and severity name.
    #[test]
    fn journal_entry_maps_fixed_fields_for_diagnostics() {
        let user = entry(&[
            ("__CURSOR", "c1"),
            ("__REALTIME_TIMESTAMP", "1700000000123456"),
            ("PRIORITY", "4"),
            ("_SYSTEMD_USER_UNIT", "app.service"),
            ("_SYSTEMD_UNIT", "user@1000.service"),
            ("MESSAGE", "hello"),
        ]);
        let forwarded = event_of(map_entry(&user, "journal.user").expect("maps"));
        assert_eq!(forwarded.event.event_time, 1_700_000_000_123);
        assert_eq!(forwarded.event.level, EventLevel::Warn);
        assert_eq!(forwarded.event.target, "journal.user");
        let names: Vec<&str> = forwarded
            .event
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(
            &names[..5],
            ["service", "host", "severity", "message", "journal_cursor"]
        );
        assert_eq!(
            forwarded.event.fields[0].value,
            FieldValue::Text("app.service".into())
        );
        assert_eq!(
            forwarded.event.fields[2].value,
            FieldValue::Text("warning".into())
        );
        assert!(!forwarded.truncated);

        let fallback = entry(&[
            ("__CURSOR", "c2"),
            ("__REALTIME_TIMESTAMP", "1000"),
            ("PRIORITY", "3"),
            ("SYSLOG_IDENTIFIER", "kernel-thing"),
            ("_COMM", "ignored"),
        ]);
        let forwarded = event_of(map_entry(&fallback, "journal.system").expect("maps"));
        assert_eq!(
            forwarded.event.fields[0].value,
            FieldValue::Text("kernel-thing".into())
        );
        assert_eq!(forwarded.event.level, EventLevel::Error);
    }

    /// INVARIANT: the forwarder's own unit output is never forwarded, so it cannot feed itself.
    #[test]
    fn forwarder_skips_its_own_output() {
        let own = entry(&[
            ("__CURSOR", "c3"),
            ("__REALTIME_TIMESTAMP", "1000"),
            ("_SYSTEMD_USER_UNIT", "sparrow-journal@user.service"),
        ]);
        assert_eq!(map_entry(&own, "journal.user"), Ok(Mapping::OwnUnit));
    }

    /// INVARIANT: an entry over the event bound keeps its fixed fields, reports the cut, and
    /// its BLAKE3 digest is that of the full message.
    #[test]
    fn oversized_journal_entry_is_truncated_visibly() {
        let message = "é".repeat(60_000);
        let big = entry(&[
            ("__CURSOR", "c4"),
            ("__REALTIME_TIMESTAMP", "1000"),
            ("MESSAGE", &message),
        ]);
        let forwarded = event_of(map_entry(&big, "journal.system").expect("maps"));
        assert!(forwarded.truncated);
        let kept = match &forwarded.event.fields[3].value {
            FieldValue::Text(text) => text.clone(),
            other => panic!("message is text, got {other:?}"),
        };
        assert!(message.starts_with(&kept));
        assert!(kept.len() < message.len());
        let names: Vec<&str> = forwarded
            .event
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert!(names.contains(&"message_truncated"));
        let digest = forwarded
            .event
            .fields
            .iter()
            .find(|f| f.name == "message_blake3")
            .expect("digest field");
        assert_eq!(
            digest.value,
            FieldValue::Text(purrdf_hash::hex::encode(
                purrdf_hash::blake3::hash(message.as_bytes()).as_bytes()
            ))
        );
        assert!(forwarded.event.admission_bytes().is_ok());
    }
}
