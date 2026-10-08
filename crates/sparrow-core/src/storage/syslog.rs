// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Syslog packet grammar and immutable protocol evidence; raw bytes remain authoritative.

use super::diagnostic::{DiagnosticDimension, DiagnosticSnapshot, DiagnosticValue};
use super::source_time::EventTime;
use super::{Catalog, Error, Record, integer, read_field};
use purrdf_lex::json::{Object, Value};
use std::collections::BTreeSet;

/// Maximum raw packet admitted by the protocol interpreter.
pub const SYSLOG_MESSAGE_BYTES: usize = 64 * 1024;
/// Maximum structured elements captured in one packet.
pub const SYSLOG_ELEMENTS: usize = 32;
/// Maximum ordered parameter occurrences across all elements.
pub const SYSLOG_PARAMETERS: usize = 128;

const MODERN_HEADERS: [(&str, usize); 5] = [
    ("timestamp", 32),
    ("host", 255),
    ("app", 48),
    ("procid", 128),
    ("msgid", 32),
];

/// Immutable interpretation identity, independent of listener and JSON mappings.
pub fn syslog_profile_id() -> String {
    format!("syslog-profile:{}", super::hex::encode(super::blake3::hash(b"sparrow-syslog/1:rfc5424+rfc3164:raw-spans:millisecond-exact:unknown-legacy:sd-encoded-spans").as_bytes()))
}

/// Supported packet dialect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyslogDialect {
    /// Modern versioned header and structured data.
    Rfc5424,
    /// Legacy local timestamp without an inferred year or zone.
    Rfc3164,
}

/// One interpreted header spelling and its exact original byte evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyslogField {
    /// Closed header role: timestamp, host, app, procid, msgid or tag.
    pub kind: String,
    /// Original ASCII spelling, with NILVALUE represented by absence.
    pub value: String,
    /// Inclusive original artifact offset.
    pub source_start: u64,
    /// Exclusive original artifact offset.
    pub source_end: u64,
}

/// One parameter occurrence; encoded value evidence includes original escapes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyslogParameter {
    /// Original case-sensitive parameter name; repeated names remain occurrences.
    pub name: String,
    /// First value byte inside the original quotes.
    pub value_start: u64,
    /// First byte after the encoded value, before the closing quote.
    pub value_end: u64,
}

/// One ordered structured-data element.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyslogElement {
    /// Case-sensitive structured-data identity.
    pub id: String,
    /// Original opening bracket offset.
    pub source_start: u64,
    /// First byte after the closing bracket.
    pub source_end: u64,
    /// Ordered encoded parameter evidence.
    pub parameters: Vec<SyslogParameter>,
}

/// Bounded packet interpretation; a diagnostic retains raw acceptance without claiming a parse.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyslogSnapshot {
    /// Versioned protocol interpretation, distinct from JSON mapping profiles.
    pub profile_id: String,
    /// Parsed dialect, absent on interpretation failure.
    pub dialect: Option<SyslogDialect>,
    /// Raw PRI numeric value, when the whole packet parses.
    pub priority: Option<u16>,
    /// Original PRI digits start.
    pub priority_start: u64,
    /// Original PRI digits end.
    pub priority_end: u64,
    /// Interpreted header fields, in packet order.
    pub fields: Vec<SyslogField>,
    /// Ordered structured-data elements with encoded value references.
    pub structured_data: Vec<SyslogElement>,
    /// First original message-body byte; the body can be arbitrary binary data.
    pub message_start: u64,
    /// Explicit malformed/unsupported/bounded-interpretation diagnostic.
    pub diagnostic: Option<String>,
}

fn span(first: u64, last: u64, start: u64, end: u64) -> bool {
    first >= start && first <= last && last <= end
}
fn sd_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 32
        && name
            .bytes()
            .all(|byte| (33..=126).contains(&byte) && !b"=]\"".contains(&byte))
}

impl Catalog for SyslogSnapshot {
    fn encode(&self) -> Value {
        Object::new()
            .with("profile_id", self.profile_id.clone())
            .with(
                "dialect",
                self.dialect.map(|value| match value {
                    SyslogDialect::Rfc5424 => "rfc5424",
                    SyslogDialect::Rfc3164 => "rfc3164",
                }),
            )
            .with("priority", self.priority.map(u64::from))
            .with("priority_start", self.priority_start.to_string())
            .with("priority_end", self.priority_end.to_string())
            .with(
                "fields",
                self.fields
                    .iter()
                    .map(|field| {
                        Object::new()
                            .with("kind", field.kind.clone())
                            .with("value", field.value.clone())
                            .with("source_start", field.source_start.to_string())
                            .with("source_end", field.source_end.to_string())
                            .into()
                    })
                    .collect::<Vec<Value>>(),
            )
            .with(
                "structured_data",
                self.structured_data
                    .iter()
                    .map(|element| {
                        Object::new()
                            .with("id", element.id.clone())
                            .with("source_start", element.source_start.to_string())
                            .with("source_end", element.source_end.to_string())
                            .with(
                                "parameters",
                                element
                                    .parameters
                                    .iter()
                                    .map(|parameter| {
                                        Object::new()
                                            .with("name", parameter.name.clone())
                                            .with("value_start", parameter.value_start.to_string())
                                            .with("value_end", parameter.value_end.to_string())
                                            .into()
                                    })
                                    .collect::<Vec<Value>>(),
                            )
                            .into()
                    })
                    .collect::<Vec<Value>>(),
            )
            .with("message_start", self.message_start.to_string())
            .with("diagnostic", self.diagnostic.clone())
            .into()
    }

    fn decode(value: &Value) -> Result<Self, Error> {
        let fields = value
            .get("fields")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "syslog header fields missing"))?;
        let elements = value
            .get("structured_data")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "syslog structured data missing"))?;
        if fields.len() > 6 || elements.len() > SYSLOG_ELEMENTS {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "syslog snapshot exceeds its interpretation bounds",
            ));
        }
        let dialect: Option<String> = read_field(value, "dialect")?;
        let dialect = match dialect.as_deref() {
            None => None,
            Some("rfc5424") => Some(SyslogDialect::Rfc5424),
            Some("rfc3164") => Some(SyslogDialect::Rfc3164),
            _ => return Err(Error::new("STORAGE_FAILURE", "invalid syslog dialect")),
        };
        let state = Self {
            profile_id: read_field(value, "profile_id")?,
            dialect,
            priority: read_field(value, "priority")?,
            priority_start: integer(value, "priority_start")?,
            priority_end: integer(value, "priority_end")?,
            fields: fields
                .iter()
                .map(|field| {
                    Ok(SyslogField {
                        kind: read_field(field, "kind")?,
                        value: read_field(field, "value")?,
                        source_start: integer(field, "source_start")?,
                        source_end: integer(field, "source_end")?,
                    })
                })
                .collect::<Result<_, Error>>()?,
            structured_data: elements
                .iter()
                .map(|element| {
                    let parameters = element
                        .get("parameters")
                        .and_then(Value::as_array)
                        .ok_or_else(|| {
                            Error::new("STORAGE_FAILURE", "syslog parameter evidence missing")
                        })?;
                    if parameters.len() > SYSLOG_PARAMETERS {
                        return Err(Error::new(
                            "STORAGE_FAILURE",
                            "syslog parameter evidence exceeds its bound",
                        ));
                    }
                    Ok(SyslogElement {
                        id: read_field(element, "id")?,
                        source_start: integer(element, "source_start")?,
                        source_end: integer(element, "source_end")?,
                        parameters: parameters
                            .iter()
                            .map(|parameter| {
                                Ok(SyslogParameter {
                                    name: read_field(parameter, "name")?,
                                    value_start: integer(parameter, "value_start")?,
                                    value_end: integer(parameter, "value_end")?,
                                })
                            })
                            .collect::<Result<_, Error>>()?,
                    })
                })
                .collect::<Result<_, Error>>()?,
            message_start: integer(value, "message_start")?,
            diagnostic: read_field(value, "diagnostic")?,
        };
        if state.profile_id != syslog_profile_id()
            || state
                .diagnostic
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > 256)
            || state.priority.is_some_and(|priority| priority > 191)
            || state.dialect.is_some() != state.priority.is_some()
            || state.diagnostic.is_some() == state.dialect.is_some()
            || (state.diagnostic.is_some()
                && (!state.fields.is_empty() || !state.structured_data.is_empty()))
            || state
                .structured_data
                .iter()
                .map(|element| element.parameters.len())
                .sum::<usize>()
                > SYSLOG_PARAMETERS
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "syslog snapshot invariants inconsistent",
            ));
        }
        Ok(state)
    }
}

impl SyslogSnapshot {
    fn field(&self, kind: &str) -> Option<(usize, &SyslogField)> {
        self.fields
            .iter()
            .enumerate()
            .find(|(_, field)| field.kind == kind)
    }

    pub(super) fn validate_spans(&self, start: u64, end: u64) -> Result<(), Error> {
        let mut names = BTreeSet::new();
        let mut elements = BTreeSet::new();
        if end
            .checked_sub(start)
            .is_none_or(|length| length > SYSLOG_MESSAGE_BYTES as u64)
            || !span(self.message_start, self.message_start, start, end)
            || (self.priority.is_some()
                && (!span(self.priority_start, self.priority_end, start, end)
                    || self.priority_start == self.priority_end))
            || self.fields.iter().any(|field| {
                let maximum = match self.dialect {
                    Some(SyslogDialect::Rfc5424) => MODERN_HEADERS
                        .iter()
                        .find(|(name, _)| *name == field.kind)
                        .map(|(_, limit)| *limit),
                    Some(SyslogDialect::Rfc3164) => match field.kind.as_str() {
                        "timestamp" => Some(15),
                        "host" => Some(255),
                        "tag" => Some(32),
                        _ => None,
                    },
                    None => None,
                };
                !names.insert(&field.kind)
                    || maximum.is_none_or(|maximum| field.value.len() > maximum)
                    || field.value.is_empty()
                    || !field.value.is_ascii()
                    || (field.kind == "timestamp"
                        && match self.dialect {
                            Some(SyslogDialect::Rfc5424) => timestamp(&field.value).is_err(),
                            Some(SyslogDialect::Rfc3164) => {
                                !legacy_timestamp(field.value.as_bytes())
                            }
                            None => true,
                        })
                    || (field.kind != "timestamp"
                        && !field.value.bytes().all(|byte| (33..=126).contains(&byte)))
                    || !span(field.source_start, field.source_end, start, end)
                    || field.source_end - field.source_start != field.value.len() as u64
            })
            || self
                .fields
                .windows(2)
                .any(|pair| pair[0].source_end > pair[1].source_start)
            || (self.dialect == Some(SyslogDialect::Rfc3164)
                && (self.field("timestamp").is_none() || self.field("host").is_none()))
            || self.priority.is_some_and(|priority| {
                self.priority_start != start + 1
                    || self.priority_end - self.priority_start != priority.to_string().len() as u64
            })
            || self.structured_data.iter().any(|element| {
                !sd_name(&element.id)
                    || !elements.insert(&element.id)
                    || !span(element.source_start, element.source_end, start, end)
                    || element.parameters.iter().any(|parameter| {
                        !sd_name(&parameter.name)
                            || !span(
                                parameter.value_start,
                                parameter.value_end,
                                element.source_start,
                                element.source_end,
                            )
                    })
            })
            || (self.dialect == Some(SyslogDialect::Rfc3164) && !self.structured_data.is_empty())
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "syslog protocol evidence outside its packet or malformed",
            ));
        }
        Ok(())
    }

    pub(super) fn event_time(&self) -> EventTime {
        let unknown = |reason: &str| EventTime::Unknown {
            reason: reason.to_owned(),
            profile_id: Some(self.profile_id.clone()),
        };
        if self.diagnostic.is_some() {
            return unknown("syslog_malformed");
        }
        if self.dialect == Some(SyslogDialect::Rfc3164) {
            return unknown("syslog_legacy_time");
        }
        let Some((ordinal, field)) = self.field("timestamp") else {
            return unknown("syslog_nil_time");
        };
        match timestamp(&field.value) {
            Ok((seconds, nanos)) if seconds >= 0 && nanos % 1_000_000 == 0 => EventTime::Known {
                millis: seconds as u64 * 1000 + u64::from(nanos / 1_000_000),
                profile_id: self.profile_id.clone(),
                occurrence: ordinal as u64,
                source_start: field.source_start,
                source_end: field.source_end,
            },
            Ok((_, nanos)) if nanos % 1_000_000 != 0 => unknown("syslog_submillisecond"),
            _ => unknown("invalid_instant"),
        }
    }

    pub(super) fn diagnostics(&self) -> DiagnosticSnapshot {
        let field = |kind: &str| {
            self.field(kind).map_or(
                DiagnosticValue::Unknown(
                    if self.diagnostic.is_some() {
                        "projection_failed"
                    } else {
                        "missing_field"
                    }
                    .into(),
                ),
                |(ordinal, field)| DiagnosticValue::Known {
                    value: field.value.clone(),
                    occurrence: ordinal as u64,
                    source_start: field.source_start,
                    source_end: field.source_end,
                },
            )
        };
        DiagnosticSnapshot {
            profile_id: Some(self.profile_id.clone()),
            fields: [
                (DiagnosticDimension::Host, field("host")),
                (
                    DiagnosticDimension::Service,
                    field(if self.dialect == Some(SyslogDialect::Rfc3164) {
                        "tag"
                    } else {
                        "app"
                    }),
                ),
                (
                    DiagnosticDimension::Severity,
                    self.priority.map_or(
                        DiagnosticValue::Unknown("projection_failed".into()),
                        |priority| DiagnosticValue::Known {
                            value: (priority % 8).to_string(),
                            occurrence: 0,
                            source_start: self.priority_start,
                            source_end: self.priority_end,
                        },
                    ),
                ),
            ]
            .into(),
        }
    }

    pub(super) fn validate_record(&self, record: &Record) -> Result<(), Error> {
        self.validate_spans(record.byte_start, record.byte_end)?;
        if record.event_time.encode() != self.event_time().encode()
            || record.diagnostics.encode() != self.diagnostics().encode()
            || record.producer.is_some()
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "syslog record interpretation disagrees with immutable protocol evidence",
            ));
        }
        Ok(())
    }
}

impl super::Store {
    /// Receive one protocol message with a fresh occurrence; syslog has no replay receipt on its wire.
    pub fn ingest_syslog(
        &self,
        source: &str,
        bytes: &[u8],
        now: u64,
    ) -> Result<super::Artifact, Error> {
        self.ingest(
            &super::random_id("syslog-intake")?,
            source,
            "syslog",
            bytes,
            now,
        )
    }
}

fn timestamp(value: &str) -> Result<(i64, u32), String> {
    if value.len() < 20
        || value.as_bytes().get(10) != Some(&b'T')
        || value.as_bytes()[17..19]
            .iter()
            .any(|byte| !byte.is_ascii_digit())
        || &value.as_bytes()[17..19] > b"59"
        || value.contains('z')
        || value.find('.').is_some_and(|offset| {
            !(1..=6).contains(
                &value.as_bytes()[offset + 1..]
                    .iter()
                    .take_while(|byte| byte.is_ascii_digit())
                    .count(),
            )
        })
    {
        return Err("invalid RFC 5424 timestamp spelling".into());
    }
    purrdf::xsd::rfc3339::parse(
        value,
        purrdf::xsd::rfc3339::Separator::Rfc3339Abnf,
        purrdf::xsd::rfc3339::LeapSecond::MonthEnd,
    )
    .map_err(|error| error.to_string())
}

fn legacy_timestamp(time: &[u8]) -> bool {
    if time.len() != 15 {
        return false;
    }
    let month = [
        b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug", b"Sep", b"Oct", b"Nov",
        b"Dec",
    ]
    .iter()
    .any(|month| time.starts_with(*month));
    let number = |slice: &[u8]| {
        std::str::from_utf8(slice)
            .ok()
            .and_then(|text| text.trim_start().parse::<u8>().ok())
    };
    month
        && time[3] == b' '
        && time[6] == b' '
        && time[9] == b':'
        && time[12] == b':'
        && (time[4] == b' ' || time[4].is_ascii_digit())
        && time[5].is_ascii_digit()
        && number(&time[4..6]).is_some_and(|day| (1..=31).contains(&day))
        && time[7..9]
            .iter()
            .chain(&time[10..12])
            .chain(&time[13..15])
            .all(u8::is_ascii_digit)
        && number(&time[7..9]).is_some_and(|hour| hour < 24)
        && number(&time[10..12]).is_some_and(|minute| minute < 60)
        && number(&time[13..15]).is_some_and(|second| second < 60)
}

struct Packet<'a> {
    bytes: &'a [u8],
    offset: usize,
    start: u64,
}
impl Packet<'_> {
    fn failure(&self, message: &str) -> String {
        format!("{message} at packet byte {}", self.offset)
    }
    fn head(&mut self, maximum: usize) -> Result<(String, u64, u64), String> {
        let first = self.offset;
        let count = self.bytes[first..]
            .iter()
            .position(|byte| *byte == b' ')
            .ok_or_else(|| self.failure("header separator missing"))?;
        if count == 0
            || count > maximum
            || self.bytes[first..first + count]
                .iter()
                .any(|byte| !(33..=126).contains(byte))
        {
            return Err(self.failure("invalid bounded ASCII header"));
        }
        self.offset += count + 1;
        Ok((
            String::from_utf8(self.bytes[first..first + count].to_vec())
                .map_err(|error| error.to_string())?,
            self.start + first as u64,
            self.start + (first + count) as u64,
        ))
    }
    fn header(
        &mut self,
        fields: &mut Vec<SyslogField>,
        kind: &str,
        maximum: usize,
    ) -> Result<(), String> {
        let (value, source_start, source_end) = self.head(maximum)?;
        if value != "-" {
            fields.push(SyslogField {
                kind: kind.to_owned(),
                value,
                source_start,
                source_end,
            });
        }
        Ok(())
    }
    fn name(&mut self) -> Result<String, String> {
        let first = self.offset;
        while self.offset < self.bytes.len()
            && (33..=126).contains(&self.bytes[self.offset])
            && !b"=]\"".contains(&self.bytes[self.offset])
        {
            self.offset += 1;
        }
        let value = std::str::from_utf8(&self.bytes[first..self.offset])
            .map_err(|error| error.to_string())?;
        if !sd_name(value) {
            return Err(self.failure("invalid structured-data name"));
        }
        Ok(value.to_owned())
    }
    fn structured(&mut self) -> Result<Vec<SyslogElement>, String> {
        if self.bytes.get(self.offset) == Some(&b'-') {
            self.offset += 1;
            return Ok(Vec::new());
        }
        let mut elements = Vec::new();
        let mut names = BTreeSet::new();
        let mut count = 0;
        while self.bytes.get(self.offset) == Some(&b'[') {
            if elements.len() == SYSLOG_ELEMENTS {
                return Err(self.failure("structured-data element admission exceeded"));
            }
            let first = self.offset;
            self.offset += 1;
            let id = self.name()?;
            if !names.insert(id.clone()) {
                return Err(self.failure("duplicate structured-data identity"));
            }
            let mut parameters = Vec::new();
            while self.bytes.get(self.offset) == Some(&b' ') {
                if count == SYSLOG_PARAMETERS {
                    return Err(self.failure("structured-data parameter admission exceeded"));
                }
                self.offset += 1;
                let name = self.name()?;
                if self.bytes.get(self.offset..self.offset + 2) != Some(b"=\"") {
                    return Err(self.failure("parameter quote missing"));
                }
                self.offset += 2;
                let value_start = self.offset;
                loop {
                    match self.bytes.get(self.offset) {
                        Some(b'"') => break,
                        Some(b']') => return Err(self.failure("unescaped parameter bracket")),
                        Some(b'\\')
                            if self
                                .bytes
                                .get(self.offset + 1)
                                .is_some_and(|byte| b"\\\"]".contains(byte)) =>
                        {
                            self.offset += 2
                        }
                        Some(_) => self.offset += 1,
                        None => return Err(self.failure("unterminated parameter value")),
                    }
                }
                std::str::from_utf8(&self.bytes[value_start..self.offset])
                    .map_err(|_| self.failure("parameter value is not UTF-8"))?;
                parameters.push(SyslogParameter {
                    name,
                    value_start: self.start + value_start as u64,
                    value_end: self.start + self.offset as u64,
                });
                self.offset += 1;
                count += 1;
            }
            if self.bytes.get(self.offset) != Some(&b']') {
                return Err(self.failure("structured-data closing bracket missing"));
            }
            self.offset += 1;
            elements.push(SyslogElement {
                id,
                source_start: self.start + first as u64,
                source_end: self.start + self.offset as u64,
                parameters,
            });
        }
        if elements.is_empty() {
            return Err(self.failure("structured data missing"));
        }
        Ok(elements)
    }
}

pub(super) fn capture(bytes: &[u8], start: u64) -> Result<SyslogSnapshot, Error> {
    if bytes.len() > SYSLOG_MESSAGE_BYTES {
        return Err(Error::new(
            "RESOURCE_LIMIT",
            "syslog packet exceeds 65536 raw bytes",
        ));
    }
    let parsed = parse_packet(bytes, start);
    Ok(parsed.unwrap_or_else(|diagnostic| SyslogSnapshot {
        profile_id: syslog_profile_id(),
        dialect: None,
        priority: None,
        priority_start: start,
        priority_end: start,
        fields: Vec::new(),
        structured_data: Vec::new(),
        message_start: start,
        diagnostic: Some(diagnostic),
    }))
}

fn parse_packet(bytes: &[u8], start: u64) -> Result<SyslogSnapshot, String> {
    let mut packet = Packet {
        bytes,
        offset: 0,
        start,
    };
    if bytes.first() != Some(&b'<') {
        return Err(packet.failure("priority opening bracket missing"));
    }
    let end = bytes
        .iter()
        .position(|byte| *byte == b'>')
        .filter(|end| (2..=4).contains(end))
        .ok_or_else(|| packet.failure("priority closing bracket missing"))?;
    let digits = &bytes[1..end];
    if !digits.iter().all(u8::is_ascii_digit) || (digits.len() > 1 && digits[0] == b'0') {
        return Err(packet.failure("invalid priority spelling"));
    }
    let priority: u16 = std::str::from_utf8(digits)
        .map_err(|error| error.to_string())?
        .parse()
        .map_err(|_| packet.failure("invalid priority"))?;
    if priority > 191 {
        return Err(packet.failure("priority exceeds facility/severity domain"));
    }
    packet.offset = end + 1;
    let dialect = if bytes.get(packet.offset).is_some_and(u8::is_ascii_digit) {
        SyslogDialect::Rfc5424
    } else {
        SyslogDialect::Rfc3164
    };
    let mut fields = Vec::new();
    let structured_data = match dialect {
        SyslogDialect::Rfc5424 => {
            if packet.head(3)?.0 != "1" {
                return Err(packet.failure("unsupported syslog version"));
            }
            for (kind, maximum) in MODERN_HEADERS {
                packet.header(&mut fields, kind, maximum)?;
            }
            if let Some(field) = fields.iter().find(|field| field.kind == "timestamp") {
                timestamp(&field.value)
                    .map_err(|_| packet.failure("invalid RFC 5424 timestamp"))?;
            }
            let structured = packet.structured()?;
            if packet.offset < bytes.len() {
                if bytes[packet.offset] != b' ' {
                    return Err(packet.failure("message separator missing"));
                }
                packet.offset += 1;
            }
            if bytes[packet.offset..].starts_with(&[0xef, 0xbb, 0xbf]) {
                std::str::from_utf8(&bytes[packet.offset + 3..])
                    .map_err(|_| packet.failure("BOM message is not UTF-8"))?;
            }
            structured
        }
        SyslogDialect::Rfc3164 => {
            let first = packet.offset;
            let time = bytes
                .get(first..first + 15)
                .ok_or_else(|| packet.failure("legacy timestamp missing"))?;
            if !legacy_timestamp(time) || bytes.get(first + 15) != Some(&b' ') {
                return Err(packet.failure("invalid legacy timestamp"));
            }
            fields.push(SyslogField {
                kind: "timestamp".into(),
                value: std::str::from_utf8(time)
                    .map_err(|error| error.to_string())?
                    .into(),
                source_start: start + first as u64,
                source_end: start + (first + 15) as u64,
            });
            packet.offset += 16;
            let (value, source_start, source_end) = packet.head(255)?;
            fields.push(SyslogField {
                kind: "host".into(),
                value,
                source_start,
                source_end,
            });
            let first = packet.offset;
            let length = bytes[first..]
                .iter()
                .take_while(|byte| byte.is_ascii_alphanumeric())
                .count();
            if (1..=32).contains(&length)
                && bytes
                    .get(first + length)
                    .is_some_and(|byte| b"[: ".contains(byte))
            {
                fields.push(SyslogField {
                    kind: "tag".into(),
                    value: std::str::from_utf8(&bytes[first..first + length])
                        .map_err(|error| error.to_string())?
                        .into(),
                    source_start: start + first as u64,
                    source_end: start + (first + length) as u64,
                });
            }
            Vec::new()
        }
    };
    Ok(SyslogSnapshot {
        profile_id: syslog_profile_id(),
        dialect: Some(dialect),
        priority: Some(priority),
        priority_start: start + 1,
        priority_end: start + end as u64,
        fields,
        structured_data,
        message_start: start + packet.offset as u64,
        diagnostic: None,
    })
}
