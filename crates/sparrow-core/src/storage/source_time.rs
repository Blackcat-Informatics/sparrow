// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Immutable source mappings and native timestamp evidence captured on ingestion.

use super::{Error, Record, Store, Writer, blake3, field, hex, integer, json, parse, read_field};
use purrdf_lex::json::{Object, Value, record::DecodeError};
use rocksdb::WriteBatch;

pub(super) const LOG_FENCE: &str = "log-time-profile-law";

pub(super) fn validate_log_fence(db: &rocksdb::DB) -> Result<(), Error> {
    let format = db.get("store_format")?;
    let law = db.get(LOG_FENCE)?;
    if law
        .as_deref()
        .is_some_and(|law| law != super::log_time::LAW.as_bytes())
        || format
            .as_deref()
            .is_some_and(super::writer_format::has_log_time)
            != law.is_some()
    {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "log time interpretation law and writer fence disagree",
        ));
    }
    Ok(())
}

/// Explicit native scalar interpretation; no field-name or unit inference.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeRepresentation {
    /// Native decoded RFC 3339 string, exact to milliseconds.
    Rfc3339,
    /// Native integer lexical value in Unix milliseconds.
    UnixMillis,
    /// Native integer lexical value in Unix seconds.
    UnixSeconds,
}
impl TimeRepresentation {
    /// Stable profile representation name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Rfc3339 => "rfc3339",
            Self::UnixMillis => "unix_millis",
            Self::UnixSeconds => "unix_seconds",
        }
    }
    fn decode(value: &str) -> Result<Self, Error> {
        match value {
            "rfc3339" => Ok(Self::Rfc3339),
            "unix_millis" => Ok(Self::UnixMillis),
            "unix_seconds" => Ok(Self::UnixSeconds),
            _ => Err(Error::new("STORAGE_FAILURE", "unknown time representation")),
        }
    }
}

/// Immutable source binding. Changing a mapping requires a new logical source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceTimeProfile {
    /// Stable content identity, including source, pointer and clock law.
    pub id: String,
    /// Exact source name, never inferred from a filename.
    pub source: String,
    /// JSON pointer; empty and inactive when `log_capture` is present.
    pub path: String,
    /// Declared units/representation.
    pub representation: TimeRepresentation,
    /// Explicit whole-record log capture; absent for a JSON pointer mapping.
    pub log_capture: Option<super::log_time::LogTimeCapture>,
}

impl SourceTimeProfile {
    pub(super) fn new(
        source: &str,
        path: &str,
        representation: TimeRepresentation,
    ) -> Result<Self, Error> {
        if source.is_empty()
            || source.len() > 512
            || path.len() > 4096
            || purrdf_lex::json_pointer::tokens(path).is_none()
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "require source 1..512 bytes and a concrete JSON pointer of at most 4096 bytes",
            ));
        }
        let mut bytes = Vec::new();
        for part in [
            "sparrow-source-time-ms-v1",
            source,
            path,
            representation.name(),
        ] {
            purrdf_hash::frame::frame_le(&mut bytes, part.as_bytes());
        }
        Ok(Self {
            id: format!(
                "time-profile:{}",
                hex::encode(blake3::hash(&bytes).as_bytes())
            ),
            source: source.into(),
            path: path.into(),
            representation,
            log_capture: None,
        })
    }
    fn new_log(
        source: &str,
        capture: &super::log_time::LogTimeCapture,
        representation: TimeRepresentation,
    ) -> Result<Self, Error> {
        let mut profile = Self::new(source, "", representation)?;
        capture.validate_controls()?;
        let mut bytes = Vec::new();
        for part in [
            super::log_time::LAW,
            source,
            &capture.pattern,
            &capture.flags,
            &capture.group.to_string(),
            representation.name(),
        ] {
            purrdf_hash::frame::frame_le(&mut bytes, part.as_bytes());
        }
        profile.id = format!(
            "time-profile:{}",
            hex::encode(blake3::hash(&bytes).as_bytes())
        );
        profile.log_capture = Some(capture.clone());
        Ok(profile)
    }
    pub(crate) fn encode(&self) -> Value {
        let mut value = Object::new()
            .with("id", self.id.clone())
            .with("source", self.source.clone())
            .with("path", self.path.clone())
            .with("representation", self.representation.name());
        if let Some(capture) = &self.log_capture {
            value = value.with("log_capture", capture.descriptor());
        }
        value.into()
    }
    pub(super) fn decode(value: &Value) -> Result<Self, Error> {
        let source = read_field::<String>(value, "source")?;
        let path = read_field::<String>(value, "path")?;
        let representation =
            TimeRepresentation::decode(&read_field::<String>(value, "representation")?)?;
        let capture = value
            .get("log_capture")
            .filter(|v| !v.is_null())
            .map(|value| -> Result<_, Error> {
                if read_field::<String>(value, "law")? != super::log_time::LAW || !path.is_empty() {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "invalid log time extraction law",
                    ));
                }
                Ok(super::log_time::LogTimeCapture {
                    pattern: read_field(value, "pattern")?,
                    group: read_field(value, "group")?,
                    flags: read_field(value, "flags")?,
                })
            })
            .transpose()?;
        let profile = match capture {
            Some(capture) => Self::new_log(&source, &capture, representation),
            None => Self::new(&source, &path, representation),
        }
        .map_err(|e| {
            if e.code == "RESOURCE_LIMIT" {
                e
            } else {
                Error::new("STORAGE_FAILURE", e.to_string())
            }
        })?;
        if profile.id != read_field::<String>(value, "id")? {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "source time profile identity mismatch",
            ));
        }
        Ok(profile)
    }
}

/// Source event time remains separate from the acceptance/retention clock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventTime {
    /// Unique declared native occurrence produced an exact source instant.
    Known {
        /// UTC Unix milliseconds, independent of server acceptance.
        millis: u64,
        /// Immutable mapping identity at ingestion.
        profile_id: String,
        /// Ordered field occurrence within the declared interpretation profile.
        occurrence: u64,
        /// Original artifact span start.
        source_start: u64,
        /// Original artifact span end.
        source_end: u64,
    },
    /// No trustworthy source instant; reason is retained rather than guessed.
    Unknown {
        /// Stable reason code.
        reason: String,
        /// Mapping identity when a mapping was declared.
        profile_id: Option<String>,
    },
}
/// Which clock placed a record on the event timeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventTimeBasis {
    /// The source profile supplied the instant.
    Source,
    /// No source instant exists; the record's receipt time is the marked fallback.
    Receipt,
}

impl EventTimeBasis {
    /// Stable agent and RDF spelling.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Receipt => "receipt",
        }
    }
}

/// An event-timeline instant and the clock that supplied it, derived at read time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventPlacement {
    /// UTC Unix milliseconds on the event timeline.
    pub millis: u64,
    /// Source instant or receipt-time fallback.
    pub basis: EventTimeBasis,
}

impl EventPlacement {
    /// The marked receipt-time fallback of a record without a source instant.
    pub(crate) const fn receipt(received_at: u64) -> Self {
        Self {
            millis: received_at,
            basis: EventTimeBasis::Receipt,
        }
    }
}

impl EventTime {
    pub(crate) fn unknown(reason: &str, profile: Option<&SourceTimeProfile>) -> Self {
        Self::Unknown {
            reason: reason.into(),
            profile_id: profile.map(|profile| profile.id.clone()),
        }
    }
    /// The source instant, absent for unknown source time.
    pub fn millis(&self) -> Option<u64> {
        match self {
            Self::Known { millis, .. } => Some(*millis),
            Self::Unknown { .. } => None,
        }
    }
    /// Read-time event-timeline placement: the source instant, or the record's receipt
    /// time marked as a fallback. Stored evidence, including the unknown reason, is unchanged.
    #[must_use]
    pub fn placement(&self, received_at: u64) -> EventPlacement {
        match self {
            Self::Known { millis, .. } => EventPlacement {
                millis: *millis,
                basis: EventTimeBasis::Source,
            },
            Self::Unknown { .. } => EventPlacement::receipt(received_at),
        }
    }
    pub(crate) fn encode(&self) -> Value {
        match self {
            Self::Known {
                millis,
                profile_id,
                occurrence,
                source_start,
                source_end,
            } => Object::new()
                .with("state", "known")
                .with("millis", millis.to_string())
                .with("profile_id", profile_id.clone())
                .with("occurrence", occurrence.to_string())
                .with("source_start", source_start.to_string())
                .with("source_end", source_end.to_string())
                .into(),
            Self::Unknown { reason, profile_id } => Object::new()
                .with("state", "unknown")
                .with("reason", reason.clone())
                .with(
                    "profile_id",
                    profile_id.clone().map(Value::from).unwrap_or(Value::Null),
                )
                .into(),
        }
    }
    pub(crate) fn decode(value: &Value) -> Result<Self, Error> {
        match read_field::<String>(value, "state")?.as_str() {
            "known" => {
                let millis = integer(value, "millis")?;
                crate::format_instant(millis)
                    .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
                let profile_id: String = read_field(value, "profile_id")?;
                validate_profile_id(&profile_id)?;
                Ok(Self::Known {
                    millis,
                    profile_id,
                    occurrence: integer(value, "occurrence")?,
                    source_start: integer(value, "source_start")?,
                    source_end: integer(value, "source_end")?,
                })
            }
            "unknown" => {
                let reason: String = read_field(value, "reason")?;
                if ![
                    "legacy_unprofiled",
                    "no_profile",
                    "not_json",
                    "projection_failed",
                    "missing_field",
                    "ambiguous_field",
                    "wrong_kind",
                    "invalid_instant",
                    "syslog_malformed",
                    "syslog_legacy_time",
                    "syslog_nil_time",
                    "syslog_submillisecond",
                    "log_input_unavailable",
                    "log_invalid_utf8",
                    "log_no_match",
                    "log_capture_missing",
                    "log_wrong_format",
                ]
                .contains(&reason.as_str())
                {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "unknown source time diagnostic",
                    ));
                }
                let profile_id: Option<String> = match field(value, "profile_id")? {
                    Value::Null => None,
                    value => Some(
                        value
                            .as_str()
                            .ok_or_else(|| {
                                Error::new("STORAGE_FAILURE", "invalid time profile identity")
                            })?
                            .into(),
                    ),
                };
                if let Some(profile) = &profile_id {
                    validate_profile_id(profile)?;
                }
                if profile_id.is_none()
                    != ["no_profile", "legacy_unprofiled"].contains(&reason.as_str())
                {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "source time diagnostic contradicts profile presence",
                    ));
                }
                Ok(Self::Unknown { reason, profile_id })
            }
            _ => Err(Error::new("STORAGE_FAILURE", "unknown source time state")),
        }
    }
}

fn validate_profile_id(id: &str) -> Result<(), Error> {
    if id == super::syslog::syslog_profile_id() {
        return Ok(());
    }
    if id
        .strip_prefix("time-profile:")
        .and_then(purrdf_hash::hex::decode_32_canonical)
        .is_none()
    {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "invalid source time profile identity",
        ));
    }
    Ok(())
}

impl purrdf_lex::json::record::FromJson for EventTime {
    fn from_json(value: &Value) -> Result<Self, DecodeError> {
        Self::decode(value).map_err(DecodeError::custom)
    }
}

pub(super) fn source_key(source: &str) -> String {
    format!("source-time:{}", super::source_summary::source_hash(source))
}

/// Whether the immutable source mapping registry has room for one more mapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Admission {
    Room,
    /// 1024 mappings exist. Mappings are never removed, so this does not change.
    Full,
}

impl Admission {
    /// The refusal an explicit mapping declaration gets when the registry is full. A log
    /// is never refused for this (#227); only a declaration is.
    pub(super) fn full_refusal() -> Error {
        Error::new(
            "RESOURCE_LIMIT",
            "source profile registry admits at most 1024 immutable mappings",
        )
    }

    pub(super) fn or_refuse(self) -> Result<(), Error> {
        match self {
            Self::Room => Ok(()),
            Self::Full => Err(Self::full_refusal()),
        }
    }
}

impl Writer {
    pub(super) fn admit_source_profile(
        &self,
        prefix: &str,
        encoded: &[u8],
        validate: impl Fn(&[u8], &[u8]) -> Result<(), Error>,
    ) -> Result<Admission, Error> {
        for (count, entry) in self.db.prefix_iterator(prefix).enumerate() {
            let (key, bytes) = entry?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            validate(&key, &bytes)?;
            if count == 1023 {
                return Ok(Admission::Full);
            }
        }
        if self
            .physical_bytes()?
            .saturating_add(encoded.len() as u64 * 4 + 65536)
            > self.config.disk_budget
        {
            return Err(Error::new(
                "STORAGE_PRESSURE",
                "source profile exceeds physical control admission budget",
            ));
        }
        Ok(Admission::Room)
    }

    /// Bind `profile` to its source. `None` means the registry is full: nothing was bound.
    /// An identical existing mapping is returned; a different one conflicts.
    pub(super) fn bind_time_profile(
        &self,
        profile: SourceTimeProfile,
    ) -> Result<Option<SourceTimeProfile>, Error> {
        let source = profile.source.as_str();
        if let Some(previous) = self.time_profile(source)? {
            if previous != profile {
                return Err(Error::new(
                    "CONFLICT",
                    "source time mapping is immutable; use a new logical source",
                ));
            }
            return Ok(Some(previous));
        }
        let mut batch = WriteBatch::default();
        let encoded = json::write_compact(&profile.encode());
        let admission =
            self.admit_source_profile("source-time:", encoded.as_bytes(), |key, bytes| {
                let retained = SourceTimeProfile::decode(&parse(bytes)?)?;
                if key != source_key(&retained.source).as_bytes() {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "source time registry key mismatch",
                    ));
                }
                Ok(())
            })?;
        if admission == Admission::Full {
            return Ok(None);
        }
        batch.put(source_key(source), encoded);
        if profile.log_capture.is_some() {
            let format = self
                .db
                .get("store_format")?
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "writer format missing"))?;
            batch.put(
                "store_format",
                super::writer_format::enable_log_time(&format)?,
            );
            batch.put(LOG_FENCE, super::log_time::LAW);
        }
        self.commit(batch)?;
        Ok(Some(profile))
    }

    pub(super) fn validate_event_record(&self, record: &Record) -> Result<(), Error> {
        validate_event_record(&super::store_counters::Reader::db(&self.db), record)
    }
    pub(super) fn time_profile(&self, source: &str) -> Result<Option<SourceTimeProfile>, Error> {
        time_profile(&super::store_counters::Reader::db(&self.db), source)
    }
}

/// Check a record's captured event time and diagnostics against its source's immutable
/// bindings, read from `reader` (the database under the writer, or a reader's snapshot).
pub(super) fn validate_event_record(
    reader: &super::store_counters::Reader<'_>,
    record: &Record,
) -> Result<(), Error> {
    super::diagnostic::validate_diagnostic_record(reader, record)?;
    if let Some(snapshot) = &record.syslog {
        return snapshot.validate_record(record);
    }
    let profile_id = match &record.event_time {
        EventTime::Known { profile_id, .. } => Some(profile_id),
        EventTime::Unknown { profile_id, .. } => profile_id.as_ref(),
    };
    if let Some(id) = profile_id {
        let profile = time_profile(reader, &record.source)?
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "record source time profile missing"))?;
        if profile.id != *id {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "record source time profile disagrees with immutable binding",
            ));
        }
    }
    Ok(())
}

/// A source's bound time profile, read from `reader`.
pub(super) fn time_profile(
    reader: &super::store_counters::Reader<'_>,
    source: &str,
) -> Result<Option<SourceTimeProfile>, Error> {
    let Some(bytes) = reader.get(source_key(source).as_bytes())? else {
        return Ok(None);
    };
    let profile = SourceTimeProfile::decode(&parse(&bytes)?)?;
    if profile.log_capture.is_some()
        && !super::writer_format::has_log_time(
            &reader
                .get(b"store_format")?
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "writer format missing"))?,
        )
    {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "log time mapping lacks its writer fence",
        ));
    }
    if profile.source != source {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "source binding identity mismatch",
        ));
    }
    Ok(Some(profile))
}

impl Store {
    /// Permanently and durably bind this source timestamp interpretation.
    /// Identical retries are idempotent; conflicting mappings cannot reinterpret history.
    pub fn define_time_profile(
        &self,
        source: &str,
        path: &str,
        representation: TimeRepresentation,
    ) -> Result<SourceTimeProfile, Error> {
        self.bind_time_profile(SourceTimeProfile::new(source, path, representation)?)
    }

    /// Bind an explicit native whole-record log timestamp capture durably.
    pub fn define_log_time_profile(
        &self,
        source: &str,
        capture: &super::log_time::LogTimeCapture,
        representation: TimeRepresentation,
    ) -> Result<SourceTimeProfile, Error> {
        capture.compiled()?;
        self.bind_time_profile(SourceTimeProfile::new_log(source, capture, representation)?)
    }

    fn bind_time_profile(&self, profile: SourceTimeProfile) -> Result<SourceTimeProfile, Error> {
        match self.lock()?.bind_time_profile(profile)? {
            Some(bound) => Ok(bound),
            None => Err(Admission::full_refusal()),
        }
    }
    /// Read a source declared immutable mapping, or explicit absence.
    pub fn time_profile(&self, source: &str) -> Result<Option<SourceTimeProfile>, Error> {
        if source.is_empty() || source.len() > 512 {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "source must be 1..512 bytes",
            ));
        }
        self.lock()?.time_profile(source)
    }
}

pub(super) fn capture(
    profile: Option<&SourceTimeProfile>,
    facts: Option<&Value>,
    input: (&str, Option<&[u8]>),
    byte_start: u64,
    byte_end: u64,
) -> Result<EventTime, Error> {
    let Some(profile) = profile else {
        return Ok(EventTime::unknown("no_profile", None));
    };
    if let Some(capture) = &profile.log_capture {
        return if input.0 == "log" {
            capture.capture(profile, input.1, byte_start, byte_end)
        } else {
            Ok(EventTime::unknown("log_wrong_format", Some(profile)))
        };
    }
    let Some(facts) = facts else {
        return Ok(EventTime::unknown("not_json", Some(profile)));
    };
    match read_field::<String>(facts, "state")?.as_str() {
        "complete" => {}
        "failed" => return Ok(EventTime::unknown("projection_failed", Some(profile))),
        _ => {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "unknown native time projection state",
            ));
        }
    }
    let values = field(facts, "values")?
        .as_array()
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "native time projection values missing"))?;
    let value = match super::diagnostic::mapped_occurrence(values, &profile.path)? {
        super::diagnostic::MappedOccurrence::Missing => {
            return Ok(EventTime::unknown("missing_field", Some(profile)));
        }
        super::diagnostic::MappedOccurrence::Ambiguous => {
            return Ok(EventTime::unknown("ambiguous_field", Some(profile)));
        }
        super::diagnostic::MappedOccurrence::Value(value) => value,
    };
    let kind = read_field::<String>(value, "kind")?;
    if kind
        != match profile.representation {
            TimeRepresentation::Rfc3339 => "string",
            _ => "number",
        }
    {
        return Ok(EventTime::unknown("wrong_kind", Some(profile)));
    }
    let text = match profile.representation {
        TimeRepresentation::Rfc3339 => match value.get("decoded") {
            Some(value) => Some(value.as_str().ok_or_else(|| {
                Error::new("STORAGE_FAILURE", "decoded timestamp must be a string")
            })?),
            None if value.get("decoding_error").is_some() => None,
            None => {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "native timestamp string lacks decoded evidence",
                ));
            }
        },
        _ => Some(
            value
                .get("lexical")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    Error::new(
                        "STORAGE_FAILURE",
                        "native timestamp number lacks lexical evidence",
                    )
                })?,
        ),
    };
    let parsed = text
        .map(|text| interpret(text, profile.representation))
        .transpose()?
        .flatten();
    let Some(millis) = parsed else {
        return Ok(EventTime::unknown("invalid_instant", Some(profile)));
    };
    known(
        profile,
        millis,
        integer(value, "occurrence")?,
        byte_start..byte_end,
        integer(value, "start")?..integer(value, "end")?,
    )
}

pub(super) fn interpret(
    text: &str,
    representation: TimeRepresentation,
) -> Result<Option<u64>, Error> {
    let parsed = match representation {
        TimeRepresentation::Rfc3339 => match crate::parse_instant(text) {
            Ok(millis) => Some(millis),
            Err(error) if error.code == "RESOURCE_LIMIT" => return Err(error),
            Err(_) => None,
        },
        representation => match purrdf::xsd::numeric::parse_integer(text) {
            Ok(value) => Some(value),
            Err(purrdf::xsd::XsdError::Storage(error)) => {
                return Err(Error::new("RESOURCE_LIMIT", error.to_string()));
            }
            Err(_) => None,
        }
        .and_then(|value| value.to_i128().ok())
        .and_then(|value| u64::try_from(value).ok())
        .and_then(|value| {
            if representation == TimeRepresentation::UnixSeconds {
                value.checked_mul(1000)
            } else {
                Some(value)
            }
        }),
    };
    Ok(parsed.filter(|millis| crate::format_instant(*millis).is_ok()))
}

pub(super) fn known(
    profile: &SourceTimeProfile,
    millis: u64,
    occurrence: u64,
    record: std::ops::Range<u64>,
    range: std::ops::Range<u64>,
) -> Result<EventTime, Error> {
    let source_start = record
        .start
        .checked_add(range.start)
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "source time span overflow"))?;
    let source_end = record
        .start
        .checked_add(range.end)
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "source time span overflow"))?;
    if source_start > source_end || source_end > record.end {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "source time evidence outside record",
        ));
    }
    Ok(EventTime::Known {
        millis,
        profile_id: profile.id.clone(),
        occurrence,
        source_start,
        source_end,
    })
}
