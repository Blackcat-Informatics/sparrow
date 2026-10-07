// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Immutable source mappings and native timestamp evidence captured on ingestion.

use super::{Error, Record, Store, Writer, blake3, field, hex, integer, json, parse, read_field};
use purrdf_lex::json::{Object, Value, record::DecodeError};
use rocksdb::WriteBatch;

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
    /// Exact concrete native RFC 6901 pointer.
    pub path: String,
    /// Declared units/representation.
    pub representation: TimeRepresentation,
}

impl SourceTimeProfile {
    fn new(source: &str, path: &str, representation: TimeRepresentation) -> Result<Self, Error> {
        if source.is_empty()
            || source.len() > 512
            || path.len() > 4096
            || (!path.is_empty() && !path.starts_with('/'))
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "require source 1..512 bytes and a concrete JSON pointer of at most 4096 bytes",
            ));
        }
        if path
            .split('/')
            .any(|token| purrdf_lex::json_pointer::unescape_token(token).is_none())
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "invalid native JSON pointer escape",
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
        })
    }
    pub(crate) fn encode(&self) -> Value {
        Object::new()
            .with("id", self.id.clone())
            .with("source", self.source.clone())
            .with("path", self.path.clone())
            .with("representation", self.representation.name())
            .into()
    }
    fn decode(value: &Value) -> Result<Self, Error> {
        let source = read_field::<String>(value, "source")?;
        let path = read_field::<String>(value, "path")?;
        let representation =
            TimeRepresentation::decode(&read_field::<String>(value, "representation")?)?;
        let profile = Self::new(&source, &path, representation)
            .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
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
        /// Native ordered JSON occurrence identity.
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

fn source_key(source: &str) -> String {
    format!(
        "source-time:{}",
        hex::encode(blake3::hash(source.as_bytes()).as_bytes())
    )
}

impl Writer {
    pub(super) fn validate_event_record(&self, record: &Record) -> Result<(), Error> {
        let profile_id = match &record.event_time {
            EventTime::Known { profile_id, .. } => Some(profile_id),
            EventTime::Unknown { profile_id, .. } => profile_id.as_ref(),
        };
        if let Some(id) = profile_id {
            let profile = self.time_profile(&record.source)?.ok_or_else(|| {
                Error::new("STORAGE_FAILURE", "record source time profile missing")
            })?;
            if profile.id != *id {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "record source time profile disagrees with immutable binding",
                ));
            }
        }
        Ok(())
    }
    pub(super) fn time_profile(&self, source: &str) -> Result<Option<SourceTimeProfile>, Error> {
        let Some(bytes) = self.db.get(source_key(source))? else {
            return Ok(None);
        };
        let profile = SourceTimeProfile::decode(&parse(&bytes)?)?;
        if profile.source != source {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "source binding identity mismatch",
            ));
        }
        Ok(Some(profile))
    }
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
        let profile = SourceTimeProfile::new(source, path, representation)?;
        let writer = self.lock()?;
        if let Some(previous) = writer.time_profile(source)? {
            if previous != profile {
                return Err(Error::new(
                    "CONFLICT",
                    "source time mapping is immutable; use a new logical source",
                ));
            }
            return Ok(previous);
        }
        for (count, entry) in writer.db.prefix_iterator("source-time:").enumerate() {
            let (key, bytes) = entry?;
            if !key.starts_with(b"source-time:") {
                break;
            }
            if count == 1023 {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "source time registry admits at most 1024 immutable mappings",
                ));
            }
            let retained = SourceTimeProfile::decode(&parse(&bytes)?)?;
            if key.as_ref() != source_key(&retained.source).as_bytes() {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "source time registry key mismatch",
                ));
            }
        }
        let mut batch = WriteBatch::default();
        let encoded = json::write_compact(&profile.encode());
        if writer
            .physical_bytes()?
            .saturating_add(encoded.len() as u64 * 4 + 65536)
            > writer.config.disk_budget
        {
            return Err(Error::new(
                "STORAGE_PRESSURE",
                "source time mapping exceeds physical control admission budget",
            ));
        }
        batch.put(source_key(source), encoded);
        writer.commit(batch)?;
        Ok(profile)
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
    byte_start: u64,
    byte_end: u64,
) -> Result<EventTime, Error> {
    let Some(profile) = profile else {
        return Ok(EventTime::unknown("no_profile", None));
    };
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
    let mut selected = None;
    for value in values {
        if read_field::<String>(value, "path")? == profile.path {
            if selected.is_some() {
                return Ok(EventTime::unknown("ambiguous_field", Some(profile)));
            }
            selected = Some(value);
        }
    }
    let Some(value) = selected else {
        return Ok(EventTime::unknown("missing_field", Some(profile)));
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
    let parsed = match profile.representation {
        TimeRepresentation::Rfc3339 => match value.get("decoded") {
            Some(value) => Some(value.as_str().ok_or_else(|| {
                Error::new("STORAGE_FAILURE", "decoded timestamp must be a string")
            })?)
            .and_then(|text| crate::parse_instant(text).ok()),
            None if value.get("decoding_error").is_some() => None,
            None => {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "native timestamp string lacks decoded evidence",
                ));
            }
        },
        representation => {
            let lexical = read_field::<String>(value, "lexical")?;
            purrdf::xsd::numeric::parse_integer(&lexical)
                .ok()
                .and_then(|value| u64::try_from(value).ok())
                .and_then(|value| {
                    if representation == TimeRepresentation::UnixSeconds {
                        value.checked_mul(1000)
                    } else {
                        Some(value)
                    }
                })
        }
    };
    let Some(millis) = parsed.filter(|millis| crate::format_instant(*millis).is_ok()) else {
        return Ok(EventTime::unknown("invalid_instant", Some(profile)));
    };
    let source_start = byte_start
        .checked_add(integer(value, "start")?)
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "source time span overflow"))?;
    let source_end = byte_start
        .checked_add(integer(value, "end")?)
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "source time span overflow"))?;
    if source_start > source_end || source_end > byte_end {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "source time evidence outside record",
        ));
    }
    Ok(EventTime::Known {
        millis,
        profile_id: profile.id.clone(),
        occurrence: integer(value, "occurrence")?,
        source_start,
        source_end,
    })
}
