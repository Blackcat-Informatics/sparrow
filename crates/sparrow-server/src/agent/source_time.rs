// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Typed source mapping operations and one event-evidence renderer.

use super::{Result, failure, instant, optional_value, wire};
use purrdf_lex::json::{
    Object, Value,
    record::{DecodeError, Record},
};

pub(crate) struct Input {
    pub source: String,
    pub definition: Option<Definition>,
}

pub(crate) enum Definition {
    Json(String, wire::TimeRepresentation),
    Log(sparrow_core::LogTimeCapture, wire::TimeRepresentation),
}

pub(super) fn definition(fields: &mut Record<'_>) -> std::result::Result<Definition, DecodeError> {
    let repr = representation(&fields.required::<String>("representation")?)?;
    let path: Option<String> = fields.optional("path")?;
    let log = fields.raw("log_capture")?;
    match (path, log) {
        (Some(path), None) => Ok(Definition::Json(path, repr)),
        (None, Some(log)) => {
            let mut input = Record::new(log, "log timestamp capture")?;
            let pattern = input.required_with("pattern", |value| {
                let value = value
                    .as_str()
                    .ok_or_else(|| DecodeError::custom("log pattern requires a string"))?;
                if value.len() > 4096 {
                    return Err(DecodeError::custom("log pattern exceeds 4096 bytes"));
                }
                Ok(value.to_owned())
            })?;
            let group: u32 = input.required("group")?;
            let flags = match input.raw("flags")? {
                Some(value) => {
                    let value = value
                        .as_str()
                        .ok_or_else(|| DecodeError::custom("log flags require a string"))?;
                    if value.len() > 2 {
                        return Err(DecodeError::custom("log flags exceed two bytes"));
                    }
                    value.to_owned()
                }
                None => String::new(),
            };
            input.deny_unknown()?;
            Ok(Definition::Log(
                sparrow_core::LogTimeCapture {
                    pattern,
                    group,
                    flags,
                },
                repr,
            ))
        }
        _ => Err(DecodeError::custom(
            "define exactly one JSON path or log_capture",
        )),
    }
}

pub(super) fn representation(
    value: &str,
) -> std::result::Result<wire::TimeRepresentation, DecodeError> {
    match value {
        "rfc3339" => Ok(wire::TimeRepresentation::Rfc3339),
        "unix_millis" => Ok(wire::TimeRepresentation::UnixMillis),
        "unix_seconds" => Ok(wire::TimeRepresentation::UnixSeconds),
        _ => Err(DecodeError::unknown_variant(
            value,
            &["rfc3339", "unix_millis", "unix_seconds"],
        )),
    }
}

fn profile(value: wire::source_time_profile::Reader<'_>) -> Result<Value> {
    let extraction: Value = match value.get_extraction()?.which()? {
        wire::time_extraction::JsonPointer(()) => Object::new()
            .with("kind", "json_pointer")
            .with("path", value.get_path()?.to_str()?)
            .into(),
        wire::time_extraction::LogCapture(value) => crate::time_wire::capture(value?)?.descriptor(),
    };
    Ok(Object::new()
        .with("store_id", value.get_store_id()?.to_str()?)
        .with("profile_id", value.get_id()?.to_str()?)
        .with("source", value.get_source()?.to_str()?)
        .with("path", value.get_path()?.to_str()?)
        .with("extraction", extraction)
        .with(
            "representation",
            match value.get_representation()? {
                wire::TimeRepresentation::Rfc3339 => "rfc3339",
                wire::TimeRepresentation::UnixMillis => "unix_millis",
                wire::TimeRepresentation::UnixSeconds => "unix_seconds",
            },
        )
        .with(
            "durability",
            match value.get_durability()? {
                wire::Durability::Durable => "durable",
                wire::Durability::Buffered => "buffered",
            },
        )
        .into())
}

impl Input {
    pub(super) fn decode(value: &Value) -> Result<Self> {
        let decode = || -> std::result::Result<Self, DecodeError> {
            let mut fields = Record::new(value, "source time definition")?;
            let result = Self {
                source: fields.required("source")?,
                definition: Some(definition(&mut fields)?),
            };
            fields.deny_unknown()?;
            Ok(result)
        };
        decode().map_err(|error| super::invalid(error.to_string()))
    }

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        sparrow_client::Discovery::fetch(client).await?;
        if let Some(definition) = &self.definition {
            let mut call = client.source_time_define_request();
            let mut input = call.get().init_definition();
            input.set_source(&self.source);
            input.set_schema_version(sparrow_client::SCHEMA_VERSION);
            match definition {
                Definition::Json(path, representation) => {
                    input.set_path(path);
                    input.set_representation(*representation);
                }
                Definition::Log(capture, representation) => {
                    crate::time_wire::encode(
                        input.reborrow().init_extraction().init_log_capture(),
                        capture,
                    );
                    input.set_representation(*representation);
                }
            }
            let response = call.send().promise.await?;
            return match response.get()?.get_result()?.which()? {
                wire::result::Ok(value) => profile(value?),
                wire::result::Error(error) => Err(failure(error?)),
            };
        }
        let mut call = client.source_time_get_request();
        call.get().set_source(&self.source);
        call.get()
            .set_schema_version(sparrow_client::SCHEMA_VERSION);
        let response = call.send().promise.await?;
        let found = match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => value?,
            wire::result::Error(error) => return Err(failure(error?)),
        };
        let value = match found.which()? {
            wire::optional_source_time_profile::Absent(()) => Value::Null,
            wire::optional_source_time_profile::Value(value) => profile(value?)?,
        };
        Ok(Object::new().with("profile", value).into())
    }
}

pub(super) fn event(value: wire::event_time::Reader<'_>) -> Result<Value> {
    match value.which()? {
        wire::event_time::Known(value) => {
            let value = value?;
            Ok(Object::new()
                .with("state", "known")
                .with("instant", instant(value.get_instant()?)?)
                .with("profile_id", value.get_profile_id()?.to_str()?)
                .with("occurrence", value.get_occurrence().to_string())
                .with("source_start", value.get_source_start().to_string())
                .with("source_end", value.get_source_end().to_string())
                .into())
        }
        wire::event_time::Unknown(value) => {
            let value = value?;
            Ok(Object::new()
                .with("state", "unknown")
                .with("profile_id", optional_value(value.get_profile_id()?)?)
                .with(
                    "reason",
                    match value.get_reason()? {
                        wire::UnknownTimeReason::LegacyUnprofiled => "legacy_unprofiled",
                        wire::UnknownTimeReason::NoProfile => "no_profile",
                        wire::UnknownTimeReason::NotJson => "not_json",
                        wire::UnknownTimeReason::ProjectionFailed => "projection_failed",
                        wire::UnknownTimeReason::MissingField => "missing_field",
                        wire::UnknownTimeReason::AmbiguousField => "ambiguous_field",
                        wire::UnknownTimeReason::WrongKind => "wrong_kind",
                        wire::UnknownTimeReason::InvalidInstant => "invalid_instant",
                        wire::UnknownTimeReason::SyslogMalformed => "syslog_malformed",
                        wire::UnknownTimeReason::SyslogLegacyTime => "syslog_legacy_time",
                        wire::UnknownTimeReason::SyslogNilTime => "syslog_nil_time",
                        wire::UnknownTimeReason::SyslogSubmillisecond => "syslog_submillisecond",
                        wire::UnknownTimeReason::LogInputUnavailable => "log_input_unavailable",
                        wire::UnknownTimeReason::LogInvalidUtf8 => "log_invalid_utf8",
                        wire::UnknownTimeReason::LogNoMatch => "log_no_match",
                        wire::UnknownTimeReason::LogCaptureMissing => "log_capture_missing",
                        wire::UnknownTimeReason::LogWrongFormat => "log_wrong_format",
                    },
                )
                .into())
        }
    }
}
