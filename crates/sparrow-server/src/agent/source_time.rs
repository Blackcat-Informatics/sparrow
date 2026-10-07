// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Typed source mapping operations and one event-evidence renderer.

use super::{Result, failure, instant, optional_value, wire};
use purrdf_lex::json::{Object, Value, record::DecodeError};

pub(crate) struct Input {
    pub source: String,
    pub definition: Option<(String, wire::TimeRepresentation)>,
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
    Ok(Object::new()
        .with("store_id", value.get_store_id()?.to_str()?)
        .with("profile_id", value.get_id()?.to_str()?)
        .with("source", value.get_source()?.to_str()?)
        .with("path", value.get_path()?.to_str()?)
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
    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        if let Some((path, representation)) = &self.definition {
            let mut call = client.source_time_define_request();
            let mut input = call.get().init_definition();
            input.set_source(&self.source);
            input.set_path(path);
            input.set_representation(*representation);
            let response = call.send().promise.await?;
            return match response.get()?.get_result()?.which()? {
                wire::result::Ok(value) => profile(value?),
                wire::result::Error(error) => Err(failure(error?)),
            };
        }
        let mut call = client.source_time_get_request();
        call.get().set_source(&self.source);
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
                    },
                )
                .into())
        }
    }
}
