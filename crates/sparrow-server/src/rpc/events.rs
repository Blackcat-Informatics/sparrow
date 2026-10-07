// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Typed event normalization through the native JSON writer and shared record owner.

use purrdf_lex::json::{self, Number, Object, Value};
use sparrow_client::{EventBatch, EventField, EventKind, EventLevel, EventSpan, FieldValue};
use sparrow_core::Error;

pub(super) fn source_bytes(batch: &EventBatch) -> Result<Vec<u8>, Error> {
    batch.validate().map_err(super::invalid)?;
    let instance = purrdf_hash::hex::encode(&batch.client_instance);
    let mut bytes = String::new();
    for event in &batch.events {
        let fields = Value::array(event.fields.iter().map(field));
        let context = Value::array(event.context.iter().map(span));
        let value = Object::new()
            .with("profile", "sparrow.events/1")
            .with("client_instance", instance.clone())
            .with("sequence", event.sequence)
            .with("event_time", event.event_time)
            .with(
                "kind",
                match event.kind {
                    EventKind::Log => "log",
                    EventKind::SpanNew => "span_new",
                    EventKind::SpanRecord => "span_record",
                    EventKind::SpanEnter => "span_enter",
                    EventKind::SpanExit => "span_exit",
                    EventKind::SpanClose => "span_close",
                    EventKind::SpanFollowsFrom => "span_follows_from",
                },
            )
            .with(
                "level",
                match event.level {
                    EventLevel::Error => "error",
                    EventLevel::Warn => "warn",
                    EventLevel::Info => "info",
                    EventLevel::Debug => "debug",
                    EventLevel::Trace => "trace",
                },
            )
            .with("name", event.name.clone())
            .with("target", event.target.clone())
            .with("fields", fields)
            .with("context", context)
            .with("span", event.span.map_or(Value::Null, Value::from))
            .with(
                "follows_from",
                event.follows_from.map_or(Value::Null, Value::from),
            );
        json::write_into(&mut bytes, &value.into(), json::Format::COMPACT);
        bytes.push('\n');
        if bytes.len() > sparrow_client::MAX_EVENT_BATCH_BYTES {
            return Err(Error {
                code: "RESOURCE_LIMIT",
                message: "event batch expansion exceeded its admitted JSONL bound".into(),
            });
        }
    }
    Ok(bytes.into_bytes())
}

fn span(span: &EventSpan) -> Value {
    Object::new()
        .with("id", span.id)
        .with("parent", span.parent.map_or(Value::Null, Value::from))
        .with("name", span.name.clone())
        .with("target", span.target.clone())
        .with("fields", Value::array(span.fields.iter().map(field)))
        .into()
}

fn field(field: &EventField) -> Value {
    let (kind, value) = match &field.value {
        FieldValue::Text(value) => ("text", Value::from(value.clone())),
        FieldValue::Boolean(value) => ("bool", Value::from(*value)),
        FieldValue::Signed(value) => ("i64", Value::from(*value)),
        FieldValue::Unsigned(value) => ("u64", Value::from(*value)),
        FieldValue::Signed128(value) => ("i128", Value::from(*value)),
        FieldValue::Unsigned128(value) => ("u128", Value::from(*value)),
        FieldValue::Bytes(value) => ("bytes_hex", Value::from(purrdf_hash::hex::encode(value))),
        FieldValue::Debug(value) => ("debug", Value::from(value.clone())),
        FieldValue::Error(value) => ("error", Value::from(value.clone())),
        FieldValue::Float64Bits(bits) => {
            let number = f64::from_bits(*bits);
            let mut out = Object::new()
                .with("name", field.name.clone())
                .with("kind", "f64")
                .with("bits", *bits);
            // Nonfinite values are explicit; native Value::from(f64) would map them to null.
            out = match Number::from_f64(number) {
                Some(value) => out.with("value", Value::Number(value)),
                None => out.with(
                    "nonfinite",
                    if number.is_nan() {
                        "nan"
                    } else if number.is_sign_negative() {
                        "negative_infinity"
                    } else {
                        "positive_infinity"
                    },
                ),
            };
            return out.into();
        }
    };
    Object::new()
        .with("name", field.name.clone())
        .with("kind", kind)
        .with("value", value)
        .into()
}
