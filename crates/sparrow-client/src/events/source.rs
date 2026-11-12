// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Typed event normalization through the native JSON writer and shared record owner.

use crate::Error;
use crate::{EventBatch, EventField, EventKind, EventLevel, EventSpan, FieldValue};
use purrdf_lex::json::{self, Number, Object, Value};

impl EventBatch {
    /// Encode the canonical bounded source bytes once for daemon publication and producer receipt verification.
    pub fn source_bytes(&self) -> Result<Vec<u8>, Error> {
        let batch = self;
        batch.validate()?;
        let instance = purrdf_hash::hex::encode(&batch.client_instance);
        let mut bytes = String::new();
        for event in &batch.events {
            let value = event_value(&instance, event);
            json::write_into(&mut bytes, &value, json::Format::COMPACT);
            bytes.push('\n');
            if bytes.len() > crate::MAX_EVENT_BATCH_BYTES {
                return Err(Error::InvalidArgument(
                    "event batch expansion exceeded its admitted JSONL bound",
                ));
            }
        }
        Ok(bytes.into_bytes())
    }

    /// The same canonical event lines as [`Self::source_bytes`] without its expansion bound,
    /// so a quarantined batch is always written whole, whatever refused it.
    pub(crate) fn quarantine_bytes(&self) -> Vec<u8> {
        let instance = purrdf_hash::hex::encode(&self.client_instance);
        let mut bytes = String::new();
        for event in &self.events {
            json::write_into(
                &mut bytes,
                &event_value(&instance, event),
                json::Format::COMPACT,
            );
            bytes.push('\n');
        }
        bytes.into_bytes()
    }
}

fn event_value(instance: &str, event: &crate::ProducerEvent) -> Value {
    let fields = Value::array(event.fields.iter().map(field));
    let context = Value::array(event.context.iter().map(span));
    let value = Object::new()
        .with("profile", "sparrow.events/1")
        .with("client_instance", instance.to_owned())
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
    value.into()
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
