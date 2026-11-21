// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use super::Console;
use crate::{
    EventKind, EventLevel, FieldValue, ProducerEvent,
    logging::{Fields, bounded_text, render},
};
use log::kv::{self, VisitSource, VisitValue};

// Use the native log visitor so a u64 doesn't become text and an i128 doesn't narrow.
struct Pair<'a> {
    name: &'a str,
    fields: &'a mut Fields,
}

fn invalid_field() -> kv::Error {
    kv::Error::msg("Sparrow log field exceeds its capture bound")
}

impl<'v> VisitValue<'v> for Pair<'_> {
    fn visit_any(&mut self, v: kv::Value<'_>) -> Result<(), kv::Error> {
        let text = render(format_args!("{v:?}")).map_err(|()| invalid_field())?;
        self.fields.push(self.name, FieldValue::Debug(text));
        Ok(())
    }
    fn visit_str(&mut self, v: &str) -> Result<(), kv::Error> {
        self.fields.push(
            self.name,
            FieldValue::Text(bounded_text(v).map_err(|()| invalid_field())?),
        );
        Ok(())
    }
    fn visit_i64(&mut self, v: i64) -> Result<(), kv::Error> {
        self.fields.push(self.name, FieldValue::Signed(v));
        Ok(())
    }
    fn visit_u64(&mut self, v: u64) -> Result<(), kv::Error> {
        self.fields.push(self.name, FieldValue::Unsigned(v));
        Ok(())
    }
    fn visit_i128(&mut self, v: i128) -> Result<(), kv::Error> {
        self.fields.push(self.name, FieldValue::Signed128(v));
        Ok(())
    }
    fn visit_u128(&mut self, v: u128) -> Result<(), kv::Error> {
        self.fields.push(self.name, FieldValue::Unsigned128(v));
        Ok(())
    }
    fn visit_f64(&mut self, v: f64) -> Result<(), kv::Error> {
        self.fields
            .push(self.name, FieldValue::Float64Bits(v.to_bits()));
        Ok(())
    }
    fn visit_bool(&mut self, v: bool) -> Result<(), kv::Error> {
        self.fields.push(self.name, FieldValue::Boolean(v));
        Ok(())
    }
}

impl<'v> VisitSource<'v> for Fields {
    fn visit_pair(&mut self, key: kv::Key<'v>, value: kv::Value<'v>) -> Result<(), kv::Error> {
        value.visit(Pair {
            name: key.as_str(),
            fields: self,
        })?;
        if self.failed() {
            Err(invalid_field())
        } else {
            Ok(())
        }
    }
}

impl Console {
    fn capture_record(&self, record: &log::Record<'_>) -> Result<ProducerEvent, ()> {
        let mut fields = Fields::default();
        fields.push("message", FieldValue::Text(render(*record.args())?));
        record.key_values().visit(&mut fields).map_err(|_| ())?;
        let event = ProducerEvent {
            sequence: 0,
            event_time: crate::unix_millis_now().map_err(|_| ())?,
            kind: EventKind::Log,
            level: match record.level() {
                log::Level::Error => EventLevel::Error,
                log::Level::Warn => EventLevel::Warn,
                log::Level::Info => EventLevel::Info,
                log::Level::Debug => EventLevel::Debug,
                log::Level::Trace => EventLevel::Trace,
            },
            name: "log".into(),
            target: bounded_text(record.target())?,
            fields: fields.finish()?,
            context: Vec::new(),
            span: None,
            follows_from: None,
        };
        crate::logging::in_current_span(event, &self.slots)
    }
}

/// Use `log::set_boxed_logger(Box::new(console.clone()))` and set the desired
/// `log::set_max_level` in an application that owns the process logger.
impl log::Log for Console {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        match self.capture_record(record) {
            Ok(event) => self.emit(event),
            Err(()) => self.capture_failed(),
        }
    }

    fn flush(&self) {
        // The synchronous facade has no result channel. A deliberate flush may wait for a
        // slow console even inside a Tokio task, so the async API is preferable there.
        let _ = self.flush_sync(false);
    }
}
