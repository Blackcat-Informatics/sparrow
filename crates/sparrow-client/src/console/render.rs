// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use crate::{EventField, EventLevel, FieldValue, ProducerEvent};
use std::{
    fmt,
    io::{self, Write},
};

struct Visible<'a>(&'a str, bool);
const CONTINUATION: &str = "\n                  │ ";

impl fmt::Display for Visible<'_> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        use fmt::Write as _;
        for ch in self.0.chars() {
            match ch {
                '\n' if self.1 => out.write_str(CONTINUATION)?,
                ch if ch.is_control() => write!(out, "{}", ch.escape_default())?,
                ch => out.write_char(ch)?,
            }
        }
        Ok(())
    }
}

fn value(out: &mut Vec<u8>, field: &FieldValue) -> io::Result<()> {
    match field {
        FieldValue::Text(s) | FieldValue::Debug(s) | FieldValue::Error(s) => {
            write!(out, "{}", Visible(s, false))
        }
        FieldValue::Signed(n) => write!(out, "{n}"),
        FieldValue::Unsigned(n) => write!(out, "{n}"),
        FieldValue::Signed128(n) => write!(out, "{n}"),
        FieldValue::Unsigned128(n) => write!(out, "{n}"),
        FieldValue::Float64Bits(bits) => write!(out, "{}", f64::from_bits(*bits)),
        FieldValue::Boolean(flag) => write!(out, "{flag}"),
        FieldValue::Bytes(bytes) => write!(out, "0x{}", purrdf_hash::hex::encode(bytes)),
    }
}

pub(super) struct Line {
    event_time: u64,
    level: EventLevel,
    target: String,
    name: String,
    fields: Vec<EventField>,
    span: Option<String>,
}

impl Line {
    // The local queue owns only what it prints. Copy no ancestry or span fields, and
    // transfer the event's allocations outright when the console is its only destination.
    pub(super) fn capture(event: &mut ProducerEvent, retain: bool) -> Self {
        let span = event.context.last().map(|span| span.name.clone());
        let (name, target, fields) = if retain {
            (
                event.name.clone(),
                event.target.clone(),
                event.fields.clone(),
            )
        } else {
            (
                std::mem::take(&mut event.name),
                std::mem::take(&mut event.target),
                std::mem::take(&mut event.fields),
            )
        };
        Self {
            event_time: event.event_time,
            level: event.level,
            target,
            name,
            fields,
            span,
        }
    }
}

pub(super) fn line(out: &mut Vec<u8>, event: &Line, color: bool) -> io::Result<()> {
    let sec = event.event_time / 1000;
    let (label, style) = match event.level {
        EventLevel::Trace => ("TRACE", "\x1b[2m"),
        EventLevel::Debug => ("DEBUG", "\x1b[36m"),
        EventLevel::Info => ("INFO", "\x1b[32m"),
        EventLevel::Warn => ("WARN", "\x1b[1;33m"),
        EventLevel::Error => ("ERROR", "\x1b[1;31m"),
    };
    let dim = if color { "\x1b[2m" } else { "" };
    let reset = if color { "\x1b[0m" } else { "" };
    let style = if color { style } else { "" };
    write!(
        out,
        "{dim}{:02}:{:02}:{:02}{reset}  {style}{label:<5}{reset}  {dim}{}{reset}  ",
        sec / 3600 % 24,
        sec / 60 % 60,
        sec % 60,
        Visible(&event.target, false)
    )?;
    let message = event.fields.iter().position(|f| f.name == "message");
    let text = match message.map(|i| &event.fields[i].value) {
        Some(FieldValue::Text(s) | FieldValue::Debug(s) | FieldValue::Error(s)) => Some(s.as_str()),
        None => Some(event.name.as_str()),
        Some(other) => {
            value(out, other)?;
            None
        }
    };
    let mut continuation = None;
    if let Some(text) = text {
        let (first, detail) = text
            .split_once('\n')
            .map_or((text, None), |(a, b)| (a, Some(b)));
        write!(out, "{}", Visible(first, false))?;
        continuation = detail;
    }
    for (idx, field) in event.fields.iter().enumerate() {
        if Some(idx) != message {
            write!(out, "  {dim}{}={reset}", Visible(&field.name, false))?;
            value(out, &field.value)?;
        }
    }
    if let Some(span) = &event.span {
        write!(out, "  {dim}in {}{reset}", Visible(span, false))?;
    }
    if let Some(details) = continuation {
        write!(out, "{CONTINUATION}{}", Visible(details, true))?;
    }
    writeln!(out)
}
