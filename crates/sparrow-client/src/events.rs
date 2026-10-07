// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Shared bounded producer event types and generated wire adapters.

use crate::{Error, EventKind, EventLevel, sparrow_capnp as wire};

/// Maximum conservative JSONL expansion for one typed batch.
pub const MAX_EVENT_BATCH_BYTES: usize = 1024 * 1024;
/// Maximum independent occurrences in one atomic batch.
pub const MAX_EVENT_BATCH_EVENTS: usize = 256;
/// Maximum conservative expansion for one event, including its span context.
pub const MAX_EVENT_BYTES: usize = 64 * 1024;
/// Maximum fields on an event or one span snapshot.
pub const MAX_EVENT_FIELDS: usize = 128;
/// Maximum captured ancestors/current spans on an event.
pub const MAX_EVENT_SPANS: usize = 32;

/// A captured Rust field type. Float bits retain NaN payloads and signed zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldValue {
    /// UTF-8 text recorded through the string visitor.
    Text(String),
    /// A native boolean.
    Boolean(bool),
    /// A signed 64-bit integer.
    Signed(i64),
    /// An unsigned 64-bit integer.
    Unsigned(u64),
    /// A signed 128-bit integer.
    Signed128(i128),
    /// An unsigned 128-bit integer.
    Unsigned128(u128),
    /// Exact IEEE 754 binary64 bits, including nonfinite values.
    Float64Bits(u64),
    /// Exact binary field content.
    Bytes(Vec<u8>),
    /// Explicit debug-rendered content, never presented as a native string field.
    Debug(String),
    /// Explicit error-rendered content.
    Error(String),
}

/// One ordered field; repeated names remain independent occurrences.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventField {
    /// Original field name.
    pub name: String,
    /// Captured typed value.
    pub value: FieldValue,
}

/// One span snapshot, scoped by the batch's producer instance identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSpan {
    /// Nonzero span identity within this client instance.
    pub id: u64,
    /// Declared parent span identity, when present.
    pub parent: Option<u64>,
    /// Original metadata name.
    pub name: String,
    /// Original metadata target.
    pub target: String,
    /// Ordered typed fields captured for this span.
    pub fields: Vec<EventField>,
}

/// An event or span lifecycle observation, independent of the server receipt clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerEvent {
    /// Nonzero monotonic client sequence. Gaps may denote producer-side drops.
    pub sequence: u64,
    /// Producer UTC event time in Unix milliseconds.
    pub event_time: u64,
    /// Observation kind.
    pub kind: EventKind,
    /// Original severity.
    pub level: EventLevel,
    /// Original metadata name.
    pub name: String,
    /// Original metadata target.
    pub target: String,
    /// Ordered event/update fields.
    pub fields: Vec<EventField>,
    /// Declared root-to-current span context, captured at this observation.
    pub context: Vec<EventSpan>,
    /// Subject span for a lifecycle observation or current span for a log.
    pub span: Option<u64>,
    /// Antecedent of a follows-from observation.
    pub follows_from: Option<u64>,
}

/// An immutable bounded delivery unit. Keep this exact value across retries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventBatch {
    /// Stable server retry identity.
    pub request_id: String,
    /// Logical source; its time profile must use `/event_time` in Unix milliseconds.
    pub source: String,
    /// Fresh producer-process instance identity, stable across reconnects.
    pub client_instance: [u8; 16],
    /// Independent observations in strictly increasing sequence order.
    pub events: Vec<ProducerEvent>,
}

struct Budget {
    left: usize,
    limit: usize,
}
impl Budget {
    fn new(limit: usize) -> Self {
        Self { left: limit, limit }
    }
    fn charge(&mut self, amount: usize) -> Result<(), Error> {
        self.left = self.left.checked_sub(amount).ok_or(Error::InvalidArgument(
            "typed event expansion exceeds its bounded byte quota",
        ))?;
        Ok(())
    }
    fn text(&mut self, text: &str) -> Result<(), Error> {
        if text.len() > 16 * 1024 {
            return Err(Error::InvalidArgument("event text exceeds 16 KiB"));
        }
        self.charge(text.len() * 6)
    }
    fn read_text(&mut self, text: capnp::text::Reader<'_>) -> Result<String, Error> {
        let text = text.to_str()?;
        self.text(text)?;
        Ok(text.to_owned())
    }
    fn fields(&mut self, count: usize) -> Result<(), Error> {
        if count > MAX_EVENT_FIELDS {
            return Err(Error::InvalidArgument("event/span fields exceed 128"));
        }
        self.charge(count * 128)
    }
    fn binary(&mut self, count: usize) -> Result<(), Error> {
        self.charge(
            count
                .checked_mul(2)
                .ok_or(Error::InvalidArgument("binary event field length overflow"))?,
        )
    }
    fn context(&mut self, count: usize) -> Result<(), Error> {
        if count > MAX_EVENT_SPANS {
            return Err(Error::InvalidArgument("event context exceeds 32 spans"));
        }
        self.charge(count * 512)
    }
    fn used(&self) -> usize {
        self.limit - self.left
    }
}

impl EventBatch {
    /// Validate labels, ordering, typed relationships, and conservative byte quotas.
    pub fn validate(&self) -> Result<(), Error> {
        if self.request_id.is_empty()
            || self.request_id.len() > 128
            || self.source.is_empty()
            || self.source.len() > 512
        {
            return Err(Error::InvalidArgument(
                "require bounded nonempty request/source labels",
            ));
        }
        check_count(self.events.len())?;
        let mut batch = Budget::new(MAX_EVENT_BATCH_BYTES);
        batch.charge(1024)?;
        batch.text(&self.request_id)?;
        batch.text(&self.source)?;
        let mut previous = 0;
        for event in &self.events {
            if event.sequence <= previous {
                return Err(Error::InvalidArgument(
                    "event sequences must be nonzero and strictly increasing",
                ));
            }
            previous = event.sequence;
            batch.charge(event.admission_bytes()?)?;
        }
        Ok(())
    }

    /// Encode the generated typed contract after shared admission validation.
    pub fn encode(&self, mut out: wire::event_batch::Builder<'_>) -> Result<(), Error> {
        self.validate()?;
        out.set_request_id(&self.request_id);
        out.set_source(&self.source);
        out.set_client_instance(&self.client_instance);
        let mut events = out.init_events(self.events.len() as u32);
        for (index, event) in self.events.iter().enumerate() {
            let mut out = events.reborrow().get(index as u32);
            out.set_sequence(event.sequence);
            out.reborrow()
                .init_event_time()
                .set_unix_millis(event.event_time);
            out.set_kind(event.kind);
            out.set_level(event.level);
            out.set_name(&event.name);
            out.set_target(&event.target);
            encode_fields(
                &event.fields,
                out.reborrow().init_fields(event.fields.len() as u32),
            );
            optional_span(out.reborrow().init_span(), event.span);
            optional_span(out.reborrow().init_follows_from(), event.follows_from);
            let mut context = out.init_context(event.context.len() as u32);
            for (index, span) in event.context.iter().enumerate() {
                let mut out = context.reborrow().get(index as u32);
                out.set_id(span.id);
                optional_span(out.reborrow().init_parent(), span.parent);
                out.set_name(&span.name);
                out.set_target(&span.target);
                encode_fields(&span.fields, out.init_fields(span.fields.len() as u32));
            }
        }
        Ok(())
    }

    /// Decode with quotas checked before allocating strings, binary data or lists.
    pub fn decode(value: wire::event_batch::Reader<'_>) -> Result<Self, Error> {
        let mut batch = Budget::new(MAX_EVENT_BATCH_BYTES);
        batch.charge(1024)?;
        let request_id = batch.read_text(value.get_request_id()?)?;
        let source = batch.read_text(value.get_source()?)?;
        let client_instance = value
            .get_client_instance()?
            .try_into()
            .map_err(|_| Error::InvalidArgument("client instance requires exactly 16 bytes"))?;
        let list = value.get_events()?;
        check_count(list.len() as usize)?;
        let mut events = Vec::with_capacity(list.len() as usize);
        for value in list.iter() {
            let mut budget = Budget::new(batch.left.min(MAX_EVENT_BYTES));
            budget.charge(1024)?;
            let name = budget.read_text(value.get_name()?)?;
            let target = budget.read_text(value.get_target()?)?;
            let fields = decode_fields(value.get_fields()?, &mut budget)?;
            let spans = value.get_context()?;
            budget.context(spans.len() as usize)?;
            let mut context = Vec::with_capacity(spans.len() as usize);
            for span in spans.iter() {
                context.push(EventSpan {
                    id: span.get_id(),
                    parent: read_span(span.get_parent()?)?,
                    name: budget.read_text(span.get_name()?)?,
                    target: budget.read_text(span.get_target()?)?,
                    fields: decode_fields(span.get_fields()?, &mut budget)?,
                });
            }
            batch.charge(budget.used())?;
            events.push(ProducerEvent {
                sequence: value.get_sequence(),
                event_time: value.get_event_time()?.get_unix_millis(),
                kind: value.get_kind()?,
                level: value.get_level()?,
                name,
                target,
                fields,
                context,
                span: read_span(value.get_span()?)?,
                follows_from: read_span(value.get_follows_from()?)?,
            });
        }
        let result = Self {
            request_id,
            source,
            client_instance,
            events,
        };
        result.validate()?;
        Ok(result)
    }
}

impl ProducerEvent {
    /// Conservative source expansion and owned allocation charge, independent of sequence assignment.
    /// Unused container/string capacity is admitted too, so a tiny value cannot retain an oversized allocation.
    pub fn admission_bytes(&self) -> Result<usize, Error> {
        let mut budget = Budget::new(MAX_EVENT_BYTES);
        budget.charge(1024)?;
        owned_text(&self.name, &mut budget)?;
        owned_text(&self.target, &mut budget)?;
        measure_fields(&self.fields, self.fields.capacity(), &mut budget)?;
        budget.context(self.context.capacity())?;
        for span in &self.context {
            measure_span(span, &mut budget)?;
        }
        relationships(self)?;
        Ok(budget.used())
    }
}

impl EventSpan {
    /// Conservative owned snapshot charge, including one context entry.
    pub fn admission_bytes(&self) -> Result<usize, Error> {
        let mut budget = Budget::new(MAX_EVENT_BYTES);
        budget.context(1)?;
        measure_span(self, &mut budget)?;
        Ok(budget.used())
    }
}

fn measure_span(span: &EventSpan, budget: &mut Budget) -> Result<(), Error> {
    owned_text(&span.name, budget)?;
    owned_text(&span.target, budget)?;
    measure_fields(&span.fields, span.fields.capacity(), budget)
}

fn owned_text(value: &String, budget: &mut Budget) -> Result<(), Error> {
    budget.text(value)?;
    budget.charge(value.capacity().saturating_sub(value.len() * 6))
}

fn check_count(count: usize) -> Result<(), Error> {
    if count == 0 || count > MAX_EVENT_BATCH_EVENTS {
        return Err(Error::InvalidArgument(
            "event batch requires 1..256 observations",
        ));
    }
    Ok(())
}

fn relationships(event: &ProducerEvent) -> Result<(), Error> {
    if event.span == Some(0)
        || event.follows_from == Some(0)
        || (event.kind != EventKind::Log && event.span.is_none())
        || (event.kind == EventKind::SpanFollowsFrom
            && (event.follows_from.is_none() || event.span == event.follows_from))
        || (event.kind != EventKind::SpanFollowsFrom && event.follows_from.is_some())
    {
        return Err(Error::InvalidArgument(
            "inconsistent typed event span relationship",
        ));
    }
    for (index, span) in event.context.iter().enumerate() {
        if span.id == 0
            || span.parent == Some(0)
            || span.parent == Some(span.id)
            || event.context[..index]
                .iter()
                .any(|previous| previous.id == span.id)
            || (index != 0 && span.parent != Some(event.context[index - 1].id))
        {
            return Err(Error::InvalidArgument(
                "span context requires a unique ordered parent chain",
            ));
        }
    }
    Ok(())
}

fn measure_fields(
    fields: &[EventField],
    capacity: usize,
    budget: &mut Budget,
) -> Result<(), Error> {
    budget.fields(capacity)?;
    for field in fields {
        owned_text(&field.name, budget)?;
        match &field.value {
            FieldValue::Text(value) | FieldValue::Debug(value) | FieldValue::Error(value) => {
                owned_text(value, budget)?
            }
            FieldValue::Bytes(value) => {
                budget.binary(value.len())?;
                budget.charge(value.capacity().saturating_sub(value.len() * 2))?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn optional_span(out: wire::optional_span_id::Builder<'_>, value: Option<u64>) {
    crate::encode_optional(
        out,
        value,
        |mut out, value| out.set_value(value),
        |mut out| out.set_absent(()),
    );
}
fn read_span(value: wire::optional_span_id::Reader<'_>) -> Result<Option<u64>, Error> {
    match value.which()? {
        wire::optional_span_id::Value(value) => Ok(Some(value)),
        wire::optional_span_id::Absent(()) => Ok(None),
    }
}

fn encode_fields(
    fields: &[EventField],
    mut out: capnp::struct_list::Builder<'_, wire::event_field::Owned>,
) {
    for (index, field) in fields.iter().enumerate() {
        let mut out = out.reborrow().get(index as u32);
        out.set_name(&field.name);
        match &field.value {
            FieldValue::Text(value) => out.set_text(value),
            FieldValue::Boolean(value) => out.set_boolean(*value),
            FieldValue::Signed(value) => out.set_signed(*value),
            FieldValue::Unsigned(value) => out.set_unsigned(*value),
            FieldValue::Signed128(value) => wide(out.init_signed128(), *value as u128),
            FieldValue::Unsigned128(value) => wide(out.init_unsigned128(), *value),
            FieldValue::Float64Bits(value) => out.set_float64_bits(*value),
            FieldValue::Bytes(value) => out.set_bytes(value),
            FieldValue::Debug(value) => out.set_debug(value),
            FieldValue::Error(value) => out.set_error(value),
        }
    }
}
fn wide(mut out: wire::wide_integer::Builder<'_>, value: u128) {
    out.set_high((value >> 64) as u64);
    out.set_low(value as u64);
}
fn read_wide(value: wire::wide_integer::Reader<'_>) -> u128 {
    (u128::from(value.get_high()) << 64) | u128::from(value.get_low())
}
fn decode_fields(
    values: capnp::struct_list::Reader<'_, wire::event_field::Owned>,
    budget: &mut Budget,
) -> Result<Vec<EventField>, Error> {
    budget.fields(values.len() as usize)?;
    let mut result = Vec::with_capacity(values.len() as usize);
    for value in values.iter() {
        let name = budget.read_text(value.get_name()?)?;
        let value = match value.which()? {
            wire::event_field::Text(value) => FieldValue::Text(budget.read_text(value?)?),
            wire::event_field::Boolean(value) => FieldValue::Boolean(value),
            wire::event_field::Signed(value) => FieldValue::Signed(value),
            wire::event_field::Unsigned(value) => FieldValue::Unsigned(value),
            wire::event_field::Signed128(value) => FieldValue::Signed128(read_wide(value?) as i128),
            wire::event_field::Unsigned128(value) => FieldValue::Unsigned128(read_wide(value?)),
            wire::event_field::Float64Bits(value) => FieldValue::Float64Bits(value),
            wire::event_field::Bytes(value) => {
                let value = value?;
                budget.binary(value.len())?;
                FieldValue::Bytes(value.to_vec())
            }
            wire::event_field::Debug(value) => FieldValue::Debug(budget.read_text(value?)?),
            wire::event_field::Error(value) => FieldValue::Error(budget.read_text(value?)?),
        };
        result.push(EventField { name, value });
    }
    Ok(result)
}

mod source;
