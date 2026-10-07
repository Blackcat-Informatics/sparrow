// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Bounded tracing capture feeding the single producer delivery path.

use crate::{Delivery, EventField, EventKind, EventLevel, EventSpan, FieldValue, ProducerEvent};
use std::{
    fmt::{self, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

const TEXT_LIMIT: usize = 4096;
const FIELD_BYTES: usize = 16 * 1024;
const SPAN_BYTES: usize = 16 * 1024;
const LIVE_SPANS: usize = 1024;

/// Composable structured logging layer; never installs a global subscriber.
///
/// Attach one Sparrow layer to a subscriber implementing `LookupSpan`, alongside
/// other application layers. Callbacks perform no transport I/O or queue waits.
/// At most 1024 live span snapshots, each charged at most 16 KiB, are retained.
/// A failed span update invalidates its context rather than publishing stale fields.
/// Capture failures enter `Delivery`'s drop counters and prevent a clean flush fence.
/// User-defined formatters still control their own computation and allocations.
pub struct TracingLayer {
    delivery: Delivery,
    slots: Arc<AtomicUsize>,
}

struct SpanCapture {
    owner: Arc<AtomicUsize>,
    held: bool,
    invalid: AtomicBool,
    snapshot: Mutex<Option<EventSpan>>,
}
impl Drop for SpanCapture {
    fn drop(&mut self) {
        if self.held {
            self.owner.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl TracingLayer {
    /// Share the existing delivery worker and its diagnostics/fences.
    pub fn new(delivery: Delivery) -> Self {
        Self {
            delivery,
            slots: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn emit(&self, event: Result<ProducerEvent, ()>) {
        match event {
            Ok(event) => {
                self.delivery.try_send(event);
            }
            Err(()) => self.delivery.capture_failed(),
        }
    }

    fn observation<S>(
        &self,
        metadata: &Metadata<'_>,
        kind: EventKind,
        fields: Vec<EventField>,
        id: Option<&Id>,
        follows: Option<&Id>,
        ctx: &Context<'_, S>,
    ) -> Result<ProducerEvent, ()>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let mut event = ProducerEvent {
            sequence: 0,
            event_time: crate::unix_millis_now().map_err(|_| ())?,
            kind,
            level: match *metadata.level() {
                tracing::Level::ERROR => EventLevel::Error,
                tracing::Level::WARN => EventLevel::Warn,
                tracing::Level::INFO => EventLevel::Info,
                tracing::Level::DEBUG => EventLevel::Debug,
                tracing::Level::TRACE => EventLevel::Trace,
            },
            name: bounded_text(metadata.name())?,
            target: bounded_text(metadata.target())?,
            fields,
            context: Vec::new(),
            span: id.map(|id| self.span_identity(id, ctx)).transpose()?,
            follows_from: follows.map(|id| self.span_identity(id, ctx)).transpose()?,
        };
        let mut bytes = event.admission_bytes().map_err(|_| ())?;
        if let Some(id) = id {
            let span = ctx.span(id).ok_or(())?;
            // Walk leaf-to-root: Scope::from_root would first collect an unbounded ancestry.
            for span in span.scope() {
                if event.context.len() == crate::MAX_EVENT_SPANS {
                    return Err(());
                }
                let extensions = span.extensions();
                let capture = extensions.get::<SpanCapture>().ok_or(())?;
                if !Arc::ptr_eq(&capture.owner, &self.slots)
                    || capture.invalid.load(Ordering::SeqCst)
                {
                    return Err(());
                }
                let snapshot = capture.snapshot.try_lock().map_err(|_| ())?;
                let snapshot = snapshot.as_ref().ok_or(())?;
                bytes += snapshot.admission_bytes().map_err(|_| ())?;
                if bytes > crate::MAX_EVENT_BYTES {
                    return Err(());
                }
                event.context.push(snapshot.clone());
            }
            event.context.reverse();
        }
        event.admission_bytes().map_err(|_| ())?;
        Ok(event)
    }

    fn span_identity<S>(&self, id: &Id, ctx: &Context<'_, S>) -> Result<u64, ()>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let span = ctx.span(id).ok_or(())?;
        let extensions = span.extensions();
        let capture = extensions.get::<SpanCapture>().ok_or(())?;
        if !Arc::ptr_eq(&capture.owner, &self.slots) || capture.invalid.load(Ordering::SeqCst) {
            return Err(());
        }
        Ok(capture
            .snapshot
            .try_lock()
            .map_err(|_| ())?
            .as_ref()
            .ok_or(())?
            .id)
    }

    fn lifecycle<S>(&self, id: &Id, kind: EventKind, follows: Option<&Id>, ctx: Context<'_, S>)
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let result = ctx.span(id).ok_or(()).and_then(|span| {
            self.observation(span.metadata(), kind, Vec::new(), Some(id), follows, &ctx)
        });
        self.emit(result);
    }
}

impl<S> Layer<S> for TracingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            self.delivery.capture_failed();
            return;
        };
        let held = self
            .slots
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                (count < LIVE_SPANS).then_some(count + 1)
            })
            .is_ok();
        if !held {
            self.delivery.capture_failed();
            return;
        }
        let snapshot = (|| {
            if !held {
                return Err(());
            }
            let mut visitor = Fields::default();
            attrs.record(&mut visitor);
            let parent = if attrs.is_root() {
                None
            } else {
                attrs
                    .parent()
                    .cloned()
                    .or_else(|| ctx.current_span().id().cloned())
            };
            let snapshot = EventSpan {
                id: self.delivery.next_span_id().map_err(|_| ())?,
                parent: parent
                    .as_ref()
                    .map(|parent| self.span_identity(parent, &ctx))
                    .transpose()?,
                name: bounded_text(attrs.metadata().name())?,
                target: bounded_text(attrs.metadata().target())?,
                fields: visitor.finish()?,
            };
            check_snapshot(&snapshot)?;
            Ok(snapshot)
        })();
        let mut extensions = span.extensions_mut();
        if extensions.get_mut::<SpanCapture>().is_some() {
            if held {
                self.slots.fetch_sub(1, Ordering::SeqCst);
            }
            self.delivery.capture_failed();
            return;
        }
        extensions.insert(SpanCapture {
            owner: self.slots.clone(),
            held,
            invalid: AtomicBool::new(snapshot.is_err()),
            snapshot: Mutex::new(snapshot.ok()),
        });
        drop(extensions);
        drop(span);
        self.lifecycle(id, EventKind::SpanNew, None, ctx);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let result = (|| {
            let span = ctx.span(id).ok_or(())?;
            let extensions = span.extensions();
            let capture = extensions.get::<SpanCapture>().ok_or(())?;
            if !Arc::ptr_eq(&capture.owner, &self.slots) {
                return Err(());
            }
            let update = (|| {
                if capture.invalid.load(Ordering::SeqCst) {
                    return Err(());
                }
                let mut visitor = Fields::default();
                values.record(&mut visitor);
                let fields = visitor.finish()?;
                let mut state = capture.snapshot.try_lock().map_err(|_| ())?;
                let mut snapshot = state.as_ref().ok_or(())?.clone();
                for field in &fields {
                    if let Some(old) = snapshot
                        .fields
                        .iter_mut()
                        .find(|old| old.name == field.name)
                    {
                        *old = field.clone();
                    } else {
                        snapshot.fields.push(field.clone());
                    }
                }
                check_snapshot(&snapshot)?;
                *state = Some(snapshot);
                Ok(fields)
            })();
            if update.is_err() {
                capture.invalid.store(true, Ordering::SeqCst);
            }
            let fields = update?;
            drop(extensions);
            self.observation(
                span.metadata(),
                EventKind::SpanRecord,
                fields,
                Some(id),
                None,
                &ctx,
            )
        })();
        self.emit(result);
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let result = (|| {
            let mut visitor = Fields::default();
            event.record(&mut visitor);
            let id = ctx.event_span(event).map(|span| span.id());
            self.observation(
                event.metadata(),
                EventKind::Log,
                visitor.finish()?,
                id.as_ref(),
                None,
                &ctx,
            )
        })();
        self.emit(result);
    }
    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        self.lifecycle(id, EventKind::SpanEnter, None, ctx);
    }
    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        self.lifecycle(id, EventKind::SpanExit, None, ctx);
    }
    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        self.lifecycle(&id, EventKind::SpanClose, None, ctx.clone());
        if let Some(span) = ctx.span(&id) {
            let mut extensions = span.extensions_mut();
            if extensions
                .get_mut::<SpanCapture>()
                .is_some_and(|capture| Arc::ptr_eq(&capture.owner, &self.slots))
            {
                extensions.remove::<SpanCapture>();
            }
        }
    }
    fn on_follows_from(&self, id: &Id, follows: &Id, ctx: Context<'_, S>) {
        self.lifecycle(id, EventKind::SpanFollowsFrom, Some(follows), ctx);
    }
    fn on_id_change(&self, old: &Id, new: &Id, ctx: Context<'_, S>) {
        // A remapping subscriber needs an explicit wire identity law; refuse
        // subsequent snapshots instead of silently changing producer identity.
        for id in [old, new] {
            if let Some(span) = ctx.span(id)
                && let Some(capture) = span.extensions().get::<SpanCapture>()
            {
                capture.invalid.store(true, Ordering::SeqCst);
            }
        }
        self.delivery.capture_failed();
    }
}

fn check_snapshot(span: &EventSpan) -> Result<(), ()> {
    if span.admission_bytes().map_err(|_| ())? > SPAN_BYTES {
        return Err(());
    }
    Ok(())
}

fn bounded_text(value: &str) -> Result<String, ()> {
    if value.len() > TEXT_LIMIT {
        return Err(());
    }
    Ok(value.to_owned())
}

#[derive(Default)]
struct Fields {
    fields: Vec<EventField>,
    bytes: usize,
    failed: bool,
}
impl Fields {
    fn finish(self) -> Result<Vec<EventField>, ()> {
        if self.failed {
            Err(())
        } else {
            Ok(self.fields)
        }
    }
    fn push(&mut self, field: &Field, value: FieldValue) {
        if self.failed {
            return;
        }
        let bytes = match &value {
            FieldValue::Text(v) | FieldValue::Debug(v) | FieldValue::Error(v) => v.len(),
            FieldValue::Bytes(v) => v.len(),
            _ => 16,
        };
        let charge = bytes + field.name().len() + 128;
        if field.name().len() > TEXT_LIMIT
            || self.fields.len() == crate::MAX_EVENT_FIELDS
            || charge > FIELD_BYTES - self.bytes
        {
            self.failed = true;
            return;
        }
        self.bytes += charge;
        self.fields.push(EventField {
            name: field.name().to_owned(),
            value,
        });
    }
    fn text(&mut self, field: &Field, value: Result<String, ()>, kind: fn(String) -> FieldValue) {
        match value {
            Ok(value) => self.push(field, kind(value)),
            Err(()) => self.failed = true,
        }
    }
}

struct Render {
    text: String,
    failed: bool,
}
impl fmt::Write for Render {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if self.failed || value.len() > TEXT_LIMIT - self.text.len() {
            self.failed = true;
            return Err(fmt::Error);
        }
        self.text.push_str(value);
        Ok(())
    }
}
fn render(value: fmt::Arguments<'_>) -> Result<String, ()> {
    let mut out = Render {
        text: String::new(),
        failed: false,
    };
    let result = out.write_fmt(value);
    if result.is_err() || out.failed {
        Err(())
    } else {
        Ok(out.text)
    }
}

impl Visit for Fields {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.push(field, FieldValue::Signed(value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.push(field, FieldValue::Unsigned(value));
    }
    fn record_i128(&mut self, field: &Field, value: i128) {
        self.push(field, FieldValue::Signed128(value));
    }
    fn record_u128(&mut self, field: &Field, value: u128) {
        self.push(field, FieldValue::Unsigned128(value));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.push(field, FieldValue::Boolean(value));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.push(field, FieldValue::Float64Bits(value.to_bits()));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        if !self.failed {
            self.text(field, bounded_text(value), FieldValue::Text);
        }
    }
    fn record_bytes(&mut self, field: &Field, value: &[u8]) {
        if self.failed {
            return;
        }
        if value.len() > TEXT_LIMIT {
            self.failed = true;
            return;
        }
        self.push(field, FieldValue::Bytes(value.to_owned()));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if !self.failed {
            self.text(field, render(format_args!("{value:?}")), FieldValue::Debug);
        }
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        if !self.failed {
            self.text(field, render(format_args!("{value}")), FieldValue::Error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeliveryConfig, DropReason, FlushError};
    use std::time::Duration;
    use tracing_subscriber::prelude::*;

    #[test]
    fn formatter_refusal_is_sticky_even_if_user_debug_swallows_the_error() {
        struct Swallows;
        impl fmt::Debug for Swallows {
            fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
                let _ = out.write_str(&"x".repeat(TEXT_LIMIT + 1));
                out.write_str("")
            }
        }
        assert!(render(format_args!("{:?}", Swallows)).is_err());
        assert_eq!(render(format_args!("hello {}", 42)).unwrap(), "hello 42");
    }

    #[test]
    fn independent_subscribers_share_the_producer_span_identity_allocator() {
        let dir = tempfile::tempdir().unwrap();
        let delivery = Delivery::start(DeliveryConfig::new(
            dir.path().join("missing.sock"),
            "identity",
        ))
        .unwrap();
        let capture = || {
            let subscriber =
                Arc::new(tracing_subscriber::registry().with(TracingLayer::new(delivery.clone())));
            tracing::subscriber::with_default(subscriber.clone(), || {
                let span = tracing::info_span!("independent-registry");
                let lookup = subscriber.span(&span.id().unwrap()).unwrap();
                let extensions = lookup.extensions();
                let capture = extensions.get::<SpanCapture>().unwrap();
                capture.snapshot.lock().unwrap().as_ref().unwrap().id
            })
        };
        let first = capture();
        let second = capture();
        let direct = delivery.next_span_id().unwrap();
        assert!(first > 0);
        assert_eq!(second, first + 1);
        assert_eq!(direct, second + 1);
    }

    #[test]
    fn failed_updates_refuse_stale_context_and_live_span_slots_are_reclaimed() {
        let dir = tempfile::tempdir().unwrap();
        let delivery = Delivery::start(DeliveryConfig::new(
            dir.path().join("missing.sock"),
            "capture",
        ))
        .unwrap();
        let layer = TracingLayer::new(delivery.clone());
        let slots = layer.slots.clone();
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("bounded", message = "original");
            assert_eq!(slots.load(Ordering::SeqCst), 1);
            let entered = span.enter();
            span.record("message", "x".repeat(TEXT_LIMIT + 1));
            let before = delivery.stats().unwrap().dropped_by_reason[DropReason::Invalid as usize];
            tracing::info!("must refuse stale snapshot");
            let after = delivery.stats().unwrap().dropped_by_reason[DropReason::Invalid as usize];
            assert_eq!(after, before + 1);
            drop(entered);
            drop(span);
            assert_eq!(slots.load(Ordering::SeqCst), 0);
            let spans: Vec<_> = (0..=LIVE_SPANS)
                .map(|_| tracing::info_span!("capacity"))
                .collect();
            assert_eq!(slots.load(Ordering::SeqCst), LIVE_SPANS);
            drop(spans);
            assert_eq!(slots.load(Ordering::SeqCst), 0);
        });
        assert!(matches!(
            delivery.flush(Duration::from_millis(1)),
            Err(FlushError::TimedOut(_)) | Err(FlushError::Dropped(_))
        ));
    }
}
