// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Shared bounded correlation controls and descriptor codec for RPC and adapters.

use crate::sparrow_capnp as wire;
use sparrow_core::{
    CorrelationCapture, CorrelationClock as Clock, CorrelationInterval as Interval,
    CorrelationRelation as Relation, CorrelationSelector as Selector,
};

pub(crate) fn interval(value: wire::correlation_interval::Reader<'_>) -> capnp::Result<Interval> {
    Ok(Interval {
        clock: match value.get_clock()? {
            wire::CorrelationClock::Received => Clock::Received,
            wire::CorrelationClock::Event => Clock::Event,
        },
        from: value.get_from()?.get_unix_millis(),
        before: value.get_before()?.get_unix_millis(),
    })
}

fn text(value: capnp::text::Reader<'_>) -> capnp::Result<String> {
    if value.is_empty() || value.len() > 512 {
        return Err(capnp::Error::failed(
            "correlation value requires 1..512 bytes".into(),
        ));
    }
    Ok(value.to_str()?.to_owned())
}

pub(crate) fn selector(
    value: wire::correlation_selector::Reader<'_>,
    store: &str,
) -> capnp::Result<Selector> {
    Ok(match value.which()? {
        wire::correlation_selector::Evidence(ids) => {
            let ids = ids?;
            if ids.is_empty() || ids.len() > 32 {
                return Err(capnp::Error::failed(
                    "correlation admits 1..32 seed references".into(),
                ));
            }
            let mut selected = Vec::new();
            for reference in ids {
                let (owner, id) = sparrow_client::decode_evidence(reference)?;
                if owner != store {
                    return Err(capnp::Error::failed(
                        "correlation seed requires the selected store and bounded evidence ID"
                            .into(),
                    ));
                }
                selected.push(id);
            }
            Selector::Evidence(selected)
        }
        wire::correlation_selector::Diagnostic(value) => {
            let value = value?;
            Selector::Diagnostic(
                crate::diagnostics::dimension(value.get_dimension()?),
                text(value.get_value()?)?,
            )
        }
        wire::correlation_selector::Source(value) => Selector::Source(text(value?)?),
        wire::correlation_selector::Time(value) => Selector::Time(interval(value?)?),
    })
}

pub(crate) fn relation(value: wire::correlation_relation::Reader<'_>) -> capnp::Result<Relation> {
    Ok(match value.which()? {
        wire::correlation_relation::TraceId(()) => Relation::TraceId,
        wire::correlation_relation::SpanId(()) => Relation::SpanId,
        wire::correlation_relation::Source(()) => Relation::Source,
        wire::correlation_relation::Time(value) => Relation::Time(interval(value?)?),
    })
}

fn encode_interval(mut out: wire::correlation_interval::Builder<'_>, value: Interval) {
    out.set_clock(match value.clock {
        Clock::Received => wire::CorrelationClock::Received,
        Clock::Event => wire::CorrelationClock::Event,
    });
    out.reborrow().init_from().set_unix_millis(value.from);
    out.init_before().set_unix_millis(value.before);
}

pub(crate) fn encode_selector(
    mut out: wire::correlation_selector::Builder<'_>,
    value: &Selector,
    store: &str,
) -> capnp::Result<()> {
    match value {
        Selector::Evidence(ids) => {
            let mut targets = out.init_evidence(ids.len() as u32);
            for (index, id) in ids.iter().enumerate() {
                sparrow_client::encode_evidence(targets.reborrow().get(index as u32), store, id)
                    .map_err(|error| capnp::Error::failed(error.to_string()))?;
            }
        }
        Selector::Diagnostic(dimension, value) => {
            let mut target = out.init_diagnostic();
            target.set_dimension(crate::diagnostics::wire_dimension(*dimension));
            target.set_value(value);
        }
        Selector::Source(value) => out.set_source(value),
        Selector::Time(value) => encode_interval(out.init_time(), *value),
    }
    Ok(())
}

pub(crate) fn encode_relation(mut out: wire::correlation_relation::Builder<'_>, value: Relation) {
    match value {
        Relation::TraceId => out.set_trace_id(()),
        Relation::SpanId => out.set_span_id(()),
        Relation::Source => out.set_source(()),
        Relation::Time(value) => encode_interval(out.init_time(), value),
    }
}

pub(crate) fn encode(
    mut out: wire::correlation_capture::Builder<'_>,
    capture: &CorrelationCapture,
    store: &str,
) -> capnp::Result<()> {
    out.set_profile(sparrow_core::CORRELATION_PROFILE);
    out.set_query_id(&capture.query_id);
    encode_selector(out.reborrow().init_selector(), &capture.selector, store)?;
    encode_relation(out.init_relation(), capture.relation);
    Ok(())
}

pub(crate) fn decode(
    value: wire::correlation_capture::Reader<'_>,
    store: &str,
) -> capnp::Result<CorrelationCapture> {
    if value.get_profile()?.to_str()? != sparrow_core::CORRELATION_PROFILE
        || value.get_query_id()?.len() > 128
    {
        return Err(capnp::Error::failed(
            "unsupported correlation descriptor".into(),
        ));
    }
    let capture = CorrelationCapture {
        query_id: value.get_query_id()?.to_str()?.to_owned(),
        selector: selector(value.get_selector()?, store)?,
        relation: relation(value.get_relation()?)?,
    };
    capture
        .validate()
        .map_err(|error| capnp::Error::failed(error.to_string()))?;
    Ok(capture)
}
