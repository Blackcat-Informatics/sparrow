// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Closed controls for native fixed-recipe correlation, shared by CLI and MCP.

use super::{Result, failure, sparql, wire};
use purrdf_lex::json::{
    Object, Value,
    record::{DecodeError, Record},
};
use sparrow_core::{
    CorrelationClock as Clock, CorrelationInterval as Interval, CorrelationRelation as Relation,
    CorrelationSelector as Selector,
};

pub(crate) struct Input {
    controls: sparql::Controls,
    selector: Selector,
    relation: Relation,
}

fn interval(value: &Value) -> std::result::Result<Interval, DecodeError> {
    let mut input = Record::new(value, "correlation interval")?;
    let clock = match input.tag("clock", &["received", "event"])? {
        "received" => Clock::Received,
        "event" => Clock::Event,
        _ => return Err(DecodeError::custom("clock must be received or event")),
    };
    let from: String = input.required("from")?;
    let before: String = input.required("before")?;
    input.deny_unknown()?;
    Ok(Interval {
        clock,
        from: sparrow_core::parse_instant(&from).map_err(DecodeError::custom)?,
        before: sparrow_core::parse_instant(&before).map_err(DecodeError::custom)?,
    })
}

fn selector(value: &Value) -> std::result::Result<Selector, DecodeError> {
    let mut input = Record::new(value, "correlation selector")?;
    let text = |value: &Value, maximum| {
        let value = value
            .as_str()
            .ok_or_else(|| DecodeError::custom("correlation value requires a string"))?;
        if value.is_empty() || value.len() > maximum {
            return Err(DecodeError::custom(
                "correlation value exceeds its byte admission",
            ));
        }
        Ok(value.to_owned())
    };
    let selector = match input.tag("kind", &["evidence", "diagnostic", "source", "time"])? {
        "evidence" => {
            let values = input.required_with("ids", |value| {
                value
                    .as_array()
                    .ok_or_else(|| DecodeError::custom("seed references require an array"))
            })?;
            if values.is_empty() || values.len() > 32 {
                return Err(DecodeError::custom(
                    "correlation requires 1..32 seed references",
                ));
            }
            let ids = values
                .iter()
                .map(|value| text(value, 128))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Selector::Evidence(ids)
        }
        "diagnostic" => Selector::Diagnostic(
            sparrow_core::DiagnosticDimension::from_name(&input.required::<String>("dimension")?)
                .map_err(DecodeError::custom)?,
            input.required_with("value", |value| text(value, 512))?,
        ),
        "source" => Selector::Source(input.required_with("value", |value| text(value, 512))?),
        "time" => Selector::Time(interval(
            input
                .raw("interval")?
                .ok_or_else(|| DecodeError::custom("missing correlation interval"))?,
        )?),
        _ => {
            return Err(DecodeError::custom(
                "selector kind must be evidence, diagnostic, source or time",
            ));
        }
    };
    input.deny_unknown()?;
    Ok(selector)
}

fn relation(value: &Value) -> std::result::Result<Relation, DecodeError> {
    if let Some(value) = value.as_str() {
        return match value {
            "shared_trace_id" => Ok(Relation::TraceId),
            "shared_span_id" => Ok(Relation::SpanId),
            "shared_source" => Ok(Relation::Source),
            _ => Err(DecodeError::custom(
                "relationship must be shared_trace_id, shared_span_id, shared_source or cooccurs_in_interval",
            )),
        };
    }
    let mut input = Record::new(value, "temporal correlation relation")?;
    let kind: String = input.required("kind")?;
    if kind != "cooccurs_in_interval" {
        return Err(DecodeError::custom(
            "temporal relation kind must be cooccurs_in_interval",
        ));
    }
    let interval = interval(
        input
            .raw("interval")?
            .ok_or_else(|| DecodeError::custom("missing correlation interval"))?,
    )?;
    input.deny_unknown()?;
    Ok(Relation::Time(interval))
}

impl Input {
    pub(super) fn decode(value: &Value) -> Result<Self> {
        let decode = || -> std::result::Result<Self, DecodeError> {
            let mut input = Record::new(value, "correlation request")?;
            let controls = sparql::Controls::decode(&mut input)?;
            let selector = selector(
                input
                    .raw("selector")?
                    .ok_or_else(|| DecodeError::custom("missing correlation selector"))?,
            )?;
            let relation = relation(
                input
                    .raw("relation")?
                    .ok_or_else(|| DecodeError::custom("missing correlation relation"))?,
            )?;
            input.deny_unknown()?;
            Ok(Self {
                controls,
                selector,
                relation,
            })
        };
        decode().map_err(|error| super::invalid(error.to_string()))
    }

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        self.controls.require(client).await?;
        let mut request = client.correlate_request();
        let mut query = request.get().init_query();
        query.set_store_id(&self.controls.store);
        query.set_schema_version(sparrow_client::SCHEMA_VERSION);
        super::fill_scope(query.reborrow().init_scope(), &self.controls.options)?;
        crate::codec::encode_correlation_selector(
            query.reborrow().init_selector(),
            &self.selector,
            &self.controls.store,
        )?;
        crate::codec::encode_correlation_relation(query.reborrow().init_relation(), self.relation);
        self.controls.fill_limits(query.init_limits());
        let response = request.send().promise.await?;
        match response.get()?.get_result()?.which()? {
            wire::result::Ok(page) => sparql::render(page?),
            wire::result::Error(error) => Err(failure(error?)),
        }
    }
}

pub(super) fn schema() -> Value {
    let mut root = sparql::schema()
        .as_object()
        .expect("native query schema is an object")
        .clone();
    let mut properties = root
        .remove("properties")
        .expect("native query schema has properties")
        .as_object()
        .expect("query properties are an object")
        .clone();
    for name in ["query", "base_iri", "bindings", "layers"] {
        properties.remove(name);
    }
    let interval = Object::new()
        .with("type", "object")
        .with("additionalProperties", false)
        .with("required", vec!["clock", "from", "before"])
        .with(
            "properties",
            Object::new()
                .with(
                    "clock",
                    Object::new().with("enum", vec!["received", "event"]),
                )
                .with(
                    "from",
                    Object::new()
                        .with("type", "string")
                        .with("format", "date-time"),
                )
                .with(
                    "before",
                    Object::new()
                        .with("type", "string")
                        .with("format", "date-time"),
                ),
        );
    let interval: Value = interval.into();
    let mut variants = Vec::new();
    for (kind, field, shape) in [
        (
            "evidence",
            "ids",
            Value::from(
                Object::new()
                    .with("type", "array")
                    .with("minItems", 1_u32)
                    .with("maxItems", 32_u32)
                    .with(
                        "items",
                        Object::new()
                            .with("type", "string")
                            .with("maxLength", 128_u32),
                    ),
            ),
        ),
        (
            "source",
            "value",
            Value::from(
                Object::new()
                    .with("type", "string")
                    .with("minLength", 1_u32)
                    .with("maxLength", 512_u32),
            ),
        ),
        ("time", "interval", interval.clone()),
    ] {
        variants.push(Value::from(
            Object::new()
                .with("type", "object")
                .with("additionalProperties", false)
                .with("required", vec!["kind", field])
                .with(
                    "properties",
                    Object::new()
                        .with("kind", Object::new().with("const", kind))
                        .with(field, shape),
                ),
        ));
    }
    variants.push(
        Object::new()
            .with("type", "object")
            .with("additionalProperties", false)
            .with("required", vec!["kind", "dimension", "value"])
            .with(
                "properties",
                Object::new()
                    .with("kind", Object::new().with("const", "diagnostic"))
                    .with(
                        "dimension",
                        Object::new().with(
                            "enum",
                            vec!["service", "host", "severity", "trace_id", "span_id"],
                        ),
                    )
                    .with(
                        "value",
                        Object::new()
                            .with("type", "string")
                            .with("minLength", 1_u32)
                            .with("maxLength", 512_u32),
                    ),
            )
            .into(),
    );
    properties.insert("selector", Object::new().with("oneOf", variants));
    properties.insert(
        "relation",
        Object::new().with(
            "oneOf",
            vec![
                Value::from(Object::new().with(
                    "enum",
                    vec!["shared_trace_id", "shared_span_id", "shared_source"],
                )),
                Object::new()
                    .with("type", "object")
                    .with("additionalProperties", false)
                    .with("required", vec!["kind", "interval"])
                    .with(
                        "properties",
                        Object::new()
                            .with("kind", Object::new().with("const", "cooccurs_in_interval"))
                            .with("interval", interval),
                    )
                    .into(),
            ],
        ),
    );
    root.insert("required", vec!["store_id", "selector", "relation"]);
    root.insert("properties", properties);
    root.into()
}
