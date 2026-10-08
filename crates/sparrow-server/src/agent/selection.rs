// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One scope codec for native JSON agent inputs and generated typed requests.

use super::{Result, decimal_u64, evidence, wire};
use purrdf_lex::json::{
    Object, Value,
    record::{DecodeError, Record},
};

#[derive(Default)]
enum Membership {
    #[default]
    Live,
    Evidence {
        store: String,
        ids: Vec<String>,
    },
    Pin {
        store: String,
        id: String,
        revision: Option<u64>,
    },
}

#[derive(Default)]
pub(super) struct Input {
    membership: Membership,
    sources: Option<sparrow_core::QueryAlternatives>,
    diagnostics: std::collections::BTreeMap<
        sparrow_core::DiagnosticDimension,
        sparrow_core::QueryAlternatives,
    >,
}

pub(super) fn decode(value: &Value) -> std::result::Result<Input, DecodeError> {
    let mut fields = Record::new(value, "query scope")?;
    let kind: String = fields.required("kind")?;
    let result = match kind.as_str() {
        "live" => Membership::Live,
        "selection" => {
            let store = fields.required("store_id")?;
            let artifacts: Vec<String> = fields.optional("artifact_ids")?.unwrap_or_default();
            let records: Vec<String> = fields.optional("record_ids")?.unwrap_or_default();
            if artifacts.len().saturating_add(records.len()) == 0
                || artifacts.len().saturating_add(records.len()) > 1000
                || artifacts
                    .iter()
                    .any(|id| !id.starts_with("artifact:") || id.len() > 128)
                || records
                    .iter()
                    .any(|id| !id.starts_with("record:") || id.len() > 128)
                || value.get("artifact_ids").is_some() && artifacts.is_empty()
                || value.get("record_ids").is_some() && records.is_empty()
            {
                return Err(DecodeError::custom(
                    "selection requires 1..1000 bounded artifact/record IDs; present arrays must be nonempty",
                ));
            }
            Membership::Evidence {
                store,
                ids: artifacts.into_iter().chain(records).collect(),
            }
        }
        "pin" => {
            let store = fields.required("store_id")?;
            let id: String = fields.required("pin_id")?;
            let revision = fields
                .raw("expected_revision")?
                .map(decimal_u64)
                .transpose()?;
            if !id.starts_with("pin:") || id.len() > 128 || revision == Some(0) {
                return Err(DecodeError::custom(
                    "pin scope requires a bounded stable ID and positive decimal revision",
                ));
            }
            Membership::Pin {
                store,
                id,
                revision,
            }
        }
        _ => {
            return Err(DecodeError::custom(
                "scope kind must be live, selection or pin",
            ));
        }
    };
    let mut sources = None;
    let mut diagnostics = std::collections::BTreeMap::new();
    if let Some(value) = fields.raw("filter")? {
        let mut filter = Record::new(value, "scope filter")?;
        sources = filter
            .optional::<Vec<String>>("source_ids")?
            .map(sparrow_core::QueryAlternatives::new)
            .transpose()
            .map_err(DecodeError::custom)?;
        for name in ["service", "host", "severity", "trace_id", "span_id"] {
            if let Some(values) = filter.optional::<Vec<String>>(name)? {
                diagnostics.insert(
                    sparrow_core::DiagnosticDimension::from_name(name)
                        .map_err(DecodeError::custom)?,
                    sparrow_core::QueryAlternatives::new(values).map_err(DecodeError::custom)?,
                );
            }
        }
        filter.deny_unknown()?;
        if sources.is_none() && diagnostics.is_empty() {
            return Err(DecodeError::custom(
                "scope filter requires at least one nonempty supported array",
            ));
        }
    }

    fields.deny_unknown()?;
    Ok(Input {
        membership: result,
        sources,
        diagnostics,
    })
}

/// Artifact metadata admits the shared membership vocabulary, without silent filter omission.
pub(super) fn decode_membership(value: &Value) -> std::result::Result<Input, DecodeError> {
    let result = decode(value)?;
    if result.sources.is_some() || !result.diagnostics.is_empty() {
        return Err(DecodeError::custom(
            "artifact discovery accepts membership-only scope; use outer source and original receipt-time filters",
        ));
    }
    Ok(result)
}

impl Input {
    pub(super) async fn require_supported(&self, client: &wire::sparrow::Client) -> Result<()> {
        sparrow_client::Discovery::fetch(client).await?;
        Ok(())
    }

    pub(super) fn encode(&self, mut scope: wire::scope::Builder<'_>) -> Result<()> {
        if let Some(filter) = &self.sources {
            let mut target = scope
                .reborrow()
                .init_source_ids()
                .init_value(filter.ids().len() as u32);
            for (index, id) in filter.ids().iter().enumerate() {
                target.set(index as u32, id);
            }
        }
        let mut filters = scope
            .reborrow()
            .init_diagnostics(self.diagnostics.len() as u32);
        for (index, (dimension, values)) in self.diagnostics.iter().enumerate() {
            let mut row = filters.reborrow().get(index as u32);
            row.set_dimension(crate::diagnostics::wire_dimension(*dimension));
            let mut target = row.init_alternatives(values.ids().len() as u32);
            for (index, value) in values.ids().iter().enumerate() {
                target.set(index as u32, value);
            }
        }
        self.encode_membership(scope.init_selection())
    }

    /// Shared typed evidence/pin/live codec for record and artifact requests.
    pub(super) fn encode_membership(
        &self,
        mut target: wire::query_selection::Builder<'_>,
    ) -> Result<()> {
        match &self.membership {
            Membership::Live => target.set_live(()),
            Membership::Evidence { store, ids } => {
                let mut list = target.init_evidence(ids.len() as u32);
                for (index, id) in ids.iter().enumerate() {
                    evidence(list.reborrow().get(index as u32), store, id)?;
                }
            }
            Membership::Pin {
                store,
                id,
                revision,
            } => {
                let mut pin = target.init_pin();
                pin.set_store_id(store);
                pin.set_id(id);
                sparrow_client::encode_optional(
                    pin.init_expected_revision(),
                    *revision,
                    |mut target, value| target.set_value(value),
                    |mut target| target.set_absent(()),
                );
            }
        }
        Ok(())
    }
}

pub(super) fn schema(filters: bool) -> Value {
    let string = || Value::from(Object::new().with("type", "string"));
    let alternatives = Value::from(
        Object::new()
            .with("type", "array")
            .with("minItems", 1_u64)
            .with("maxItems", 128_u64)
            .with(
                "items",
                Object::new()
                    .with("type", "string")
                    .with("minLength", 1_u64)
                    .with("maxLength", 512_u64),
            ),
    );
    let mut properties = Object::new();
    for name in [
        "source_ids",
        "service",
        "host",
        "severity",
        "trace_id",
        "span_id",
    ] {
        properties = properties.with(name, alternatives.clone());
    }
    let filter = Value::from(
        Object::new()
            .with("type", "object")
            .with("additionalProperties", false)
            .with("minProperties", 1_u64)
            .with("properties", properties),
    );
    let object = |mut properties: Object, required: Vec<&str>| {
        if filters {
            properties = properties.with("filter", filter.clone());
        }
        Value::from(
            Object::new()
                .with("type", "object")
                .with("additionalProperties", false)
                .with("properties", properties)
                .with("required", required),
        )
    };
    let kind = |name: &str| Value::from(Object::new().with("const", name));
    let ids = || {
        Value::from(
            Object::new()
                .with("type", "array")
                .with("minItems", 1_u64)
                .with("maxItems", 1000_u64)
                .with("items", string()),
        )
    };
    Object::new()
        .with(
            "oneOf",
            Value::array([
                object(Object::new().with("kind", kind("live")), vec!["kind"]),
                {
                    let mut properties = Object::new()
                        .with("kind", kind("selection"))
                        .with("store_id", string())
                        .with("artifact_ids", ids())
                        .with("record_ids", ids());
                    if filters {
                        properties = properties.with("filter", filter.clone());
                    }
                    Value::from(
                        Object::new()
                            .with("type", "object")
                            .with("additionalProperties", false)
                            .with("properties", properties)
                            .with("required", vec!["kind", "store_id"])
                            .with(
                                "anyOf",
                                Value::array([
                                    Object::new().with("required", vec!["artifact_ids"]),
                                    Object::new().with("required", vec!["record_ids"]),
                                ]),
                            ),
                    )
                },
                object(
                    Object::new()
                        .with("kind", kind("pin"))
                        .with("store_id", string())
                        .with("pin_id", string())
                        .with("expected_revision", string()),
                    vec!["kind", "store_id", "pin_id"],
                ),
            ]),
        )
        .into()
}
