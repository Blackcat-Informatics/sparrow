// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One scope codec for native JSON agent inputs and generated typed requests.

use super::{Result, decimal_u64, evidence, wire};
use purrdf_lex::json::{
    Object, Value,
    record::{DecodeError, Record},
};

#[derive(Default)]
pub(super) enum Input {
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

pub(super) fn decode(value: &Value) -> std::result::Result<Input, DecodeError> {
    let mut fields = Record::new(value, "query scope")?;
    let kind: String = fields.required("kind")?;
    let result = match kind.as_str() {
        "live" => Input::Live,
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
            Input::Evidence {
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
            Input::Pin {
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
    fields.deny_unknown()?;
    Ok(result)
}

impl Input {
    pub(super) fn encode(&self, mut target: wire::query_selection::Builder<'_>) -> Result<()> {
        match self {
            Self::Live => target.set_live(()),
            Self::Evidence { store, ids } => {
                let mut list = target.init_evidence(ids.len() as u32);
                for (index, id) in ids.iter().enumerate() {
                    evidence(list.reborrow().get(index as u32), store, id)?;
                }
            }
            Self::Pin {
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

pub(super) fn schema() -> Value {
    let string = || Value::from(Object::new().with("type", "string"));
    let object = |properties: Object, required: Vec<&str>| {
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
                Value::from(
                    Object::new()
                        .with("type", "object")
                        .with("additionalProperties", false)
                        .with(
                            "properties",
                            Object::new()
                                .with("kind", kind("selection"))
                                .with("store_id", string())
                                .with("artifact_ids", ids())
                                .with("record_ids", ids()),
                        )
                        .with("required", vec!["kind", "store_id"])
                        .with(
                            "anyOf",
                            Value::array([
                                Object::new().with("required", vec!["artifact_ids"]),
                                Object::new().with("required", vec!["record_ids"]),
                            ]),
                        ),
                ),
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
