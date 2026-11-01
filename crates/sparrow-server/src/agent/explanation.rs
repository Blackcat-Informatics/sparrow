// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Typed measured-work rendering; native counters and forecasts keep their meaning.

use super::{Result, optional_value, query_receipt::limit, wire};
use purrdf_lex::json::{Object, Value};

pub(super) fn render(value: wire::sparql_explanation::Reader<'_>) -> Result<Value> {
    let source = value.get_schedule()?;
    if source.len() as usize != purrdf::sparql::CHARGE_SCHEDULE.len() {
        return Err(super::invalid(
            "native explanation has an incomplete charge schedule",
        ));
    }
    let mut schedule = Vec::new();
    for (index, row) in source.iter().enumerate() {
        let (label, cost) = purrdf::sparql::CHARGE_SCHEDULE[index];
        if row.get_label()?.to_str()? != label || row.get_cost() != cost {
            return Err(super::invalid(
                "native explanation charge schedule disagrees with this client",
            ));
        }
        schedule.push(Value::from(
            Object::new()
                .with("label", label)
                .with("cost", cost.to_string()),
        ));
    }
    let relations = value.get_relations()?;
    if relations.len() > 2 || !value.get_aggregates()?.is_empty() {
        return Err(super::invalid(
            "native explanation exceeds its ordinary stored-text registry contract",
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut relation_names = Vec::new();
    for relation in relations {
        let relation = relation?;
        let name = relation.to_str()?;
        if name.is_empty() || name.len() > 1024 || !seen.insert(name) {
            return Err(super::invalid(
                "invalid native relation declaration inventory",
            ));
        }
        relation_names.push(Value::from(name));
    }
    let indexes = value.get_indexes()?;
    let entries = value.get_items()?;
    if entries.len() > 1000 {
        return Err(super::invalid(
            "native explanation exceeds its page item ceiling",
        ));
    }
    let items = entries.iter().map(item).collect::<Result<Vec<_>>>()?;
    Ok(Object::new()
        .with(
            "profile",
            Object::new()
                .with("id", value.get_profile()?.to_str()?)
                .with("version", value.get_version())
                .with("digest", value.get_digest()?.to_str()?)
                .with("stop_poll_fuel", value.get_stop_poll_fuel().to_string()),
        )
        .with("schedule", Value::array(schedule))
        .with(
            "statement_indexes",
            Object::new()
                .with("reifier_targets", indexes.get_reifier_targets())
                .with("reifier_components", indexes.get_reifier_components())
                .with("annotation_values", indexes.get_annotation_values()),
        )
        .with("relations", Value::array(relation_names))
        .with("aggregates", Value::array(std::iter::empty::<Value>()))
        .with("items", Value::array(items))
        .into())
}

fn item(value: wire::native_explanation_item::Reader<'_>) -> Result<Value> {
    Ok(match value.which()? {
        wire::native_explanation_item::JoinOrder(text) => Object::new()
            .with("kind", "join_order")
            .with("text", text?.to_str()?)
            .into(),
        wire::native_explanation_item::Statistic(text) => Object::new()
            .with("kind", "statistic")
            .with("text", text?.to_str()?)
            .into(),
        wire::native_explanation_item::Relation(value) => {
            let value = value?;
            let arity = value
                .get_subject_arity()
                .checked_add(value.get_object_arity())
                .ok_or_else(|| super::invalid("native relation arity overflow"))?;
            let source = value.get_modes()?;
            if arity > 64 || source.is_empty() || source.len() > 64 {
                return Err(super::invalid("invalid native relation modes"));
            }
            let mut modes = Vec::new();
            for mode in source {
                let code = mode.get_code()?.to_str()?;
                if code.len() != arity as usize
                    || !code.bytes().all(|byte| matches!(byte, b'b' | b'f'))
                {
                    return Err(super::invalid(
                        "native relation mode disagrees with its arity",
                    ));
                }
                modes.push(Value::from(Object::new().with("code", code).with(
                    "rows_per_invocation",
                    mode.get_rows_per_invocation().to_string(),
                )));
            }
            let volatility = match value.get_volatility()? {
                wire::NativeRelationVolatility::Stable => purrdf::sparql::Volatility::Stable,
                wire::NativeRelationVolatility::Volatile => purrdf::sparql::Volatility::Volatile,
            };
            Object::new()
                .with("kind", "relation")
                .with("iri", value.get_iri()?.to_str()?)
                .with("subject_arity", value.get_subject_arity())
                .with("object_arity", value.get_object_arity())
                .with("volatility", volatility.label())
                .with("modes", Value::array(modes))
                .into()
        }
        wire::native_explanation_item::Node(node) => {
            let node = node?;
            let fuel = node.get_fuel()?;
            if fuel.len() as usize != purrdf::sparql::CHARGE_SCHEDULE.len() {
                return Err(super::invalid(
                    "native node charge vector disagrees with its schedule",
                ));
            }
            let estimate = match node.get_estimate()?.which()? {
                wire::optional_native_plan_estimate::Absent(()) => Value::Null,
                wire::optional_native_plan_estimate::Value(value) => {
                    let value = value?;
                    Object::new()
                        .with("rows", value.get_rows().to_string())
                        .with("peak_rows", value.get_peak_rows().to_string())
                        .with("columns", value.get_columns().to_string())
                        .with("peak_cells", value.get_peak_cells().to_string())
                        .into()
                }
            };
            Object::new()
                .with("kind", "node")
                .with("ordinal", node.get_ordinal().to_string())
                .with("depth", node.get_depth().to_string())
                .with("label", node.get_label()?.to_str()?)
                .with("fuel", Value::array(fuel.iter().map(|n| n.to_string())))
                .with("fuel_total", node.get_fuel_total().to_string())
                .with("rows", node.get_rows().to_string())
                .with("cells", node.get_cells().to_string())
                .with("estimate", estimate)
                .into()
        }
        wire::native_explanation_item::Shape(shape) => {
            let shape = shape?;
            let estimate = match shape.get_estimate()?.which()? {
                wire::optional_native_access_estimate::Absent(()) => Value::Null,
                wire::optional_native_access_estimate::Value(value) => {
                    let value = value?;
                    let residency = match value.get_residency()? {
                        wire::NativeReadResidency::Absent => Value::Null,
                        wire::NativeReadResidency::Resident => "resident".into(),
                        wire::NativeReadResidency::NonResident => "non_resident".into(),
                        wire::NativeReadResidency::Mixed => "mixed".into(),
                    };
                    Object::new()
                        .with("work", value.get_work().to_string())
                        .with("rows", value.get_rows().to_string())
                        .with("pages", limit(value.get_pages()?)?)
                        .with("residency", residency)
                        .with(
                            "cardinality_fallback",
                            limit(value.get_cardinality_fallback()?)?,
                        )
                        .into()
                }
            };
            Object::new()
                .with("kind", "shape")
                .with("node", limit(shape.get_node()?)?)
                .with("operator", optional_value(shape.get_operator()?)?)
                .with("provider", optional_value(shape.get_provider()?)?)
                .with("registry", limit(shape.get_registry()?)?)
                .with(
                    "work",
                    Object::new()
                        .with("source_visits", shape.get_source_visits().to_string())
                        .with("transitions", shape.get_transitions().to_string())
                        .with("output_rows", shape.get_output_rows().to_string()),
                )
                .with("estimate", estimate)
                .into()
        }
    })
}
