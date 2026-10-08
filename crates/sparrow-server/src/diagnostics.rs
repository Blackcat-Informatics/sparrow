// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One typed vocabulary and evidence codec for daemon and agent adapters.

use crate::sparrow_capnp as wire;
use purrdf_lex::json::{Object, Value};
use sparrow_core::{
    DiagnosticDimension as Dimension, DiagnosticProfile, DiagnosticSnapshot, DiagnosticValue,
};
use std::collections::BTreeMap;

pub(crate) fn dimension(value: wire::DiagnosticDimension) -> Dimension {
    match value {
        wire::DiagnosticDimension::Service => Dimension::Service,
        wire::DiagnosticDimension::Host => Dimension::Host,
        wire::DiagnosticDimension::Severity => Dimension::Severity,
        wire::DiagnosticDimension::TraceId => Dimension::TraceId,
        wire::DiagnosticDimension::SpanId => Dimension::SpanId,
    }
}

pub(crate) fn wire_dimension(value: Dimension) -> wire::DiagnosticDimension {
    match value {
        Dimension::Service => wire::DiagnosticDimension::Service,
        Dimension::Host => wire::DiagnosticDimension::Host,
        Dimension::Severity => wire::DiagnosticDimension::Severity,
        Dimension::TraceId => wire::DiagnosticDimension::TraceId,
        Dimension::SpanId => wire::DiagnosticDimension::SpanId,
    }
}

pub(crate) fn mappings(
    value: capnp::struct_list::Reader<'_, wire::diagnostic_mapping::Owned>,
) -> capnp::Result<Result<BTreeMap<Dimension, String>, sparrow_core::Error>> {
    if value.is_empty() || value.len() > 5 {
        return Ok(Err(sparrow_core::Error::new(
            "INVALID_ARGUMENT",
            "require 1..5 diagnostic mappings",
        )));
    }
    let mut result = BTreeMap::new();
    for row in value.iter() {
        let path = row.get_path()?;
        if path.len() > 4096
            || result
                .insert(dimension(row.get_dimension()?), path.to_str()?.to_owned())
                .is_some()
        {
            return Ok(Err(sparrow_core::Error::new(
                "INVALID_ARGUMENT",
                "diagnostic mappings must have distinct dimensions and pointers of at most 4096 bytes",
            )));
        }
    }
    Ok(Ok(result))
}

pub(crate) fn encode_mappings(
    mut target: capnp::struct_list::Builder<'_, wire::diagnostic_mapping::Owned>,
    values: &BTreeMap<Dimension, String>,
) {
    for (index, (dimension, path)) in values.iter().enumerate() {
        let mut row = target.reborrow().get(index as u32);
        row.set_dimension(wire_dimension(*dimension));
        row.set_path(path);
    }
}

pub(crate) fn profile(
    mut target: wire::diagnostic_profile::Builder<'_>,
    profile: &DiagnosticProfile,
    store: &str,
) {
    target.set_store_id(store);
    target.set_id(&profile.id);
    target.set_source(&profile.source);
    encode_mappings(
        target.init_mappings(profile.mappings.len() as u32),
        &profile.mappings,
    );
}

pub(crate) fn snapshot(
    mut target: wire::diagnostic_snapshot::Builder<'_>,
    value: &DiagnosticSnapshot,
) {
    if let Some(id) = &value.profile_id {
        target.reborrow().init_profile_id().set_value(id);
    }
    let mut rows = target.init_fields(value.fields.len() as u32);
    for (index, (dimension, value)) in value.fields.iter().enumerate() {
        let mut row = rows.reborrow().get(index as u32);
        row.set_dimension(wire_dimension(*dimension));
        match value {
            DiagnosticValue::Unknown(reason) => row.set_unknown(reason),
            DiagnosticValue::Known {
                value,
                occurrence,
                source_start,
                source_end,
            } => {
                let mut row = row.init_known();
                row.set_value(value);
                row.set_occurrence(*occurrence);
                row.set_source_start(*source_start);
                row.set_source_end(*source_end);
            }
        }
    }
}

pub(crate) fn render_snapshot(
    value: wire::diagnostic_snapshot::Reader<'_>,
) -> capnp::Result<Value> {
    let profile = match value.get_profile_id()?.which()? {
        wire::optional_text::Absent(()) => Value::Null,
        wire::optional_text::Value(id) => Value::from(id?.to_str()?),
    };
    let rows = value
        .get_fields()?
        .iter()
        .map(|row| {
            let row_out = Object::new().with("dimension", dimension(row.get_dimension()?).name());
            Ok(Value::from(match row.which()? {
                wire::diagnostic_field::Unknown(reason) => row_out
                    .with("state", "unknown")
                    .with("reason", reason?.to_str()?),
                wire::diagnostic_field::Known(value) => {
                    let value = value?;
                    row_out
                        .with("state", "known")
                        .with("value", value.get_value()?.to_str()?)
                        .with("occurrence", value.get_occurrence().to_string())
                        .with("source_start", value.get_source_start().to_string())
                        .with("source_end", value.get_source_end().to_string())
                }
            }))
        })
        .collect::<capnp::Result<Vec<_>>>()?;
    Ok(Object::new()
        .with("profile_id", profile)
        .with("fields", Value::array(rows))
        .into())
}

pub(crate) fn render_profile(value: wire::diagnostic_profile::Reader<'_>) -> capnp::Result<Value> {
    let mappings = value
        .get_mappings()?
        .iter()
        .map(|row| {
            Ok(Value::from(
                Object::new()
                    .with("dimension", dimension(row.get_dimension()?).name())
                    .with("path", row.get_path()?.to_str()?),
            ))
        })
        .collect::<capnp::Result<Vec<_>>>()?;
    Ok(Object::new()
        .with("store_id", value.get_store_id()?.to_str()?)
        .with("profile_id", value.get_id()?.to_str()?)
        .with("source", value.get_source()?.to_str()?)
        .with("mappings", Value::array(mappings))
        .into())
}
