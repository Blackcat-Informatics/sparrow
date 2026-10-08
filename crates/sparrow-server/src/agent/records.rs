// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One generated-client operation for same-record queries in CLI and MCP.

use super::{
    Options, Result, failure, fill_scope, instant, invalid, kind_name, optional, optional_value,
    parse_kind, receipt, search, wire,
};
use purrdf_lex::json::{
    Object, Value,
    record::{DecodeError, Record},
};

pub(crate) struct Input {
    pub options: Options,
    pub text: Option<String>,
    pub fields: Vec<sparrow_core::FieldConstraint>,
    pub event_interval: Option<sparrow_core::EventInterval>,
}

pub(super) fn interval(
    from: Option<String>,
    before: Option<String>,
) -> Result<Option<sparrow_core::EventInterval>> {
    match (from, before) {
        (None, None) => Ok(None),
        (Some(from), Some(before)) => {
            let from = sparrow_core::parse_instant(&from).map_err(|e| invalid(e.to_string()))?;
            let before =
                sparrow_core::parse_instant(&before).map_err(|e| invalid(e.to_string()))?;
            if from >= before {
                return Err(invalid("event_from must precede event_before"));
            }
            Ok(Some(sparrow_core::EventInterval { from, before }))
        }
        _ => Err(invalid(
            "event_from and event_before must be supplied together",
        )),
    }
}

pub(super) fn decode_fields(
    value: &Value,
) -> std::result::Result<Vec<sparrow_core::FieldConstraint>, DecodeError> {
    let values = value
        .as_array()
        .ok_or_else(|| DecodeError::invalid_value(value, "field constraint array"))?;
    if values.len() > 32 {
        return Err(DecodeError::custom("at most 32 field constraints"));
    }
    values
        .iter()
        .map(|value| {
            let mut fields = Record::new(value, "field constraint")?;
            let kind: Option<String> = fields.optional("kind")?;
            kind.as_deref().map(parse_kind).transpose()?;
            let result = sparrow_core::FieldConstraint {
                predicate: sparrow_core::JsonPredicate {
                    path: fields.optional("path")?,
                    kind,
                    lexical: fields.optional("lexical")?,
                    decoded: fields.optional("decoded")?,
                },
                parent_binding: fields.optional("parent_binding")?,
                parent_path: fields.optional("parent_path")?,
            };
            fields.deny_unknown()?;
            Ok(result)
        })
        .collect()
}

pub(super) fn read_fields(path: &str) -> Result<Vec<sparrow_core::FieldConstraint>> {
    decode_fields(&read_control_file(path)?).map_err(|e| invalid(e.to_string()))
}

pub(super) fn read_control_file(path: &str) -> Result<Value> {
    use std::io::Read;
    let file = std::fs::File::open(path).map_err(|e| invalid(e.to_string()))?;
    let metadata = file.metadata().map_err(|e| invalid(e.to_string()))?;
    if !metadata.is_file() || metadata.len() > 256 * 1024 {
        return Err(invalid(
            "query control file must be a regular file of at most 256 KiB",
        ));
    }
    let mut bytes = Vec::new();
    file.take(256 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| invalid(e.to_string()))?;
    if bytes.len() > 256 * 1024 {
        return Err(invalid("query control file grew beyond 256 KiB"));
    }
    let value = purrdf_lex::json::read_slice(
        &bytes,
        purrdf_lex::json::Limits {
            max_string_bytes: 256 * 1024,
            unique_members: true,
            max_depth: 8,
            max_values: 4096,
        },
    )
    .map_err(|e| invalid(e.to_string()))?;
    Ok(value)
}

impl Input {
    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        let mut call = client.records_search_request();
        let mut input = call.get().init_query();
        fill_scope(input.reborrow().init_scope(), &self.options)?;
        optional(input.reborrow().init_cursor(), &self.options.cursor);
        optional(input.reborrow().init_text(), &self.text);
        input.set_require_complete_index(self.options.require_complete_index);
        if let Some(interval) = self.event_interval {
            let mut interval_out = input.reborrow().init_event_interval().init_value();
            interval_out
                .reborrow()
                .init_from()
                .set_unix_millis(interval.from);
            interval_out.init_before().set_unix_millis(interval.before);
        }
        let mut fields = input.init_fields(self.fields.len() as u32);
        for (index, field) in self.fields.iter().enumerate() {
            let mut target = fields.reborrow().get(index as u32);
            optional(
                target.reborrow().init_parent_binding(),
                &field.parent_binding,
            );
            optional(target.reborrow().init_parent_path(), &field.parent_path);
            let mut target = target.init_predicate();
            optional(target.reborrow().init_path(), &field.predicate.path);
            optional(target.reborrow().init_lexical(), &field.predicate.lexical);
            optional(target.reborrow().init_decoded(), &field.predicate.decoded);
            if let Some(kind) = &field.predicate.kind {
                target
                    .init_kind()
                    .set_value(parse_kind(kind).map_err(|e| invalid(e.to_string()))?);
            }
        }
        let response = call.send().promise.await?;
        let found = match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => value?,
            wire::result::Error(error) => return Err(failure(error?)),
        };
        let basis = found.get_time_basis()?;
        if (basis == wire::TimeBasis::EventTime) != self.event_interval.is_some() {
            return Err(super::AgentError { code: "UNSUPPORTED".into(), message: "daemon did not honor the requested time basis; use the matching schema-10 executable".into(), retryable: false });
        }
        render(found)
    }
}

pub(super) fn render(found: wire::records_results::Reader<'_>) -> Result<Value> {
    let basis = found.get_time_basis()?;
    let text = match found.get_text()?.which()? {
        wire::optional_text_results::Absent(()) => Value::Null,
        wire::optional_text_results::Value(value) => Value::from(search::render(value?)?),
    };
    let rows = found
        .get_rows()?
        .iter()
        .map(|row| {
            let fields = row
                .get_fields()?
                .iter()
                .map(|field| {
                    let parent = match field.get_bound_parent()?.which()? {
                        wire::optional_occurrence::Absent(()) => Value::Null,
                        wire::optional_occurrence::Value(value) => Value::from(value.to_string()),
                    };
                    Ok(Object::new()
                        .with("predicate_index", field.get_predicate_index().to_string())
                        .with("occurrence", field.get_occurrence().to_string())
                        .with("path", field.get_path()?.to_str()?)
                        .with("kind", kind_name(field.get_kind()?))
                        .with("source_start", field.get_source_start().to_string())
                        .with("source_end", field.get_source_end().to_string())
                        .with(
                            "parent_binding",
                            optional_value(field.get_parent_binding()?)?,
                        )
                        .with("bound_parent", parent)
                        .into())
                })
                .collect::<Result<Vec<Value>>>()?;
            Ok(Object::new()
                .with("record_id", row.get_record_id()?.to_str()?)
                .with("artifact_id", row.get_artifact_id()?.to_str()?)
                .with("source", row.get_source()?.to_str()?)
                .with("received_at", instant(row.get_received_at()?)?)
                .with("byte_start", row.get_byte_start().to_string())
                .with("byte_end", row.get_byte_end().to_string())
                .with(
                    "diagnostics",
                    crate::diagnostics::render_snapshot(row.get_diagnostics()?)?,
                )
                .with(
                    "event_time",
                    super::source_time::event(row.get_event_time()?)?,
                )
                .with("fields", Value::array(fields))
                .into())
        })
        .collect::<Result<Vec<Value>>>()?;
    let coverage = found.get_event_coverage()?;
    let page = super::catalog::page(found.get_page()?)?;
    Ok(Object::new()
        .with("operation", "records_search")
        .with("page", page)
        .with(
            "event_coverage",
            Object::new()
                .with("evaluated", coverage.get_evaluated().to_string())
                .with("unknown", coverage.get_unknown().to_string())
                .with(
                    "outside_interval",
                    coverage.get_outside_interval().to_string(),
                )
                .with("membership_complete", coverage.get_membership_complete()),
        )
        .with("receipt", receipt(found.get_receipt()?)?)
        .with(
            "time_basis",
            match basis {
                wire::TimeBasis::ReceivedAt => "received_at",
                wire::TimeBasis::EventTime => "event_time",
            },
        )
        .with("field_visits", found.get_field_visits().to_string())
        .with(
            "qualified_records",
            super::render_count(found.get_qualified_records()?)?,
        )
        .with(
            "known_qualified_records",
            found.get_known_qualified_records().to_string(),
        )
        .with("rows", Value::array(rows))
        .with("text", text)
        .into())
}
