// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Native JSON discovery reply rendering shared by direct and cursor-only reads.
use super::{Result, instant, kind_name, optional_value, receipt, wire};
use purrdf_lex::json::{Object, Value};

pub(super) fn values(found: wire::values::Reader<'_>) -> Result<Value> {
    let rows = found
        .get_rows()?
        .iter()
        .map(|row| {
            Ok(Value::from(
                Object::new()
                    .with("record_id", row.get_record_id()?.to_str()?)
                    .with("artifact_id", row.get_artifact_id()?.to_str()?)
                    .with("source", row.get_source()?.to_str()?)
                    .with("received_at", instant(row.get_received_at()?)?)
                    .with("source_start", row.get_source_start().to_string())
                    .with("source_end", row.get_source_end().to_string())
                    .with("occurrence", row.get_occurrence().to_string())
                    .with("path", row.get_path()?.to_str()?)
                    .with("kind", kind_name(row.get_kind()?))
                    .with("lexical", optional_value(row.get_lexical()?)?)
                    .with("decoded", optional_value(row.get_decoded()?)?),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Object::new()
        .with("operation", "values_search")
        .with("receipt", receipt(found.get_receipt()?)?)
        .with("page", super::catalog::page(found.get_page()?)?)
        .with("rows", Value::array(rows))
        .into())
}

pub(super) fn keys(found: wire::keys::Reader<'_>) -> Result<Value> {
    let rows = found
        .get_rows()?
        .iter()
        .map(|row| {
            Ok(Value::from(
                Object::new()
                    .with("path", row.get_path()?.to_str()?)
                    .with("kind", kind_name(row.get_kind()?))
                    .with("occurrences", row.get_occurrences().to_string())
                    .with("records", row.get_records().to_string()),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Object::new()
        .with("operation", "keys_list")
        .with("receipt", receipt(found.get_receipt()?)?)
        .with("page", super::catalog::page(found.get_page()?)?)
        .with("rows", Value::array(rows))
        .into())
}
