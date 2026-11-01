// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One source mapping administration path for CLI and MCP.

use super::{Result, failure, wire};
use purrdf_lex::json::{
    Object, Value,
    record::{DecodeError, Record},
};
use sparrow_core::DiagnosticDimension;
use std::collections::BTreeMap;

pub(crate) struct Input {
    pub source: String,
    pub mappings: Option<BTreeMap<DiagnosticDimension, String>>,
}

pub(super) fn decode_mappings(
    value: &Value,
) -> std::result::Result<BTreeMap<DiagnosticDimension, String>, DecodeError> {
    let mut fields = Record::new(value, "diagnostic mappings")?;
    let mut mappings = BTreeMap::new();
    for name in ["service", "host", "severity", "trace_id", "span_id"] {
        if let Some(path) = fields.optional::<String>(name)? {
            if path.len() > 4096 || purrdf_lex::json_pointer::tokens(&path).is_none() {
                return Err(DecodeError::custom(
                    "mapping requires a valid native JSON pointer of at most 4096 bytes",
                ));
            }
            mappings.insert(
                DiagnosticDimension::from_name(name).map_err(DecodeError::custom)?,
                path,
            );
        }
    }
    fields.deny_unknown()?;
    if mappings.is_empty() {
        return Err(DecodeError::custom(
            "at least one explicit diagnostic mapping is required",
        ));
    }
    Ok(mappings)
}

impl Input {
    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        sparrow_client::Discovery::fetch(client).await?;
        if let Some(mappings) = &self.mappings {
            let mut request = client.diagnostic_define_request();
            let mut definition = request.get().init_definition();
            definition.set_source(&self.source);
            crate::diagnostics::encode_mappings(
                definition.init_mappings(mappings.len() as u32),
                mappings,
            );
            let response = request.send().promise.await?;
            return match response.get()?.get_result()?.which()? {
                wire::result::Ok(value) => Ok(crate::diagnostics::render_profile(value?)?),
                wire::result::Error(error) => Err(failure(error?)),
            };
        }
        let mut request = client.diagnostic_get_request();
        request.get().set_source(&self.source);
        let response = request.send().promise.await?;
        let result = match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => value?,
            wire::result::Error(error) => return Err(failure(error?)),
        };
        let profile = match result.which()? {
            wire::optional_diagnostic_profile::Absent(()) => Value::Null,
            wire::optional_diagnostic_profile::Value(value) => {
                crate::diagnostics::render_profile(value?)?
            }
        };
        Ok(Object::new().with("profile", profile).into())
    }
}
