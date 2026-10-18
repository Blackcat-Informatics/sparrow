// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One capability document for CLI and MCP over either typed transport.

use super::{Result, status, tool, wire};
use capnp::introspect::{Introspect, TypeVariant};
use purrdf_lex::json::{Object, Value};

pub(super) async fn execute(client: &wire::sparrow::Client) -> Result<Value> {
    let status = status::execute(client).await?;
    let TypeVariant::Enum(raw) = wire::Format::introspect().which() else {
        return Err(sparrow_client::Error::Protocol("format schema is not an enum").into());
    };
    let formats = capnp::schema::EnumSchema::new(raw)
        .get_enumerants()?
        .iter()
        .map(|value| {
            wire::Format::try_from(value.get_ordinal())
                .map(sparrow_client::format_label)
                .map(Value::from)
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(Object::new()
        .with("status", status)
        .with("tools", tool::definitions())
        .with("formats", Value::array(formats))
        .with(
            "vocabulary",
            Object::new().with(
                "occurrence_namespace",
                sparrow_core::RDF_VOCABULARY_NAMESPACE,
            ),
        )
        .with("transport", "capnp")
        .with("transports", Value::array(["unix", "tcp"]))
        .with("retention_days", 30_u64)
        .with("time_basis", "received_at")
        .with(
            "records_time_bases",
            Value::array(["received_at", "event_time"]),
        )
        .with("mcp_protocol", "2025-11-25")
        .into())
}
