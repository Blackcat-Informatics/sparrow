// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Shared CLI/MCP rendering of the typed durable upload checkpoint.

use super::{Result, wire};
use purrdf_lex::json::{Object, Value};

pub(crate) struct Input {
    pub store: String,
    pub id: String,
}

impl Input {
    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        let p = sparrow_client::get_import_progress(client, &self.store, &self.id).await?;
        Ok(Object::new()
            .with("store_id", p.store_id)
            .with("import_id", p.id)
            .with("request_id", p.request_id)
            .with("source", p.source)
            .with("format", sparrow_client::format_label(p.format))
            .with("byte_length", p.byte_length.to_string())
            .with("payload_id", p.payload_id)
            .with("received_bytes", p.received_bytes.to_string())
            .with(
                "expires_at",
                sparrow_core::format_instant(p.expires_at)
                    .map_err(|e| super::invalid(e.to_string()))?,
            )
            .with("sealed", p.sealed)
            .with("layout_complete", p.layout_complete)
            .with("layout_records", p.layout_records.to_string())
            .with("published_artifact", p.published_artifact)
            .into())
    }
}
