// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Shared CLI/MCP file-generation read through the generated capability.

use super::{Result, failure, invalid, wire};
use purrdf_lex::json::Value;

pub(crate) struct Input {
    pub store: String,
    pub id: String,
}
impl Input {
    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        if !self.id.starts_with("file-generation:") || self.id.len() > 128 {
            return Err(invalid(
                "file generation requires an opaque file-generation ID",
            ));
        }
        sparrow_client::Discovery::fetch(client).await?;
        let mut request = client.tail_generation_get_request();
        let mut query = request.get().init_query();
        query.set_store_id(&self.store);
        query.set_id(&self.id);
        query.set_schema_version(sparrow_client::SCHEMA_VERSION);
        let response = request.send().promise.await?;
        match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => crate::tail_wire::render_generation(value?),
            wire::result::Error(error) => Err(failure(error?)),
        }
    }
}
