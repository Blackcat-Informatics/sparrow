// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Cursor-only continuation through the existing typed page renderers.

use super::{Result, failure, wire};
use purrdf_lex::json::Value;

pub(crate) struct ReleaseInput {
    pub store: String,
    pub snapshot: String,
}
impl ReleaseInput {
    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        let receipt = sparrow_client::release_result(client, &self.store, &self.snapshot).await?;
        Ok(purrdf_lex::json::Object::new()
            .with("store_id", receipt.store_id)
            .with("snapshot_id", receipt.snapshot_id)
            .with("released", receipt.released)
            .into())
    }
}

pub(crate) struct Input {
    pub store: String,
    pub cursor: String,
}
impl Input {
    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        sparrow_client::Discovery::fetch(client).await?;
        let mut call = client.result_next_request();
        let mut query = call.get().init_query();
        query.set_store_id(&self.store);
        query.set_schema_version(sparrow_client::SCHEMA_VERSION);
        query.set_cursor(&self.cursor);
        let response = call.send().promise.await?;
        let page = match response.get()?.get_result()?.which()? {
            wire::result::Ok(page) => page?,
            wire::result::Error(error) => return Err(failure(error?)),
        };
        render(page)
    }
}

pub(super) fn render(page: wire::result_page::Reader<'_>) -> Result<Value> {
    match page.which()? {
        wire::result_page::Records(page) => super::records::render(page?),
        wire::result_page::Text(page) => super::search::page(page?),
        wire::result_page::Keys(page) => super::json::keys(page?),
        wire::result_page::Values(page) => super::json::values(page?),
        wire::result_page::Artifacts(page) => super::artifacts::render(page?),
        wire::result_page::Sources(page) => super::sources::render(page?),
        wire::result_page::Pins(page) => super::pin::read::render(page?),
        wire::result_page::Sparql(page) => super::sparql::render(page?),
    }
}
