// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One store/source-bound runtime listener read for CLI and MCP.

use super::{Result, failure, wire};
use purrdf_lex::json::{Object, Value};

pub(crate) struct Input {
    pub store: String,
    pub source: String,
    pub id: String,
}

impl Input {
    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        sparrow_client::Discovery::fetch(client).await?;
        let mut request = client.syslog_listener_get_request();
        let mut query = request.get().init_query();
        query.set_store_id(&self.store);
        query.set_schema_version(sparrow_client::SCHEMA_VERSION);
        query.set_source(&self.source);
        query.set_id(&self.id);
        let response = request.send().promise.await?;
        let listener = match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => value?,
            wire::result::Error(error) => return Err(failure(error?)),
        };
        Ok(Object::new()
            .with("store_id", self.store.clone())
            .with("operation", "syslog_listener_get")
            .with("listener", crate::syslog_wire::listener(listener)?)
            .into())
    }
}
