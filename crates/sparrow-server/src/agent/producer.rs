// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One generated producer-window read and rendering path for CLI and MCP.

use super::{Result, failure, instant, invalid, wire};
use purrdf_lex::json::{Object, Value};

pub(crate) struct Input {
    pub store: String,
    pub id: String,
}

impl Input {
    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        if !self.id.starts_with("producer-stream:") || self.id.len() > 128 {
            return Err(invalid(
                "producer stream requires an opaque producer-stream ID",
            ));
        }
        sparrow_client::Discovery::fetch(client).await?;
        let mut request = client.producer_stream_get_request();
        let mut query = request.get().init_query();
        query.set_store_id(&self.store);
        query.set_id(&self.id);
        query.set_schema_version(sparrow_client::SCHEMA_VERSION);
        let response = request.send().promise.await?;
        let stream = match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => value?,
            wire::result::Error(error) => return Err(failure(error?)),
        };
        let mut gaps = Vec::new();
        for gap in stream.get_gaps()? {
            gaps.push(Value::from(
                Object::new()
                    .with("first", gap.get_first().to_string())
                    .with("last", gap.get_last().to_string()),
            ));
        }
        Ok(Object::new()
            .with("store_id", stream.get_store_id()?.to_str()?)
            .with("stream_id", stream.get_id()?.to_str()?)
            .with("source", stream.get_source()?.to_str()?)
            .with(
                "producer_instance",
                purrdf_hash::hex::encode(stream.get_instance()?),
            )
            .with("first_sequence", stream.get_first_sequence().to_string())
            .with("last_sequence", stream.get_last_sequence().to_string())
            .with("accepted_events", stream.get_accepted_events().to_string())
            .with(
                "unobserved_sequences",
                stream.get_unobserved_sequences().to_string(),
            )
            .with("gaps", gaps)
            .with("gaps_complete", stream.get_gaps_complete())
            .with("continuity_unknown", stream.get_continuity_unknown())
            .with(
                "first_received_at",
                instant(stream.get_first_received_at()?)?,
            )
            .with("last_received_at", instant(stream.get_last_received_at()?)?)
            .with("expires_at", instant(stream.get_expires_at()?)?)
            .into())
    }
}
