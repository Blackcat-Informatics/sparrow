// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One payload-id lookup client and renderer shared by CLI and MCP (#323).

use super::{Result, failure, invalid, optional, wire};
use purrdf_lex::json::{Object, Value};

pub(crate) struct Input {
    pub store: String,
    pub payload_id: String,
    pub cursor: Option<String>,
    pub page_size: u32,
}

impl Input {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        let [store, payload_id, flags @ ..] = args else {
            return Err(invalid("payload requires STORE_ID PAYLOAD_ID"));
        };
        let mut input = Self {
            store: store.clone(),
            payload_id: payload_id.clone(),
            cursor: None,
            page_size: 200,
        };
        let mut flags = flags.iter();
        while let Some(flag) = flags.next() {
            let value = flags
                .next()
                .ok_or_else(|| invalid(format!("missing value for {flag}")))?;
            match flag.as_str() {
                "--cursor" => input.cursor = Some(value.clone()),
                "--page-size" => {
                    input.page_size = value
                        .parse()
                        .map_err(|_| invalid("page-size must be a positive integer"))?;
                }
                _ => return Err(invalid(format!("unknown payload option: {flag}"))),
            }
        }
        Ok(input)
    }

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        sparrow_client::Discovery::fetch(client).await?;
        let mut request = client.payload_lookup_request();
        let mut query = request.get().init_query();
        query.set_store_id(&self.store);
        query.set_schema_version(sparrow_client::SCHEMA_VERSION);
        query.set_payload_id(&self.payload_id);
        optional(query.reborrow().init_cursor(), &self.cursor);
        query.set_page_size(self.page_size);
        let response = request.send().promise.await?;
        let page = match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => value?,
            wire::result::Error(error) => return Err(failure(error?)),
        };
        let mut artifacts = Vec::new();
        for id in page.get_artifact_ids()? {
            artifacts.push(Value::from(id?.to_str()?));
        }
        let next = match page.get_next()?.which()? {
            wire::optional_text::Absent(()) => Value::Null,
            wire::optional_text::Value(value) => Value::from(value?.to_str()?),
        };
        let complete = page.get_complete();
        Ok(Object::new()
            .with("store_id", page.get_store_id()?.to_str()?)
            .with("payload_id", page.get_payload_id()?.to_str()?)
            // Exact once the payload-index migration is complete; until then every listed
            // artifact is held and an unlisted one is unknown.
            .with(
                "answer_certainty",
                if complete { "exact" } else { "pending" },
            )
            .with("artifact_ids", Value::array(artifacts))
            .with("next_cursor", next)
            .with("operation", "payload_lookup")
            .into())
    }
}
