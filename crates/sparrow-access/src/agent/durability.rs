// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Agent rendering delegates synchronization to the shared typed client.
use super::{Result, wire};
use purrdf_lex::json::{Object, Value};

pub(super) fn watermark(value: &sparrow_client::DurabilityWatermark) -> Value {
    Object::new()
        .with("store_id", value.store_id.clone())
        .with("session_id", value.session_id.clone())
        .with("sequence", value.sequence.to_string())
        .into()
}

pub(super) async fn execute(
    client: &wire::sparrow::Client,
    through: &sparrow_client::DurabilityWatermark,
) -> Result<Value> {
    let reached = sparrow_client::flush_through(client, through).await?;
    Ok(Object::new()
        .with("durability", "durable")
        .with("reached", watermark(&reached))
        .into())
}
