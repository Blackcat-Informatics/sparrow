// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use super::Result;
use super::{failure, wire};
use purrdf_lex::json::{Object, Value};
use wire::result;

pub(super) async fn execute(client: &wire::sparrow::Client) -> Result<Value> {
    let response = client.status_request().send().promise.await?;
    let value = match response.get()?.get_result()?.which()? {
        result::Ok(value) => value?,
        result::Error(error) => return Err(failure(error?)),
    };
    if value.get_schema_version() != 10
        || value.get_text_analyzer()?.is_empty()
        || value.get_text_profile()?.is_empty()
        || value.get_rdf_profile()?.is_empty()
    {
        return Err(super::AgentError { code: "UNSUPPORTED".into(), message: "daemon discovery contract requires schema version 10 with typed resumable imports, source-time profiles and event intervals; use a matching daemon executable".into(), retryable: false });
    }
    let output = Object::new()
        .with("store_id", value.get_store_id()?.to_str()?)
        .with("store_format", value.get_store_format()?.to_str()?)
        .with("schema_version", u64::from(value.get_schema_version()))
        .with("rdf_profile", value.get_rdf_profile()?.to_str()?)
        .with("text_analyzer", value.get_text_analyzer()?.to_str()?)
        .with("text_profile", value.get_text_profile()?.to_str()?)
        .with("physical_bytes", value.get_physical_bytes().to_string())
        .with("disk_budget", value.get_disk_budget().to_string())
        .with(
            "maintenance_reserve",
            value.get_maintenance_reserve().to_string(),
        )
        .with("live_records", value.get_live_records().to_string())
        .with(
            "unique_payload_chunks",
            value.get_unique_payload_chunks().to_string(),
        )
        .with(
            "unique_json_projections",
            value.get_unique_json_projections().to_string(),
        )
        .with("active_pins", value.get_active_pins().to_string())
        .with(
            "retry_window_seconds",
            value.get_retry_window_seconds().to_string(),
        )
        .with(
            "max_artifact_bytes",
            value.get_max_artifact_bytes().to_string(),
        )
        .with(
            "max_records_per_artifact",
            value.get_max_records_per_artifact().to_string(),
        )
        .with("max_pin_members", value.get_max_pin_members().to_string())
        .with("pin_read_handles", value.get_pin_read_handles().to_string())
        .with("pin_read_bytes", value.get_pin_read_bytes().to_string())
        .with(
            "max_pin_read_handles",
            value.get_max_pin_read_handles().to_string(),
        )
        .with(
            "max_pin_read_bytes",
            value.get_max_pin_read_bytes().to_string(),
        )
        .with(
            "pin_read_lease_ms",
            value.get_pin_read_lease_ms().to_string(),
        );
    Ok(output.into())
}
