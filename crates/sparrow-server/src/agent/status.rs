// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use super::Result;
use super::{failure, wire};
use purrdf_lex::json::{Object, Value};
use wire::result;

pub(super) async fn execute(client: &wire::sparrow::Client) -> Result<Value> {
    let discovery = sparrow_client::Discovery::fetch(client).await?;
    let response = client.status_request().send().promise.await?;
    let value = match response.get()?.get_result()?.which()? {
        result::Ok(value) => value?,
        result::Error(error) => return Err(failure(error?)),
    };
    if value.get_schema_version() != discovery.schema_version
        || value.get_store_id()?.to_str()? != discovery.store_id
        || value.get_durability_status()?.get_session_id()?.to_str()?
            != discovery.durability_session
        || value.get_max_artifact_bytes() != discovery.max_artifact_bytes
        || value.get_max_records_per_artifact() != discovery.max_records_per_artifact
        || value.get_text_analyzer()?.to_str()? != discovery.text_analyzer
        || value.get_text_profile()?.to_str()? != discovery.text_profile
        || value.get_rdf_profile()?.to_str()? != discovery.rdf_profile
        || value.get_max_event_batch_bytes() != discovery.max_event_batch_bytes
        || value.get_max_event_batch_events() != discovery.max_event_batch_events
        || value.get_durability_status()?.get_max_pending_writes() != discovery.max_buffered_writes
        || value.get_durability_status()?.get_max_pending_bytes() != discovery.max_buffered_bytes
        || value.get_durability_status()?.get_flush_interval_ms() != discovery.flush_interval_ms
    {
        return Err(sparrow_client::Error::Protocol(
            "status contradicts discovered identity, profiles or admission limits",
        )
        .into());
    }
    let durability = value.get_durability_status()?;
    let state = Object::new()
        .with("session_id", durability.get_session_id()?.to_str()?)
        .with(
            "accepted_through",
            durability.get_accepted_through().to_string(),
        )
        .with(
            "durable_through",
            durability.get_durable_through().to_string(),
        )
        .with("pending_writes", u64::from(durability.get_pending_writes()))
        .with("pending_bytes", durability.get_pending_bytes().to_string())
        .with(
            "oldest_pending_ms",
            durability.get_oldest_pending_ms().to_string(),
        )
        .with("flush_lag_ms", durability.get_flush_lag_ms().to_string())
        .with(
            "flush_interval_ms",
            u64::from(durability.get_flush_interval_ms()),
        )
        .with(
            "max_pending_writes",
            u64::from(durability.get_max_pending_writes()),
        )
        .with(
            "max_pending_bytes",
            durability.get_max_pending_bytes().to_string(),
        )
        .with(
            "failure",
            match durability.get_failure()?.which()? {
                wire::optional_text::Absent(()) => Value::Null,
                wire::optional_text::Value(value) => Value::from(value?.to_str()?),
            },
        );
    let mut watches = Vec::new();
    for watch in value.get_consume_watches()? {
        let status = Object::new()
            .with("id", watch.get_id()?.to_str()?)
            .with("root", watch.get_root()?.to_str()?)
            .with("revision", watch.get_revision().to_string())
            .with("enabled", watch.get_enabled())
            .with(
                "definition",
                super::watch::definition(watch.get_definition()?)?,
            )
            .with(
                "coverage",
                match watch.get_coverage()? {
                    wire::WatchCoverage::Covered => "covered",
                    wire::WatchCoverage::Reconciling => "reconciling",
                },
            )
            .with("directories", u64::from(watch.get_directories()))
            .with("candidates", u64::from(watch.get_candidates()))
            .with("deferred", watch.get_deferred())
            .with("pending_claims", u64::from(watch.get_pending_claims()))
            .with("completed_claims", watch.get_completed_claims().to_string())
            .with("reconciliations", watch.get_reconciliations().to_string())
            .with("coverage_losses", watch.get_coverage_losses().to_string())
            .with(
                "failure",
                match watch.get_failure()?.which()? {
                    wire::optional_text::Absent(()) => Value::Null,
                    wire::optional_text::Value(value) => Value::from(value?.to_str()?),
                },
            );
        watches.push(Value::from(status));
    }
    let output = Object::new()
        .with(
            "pressure",
            crate::pressure::decode(value.get_pressure_status()?)?,
        )
        .with("consume_watches", Value::from(watches))
        .with("durability", Value::from(state))
        .with("store_id", value.get_store_id()?.to_str()?)
        .with("store_format", value.get_store_format()?.to_str()?)
        .with("schema_version", u64::from(value.get_schema_version()))
        .with(
            "max_event_batch_bytes",
            u64::from(value.get_max_event_batch_bytes()),
        )
        .with(
            "max_event_batch_events",
            u64::from(value.get_max_event_batch_events()),
        )
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
    let output = output
        .with(
            "artifact_read_handles",
            value.get_artifact_read_handles().to_string(),
        )
        .with(
            "artifact_read_bytes",
            value.get_artifact_read_bytes().to_string(),
        )
        .with(
            "max_artifact_read_handles",
            value.get_max_artifact_read_handles().to_string(),
        )
        .with(
            "max_artifact_read_bytes",
            value.get_max_artifact_read_bytes().to_string(),
        )
        .with(
            "artifact_read_lease_ms",
            value.get_artifact_read_lease_ms().to_string(),
        );
    Ok(output.into())
}
