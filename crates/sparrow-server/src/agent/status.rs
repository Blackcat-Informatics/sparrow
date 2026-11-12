// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

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
        || value.get_max_inline_artifact_bytes() != discovery.max_inline_artifact_bytes
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
            .with(
                "tail_generation_ids",
                Value::array(
                    watch
                        .get_tail_generation_ids()?
                        .iter()
                        .map(|id| -> Result<Value> { Ok(id?.to_str()?.into()) })
                        .collect::<Result<Vec<_>>>()?,
                ),
            )
            .with("tail_admission_waiting", watch.get_tail_admission_waiting())
            .with("tail_read_bytes", watch.get_tail_read_bytes().to_string())
            .with("tail_descriptors", u64::from(watch.get_tail_descriptors()))
            .with(
                "tail_checkpoint_records",
                watch.get_tail_checkpoint_records().to_string(),
            )
            .with("candidates", u64::from(watch.get_candidates()))
            .with("deferred", watch.get_deferred())
            .with("pending_claims", u64::from(watch.get_pending_claims()))
            .with(
                "retired_claims",
                super::watch::retired_claims(watch.get_retired_claims()?)?,
            )
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
    let ingest_socket = match value.get_ingest_socket()?.which()? {
        wire::optional_ingest_socket_status::Absent(()) => Value::Null,
        wire::optional_ingest_socket_status::Value(socket) => {
            let socket = socket?;
            Object::new()
                .with("path", socket.get_path()?.to_str()?)
                .with("gid", u64::from(socket.get_gid()))
                .with("max_connections", u64::from(socket.get_max_connections()))
                .with(
                    "active_connections",
                    u64::from(socket.get_active_connections()),
                )
                .with(
                    "refused_connections",
                    socket.get_refused_connections().to_string(),
                )
                .into()
        }
    };
    let output = Object::new()
        .with("ingest_socket", ingest_socket)
        .with(
            "pressure",
            crate::pressure::decode(value.get_pressure_status()?)?,
        )
        .with("recompression", recompression(value.get_recompression()?)?)
        .with("migrations", migrations(value.get_migrations()?)?)
        .with("query_only", value.get_query_only())
        .with("backup", super::backup::render(value.get_backup()?)?)
        .with(
            "stream_evidence_loss",
            stream_evidence_loss(value.get_stream_evidence_loss()?)?,
        )
        .with(
            "source_time_loss",
            stream_evidence_loss(value.get_source_time_loss()?)?,
        )
        .with(
            "tail_registry_evidence",
            tail_registry_evidence(value.get_tail_registry_evidence()?)?,
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
        .with(
            "counts",
            if value.get_counts_pending() {
                "pending"
            } else {
                "exact"
            },
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
            "retention_seconds",
            value.get_retention_seconds().to_string(),
        )
        .with(
            "max_inline_artifact_bytes",
            value.get_max_inline_artifact_bytes().to_string(),
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
    let output = output
        .with(
            "source_read_handles",
            value.get_source_read_handles().to_string(),
        )
        .with(
            "source_read_bytes",
            value.get_source_read_bytes().to_string(),
        )
        .with(
            "max_source_read_handles",
            value.get_max_source_read_handles().to_string(),
        )
        .with(
            "max_source_read_bytes",
            value.get_max_source_read_bytes().to_string(),
        )
        .with(
            "source_read_lease_ms",
            value.get_source_read_lease_ms().to_string(),
        );
    let output = output
        .with(
            "syslog_listeners",
            crate::syslog_wire::listeners(value.get_syslog_listeners()?)?,
        )
        .with(
            "producer_limits",
            Object::new()
                .with(
                    "max_streams",
                    u64::from(value.get_producer_limits()?.get_max_streams()),
                )
                .with(
                    "max_gap_intervals",
                    u64::from(value.get_producer_limits()?.get_max_gap_intervals()),
                )
                .with(
                    "max_registry_bytes",
                    value
                        .get_producer_limits()?
                        .get_max_registry_bytes()
                        .to_string(),
                )
                .with(
                    "metadata_retention_seconds",
                    value
                        .get_producer_limits()?
                        .get_metadata_retention_seconds()
                        .to_string(),
                ),
        )
        .with(
            "sparql_read_handles",
            value.get_sparql_read_handles().to_string(),
        )
        .with(
            "sparql_read_bytes",
            value.get_sparql_read_bytes().to_string(),
        )
        .with(
            "max_sparql_read_handles",
            value.get_max_sparql_read_handles().to_string(),
        )
        .with(
            "max_sparql_read_bytes",
            value.get_max_sparql_read_bytes().to_string(),
        )
        .with(
            "sparql_read_lease_ms",
            value.get_sparql_read_lease_ms().to_string(),
        )
        .with(
            "record_read_handles",
            value.get_record_read_handles().to_string(),
        )
        .with(
            "record_read_bytes",
            value.get_record_read_bytes().to_string(),
        )
        .with(
            "max_record_read_handles",
            value.get_max_record_read_handles().to_string(),
        )
        .with(
            "max_record_read_bytes",
            value.get_max_record_read_bytes().to_string(),
        )
        .with(
            "record_read_lease_ms",
            value.get_record_read_lease_ms().to_string(),
        );
    Ok(output.into())
}

/// Gap evidence lost to a full producer-stream or receiver-window registry (#215).
fn stream_evidence_loss(value: wire::stream_evidence_loss::Reader<'_>) -> Result<Value> {
    let mut named = Vec::new();
    for stream in value.get_named()? {
        named.push(Value::from(
            Object::new()
                .with(
                    "kind",
                    match stream.get_kind()? {
                        wire::LostStreamKind::Producer => "producer",
                        wire::LostStreamKind::Receiver => "receiver",
                        wire::LostStreamKind::SourceTime => "source_time",
                    },
                )
                .with("source", stream.get_source()?.to_str()?)
                .with("instance", stream.get_instance()?.to_str()?)
                .with("batches", stream.get_batches().to_string())
                .with("events", stream.get_events().to_string()),
        ));
    }
    Ok(Object::new()
        .with("max_named", u64::from(value.get_max_named()))
        .with("named", Value::from(named))
        .with("other_streams", value.get_other_streams().to_string())
        .with("other_batches", value.get_other_batches().to_string())
        .with("other_events", value.get_other_events().to_string())
        .with("other_streams_exact", value.get_other_streams_exact())
        .with("total_batches", value.get_total_batches().to_string())
        .with("total_events", value.get_total_events().to_string())
        .into())
}

/// Files tailed without a stored checkpoint, and rows retired for room (#224).
fn tail_registry_evidence(value: wire::tail_registry_evidence::Reader<'_>) -> Result<Value> {
    let mut named = Vec::new();
    for file in value.get_named()? {
        named.push(Value::from(
            Object::new()
                .with("watch", file.get_watch()?.to_str()?)
                .with("path", file.get_path()?.to_str()?)
                .with("source", file.get_source()?.to_str()?)
                .with("generations", file.get_generations().to_string())
                .with("records", file.get_records().to_string())
                .with("bytes", file.get_bytes().to_string()),
        ));
    }
    Ok(Object::new()
        .with("max_named", u64::from(value.get_max_named()))
        .with("named", Value::from(named))
        .with("other_files", value.get_other_files().to_string())
        .with(
            "other_generations",
            value.get_other_generations().to_string(),
        )
        .with("other_records", value.get_other_records().to_string())
        .with("other_bytes", value.get_other_bytes().to_string())
        .with("other_files_exact", value.get_other_files_exact())
        .with(
            "total_generations",
            value.get_total_generations().to_string(),
        )
        .with("total_records", value.get_total_records().to_string())
        .with("total_bytes", value.get_total_bytes().to_string())
        .with("retired_terminal", value.get_retired_terminal().to_string())
        .with(
            "retired_duplicate_risk",
            value.get_retired_duplicate_risk().to_string(),
        )
        .into())
}

fn recompression(value: wire::recompression_status::Reader<'_>) -> Result<Value> {
    let phase = match value.get_phase()? {
        wire::RecompressionPhase::Running => "running",
        wire::RecompressionPhase::Paused => "paused",
        wire::RecompressionPhase::Complete => "complete",
    };
    Ok(Object::new()
        .with("profile", value.get_profile()?.to_str()?)
        .with("phase", phase)
        .with("remaining_bytes", value.get_remaining_bytes().to_string())
        .with("waiting", super::optional_value(value.get_waiting()?)?)
        .with("failure", super::optional_value(value.get_failure()?)?)
        .into())
}

fn migrations(
    value: capnp::struct_list::Reader<'_, wire::migration_status::Owned>,
) -> Result<Value> {
    let mut rows = Vec::with_capacity(value.len() as usize);
    for migration in value {
        rows.push(Value::from(
            Object::new()
                .with("name", migration.get_name()?.to_str()?)
                .with("done", migration.get_done().to_string())
                .with(
                    "total",
                    if migration.get_total_pending() {
                        "counting".to_owned()
                    } else {
                        migration.get_total().to_string()
                    },
                )
                .with(
                    "watermark",
                    super::optional_value(migration.get_watermark()?)?,
                )
                .with("failure", super::optional_value(migration.get_failure()?)?),
        ));
    }
    Ok(Value::from(rows))
}
