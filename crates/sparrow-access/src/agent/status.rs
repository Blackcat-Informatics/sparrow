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
    let profiles = value.get_producer_profiles()?;
    for (actual, expected) in profiles.iter().zip(&discovery.producer_profiles) {
        if actual?.to_str()? != expected {
            return Err(sparrow_client::Error::Protocol(
                "status contradicts discovered producer profiles",
            )
            .into());
        }
    }
    if profiles.len() as usize != discovery.producer_profiles.len()
        || value.get_schema_version() != discovery.schema_version
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
        )
        .with("latches", durability.get_latches().to_string())
        .with("recoveries", durability.get_recoveries().to_string())
        .with(
            "writer_recoveries",
            durability.get_writer_recoveries().to_string(),
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
            .with("publish_hold", watch.get_publish_hold())
            .with("held_claims", u64::from(watch.get_held_claims()))
            .with(
                "oldest_held_first_seen",
                super::watch::clock_text(watch.get_oldest_held_first_seen()?)?,
            )
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
                .with("ceiling_wait", wait(socket.get_ceiling_wait()?))
                .with("lease_expiries", socket.get_lease_expiries().to_string())
                .into()
        }
    };
    let journals = if value.has_import_journals() {
        let journals = value.get_import_journals()?;
        Object::new()
            .with(
                "limits",
                import_journal_limits(Some(journals.get_limits()?.into())),
            )
            .with("active", journals.get_active())
            .into()
    } else {
        Value::Null
    };
    let output = Object::new()
        .with("ingest_socket", ingest_socket)
        .with("admission", admission(value.get_admission()?)?)
        .with("import_journals", journals)
        .with(
            "connection_lease",
            connection_lease(
                value
                    .has_connection_lease()
                    .then(|| value.get_connection_lease().map(|lease| lease.into()))
                    .transpose()?,
            ),
        )
        .with("floor_pause", floor_pause(value.get_floor_pause()?))
        .with(
            "staging_refusals",
            staging_refusals(value.get_staging_refusals()?)?,
        )
        .with("archive_halvings", value.get_archive_halvings().to_string())
        .with(
            "archive_worker",
            archive_worker(value.get_archive_worker()?)?,
        )
        .with(
            "pressure",
            crate::codec::render_pressure(value.get_pressure_status()?)?,
        )
        .with("recompression", recompression(value.get_recompression()?)?)
        .with("migrations", migrations(value.get_migrations()?)?)
        .with("query_only", value.get_query_only())
        .with(
            "instance_role",
            sparrow_client::instance_role_label(value.get_instance_role()?),
        )
        .with("retired_claim_count", value.get_retired_claim_count())
        .with("max_retired_claims", value.get_max_retired_claims())
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
            "producer_profiles",
            Value::array(discovery.producer_profiles.iter().map(String::as_str)),
        )
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
            crate::codec::render_syslog_listeners(value.get_syslog_listeners()?)?,
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

/// One wait counter: waits that blocked, waits in progress, the longest and those that timed out.
fn wait(status: wire::wait_status::Reader<'_>) -> Value {
    Object::new()
        .with("waits", status.get_waits().to_string())
        .with("waiting", u64::from(status.get_waiting()))
        .with("longest_wait_ms", status.get_longest_wait_ms().to_string())
        .with("timeouts", status.get_timeouts().to_string())
        .into()
}

/// The free-space pause counters (#257); a daemon that predates them reports zeros.
fn floor_pause(status: wire::floor_pause_status::Reader<'_>) -> Value {
    Object::new()
        .with("paused_since", status.get_paused_since().to_string())
        .with(
            "connections_accepted",
            status.get_connections_accepted().to_string(),
        )
        .with("writes_refused", status.get_writes_refused().to_string())
        .with("reads_served", status.get_reads_served().to_string())
        .with(
            "read_staging_refused",
            status.get_read_staging_refused().to_string(),
        )
        .into()
}

/// The archive worker's phase, watermark and last failure (#365); a daemon that predates them
/// reports `not_configured`, an empty watermark and no failure.
fn archive_worker(status: wire::archive_worker_status::Reader<'_>) -> Result<Value> {
    let phase = match status.get_phase()? {
        wire::ArchivePhase::NotConfigured => "not_configured",
        wire::ArchivePhase::Idle => "idle",
        wire::ArchivePhase::Preparing => "preparing",
        wire::ArchivePhase::Writing => "writing",
        wire::ArchivePhase::Verifying => "verifying",
        wire::ArchivePhase::Holding => "holding",
    };
    let failure = match status.get_failure()?.which()? {
        wire::optional_archive_failure::Absent(()) => Value::Null,
        wire::optional_archive_failure::Value(failure) => {
            let failure = failure?;
            Object::new()
                .with("code", failure.get_code()?.to_str()?)
                .with("segment", failure.get_segment()?.to_str()?)
                .with("reason", failure.get_reason()?.to_str()?)
                .with(
                    "since",
                    match failure.get_since()?.which()? {
                        wire::optional_instant::Absent(()) => Value::Null,
                        wire::optional_instant::Value(instant) => super::instant(instant?)?.into(),
                    },
                )
                .into()
        }
    };
    let held = named_record(
        status.get_held_record_id()?.to_str()?,
        status.get_held_reason()?.to_str()?,
    );
    let marked: Value = Object::new()
        .with("last_segment", status.get_last_segment_marked().to_string())
        .with("total", status.get_marked_total().to_string())
        .with(
            "last",
            named_record(
                status.get_last_marked_record_id()?.to_str()?,
                status.get_last_marked_reason()?.to_str()?,
            ),
        )
        .into();
    Ok(Object::new()
        .with("phase", phase)
        .with("through", status.get_through()?.to_str()?)
        .with("intent_in_flight", status.get_intent_in_flight())
        .with("failure", failure)
        .with("restarts", status.get_restarts().to_string())
        .with("held", held)
        .with("marked", marked)
        .into())
}

/// A record the archive names with its reason, or `null` when no record is named.
fn named_record(record_id: &str, reason: &str) -> Value {
    if record_id.is_empty() {
        Value::Null
    } else {
        Object::new()
            .with("record_id", record_id)
            .with("reason", reason)
            .into()
    }
}

/// RDF staging refusals and the latest one (#340); a daemon that predates them reports zeros.
fn staging_refusals(status: wire::staging_refusal_status::Reader<'_>) -> capnp::Result<Value> {
    Ok(Object::new()
        .with("refusals", status.get_refusals().to_string())
        .with("last_limit", status.get_last_limit()?.to_str()?)
        .with("last_source", status.get_last_source()?.to_str()?)
        .with("last_graph", status.get_last_graph()?.to_str()?)
        .with("last_bytes", status.get_last_bytes().to_string())
        .into())
}

/// Permit, import-slot and connection-ceiling waits (#246).
fn admission(status: wire::admission_status::Reader<'_>) -> capnp::Result<Value> {
    let connections = match status.get_rpc_connections()?.which()? {
        wire::optional_connection_ceiling_status::Absent(()) => Value::Null,
        wire::optional_connection_ceiling_status::Value(ceiling) => {
            let ceiling = ceiling?;
            Object::new()
                .with("max_connections", u64::from(ceiling.get_max_connections()))
                .with(
                    "active_connections",
                    u64::from(ceiling.get_active_connections()),
                )
                .with("ceiling_wait", wait(ceiling.get_ceiling_wait()?))
                .with("lease_expiries", ceiling.get_lease_expiries().to_string())
                .into()
        }
    };
    let tcp = if status.has_public_tcp_connections() {
        let tcp = status.get_public_tcp_connections()?;
        Object::new()
            .with("active_connections", tcp.get_active_connections())
            .with("idle_expiries", tcp.get_idle_expiries().to_string())
            .with("max_age_expiries", tcp.get_max_age_expiries().to_string())
            .into()
    } else {
        Value::Null
    };
    Ok(Object::new()
        .with("writer_reserve", u64::from(status.get_writer_reserve()))
        .with("reader_slots", u64::from(status.get_reader_slots()))
        .with(
            "writer_wait_limit_ms",
            u64::from(status.get_writer_wait_limit_ms()),
        )
        .with("writer_permit_wait", wait(status.get_writer_permit_wait()?))
        .with("reader_refusals", status.get_reader_refusals().to_string())
        .with("import_slot_wait", wait(status.get_import_slot_wait()?))
        .with(
            "import_slot_wait_limit_ms",
            u64::from(status.get_import_slot_wait_limit_ms()),
        )
        .with("rpc_connections", connections)
        .with("public_tcp_connections", tcp)
        .into())
}

fn connection_lease(limits: Option<sparrow_client::ConnectionLeaseLimits>) -> Value {
    limits.map_or(Value::Null, |limits| {
        Object::new()
            .with("idle_seconds", limits.idle_seconds)
            .with("tcp_first_input_seconds", limits.tcp_first_input_seconds)
            .with("max_age_seconds", limits.max_age_seconds)
            .into()
    })
}

fn import_journal_limits(limits: Option<sparrow_client::ImportJournalLimits>) -> Value {
    limits.map_or(Value::Null, |limits| {
        Object::new()
            .with("total", limits.total)
            .with("anonymous_threshold", limits.anonymous_threshold)
            .into()
    })
}

/// The identity the connection learned when it opened, which the ingest socket admits: the store
/// and its role, so a sender can refuse a store that is not `scratch` before it sends anything.
pub(super) fn discovery(discovery: &sparrow_client::Discovery) -> Value {
    let object = Object::new()
        .with("operation", "discover")
        .with("store_id", discovery.store_id.as_str())
        .with("schema_version", discovery.schema_version)
        .with(
            "connection_lease",
            connection_lease(discovery.connection_lease),
        )
        .with("max_import_bytes", discovery.max_import_bytes.to_string())
        .with(
            "import_journal_limits",
            import_journal_limits(discovery.import_journal_limits),
        )
        .with(
            "producer_profiles",
            Value::array(discovery.producer_profiles.iter().map(String::as_str)),
        );
    match discovery.unknown_instance_role {
        Some(ordinal) => object
            .with("instance_role", "unknown")
            .with("instance_role_ordinal", u64::from(ordinal))
            .into(),
        None => object
            .with(
                "instance_role",
                sparrow_client::instance_role_label(discovery.instance_role),
            )
            .into(),
    }
}

#[cfg(test)]
mod archive_worker_tests {
    use super::{Object, Value, archive_worker, wire};

    /// Render an `ArchiveWorkerStatus` message through the reader that `status` uses.
    fn rendered(fill: impl FnOnce(wire::archive_worker_status::Builder<'_>)) -> Object {
        let mut message = capnp::message::Builder::new_default();
        let mut root = message.init_root::<wire::archive_worker_status::Builder<'_>>();
        root.set_phase(wire::ArchivePhase::Idle);
        root.reborrow().init_failure().set_absent(());
        fill(root);
        let reader = message
            .get_root_as_reader::<wire::archive_worker_status::Reader<'_>>()
            .unwrap();
        match archive_worker(reader).unwrap() {
            Value::Object(ref object) => object.clone(),
            other => panic!("status renders as an object, got {other}"),
        }
    }

    /// INVARIANT: the status JSON names the held record with its reason, and reports the mark counts
    /// and the last marked record. With nothing held or marked, both are `null` and zero.
    #[test]
    fn archive_status_renders_holds_and_marks() {
        let object = rendered(|mut root| {
            root.set_held_record_id("record:big");
            root.set_held_reason("oversized");
            root.set_last_segment_marked(2);
            root.set_marked_total(5);
            root.set_last_marked_record_id("record:last");
            root.set_last_marked_reason("projection_failed");
        });
        let held = match object.get("held") {
            Some(Value::Object(held)) => held,
            other => panic!("held should be an object, got {other:?}"),
        };
        assert_eq!(
            held.get("record_id").and_then(Value::as_str),
            Some("record:big")
        );
        assert_eq!(
            held.get("reason").and_then(Value::as_str),
            Some("oversized")
        );
        let marked = match object.get("marked") {
            Some(Value::Object(marked)) => marked,
            other => panic!("marked should be an object, got {other:?}"),
        };
        assert_eq!(
            marked.get("last_segment").and_then(Value::as_str),
            Some("2")
        );
        assert_eq!(marked.get("total").and_then(Value::as_str), Some("5"));
        let last = match marked.get("last") {
            Some(Value::Object(last)) => last,
            other => panic!("last should be an object, got {other:?}"),
        };
        assert_eq!(
            last.get("record_id").and_then(Value::as_str),
            Some("record:last")
        );
        assert_eq!(
            last.get("reason").and_then(Value::as_str),
            Some("projection_failed")
        );

        let empty = rendered(|_| {});
        assert!(
            matches!(empty.get("held"), Some(Value::Null)),
            "no hold renders null"
        );
        let empty_marked = match empty.get("marked") {
            Some(Value::Object(marked)) => marked,
            other => panic!("marked should be an object, got {other:?}"),
        };
        assert_eq!(
            empty_marked.get("last_segment").and_then(Value::as_str),
            Some("0")
        );
        assert_eq!(empty_marked.get("total").and_then(Value::as_str), Some("0"));
        assert!(matches!(empty_marked.get("last"), Some(Value::Null)));
    }
}
