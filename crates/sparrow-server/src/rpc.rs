// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Typed adapters over the shared evidence owner; no transport JSON envelopes.

use crate::sparrow_capnp as wire;
use purrdf_lex::json::{Value, record::Record as JsonRecord};
use sparrow_core::{
    Artifact, Error, JsonQuery, Pin, PinPageBody, PinRead, PinSelector, PinSummary, QueryScope,
    Record, Store,
};
use std::{rc::Rc, sync::Arc};
use tokio::sync::Semaphore;

fn invalid(error: impl std::fmt::Display) -> Error {
    Error::new("INVALID_ARGUMENT", error.to_string())
}

/// Read the server's millisecond acceptance clock.
pub fn now() -> Result<u64, Error> {
    sparrow_client::unix_millis_now().map_err(invalid)
}

/// Shared storage owner exposed through the generated RPC capability.
pub struct Rpc {
    store: Store,
    requests: Arc<Semaphore>,
    max_artifact_bytes: usize,
    max_pin_members: usize,
    watches: crate::watches::WatchRegistry,
    watch_controller: Option<crate::WatchController>,
}

impl Rpc {
    /// Construct the one typed adapter with explicit admission limits.
    pub fn new(
        store: Store,
        requests: Arc<Semaphore>,
        max_artifact_bytes: usize,
        max_pin_members: usize,
    ) -> Self {
        Self {
            store,
            requests,
            max_artifact_bytes,
            max_pin_members,
            watches: crate::watches::WatchRegistry::default(),
            watch_controller: None,
        }
    }

    /// Attach the daemon's one bounded configured-watch snapshot owner.
    pub fn with_watches(mut self, watches: crate::watches::WatchRegistry) -> Self {
        self.watches = watches;
        self
    }

    /// Attach the one bounded administration worker; requires two connection slots.
    pub fn with_watch_controller(mut self, controller: crate::WatchController) -> Self {
        self.watch_controller = Some(controller);
        self
    }

    async fn mutate_watch(
        &self,
        store: &str,
        action: crate::watch_admin::Action,
    ) -> Result<crate::watch_admin::ManagedWatch, Error> {
        self.check_store(store)?;
        let controller = self.controller()?;
        // The finite command channel owns admission. Holding a storage work permit
        // here could deadlock the worker's own typed publication RPC.
        controller.submit(action).await
    }

    fn controller(&self) -> Result<&crate::WatchController, Error> {
        self.watch_controller.as_ref().ok_or_else(|| crate::watch_admin::failure("UNSUPPORTED", "watch administration requires an owned worker and at least two configured RPC connections"))
    }

    async fn handoff_request(
        &self,
        value: wire::watch_handoff::Reader<'_>,
        out: wire::result::Builder<'_, wire::handoff_receipt::Owned>,
        read_only: bool,
    ) -> capnp::Result<()> {
        let request = match sparrow_client::CompletedHandoff::decode(value) {
            Ok(request) => request,
            Err(error) => {
                failure(out, invalid(error));
                return Ok(());
            }
        };
        let result = async {
            self.check_store(&request.store_id)?;
            let intent = crate::handoff::intent(&request)?;
            if read_only {
                self.work(move |store, now| {
                    store.consumption_handoff_get(&intent, now)?.ok_or_else(|| {
                        Error::new("NOT_FOUND", "handoff request has not been admitted")
                    })
                })
                .await
            } else {
                self.controller()?.handoff(intent).await
            }
        }
        .await;
        crate::handoff::result(out, &request, result)
    }

    async fn work<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Store, u64) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let permit = self
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error {
                code: "RESOURCE_LIMIT",
                message: "RPC operation concurrency is at its configured limit".into(),
            })?;
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation(store, now()?)
        })
        .await
        .map_err(|e| Error {
            code: "STORAGE_FAILURE",
            message: format!("storage task failed: {e}"),
        })?
    }

    fn scoped_id(&self, evidence: wire::evidence::Reader<'_>) -> Result<String, Error> {
        let store = evidence
            .get_store_id()
            .map_err(invalid)?
            .to_str()
            .map_err(invalid)?;
        match evidence.which().map_err(invalid)? {
            wire::evidence::Artifact(id) => {
                let id = id.map_err(invalid)?.to_str().map_err(invalid)?;
                self.reference(store, id, "artifact:")
            }
            wire::evidence::Record(id) => {
                let id = id.map_err(invalid)?.to_str().map_err(invalid)?;
                self.reference(store, id, "record:")
            }
        }
    }

    fn reference(&self, store: &str, id: &str, prefix: &str) -> Result<String, Error> {
        self.check_store(store)?;
        if !id.starts_with(prefix) {
            return Err(invalid("reference has the wrong evidence kind"));
        }
        Ok(id.to_owned())
    }

    fn import_reference(&self, value: wire::import_ref::Reader<'_>) -> Result<String, Error> {
        let store = value
            .get_store_id()
            .map_err(invalid)?
            .to_str()
            .map_err(invalid)?;
        let id = value.get_id().map_err(invalid)?.to_str().map_err(invalid)?;
        if store.len() > 128 || id.len() != 71 {
            return Err(invalid("require a bounded store-scoped import identity"));
        }
        self.reference(store, id, "import:")
    }

    fn check_store(&self, store: &str) -> Result<(), Error> {
        if store != self.store.store_id()? {
            return Err(Error {
                code: "STORE_MISMATCH",
                message: "reference belongs to a different store".into(),
            });
        }
        Ok(())
    }
}

fn json_kind(kind: &str) -> capnp::Result<wire::JsonKind> {
    match kind {
        "string" => Ok(wire::JsonKind::String),
        "number" => Ok(wire::JsonKind::Number),
        "true" => Ok(wire::JsonKind::BooleanTrue),
        "false" => Ok(wire::JsonKind::BooleanFalse),
        "null" => Ok(wire::JsonKind::Null),
        "array" => Ok(wire::JsonKind::Array),
        "object" => Ok(wire::JsonKind::Object),
        _ => Err(capnp::Error::failed("invalid native JSON kind".into())),
    }
}

fn optional_kind(value: wire::optional_kind::Reader<'_>) -> capnp::Result<Option<String>> {
    match value.which()? {
        wire::optional_kind::Absent(()) => Ok(None),
        wire::optional_kind::Value(kind) => Ok(Some(
            match kind? {
                wire::JsonKind::String => "string",
                wire::JsonKind::Number => "number",
                wire::JsonKind::BooleanTrue => "true",
                wire::JsonKind::BooleanFalse => "false",
                wire::JsonKind::Null => "null",
                wire::JsonKind::Array => "array",
                wire::JsonKind::Object => "object",
            }
            .to_owned(),
        )),
    }
}

pub(crate) fn failure<T: capnp::traits::Owned + 'static>(
    output: wire::result::Builder<'_, T>,
    error: Error,
) {
    let mut out = output.init_error();
    out.set_code(error.code);
    out.set_message(&error.message);
    out.set_retryable(matches!(error.code, "STORAGE_PRESSURE" | "RESOURCE_LIMIT"));
}

pub(crate) use sparrow_client::{format_label as format, parse_format as stored_format};

fn artifact(
    mut out: wire::artifact::Builder<'_>,
    value: Artifact,
    store: &str,
) -> capnp::Result<()> {
    out.set_durability(if value.buffered.is_some() {
        wire::Durability::Buffered
    } else {
        wire::Durability::Durable
    });
    if let Some(token) = value.buffered {
        durability_watermark(out.reborrow().init_buffered_through().init_value(), token)?;
    }
    out.set_store_id(store);
    out.set_id(&value.id);
    out.set_source(&value.source);
    out.set_format(stored_format(&value.format)?);
    out.reborrow()
        .init_received_at()
        .set_unix_millis(value.received_at);
    out.reborrow()
        .init_expires_at()
        .set_unix_millis(value.expires_at);
    out.set_byte_length(value.byte_length);
    out.set_payload_id(&value.payload_id);
    let mut records = out.init_record_ids(
        value
            .record_ids
            .len()
            .try_into()
            .map_err(|_| capnp::Error::failed("record count overflow".into()))?,
    );
    for (index, id) in value.record_ids.iter().enumerate() {
        records.set(index as u32, id);
    }
    Ok(())
}

fn durability_watermark(
    out: wire::durability_watermark::Builder<'_>,
    value: sparrow_core::DurabilityWatermark,
) -> capnp::Result<()> {
    sparrow_client::DurabilityWatermark {
        store_id: value.store_id,
        session_id: value.session_id,
        sequence: value.sequence,
    }
    .encode(out)
    .map_err(|error| capnp::Error::failed(error.to_string()))
}

fn durability_mode(value: wire::Durability) -> sparrow_core::Durability {
    match value {
        wire::Durability::Buffered => sparrow_core::Durability::Buffered,
        wire::Durability::Durable => sparrow_core::Durability::Durable,
    }
}

fn import_progress(
    mut out: wire::import_progress::Builder<'_>,
    value: sparrow_core::ImportManifest,
    store: &str,
) -> capnp::Result<()> {
    out.set_store_id(store);
    out.set_id(&value.id);
    out.set_request_id(&value.request_id);
    out.set_source(&value.source);
    out.set_format(stored_format(&value.format)?);
    out.set_byte_length(value.byte_length);
    out.set_payload_id(&value.payload_id);
    out.set_received_bytes(value.received_bytes);
    out.reborrow()
        .init_expires_at()
        .set_unix_millis(value.expires_at);
    out.set_sealed(value.sealed);
    out.set_layout_complete(value.layout_complete);
    out.set_layout_records(value.layout_records);
    match value.published_artifact {
        Some(id) => out.init_published_artifact().set_value(id),
        None => out.init_published_artifact().set_absent(()),
    }
    Ok(())
}

fn record(mut out: wire::record::Builder<'_>, value: Record, store: &str) -> capnp::Result<()> {
    out.set_store_id(store);
    out.set_id(&value.id);
    out.set_artifact_id(&value.artifact_id);
    out.set_source(&value.source);
    out.reborrow()
        .init_received_at()
        .set_unix_millis(value.received_at);
    out.reborrow()
        .init_expires_at()
        .set_unix_millis(value.expires_at);
    out.set_byte_start(value.byte_start);
    out.set_byte_end(value.byte_end);
    event_time(out.init_event_time(), &value.event_time)
}

fn time_representation(value: wire::TimeRepresentation) -> sparrow_core::TimeRepresentation {
    match value {
        wire::TimeRepresentation::Rfc3339 => sparrow_core::TimeRepresentation::Rfc3339,
        wire::TimeRepresentation::UnixMillis => sparrow_core::TimeRepresentation::UnixMillis,
        wire::TimeRepresentation::UnixSeconds => sparrow_core::TimeRepresentation::UnixSeconds,
    }
}

fn source_time_profile(
    mut out: wire::source_time_profile::Builder<'_>,
    profile: &sparrow_core::SourceTimeProfile,
    store: &str,
) {
    out.set_store_id(store);
    out.set_id(&profile.id);
    out.set_source(&profile.source);
    out.set_path(&profile.path);
    out.set_representation(match profile.representation {
        sparrow_core::TimeRepresentation::Rfc3339 => wire::TimeRepresentation::Rfc3339,
        sparrow_core::TimeRepresentation::UnixMillis => wire::TimeRepresentation::UnixMillis,
        sparrow_core::TimeRepresentation::UnixSeconds => wire::TimeRepresentation::UnixSeconds,
    });
}

fn event_time(
    out: wire::event_time::Builder<'_>,
    value: &sparrow_core::EventTime,
) -> capnp::Result<()> {
    match value {
        sparrow_core::EventTime::Known {
            millis,
            profile_id,
            occurrence,
            source_start,
            source_end,
        } => {
            let mut out = out.init_known();
            out.reborrow().init_instant().set_unix_millis(*millis);
            out.set_profile_id(profile_id);
            out.set_occurrence(*occurrence);
            out.set_source_start(*source_start);
            out.set_source_end(*source_end);
        }
        sparrow_core::EventTime::Unknown { reason, profile_id } => {
            let mut out = out.init_unknown();
            out.set_reason(match reason.as_str() {
                "no_profile" => wire::UnknownTimeReason::NoProfile,
                "not_json" => wire::UnknownTimeReason::NotJson,
                "projection_failed" => wire::UnknownTimeReason::ProjectionFailed,
                "missing_field" => wire::UnknownTimeReason::MissingField,
                "ambiguous_field" => wire::UnknownTimeReason::AmbiguousField,
                "wrong_kind" => wire::UnknownTimeReason::WrongKind,
                "invalid_instant" => wire::UnknownTimeReason::InvalidInstant,
                "legacy_unprofiled" => wire::UnknownTimeReason::LegacyUnprofiled,
                _ => {
                    return Err(capnp::Error::failed(
                        "invalid stored source time diagnostic".into(),
                    ));
                }
            });
            if let Some(profile) = profile_id {
                out.init_profile_id().set_value(profile);
            }
        }
    }
    Ok(())
}

fn pin(
    mut out: wire::pin::Builder<'_>,
    value: Pin,
    store: &str,
    request_id: &str,
) -> capnp::Result<()> {
    out.set_store_id(store);
    out.set_id(&value.id);
    out.set_name(&value.name);
    out.set_request_id(request_id);
    out.set_durability(wire::Durability::Durable);
    out.set_revision(value.revision);
    out.reborrow()
        .init_expires_at()
        .set_unix_millis(value.expires_at);
    let mut members = out.init_members(
        value
            .members
            .len()
            .try_into()
            .map_err(|_| capnp::Error::failed("pin member count overflow".into()))?,
    );
    for (index, id) in value.members.iter().enumerate() {
        let mut target = members.reborrow().get(index as u32);
        target.set_store_id(store);
        if id.starts_with("artifact:") {
            target.set_artifact(id);
        } else {
            target.set_record(id);
        }
    }
    Ok(())
}

fn pin_summary(mut out: wire::pin_summary::Builder<'_>, pin: &PinSummary) {
    out.set_id(&pin.id);
    out.set_name(&pin.name);
    out.set_revision(pin.revision);
    out.set_direct_member_count(pin.direct_member_count as u64);
    out.init_expires_at().set_unix_millis(pin.expires_at);
}

fn context_side(
    mut out: wire::context_side::Builder<'_>,
    value: sparrow_core::ContextSide,
    store: &str,
) -> capnp::Result<()> {
    out.set_visited(value.visited);
    out.set_retired(value.retired);
    out.set_frontier(match value.frontier {
        sparrow_core::ContextFrontier::RequestedCount => wire::ContextFrontier::RequestedCount,
        sparrow_core::ContextFrontier::ScopeBoundary => wire::ContextFrontier::ScopeBoundary,
        sparrow_core::ContextFrontier::ScanBudget => wire::ContextFrontier::ScanBudget,
    });
    let mut rows = out.init_rows(value.records.len() as u32);
    for (index, value) in value.records.into_iter().enumerate() {
        record(rows.reborrow().get(index as u32), value, store)?;
    }
    Ok(())
}

fn optional(value: wire::optional_text::Reader<'_>) -> capnp::Result<Option<String>> {
    match value.which()? {
        wire::optional_text::Absent(()) => Ok(None),
        wire::optional_text::Value(value) => Ok(Some(value?.to_str()?.to_owned())),
    }
}

fn scope(value: wire::scope::Reader<'_>, rpc: &Rpc) -> capnp::Result<Result<QueryScope, Error>> {
    let source = optional(value.get_source()?)?;
    let received_from = value.get_received_from()?.get_unix_millis();
    let received_before = value.get_received_before()?.get_unix_millis();
    let selection = match value.get_selection()?.which()? {
        wire::query_selection::Live(()) => Ok(sparrow_core::QuerySelection::Live),
        wire::query_selection::Evidence(ids) => {
            let ids = ids?;
            if ids.is_empty() || ids.len() > 1000 {
                Err(invalid("explicit selection requires 1..1000 references"))
            } else {
                ids.iter()
                    .map(|id| rpc.scoped_id(id))
                    .collect::<Result<Vec<_>, _>>()
                    .map(sparrow_core::QuerySelection::Evidence)
            }
        }
        wire::query_selection::Pin(pin) => {
            let pin = pin?;
            let expected_revision = match pin.get_expected_revision()?.which()? {
                wire::optional_revision::Absent(()) => None,
                wire::optional_revision::Value(value) => Some(value),
            };
            rpc.reference(
                pin.get_store_id()?.to_str()?,
                pin.get_id()?.to_str()?,
                "pin:",
            )
            .map(|id| sparrow_core::QuerySelection::Pin {
                id,
                expected_revision,
            })
        }
    };
    Ok(selection.map(|selection| QueryScope {
        selection,
        source,
        received_from,
        received_before,
        limit: value.get_limit() as usize,
        scan_budget: value.get_scan_budget() as usize,
    }))
}

fn field<T: purrdf_lex::json::record::FromJson>(
    value: &Value,
    name: &'static str,
) -> capnp::Result<T> {
    JsonRecord::new(value, "stored operation result")
        .and_then(|mut record| record.required(name))
        .map_err(|e| capnp::Error::failed(e.to_string()))
}

fn number(value: &Value, name: &'static str) -> capnp::Result<u64> {
    field::<String>(value, name)?
        .parse()
        .map_err(|e| capnp::Error::failed(format!("stored count {name}: {e}")))
}

fn receipt(mut out: wire::receipt::Builder<'_>, value: &Value) -> capnp::Result<()> {
    out.set_store_id(field::<String>(value, "store_id")?);
    out.reborrow().init_membership_at().set_unix_millis(
        sparrow_core::parse_instant(&field::<String>(value, "membership_at")?)
            .map_err(|e| capnp::Error::failed(e.to_string()))?,
    );
    out.set_execution(match field::<String>(value, "execution")?.as_str() {
        "complete" => wire::Execution::Complete,
        "limited" => wire::Execution::Limited,
        _ => return Err(capnp::Error::failed("invalid execution receipt".into())),
    });
    out.set_certainty(match field::<String>(value, "certainty")?.as_str() {
        "exact" => wire::Certainty::Exact,
        "incomplete" => wire::Certainty::Incomplete,
        _ => return Err(capnp::Error::failed("invalid certainty receipt".into())),
    });
    let coverage = value
        .get("coverage")
        .ok_or_else(|| capnp::Error::failed("missing query coverage".into()))?;
    let mut target = out.init_coverage();
    target.set_visited(number(coverage, "visited")?);
    target.set_eligible_records(number(coverage, "eligible_records")?);
    target.set_failed_json_records(number(coverage, "failed_json_records")?);
    target.set_non_json_records(number(coverage, "non_json_records")?);
    target.set_decoding_failures(number(coverage, "decoding_failures")?);
    Ok(())
}

fn rows(value: &Value) -> capnp::Result<&[Value]> {
    value
        .get("rows")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| capnp::Error::failed("missing stored query rows".into()))
}

fn text_results(mut out: wire::text_results::Builder<'_>, value: &Value) -> capnp::Result<()> {
    receipt(out.reborrow().init_receipt(), value)?;
    let ranking = value
        .get("ranking")
        .ok_or_else(|| capnp::Error::failed("missing text ranking".into()))?;
    let mut target = out.reborrow().init_ranking();
    target.set_profile(field::<String>(ranking, "profile")?);
    target.set_corpus_id(field::<String>(ranking, "corpus_id")?);
    target.set_corpus_profile(field::<String>(ranking, "corpus_profile")?);
    target.set_analyzer(field::<String>(ranking, "analyzer")?);
    target.set_documents(number(ranking, "documents")?);
    target.set_tokens(number(ranking, "tokens")?);
    target.set_membership_complete(field::<bool>(ranking, "membership_complete")?);
    let terms = ranking
        .get("terms")
        .and_then(Value::as_array)
        .ok_or_else(|| capnp::Error::failed("missing query terms".into()))?;
    let mut target_terms = target.init_terms(terms.len() as u32);
    for (index, term) in terms.iter().enumerate() {
        target_terms.set(
            index as u32,
            term.as_str()
                .ok_or_else(|| capnp::Error::failed("invalid query term".into()))?,
        );
    }
    let coverage = value
        .get("text_coverage")
        .ok_or_else(|| capnp::Error::failed("missing text coverage".into()))?;
    let mut target = out.reborrow().init_coverage();
    target.set_failed_or_incomplete(number(coverage, "failed_or_incomplete")?);
    target.set_not_applicable(number(coverage, "not_applicable")?);
    target.set_posting_visits(number(coverage, "posting_visits")?);
    let source = rows(value)?;
    let mut target = out.init_rows(source.len() as u32);
    for (index, row) in source.iter().enumerate() {
        let mut target = target.reborrow().get(index as u32);
        target.set_record_id(field::<String>(row, "record_id")?);
        target.set_artifact_id(field::<String>(row, "artifact_id")?);
        target.set_source(field::<String>(row, "source")?);
        target.reborrow().init_received_at().set_unix_millis(
            sparrow_core::parse_instant(&field::<String>(row, "received_at")?)
                .map_err(|e| capnp::Error::failed(e.to_string()))?,
        );
        target.set_score(field::<String>(row, "score")?);
        target.set_evidence_limited(field::<bool>(row, "evidence_limited")?);
        let evidence = row
            .get("evidence")
            .and_then(Value::as_array)
            .ok_or_else(|| capnp::Error::failed("missing text evidence".into()))?;
        let mut targets = target.init_evidence(evidence.len() as u32);
        for (index, evidence) in evidence.iter().enumerate() {
            let mut target = targets.reborrow().get(index as u32);
            target.set_term(field::<String>(evidence, "term")?);
            target.set_source_start(number(evidence, "source_start")?);
            target.set_source_end(number(evidence, "source_end")?);
            let span = evidence
                .get("span")
                .ok_or_else(|| capnp::Error::failed("missing text span".into()))?;
            target.set_coarse(field::<bool>(span, "coarse")?);
            target.set_basis(match field::<String>(span, "basis")?.as_str() {
                "raw_record" => wire::TextBasis::RawRecord,
                "decoded_json_scalar" => wire::TextBasis::DecodedJsonScalar,
                _ => return Err(capnp::Error::failed("invalid text basis".into())),
            });
            if let Some(path) = span.get("path").and_then(Value::as_str) {
                target.reborrow().init_path().set_value(path);
            }
            let offset = number(evidence, "source_start")?
                .checked_sub(number(span, "start")?)
                .ok_or_else(|| capnp::Error::failed("invalid text offset".into()))?;
            let contributors = span
                .get("contributors")
                .and_then(Value::as_array)
                .ok_or_else(|| capnp::Error::failed("missing text contributors".into()))?;
            let mut target = target.init_contributors(contributors.len() as u32);
            for (index, span) in contributors.iter().enumerate() {
                let mut target = target.reborrow().get(index as u32);
                target.set_start(
                    offset
                        .checked_add(number(span, "start")?)
                        .ok_or_else(|| capnp::Error::failed("text span overflow".into()))?,
                );
                target.set_end(
                    offset
                        .checked_add(number(span, "end")?)
                        .ok_or_else(|| capnp::Error::failed("text span overflow".into()))?,
                );
            }
        }
    }
    Ok(())
}

fn watch_mutation_result(
    mut out: wire::result::Builder<'_, wire::watch_mutation::Owned>,
    store: &str,
    request: &str,
    result: Result<crate::watch_admin::ManagedWatch, Error>,
) -> capnp::Result<()> {
    match result {
        Ok(watch) => {
            let mut receipt = out.reborrow().init_ok();
            receipt.set_request_id(request);
            receipt.set_durability(wire::Durability::Durable);
            crate::watch_admin::encode_info(receipt.init_watch(), store, &watch)?;
        }
        Err(error) => failure(out, error),
    }
    Ok(())
}

impl wire::sparrow::Server for Rpc {
    async fn watch_handoff(
        self: Rc<Self>,
        params: wire::sparrow::WatchHandoffParams,
        mut results: wire::sparrow::WatchHandoffResults,
    ) -> capnp::Result<()> {
        self.handoff_request(
            params.get()?.get_request()?,
            results.get().init_result(),
            false,
        )
        .await
    }

    async fn watch_handoff_get(
        self: Rc<Self>,
        params: wire::sparrow::WatchHandoffGetParams,
        mut results: wire::sparrow::WatchHandoffGetResults,
    ) -> capnp::Result<()> {
        self.handoff_request(
            params.get()?.get_request()?,
            results.get().init_result(),
            true,
        )
        .await
    }

    async fn watch_list(
        self: Rc<Self>,
        _: wire::sparrow::WatchListParams,
        mut results: wire::sparrow::WatchListResults,
    ) -> capnp::Result<()> {
        let store = self
            .store
            .store_id()
            .map_err(|error| capnp::Error::failed(error.to_string()))?;
        let watches = self
            .watches
            .snapshot()
            .map_err(|error| capnp::Error::failed(error.to_string()))?;
        let mut out = results.get().init_result().init_ok();
        out.set_store_id(&store);
        let mut rows = out.init_watches(watches.len() as u32);
        for (index, watch) in watches.into_iter().enumerate() {
            crate::watch_admin::encode_info(
                rows.reborrow().get(index as u32),
                &store,
                &crate::watch_admin::ManagedWatch {
                    definition: watch.definition,
                    revision: watch.revision,
                    enabled: watch.enabled,
                },
            )?;
        }
        Ok(())
    }

    async fn watch_put(
        self: Rc<Self>,
        params: wire::sparrow::WatchPutParams,
        mut results: wire::sparrow::WatchPutResults,
    ) -> capnp::Result<()> {
        let request = params.get()?.get_request()?;
        let store = request.get_store_id()?.to_str()?;
        let request_id = request.get_request_id()?.to_str()?;
        if request_id.is_empty() || request_id.len() > 128 {
            failure(
                results.get().init_result(),
                invalid("watch request identity requires 1..128 bytes"),
            );
            return Ok(());
        }
        let definition = match crate::watch_admin::decode_definition(request.get_definition()?) {
            Ok(definition) => definition,
            Err(error) => {
                failure(results.get().init_result(), error);
                return Ok(());
            }
        };
        let result = self
            .mutate_watch(
                store,
                crate::watch_admin::Action::Put {
                    request_id: request_id.into(),
                    expected: request.get_expected_revision(),
                    definition,
                },
            )
            .await;
        watch_mutation_result(results.get().init_result(), store, request_id, result)
    }

    async fn watch_remove(
        self: Rc<Self>,
        params: wire::sparrow::WatchRemoveParams,
        mut results: wire::sparrow::WatchRemoveResults,
    ) -> capnp::Result<()> {
        let request = params.get()?.get_request()?;
        let store = request.get_store_id()?.to_str()?;
        let request_id = request.get_request_id()?.to_str()?;
        let id = request.get_id()?.to_str()?;
        if request_id.is_empty() || request_id.len() > 128 || id.is_empty() || id.len() > 128 {
            failure(
                results.get().init_result(),
                invalid("watch and request identities require 1..128 bytes"),
            );
            return Ok(());
        }
        let result = self
            .mutate_watch(
                store,
                crate::watch_admin::Action::Remove {
                    request_id: request_id.into(),
                    expected: request.get_expected_revision(),
                    id: id.into(),
                },
            )
            .await;
        watch_mutation_result(results.get().init_result(), store, request_id, result)
    }

    async fn events_ingest(
        self: Rc<Self>,
        params: wire::sparrow::EventsIngestParams,
        mut results: wire::sparrow::EventsIngestResults,
    ) -> capnp::Result<()> {
        let mode = durability_mode(params.get()?.get_durability()?);
        let input = match sparrow_client::EventBatch::decode(params.get()?.get_batch()?) {
            Ok(input) => input,
            Err(error) => {
                failure(results.get().init_result(), invalid(error));
                return Ok(());
            }
        };
        // Normalization is bounded independently of the configured storage admission.
        let bytes = match input.source_bytes().map_err(invalid) {
            Ok(bytes) => bytes,
            Err(error) => {
                failure(results.get().init_result(), error);
                return Ok(());
            }
        };
        if bytes.len() > self.max_artifact_bytes {
            failure(
                results.get().init_result(),
                Error {
                    code: "RESOURCE_LIMIT",
                    message: "event batch exceeds configured artifact admission".into(),
                },
            );
            return Ok(());
        }
        match self
            .work(move |store, now| {
                store.define_time_profile(
                    &input.source,
                    "/event_time",
                    sparrow_core::TimeRepresentation::UnixMillis,
                )?;
                store.ingest_with_durability(
                    &input.request_id,
                    &input.source,
                    "jsonl",
                    &bytes,
                    now,
                    mode,
                )
            })
            .await
        {
            Ok(value) => artifact(
                results.get().init_result().init_ok(),
                value,
                &self
                    .store
                    .store_id()
                    .map_err(|error| capnp::Error::failed(error.to_string()))?,
            )?,
            Err(error) => failure(results.get().init_result(), error),
        }
        Ok(())
    }
    async fn import_begin(
        self: Rc<Self>,
        params: wire::sparrow::ImportBeginParams,
        mut results: wire::sparrow::ImportBeginResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_request()?;
        let request = input.get_request_id()?.to_str()?;
        let source = input.get_source()?.to_str()?;
        let payload = input.get_payload_id()?.to_str()?;
        if request.len() > 128 || source.len() > 512 || payload.len() != 71 {
            failure(
                results.get().init_result(),
                invalid("import labels or digest exceed admitted bounds"),
            );
            return Ok(());
        }
        let request = request.to_owned();
        let source = source.to_owned();
        let payload = payload.to_owned();
        let kind = format(input.get_format()?);
        let length = input.get_byte_length();
        match self
            .work(move |store, now| {
                Ok((
                    store.import_start(&request, &source, kind, length, &payload, now)?,
                    store.store_id()?,
                ))
            })
            .await
        {
            Ok((sparrow_core::ImportStart::Staging(value), store)) => import_progress(
                results.get().init_result().init_ok().init_staging(),
                value,
                &store,
            )?,
            Ok((sparrow_core::ImportStart::Accepted(value), store)) => artifact(
                results.get().init_result().init_ok().init_accepted(),
                value,
                &store,
            )?,
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn import_append(
        self: Rc<Self>,
        params: wire::sparrow::ImportAppendParams,
        mut results: wire::sparrow::ImportAppendResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_request()?;
        let bytes = input.get_bytes()?;
        if bytes.len() > sparrow_core::IMPORT_CHUNK_BYTES {
            failure(
                results.get().init_result(),
                invalid("import chunk exceeds 64 KiB"),
            );
            return Ok(());
        }
        let id = match self.import_reference(input.get_import()?) {
            Ok(id) => id,
            Err(e) => {
                failure(results.get().init_result(), e);
                return Ok(());
            }
        };
        let bytes = bytes.to_vec();
        let offset = input.get_offset();
        match self
            .work(move |store, now| {
                Ok((
                    store.import_append(&id, offset, &bytes, now)?,
                    store.store_id()?,
                ))
            })
            .await
        {
            Ok((value, store)) => {
                import_progress(results.get().init_result().init_ok(), value, &store)?
            }
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn import_get(
        self: Rc<Self>,
        params: wire::sparrow::ImportGetParams,
        mut results: wire::sparrow::ImportGetResults,
    ) -> capnp::Result<()> {
        let id = match self.import_reference(params.get()?.get_import()?) {
            Ok(id) => id,
            Err(e) => {
                failure(results.get().init_result(), e);
                return Ok(());
            }
        };
        match self
            .work(move |store, now| Ok((store.import_get(&id, now)?, store.store_id()?)))
            .await
        {
            Ok((value, store)) => {
                import_progress(results.get().init_result().init_ok(), value, &store)?
            }
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn import_seal(
        self: Rc<Self>,
        params: wire::sparrow::ImportSealParams,
        mut results: wire::sparrow::ImportSealResults,
    ) -> capnp::Result<()> {
        let id = match self.import_reference(params.get()?.get_import()?) {
            Ok(id) => id,
            Err(e) => {
                failure(results.get().init_result(), e);
                return Ok(());
            }
        };
        match self
            .work(move |store, now| Ok((store.import_seal(&id, now)?, store.store_id()?)))
            .await
        {
            Ok((value, store)) => {
                import_progress(results.get().init_result().init_ok(), value, &store)?
            }
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn import_layout(
        self: Rc<Self>,
        params: wire::sparrow::ImportLayoutParams,
        mut results: wire::sparrow::ImportLayoutResults,
    ) -> capnp::Result<()> {
        let id = match self.import_reference(params.get()?.get_import()?) {
            Ok(id) => id,
            Err(e) => {
                failure(results.get().init_result(), e);
                return Ok(());
            }
        };
        match self
            .work(move |store, now| Ok((store.import_prepare_layout(&id, now)?, store.store_id()?)))
            .await
        {
            Ok((value, store)) => {
                import_progress(results.get().init_result().init_ok(), value, &store)?
            }
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn import_prepare(
        self: Rc<Self>,
        params: wire::sparrow::ImportPrepareParams,
        mut results: wire::sparrow::ImportPrepareResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_request()?;
        let id = match self.import_reference(input.get_import()?) {
            Ok(id) => id,
            Err(e) => {
                failure(results.get().init_result(), e);
                return Ok(());
            }
        };
        let expected = input.get_expected_record();
        match self
            .work(move |store, now| {
                Ok((
                    store.import_prepare_record(&id, expected, now)?,
                    store.store_id()?,
                ))
            })
            .await
        {
            Ok((value, store)) => {
                let mut out = results.get().init_result().init_ok();
                out.set_store_id(store);
                out.set_import_id(&value.import_id);
                out.set_reserved_artifact_id(&value.artifact_id);
                out.set_prepared_records(value.prepared_records);
                out.set_total_records(value.total_records);
                out.reborrow()
                    .init_received_at()
                    .set_unix_millis(value.received_at);
                out.reborrow()
                    .init_expires_at()
                    .set_unix_millis(value.expires_at);
                match value.raw {
                    None => out.reborrow().init_raw().set_absent(()),
                    Some(raw) => {
                        let mut raw_out = out.reborrow().init_raw().init_value();
                        raw_out.set_record_index(raw.record_index);
                        raw_out.set_reserved_record_id(raw.record_id);
                        raw_out.set_prepared_chunks(raw.prepared_chunks);
                        raw_out.set_total_chunks(raw.total_chunks);
                    }
                }
                match value.source_time_profile {
                    Some(id) => out.init_source_time_profile().set_value(id),
                    None => out.init_source_time_profile().set_absent(()),
                }
            }
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn import_publish(
        self: Rc<Self>,
        params: wire::sparrow::ImportPublishParams,
        mut results: wire::sparrow::ImportPublishResults,
    ) -> capnp::Result<()> {
        let id = match self.import_reference(params.get()?.get_import()?) {
            Ok(id) => id,
            Err(e) => {
                failure(results.get().init_result(), e);
                return Ok(());
            }
        };
        match self
            .work(move |store, now| Ok((store.import_publish(&id, now)?, store.store_id()?)))
            .await
        {
            Ok((value, store)) => artifact(results.get().init_result().init_ok(), value, &store)?,
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn import_cancel(
        self: Rc<Self>,
        params: wire::sparrow::ImportCancelParams,
        mut results: wire::sparrow::ImportCancelResults,
    ) -> capnp::Result<()> {
        let id = match self.import_reference(params.get()?.get_import()?) {
            Ok(id) => id,
            Err(e) => {
                failure(results.get().init_result(), e);
                return Ok(());
            }
        };
        match self
            .work(move |store, now| {
                store.import_cancel(&id, now)?;
                Ok((id, store.store_id()?))
            })
            .await
        {
            Ok((id, store)) => {
                let mut out = results.get().init_result().init_ok().init_import();
                out.set_store_id(store);
                out.set_id(id);
            }
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn rdf_inspect(
        self: Rc<Self>,
        params: wire::sparrow::RdfInspectParams,
        mut results: wire::sparrow::RdfInspectResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_record()?;
        let id = self.reference(
            input.get_store_id()?.to_str()?,
            input.get_id()?.to_str()?,
            "record:",
        );
        let result = match id {
            Ok(id) => {
                self.work(move |store, now| store.rdf_inspect(&id, now))
                    .await
            }
            Err(error) => Err(error),
        };
        match result {
            Err(error) => failure(results.get().init_result(), error),
            Ok(value) => {
                let mut out = results.get().init_result().init_ok();
                out.set_store_id(field::<String>(&value, "store_id")?);
                out.set_record_id(field::<String>(&value, "record_id")?);
                out.reborrow().init_membership_at().set_unix_millis(
                    sparrow_core::parse_instant(&field::<String>(&value, "membership_at")?)
                        .map_err(|e| capnp::Error::failed(e.to_string()))?,
                );
                out.set_metadata_graph(field::<String>(&value, "metadata_graph")?);
                out.set_metadata_primary_rows(number(&value, "metadata_primary_rows")?);
                out.set_state(match field::<String>(&value, "state")?.as_str() {
                    "complete" => wire::ProjectionState::Complete,
                    "failed" => wire::ProjectionState::Failed,
                    "not_applicable" => wire::ProjectionState::NotApplicable,
                    _ => return Err(capnp::Error::failed("invalid RDF inspection state".into())),
                });
                if let Some(text) = value.get("content_graph").and_then(Value::as_str) {
                    out.reborrow().init_content_graph().set_value(text);
                }
                if let Some(text) = value.get("diagnostic").and_then(Value::as_str) {
                    out.reborrow().init_diagnostic().set_value(text);
                }
                out.set_primary_rows(number(&value, "primary_rows")?);
                out.set_reifier_rows(number(&value, "reifier_rows")?);
                out.set_annotation_rows(number(&value, "annotation_rows")?);
            }
        }
        Ok(())
    }
    async fn records_context(
        self: Rc<Self>,
        params: wire::sparrow::RecordsContextParams,
        mut results: wire::sparrow::RecordsContextResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_query()?;
        let anchor = input.get_anchor()?;
        let reference = self.reference(
            anchor.get_store_id()?.to_str()?,
            anchor.get_id()?.to_str()?,
            "record:",
        );
        let scope = scope(input.get_scope()?, &self)?;
        let before = input.get_before() as usize;
        let after = input.get_after() as usize;
        let ordering = input.get_ordering().map_err(invalid);
        let found = match (reference, ordering) {
            (Ok(id), Ok(wire::ContextOrder::ReceivedAtRecordId)) => {
                self.work(move |store, now| store.records_context(&id, &scope?, before, after, now))
                    .await
            }
            (Err(error), _) | (_, Err(error)) => Err(error),
        };
        match found {
            Err(error) => failure(results.get().init_result(), error),
            Ok(value) => {
                let mut out = results.get().init_result().init_ok();
                out.set_ordering(wire::ContextOrder::ReceivedAtRecordId);
                out.set_continuity_unknown(true);
                let mut receipt = out.reborrow().init_receipt();
                receipt.set_store_id(&value.store_id);
                receipt
                    .reborrow()
                    .init_membership_at()
                    .set_unix_millis(value.membership_at);
                let limited = [value.before.frontier, value.after.frontier]
                    .contains(&sparrow_core::ContextFrontier::ScanBudget);
                receipt.set_execution(if limited {
                    wire::Execution::Limited
                } else {
                    wire::Execution::Complete
                });
                receipt.set_certainty(if limited {
                    wire::Certainty::Incomplete
                } else {
                    wire::Certainty::Exact
                });
                let mut coverage = receipt.init_coverage();
                coverage.set_visited(1 + value.before.visited + value.after.visited);
                coverage.set_eligible_records(
                    (1 + value.before.records.len() + value.after.records.len()) as u64,
                );
                record(out.reborrow().init_anchor(), value.anchor, &value.store_id)?;
                context_side(out.reborrow().init_before(), value.before, &value.store_id)?;
                context_side(out.init_after(), value.after, &value.store_id)?;
            }
        }
        Ok(())
    }
    async fn source_time_define(
        self: Rc<Self>,
        params: wire::sparrow::SourceTimeDefineParams,
        mut results: wire::sparrow::SourceTimeDefineResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_definition()?;
        let source = input.get_source()?.to_str()?.to_owned();
        let path = input.get_path()?.to_str()?.to_owned();
        let representation = time_representation(input.get_representation()?);
        match self
            .work(move |store, _| {
                Ok((
                    store.define_time_profile(&source, &path, representation)?,
                    store.store_id()?,
                ))
            })
            .await
        {
            Err(error) => failure(results.get().init_result(), error),
            Ok((profile, store)) => {
                source_time_profile(results.get().init_result().init_ok(), &profile, &store)
            }
        }
        Ok(())
    }
    async fn source_time_get(
        self: Rc<Self>,
        params: wire::sparrow::SourceTimeGetParams,
        mut results: wire::sparrow::SourceTimeGetResults,
    ) -> capnp::Result<()> {
        let source = params.get()?.get_source()?.to_str()?.to_owned();
        match self
            .work(move |store, _| Ok((store.time_profile(&source)?, store.store_id()?)))
            .await
        {
            Err(error) => failure(results.get().init_result(), error),
            Ok((Some(profile), store)) => source_time_profile(
                results.get().init_result().init_ok().init_value(),
                &profile,
                &store,
            ),
            Ok((None, _)) => results.get().init_result().init_ok().set_absent(()),
        }
        Ok(())
    }

    async fn records_search(
        self: Rc<Self>,
        params: wire::sparrow::RecordsSearchParams,
        mut results: wire::sparrow::RecordsSearchResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_query()?;
        let scope = scope(input.get_scope()?, &self)?;
        let fields = input.get_fields()?;
        if fields.len() > 32 {
            failure(
                results.get().init_result(),
                Error {
                    code: "RESOURCE_LIMIT",
                    message: "at most 32 field constraints".into(),
                },
            );
            return Ok(());
        }
        let fields = fields
            .iter()
            .map(|field| {
                let predicate = field.get_predicate()?;
                Ok(sparrow_core::FieldConstraint {
                    predicate: sparrow_core::JsonPredicate {
                        path: optional(predicate.get_path()?)?,
                        kind: optional_kind(predicate.get_kind()?)?,
                        lexical: optional(predicate.get_lexical()?)?,
                        decoded: optional(predicate.get_decoded()?)?,
                    },
                    parent_binding: optional(field.get_parent_binding()?)?,
                    parent_path: optional(field.get_parent_path()?)?,
                })
            })
            .collect::<capnp::Result<Vec<_>>>()?;
        let query = sparrow_core::RecordQuery {
            event_interval: match input.get_event_interval()?.which()? {
                wire::optional_event_interval::Absent(()) => None,
                wire::optional_event_interval::Value(value) => {
                    let value = value?;
                    Some(sparrow_core::EventInterval {
                        from: value.get_from()?.get_unix_millis(),
                        before: value.get_before()?.get_unix_millis(),
                    })
                }
            },
            text: optional(input.get_text()?)?,
            fields,
            require_complete_index: input.get_require_complete_index(),
        };
        let has_text = query.text.is_some();
        match self
            .work(move |store, now| store.records_query(&scope?, &query, now))
            .await
        {
            Err(error) => failure(results.get().init_result(), error),
            Ok(value) => {
                let mut out = results.get().init_result().init_ok();
                receipt(out.reborrow().init_receipt(), &value)?;
                out.set_field_visits(number(&value, "field_visits")?);
                out.set_qualified_records(number(&value, "qualified_records")?);
                out.set_time_basis(match field::<String>(&value, "time_basis")?.as_str() {
                    "received_at" => wire::TimeBasis::ReceivedAt,
                    "event_time" => wire::TimeBasis::EventTime,
                    _ => return Err(capnp::Error::failed("invalid captured time basis".into())),
                });
                let coverage = value
                    .get("event_coverage")
                    .ok_or_else(|| capnp::Error::failed("missing event coverage".into()))?;
                let mut target = out.reborrow().init_event_coverage();
                target.set_evaluated(number(coverage, "evaluated")?);
                target.set_unknown(number(coverage, "unknown")?);
                target.set_outside_interval(number(coverage, "outside_interval")?);
                target.set_membership_complete(field::<bool>(coverage, "membership_complete")?);
                if has_text {
                    text_results(out.reborrow().init_text().init_value(), &value)?;
                }
                let source = rows(&value)?;
                let mut targets = out.init_rows(source.len() as u32);
                for (index, row) in source.iter().enumerate() {
                    let mut target = targets.reborrow().get(index as u32);
                    target.set_record_id(field::<String>(row, "record_id")?);
                    target.set_artifact_id(field::<String>(row, "artifact_id")?);
                    target.set_source(field::<String>(row, "source")?);
                    target.reborrow().init_received_at().set_unix_millis(
                        sparrow_core::parse_instant(&field::<String>(row, "received_at")?)
                            .map_err(|e| capnp::Error::failed(e.to_string()))?,
                    );
                    target.set_byte_start(number(row, "byte_start")?);
                    target.set_byte_end(number(row, "byte_end")?);
                    let event = row.get("event_time").ok_or_else(|| {
                        capnp::Error::failed("missing captured event time".into())
                    })?;
                    let event =
                        <sparrow_core::EventTime as purrdf_lex::json::record::FromJson>::from_json(
                            event,
                        )
                        .map_err(|e| capnp::Error::failed(e.to_string()))?;
                    event_time(target.reborrow().init_event_time(), &event)?;
                    let fields = row.get("fields").and_then(Value::as_array).ok_or_else(|| {
                        capnp::Error::failed("missing captured field witnesses".into())
                    })?;
                    let mut targets = target.init_fields(fields.len() as u32);
                    for (index, field_value) in fields.iter().enumerate() {
                        let mut target = targets.reborrow().get(index as u32);
                        target.set_predicate_index(
                            u32::try_from(number(field_value, "predicate_index")?)
                                .map_err(|e| capnp::Error::failed(e.to_string()))?,
                        );
                        target.set_occurrence(number(field_value, "occurrence")?);
                        target.set_path(field::<String>(field_value, "path")?);
                        target.set_kind(json_kind(&field::<String>(field_value, "kind")?)?);
                        target.set_source_start(number(field_value, "source_start")?);
                        target.set_source_end(number(field_value, "source_end")?);
                        if let Some(label) =
                            field_value.get("parent_binding").and_then(Value::as_str)
                        {
                            target.reborrow().init_parent_binding().set_value(label);
                        }
                        if !field_value.get("bound_parent").is_some_and(Value::is_null) {
                            target
                                .init_bound_parent()
                                .set_value(number(field_value, "bound_parent")?);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn text_search(
        self: Rc<Self>,
        params: wire::sparrow::TextSearchParams,
        mut results: wire::sparrow::TextSearchResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_query()?;
        let scope = scope(input.get_scope()?, &self)?;
        let query = input.get_text()?.to_str()?.to_owned();
        match self
            .work(move |store, now| store.text_query(&scope?, &query, now))
            .await
        {
            Err(error) => failure(results.get().init_result(), error),
            Ok(value) => {
                let out = results.get().init_result().init_ok();
                text_results(out, &value)?;
            }
        }
        Ok(())
    }

    async fn status(
        self: Rc<Self>,
        _: wire::sparrow::StatusParams,
        mut results: wire::sparrow::StatusResults,
    ) -> capnp::Result<()> {
        match self
            .work(|store, now| {
                Ok((
                    store.status(now)?,
                    store.durability_status()?,
                    store.pressure_status()?,
                ))
            })
            .await
        {
            Err(e) => failure(results.get().init_result(), e),
            Ok((value, durability, pressure)) => {
                let mut out = results.get().init_result().init_ok();
                crate::pressure::encode(out.reborrow().init_pressure_status(), &pressure)?;
                let watches = self
                    .watches
                    .snapshot()
                    .map_err(|error| capnp::Error::failed(error.to_string()))?;
                let mut states = out.reborrow().init_consume_watches(watches.len() as u32);
                for (index, watch) in watches.into_iter().enumerate() {
                    let mut state = states.reborrow().get(index as u32);
                    state.set_id(&watch.id);
                    state.set_root(&watch.root);
                    state.set_revision(watch.revision);
                    state.set_enabled(watch.enabled);
                    crate::watch_admin::encode_definition(
                        state.reborrow().init_definition(),
                        &watch.definition,
                    )?;
                    state.set_coverage(if watch.covered {
                        wire::WatchCoverage::Covered
                    } else {
                        wire::WatchCoverage::Reconciling
                    });
                    state.set_directories(watch.directories);
                    state.set_candidates(watch.candidates);
                    state.set_deferred(watch.deferred);
                    state.set_pending_claims(watch.pending);
                    state.set_completed_claims(watch.completed);
                    state.set_reconciliations(watch.reconciliations);
                    state.set_coverage_losses(watch.coverage_losses);
                    sparrow_client::encode_optional(
                        state.init_failure(),
                        watch.failure,
                        |mut out, value| out.set_value(&value),
                        |mut out| out.set_absent(()),
                    );
                }
                {
                    let mut state = out.reborrow().init_durability_status();
                    state.set_session_id(&durability.session_id);
                    state.set_accepted_through(durability.accepted_through);
                    state.set_durable_through(durability.durable_through);
                    state.set_pending_writes(durability.pending_writes as u32);
                    state.set_pending_bytes(durability.pending_bytes as u64);
                    state.set_oldest_pending_ms(durability.oldest_pending_ms);
                    state.set_max_pending_writes(sparrow_core::MAX_BUFFERED_WRITES as u32);
                    state.set_max_pending_bytes(sparrow_core::MAX_BUFFERED_BYTES as u64);
                    state.set_flush_interval_ms(durability.flush_interval_ms);
                    state.set_flush_lag_ms(durability.flush_lag_ms);
                    sparrow_client::encode_optional(
                        state.init_failure(),
                        durability.failure,
                        |mut out, value| out.set_value(&value),
                        |mut out| out.set_absent(()),
                    );
                }
                out.set_store_id(field::<String>(&value, "store_id")?);
                out.set_store_format(field::<String>(&value, "store_format")?);
                out.set_schema_version(sparrow_client::SCHEMA_VERSION);
                out.set_max_event_batch_bytes(sparrow_client::MAX_EVENT_BATCH_BYTES as u32);
                out.set_max_event_batch_events(sparrow_client::MAX_EVENT_BATCH_EVENTS as u32);
                out.set_rdf_profile(field::<String>(&value, "rdf_profile")?);
                out.set_text_analyzer(field::<String>(&value, "text_analyzer")?);
                out.set_text_profile(field::<String>(&value, "text_profile")?);
                out.set_physical_bytes(number(&value, "physical_bytes")?);
                out.set_disk_budget(number(&value, "disk_budget")?);
                out.set_maintenance_reserve(number(&value, "maintenance_reserve")?);
                out.set_live_records(number(&value, "live_records")?);
                out.set_unique_payload_chunks(number(&value, "unique_payload_chunks")?);
                out.set_active_pins(number(&value, "active_pins")?);
                out.set_unique_json_projections(number(&value, "unique_json_projections")?);
                out.set_retry_window_seconds(number(&value, "retry_window_seconds")?);
                out.set_max_artifact_bytes(number(&value, "max_artifact_bytes")?);
                out.set_max_records_per_artifact(number(&value, "max_records_per_artifact")?);
                out.set_max_pin_members(number(&value, "max_pin_members")?);
                out.set_pin_read_handles(number(&value, "pin_read_handles")?);
                out.set_pin_read_bytes(number(&value, "pin_read_bytes")?);
                out.set_max_pin_read_handles(number(&value, "max_pin_read_handles")?);
                out.set_max_pin_read_bytes(number(&value, "max_pin_read_bytes")?);
                out.set_pin_read_lease_ms(number(&value, "pin_read_lease_ms")?);
            }
        }
        Ok(())
    }

    async fn ingest(
        self: Rc<Self>,
        params: wire::sparrow::IngestParams,
        mut results: wire::sparrow::IngestResults,
    ) -> capnp::Result<()> {
        let request = params.get()?.get_request()?;
        let bytes = request.get_bytes()?;
        if bytes.len() > self.max_artifact_bytes {
            failure(
                results.get().init_result(),
                Error {
                    code: "RESOURCE_LIMIT",
                    message: "artifact exceeds configured ingestion bound".into(),
                },
            );
            return Ok(());
        }
        let bytes = bytes.to_vec();
        let request_id = request.get_request_id()?.to_str()?.to_owned();
        let source = request.get_source()?.to_str()?.to_owned();
        let format = format(request.get_format()?);
        let mode = durability_mode(request.get_durability()?);
        match self
            .work(move |store, now| {
                store.ingest_with_durability(&request_id, &source, format, &bytes, now, mode)
            })
            .await
        {
            Ok(value) => artifact(
                results.get().init_result().init_ok(),
                value,
                &self
                    .store
                    .store_id()
                    .map_err(|e| capnp::Error::failed(e.to_string()))?,
            )?,
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn flush(
        self: Rc<Self>,
        params: wire::sparrow::FlushParams,
        mut results: wire::sparrow::FlushResults,
    ) -> capnp::Result<()> {
        let token = sparrow_client::DurabilityWatermark::decode(params.get()?.get_through()?)
            .map_err(|error| capnp::Error::failed(error.to_string()))?;
        let required = sparrow_core::DurabilityWatermark {
            store_id: token.store_id,
            session_id: token.session_id,
            sequence: token.sequence,
        };
        match self
            .work(move |store, _| store.flush_through(&required))
            .await
        {
            Ok(value) => durability_watermark(results.get().init_result().init_ok(), value)?,
            Err(error) => failure(results.get().init_result(), error),
        }
        Ok(())
    }

    async fn artifact_get(
        self: Rc<Self>,
        params: wire::sparrow::ArtifactGetParams,
        mut results: wire::sparrow::ArtifactGetResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_artifact()?;
        let result = match self.reference(
            input.get_store_id()?.to_str()?,
            input.get_id()?.to_str()?,
            "artifact:",
        ) {
            Ok(id) => self.work(move |store, _| store.artifact(&id)).await,
            Err(e) => Err(e),
        };
        match result {
            Ok(value) => artifact(
                results.get().init_result().init_ok(),
                value,
                &self
                    .store
                    .store_id()
                    .map_err(|e| capnp::Error::failed(e.to_string()))?,
            )?,
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn record_get(
        self: Rc<Self>,
        params: wire::sparrow::RecordGetParams,
        mut results: wire::sparrow::RecordGetResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_record()?;
        let result = match self.reference(
            input.get_store_id()?.to_str()?,
            input.get_id()?.to_str()?,
            "record:",
        ) {
            Ok(id) => self.work(move |store, _| store.record(&id)).await,
            Err(e) => Err(e),
        };
        match result {
            Ok(value) => record(
                results.get().init_result().init_ok(),
                value,
                &self
                    .store
                    .store_id()
                    .map_err(|e| capnp::Error::failed(e.to_string()))?,
            )?,
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn bytes_read(
        self: Rc<Self>,
        params: wire::sparrow::BytesReadParams,
        mut results: wire::sparrow::BytesReadResults,
    ) -> capnp::Result<()> {
        let request = params.get()?.get_request()?;
        let offset = request.get_offset();
        let length = request.get_length() as usize;
        let result = match self.scoped_id(request.get_evidence()?) {
            Ok(id) => {
                self.work(move |store, now| store.bytes(&id, offset, length, now))
                    .await
            }
            Err(e) => Err(e),
        };
        match result {
            Ok(value) => {
                let mut out = results.get().init_result().init_ok();
                out.set_store_id(
                    self.store
                        .store_id()
                        .map_err(|e| capnp::Error::failed(e.to_string()))?,
                );
                out.set_data(&value);
            }
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn pin(
        self: Rc<Self>,
        params: wire::sparrow::PinParams,
        mut results: wire::sparrow::PinResults,
    ) -> capnp::Result<()> {
        let request = params.get()?.get_request()?;
        let request_id = request.get_request_id()?.to_str()?.to_owned();
        let name = request.get_name()?.to_str()?.to_owned();
        let days = u64::from(request.get_days());
        let targets = request.get_targets()?;
        if targets.len() as usize > self.max_pin_members {
            failure(
                results.get().init_result(),
                Error {
                    code: "RESOURCE_LIMIT",
                    message: "pin input exceeds configured membership limit".into(),
                },
            );
            return Ok(());
        }
        let targets: Result<Vec<_>, _> = targets
            .iter()
            .map(|target| self.scoped_id(target))
            .collect();
        let result = match targets {
            Ok(targets) => {
                self.work({
                    let request_id = request_id.clone();
                    move |store, now| store.pin(&request_id, &name, &targets, days, now)
                })
                .await
            }
            Err(e) => Err(e),
        };
        match result {
            Ok(value) => pin(
                results.get().init_result().init_ok(),
                value,
                &self
                    .store
                    .store_id()
                    .map_err(|e| capnp::Error::failed(e.to_string()))?,
                &request_id,
            )?,
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn pin_read(
        self: Rc<Self>,
        params: wire::sparrow::PinReadParams,
        mut results: wire::sparrow::PinReadResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_query()?;
        if let Err(error) = self.check_store(input.get_store_id()?.to_str()?) {
            failure(results.get().init_result(), error);
            return Ok(());
        }
        let selector = match input.get_selector()?.which()? {
            wire::pin_selector::Active(()) => PinSelector::Active,
            wire::pin_selector::Id(text) | wire::pin_selector::Name(text) => {
                let text = text?.to_str()?;
                if text.len() > 128 {
                    failure(
                        results.get().init_result(),
                        invalid("pin selector exceeds 128 bytes"),
                    );
                    return Ok(());
                }
                match input.get_selector()?.which()? {
                    wire::pin_selector::Id(_) => PinSelector::Id(text.to_owned()),
                    _ => PinSelector::Name(text.to_owned()),
                }
            }
        };
        let cursor = match input.get_cursor()?.which()? {
            wire::optional_text::Absent(()) => None,
            wire::optional_text::Value(text) => {
                let text = text?.to_str()?;
                if text.len() > 160 {
                    failure(
                        results.get().init_result(),
                        invalid("pin cursor exceeds 160 bytes"),
                    );
                    return Ok(());
                }
                Some(text.to_owned())
            }
        };
        let query = PinRead {
            selector,
            cursor,
            page_size: input.get_page_size() as usize,
            scan_budget: input.get_scan_budget() as usize,
        };
        match self
            .work(move |store, now| store.pin_read(&query, now))
            .await
        {
            Err(error) => failure(results.get().init_result(), error),
            Ok(page) => {
                let mut out = results.get().init_result().init_ok();
                let mut receipt = out.reborrow().init_receipt();
                receipt.set_store_id(&page.store_id);
                receipt.set_snapshot_id(&page.snapshot_id);
                receipt.reborrow().init_as_of().set_unix_millis(page.as_of);
                receipt
                    .reborrow()
                    .init_lease_expires_at()
                    .set_unix_millis(page.lease_expires_at);
                receipt.set_total_items(page.total_items as u64);
                receipt.set_page_start(page.page_start as u64);
                receipt.set_page_size(page.page_size as u32);
                receipt.set_scan_budget(page.scan_budget as u32);
                receipt.set_scanned(page.scanned as u64);
                let mut cursor = receipt.init_next_cursor();
                match &page.next_cursor {
                    Some(value) => cursor.set_value(value),
                    None => cursor.set_absent(()),
                }
                match &page.body {
                    PinPageBody::Collections(rows) => {
                        let mut list = out.init_collections(rows.len() as u32);
                        for (index, row) in rows.iter().enumerate() {
                            pin_summary(list.reborrow().get(index as u32), row);
                        }
                    }
                    PinPageBody::Members { pin, members } => {
                        let mut body = out.init_members();
                        pin_summary(body.reborrow().init_pin(), pin);
                        let mut list = body.init_members(members.len() as u32);
                        for (index, id) in members.iter().enumerate() {
                            let mut item = list.reborrow().get(index as u32);
                            item.set_store_id(&page.store_id);
                            if id.starts_with("artifact:") {
                                item.set_artifact(id);
                            } else {
                                item.set_record(id);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn pin_renew(
        self: Rc<Self>,
        params: wire::sparrow::PinRenewParams,
        mut results: wire::sparrow::PinRenewResults,
    ) -> capnp::Result<()> {
        let request = params.get()?.get_request()?;
        let request_id = request.get_request_id()?.to_str()?.to_owned();
        let days = u64::from(request.get_days());
        let revision = match request.get_expected_revision()?.which()? {
            wire::optional_revision::Absent(()) => None,
            wire::optional_revision::Value(value) => Some(value),
        };
        let result = match self.reference(
            request.get_store_id()?.to_str()?,
            request.get_pin_id()?.to_str()?,
            "pin:",
        ) {
            Ok(id) => {
                self.work({
                    let request_id = request_id.clone();
                    move |store, now| store.renew_pin(&request_id, &id, days, revision, now)
                })
                .await
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(value) => pin(
                results.get().init_result().init_ok(),
                value,
                &self
                    .store
                    .store_id()
                    .map_err(|e| capnp::Error::failed(e.to_string()))?,
                &request_id,
            )?,
            Err(error) => failure(results.get().init_result(), error),
        }
        Ok(())
    }

    async fn pin_release(
        self: Rc<Self>,
        params: wire::sparrow::PinReleaseParams,
        mut results: wire::sparrow::PinReleaseResults,
    ) -> capnp::Result<()> {
        let request = params.get()?.get_request()?;
        let store_id = request.get_store_id()?.to_str()?.to_owned();
        let request_id = request.get_request_id()?.to_str()?.to_owned();
        let pin_id = request.get_pin_id()?.to_str()?.to_owned();
        let result = if store_id
            != self
                .store
                .store_id()
                .map_err(|e| capnp::Error::failed(e.to_string()))?
        {
            Err(Error {
                code: "STORE_MISMATCH",
                message: "pin belongs to a different store".into(),
            })
        } else {
            self.work(move |store, now| store.release_retry(&request_id, &pin_id, now))
                .await
        };
        match result {
            Ok(value) => {
                let mut out = results.get().init_result().init_ok();
                out.set_store_id(&store_id);
                out.set_request_id(field::<String>(&value, "request_id")?);
                out.set_pin_id(field::<String>(&value, "pin_id")?);
                out.init_released_at().set_unix_millis(
                    sparrow_core::parse_instant(&field::<String>(&value, "released_at")?)
                        .map_err(|e| capnp::Error::failed(e.to_string()))?,
                );
            }
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn keys_list(
        self: Rc<Self>,
        params: wire::sparrow::KeysListParams,
        mut results: wire::sparrow::KeysListResults,
    ) -> capnp::Result<()> {
        let scope = scope(params.get()?.get_scope()?, &self)?;
        match self
            .work(move |store, now| store.json_query(&scope?, &JsonQuery::Keys, now))
            .await
        {
            Ok(value) => {
                let mut out = results.get().init_result().init_ok();
                receipt(out.reborrow().init_receipt(), &value)?;
                let source = rows(&value)?;
                let mut target = out.init_rows(source.len() as u32);
                for (index, row) in source.iter().enumerate() {
                    let mut target = target.reborrow().get(index as u32);
                    target.set_path(field::<String>(row, "path")?);
                    target.set_kind(json_kind(&field::<String>(row, "kind")?)?);
                    target.set_occurrences(number(row, "occurrences")?);
                    target.set_records(number(row, "records")?);
                }
            }
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn values_search(
        self: Rc<Self>,
        params: wire::sparrow::ValuesSearchParams,
        mut results: wire::sparrow::ValuesSearchResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_query()?;
        let scope = scope(input.get_scope()?, &self)?;
        let query = JsonQuery::Values {
            path: optional(input.get_path()?)?,
            kind: optional_kind(input.get_kind()?)?,
            lexical: optional(input.get_lexical()?)?,
            decoded: optional(input.get_decoded()?)?,
        };
        match self
            .work(move |store, now| store.json_query(&scope?, &query, now))
            .await
        {
            Ok(value) => {
                let mut out = results.get().init_result().init_ok();
                receipt(out.reborrow().init_receipt(), &value)?;
                let source = rows(&value)?;
                let mut target = out.init_rows(source.len() as u32);
                for (index, row) in source.iter().enumerate() {
                    let mut target = target.reborrow().get(index as u32);
                    target.set_record_id(field::<String>(row, "record_id")?);
                    target.set_artifact_id(field::<String>(row, "artifact_id")?);
                    target.set_source(field::<String>(row, "source")?);
                    target.reborrow().init_received_at().set_unix_millis(
                        sparrow_core::parse_instant(&field::<String>(row, "received_at")?)
                            .map_err(|e| capnp::Error::failed(e.to_string()))?,
                    );
                    target.set_source_start(number(row, "source_start")?);
                    target.set_source_end(number(row, "source_end")?);
                    let fact = row
                        .get("value")
                        .ok_or_else(|| capnp::Error::failed("missing native JSON match".into()))?;
                    target.set_occurrence(number(fact, "occurrence")?);
                    target.set_path(field::<String>(fact, "path")?);
                    target.set_kind(json_kind(&field::<String>(fact, "kind")?)?);
                    let mut fields = JsonRecord::new(fact, "native JSON match")
                        .map_err(|e| capnp::Error::failed(e.to_string()))?;
                    let lexical: Option<String> = fields
                        .optional("lexical")
                        .map_err(|e| capnp::Error::failed(e.to_string()))?;
                    let decoded: Option<String> = fields
                        .optional("decoded")
                        .map_err(|e| capnp::Error::failed(e.to_string()))?;
                    if let Some(lexical) = lexical {
                        target.reborrow().init_lexical().set_value(&lexical);
                    }
                    if let Some(decoded) = decoded {
                        target.reborrow().init_decoded().set_value(&decoded);
                    }
                }
            }
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelled_rpc_keeps_worker_admission_until_storage_work_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(
            dir.path(),
            sparrow_core::Config {
                buffered_flush_interval_ms: sparrow_core::DEFAULT_FLUSH_INTERVAL_MS,
                disk_budget: 32 * 1024 * 1024,
                maintenance_reserve: 4 * 1024 * 1024,
                max_artifact_bytes: 1024 * 1024,
                max_records: 100,
                max_pin_members: 100,
                text_analyzer: sparrow_core::resolve_text_analyzer(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../../../purrdf/crates/text/lexicons/artifacts"
                ))
                .unwrap(),
            },
        )
        .unwrap();
        let admission = Arc::new(Semaphore::new(1));
        let rpc = Arc::new(Rpc::new(store, admission.clone(), 1024 * 1024, 100));
        let (entered, running) = tokio::sync::oneshot::channel();
        let (release, paused) = std::sync::mpsc::channel();
        let task = tokio::spawn(async move {
            rpc.work(move |_, _| {
                entered
                    .send(())
                    .map_err(|_| invalid("test caller disappeared"))?;
                paused.recv().map_err(invalid)?;
                Ok(())
            })
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), running)
            .await
            .unwrap()
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(admission.available_permits(), 0);
        release.send(()).unwrap();
        let permit = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            admission.clone().acquire_owned(),
        )
        .await
        .unwrap()
        .unwrap();
        drop(permit);
        assert_eq!(admission.available_permits(), 1);
    }
}
