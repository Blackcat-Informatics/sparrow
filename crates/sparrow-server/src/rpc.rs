// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Typed adapters over the shared evidence owner; no transport JSON envelopes.

use crate::sparrow_capnp as wire;
use purrdf_lex::json::{Value, record::Record as JsonRecord};
use sparrow_core::{Artifact, Error, JsonQuery, Pin, QueryScope, Record, Store};
use std::{
    rc::Rc,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;

fn invalid(error: impl std::fmt::Display) -> Error {
    Error {
        code: "INVALID_ARGUMENT",
        message: error.to_string(),
    }
}

/// Read the server's millisecond acceptance clock.
pub fn now() -> Result<u64, Error> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(invalid)?
        .as_millis()
        .try_into()
        .map_err(invalid)
}

/// Shared storage owner exposed through the generated RPC capability.
pub struct Rpc {
    store: Store,
    requests: Arc<Semaphore>,
    max_artifact_bytes: usize,
    max_pin_members: usize,
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
        }
    }

    async fn work<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Store, u64) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let _permit = self
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error {
                code: "RESOURCE_LIMIT",
                message: "RPC operation concurrency is at its configured limit".into(),
            })?;
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || operation(store, now()?))
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
        if store != self.store.store_id()? {
            return Err(Error {
                code: "STORE_MISMATCH",
                message: "reference belongs to a different store".into(),
            });
        }
        if !id.starts_with(prefix) {
            return Err(invalid("reference has the wrong evidence kind"));
        }
        Ok(id.to_owned())
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

fn failure<T: capnp::traits::Owned + 'static>(output: wire::result::Builder<'_, T>, error: Error) {
    let mut out = output.init_error();
    out.set_code(error.code);
    out.set_message(&error.message);
    out.set_retryable(matches!(error.code, "STORAGE_PRESSURE" | "RESOURCE_LIMIT"));
}

fn format(value: wire::Format) -> &'static str {
    match value {
        wire::Format::Jsonl => "jsonl",
        wire::Format::Log => "log",
        wire::Format::Json => "json",
        wire::Format::Markdown => "markdown",
        wire::Format::Opaque => "opaque",
    }
}

fn artifact(
    mut out: wire::artifact::Builder<'_>,
    value: Artifact,
    store: &str,
) -> capnp::Result<()> {
    out.set_store_id(store);
    out.set_id(&value.id);
    out.set_source(&value.source);
    out.set_format(match value.format.as_str() {
        "jsonl" => wire::Format::Jsonl,
        "log" => wire::Format::Log,
        "json" => wire::Format::Json,
        "markdown" => wire::Format::Markdown,
        "opaque" => wire::Format::Opaque,
        _ => {
            return Err(capnp::Error::failed(
                "invalid stored artifact format".into(),
            ));
        }
    });
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

fn record(mut out: wire::record::Builder<'_>, value: Record, store: &str) {
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
}

fn pin(mut out: wire::pin::Builder<'_>, value: Pin, store: &str) -> capnp::Result<()> {
    out.set_store_id(store);
    out.set_id(&value.id);
    out.set_name(&value.name);
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

fn optional(value: wire::optional_text::Reader<'_>) -> capnp::Result<Option<String>> {
    match value.which()? {
        wire::optional_text::Absent(()) => Ok(None),
        wire::optional_text::Value(value) => Ok(Some(value?.to_str()?.to_owned())),
    }
}

fn scope(value: wire::scope::Reader<'_>) -> capnp::Result<QueryScope> {
    Ok(QueryScope {
        source: optional(value.get_source()?)?,
        received_from: value.get_received_from()?.get_unix_millis(),
        received_before: value.get_received_before()?.get_unix_millis(),
        limit: value.get_limit() as usize,
        scan_budget: value.get_scan_budget() as usize,
    })
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

impl wire::sparrow::Server for Rpc {
    async fn text_search(
        self: Rc<Self>,
        params: wire::sparrow::TextSearchParams,
        mut results: wire::sparrow::TextSearchResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_query()?;
        let scope = scope(input.get_scope()?)?;
        let query = input.get_text()?.to_str()?.to_owned();
        match self
            .work(move |store, now| store.text_query(&scope, &query, now))
            .await
        {
            Err(error) => failure(results.get().init_result(), error),
            Ok(value) => {
                let mut out = results.get().init_result().init_ok();
                receipt(out.reborrow().init_receipt(), &value)?;
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
                            .ok_or_else(|| {
                                capnp::Error::failed("missing text contributors".into())
                            })?;
                        let mut target = target.init_contributors(contributors.len() as u32);
                        for (index, span) in contributors.iter().enumerate() {
                            let mut target = target.reborrow().get(index as u32);
                            target.set_start(
                                offset.checked_add(number(span, "start")?).ok_or_else(|| {
                                    capnp::Error::failed("text span overflow".into())
                                })?,
                            );
                            target.set_end(offset.checked_add(number(span, "end")?).ok_or_else(
                                || capnp::Error::failed("text span overflow".into()),
                            )?);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn status(
        self: Rc<Self>,
        _: wire::sparrow::StatusParams,
        mut results: wire::sparrow::StatusResults,
    ) -> capnp::Result<()> {
        match self.work(|store, now| store.status(now)).await {
            Err(e) => failure(results.get().init_result(), e),
            Ok(value) => {
                let mut out = results.get().init_result().init_ok();
                out.set_store_id(field::<String>(&value, "store_id")?);
                out.set_store_format(field::<String>(&value, "store_format")?);
                out.set_schema_version(2);
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
        match self
            .work(move |store, now| store.ingest(&request_id, &source, format, &bytes, now))
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
            ),
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
                self.work(move |store, now| store.pin(&request_id, &name, &targets, days, now))
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
            )?,
            Err(e) => failure(results.get().init_result(), e),
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
        let scope = scope(params.get()?.get_scope()?)?;
        match self
            .work(move |store, now| store.json_query(&scope, &JsonQuery::Keys, now))
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
        let scope = scope(input.get_scope()?)?;
        let query = JsonQuery::Values {
            path: optional(input.get_path()?)?,
            kind: optional_kind(input.get_kind()?)?,
            lexical: optional(input.get_lexical()?)?,
            decoded: optional(input.get_decoded()?)?,
        };
        match self
            .work(move |store, now| store.json_query(&scope, &query, now))
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
