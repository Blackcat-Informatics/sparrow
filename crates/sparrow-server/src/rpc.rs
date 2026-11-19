// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Typed adapters over the shared evidence owner; no transport JSON envelopes.

use crate::sparrow_capnp as wire;
mod correlation;
mod ingest_status;
mod sparql;
use purrdf_lex::json::{Value, record::Record as JsonRecord};
use sparrow_core::{
    Artifact, Error, JsonQuery, Pin, PinPageBody, PinRead, PinSelector, PinSummary, QueryScope,
    Record, Store,
};
use std::{rc::Rc, sync::Arc};
use tokio::sync::Semaphore;

pub(crate) fn invalid(error: impl std::fmt::Display) -> Error {
    Error::new("INVALID_ARGUMENT", error.to_string())
}

fn catalog_receipt<B>(
    mut out: wire::catalog_read_receipt::Builder<'_>,
    page: &sparrow_core::CatalogReadPage<B>,
) {
    out.set_store_id(&page.store_id);
    out.set_snapshot_id(&page.snapshot_id);
    out.reborrow().init_as_of().set_unix_millis(page.as_of);
    out.reborrow()
        .init_lease_expires_at()
        .set_unix_millis(page.lease_expires_at);
    out.set_total_items(page.total_items as u64);
    out.set_page_start(page.page_start as u64);
    out.set_page_size(page.page_size as u32);
    out.set_scan_budget(page.scan_budget as u32);
    out.set_scanned(page.scanned as u64);
    if let Some(build) = page.build {
        let mut out = out.reborrow().init_build();
        out.set_read(build.read);
        out.set_total(build.total.unwrap_or(0));
        out.set_total_pending(build.total.is_none());
    }
    sparrow_client::encode_optional(
        out.init_next_cursor(),
        page.next_cursor.as_deref(),
        |mut out, value| out.set_value(value),
        |mut out| out.set_absent(()),
    );
}

fn instance_role(role: Option<sparrow_core::InstanceRole>) -> wire::InstanceRole {
    match role {
        None => wire::InstanceRole::Unspecified,
        Some(sparrow_core::InstanceRole::Production) => wire::InstanceRole::Production,
        Some(sparrow_core::InstanceRole::Scratch) => wire::InstanceRole::Scratch,
    }
}

fn encode_durability(
    mut state: wire::durability_status::Builder<'_>,
    durability: &sparrow_core::DurabilityStatus,
) {
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
        durability.failure.as_deref(),
        |mut out, value| out.set_value(value),
        |mut out| out.set_absent(()),
    );
}

fn refusal(refusal: crate::admission::Refusal) -> Error {
    Error::new(refusal.code, refusal.message)
}

/// Read the server's millisecond acceptance clock.
pub fn now() -> Result<u64, Error> {
    sparrow_client::unix_millis_now().map_err(invalid)
}

/// Shared storage owner exposed through the generated RPC capability.
#[derive(Clone)]
pub struct Rpc {
    connection: sparrow_core::QueryCancellation,
    syslog: crate::syslog_listener::SyslogRegistry,
    store: Store,
    admission: Arc<crate::admission::Admission>,
    /// The shared RPC and TCP connection ceiling, reported by `status`.
    connections: Option<Arc<crate::admission::Ceiling>>,
    max_pin_members: usize,
    watches: crate::watches::WatchRegistry,
    watch_controller: Option<crate::WatchController>,
    query_only: bool,
    /// Present only on an ingest-socket connection: the caller its evidence records.
    intake: Option<sparrow_core::Intake>,
    /// The configured ingest socket's counters, reported by `status`.
    ingest_socket: Option<crate::ingest_socket::IngestSocket>,
    /// The administrator an admin operation records: the private socket's kernel-reported
    /// peer, or a CHAP-authenticated TCP peer. Absent, an admin operation that records its
    /// caller is refused.
    admin: Option<String>,
    /// The free-space pause's counters (#257), shared with the daemon's accept loops.
    pause: crate::floor_pause::FloorPause,
    /// The archive worker's published state (#365); `notConfigured` without a worker.
    archive: crate::archive::ArchiveStatus,
}

/// The full typed service and the ingest socket run no CHAP; only anonymous TCP does.
pub(crate) fn chap_unsupported() -> Error {
    Error::new(
        "UNSUPPORTED",
        "CHAP applies only to an anonymous public TCP connection".to_owned(),
    )
}

pub(crate) struct CancelQuery(pub(crate) sparrow_core::QueryCancellation);
impl Drop for CancelQuery {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// A completed capture stays private until its response has been rendered.
/// An undelivered page releases the handle it published; a delivered live
/// continuation releases the handle it continued, which the client no longer needs.
struct QueryReply<B> {
    page: sparrow_core::CatalogReadPage<B>,
    store: Store,
    delivered: bool,
    owns_capture: bool,
    supersedes: Option<String>,
}
impl<B> QueryReply<B> {
    /// Own the page's handle unless it is the handle `cursor` already names.
    fn new(page: sparrow_core::CatalogReadPage<B>, store: Store, cursor: Option<&str>) -> Self {
        let continued = cursor
            .and_then(|cursor| cursor.split_once('/'))
            .map(|(id, _)| id);
        let owns_capture = continued != Some(page.snapshot_id.as_str());
        let supersedes = continued.filter(|_| owns_capture).map(str::to_owned);
        Self {
            page,
            store,
            delivered: false,
            owns_capture,
            supersedes,
        }
    }
}
impl<B> Drop for QueryReply<B> {
    fn drop(&mut self) {
        let release = if self.delivered {
            self.supersedes.as_deref()
        } else {
            self.owns_capture.then_some(self.page.snapshot_id.as_str())
        };
        if let Some(snapshot) = release {
            let released = now().and_then(|now| {
                self.store
                    .result_release(&self.page.store_id, snapshot, now)
            });
            if let Err(error) = released {
                eprintln!("sparrowd: query handle release failed: {error}");
            }
        }
    }
}

/// A `result_next` page: a guarded live artifact continuation, or a replay.
enum Next {
    Artifacts(Box<QueryReply<sparrow_core::ArtifactPageBody>>),
    Replay(Box<sparrow_core::ResultPage>),
}

impl Rpc {
    /// Construct the one typed adapter with explicit admission limits.
    pub fn new(store: Store, requests: Arc<Semaphore>, max_pin_members: usize) -> Self {
        Self {
            connection: Default::default(),
            syslog: crate::syslog_listener::SyslogRegistry::default(),
            pause: crate::floor_pause::FloorPause::new(store.space_floor()),
            archive: crate::archive::ArchiveStatus::new(),
            store,
            admission: crate::admission::Admission::new(requests),
            connections: None,
            max_pin_members,
            watches: crate::watches::WatchRegistry::default(),
            watch_controller: None,
            query_only: false,
            intake: None,
            ingest_socket: None,
            admin: None,
        }
    }

    /// The store's one free-space floor, which the accept loop observes.
    pub(crate) fn store_floor(&self) -> &sparrow_core::SpaceFloor {
        self.store.space_floor()
    }

    /// The pause counters the accept loops and `status` share.
    #[must_use]
    pub fn floor_pause(&self) -> &crate::floor_pause::FloorPause {
        &self.pause
    }

    /// Report the archive worker's published state in `status`.
    #[must_use]
    pub fn with_archive_status(mut self, archive: crate::archive::ArchiveStatus) -> Self {
        self.archive = archive;
        self
    }

    /// Use the daemon's one admission, which shutdown closes to interrupt waiting writers.
    #[must_use]
    pub fn with_admission(mut self, admission: Arc<crate::admission::Admission>) -> Self {
        self.admission = admission;
        self
    }

    /// Report the shared RPC and TCP connection ceiling in `status`.
    #[must_use]
    pub fn with_connections(mut self, ceiling: Arc<crate::admission::Ceiling>) -> Self {
        self.connections = Some(ceiling);
        self
    }

    /// Serve reads only: every connection's service refuses writes with `READ_ONLY`.
    #[must_use]
    pub fn query_only(mut self) -> Self {
        self.query_only = true;
        self
    }

    /// The write gate in front of this service; every connection's capability is one.
    pub(crate) fn into_gate(self) -> crate::query_only::WriteGate {
        let query_only = self.query_only;
        crate::query_only::WriteGate::new(self, query_only)
    }

    /// The typed service a Unix connection or CHAP session receives, behind the write gate.
    pub(crate) fn into_client(self) -> wire::sparrow::Client {
        capnp_rpc::new_client(self.into_gate())
    }

    /// Persistent store identity bound into CHAP responses.
    pub(crate) fn store_id(&self) -> Result<String, Error> {
        self.store.store_id()
    }

    pub(crate) fn for_connection(&self) -> (Self, CancelQuery) {
        let mut rpc = self.clone();
        rpc.connection = sparrow_core::QueryCancellation::default();
        let guard = CancelQuery(rpc.connection.clone());
        (rpc, guard)
    }

    /// Attach the daemon's one bounded configured-watch snapshot owner.
    pub fn with_watches(mut self, watches: crate::watches::WatchRegistry) -> Self {
        self.watches = watches;
        self
    }
    /// Attach the same finite syslog listener registry used by intake.
    pub fn with_syslog(mut self, registry: crate::syslog_listener::SyslogRegistry) -> Self {
        self.syslog = registry;
        self
    }

    /// Report this configured ingest socket in `status`.
    #[must_use]
    pub fn with_ingest_socket(mut self, socket: crate::ingest_socket::IngestSocket) -> Self {
        self.ingest_socket = Some(socket);
        self
    }

    /// Identify this connection's administrator, recorded by admin operations that name
    /// their caller.
    pub(crate) fn with_admin(mut self, caller: String) -> Self {
        self.admin = Some(caller);
        self
    }

    /// Record this ingest-socket caller on every artifact, record and import it creates.
    pub(crate) fn with_peer(mut self, credential: sparrow_core::PeerCredential) -> Self {
        self.intake = Some(sparrow_core::Intake::IngestSocket(credential));
        self
    }

    /// Run one import operation only for the caller its importBegin bound.
    async fn import_work<T: Send + 'static>(
        &self,
        id: String,
        operation: impl FnOnce(&Store, &str, u64) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let caller = self.intake;
        self.work(move |store, now| {
            store.import_admit(&id, caller.as_ref(), now)?;
            operation(&store, &id, now)
        })
        .await
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

    async fn tail_finish_request(
        &self,
        value: wire::tail_finish_intent::Reader<'_>,
        out: wire::result::Builder<'_, wire::tail_generation::Owned>,
        read_only: bool,
    ) -> capnp::Result<()> {
        let request = match sparrow_client::TailFinishIntent::decode(value) {
            Ok(request) => request,
            Err(error) => {
                failure(out, invalid(error));
                return Ok(());
            }
        };
        let result = async {
            self.check_store(&request.store_id)?;
            let intent = crate::tail_wire::intent(&request)?;
            if read_only {
                self.lookup(move |store, _| {
                    store.tail_finish_get(&intent)?.ok_or_else(|| {
                        Error::new("NOT_FOUND", "tail finish request has not been admitted")
                    })
                })
                .await
            } else {
                self.controller()?.tail_finish(intent).await
            }
        }
        .await;
        match result {
            Ok(generation) => {
                crate::tail_wire::generation(out.init_ok(), &generation, &request.store_id)?
            }
            Err(error) => failure(out, error),
        }
        Ok(())
    }

    /// Run a write on the intake path. It waits for an operation permit, up to the writer wait
    /// bound, then answers the retryable `RESOURCE_LIMIT`.
    async fn work<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Store, u64) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let permit = self.admission.writer().await.map_err(refusal)?;
        let result = self.run(permit, operation).await;
        if let Err(error) = &result {
            self.pause.write_refused(error);
        }
        result
    }

    /// As [`Rpc::work`], waiting for a permit no later than `deadline`.
    async fn work_until<T: Send + 'static>(
        &self,
        deadline: tokio::time::Instant,
        operation: impl FnOnce(Store, u64) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let permit = self
            .admission
            .writer_until(deadline)
            .await
            .map_err(refusal)?;
        let result = self.run(permit, operation).await;
        if let Err(error) = &result {
            self.pause.write_refused(error);
        }
        result
    }

    /// Run a `status` read, which takes no operation permit (see `Admission::diagnostic`).
    async fn diagnostic<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Store, u64) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let permit = self.admission.diagnostic().map_err(refusal)?;
        let paused = self.pause.paused();
        let result = self.run(permit, operation).await;
        self.pause.read_finished(paused, &result);
        result
    }

    /// Run a read: it takes a reader slot and a permit only if both are free now.
    async fn lookup<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Store, u64) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let permit = self.admission.reader().map_err(refusal)?;
        let paused = self.pause.paused();
        let result = self.run(permit, operation).await;
        self.pause.read_finished(paused, &result);
        result
    }

    async fn run<T: Send + 'static>(
        &self,
        permit: crate::admission::Admitted,
        operation: impl FnOnce(Store, u64) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation(store, now()?)
        })
        .await
        .map_err(|e| Error::new("STORAGE_FAILURE", format!("storage task failed: {e}")))?
    }

    async fn read_work<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Store, u64, sparrow_core::QueryCancellation) -> Result<T, Error>
        + Send
        + 'static,
    ) -> Result<T, Error> {
        let cancellation = self.connection.child();
        let _caller = CancelQuery(cancellation.clone());
        self.lookup(move |store, now| {
            cancellation.checkpoint()?;
            let reply = operation(store, now, cancellation.clone())?;
            cancellation.checkpoint()?;
            Ok(reply)
        })
        .await
    }

    async fn query_work<B: Send + 'static>(
        &self,
        cursor: Option<String>,
        operation: impl FnOnce(
            Store,
            u64,
            sparrow_core::QueryCancellation,
        ) -> Result<sparrow_core::CatalogReadPage<B>, Error>
        + Send
        + 'static,
    ) -> Result<QueryReply<B>, Error> {
        self.read_work(move |store, now, cancellation| {
            let page = operation(store.clone(), now, cancellation.clone())?;
            Ok(QueryReply::new(page, store, cursor.as_deref()))
        })
        .await
    }

    fn tail_reference(
        &self,
        query: wire::tail_generation_query::Reader<'_>,
    ) -> Result<String, Error> {
        self.check_store(
            query
                .get_store_id()
                .map_err(invalid)?
                .to_str()
                .map_err(invalid)?,
        )?;
        if query.get_schema_version() != sparrow_client::SCHEMA_VERSION {
            return Err(Error::new(
                "UNSUPPORTED",
                "file-generation reads require the current discovered schema",
            ));
        }
        let id = query.get_id().map_err(invalid)?;
        if id.len() > 128 {
            return Err(invalid("file-generation ID exceeds 128 bytes"));
        }
        let id = id.to_str().map_err(invalid)?;
        if !id.starts_with("file-generation:") {
            return Err(invalid("reference has the wrong evidence kind"));
        }
        Ok(id.to_owned())
    }

    fn scoped_id(&self, evidence: wire::evidence::Reader<'_>) -> Result<String, Error> {
        let (store, id) = sparrow_client::decode_evidence(evidence).map_err(invalid)?;
        self.check_store(&store)?;
        Ok(id)
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
            return Err(Error::new(
                "STORE_MISMATCH",
                "reference belongs to a different store",
            ));
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

fn records_page(
    mut out: wire::records_results::Builder<'_>,
    page: &sparrow_core::RecordReadPage,
    value: &Value,
) -> capnp::Result<()> {
    catalog_receipt(out.reborrow().init_page(), page);
    receipt(out.reborrow().init_receipt(), value, Some(page))?;
    out.set_field_visits(number(value, "field_visits")?);
    count(
        out.reborrow().init_qualified_records(),
        value,
        "qualified_records",
    )?;
    out.set_known_qualified_records(number(value, "known_qualified_records")?);
    out.set_event_time_fallback(number(value, "event_time_fallback")?);
    out.set_time_basis(match field::<String>(value, "time_basis")?.as_str() {
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
    if value.get("ranking").is_some() {
        text_results(out.reborrow().init_text().init_value(), value, page)?;
    }
    let source = rows(value)?;
    if let Some(counts) = value.get("artifact_group_counts") {
        let mut groups = std::collections::BTreeMap::<String, Vec<u32>>::new();
        for (index, row) in source.iter().enumerate() {
            groups
                .entry(field::<String>(row, "artifact_id")?)
                .or_default()
                .push(index as u32);
        }
        let mut targets = out
            .reborrow()
            .init_artifact_groups()
            .init_value(groups.len() as u32);
        for (index, (artifact, indices)) in groups.into_iter().enumerate() {
            let mut target = targets.reborrow().get(index as u32);
            target.set_artifact_id(&artifact);
            let captured = counts
                .get(&artifact)
                .ok_or_else(|| capnp::Error::failed("captured artifact count missing".into()))?;
            target.set_captured_matches(
                <u64 as purrdf_lex::json::record::FromJson>::from_json(captured)
                    .map_err(|e| capnp::Error::failed(e.to_string()))?,
            );
            let mut positions = target.init_page_indices(indices.len() as u32);
            for (position, row) in indices.into_iter().enumerate() {
                positions.set(position as u32, row);
            }
        }
    }
    let mut targets = out.init_rows(source.len() as u32);
    for (index, row) in source.iter().enumerate() {
        let mut target = targets.reborrow().get(index as u32);
        target.set_record_id(field::<String>(row, "record_id")?);
        target.set_artifact_id(field::<String>(row, "artifact_id")?);
        target.set_source(field::<String>(row, "source")?);
        let received_at = sparrow_core::parse_instant(&field::<String>(row, "received_at")?)
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        target
            .reborrow()
            .init_received_at()
            .set_unix_millis(received_at);
        target.set_byte_start(number(row, "byte_start")?);
        target.set_byte_end(number(row, "byte_end")?);
        let event = row
            .get("event_time")
            .ok_or_else(|| capnp::Error::failed("missing captured event time".into()))?;
        let event =
            <sparrow_core::EventTime as purrdf_lex::json::record::FromJson>::from_json(event)
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
        event_time(target.reborrow().init_event_time(), &event, received_at)?;
        crate::diagnostics::snapshot(
            target.reborrow().init_diagnostics(),
            &field::<sparrow_core::DiagnosticSnapshot>(row, "diagnostics")?,
        );
        let fields = row
            .get("fields")
            .and_then(Value::as_array)
            .ok_or_else(|| capnp::Error::failed("missing captured field witnesses".into()))?;
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
            if let Some(label) = field_value.get("parent_binding").and_then(Value::as_str) {
                target.reborrow().init_parent_binding().set_value(label);
            }
            if !field_value.get("bound_parent").is_some_and(Value::is_null) {
                target
                    .init_bound_parent()
                    .set_value(number(field_value, "bound_parent")?);
            }
        }
    }
    Ok(())
}

fn text_page(
    mut out: wire::text_search_page::Builder<'_>,
    page: &sparrow_core::RecordReadPage,
    value: &Value,
) -> capnp::Result<()> {
    catalog_receipt(out.reborrow().init_page(), page);
    text_results(out.init_result(), value, page)?;
    Ok(())
}

fn artifacts_page(
    mut out: wire::artifact_list_page::Builder<'_>,
    page: &sparrow_core::ArtifactReadPage,
) -> capnp::Result<()> {
    catalog_receipt(out.reborrow().init_receipt(), page);
    out.set_receipt_walk(page.body.receipt_walk());
    let encoded = page.body.diagnostic_coverage.encode(
        page.body.diagnostic_coverage.evaluated,
        page.body.diagnostic_coverage.matched,
        [0; 3],
        false,
    );
    diagnostic_coverage(
        out.reborrow().init_coverage(),
        encoded
            .get("diagnostic_scope")
            .ok_or_else(|| capnp::Error::failed("missing artifact diagnostic coverage".into()))?,
    )?;
    count(
        out.reborrow().init_matching_records(),
        &encoded,
        "eligible_records",
    )?;
    let mut rows = out.init_rows(page.body.artifacts.len() as u32);
    for (index, summary) in page.body.artifacts.iter().enumerate() {
        let mut row = rows.reborrow().get(index as u32);
        row.set_id(&summary.id);
        row.set_source(&summary.source);
        row.set_complete_bytes_available(summary.complete_bytes_available);
        row.set_retained_records(summary.retained_records as u64);
        let encoded = summary.diagnostic_coverage.encode(
            summary.diagnostic_coverage.evaluated,
            summary.diagnostic_coverage.matched,
            [0; 3],
            false,
        );
        diagnostic_coverage(
            row.reborrow().init_coverage(),
            encoded
                .get("diagnostic_scope")
                .ok_or_else(|| capnp::Error::failed("missing artifact record coverage".into()))?,
        )?;
        count(
            row.reborrow().init_matching_records(),
            &encoded,
            "eligible_records",
        )?;
        let mut original = row.init_origin();
        match &summary.origin {
            None => original.set_absent(()),
            Some(origin) => {
                let mut original = original.init_value();
                original.set_format(stored_format(&origin.format)?);
                original
                    .reborrow()
                    .init_received_at()
                    .set_unix_millis(origin.received_at);
                original
                    .reborrow()
                    .init_expires_at()
                    .set_unix_millis(origin.expires_at);
                original.set_byte_length(origin.byte_length);
                original.set_original_records(origin.original_records as u64);
            }
        }
    }
    Ok(())
}

fn sources_page(
    mut out: wire::source_list_page::Builder<'_>,
    page: &sparrow_core::SourceReadPage,
) -> capnp::Result<()> {
    catalog_receipt(out.reborrow().init_receipt(), page);
    let mut rows = out.init_rows(page.body.sources.len() as u32);
    for (index, summary) in page.body.sources.iter().enumerate() {
        let mut row = rows.reborrow().get(index as u32);
        row.set_source_id(&summary.source_id);
        row.set_rdf_iri(&summary.rdf_iri);
        row.set_complete_artifacts(summary.complete_artifacts as u64);
        row.set_fragment_parents(summary.fragment_parents as u64);
        row.set_retained_records(summary.retained_records as u64);
        row.set_continuity_unknown(summary.continuity_unknown);
        sparrow_client::encode_optional(
            row.reborrow().init_time_profile_id(),
            summary.time_profile_id.as_deref(),
            |mut out, value| out.set_value(value),
            |mut out| out.set_absent(()),
        );
        sparrow_client::encode_optional(
            row.reborrow().init_diagnostic_profile_id(),
            summary.diagnostic_profile_id.as_deref(),
            |mut out, value| out.set_value(value),
            |mut out| out.set_absent(()),
        );
        let mut streams = row
            .reborrow()
            .init_producer_stream_ids(summary.producer_stream_ids.len() as u32);
        for (index, id) in summary.producer_stream_ids.iter().enumerate() {
            streams.set(index as u32, id);
        }
        let mut generations = row
            .reborrow()
            .init_tail_generation_ids(summary.tail_generation_ids.len() as u32);
        for (index, id) in summary.tail_generation_ids.iter().enumerate() {
            generations.set(index as u32, id);
        }
        let mut listeners = row
            .reborrow()
            .init_syslog_listener_ids(summary.syslog_listener_ids.len() as u32);
        for (index, id) in summary.syslog_listener_ids.iter().enumerate() {
            listeners.set(index as u32, id);
        }
        let mut receivers = row
            .reborrow()
            .init_receiver_window_ids(summary.receiver_window_ids.len() as u32);
        for (index, id) in summary.receiver_window_ids.iter().enumerate() {
            receivers.set(index as u32, id);
        }
        let mut watches = row.init_watches(summary.watches.len() as u32);
        for (index, binding) in summary.watches.iter().enumerate() {
            let mut watch = watches.reborrow().get(index as u32);
            watch.set_watch_id(&binding.watch_id);
            watch.set_revision(binding.revision);
            watch.set_enabled(binding.enabled);
        }
    }
    Ok(())
}

fn pins_page(
    mut out: wire::pin_read_page::Builder<'_>,
    page: &sparrow_core::PinReadPage,
) -> capnp::Result<()> {
    catalog_receipt(out.reborrow().init_receipt(), page);
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
    Ok(())
}

fn keys_page(
    mut out: wire::keys::Builder<'_>,
    page: &sparrow_core::RecordReadPage,
    value: &Value,
) -> capnp::Result<()> {
    catalog_receipt(out.reborrow().init_page(), page);
    receipt(out.reborrow().init_receipt(), value, Some(page))?;
    let source = rows(value)?;
    let mut target = out.init_rows(source.len() as u32);
    for (index, row) in source.iter().enumerate() {
        let mut target = target.reborrow().get(index as u32);
        target.set_path(field::<String>(row, "path")?);
        target.set_kind(json_kind(&field::<String>(row, "kind")?)?);
        target.set_occurrences(number(row, "occurrences")?);
        target.set_records(number(row, "records")?);
    }
    Ok(())
}

fn values_page(
    mut out: wire::values::Builder<'_>,
    page: &sparrow_core::RecordReadPage,
    value: &Value,
) -> capnp::Result<()> {
    catalog_receipt(out.reborrow().init_page(), page);
    receipt(out.reborrow().init_receipt(), value, Some(page))?;
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
    Ok(())
}

fn query_text(
    value: wire::optional_text::Reader<'_>,
    maximum: usize,
    message: &str,
) -> Result<Option<String>, Error> {
    if let wire::optional_text::Value(text) = value.which().map_err(invalid)?
        && text.map_err(invalid)?.len() > maximum
    {
        return Err(invalid(message));
    }
    optional(value).map_err(invalid)
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
    out.set_retryable(error.is_retryable());
    // Additive label (#257): the code and the retryable flag stay what older clients read.
    out.set_class(if error.is_free_space_floor() {
        wire::FailureClass::FreeSpaceFloor
    } else {
        wire::FailureClass::Unclassified
    });
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
    intake(out.reborrow().init_intake(), value.intake.as_ref());
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

/// The typed form of an artifact's or record's ingest-socket provenance.
fn intake(mut out: wire::optional_intake::Builder<'_>, value: Option<&sparrow_core::Intake>) {
    match value {
        Some(sparrow_core::Intake::IngestSocket(credential)) => {
            let mut out = out.init_value();
            out.set_endpoint(wire::IntakeEndpoint::IngestSocket);
            out.set_uid(credential.uid);
            out.set_gid(credential.gid);
            out.set_pid(credential.pid);
        }
        None => out.set_absent(()),
    }
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

fn import_preparation(
    mut out: wire::import_preparation::Builder<'_>,
    value: sparrow_core::ImportPreparation,
    store: &str,
) {
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
    if let Some(raw) = value.raw {
        let mut raw_out = out.reborrow().init_raw().init_value();
        raw_out.set_record_index(raw.record_index);
        raw_out.set_reserved_record_id(raw.record_id);
        raw_out.set_prepared_chunks(raw.prepared_chunks);
        raw_out.set_total_chunks(raw.total_chunks);
    }
    if let Some(id) = value.source_time_profile {
        out.reborrow().init_source_time_profile().set_value(id);
    }
    if let Some(id) = value.diagnostic_profile {
        out.init_diagnostic_profile().set_value(id);
    }
}

fn encode_stream_evidence_loss(
    mut out: wire::stream_evidence_loss::Builder<'_>,
    loss: &sparrow_core::StreamEvidenceLoss,
) {
    out.set_max_named(sparrow_core::MAX_NAMED_LOST_STREAMS as u32);
    out.set_other_streams(loss.other_streams);
    out.set_other_batches(loss.other_batches);
    out.set_other_events(loss.other_events);
    out.set_other_streams_exact(loss.other_streams_exact);
    out.set_total_batches(loss.total_batches);
    out.set_total_events(loss.total_events);
    let mut named = out.init_named(loss.named.len() as u32);
    for (index, stream) in loss.named.iter().enumerate() {
        let mut row = named.reborrow().get(index as u32);
        row.set_kind(match stream.kind {
            sparrow_core::LostStreamKind::Producer => wire::LostStreamKind::Producer,
            sparrow_core::LostStreamKind::Receiver => wire::LostStreamKind::Receiver,
            sparrow_core::LostStreamKind::SourceTime => wire::LostStreamKind::SourceTime,
        });
        row.set_source(&stream.source);
        row.set_instance(&stream.instance);
        row.set_batches(stream.batches);
        row.set_events(stream.events);
    }
}

fn encode_tail_registry_evidence(
    mut out: wire::tail_registry_evidence::Builder<'_>,
    evidence: &sparrow_core::TailRegistryEvidence,
) {
    out.set_max_named(sparrow_core::MAX_NAMED_UNRECORDED_TAILS as u32);
    out.set_other_files(evidence.other_files);
    out.set_other_generations(evidence.other_generations);
    out.set_other_records(evidence.other_records);
    out.set_other_bytes(evidence.other_bytes);
    out.set_other_files_exact(evidence.other_files_exact);
    out.set_total_generations(evidence.total_generations);
    out.set_total_records(evidence.total_records);
    out.set_total_bytes(evidence.total_bytes);
    out.set_retired_terminal(evidence.retired_terminal);
    out.set_retired_duplicate_risk(evidence.retired_duplicate_risk);
    let mut named = out.init_named(evidence.named.len() as u32);
    for (index, file) in evidence.named.iter().enumerate() {
        let mut row = named.reborrow().get(index as u32);
        row.set_watch(&file.watch);
        row.set_path(&file.path);
        row.set_source(&file.source);
        row.set_generations(file.generations);
        row.set_records(file.records);
        row.set_bytes(file.bytes);
    }
}

fn producer_limits(
    mut out: wire::producer_limits::Builder<'_>,
    value: &purrdf_lex::json::Value,
) -> capnp::Result<()> {
    out.set_max_streams(
        u32::try_from(number(value, "max_producer_streams")?)
            .map_err(|error| capnp::Error::failed(error.to_string()))?,
    );
    out.set_max_gap_intervals(
        u32::try_from(number(value, "max_producer_gap_intervals")?)
            .map_err(|error| capnp::Error::failed(error.to_string()))?,
    );
    out.set_max_registry_bytes(number(value, "max_producer_registry_bytes")?);
    out.set_metadata_retention_seconds(number(value, "producer_metadata_retention_seconds")?);
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
    event_time(
        out.reborrow().init_event_time(),
        &value.event_time,
        value.received_at,
    )?;
    crate::diagnostics::snapshot(out.reborrow().init_diagnostics(), &value.diagnostics);
    crate::syslog_wire::snapshot(out.reborrow().init_syslog(), value.syslog.as_ref())?;
    crate::tail_wire::position(out.reborrow().init_tail(), value.tail.as_ref())?;
    intake(out.reborrow().init_intake(), value.intake.as_ref());
    match value.receiver {
        Some(position) => {
            let mut receiver = out.reborrow().init_receiver().init_value();
            receiver.set_window_id(&position.window_id);
            receiver.set_sequence(position.sequence);
        }
        None => out.reborrow().init_receiver().set_absent(()),
    }
    match value.producer {
        Some(position) => {
            let mut producer = out.init_producer().init_value();
            producer.set_stream_id(&position.stream_id);
            producer.set_sequence(position.sequence);
        }
        None => out.init_producer().set_absent(()),
    }
    Ok(())
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
    match &profile.log_capture {
        Some(capture) => {
            crate::time_wire::encode(out.reborrow().init_extraction().init_log_capture(), capture)
        }
        None => out.reborrow().init_extraction().set_json_pointer(()),
    }
    out.set_representation(match profile.representation {
        sparrow_core::TimeRepresentation::Rfc3339 => wire::TimeRepresentation::Rfc3339,
        sparrow_core::TimeRepresentation::UnixMillis => wire::TimeRepresentation::UnixMillis,
        sparrow_core::TimeRepresentation::UnixSeconds => wire::TimeRepresentation::UnixSeconds,
    });
}

/// Render stored source evidence with its read-time placement; an unknown source time
/// carries the receipt instant that places it on the event timeline.
fn event_time(
    out: wire::event_time::Builder<'_>,
    value: &sparrow_core::EventTime,
    received_at: u64,
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
            out.reborrow()
                .init_fallback()
                .set_unix_millis(value.placement(received_at).millis);
            out.set_reason(match reason.as_str() {
                "no_profile" => wire::UnknownTimeReason::NoProfile,
                "not_json" => wire::UnknownTimeReason::NotJson,
                "projection_failed" => wire::UnknownTimeReason::ProjectionFailed,
                "missing_field" => wire::UnknownTimeReason::MissingField,
                "ambiguous_field" => wire::UnknownTimeReason::AmbiguousField,
                "wrong_kind" => wire::UnknownTimeReason::WrongKind,
                "invalid_instant" => wire::UnknownTimeReason::InvalidInstant,
                "legacy_unprofiled" => wire::UnknownTimeReason::LegacyUnprofiled,
                "syslog_malformed" => wire::UnknownTimeReason::SyslogMalformed,
                "syslog_legacy_time" => wire::UnknownTimeReason::SyslogLegacyTime,
                "syslog_nil_time" => wire::UnknownTimeReason::SyslogNilTime,
                "syslog_submillisecond" => wire::UnknownTimeReason::SyslogSubmillisecond,
                "log_input_unavailable" => wire::UnknownTimeReason::LogInputUnavailable,
                "log_invalid_utf8" => wire::UnknownTimeReason::LogInvalidUtf8,
                "log_no_match" => wire::UnknownTimeReason::LogNoMatch,
                "log_capture_missing" => wire::UnknownTimeReason::LogCaptureMissing,
                "log_wrong_format" => wire::UnknownTimeReason::LogWrongFormat,
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

fn context_capture(
    mut out: wire::context_capture::Builder<'_>,
    value: &sparrow_core::Context,
) -> capnp::Result<()> {
    query_scope_receipt(out.reborrow().init_scope(), &value.scope)?;
    out.set_snapshot_id(&value.snapshot_id);
    out.reborrow()
        .init_as_of()
        .set_unix_millis(value.membership_at);
    out.set_host_profile(sparrow_core::CONTEXT_CAPTURE_PROFILE);
    out.reborrow().init_native_governor().set_absent(());
    let limits = &value.limits;
    let mut target = out.init_limits();
    target.set_direction_visits(limits.direction_visits);
    target.set_catalog_bytes(limits.catalog_bytes);
    target.set_response_bytes(limits.response_bytes);
    target.set_output_records(limits.output_records);
    target.set_requested_before(limits.requested_before);
    target.set_requested_after(limits.requested_after);
    target.set_membership_member_visits(limits.membership_member_visits);
    target.set_membership_record_visits(limits.membership_record_visits);
    target.set_scope_receipt_bytes(limits.scope_receipt_bytes);
    Ok(())
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

/// One generated selection decoder validates store-scoped references for every query owner.
fn selection(
    value: wire::query_selection::Reader<'_>,
    rpc: &Rpc,
) -> capnp::Result<Result<sparrow_core::QuerySelection, Error>> {
    let result = match value.which()? {
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
    Ok(result)
}

fn scope(value: wire::scope::Reader<'_>, rpc: &Rpc) -> capnp::Result<Result<QueryScope, Error>> {
    if value.get_schema_version() != sparrow_client::SCHEMA_VERSION {
        return Ok(Err(Error::new(
            "UNSUPPORTED",
            format!(
                "query schema {} is unsupported; require schema {} and current discovery",
                value.get_schema_version(),
                sparrow_client::SCHEMA_VERSION
            ),
        )));
    }
    let source = optional(value.get_source()?)?;
    let filters = value.get_diagnostics()?;
    if filters.len() > 5 {
        return Ok(Err(invalid("at most five diagnostic scope dimensions")));
    }
    let mut diagnostics = std::collections::BTreeMap::new();
    for filter in filters.iter() {
        let dimension = crate::diagnostics::dimension(filter.get_dimension()?);
        let alternatives = filter.get_alternatives()?;
        if alternatives.is_empty() || alternatives.len() > 128 {
            return Ok(Err(invalid(
                "diagnostic alternatives require 1..128 strings",
            )));
        }
        let mut values = Vec::with_capacity(alternatives.len() as usize);
        for value in alternatives.iter() {
            let value = value?;
            if value.len() > 512 {
                return Ok(Err(invalid("diagnostic filter string exceeds 512 bytes")));
            }
            values.push(value.to_str()?.to_owned());
        }
        let alternatives = match sparrow_core::QueryAlternatives::new(values) {
            Ok(value) => value,
            Err(e) => return Ok(Err(e)),
        };
        if diagnostics.insert(dimension, alternatives).is_some() {
            return Ok(Err(invalid("duplicate diagnostic filter dimension")));
        }
    }
    let sources = match value.get_source_ids()?.which()? {
        wire::optional_source_ids::Absent(()) => None,
        wire::optional_source_ids::Value(ids) => {
            let ids = ids?;
            if ids.is_empty() || ids.len() > 128 {
                return Ok(Err(invalid("source_ids requires 1..128 source IDs")));
            }
            let mut names = Vec::with_capacity(ids.len() as usize);
            for id in ids.iter() {
                let id = id?;
                if id.len() > 512 {
                    return Ok(Err(invalid("source ID exceeds 512 bytes")));
                }
                names.push(id.to_str()?.to_owned());
            }
            match sparrow_core::QueryAlternatives::new(names) {
                Ok(filter) => Some(filter),
                Err(error) => return Ok(Err(error)),
            }
        }
    };
    let received_from = value.get_received_from()?.get_unix_millis();
    let received_before = value.get_received_before()?.get_unix_millis();
    let selection = selection(value.get_selection()?, rpc)?;
    Ok(selection.map(|selection| QueryScope {
        selection,
        source,
        sources,
        diagnostics,
        require_complete_index: value.get_require_complete_index(),
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

fn count(
    mut out: wire::count::Builder<'_>,
    value: &Value,
    name: &'static str,
) -> capnp::Result<()> {
    match value.get(name) {
        Some(Value::Null) => out.set_unknown(()),
        Some(_) => out.set_known(number(value, name)?),
        None => return Err(capnp::Error::failed(format!("missing count {name}"))),
    }
    Ok(())
}

fn coverage(mut target: wire::coverage::Builder<'_>, coverage: &Value) -> capnp::Result<()> {
    target.set_visited(number(coverage, "visited")?);
    count(
        target.reborrow().init_eligible_records(),
        coverage,
        "eligible_records",
    )?;
    target.set_known_eligible_records(number(coverage, "known_eligible_records")?);
    target.set_failed_json_records(number(coverage, "failed_json_records")?);
    target.set_non_json_records(number(coverage, "non_json_records")?);
    target.set_decoding_failures(number(coverage, "decoding_failures")?);
    let scope = coverage
        .get("diagnostic_scope")
        .ok_or_else(|| capnp::Error::failed("missing diagnostic coverage".into()))?;
    diagnostic_coverage(target.init_diagnostic_scope(), scope)
}

fn diagnostic_coverage(
    mut out: wire::diagnostic_coverage::Builder<'_>,
    scope: &Value,
) -> capnp::Result<()> {
    out.set_evaluated(number(scope, "evaluated")?);
    out.set_matched(number(scope, "matched")?);
    out.set_excluded(number(scope, "excluded")?);
    out.set_unknown(number(scope, "unknown")?);
    out.set_membership_complete(field::<bool>(scope, "membership_complete")?);
    Ok(())
}

fn query_scope_receipt(
    mut out: wire::query_scope_receipt::Builder<'_>,
    scope: &Value,
) -> capnp::Result<()> {
    out.set_profile(field::<String>(scope, "profile")?);
    out.set_id(field::<String>(scope, "id")?);
    out.set_selection(match field::<String>(scope, "selection")?.as_str() {
        "live" => wire::ScopeSelectionKind::Live,
        "evidence" => wire::ScopeSelectionKind::Evidence,
        "pin" => wire::ScopeSelectionKind::Pin,
        _ => {
            return Err(capnp::Error::failed(
                "invalid captured selection kind".into(),
            ));
        }
    });
    let pin = scope
        .get("pin")
        .ok_or_else(|| capnp::Error::failed("missing captured pin".into()))?;
    if pin.is_null() {
        out.reborrow().init_pin().set_absent(());
    } else {
        let mut target = out.reborrow().init_pin().init_value();
        target.set_id(field::<String>(pin, "id")?);
        target.set_revision(field::<u64>(pin, "revision")?);
        target.set_expires_at_ms(field::<u64>(pin, "expires_at_ms")?);
    }
    count(
        out.reborrow().init_direct_members(),
        scope,
        "direct_members",
    )?;
    count(
        out.reborrow().init_expanded_records(),
        scope,
        "expanded_records",
    )?;
    out.set_has_exact_source(field::<bool>(scope, "has_exact_source")?);
    out.set_source_alternatives(field::<u32>(scope, "source_alternatives")?);
    out.set_diagnostic_dimensions(field::<u32>(scope, "diagnostic_dimensions")?);
    out.set_received_from_ms(field::<u64>(scope, "received_from_ms")?);
    out.set_received_before_ms(field::<u64>(scope, "received_before_ms")?);
    out.set_require_complete_index(field::<bool>(scope, "require_complete_index")?);
    Ok(())
}

fn query_capture(
    mut out: wire::query_capture::Builder<'_>,
    page: &sparrow_core::RecordReadPage,
    result: &Value,
) -> capnp::Result<()> {
    catalog_receipt(out.reborrow().init_page(), page);
    let evidence = result
        .get("query_evidence")
        .ok_or_else(|| capnp::Error::failed("missing frozen query evidence".into()))?;
    query_scope_receipt(
        out.reborrow().init_scope(),
        evidence
            .get("scope")
            .ok_or_else(|| capnp::Error::failed("missing frozen scope".into()))?,
    )?;
    let profiles = evidence
        .get("profile_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| capnp::Error::failed("missing frozen profiles".into()))?;
    let mut targets = out.reborrow().init_profile_ids(profiles.len() as u32);
    for (i, profile) in profiles.iter().enumerate() {
        let mut target = targets.reborrow().get(i as u32);
        target.set_role(match field::<String>(profile, "role")?.as_str() {
            "json_projection" => wire::QueryProfileRole::JsonProjection,
            "host_admission" => wire::QueryProfileRole::HostAdmission,
            "text_analyzer" => wire::QueryProfileRole::TextAnalyzer,
            "text_ranking" => wire::QueryProfileRole::TextRanking,
            "text_corpus" => wire::QueryProfileRole::TextCorpus,
            _ => return Err(capnp::Error::failed("invalid captured profile role".into())),
        });
        target.set_id(field::<String>(profile, "id")?);
    }
    // These query paths do not invoke a native governor. Preserve explicit absence.
    if !evidence.get("native_governor").is_some_and(Value::is_null) {
        return Err(capnp::Error::failed(
            "unexpected native governor evidence".into(),
        ));
    }
    let limits = evidence
        .get("limits")
        .ok_or_else(|| capnp::Error::failed("missing captured query limits".into()))?;
    let mut target = out.init_limits();
    target.set_candidate_visits(field::<u64>(limits, "candidate_visits")?);
    target.set_membership_member_visits(field::<u64>(limits, "membership_member_visits")?);
    target.set_membership_record_visits(field::<u64>(limits, "membership_record_visits")?);
    target.set_capture_items(field::<u64>(limits, "capture_items")?);
    target.set_capture_bytes(field::<u64>(limits, "capture_bytes")?);
    target.set_row_bytes(field::<u64>(limits, "row_bytes")?);
    target.set_metadata_bytes(field::<u64>(limits, "metadata_bytes")?);
    target.set_page_bytes(field::<u64>(limits, "page_bytes")?);
    target.set_result_handles(field::<u64>(limits, "result_handles")?);
    target.set_result_bytes(field::<u64>(limits, "result_bytes")?);
    target.set_lease_ms(field::<u64>(limits, "lease_ms")?);
    for name in [
        "json_value_visits",
        "query_path_bytes",
        "key_aggregation_bytes",
        "field_visits",
        "posting_visits",
        "text_membership_bytes",
        "query_text_bytes",
        "query_terms",
        "field_constraints",
        "compiled_field_bytes",
    ] {
        let mut limit = match name {
            "json_value_visits" => target.reborrow().init_json_value_visits(),
            "query_path_bytes" => target.reborrow().init_query_path_bytes(),
            "key_aggregation_bytes" => target.reborrow().init_key_aggregation_bytes(),
            "field_visits" => target.reborrow().init_field_visits(),
            "posting_visits" => target.reborrow().init_posting_visits(),
            "text_membership_bytes" => target.reborrow().init_text_membership_bytes(),
            "query_text_bytes" => target.reborrow().init_query_text_bytes(),
            "query_terms" => target.reborrow().init_query_terms(),
            "field_constraints" => target.reborrow().init_field_constraints(),
            "compiled_field_bytes" => target.reborrow().init_compiled_field_bytes(),
            _ => return Err(capnp::Error::failed("invalid query budget name".into())),
        };
        if limits.get(name).is_some_and(Value::is_null) {
            limit.set_absent(());
        } else {
            limit.set_value(field::<u64>(limits, name)?);
        }
    }
    Ok(())
}

fn receipt(
    mut out: wire::receipt::Builder<'_>,
    value: &Value,
    page: Option<&sparrow_core::RecordReadPage>,
) -> capnp::Result<()> {
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
    self::coverage(out.reborrow().init_coverage(), coverage)?;
    if let Some(page) = page {
        query_capture(out.init_capture().init_value(), page, value)?;
    }
    Ok(())
}

fn rows(value: &Value) -> capnp::Result<&[Value]> {
    value
        .get("rows")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| capnp::Error::failed("missing stored query rows".into()))
}

fn text_results(
    mut out: wire::text_results::Builder<'_>,
    value: &Value,
    page: &sparrow_core::RecordReadPage,
) -> capnp::Result<()> {
    receipt(out.reborrow().init_receipt(), value, Some(page))?;
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
    owner: &Store,
    store: &str,
    request: &str,
    result: Result<crate::watch_admin::ManagedWatch, Error>,
) -> capnp::Result<()> {
    match result.and_then(|watch| Ok((watch, owner.consumption_retired()?))) {
        Ok((watch, retired)) => {
            let mut receipt = out.reborrow().init_ok();
            receipt.set_request_id(request);
            receipt.set_durability(wire::Durability::Durable);
            crate::watch_admin::encode_info(receipt.init_watch(), store, &watch, &retired)?;
        }
        Err(error) => failure(out, error),
    }
    Ok(())
}

impl wire::sparrow::Server for Rpc {
    async fn auth_challenge(
        self: Rc<Self>,
        _: wire::sparrow::AuthChallengeParams,
        mut results: wire::sparrow::AuthChallengeResults,
    ) -> capnp::Result<()> {
        failure(results.get().init_result(), chap_unsupported());
        Ok(())
    }

    async fn authenticate(
        self: Rc<Self>,
        _: wire::sparrow::AuthenticateParams,
        mut results: wire::sparrow::AuthenticateResults,
    ) -> capnp::Result<()> {
        failure(results.get().init_result(), chap_unsupported());
        Ok(())
    }

    async fn tail_finish(
        self: Rc<Self>,
        params: wire::sparrow::TailFinishParams,
        mut results: wire::sparrow::TailFinishResults,
    ) -> capnp::Result<()> {
        self.tail_finish_request(
            params.get()?.get_request()?,
            results.get().init_result(),
            false,
        )
        .await
    }

    async fn tail_finish_get(
        self: Rc<Self>,
        params: wire::sparrow::TailFinishGetParams,
        mut results: wire::sparrow::TailFinishGetResults,
    ) -> capnp::Result<()> {
        self.tail_finish_request(
            params.get()?.get_request()?,
            results.get().init_result(),
            true,
        )
        .await
    }

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
        let retired = match self.store.consumption_retired() {
            Ok(retired) => retired,
            Err(error) => {
                failure(results.get().init_result(), error);
                return Ok(());
            }
        };
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
                &retired,
            )?;
        }
        Ok(())
    }

    /// The online backup was removed. The method keeps its ordinal (the wire is additive-only)
    /// and answers every caller with `UNSUPPORTED`.
    async fn backup(
        self: Rc<Self>,
        _params: wire::sparrow::BackupParams,
        mut results: wire::sparrow::BackupResults,
    ) -> capnp::Result<()> {
        failure(
            results.get().init_result(),
            Error::new(
                "UNSUPPORTED",
                "the online store backup was removed; Sparrow backs up nothing except the archive age-out to GTS files",
            ),
        );
        Ok(())
    }

    async fn watch_claims(
        self: Rc<Self>,
        params: wire::sparrow::WatchClaimsParams,
        mut results: wire::sparrow::WatchClaimsResults,
    ) -> capnp::Result<()> {
        let query = params.get()?.get_query()?;
        let store = query.get_store_id()?.to_str()?;
        let since = query.get_since()?.get_unix_millis();
        let until = match query.get_until()?.which()? {
            wire::optional_instant::Value(at) => Some(at?.get_unix_millis()),
            wire::optional_instant::Absent(()) => None,
        };
        let result = match self.check_store(store) {
            // The listing decodes up to 2048 journals: run it off the connection's event loop.
            Ok(()) => {
                self.lookup(move |store, as_of| {
                    let until = until.unwrap_or(as_of);
                    Ok((as_of, until, store.consumption_claims(since, until)?))
                })
                .await
            }
            Err(error) => Err(error),
        };
        match result {
            Ok((as_of, until, claims)) => crate::watch_admin::encode_claims(
                results.get().init_result().init_ok(),
                store,
                (as_of, since, until),
                &claims,
            ),
            Err(error) => failure(results.get().init_result(), error),
        }
        Ok(())
    }

    async fn watch_claim_retire(
        self: Rc<Self>,
        params: wire::sparrow::WatchClaimRetireParams,
        mut results: wire::sparrow::WatchClaimRetireResults,
    ) -> capnp::Result<()> {
        let request = params.get()?.get_request()?;
        let store = request.get_store_id()?.to_str()?;
        let watch = request.get_watch()?.to_str()?;
        let claim = request.get_claim_id()?.to_str()?;
        let reason = request.get_reason()?.to_str()?;
        let result = async {
            self.check_store(store)?;
            if watch.is_empty() || watch.len() > 128 || claim.is_empty() || claim.len() > 128 {
                return Err(invalid("watch and claim identities require 1..128 bytes"));
            }
            let caller = self.admin.clone().ok_or_else(|| {
                Error::new(
                    "PERMISSION_DENIED",
                    "claim retirement records its caller; this connection has no identified administrator",
                )
            })?;
            self.controller()?
                .retire_claim(crate::watch_admin::ClaimRetirement {
                    watch: watch.into(),
                    claim: claim.into(),
                    caller,
                    reason: reason.into(),
                })
                .await
        }
        .await;
        match result {
            Ok(retired) => crate::watch_admin::encode_retired_claim(
                results.get().init_result().init_ok(),
                &retired,
            ),
            Err(error) => failure(results.get().init_result(), error),
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
        watch_mutation_result(
            results.get().init_result(),
            &self.store,
            store,
            request_id,
            result,
        )
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
        watch_mutation_result(
            results.get().init_result(),
            &self.store,
            store,
            request_id,
            result,
        )
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
        // Batch validation bounds normalization by MAX_EVENT_BATCH_EVENTS and
        // MAX_EVENT_BATCH_BYTES, well inside the store's inline ingest bound.
        let bytes = match input.source_bytes().map_err(invalid) {
            Ok(bytes) => bytes,
            Err(error) => {
                failure(results.get().init_result(), error);
                return Ok(());
            }
        };
        let intake = self.intake;
        match self
            .work(move |store, now| {
                // The store binds the source's /event_time mapping; a full source registry
                // stores the batch with unknown time and counts it (#227).
                store.ingest_events_with_intake(
                    &input.request_id,
                    &input.source,
                    &bytes,
                    &sparrow_core::ProducerBatch {
                        instance: input.client_instance,
                        sequences: input.events.iter().map(|event| event.sequence).collect(),
                    },
                    intake,
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
        let intake = self.intake;
        let admission = self.admission.clone();
        match admission
            .import_slot(
                |deadline| {
                    let (request, source, payload) =
                        (request.clone(), source.clone(), payload.clone());
                    self.work_until(deadline, move |store, now| {
                        Ok((
                            store.import_start_with_intake(
                                &request, &source, kind, length, &payload, intake, now,
                            )?,
                            store.store_id()?,
                        ))
                    })
                },
                |error: &Error| {
                    error.code == "RESOURCE_LIMIT"
                        && error.message == sparrow_core::IMPORT_JOURNALS_OCCUPIED
                },
            )
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
            .import_work(id, move |store, id, now| {
                Ok((
                    store.import_append(id, offset, &bytes, now)?,
                    store.store_id()?,
                ))
            })
            .await
        {
            Ok((value, store)) => {
                #[cfg(test)]
                sparrow_test_support::crash::checkpoint("upload_chunk_committed");
                import_progress(results.get().init_result().init_ok(), value, &store)?
            }
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn retention_metrics(
        self: Rc<Self>,
        params: wire::sparrow::RetentionMetricsParams,
        mut results: wire::sparrow::RetentionMetricsResults,
    ) -> capnp::Result<()> {
        let query = params.get()?.get_query()?;
        if let Err(error) = self.check_store(query.get_store_id()?.to_str()?) {
            failure(results.get().init_result(), error);
            return Ok(());
        }
        if query.get_schema_version() != sparrow_client::SCHEMA_VERSION {
            failure(
                results.get().init_result(),
                Error::new(
                    "UNSUPPORTED",
                    "retention metrics require the current discovered schema",
                ),
            );
            return Ok(());
        }
        let limits = sparrow_core::RetentionReadLimits {
            max_visits: query.get_max_visits() as usize,
            catalog_bytes: query.get_catalog_bytes() as usize,
            payload_bytes: u64::from(query.get_payload_bytes()),
        };
        match self
            .read_work(move |store, now, cancellation| {
                store.retention_metrics_cancellable(now, limits, &cancellation)
            })
            .await
        {
            Err(error) => failure(results.get().init_result(), error),
            Ok(value) => {
                let mut out = results.get().init_result().init_ok();
                out.set_store_id(&value.store_id);
                out.reborrow().init_as_of().set_unix_millis(value.as_of);
                out.set_live_records(value.live_records);
                out.set_live_artifacts(value.live_artifacts);
                out.set_pinned_records(value.pinned_records);
                out.set_pinned_artifacts(value.pinned_artifacts);
                match value.oldest_received_at {
                    Some(clock) => out
                        .reborrow()
                        .init_oldest_received_at()
                        .init_value()
                        .set_unix_millis(clock),
                    None => out.reborrow().init_oldest_received_at().set_absent(()),
                }
                out.set_retained_window_ms(value.retained_window_ms);
                out.set_owned_payload_bytes(value.owned_payload_bytes);
                out.set_live_payload_bytes(value.live_payload_bytes);
                out.set_pin_held_payload_bytes(value.pin_held_payload_bytes);
                out.set_retirable_payload_bytes(value.retirable_payload_bytes);
                out.set_unclassified_payload_bytes(value.unclassified_payload_bytes);
                out.set_payload_chunks(value.payload_chunks);
                out.set_visits(value.visits);
            }
        }
        Ok(())
    }

    async fn ingest_status(
        self: Rc<Self>,
        params: wire::sparrow::IngestStatusParams,
        mut results: wire::sparrow::IngestStatusResults,
    ) -> capnp::Result<()> {
        let query = params.get()?.get_query()?;
        let input = || -> Result<_, Error> {
            self.check_store(
                query
                    .get_store_id()
                    .map_err(invalid)?
                    .to_str()
                    .map_err(invalid)?,
            )?;
            if query.get_schema_version() != sparrow_client::SCHEMA_VERSION {
                return Err(Error::new(
                    "UNSUPPORTED",
                    "ingestion status requires the current discovered schema",
                ));
            }
            let selector =
                sparrow_client::IngestSelector::decode(query.get_selector().map_err(invalid)?)
                    .map_err(invalid)?;
            let selector = match selector {
                sparrow_client::IngestSelector::Request(id) => {
                    sparrow_core::IngestSelector::Request(id)
                }
                sparrow_client::IngestSelector::Import(id) => {
                    sparrow_core::IngestSelector::Import(id)
                }
                sparrow_client::IngestSelector::Handoff(id) => {
                    sparrow_core::IngestSelector::Handoff(id)
                }
            };
            Ok((
                selector,
                sparrow_core::IngestReadLimits {
                    scan_budget: query.get_scan_budget() as usize,
                    catalog_bytes: query.get_catalog_bytes() as usize,
                },
            ))
        };
        let (selector, limits) = match input() {
            Ok(input) => input,
            Err(error) => {
                failure(results.get().init_result(), error);
                return Ok(());
            }
        };
        match self
            .lookup(move |store, now| store.ingest_status(&selector, limits, now))
            .await
        {
            Ok(status) => ingest_status::encode(results.get().init_result().init_ok(), status)?,
            Err(error) => failure(results.get().init_result(), error),
        }
        Ok(())
    }

    async fn payload_lookup(
        self: Rc<Self>,
        params: wire::sparrow::PayloadLookupParams,
        mut results: wire::sparrow::PayloadLookupResults,
    ) -> capnp::Result<()> {
        let query = params.get()?.get_query()?;
        let input = || -> Result<(String, String, Option<String>, usize), Error> {
            let store_id = query
                .get_store_id()
                .map_err(invalid)?
                .to_str()
                .map_err(invalid)?
                .to_owned();
            self.check_store(&store_id)?;
            if query.get_schema_version() != sparrow_client::SCHEMA_VERSION {
                return Err(Error::new(
                    "UNSUPPORTED",
                    "payload lookups require the current discovered schema",
                ));
            }
            let payload_id = query
                .get_payload_id()
                .map_err(invalid)?
                .to_str()
                .map_err(invalid)?
                .to_owned();
            let cursor = optional(query.get_cursor().map_err(invalid)?).map_err(invalid)?;
            if cursor.as_ref().is_some_and(|cursor| cursor.len() > 160) {
                return Err(Error::new(
                    "INVALID_ARGUMENT",
                    "payload lookup cursor exceeds 160 bytes",
                ));
            }
            Ok((store_id, payload_id, cursor, query.get_page_size() as usize))
        };
        let (store_id, payload_id, cursor, page_size) = match input() {
            Ok(input) => input,
            Err(error) => {
                failure(results.get().init_result(), error);
                return Ok(());
            }
        };
        let echoed = (store_id, payload_id.clone());
        match self
            .lookup(move |store, _| store.payload_lookup(&payload_id, cursor.as_deref(), page_size))
            .await
        {
            Ok(page) => {
                let mut out = results.get().init_result().init_ok();
                out.set_store_id(&echoed.0);
                out.set_payload_id(&echoed.1);
                out.set_complete(page.complete);
                let mut ids = out
                    .reborrow()
                    .init_artifact_ids(page.artifacts.len() as u32);
                for (index, id) in page.artifacts.iter().enumerate() {
                    ids.set(index as u32, id);
                }
                match page.next {
                    Some(next) => out.init_next().set_value(&next),
                    None => out.init_next().set_absent(()),
                }
            }
            Err(error) => failure(results.get().init_result(), error),
        }
        Ok(())
    }

    async fn ingest_part_receipt(
        self: Rc<Self>,
        params: wire::sparrow::IngestPartReceiptParams,
        mut results: wire::sparrow::IngestPartReceiptResults,
    ) -> capnp::Result<()> {
        let query = params.get()?.get_query()?;
        let input = || -> Result<String, Error> {
            self.check_store(
                query
                    .get_store_id()
                    .map_err(invalid)?
                    .to_str()
                    .map_err(invalid)?,
            )?;
            if query.get_schema_version() != sparrow_client::SCHEMA_VERSION {
                return Err(Error::new(
                    "UNSUPPORTED",
                    "part receipt reads require the current discovered schema",
                ));
            }
            Ok(query
                .get_request_id()
                .map_err(invalid)?
                .to_str()
                .map_err(invalid)?
                .to_owned())
        };
        let request_id = match input() {
            Ok(request_id) => request_id,
            Err(error) => {
                failure(results.get().init_result(), error);
                return Ok(());
            }
        };
        match self
            .lookup(move |store, now| store.ingest_part_receipt(&request_id, now))
            .await
        {
            Ok(state) => {
                let mut out = results.get().init_result().init_ok();
                let (receipt, mut slot) = match state {
                    sparrow_core::PartReceiptState::Absent => {
                        out.set_absent(());
                        return Ok(());
                    }
                    sparrow_core::PartReceiptState::Stored(receipt) => (receipt, out.init_stored()),
                    sparrow_core::PartReceiptState::Evicted(receipt) => {
                        (receipt, out.init_evicted())
                    }
                    sparrow_core::PartReceiptState::Evicting(receipt) => {
                        (receipt, out.init_evicting())
                    }
                };
                slot.set_request_id(&receipt.request_id);
                slot.set_artifact_id(&receipt.artifact_id);
                slot.set_byte_length(receipt.byte_length);
                match sparrow_client::parse_format(&receipt.format) {
                    Ok(format) => slot.set_format(format),
                    Err(error) => {
                        // A stored format label that does not parse is a broken store, not a
                        // transport fault.
                        failure(
                            results.get().init_result(),
                            Error::new("STORAGE_FAILURE", error.to_string()),
                        );
                    }
                }
            }
            Err(error) => failure(results.get().init_result(), error),
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
            .import_work(id, |store, id, now| {
                Ok((store.import_get(id, now)?, store.store_id()?))
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
            .import_work(id, |store, id, now| {
                Ok((store.import_seal(id, now)?, store.store_id()?))
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
            .import_work(id, |store, id, now| {
                Ok((store.import_prepare_layout(id, now)?, store.store_id()?))
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
            .import_work(id, move |store, id, now| {
                Ok((
                    store.import_prepare_record(id, expected, now)?,
                    store.store_id()?,
                ))
            })
            .await
        {
            Ok((value, store)) => {
                import_preparation(results.get().init_result().init_ok(), value, &store);
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
            .import_work(id, |store, id, now| {
                Ok((store.import_publish(id, now)?, store.store_id()?))
            })
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
                self.lookup(move |store, now| store.rdf_inspect(&id, now))
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
                out.set_diagnostic_primary_rows(number(&value, "diagnostic_primary_rows")?);
                if let Some(graph) = value.get("diagnostic_graph").and_then(Value::as_str) {
                    out.reborrow().init_diagnostic_graph().set_value(graph);
                }
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
                self.read_work(move |store, now, cancellation| {
                    store.records_context_cancellable(
                        &id,
                        &scope?,
                        (before, after),
                        now,
                        &cancellation,
                    )
                })
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
                context_capture(out.reborrow().init_capture(), &value)?;
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
                receipt.set_certainty(if !value.diagnostics.complete(limited) {
                    wire::Certainty::Incomplete
                } else {
                    wire::Certainty::Exact
                });
                coverage(
                    receipt.init_coverage(),
                    &value.diagnostics.encode(
                        1 + value.before.visited + value.after.visited,
                        (1 + value.before.records.len() + value.after.records.len()) as u64,
                        [0; 3],
                        limited,
                    ),
                )?;
                record(out.reborrow().init_anchor(), value.anchor, &value.store_id)?;
                context_side(out.reborrow().init_before(), value.before, &value.store_id)?;
                context_side(out.init_after(), value.after, &value.store_id)?;
            }
        }
        Ok(())
    }
    async fn diagnostic_define(
        self: Rc<Self>,
        params: wire::sparrow::DiagnosticDefineParams,
        mut results: wire::sparrow::DiagnosticDefineResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_definition()?;
        let source = input.get_source()?;
        if source.len() > 512 {
            failure(
                results.get().init_result(),
                invalid("diagnostic source exceeds 512 bytes"),
            );
            return Ok(());
        }
        let source = source.to_str()?.to_owned();
        let mappings = crate::diagnostics::mappings(input.get_mappings()?)?;
        match self
            .work(move |store, _| {
                Ok((
                    store.define_diagnostic_profile(&source, mappings?)?,
                    store.store_id()?,
                ))
            })
            .await
        {
            Ok((profile, store)) => {
                crate::diagnostics::profile(results.get().init_result().init_ok(), &profile, &store)
            }
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn diagnostic_get(
        self: Rc<Self>,
        params: wire::sparrow::DiagnosticGetParams,
        mut results: wire::sparrow::DiagnosticGetResults,
    ) -> capnp::Result<()> {
        let source = params.get()?.get_source()?;
        if source.len() > 512 {
            failure(
                results.get().init_result(),
                invalid("diagnostic source exceeds 512 bytes"),
            );
            return Ok(());
        }
        let source = source.to_str()?.to_owned();
        match self
            .lookup(move |store, _| Ok((store.diagnostic_profile(&source)?, store.store_id()?)))
            .await
        {
            Ok((Some(profile), store)) => crate::diagnostics::profile(
                results.get().init_result().init_ok().init_value(),
                &profile,
                &store,
            ),
            Ok((None, _)) => results.get().init_result().init_ok().set_absent(()),
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }

    async fn source_time_define(
        self: Rc<Self>,
        params: wire::sparrow::SourceTimeDefineParams,
        mut results: wire::sparrow::SourceTimeDefineResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_definition()?;
        if input.get_schema_version() != sparrow_client::SCHEMA_VERSION {
            failure(
                results.get().init_result(),
                Error::new(
                    "UNSUPPORTED",
                    "source time definition requires the current schema",
                ),
            );
            return Ok(());
        }
        if input.get_source()?.len() > 512 || input.get_path()?.len() > 4096 {
            failure(
                results.get().init_result(),
                invalid("source time labels exceed their byte admission"),
            );
            return Ok(());
        }
        let source = input.get_source()?.to_str()?.to_owned();
        let path = input.get_path()?.to_str()?.to_owned();
        let representation = time_representation(input.get_representation()?);
        let capture = match input.get_extraction()?.which()? {
            wire::time_extraction::JsonPointer(()) => None,
            wire::time_extraction::LogCapture(value) => {
                if !path.is_empty() {
                    failure(
                        results.get().init_result(),
                        invalid("log time extraction cannot also name a JSON path"),
                    );
                    return Ok(());
                }
                match crate::time_wire::capture(value?) {
                    Ok(value) => Some(value),
                    Err(error) => {
                        failure(results.get().init_result(), invalid(error));
                        return Ok(());
                    }
                }
            }
        };
        match self
            .work(move |store, _| {
                Ok((
                    match &capture {
                        Some(capture) => {
                            store.define_log_time_profile(&source, capture, representation)?
                        }
                        None => store.define_time_profile(&source, &path, representation)?,
                    },
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
        let input = params.get()?;
        if input.get_schema_version() != sparrow_client::SCHEMA_VERSION {
            failure(
                results.get().init_result(),
                Error::new(
                    "UNSUPPORTED",
                    "source time lookup requires the current schema",
                ),
            );
            return Ok(());
        }
        if input.get_source()?.len() > 512 {
            failure(
                results.get().init_result(),
                invalid("source time label exceeds its byte admission"),
            );
            return Ok(());
        }
        let source = input.get_source()?.to_str()?.to_owned();
        match self
            .lookup(move |store, _| Ok((store.time_profile(&source)?, store.store_id()?)))
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
                Error::input_bound("at most 32 field constraints"),
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
        let cursor = query_text(input.get_cursor()?, 160, "query cursor exceeds 160 bytes");
        let operation = if input.get_group_by_artifact() {
            sparrow_core::RecordOperation::ArtifactGroups
        } else {
            sparrow_core::RecordOperation::Records
        };
        match self
            .query_work(
                cursor.as_ref().ok().cloned().flatten(),
                move |store, now, cancellation| {
                    store.records_read_cancellable(
                        &sparrow_core::RecordRead {
                            operation,
                            scope: scope?,
                            query,
                            cursor: cursor?,
                        },
                        now,
                        &cancellation,
                    )
                },
            )
            .await
        {
            Err(error) => failure(results.get().init_result(), error),
            Ok(mut reply) => {
                let value = reply
                    .page
                    .body
                    .result()
                    .map_err(|e| capnp::Error::failed(e.to_string()))?;
                records_page(results.get().init_result().init_ok(), &reply.page, &value)?;
                reply.delivered = true;
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
        if input.get_text()?.len() > 65536 {
            failure(
                results.get().init_result(),
                Error::new("RESOURCE_LIMIT", "text query exceeds 64 KiB"),
            );
            return Ok(());
        }
        let query = sparrow_core::RecordQuery {
            text: Some(input.get_text()?.to_str()?.to_owned()),
            ..Default::default()
        };
        let cursor = query_text(input.get_cursor()?, 160, "query cursor exceeds 160 bytes");
        match self
            .query_work(
                cursor.as_ref().ok().cloned().flatten(),
                move |store, now, cancellation| {
                    store.records_read_cancellable(
                        &sparrow_core::RecordRead {
                            operation: sparrow_core::RecordOperation::Text,
                            scope: scope?,
                            query,
                            cursor: cursor?,
                        },
                        now,
                        &cancellation,
                    )
                },
            )
            .await
        {
            Err(error) => failure(results.get().init_result(), error),
            Ok(mut reply) => {
                let value = reply
                    .page
                    .body
                    .result()
                    .map_err(|e| capnp::Error::failed(e.to_string()))?;
                text_page(results.get().init_result().init_ok(), &reply.page, &value)?;
                reply.delivered = true;
            }
        }
        Ok(())
    }

    async fn discover(
        self: Rc<Self>,
        _: wire::sparrow::DiscoverParams,
        mut results: wire::sparrow::DiscoverResults,
    ) -> capnp::Result<()> {
        let paused = self.pause.paused();
        let discovered = self
            .work(|store, _| Ok((store.properties()?, store.durability_status()?)))
            .await;
        self.pause.read_finished(paused, &discovered);
        match discovered {
            Err(error) => failure(results.get().init_result(), error),
            Ok((value, durability)) => {
                let mut out = results.get().init_result().init_ok();
                out.set_store_id(field::<String>(&value, "store_id")?);
                out.set_schema_version(sparrow_client::SCHEMA_VERSION);
                out.set_max_inline_artifact_bytes(number(&value, "max_inline_artifact_bytes")?);
                out.set_max_records_per_artifact(number(&value, "max_records_per_artifact")?);
                out.set_max_import_bytes(number(&value, "max_import_bytes")?);
                out.set_raw_record_bytes(number(&value, "max_raw_record_bytes")?);
                out.set_retire_row_budget(number(&value, "retire_row_budget")?);
                out.set_retire_chunk_charge(number(&value, "retire_chunk_charge")?);
                out.set_retire_record_charge(number(&value, "retire_record_charge")?);
                out.set_text_analyzer(field::<String>(&value, "text_analyzer")?);
                out.set_text_profile(field::<String>(&value, "text_profile")?);
                out.set_rdf_profile(field::<String>(&value, "rdf_profile")?);
                out.set_max_event_batch_bytes(sparrow_client::MAX_EVENT_BATCH_BYTES as u32);
                out.set_max_event_batch_events(sparrow_client::MAX_EVENT_BATCH_EVENTS as u32);
                producer_limits(out.reborrow().init_producer_limits(), &value)?;
                out.set_instance_role(instance_role(self.store.instance_role()));
                encode_durability(out.init_durability_status(), &durability);
            }
        }
        Ok(())
    }

    async fn status(
        self: Rc<Self>,
        _: wire::sparrow::StatusParams,
        mut results: wire::sparrow::StatusResults,
    ) -> capnp::Result<()> {
        let syslog = match self.syslog.snapshot() {
            Ok(value) => value,
            Err(error) => {
                failure(
                    results.get().init_result(),
                    Error::new("STORAGE_FAILURE", error.to_string()),
                );
                return Ok(());
            }
        };
        match self
            .diagnostic(|store, now| {
                let (status, migrations) = store.status_report(now)?;
                Ok((
                    status,
                    store.durability_status()?,
                    store.pressure_status()?,
                    store.recompression_status()?,
                    migrations,
                    store.consumption_retired()?,
                ))
            })
            .await
        {
            Err(e) => failure(results.get().init_result(), e),
            Ok((value, durability, pressure, recompression, migrations, retired)) => {
                let mut out = results.get().init_result().init_ok();
                crate::pressure::encode(out.reborrow().init_pressure_status(), &pressure)?;
                crate::recompression::encode(out.reborrow().init_recompression(), &recompression);
                crate::recompression::encode_migrations(
                    out.reborrow().init_migrations(migrations.len() as u32),
                    &migrations,
                );
                let watches = self
                    .watches
                    .snapshot()
                    .map_err(|error| capnp::Error::failed(error.to_string()))?;
                let mut states = out.reborrow().init_consume_watches(watches.len() as u32);
                for (index, watch) in watches.into_iter().enumerate() {
                    let mut state = states.reborrow().get(index as u32);
                    state.set_tail_admission_waiting(watch.tail_admission_waiting);
                    state.set_publish_hold(watch.definition.publish_hold);
                    state.set_held_claims(watch.held_claims);
                    crate::watch_admin::encode_clock(
                        state.reborrow().init_oldest_held_first_seen(),
                        watch.oldest_held_first_seen,
                    );
                    state.set_tail_read_bytes(watch.tail_read_bytes);
                    state.set_tail_descriptors(watch.tail_descriptors);
                    state.set_tail_checkpoint_records(watch.tail_checkpoint_records);
                    let mut generations = state
                        .reborrow()
                        .init_tail_generation_ids(watch.tail_generation_ids.len() as u32);
                    for (index, id) in watch.tail_generation_ids.iter().enumerate() {
                        generations.set(index as u32, id);
                    }
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
                    crate::watch_admin::encode_retired(
                        |count| state.reborrow().init_retired_claims(count),
                        &watch.id,
                        &retired,
                    );
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
                encode_durability(out.reborrow().init_durability_status(), &durability);
                out.set_store_id(field::<String>(&value, "store_id")?);
                out.set_store_format(field::<String>(&value, "store_format")?);
                out.set_schema_version(sparrow_client::SCHEMA_VERSION);
                out.set_query_only(self.query_only);
                out.set_instance_role(instance_role(self.store.instance_role()));
                encode_stream_evidence_loss(
                    out.reborrow().init_stream_evidence_loss(),
                    &self.store.stream_evidence_loss(),
                );
                encode_stream_evidence_loss(
                    out.reborrow().init_source_time_loss(),
                    &self.store.source_time_evidence_loss(),
                );
                encode_tail_registry_evidence(
                    out.reborrow().init_tail_registry_evidence(),
                    &self.store.tail_registry_evidence(),
                );
                // The removed backup's field keeps its ordinal and always reads as before for a
                // daemon without one: unconfigured, no root, no failure.
                {
                    let mut backup = out.reborrow().init_backup();
                    backup.set_phase(wire::BackupPhase::Unconfigured);
                    backup.set_root("");
                    backup.init_failure().set_absent(());
                }
                self.admission
                    .encode(out.reborrow().init_admission(), self.connections.as_deref());
                self.pause.encode(out.reborrow().init_floor_pause());
                match &self.ingest_socket {
                    Some(socket) => socket.encode(out.reborrow().init_ingest_socket().init_value()),
                    None => out.reborrow().init_ingest_socket().set_absent(()),
                }
                let mut syslog_rows = out.reborrow().init_syslog_listeners(syslog.len() as u32);
                for (index, status) in syslog.iter().enumerate() {
                    status.encode(syslog_rows.reborrow().get(index as u32));
                }
                out.set_max_event_batch_bytes(sparrow_client::MAX_EVENT_BATCH_BYTES as u32);
                producer_limits(out.reborrow().init_producer_limits(), &value)?;
                out.set_max_event_batch_events(sparrow_client::MAX_EVENT_BATCH_EVENTS as u32);
                out.set_rdf_profile(field::<String>(&value, "rdf_profile")?);
                out.set_text_analyzer(field::<String>(&value, "text_analyzer")?);
                out.set_text_profile(field::<String>(&value, "text_profile")?);
                out.set_physical_bytes(number(&value, "physical_bytes")?);
                out.set_disk_budget(number(&value, "disk_budget")?);
                out.set_maintenance_reserve(number(&value, "maintenance_reserve")?);
                out.set_counts_pending(match field::<String>(&value, "counts")?.as_str() {
                    "pending" => true,
                    "exact" => false,
                    _ => {
                        return Err(capnp::Error::failed(
                            "status counts state is neither pending nor exact".into(),
                        ));
                    }
                });
                out.set_live_records(number(&value, "live_records")?);
                out.set_unique_payload_chunks(number(&value, "unique_payload_chunks")?);
                out.set_active_pins(number(&value, "active_pins")?);
                out.set_unique_json_projections(number(&value, "unique_json_projections")?);
                out.set_retry_window_seconds(number(&value, "retry_window_seconds")?);
                out.set_retention_seconds(number(&value, "retention_seconds")?);
                out.set_max_inline_artifact_bytes(number(&value, "max_inline_artifact_bytes")?);
                out.set_max_records_per_artifact(number(&value, "max_records_per_artifact")?);
                out.set_max_pin_members(number(&value, "max_pin_members")?);
                out.set_import_over_bound_refusals(number(&value, "import_over_bound_refusals")?);
                {
                    let mut staging = out.reborrow().init_staging_refusals();
                    staging.set_refusals(number(&value, "staging_refusals")?);
                    staging.set_last_limit(&field::<String>(&value, "staging_refused_last_limit")?);
                    staging
                        .set_last_source(&field::<String>(&value, "staging_refused_last_source")?);
                    staging.set_last_graph(&field::<String>(&value, "staging_refused_last_graph")?);
                    staging.set_last_bytes(number(&value, "staging_refused_last_bytes")?);
                }
                out.set_archive_halvings(number(&value, "archive_halvings")?);
                self.archive.encode(out.reborrow().init_archive_worker());
                out.set_pin_read_handles(number(&value, "pin_read_handles")?);
                out.set_pin_read_bytes(number(&value, "pin_read_bytes")?);
                out.set_max_pin_read_handles(number(&value, "max_pin_read_handles")?);
                out.set_max_pin_read_bytes(number(&value, "max_pin_read_bytes")?);
                out.set_pin_read_lease_ms(number(&value, "pin_read_lease_ms")?);
                out.set_artifact_read_handles(number(&value, "artifact_read_handles")?);
                out.set_artifact_read_bytes(number(&value, "artifact_read_bytes")?);
                out.set_max_artifact_read_handles(number(&value, "max_artifact_read_handles")?);
                out.set_max_artifact_read_bytes(number(&value, "max_artifact_read_bytes")?);
                out.set_artifact_read_lease_ms(number(&value, "artifact_read_lease_ms")?);
                out.set_source_read_handles(number(&value, "source_read_handles")?);
                out.set_source_read_bytes(number(&value, "source_read_bytes")?);
                out.set_max_source_read_handles(number(&value, "max_source_read_handles")?);
                out.set_max_source_read_bytes(number(&value, "max_source_read_bytes")?);
                out.set_source_read_lease_ms(number(&value, "source_read_lease_ms")?);
                out.set_record_read_handles(number(&value, "record_read_handles")?);
                out.set_record_read_bytes(number(&value, "record_read_bytes")?);
                out.set_max_record_read_handles(number(&value, "max_record_read_handles")?);
                out.set_max_record_read_bytes(number(&value, "max_record_read_bytes")?);
                out.set_record_read_lease_ms(number(&value, "record_read_lease_ms")?);
                out.set_sparql_read_handles(number(&value, "sparql_read_handles")?);
                out.set_sparql_read_bytes(number(&value, "sparql_read_bytes")?);
                out.set_max_sparql_read_handles(number(&value, "max_sparql_read_handles")?);
                out.set_max_sparql_read_bytes(number(&value, "max_sparql_read_bytes")?);
                out.set_sparql_read_lease_ms(number(&value, "sparql_read_lease_ms")?);
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
        // Refuse before copying; the store owns this bound and the staged import has none.
        if bytes.len() > sparrow_core::INLINE_ARTIFACT_BYTES {
            failure(
                results.get().init_result(),
                // No retry shortens the request: not retryable (#227).
                Error::new(
                    "INVALID_ARGUMENT",
                    "inline ingestion carries at most 16 MiB; stage a larger artifact through importBegin",
                ),
            );
            return Ok(());
        }
        let bytes = bytes.to_vec();
        let request_id = request.get_request_id()?.to_str()?.to_owned();
        let source = request.get_source()?.to_str()?.to_owned();
        let format = format(request.get_format()?);
        let mode = durability_mode(request.get_durability()?);
        let intake = self.intake;
        match self
            .work(move |store, now| {
                store.ingest_with_intake(&request_id, &source, format, &bytes, intake, now, mode)
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
            Ok(id) => self.lookup(move |store, _| store.artifact(&id)).await,
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
            Ok(id) => self.lookup(move |store, _| store.record(&id)).await,
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
        let target = match request.which()? {
            wire::read_bytes::Evidence(evidence) => self.scoped_id(evidence?),
            wire::read_bytes::FileGeneration(query) => self.tail_reference(query?),
        };
        let result = match target {
            Ok(id) => {
                self.lookup(move |store, now| store.bytes(&id, offset, length, now))
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
                Error::input_bound("pin input exceeds configured membership limit"),
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

    async fn artifacts_list(
        self: Rc<Self>,
        params: wire::sparrow::ArtifactsListParams,
        mut results: wire::sparrow::ArtifactsListResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_query()?;
        if let Err(error) = self.check_store(input.get_store_id()?.to_str()?) {
            failure(results.get().init_result(), error);
            return Ok(());
        }
        if input.get_schema_version() != sparrow_client::SCHEMA_VERSION {
            failure(
                results.get().init_result(),
                Error::new(
                    "UNSUPPORTED",
                    "artifact listing requires the current discovered schema",
                ),
            );
            return Ok(());
        }
        for (value, maximum) in [(input.get_source()?, 512), (input.get_cursor()?, 160)] {
            if let wire::optional_text::Value(value) = value.which()?
                && value?.len() > maximum
            {
                failure(
                    results.get().init_result(),
                    invalid("artifact selector or cursor exceeds its admission bound"),
                );
                return Ok(());
            }
        }
        let (received_from, received_before) = match input.get_received()?.which()? {
            wire::optional_artifact_receipt_interval::Unfiltered(()) => (0, u64::MAX),
            wire::optional_artifact_receipt_interval::Interval(value) => {
                let value = value?;
                (
                    value.get_from()?.get_unix_millis(),
                    value.get_before()?.get_unix_millis(),
                )
            }
        };
        let selected = match scope(input.get_scope()?, &self)? {
            Ok(selection) => selection,
            Err(error) => {
                failure(results.get().init_result(), error);
                return Ok(());
            }
        };
        if selected.source.is_some()
            || selected.received_from != 0
            || selected.received_before != u64::MAX
            || selected.limit != 100
            || selected.scan_budget != 10000
        {
            failure(
                results.get().init_result(),
                invalid(
                    "artifact scope uses membership and filters; source, receipt interval and limits belong to the outer artifact query",
                ),
            );
            return Ok(());
        }
        let query = sparrow_core::ArtifactRead {
            selector: sparrow_core::ArtifactSelector {
                selection: selected.selection,
                sources: selected.sources,
                diagnostics: selected.diagnostics,
                require_complete_index: selected.require_complete_index,
                source: optional(input.get_source()?)?,
                received_from,
                received_before,
            },
            cursor: optional(input.get_cursor()?)?,
            page_size: input.get_page_size() as usize,
            scan_budget: input.get_scan_budget() as usize,
        };
        match self
            .query_work(query.cursor.clone(), move |store, now, cancellation| {
                store.artifact_read_cancellable(&query, now, &cancellation)
            })
            .await
        {
            Err(error) => failure(results.get().init_result(), error),
            Ok(mut reply) => {
                artifacts_page(results.get().init_result().init_ok(), &reply.page)?;
                reply.delivered = true;
            }
        }
        Ok(())
    }

    async fn tail_generation_get(
        self: Rc<Self>,
        params: wire::sparrow::TailGenerationGetParams,
        mut results: wire::sparrow::TailGenerationGetResults,
    ) -> capnp::Result<()> {
        let query = params.get()?.get_query()?;
        let id = match self.tail_reference(query) {
            Ok(id) => id,
            Err(error) => {
                failure(results.get().init_result(), error);
                return Ok(());
            }
        };
        match self
            .lookup(move |store, _| store.tail_generation(&id))
            .await
        {
            Err(error) => failure(results.get().init_result(), error),
            Ok(generation) => crate::tail_wire::generation(
                results.get().init_result().init_ok(),
                &generation,
                &self
                    .store
                    .store_id()
                    .map_err(|error| capnp::Error::failed(error.to_string()))?,
            )?,
        }
        Ok(())
    }

    async fn syslog_listener_get(
        self: Rc<Self>,
        params: wire::sparrow::SyslogListenerGetParams,
        mut results: wire::sparrow::SyslogListenerGetResults,
    ) -> capnp::Result<()> {
        let query = params.get()?.get_query()?;
        if let Err(error) = self.check_store(query.get_store_id()?.to_str()?) {
            failure(results.get().init_result(), error);
            return Ok(());
        }
        if query.get_schema_version() != sparrow_client::SCHEMA_VERSION {
            failure(
                results.get().init_result(),
                Error::new(
                    "UNSUPPORTED",
                    "syslog listener reads require the current discovered schema",
                ),
            );
            return Ok(());
        }
        let source = query.get_source()?;
        let id = query.get_id()?;
        if source.is_empty() || source.len() > 512 || id.is_empty() || id.len() > 128 {
            failure(
                results.get().init_result(),
                invalid(
                    "syslog listener requires source 1..512 bytes and listener ID 1..128 bytes",
                ),
            );
            return Ok(());
        }
        let source = source.to_str()?;
        let id = id.to_str()?;
        match self.syslog.snapshot() {
            Err(error) => failure(
                results.get().init_result(),
                Error::new("STORAGE_FAILURE", error.to_string()),
            ),
            Ok(listeners) => match listeners.into_iter().find(|listener| {
                listener.definition.source == source && listener.definition.id == id
            }) {
                Some(listener) => listener.encode(results.get().init_result().init_ok()),
                None => failure(
                    results.get().init_result(),
                    Error::new(
                        "NOT_FOUND",
                        "syslog listener is not currently configured under that exact source",
                    ),
                ),
            },
        }
        Ok(())
    }

    async fn receiver_window_get(
        self: Rc<Self>,
        params: wire::sparrow::ReceiverWindowGetParams,
        mut results: wire::sparrow::ReceiverWindowGetResults,
    ) -> capnp::Result<()> {
        let query = params.get()?.get_query()?;
        if let Err(error) = self.check_store(query.get_store_id()?.to_str()?) {
            failure(results.get().init_result(), error);
            return Ok(());
        }
        if query.get_schema_version() != sparrow_client::SCHEMA_VERSION {
            failure(
                results.get().init_result(),
                Error::new(
                    "UNSUPPORTED",
                    "receiver window reads require the current discovered schema",
                ),
            );
            return Ok(());
        }
        let id = query.get_id()?;
        if id.len() > 128 {
            failure(
                results.get().init_result(),
                invalid("receiver window ID exceeds 128 bytes"),
            );
            return Ok(());
        }
        let id = id.to_str()?.to_owned();
        match self
            .lookup(move |store, now| store.receiver_window(&id, now))
            .await
        {
            Err(error) => failure(results.get().init_result(), error),
            Ok(window) => {
                let mut out = results.get().init_result().init_ok();
                out.set_store_id(
                    &self
                        .store
                        .store_id()
                        .map_err(|error| capnp::Error::failed(error.to_string()))?,
                );
                out.set_id(&window.id);
                out.set_source(&window.source);
                out.set_listener(&window.listener);
                out.set_instance(&window.instance);
                out.set_transport(match window.transport.as_str() {
                    "udp" => wire::ReceiverTransport::Udp,
                    "tcp_octet_counting" => wire::ReceiverTransport::TcpOctetCounting,
                    "tcp_lf" => wire::ReceiverTransport::TcpLf,
                    "tcp_cr_lf" => wire::ReceiverTransport::TcpCrLf,
                    _ => {
                        return Err(capnp::Error::failed(
                            "validated receiver transport changed".into(),
                        ));
                    }
                });
                out.set_observations(window.observations);
                out.set_accepted(window.accepted);
                out.set_discarded(window.discarded);
                out.set_refused(window.refused);
                out.set_truncated(window.truncated);
                out.set_framing_failures(window.framing_failures);
                out.set_timed_out(window.timed_out);
                out.set_gaps_complete(window.gaps_complete);
                out.set_continuity_unknown(true);
                out.set_pre_commit_loss_unknown(true);
                out.reborrow()
                    .init_first_received_at()
                    .set_unix_millis(window.first_received_at);
                out.reborrow()
                    .init_last_received_at()
                    .set_unix_millis(window.last_received_at);
                out.reborrow()
                    .init_expires_at()
                    .set_unix_millis(window.expires_at);
                let mut gaps = out.init_gaps(window.gaps.len() as u32);
                for (index, gap) in window.gaps.iter().enumerate() {
                    let mut row = gaps.reborrow().get(index as u32);
                    row.set_first(gap.first);
                    row.set_last(gap.last);
                }
            }
        }
        Ok(())
    }

    async fn producer_stream_get(
        self: Rc<Self>,
        params: wire::sparrow::ProducerStreamGetParams,
        mut results: wire::sparrow::ProducerStreamGetResults,
    ) -> capnp::Result<()> {
        let query = params.get()?.get_query()?;
        if let Err(error) = self.check_store(query.get_store_id()?.to_str()?) {
            failure(results.get().init_result(), error);
            return Ok(());
        }
        if query.get_schema_version() != sparrow_client::SCHEMA_VERSION {
            failure(
                results.get().init_result(),
                Error::new(
                    "UNSUPPORTED",
                    "producer stream reads require the current discovered schema",
                ),
            );
            return Ok(());
        }
        let id = query.get_id()?;
        if id.len() > 128 {
            failure(
                results.get().init_result(),
                invalid("producer stream ID exceeds 128 bytes"),
            );
            return Ok(());
        }
        let id = id.to_str()?.to_owned();
        match self
            .lookup(move |store, now| store.producer_stream(&id, now))
            .await
        {
            Err(error) => failure(results.get().init_result(), error),
            Ok(stream) => {
                let mut out = results.get().init_result().init_ok();
                out.set_store_id(
                    &self
                        .store
                        .store_id()
                        .map_err(|error| capnp::Error::failed(error.to_string()))?,
                );
                out.set_id(&stream.id);
                out.set_source(&stream.source);
                let instance = purrdf_hash::hex::decode(&stream.instance)
                    .map_err(|error| capnp::Error::failed(error.to_string()))?;
                out.set_instance(&instance);
                out.set_first_sequence(stream.first_sequence);
                out.set_last_sequence(stream.last_sequence);
                out.set_accepted_events(stream.accepted_events);
                out.set_unobserved_sequences(stream.unobserved_sequences);
                out.set_gaps_complete(stream.gaps_complete);
                out.set_continuity_unknown(true);
                out.reborrow()
                    .init_first_received_at()
                    .set_unix_millis(stream.first_received_at);
                out.reborrow()
                    .init_last_received_at()
                    .set_unix_millis(stream.last_received_at);
                out.reborrow()
                    .init_expires_at()
                    .set_unix_millis(stream.expires_at);
                let mut gaps = out.init_gaps(stream.gaps.len() as u32);
                for (index, gap) in stream.gaps.iter().enumerate() {
                    let mut row = gaps.reborrow().get(index as u32);
                    row.set_first(gap.first);
                    row.set_last(gap.last);
                }
            }
        }
        Ok(())
    }

    async fn sources_list(
        self: Rc<Self>,
        params: wire::sparrow::SourcesListParams,
        mut results: wire::sparrow::SourcesListResults,
    ) -> capnp::Result<()> {
        let input = params.get()?.get_query()?;
        if let Err(error) = self.check_store(input.get_store_id()?.to_str()?) {
            failure(results.get().init_result(), error);
            return Ok(());
        }
        if input.get_schema_version() != sparrow_client::SCHEMA_VERSION {
            failure(
                results.get().init_result(),
                Error::new(
                    "UNSUPPORTED",
                    "source listing requires the current discovered schema",
                ),
            );
            return Ok(());
        }
        for (value, maximum) in [(input.get_source()?, 512), (input.get_cursor()?, 160)] {
            if let wire::optional_text::Value(value) = value.which()?
                && value?.len() > maximum
            {
                failure(
                    results.get().init_result(),
                    invalid("source selector or cursor exceeds its admission bound"),
                );
                return Ok(());
            }
        }
        let query = sparrow_core::SourceRead {
            selector: sparrow_core::SourceSelector {
                source: optional(input.get_source()?)?,
            },
            cursor: optional(input.get_cursor()?)?,
            page_size: input.get_page_size() as usize,
            scan_budget: input.get_scan_budget() as usize,
        };
        let configured = match self.watches.snapshot() {
            Ok(watches) => watches
                .into_iter()
                .map(|watch| sparrow_core::SourceWatchBinding {
                    source: watch.definition.source,
                    watch_id: watch.id,
                    revision: watch.revision,
                    enabled: watch.enabled,
                })
                .collect::<Vec<_>>(),
            Err(error) => {
                failure(
                    results.get().init_result(),
                    Error::new("STORAGE_FAILURE", error.to_string()),
                );
                return Ok(());
            }
        };
        let intakes = match self.syslog.sources() {
            Ok(intakes) => intakes,
            Err(error) => {
                failure(
                    results.get().init_result(),
                    Error::new("STORAGE_FAILURE", error.to_string()),
                );
                return Ok(());
            }
        };
        match self
            .query_work(query.cursor.clone(), move |store, now, cancellation| {
                store.source_read_with_syslog_cancellable(
                    &query,
                    now,
                    &configured,
                    &intakes,
                    &cancellation,
                )
            })
            .await
        {
            Err(error) => failure(results.get().init_result(), error),
            Ok(mut reply) => {
                sources_page(results.get().init_result().init_ok(), &reply.page)?;
                reply.delivered = true;
            }
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
            .query_work(query.cursor.clone(), move |store, now, cancellation| {
                store.pin_read_cancellable(&query, now, &cancellation)
            })
            .await
        {
            Err(error) => failure(results.get().init_result(), error),
            Ok(mut reply) => {
                pins_page(results.get().init_result().init_ok(), &reply.page)?;
                reply.delivered = true;
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
            Err(Error::new(
                "STORE_MISMATCH",
                "pin belongs to a different store",
            ))
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

    async fn result_release(
        self: Rc<Self>,
        params: wire::sparrow::ResultReleaseParams,
        mut results: wire::sparrow::ResultReleaseResults,
    ) -> capnp::Result<()> {
        let query = params.get()?.get_query()?;
        if query.get_schema_version() != sparrow_client::SCHEMA_VERSION {
            failure(
                results.get().init_result(),
                Error::new("UNSUPPORTED", "result release requires the current schema"),
            );
            return Ok(());
        }
        if query.get_store_id()?.len() > 128 || query.get_snapshot_id()?.len() > 128 {
            failure(
                results.get().init_result(),
                invalid("result release identity exceeds admission"),
            );
            return Ok(());
        }
        let store_id = query.get_store_id()?.to_str()?.to_owned();
        let snapshot_id = query.get_snapshot_id()?.to_str()?.to_owned();
        let (store, snapshot) = (store_id.clone(), snapshot_id.clone());
        match self
            .work(move |owner, now| owner.result_release(&store, &snapshot, now))
            .await
        {
            Ok(released) => {
                let mut out = results.get().init_result().init_ok();
                out.set_store_id(&store_id);
                out.set_snapshot_id(&snapshot_id);
                out.set_released(released);
            }
            Err(error) => failure(results.get().init_result(), error),
        }
        Ok(())
    }

    async fn result_next(
        self: Rc<Self>,
        params: wire::sparrow::ResultNextParams,
        mut results: wire::sparrow::ResultNextResults,
    ) -> capnp::Result<()> {
        let query = params.get()?.get_query()?;
        if query.get_schema_version() != sparrow_client::SCHEMA_VERSION {
            failure(
                results.get().init_result(),
                Error::new(
                    "UNSUPPORTED",
                    "result continuation requires the current schema",
                ),
            );
            return Ok(());
        }
        if query.get_cursor()?.len() > 160 || query.get_store_id()?.len() > 128 {
            failure(
                results.get().init_result(),
                invalid("result cursor/store identity exceeds admission"),
            );
            return Ok(());
        }
        let store_id = query.get_store_id()?.to_str()?.to_owned();
        let cursor = query.get_cursor()?.to_str()?.to_owned();
        // A live artifact continuation publishes a page under the same delivery
        // guard as `artifacts_list`; every other operation replays its capture.
        match self
            .read_work(move |store, now, _| {
                Ok(match store.result_next(&store_id, &cursor, now)? {
                    sparrow_core::ResultPage::Artifacts(page) => {
                        Next::Artifacts(Box::new(QueryReply::new(page, store, Some(&cursor))))
                    }
                    page => Next::Replay(Box::new(page)),
                })
            })
            .await
        {
            Err(error) => failure(results.get().init_result(), error),
            Ok(Next::Artifacts(mut reply)) => {
                artifacts_page(
                    results.get().init_result().init_ok().init_artifacts(),
                    &reply.page,
                )?;
                reply.delivered = true;
            }
            Ok(Next::Replay(page)) => {
                let out = results.get().init_result().init_ok();
                match *page {
                    sparrow_core::ResultPage::Records(page) => records_page(
                        out.init_records(),
                        &page,
                        &page
                            .body
                            .result()
                            .map_err(|e| capnp::Error::failed(e.to_string()))?,
                    )?,
                    sparrow_core::ResultPage::Text(page) => text_page(
                        out.init_text(),
                        &page,
                        &page
                            .body
                            .result()
                            .map_err(|e| capnp::Error::failed(e.to_string()))?,
                    )?,
                    sparrow_core::ResultPage::Keys(page) => keys_page(
                        out.init_keys(),
                        &page,
                        &page
                            .body
                            .result()
                            .map_err(|e| capnp::Error::failed(e.to_string()))?,
                    )?,
                    sparrow_core::ResultPage::Values(page) => values_page(
                        out.init_values(),
                        &page,
                        &page
                            .body
                            .result()
                            .map_err(|e| capnp::Error::failed(e.to_string()))?,
                    )?,
                    sparrow_core::ResultPage::Artifacts(page) => {
                        artifacts_page(out.init_artifacts(), &page)?
                    }
                    sparrow_core::ResultPage::Sources(page) => {
                        sources_page(out.init_sources(), &page)?
                    }
                    sparrow_core::ResultPage::Pins(page) => pins_page(out.init_pins(), &page)?,
                    sparrow_core::ResultPage::Sparql(page) => {
                        if let Err(error) = sparql::page(out.init_sparql(), &page) {
                            failure(results.get().init_result(), error);
                        }
                    }
                    sparrow_core::ResultPage::SparqlExplanation(page) => {
                        if let Err(error) = sparql::explanation_page(out.init_sparql(), &page) {
                            failure(results.get().init_result(), error);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn sparql_query(
        self: Rc<Self>,
        params: wire::sparrow::SparqlQueryParams,
        mut results: wire::sparrow::SparqlQueryResults,
    ) -> capnp::Result<()> {
        sparql::query(
            &self,
            params.get()?.get_query()?,
            results.get().init_result(),
            sparql::Operation::Query,
        )
        .await
    }

    async fn sparql_explain(
        self: Rc<Self>,
        params: wire::sparrow::SparqlExplainParams,
        mut results: wire::sparrow::SparqlExplainResults,
    ) -> capnp::Result<()> {
        sparql::query(
            &self,
            params.get()?.get_query()?,
            results.get().init_result(),
            sparql::Operation::Explain,
        )
        .await
    }

    async fn correlate(
        self: Rc<Self>,
        params: wire::sparrow::CorrelateParams,
        mut results: wire::sparrow::CorrelateResults,
    ) -> capnp::Result<()> {
        correlation::query(&self, params, &mut results).await
    }

    async fn keys_list(
        self: Rc<Self>,
        params: wire::sparrow::KeysListParams,
        mut results: wire::sparrow::KeysListResults,
    ) -> capnp::Result<()> {
        let input = params.get()?;
        let scope = scope(input.get_scope()?, &self)?;
        let cursor = query_text(input.get_cursor()?, 160, "query cursor exceeds 160 bytes");
        let path = query_text(
            input.get_path()?,
            sparrow_core::JSON_QUERY_PATH_BYTES,
            "JSON query path exceeds 65536 UTF-8 bytes",
        );
        match self
            .query_work(
                cursor.as_ref().ok().cloned().flatten(),
                move |store, now, cancellation| {
                    store.json_read_cancellable(
                        &sparrow_core::JsonRead {
                            scope: scope?,
                            query: JsonQuery::Keys { path: path? },
                            cursor: cursor?,
                        },
                        now,
                        &cancellation,
                    )
                },
            )
            .await
        {
            Ok(mut reply) => {
                let value = reply
                    .page
                    .body
                    .result()
                    .map_err(|e| capnp::Error::failed(e.to_string()))?;
                keys_page(results.get().init_result().init_ok(), &reply.page, &value)?;
                reply.delivered = true;
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
        let cursor = query_text(input.get_cursor()?, 160, "query cursor exceeds 160 bytes");
        let path = query_text(
            input.get_path()?,
            sparrow_core::JSON_QUERY_PATH_BYTES,
            "JSON query path exceeds 65536 UTF-8 bytes",
        );
        let kind = optional_kind(input.get_kind()?)?;
        let lexical = optional(input.get_lexical()?)?;
        let decoded = optional(input.get_decoded()?)?;
        match self
            .query_work(
                cursor.as_ref().ok().cloned().flatten(),
                move |store, now, cancellation| {
                    store.json_read_cancellable(
                        &sparrow_core::JsonRead {
                            scope: scope?,
                            query: JsonQuery::Values {
                                path: path?,
                                kind,
                                lexical,
                                decoded,
                            },
                            cursor: cursor?,
                        },
                        now,
                        &cancellation,
                    )
                },
            )
            .await
        {
            Ok(mut reply) => {
                let value = reply
                    .page
                    .body
                    .result()
                    .map_err(|e| capnp::Error::failed(e.to_string()))?;
                values_page(results.get().init_result().init_ok(), &reply.page, &value)?;
                reply.delivered = true;
            }
            Err(e) => failure(results.get().init_result(), e),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One lifetime scenario drives every native catalogue body and its replayable cursor.
    async fn capture_lifetime<B: Send + 'static, F>(
        rpc: &Rpc,
        admission: &Arc<Semaphore>,
        handles: &str,
        capture: F,
    ) where
        F: Fn(
                Store,
                u64,
                &sparrow_core::QueryCancellation,
                Option<String>,
            ) -> Result<sparrow_core::CatalogReadPage<B>, Error>
            + Clone
            + Send
            + 'static,
    {
        for owns_capture in [true, false] {
            let original = if owns_capture {
                None
            } else {
                let action = capture.clone();
                let mut reply = rpc
                    .query_work(None, move |store, now, token| {
                        action(store, now, &token, None)
                    })
                    .await
                    .unwrap();
                let original = (
                    reply.page.store_id.clone(),
                    reply.page.snapshot_id.clone(),
                    reply
                        .page
                        .next_cursor
                        .clone()
                        .expect("fixture requires continuation"),
                );
                reply.delivered = true;
                drop(reply);
                Some(original)
            };
            let cursor = original.as_ref().map(|(_, _, cursor)| cursor.clone());
            let expected = if owns_capture { "0" } else { "1" };
            for (after_capture, connection_close) in
                [(false, false), (true, false), (false, true), (true, true)]
            {
                let (worker, connection_guard) = rpc.for_connection();
                let action = capture.clone();
                let cursor = cursor.clone();
                let (entered, running) = tokio::sync::oneshot::channel();
                let (release, paused) = std::sync::mpsc::channel();
                let held = cursor.clone();
                let task = tokio::spawn(async move {
                    worker
                        .query_work(held, move |store, now, cancellation| {
                            let page = if after_capture {
                                Some(action(store.clone(), now, &cancellation, cursor.clone())?)
                            } else {
                                None
                            };
                            entered
                                .send(())
                                .map_err(|_| invalid("test caller disappeared"))?;
                            paused.recv().map_err(invalid)?;
                            match page {
                                Some(page) => Ok(page),
                                None => action(store, now, &cancellation, cursor),
                            }
                        })
                        .await
                });
                tokio::time::timeout(std::time::Duration::from_secs(5), running)
                    .await
                    .unwrap()
                    .unwrap();
                if connection_close {
                    drop(connection_guard);
                } else {
                    task.abort();
                }
                assert_eq!(admission.available_permits(), 0);
                release.send(()).unwrap();
                if connection_close {
                    assert!(matches!(task.await, Ok(Err(error)) if error.code == "CANCELLED"));
                } else {
                    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
                }
                let permit = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    admission.clone().acquire_owned(),
                )
                .await
                .unwrap()
                .unwrap();
                drop(permit);
                assert_eq!(
                    rpc.store.status(now().unwrap()).unwrap()[handles].as_str(),
                    Some(expected)
                );
            }
            let action = capture.clone();
            let held = cursor.clone();
            let reply = rpc
                .query_work(held, move |store, now, token| {
                    action(store, now, &token, cursor)
                })
                .await
                .unwrap();
            drop(reply);
            assert_eq!(
                rpc.store.status(now().unwrap()).unwrap()[handles].as_str(),
                Some(expected)
            );
            if let Some((store_id, id, cursor)) = original {
                // A live artifact continuation publishes its own page; release it too.
                let next = rpc
                    .store
                    .result_next(&store_id, &cursor, now().unwrap())
                    .unwrap();
                if let sparrow_core::ResultPage::Artifacts(page) = &next
                    && page.snapshot_id != id
                {
                    rpc.store
                        .result_release(&store_id, &page.snapshot_id, now().unwrap())
                        .unwrap();
                }
                assert!(
                    rpc.store
                        .result_release(&store_id, &id, now().unwrap())
                        .unwrap()
                );
            }
            assert_eq!(
                rpc.store.status(now().unwrap()).unwrap()[handles].as_str(),
                Some("0")
            );
        }
    }

    #[tokio::test]
    async fn cancelled_rpc_keeps_worker_admission_until_storage_work_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let config =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let store = crate::ServerConfig::read(&config)
            .unwrap()
            .open_store()
            .unwrap();
        let admission = Arc::new(Semaphore::new(1));
        let rpc = Arc::new(Rpc::new(store, admission.clone(), 100));
        let (entered, running) = tokio::sync::oneshot::channel();
        let (release, paused) = std::sync::mpsc::channel();
        let worker = rpc.clone();
        let task = tokio::spawn(async move {
            worker
                .work(move |store, now| {
                    entered
                        .send(())
                        .map_err(|_| invalid("test caller disappeared"))?;
                    paused.recv().map_err(invalid)?;
                    store.ingest(
                        "cancelled-intake",
                        "mutation",
                        "opaque",
                        b"admitted mutation",
                        now,
                    )
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
        assert_eq!(
            rpc.store.status(now().unwrap()).unwrap()["live_records"].as_str(),
            Some("1")
        );

        let instant = now().unwrap();
        rpc.store
            .ingest(
                "cancel-a",
                "cancel-a",
                "json",
                br#"{"value":"needle"}"#,
                instant,
            )
            .unwrap();
        rpc.store
            .ingest(
                "cancel-b",
                "cancel-b",
                "json",
                br#"{"value":"needle"}"#,
                instant,
            )
            .unwrap();
        let query = sparrow_core::RecordRead {
            operation: sparrow_core::RecordOperation::Records,
            scope: QueryScope {
                selection: sparrow_core::QuerySelection::Live,
                source: None,
                sources: None,
                diagnostics: Default::default(),
                require_complete_index: false,
                received_from: instant,
                received_before: instant + 1,
                limit: 1,
                scan_budget: 100,
            },
            query: Default::default(),
            cursor: None,
        };
        capture_lifetime(
            &rpc,
            &admission,
            "record_read_handles",
            move |store, now, token, cursor| {
                let mut query = query.clone();
                query.cursor = cursor;
                store.records_read_cancellable(&query, now, token)
            },
        )
        .await;
        capture_lifetime(
            &rpc,
            &admission,
            "source_read_handles",
            |store, now, token, cursor| {
                store.source_read_with_syslog_cancellable(
                    &sparrow_core::SourceRead {
                        cursor,
                        page_size: 1,
                        ..Default::default()
                    },
                    now,
                    &[],
                    &[],
                    token,
                )
            },
        )
        .await;
        // A live listing: an abandoned continuation keeps the delivered cursor.
        capture_lifetime(
            &rpc,
            &admission,
            "artifact_read_handles",
            |store, now, token, cursor| {
                store.artifact_read_cancellable(
                    &sparrow_core::ArtifactRead {
                        cursor,
                        page_size: 1,
                        ..Default::default()
                    },
                    now,
                    token,
                )
            },
        )
        .await;
        let records = rpc
            .store
            .records(instant)
            .unwrap()
            .into_iter()
            .map(|record| record.id)
            .collect::<Vec<_>>();
        let pin = rpc
            .store
            .pin("cancel-pin-a", "cancel-pin-a", &records, 40, instant)
            .unwrap();
        rpc.store
            .pin("cancel-pin-b", "cancel-pin-b", &records, 40, instant)
            .unwrap();
        capture_lifetime(
            &rpc,
            &admission,
            "pin_read_handles",
            |store, now, token, cursor| {
                store.pin_read_cancellable(
                    &PinRead {
                        cursor,
                        page_size: 1,
                        ..Default::default()
                    },
                    now,
                    token,
                )
            },
        )
        .await;
        capture_lifetime(
            &rpc,
            &admission,
            "pin_read_handles",
            move |store, now, token, cursor| {
                store.pin_read_cancellable(
                    &PinRead {
                        selector: PinSelector::Id(pin.id.clone()),
                        cursor,
                        page_size: 1,
                        ..Default::default()
                    },
                    now,
                    token,
                )
            },
        )
        .await;
        assert_eq!(
            rpc.store.status(now().unwrap()).unwrap()["active_pins"].as_str(),
            Some("2")
        );
        admission.close();
        let error = rpc
            .work(|store, now| {
                store.ingest(
                    "closed-intake",
                    "closed",
                    "opaque",
                    b"must not publish",
                    now,
                )
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, "SHUTTING_DOWN");
        assert_eq!(
            rpc.store.status(now().unwrap()).unwrap()["live_records"].as_str(),
            Some("3")
        );
        let mut message = capnp::message::Builder::new_default();
        failure(
            message.init_root::<wire::result::Builder<wire::artifact::Owned>>(),
            error,
        );
        let reply = message
            .get_root_as_reader::<wire::result::Reader<wire::artifact::Owned>>()
            .unwrap();
        let wire::result::Error(error) = reply.which().unwrap() else {
            panic!("closed admission rendered a successful reply");
        };
        let error = error.unwrap();
        assert_eq!(error.get_code().unwrap().to_str().unwrap(), "SHUTTING_DOWN");
        assert!(error.get_retryable());
    }

    /// An RPC over a fresh store whose operation budget is `slots` permits.
    fn admission_fixture(slots: usize) -> (tempfile::TempDir, Arc<Rpc>, Arc<Semaphore>) {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let config =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let store = crate::ServerConfig::read(&config)
            .unwrap()
            .open_store()
            .unwrap();
        let operations = Arc::new(Semaphore::new(slots));
        let rpc = Arc::new(Rpc::new(store, operations.clone(), 100));
        (dir, rpc, operations)
    }

    /// Run a no-op write on its own task and let it reach its wait.
    async fn spawn_write(rpc: &Arc<Rpc>) -> tokio::task::JoinHandle<Result<u8, Error>> {
        let rpc = rpc.clone();
        let task = tokio::spawn(async move { rpc.work(|_, _| Ok(7u8)).await });
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        task
    }

    /// INVARIANT (#246): a writer that finds the permit budget exhausted waits for a permit
    /// and is served when one frees, instead of being refused at once. Waiters are served in
    /// arrival order, and the wait is counted.
    #[tokio::test(start_paused = true)]
    async fn a_writer_waits_for_a_permit_and_is_served_in_arrival_order() {
        let (_dir, rpc, operations) = admission_fixture(1);
        let held = operations.clone().acquire_owned().await.unwrap();
        let first = spawn_write(&rpc).await;
        let second = spawn_write(&rpc).await;
        assert!(
            !first.is_finished(),
            "a writer was refused instead of waiting"
        );
        assert!(!second.is_finished());
        assert_eq!(rpc.admission.writer_waits().waiting, 2);
        drop(held);
        assert_eq!(first.await.unwrap().unwrap(), 7);
        assert_eq!(second.await.unwrap().unwrap(), 7);
        let waits = rpc.admission.writer_waits();
        assert_eq!((waits.waits, waits.waiting, waits.timeouts), (2, 0, 0));
    }

    /// INVARIANT (#246): a writer that waits the whole bound is answered with the retryable
    /// `RESOURCE_LIMIT`, after the bound and before the client's own `rpc_timeout`; the timeout
    /// and the longest wait are counted.
    #[tokio::test(start_paused = true)]
    async fn a_writer_that_waits_out_its_bound_gets_the_retryable_resource_limit() {
        let (_dir, rpc, operations) = admission_fixture(1);
        let _held = operations.clone().acquire_owned().await.unwrap();
        let started = tokio::time::Instant::now();
        let error = spawn_write(&rpc).await.await.unwrap().unwrap_err();
        assert_eq!(error.code, "RESOURCE_LIMIT");
        let waited = started.elapsed();
        assert!(waited >= crate::admission::WRITER_PERMIT_WAIT, "{waited:?}");
        assert!(
            waited < sparrow_client::DeliveryConfig::new("rpc.sock", "client").rpc_timeout,
            "{waited:?}"
        );
        let mut message = capnp::message::Builder::new_default();
        failure(
            message.init_root::<wire::result::Builder<wire::artifact::Owned>>(),
            error,
        );
        let reply = message
            .get_root_as_reader::<wire::result::Reader<wire::artifact::Owned>>()
            .unwrap();
        let wire::result::Error(error) = reply.which().unwrap() else {
            panic!("a refusal rendered a successful reply");
        };
        // The client's retry class is the wire flag: occupancy stays retryable (#397).
        assert!(sparrow_client::Error::domain(error.unwrap()).retryable());
        let waits = rpc.admission.writer_waits();
        assert_eq!((waits.waits, waits.waiting, waits.timeouts), (1, 0, 1));
        assert!(waits.longest_ms >= 3000);
    }

    /// INVARIANT (#246): shutdown interrupts a waiting writer at once with `SHUTTING_DOWN`
    /// (not after its bound as `RESOURCE_LIMIT`), whether shutdown begins or the budget closes.
    #[tokio::test(start_paused = true)]
    async fn shutdown_interrupts_waiting_writers() {
        let started = tokio::time::Instant::now();
        // Shutdown beginning interrupts a writer queued behind the drain.
        let (_dir, rpc, operations) = admission_fixture(1);
        let _held = operations.clone().acquire_owned().await.unwrap();
        let waiting = spawn_write(&rpc).await;
        assert!(!waiting.is_finished());
        rpc.admission.begin_shutdown();
        assert_eq!(waiting.await.unwrap().unwrap_err().code, "SHUTTING_DOWN");
        assert_eq!(rpc.admission.writer_waits().waiting, 0);
        // So does the budget closing, and a writer that arrives after is told the same.
        let (_dir, rpc, operations) = admission_fixture(1);
        let _held = operations.clone().acquire_owned().await.unwrap();
        let waiting = spawn_write(&rpc).await;
        assert!(!waiting.is_finished());
        operations.close();
        assert_eq!(waiting.await.unwrap().unwrap_err().code, "SHUTTING_DOWN");
        assert_eq!(
            spawn_write(&rpc).await.await.unwrap().unwrap_err().code,
            "SHUTTING_DOWN"
        );
        assert_eq!(started.elapsed(), std::time::Duration::ZERO);
    }

    /// INVARIANT (#246, #91): readers can hold at most `max - writer_reserve` permits, so a
    /// query storm leaves the reserved permit to ingest, and a reader beyond its share is
    /// refused at once and counted rather than queued ahead of a writer.
    #[tokio::test(start_paused = true)]
    async fn a_query_storm_cannot_starve_the_writer_sub_pool() {
        let (_dir, rpc, _operations) = admission_fixture(4);
        assert_eq!(rpc.admission.writer_reserve(), 1);
        assert_eq!(rpc.admission.reader_slots(), 3);
        let storm: Vec<_> = (0..3).map(|_| rpc.admission.reader().unwrap()).collect();
        let refused = rpc.lookup(|_, _| Ok(1u8)).await.unwrap_err();
        assert_eq!(refused.code, "RESOURCE_LIMIT");
        assert_eq!(rpc.admission.reader_refusals(), 1);
        let started = tokio::time::Instant::now();
        assert_eq!(rpc.work(|_, _| Ok(7u8)).await.unwrap(), 7);
        assert_eq!(started.elapsed(), std::time::Duration::ZERO);
        drop(storm);
        assert_eq!(rpc.lookup(|_, _| Ok(1u8)).await.unwrap(), 1);
    }

    /// INVARIANT (#246): `status` is served while writers hold every operation permit, because
    /// its handler takes the diagnostic class; a handler that went back to the reader class
    /// would be refused here with `RESOURCE_LIMIT`, hiding the counters that explain it.
    #[tokio::test(start_paused = true)]
    async fn the_status_handler_takes_no_operation_permit() {
        let (_dir, rpc, operations) = admission_fixture(1);
        let _writers = operations.clone().acquire_owned().await.unwrap();
        let client = Rpc::clone(&rpc).into_client();
        let response = client.status_request().send().promise.await.unwrap();
        let result = response.get().unwrap().get_result().unwrap();
        assert!(
            matches!(result.which().unwrap(), wire::result::Ok(_)),
            "status was refused while writers held the budget"
        );
    }
}
