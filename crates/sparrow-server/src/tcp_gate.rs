// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Anonymous public TCP admission and its shared-secret CHAP upgrade.
//!
//! An anonymous peer may discover the store, ingest and import. Every other listed
//! operation returns a typed `UNAUTHENTICATED` failure; an operation added to the
//! schema without a decision here falls back to the generated `unimplemented` error.
//! A successful CHAP exchange returns the connection's ungated typed service.

use crate::{rpc::failure, sparrow_capnp::sparrow};
use sparrow_client::{CHAP_BYTES, TcpSecret};
use sparrow_core::Error;
use std::{cell::Cell, rc::Rc};

/// One public TCP connection's anonymous capability and single CHAP attempt.
pub(crate) struct AnonymousTcp {
    rpc: Rc<crate::Rpc>,
    secret: TcpSecret,
    nonce: Cell<Option<[u8; CHAP_BYTES]>>,
    finished: Cell<bool>,
}

impl AnonymousTcp {
    pub(crate) fn new(rpc: crate::Rpc, secret: TcpSecret) -> Self {
        Self {
            rpc: Rc::new(rpc),
            secret,
            nonce: Cell::new(None),
            finished: Cell::new(false),
        }
    }

    fn open(&self) -> Result<(), Error> {
        if self.finished.get() {
            return Err(unauthenticated(
                "CHAP on this connection has already succeeded or failed; reconnect",
            ));
        }
        Ok(())
    }

    fn challenge(&self) -> Result<[u8; CHAP_BYTES], Error> {
        self.open()?;
        let mut nonce = [0; CHAP_BYTES];
        getrandom::fill(&mut nonce)
            .map_err(|error| Error::new("STORAGE_FAILURE", format!("CHAP nonce: {error}")))?;
        self.nonce.set(Some(nonce));
        Ok(nonce)
    }

    /// Consume the challenge and finish this connection's only attempt before any await.
    fn session(&self, response: &[u8]) -> Result<crate::Rpc, Error> {
        self.open()?;
        self.finished.set(true);
        let nonce = self
            .nonce
            .take()
            .ok_or_else(|| unauthenticated("authenticate requires an outstanding authChallenge"))?;
        if !self.secret.verify(&self.rpc.store_id()?, &nonce, response) {
            return Err(unauthenticated("CHAP response refused; reconnect to retry"));
        }
        Ok((*self.rpc).clone())
    }
}

fn unauthenticated(message: &str) -> Error {
    Error::new("UNAUTHENTICATED", message.to_owned())
}

macro_rules! forward {
    ($($method:ident: $params:ident, $results:ident;)*) => {$(
        async fn $method(
            self: Rc<Self>,
            params: sparrow::$params,
            results: sparrow::$results,
        ) -> capnp::Result<()> {
            <crate::Rpc as sparrow::Server>::$method(self.rpc.clone(), params, results).await
        }
    )*};
}

macro_rules! refuse {
    ($($method:ident: $params:ident, $results:ident;)*) => {$(
        async fn $method(
            self: Rc<Self>,
            _: sparrow::$params,
            mut results: sparrow::$results,
        ) -> capnp::Result<()> {
            failure(
                results.get().init_result(),
                unauthenticated("public TCP peers may only discover, ingest and import before a CHAP-authenticated session"),
            );
            Ok(())
        }
    )*};
}

impl sparrow::Server for AnonymousTcp {
    forward! {
        ingest: IngestParams, IngestResults;
        import_begin: ImportBeginParams, ImportBeginResults;
        import_append: ImportAppendParams, ImportAppendResults;
        import_get: ImportGetParams, ImportGetResults;
        import_seal: ImportSealParams, ImportSealResults;
        import_layout: ImportLayoutParams, ImportLayoutResults;
        import_prepare: ImportPrepareParams, ImportPrepareResults;
        import_publish: ImportPublishParams, ImportPublishResults;
        import_cancel: ImportCancelParams, ImportCancelResults;
        events_ingest: EventsIngestParams, EventsIngestResults;
        flush: FlushParams, FlushResults;
        discover: DiscoverParams, DiscoverResults;
    }

    refuse! {
        status: StatusParams, StatusResults;
        artifact_get: ArtifactGetParams, ArtifactGetResults;
        record_get: RecordGetParams, RecordGetResults;
        bytes_read: BytesReadParams, BytesReadResults;
        pin: PinParams, PinResults;
        pin_release: PinReleaseParams, PinReleaseResults;
        keys_list: KeysListParams, KeysListResults;
        values_search: ValuesSearchParams, ValuesSearchResults;
        text_search: TextSearchParams, TextSearchResults;
        records_context: RecordsContextParams, RecordsContextResults;
        rdf_inspect: RdfInspectParams, RdfInspectResults;
        pin_renew: PinRenewParams, PinRenewResults;
        pin_read: PinReadParams, PinReadResults;
        records_search: RecordsSearchParams, RecordsSearchResults;
        source_time_define: SourceTimeDefineParams, SourceTimeDefineResults;
        source_time_get: SourceTimeGetParams, SourceTimeGetResults;
        watch_list: WatchListParams, WatchListResults;
        watch_put: WatchPutParams, WatchPutResults;
        watch_remove: WatchRemoveParams, WatchRemoveResults;
        watch_handoff: WatchHandoffParams, WatchHandoffResults;
        watch_handoff_get: WatchHandoffGetParams, WatchHandoffGetResults;
        diagnostic_define: DiagnosticDefineParams, DiagnosticDefineResults;
        diagnostic_get: DiagnosticGetParams, DiagnosticGetResults;
        artifacts_list: ArtifactsListParams, ArtifactsListResults;
        sources_list: SourcesListParams, SourcesListResults;
        result_next: ResultNextParams, ResultNextResults;
        producer_stream_get: ProducerStreamGetParams, ProducerStreamGetResults;
        tail_generation_get: TailGenerationGetParams, TailGenerationGetResults;
        tail_finish: TailFinishParams, TailFinishResults;
        tail_finish_get: TailFinishGetParams, TailFinishGetResults;
        result_release: ResultReleaseParams, ResultReleaseResults;
        syslog_listener_get: SyslogListenerGetParams, SyslogListenerGetResults;
        receiver_window_get: ReceiverWindowGetParams, ReceiverWindowGetResults;
        ingest_status: IngestStatusParams, IngestStatusResults;
        retention_metrics: RetentionMetricsParams, RetentionMetricsResults;
        sparql_query: SparqlQueryParams, SparqlQueryResults;
        correlate: CorrelateParams, CorrelateResults;
        sparql_explain: SparqlExplainParams, SparqlExplainResults;
    }

    async fn auth_challenge(
        self: Rc<Self>,
        _: sparrow::AuthChallengeParams,
        mut results: sparrow::AuthChallengeResults,
    ) -> capnp::Result<()> {
        let out = results.get().init_result();
        match self.challenge() {
            Ok(nonce) => out.init_ok().set_nonce(&nonce),
            Err(error) => failure(out, error),
        }
        Ok(())
    }

    async fn authenticate(
        self: Rc<Self>,
        params: sparrow::AuthenticateParams,
        mut results: sparrow::AuthenticateResults,
    ) -> capnp::Result<()> {
        let out = results.get().init_result();
        match self.session(params.get()?.get_response()?) {
            Ok(session) => out
                .init_ok()
                .set_session(capnp_rpc::new_client::<sparrow::Client, _>(session)),
            Err(error) => failure(out, error),
        }
        Ok(())
    }
}
