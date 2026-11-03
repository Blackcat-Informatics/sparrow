// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Anonymous public TCP admission and its shared-secret CHAP upgrade.
//!
//! An anonymous peer may discover the store, ingest and import. Every other listed
//! operation returns a typed `UNAUTHENTICATED` failure; an operation added to the
//! schema without a decision here falls back to the generated `unimplemented` error.
//! A successful CHAP exchange returns the connection's complete typed service. Every
//! forward, and the session, passes through the one write gate in `query_only`.

use crate::{rpc::failure, sparrow_capnp::sparrow};
use sparrow_client::{CHAP_BYTES, TcpSecret};
use sparrow_core::Error;
use std::{cell::Cell, rc::Rc};

/// One public TCP connection's anonymous capability and single CHAP attempt.
pub(crate) struct AnonymousTcp {
    gate: Rc<crate::query_only::WriteGate>,
    secret: TcpSecret,
    nonce: Cell<Option<[u8; CHAP_BYTES]>>,
    finished: Cell<bool>,
}

impl AnonymousTcp {
    pub(crate) fn new(rpc: crate::Rpc, secret: TcpSecret) -> Self {
        Self {
            gate: Rc::new(rpc.into_gate()),
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
        if !self
            .secret
            .verify(&self.gate.rpc().store_id()?, &nonce, response)
        {
            return Err(unauthenticated("CHAP response refused; reconnect to retry"));
        }
        Ok(self.gate.rpc().clone())
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
            <crate::query_only::WriteGate as sparrow::Server>::$method(self.gate.clone(), params, results).await
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
            Ok(session) => out.init_ok().set_session(session.into_client()),
            Err(error) => failure(out, error),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// INVARIANT: an anonymous TCP peer of a query-only service reaches the same write gate
    /// as a Unix connection, so the writes it may otherwise call are refused with READ_ONLY.
    #[tokio::test(flavor = "current_thread")]
    async fn anonymous_tcp_writes_pass_through_the_query_only_gate() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let config =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let store = crate::ServerConfig::read(&config)
            .unwrap()
            .open_store()
            .unwrap();
        let rpc = crate::Rpc::new(
            store,
            std::sync::Arc::new(tokio::sync::Semaphore::new(4)),
            100,
        )
        .query_only();
        let secret = dir.path().join("tcp.secret");
        std::fs::write(&secret, [9; 32]).unwrap();
        std::fs::set_permissions(
            &secret,
            <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o600),
        )
        .unwrap();
        let client: sparrow::Client =
            capnp_rpc::new_client(AnonymousTcp::new(rpc, TcpSecret::read(&secret).unwrap()));
        let mut request = client.ingest_request();
        let mut input = request.get().init_request();
        input.set_request_id("anonymous-query-only");
        input.set_source("tcp");
        input.set_format(crate::sparrow_capnp::Format::Jsonl);
        input.set_bytes(b"{\"refused\":true}\n");
        let response = request.send().promise.await.unwrap();
        match response
            .get()
            .unwrap()
            .get_result()
            .unwrap()
            .which()
            .unwrap()
        {
            crate::sparrow_capnp::result::Error(error) => {
                assert_eq!(
                    error.unwrap().get_code().unwrap().to_str().unwrap(),
                    "READ_ONLY"
                );
            }
            crate::sparrow_capnp::result::Ok(_) => panic!("query-only TCP ingest was admitted"),
        }
    }
}
