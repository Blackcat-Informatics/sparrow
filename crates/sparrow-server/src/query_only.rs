// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The one write gate every connection's typed service passes through.
//!
//! Every operation is classified here, once. A read is forwarded to the typed service
//! unchanged. A write is forwarded too, unless the daemon is query-only; then it returns a
//! typed `READ_ONLY` failure before its request is decoded. Unix connections, CHAP sessions
//! and the anonymous TCP gate all forward through this type. An operation added to the schema
//! without a decision here falls back to the generated `unimplemented` error, so no new
//! operation can write by omission.

use crate::{rpc::failure, sparrow_capnp::sparrow};
use sparrow_core::Error;
use std::rc::Rc;

/// The complete typed service of one connection, with writes refused in query-only mode.
pub(crate) struct WriteGate {
    rpc: Rc<crate::Rpc>,
    query_only: bool,
}

impl WriteGate {
    pub(crate) fn new(rpc: crate::Rpc, query_only: bool) -> Self {
        Self {
            rpc: Rc::new(rpc),
            query_only,
        }
    }

    /// The service behind the gate, for the store identity CHAP binds and its sessions.
    pub(crate) fn rpc(&self) -> &crate::Rpc {
        &self.rpc
    }
}

fn read_only() -> Error {
    Error::new(
        "READ_ONLY",
        "this daemon is in query-only mode and refuses every write; it serves reads only",
    )
}

macro_rules! classify {
    (
        reads { $($read:ident: $read_params:ident, $read_results:ident;)* }
        writes { $($write:ident: $write_params:ident, $write_results:ident;)* }
    ) => {
        impl sparrow::Server for WriteGate {
            $(
                async fn $read(
                    self: Rc<Self>,
                    params: sparrow::$read_params,
                    results: sparrow::$read_results,
                ) -> capnp::Result<()> {
                    <crate::Rpc as sparrow::Server>::$read(self.rpc.clone(), params, results).await
                }
            )*
            $(
                async fn $write(
                    self: Rc<Self>,
                    params: sparrow::$write_params,
                    mut results: sparrow::$write_results,
                ) -> capnp::Result<()> {
                    if self.query_only {
                        failure(results.get().init_result(), read_only());
                        return Ok(());
                    }
                    <crate::Rpc as sparrow::Server>::$write(self.rpc.clone(), params, results).await
                }
            )*
        }
    };
}

classify! {
    reads {
        status: StatusParams, StatusResults;
        discover: DiscoverParams, DiscoverResults;
        auth_challenge: AuthChallengeParams, AuthChallengeResults;
        authenticate: AuthenticateParams, AuthenticateResults;
        artifact_get: ArtifactGetParams, ArtifactGetResults;
        record_get: RecordGetParams, RecordGetResults;
        bytes_read: BytesReadParams, BytesReadResults;
        keys_list: KeysListParams, KeysListResults;
        values_search: ValuesSearchParams, ValuesSearchResults;
        text_search: TextSearchParams, TextSearchResults;
        records_context: RecordsContextParams, RecordsContextResults;
        records_search: RecordsSearchParams, RecordsSearchResults;
        rdf_inspect: RdfInspectParams, RdfInspectResults;
        pin_read: PinReadParams, PinReadResults;
        source_time_get: SourceTimeGetParams, SourceTimeGetResults;
        diagnostic_get: DiagnosticGetParams, DiagnosticGetResults;
        watch_list: WatchListParams, WatchListResults;
        watch_handoff_get: WatchHandoffGetParams, WatchHandoffGetResults;
        tail_generation_get: TailGenerationGetParams, TailGenerationGetResults;
        tail_finish_get: TailFinishGetParams, TailFinishGetResults;
        artifacts_list: ArtifactsListParams, ArtifactsListResults;
        sources_list: SourcesListParams, SourcesListResults;
        result_next: ResultNextParams, ResultNextResults;
        // Releases an in-memory query capture; it writes nothing to the store.
        result_release: ResultReleaseParams, ResultReleaseResults;
        producer_stream_get: ProducerStreamGetParams, ProducerStreamGetResults;
        syslog_listener_get: SyslogListenerGetParams, SyslogListenerGetResults;
        receiver_window_get: ReceiverWindowGetParams, ReceiverWindowGetResults;
        ingest_status: IngestStatusParams, IngestStatusResults;
        ingest_part_receipt: IngestPartReceiptParams, IngestPartReceiptResults;
        import_get: ImportGetParams, ImportGetResults;
        retention_metrics: RetentionMetricsParams, RetentionMetricsResults;
        sparql_query: SparqlQueryParams, SparqlQueryResults;
        sparql_explain: SparqlExplainParams, SparqlExplainResults;
        correlate: CorrelateParams, CorrelateResults;
        watch_claims: WatchClaimsParams, WatchClaimsResults;
    }
    writes {
        ingest: IngestParams, IngestResults;
        events_ingest: EventsIngestParams, EventsIngestResults;
        import_begin: ImportBeginParams, ImportBeginResults;
        import_append: ImportAppendParams, ImportAppendResults;
        import_seal: ImportSealParams, ImportSealResults;
        import_layout: ImportLayoutParams, ImportLayoutResults;
        import_prepare: ImportPrepareParams, ImportPrepareResults;
        import_publish: ImportPublishParams, ImportPublishResults;
        import_cancel: ImportCancelParams, ImportCancelResults;
        flush: FlushParams, FlushResults;
        pin: PinParams, PinResults;
        pin_renew: PinRenewParams, PinRenewResults;
        pin_release: PinReleaseParams, PinReleaseResults;
        watch_put: WatchPutParams, WatchPutResults;
        watch_remove: WatchRemoveParams, WatchRemoveResults;
        watch_handoff: WatchHandoffParams, WatchHandoffResults;
        tail_finish: TailFinishParams, TailFinishResults;
        source_time_define: SourceTimeDefineParams, SourceTimeDefineResults;
        diagnostic_define: DiagnosticDefineParams, DiagnosticDefineResults;
        watch_claim_retire: WatchClaimRetireParams, WatchClaimRetireResults;
        // Writes nothing to the store, but a query-only daemon takes no backup (#156).
        backup: BackupParams, BackupResults;
    }
}
