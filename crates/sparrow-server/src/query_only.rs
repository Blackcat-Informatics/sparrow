// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The one write refusal of a query-only daemon.
//!
//! Every operation is classified here, once: a read is forwarded to the typed service
//! unchanged, and a write returns a typed `READ_ONLY` failure before its request is decoded.
//! An operation added to the schema without a decision here falls back to the generated
//! `unimplemented` error, so no new operation can write by omission.

use crate::{rpc::failure, sparrow_capnp::sparrow};
use sparrow_core::Error;
use std::rc::Rc;

/// The complete typed service of a query-only daemon, with every write refused.
pub(crate) struct QueryOnly(Rc<crate::Rpc>);

impl QueryOnly {
    pub(crate) fn new(rpc: crate::Rpc) -> Self {
        Self(Rc::new(rpc))
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
        impl sparrow::Server for QueryOnly {
            $(
                async fn $read(
                    self: Rc<Self>,
                    params: sparrow::$read_params,
                    results: sparrow::$read_results,
                ) -> capnp::Result<()> {
                    <crate::Rpc as sparrow::Server>::$read(self.0.clone(), params, results).await
                }
            )*
            $(
                async fn $write(
                    self: Rc<Self>,
                    _: sparrow::$write_params,
                    mut results: sparrow::$write_results,
                ) -> capnp::Result<()> {
                    failure(results.get().init_result(), read_only());
                    Ok(())
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
        import_get: ImportGetParams, ImportGetResults;
        retention_metrics: RetentionMetricsParams, RetentionMetricsResults;
        sparql_query: SparqlQueryParams, SparqlQueryResults;
        sparql_explain: SparqlExplainParams, SparqlExplainResults;
        correlate: CorrelateParams, CorrelateResults;
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
    }
}
