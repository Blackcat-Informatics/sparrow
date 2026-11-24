// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The one producer-only admission list, shared by anonymous public TCP and the ingest socket.
//!
//! Every schema operation is decided here once: a producer operation is forwarded, every
//! other one is refused with the endpoint's code, and the two CHAP operations go to the
//! endpoint's policy. An operation added to the schema without a decision here falls back
//! to the generated `unimplemented` error, and a test compares this list with the schema.
//! Every forward passes through the one write gate in `query_only`.

use crate::{query_only::WriteGate, rpc::failure, sparrow_capnp::sparrow};
use sparrow_client::CHAP_BYTES;
use sparrow_core::Error;
use std::rc::Rc;

/// What differs between the endpoints that share the producer-only list.
pub(crate) trait Policy: 'static {
    /// The failure every non-producer operation returns.
    fn refusal(&self) -> Error;
    /// Answer `authChallenge`.
    fn challenge(&self, gate: &WriteGate) -> Result<[u8; CHAP_BYTES], Error>;
    /// Answer `authenticate` with a complete session, or refuse it.
    fn session(&self, gate: &WriteGate, response: &[u8]) -> Result<crate::Rpc, Error>;
}

/// One connection's producer-only capability under an endpoint policy.
pub(crate) struct IngestGate<P> {
    gate: Rc<WriteGate>,
    policy: P,
}

impl<P: Policy> IngestGate<P> {
    pub(crate) fn new(rpc: crate::Rpc, policy: P) -> Self {
        Self {
            gate: Rc::new(rpc.into_gate()),
            policy,
        }
    }
}

/// The ingest socket: members of one group may produce evidence and nothing else.
pub(crate) struct IngestSocketPolicy;

impl Policy for IngestSocketPolicy {
    fn refusal(&self) -> Error {
        Error::new(
            "PERMISSION_DENIED",
            "use the private socket for this operation",
        )
    }

    fn challenge(&self, _: &WriteGate) -> Result<[u8; CHAP_BYTES], Error> {
        Err(crate::rpc::chap_unsupported())
    }

    fn session(&self, _: &WriteGate, _: &[u8]) -> Result<crate::Rpc, Error> {
        Err(crate::rpc::chap_unsupported())
    }
}

macro_rules! decide {
    (
        forward { $($forward:ident: $forward_params:ident, $forward_results:ident;)* }
        refuse { $($refuse:ident: $refuse_params:ident, $refuse_results:ident;)* }
    ) => {
        impl<P: Policy> sparrow::Server for IngestGate<P> {
            $(
                async fn $forward(
                    self: Rc<Self>,
                    params: sparrow::$forward_params,
                    results: sparrow::$forward_results,
                ) -> capnp::Result<()> {
                    <WriteGate as sparrow::Server>::$forward(self.gate.clone(), params, results).await
                }
            )*
            $(
                async fn $refuse(
                    self: Rc<Self>,
                    _: sparrow::$refuse_params,
                    mut results: sparrow::$refuse_results,
                ) -> capnp::Result<()> {
                    failure(results.get().init_result(), self.policy.refusal());
                    Ok(())
                }
            )*

            async fn auth_challenge(
                self: Rc<Self>,
                _: sparrow::AuthChallengeParams,
                mut results: sparrow::AuthChallengeResults,
            ) -> capnp::Result<()> {
                let out = results.get().init_result();
                match self.policy.challenge(&self.gate) {
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
                match self.policy.session(&self.gate, params.get()?.get_response()?) {
                    Ok(session) => out.init_ok().set_session(session.into_client()),
                    Err(error) => failure(out, error),
                }
                Ok(())
            }
        }

        /// Every operation this gate decides, in schema snake case.
        #[cfg(test)]
        const DECISIONS: &[&str] = &[
            $(stringify!($forward),)*
            $(stringify!($refuse),)*
            "auth_challenge",
            "authenticate",
        ];
    };
}

decide! {
    forward {
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
        sealed_events_ingest: SealedEventsIngestParams, SealedEventsIngestResults;
        flush: FlushParams, FlushResults;
        discover: DiscoverParams, DiscoverResults;
    }
    refuse {
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
        ingest_part_receipt: IngestPartReceiptParams, IngestPartReceiptResults;
        payload_lookup: PayloadLookupParams, PayloadLookupResults;
        retention_metrics: RetentionMetricsParams, RetentionMetricsResults;
        sparql_query: SparqlQueryParams, SparqlQueryResults;
        correlate: CorrelateParams, CorrelateResults;
        sparql_explain: SparqlExplainParams, SparqlExplainResults;
        watch_claim_retire: WatchClaimRetireParams, WatchClaimRetireResults;
        backup: BackupParams, BackupResults;
        watch_claims: WatchClaimsParams, WatchClaimsResults;
    }
}

#[cfg(test)]
mod tests {
    use super::{DECISIONS, IngestGate, IngestSocketPolicy};
    use crate::sparrow_capnp::{result, sparrow};

    fn code<T: capnp::traits::Owned>(value: result::Reader<'_, T>) -> Option<String> {
        match value.which().unwrap() {
            result::Ok(_) => None,
            result::Error(error) => Some(error.unwrap().get_code().unwrap().to_string().unwrap()),
        }
    }

    /// INVARIANT: an import is bound to the uid of the connection that ran importBegin, so
    /// another caller cannot alter, publish or cancel it. Publication retains the original
    /// credentials even when the owner resumes from a different connection.
    #[tokio::test(flavor = "current_thread")]
    async fn import_resumed_by_another_uid_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let config =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let store = crate::ServerConfig::read(&config)
            .unwrap()
            .open_store()
            .unwrap();
        let rpc = crate::Rpc::new(
            store.clone(),
            std::sync::Arc::new(tokio::sync::Semaphore::new(4)),
            100,
        );
        let caller = |credential| -> sparrow::Client {
            capnp_rpc::new_client(IngestGate::new(
                rpc.clone().with_peer(credential),
                IngestSocketPolicy,
            ))
        };
        let initiator = sparrow_core::PeerCredential {
            uid: 951,
            gid: 900,
            pid: 42,
        };
        let owner = caller(initiator);
        let other = caller(sparrow_core::PeerCredential {
            uid: 952,
            ..initiator
        });
        let bytes = b"resumed\n";
        let mut begin = owner.import_begin_request();
        let mut input = begin.get().init_request();
        input.set_request_id("bound-import");
        input.set_source("peer");
        input.set_format(crate::sparrow_capnp::Format::Log);
        input.set_byte_length(bytes.len() as u64);
        input.set_payload_id(format!(
            "blake3:{}",
            purrdf_hash::hex::encode(purrdf_hash::blake3::hash(bytes).as_bytes())
        ));
        let begun = begin.send().promise.await.unwrap();
        let staging = match begun.get().unwrap().get_result().unwrap().which().unwrap() {
            result::Ok(start) => match start.unwrap().which().unwrap() {
                crate::sparrow_capnp::import_start::Staging(staging) => staging.unwrap(),
                crate::sparrow_capnp::import_start::Accepted(_) => panic!("import was accepted"),
            },
            result::Error(error) => {
                panic!("importBegin failed: {:?}", error.unwrap().get_message())
            }
        };
        let (store_id, id) = (
            staging.get_store_id().unwrap().to_str().unwrap().to_owned(),
            staging.get_id().unwrap().to_str().unwrap().to_owned(),
        );
        let append = |client: &sparrow::Client| {
            let mut request = client.import_append_request();
            let mut chunk = request.get().init_request();
            let mut import = chunk.reborrow().init_import();
            import.set_store_id(&store_id);
            import.set_id(&id);
            chunk.set_offset(0);
            chunk.set_bytes(bytes);
            request.send().promise
        };
        let refused = append(&other).await.unwrap();
        assert_eq!(
            code(refused.get().unwrap().get_result().unwrap()).as_deref(),
            Some("PERMISSION_DENIED")
        );
        let mut publish = other.import_publish_request();
        let mut import = publish.get().init_import();
        import.set_store_id(&store_id);
        import.set_id(&id);
        let refused = publish.send().promise.await.unwrap();
        assert_eq!(
            code(refused.get().unwrap().get_result().unwrap()).as_deref(),
            Some("PERMISSION_DENIED")
        );
        let mut cancel = other.import_cancel_request();
        let mut import = cancel.get().init_import();
        import.set_store_id(&store_id);
        import.set_id(&id);
        let refused = cancel.send().promise.await.unwrap();
        assert_eq!(
            code(refused.get().unwrap().get_result().unwrap()).as_deref(),
            Some("PERMISSION_DENIED"),
            "a different UID must not cancel the staged import"
        );

        drop(owner);
        let resumed_owner = caller(sparrow_core::PeerCredential {
            gid: 901,
            pid: 43,
            ..initiator
        });
        let resumed = append(&resumed_owner).await.unwrap();
        assert_eq!(code(resumed.get().unwrap().get_result().unwrap()), None);

        let path = dir.path().join("resumed.log");
        std::fs::write(&path, bytes).unwrap();
        let mut file = sparrow_client::open_completed_file(&path).unwrap();
        let receipt = sparrow_client::import_open_file(
            &resumed_owner,
            "bound-import",
            "peer",
            crate::sparrow_capnp::Format::Log,
            &mut file,
        )
        .await
        .unwrap();
        let expected = Some(sparrow_core::Intake::IngestSocket(initiator));
        let artifact = store.artifact(&receipt.id).unwrap();
        assert_eq!(artifact.intake, expected);
        assert_eq!(artifact.record_ids.len(), 1);
        assert_eq!(
            store.record(&artifact.record_ids[0]).unwrap().intake,
            expected
        );
    }

    /// INVARIANT: the shared producer-only gate decides every `Sparrow` operation exactly
    /// once, so an operation added to the schema cannot reach a producer endpoint by
    /// omission or silently fall back to `unimplemented`.
    #[test]
    fn every_schema_operation_has_an_ingest_gate_decision() {
        let schema = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../sparrow-client/schema/sparrow.capnp"
        ));
        let body = schema
            .split_once("interface Sparrow {")
            .and_then(|(_, rest)| rest.split_once("\n}"))
            .expect("schema declares interface Sparrow")
            .0;
        let mut methods: Vec<String> = body
            .lines()
            .map(str::trim)
            .filter(|line| !line.starts_with('#') && line.contains(" @"))
            .map(|line| {
                let name = line.split_once(" @").expect("method ordinal").0;
                name.chars().fold(String::new(), |mut snake, letter| {
                    if letter.is_ascii_uppercase() {
                        snake.push('_');
                    }
                    snake.push(letter.to_ascii_lowercase());
                    snake
                })
            })
            .collect();
        methods.sort();
        let mut decided: Vec<String> = DECISIONS.iter().map(|name| (*name).to_owned()).collect();
        decided.sort();
        let unique = decided.len();
        decided.dedup();
        assert_eq!(unique, decided.len(), "an operation is decided twice");
        assert_eq!(methods, decided);
    }
}
