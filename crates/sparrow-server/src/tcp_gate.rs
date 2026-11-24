// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Anonymous public TCP admission and its shared-secret CHAP upgrade.
//!
//! Anonymous peers have the producer operations defined in `ingest_gate`. Read and
//! administrative operations return a typed `UNAUTHENTICATED` failure.
//! A successful CHAP exchange returns the connection's complete typed service. Every
//! forward, and the session, passes through the one write gate in `query_only`.

use crate::query_only::WriteGate;
use sparrow_client::{CHAP_BYTES, TcpSecret};
use sparrow_core::Error;
use std::cell::Cell;

/// One public TCP connection's single CHAP attempt.
pub(crate) struct AnonymousTcp {
    secret: TcpSecret,
    /// The connection's peer address, recorded as the administrator once CHAP succeeds.
    peer: String,
    nonce: Cell<Option<[u8; CHAP_BYTES]>>,
    finished: Cell<bool>,
}

impl AnonymousTcp {
    pub(crate) fn new(secret: TcpSecret, peer: String) -> Self {
        Self {
            secret,
            peer,
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
}

impl crate::ingest_gate::Policy for AnonymousTcp {
    fn refusal(&self) -> Error {
        unauthenticated("this operation requires a CHAP-authenticated TCP session")
    }

    fn challenge(&self, _: &WriteGate) -> Result<[u8; CHAP_BYTES], Error> {
        self.open()?;
        let mut nonce = [0; CHAP_BYTES];
        getrandom::fill(&mut nonce)
            .map_err(|error| Error::new("STORAGE_FAILURE", format!("CHAP nonce: {error}")))?;
        self.nonce.set(Some(nonce));
        Ok(nonce)
    }

    /// Consume the challenge and finish this connection's only attempt before any await.
    fn session(&self, gate: &WriteGate, response: &[u8]) -> Result<crate::Rpc, Error> {
        self.open()?;
        self.finished.set(true);
        let nonce = self
            .nonce
            .take()
            .ok_or_else(|| unauthenticated("authenticate requires an outstanding authChallenge"))?;
        if !self
            .secret
            .verify(&gate.rpc().store_id()?, &nonce, response)
        {
            return Err(unauthenticated("CHAP response refused; reconnect to retry"));
        }
        Ok(gate
            .rpc()
            .clone()
            .with_producer(crate::source_binding::Caller::Chap {})
            .with_admin(format!("tcp-chap peer={}", self.peer)))
    }
}

fn unauthenticated(message: &str) -> Error {
    Error::new("UNAUTHENTICATED", message.to_owned())
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
        let client: crate::sparrow_capnp::sparrow::Client =
            capnp_rpc::new_client(crate::ingest_gate::IngestGate::new(
                rpc,
                AnonymousTcp::new(TcpSecret::read(&secret).unwrap(), "test".into()),
            ));
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
