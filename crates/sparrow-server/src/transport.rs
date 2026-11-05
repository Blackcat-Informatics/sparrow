// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use tokio_util::compat::TokioAsyncReadCompatExt;

/// Serve one owned byte stream through the shared generated capability.
/// Run within a Tokio LocalSet. The caller owns connection admission and its lease.
pub async fn serve_connection<S>(
    stream: S,
    client: crate::sparrow_capnp::sparrow::Client,
) -> capnp::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + 'static,
{
    let (reader, writer) = stream.compat().split();
    let network = twoparty::VatNetwork::new(
        reader,
        writer,
        rpc_twoparty_capnp::Side::Server,
        capnp::message::ReaderOptions {
            traversal_limit_in_words: Some(8 * 1024 * 1024),
            nesting_limit: 64,
        },
    );
    RpcSystem::new(Box::new(network), Some(client.client)).await
}

/// Who reached the daemon, which selects the bootstrap capability.
pub enum Access {
    /// The private Unix socket: the complete typed service, or its query-only gate.
    Local,
    /// Public TCP: the anonymous gate, upgraded by CHAP with this secret.
    PublicTcp(sparrow_client::TcpSecret),
}

/// Every Unix or TCP connection closes this long after admission.
pub(crate) const CONNECTION_LEASE: std::time::Duration = std::time::Duration::from_secs(60);

/// Apply the same connection ceiling and lease to Unix and public TCP streams.
pub fn admit_connection<S>(
    stream: S,
    rpc: crate::Rpc,
    connections: std::sync::Arc<tokio::sync::Semaphore>,
    access: Access,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + 'static,
{
    let Ok(permit) = connections.try_acquire_owned() else {
        return;
    };
    tokio::task::spawn_local(async move {
        let _permit = permit;
        let (rpc, _lifetime) = rpc.for_connection();
        let client = match access {
            Access::Local => rpc.into_client(),
            Access::PublicTcp(secret) => {
                capnp_rpc::new_client(crate::tcp_gate::AnonymousTcp::new(rpc, secret))
            }
        };
        match tokio::time::timeout(CONNECTION_LEASE, serve_connection(stream, client)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => eprintln!("sparrowd: RPC connection failed: {error}"),
            Err(_) => eprintln!("sparrowd: RPC connection lease expired"),
        }
    });
}
