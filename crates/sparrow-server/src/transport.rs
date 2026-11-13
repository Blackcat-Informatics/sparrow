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
    /// The private Unix socket: the complete typed service, or its query-only gate. Its
    /// admin operations record this caller (see [`local_caller`]).
    Local(String),
    /// Public TCP: the anonymous gate, upgraded by CHAP with this secret; the authenticated
    /// session records this peer address as its administrator.
    PublicTcp(sparrow_client::TcpSecret, String),
    /// The group-accessible ingest socket: producer operations only, each recording this
    /// kernel-reported caller in the provenance of the evidence it creates.
    IngestSocket(sparrow_core::PeerCredential),
}

/// The private socket's caller as the kernel reports it, which admin operations record. The
/// socket is mode 0600, so only the daemon's own account reaches it; the process is named.
pub fn local_caller(stream: &tokio::net::UnixStream) -> std::io::Result<String> {
    let credential = stream.peer_cred()?;
    Ok(format!(
        "unix uid={} pid={}",
        credential.uid(),
        credential
            .pid()
            .map_or_else(|| "unknown".to_owned(), |pid| pid.to_string())
    ))
}

/// Every Unix or TCP connection closes this long after admission.
pub(crate) const CONNECTION_LEASE: std::time::Duration = std::time::Duration::from_secs(60);

/// Serve one accepted stream under a connection slot taken before it was accepted, and the
/// lease. The slot frees when the connection ends, which includes the lease expiring.
pub fn admit_connection<S>(stream: S, rpc: crate::Rpc, slot: crate::admission::Slot, access: Access)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + 'static,
{
    let connection = slot.activate();
    tokio::task::spawn_local(async move {
        let connection = connection;
        let (rpc, _lifetime) = rpc.for_connection();
        let client = match access {
            Access::Local(caller) => rpc.with_admin(caller).into_client(),
            Access::PublicTcp(secret, peer) => {
                capnp_rpc::new_client(crate::ingest_gate::IngestGate::new(
                    rpc,
                    crate::tcp_gate::AnonymousTcp::new(secret, peer),
                ))
            }
            Access::IngestSocket(credential) => {
                capnp_rpc::new_client(crate::ingest_gate::IngestGate::new(
                    rpc.with_peer(credential),
                    crate::ingest_gate::IngestSocketPolicy,
                ))
            }
        };
        match tokio::time::timeout(CONNECTION_LEASE, serve_connection(stream, client)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => eprintln!("sparrowd: RPC connection failed: {error}"),
            Err(_) => {
                connection.ceiling().lease_expired();
                eprintln!(
                    "sparrowd: {} lease expired; the connection was closed",
                    connection.ceiling().name()
                );
            }
        }
    });
}
