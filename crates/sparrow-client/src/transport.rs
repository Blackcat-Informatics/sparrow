// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Generated typed client transport, shared by CLI and agent adapters.

use crate::sparrow_capnp::sparrow;
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use std::path::Path;
use tokio_util::compat::TokioAsyncReadCompatExt;

/// An owned local RPC connection; dropping it closes this client's transport.
pub struct Connection {
    /// Typed Sparrow capability; no JSON operation dispatch is used on the wire.
    pub client: sparrow::Client,
    task: tokio::task::JoinHandle<capnp::Result<()>>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Connect to the daemon's private Unix socket within a Tokio `LocalSet`.
pub async fn connect(socket: &Path) -> capnp::Result<Connection> {
    connect_typed(socket).await.map_err(|error| match error {
        crate::Error::Io(error) => error.into(),
        crate::Error::Transport(error) => error,
        error => capnp::Error::failed(error.to_string()),
    })
}

pub(crate) async fn connect_typed(socket: &Path) -> Result<Connection, crate::Error> {
    let stream = tokio::net::UnixStream::connect(socket).await?;
    Ok(from_stream(stream))
}

/// Connect to a public typed TCP endpoint within a Tokio `LocalSet`.
pub async fn connect_tcp(address: std::net::SocketAddr) -> Result<Connection, crate::Error> {
    let stream = tokio::net::TcpStream::connect(address).await?;
    stream.set_nodelay(true)?;
    Ok(from_stream(stream))
}

fn from_stream<S>(stream: S) -> Connection
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + 'static,
{
    let (reader, writer) = stream.compat().split();
    let network = twoparty::VatNetwork::new(
        reader,
        writer,
        rpc_twoparty_capnp::Side::Client,
        capnp::message::ReaderOptions {
            traversal_limit_in_words: Some(8 * 1024 * 1024),
            nesting_limit: 64,
        },
    );
    let mut system = RpcSystem::new(Box::new(network), None);
    let client = system.bootstrap(rpc_twoparty_capnp::Side::Server);
    let task = tokio::task::spawn_local(system);
    Connection { client, task }
}
