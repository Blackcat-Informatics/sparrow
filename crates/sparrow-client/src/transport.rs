// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Generated typed client transport, shared by CLI and agent adapters.

use crate::sparrow_capnp::sparrow;
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use std::path::Path;
use tokio_util::compat::TokioAsyncReadCompatExt;

/// Explicit typed RPC destination, shared by direct clients and logging workers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Endpoint {
    /// Private local Unix socket.
    Unix(std::path::PathBuf),
    /// Numeric TCP address used anonymously: discovery, ingestion, flush and imports only.
    Tcp(std::net::SocketAddr),
    /// Numeric TCP address whose session is authenticated by the shared-secret CHAP law.
    AuthenticatedTcp(std::net::SocketAddr, crate::TcpSecret),
}

impl Endpoint {
    /// Read an example/CLI destination: `tcp://IP:PORT` or a Unix socket path.
    pub fn parse(value: &str) -> Result<Self, crate::Error> {
        let endpoint = match value.strip_prefix("tcp://") {
            Some(address) => Self::Tcp(address.parse().map_err(|_| {
                crate::Error::InvalidArgument("TCP endpoint requires a numeric IP address and port")
            })?),
            None if !value.starts_with('/') && value.contains("://") => {
                return Err(crate::Error::InvalidArgument(
                    "endpoint requires a Unix socket path or tcp://IP:PORT",
                ));
            }
            None => Self::Unix(value.into()),
        };
        endpoint.validate()?;
        Ok(endpoint)
    }

    /// Read `[--secret-file PATH] ENDPOINT` from the front of command arguments.
    /// Returns the endpoint and the remaining arguments; a secret requires TCP.
    pub fn from_args(args: &[String]) -> Result<(Self, &[String]), crate::Error> {
        let (secret_path, rest) = match args {
            [flag, path, rest @ ..] if flag == "--secret-file" => (Some(Path::new(path)), rest),
            rest => (None, rest),
        };
        let [endpoint, rest @ ..] = rest else {
            return Err(crate::Error::InvalidArgument(
                "arguments require [--secret-file PATH] ENDPOINT",
            ));
        };
        let endpoint = Self::parse(endpoint)?;
        let endpoint = match secret_path {
            Some(path) => endpoint.with_secret_file(path)?,
            None => endpoint,
        };
        Ok((endpoint, rest))
    }

    /// Authenticate this TCP destination using a private shared-secret file.
    pub fn with_secret_file(self, path: &Path) -> Result<Self, crate::Error> {
        let Self::Tcp(address) = self else {
            return Err(crate::Error::InvalidArgument(
                "a secret file requires an unauthenticated tcp:// endpoint",
            ));
        };
        Ok(Self::AuthenticatedTcp(
            address,
            crate::TcpSecret::read(path)?,
        ))
    }

    pub(crate) fn validate(&self) -> Result<(), crate::Error> {
        match self {
            Self::Unix(path) => {
                let bytes = path.as_os_str().as_encoded_bytes();
                if bytes.is_empty() || bytes.contains(&0) {
                    return Err(crate::Error::InvalidArgument(
                        "Unix socket endpoint requires a nonempty pathname without NUL bytes",
                    ));
                }
                std::os::unix::net::SocketAddr::from_pathname(path).map_err(|_| {
                    crate::Error::InvalidArgument("Unix socket endpoint exceeds the system address limit; shorten the resolved socket pathname (107 bytes maximum on Linux)")
                })?;
            }
            Self::Tcp(address) | Self::AuthenticatedTcp(address, _) if address.port() == 0 => {
                return Err(crate::Error::InvalidArgument(
                    "TCP endpoint requires a nonzero port",
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

/// An owned RPC connection; dropping it closes this client's transport.
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
