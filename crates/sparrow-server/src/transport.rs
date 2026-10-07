// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use tokio_util::compat::TokioAsyncReadCompatExt;

/// Serve one owned Unix stream through the shared generated capability.
/// Run within a Tokio LocalSet. The caller owns connection admission and its lease.
pub async fn serve_connection(
    stream: tokio::net::UnixStream,
    client: crate::sparrow_capnp::sparrow::Client,
) -> capnp::Result<()> {
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
