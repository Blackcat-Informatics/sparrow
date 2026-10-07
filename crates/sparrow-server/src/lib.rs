// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Typed Cap'n Proto contract and daemon RPC implementation.

/// Generated schema bindings, shared by server and clients.
// capnpc 0.27 emits an unused generic parameter in Result<T>'s annotation
// lookup. Keep this generator-specific style allowance out of handwritten code.
#[allow(missing_docs, clippy::extra_unused_type_parameters)]
pub mod sparrow_capnp {
    include!(concat!(env!("OUT_DIR"), "/sparrow_capnp.rs"));
}

mod rpc;
pub use rpc::{Rpc, now};
mod client;
pub use client::{Connection, connect};
