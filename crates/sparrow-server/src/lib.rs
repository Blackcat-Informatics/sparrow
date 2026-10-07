// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Typed Cap'n Proto contract and daemon RPC implementation.

pub use sparrow_client::{Connection, connect, sparrow_capnp};
mod transport;
pub use transport::serve_connection;

mod rpc;
pub use rpc::{Rpc, now};
mod agent;
pub use agent::{AgentError, AgentResult, run_cli};
mod mcp;
pub use mcp::run_mcp;
