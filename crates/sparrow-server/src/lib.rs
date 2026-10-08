// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Typed Cap'n Proto contract and daemon RPC implementation.

pub use sparrow_client::{Connection, connect, sparrow_capnp};
mod config;
mod diagnostics;
mod source_file;
pub mod syslog_listener;
mod syslog_wire;
mod tail_wire;
mod tailing;
pub use config::{ConsumeWatch, ProducerContract, ServerConfig};
pub mod consumption;
mod handoff;
mod pressure;
mod transport;
mod watch_admin;
pub mod watches;
pub use transport::{admit_connection, serve_connection};
pub use watch_admin::WatchController;

mod rpc;
pub use rpc::{Rpc, now};
mod agent;
pub use agent::{AgentError, AgentResult, run_cli};
mod mcp;
pub use mcp::run_mcp;
