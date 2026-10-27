// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Typed Cap'n Proto contract and daemon RPC implementation.

pub use sparrow_client::{Connection, connect, sparrow_capnp};
mod config;
mod correlation_wire;
mod diagnostics;
mod source_file;
pub mod syslog_listener;
mod syslog_wire;
mod tail_wire;
mod tailing;
mod text_wire;
mod time_wire;
pub use config::{ConsumeWatch, ProducerContract, ServerConfig};
pub mod consumption;
mod handoff;
mod pressure;
mod tcp_gate;
mod transport;
mod watch_admin;
pub mod watches;
pub use transport::{Access, admit_connection, serve_connection};
pub use watch_admin::WatchController;

mod rdf_wire;
mod rpc;
pub use rpc::{Rpc, now};
mod agent;
pub use agent::{AgentError, AgentResult, run_cli};
mod mcp;
pub use mcp::run_mcp;
