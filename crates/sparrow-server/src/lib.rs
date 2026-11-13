// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Typed Cap'n Proto contract and daemon RPC implementation.

pub use sparrow_client::{Connection, connect, sparrow_capnp};
pub mod admission;
mod backup;
mod config;
mod correlation_wire;
mod diagnostics;
pub mod floor_pause;
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
mod ingest_gate;
pub mod ingest_socket;
mod pressure;
pub mod pressure_wake;
mod query_only;
mod recompression;
mod tcp_gate;
mod transport;
mod watch_admin;
pub mod watches;
pub use transport::{Access, admit_connection, local_caller, serve_connection};
pub use watch_admin::WatchController;

mod rdf_wire;
mod rpc;
pub mod serving;
pub use rpc::{Rpc, now};
mod agent;
pub use agent::{AgentError, AgentResult, run_cli};
mod mcp;
pub use mcp::run_mcp;
