// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Typed Cap'n Proto contract and daemon RPC implementation.

pub use sparrow_client::{Connection, connect, sparrow_capnp};
pub mod access_wire;
pub mod admission;
pub mod archive;
mod config;
mod correlation_wire;
mod diagnostics;
pub mod floor_pause;
pub mod journal_log;
pub mod local_endpoint;
mod source_binding;
mod source_file;
pub use source_binding::SourceBindings;
pub mod staging_refusal;
pub mod start_wait;
pub mod syslog_listener;
mod syslog_spill;
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
mod udp_kernel;
mod watch_admin;
pub mod watches;
pub use transport::{Access, admit_connection, serve_connection};
pub use watch_admin::WatchController;

mod rdf_wire;
mod rpc;
pub mod serving;
pub use rpc::{Rpc, now};
