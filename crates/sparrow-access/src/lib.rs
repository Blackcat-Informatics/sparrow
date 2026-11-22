// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! CLI and stdio MCP access to Sparrow's typed daemon operations.

use sparrow_client::sparrow_capnp;
use sparrow_server::{ConsumeWatch, ProducerContract, access_wire as codec};

mod agent;
mod mcp;
mod tail_wire;
mod text_wire;

pub use agent::{AgentError, AgentResult, run_cli};
pub use mcp::run_mcp;
