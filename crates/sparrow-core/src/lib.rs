// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Persistent evidence ownership and typed Sparrow operations.

mod instant;
mod projection;
mod query;
mod storage;
mod text;

pub use instant::{format_instant, parse_instant};
pub use query::{JsonQuery, QueryScope};
pub use storage::context::{Context, ContextFrontier, ContextSide};
pub use storage::{Artifact, Config, Error, Pin, Record, Store};
pub use text::resolve_text_analyzer;
