// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Daemon encoding of the table-rewrite status; the agent renders it for CLI and MCP.

use crate::sparrow_capnp as wire;
use sparrow_core::{RecompressionPhase, RecompressionStatus};

pub(crate) fn encode(
    mut out: wire::recompression_status::Builder<'_>,
    status: &RecompressionStatus,
) {
    out.set_profile(status.profile);
    out.set_phase(match status.phase {
        RecompressionPhase::Running => wire::RecompressionPhase::Running,
        RecompressionPhase::Paused => wire::RecompressionPhase::Paused,
        RecompressionPhase::Complete => wire::RecompressionPhase::Complete,
    });
    out.set_remaining_bytes(status.remaining_bytes);
    let text = |field: wire::optional_text::Builder<'_>, value: Option<&str>| {
        sparrow_client::encode_optional(
            field,
            value,
            |mut field, value| field.set_value(value),
            |mut field| field.set_absent(()),
        );
    };
    text(out.reborrow().init_waiting(), status.waiting.as_deref());
    text(out.init_failure(), status.failure.as_deref());
}
