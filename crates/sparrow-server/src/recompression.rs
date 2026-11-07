// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Daemon encoding of the table-rewrite and online migration status; the agent renders
//! them for CLI and MCP.

use crate::agent::optional;
use crate::sparrow_capnp as wire;
use sparrow_core::{MigrationStatus, RecompressionPhase, RecompressionStatus};

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
    optional(out.reborrow().init_waiting(), &status.waiting);
    optional(out.init_failure(), &status.failure);
}

pub(crate) fn encode_migrations(
    mut out: capnp::struct_list::Builder<'_, wire::migration_status::Owned>,
    migrations: &[MigrationStatus],
) {
    for (index, migration) in migrations.iter().enumerate() {
        let mut target = out.reborrow().get(index as u32);
        target.set_name(migration.name);
        target.set_done(migration.done);
        target.set_total(migration.total);
        optional(target.reborrow().init_watermark(), &migration.watermark);
        optional(target.init_failure(), &migration.failure);
    }
}
