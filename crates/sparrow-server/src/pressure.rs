// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One pressure status wire adapter shared by daemon, CLI and stdio MCP.

use crate::sparrow_capnp as wire;
use purrdf_lex::json::{Object, Value};
use sparrow_core::{EvictionFrontier, PressurePhase, PressureStatus};

pub(crate) fn encode(
    mut out: wire::pressure_status::Builder<'_>,
    status: &PressureStatus,
) -> capnp::Result<()> {
    out.set_phase(match status.phase {
        PressurePhase::Ready => wire::PressurePhase::Ready,
        PressurePhase::Requested => wire::PressurePhase::Requested,
        PressurePhase::AwaitingReclamation => wire::PressurePhase::AwaitingReclamation,
        PressurePhase::Blocked => wire::PressurePhase::Blocked,
    });
    out.set_requested_bytes(status.requested_bytes);
    out.set_passes(status.passes);
    out.set_evicted_records(status.evicted_records);
    out.set_payload_refs_retired(status.payload_refs_retired);
    out.set_pending_flushes(status.pending_flushes);
    out.set_pending_compactions(status.pending_compactions);
    out.set_snapshots(status.snapshots);
    if let Some(report) = &status.last_pass {
        let mut pass = out.reborrow().init_last_pass().init_present();
        pass.set_records(report.records.len() as u32);
        pass.set_artifacts(report.artifacts.len() as u32);
        pass.set_visited(report.visited as u32);
        pass.set_protected(report.protected as u32);
        pass.set_payload_refs(report.payload_refs as u32);
        pass.set_pending_manifest_refs(report.pending_manifest_refs as u64);
        pass.set_frontier(match report.frontier {
            EvictionFrontier::Exhausted => wire::EvictionFrontier::Exhausted,
            EvictionFrontier::RecordLimit => wire::EvictionFrontier::RecordLimit,
            EvictionFrontier::ScanBudget => wire::EvictionFrontier::ScanBudget,
            EvictionFrontier::PayloadBudget => wire::EvictionFrontier::PayloadBudget,
        });
    } else {
        out.reborrow().init_last_pass().set_absent(());
    }
    sparrow_client::encode_optional(
        out.init_failure(),
        status.failure.as_deref(),
        |mut out, value| out.set_value(value),
        |mut out| out.set_absent(()),
    );
    Ok(())
}

pub(crate) fn decode(value: wire::pressure_status::Reader<'_>) -> capnp::Result<Value> {
    let phase = match value.get_phase()? {
        wire::PressurePhase::Ready => "ready",
        wire::PressurePhase::Requested => "requested",
        wire::PressurePhase::AwaitingReclamation => "awaiting_reclamation",
        wire::PressurePhase::Blocked => "blocked",
    };
    let pass = match value.get_last_pass()?.which()? {
        wire::pressure_pass::Absent(()) => Value::Null,
        wire::pressure_pass::Present(pass) => {
            let pass = pass?;
            let frontier = match pass.get_frontier()? {
                wire::EvictionFrontier::Exhausted => "exhausted",
                wire::EvictionFrontier::RecordLimit => "record_limit",
                wire::EvictionFrontier::ScanBudget => "scan_budget",
                wire::EvictionFrontier::PayloadBudget => "payload_budget",
            };
            Object::new()
                .with("records", u64::from(pass.get_records()))
                .with("artifacts", u64::from(pass.get_artifacts()))
                .with("visited", u64::from(pass.get_visited()))
                .with("protected", u64::from(pass.get_protected()))
                .with("payload_refs", u64::from(pass.get_payload_refs()))
                .with(
                    "pending_manifest_refs",
                    pass.get_pending_manifest_refs().to_string(),
                )
                .with("frontier", frontier)
                .into()
        }
    };
    let failure = match value.get_failure()?.which()? {
        wire::optional_text::Absent(()) => Value::Null,
        wire::optional_text::Value(value) => Value::from(value?.to_str()?),
    };
    Ok(Object::new()
        .with("phase", phase)
        .with("requested_bytes", value.get_requested_bytes().to_string())
        .with("passes", value.get_passes().to_string())
        .with("evicted_records", value.get_evicted_records().to_string())
        .with(
            "payload_refs_retired",
            value.get_payload_refs_retired().to_string(),
        )
        .with("pending_flushes", value.get_pending_flushes().to_string())
        .with(
            "pending_compactions",
            value.get_pending_compactions().to_string(),
        )
        .with("snapshots", value.get_snapshots().to_string())
        .with("last_pass", pass)
        .with("failure", failure)
        .into())
}
