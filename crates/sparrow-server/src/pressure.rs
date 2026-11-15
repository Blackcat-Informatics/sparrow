// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

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
        PressurePhase::AwaitingArchive => wire::PressurePhase::AwaitingArchive,
    });
    out.set_requested_bytes(status.requested_bytes);
    out.set_passes(status.passes);
    out.set_evicted_records(status.evicted_records);
    out.set_evicted_recordless_artifacts(status.evicted_recordless_artifacts);
    out.set_payload_refs_retired(status.payload_refs_retired);
    out.set_pending_flushes(status.pending_flushes);
    out.set_pending_compactions(status.pending_compactions);
    out.set_snapshots(status.snapshots);
    out.set_observed_at(status.observed_at);
    out.set_linked_bytes(status.linked_bytes);
    out.set_episode_since(status.episode_since);
    out.set_phase_since(status.phase_since);
    out.set_wait_since(status.wait_since);
    out.set_wait_before_bytes(status.wait_before_bytes);
    out.set_forced_compactions(status.forced_compactions);
    out.set_waits_released(status.waits_released);
    out.set_stalled_passes(status.stalled_passes);
    out.set_refused_writes(status.refused_writes);
    out.set_floor_paused_since(status.floor.paused_since);
    out.set_floor_pauses(status.floor.pauses);
    out.set_floor_resumes(status.floor.resumes);
    out.set_floor_refused_writes(status.floor.refused_writes);
    out.set_forced_compaction_ms(status.forced_compaction_ms);
    out.set_forced_failures(status.forced_failures);
    out.set_native_errors(status.native_errors);
    out.set_floor_requests(status.floor_requests);
    out.set_floor_requested_bytes(status.floor_requested_bytes);
    out.set_floor_passes(status.floor_passes);
    out.set_floor_freed_bytes(status.floor_freed_bytes);
    out.set_disk_budget(status.disk_budget);
    out.set_maintenance_reserve(status.maintenance_reserve);
    out.set_archive_configured(status.archive_configured);
    out.set_archive_held(status.archive_held);
    out.set_reserve_headroom(status.reserve_headroom());
    let mut families = out
        .reborrow()
        .init_forced_compaction_families(status.forced_compaction_families.len() as u32);
    for (index, size) in status.forced_compaction_families.iter().enumerate() {
        let mut family = families.reborrow().get(index as u32);
        family.set_family(&size.family);
        family.set_before_bytes(size.before_bytes);
        family.set_after_bytes(size.after_bytes);
    }
    sparrow_client::encode_optional(
        out.reborrow().init_waiting_on(),
        status.waiting_on.as_deref(),
        |mut out, value| out.set_value(value),
        |mut out| out.set_absent(()),
    );
    if let Some(report) = &status.last_pass {
        let mut pass = out.reborrow().init_last_pass().init_present();
        pass.set_records(report.records.len() as u32);
        pass.set_artifacts(report.artifacts.len() as u32);
        pass.set_recordless_artifacts(report.recordless_artifacts as u32);
        pass.set_visited(report.visited as u32);
        pass.set_protected(report.protected as u32);
        pass.set_payload_refs(report.payload_refs as u32);
        pass.set_pending_manifest_refs(report.pending_manifest_refs as u64);
        pass.set_continuation(report.continuation);
        pass.set_frontier(match report.frontier {
            EvictionFrontier::Exhausted => wire::EvictionFrontier::Exhausted,
            EvictionFrontier::UnitLimit => wire::EvictionFrontier::UnitLimit,
            EvictionFrontier::ScanBudget => wire::EvictionFrontier::ScanBudget,
            EvictionFrontier::PayloadBudget => wire::EvictionFrontier::PayloadBudget,
            EvictionFrontier::CatalogBudget => wire::EvictionFrontier::CatalogBudget,
            EvictionFrontier::ArchiveLag => wire::EvictionFrontier::ArchiveLag,
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
        wire::PressurePhase::AwaitingArchive => "awaiting_archive",
    };
    let pass = match value.get_last_pass()?.which()? {
        wire::pressure_pass::Absent(()) => Value::Null,
        wire::pressure_pass::Present(pass) => {
            let pass = pass?;
            let frontier = match pass.get_frontier()? {
                wire::EvictionFrontier::Exhausted => "exhausted",
                wire::EvictionFrontier::UnitLimit => "unit_limit",
                wire::EvictionFrontier::ScanBudget => "scan_budget",
                wire::EvictionFrontier::PayloadBudget => "payload_budget",
                wire::EvictionFrontier::CatalogBudget => "catalog_budget",
                wire::EvictionFrontier::ArchiveLag => "archive_lag",
            };
            Object::new()
                .with("records", u64::from(pass.get_records()))
                .with("artifacts", u64::from(pass.get_artifacts()))
                .with(
                    "recordless_artifacts",
                    u64::from(pass.get_recordless_artifacts()),
                )
                .with("visited", u64::from(pass.get_visited()))
                .with("protected", u64::from(pass.get_protected()))
                .with("payload_refs", u64::from(pass.get_payload_refs()))
                .with(
                    "pending_manifest_refs",
                    pass.get_pending_manifest_refs().to_string(),
                )
                .with("frontier", frontier)
                .with("continuation", pass.get_continuation())
                .into()
        }
    };
    let failure = match value.get_failure()?.which()? {
        wire::optional_text::Absent(()) => Value::Null,
        wire::optional_text::Value(value) => Value::from(value?.to_str()?),
    };
    let waiting_on = match value.get_waiting_on()?.which()? {
        wire::optional_text::Absent(()) => Value::Null,
        wire::optional_text::Value(value) => Value::from(value?.to_str()?),
    };
    let mut families = Vec::new();
    for family in value.get_forced_compaction_families()? {
        families.push(Value::from(
            Object::new()
                .with("family", family.get_family()?.to_str()?)
                .with("before_bytes", family.get_before_bytes().to_string())
                .with("after_bytes", family.get_after_bytes().to_string()),
        ));
    }
    // Ages are measured to the last pass's own clock, so they need no clock here.
    let since = |started: u64| {
        if started == 0 {
            0
        } else {
            value.get_observed_at().saturating_sub(started)
        }
    };
    Ok(Object::new()
        .with("phase", phase)
        .with("requested_bytes", value.get_requested_bytes().to_string())
        .with("passes", value.get_passes().to_string())
        .with("evicted_records", value.get_evicted_records().to_string())
        .with(
            "evicted_recordless_artifacts",
            value.get_evicted_recordless_artifacts().to_string(),
        )
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
        .with("linked_bytes", value.get_linked_bytes().to_string())
        .with("disk_budget", value.get_disk_budget().to_string())
        .with(
            "maintenance_reserve",
            value.get_maintenance_reserve().to_string(),
        )
        .with("reserve_headroom", value.get_reserve_headroom().to_string())
        .with("archive_configured", value.get_archive_configured())
        .with("archive_held", value.get_archive_held())
        .with("observed_at", value.get_observed_at().to_string())
        .with("episode_since", value.get_episode_since().to_string())
        .with(
            "episode_age_ms",
            since(value.get_episode_since()).to_string(),
        )
        .with("phase_since", value.get_phase_since().to_string())
        .with("phase_age_ms", since(value.get_phase_since()).to_string())
        .with("wait_since", value.get_wait_since().to_string())
        .with("wait_age_ms", since(value.get_wait_since()).to_string())
        .with(
            "wait_before_bytes",
            value.get_wait_before_bytes().to_string(),
        )
        .with(
            "forced_compactions",
            value.get_forced_compactions().to_string(),
        )
        .with("waits_released", value.get_waits_released().to_string())
        .with("stalled_passes", value.get_stalled_passes().to_string())
        .with("refused_writes", value.get_refused_writes().to_string())
        .with("waiting_on", waiting_on)
        .with(
            "forced_compaction_ms",
            value.get_forced_compaction_ms().to_string(),
        )
        .with("forced_failures", value.get_forced_failures().to_string())
        .with("native_errors", value.get_native_errors().to_string())
        .with("forced_compaction_families", families)
        .with(
            "free_space_floor",
            Object::new()
                .with("paused_since", value.get_floor_paused_since().to_string())
                .with("pauses", value.get_floor_pauses().to_string())
                .with("resumes", value.get_floor_resumes().to_string())
                .with(
                    "refused_writes",
                    value.get_floor_refused_writes().to_string(),
                )
                .with("reclaim_requests", value.get_floor_requests().to_string())
                .with(
                    "requested_bytes",
                    value.get_floor_requested_bytes().to_string(),
                )
                .with("passes", value.get_floor_passes().to_string())
                .with("freed_bytes", value.get_floor_freed_bytes().to_string()),
        )
        .with("last_pass", pass)
        .with("failure", failure)
        .into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_catalog_frontier_preserves_continuation_through_agent_rendering() {
        let status = PressureStatus {
            phase: PressurePhase::Requested,
            requested_bytes: 1024,
            last_pass: Some(sparrow_core::EvictionReport {
                visited: 1,
                protected: 1,
                recordless_artifacts: 1,
                frontier: EvictionFrontier::CatalogBudget,
                continuation: true,
                ..sparrow_core::EvictionReport::default()
            }),
            ..PressureStatus::default()
        };
        let mut message = capnp::message::Builder::new_default();
        encode(message.init_root(), &status).unwrap();
        let value = decode(message.get_root_as_reader().unwrap()).unwrap();
        assert_eq!(value["phase"].as_str(), Some("requested"));
        assert_eq!(
            value["last_pass"]["frontier"].as_str(),
            Some("catalog_budget")
        );
        assert_eq!(value["last_pass"]["continuation"].as_bool(), Some(true));
        assert_eq!(value["requested_bytes"].as_str(), Some("1024"));
        assert_eq!(
            value["last_pass"]["recordless_artifacts"]
                .as_number()
                .unwrap()
                .as_u64(),
            Some(1)
        );
        // Waiting for the archive is distinct from a blocked pass on every surface.
        let waiting = PressureStatus {
            phase: PressurePhase::AwaitingArchive,
            last_pass: Some(sparrow_core::EvictionReport {
                frontier: EvictionFrontier::ArchiveLag,
                continuation: true,
                ..sparrow_core::EvictionReport::default()
            }),
            ..PressureStatus::default()
        };
        let mut message = capnp::message::Builder::new_default();
        encode(message.init_root(), &waiting).unwrap();
        let value = decode(message.get_root_as_reader().unwrap()).unwrap();
        assert_eq!(value["phase"].as_str(), Some("awaiting_archive"));
        assert_eq!(value["last_pass"]["frontier"].as_str(), Some("archive_lag"));
    }

    // Value: #211 adds wait age, byte, refusal and floor measures to the status wire; an
    // operator reads them through CLI and MCP. Protects: every new measure survives the
    // wire, ages are measured to the pass's own clock (no clock in the decoder), and a
    // status with none of them decodes as zeros, as an older server's does.
    #[test]
    fn wait_and_floor_measures_survive_the_wire_with_ages_to_the_passes_clock() {
        let status = PressureStatus {
            phase: PressurePhase::AwaitingReclamation,
            observed_at: 100_000,
            linked_bytes: 700,
            episode_since: 40_000,
            phase_since: 90_000,
            wait_since: 70_000,
            wait_before_bytes: 650,
            forced_compactions: 2,
            forced_compaction_ms: 4_200,
            forced_compaction_families: vec![sparrow_core::FamilySize {
                family: "payload".into(),
                before_bytes: 900,
                after_bytes: 400,
            }],
            forced_failures: 1,
            native_errors: 3,
            waits_released: 1,
            stalled_passes: 9,
            refused_writes: 31,
            waiting_on: Some("a forced compaction is running".into()),
            floor_requests: 4,
            floor_requested_bytes: 2_048,
            floor_passes: 6,
            floor_freed_bytes: 1_536,
            disk_budget: 1_000,
            maintenance_reserve: 100,
            archive_held: true,
            floor: sparrow_core::FloorStatus {
                paused_since: 99_000,
                pauses: 3,
                resumes: 2,
                refused_writes: 5,
            },
            ..PressureStatus::default()
        };
        let mut message = capnp::message::Builder::new_default();
        encode(message.init_root(), &status).unwrap();
        let value = decode(message.get_root_as_reader().unwrap()).unwrap();
        for (name, expected) in [
            ("linked_bytes", "700"),
            ("disk_budget", "1000"),
            ("maintenance_reserve", "100"),
            ("reserve_headroom", "200"),
            ("wait_before_bytes", "650"),
            ("wait_age_ms", "30000"),
            ("episode_age_ms", "60000"),
            ("phase_age_ms", "10000"),
            ("forced_compactions", "2"),
            ("waits_released", "1"),
            ("stalled_passes", "9"),
            ("refused_writes", "31"),
            ("forced_compaction_ms", "4200"),
            ("forced_failures", "1"),
            ("native_errors", "3"),
        ] {
            assert_eq!(value[name].as_str(), Some(expected), "{name}");
        }
        assert_eq!(
            value["waiting_on"].as_str(),
            Some("a forced compaction is running")
        );
        assert_eq!(value["archive_held"].as_bool(), Some(true));
        assert_eq!(value["archive_configured"].as_bool(), Some(false));
        let family = &value["forced_compaction_families"][0];
        assert_eq!(family["family"].as_str(), Some("payload"));
        assert_eq!(family["before_bytes"].as_str(), Some("900"));
        assert_eq!(family["after_bytes"].as_str(), Some("400"));
        let floor = &value["free_space_floor"];
        assert_eq!(floor["paused_since"].as_str(), Some("99000"));
        assert_eq!(floor["pauses"].as_str(), Some("3"));
        assert_eq!(floor["resumes"].as_str(), Some("2"));
        assert_eq!(floor["refused_writes"].as_str(), Some("5"));
        // #267: what the floor asked of reclaim, next to the floor's own counters.
        assert_eq!(floor["reclaim_requests"].as_str(), Some("4"));
        assert_eq!(floor["requested_bytes"].as_str(), Some("2048"));
        assert_eq!(floor["passes"].as_str(), Some("6"));
        assert_eq!(floor["freed_bytes"].as_str(), Some("1536"));
        let mut message = capnp::message::Builder::new_default();
        encode(message.init_root(), &PressureStatus::default()).unwrap();
        let quiet = decode(message.get_root_as_reader().unwrap()).unwrap();
        assert_eq!(quiet["wait_age_ms"].as_str(), Some("0"));
        assert!(quiet["waiting_on"].is_null());
    }
}
