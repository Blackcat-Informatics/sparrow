// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use std::collections::BTreeMap;

use sparrow_core::Store;

use crate::error::ArchiveError;
use crate::segment::{ArchivedArtifact, ArchivedRecord, Segment};

/// What one [`restore`] call did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoreReport {
    /// Artifacts the segments held in full.
    pub artifacts: usize,
    /// Records those artifacts hold.
    pub records: usize,
}

/// Restore every artifact the given segments hold in full into `store`, under its ORIGINAL
/// artifact and record identities, receipt time and source (the restore target is a fresh
/// store, so the original identities cannot collide). An artifact's records may be spread
/// over several segments; an artifact whose records are not all present, or whose bytes do
/// not add up to its recorded length, is refused by name and nothing of it is written.
///
/// Repeating a restore adds nothing: the request identity is derived from the artifact
/// identity, so the store returns the first restore's receipt.
///
/// # Errors
/// The first segment, artifact or store refusal; earlier artifacts stay restored.
pub fn restore(store: &Store, segments: &[Segment]) -> Result<RestoreReport, ArchiveError> {
    let mut artifacts: BTreeMap<String, ArchivedArtifact> = BTreeMap::new();
    let mut records: BTreeMap<String, Vec<ArchivedRecord>> = BTreeMap::new();
    for segment in segments {
        for artifact in segment.artifacts() {
            artifacts.insert(artifact.id.clone(), artifact.clone());
        }
        for record in segment.records() {
            records
                .entry(record.artifact_id.clone())
                .or_default()
                .push(record.clone());
        }
    }
    let mut report = RestoreReport {
        artifacts: 0,
        records: 0,
    };
    for (id, artifact) in &artifacts {
        let mut held = records.remove(id).unwrap_or_default();
        held.sort_by_key(|record| record.byte_start);
        held.dedup_by(|a, b| a.id == b.id);
        let mut bytes = Vec::new();
        for record in &held {
            if record.byte_start != bytes.len() as u64
                || record.byte_end != record.byte_start + record.bytes.len() as u64
            {
                return Err(ArchiveError::Incomplete {
                    reason: format!(
                        "artifact {id}: record {} does not continue the bytes at offset {}",
                        record.id,
                        bytes.len()
                    ),
                });
            }
            bytes.extend_from_slice(&record.bytes);
        }
        if bytes.len() as u64 != artifact.byte_length {
            return Err(ArchiveError::Incomplete {
                reason: format!(
                    "artifact {id}: the segments hold {} of its {} bytes",
                    bytes.len(),
                    artifact.byte_length
                ),
            });
        }
        let record_ids: Vec<String> = held.iter().map(|record| record.id.clone()).collect();
        store
            .restore_artifact(
                &format!("restore:{id}"),
                &artifact.source,
                &artifact.format,
                &bytes,
                id,
                &record_ids,
                artifact.received_at,
            )
            .map_err(ArchiveError::Store)?;
        report.artifacts += 1;
        report.records += record_ids.len();
    }
    if let Some(orphan) = records.keys().next() {
        return Err(ArchiveError::Incomplete {
            reason: format!("the segments hold records of {orphan} but not its artifact metadata"),
        });
    }
    Ok(report)
}
