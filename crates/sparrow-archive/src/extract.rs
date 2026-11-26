// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::error::ArchiveError;
use crate::segment::{ArchivedArtifact, ArchivedRecord, Segment};

/// One artifact's whole bytes, taken from the segments at `paths` (every segment under the
/// archive directory). Its records are joined in byte order, wherever they were archived,
/// and the result is returned only when its length equals the artifact's recorded
/// `byte_length`. The decoder has already hashed each record's bytes and compared them
/// with the digest the archive's link quad names for it. Nothing is
/// written anywhere.
///
/// Every segment is decoded, so a damaged segment fails the call by name even when it does
/// not hold the artifact: it might have held a piece of it. A record that repeats in several
/// segments counts once when every copy is identical; copies that differ, artifact metadata
/// that differs between segments, and a record whose source or receipt time differs from its
/// artifact's each fail the call.
///
/// # Errors
/// A segment that cannot be read or decoded, an `artifact_id` no segment holds, or an
/// artifact whose records are not all present or whose bytes do not add up to its recorded
/// length; the last two name the artifact, and never return partial bytes.
pub fn extract(paths: &[PathBuf], artifact_id: &str) -> Result<Vec<u8>, ArchiveError> {
    let incomplete = |reason: String| ArchiveError::Incomplete {
        reason: format!("artifact {artifact_id}: {reason}"),
    };
    let mut artifact: Option<ArchivedArtifact> = None;
    let mut held: BTreeMap<String, ArchivedRecord> = BTreeMap::new();
    for path in paths {
        let segment = Segment::open(path)?;
        for found in segment.artifacts().iter().filter(|a| a.id == artifact_id) {
            match &artifact {
                Some(first) if first != found => {
                    return Err(incomplete(format!(
                        "{} holds metadata that conflicts with another segment's",
                        path.display()
                    )));
                }
                _ => artifact = Some(found.clone()),
            }
        }
        for found in segment
            .records()
            .iter()
            .filter(|record| record.artifact_id == artifact_id)
        {
            match held.get(&found.id) {
                Some(first) if first != found => {
                    return Err(incomplete(format!(
                        "record {} appears in more than one segment with different contents",
                        found.id
                    )));
                }
                Some(_) => {}
                None => {
                    held.insert(found.id.clone(), found.clone());
                }
            }
        }
    }
    let mut records: Vec<ArchivedRecord> = held.into_values().collect();
    let Some(artifact) = artifact else {
        return Err(if records.is_empty() {
            ArchiveError::NotFound {
                artifact_id: artifact_id.to_owned(),
            }
        } else {
            incomplete("the segments hold its records but not its metadata".to_owned())
        });
    };
    if let Some(record) = records.iter().find(|record| {
        record.source != artifact.source || record.received_at != artifact.received_at
    }) {
        return Err(incomplete(format!(
            "record {} disagrees with the artifact on its source or receipt time",
            record.id
        )));
    }
    records.sort_by_key(|record| record.byte_start);
    let mut bytes = Vec::new();
    for record in &records {
        if record.byte_start != bytes.len() as u64
            || record.byte_end != record.byte_start + record.bytes.len() as u64
        {
            return Err(incomplete(format!(
                "record {} does not continue the bytes at offset {}; a segment is missing",
                record.id,
                bytes.len()
            )));
        }
        bytes.extend_from_slice(&record.bytes);
    }
    if bytes.len() as u64 != artifact.byte_length {
        return Err(incomplete(format!(
            "the segments hold {} of its {} bytes",
            bytes.len(),
            artifact.byte_length
        )));
    }
    Ok(bytes)
}
