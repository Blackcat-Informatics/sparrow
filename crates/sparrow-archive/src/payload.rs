// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use std::path::PathBuf;

use crate::error::ArchiveError;
use crate::segment::Segment;

/// One artifact whose payload identity matches, and the segment file whose metadata names it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadMatch {
    /// Original artifact identity, `artifact:<hex>`.
    pub artifact_id: String,
    /// The segment file that holds the artifact's metadata.
    pub path: PathBuf,
}

/// Check that `value` has the payload identity form the archive writes: `blake3:` followed
/// by exactly 64 lower-case hex digits.
///
/// # Errors
/// [`ArchiveError::InvalidPayloadId`] when `value` has any other form.
pub fn validate_payload_id(value: &str) -> Result<(), ArchiveError> {
    let canonical = value.strip_prefix("blake3:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    });
    if canonical {
        Ok(())
    } else {
        Err(ArchiveError::InvalidPayloadId {
            value: value.to_owned(),
        })
    }
}

/// Every artifact among the segments at `paths` whose payload identity is `payload_id`, one
/// match per segment that names it, in the order of `paths`. An empty result means no
/// segment holds that payload. Nothing is written anywhere.
///
/// Every segment is decoded in memory, as [`crate::extract`] does, so a damaged segment fails
/// the call by name even when it does not hold the payload. No index exists, so each call
/// reads the whole archive directory.
///
/// # Errors
/// A `payload_id` that is not `blake3:` followed by 64 lower-case hex digits, or a segment
/// that cannot be read or decoded.
pub fn find_payload(
    paths: &[PathBuf],
    payload_id: &str,
) -> Result<Vec<PayloadMatch>, ArchiveError> {
    validate_payload_id(payload_id)?;
    let mut matches = Vec::new();
    for path in paths {
        let segment = Segment::open(path)?;
        for artifact in segment
            .artifacts()
            .iter()
            .filter(|artifact| artifact.payload_id == payload_id)
        {
            matches.push(PayloadMatch {
                artifact_id: artifact.id.clone(),
                path: path.clone(),
            });
        }
    }
    Ok(matches)
}
