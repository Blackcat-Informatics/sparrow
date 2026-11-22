// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The one place the worker names how a segment's files are encoded: one GTS file per
//! segment, decoded by `sparrow_archive::verify`, the same decoder the offline reader runs.

/// Decode the segment, or say why it cannot be decoded. The set must be exactly one `.gts`
/// file. The GTS reader checks the frame hash chain but cannot hash a transformed blob's
/// decompressed bytes, so the shared decoder also hashes each record's bytes against the
/// `payloadBlake3` digest its link quad names; any reader diagnostic or any mismatch fails
/// the segment.
pub(super) fn verify(files: &[(String, Vec<u8>)]) -> Result<(), String> {
    let [(name, bytes)] = files else {
        return Err(format!(
            "a segment is one .gts file, not {} files",
            files.len()
        ));
    };
    if !name.ends_with(".gts") {
        return Err(format!("{name} is not a .gts file"));
    }
    sparrow_archive::verify(bytes).map_err(|reason| format!("{name} does not decode: {reason}"))
}
