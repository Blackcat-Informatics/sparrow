// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The one place that knows how a segment's files are encoded: one GTS file per segment,
//! read back with PurRDF's GTS reader at the pinned revision.

/// Read the segment back as GTS, or say why it cannot be. The reader checks the frame hash
/// chain and every blob digest and reports each break as a diagnostic; any diagnostic fails
/// the segment. The set must be exactly one `.gts` file.
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
    let graph = purrdf::gts::reader::read(bytes, false, None);
    if let Some(first) = graph.diagnostics.first() {
        return Err(format!(
            "{name} does not read back as GTS: {} diagnostics, first {} {}",
            graph.diagnostics.len(),
            first.code,
            first.detail
        ));
    }
    Ok(())
}
