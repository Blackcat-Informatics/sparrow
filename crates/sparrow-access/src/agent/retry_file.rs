// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One exclusive, synchronized producer retry file outside enabled intake roots.

use super::{Result, invalid, wire};

pub(super) fn parent(
    output: &str,
    list: wire::watch_list::Reader<'_>,
) -> Result<std::path::PathBuf> {
    let parent = std::path::Path::new(output)
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let destination = std::fs::canonicalize(parent).map_err(sparrow_client::Error::Io)?;
    for watch in list.get_watches()? {
        if watch.get_enabled() {
            let root = std::fs::canonicalize(watch.get_definition()?.get_root()?.to_str()?)
                .map_err(sparrow_client::Error::Io)?;
            if destination.starts_with(root) {
                return Err(invalid(
                    "save producer retry intents outside enabled intake roots so the intent cannot itself be ingested",
                ));
            }
        }
    }
    Ok(parent.to_path_buf())
}

pub(super) fn save(output: &str, parent: &std::path::Path, bytes: &[u8]) -> Result<()> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(output)
        .map_err(sparrow_client::Error::Io)?;
    file.write_all(bytes).map_err(sparrow_client::Error::Io)?;
    file.sync_all().map_err(sparrow_client::Error::Io)?;
    std::fs::File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(sparrow_client::Error::Io)?;
    Ok(())
}
