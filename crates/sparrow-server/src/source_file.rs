// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Shared descriptor-anchored discovery; no source claim or deletion authority.

use crate::consumption::Error;
use rustix::fs::{self, Mode, OFlags, ResolveFlags};
use sparrow_client::CompletedFileIdentity;
use sparrow_core::{ConsumptionDirectories, Store};
use std::{
    fs::File,
    os::{fd::AsFd, unix::fs::MetadataExt},
    path::Path,
};

pub(crate) const BENEATH: ResolveFlags = ResolveFlags::BENEATH
    .union(ResolveFlags::NO_SYMLINKS)
    .union(ResolveFlags::NO_XDEV);

pub(crate) fn open(
    dir: impl AsFd,
    path: &Path,
    flags: OFlags,
    resolve: ResolveFlags,
) -> Result<File, Error> {
    fs::openat2(dir, path, flags | OFlags::CLOEXEC | OFlags::NONBLOCK, if flags.contains(OFlags::CREATE) { Mode::from_raw_mode(0o600) } else { Mode::empty() }, resolve)
        .map(File::from).map_err(|error| {
            if error == rustix::io::Errno::NOSYS { Error::State("completed consumption requires Linux openat2; no weaker path-resolution fallback is admitted") }
            else { Error::from(error) }
        })
}

pub(crate) fn root(store: &Store, path: &Path) -> Result<File, Error> {
    let root = open(
        fs::CWD,
        path,
        OFlags::RDONLY | OFlags::DIRECTORY,
        ResolveFlags::NO_SYMLINKS,
    )?;
    let location = std::fs::canonicalize(path)?;
    let data = std::fs::canonicalize(store.data_path()?)?;
    if location.starts_with(&data) || data.starts_with(&location) {
        return Err(Error::State(
            "producer root must not overlap Sparrow's own database",
        ));
    }
    Ok(root)
}

pub(crate) fn directory(root: &File, relative: &Path) -> Result<File, Error> {
    open(
        root,
        if relative.as_os_str().is_empty() {
            Path::new(".")
        } else {
            relative
        },
        OFlags::RDONLY | OFlags::DIRECTORY,
        BENEATH,
    )
}

pub(crate) fn root_binding(root: &File, path: &Path) -> Result<(), Error> {
    let linked = open(
        fs::CWD,
        path,
        OFlags::RDONLY | OFlags::DIRECTORY,
        ResolveFlags::NO_SYMLINKS,
    )?;
    let linked = linked.metadata()?;
    let held = root.metadata()?;
    if linked.dev() != held.dev() || linked.ino() != held.ino() {
        return Err(Error::State(
            "configured consume root changed; preserve pending claims and restore its exact binding",
        ));
    }
    Ok(())
}

pub(crate) fn observe(
    root: &File,
    relative: &Path,
) -> Result<Option<CompletedFileIdentity>, Error> {
    use std::os::fd::AsRawFd;
    let parent = match parent(root, relative) {
        Ok(parent) => parent,
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let name = relative
        .file_name()
        .ok_or(Error::State("discovery requires a relative filename"))?;
    let path = std::path::PathBuf::from(format!("/proc/self/fd/{}", parent.as_raw_fd())).join(name);
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || metadata.dev() != root.metadata()?.dev() {
        return Ok(None);
    }
    Ok(Some(CompletedFileIdentity::capture(&metadata)))
}

pub(crate) fn parent(root: &File, relative: &Path) -> Result<File, Error> {
    open(
        root,
        relative
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
        OFlags::RDONLY | OFlags::DIRECTORY,
        BENEATH,
    )
}

pub(crate) fn directories(root: &File, parent: &File) -> Result<ConsumptionDirectories, Error> {
    let root = CompletedFileIdentity::capture(&root.metadata()?);
    let parent = CompletedFileIdentity::capture(&parent.metadata()?);
    Ok(ConsumptionDirectories {
        root_device: root.device,
        root_inode: root.inode,
        parent_device: parent.device,
        parent_inode: parent.inode,
    })
}
