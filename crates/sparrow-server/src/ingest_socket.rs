// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The group-accessible ingest-only Unix socket: its startup directory and socket checks,
//! the caller credentials read on accept, and its own connection ceiling and counters.

use crate::admission::{Ceiling, Slot};
use std::{
    io,
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

/// The directory must be setgid, so the socket inherits its group, and closed to others.
const DIRECTORY_MODE: u32 = 0o2750;
/// Owner and group may connect; nobody else may.
pub const SOCKET_MODE: u32 = 0o660;

/// One configured ingest socket: its identity, its own connection ceiling and its refusals.
/// Clones share the ceiling and counters, so `status` reports what the accept loop admits.
#[derive(Clone)]
pub struct IngestSocket {
    inner: Arc<Inner>,
}

struct Inner {
    path: PathBuf,
    gid: u32,
    ceiling: Arc<Ceiling>,
    refused: AtomicU64,
}

impl IngestSocket {
    /// Track a socket at `path` for members of `gid`, admitting at most `max_connections`.
    #[must_use]
    pub fn new(path: PathBuf, gid: u32, max_connections: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                path,
                gid,
                ceiling: Ceiling::new("ingest socket connection ceiling", max_connections),
                refused: AtomicU64::new(0),
            }),
        }
    }

    /// The connection ceiling: take a slot before accepting, so a peer over the ceiling waits
    /// in the kernel backlog and is served when a slot frees, instead of being reset.
    #[must_use]
    pub fn ceiling(&self) -> &Arc<Ceiling> {
        &self.inner.ceiling
    }

    /// Admit one accepted stream with its kernel-reported caller under the slot taken for it.
    /// A stream whose credentials cannot be read is dropped unanswered and counted. Run within
    /// a Tokio `LocalSet`.
    pub fn admit(&self, stream: tokio::net::UnixStream, rpc: crate::Rpc, slot: Slot) -> bool {
        let credential = match stream.peer_cred().and_then(evidence_peer) {
            Ok(credential) => credential,
            Err(error) => {
                self.inner.refused.fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "sparrowd: refused an ingest-socket connection without peer credentials: {error}"
                );
                return false;
            }
        };
        crate::admit_connection(stream, rpc, slot, crate::Access::IngestSocket(credential));
        true
    }

    pub(crate) fn encode(&self, mut out: crate::sparrow_capnp::ingest_socket_status::Builder<'_>) {
        let inner = &self.inner;
        out.set_path(inner.path.to_string_lossy().as_ref());
        out.set_gid(inner.gid);
        let ceiling = inner.ceiling.snapshot();
        out.set_max_connections(ceiling.max);
        out.set_active_connections(ceiling.active);
        out.set_lease_expiries(ceiling.lease_expiries);
        crate::admission::encode_wait(out.reborrow().init_ceiling_wait(), ceiling.waits);
        out.set_refused_connections(inner.refused.load(Ordering::Relaxed));
    }
}

/// The caller's uid, gid and pid; a connection without all three is never admitted.
fn evidence_peer(credential: tokio::net::unix::UCred) -> io::Result<sparrow_core::PeerCredential> {
    let pid = credential
        .pid()
        .and_then(|pid| u32::try_from(pid).ok())
        .filter(|pid| *pid != 0)
        .ok_or_else(|| io::Error::other("the kernel reported no process ID for the peer"))?;
    Ok(sparrow_core::PeerCredential {
        uid: credential.uid(),
        gid: credential.gid(),
        pid,
    })
}

fn refuse(path: &Path, problem: &str, remedy: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
            "ingest socket directory {}: {problem}; {remedy}",
            path.display()
        ),
    )
}

/// Refuse to serve the ingest socket from `dir` unless it is a real directory, owned by
/// `owner` (the daemon's uid), with group `gid` and mode exactly 2750.
pub fn check_directory(dir: &Path, owner: u32, gid: u32) -> io::Result<()> {
    let metadata = std::fs::symlink_metadata(dir).map_err(|error| {
        refuse(
            dir,
            &error.to_string(),
            "create it as packaging/tmpfiles/sparrow-ingest.conf does",
        )
    })?;
    if metadata.file_type().is_symlink() {
        return Err(refuse(
            dir,
            "is a symbolic link",
            "point ingest_socket_path into the real directory",
        ));
    }
    if !metadata.is_dir() {
        return Err(refuse(dir, "is not a directory", "create the directory"));
    }
    if metadata.uid() != owner {
        return Err(refuse(
            dir,
            &format!(
                "is owned by uid {}, not the daemon's uid {owner}",
                metadata.uid()
            ),
            "chown it to the daemon's account",
        ));
    }
    if metadata.gid() != gid {
        return Err(refuse(
            dir,
            &format!("has group {}, not ingest_socket_gid {gid}", metadata.gid()),
            "chgrp it to the ingest group",
        ));
    }
    let mode = metadata.mode() & 0o7777;
    if mode != DIRECTORY_MODE {
        return Err(refuse(
            dir,
            &format!("has mode {mode:04o}, not {DIRECTORY_MODE:04o}"),
            "chmod 2750 it so the socket inherits the group and others are refused",
        ));
    }
    Ok(())
}

/// After binding: the socket must have inherited group `gid` and carry mode 0660.
pub fn check_socket(path: &Path, gid: u32) -> io::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    let mode = metadata.mode() & 0o7777;
    if !metadata.file_type().is_socket() || metadata.gid() != gid || mode != SOCKET_MODE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "ingest socket {} has group {} and mode {mode:04o}, not group {gid} and mode {SOCKET_MODE:04o}; check that its directory is setgid and nothing else changes the name",
                path.display(),
                metadata.gid()
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// INVARIANT: startup refuses an ingest directory that is a symlink, is owned by
    /// another account, has another group, or has any mode other than 2750.
    #[test]
    fn ingest_socket_directory_with_wrong_mode_owner_or_group_fails_startup() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("ingest");
        std::fs::create_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o2750)).unwrap();
        let metadata = std::fs::metadata(&dir).unwrap();
        let (owner, gid) = (metadata.uid(), metadata.gid());
        check_directory(&dir, owner, gid).unwrap();

        let refused = |path: &Path, owner: u32, gid: u32, expected: &str| {
            let error = check_directory(path, owner, gid).unwrap_err().to_string();
            assert!(error.contains(expected), "{error}");
        };
        refused(&dir, owner.wrapping_add(1), gid, "not the daemon's uid");
        refused(&dir, owner, gid.wrapping_add(1), "not ingest_socket_gid");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o2770)).unwrap();
        refused(&dir, owner, gid, "has mode 2770");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o750)).unwrap();
        refused(&dir, owner, gid, "has mode 0750");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o2750)).unwrap();
        let link = root.path().join("link");
        std::os::unix::fs::symlink(&dir, &link).unwrap();
        refused(&link, owner, gid, "symbolic link");
    }
}
