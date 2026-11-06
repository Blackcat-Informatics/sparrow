// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Local endpoint recovery under the already-open store's exclusive native lock.

use std::{
    io,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

pub(super) struct LocalEndpoint {
    pub(super) listener: tokio::net::UnixListener,
    path: PathBuf,
    identity: (u64, u64),
}

impl LocalEndpoint {
    pub(super) async fn bind(path: &Path, store: &sparrow_core::Store) -> io::Result<Self> {
        if path.parent() != Some(store.data_path().map_err(io::Error::other)?.as_path()) {
            return Err(io::Error::other(
                "local endpoint must be inside the owned private store",
            ));
        }
        let endpoint = Self::bind_recovering(path).await?;
        // Members of the daemon's group connect; data_dir (0710) admits no one else.
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660))?;
        Ok(endpoint)
    }

    /// The ingest socket: a checked setgid directory outside the store, and a socket that
    /// inherited its group and carries mode 0660. A mismatch fails startup.
    pub(super) async fn bind_ingest(path: &Path, gid: u32) -> io::Result<Self> {
        let directory = path
            .parent()
            .ok_or_else(|| io::Error::other("ingest socket path has no directory"))?;
        sparrow_server::ingest_socket::check_directory(
            directory,
            rustix::process::geteuid().as_raw(),
            gid,
        )?;
        let endpoint = Self::bind_recovering(path).await?;
        let checked = std::fs::set_permissions(
            path,
            std::fs::Permissions::from_mode(sparrow_server::ingest_socket::SOCKET_MODE),
        )
        .and_then(|()| sparrow_server::ingest_socket::check_socket(path, gid));
        if let Err(error) = checked {
            endpoint.remove_name()?;
            return Err(error);
        }
        Ok(endpoint)
    }

    /// Bind `path`, replacing a stale name only after a refused connection proves it dead.
    async fn bind_recovering(path: &Path) -> io::Result<Self> {
        let listener = match tokio::net::UnixListener::bind(path) {
            Ok(listener) => listener,
            Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
                let original = socket_identity(path)?;
                match tokio::time::timeout(
                    Duration::from_millis(250),
                    tokio::net::UnixStream::connect(path),
                )
                .await
                {
                    Ok(Err(error)) if error.kind() == io::ErrorKind::ConnectionRefused => {}
                    Ok(Ok(_)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::AddrInUse,
                            "local endpoint has a live listener; preserving it",
                        ));
                    }
                    Ok(Err(error)) => {
                        return Err(io::Error::new(
                            error.kind(),
                            format!("local endpoint liveness is unproven; preserving it: {error}"),
                        ));
                    }
                    Err(_) => {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "local endpoint liveness probe timed out; preserving it",
                        ));
                    }
                }
                remove_socket(path, original)?;
                eprintln!(
                    "sparrowd: removed refused stale local endpoint {}",
                    path.display()
                );
                tokio::net::UnixListener::bind(path)?
            }
            Err(error) => return Err(error),
        };
        let identity = socket_identity(path)?;
        Ok(Self {
            listener,
            path: path.to_owned(),
            identity,
        })
    }

    pub(super) fn remove_name(&self) -> io::Result<()> {
        remove_socket(&self.path, self.identity)
    }
}

fn socket_identity(path: &Path) -> io::Result<(u64, u64)> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_socket() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "local endpoint name is not a socket; preserving it",
        ));
    }
    Ok((metadata.dev(), metadata.ino()))
}

fn remove_socket(path: &Path, expected: (u64, u64)) -> io::Result<()> {
    if socket_identity(path)? != expected {
        return Err(io::Error::other(
            "local endpoint identity changed; preserving its replacement",
        ));
    }
    std::fs::remove_file(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_socket_cleanup_preserves_replaced_names() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rpc.sock");
        let _original = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let identity = socket_identity(&path).unwrap();
        std::fs::rename(&path, dir.path().join("original.sock")).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        assert_eq!(
            remove_socket(&path, identity).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
        std::fs::remove_file(&path).unwrap();
        let _replacement = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let replacement_identity = socket_identity(&path).unwrap();
        assert!(
            remove_socket(&path, identity)
                .unwrap_err()
                .to_string()
                .contains("identity changed")
        );
        assert_eq!(socket_identity(&path).unwrap(), replacement_identity);
        assert!(std::os::unix::net::UnixStream::connect(&path).is_ok());
    }
}
