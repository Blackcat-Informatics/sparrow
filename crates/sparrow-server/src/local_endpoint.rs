// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Local endpoint recovery under the already-open store's exclusive native lock.
//!
//! A Unix socket's name can be removed while the daemon runs (a cleanup script, an operator).
//! The listener keeps working but no client can reach it, so the accept loop checks the name
//! on each of its one-second observations ([`LocalEndpoint::restore_name`]) and binds it again.
//! The rules that apply when the daemon starts apply then too: a name that another listener
//! answers on, or that is not a socket, is preserved, and a socket name is replaced only once a
//! refused connection proves it dead. The ingest socket's directory, group and mode are checked
//! again on every bind.

use std::{
    cell::{Cell, RefCell},
    io,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

/// What the name must carry once bound.
#[derive(Clone, Copy)]
enum Kind {
    /// The private socket inside the store directory, mode 0660.
    Private,
    /// The ingest socket in its checked setgid directory, owned by this group.
    Ingest { gid: u32 },
}

/// A Unix socket the daemon listens on, and the name it is reachable by.
pub struct LocalEndpoint {
    listener: RefCell<tokio::net::UnixListener>,
    path: PathBuf,
    identity: Cell<(u64, u64)>,
    kind: Kind,
    /// The standing problem last journaled, so a problem that lasts is journaled once.
    reported: RefCell<Option<String>>,
}

impl LocalEndpoint {
    /// The private socket, inside the owned store directory.
    ///
    /// # Errors
    /// A path outside the store directory, a live or unproven listener on the name, or a bind
    /// failure.
    pub async fn bind(path: &Path, store: &sparrow_core::Store) -> io::Result<Self> {
        if path.parent() != Some(store.data_path().map_err(io::Error::other)?.as_path()) {
            return Err(io::Error::other(
                "local endpoint must be inside the owned private store",
            ));
        }
        recover_stale(path).await?;
        Self::bound(path, Kind::Private)
    }

    /// The ingest socket: a checked setgid directory outside the store, and a socket that
    /// inherited its group and carries mode 0660. A mismatch fails startup.
    ///
    /// # Errors
    /// A directory or socket that fails its checks, a live or unproven listener on the name,
    /// or a bind failure.
    pub async fn bind_ingest(path: &Path, gid: u32) -> io::Result<Self> {
        recover_stale(path).await?;
        Self::bound(path, Kind::Ingest { gid })
    }

    /// Bind the free name `path` and give it the mode and group `kind` requires.
    fn bound(path: &Path, kind: Kind) -> io::Result<Self> {
        let (listener, identity) = bind_name(path, kind)?;
        Ok(Self {
            listener: RefCell::new(listener),
            path: path.to_owned(),
            identity: Cell::new(identity),
            kind,
            reported: RefCell::new(None),
        })
    }

    /// Accept the next connection on the current listener.
    ///
    /// # Errors
    /// The listener's accept failure.
    pub async fn accept(
        &self,
    ) -> io::Result<(tokio::net::UnixStream, tokio::net::unix::SocketAddr)> {
        std::future::poll_fn(|cx| self.listener.borrow().poll_accept(cx)).await
    }

    /// Bind the name again when it no longer reaches this endpoint; the journal line to write,
    /// if any. A removed name is bound at once, and a refused stale socket left on the name is
    /// replaced. A name another live listener answers on, or that is not a socket, is
    /// preserved: that problem, and a bind that fails, is journaled once while it lasts and
    /// tried again on the next call.
    pub async fn restore_name(&self) -> Option<String> {
        let restored = match socket_identity(&self.path) {
            Ok(identity) if identity == self.identity.get() => {
                self.reported.replace(None);
                return None;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.rebind().map(|()| "was removed; bound it again")
            }
            Ok(_) => match recover_stale(&self.path).await {
                Ok(()) => self
                    .rebind()
                    .map(|()| "named a refused stale socket; replaced it"),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };
        let line = match restored {
            Ok(done) => {
                self.reported.replace(None);
                format!("sparrowd: local endpoint {} {done}", self.path.display())
            }
            Err(error) => {
                let problem = format!(
                    "sparrowd: local endpoint {} does not reach this daemon: {error}",
                    self.path.display()
                );
                if self.reported.borrow().as_deref() == Some(problem.as_str()) {
                    return None;
                }
                self.reported.replace(Some(problem.clone()));
                problem
            }
        };
        Some(line)
    }

    /// Bind the free name again and replace the listener with the new one.
    fn rebind(&self) -> io::Result<()> {
        let (listener, identity) = bind_name(&self.path, self.kind)?;
        self.listener.replace(listener);
        self.identity.set(identity);
        Ok(())
    }

    /// Remove the name if it still names this endpoint's socket.
    ///
    /// # Errors
    /// A name that is no longer this socket, which is preserved, or a removal failure.
    pub fn remove_name(&self) -> io::Result<()> {
        remove_socket(&self.path, self.identity.get())
    }
}

/// Bind the free name `path` for `kind` and return the listener and the name's identity. An
/// ingest name that fails its group or mode check is removed again.
fn bind_name(path: &Path, kind: Kind) -> io::Result<(tokio::net::UnixListener, (u64, u64))> {
    if let Kind::Ingest { gid } = kind {
        let directory = path
            .parent()
            .ok_or_else(|| io::Error::other("ingest socket path has no directory"))?;
        crate::ingest_socket::check_directory(directory, rustix::process::geteuid().as_raw(), gid)?;
    }
    let listener = tokio::net::UnixListener::bind(path)?;
    let identity = socket_identity(path)?;
    let checked = match kind {
        // Members of the daemon's group connect; data_dir (0710) admits no one else.
        Kind::Private => std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660)),
        Kind::Ingest { gid } => std::fs::set_permissions(
            path,
            std::fs::Permissions::from_mode(crate::ingest_socket::SOCKET_MODE),
        )
        .and_then(|()| crate::ingest_socket::check_socket(path, gid)),
    };
    if let Err(error) = checked {
        remove_socket(path, identity)?;
        return Err(error);
    }
    Ok((listener, identity))
}

/// Remove a socket left on `path` once a refused connection proves no listener is behind it;
/// a free name needs nothing. A live listener, a name that is not a socket, and a liveness
/// probe that neither connects nor is refused all preserve the name.
async fn recover_stale(path: &Path) -> io::Result<()> {
    let original = match socket_identity(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        identity => identity?,
    };
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
    Ok(())
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

    /// INVARIANT (#447): restoring a name keeps the liveness rules of a start. A name replaced
    /// by a file that is not a socket, or by another live listener, is preserved untouched and
    /// journaled once while the problem lasts; once the name is free it is bound again, and
    /// the endpoint accepts on it.
    #[tokio::test(flavor = "current_thread")]
    async fn restoring_a_name_preserves_a_replacement_and_rebinds_a_free_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rpc.sock");
        let endpoint = LocalEndpoint::bound(&path, Kind::Private).unwrap();
        assert_eq!(endpoint.restore_name().await, None);

        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        let line = endpoint.restore_name().await.unwrap();
        assert!(line.contains("not a socket; preserving it"), "{line}");
        assert_eq!(endpoint.restore_name().await, None);
        assert_eq!(std::fs::read(&path).unwrap(), b"replacement");

        std::fs::remove_file(&path).unwrap();
        let live = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let line = endpoint.restore_name().await.unwrap();
        assert!(line.contains("live listener; preserving it"), "{line}");
        drop(live);
        std::fs::remove_file(&path).unwrap();

        let line = endpoint.restore_name().await.unwrap();
        assert!(line.contains("was removed; bound it again"), "{line}");
        let connected = tokio::net::UnixStream::connect(&path);
        let (accepted, connected) = tokio::join!(endpoint.accept(), connected);
        accepted.unwrap();
        connected.unwrap();
        endpoint.remove_name().unwrap();
    }

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
