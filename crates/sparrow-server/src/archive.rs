// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The archive worker: moves aged-out evidence onto the archive root and marks it archived.
//!
//! One named thread takes each prepared segment from the store, writes its files under
//! `<root>/<store_id>/<segment>/`, syncs the files and the directories, reads every file
//! back byte-equal and passes the set to `decode::verify`, and only then commits the
//! store's `archive:through` watermark. A failure at any step leaves the watermark where it
//! was and is journaled with its cause; the next pass writes the same segment again.
//!
//! Everything here is format-free: a segment is named byte buffers, and `decode` is the one
//! module that knows how they are encoded. The thread runs every filesystem call on the
//! archive, so a hung network mount can stall this thread and nothing else.

mod decode;

use sparrow_core::{ArchiveHold, PreparedSegment, Store};
use std::{
    fmt, fs,
    io::{self, Write},
    path::{Component, Path, PathBuf},
    sync::mpsc,
    thread,
    time::Duration,
};

/// One structured line for a held record, written on every pass that sees one. The count of
/// held records and the bytes waiting behind the first are not known to the store.
fn journal_held(held: &ArchiveHold) {
    eprintln!(
        "sparrowd: archive held record={} bytes={} position={} count=unknown bytes_waiting=unknown; the watermark stays behind it",
        held.record_id, held.bytes, held.position
    );
    tracing::warn!(
        record = %held.record_id,
        bytes = held.bytes,
        position = %held.position,
        count = "unknown",
        bytes_waiting = "unknown",
        "archive held a record; the watermark stays behind it"
    );
}

/// Why a segment is not durable on the archive root.
#[derive(Debug)]
pub(crate) enum ArchiveError {
    /// A filesystem step failed.
    Io {
        step: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    /// A segment name or store id is not a single plain path component.
    Name(String),
    /// What was read back differs from what was written, or the tables do not decode.
    Verify { path: PathBuf, reason: String },
    /// The archive root does not hold this store's `STORE_ID`: an unmounted mount point, or
    /// another store's archive.
    Root { path: PathBuf, reason: String },
    /// The store refused a step.
    Store(sparrow_core::Error),
}

impl fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { step, path, source } => {
                write!(f, "{step} {} failed: {source}", path.display())
            }
            Self::Name(name) => write!(f, "{name:?} is not a plain single path component"),
            Self::Verify { path, reason } => {
                write!(f, "read-back of {} failed: {reason}", path.display())
            }
            Self::Root { path, reason } => {
                write!(
                    f,
                    "archive root {} is not this store's: {reason}",
                    path.display()
                )
            }
            Self::Store(error) => write!(f, "store: {} {}", error.code, error.message),
        }
    }
}

impl std::error::Error for ArchiveError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

fn io_at(step: &'static str, path: &Path) -> impl FnOnce(io::Error) -> ArchiveError {
    let path = path.to_path_buf();
    move |source| ArchiveError::Io { step, path, source }
}

fn plain_component(name: &str) -> Result<(), ArchiveError> {
    let mut parts = Path::new(name).components();
    match (parts.next(), parts.next()) {
        (Some(Component::Normal(part)), None) if part == name => Ok(()),
        _ => Err(ArchiveError::Name(name.to_owned())),
    }
}

fn sync_dir(path: &Path) -> Result<(), ArchiveError> {
    fs::File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(io_at("sync directory", path))
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), ArchiveError> {
    let mut file = fs::File::create(path).map_err(io_at("create", path))?;
    file.write_all(bytes).map_err(io_at("write", path))?;
    file.sync_all().map_err(io_at("sync", path))
}

/// Write each file as `<name>.partial`, sync it, rename it into place and sync the store
/// directory. A file already at its name is a retry of an earlier pass: it is kept and
/// verified, never overwritten.
fn write_segment(store_dir: &Path, segment: &PreparedSegment) -> Result<(), ArchiveError> {
    for (name, bytes) in &segment.files {
        plain_component(name)?;
        let target = store_dir.join(name);
        if target.exists() {
            continue;
        }
        // A stale partial from a crashed pass is truncated by the create.
        let partial = store_dir.join(format!("{name}.partial"));
        write_synced(&partial, bytes)?;
        fs::rename(&partial, &target).map_err(io_at("rename into place", &target))?;
    }
    sync_dir(store_dir)
}

/// Read every file back and require it equal to what was written, then let the decoder
/// check the set.
fn verify_segment(store_dir: &Path, segment: &PreparedSegment) -> Result<(), ArchiveError> {
    for (name, expected) in &segment.files {
        let path = store_dir.join(name);
        let read = fs::read(&path).map_err(io_at("read back", &path))?;
        if read != *expected {
            return Err(ArchiveError::Verify {
                path,
                reason: format!(
                    "{} bytes read, {} written, or contents differ",
                    read.len(),
                    expected.len()
                ),
            });
        }
    }
    decode::verify(&segment.files).map_err(|reason| ArchiveError::Verify {
        path: store_dir.to_path_buf(),
        reason,
    })
}

/// The root must already hold this store's `STORE_ID`, the file the backup engine writes: an
/// unmounted mount point is an empty directory on the local disk and has none. Read only; the
/// worker never creates or changes it.
fn require_store_id(root: &Path, store_id: &str) -> Result<(), ArchiveError> {
    let path = root.join("STORE_ID");
    let recorded = match fs::read_to_string(&path) {
        Ok(recorded) => recorded,
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            return Err(ArchiveError::Root {
                path: root.to_path_buf(),
                reason: "STORE_ID is missing; is the archive mounted?".into(),
            });
        }
        Err(source) => return Err(io_at("read", &path)(source)),
    };
    if recorded.trim_end() == store_id {
        Ok(())
    } else {
        Err(ArchiveError::Root {
            path: root.to_path_buf(),
            reason: format!("STORE_ID names {:?}, not {store_id}", recorded.trim_end()),
        })
    }
}

/// Make one segment durable under `<root>/<store_id>/` and only then advance the watermark.
///
/// `root` must already exist and hold this store's `STORE_ID`: a missing or unmounted root is
/// a failure, never a directory created on the local disk. Only `<root>/<store_id>` is created; nothing else in `root` is touched.
pub(crate) fn archive_segment(
    store: &Store,
    root: &Path,
    store_id: &str,
    segment: &PreparedSegment,
) -> Result<PathBuf, ArchiveError> {
    plain_component(store_id)?;
    if !root.is_dir() {
        return Err(ArchiveError::Io {
            step: "find archive root",
            path: root.to_path_buf(),
            source: io::Error::new(io::ErrorKind::NotFound, "not a directory; is it mounted?"),
        });
    }
    require_store_id(root, store_id)?;
    let store_dir = root.join(store_id);
    if !store_dir.exists() {
        fs::create_dir(&store_dir).map_err(io_at("create", &store_dir))?;
        sync_dir(root)?;
    }
    write_segment(&store_dir, segment)?;
    verify_segment(&store_dir, segment)?;
    store
        .archive_commit(&segment.segment_id)
        .map_err(ArchiveError::Store)?;
    Ok(store_dir)
}

/// One archive pass over the backlog: segments until none is due, then a look at whether a
/// held record stands at the head. Returns how many segments committed.
fn drain(store: &Store, root: &Path, store_id: &str, now: u64) -> Result<u64, ArchiveError> {
    let mut committed = 0;
    while let Some(segment) = store.archive_prepare(now).map_err(ArchiveError::Store)? {
        let dir = archive_segment(store, root, store_id, &segment)?;
        committed += 1;
        eprintln!(
            "sparrowd: archive committed segment={} records={} artifacts={} retired_records={} dir={}",
            segment.name(),
            segment.records,
            segment.artifacts,
            segment.retired_records,
            dir.display()
        );
        tracing::info!(
            segment = %segment.name(),
            records = segment.records,
            artifacts = segment.artifacts,
            retired_records = segment.retired_records,
            dir = %dir.display(),
            "archive segment committed"
        );
    }
    if let Some(held) = store.archive_held().map_err(ArchiveError::Store)? {
        journal_held(&held);
    }
    Ok(committed)
}

/// Handle to the running archive thread. Dropping it asks the thread to stop; the thread is
/// never joined, because a hung network mount must not hold the daemon's shutdown.
pub struct Worker {
    _stop: mpsc::Sender<()>,
}

impl Worker {
    /// Start the thread. `every` is how long it waits after a pass finds nothing due, or fails,
    /// before looking again; the store itself decides what is due at its 75 per cent start.
    pub fn start(store: Store, root: PathBuf, every: Duration) -> Result<Self, io::Error> {
        let (stop, stopped) = mpsc::channel::<()>();
        thread::Builder::new()
            .name("sparrow-archive".into())
            .spawn(move || run(&store, &root, every, &stopped))?;
        Ok(Self { _stop: stop })
    }
}

fn run(store: &Store, root: &Path, every: Duration, stopped: &mpsc::Receiver<()>) {
    loop {
        let outcome = match (store.store_id(), crate::now()) {
            (Ok(id), Ok(now)) => drain(store, root, &id, now).map_err(|error| error.to_string()),
            (Err(error), _) => Err(format!("store id: {} {}", error.code, error.message)),
            (_, Err(error)) => Err(format!("clock: {error}")),
        };
        if let Err(cause) = outcome {
            eprintln!("sparrowd: archive pass failed; the watermark did not move: {cause}");
            tracing::error!(%cause, "archive pass failed; the watermark did not move");
        }
        // A timed wait on the stop channel: it ends when the worker is dropped.
        if !matches!(
            stopped.recv_timeout(every),
            Err(mpsc::RecvTimeoutError::Timeout)
        ) {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_core::Config;

    /// A time after every window and horizon, so the store reports its backlog due.
    const LATER: u64 = 4_000_000_000;

    /// A store that archives, holding one ingested artifact, and its prepared segment.
    fn archiving_store(dir: &Path) -> (Store, PreparedSegment) {
        let config = Config {
            archive: Some(sparrow_core::ArchivePolicy::default()),
            ..sparrow_test_support::storage_config!(Config)
        };
        let store = Store::open(dir, config).unwrap();
        store.ingest("r", "source", "log", b"a\nb\n", 1).unwrap();
        let segment = store
            .archive_prepare(LATER)
            .unwrap()
            .expect("a due segment");
        (store, segment)
    }

    /// An archive root that holds `id` as its `STORE_ID`, as the backup engine leaves it.
    fn nas(dir: &Path, id: &str) -> PathBuf {
        let root = dir.join("nas");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("STORE_ID"), format!("{id}\n")).unwrap();
        root
    }

    fn through(store: &Store) -> Option<String> {
        store.archive_state().unwrap().unwrap().through
    }

    /// INVARIANT: a missing archive root fails and leaves the watermark where it was; the
    /// root is never created on the local disk.
    #[test]
    fn the_watermark_stays_when_the_archive_root_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let (store, segment) = archiving_store(&dir.path().join("store"));
        let missing = dir.path().join("not-mounted");
        let error = archive_segment(&store, &missing, "s", &segment).unwrap_err();
        assert!(matches!(error, ArchiveError::Io { .. }), "{error}");
        assert_eq!(through(&store), None);
        assert!(!missing.exists());
    }

    /// INVARIANT: a root that is not this store's (an unmounted mount point with no `STORE_ID`,
    /// or another store's archive) is refused, nothing is written into it and the watermark
    /// stays.
    #[test]
    fn the_watermark_stays_when_the_root_is_not_this_stores() {
        let dir = tempfile::tempdir().unwrap();
        let (store, segment) = archiving_store(&dir.path().join("store"));
        let other = nas(dir.path(), "another-store");
        let error = archive_segment(&store, &other, "s", &segment).unwrap_err();
        assert!(matches!(error, ArchiveError::Root { .. }), "{error}");
        assert!(!other.join("s").exists());
        let bare = dir.path().join("bare");
        fs::create_dir(&bare).unwrap();
        let error = archive_segment(&store, &bare, "s", &segment).unwrap_err();
        assert!(matches!(error, ArchiveError::Root { .. }), "{error}");
        assert_eq!(fs::read_dir(&bare).unwrap().count(), 0);
        assert_eq!(through(&store), None);
    }

    /// INVARIANT: a segment that reads back different from what was written is never marked
    /// archived. A file already at the segment's name stands in for a damaged write.
    #[test]
    fn the_watermark_stays_when_the_read_back_differs() {
        let dir = tempfile::tempdir().unwrap();
        let (store, segment) = archiving_store(&dir.path().join("store"));
        let root = nas(dir.path(), "s");
        fs::create_dir(root.join("s")).unwrap();
        fs::write(root.join("s").join(&segment.files[0].0), b"not a segment").unwrap();
        let error = archive_segment(&store, &root, "s", &segment).unwrap_err();
        assert!(matches!(error, ArchiveError::Verify { .. }), "{error}");
        assert_eq!(through(&store), None);
    }

    /// INVARIANT: a segment written, synced, read back and decoded advances the watermark to
    /// its last position, leaves no partial file, and the file on disk decodes.
    #[test]
    fn a_verified_segment_advances_the_watermark_and_its_file_decodes() {
        let dir = tempfile::tempdir().unwrap();
        let (store, segment) = archiving_store(&dir.path().join("store"));
        let root = nas(dir.path(), "s");
        let written = archive_segment(&store, &root, "s", &segment).unwrap();
        assert_eq!(written, root.join("s"));
        assert_eq!(through(&store).as_deref(), Some(segment.through.as_str()));
        let (name, _) = &segment.files[0];
        decode::verify(&[(name.clone(), fs::read(written.join(name)).unwrap())]).unwrap();
        let names: Vec<_> = fs::read_dir(&written)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, [std::ffi::OsString::from(name)]);
    }
}
