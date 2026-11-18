// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The archive worker: moves aged-out evidence onto the archive root and marks it archived.
//!
//! One named thread takes each prepared segment from the store, writes its files directly
//! under `<root>/<store_id>/` as `<from>-<through>.gts`, syncs the files and the
//! directories, reads every file back byte-equal and passes the set to `decode::verify` (the
//! reader's decoder, which also hashes each record's bytes against its link digest), and
//! only then commits the store's `archive:through` watermark. A failure at any step leaves
//! the watermark where it was and is journaled with its cause; the next pass writes the
//! same segment again.
//!
//! Everything here is format-free: a segment is named byte buffers, and `decode` is the one
//! module that knows how they are encoded. The thread runs every filesystem call on the
//! archive, so a hung network mount can stall this thread and nothing else. A second
//! thread, the watchdog, watches the step in flight from outside: it journals a stall line
//! while a step runs longer than `stall_warn_secs`, and a pass whose steps overran
//! `io_timeout_secs` is not committed (see [`Flight`]).

mod decode;

use sparrow_core::{ArchiveHold, HoldReason, PreparedSegment, Store};
use std::{
    fmt, fs,
    io::{self, Write},
    path::{Component, Path, PathBuf},
    sync::mpsc,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    thread,
    time::{Duration, Instant},
};

/// The archive step in flight, visible to the watchdog thread. The archive thread is blocked
/// inside the syscall when a mount hangs, so only another thread can notice. Every time
/// comes in as an argument, so the stall decision is testable without a clock.
///
/// `io_timeout_secs` cannot cancel a blocking call. Its effect loses nothing: a step that
/// ran at least that long marks the pass as overrun, the segment is not committed on that
/// pass, and the next pass finds its files, verifies them and commits then. The worker is
/// one thread, so it starts no further step until the stuck call returns.
pub(crate) struct Flight {
    warn: Duration,
    timeout: Duration,
    /// The directory standing for the local root disk: `/`. An archive directory on its
    /// device, or on a local filesystem of its type, is not on its mount.
    filesystem_root: PathBuf,
    state: Mutex<FlightState>,
}

#[derive(Default)]
struct FlightState {
    current: Option<Current>,
    overran: bool,
}

struct Current {
    step: &'static str,
    path: PathBuf,
    since: Instant,
    reported: Option<Instant>,
}

/// What the watchdog journals for a step that has run too long.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Stall {
    step: &'static str,
    path: PathBuf,
    elapsed: Duration,
    timed_out: bool,
}

impl Flight {
    pub(crate) fn new(warn: Duration, timeout: Duration) -> Self {
        Self {
            warn,
            timeout,
            filesystem_root: PathBuf::from("/"),
            state: Mutex::new(FlightState::default()),
        }
    }

    /// The same flight with another directory standing for the root filesystem, so a test
    /// needs no mount.
    #[cfg(test)]
    fn rooted_at(mut self, filesystem_root: impl Into<PathBuf>) -> Self {
        self.filesystem_root = filesystem_root.into();
        self
    }

    fn state(&self) -> MutexGuard<'_, FlightState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn begin_at(&self, now: Instant, step: &'static str, path: &Path) {
        self.state().current = Some(Current {
            step,
            path: path.to_path_buf(),
            since: now,
            reported: None,
        });
    }

    fn end_at(&self, now: Instant) {
        let mut state = self.state();
        if let Some(current) = state.current.take()
            && now.saturating_duration_since(current.since) >= self.timeout
        {
            state.overran = true;
        }
    }

    /// Mark a step in flight until the guard drops.
    fn step<'a>(&'a self, step: &'static str, path: &Path) -> StepGuard<'a> {
        self.begin_at(Instant::now(), step, path);
        StepGuard(self)
    }

    /// The report due for the step in flight at `now`: none before `warn`, then one each
    /// `warn` while the step lasts, so the journal grows at a bounded rate.
    pub(crate) fn observe(&self, now: Instant) -> Option<Stall> {
        let mut state = self.state();
        let current = state.current.as_mut()?;
        let elapsed = now.saturating_duration_since(current.since);
        let due = elapsed >= self.warn
            && current
                .reported
                .is_none_or(|at| now.saturating_duration_since(at) >= self.warn);
        if !due {
            return None;
        }
        current.reported = Some(now);
        let report = Stall {
            step: current.step,
            path: current.path.clone(),
            elapsed,
            timed_out: elapsed >= self.timeout,
        };
        if report.timed_out {
            state.overran = true;
        }
        Some(report)
    }

    /// Forget an overrun from an earlier pass; a pass is judged on its own steps.
    fn start_pass(&self) {
        self.state().overran = false;
    }

    fn overran(&self) -> bool {
        self.state().overran
    }
}

struct StepGuard<'a>(&'a Flight);

impl Drop for StepGuard<'_> {
    fn drop(&mut self) {
        self.0.end_at(Instant::now());
    }
}

fn journal_stall(stall: &Stall) {
    let verdict = if stall.timed_out {
        "past io_timeout_secs; the segment will not be committed on this pass"
    } else {
        "still running"
    };
    eprintln!(
        "sparrowd: archive stalled step={:?} path={} elapsed_secs={} {verdict}",
        stall.step,
        stall.path.display(),
        stall.elapsed.as_secs()
    );
    tracing::warn!(
        step = stall.step,
        path = %stall.path.display(),
        elapsed_secs = stall.elapsed.as_secs(),
        timed_out = stall.timed_out,
        "archive step stalled"
    );
}

/// Poll the flight at a rate that keeps a report within a second or two of its due time.
fn watch(flight: &Flight, stopped: &mpsc::Receiver<()>) {
    let poll = (flight.warn / 4).clamp(Duration::from_millis(100), Duration::from_secs(5));
    while matches!(
        stopped.recv_timeout(poll),
        Err(mpsc::RecvTimeoutError::Timeout)
    ) {
        if let Some(stall) = flight.observe(Instant::now()) {
            journal_stall(&stall);
        }
    }
}

/// One structured line for a held record, written on every pass that sees one. The count of
/// held records and the bytes waiting behind the first are not known to the store.
fn journal_held(held: &ArchiveHold) {
    let reason = match held.reason {
        HoldReason::TooLarge => "oversized",
        HoldReason::ProjectionFailed => "projection_failed",
    };
    eprintln!(
        "sparrowd: archive held record={} reason={reason} bytes={} position={} \
         count=unknown bytes_waiting=unknown; the watermark stays behind it",
        held.record_id, held.bytes, held.position
    );
    tracing::warn!(
        record = %held.record_id,
        reason,
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
    /// The archive directory is on the root filesystem's device, or on a local filesystem of
    /// the same type as the root's, so the share that should hold it is not mounted and the
    /// directory is an ordinary local one. `found` names which. Nothing was written.
    NotOnMount {
        path: PathBuf,
        filesystem_root: PathBuf,
        found: String,
    },
    /// The archive root is not this store's to use: another store's `STORE_ID`, or a directory
    /// with content but no `STORE_ID`.
    Root { path: PathBuf, reason: String },
    /// A step ran at least `io_timeout_secs`. Nothing was committed; the next pass verifies
    /// the files already on the root and commits then.
    Overran,
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
            Self::NotOnMount {
                path,
                filesystem_root,
                found,
            } => write!(
                f,
                "archive directory {} is not on its expected mount: {found} ({} is the local \
                 root), so the archive share is not mounted and the directory is on the local \
                 disk; nothing was written",
                path.display(),
                filesystem_root.display()
            ),
            Self::Root { path, reason } => {
                write!(
                    f,
                    "archive root {} is not this store's: {reason}",
                    path.display()
                )
            }
            Self::Overran => write!(
                f,
                "an archive step ran past io_timeout_secs; the segment was not committed and \
                 is verified again on the next pass"
            ),
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

fn sync_dir(flight: &Flight, path: &Path) -> Result<(), ArchiveError> {
    let _step = flight.step("sync directory", path);
    fs::File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(io_at("sync directory", path))
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), ArchiveError> {
    let mut file = fs::File::create(path).map_err(io_at("create", path))?;
    file.write_all(bytes).map_err(io_at("write", path))?;
    file.sync_all().map_err(io_at("sync", path))
}

/// Publish `bytes` as `<dir>/<name>`: write `<name>.partial`, sync it and rename it into
/// place. The caller syncs `dir`. A stale partial from a crashed pass is truncated by the
/// create. The one publication step of the archive: segments and `STORE_ID` both use it.
fn publish(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), ArchiveError> {
    let target = dir.join(name);
    let partial = dir.join(format!("{name}.partial"));
    write_synced(&partial, bytes)?;
    fs::rename(&partial, &target).map_err(io_at("rename into place", &target))
}

/// Write each file as `<name>.partial`, sync it, rename it into place and sync the store
/// directory. A file already at its name is a retry of an earlier pass: it is kept and
/// verified, never overwritten.
fn write_segment(
    flight: &Flight,
    store_dir: &Path,
    segment: &PreparedSegment,
) -> Result<(), ArchiveError> {
    for (name, bytes) in &segment.files {
        plain_component(name)?;
        let target = store_dir.join(name);
        let _step = flight.step("write segment file", &target);
        if target.exists() {
            continue;
        }
        publish(store_dir, name, bytes)?;
    }
    sync_dir(flight, store_dir)
}

/// Read every file back and require it equal to what was written, then let the decoder
/// check the set.
fn verify_segment(
    flight: &Flight,
    store_dir: &Path,
    segment: &PreparedSegment,
) -> Result<(), ArchiveError> {
    for (name, expected) in &segment.files {
        let path = store_dir.join(name);
        let read = {
            let _step = flight.step("read back", &path);
            fs::read(&path).map_err(io_at("read back", &path))?
        };
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

/// Filesystem magic numbers (`statfs` `f_type`) of the network filesystems an archive share is
/// mounted as: NFS, SMB (the legacy `smb`, CIFS and SMB2), Ceph, AFS, 9P, Lustre, NCP and
/// Coda. FUSE is left out: it fronts local storage as often as a share.
const NETWORK_FILESYSTEMS: &[(u64, &str)] = &[
    (0x6969, "nfs"),
    (0x517B, "smb"),
    (0xFF53_4D42, "cifs"),
    (0xFE53_4D42, "smb2"),
    (0x00C3_6400, "ceph"),
    (0x6B41_4653, "afs"),
    (0x0102_1997, "9p"),
    (0x0BD0_0BD0, "lustre"),
    (0x564C, "ncp"),
    (0x7375_7245, "coda"),
];

/// The name of a network filesystem type, or `None` for any other.
fn network_filesystem(f_type: u64) -> Option<&'static str> {
    NETWORK_FILESYSTEMS
        .iter()
        .find_map(|&(magic, name)| (magic == f_type).then_some(name))
}

/// Where a path lives: its device and its filesystem type (`statfs` `f_type`).
#[derive(Clone, Copy)]
struct Placement {
    device: u64,
    f_type: u64,
}

/// The archive directory must be on its own mount. The archive sits below a network mount
/// point; when the share is mounted the directory has the share's device and a network
/// filesystem type, and when it is not, the mount point is an ordinary directory of some
/// local filesystem. Two rules, pure over the values read from the directory and from `/`:
///
/// 1. the device must differ from the root filesystem's; and
/// 2. the filesystem type must be a network filesystem or differ from the root's type. This
///    catches an unmounted share whose parent is another local mount of the root's type (a
///    btrfs subvolume under `/home`), which has a device of its own. It also refuses a
///    second local disk of the root's type mounted as the archive. A mount point inside a
///    local filesystem of a type other than `/`'s passes both rules, and when it is empty
///    [`ensure_store_id`] adopts it and the segments go to the local disk. `STORE_ID` stops
///    only a directory that has content or another store's id. The configured `dir` must
///    therefore be a directory inside the share, never the mount point itself: an
///    unmounted share leaves no such subdirectory, so the root is then missing and refused.
fn require_own_mount(
    dir: &Path,
    found: Placement,
    filesystem_root: &Path,
    root: Placement,
) -> Result<(), ArchiveError> {
    let refuse = |found: String| ArchiveError::NotOnMount {
        path: dir.to_path_buf(),
        filesystem_root: filesystem_root.to_path_buf(),
        found,
    };
    if found.device == root.device {
        return Err(refuse(format!(
            "it is on device {}, the same as {}",
            found.device,
            filesystem_root.display()
        )));
    }
    if network_filesystem(found.f_type).is_none() && found.f_type == root.f_type {
        return Err(refuse(format!(
            "its filesystem type is {:#x}, the same local type as {} and not a network filesystem",
            found.f_type,
            filesystem_root.display()
        )));
    }
    Ok(())
}

/// Read the two placements and apply [`require_own_mount`]. Read only.
fn require_mounted(flight: &Flight, dir: &Path) -> Result<(), ArchiveError> {
    use std::os::unix::fs::MetadataExt;
    let place = |path: &Path| {
        let _step = flight.step("read device", path);
        let device = fs::metadata(path)
            .map(|metadata| metadata.dev())
            .map_err(io_at("read device", path))?;
        let stat = rustix::fs::statfs(path)
            .map_err(|errno| io_at("read filesystem type", path)(errno.into()))?;
        // The kernel's word is signed on some targets; the magic numbers are 32-bit patterns.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let f_type = u64::from(stat.f_type as u32);
        Ok::<_, ArchiveError>(Placement { device, f_type })
    };
    require_own_mount(
        dir,
        place(dir)?,
        &flight.filesystem_root,
        place(&flight.filesystem_root)?,
    )
}

/// Whether the root holds nothing an archive owns: no entry at all, apart from the
/// `STORE_ID.partial` an earlier first pass may have left before its rename.
fn is_empty_root(flight: &Flight, root: &Path) -> Result<bool, ArchiveError> {
    let _step = flight.step("list archive root", root);
    for entry in fs::read_dir(root).map_err(io_at("list archive root", root))? {
        let entry = entry.map_err(io_at("list archive root", root))?;
        if entry.file_name() != "STORE_ID.partial" {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The root must hold this store's `STORE_ID`. A root that is on its mount and empty of
/// archive content gets it written here, durably, on the first pass; a root that holds any
/// other entry but no `STORE_ID` is refused, never adopted, and an existing `STORE_ID` is
/// never overwritten: it is accepted when it names this store and refused when it names
/// another.
fn ensure_store_id(flight: &Flight, root: &Path, store_id: &str) -> Result<(), ArchiveError> {
    let path = root.join("STORE_ID");
    let read = {
        let _step = flight.step("read STORE_ID", &path);
        fs::read_to_string(&path)
    };
    let recorded = match read {
        Ok(recorded) => recorded,
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            if !is_empty_root(flight, root)? {
                return Err(ArchiveError::Root {
                    path: root.to_path_buf(),
                    reason: "STORE_ID is missing but the directory holds other entries, so it is \
                             not an empty archive root and is not adopted. Point `[archive] dir` \
                             at an empty directory, or write this store's id and a newline into \
                             STORE_ID if the directory is this store's archive"
                        .into(),
                });
            }
            {
                let _step = flight.step("write STORE_ID", &path);
                publish(root, "STORE_ID", format!("{store_id}\n").as_bytes())?;
            }
            return sync_dir(flight, root);
        }
        Err(source) => return Err(io_at("read", &path)(source)),
    };
    if recorded.trim_end() == store_id {
        Ok(())
    } else {
        Err(ArchiveError::Root {
            path: root.to_path_buf(),
            reason: format!(
                "STORE_ID names {:?}, not {store_id}; this root belongs to another store. \
                 Point `[archive] dir` at this store's archive directory, or mount the right archive",
                recorded.trim_end()
            ),
        })
    }
}

/// Make one segment durable under `<root>/<store_id>/` and only then advance the watermark.
///
/// `root` must already exist and be on its own mount (see [`require_own_mount`]): a missing or
/// unmounted root is a failure, never a directory created on the local disk. It must hold this
/// store's `STORE_ID`, which an empty root is given here (see [`ensure_store_id`]). Only
/// `<root>/<store_id>` and `<root>/STORE_ID` are created; nothing else in `root` is touched.
/// A step that ran at least `io_timeout_secs`
/// fails the pass with [`ArchiveError::Overran`] before the commit; the files stay, and the
/// next pass verifies and commits them.
pub(crate) fn archive_segment(
    flight: &Flight,
    store: &Store,
    root: &Path,
    store_id: &str,
    segment: &PreparedSegment,
) -> Result<PathBuf, ArchiveError> {
    plain_component(store_id)?;
    flight.start_pass();
    let root_is_dir = {
        let _step = flight.step("find archive root", root);
        root.is_dir()
    };
    if !root_is_dir {
        return Err(ArchiveError::Io {
            step: "find archive root",
            path: root.to_path_buf(),
            source: io::Error::new(io::ErrorKind::NotFound, "not a directory; is it mounted?"),
        });
    }
    require_mounted(flight, root)?;
    ensure_store_id(flight, root, store_id)?;
    let store_dir = root.join(store_id);
    let missing = {
        let _step = flight.step("find store directory", &store_dir);
        !store_dir.exists()
    };
    if missing {
        {
            let _step = flight.step("create", &store_dir);
            fs::create_dir(&store_dir).map_err(io_at("create", &store_dir))?;
        }
        sync_dir(flight, root)?;
    }
    write_segment(flight, &store_dir, segment)?;
    verify_segment(flight, &store_dir, segment)?;
    if flight.overran() {
        return Err(ArchiveError::Overran);
    }
    store
        .archive_commit(&segment.segment_id)
        .map_err(ArchiveError::Store)?;
    Ok(store_dir)
}

/// One archive pass over the backlog: segments until none is due, then a look at whether a
/// held record stands at the head. Returns how many segments committed.
fn drain(
    flight: &Flight,
    store: &Store,
    root: &Path,
    store_id: &str,
    now: u64,
) -> Result<u64, ArchiveError> {
    let mut committed = 0;
    while let Some(segment) = store.archive_prepare(now).map_err(ArchiveError::Store)? {
        if let Some(short) = segment.shortened {
            let halvings_total = store.archive_halvings();
            eprintln!(
                "sparrowd: archive prepare shortened selected={} kept={} first_code={} halvings_total={halvings_total}",
                short.selected, short.kept, short.first_code
            );
            tracing::warn!(
                selected = short.selected,
                kept = short.kept,
                first_code = short.first_code,
                halvings_total,
                "archive prepare returned fewer entries than it selected"
            );
        }
        let dir = archive_segment(flight, store, root, store_id, &segment)?;
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
    _watch_stop: mpsc::Sender<()>,
}

impl Worker {
    /// Start the thread and its watchdog. `every` is how long it waits after a pass finds
    /// nothing due, or fails, before looking again; the store itself decides what is due at
    /// its 75 per cent start. `stall_warn` and `io_timeout` are the `[archive]` settings of
    /// that name; see [`Flight`] for what each does.
    pub fn start(
        store: Store,
        root: PathBuf,
        every: Duration,
        stall_warn: Duration,
        io_timeout: Duration,
    ) -> Result<Self, io::Error> {
        let flight = Arc::new(Flight::new(stall_warn, io_timeout));
        let (stop, stopped) = mpsc::channel::<()>();
        let (watch_stop, watch_stopped) = mpsc::channel::<()>();
        let watched = Arc::clone(&flight);
        thread::Builder::new()
            .name("sparrow-archive-watch".into())
            .spawn(move || watch(&watched, &watch_stopped))?;
        thread::Builder::new()
            .name("sparrow-archive".into())
            .spawn(move || run(&flight, &store, &root, every, &stopped))?;
        Ok(Self {
            _stop: stop,
            _watch_stop: watch_stop,
        })
    }
}

fn run(flight: &Flight, store: &Store, root: &Path, every: Duration, stopped: &mpsc::Receiver<()>) {
    loop {
        let outcome = match (store.store_id(), crate::now()) {
            (Ok(id), Ok(now)) => {
                drain(flight, store, root, &id, now).map_err(|error| error.to_string())
            }
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
    use std::os::unix::fs::MetadataExt;

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

    /// An archive root that already holds `id` as its `STORE_ID`, as an earlier pass left it.
    fn nas(dir: &Path, id: &str) -> PathBuf {
        let root = dir.join("nas");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("STORE_ID"), format!("{id}\n")).unwrap();
        root
    }

    fn flight() -> Flight {
        // `/proc` is a device of its own, so a test directory is never on the "root" device.
        Flight::new(Duration::from_secs(120), Duration::from_secs(900)).rooted_at("/proc")
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
        let error = archive_segment(&flight(), &store, &missing, "s", &segment).unwrap_err();
        assert!(matches!(error, ArchiveError::Io { .. }), "{error}");
        assert_eq!(through(&store), None);
        assert!(!missing.exists());
    }

    /// INVARIANT: a root that is not this store's is refused, nothing is written into it and
    /// the watermark stays: another store's `STORE_ID` is never overwritten, and a directory
    /// that holds segments but no `STORE_ID` is not empty and is not adopted.
    #[test]
    fn the_watermark_stays_when_the_root_is_not_this_stores() {
        let dir = tempfile::tempdir().unwrap();
        let (store, segment) = archiving_store(&dir.path().join("store"));
        let other = nas(dir.path(), "another-store");
        let error = archive_segment(&flight(), &store, &other, "s", &segment).unwrap_err();
        assert!(matches!(error, ArchiveError::Root { .. }), "{error}");
        assert!(!other.join("s").exists());
        assert_eq!(
            fs::read_to_string(other.join("STORE_ID")).unwrap(),
            "another-store\n",
            "the other store's id is not overwritten"
        );
        assert_eq!(
            fs::read_dir(&other).unwrap().count(),
            1,
            "the other store's root holds only its STORE_ID"
        );
        let stranger = dir.path().join("stranger");
        fs::create_dir_all(stranger.join("s")).unwrap();
        fs::write(stranger.join("s").join("0-1.gts"), b"segment").unwrap();
        let error = archive_segment(&flight(), &store, &stranger, "s", &segment).unwrap_err();
        assert!(matches!(error, ArchiveError::Root { .. }), "{error}");
        assert!(error.to_string().contains("not adopted"), "{error}");
        assert!(!stranger.join("STORE_ID").exists(), "never adopted");
        assert_eq!(through(&store), None);
    }

    /// INVARIANT: the first pass over an empty archive directory on its mount writes this
    /// store's `STORE_ID` itself (`<id>` and a newline, no partial left behind), the segment
    /// follows, and a second pass accepts the file it wrote without rewriting it.
    #[test]
    fn the_first_pass_writes_store_id_into_an_empty_directory_and_the_second_accepts_it() {
        let dir = tempfile::tempdir().unwrap();
        let (store, segment) = archiving_store(&dir.path().join("store"));
        let root = dir.path().join("nas");
        fs::create_dir(&root).unwrap();
        archive_segment(&flight(), &store, &root, "s", &segment).unwrap();
        assert_eq!(fs::read_to_string(root.join("STORE_ID")).unwrap(), "s\n");
        assert!(!root.join("STORE_ID.partial").exists());
        assert_eq!(through(&store).as_deref(), Some(segment.through.as_str()));
        let written = fs::metadata(root.join("STORE_ID")).unwrap().ino();
        store.ingest("r2", "source", "log", b"c\nd\n", 2).unwrap();
        let second = store
            .archive_prepare(LATER)
            .unwrap()
            .expect("a second segment");
        archive_segment(&flight(), &store, &root, "s", &second).unwrap();
        assert_eq!(
            fs::metadata(root.join("STORE_ID")).unwrap().ino(),
            written,
            "an existing matching STORE_ID is accepted, not rewritten"
        );
    }

    /// INVARIANT: a `STORE_ID.partial` left by an interrupted first pass does not make the
    /// directory non-empty: the next pass writes `STORE_ID` and no partial remains.
    #[test]
    fn a_leftover_store_id_partial_does_not_block_the_first_write() {
        let dir = tempfile::tempdir().unwrap();
        let (store, segment) = archiving_store(&dir.path().join("store"));
        let root = dir.path().join("nas");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("STORE_ID.partial"), b"half").unwrap();
        archive_segment(&flight(), &store, &root, "s", &segment).unwrap();
        assert_eq!(fs::read_to_string(root.join("STORE_ID")).unwrap(), "s\n");
        assert!(!root.join("STORE_ID.partial").exists());
    }

    /// INVARIANT: the mount rules are pure over device and filesystem type: the same device
    /// is refused whatever the type; a different device is accepted on a network filesystem
    /// and on a local type unlike the root's, and refused on the root's own local type (an
    /// unmounted share under another local subvolume), with the type named in the message.
    #[test]
    fn the_mount_rules_refuse_the_roots_device_and_its_local_filesystem_type() {
        const NFS: u64 = 0x6969;
        const BTRFS: u64 = 0x9123_683E;
        const TMPFS: u64 = 0x0102_1994;
        let placement = |device, f_type| Placement { device, f_type };
        let root = placement(1, BTRFS);
        let table = [
            (placement(1, BTRFS), false),
            (placement(1, NFS), false),
            (placement(2, NFS), true),
            (placement(2, 0xFF53_4D42), true),
            (placement(2, TMPFS), true),
            (placement(2, BTRFS), false),
        ];
        for (found, accepted) in table {
            let verdict = require_own_mount(Path::new("/a"), found, Path::new("/"), root);
            assert_eq!(
                verdict.is_ok(),
                accepted,
                "device {} type {:#x}: {verdict:?}",
                found.device,
                found.f_type
            );
        }
        let error = require_own_mount(Path::new("/a"), placement(2, BTRFS), Path::new("/"), root)
            .unwrap_err();
        assert!(matches!(error, ArchiveError::NotOnMount { .. }), "{error}");
        assert!(error.to_string().contains("0x9123683e"), "{error}");
        assert_eq!(network_filesystem(NFS), Some("nfs"));
        assert_eq!(network_filesystem(BTRFS), None);
    }

    /// INVARIANT: an archive directory on the root filesystem's device (an unmounted share
    /// leaves its mount point as a local directory) is refused with the named `NotOnMount`
    /// error, nothing is written into it and the watermark stays; a directory on another device
    /// is accepted. A test directory stands for the root filesystem, so no mount is needed.
    #[test]
    fn an_archive_directory_on_the_root_device_is_refused_before_anything_is_written() {
        let dir = tempfile::tempdir().unwrap();
        let (store, segment) = archiving_store(&dir.path().join("store"));
        let root = nas(dir.path(), "s");
        let on_root =
            Flight::new(Duration::from_secs(120), Duration::from_secs(900)).rooted_at(dir.path());
        let error = archive_segment(&on_root, &store, &root, "s", &segment).unwrap_err();
        assert!(matches!(error, ArchiveError::NotOnMount { .. }), "{error}");
        assert!(
            error.to_string().contains("not on its expected mount"),
            "{error}"
        );
        assert_eq!(fs::read_dir(&root).unwrap().count(), 1, "only its STORE_ID");
        assert_eq!(through(&store), None);
        archive_segment(&flight(), &store, &root, "s", &segment).unwrap();
    }

    /// INVARIANT: a step in flight is reported once it passes `stall_warn_secs`, then at most
    /// once per `stall_warn_secs`; a step that ran to `io_timeout_secs` marks the pass overrun,
    /// so it is not committed, and a new pass starts clean. The clock is injected.
    #[test]
    fn a_stalled_step_is_reported_at_a_bounded_rate_and_an_overrun_blocks_the_commit() {
        let flight = Flight::new(Duration::from_secs(10), Duration::from_secs(30));
        let t0 = Instant::now();
        let at = |secs| t0 + Duration::from_secs(secs);
        assert_eq!(flight.observe(at(100)), None, "nothing in flight");
        flight.begin_at(t0, "read back", Path::new("/nas/s/a.gts"));
        assert_eq!(flight.observe(at(9)), None);
        let first = flight.observe(at(10)).expect("due at the warn age");
        assert_eq!(first.step, "read back");
        assert_eq!(first.path, Path::new("/nas/s/a.gts"));
        assert_eq!(first.elapsed, Duration::from_secs(10));
        assert!(!first.timed_out);
        assert_eq!(flight.observe(at(19)), None, "bounded rate");
        assert!(flight.observe(at(20)).is_some());
        assert!(!flight.overran());
        assert!(flight.observe(at(30)).expect("due").timed_out);
        assert!(flight.overran());
        flight.end_at(at(31));
        flight.start_pass();
        assert!(!flight.overran());
        flight.begin_at(at(40), "sync", Path::new("/nas/s"));
        flight.end_at(at(70));
        assert!(flight.overran(), "a step that returns late still overran");
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
        let error = archive_segment(&flight(), &store, &root, "s", &segment).unwrap_err();
        assert!(matches!(error, ArchiveError::Verify { .. }), "{error}");
        assert_eq!(through(&store), None);
    }

    /// INVARIANT (#352): a segment whose record bytes do not hash to the digest their link
    /// names is refused before the commit, so the watermark stays and the originals stay
    /// unretired. The container is internally consistent (only the per-record check can
    /// tell), and the same segment with matching bytes decodes.
    #[test]
    fn a_record_whose_bytes_differ_from_its_link_digest_is_refused_before_the_commit() {
        let dir = tempfile::tempdir().unwrap();
        let (store, mut segment) = archiving_store(&dir.path().join("store"));
        let root = nas(dir.path(), "s");
        decode::verify(&[(
            segment.files[0].0.clone(),
            sparrow_test_support::forged_segment(b"alpha\n", b"alpha\n"),
        )])
        .unwrap();
        let genuine = segment.files[0].1.clone();
        segment.files[0].1 = sparrow_test_support::forged_segment(b"alpha\n", b"bravo\n");
        let error = archive_segment(&flight(), &store, &root, "s", &segment).unwrap_err();
        assert!(
            matches!(&error, ArchiveError::Verify { reason, .. } if reason.contains("does not decode: record record:bb")),
            "{error}"
        );
        assert_eq!(through(&store), None);

        // Recovery: the refused file stays on disk for inspection (nothing is deleted), the
        // original is unretired, and once the operator clears the refused file the worker
        // writes the genuine segment and commits it.
        let refused = root.join("s").join(&segment.files[0].0);
        assert!(refused.exists(), "the refused file is kept");
        fs::remove_file(&refused).unwrap();
        segment.files[0].1 = genuine.clone();
        archive_segment(&flight(), &store, &root, "s", &segment).unwrap();
        assert_eq!(through(&store).as_deref(), Some(segment.through.as_str()));
        assert_eq!(fs::read(&refused).unwrap(), genuine);
    }

    /// INVARIANT (operators depend on it): a pass that finds files an earlier pass left, whether
    /// it crashed after the rename and before the commit, died with a stale `.partial`, or
    /// overran its I/O budget before the commit, recovers by itself. It keeps an identical
    /// final file (neither rewrites nor refuses it), replaces a stale partial, verifies,
    /// commits and moves the watermark exactly once; the next identical pass does nothing.
    #[test]
    fn a_pass_after_a_crash_between_the_rename_and_the_commit_recovers_by_itself() {
        use std::os::unix::fs::PermissionsExt;
        let overrun = || Flight::new(Duration::from_secs(120), Duration::ZERO).rooted_at("/proc");
        // (case, leftover the earlier pass left in the store directory)
        for case in ["final file", "stale partial", "overran before the commit"] {
            let dir = tempfile::tempdir().unwrap();
            let (store, segment) = archiving_store(&dir.path().join("store"));
            let root = nas(dir.path(), "s");
            let store_dir = root.join("s");
            let (name, bytes) = segment.files[0].clone();
            let target = store_dir.join(&name);
            fs::create_dir(&store_dir).unwrap();
            match case {
                "final file" => {
                    // The state after a crash after the rename: exact bytes at the final name.
                    // Read-only, so a worker that rewrote it (a rename of a fresh 0600 file)
                    // would change its mode, and one that refused it would fail the pass.
                    fs::write(&target, &bytes).unwrap();
                    fs::set_permissions(&target, fs::Permissions::from_mode(0o444)).unwrap();
                }
                "stale partial" => {
                    fs::write(store_dir.join(format!("{name}.partial")), b"half a seg").unwrap();
                }
                _ => {
                    let error = drain(&overrun(), &store, &root, "s", LATER).unwrap_err();
                    assert!(matches!(error, ArchiveError::Overran), "{case}: {error}");
                    assert_eq!(through(&store), None, "{case}: an overrun never commits");
                    fs::set_permissions(&target, fs::Permissions::from_mode(0o444)).unwrap();
                }
            }
            let before = fs::metadata(&target)
                .ok()
                .map(|m| (m.ino(), m.mtime_nsec(), m.mode()));

            let committed = drain(&flight(), &store, &root, "s", LATER).unwrap();
            assert_eq!(committed, 1, "{case}: one commit");
            assert_eq!(
                through(&store).as_deref(),
                Some(segment.through.as_str()),
                "{case}"
            );
            assert_eq!(
                fs::read(&target).unwrap(),
                bytes,
                "{case}: the segment's bytes"
            );
            let after = fs::metadata(&target).unwrap();
            if before.is_some() {
                assert_eq!(
                    before,
                    Some((after.ino(), after.mtime_nsec(), after.mode())),
                    "{case}: an identical file is kept, not rewritten"
                );
            }
            let listing = || {
                let mut names: Vec<_> = fs::read_dir(&store_dir)
                    .unwrap()
                    .map(|e| e.unwrap().file_name())
                    .collect();
                names.sort();
                names
            };
            assert_eq!(
                listing(),
                [std::ffi::OsString::from(&name)],
                "{case}: no partial left"
            );

            let settled = (through(&store), listing(), after.ino(), after.mtime_nsec());
            assert_eq!(
                drain(&flight(), &store, &root, "s", LATER).unwrap(),
                0,
                "{case}: nothing due"
            );
            let again = fs::metadata(&target).unwrap();
            assert_eq!(
                (through(&store), listing(), again.ino(), again.mtime_nsec()),
                settled,
                "{case}: the second pass changes nothing"
            );
        }
    }

    /// INVARIANT: a segment written, synced, read back and decoded advances the watermark to
    /// its last position, leaves no partial file, and the file on disk decodes.
    #[test]
    fn a_verified_segment_advances_the_watermark_and_its_file_decodes() {
        let dir = tempfile::tempdir().unwrap();
        let (store, segment) = archiving_store(&dir.path().join("store"));
        let root = nas(dir.path(), "s");
        let written = archive_segment(&flight(), &store, &root, "s", &segment).unwrap();
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
