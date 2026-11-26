// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The archive worker: moves aged-out evidence onto the archive root and marks it archived.
//!
//! The store worker prepares segments and commits their watermark. The process's archive
//! filesystem worker receives one owned segment at a time, writes it, and verifies it byte
//! for byte and through the reader's decoder. It holds no store handle. The store worker can
//! abandon the result while a mount call is blocked and can join independently of that call.

mod decode;
mod status;

pub use status::ArchiveStatus;
use status::Phase;

use sparrow_core::{ArchiveHold, HoldReason, PreparedSegment, Store};
use std::{
    fmt, fs,
    io::{self, Write},
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    sync::mpsc,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    thread,
    time::{Duration, Instant},
};

#[cfg(test)]
type ReadbackHook = dyn Fn(&Path) + Send + Sync;

/// A test point run with no state of the worker's own. It may panic to stand for a fault.
#[cfg(test)]
type FaultHook = dyn Fn() + Send + Sync;

#[derive(Clone)]
struct Timing {
    warn: Duration,
    timeout: Duration,
    filesystem_root: PathBuf,
    #[cfg(test)]
    before_readback: Option<Arc<ReadbackHook>>,
    /// Run at the top of each coordinator loop turn, outside any pass.
    #[cfg(test)]
    loop_fault: Option<Arc<FaultHook>>,
    /// Run in the filesystem thread after a job's result is taken, outside its job guard.
    #[cfg(test)]
    job_fault: Option<Arc<FaultHook>>,
}

impl Timing {
    fn new(warn: Duration, timeout: Duration) -> Self {
        Self {
            warn,
            timeout,
            filesystem_root: PathBuf::from("/"),
            #[cfg(test)]
            before_readback: None,
            #[cfg(test)]
            loop_fault: None,
            #[cfg(test)]
            job_fault: None,
        }
    }

    /// A directory standing in for the local root filesystem, so the test needs no mount.
    #[cfg(test)]
    fn rooted_at(mut self, root: impl Into<PathBuf>) -> Self {
        self.filesystem_root = root.into();
        self
    }
}

/// Each filesystem job publishes its current step here. The store worker owns the clock,
/// journals stalls, and abandons the job when the current step reaches its deadline.
struct Flight {
    timing: Timing,
    waiter: thread::Thread,
    state: Mutex<FlightState>,
}

#[derive(Default)]
struct FlightState {
    current: Option<Current>,
    abandoned: Option<Abandoned>,
    segment: String,
    verifying: bool,
}

struct Current {
    step: &'static str,
    path: PathBuf,
    since: Instant,
    reported: Option<Instant>,
}

/// What the store worker journals for a step that has run too long.
#[derive(Debug, PartialEq, Eq)]
struct Stall {
    step: &'static str,
    path: PathBuf,
    elapsed: Duration,
    timed_out: bool,
}

#[derive(Clone, Debug)]
struct Abandoned {
    step: &'static str,
    path: PathBuf,
    segment: String,
    cause: WaitEnd,
}

#[derive(Clone, Copy, Debug)]
enum WaitEnd {
    Deadline,
    Shutdown,
    Occupied,
}

impl Flight {
    fn new(timing: Timing) -> Self {
        Self {
            timing,
            waiter: thread::current(),
            state: Mutex::new(FlightState::default()),
        }
    }

    fn state(&self) -> MutexGuard<'_, FlightState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn begin_at(&self, now: Instant, step: &'static str, path: &Path) -> Result<(), ArchiveError> {
        let mut state = self.state();
        if let Some(abandoned) = &state.abandoned {
            return Err(ArchiveError::Overran(abandoned.clone()));
        }
        state.current = Some(Current {
            step,
            path: path.to_path_buf(),
            since: now,
            reported: None,
        });
        self.waiter.unpark();
        Ok(())
    }

    fn end_at(&self, now: Instant) {
        let mut state = self.state();
        if let Some(current) = state.current.take()
            && now.saturating_duration_since(current.since) >= self.timing.timeout
            && state.abandoned.is_none()
        {
            state.abandoned = Some(Abandoned {
                step: current.step,
                path: current.path,
                segment: state.segment.clone(),
                cause: WaitEnd::Deadline,
            });
        }
        self.waiter.unpark();
    }

    /// Mark a step in flight until the guard drops.
    fn step<'a>(&'a self, step: &'static str, path: &Path) -> Result<StepGuard<'a>, ArchiveError> {
        self.begin_at(Instant::now(), step, path)?;
        Ok(StepGuard(self))
    }

    /// The report due for the step in flight at `now`: none before `warn`, then one each
    /// `warn` while the step lasts, so the journal grows at a bounded rate.
    fn observe(&self, now: Instant) -> Option<Stall> {
        let mut state = self.state();
        let current = state.current.as_mut()?;
        let elapsed = now.saturating_duration_since(current.since);
        let timed_out = elapsed >= self.timing.timeout;
        let due = timed_out
            || (elapsed >= self.timing.warn
                && current
                    .reported
                    .is_none_or(|at| now.saturating_duration_since(at) >= self.timing.warn));
        if !due {
            return None;
        }
        current.reported = Some(now);
        let report = Stall {
            step: current.step,
            path: current.path.clone(),
            elapsed,
            timed_out,
        };
        if timed_out && state.abandoned.is_none() {
            state.abandoned = Some(Abandoned {
                step: report.step,
                path: report.path.clone(),
                segment: state.segment.clone(),
                cause: WaitEnd::Deadline,
            });
        }
        Some(report)
    }

    /// The next warning or abandonment deadline for the current step.
    fn due(&self) -> Option<Instant> {
        let state = self.state();
        let current = state.current.as_ref()?;
        [
            current
                .reported
                .unwrap_or(current.since)
                .checked_add(self.timing.warn),
            current.since.checked_add(self.timing.timeout),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn abandoned(&self) -> Option<Abandoned> {
        self.state().abandoned.clone()
    }

    fn abandon(&self, root: &Path) -> ArchiveError {
        let mut state = self.state();
        let stopped = Self::location(&state, root, WaitEnd::Shutdown);
        ArchiveError::Overran(state.abandoned.get_or_insert(stopped).clone())
    }

    fn location(state: &FlightState, root: &Path, cause: WaitEnd) -> Abandoned {
        let (step, path) = if let Some(current) = &state.current {
            (current.step, current.path.clone())
        } else if let Some(abandoned) = &state.abandoned {
            (abandoned.step, abandoned.path.clone())
        } else {
            ("prepare or verify segment", root.to_path_buf())
        };
        Abandoned {
            step,
            path,
            segment: state.segment.clone(),
            cause,
        }
    }

    fn verifying(&self) {
        self.state().verifying = true;
        self.waiter.unpark();
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
enum ArchiveError {
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
    /// The coordinator abandoned this filesystem result, or an earlier job still owns the slot.
    Overran(Abandoned),
    /// Filesystem work panicked. The process-owned worker still takes the next job.
    Panicked(String),
    /// The store refused a step.
    Store(sparrow_core::Error),
}

impl ArchiveError {
    /// The error class `status` reports; a store refusal keeps the store's own code.
    fn code(&self) -> &'static str {
        match self {
            Self::Io { .. } => "ARCHIVE_IO",
            Self::Name(_) => "INVALID_ARGUMENT",
            Self::Verify { .. } => "ARCHIVE_VERIFY",
            Self::NotOnMount { .. } => "ARCHIVE_NOT_ON_MOUNT",
            Self::Root { .. } => "ARCHIVE_ROOT",
            Self::Overran(_) => "ARCHIVE_OVERRAN",
            Self::Panicked(_) => "ARCHIVE_PANIC",
            Self::Store(error) => error.code,
        }
    }
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
            Self::Overran(at) => {
                let cause = match at.cause {
                    WaitEnd::Deadline => "archive step reached io_timeout_secs",
                    WaitEnd::Shutdown => "archive stop abandoned filesystem work",
                    WaitEnd::Occupied => "earlier archive filesystem work still owns the slot",
                };
                write!(
                    f,
                    "{cause} at {} {}; the segment remains uncommitted; retry after the archive filesystem responds",
                    at.step,
                    at.path.display(),
                )
            }
            Self::Panicked(cause) => write!(f, "pass panicked: {cause}"),
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
    let _step = flight.step("sync directory", path)?;
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
        let _step = flight.step("write segment file", &target)?;
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
            let _step = flight.step("read back", &path)?;
            #[cfg(test)]
            if let Some(hook) = &flight.timing.before_readback {
                hook(&path);
            }
            fs::read(&path).map_err(io_at("read back", &path))?
        };
        if read != *expected {
            let reason = format!(
                "{} bytes read, {} written, or contents differ; if an earlier pass left \
                 a valid file under another identity, move {} aside",
                read.len(),
                expected.len(),
                path.display()
            );
            return Err(ArchiveError::Verify { path, reason });
        }
    }
    decode::verify(&segment.files).map_err(|reason| {
        // The bytes were written as prepared and the decoder refuses them. Move each file
        // aside to `<name>.<segment id hex>.refused` and keep it for inspection, so a rebuild
        // of the same range under another identity can take the name. The identity covers the
        // bytes, so refused files of different bytes never collide; the same bytes refused
        // again replace one identical copy. The watermark has not moved, nothing is deleted,
        // and the decoder's reason survives any failure to set the file aside.
        let reason = match set_aside(flight, store_dir, segment) {
            Ok(()) => reason,
            Err(failure) => format!("{reason}; {failure}"),
        };
        ArchiveError::Verify {
            path: store_dir.to_path_buf(),
            reason,
        }
    })
}

/// Rename each refused file to `<name>.<segment id hex>.refused`, then sync the directory.
fn set_aside(flight: &Flight, store_dir: &Path, segment: &PreparedSegment) -> Result<(), String> {
    for (name, _) in &segment.files {
        let path = store_dir.join(name);
        let aside = store_dir.join(format!("{name}.{}.refused", segment.id_hex()));
        let _step = flight
            .step("set a refused segment aside", &path)
            .map_err(|error| error.to_string())?;
        fs::rename(&path, &aside).map_err(|error| {
            format!(
                "setting {} aside as {} failed: {error}",
                path.display(),
                aside.display()
            )
        })?;
    }
    sync_dir(flight, store_dir).map_err(|error| error.to_string())
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
        let _step = flight.step("read device", path)?;
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
        &flight.timing.filesystem_root,
        place(&flight.timing.filesystem_root)?,
    )
}

/// Whether the root holds nothing an archive owns: no entry at all, apart from the
/// `STORE_ID.partial` an earlier first pass may have left before its rename.
fn is_empty_root(flight: &Flight, root: &Path) -> Result<bool, ArchiveError> {
    let _step = flight.step("list archive root", root)?;
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
        let _step = flight.step("read STORE_ID", &path)?;
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
                let _step = flight.step("write STORE_ID", &path)?;
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

/// Make one segment durable under `<root>/<store_id>/` and verify its exact bytes.
///
/// `root` must already exist and be on its own mount (see [`require_own_mount`]): a missing or
/// unmounted root is a failure, never a directory created on the local disk. It must hold this
/// store's `STORE_ID`, which an empty root is given here (see [`ensure_store_id`]). Only
/// `<root>/<store_id>` and `<root>/STORE_ID` are created; nothing else in `root` is touched.
/// The filesystem job owns no store and therefore cannot advance the watermark.
fn write_and_verify(
    flight: &Flight,
    root: &Path,
    store_id: &str,
    segment: &PreparedSegment,
) -> Result<PathBuf, ArchiveError> {
    plain_component(store_id)?;
    let root_is_dir = {
        let _step = flight.step("find archive root", root)?;
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
        let _step = flight.step("find store directory", &store_dir)?;
        !store_dir.exists()
    };
    if missing {
        let _step = flight.step("create", &store_dir)?;
        fs::create_dir(&store_dir).map_err(io_at("create", &store_dir))?;
    }
    // An earlier job may have published STORE_ID or created the child directory without
    // completing its root sync. Confirm both parent links even when reusing their names.
    sync_dir(flight, root)?;
    write_segment(flight, &store_dir, segment)?;
    flight.verifying();
    verify_segment(flight, &store_dir, segment)?;
    Ok(store_dir)
}

/// Publish the store's watermark and whether an intent is in flight.
fn publish_position(status: &ArchiveStatus, store: &Store) -> Result<(), ArchiveError> {
    let state = store.archive_state().map_err(ArchiveError::Store)?;
    let (through, intent) = state.map_or((None, false), |state| {
        (state.through, state.intent.is_some())
    });
    status.position(through, intent);
    Ok(())
}

/// The segment's `.gts` file name, which status and the journal name a failure by.
fn segment_file_name(segment: &PreparedSegment) -> String {
    segment
        .files
        .first()
        .map_or_else(|| segment.name(), |(name, _)| name.clone())
}

/// A process-owned archive filesystem worker. Keep it across store sessions. There is one
/// reservation for preparation and filesystem work together, so a replacement session
/// cannot queue a segment while an abandoned filesystem call still owns the slot.
#[derive(Clone)]
pub struct Filesystem {
    slot: Arc<Slot>,
    jobs: Arc<Mutex<mpsc::SyncSender<FilesystemJob>>>,
}

#[derive(Default)]
struct Slot {
    flight: Mutex<Option<Arc<Flight>>>,
    #[cfg(test)]
    changed: std::sync::Condvar,
}

struct Reservation {
    slot: Arc<Slot>,
    flight: Arc<Flight>,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        *self
            .slot
            .flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
        #[cfg(test)]
        self.slot.changed.notify_all();
    }
}

type JobResult = Result<(PreparedSegment, PathBuf), ArchiveError>;

struct FilesystemJob {
    reservation: Reservation,
    root: PathBuf,
    store_id: String,
    segment: PreparedSegment,
    reply: mpsc::Sender<JobResult>,
}

/// Start one filesystem thread with a queue of one job, and return the queue's sender.
fn spawn_filesystem_thread() -> io::Result<mpsc::SyncSender<FilesystemJob>> {
    let (jobs, incoming) = mpsc::sync_channel::<FilesystemJob>(1);
    thread::Builder::new()
        .name("sparrow-archive-io".into())
        .spawn(move || {
            while let Ok(job) = incoming.recv() {
                job.run();
            }
        })?;
    Ok(jobs)
}

impl FilesystemJob {
    fn run(self) {
        let flight = Arc::clone(&self.reservation.flight);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            write_and_verify(&flight, &self.root, &self.store_id, &self.segment)
        }))
        .unwrap_or_else(|panic| Err(ArchiveError::Panicked(panic_message(panic.as_ref()))))
        .map(|directory| (self.segment, directory));
        #[cfg(test)]
        if let Some(fault) = &flight.timing.job_fault {
            fault();
        }
        // The slot is released only after the native work has returned, even if the receiver
        // has already gone. No queued job can touch the old job's temporary names.
        drop(self.reservation);
        // A deadline or shutdown deliberately drops this job's unique reply receiver.
        drop(self.reply.send(result));
        flight.waiter.unpark();
    }
}

impl Filesystem {
    /// Start the single filesystem worker. The process never joins it because a mount
    /// syscall can remain blocked after an archive or storage session stops.
    pub fn start() -> io::Result<Self> {
        Ok(Self {
            slot: Arc::new(Slot::default()),
            jobs: Arc::new(Mutex::new(spawn_filesystem_thread()?)),
        })
    }

    /// Hand `job` to the filesystem thread. When that thread has ended outside its job guard,
    /// start a replacement with the same one-job queue and give it the job, so the next pass
    /// archives again. The single slot still bounds the work outstanding at any time.
    fn submit(&self, job: FilesystemJob) -> Result<(), String> {
        let mut jobs = self.jobs.lock().unwrap_or_else(PoisonError::into_inner);
        let job = match jobs.try_send(job) {
            Ok(()) => return Ok(()),
            Err(mpsc::TrySendError::Full(_)) => {
                return Err(
                    "the process-owned archive worker cannot accept its reservation".into(),
                );
            }
            Err(mpsc::TrySendError::Disconnected(job)) => job,
        };
        *jobs = spawn_filesystem_thread()
            .map_err(|error| format!("the archive filesystem worker could not restart: {error}"))?;
        jobs.try_send(job).map_err(|_| {
            "the replacement archive filesystem worker cannot accept its reservation".to_owned()
        })
    }

    fn reserve(&self, timing: &Timing, root: &Path) -> Result<Reservation, ArchiveError> {
        let mut active = self
            .slot
            .flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(flight) = &*active {
            return Err(ArchiveError::Overran(Flight::location(
                &flight.state(),
                root,
                WaitEnd::Occupied,
            )));
        }
        let flight = Arc::new(Flight::new(timing.clone()));
        *active = Some(Arc::clone(&flight));
        Ok(Reservation {
            slot: Arc::clone(&self.slot),
            flight,
        })
    }
}

struct Coordinator {
    filesystem: Filesystem,
    root: PathBuf,
    timing: Timing,
    status: ArchiveStatus,
    stop: Arc<AtomicBool>,
}

impl Coordinator {
    fn stopped(&self) -> bool {
        self.stop.load(Ordering::Acquire)
    }

    /// The point at the top of each loop turn, outside any pass. Production has no fault here.
    fn loop_turn(&self) {
        #[cfg(test)]
        if let Some(fault) = &self.timing.loop_fault {
            fault();
        }
    }

    fn write_and_wait(
        &self,
        reservation: Reservation,
        store_id: &str,
        segment: PreparedSegment,
    ) -> JobResult {
        let flight = Arc::clone(&reservation.flight);
        flight.state().segment = segment_file_name(&segment);
        if self.stopped() {
            return Err(flight.abandon(&self.root));
        }
        let (reply, received) = mpsc::channel();
        let unavailable = |error: &str| {
            io_at("run archive filesystem job", &self.root)(io::Error::other(error.to_owned()))
        };
        self.filesystem
            .submit(FilesystemJob {
                reservation,
                root: self.root.clone(),
                store_id: store_id.to_owned(),
                segment,
                reply,
            })
            .map_err(|error| unavailable(&error))?;
        self.status.phase(Phase::Writing);

        loop {
            if self.stopped() {
                return Err(flight.abandon(&self.root));
            }
            if let Some(stall) = flight.observe(Instant::now()) {
                journal_stall(&stall);
            }
            if let Some(abandoned) = flight.abandoned() {
                return Err(ArchiveError::Overran(abandoned));
            }
            if flight.state().verifying {
                self.status.phase(Phase::Verifying);
            }
            match received.try_recv() {
                Ok(result) => {
                    // A completed reply can race a step ending after its deadline. All
                    // step guards have dropped by the send, so check the recorded deadline
                    // again before allowing the coordinator to commit.
                    return match flight.abandoned() {
                        Some(abandoned) => Err(ArchiveError::Overran(abandoned)),
                        None => result,
                    };
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err(unavailable("the archive filesystem worker lost its reply"));
                }
                Err(mpsc::TryRecvError::Empty) => {}
            }
            // Starting or ending a step, completing the reply and requesting shutdown all
            // unpark this thread. An unpark before this call leaves a token for the wait.
            match flight.due() {
                Some(at) => thread::park_timeout(at.saturating_duration_since(Instant::now())),
                None => thread::park(),
            }
        }
    }

    fn archive(
        &self,
        store: &Store,
        reservation: Reservation,
        store_id: &str,
        segment: PreparedSegment,
    ) -> JobResult {
        let flight = Arc::clone(&reservation.flight);
        let completed = self.write_and_wait(reservation, store_id, segment)?;
        if self.stopped() {
            return Err(flight.abandon(&self.root));
        }
        // A shutdown racing this authorized commit joins the coordinator before another
        // session can open. Only this coordinator ever receives and commits this reply.
        store
            .archive_commit(&completed.0.segment_id)
            .map_err(ArchiveError::Store)?;
        Ok(completed)
    }

    /// One archive pass over the backlog: segments until none is due, then a look at whether a
    /// held record stands at the head. Returns how many segments committed. The pass publishes
    /// its phase and position to status as steps end; a failure becomes the standing failure
    /// until a pass ends without one.
    fn drain(&self, store: &Store, store_id: &str, now: u64) -> Result<u64, ArchiveError> {
        let mut segment = String::new();
        let outcome = self.drain_segments(store, store_id, now, &mut segment);
        match &outcome {
            Ok(_) if !self.stopped() => self.status.pass_succeeded(),
            Ok(_) => {}
            Err(error) => {
                // The pass failure is the one reported; a position that cannot be read keeps the
                // last published one and is journaled.
                if !self.stopped()
                    && let Err(secondary) = publish_position(&self.status, store)
                {
                    eprintln!(
                        "sparrowd: archive status could not read the watermark after a failed pass: {secondary}"
                    );
                    tracing::warn!(%secondary, "archive status could not read the watermark");
                }
                if let ArchiveError::Panicked(cause) = error {
                    note_panic(&self.status, cause);
                }
                if let ArchiveError::Overran(at) = error
                    && !at.segment.is_empty()
                {
                    segment.clone_from(&at.segment);
                }
                self.status
                    .pass_failed(error.code(), &segment, &error.to_string(), Some(now));
                #[cfg(test)]
                {
                    let _active = self.filesystem.slot.flight.lock().unwrap();
                    self.filesystem.slot.changed.notify_all();
                }
            }
        }
        outcome
    }

    fn drain_segments(
        &self,
        store: &Store,
        store_id: &str,
        now: u64,
        failed_segment: &mut String,
    ) -> Result<u64, ArchiveError> {
        let mut committed = 0;
        while !self.stopped() {
            let reservation = self.filesystem.reserve(&self.timing, &self.root)?;
            self.status.phase(Phase::Preparing);
            let Some(segment) = store.archive_prepare(now).map_err(ArchiveError::Store)? else {
                break;
            };
            if self.stopped() {
                return Ok(committed);
            }
            *failed_segment = segment_file_name(&segment);
            publish_position(&self.status, store)?;
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
            let (segment, dir) = self.archive(store, reservation, store_id, segment)?;
            failed_segment.clear();
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
            if self.stopped() {
                return Ok(committed);
            }
            publish_position(&self.status, store)?;
        }
        if self.stopped() {
            return Ok(committed);
        }
        publish_position(&self.status, store)?;
        if !self.stopped()
            && let Some(held) = store.archive_held().map_err(ArchiveError::Store)?
        {
            journal_held(&held);
        }
        Ok(committed)
    }
}

/// Handle to the store-owning archive coordinator. Shutdown joins this coordinator without
/// waiting for an outstanding filesystem call.
pub struct Worker {
    stop: Arc<AtomicBool>,
    coordinator: Option<thread::JoinHandle<()>>,
}

impl Worker {
    /// Start the coordinator for this store. `every` is the interval between passes.
    /// `stall_warn` sets the warning cadence and `io_timeout` bounds how long the
    /// coordinator waits for a filesystem step.
    pub fn start(
        filesystem: &Filesystem,
        store: Store,
        root: PathBuf,
        every: Duration,
        stall_warn: Duration,
        io_timeout: Duration,
        status: ArchiveStatus,
    ) -> Result<Self, io::Error> {
        Self::start_configured(
            filesystem,
            store,
            root,
            every,
            Timing::new(stall_warn, io_timeout),
            status,
        )
    }

    fn start_configured(
        filesystem: &Filesystem,
        store: Store,
        root: PathBuf,
        every: Duration,
        timing: Timing,
        status: ArchiveStatus,
    ) -> io::Result<Self> {
        publish_position(&status, &store).map_err(io::Error::other)?;
        if status.snapshot().failure.is_none() {
            status.phase(Phase::Idle);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let owner = Coordinator {
            filesystem: filesystem.clone(),
            root,
            timing,
            status,
            stop: Arc::clone(&stop),
        };
        let coordinator = thread::Builder::new()
            .name("sparrow-archive".into())
            .spawn(move || {
                supervise(&owner.status, every, &owner.stop, || {
                    run(
                        &owner.status,
                        every,
                        &owner.stop,
                        || pass(&owner, &store),
                        || owner.loop_turn(),
                    );
                });
            })?;
        Ok(Self {
            stop,
            coordinator: Some(coordinator),
        })
    }

    /// Stop the coordinator and wait for any store operation it already admitted.
    pub async fn shutdown(mut self) -> io::Result<()> {
        self.request_stop();
        let coordinator = self
            .coordinator
            .take()
            .ok_or_else(|| io::Error::other("archive coordinator join owner missing"))?;
        tokio::task::spawn_blocking(move || {
            coordinator
                .join()
                .map_err(|panic| io::Error::other(panic_message(panic.as_ref())))
        })
        .await
        .map_err(io::Error::other)?
    }

    fn request_stop(&self) {
        self.stop.store(true, Ordering::Release);
        if let Some(coordinator) = &self.coordinator {
            coordinator.thread().unpark();
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.request_stop();
    }
}

/// Record a failure that came before `drain` and return its cause for the journal.
fn fail_pass(status: &ArchiveStatus, code: &'static str, cause: String, at: Option<u64>) -> String {
    status.pass_failed(code, "", &cause, at);
    cause
}

/// `cause`, with the clock's own error appended when the clock failed in the same pass.
fn with_clock_error(cause: String, clock: &Result<u64, sparrow_core::Error>) -> String {
    match clock {
        Ok(_) => cause,
        Err(clock) => format!("{cause}; clock: {clock}"),
    }
}

/// One pass: publish the store's position, read the store id and the clock, and drain. A
/// failure before `drain` is recorded in status here; `drain` records its own.
fn pass(owner: &Coordinator, store: &Store) -> Result<u64, String> {
    if owner.stopped() {
        return Ok(0);
    }
    let clock = crate::now();
    let at = clock.as_ref().ok().copied();
    if let Err(error) = publish_position(&owner.status, store) {
        let cause = with_clock_error(error.to_string(), &clock);
        return Err(fail_pass(&owner.status, error.code(), cause, at));
    }
    let id = match store.store_id() {
        Ok(id) => id,
        Err(error) => {
            let cause = with_clock_error(
                format!("store id: {} {}", error.code, error.message),
                &clock,
            );
            return Err(fail_pass(&owner.status, error.code, cause, at));
        }
    };
    let now = match clock {
        Ok(now) => now,
        Err(error) => {
            let cause = format!("clock: {error}");
            return Err(fail_pass(&owner.status, error.code, cause, None));
        }
    };
    owner
        .drain(store, &id, now)
        .map_err(|error| error.to_string())
}

/// Run `pass` until stopped, `every` apart. `turn` runs at the top of each turn, outside
/// `guarded_pass`, so a panic it raises escapes this function to [`supervise`].
fn run(
    status: &ArchiveStatus,
    every: Duration,
    stop: &AtomicBool,
    mut pass: impl FnMut() -> Result<u64, String>,
    turn: impl Fn(),
) {
    while !stop.load(Ordering::Acquire) {
        turn();
        let outcome = guarded_pass(status, &mut pass);
        if let Err(cause) = outcome {
            eprintln!("sparrowd: archive pass failed; the watermark did not move: {cause}");
            tracing::error!(%cause, "archive pass failed; the watermark did not move");
        }
        park_until(Instant::now(), every, stop);
    }
}

/// Wait until `every` after `started`, or until stopped.
fn park_until(started: Instant, every: Duration, stop: &AtomicBool) {
    while !stop.load(Ordering::Acquire) {
        let remaining = every.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            break;
        }
        thread::park_timeout(remaining);
    }
}

/// Run the coordinator loop until it is stopped. A panic that escapes the loop, which only
/// happens outside a pass, is recorded as `ARCHIVE_PANIC`, counted as a restart, and the loop
/// is entered again after `every`. Every pass starts from the store's durable watermark and
/// intent, so the restart needs no recovery of its own.
fn supervise(status: &ArchiveStatus, every: Duration, stop: &AtomicBool, mut body: impl FnMut()) {
    while !stop.load(Ordering::Acquire) {
        let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(&mut body)) else {
            return;
        };
        let cause = panic_message(panic.as_ref());
        note_panic(status, &cause);
        fail_pass(
            status,
            "ARCHIVE_PANIC",
            format!("archive loop panicked: {cause}"),
            crate::now().ok(),
        );
        park_until(Instant::now(), every, stop);
    }
}

/// Run one pass, restarting the worker from a clean flight when the pass panics. A panic in a
/// pass would otherwise end the archive thread, and nothing would archive again while the
/// daemon ran. The panic hook has already journaled the panic itself; this adds the restart
/// line, records the failure as `ARCHIVE_PANIC` in status and counts the restart there. The
/// steps of the cut-short pass left the flight as their guards dropped, and every pass starts
/// again from the store's durable watermark and intent, so a pass cut short by a panic is
/// redone the way a pass cut short by a crash is.
fn guarded_pass(
    status: &ArchiveStatus,
    pass: impl FnOnce() -> Result<u64, String>,
) -> Result<u64, String> {
    let panic = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(pass)) {
        Ok(outcome) => return outcome,
        Err(panic) => panic,
    };
    let cause = panic_message(panic.as_ref());
    note_panic(status, &cause);
    Err(fail_pass(
        status,
        "ARCHIVE_PANIC",
        format!("pass panicked: {cause}"),
        crate::now().ok(),
    ))
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|text| (*text).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a pass panicked with a non-text payload".to_owned())
}

fn note_panic(status: &ArchiveStatus, cause: &str) {
    let restarts = status.restarted();
    eprintln!(
        "sparrowd: archive pass panicked; restarting the archive worker restarts={restarts}: {cause}"
    );
    tracing::error!(%cause, restarts, "archive pass panicked; restarting the archive worker");
}

#[cfg(test)]
mod tests {
    use super::*;
    use sparrow_core::Config;
    use std::os::unix::fs::MetadataExt;

    /// INVARIANT (#447): a pass that panics no longer ends the archive thread unnoticed. The
    /// worker keeps running and runs the next pass; status reports the panic as the standing
    /// `ARCHIVE_PANIC` failure with its message and counts the restart.
    #[test]
    fn a_panicking_pass_is_recorded_and_the_worker_runs_the_next_pass() {
        let status = ArchiveStatus::new();
        let stop = AtomicBool::new(false);
        let mut passes = 0;
        let mut seen = None;
        run(
            &status,
            Duration::ZERO,
            &stop,
            || {
                passes += 1;
                if passes == 1 {
                    panic!("an injected archive panic");
                }
                seen = Some(status.snapshot());
                stop.store(true, Ordering::Release);
                Ok(0)
            },
            || {},
        );
        assert_eq!(passes, 2);
        let snapshot = seen.unwrap();
        assert_eq!(snapshot.restarts, 1);
        assert_eq!(snapshot.phase, Phase::Holding);
        let failure = snapshot.failure.unwrap();
        assert_eq!(failure.code, "ARCHIVE_PANIC");
        assert!(
            failure.reason.contains("an injected archive panic"),
            "{failure:?}"
        );
    }

    /// Wait up to `limit` for `done`, checking every few milliseconds. Used only to observe
    /// another thread's status in these tests.
    fn wait_for(limit: Duration, mut done: impl FnMut() -> bool) -> bool {
        let end = Instant::now() + limit;
        while !done() {
            if Instant::now() >= end {
                return false;
            }
            thread::sleep(Duration::from_millis(5));
        }
        true
    }

    /// INVARIANT (#447): a coordinator loop that exits outside its guarded pass is restarted by
    /// the supervisor, which records `ARCHIVE_PANIC` and counts the restart. The restarted loop
    /// archives from the durable watermark: the segment committed before the fault is not
    /// rewritten, and the next segment is written and advances the watermark.
    #[test]
    fn a_coordinator_that_exits_outside_its_pass_restarts_and_archives_from_the_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let (store, first) = archiving_store(&data);
        let root = nas(dir.path(), &store.store_id().unwrap());
        let first_file =
            archive_segment(&timing(), &store, &root, &store.store_id().unwrap(), &first).unwrap();
        let (first_name, _) = &first.files[0];
        let first_bytes = fs::read(first_file.join(first_name)).unwrap();
        store.ingest("r2", "source", "log", b"c\nd\n", 2).unwrap();
        let second = store
            .archive_prepare(LATER)
            .unwrap()
            .expect("the second artifact is due");
        assert_ne!(second.through, first.through);

        let mut faulted = timing();
        let fault = Arc::new(AtomicBool::new(true));
        let armed = Arc::clone(&fault);
        faulted.loop_fault = Some(Arc::new(move || {
            if armed.swap(false, Ordering::AcqRel) {
                panic!("an injected coordinator exit outside its pass");
            }
        }));
        let status = ArchiveStatus::new();
        let worker = Worker::start_configured(
            &Filesystem::start().unwrap(),
            store.clone(),
            root.clone(),
            Duration::from_millis(10),
            faulted,
            status.clone(),
        )
        .unwrap();
        assert!(
            wait_for(Duration::from_secs(30), || {
                through(&store).as_deref() == Some(second.through.as_str())
            }),
            "the restarted coordinator did not commit the next segment"
        );
        let snapshot = status.snapshot();
        assert_eq!(snapshot.restarts, 1);
        assert!(snapshot.failure.is_none(), "{:?}", snapshot.failure);
        assert_eq!(fs::read(first_file.join(first_name)).unwrap(), first_bytes);
        let (name, _) = &second.files[0];
        decode::verify(&[(
            name.clone(),
            fs::read(root.join(store.store_id().unwrap()).join(name)).unwrap(),
        )])
        .unwrap();
        drop(worker);
    }

    /// INVARIANT (#447): a filesystem thread that exits outside its job guard is replaced when
    /// the next job arrives. The pass that lost its job reports the failure, the worker does not
    /// count a restart, and the replacement archives the segment and advances the watermark.
    #[test]
    fn a_filesystem_thread_that_exits_outside_its_job_is_replaced_and_the_next_pass_archives() {
        let dir = tempfile::tempdir().unwrap();
        let (store, segment) = archiving_store(&dir.path().join("store"));
        let store_id = store.store_id().unwrap();
        let root = nas(dir.path(), &store_id);
        let mut faulted = timing();
        let fault = Arc::new(AtomicBool::new(true));
        let armed = Arc::clone(&fault);
        faulted.job_fault = Some(Arc::new(move || {
            if armed.swap(false, Ordering::AcqRel) {
                panic!("an injected filesystem thread exit outside its job");
            }
        }));
        let status = ArchiveStatus::new();
        let filesystem = Filesystem::start().unwrap();
        let worker = Worker::start_configured(
            &filesystem,
            store.clone(),
            root.clone(),
            Duration::from_millis(10),
            faulted,
            status.clone(),
        )
        .unwrap();
        assert!(
            wait_for(Duration::from_secs(30), || status
                .snapshot()
                .failure
                .is_some()),
            "the lost job was never reported"
        );
        let lost = status.snapshot().failure.unwrap();
        assert!(
            lost.reason.contains("archive filesystem worker"),
            "{lost:?}"
        );
        assert!(
            wait_for(Duration::from_secs(30), || {
                through(&store).as_deref() == Some(segment.through.as_str())
            }),
            "the replacement filesystem thread did not archive the segment"
        );
        let snapshot = status.snapshot();
        assert_eq!(snapshot.restarts, 0);
        assert!(snapshot.failure.is_none(), "{:?}", snapshot.failure);
        let (name, _) = &segment.files[0];
        decode::verify(&[(
            name.clone(),
            fs::read(root.join(&store_id).join(name)).unwrap(),
        )])
        .unwrap();
        drop(worker);
    }

    /// A time after every window and horizon, so the store reports its backlog due.
    const LATER: u64 = 4_000_000_000;

    fn archiving_config() -> Config {
        Config {
            archive: Some(sparrow_core::ArchivePolicy::default()),
            ..sparrow_test_support::storage_config!(Config)
        }
    }

    /// A store that archives, holding one ingested artifact, and its prepared segment.
    fn archiving_store(dir: &Path) -> (Store, PreparedSegment) {
        let store = Store::open(dir, archiving_config()).unwrap();
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

    fn timing() -> Timing {
        // `/proc` is a device of its own, so a test directory is never on the "root" device.
        Timing::new(Duration::from_secs(120), Duration::from_secs(900)).rooted_at("/proc")
    }

    fn coordinator(
        filesystem: Filesystem,
        timing: Timing,
        root: &Path,
        status: ArchiveStatus,
    ) -> Coordinator {
        Coordinator {
            filesystem,
            root: root.to_path_buf(),
            timing,
            status,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    fn archive_segment(
        timing: &Timing,
        store: &Store,
        root: &Path,
        store_id: &str,
        segment: &PreparedSegment,
    ) -> Result<PathBuf, ArchiveError> {
        let owner = coordinator(
            Filesystem::start().unwrap(),
            timing.clone(),
            root,
            ArchiveStatus::new(),
        );
        let reservation = owner.filesystem.reserve(timing, root)?;
        owner
            .archive(store, reservation, store_id, segment.clone())
            .map(|(_, directory)| directory)
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
        let error = archive_segment(&timing(), &store, &missing, "s", &segment).unwrap_err();
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
        let error = archive_segment(&timing(), &store, &other, "s", &segment).unwrap_err();
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
        let error = archive_segment(&timing(), &store, &stranger, "s", &segment).unwrap_err();
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
        archive_segment(&timing(), &store, &root, "s", &segment).unwrap();
        assert_eq!(fs::read_to_string(root.join("STORE_ID")).unwrap(), "s\n");
        assert!(!root.join("STORE_ID.partial").exists());
        assert_eq!(through(&store).as_deref(), Some(segment.through.as_str()));
        let written = fs::metadata(root.join("STORE_ID")).unwrap().ino();
        store.ingest("r2", "source", "log", b"c\nd\n", 2).unwrap();
        let second = store
            .archive_prepare(LATER)
            .unwrap()
            .expect("a second segment");
        archive_segment(&timing(), &store, &root, "s", &second).unwrap();
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
        archive_segment(&timing(), &store, &root, "s", &segment).unwrap();
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
        let on_root = timing().rooted_at(dir.path());
        let error = archive_segment(&on_root, &store, &root, "s", &segment).unwrap_err();
        assert!(matches!(error, ArchiveError::NotOnMount { .. }), "{error}");
        assert!(
            error.to_string().contains("not on its expected mount"),
            "{error}"
        );
        assert_eq!(fs::read_dir(&root).unwrap().count(), 1, "only its STORE_ID");
        assert_eq!(through(&store), None);
        archive_segment(&timing(), &store, &root, "s", &segment).unwrap();
    }

    /// INVARIANT: a step in flight is reported once it passes `stall_warn_secs`, then at most
    /// once per `stall_warn_secs`; a step that ran to `io_timeout_secs` marks the pass overrun,
    /// so it is not committed, and a new pass starts clean. The clock is injected.
    #[test]
    fn a_stalled_step_is_reported_at_a_bounded_rate_and_an_overrun_blocks_the_commit() {
        let timing = Timing::new(Duration::from_secs(10), Duration::from_secs(30));
        let flight = Flight::new(timing.clone());
        let t0 = Instant::now();
        let at = |secs| t0 + Duration::from_secs(secs);
        assert_eq!(flight.observe(at(100)), None, "nothing in flight");
        flight
            .begin_at(t0, "read back", Path::new("/nas/s/a.gts"))
            .unwrap();
        assert_eq!(flight.observe(at(9)), None);
        let first = flight.observe(at(10)).expect("due at the warn age");
        assert_eq!(first.step, "read back");
        assert_eq!(first.path, Path::new("/nas/s/a.gts"));
        assert_eq!(first.elapsed, Duration::from_secs(10));
        assert!(!first.timed_out);
        assert_eq!(flight.observe(at(19)), None, "bounded rate");
        assert!(flight.observe(at(20)).is_some());
        assert!(flight.abandoned().is_none());
        assert!(flight.observe(at(30)).expect("due").timed_out);
        assert!(flight.abandoned().is_some());
        flight.end_at(at(31));
        let flight = Flight::new(timing);
        assert!(flight.abandoned().is_none());
        flight
            .begin_at(at(40), "sync", Path::new("/nas/s"))
            .unwrap();
        flight.end_at(at(70));
        assert!(
            flight.abandoned().is_some(),
            "a step that returns late still overran"
        );
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
        let error = archive_segment(&timing(), &store, &root, "s", &segment).unwrap_err();
        assert!(matches!(error, ArchiveError::Verify { .. }), "{error}");
        assert_eq!(through(&store), None);
    }

    /// INVARIANT (#352): a segment whose record bytes do not hash to the digest their link
    /// names is refused before the commit, so the watermark stays and the originals stay
    /// unretired. The container is internally consistent (only the per-record check can
    /// tell). The refused file is set aside, and a rebuild of the same range under another
    /// identity replaces the intent and commits. The fixture's mismatch surfaces as a record
    /// with no blob under its link's digest, not as a hash comparison.
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
        let error = archive_segment(&timing(), &store, &root, "s", &segment).unwrap_err();
        assert!(
            matches!(&error, ArchiveError::Verify { reason, .. } if reason.contains("does not decode: record record:bb")),
            "{error}"
        );
        assert_eq!(through(&store), None);

        // Recovery: the refused bytes are set aside for inspection (nothing is deleted), the
        // intent stays, and the originals stay unretired. The encoder then yields good bytes,
        // so the rebuilt segment has another identity over the same range: the store replaces
        // the intent and the worker writes the rebuilt segment and commits it.
        let target = root.join("s").join(&segment.files[0].0);
        let aside_of = |segment: &PreparedSegment| {
            root.join("s").join(format!(
                "{}.{}.refused",
                segment.files[0].0,
                segment.id_hex()
            ))
        };
        let aside = aside_of(&segment);
        assert!(!target.exists(), "the name is free for the rebuild");
        assert_eq!(
            fs::read(&aside).unwrap(),
            sparrow_test_support::forged_segment(b"alpha\n", b"bravo\n")
        );
        assert_eq!(
            store
                .archive_state()
                .unwrap()
                .unwrap()
                .intent
                .map(|i| i.segment_id),
            Some(segment.segment_id)
        );
        // A second refusal with different bytes keeps the first refused file.
        let mut second = segment.clone();
        second.segment_id = [6; 32];
        second.files[0].1 = sparrow_test_support::forged_segment(b"alpha\n", b"charlie\n");
        archive_segment(&timing(), &store, &root, "s", &second).unwrap_err();
        assert_eq!(
            fs::read(&aside).unwrap(),
            sparrow_test_support::forged_segment(b"alpha\n", b"bravo\n"),
            "the earlier refused file is kept"
        );
        assert_eq!(
            fs::read(aside_of(&second)).unwrap(),
            sparrow_test_support::forged_segment(b"alpha\n", b"charlie\n")
        );
        let rebuilt = PreparedSegment {
            segment_id: [7; 32],
            files: vec![(segment.files[0].0.clone(), genuine.clone())],
            ..segment.clone()
        };
        store
            .archive_intend(&sparrow_core::ArchiveIntent {
                segment_id: rebuilt.segment_id,
                first: rebuilt.from.clone(),
                last: rebuilt.through.clone(),
                records: rebuilt.records,
                artifacts: rebuilt.artifacts,
            })
            .unwrap();
        assert_eq!(through(&store), None, "replacing the intent moves nothing");
        archive_segment(&timing(), &store, &root, "s", &rebuilt).unwrap();
        assert_eq!(through(&store).as_deref(), Some(segment.through.as_str()));
        assert_eq!(fs::read(&target).unwrap(), genuine);
    }

    /// INVARIANT (operators depend on it): a pass that finds files an earlier pass left, whether
    /// it crashed after the rename and before the commit or died with a stale `.partial`,
    /// recovers by itself. It keeps an identical
    /// final file (neither rewrites nor refuses it), replaces a stale partial, verifies,
    /// commits and moves the watermark exactly once; the next identical pass does nothing.
    #[test]
    fn a_pass_after_a_crash_between_the_rename_and_the_commit_recovers_by_itself() {
        use std::os::unix::fs::PermissionsExt;
        for case in ["final file", "stale partial"] {
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
                _ => unreachable!(),
            }
            let before = fs::metadata(&target)
                .ok()
                .map(|m| (m.ino(), m.mtime_nsec(), m.mode()));

            let owner = coordinator(
                Filesystem::start().unwrap(),
                timing(),
                &root,
                ArchiveStatus::new(),
            );
            let committed = owner.drain(&store, "s", LATER).unwrap();
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
                owner.drain(&store, "s", LATER).unwrap(),
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

    /// INVARIANT (#365): status shows a held worker and why, and shows it recovered. A forged
    /// file under the segment's name is refused by the worker's read-back; the published state
    /// is `holding` with the failure's class, segment file name, reason and start, and the
    /// watermark behind with the intent in flight. After the operator moves the file aside, the
    /// next pass commits: the failure is gone, the worker is idle and the watermark advanced.
    /// Times come from the pass's `now` argument.
    #[test]
    fn status_shows_a_refused_segment_as_holding_and_clears_when_a_pass_commits() {
        let dir = tempfile::tempdir().unwrap();
        let (store, segment) = archiving_store(&dir.path().join("store"));
        let root = nas(dir.path(), "s");
        let (name, _) = segment.files[0].clone();
        let forged = root.join("s").join(&name);
        fs::create_dir(root.join("s")).unwrap();
        fs::write(
            &forged,
            sparrow_test_support::forged_segment(b"alpha\n", b"bravo\n"),
        )
        .unwrap();

        let status = ArchiveStatus::new();
        assert_eq!(status.snapshot().phase, Phase::NotConfigured);
        let owner = coordinator(
            Filesystem::start().unwrap(),
            timing(),
            &root,
            status.clone(),
        );
        owner.drain(&store, "s", LATER).unwrap_err();
        owner.drain(&store, "s", LATER + 60_000).unwrap_err();

        let held = status.snapshot();
        assert_eq!(held.phase, Phase::Holding);
        assert_eq!(held.through, None);
        assert!(
            held.intent_in_flight,
            "the prepared segment is not committed"
        );
        let failure = held.failure.expect("the refusal is the standing failure");
        assert_eq!(failure.code, "ARCHIVE_VERIFY");
        assert_eq!(failure.segment, name);
        assert!(
            failure.reason.contains("contents differ"),
            "{}",
            failure.reason
        );
        assert_eq!(
            failure.since,
            Some(LATER),
            "a repeated failure keeps its start"
        );

        fs::rename(&forged, root.join("s").join(format!("{name}.moved"))).unwrap();
        assert_eq!(owner.drain(&store, "s", LATER + 120_000).unwrap(), 1);
        let recovered = status.snapshot();
        assert_eq!(recovered.phase, Phase::Idle);
        assert_eq!(recovered.failure, None);
        assert_eq!(recovered.through.as_deref(), Some(segment.through.as_str()));
        assert!(!recovered.intent_in_flight);
    }

    /// INVARIANT (#365): status is right after a restart, before any pass reaches `drain`. A
    /// worker started on a store that already holds a committed watermark and an uncommitted
    /// intent reports both at once; the archive root is missing, so no pass can get past
    /// its first step.
    #[tokio::test]
    async fn a_started_worker_reports_the_stores_watermark_and_intent() {
        let dir = tempfile::tempdir().unwrap();
        let (store, first) = archiving_store(&dir.path().join("store"));
        let root = nas(dir.path(), "s");
        archive_segment(&timing(), &store, &root, "s", &first).unwrap();
        store.ingest("r2", "source", "log", b"c\nd\n", 2).unwrap();
        store
            .archive_prepare(LATER)
            .unwrap()
            .expect("a second segment");

        let status = ArchiveStatus::new();
        let filesystem = Filesystem::start().unwrap();
        let worker = Worker::start(
            &filesystem,
            store.clone(),
            dir.path().join("not-mounted"),
            Duration::from_secs(3600),
            Duration::from_secs(120),
            Duration::from_secs(900),
            status.clone(),
        )
        .unwrap();
        let started = status.snapshot();
        assert_eq!(started.through.as_deref(), Some(first.through.as_str()));
        assert!(started.intent_in_flight);
        worker.shutdown().await.unwrap();
    }

    /// The filesystem deadline records a hold while read-back is still blocked. Shutdown
    /// releases the store, and only the next verified pass can commit the retained intent.
    #[tokio::test]
    async fn blocked_archive_readback_releases_the_store_and_fences_its_late_reply() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let (store, prepared) = archiving_store(&data);
        let store_id = store.store_id().unwrap();
        let root = nas(dir.path(), &store_id);
        let filesystem = Filesystem::start().unwrap();
        let (entered_tx, entered) = tokio::sync::oneshot::channel();
        let (release, blocked) = mpsc::channel();
        let barrier = Mutex::new(Some((entered_tx, blocked)));
        let mut blocked_timing = timing();
        blocked_timing.warn = Duration::from_secs(5);
        blocked_timing.timeout = Duration::from_secs(30);
        blocked_timing.before_readback = Some(Arc::new(move |path| {
            let next = barrier.lock().unwrap().take();
            if let Some((entered, blocked)) = next {
                entered.send(path.to_path_buf()).unwrap();
                blocked.recv_timeout(Duration::from_secs(30)).unwrap();
            }
        }));
        let status = ArchiveStatus::new();
        let worker = Worker::start_configured(
            &filesystem,
            store,
            root.clone(),
            Duration::from_secs(60),
            blocked_timing,
            status.clone(),
        )
        .unwrap();
        let path = tokio::time::timeout(Duration::from_secs(10), entered)
            .await
            .expect("filesystem job did not reach read-back")
            .unwrap();
        {
            let active = filesystem.slot.flight.lock().unwrap();
            let flight = active.as_ref().unwrap();
            // Advance just the protected step. The coordinator's own waiter applies the
            // deadline while the filesystem remains at the read-back barrier.
            flight.state().current.as_mut().unwrap().since = Instant::now() - flight.timing.timeout;
            flight.waiter.unpark();
            let (active, expired) = filesystem
                .slot
                .changed
                .wait_timeout_while(active, Duration::from_secs(5), |_| {
                    status.snapshot().phase != Phase::Holding
                })
                .unwrap();
            assert!(
                !expired.timed_out(),
                "archive deadline never entered holding"
            );
            assert!(matches!(
                active.as_ref().unwrap().abandoned().unwrap().cause,
                WaitEnd::Deadline
            ));
        }
        let abandoned = status.snapshot();
        assert_eq!(abandoned.phase, Phase::Holding);
        assert!(abandoned.intent_in_flight && abandoned.through.is_none());
        let failure = abandoned.failure.as_ref().unwrap();
        assert_eq!(failure.code, "ARCHIVE_OVERRAN");
        assert!(
            failure.reason.contains("io_timeout_secs")
                && failure.reason.contains("read back")
                && failure.reason.contains(path.to_string_lossy().as_ref())
        );
        tokio::time::timeout(Duration::from_secs(5), worker.shutdown())
            .await
            .expect("archive shutdown waited for the filesystem")
            .unwrap();
        assert_eq!(status.snapshot(), abandoned);

        // The old coordinator is joined, while the read-back still holds the only slot.
        let reopened = Store::open(&data, archiving_config()).expect("old store retains LOCK");
        let owner = coordinator(filesystem.clone(), timing(), &root, status.clone());
        let busy = owner.drain(&reopened, &store_id, LATER).unwrap_err();
        assert!(matches!(
            busy,
            ArchiveError::Overran(Abandoned {
                cause: WaitEnd::Occupied,
                ..
            })
        ));
        let held = status.snapshot();
        assert_eq!(held.phase, Phase::Holding);
        let busy_failure = held.failure.as_ref().unwrap();
        assert_eq!(busy_failure.segment, failure.segment);
        assert_eq!(busy_failure.since, failure.since);
        assert_eq!(through(&reopened), None);
        let stopped = coordinator(filesystem.clone(), timing(), &root, status.clone());
        stopped.stop.store(true, Ordering::Release);
        assert_eq!(stopped.drain(&reopened, &store_id, LATER).unwrap(), 0);
        assert_eq!(status.snapshot(), held);

        release.send(()).unwrap();
        let active = filesystem.slot.flight.lock().unwrap();
        let (active, _) = filesystem
            .slot
            .changed
            .wait_timeout_while(active, Duration::from_secs(5), |flight| flight.is_some())
            .unwrap();
        assert!(active.is_none(), "abandoned filesystem job did not settle");
        drop(active);
        assert_eq!(status.snapshot(), held);
        assert_eq!(through(&reopened), None);
        assert!(reopened.archive_state().unwrap().unwrap().intent.is_some());
        assert_eq!(owner.drain(&reopened, &store_id, LATER).unwrap(), 1);
        assert_eq!(
            through(&reopened).as_deref(),
            Some(prepared.through.as_str())
        );
        assert_eq!(owner.drain(&reopened, &store_id, LATER).unwrap(), 0);
        assert_eq!(status.snapshot().phase, Phase::Idle);
    }

    /// INVARIANT: a segment written, synced, read back and decoded advances the watermark to
    /// its last position, leaves no partial file, and the file on disk decodes.
    #[test]
    fn a_verified_segment_advances_the_watermark_and_its_file_decodes() {
        let dir = tempfile::tempdir().unwrap();
        let (store, segment) = archiving_store(&dir.path().join("store"));
        let root = nas(dir.path(), "s");
        let written = archive_segment(&timing(), &store, &root, "s", &segment).unwrap();
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
