// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Online backups of the live store with RocksDB's `BackupEngine` (#156).
//!
//! A backup never stops, pauses or drains the daemon, and never takes the writer lock: it
//! clones the database handle the unlocked readers already share and copies on one dedicated
//! thread at the lowest CPU priority. RocksDB disables obsolete-file deletion for the copy,
//! lists the live table, manifest and WAL files (writing the WAL buffer to the operating
//! system first, without a sync), and copies them; nothing is flushed, so the memtable and
//! ingest are untouched. Table files are shared between backups, so every backup after the
//! first copies only the tables written since. The engine then checks the backup's file sizes
//! and purges the oldest beyond the configured count. The files RocksDB holds for the copy do
//! not count against the disk budget, and a guard releases them before free space nears the
//! floor ([`start`]).
//!
//! The backup root holds `STORE_ID`, naming the one store whose backups it keeps, and
//! `engine/`, the engine's directory. A root that names another store is refused.

use super::Error;
use super::space::SpaceFloor;
use rocksdb::DB;
use rocksdb::backup::{BackupEngine, BackupEngineOptions, RestoreOptions};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

/// The filesystems a production backup root may be on: network filesystems only. Sparrow never
/// backs up to a local disk (Patrick, 2026-11-08).
pub const NETWORK_FILESYSTEMS: [&str; 4] = ["nfs", "nfs4", "cifs", "smb3"];
/// The engine's directory below the backup root.
pub(crate) const ENGINE_DIR: &str = "engine";
/// The file below the backup root that names the store whose backups the root keeps.
pub(crate) const STORE_ID_FILE: &str = "STORE_ID";
/// The lowest CPU priority (the highest nice value). The kernel derives the lowest best-effort
/// I/O priority from it, which the BFQ scheduler honors.
const LOWEST_PRIORITY: i32 = 19;

/// Where online backups go and how many the engine keeps. Built only through [`Self::new`],
/// which refuses a root that is not on a network filesystem or shares the store's filesystem.
#[derive(Clone, Debug)]
pub struct BackupTarget {
    root: PathBuf,
    keep: u32,
    data_dir: PathBuf,
    /// Production targets recheck the canonical root's mount before every backup.
    network: bool,
}

impl BackupTarget {
    /// A backup root for the store in `data_dir`, keeping `keep` backups. Only the paths are
    /// checked here: whether the root is on a network filesystem apart from the store's is
    /// judged at every backup, before anything is created, so a daemon starting before the NAS
    /// is mounted still starts and serves, and only its backups are refused until the mount
    /// returns. Nothing on the network filesystem is touched here.
    pub fn new(root: PathBuf, keep: u32, data_dir: &Path) -> Result<Self, Error> {
        Self::lexical(root, keep, data_dir)
    }

    /// A target on any filesystem, for tests that cannot mount a network filesystem.
    #[cfg(feature = "test-support")]
    pub fn local_for_test(root: PathBuf, keep: u32, data_dir: &Path) -> Result<Self, Error> {
        let mut target = Self::lexical(root, keep, data_dir)?;
        target.network = false;
        Ok(target)
    }

    fn lexical(root: PathBuf, keep: u32, data_dir: &Path) -> Result<Self, Error> {
        let normal = |path: &Path| {
            path.is_absolute()
                && path
                    .components()
                    .all(|part| !matches!(part, Component::ParentDir | Component::CurDir))
        };
        if !normal(&root) || !normal(data_dir) {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                format!(
                    "backup_root {} and data_dir {} must be absolute paths without '.' or '..'",
                    root.display(),
                    data_dir.display()
                ),
            ));
        }
        if root.starts_with(data_dir) || data_dir.starts_with(&root) {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                format!(
                    "backup_root {} must be outside data_dir {} and must not contain it",
                    root.display(),
                    data_dir.display()
                ),
            ));
        }
        if keep == 0 {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "backup_keep must keep at least one backup",
            ));
        }
        Ok(Self {
            root,
            keep,
            data_dir: data_dir.to_path_buf(),
            network: true,
        })
    }

    /// The backup root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// How many backups the engine keeps.
    #[must_use]
    pub fn keep(&self) -> u32 {
        self.keep
    }
}

/// One mounted filesystem from `/proc/self/mountinfo`.
#[derive(Debug)]
struct Mount {
    device: String,
    point: PathBuf,
    fstype: String,
}

fn read_mounts() -> Result<Vec<Mount>, Error> {
    let text = std::fs::read_to_string("/proc/self/mountinfo").map_err(|error| {
        Error::new(
            "STORAGE_FAILURE",
            format!(
                "cannot read /proc/self/mountinfo to find the backup root's filesystem: {error}"
            ),
        )
    })?;
    text.lines().map(parse_mount).collect()
}

/// `ID PARENT MAJOR:MINOR ROOT POINT OPTIONS [OPTIONAL...] - FSTYPE SOURCE SUPER`, with
/// spaces, tabs, newlines and backslashes in the mount point escaped as octal.
fn parse_mount(line: &str) -> Result<Mount, Error> {
    let malformed = || {
        Error::new(
            "STORAGE_FAILURE",
            format!("malformed /proc/self/mountinfo line: {line}"),
        )
    };
    let mut fields = line.split(' ');
    let device = fields.nth(2).ok_or_else(malformed)?;
    let point = fields.nth(1).ok_or_else(malformed)?;
    let fstype = fields
        .skip_while(|field| *field != "-")
        .nth(1)
        .ok_or_else(malformed)?;
    let mut bytes = Vec::with_capacity(point.len());
    let raw = point.as_bytes();
    let mut index = 0;
    while index < raw.len() {
        let octal = raw.get(index + 1..index + 4).filter(|digits| {
            raw[index] == b'\\' && digits.iter().all(|digit| (b'0'..=b'7').contains(digit))
        });
        match octal {
            Some(digits) => {
                let value = digits
                    .iter()
                    .fold(0_u32, |value, digit| value * 8 + u32::from(digit - b'0'));
                bytes.push(u8::try_from(value).map_err(|_| malformed())?);
                index += 4;
            }
            None => {
                bytes.push(raw[index]);
                index += 1;
            }
        }
    }
    Ok(Mount {
        device: device.to_owned(),
        point: PathBuf::from(
            <std::ffi::OsString as std::os::unix::ffi::OsStringExt>::from_vec(bytes),
        ),
        fstype: fstype.to_owned(),
    })
}

/// The filesystem holding `path`: the longest mount point it lies under, the later of two
/// mounts on one point (the one that shadows the other).
fn mount_of<'a>(path: &Path, mounts: &'a [Mount]) -> Result<&'a Mount, Error> {
    mounts
        .iter()
        .enumerate()
        .filter(|(_, mount)| path.starts_with(&mount.point))
        .max_by_key(|(order, mount)| (mount.point.components().count(), *order))
        .map(|(_, mount)| mount)
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                format!("no mounted filesystem holds {}", path.display()),
            )
        })
}

/// Refuse a root that is not on a network filesystem, or whose filesystem (by device number,
/// which bind mounts share) is the store's.
fn check_mounts(root: &Path, data_dir: &Path, mounts: &[Mount]) -> Result<(), Error> {
    let backup = mount_of(root, mounts)?;
    if !NETWORK_FILESYSTEMS.contains(&backup.fstype.as_str()) {
        return Err(Error::new(
            "INVALID_ARGUMENT",
            format!(
                "backup_root {} is on {} (mounted at {}), not a network filesystem ({}); Sparrow never backs up to a local disk. Mount the NAS archive and name a directory on it",
                root.display(),
                backup.fstype,
                backup.point.display(),
                NETWORK_FILESYSTEMS.join(", ")
            ),
        ));
    }
    let store = mount_of(data_dir, mounts)?;
    if store.device == backup.device {
        return Err(Error::new(
            "INVALID_ARGUMENT",
            format!(
                "backup_root {} shares the store's filesystem {} ({} at {})",
                root.display(),
                data_dir.display(),
                store.fstype,
                store.point.display()
            ),
        ));
    }
    Ok(())
}

/// Where the latest backup request stands.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BackupPhase {
    /// No backup has run since the daemon started.
    #[default]
    Idle,
    /// A backup is copying.
    Running,
    /// The latest backup completed and its files verified.
    Complete,
    /// The latest backup failed; `failure` says why.
    Failed,
}

/// The latest backup since the daemon started, as `status` reports it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackupStatus {
    /// Where the latest request stands.
    pub phase: BackupPhase,
    /// When the latest backup was requested, in Unix milliseconds; 0 while idle.
    pub started_at: u64,
    /// When it completed or failed, in Unix milliseconds; 0 while idle or running.
    pub finished_at: u64,
    /// The engine's identity of the completed backup; 0 until one completes.
    pub backup_id: u32,
    /// The completed backup's file count, shared table files included.
    pub files: u32,
    /// The completed backup's bytes, shared table files included.
    pub bytes: u64,
    /// The latest failure, present only in the failed phase.
    pub failure: Option<String>,
}

/// The one backup slot a store's clones share: at most one backup runs at a time.
#[derive(Clone, Default)]
pub(crate) struct Slot(Arc<Mutex<BackupStatus>>);

impl Slot {
    fn lock(&self) -> std::sync::MutexGuard<'_, BackupStatus> {
        // A panic cannot leave the status half-written: every update replaces whole fields.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn status(&self) -> BackupStatus {
        self.lock().clone()
    }

    /// Mark a backup running, or refuse while one already is.
    fn begin(&self, now: u64) -> Result<BackupStatus, Error> {
        let mut status = self.lock();
        if status.phase == BackupPhase::Running {
            return Err(Error::new(
                "CONFLICT",
                format!(
                    "a backup started at {} is still running; wait for status to report it complete or failed",
                    crate::format_instant(status.started_at)
                        .unwrap_or_else(|_| status.started_at.to_string())
                ),
            ));
        }
        *status = BackupStatus {
            phase: BackupPhase::Running,
            started_at: now,
            ..BackupStatus::default()
        };
        Ok(status.clone())
    }

    fn finish(&self, outcome: Result<Completed, Error>) {
        let finished_at = unix_millis();
        let mut status = self.lock();
        let started_at = status.started_at;
        *status = match outcome {
            Ok(done) => BackupStatus {
                phase: BackupPhase::Complete,
                started_at,
                finished_at,
                backup_id: done.backup_id,
                files: done.files,
                bytes: done.bytes,
                failure: None,
            },
            Err(error) => BackupStatus {
                phase: BackupPhase::Failed,
                started_at,
                finished_at,
                failure: Some(format!("{}: {}", error.code, error.message)),
                ..BackupStatus::default()
            },
        };
    }
}

/// Marks the slot failed if the backup thread ends without recording an outcome (a panic), so
/// a later request is not refused forever.
struct Unfinished(Option<Slot>);

impl Unfinished {
    fn finish(mut self, outcome: Result<Completed, Error>) {
        if let Some(slot) = self.0.take() {
            slot.finish(outcome);
        }
    }
}

impl Drop for Unfinished {
    fn drop(&mut self) {
        if let Some(slot) = self.0.take() {
            slot.finish(Err(Error::new(
                "STORAGE_FAILURE",
                "the backup thread ended without a result",
            )));
        }
    }
}

struct Completed {
    backup_id: u32,
    files: u32,
    bytes: u64,
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

fn failure(context: &str, error: impl std::fmt::Display) -> Error {
    Error::new("STORAGE_FAILURE", format!("{context}: {error}"))
}

/// How often the guard measures free space while a backup runs.
const GUARD_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

/// Start a backup of `db` into `target` on its own thread and return at once.
///
/// While RocksDB copies, it keeps every file that becomes obsolete. They do not count against
/// the disk budget (super::linked_physical_bytes), but they do occupy the filesystem, so a guard
/// thread at the daemon's normal priority, which never touches the backup root, measures free
/// space every [`GUARD_INTERVAL`]. Once free space falls below twice the store's floor, it
/// re-enables file deletions: RocksDB deletes the held files at once, and the backup fails if it
/// still needed one. Ingest therefore never meets the floor because of a backup, even when the
/// copy is stalled on an unresponsive network filesystem.
pub(crate) fn start(
    slot: &Slot,
    db: Arc<DB>,
    space: SpaceFloor,
    store_id: String,
    target: &BackupTarget,
    now: u64,
) -> Result<BackupStatus, Error> {
    let running = slot.begin(now)?;
    let unfinished = Unfinished(Some(slot.clone()));
    let (stop, stopped) = std::sync::mpsc::channel::<()>();
    let guard = {
        let db = Arc::clone(&db);
        std::thread::Builder::new()
            .name("sparrow-backup-guard".into())
            .spawn(move || guard(&db, &space, &stopped))
    };
    let guard = match guard {
        Ok(guard) => guard,
        Err(error) => {
            let error = failure("cannot start the backup's free-space guard", error);
            unfinished.finish(Err(Error::new(error.code, error.message.clone())));
            return Err(error);
        }
    };
    let target = target.clone();
    let spawned = std::thread::Builder::new()
        .name("sparrow-backup".into())
        .spawn(move || {
            let outcome = run(db, &store_id, &target);
            drop(stop);
            let released = guard
                .join()
                .unwrap_or_else(|_| Some("the free-space guard ended without a result".to_owned()));
            unfinished.finish(match (outcome, released) {
                (Err(error), Some(reason)) => Err(Error::new(
                    error.code,
                    format!(
                        "{}; the guard released the files the backup held because {reason}",
                        error.message
                    ),
                )),
                (outcome, _) => outcome,
            });
        });
    match spawned {
        Ok(_) => Ok(running),
        Err(error) => {
            // The closure, with the guard's stop sender and the unfinished marker in it, dropped
            // with the failed spawn: the guard ends, and the slot already reads failed.
            Err(failure("cannot start the backup thread", error))
        }
    }
}

/// Release the files RocksDB holds for a backup whenever free space falls below twice the
/// floor, until the backup ends. Returns why it first released, if it did.
fn guard(db: &DB, space: &SpaceFloor, stopped: &std::sync::mpsc::Receiver<()>) -> Option<String> {
    let mut released = None;
    while let Err(std::sync::mpsc::RecvTimeoutError::Timeout) = stopped.recv_timeout(GUARD_INTERVAL)
    {
        if let Some(reason) = release_if_short(db, space) {
            released.get_or_insert(reason);
        }
    }
    released
}

/// One guard check: below twice the floor, or when free space cannot be measured, re-enable file
/// deletions. RocksDB counts disables and never goes below zero, and nothing else in Sparrow
/// disables them, so a release before the backup's own disable, or after its own enable, changes
/// nothing; a later check releases again if the shortfall persists.
fn release_if_short(db: &DB, space: &SpaceFloor) -> Option<String> {
    let reason = match space.shortfall_after(space.min_free()) {
        Ok(None) => return None,
        Ok(Some(shortfall)) => shortfall,
        Err(error) => format!("free space could not be measured: {}", error.message),
    };
    // Already enabled: nothing is held, and re-enabling would only rescan the directory under
    // RocksDB's database lock on every check while space stays short.
    if let Ok(Some(enabled)) = db.property_int_value("rocksdb.is-file-deletions-enabled")
        && enabled != 0
    {
        return Some(reason);
    }
    match db.enable_file_deletions() {
        Ok(()) => Some(reason),
        Err(error) => Some(format!(
            "{reason}, and re-enabling file deletions failed: {error}"
        )),
    }
}

fn run(db: Arc<DB>, store_id: &str, target: &BackupTarget) -> Result<Completed, Error> {
    // setpriority(PRIO_PROCESS, 0) applies to the calling thread on Linux. RocksDB's copy
    // workers are created when the engine opens below, on this thread, and inherit it.
    rustix::process::setpriority_process(None, LOWEST_PRIORITY)
        .map_err(|error| failure("cannot lower the backup thread's priority", error))?;
    let root = &target.root;
    if target.network {
        // Judged from the mount table before anything is created, so an unmounted NAS never
        // gains a local directory in its place.
        check_mounts(root, &target.data_dir, &read_mounts()?)?;
    }
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(root)
        .map_err(|error| failure(&format!("cannot create {}", root.display()), error))?;
    if target.network {
        // A symbolic link on the way could still lead off the network filesystem.
        let canonical = root
            .canonicalize()
            .map_err(|error| failure(&format!("cannot resolve {}", root.display()), error))?;
        check_mounts(&canonical, &target.data_dir, &read_mounts()?)?;
    }
    claim_root(root, store_id)?;
    let env = rocksdb::Env::new().map_err(|error| failure("backup environment", error))?;
    let mut options = BackupEngineOptions::new(root.join(ENGINE_DIR))
        .map_err(|error| failure("backup engine options", error))?;
    // One copy at a time, each file synchronized before the backup's metadata names it.
    options.set_max_background_operations(1);
    options.set_sync(true);
    let mut engine = BackupEngine::open(&options, &env).map_err(|error| {
        failure(
            &format!("cannot open the backup engine in {}", root.display()),
            error,
        )
    })?;
    engine
        .create_new_backup_flush(&*db, false)
        .map_err(|error| failure("online backup", error))?;
    // Release the store as soon as the copy is done; verifying reads only the backup.
    drop(db);
    let latest = engine
        .get_backup_info()
        .into_iter()
        .max_by_key(|info| info.backup_id)
        .ok_or_else(|| {
            failure(
                "online backup",
                "the engine lists no backup after creating one",
            )
        })?;
    engine.verify_backup(latest.backup_id).map_err(|error| {
        failure(
            &format!("backup {} failed verification", latest.backup_id),
            error,
        )
    })?;
    engine
        .purge_old_backups(target.keep as usize)
        .map_err(|error| failure("cannot purge old backups", error))?;
    Ok(Completed {
        backup_id: latest.backup_id,
        files: latest.num_files,
        bytes: latest.size,
    })
}

/// Bind the root to `store_id` on its first backup; refuse a root bound to another store, so
/// two stores never share one engine directory.
fn claim_root(root: &Path, store_id: &str) -> Result<(), Error> {
    let path = root.join(STORE_ID_FILE);
    match std::fs::read_to_string(&path) {
        Ok(recorded) if recorded.trim_end() == store_id => Ok(()),
        Ok(recorded) => Err(Error::new(
            "CONFLICT",
            format!(
                "{} holds the backups of {}, not of this store {store_id}; name another backup_root",
                root.display(),
                recorded.trim_end()
            ),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let partial = root.join(format!("{STORE_ID_FILE}.partial"));
            let write = || -> std::io::Result<()> {
                use std::io::Write;
                let mut file = std::fs::File::create(&partial)?;
                file.write_all(format!("{store_id}\n").as_bytes())?;
                file.sync_all()?;
                std::fs::rename(&partial, &path)?;
                std::fs::File::open(root)?.sync_all()
            };
            write().map_err(|error| failure(&format!("cannot write {}", path.display()), error))
        }
        Err(error) => Err(failure(&format!("cannot read {}", path.display()), error)),
    }
}

/// Restore backup `id` (the latest when absent) from the backup root `root` into `target`,
/// which must be an empty or absent directory, never a live store. Returns the restored id.
pub fn restore(root: &Path, id: Option<u32>, target: &Path) -> Result<u32, Error> {
    match std::fs::read_dir(target) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                return Err(Error::new(
                    "INVALID_ARGUMENT",
                    format!(
                        "{} is not empty; a backup restores only into an empty directory, never over a store",
                        target.display()
                    ),
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(target)
                .map_err(|error| failure(&format!("cannot create {}", target.display()), error))?;
        }
        Err(error) => return Err(failure(&format!("cannot read {}", target.display()), error)),
    }
    let engine_dir = root.join(ENGINE_DIR);
    if !engine_dir.is_dir() {
        return Err(Error::new(
            "NOT_FOUND",
            format!("{} holds no backup engine directory", root.display()),
        ));
    }
    let env = rocksdb::Env::new().map_err(|error| failure("backup environment", error))?;
    let options = BackupEngineOptions::new(&engine_dir)
        .map_err(|error| failure("backup engine options", error))?;
    let mut engine = BackupEngine::open(&options, &env).map_err(|error| {
        failure(
            &format!("cannot open the backup engine in {}", engine_dir.display()),
            error,
        )
    })?;
    let backups = engine.get_backup_info();
    let id = match id {
        Some(id) if backups.iter().any(|info| info.backup_id == id) => id,
        Some(id) => {
            return Err(Error::new(
                "NOT_FOUND",
                format!("{} holds no backup {id}", engine_dir.display()),
            ));
        }
        None => backups
            .iter()
            .map(|info| info.backup_id)
            .max()
            .ok_or_else(|| {
                Error::new(
                    "NOT_FOUND",
                    format!("{} holds no backup", engine_dir.display()),
                )
            })?,
    };
    // Restoring reads every file back and checks its checksum against the backup's metadata.
    engine
        .restore_from_backup(target, target, &RestoreOptions::default(), id)
        .map_err(|error| failure(&format!("cannot restore backup {id}"), error))?;
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::{DB, SpaceFloor, check_mounts, parse_mount, release_if_short};
    use std::path::Path;

    fn walked_bytes(path: &Path) -> u64 {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let metadata = entry.metadata().unwrap();
                if metadata.is_dir() {
                    walked_bytes(&entry.path())
                } else {
                    metadata.len()
                }
            })
            .sum()
    }

    /// INVARIANT (issue #156; Patrick, P28: "All backups MUST be online and perturb no
    /// operations"): files RocksDB keeps only because a backup disabled file deletions do not
    /// count against the disk budget, so they can neither refuse a write nor evict evidence; and
    /// the backup's guard leaves them alone while free space is above twice the floor and
    /// releases them at once below it, so they never push ingest to the floor.
    #[test]
    fn files_held_for_a_backup_do_not_count_and_the_guard_releases_them_near_the_floor() {
        let dir = tempfile::tempdir().unwrap();
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf(&options, dir.path(), super::super::FAMILIES).unwrap();
        let held = |db: &DB| {
            db.property_int_value("rocksdb.obsolete-sst-files-size")
                .unwrap()
                .unwrap()
        };
        for round in 0..3_u8 {
            for key in 0..2000_u32 {
                db.put(key.to_be_bytes(), vec![round; 512]).unwrap();
            }
            db.flush().unwrap();
        }
        // What BackupEngine does for its copy; the compaction then obsoletes every table.
        db.disable_file_deletions().unwrap();
        db.compact_range::<&[u8], &[u8]>(None, None);
        let obsolete = held(&db);
        assert!(obsolete > 0, "the compaction left no held table");
        let counted = super::super::linked_physical_bytes(&db).unwrap();
        assert!(
            counted + obsolete <= walked_bytes(dir.path()),
            "counted {counted} bytes with {obsolete} held of {} on disk",
            walked_bytes(dir.path())
        );
        assert_eq!(release_if_short(&db, &SpaceFloor::new(dir.path(), 1)), None);
        assert_eq!(held(&db), obsolete, "released above twice the floor");
        let reason = release_if_short(&db, &SpaceFloor::new(dir.path(), u64::MAX / 4)).unwrap();
        assert!(reason.contains("bytes free"), "{reason}");
        assert_eq!(held(&db), 0, "the guard left held tables on disk");
        assert_eq!(
            super::super::linked_physical_bytes(&db).unwrap(),
            walked_bytes(dir.path()),
            "once nothing is held, every file counts"
        );
    }

    /// INVARIANT (issue #156; Patrick, 2026-11-08: Sparrow never backs up to a local disk): the
    /// backup root's filesystem is the longest, latest mount above it, an escaped mount point is
    /// decoded, and only a network filesystem other than the store's is accepted.
    #[test]
    fn backup_root_must_be_a_network_filesystem_apart_from_the_store() {
        let mounts = [
            "22 1 0:21 / / rw - btrfs /dev/nvme0n1p2 rw",
            "947 43 0:106 / /net/synology/archives rw - nfs4 nas:/volume1/archives rw",
            "948 43 0:107 / /net/synology/my\\040share rw shared:1 - cifs //nas/share rw",
            "949 43 0:21 /store /net/synology/archives/bound rw - btrfs /dev/nvme0n1p2 rw",
        ]
        .map(|line| parse_mount(line).unwrap());
        let store = Path::new("/SPARROW/store");
        check_mounts(Path::new("/net/synology/archives/sparrow"), store, &mounts).unwrap();
        check_mounts(Path::new("/net/synology/my share/sparrow"), store, &mounts).unwrap();
        let local = check_mounts(Path::new("/SPARROW/backups"), store, &mounts).unwrap_err();
        assert!(
            local.message.contains("/SPARROW/backups is on btrfs"),
            "{}",
            local.message
        );
        let shared = check_mounts(
            Path::new("/net/synology/archives/sparrow"),
            Path::new("/net/synology/archives/store"),
            &mounts,
        )
        .unwrap_err();
        assert!(shared.message.contains("shares the store's filesystem"));
        // A bind mount of the store's device below the NAS mount is not the NAS.
        assert!(check_mounts(Path::new("/net/synology/archives/bound/b"), store, &mounts).is_err());
    }
}
