// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Bounded Linux discovery and one completed-consumption worker.
//! Readiness is meaningful only under the configured finished-file contract.

use crate::{
    ConsumeWatch,
    consumption::{ClaimRoot, Error, Recovery, RootBinding},
    watch_admin::{self, Action, Catalog, ManagedWatch},
};
use rustix::fs::{
    self,
    inotify::{self, CreateFlags, ReadFlags, WatchFlags},
};
use sparrow_client::{Client, CompletedFileIdentity};
use sparrow_core::Store;
use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    mem::MaybeUninit,
    os::{
        fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd},
        unix::ffi::OsStrExt,
    },
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::{Duration, Instant},
};

#[cfg(test)]
mod tests;

/// `sparrowd --private-intake DIR`: every watch definition the daemon opens or accepts is
/// rebound to `DIR/NAME`, where NAME is the final component of its root, so a disposable
/// copy of a store consumes its own private roots and never the roots its catalog names.
/// The catalog keeps naming the configured roots: a mutation persists them, not the
/// rebound ones.
#[derive(Clone, Debug)]
pub struct PrivateIntake {
    dir: PathBuf,
}

impl PrivateIntake {
    /// Admit an absolute existing directory, held by its canonical path.
    pub fn new(dir: &Path) -> Result<Self, Error> {
        if !dir.is_absolute() || !dir.is_dir() {
            return Err(Error::State(
                "--private-intake requires an absolute existing directory",
            ));
        }
        Ok(Self {
            dir: std::fs::canonicalize(dir)?,
        })
    }

    /// Rebind one definition's root below the private directory and return the configured
    /// root it replaced. A configured root that is the directory, lies under it or holds it
    /// is refused: rebinding would then reach the configured root itself, or put the private
    /// roots inside a root another daemon consumes, as running the mode by hand on a live
    /// intake directory would. Paths are compared canonically, and an existing configured
    /// root is also compared by device and inode, so a bind mount or other alias of it is
    /// refused too.
    fn rebind(&self, definition: &mut ConsumeWatch) -> Result<PathBuf, Error> {
        let name = match definition.root.components().next_back() {
            Some(std::path::Component::Normal(name)) => name.to_owned(),
            _ => {
                return Err(Error::State(
                    "a watch root under --private-intake needs a final path component",
                ));
            }
        };
        // A configured root absent on this host (the copy came from another) is compared as
        // written; one present is compared through its links.
        let configured =
            std::fs::canonicalize(&definition.root).unwrap_or_else(|_| definition.root.clone());
        let private = self.dir.join(&name);
        if configured.starts_with(&self.dir)
            || self.dir.starts_with(&configured)
            || aliased(
                &identity_chain(&self.dir)?,
                identity(&private)?,
                &identity_chain(&configured)?,
            )
        {
            return Err(Error::State(
                "--private-intake DIR must not be, hold or lie under a watch's configured root; choose a scratch directory",
            ));
        }
        Ok(std::mem::replace(&mut definition.root, private))
    }

    fn binding(intake: Option<&Self>) -> RootBinding {
        intake.map_or(RootBinding::Stored, |_| RootBinding::Private)
    }
}

/// The device and inode of PATH, or None when it does not exist.
fn identity(path: &Path) -> Result<Option<(u64, u64)>, Error> {
    use std::os::unix::fs::MetadataExt;
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(Some((metadata.dev(), metadata.ino()))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// The identities of PATH and each of its ancestors, PATH first; empty when PATH does not
/// exist.
fn identity_chain(path: &Path) -> Result<Vec<(u64, u64)>, Error> {
    let mut chain = Vec::new();
    for ancestor in path.ancestors() {
        match identity(ancestor)? {
            Some(found) => chain.push(found),
            None if chain.is_empty() => return Ok(chain),
            None => {}
        }
    }
    Ok(chain)
}

/// Whether, by identity, the configured root (the head of CONFIGURED, when it exists) is the
/// private directory or one of its ancestors (the head and tail of DIR), is the private root
/// PRIVATE, or holds the private directory among its own ancestors.
fn aliased(dir: &[(u64, u64)], private: Option<(u64, u64)>, configured: &[(u64, u64)]) -> bool {
    let Some(root) = configured.first() else {
        return false;
    };
    dir.contains(root)
        || private == Some(*root)
        || dir.first().is_some_and(|d| configured.contains(d))
}

const SCAN_QUANTUM: usize = 64;
const EVENT_QUANTUM: usize = 4096;
/// While free space is below the floor the worker rechecks it this often: no event reports
/// space returning.
const SPACE_RECHECK: Duration = Duration::from_secs(1);
/// Interval between consume RPC connection attempts while a claim waits for a connection.
const CONNECT_RETRY: Duration = Duration::from_secs(1);
const RETRY_FIRST: Duration = Duration::from_secs(5);
const RETRY_CEILING: Duration = Duration::from_secs(15 * 60);
/// Longest consume publication. Its connection must outlive it under the lease.
const PUBLICATION_DEADLINE: Duration = Duration::from_secs(45);
const _: () =
    assert!(PUBLICATION_DEADLINE.as_secs() < crate::transport::CONNECTION_LEASE.as_secs());

/// Delay before retrying a claim after `failures` consecutive failed publication
/// attempts: 5 seconds, doubling, at most 15 minutes. A claim that cannot finish
/// within the publication deadline otherwise repeats its work every few seconds.
fn retry_delay(failures: u32) -> Duration {
    RETRY_FIRST
        .saturating_mul(1 << failures.saturating_sub(1).min(16))
        .min(RETRY_CEILING)
}

/// Bounded operational state of an explicitly configured consumption root.
#[derive(Clone)]
pub struct WatchStatus {
    /// Persistent watch identity.
    pub id: String,
    /// Configured absolute producer root.
    pub root: String,
    /// Complete durable definition shared with administrative list replies.
    pub definition: ConsumeWatch,
    /// Compare-and-set revision of this durable definition.
    pub revision: u64,
    /// Whether new source admission is enabled; false means claims are draining.
    pub enabled: bool,
    /// Whether recursive event coverage is complete in the current epoch.
    pub covered: bool,
    /// Held kernel directory watches.
    pub directories: u32,
    /// Tracked fresh stability candidates.
    pub candidates: u32,
    /// Additional inputs were preserved outside the candidate capacity.
    pub deferred: bool,
    /// Persistently owned claims not yet retired.
    pub pending: u32,
    /// Claims completed during this worker lifetime.
    pub completed: u64,
    /// Recursive coverage epochs begun during this worker lifetime.
    pub reconciliations: u64,
    /// Overflow, lost directory coverage, or bounded event processing resets.
    pub coverage_losses: u64,
    /// Most recent unresolved bounded operational failure.
    pub failure: Option<String>,
    /// Finite retained checkpoint identities, including known terminal generations.
    pub tail_generation_ids: Vec<String>,
    /// Hard bytes in one active file capture.
    pub tail_read_bytes: u64,
    /// Held current or rotated regular file descriptors.
    pub tail_descriptors: u32,
    /// Accepted occurrences across currently known generation checkpoints.
    pub tail_checkpoint_records: u64,
    /// Publication is waiting for the shared daemon operation budget.
    pub tail_admission_waiting: bool,
}

/// One shared bounded snapshot owner for worker updates and typed RPC status.
#[derive(Clone, Default)]
pub struct WatchRegistry(Arc<Mutex<Vec<WatchStatus>>>);

impl WatchRegistry {
    /// Capture at most eight configured roots; poisoning is an actionable failure.
    pub fn snapshot(&self) -> Result<Vec<WatchStatus>, Error> {
        self.0
            .lock()
            .map(|value| value.clone())
            .map_err(|_| Error::State("consume status registry poisoned"))
    }

    /// The stored catalog's watches as a query-only daemon reports them: no root is opened or
    /// watched, so each is `reconciling` with no directories, candidates or worker counters.
    pub fn stored(store: &Store) -> Result<Self, Error> {
        let catalog = Catalog::load(store, &[])?;
        let pending = store.consumption_pending()?;
        let states = catalog
            .watches()
            .iter()
            .map(|watch| WatchStatus {
                id: watch.definition.id.clone(),
                root: watch.definition.root.to_string_lossy().into_owned(),
                definition: watch.definition.clone(),
                revision: watch.revision,
                enabled: watch.enabled,
                covered: false,
                directories: 0,
                candidates: 0,
                deferred: false,
                pending: pending
                    .iter()
                    .filter(|journal| journal.watch == watch.definition.id)
                    .count() as u32,
                completed: 0,
                reconciliations: 0,
                coverage_losses: 0,
                failure: None,
                tail_generation_ids: Vec::new(),
                tail_read_bytes: 0,
                tail_descriptors: 0,
                tail_checkpoint_records: 0,
                tail_admission_waiting: false,
            })
            .collect();
        Ok(Self(Arc::new(Mutex::new(states))))
    }

    fn update(
        &self,
        detectors: &[Detector],
        pending: &[sparrow_core::ConsumptionJournal],
    ) -> Result<(), Error> {
        let states = detectors
            .iter()
            .map(|detector| {
                let (
                    tail_generation_ids,
                    tail_read_bytes,
                    tail_descriptors,
                    tail_checkpoint_records,
                ) = match &detector.owner {
                    Owner::Consume(_) => (Vec::new(), 0, 0, 0),
                    Owner::Tail(owner) => owner.status(&detector.config.id)?,
                };
                Ok(WatchStatus {
                    id: detector.config.id.clone(),
                    root: detector.config.root.to_string_lossy().into_owned(),
                    definition: detector.config.clone(),
                    revision: detector.revision,
                    enabled: detector.enabled,
                    covered: detector.covered,
                    directories: detector.directories.len() as u32,
                    candidates: detector.candidates.len() as u32,
                    deferred: detector.deferred,
                    pending: pending
                        .iter()
                        .filter(|journal| journal.watch == detector.config.id)
                        .count() as u32,
                    completed: detector.completed,
                    reconciliations: detector.reconciliations,
                    coverage_losses: detector.coverage_losses,
                    failure: detector.failure.clone(),
                    tail_generation_ids,
                    tail_read_bytes,
                    tail_descriptors,
                    tail_checkpoint_records,
                    tail_admission_waiting: detector.tail_admission_waiting,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        *self
            .0
            .lock()
            .map_err(|_| Error::State("consume status registry poisoned"))? = states;
        Ok(())
    }
}

struct Directory {
    relative: PathBuf,
    // Keep the exact directory alive while the kernel watch refers to its inode.
    _file: File,
}
struct Scan {
    relative: PathBuf,
    entries: fs::Dir,
}
struct Candidate {
    identity: CompletedFileIdentity,
    stable_since: Instant,
}

/// The tail owner is boxed: it is more than twice the size of a claim root, and every
/// consume detector would otherwise carry its size (clippy::large_enum_variant).
enum Owner {
    Consume(ClaimRoot),
    Tail(Box<crate::tailing::TailRoot>),
}
impl Owner {
    fn directory(&self, relative: &Path) -> Result<File, Error> {
        match self {
            Self::Consume(owner) => owner.directory(relative),
            Self::Tail(owner) => owner.directory(relative),
        }
    }
    fn observe(&self, relative: &Path) -> Result<Option<CompletedFileIdentity>, Error> {
        match self {
            Self::Consume(owner) => owner.observe(relative),
            Self::Tail(owner) => owner.observe(relative),
        }
    }
    fn root_binding(&self, path: &Path) -> Result<(), Error> {
        match self {
            Self::Consume(owner) => owner.root_binding(path),
            Self::Tail(owner) => owner.root_binding(path),
        }
    }
    fn claim(&self) -> Result<&ClaimRoot, Error> {
        match self {
            Self::Consume(owner) => Ok(owner),
            Self::Tail(owner) => owner.claim(),
        }
    }
}

struct Detector {
    config: ConsumeWatch,
    revision: u64,
    enabled: bool,
    owner: Owner,
    events: OwnedFd,
    directories: BTreeMap<i32, Directory>,
    todo: VecDeque<PathBuf>,
    scan: Option<Scan>,
    candidates: BTreeMap<PathBuf, Candidate>,
    renames: BTreeMap<u32, (PathBuf, CompletedFileIdentity)>,
    covered: bool,
    deferred: bool,
    next_reconcile: Instant,
    completed: u64,
    reconciliations: u64,
    coverage_losses: u64,
    failure: Option<String>,
    /// The claim whose recovery set `failure`, if a claim's recovery did; any other
    /// failure leaves it `None`. A claim's own success or retirement clears its failure
    /// (`clear_failure_of`) and another claim's success leaves it in place; a failure that
    /// names no claim clears on any claim's success (`record_claim_success`).
    failed_claim: Option<String>,
    tail_admission_waiting: bool,
}

impl Detector {
    /// Clear the watch failure only when `claim`'s own recovery set it. A failure from another
    /// claim or from discovery stays until its own cause resolves, so a claim that failed is
    /// never hidden by an unrelated claim's success.
    fn clear_failure_of(&mut self, claim: &str) {
        if self.failed_claim.as_deref() == Some(claim) {
            self.failure = None;
            self.failed_claim = None;
        }
    }

    /// A claim finished. Its own failure clears, and so does a failure that names no claim
    /// (discovery, reservation, drain, tail attach: the watch demonstrably works again), but
    /// another claim's failure stays until that claim resolves.
    fn record_claim_success(&mut self, claim: &str) {
        if self.failed_claim.is_none() {
            self.failure = None;
        }
        self.clear_failure_of(claim);
        self.completed = self.completed.saturating_add(1);
    }

    fn open(store: Store, watch: ManagedWatch, binding: RootBinding) -> Result<Self, Error> {
        let config = watch.definition;
        let owner = match config.producer_contract {
            crate::ProducerContract::FinishedFiles => Owner::Consume(ClaimRoot::open_bound(
                store,
                &config.id,
                &config.root,
                binding,
            )?),
            crate::ProducerContract::ActiveFilesRetain => Owner::Tail(Box::new(
                crate::tailing::TailRoot::open(store, &config, watch.revision, binding)?,
            )),
        };
        let events = inotify::init(CreateFlags::CLOEXEC | CreateFlags::NONBLOCK)?;
        let mut detector = Self {
            config,
            revision: watch.revision,
            enabled: watch.enabled,
            owner,
            events,
            directories: BTreeMap::new(),
            todo: VecDeque::new(),
            scan: None,
            candidates: BTreeMap::new(),
            renames: BTreeMap::new(),
            covered: false,
            deferred: false,
            next_reconcile: Instant::now(),
            completed: 0,
            reconciliations: 0,
            coverage_losses: 0,
            failure: None,
            failed_claim: None,
            tail_admission_waiting: false,
        };
        detector.reconcile("startup")?;
        if !detector.enabled {
            detector.pause()?;
        }
        Ok(detector)
    }

    fn pause(&mut self) -> Result<(), Error> {
        self.covered = false;
        self.candidates.clear();
        self.renames.clear();
        self.directories.clear();
        self.todo.clear();
        self.scan = None;
        self.events = inotify::init(CreateFlags::CLOEXEC | CreateFlags::NONBLOCK)?;
        Ok(())
    }

    fn reconcile(&mut self, reason: &str) -> Result<(), Error> {
        self.reconciliations = self.reconciliations.saturating_add(1);
        self.owner.root_binding(&self.config.root)?;
        // A new descriptor epoch prevents queued stale watch IDs from being reused.
        self.events = inotify::init(CreateFlags::CLOEXEC | CreateFlags::NONBLOCK)?;
        self.directories.clear();
        self.todo.clear();
        self.scan = None;
        self.candidates.clear();
        self.renames.clear();
        self.covered = false;
        self.deferred = false;
        self.add_directory(Path::new(""))?;
        eprintln!(
            "sparrowd: watch={} reconciling reason={reason}",
            self.config.id
        );
        Ok(())
    }

    fn rescan_interval(&self) -> Duration {
        Duration::from_millis(60_000.max(u64::from(self.config.quiet_ms) * 2 + 1000))
    }

    /// Rediscover missed entries under continuous coverage. A consume watch keeps
    /// its kernel watches and candidate quiet clocks: a full reconciliation would
    /// restart every quiet interval and stall claims that long on each scan. Tail
    /// watches keep full reconciliation, which also records missing generations.
    fn rescan(&mut self, reason: &str) -> Result<(), Error> {
        if matches!(self.owner, Owner::Tail(_)) {
            return self.reconcile(reason);
        }
        self.owner.root_binding(&self.config.root)?;
        self.todo = self
            .directories
            .values()
            .map(|directory| directory.relative.clone())
            .collect();
        self.scan = None;
        // A MOVED_FROM already removed its candidate; an unpaired cookie is stale.
        self.renames.clear();
        self.deferred = false;
        self.next_reconcile = Instant::now() + self.rescan_interval();
        Ok(())
    }

    fn add_directory(&mut self, relative: &Path) -> Result<(), Error> {
        if self.directories.len() >= self.config.max_directories {
            return Err(Error::State(
                "consume directory coverage limit exceeded; raise max_directories or reduce the explicitly authorized root",
            ));
        }
        let file = self.owner.directory(relative)?;
        // inotify has no dirfd API. Follow only this kernel-owned descriptor alias,
        // after openat2 has admitted the directory; never a producer-supplied symlink.
        let alias = format!("/proc/self/fd/{}", file.as_raw_fd());
        let wd = inotify::add_watch(
            &self.events,
            alias.as_str(),
            WatchFlags::ALL_EVENTS | WatchFlags::ONLYDIR,
        )?;
        if self.directories.contains_key(&wd) {
            return Err(Error::State(
                "recursive directory aliases share a watch inode; disjoint coverage required",
            ));
        }
        self.directories.insert(
            wd,
            Directory {
                relative: relative.to_owned(),
                _file: file,
            },
        );
        self.todo.push_back(relative.to_owned());
        Ok(())
    }

    fn candidate(&mut self, path: PathBuf, activity: bool) -> Result<(), Error> {
        if let Owner::Tail(owner) = &mut self.owner {
            return match owner.attach(&path, &self.config, self.revision) {
                Ok(()) => Ok(()),
                Err(Error::Storage(error)) if error.code == "STORAGE_FAILURE" => {
                    Err(Error::Storage(error))
                }
                Err(Error::FinishedGeneration(message)) => {
                    self.failure = Some(message.into());
                    self.failed_claim = None;
                    Ok(())
                }
                Err(error) => {
                    self.failure = Some(error.to_string().chars().take(1024).collect());
                    self.failed_claim = None;
                    self.deferred = true;
                    Ok(())
                }
            };
        }
        let Some(identity) = self.owner.observe(&path)? else {
            self.candidates.remove(&path);
            return Ok(());
        };
        let now = Instant::now();
        if let Some(candidate) = self.candidates.get_mut(&path) {
            if activity || identity != candidate.identity {
                *candidate = Candidate {
                    identity,
                    stable_since: now,
                };
            }
        } else if self.candidates.len() < self.config.max_candidates {
            self.candidates.insert(
                path,
                Candidate {
                    identity,
                    stable_since: now,
                },
            );
        } else {
            if !self.deferred {
                eprintln!(
                    "sparrowd: watch={} candidate limit reached; excess inputs preserved for the next bounded scan",
                    self.config.id
                );
            }
            self.deferred = true;
        }
        Ok(())
    }

    fn scan_step(&mut self) -> Result<(), Error> {
        for _ in 0..SCAN_QUANTUM {
            if self.scan.is_none() {
                let Some(relative) = self.todo.pop_front() else {
                    if !self.covered {
                        // Scan observations made without complete coverage cannot
                        // shorten the fresh quiet interval after reconciliation.
                        let now = Instant::now();
                        for candidate in self.candidates.values_mut() {
                            candidate.stable_since = now;
                        }
                        if !self.deferred
                            && let Owner::Tail(owner) = &mut self.owner
                        {
                            owner.reconcile_missing(&self.config.id)?;
                        }
                        self.covered = true;
                        self.next_reconcile = now + self.rescan_interval();
                        eprintln!(
                            "sparrowd: watch={} covered directories={} candidates={}",
                            self.config.id,
                            self.directories.len(),
                            self.candidates.len()
                        );
                    }
                    return Ok(());
                };
                let directory = self.owner.directory(&relative)?;
                self.scan = Some(Scan {
                    relative,
                    entries: fs::Dir::read_from(&directory)?,
                });
            }
            let scan = self.scan.as_mut().expect("scan installed above");
            let Some(entry) = scan.entries.next() else {
                self.scan = None;
                continue;
            };
            let entry = entry?;
            let name = entry.file_name().to_bytes();
            if name == b"." || name == b".." || name == b".sparrow-inflight" {
                continue;
            }
            let relative = scan.relative.join(std::ffi::OsStr::from_bytes(name));
            if relative.as_os_str().as_bytes().len() > 2048 {
                return Err(Error::State(
                    "consume relative path exceeds the bounded journal path limit",
                ));
            }
            let kind = if entry.file_type() == fs::FileType::Unknown {
                match fs::statat(
                    scan.entries.fd()?,
                    entry.file_name(),
                    fs::AtFlags::SYMLINK_NOFOLLOW,
                ) {
                    Ok(metadata) => fs::FileType::from_raw_mode(metadata.st_mode),
                    Err(error) if error == rustix::io::Errno::NOENT => continue,
                    Err(error) => return Err(error.into()),
                }
            } else {
                entry.file_type()
            };
            match kind {
                // A rescan revisits directories it already watches.
                fs::FileType::Directory => {
                    if !self
                        .directories
                        .values()
                        .any(|directory| directory.relative == relative)
                    {
                        self.add_directory(&relative)?;
                    }
                }
                fs::FileType::RegularFile => self.candidate(relative, false)?,
                // FIFOs, devices, sockets and symlinks never become source authority.
                _ => {}
            }
        }
        Ok(())
    }

    fn drain(&mut self) -> Result<(), Error> {
        if let Owner::Tail(owner) = &mut self.owner {
            owner.take_appends()?;
        }
        let mut buffer = [MaybeUninit::uninit(); 4096];
        // Use a duplicated descriptor so events can update the rest of this detector.
        let events = rustix::io::dup(&self.events)?;
        let mut reader = inotify::Reader::new(events, &mut buffer);
        for _ in 0..EVENT_QUANTUM {
            let event = match reader.next() {
                Ok(event) => event,
                Err(error) if error == rustix::io::Errno::WOULDBLOCK => return Ok(()),
                Err(error) => return Err(error.into()),
            };
            let flags = event.events();
            if flags.intersects(
                ReadFlags::QUEUE_OVERFLOW
                    | ReadFlags::IGNORED
                    | ReadFlags::UNMOUNT
                    | ReadFlags::MOVE_SELF
                    | ReadFlags::DELETE_SELF,
            ) {
                self.coverage_losses = self.coverage_losses.saturating_add(1);
                return self.reconcile("lost event coverage");
            }
            let Some(directory) = self.directories.get(&event.wd()) else {
                return self.reconcile("unknown event directory");
            };
            let Some(name) = event.file_name() else {
                continue;
            };
            if name.to_bytes() == b".sparrow-inflight" {
                continue;
            }
            if flags.contains(ReadFlags::ISDIR) {
                if flags.intersects(
                    ReadFlags::CREATE
                        | ReadFlags::MOVED_TO
                        | ReadFlags::MOVED_FROM
                        | ReadFlags::DELETE,
                ) {
                    return self.reconcile("recursive directory change");
                }
                continue;
            }
            let relative = directory
                .relative
                .join(std::ffi::OsStr::from_bytes(name.to_bytes()));
            if flags.contains(ReadFlags::MOVED_FROM)
                && let Some(old) = self.candidates.remove(&relative)
            {
                if self.renames.len() >= self.config.max_candidates {
                    return self.reconcile("rename tracking limit");
                }
                self.renames
                    .insert(event.cookie(), (relative.clone(), old.identity));
            }
            if flags.contains(ReadFlags::MOVED_TO) {
                // Cookies identify related paths; current inode/clocks still govern.
                if let Some((old_path, old_identity)) = self.renames.remove(&event.cookie()) {
                    let current = self.owner.observe(&relative)?;
                    if current.is_some_and(|current| {
                        current.device == old_identity.device && current.inode == old_identity.inode
                    }) {
                        self.candidates.remove(&old_path);
                    }
                }
            }
            if flags.contains(ReadFlags::DELETE) {
                self.candidates.remove(&relative);
            }
            // An open adds no bytes. For a completed-file watch it restarts the quiet clock. A
            // tail watch ignores it: the daemon's own open in `attach` raises it, and reacting
            // would reopen a refused file on every event. A producer that appends or truncates
            // still raises MODIFY, so no needed attach is lost.
            let activity = if matches!(self.owner, Owner::Tail(_)) {
                ReadFlags::empty()
            } else {
                ReadFlags::OPEN
            };
            if flags.intersects(
                activity
                    | ReadFlags::MODIFY
                    | ReadFlags::ATTRIB
                    | ReadFlags::CREATE
                    | ReadFlags::CLOSE_WRITE
                    | ReadFlags::MOVED_TO,
            ) {
                self.candidate(relative, true)?;
            }
            // ACCESS and CLOSE_NOWRITE do not certify producer completion.
        }
        // Reader may have prefetched additional events. Never discard its buffer
        // while retaining readiness evidence: reset the complete coverage epoch.
        self.coverage_losses = self.coverage_losses.saturating_add(1);
        self.reconcile("event processing limit")
    }

    fn ready(&mut self) -> Result<Option<(PathBuf, CompletedFileIdentity)>, Error> {
        if matches!(self.owner, Owner::Tail(_)) {
            return Ok(None);
        }
        self.owner.root_binding(&self.config.root)?;
        if !self.covered {
            return Ok(None);
        }
        let quiet = Duration::from_millis(u64::from(self.config.quiet_ms));
        let ready = self
            .candidates
            .iter()
            .find(|(_, candidate)| candidate.stable_since.elapsed() >= quiet)
            .map(|(path, candidate)| (path.clone(), candidate.identity));
        let Some((path, expected)) = ready else {
            return Ok(None);
        };
        let observed = self.owner.observe(&path)?;
        if observed != Some(expected) {
            self.candidate(path, true)?;
            return Ok(None);
        }
        Ok(Some((path, expected)))
    }

    /// Whether this detector has work the worker must run again for at once, without
    /// waiting for an event: a scan in progress or, with space to write, a tail backlog
    /// that holds a permit.
    fn busy(&self, space_ok: bool) -> bool {
        self.enabled
            && (!self.covered
                || !self.todo.is_empty()
                || self.scan.is_some()
                || (space_ok
                    && !self.tail_admission_waiting
                    && matches!(&self.owner, Owner::Tail(owner) if !owner.caught_up())))
    }

    /// Whether a tail backlog waits for the shared operation budget.
    fn waits_for_permit(&self) -> bool {
        self.enabled
            && self.tail_admission_waiting
            && matches!(&self.owner, Owner::Tail(owner) if !owner.caught_up())
    }

    /// The earliest instant this idle detector has work without a new event: its periodic
    /// rescan and, when CLAIMS admits new reservations, the end of its first candidate's
    /// quiet interval, or now when candidate capacity freed for deferred inputs.
    fn due(&self, claims: bool) -> Option<Instant> {
        if !self.enabled || !self.covered || !self.todo.is_empty() || self.scan.is_some() {
            return None;
        }
        // The deferred-capacity rescan in `run`, which a tail watch never needs.
        if claims
            && self.deferred
            && matches!(self.owner, Owner::Consume(_))
            && self.candidates.len() < self.config.max_candidates
        {
            return Some(Instant::now());
        }
        let quiet = Duration::from_millis(u64::from(self.config.quiet_ms));
        let ready = self
            .candidates
            .values()
            .filter(|_| claims && matches!(self.owner, Owner::Consume(_)))
            .map(|candidate| candidate.stable_since + quiet)
            .min();
        Some(ready.map_or(self.next_reconcile, |ready| ready.min(self.next_reconcile)))
    }

    /// The descriptors that become readable when this detector has events to drain.
    fn descriptors(&self) -> impl Iterator<Item = BorrowedFd<'_>> {
        std::iter::once(self.events.as_fd()).chain(match &self.owner {
            Owner::Tail(owner) => Some(owner.appends()),
            Owner::Consume(_) => None,
        })
    }
}

/// One owned worker, whose failure must be checked by the daemon owner.
pub struct ConsumeWorker {
    stop: tokio::sync::watch::Sender<bool>,
    thread: Option<JoinHandle<Result<(), String>>>,
    /// Closes when the worker thread ends, by return or by panic.
    ended: tokio::sync::oneshot::Receiver<()>,
    controller: watch_admin::WatchController,
}

impl ConsumeWorker {
    /// Validate and lock every configured root before starting its native SDK worker.
    pub fn start(
        store: Store,
        configs: &[ConsumeWatch],
        socket: PathBuf,
        registry: WatchRegistry,
        operations: Arc<tokio::sync::Semaphore>,
    ) -> Result<Self, Error> {
        Self::start_with(
            store,
            configs,
            socket,
            registry,
            operations,
            None,
            crate::config::OutputPaths::default(),
        )
    }

    /// Start as [`Self::start`] does. With `private`, every stored root is first rebound
    /// below the private directory (`sparrowd --private-intake`).
    pub fn start_with(
        store: Store,
        configs: &[ConsumeWatch],
        socket: PathBuf,
        registry: WatchRegistry,
        operations: Arc<tokio::sync::Semaphore>,
        private: Option<PrivateIntake>,
        outputs: crate::config::OutputPaths,
    ) -> Result<Self, Error> {
        let mut catalog = Catalog::load(&store, configs)?;
        if let Some(intake) = &private {
            catalog.rebind_roots(|definition| intake.rebind(definition))?;
        }
        let binding = PrivateIntake::binding(private.as_ref());
        let definitions = catalog
            .watches()
            .iter()
            .map(|watch| watch.definition.clone())
            .collect::<Vec<_>>();
        ConsumeWatch::validate(&definitions)
            .and_then(|()| outputs.refuse_overlap(&definitions))
            .map_err(|error| Error::Io(std::io::Error::other(error.to_string())))?;
        let pending = store.consumption_pending()?;
        if pending
            .iter()
            .any(|journal| !definitions.iter().any(|watch| watch.id == journal.watch))
        {
            return Err(Error::State(
                "configured watches omit an unfinished consumption journal; restore its watch before starting the consumer",
            ));
        }
        let detectors = catalog
            .watches()
            .iter()
            .cloned()
            .map(|watch| Detector::open(store.clone(), watch, binding))
            .collect::<Result<Vec<_>, _>>()?;
        registry.update(&detectors, &pending)?;
        if catalog.needs_initial_commit() {
            catalog.persist(&store)?;
        }
        let (controller, commands) = watch_admin::channel();
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let (alive, ended) = tokio::sync::oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("sparrow-consume".into())
            .spawn(move || {
                // Dropped as the thread ends, which closes `ended`.
                let _alive = alive;
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| error.to_string())?;
                tokio::task::LocalSet::new()
                    .block_on(
                        &runtime,
                        run(
                            store,
                            detectors,
                            socket,
                            stopped,
                            registry,
                            catalog,
                            Control {
                                commands,
                                operations,
                                private,
                                outputs,
                            },
                        ),
                    )
                    .map_err(|error| error.to_string())
            })?;
        Ok(Self {
            stop,
            thread: Some(thread),
            ended,
            controller,
        })
    }

    /// Share the bounded command path, never a second watch mutation owner.
    pub fn controller(&self) -> watch_admin::WatchController {
        self.controller.clone()
    }

    /// A finished worker is terminal; the daemon must inspect its join result.
    pub fn is_finished(&self) -> bool {
        self.thread.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Resolve when the worker thread ends; the daemon then joins it through
    /// [`Self::shutdown`] for its result. Await it at most once.
    pub async fn ended(&mut self) {
        // The thread never sends: its sender closes as it ends, the only outcome.
        let _ = (&mut self.ended).await;
    }

    /// Stop admission and await owned work while the daemon's RPC LocalSet still runs.
    pub async fn shutdown(mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.stop.send_replace(true);
        let thread = self
            .thread
            .take()
            .ok_or("consume worker was already joined")?;
        let result = tokio::task::spawn_blocking(move || thread.join()).await?;
        result
            .map_err(|_| "completed-consumption worker panicked")?
            .map_err(Into::into)
    }
}

impl Drop for ConsumeWorker {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

/// Resolve once stop is requested.
async fn stopped(stop: &mut tokio::sync::watch::Receiver<bool>) {
    // An error means the sender is gone, which only the dropped worker owner does after
    // requesting stop.
    let _ = stop.wait_for(|stopped| *stopped).await;
}

/// What a worker with no work in hand waits for, besides stop, commands and readable
/// inotify descriptors.
#[derive(Default)]
struct Wake {
    /// The earliest deadline at which work is due without any event.
    at: Option<Instant>,
    /// A tail drain or tail claim waits for a shared operation permit.
    permit: bool,
}

impl Wake {
    fn by(&mut self, at: Instant) {
        self.at = Some(self.at.map_or(at, |current| current.min(at)));
    }
}

/// The state one worker wait reads and fills.
struct Waiting<'a> {
    store: &'a Store,
    registry: &'a WatchRegistry,
    stop: &'a mut tokio::sync::watch::Receiver<bool>,
    control: &'a mut Control,
    /// A command the wait received, handled first by the next pass.
    received: &'a mut Option<watch_admin::Command>,
    /// A permit the wait acquired, used by the next pass.
    permit: &'a mut Option<tokio::sync::OwnedSemaphorePermit>,
}

/// End one worker pass. A busy pass yields and runs again at once. An idle one blocks until
/// stop, a command, a readable inotify descriptor, a freed permit it waits for, or its
/// earliest deadline. Every journal writer runs on this worker (reservation in the pass;
/// handoff, tail completion and retirement as commands; tail capture in the pass), so the
/// pass that creates a pending claim also sees it: no other wake source is needed for one.
async fn pause(
    waiting: Waiting<'_>,
    detectors: &mut [Detector],
    space_ok: bool,
    busy: bool,
    claims: bool,
    mut wake: Wake,
) -> Result<(), Error> {
    // Never hold the shared budget across a wait.
    *waiting.permit = None;
    if busy {
        tokio::task::yield_now().await;
        return Ok(());
    }
    // The pass's own opens and reads below the roots queue events. Consume them now, so
    // they cannot end this wait and start a pass that queues more.
    for detector in detectors.iter_mut().filter(|detector| detector.enabled) {
        detector.drain()?;
    }
    // That drain may also have taken producer events from the pass: report what they
    // changed, and run again for any work they made due.
    waiting
        .registry
        .update(detectors, &waiting.store.consumption_pending()?)?;
    if detectors.iter().any(|detector| detector.busy(space_ok)) {
        tokio::task::yield_now().await;
        return Ok(());
    }
    wake.permit = wake.permit || (space_ok && detectors.iter().any(Detector::waits_for_permit));
    for at in detectors.iter().filter_map(|detector| detector.due(claims)) {
        wake.by(at);
    }
    let events = detectors
        .iter()
        .filter(|detector| detector.enabled)
        .flat_map(Detector::descriptors)
        .map(|fd| tokio::io::unix::AsyncFd::with_interest(fd, tokio::io::Interest::READABLE))
        .collect::<Result<Vec<_>, _>>()?;
    let readable = std::future::poll_fn(|context| {
        for fd in &events {
            if let std::task::Poll::Ready(ready) = fd.poll_read_ready(context) {
                return std::task::Poll::Ready(ready.map(drop));
            }
        }
        std::task::Poll::Pending
    });
    let (wants_permit, at) = (wake.permit, wake.at);
    let operations = waiting.control.operations.clone();
    let admitted = async move {
        if wants_permit {
            operations.acquire_owned().await
        } else {
            std::future::pending().await
        }
    };
    let deadline = async {
        match at {
            Some(at) => tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        biased;
        () = stopped(waiting.stop) => {}
        command = waiting.control.commands.recv() => {
            *waiting.received = Some(command.ok_or(Error::State(
                "watch administration channel closed unexpectedly",
            ))?);
        }
        ready = readable => ready?,
        admitted = admitted => {
            *waiting.permit = Some(admitted.map_err(|_| {
                Error::State("tail operation budget closed unexpectedly")
            })?);
        }
        () = deadline => {}
    }
    Ok(())
}

/// A held permit from the last wait, else one free now.
fn admit(
    operations: &Arc<tokio::sync::Semaphore>,
    held: &mut Option<tokio::sync::OwnedSemaphorePermit>,
) -> Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::TryAcquireError> {
    held.take()
        .map_or_else(|| operations.clone().try_acquire_owned(), Ok)
}

enum AdminFailure {
    Refused(sparrow_core::Error),
    Terminal(Error),
}

fn admit_handoff(
    store: &Store,
    detectors: &[Detector],
    intent: &sparrow_core::ConsumptionHandoff,
) -> Result<sparrow_core::ConsumptionJournal, AdminFailure> {
    let now = crate::now().map_err(AdminFailure::Refused)?;
    if let Some(original) = store
        .consumption_handoff_get(intent, now)
        .map_err(storage_admission)?
    {
        return Ok(original);
    }
    let detector = detectors
        .iter()
        .find(|watch| watch.config.id == intent.watch)
        .ok_or_else(|| {
            AdminFailure::Refused(watch_admin::failure(
                "NOT_FOUND",
                "handoff watch is not registered",
            ))
        })?;
    if !detector.enabled || detector.revision != intent.watch_revision {
        return Err(AdminFailure::Refused(watch_admin::failure(
            "CONFLICT",
            "handoff requires the current enabled watch revision",
        )));
    }
    if matches!(detector.owner, Owner::Tail(_)) != intent.tail_generation_id.is_some() {
        return Err(AdminFailure::Refused(watch_admin::failure(
            "CONFLICT",
            "active-file handoff must name its original finished tail generation; completed-file watches require ordinary handoff",
        )));
    }
    detector
        .owner
        .root_binding(&detector.config.root)
        .map_err(|error| AdminFailure::Refused(watch_admin::failure("CONFLICT", error)))?;
    detector
        .owner
        .claim()
        .map_err(|error| AdminFailure::Refused(watch_admin::failure("UNSUPPORTED", error)))?
        .handoff(
            intent,
            &detector.config.source,
            &detector.config.format,
            now,
        )
        .map_err(|error| match error {
            Error::Storage(error) => storage_admission(error),
            Error::Io(error) if error.kind() == std::io::ErrorKind::NotFound => {
                AdminFailure::Refused(watch_admin::failure("NOT_FOUND", error))
            }
            error => AdminFailure::Refused(watch_admin::failure("CONFLICT", error)),
        })
}

fn storage_admission(error: sparrow_core::Error) -> AdminFailure {
    if error.code == "STORAGE_FAILURE" {
        AdminFailure::Terminal(Error::Storage(error))
    } else {
        AdminFailure::Refused(error)
    }
}

/// Retire one unrecoverable claim through the watch's own claim owner, which holds its
/// private claim directory, and write the one journal line that records it.
fn retire_claim(
    detectors: &mut [Detector],
    retirement: &watch_admin::ClaimRetirement,
) -> Result<sparrow_core::RetiredConsumption, AdminFailure> {
    let now = crate::now().map_err(AdminFailure::Refused)?;
    let detector = detectors
        .iter_mut()
        .find(|detector| detector.config.id == retirement.watch)
        .ok_or_else(|| {
            AdminFailure::Refused(watch_admin::failure(
                "NOT_FOUND",
                "claim retirement names no configured watch",
            ))
        })?;
    let retired = detector
        .owner
        .claim()
        .map_err(|error| AdminFailure::Refused(watch_admin::failure("UNSUPPORTED", error)))?
        .retire(
            &retirement.claim,
            &retirement.caller,
            &retirement.reason,
            now,
        )
        .map_err(|error| match error {
            Error::Storage(error) => storage_admission(error),
            Error::State(message) => {
                AdminFailure::Refused(watch_admin::failure("CONFLICT", message))
            }
            error => AdminFailure::Refused(watch_admin::failure("STORAGE_FAILURE", error)),
        })?;
    // Clear the watch failure only when it is this claim's recovery refusal; a failure from
    // another claim or from discovery stays until its own cause resolves.
    detector.clear_failure_of(&retirement.claim);
    eprintln!(
        "sparrowd: watch={} claim={} retired-unrecoverable caller={:?} reason={:?} path={:?}",
        retirement.watch,
        retirement.claim,
        retired.retirement.caller,
        retired.retirement.reason,
        String::from_utf8_lossy(&retired.journal.relative_path),
    );
    Ok(retired)
}

fn admit_tail_finish(
    store: &Store,
    detectors: &mut [Detector],
    intent: &sparrow_core::TailFinishIntent,
) -> Result<sparrow_core::TailGeneration, AdminFailure> {
    if let Some(original) = store.tail_finish_get(intent).map_err(storage_admission)? {
        return Ok(original);
    }
    let generation = store
        .tail_generation(&intent.generation_id)
        .map_err(storage_admission)?;
    let detector = detectors
        .iter_mut()
        .find(|watch| watch.config.id == generation.declaration.watch)
        .ok_or_else(|| {
            AdminFailure::Refused(watch_admin::failure(
                "NOT_FOUND",
                "completion watch is not registered",
            ))
        })?;
    if !detector.enabled || detector.revision != intent.watch_revision {
        return Err(AdminFailure::Refused(watch_admin::failure(
            "CONFLICT",
            "completion requires the original enabled watch revision",
        )));
    }
    detector
        .owner
        .root_binding(&detector.config.root)
        .map_err(|error| AdminFailure::Refused(watch_admin::failure("CONFLICT", error)))?;
    let Owner::Tail(owner) = &mut detector.owner else {
        return Err(AdminFailure::Refused(watch_admin::failure(
            "UNSUPPORTED",
            "completion requires explicit active-file retention authority",
        )));
    };
    owner.finish(intent).map_err(|error| match error {
        Error::Storage(error) => storage_admission(error),
        error => AdminFailure::Refused(watch_admin::failure("CONFLICT", error)),
    })
}

fn acknowledge<T>(
    reply: tokio::sync::oneshot::Sender<Result<T, sparrow_core::Error>>,
    result: Result<T, AdminFailure>,
    store: &Store,
    registry: &WatchRegistry,
    detectors: &[Detector],
) -> Result<(), Error> {
    match result {
        Ok(value) => {
            registry.update(detectors, &store.consumption_pending()?)?;
            let _ = reply.send(Ok(value));
        }
        Err(AdminFailure::Refused(error)) => {
            let _ = reply.send(Err(error));
        }
        Err(AdminFailure::Terminal(error)) => {
            let _ = reply.send(Err(watch_admin::failure("STORAGE_FAILURE", &error)));
            return Err(error);
        }
    }
    Ok(())
}

fn administrate(
    store: &Store,
    catalog: &mut Catalog,
    detectors: &mut Vec<Detector>,
    action: Action,
    pending: &[sparrow_core::ConsumptionJournal],
    binding: RootBinding,
    outputs: &crate::config::OutputPaths,
) -> Result<ManagedWatch, AdminFailure> {
    let (mut next, watch, duplicate) = catalog
        .prepare(&action, crate::now().map_err(AdminFailure::Refused)?)
        .map_err(AdminFailure::Refused)?;
    if duplicate {
        return Ok(watch);
    }
    next.retire(pending);
    let definitions = next
        .watches()
        .iter()
        .map(|watch| watch.definition.clone())
        .collect::<Vec<_>>();
    ConsumeWatch::validate(&definitions)
        .and_then(|()| outputs.refuse_overlap(&definitions))
        .map_err(|error| AdminFailure::Refused(watch_admin::failure("INVALID_ARGUMENT", error)))?;
    let old = detectors
        .iter()
        .find(|detector| detector.config.id == watch.definition.id);
    if let Some(old) = old
        && matches!(old.owner, Owner::Tail(_))
        && store
            .tail_generations(&old.config.id)
            .map_err(storage_admission)?
            .iter()
            .any(|row| !row.complete && row.gap.is_none())
    {
        return Err(AdminFailure::Refused(watch_admin::failure(
            "CONFLICT",
            "active file generations bind this watch revision; complete or explicitly discontinue them before changing or removing it",
        )));
    }
    let mut prepared = None;
    if watch.enabled {
        let same_root = old.is_some_and(|old| {
            old.config.producer_contract == watch.definition.producer_contract
                && old.owner.root_binding(&watch.definition.root).is_ok()
        });
        if !same_root {
            if pending
                .iter()
                .any(|journal| journal.watch == watch.definition.id)
            {
                return Err(AdminFailure::Refused(watch_admin::failure(
                    "CONFLICT",
                    "unfinished claims still bind this watch to its original root; drain or restore that exact root before changing it",
                )));
            }
            prepared = Some(
                Detector::open(store.clone(), watch.clone(), binding).map_err(|error| {
                    AdminFailure::Refused(watch_admin::failure(
                        "INVALID_ARGUMENT",
                        format!("watch root admission refused: {error}"),
                    ))
                })?,
            );
        }
    }
    // Catalog, stable receipt and old-writer fence share one durable DB batch.
    // Any uncertain write is terminal: never keep admitting with stale authority.
    next.persist(store).map_err(|error| match error {
        Error::Storage(error) if matches!(error.code, "RESOURCE_LIMIT" | "STORAGE_PRESSURE") => {
            AdminFailure::Refused(error)
        }
        error => AdminFailure::Terminal(error),
    })?;
    *catalog = next;
    if let Some(detector) = prepared {
        detectors.retain(|old| old.config.id != watch.definition.id);
        detectors.push(detector);
    } else if let Some(detector) = detectors
        .iter_mut()
        .find(|detector| detector.config.id == watch.definition.id)
    {
        detector.config = watch.definition.clone();
        detector.revision = watch.revision;
        detector.enabled = watch.enabled;
        if watch.enabled {
            detector
                .reconcile("administrative update")
                .map_err(AdminFailure::Terminal)?;
        } else {
            detector.pause().map_err(AdminFailure::Terminal)?;
        }
    }
    detectors.retain(|detector| {
        catalog
            .watches()
            .iter()
            .any(|watch| watch.definition.id == detector.config.id)
    });
    Ok(watch)
}

struct Control {
    commands: tokio::sync::mpsc::Receiver<watch_admin::Command>,
    operations: Arc<tokio::sync::Semaphore>,
    private: Option<PrivateIntake>,
    outputs: crate::config::OutputPaths,
}

async fn run(
    store: Store,
    mut detectors: Vec<Detector>,
    socket: PathBuf,
    mut stop: tokio::sync::watch::Receiver<bool>,
    registry: WatchRegistry,
    mut catalog: Catalog,
    mut control: Control,
) -> Result<(), Error> {
    // The consume connection and when it was opened; the daemon's lease runs from there.
    let mut client: Option<(Client, Instant)> = None;
    // Next attempt and consecutive failures per pending claim.
    let mut retry = BTreeMap::<String, (Instant, u32)>::new();
    let mut connect_after = Instant::now();
    // A command or a permit the last wait received, for this pass.
    let mut received = None;
    let mut permit = None;
    while !*stop.borrow() {
        #[cfg(test)]
        tests::PASSES.with(|passes| passes.set(passes.get() + 1));
        let mut wake = Wake::default();
        // Below the free-space floor every write is refused: keep observing, write nothing.
        let space_ok = store.space_floor().shortfall()?.is_none();
        if !space_ok {
            wake.by(Instant::now() + SPACE_RECHECK);
        }
        let pending = store.consumption_pending()?;
        if space_ok && catalog.retire(&pending) {
            catalog.persist(&store)?;
            detectors.retain(|detector| {
                catalog
                    .watches()
                    .iter()
                    .any(|watch| watch.definition.id == detector.config.id)
            });
        }
        for _ in 0..watch_admin::MAX_WATCH_COMMANDS {
            let command = match received
                .take()
                .map_or_else(|| control.commands.try_recv(), Ok)
            {
                Ok(command) => command,
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    return Err(Error::State(
                        "watch administration channel closed unexpectedly",
                    ));
                }
            };
            match command {
                watch_admin::Command::Mutation { mut action, reply } => {
                    let pending = store.consumption_pending()?;
                    let rebound = match (&control.private, &mut action) {
                        (Some(intake), Action::Put { definition, .. }) => intake
                            .rebind(definition)
                            .map_err(|error| {
                                AdminFailure::Refused(watch_admin::failure(
                                    "INVALID_ARGUMENT",
                                    error,
                                ))
                            })
                            .and_then(|configured| {
                                catalog
                                    .admit_private_root(
                                        &definition.id,
                                        &definition.root,
                                        &configured,
                                    )
                                    .map_err(AdminFailure::Refused)?;
                                Ok(Some((definition.root.clone(), configured)))
                            }),
                        _ => Ok(None),
                    };
                    let result = rebound.and_then(|rebound| {
                        // The put persists the root it was given, which a failed put forgets.
                        let previous = rebound.map(|(private, configured)| {
                            let prior = catalog.remember_root(private.clone(), configured);
                            (private, prior)
                        });
                        administrate(
                            &store,
                            &mut catalog,
                            &mut detectors,
                            action,
                            &pending,
                            PrivateIntake::binding(control.private.as_ref()),
                            &control.outputs,
                        )
                        .inspect_err(|_| {
                            if let Some((private, previous)) = previous {
                                catalog.restore_root(private, previous);
                            }
                        })
                    });
                    acknowledge(reply, result, &store, &registry, &detectors)?;
                }
                watch_admin::Command::Handoff { intent, reply } => {
                    let result = admit_handoff(&store, &detectors, &intent);
                    acknowledge(reply, result, &store, &registry, &detectors)?;
                }
                watch_admin::Command::TailFinish { intent, reply } => {
                    let result = admit_tail_finish(&store, &mut detectors, &intent);
                    acknowledge(reply, result, &store, &registry, &detectors)?;
                }
                watch_admin::Command::RetireClaim { retirement, reply } => {
                    let result = retire_claim(&mut detectors, &retirement);
                    if result.is_ok() {
                        retry.remove(&retirement.claim);
                    }
                    acknowledge(reply, result, &store, &registry, &detectors)?;
                }
            }
        }
        for detector in &mut detectors {
            if !detector.enabled {
                continue;
            }
            detector.drain()?;
            if detector.covered
                && detector.todo.is_empty()
                && detector.scan.is_none()
                && Instant::now() >= detector.next_reconcile
            {
                detector.rescan("periodic scan")?;
            }
            detector.scan_step()?;
            detector.drain()?;
            if !space_ok {
                continue;
            }
            if let Owner::Tail(owner) = &mut detector.owner {
                // A caught-up owner has nothing to capture until an event, so needs no permit.
                if owner.caught_up() {
                    detector.tail_admission_waiting = false;
                    continue;
                }
                let _permit = match admit(&control.operations, &mut permit) {
                    Ok(permit) => {
                        detector.tail_admission_waiting = false;
                        permit
                    }
                    Err(tokio::sync::TryAcquireError::NoPermits) => {
                        detector.tail_admission_waiting = true;
                        continue;
                    }
                    Err(tokio::sync::TryAcquireError::Closed) => {
                        return Err(Error::State("tail operation budget closed unexpectedly"));
                    }
                };
                match owner.drain() {
                    Ok(()) => {}
                    Err(Error::Storage(error)) if error.code == "STORAGE_FAILURE" => {
                        return Err(Error::Storage(error));
                    }
                    Err(error) => {
                        detector.failure = Some(error.to_string().chars().take(1024).collect());
                        detector.failed_claim = None;
                    }
                }
            }
        }
        let pending = store.consumption_pending()?;
        registry.update(&detectors, &pending)?;
        let busy = detectors.iter().any(|detector| detector.busy(space_ok));
        let waiting = Waiting {
            store: &store,
            registry: &registry,
            stop: &mut stop,
            control: &mut control,
            received: &mut received,
            permit: &mut permit,
        };
        if !space_ok
            || detectors.is_empty()
            || (pending.is_empty()
                && detectors
                    .iter()
                    .all(|detector| matches!(detector.owner, Owner::Tail(_))))
        {
            pause(waiting, &mut detectors, space_ok, busy, false, wake).await?;
            continue;
        }
        retry.retain(|id, _| pending.iter().any(|journal| journal.import_id == *id));
        let mut work = pending
            .iter()
            .filter(|journal| {
                retry
                    .get(&journal.import_id)
                    .is_none_or(|(at, _)| *at <= Instant::now())
            })
            // A failed ordinary upload connection must not block checkpoint
            // acknowledgement, which has no transport dependency.
            .min_by_key(|journal| journal.tail.is_none())
            .map(|journal| {
                (
                    journal.watch.clone(),
                    journal.import_id.clone(),
                    journal.tail.is_some(),
                )
            });
        let claims = work.is_none() && pending.len() < sparrow_core::MAX_CONSUMPTION_JOURNALS;
        if claims {
            for detector in &mut detectors {
                if !detector.enabled {
                    continue;
                }
                if let Some((path, identity)) = detector.ready()? {
                    if *waiting.stop.borrow() {
                        break;
                    }
                    match detector.owner.claim()?.reserve(
                        &path,
                        identity,
                        &detector.config.source,
                        &detector.config.format,
                    ) {
                        Ok(journal) => {
                            detector.candidates.remove(&path);
                            work = Some((detector.config.id.clone(), journal.import_id, false));
                        }
                        Err(error) => {
                            detector.failure = Some(error.to_string().chars().take(1024).collect());
                            detector.failed_claim = None;
                            eprintln!(
                                "sparrowd: watch={} reservation refused for {:?}: {error}",
                                detector.config.id, path
                            );
                            detector.candidate(path, true)?;
                        }
                    }
                    break;
                }
                // A tail watch never fills `candidates`, so capacity is never what a refused
                // attach waits for; its periodic reconcile is the retry.
                if detector.covered
                    && detector.deferred
                    && !matches!(detector.owner, Owner::Tail(_))
                    && detector.todo.is_empty()
                    && detector.scan.is_none()
                    && detector.candidates.len() < detector.config.max_candidates
                {
                    detector.rescan("deferred candidate capacity available")?;
                }
            }
        }
        if let Some((watch, id, is_tail)) = work {
            if !is_tail {
                // A permit the last wait acquired for a tail drain is not held through an
                // ordinary connection or publication.
                *waiting.permit = None;
                // Replace a connection the lease would cut during this publication.
                if client.as_ref().is_some_and(|(_, opened)| {
                    opened.elapsed() + PUBLICATION_DEADLINE > crate::transport::CONNECTION_LEASE
                }) {
                    client = None;
                }
                if client.is_none() && Instant::now() >= connect_after {
                    let opened = Instant::now();
                    tokio::select! {
                        () = stopped(waiting.stop) => break,
                        result = tokio::time::timeout(Duration::from_secs(10), Client::connect(&socket)) => {
                            match result {
                                Ok(Ok(connected)) => client = Some((connected, opened)),
                                Ok(Err(error)) => eprintln!("sparrowd: consume RPC discovery failed: {error}"),
                                Err(_) => eprintln!("sparrowd: consume RPC discovery timed out; sources remain owned"),
                            }
                            connect_after = Instant::now() + CONNECT_RETRY;
                        }
                    }
                }
                if client.is_none() {
                    // Every other due claim waits behind this one, so only later retries
                    // and the next connection attempt can bring new work.
                    wake.by(connect_after);
                    blocked(&retry, &mut wake);
                    pause(waiting, &mut detectors, space_ok, busy, false, wake).await?;
                    continue;
                }
            }
            let _permit = if is_tail {
                match admit(&waiting.control.operations, waiting.permit) {
                    Ok(permit) => Some(permit),
                    Err(tokio::sync::TryAcquireError::NoPermits) => {
                        wake.permit = true;
                        blocked(&retry, &mut wake);
                        pause(waiting, &mut detectors, space_ok, busy, false, wake).await?;
                        continue;
                    }
                    Err(tokio::sync::TryAcquireError::Closed) => {
                        return Err(Error::State(
                            "tail claim operation budget closed unexpectedly",
                        ));
                    }
                }
            } else {
                None
            };
            let detector = detectors
                .iter_mut()
                .find(|detector| detector.config.id == watch)
                .ok_or(Error::State(
                    "pending consumption journal has no configured watch owner",
                ))?;
            let operation = async {
                if let Recovery::Claimed(mut claimed) =
                    detector.owner.claim()?.recover(&id, crate::now()?)?
                {
                    if is_tail {
                        detector.owner.claim()?.publish_tail(&mut claimed)?;
                    } else {
                        detector
                            .owner
                            .claim()?
                            .publish(
                                &client
                                    .as_ref()
                                    .ok_or(Error::State("consume RPC connection absent"))?
                                    .0,
                                &mut claimed,
                            )
                            .await?;
                    }
                    detector.owner.claim()?.remove(&claimed, crate::now()?)?;
                }
                Ok::<_, Error>(())
            };
            let result = tokio::select! {
                () = stopped(waiting.stop) => break,
                result = tokio::time::timeout(PUBLICATION_DEADLINE, operation) => result.unwrap_or(Err(Error::State("consume publication deadline expired; retain the original journal and retry"))),
            };
            if let Err(error) = result {
                // Name the claim, so an operator can act on it (retire-claim takes its ID).
                detector.failure =
                    Some(format!("claim {id}: {error}").chars().take(1024).collect());
                detector.failed_claim = Some(id.clone());
                eprintln!("sparrowd: watch={watch} claim={id} pending: {error}");
                // The claim's durable history answers `watch claims` after any restart.
                match store.consumption_attempt_pending(&id, &error.to_string(), crate::now()?) {
                    Ok(_) => {}
                    Err(record) if record.code == "STORAGE_FAILURE" => {
                        return Err(Error::Storage(record));
                    }
                    Err(record) => {
                        eprintln!(
                            "sparrowd: watch={watch} claim={id} pending attempt not recorded: {record}"
                        );
                        detector.failure = Some(
                            format!("claim {id}: {error}; attempt not recorded: {record}")
                                .chars()
                                .take(1024)
                                .collect(),
                        );
                    }
                }
                let failures = retry
                    .get(&id)
                    .map_or(0, |(_, failures)| *failures)
                    .saturating_add(1);
                retry.insert(id, (Instant::now() + retry_delay(failures), failures));
                if !is_tail {
                    client = None;
                }
            } else {
                detector.record_claim_success(&id);
            }
            if let Some(index) = detectors
                .iter()
                .position(|detector| detector.config.id == watch)
            {
                detectors.rotate_left(index + 1);
            }
            // Re-evaluate at once after a claim.
            tokio::task::yield_now().await;
            continue;
        }
        for (at, _) in retry.values() {
            wake.by(*at);
        }
        pause(waiting, &mut detectors, space_ok, busy, claims, wake).await?;
    }
    Ok(())
}

/// Wake for the retries still to come while the due claim waits.
fn blocked(retry: &BTreeMap<String, (Instant, u32)>, wake: &mut Wake) {
    let now = Instant::now();
    for (at, _) in retry.values().filter(|(at, _)| *at > now) {
        wake.by(*at);
    }
}
