// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Bounded Linux discovery and one completed-consumption worker.
//! Readiness is meaningful only under the configured finished-file contract.

use crate::{
    ConsumeWatch,
    consumption::{ClaimRoot, Error, Recovery},
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
        fd::{AsRawFd, OwnedFd},
        unix::ffi::OsStrExt,
    },
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

const SCAN_QUANTUM: usize = 64;
const EVENT_QUANTUM: usize = 4096;
const TICK: Duration = Duration::from_millis(50);

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

enum Owner {
    Consume(ClaimRoot),
    Tail(crate::tailing::TailRoot),
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
    tail_admission_waiting: bool,
}

impl Detector {
    fn open(store: Store, watch: ManagedWatch) -> Result<Self, Error> {
        let config = watch.definition;
        let owner = match config.producer_contract {
            crate::ProducerContract::FinishedFiles => {
                Owner::Consume(ClaimRoot::open(store, &config.id, &config.root)?)
            }
            crate::ProducerContract::ActiveFilesRetain => Owner::Tail(
                crate::tailing::TailRoot::open(store, &config, watch.revision)?,
            ),
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
                    Ok(())
                }
                Err(error) => {
                    self.failure = Some(error.to_string().chars().take(1024).collect());
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
                        self.next_reconcile = now
                            + Duration::from_millis(
                                60_000.max(u64::from(self.config.quiet_ms) * 2 + 1000),
                            );
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
                fs::FileType::Directory => self.add_directory(&relative)?,
                fs::FileType::RegularFile => self.candidate(relative, false)?,
                // FIFOs, devices, sockets and symlinks never become source authority.
                _ => {}
            }
        }
        Ok(())
    }

    fn drain(&mut self) -> Result<(), Error> {
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
            if flags.intersects(
                ReadFlags::OPEN
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
}

/// One owned worker, whose failure must be checked by the daemon owner.
pub struct ConsumeWorker {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<Result<(), String>>>,
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
        let mut catalog = Catalog::load(&store, configs)?;
        let definitions = catalog
            .watches()
            .iter()
            .map(|watch| watch.definition.clone())
            .collect::<Vec<_>>();
        ConsumeWatch::validate(&definitions)
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
            .map(|watch| Detector::open(store.clone(), watch))
            .collect::<Result<Vec<_>, _>>()?;
        registry.update(&detectors, &pending)?;
        if catalog.needs_initial_commit() {
            catalog.persist(&store)?;
        }
        let (controller, commands) = watch_admin::channel();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let thread = std::thread::Builder::new()
            .name("sparrow-consume".into())
            .spawn(move || {
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
                            },
                        ),
                    )
                    .map_err(|error| error.to_string())
            })?;
        Ok(Self {
            stop,
            thread: Some(thread),
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

    /// Stop admission and await owned work while the daemon's RPC LocalSet still runs.
    pub async fn shutdown(mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.stop.store(true, Ordering::Release);
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
        self.stop.store(true, Ordering::Release);
    }
}

async fn stopped(stop: &AtomicBool) {
    while !stop.load(Ordering::Acquire) {
        tokio::time::sleep(TICK).await;
    }
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
                Detector::open(store.clone(), watch.clone()).map_err(|error| {
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
}

async fn run(
    store: Store,
    mut detectors: Vec<Detector>,
    socket: PathBuf,
    stop: Arc<AtomicBool>,
    registry: WatchRegistry,
    mut catalog: Catalog,
    mut control: Control,
) -> Result<(), Error> {
    let mut client: Option<Client> = None;
    let mut retry = BTreeMap::<String, Instant>::new();
    let mut connect_after = Instant::now();
    while !stop.load(Ordering::Acquire) {
        let pending = store.consumption_pending()?;
        if catalog.retire(&pending) {
            catalog.persist(&store)?;
            detectors.retain(|detector| {
                catalog
                    .watches()
                    .iter()
                    .any(|watch| watch.definition.id == detector.config.id)
            });
        }
        for _ in 0..watch_admin::MAX_WATCH_COMMANDS {
            let command = match control.commands.try_recv() {
                Ok(command) => command,
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    return Err(Error::State(
                        "watch administration channel closed unexpectedly",
                    ));
                }
            };
            match command {
                watch_admin::Command::Mutation { action, reply } => {
                    let pending = store.consumption_pending()?;
                    let result =
                        administrate(&store, &mut catalog, &mut detectors, action, &pending);
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
            }
        }
        for detector in &mut detectors {
            if !detector.enabled {
                continue;
            }
            detector.drain()?;
            if detector.covered && Instant::now() >= detector.next_reconcile {
                detector.reconcile("periodic scan")?;
            }
            detector.scan_step()?;
            detector.drain()?;
            if let Owner::Tail(owner) = &mut detector.owner {
                let _permit = match control.operations.try_acquire() {
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
                        detector.failure = Some(error.to_string().chars().take(1024).collect())
                    }
                }
            }
        }
        let pending = store.consumption_pending()?;
        registry.update(&detectors, &pending)?;
        if detectors.is_empty()
            || (pending.is_empty()
                && detectors
                    .iter()
                    .all(|detector| matches!(detector.owner, Owner::Tail(_))))
        {
            tokio::time::sleep(TICK).await;
            continue;
        }
        retry.retain(|id, _| pending.iter().any(|journal| journal.import_id == *id));
        let mut work = pending
            .iter()
            .filter(|journal| {
                retry
                    .get(&journal.import_id)
                    .is_none_or(|at| *at <= Instant::now())
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
        if work.is_none() && pending.len() < sparrow_core::MAX_CONSUMPTION_JOURNALS {
            for detector in &mut detectors {
                if !detector.enabled {
                    continue;
                }
                if let Some((path, identity)) = detector.ready()? {
                    if stop.load(Ordering::Acquire) {
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
                            eprintln!(
                                "sparrowd: watch={} reservation refused for {:?}: {error}",
                                detector.config.id, path
                            );
                            detector.candidate(path, true)?;
                        }
                    }
                    break;
                }
                if detector.covered
                    && detector.deferred
                    && detector.candidates.len() < detector.config.max_candidates
                {
                    detector.reconcile("deferred candidate capacity available")?;
                }
            }
        }
        if let Some((watch, id, is_tail)) = work {
            if !is_tail {
                if client.is_none() && Instant::now() >= connect_after {
                    tokio::select! {
                        _ = stopped(&stop) => break,
                        result = tokio::time::timeout(Duration::from_secs(10), Client::connect(&socket)) => {
                            match result {
                                Ok(Ok(connected)) => client = Some(connected),
                                Ok(Err(error)) => eprintln!("sparrowd: consume RPC discovery failed: {error}"),
                                Err(_) => eprintln!("sparrowd: consume RPC discovery timed out; sources remain owned"),
                            }
                            connect_after = Instant::now() + Duration::from_secs(1);
                        }
                    }
                }
                if client.is_none() {
                    retry.insert(id, Instant::now() + TICK);
                    tokio::time::sleep(TICK).await;
                    continue;
                }
            }
            let _permit = if is_tail {
                match control.operations.try_acquire() {
                    Ok(permit) => Some(permit),
                    Err(tokio::sync::TryAcquireError::NoPermits) => {
                        tokio::time::sleep(TICK).await;
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
                                client
                                    .as_ref()
                                    .ok_or(Error::State("consume RPC connection absent"))?,
                                &mut claimed,
                            )
                            .await?;
                    }
                    detector.owner.claim()?.remove(&claimed, crate::now()?)?;
                }
                Ok::<_, Error>(())
            };
            let result = tokio::select! {
                _ = stopped(&stop) => break,
                result = tokio::time::timeout(Duration::from_secs(45), operation) => result.unwrap_or(Err(Error::State("consume publication deadline expired; retain the original journal and retry"))),
            };
            if let Err(error) = result {
                detector.failure = Some(error.to_string().chars().take(1024).collect());
                eprintln!("sparrowd: watch={watch} claim={id} pending: {error}");
                retry.insert(id, Instant::now() + Duration::from_secs(5));
                if !is_tail {
                    client = None;
                }
            } else {
                detector.failure = None;
                detector.completed = detector.completed.saturating_add(1);
            }
            if let Some(index) = detectors
                .iter()
                .position(|detector| detector.config.id == watch)
            {
                detectors.rotate_left(index + 1);
            }
        }
        tokio::time::sleep(TICK).await;
    }
    Ok(())
}
