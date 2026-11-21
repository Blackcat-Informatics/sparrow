// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Bounded Linux discovery and one completed-consumption worker.
//! Readiness is meaningful only under the configured finished-file contract.

use crate::{
    ConsumeWatch,
    consumption::{ClaimRoot, Error, Recovery, Refusal, RootBinding},
    watch_admin::{self, Action, Catalog, ManagedWatch, Retirement},
};
use rustix::fs::{
    self,
    inotify::{self, CreateFlags, ReadFlags, WatchFlags},
};
use sparrow_client::{Client, CompletedFileIdentity, parts::ProgressMark};
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

/// Longest wait between retries of a step that failed for a cause outside any one claim: a
/// store refusal, a watch root that cannot be read or has been replaced. Such a cause clears
/// by itself (space freed, a latch cleared, the exact root restored) or by an operator's
/// action while the daemon runs. A claim's own retry ceiling (15 minutes) would leave the
/// watch idle for a quarter of an hour after the cause is gone; one minute bounds that delay,
/// and doubling from 5 seconds keeps a persistent cause to a handful of attempts and journal
/// lines a minute at worst.
const STORE_RETRY_CEILING: Duration = Duration::from_secs(60);

/// Delay before retry number `failures` of a step that failed outside any one claim.
fn step_delay(failures: u32) -> Duration {
    retry_delay(failures).min(STORE_RETRY_CEILING)
}

/// Retry clock of one repeating step that must not end the worker when it fails. It keeps
/// the step's last failure text so the journal says it once per change and again when the
/// step recovers. It runs on the tokio clock, which is the clock the worker's waits use.
#[derive(Debug, Default)]
struct Backoff {
    failures: u32,
    at: Option<tokio::time::Instant>,
    logged: Option<String>,
}

impl Backoff {
    /// Whether the step may run now.
    fn due(&self) -> bool {
        self.at.is_none_or(|at| tokio::time::Instant::now() >= at)
    }

    /// Whether the step has failed and not yet recovered.
    fn failing(&self) -> bool {
        self.logged.is_some()
    }

    /// Record a failure of the step WHAT: schedule its retry and journal it once per change of
    /// its text.
    fn failed(&mut self, what: &str, text: String) {
        self.failures = self.failures.saturating_add(1);
        self.at = Some(tokio::time::Instant::now() + step_delay(self.failures));
        if self.logged.as_ref() != Some(&text) {
            eprintln!("sparrowd: {what} failed: {text}; retrying");
            self.logged = Some(text);
        }
    }

    /// Record that the step succeeded, and journal the recovery when it had failed.
    fn recovered(&mut self, what: &str) {
        self.failures = 0;
        self.at = None;
        if self.logged.take().is_some() {
            eprintln!("sparrowd: {what} recovered");
        }
    }

    /// Wake no later than the retry.
    fn wake(&self, wake: &mut Wake) {
        if let Some(at) = self.at {
            wake.by(at.into_std());
        }
    }
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
    /// Pending finished-file claims the publish hold holds; zero unless the hold is set.
    pub held_claims: u32,
    /// First-seen time of the oldest held claim, when the hold holds any.
    pub oldest_held_first_seen: Option<u64>,
}

/// The pending claims the worker may publish now, in pending order. A claim whose watch holds
/// publication is skipped: it stays journaled and in flight, and the wait has no deadline for
/// it, so a hold never spins the worker. Clearing the hold (a watch put) wakes the wait as any
/// command does. A tail is never held; `due` says whether a claim's retry time has come.
fn publishable<'a>(
    pending: &'a [sparrow_core::ConsumptionJournal],
    detectors: &'a [Detector],
    due: impl Fn(&str) -> bool + 'a,
) -> impl Iterator<Item = &'a sparrow_core::ConsumptionJournal> + 'a {
    pending.iter().filter(move |journal| {
        // A claim of a watch whose root is not open waits for that exact root; the claim stays
        // journaled and bound to it.
        due(&journal.import_id)
            && detectors
                .iter()
                .any(|detector| detector.config.id == journal.watch)
            && (journal.tail.is_some()
                || !detectors.iter().any(|detector| {
                    detector.config.id == journal.watch && detector.config.publish_hold
                }))
    })
}

/// Most pending finished-file claims one publish-held watch may take. Held claims never
/// complete, so each occupies one of the store's sixteen journal slots until the hold clears.
/// Four is enough to park a rollback rehearsal's in-flight claims or an operator's pause, and
/// no more than a quarter of the store, so one watch's pause never uses the whole table. This
/// share keeps held watches fair to one another; the total of all held claims is
/// `HELD_CLAIMS_TOTAL`, which binds once several watches are held.
const HELD_CLAIMS_PER_WATCH: usize = 4;

/// Journal slots every publish-held watch together never takes. Unheld watches and tail
/// generations reserve from these, so they can always reserve however many watches are held.
const HELD_RESERVED_SLOTS: usize = 4;

/// The total number of pending claims that held watches may stand, all of them together: the
/// store's journal slots (`MAX_CONSUMPTION_JOURNALS`, 16) less `HELD_RESERVED_SLOTS`, so twelve.
/// `pending` counts every pending claim, unheld and tail claims included, so a held claim is
/// refused whenever it would leave fewer than four slots free for an unheld watch or a tail.
/// Enforced in `held_may_claim`, which the claim pass consults with the count read at its start.
/// The pass reserves at most one file before it leaves the detector loop, so that count is
/// current for every reservation it makes.
const HELD_CLAIMS_TOTAL: usize = sparrow_core::MAX_CONSUMPTION_JOURNALS - HELD_RESERVED_SLOTS;

/// Whether a publish-held watch `id` may reserve another finished file: its own share is below
/// `HELD_CLAIMS_PER_WATCH` (fairness among held watches), and the total of pending claims is
/// below `HELD_CLAIMS_TOTAL`. Files it may not reserve stay unclaimed in the producer's
/// directory and are claimed, in order, once the hold clears or a held claim leaves.
fn held_may_claim(id: &str, pending: &[sparrow_core::ConsumptionJournal]) -> bool {
    let own = pending
        .iter()
        .filter(|journal| journal.watch == id && journal.tail.is_none())
        .count();
    own < HELD_CLAIMS_PER_WATCH && pending.len() < HELD_CLAIMS_TOTAL
}

/// How many pending finished-file claims of watch `id` a publish hold holds, and the first-seen
/// (reservation) time of the oldest. Zero and none unless `hold` is set; a tail is never held.
fn held_summary(
    hold: bool,
    id: &str,
    pending: &[sparrow_core::ConsumptionJournal],
) -> (u32, Option<u64>) {
    if !hold {
        return (0, None);
    }
    pending
        .iter()
        .filter(|journal| journal.watch == id && journal.tail.is_none())
        .fold((0, None), |(count, since), journal| {
            let seen = journal.history.first_seen;
            let since = match (since, seen) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            (count + 1, since)
        })
}

/// The status of a watch whose root is not open: no coverage and no worker counters, with
/// FAILURE saying why when the worker could not open it.
fn closed_status(
    watch: &ManagedWatch,
    pending: &[sparrow_core::ConsumptionJournal],
    failure: Option<String>,
) -> WatchStatus {
    let (held_claims, oldest_held_first_seen) =
        held_summary(watch.definition.publish_hold, &watch.definition.id, pending);
    WatchStatus {
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
        failure,
        tail_generation_ids: Vec::new(),
        tail_read_bytes: 0,
        tail_descriptors: 0,
        tail_checkpoint_records: 0,
        tail_admission_waiting: false,
        held_claims,
        oldest_held_first_seen,
    }
}

/// A configured watch whose root could not be opened. Its pending claims are kept, and the
/// worker opens it again on the backoff, so the exact root restored resumes it; a replacement
/// root is refused each time by the same checks as at start.
struct Unopened {
    watch: ManagedWatch,
    retry: Backoff,
}

/// Open every watch of CATALOG. A watch whose root cannot be opened does not stop the others
/// or the daemon: it is returned unopened with its failure recorded and journaled.
fn open_detectors(
    store: &Store,
    catalog: &Catalog,
    binding: RootBinding,
) -> (Vec<Detector>, Vec<Unopened>) {
    let mut detectors = Vec::new();
    let mut unopened = Vec::new();
    for watch in catalog.watches() {
        match Detector::open(store.clone(), watch.clone(), binding) {
            Ok(detector) => detectors.push(detector),
            Err(error) => {
                let mut retry = Backoff::default();
                retry.failed(
                    &format!("watch={} open", watch.definition.id),
                    error.to_string().chars().take(FAILURE_CHARS).collect(),
                );
                unopened.push(Unopened {
                    watch: watch.clone(),
                    retry,
                });
            }
        }
    }
    (detectors, unopened)
}

/// One shared bounded snapshot owner for worker updates and typed RPC status. The second
/// field names the watches whose publish hold the last update announced, so a warning is
/// written once per hold per daemon start (and again after the hold clears and is set). The
/// third names the failure of a worker pass that belongs to no one watch (a store refusal),
/// which every watch reports until the pass succeeds again.
#[derive(Clone, Default)]
pub struct WatchRegistry(
    Arc<Mutex<Vec<WatchStatus>>>,
    Arc<Mutex<Vec<String>>>,
    Arc<Mutex<Option<String>>>,
);

impl WatchRegistry {
    /// Capture at most eight configured roots; poisoning is an actionable failure.
    pub fn snapshot(&self) -> Result<Vec<WatchStatus>, Error> {
        let mut states = self
            .0
            .lock()
            .map(|value| value.clone())
            .map_err(|_| Error::State("consume status registry poisoned"))?;
        let worker = self
            .2
            .lock()
            .map_err(|_| Error::State("consume status registry poisoned"))?
            .clone();
        if let Some(worker) = worker {
            for state in &mut states {
                let text = match state.failure.take() {
                    Some(own) => format!("worker: {worker}; also: {own}"),
                    None => format!("worker: {worker}"),
                };
                state.failure = Some(text.chars().take(FAILURE_CHARS).collect());
            }
        }
        Ok(states)
    }

    /// Record, or with `None` clear, the failure of a worker pass that belongs to no one watch.
    fn note_worker_failure(&self, failure: Option<String>) {
        if let Ok(mut current) = self.2.lock() {
            *current = failure;
        }
    }

    /// The stored catalog's watches as a query-only daemon reports them: no root is opened or
    /// watched, so each is `reconciling` with no directories, candidates or worker counters.
    pub fn stored(store: &Store) -> Result<Self, Error> {
        let catalog = Catalog::load(store, &[])?;
        let pending = store.consumption_pending()?;
        let states = catalog
            .watches()
            .iter()
            .map(|watch| closed_status(watch, &pending, None))
            .collect();
        Ok(Self(
            Arc::new(Mutex::new(states)),
            Arc::new(Mutex::new(Vec::new())),
            Arc::default(),
        ))
    }

    /// Publish the registry snapshot and write the publish-hold warnings this update announces
    /// (a watch whose hold was not announced since the last clear). Returns the lines written.
    fn update(
        &self,
        detectors: &[Detector],
        unopened: &[Unopened],
        pending: &[sparrow_core::ConsumptionJournal],
    ) -> Result<Vec<String>, Error> {
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
                let (held_claims, oldest_held_first_seen) =
                    held_summary(detector.config.publish_hold, &detector.config.id, pending);
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
                    failure: detector.status_failure(),
                    tail_generation_ids,
                    tail_read_bytes,
                    tail_descriptors,
                    tail_checkpoint_records,
                    tail_admission_waiting: detector.tail_admission_waiting,
                    held_claims,
                    oldest_held_first_seen,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let states =
            states
                .into_iter()
                .chain(unopened.iter().map(|closed| {
                    closed_status(&closed.watch, pending, closed.retry.logged.clone())
                }))
                .collect::<Vec<_>>();
        let mut announced = self
            .1
            .lock()
            .map_err(|_| Error::State("publish hold announcements poisoned"))?;
        let mut lines = Vec::new();
        let mut held = Vec::new();
        for state in states.iter().filter(|state| state.definition.publish_hold) {
            held.push(state.id.clone());
            if !announced.contains(&state.id) {
                lines.push(format!(
                    "sparrowd: WARNING watch={} publish_hold is set: {} pending claims are held; the oldest was first seen at unix_millis {}; clear every hold before an older build runs",
                    state.id,
                    state.held_claims,
                    state
                        .oldest_held_first_seen
                        .map_or_else(|| "unknown".to_owned(), |at| at.to_string()),
                ));
            }
        }
        for line in &lines {
            eprintln!("{line}");
        }
        *announced = held;
        *self
            .0
            .lock()
            .map_err(|_| Error::State("consume status registry poisoned"))? = states;
        Ok(lines)
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

/// Both owners are boxed: each carries a store handle, and the enum would otherwise be as large
/// as the larger one (clippy::large_enum_variant).
enum Owner {
    Consume(Box<ClaimRoot>),
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
    /// The latest failure that names no claim (discovery, reservation, drain, tail attach).
    /// Any claim's success clears it (`record_claim_success`).
    failure: Option<String>,
    /// The latest failure a claim's own recovery set, kept beside `failure` so neither
    /// overwrites the other: only that claim's success or retirement clears it
    /// (`clear_failure_of`), never another claim's success or a claim-less failure.
    claim_failure: Option<ClaimFailure>,
    tail_admission_waiting: bool,
    /// Set by each worker pass: this publish-held watch may not reserve another file now
    /// (`held_may_claim`), so its ready files wait for the hold and need no wake of their own.
    claim_capped: bool,
    /// The text of the last failed move of each held claim, so a move that fails the same way
    /// on every pass is recorded and logged once per change of its text.
    held_move_failures: BTreeMap<String, String>,
    /// Set while this watch cannot work for a cause no claim owns: its root is unreadable or
    /// replaced, a scan or drain failed, or the store refused a step. The worker skips the
    /// watch, keeps every other watch running, and retries it on the backoff by rebuilding its
    /// coverage; the root binding is checked again first, so a replacement root is never
    /// adopted and the exact root restored resumes the watch.
    broken: Backoff,
}

/// A watch failure caused by one claim's recovery.
struct ClaimFailure {
    claim: String,
    /// The bounded failure text, which starts `claim ID: `.
    text: String,
}

/// Longest watch failure text status reports.
const FAILURE_CHARS: usize = 1024;

impl Detector {
    /// Move one claim of this publish-held watch into the private in-flight directory, exactly
    /// as an interrupted claim is, and stop there: publication waits for the hold to clear. The
    /// move is idempotent once recorded. A move that fails is recorded on the watch.
    fn move_held_claim(&mut self, store: &Store, id: &str) -> Result<(), Error> {
        let now = crate::now()?;
        let Err(error) = self
            .owner
            .claim()
            .and_then(|claim| claim.recover(id, now))
            .map(drop)
        else {
            self.held_move_failures.remove(id);
            return Ok(());
        };
        let text = error.to_string();
        // The status failure is memory only and is set every pass; the durable attempt record
        // and the log line are written once per change of the text, so a permanent failure
        // does not rewrite the journal on every pass.
        self.fail_claim(id, format!("claim {id}: {error}"));
        if self.held_move_failures.get(id) == Some(&text) {
            return Ok(());
        }
        eprintln!(
            "sparrowd: watch={} claim={id} held and not moved: {error}",
            self.config.id
        );
        if let Some(record) = store.consumption_attempt_pending(id, &text, now).err() {
            self.fail_claim(
                id,
                format!("claim {id}: {error}; attempt not recorded: {record}"),
            );
        }
        self.held_move_failures.insert(id.to_owned(), text);
        Ok(())
    }

    /// Park the watch after `error`: report it in status and the journal, and retry on the
    /// backoff (`resume`). Nothing else stops.
    fn degrade(&mut self, error: &Error) {
        let text: String = error.to_string().chars().take(FAILURE_CHARS).collect();
        self.covered = false;
        self.failure = Some(text.clone());
        self.broken
            .failed(&format!("watch={}", self.config.id), text);
    }

    /// Run STEP on this watch; a failure parks the watch (`degrade`) and yields `None`.
    fn attempt<T>(&mut self, step: impl FnOnce(&mut Self) -> Result<T, Error>) -> Option<T> {
        match step(self) {
            Ok(value) => Some(value),
            Err(error) => {
                self.degrade(&error);
                None
            }
        }
    }

    /// Whether the watch may run now. A parked watch is rebuilt once its backoff is due: its
    /// root binding is checked, coverage is re-established, and success clears the failure.
    fn resume(&mut self) -> bool {
        if !self.broken.failing() {
            return true;
        }
        if !self.broken.due() {
            return false;
        }
        match self.reconcile("recovery") {
            Ok(()) => {
                self.failure = None;
                self.broken.recovered(&format!("watch={}", self.config.id));
                true
            }
            Err(error) => {
                self.degrade(&error);
                false
            }
        }
    }

    /// Whether the watch takes events and claims: enabled and not parked.
    fn live(&self) -> bool {
        self.enabled && !self.broken.failing()
    }

    /// Record the failure of `claim`'s recovery. A failure that names no claim stays beside it.
    fn fail_claim(&mut self, claim: &str, text: String) {
        self.claim_failure = Some(ClaimFailure {
            claim: claim.to_owned(),
            text: text.chars().take(FAILURE_CHARS).collect(),
        });
    }

    /// The failure status reports: the claim failure first, then a failure that names no
    /// claim, so a claim that failed is never hidden by a later claim-less failure.
    fn status_failure(&self) -> Option<String> {
        match (&self.claim_failure, &self.failure) {
            (Some(claim), Some(other)) => Some(
                format!("{}; also: {other}", claim.text)
                    .chars()
                    .take(FAILURE_CHARS)
                    .collect(),
            ),
            (Some(claim), None) => Some(claim.text.clone()),
            (None, other) => other.clone(),
        }
    }

    /// Clear the claim failure only when `claim`'s own recovery set it. A failure from another
    /// claim or from discovery stays until its own cause resolves, so a claim that failed is
    /// never hidden by an unrelated claim's success.
    fn clear_failure_of(&mut self, claim: &str) {
        if self
            .claim_failure
            .as_ref()
            .is_some_and(|failed| failed.claim == claim)
        {
            self.claim_failure = None;
        }
    }

    /// A claim finished. Its own failure clears, and so does a failure that names no claim
    /// (discovery, reservation, drain, tail attach: the watch demonstrably works again), but
    /// another claim's failure stays until that claim resolves.
    fn record_claim_success(&mut self, claim: &str) {
        self.failure = None;
        self.clear_failure_of(claim);
        self.completed = self.completed.saturating_add(1);
    }

    fn open(store: Store, watch: ManagedWatch, binding: RootBinding) -> Result<Self, Error> {
        let config = watch.definition;
        let owner = match config.producer_contract {
            crate::ProducerContract::FinishedFiles => Owner::Consume(Box::new(
                ClaimRoot::open_bound(store, &config.id, &config.root, binding)?,
            )),
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
            claim_failure: None,
            tail_admission_waiting: false,
            claim_capped: false,
            held_move_failures: BTreeMap::new(),
            broken: Backoff::default(),
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
                Err(Error::FinishedGeneration(message)) => {
                    self.failure = Some(message.into());
                    Ok(())
                }
                Err(error) => {
                    self.failure = Some(error.to_string().chars().take(FAILURE_CHARS).collect());
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
        self.live()
            && (!self.covered
                || !self.todo.is_empty()
                || self.scan.is_some()
                || (space_ok
                    && !self.tail_admission_waiting
                    && matches!(&self.owner, Owner::Tail(owner) if !owner.caught_up())))
    }

    /// Whether a tail backlog waits for the shared operation budget.
    fn waits_for_permit(&self) -> bool {
        self.live()
            && self.tail_admission_waiting
            && matches!(&self.owner, Owner::Tail(owner) if !owner.caught_up())
    }

    /// The earliest instant this idle detector has work without a new event: its periodic
    /// rescan and, when CLAIMS admits new reservations, the end of its first candidate's
    /// quiet interval, or now when candidate capacity freed for deferred inputs.
    fn due(&self, claims: bool) -> Option<Instant> {
        if self.enabled && self.broken.failing() {
            return self.broken.at.map(tokio::time::Instant::into_std);
        }
        let claims = claims && !self.claim_capped;
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
        let (detectors, unopened) = open_detectors(&store, &catalog, binding);
        registry.update(&detectors, &unopened, &pending)?;
        let mut catalog_retry = Backoff::default();
        commit_initial_catalog(&mut catalog, &store, &mut catalog_retry);
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
                            Roster {
                                detectors,
                                unopened,
                            },
                            socket,
                            stopped,
                            registry,
                            (catalog, catalog_retry),
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
    /// The floor is paused: wait for its resume edge.
    resume: bool,
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
    unopened: &'a [Unopened],
    /// The retries of the catalog writes, which wake the wait when due.
    backoffs: [&'a Backoff; 2],
    stop: &'a mut tokio::sync::watch::Receiver<bool>,
    control: &'a mut Control,
    /// A command the wait received, handled first by the next pass.
    received: &'a mut Option<watch_admin::Command>,
    /// A permit the wait acquired, used by the next pass.
    permit: &'a mut Option<tokio::sync::OwnedSemaphorePermit>,
    /// Signalled by the floor's resume edge; a signal sent before the wait is kept.
    resumed: &'a tokio::sync::Notify,
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
    for detector in detectors.iter_mut().filter(|detector| detector.live()) {
        detector.attempt(Detector::drain);
    }
    // That drain may also have taken producer events from the pass: report what they
    // changed, and run again for any work they made due.
    waiting.registry.update(
        detectors,
        waiting.unopened,
        &waiting.store.consumption_pending()?,
    )?;
    if detectors.iter().any(|detector| detector.busy(space_ok)) {
        tokio::task::yield_now().await;
        return Ok(());
    }
    wake.permit = wake.permit || (space_ok && detectors.iter().any(Detector::waits_for_permit));
    for closed in waiting.unopened {
        closed.retry.wake(&mut wake);
    }
    for backoff in waiting.backoffs {
        backoff.wake(&mut wake);
    }
    for at in detectors.iter().filter_map(|detector| detector.due(claims)) {
        wake.by(at);
    }
    let events = detectors
        .iter()
        .filter(|detector| detector.live())
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
        () = waiting.resumed.notified(), if wake.resume => {}
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

/// Why a command was refused; the caller receives the typed error and the worker goes on.
enum AdminFailure {
    Refused(sparrow_core::Error),
}

fn admit_handoff(
    store: &Store,
    detectors: &[Detector],
    intent: &sparrow_core::ConsumptionHandoff,
) -> Result<sparrow_core::ConsumptionJournal, AdminFailure> {
    let now = crate::now().map_err(AdminFailure::Refused)?;
    if let Some(original) = store
        .consumption_handoff_get(intent, now)
        .map_err(AdminFailure::Refused)?
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
            Error::Storage(error) => AdminFailure::Refused(error),
            Error::Io(error) if error.kind() == std::io::ErrorKind::NotFound => {
                AdminFailure::Refused(watch_admin::failure("NOT_FOUND", error))
            }
            error => AdminFailure::Refused(watch_admin::failure("CONFLICT", error)),
        })
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
            Error::Storage(error) => AdminFailure::Refused(error),
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
    if let Some(original) = store
        .tail_finish_get(intent)
        .map_err(AdminFailure::Refused)?
    {
        return Ok(original);
    }
    let generation = store
        .tail_generation(&intent.generation_id)
        .map_err(AdminFailure::Refused)?;
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
        Error::Storage(error) => AdminFailure::Refused(error),
        error => AdminFailure::Refused(watch_admin::failure("CONFLICT", error)),
    })
}

fn acknowledge<T>(
    reply: tokio::sync::oneshot::Sender<Result<T, sparrow_core::Error>>,
    result: Result<T, AdminFailure>,
    store: &Store,
    registry: &WatchRegistry,
    roster: &Roster,
) -> Result<(), Error> {
    match result {
        Ok(value) => {
            registry.update(
                &roster.detectors,
                &roster.unopened,
                &store.consumption_pending()?,
            )?;
            let _ = reply.send(Ok(value));
        }
        Err(AdminFailure::Refused(error)) => {
            let _ = reply.send(Err(error));
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
            .map_err(AdminFailure::Refused)?
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
    // Catalog, stable receipt and old-writer fence share one durable DB batch. A refused write
    // is the command's refusal: the in-memory catalog stays as stored, and a write whose
    // outcome is uncertain fails the next compare-and-set instead of admitting with stale
    // authority.
    next.persist(store).map_err(|error| match error {
        Error::Storage(error) => AdminFailure::Refused(error),
        error => AdminFailure::Refused(watch_admin::failure("STORAGE_FAILURE", error)),
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
        // The definition is stored; a root that cannot be read now parks this watch, which
        // retries on its backoff, and does not undo the command.
        let rebuilt = if watch.enabled {
            detector.reconcile("administrative update")
        } else {
            detector.pause()
        };
        if let Err(error) = rebuilt {
            detector.degrade(&error);
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

/// Every watch the worker serves: those whose root is open, and those whose root could not
/// be opened and is retried.
struct Roster {
    detectors: Vec<Detector>,
    unopened: Vec<Unopened>,
}

impl Roster {
    /// Align the roster with CATALOG after it changed: drop what the catalog no longer names,
    /// and keep each unopened watch on its current definition.
    fn sync(&mut self, catalog: &Catalog) {
        let named = |id: &str| {
            catalog
                .watches()
                .iter()
                .any(|watch| watch.definition.id == id)
        };
        self.detectors.retain(|detector| named(&detector.config.id));
        let detectors = &self.detectors;
        self.unopened.retain(|closed| {
            named(&closed.watch.definition.id)
                && !detectors
                    .iter()
                    .any(|detector| detector.config.id == closed.watch.definition.id)
        });
        for closed in &mut self.unopened {
            if let Some(current) = catalog
                .watches()
                .iter()
                .find(|watch| watch.definition.id == closed.watch.definition.id)
            {
                closed.watch = current.clone();
            }
        }
    }

    /// Hand a parked watch that holds no unfinished claim back to the open path. Its held root
    /// descriptor may name a directory that was removed or replaced, which a rebuild from that
    /// descriptor can never leave; opening from the configured path, as a restart does, adopts
    /// the root as it is now, and refuses it while an unfinished claim binds the watch to its
    /// original root. A watch with claims stays parked and resumes only on the exact root.
    fn release_parked(&mut self, store: &Store) -> Result<(), Error> {
        let due = self
            .detectors
            .iter()
            .any(|detector| detector.broken.failing() && detector.broken.due());
        if !due {
            return Ok(());
        }
        let pending = store.consumption_pending()?;
        let (parked, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut self.detectors)
            .into_iter()
            .partition(|detector| {
                detector.broken.failing()
                    && detector.broken.due()
                    && !pending
                        .iter()
                        .any(|journal| journal.watch == detector.config.id)
            });
        self.detectors = kept;
        for detector in parked {
            let what = format!("watch={} open", detector.config.id);
            let mut retry = Backoff::default();
            retry.failed(&what, detector.status_failure().unwrap_or_default());
            self.unopened.push(Unopened {
                watch: ManagedWatch {
                    definition: detector.config.clone(),
                    revision: detector.revision,
                    enabled: detector.enabled,
                },
                retry,
            });
        }
        Ok(())
    }

    /// Open again each unopened watch whose retry is due. A root that is still missing,
    /// unreadable or replaced stays unopened (a replacement is refused by the same checks as
    /// at start, and pending claims stay bound to the original root); the exact root restored
    /// opens it and the watch resumes.
    fn reopen(&mut self, store: &Store, binding: RootBinding) {
        for closed in std::mem::take(&mut self.unopened) {
            let mut closed = closed;
            if !closed.retry.due() {
                self.unopened.push(closed);
                continue;
            }
            let what = format!("watch={} open", closed.watch.definition.id);
            match Detector::open(store.clone(), closed.watch.clone(), binding) {
                Ok(detector) => {
                    closed.retry.recovered(&what);
                    self.detectors.push(detector);
                }
                Err(error) => {
                    closed.retry.failed(
                        &what,
                        error.to_string().chars().take(FAILURE_CHARS).collect(),
                    );
                    self.unopened.push(closed);
                }
            }
        }
    }
}

/// Store the catalog seeded from the configuration, once. A refused write, `STORAGE_PRESSURE`
/// from a store over its maintenance reserve included, must not end the daemon, which has to
/// come up to reclaim. The catalog stays unstored (`needs_initial_commit`), the failure is
/// journaled, and the worker tries again on the backoff until the write succeeds.
fn commit_initial_catalog(catalog: &mut Catalog, store: &Store, retry: &mut Backoff) {
    if !catalog.needs_initial_commit() || !retry.due() {
        return;
    }
    match catalog.persist(store) {
        Ok(()) => retry.recovered("watch catalog store"),
        Err(error) => retry.failed("watch catalog store", error.to_string()),
    }
}

/// What one worker pass asks the loop to do next.
enum Flow {
    Next,
    Stop,
}

/// Run the worker until stop is requested. A failure the pass cannot attribute to one claim or
/// one watch (the store refusing reads or writes, a latched store, a closed budget) does not
/// end the worker: it is journaled, reported on every watch's status, answered to any waiting
/// command, and the pass runs again on the backoff, so a store that becomes healthy again
/// resumes the work with its claims and retry clocks intact.
async fn run(
    store: Store,
    mut roster: Roster,
    socket: PathBuf,
    mut stop: tokio::sync::watch::Receiver<bool>,
    registry: WatchRegistry,
    (mut catalog, mut catalog_retry): (Catalog, Backoff),
    mut control: Control,
) -> Result<(), Error> {
    let mut failing = Backoff::default();
    // A command a failed pass left unanswered.
    let mut received = None;
    let mut state = PassState::new(&store);
    while !*stop.borrow() {
        let outcome = pass(
            &store,
            &mut roster,
            &socket,
            &mut stop,
            &registry,
            (&mut catalog, &mut catalog_retry),
            &mut control,
            &mut received,
            &mut state,
        )
        .await;
        match outcome {
            Ok(Flow::Next) => {
                if failing.failing() {
                    failing.recovered("consume worker");
                    registry.note_worker_failure(None);
                }
            }
            Ok(Flow::Stop) => break,
            Err(error) => {
                let text: String = error.to_string().chars().take(FAILURE_CHARS).collect();
                registry.note_worker_failure(Some(text.clone()));
                failing.failed("consume worker pass", text);
                // A command that waits for this pass is answered at once with the failure.
                while let Ok(command) = control.commands.try_recv() {
                    refuse_command(command, &error);
                }
                if let Some(command) = received.take() {
                    refuse_command(command, &error);
                }
                let mut wake = Wake::default();
                failing.wake(&mut wake);
                let at = wake.at.map(tokio::time::Instant::from_std);
                tokio::select! {
                    () = stopped(&mut stop) => {}
                    command = control.commands.recv() => match command {
                        Some(command) => refuse_command(command, &error),
                        None => return Err(Error::State(
                            "watch administration channel closed unexpectedly",
                        )),
                    },
                    () = async {
                        match at {
                            Some(at) => tokio::time::sleep_until(at).await,
                            None => std::future::pending().await,
                        }
                    } => {}
                }
            }
        }
    }
    Ok(())
}

/// Answer COMMAND with ERROR: the worker is failing and cannot act on it now. A store error
/// keeps its own code, so the caller sees what the store said.
fn refuse_command(command: watch_admin::Command, error: &Error) {
    let refusal = match error {
        Error::Storage(error) => error.clone(),
        error => watch_admin::failure("STORAGE_FAILURE", error),
    };
    match command {
        watch_admin::Command::Mutation { reply, .. } => drop(reply.send(Err(refusal))),
        watch_admin::Command::Handoff { reply, .. } => drop(reply.send(Err(refusal))),
        watch_admin::Command::TailFinish { reply, .. } => drop(reply.send(Err(refusal))),
        watch_admin::Command::RetireClaim { reply, .. } => drop(reply.send(Err(refusal))),
    }
}

/// What the worker keeps across passes, failed ones included.
struct PassState {
    /// The consume connection and when it was opened; the daemon's lease runs from there.
    client: Option<(Client, Instant)>,
    /// Next attempt, consecutive failures, best progress and shared parts per pending claim.
    retry: BTreeMap<String, ClaimRetry>,
    connect_after: Instant,
    /// How far the retired-claim record's fill was last logged this run (`note_retired`);
    /// None until the first pass reads it.
    retired_logged: Option<RetiredFill>,
    /// A permit the last wait acquired, used by the next pass.
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    /// The floor's resume edge wakes a paused worker.
    resumed: Arc<tokio::sync::Notify>,
    /// Unsubscribes the resume edge when the worker returns.
    _resume: sparrow_core::ResumeSubscription,
    /// Retries of the catalog retirement write.
    retirement: Backoff,
}

impl PassState {
    fn new(store: &Store) -> Self {
        let resumed = Arc::new(tokio::sync::Notify::new());
        let resume = store.space_floor().subscribe_resume(Arc::new({
            let resumed = Arc::clone(&resumed);
            move || resumed.notify_one()
        }));
        Self {
            client: None,
            retry: BTreeMap::new(),
            connect_after: Instant::now(),
            retired_logged: None,
            permit: None,
            resumed,
            _resume: resume,
            retirement: Backoff::default(),
        }
    }
}

/// One pass of the worker. A returned error belongs to no one claim or watch.
#[allow(clippy::too_many_arguments)]
async fn pass(
    store: &Store,
    roster: &mut Roster,
    socket: &Path,
    stop: &mut tokio::sync::watch::Receiver<bool>,
    registry: &WatchRegistry,
    (catalog, catalog_retry): (&mut Catalog, &mut Backoff),
    control: &mut Control,
    received: &mut Option<watch_admin::Command>,
    state: &mut PassState,
) -> Result<Flow, Error> {
    let PassState {
        client,
        retry,
        connect_after,
        retired_logged,
        permit,
        resumed,
        retirement: retirement_retry,
        ..
    } = state;
    let binding = PrivateIntake::binding(control.private.as_ref());
    #[cfg(test)]
    tests::PASSES.with(|passes| passes.set(passes.get() + 1));
    let mut wake = Wake::default();
    // Below the free-space floor every write is refused: keep observing, write nothing.
    // The worker observes through the floor's ledger, so the pause it sees is recorded and
    // the edge that ends it is signalled to `resumed` by whichever duty sees it end (the
    // daemon's one-second floor observation catches free space returning from outside).
    // No timer waits for it.
    let space_ok = store.space_floor().observe(crate::now()?)?.is_none();
    wake.resume = !space_ok;
    if retired_logged.is_none() {
        let mut fill = RetiredFill::Below;
        note_retired(store, &mut fill)?;
        *retired_logged = Some(fill);
    }
    roster.release_parked(store)?;
    roster.reopen(store, binding);
    if space_ok {
        commit_initial_catalog(catalog, store, catalog_retry);
    }
    catalog_retry.wake(&mut wake);
    let pending = store.consumption_pending()?;
    if space_ok && retirement_retry.due() {
        match catalog.retire_and_store(&pending, |next| next.persist(store)) {
            Ok(Retirement::Nothing) => retirement_retry.recovered("watch retirement store"),
            Ok(Retirement::Stored) => {
                retirement_retry.recovered("watch retirement store");
                roster.sync(catalog);
            }
            Ok(Retirement::Deferred(reason)) => {
                retirement_retry.failed("watch retirement store", reason);
            }
            Err(error) => retirement_retry.failed("watch retirement store", error.to_string()),
        }
    }
    retirement_retry.wake(&mut wake);
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
                let pending = match store.consumption_pending() {
                    Ok(pending) => pending,
                    Err(error) => {
                        let _ = reply.send(Err(error.clone()));
                        return Err(error.into());
                    }
                };
                let rebound = match (&control.private, &mut action) {
                    (Some(intake), Action::Put { definition, .. }) => intake
                        .rebind(definition)
                        .map_err(|error| {
                            AdminFailure::Refused(watch_admin::failure("INVALID_ARGUMENT", error))
                        })
                        .and_then(|configured| {
                            catalog
                                .admit_private_root(&definition.id, &definition.root, &configured)
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
                        store,
                        catalog,
                        &mut roster.detectors,
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
                if result.is_ok() {
                    roster.sync(catalog);
                }
                acknowledge(reply, result, store, registry, roster)?;
            }
            watch_admin::Command::Handoff { intent, reply } => {
                let result = admit_handoff(store, &roster.detectors, &intent);
                acknowledge(reply, result, store, registry, roster)?;
            }
            watch_admin::Command::TailFinish { intent, reply } => {
                let result = admit_tail_finish(store, &mut roster.detectors, &intent);
                acknowledge(reply, result, store, registry, roster)?;
            }
            watch_admin::Command::RetireClaim { retirement, reply } => {
                let result = retire_claim(&mut roster.detectors, &retirement);
                if result.is_ok() {
                    retry.remove(&retirement.claim);
                    if let Some(fill) = retired_logged.as_mut() {
                        note_retired(store, fill)?;
                    }
                }
                acknowledge(reply, result, store, registry, roster)?;
            }
        }
    }
    for detector in &mut roster.detectors {
        // A parked watch is rebuilt when its backoff is due, and skipped until then: its
        // failure never stops another watch.
        if !detector.enabled || !detector.resume() {
            continue;
        }
        let swept = detector.attempt(|detector| {
            detector.drain()?;
            if detector.covered
                && detector.todo.is_empty()
                && detector.scan.is_none()
                && Instant::now() >= detector.next_reconcile
            {
                detector.rescan("periodic scan")?;
            }
            detector.scan_step()?;
            detector.drain()
        });
        if swept.is_none() || !space_ok {
            continue;
        }
        let mut drained: Option<Error> = None;
        if let Owner::Tail(owner) = &mut detector.owner {
            // A caught-up owner has nothing to capture until an event, so needs no permit.
            if owner.caught_up() {
                detector.tail_admission_waiting = false;
                continue;
            }
            let _permit = match admit(&control.operations, permit) {
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
            drained = owner.drain().err();
        }
        // The next pass drains again once the watch is rebuilt on its backoff.
        if let Some(error) = drained {
            detector.degrade(&error);
        }
    }
    let detectors = &mut roster.detectors;
    let pending = store.consumption_pending()?;
    registry.update(detectors, &roster.unopened, &pending)?;
    let busy = detectors.iter().any(|detector| detector.busy(space_ok));
    let waiting = Waiting {
        store,
        registry,
        unopened: &roster.unopened,
        backoffs: [&*catalog_retry, &*retirement_retry],
        stop: &mut *stop,
        control: &mut *control,
        received: &mut *received,
        permit: &mut *permit,
        resumed,
    };
    if !space_ok
        || detectors.is_empty()
        || (pending.is_empty()
            && detectors
                .iter()
                .all(|detector| matches!(detector.owner, Owner::Tail(_))))
    {
        pause(waiting, detectors, space_ok, busy, false, wake).await?;
        return Ok(Flow::Next);
    }
    retry.retain(|id, _| pending.iter().any(|journal| journal.import_id == *id));
    // Claims that a publish hold holds are kept in flight, as an interrupted claim is; the
    // publish pick below skips them until the hold clears.
    for detector in detectors
        .iter_mut()
        .filter(|detector| detector.config.publish_hold)
    {
        let held = pending
            .iter()
            .filter(|journal| journal.watch == detector.config.id && journal.tail.is_none())
            .map(|journal| journal.import_id.clone())
            .collect::<Vec<_>>();
        detector
            .held_move_failures
            .retain(|id, _| held.contains(id));
        for id in held {
            let _ = detector.attempt(|detector| detector.move_held_claim(store, &id));
        }
    }
    let mut work = publishable(&pending, detectors, |id| {
        retry.get(id).is_none_or(|entry| entry.at <= Instant::now())
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
    let (mut floor_wait, mut floor_ended) = (false, false);
    if claims {
        for detector in detectors.iter_mut() {
            if !detector.live() {
                continue;
            }
            // A held watch past its share stays unclaimed, and the next watch is tried:
            // one watch's pause never stops another watch or a tail generation.
            detector.claim_capped =
                detector.config.publish_hold && !held_may_claim(&detector.config.id, &pending);
            if detector.claim_capped {
                continue;
            }
            let Some(ready) = detector.attempt(Detector::ready) else {
                continue;
            };
            if let Some((path, identity)) = ready {
                if *waiting.stop.borrow() {
                    break;
                }
                let reserved = detector.owner.claim().and_then(|owner| {
                    owner.reserve(
                        &path,
                        identity,
                        &detector.config.source,
                        &detector.config.format,
                    )
                });
                match reserved {
                    Ok(journal) => {
                        detector.candidates.remove(&path);
                        // Under a publish hold the claim is moved in flight now and not published.
                        if detector.config.publish_hold {
                            let _ = detector.attempt(|detector| {
                                detector.move_held_claim(store, &journal.import_id)
                            });
                        } else {
                            work = Some((detector.config.id.clone(), journal.import_id, false));
                        }
                    }
                    Err(error) => {
                        // A floor refusal records the pause now, so this pass waits on its
                        // resume edge; an edge that already fired runs the next pass at once.
                        if let crate::consumption::Error::Storage(storage) = &error
                            && storage.is_free_space_floor()
                        {
                            match store.space_floor().observe(crate::now()?)? {
                                Some(_) => floor_wait = true,
                                None => floor_ended = true,
                            }
                        }
                        detector.failure =
                            Some(error.to_string().chars().take(FAILURE_CHARS).collect());
                        eprintln!(
                            "sparrowd: watch={} reservation refused for {:?}: {error}",
                            detector.config.id, path
                        );
                        let _ = detector.attempt(|detector| detector.candidate(path, true));
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
                let _ = detector
                    .attempt(|detector| detector.rescan("deferred candidate capacity available"));
            }
        }
    }
    if let Some((watch, id, is_tail)) = work {
        let roots = detectors
            .iter()
            .map(|detector| detector.config.root.clone())
            .collect::<Vec<_>>();
        // The entry exists before the first attempt, so the attempt has a cache and a mark.
        let attempt = Arc::clone(
            &retry
                .entry(id.clone())
                .or_insert_with(ClaimRetry::due)
                .attempt,
        );
        attempt.progress.restart();
        if !is_tail && retry.get(&id).is_some_and(|entry| entry.refusal.is_some()) {
            let detector = detectors
                .iter_mut()
                .find(|detector| detector.config.id == watch)
                .ok_or(Error::State(
                    "pending consumption journal has no configured watch owner",
                ))?;
            let refusal = retry
                .get(&id)
                .and_then(|entry| entry.refusal.clone())
                .ok_or(Error::State("refused claim lost its refusal"))?;
            if quarantine_claim(detector, &roots, &id, &refusal)? {
                if let Some(fill) = retired_logged.as_mut() {
                    note_retired(store, fill)?;
                }
                retry.remove(&id);
            } else {
                retry
                    .entry(id)
                    .or_insert_with(ClaimRetry::due)
                    .failed(ProgressMark::default(), false);
            }
            tokio::task::yield_now().await;
            return Ok(Flow::Next);
        }
        if !is_tail {
            // A permit the last wait acquired for a tail drain is not held through an
            // ordinary connection or publication.
            *waiting.permit = None;
            // Replace a connection the lease would cut during this publication.
            if client.as_ref().is_some_and(|(_, opened)| {
                opened.elapsed() + PUBLICATION_DEADLINE > crate::transport::CONNECTION_LEASE
            }) {
                *client = None;
            }
            if client.is_none() && Instant::now() >= *connect_after {
                let opened = Instant::now();
                tokio::select! {
                    () = stopped(waiting.stop) => return Ok(Flow::Stop),
                    result = tokio::time::timeout(Duration::from_secs(10), Client::connect(socket)) => {
                        match result {
                            Ok(Ok(connected)) => *client = Some((connected, opened)),
                            Ok(Err(error)) => eprintln!("sparrowd: consume RPC discovery failed: {error}"),
                            Err(_) => eprintln!("sparrowd: consume RPC discovery timed out; sources remain owned"),
                        }
                        *connect_after = Instant::now() + CONNECT_RETRY;
                    }
                }
            }
            if client.is_none() {
                // Every other due claim waits behind this one, so only later retries
                // and the next connection attempt can bring new work.
                wake.by(*connect_after);
                blocked(retry, &mut wake);
                pause(waiting, detectors, space_ok, busy, false, wake).await?;
                return Ok(Flow::Next);
            }
        }
        let _permit = if is_tail {
            match admit(&waiting.control.operations, waiting.permit) {
                Ok(permit) => Some(permit),
                Err(tokio::sync::TryAcquireError::NoPermits) => {
                    wake.permit = true;
                    blocked(retry, &mut wake);
                    pause(waiting, detectors, space_ok, busy, false, wake).await?;
                    return Ok(Flow::Next);
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
                            &attempt,
                        )
                        .await?;
                }
                detector.owner.claim()?.remove(&claimed, crate::now()?)?;
            }
            Ok::<_, Error>(())
        };
        let result = tokio::select! {
            () = stopped(waiting.stop) => return Ok(Flow::Stop),
            result = tokio::time::timeout(PUBLICATION_DEADLINE, operation) => result.unwrap_or(Err(Error::State("consume publication deadline expired; retain the original journal and retry"))),
        };
        if let Err(error) = result {
            // The counts lead the text, which the journal keeps by prefix; the deadline
            // text cannot carry them, because the dropped attempt cannot add them.
            let text = if is_tail {
                error.to_string()
            } else {
                format!("{}: {error}", attempt.progress.summary())
            };
            // Name the claim, so an operator can act on it (retire-claim takes its ID).
            detector.fail_claim(&id, format!("claim {id}: {text}"));
            eprintln!("sparrowd: watch={watch} claim={id} pending: {text}");
            // The claim's durable history answers `watch claims` after any restart.
            if let Some(record) = store
                .consumption_attempt_pending(&id, &text, crate::now()?)
                .err()
            {
                eprintln!(
                    "sparrowd: watch={watch} claim={id} pending attempt not recorded: {record}"
                );
                detector.fail_claim(
                    &id,
                    format!("claim {id}: {text}; attempt not recorded: {record}"),
                );
            }
            let floor =
                floor_class(&error) && store.space_floor().observe(crate::now()?)?.is_some();
            let refusal = if is_tail {
                None
            } else {
                match Refusal::of(&error) {
                    Some(refusal) => Some(refusal),
                    // An earlier run may have moved the source and stopped before it
                    // retired the claim; its refusal is on file beside the source.
                    None => match detector.owner.claim()?.interrupted_quarantine(
                        &detector.config.root,
                        &roots,
                        &id,
                    ) {
                        Ok(found) => found,
                        Err(error) => {
                            eprintln!(
                                "sparrowd: watch={watch} claim={id} quarantine record unreadable: {error}"
                            );
                            None
                        }
                    },
                }
            };
            let entry = retry.entry(id).or_insert_with(ClaimRetry::due);
            entry.failed(attempt.progress.mark(), floor);
            if refusal.is_some() {
                // Settle it on the next pass: no further upload of this claim.
                entry.at = Instant::now();
                entry.refusal = refusal;
            }
            if !is_tail {
                *client = None;
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
        return Ok(Flow::Next);
    }
    if floor_ended {
        // The floor ended between the refusal and its observation: run again at once.
        return Ok(Flow::Next);
    }
    wake.resume |= floor_wait;
    for entry in retry.values() {
        wake.by(entry.at);
    }
    pause(waiting, detectors, space_ok, busy, claims, wake).await?;
    Ok(Flow::Next)
}

/// How full the store's retired-claim record is, as the worker logs it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum RetiredFill {
    Below,
    /// Three quarters of the bound or more. Both the worker and an operator retire claims, and
    /// no row is ever removed, so the record only fills; three quarters leaves a quarter of the
    /// bound (256 retirements) to act in after the warning.
    Warn,
    /// The bound: no further claim can be retired.
    Full,
}

impl RetiredFill {
    #[must_use]
    fn of(count: usize, max: usize) -> Self {
        if count >= max {
            Self::Full
        } else if count.saturating_mul(4) >= max.saturating_mul(3) {
            Self::Warn
        } else {
            Self::Below
        }
    }
}

/// Log the retired-claim record's fill once for each level it reaches in this run, with what
/// happens next. Called at start and after each retirement.
fn note_retired(store: &Store, logged: &mut RetiredFill) -> Result<(), Error> {
    let max = sparrow_core::MAX_RETIRED_CONSUMPTION_CLAIMS;
    let count = store.consumption_retired()?.len();
    let fill = RetiredFill::of(count, max);
    if fill > *logged {
        let next = if fill == RetiredFill::Full {
            "no claim can be retired now: a refused claim parks in place holding one of the 16 journal slots"
        } else {
            "at the bound a refused claim parks in place holding one of the 16 journal slots"
        };
        eprintln!(
            "sparrowd: WARNING retired claims {count} of {max}: {next}, and with 16 parked claims no watch claims a new file; see docs/interfaces.md, Quarantining a permanently refused source"
        );
    }
    *logged = fill.max(*logged);
    Ok(())
}

/// Move the refused source of claim `id` into quarantine and retire the claim. True when the
/// claim is retired. Any failure stays on the claim and the watch's status, is logged once per change of its text, and leaves the source where it is.
fn quarantine_claim(
    detector: &mut Detector,
    roots: &[PathBuf],
    id: &str,
    refusal: &Refusal,
) -> Result<bool, Error> {
    let now = crate::now()?;
    let outcome = detector
        .owner
        .claim()
        .and_then(|claim| claim.quarantine(&detector.config.root, roots, id, refusal, now));
    match outcome {
        Ok(retired) => {
            detector.held_move_failures.remove(id);
            detector.clear_failure_of(id);
            eprintln!(
                "sparrowd: watch={} claim={id} quarantined caller={:?} reason={:?} path={:?}",
                detector.config.id,
                retired.retirement.caller,
                retired.retirement.reason,
                String::from_utf8_lossy(&retired.journal.relative_path),
            );
            Ok(true)
        }
        Err(error) => {
            let text = refusal.failure(&error.to_string());
            detector.fail_claim(id, format!("claim {id}: {text}"));
            if detector.held_move_failures.get(id) != Some(&text) {
                eprintln!(
                    "sparrowd: watch={} claim={id} pending: {text}",
                    detector.config.id
                );
                detector.held_move_failures.insert(id.to_owned(), text);
            }
            Ok(false)
        }
    }
}

/// Wake for the retries still to come while the due claim waits.
fn blocked(retry: &BTreeMap<String, ClaimRetry>, wake: &mut Wake) {
    let now = Instant::now();
    for entry in retry.values().filter(|entry| entry.at > now) {
        wake.by(entry.at);
    }
}

/// One pending claim's retry state (#280). It is created when the claim is first chosen, kept
/// through failures of every class and pruned once the claim is no longer pending.
#[derive(Debug)]
struct ClaimRetry {
    /// When the claim is next due.
    at: Instant,
    /// Consecutive failures without progress.
    failures: u32,
    /// The highest progress any attempt reached.
    best: ProgressMark,
    /// Progress and planned parts shared with each attempt.
    attempt: Arc<crate::consumption::ClaimAttempt>,
    /// The daemon's permanent refusal of this claim's input. Set, the claim is never published
    /// again: each due time moves its source into quarantine and retires it instead.
    refusal: Option<Refusal>,
}

impl ClaimRetry {
    /// A claim due now, before its first attempt.
    fn due() -> Self {
        Self {
            at: Instant::now(),
            failures: 0,
            best: ProgressMark::default(),
            attempt: Arc::default(),
            refusal: None,
        }
    }

    /// Back off after a failed attempt that reached `mark`. Only progress past the best mark
    /// resets the failure count; a bare retry keeps growing the delay. A failure under a
    /// recorded floor pause keeps the claim due, so the first pass after the resume edge takes
    /// it with its cache and mark, and does not grow the delay.
    fn failed(&mut self, mark: ProgressMark, floor: bool) {
        if mark > self.best {
            self.failures = 0;
            self.best = mark;
        }
        if floor {
            self.at = Instant::now();
            return;
        }
        self.failures = self.failures.saturating_add(1);
        self.at = Instant::now() + retry_delay(self.failures);
    }
}

/// Whether a claim attempt failed because the free-space floor paused writes.
fn floor_class(error: &Error) -> bool {
    match error {
        Error::Upload(error) => error.is_free_space_floor(),
        Error::Storage(error) => error.is_free_space_floor(),
        _ => false,
    }
}
