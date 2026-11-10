// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Background builds for listings that must first read what an unfinished migration owes
//! (#158). Such a read costs the unmigrated part of the store, which at the live size takes
//! longer than any caller's deadline. The first call registers the build and starts it at
//! once on its own thread, which holds its own snapshot. Each call then waits for the build
//! for at most [`BUILD_WAIT`]. A call that finds it unfinished answers with no rows, the
//! build's cursor and its progress, so the caller knows it is being served and how far the
//! build has read. Following the cursor resumes the same build and never restarts it. A build
//! whose cursor is abandoned is cancelled at its lease expiry, which ends its thread and
//! drops its snapshot.

use super::{
    Error,
    catalog_lease::{LEASE_MS, lock_whole as lock},
    random_id,
};
use crate::QueryCancellation;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

/// Longest one listing call waits for its build before it answers with the build's cursor.
/// Well inside the 30-second MCP tool deadline and the 60-second connection lease.
pub(super) const BUILD_WAIT: Duration = Duration::from_millis(5_000);

/// Entries a build reads between checks of its cancellation and lease.
const CHECK_EVERY: u64 = 4_096;

/// How far a listing's build has read, in the unit of the migration it completes: the
/// catalog entries it has examined, of the entries that migration still owed when the
/// build's snapshot was taken. `total` is `None` while the store counters seed, when the
/// remainder has no known size.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BuildProgress {
    /// Entries read so far.
    pub read: u64,
    /// Entries the build reads in all, once known.
    pub total: Option<u64>,
}

enum State<T> {
    Running,
    /// Kept until the build is retired, so a call that fails to deliver its first page
    /// leaves it for the next call on the same cursor.
    Done(Result<T, Error>),
}

/// Dev-only: hold a build's thread once it has read this many entries.
#[cfg(feature = "test-support")]
pub(super) type Hold = (u64, Arc<dyn Fn() + Send + Sync>);

struct Shared<T> {
    state: Mutex<State<T>>,
    ready: Condvar,
    read: AtomicU64,
    /// The total plus one once known; zero while unknown.
    total: AtomicU64,
    cancellation: QueryCancellation,
    /// When a call last waited on the build: its lease runs from then.
    polled: Mutex<Instant>,
    #[cfg(feature = "test-support")]
    hold: Option<Hold>,
}

impl<T> Shared<T> {
    fn progress(&self) -> BuildProgress {
        BuildProgress {
            read: self.read.load(Ordering::Relaxed),
            total: self.total.load(Ordering::Relaxed).checked_sub(1),
        }
    }

    fn expired(&self) -> bool {
        lock(&self.polled).elapsed() >= Duration::from_millis(LEASE_MS)
    }
}

/// What a build's thread uses to report progress and learn that it must stop.
pub(super) struct Worker<T>(Arc<Shared<T>>);

impl<T> Worker<T> {
    /// Count one catalog entry read; every [`CHECK_EVERY`] entries, refuse once the build
    /// is cancelled or its lease has ended.
    pub(super) fn entry(&self) -> Result<(), Error> {
        let read = self.0.read.fetch_add(1, Ordering::Relaxed) + 1;
        #[cfg(feature = "test-support")]
        if let Some((at, hold)) = &self.0.hold
            && read == *at
        {
            hold();
        }
        if read.is_multiple_of(CHECK_EVERY) {
            self.checkpoint()?;
        }
        Ok(())
    }

    /// Refuse once the build is cancelled or its lease has ended.
    pub(super) fn checkpoint(&self) -> Result<(), Error> {
        self.0.cancellation.checkpoint()?;
        if self.0.expired() {
            return Err(Error::new(
                "RESULT_EXPIRED",
                "listing build lease ended before the build finished",
            ));
        }
        Ok(())
    }

    /// The build's own cancellation, cancelled when its registration is dropped.
    pub(super) fn cancellation(&self) -> QueryCancellation {
        self.0.cancellation.clone()
    }

    /// Record how many entries the build reads in all.
    pub(super) fn total(&self, total: Option<u64>) {
        if let Some(total) = total {
            self.0
                .total
                .store(total.saturating_add(1), Ordering::Relaxed);
        }
    }
}

/// One registered build: what its first page needs besides the build's result, its lease,
/// and the state it shares with its thread. Dropping it cancels the thread.
pub(super) struct Build<C, T> {
    pub(super) context: C,
    expires_at: u64,
    shared: Arc<Shared<T>>,
}

impl<C, T> Build<C, T> {
    fn alive(&self, now: u64) -> bool {
        now < self.expires_at && !self.shared.expired()
    }
}

impl<C, T> Drop for Build<C, T> {
    fn drop(&mut self) {
        self.shared.cancellation.cancel();
    }
}

/// A call's view of one build, used without the registry lock.
pub(super) struct Waiter<T>(Arc<Shared<T>>);

impl<T: Clone> Waiter<T> {
    /// Wait until the build finishes, at most `budget`, refusing if the caller cancels.
    /// `Some` holds a copy of the build's result, which stays with the build until it is
    /// retired; `None` means it still runs.
    pub(super) fn wait(
        &self,
        budget: Duration,
        cancellation: &QueryCancellation,
    ) -> Result<Option<Result<T, Error>>, Error> {
        let deadline = Instant::now() + budget;
        let mut state = lock(&self.0.state);
        loop {
            match &*state {
                State::Done(Ok(result)) => return Ok(Some(Ok(result.clone()))),
                State::Done(Err(error)) => {
                    return Ok(Some(Err(Error::new(error.code, error.message.clone()))));
                }
                State::Running => {}
            }
            cancellation.checkpoint()?;
            let now = Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            let slice = (deadline - now).min(Duration::from_millis(50));
            state = self
                .0
                .ready
                .wait_timeout(state, slice)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
    }
}

impl<T> Waiter<T> {
    pub(super) fn progress(&self) -> BuildProgress {
        self.0.progress()
    }
}

/// Decrements the running count when a build's thread ends, however it ends.
struct Running(Arc<AtomicUsize>);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One listing kind's builds, at most [`super::catalog_lease::MAX_HANDLES`] at a time.
pub(super) struct Builds<C, T> {
    builds: BTreeMap<String, Build<C, T>>,
    running: Arc<AtomicUsize>,
    started: u64,
    wait: Duration,
    #[cfg(feature = "test-support")]
    hold: Option<Hold>,
}

impl<C, T> Default for Builds<C, T> {
    fn default() -> Self {
        Self {
            builds: BTreeMap::new(),
            running: Arc::default(),
            started: 0,
            wait: BUILD_WAIT,
            #[cfg(feature = "test-support")]
            hold: None,
        }
    }
}

impl<C, T: Send + 'static> Builds<C, T> {
    /// Drop every build whose lease has ended, cancelling its thread.
    pub(super) fn purge(&mut self, now: u64) {
        self.builds.retain(|_, build| build.alive(now));
    }

    /// Register a build and start `work` on its own thread at once. Returns the build's
    /// identity and a waiter for it.
    pub(super) fn start(
        &mut self,
        kind: &str,
        context: C,
        now: u64,
        work: impl FnOnce(&Worker<T>) -> Result<T, Error> + Send + 'static,
    ) -> Result<(String, Waiter<T>), Error> {
        self.purge(now);
        if self.builds.len() >= super::catalog_lease::MAX_HANDLES {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "catalog listing build count is exhausted",
            ));
        }
        let expires_at = now
            .checked_add(LEASE_MS)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "listing build deadline overflow"))?;
        let id = random_id(kind)?;
        let shared = Arc::new(Shared {
            state: Mutex::new(State::Running),
            ready: Condvar::new(),
            read: AtomicU64::new(0),
            total: AtomicU64::new(0),
            cancellation: QueryCancellation::default(),
            polled: Mutex::new(Instant::now()),
            #[cfg(feature = "test-support")]
            hold: self.hold.clone(),
        });
        let worker = Worker(Arc::clone(&shared));
        self.running.fetch_add(1, Ordering::SeqCst);
        let running = Running(Arc::clone(&self.running));
        std::thread::Builder::new()
            .name("sparrow-listing-build".into())
            .spawn(move || {
                let _running = running;
                // A panic is a failed build, not one that never finishes.
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work(&worker)))
                        .unwrap_or_else(|_| {
                            Err(Error::new(
                                "STORAGE_FAILURE",
                                "listing build failed: its thread panicked",
                            ))
                        });
                *lock(&worker.0.state) = State::Done(result);
                worker.0.ready.notify_all();
            })
            .map_err(|error| {
                Error::new(
                    "RESOURCE_LIMIT",
                    format!("listing build thread refused: {error}"),
                )
            })?;
        self.started += 1;
        self.builds.insert(
            id.clone(),
            Build {
                context,
                expires_at,
                shared: Arc::clone(&shared),
            },
        );
        Ok((id, Waiter(shared)))
    }

    /// The build a cursor names, its lease renewed from `now`, and a waiter for it.
    pub(super) fn poll(&mut self, id: &str, now: u64) -> Result<(&C, Waiter<T>), Error> {
        self.purge(now);
        let build = self.builds.get_mut(id).ok_or_else(|| {
            Error::new(
                "RESULT_EXPIRED",
                "listing build is expired, unknown, or from a previous store owner",
            )
        })?;
        build.expires_at = now
            .checked_add(LEASE_MS)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "listing build deadline overflow"))?;
        *lock(&build.shared.polled) = Instant::now();
        Ok((&build.context, Waiter(Arc::clone(&build.shared))))
    }

    /// What the build's first page needs besides its result.
    pub(super) fn context(&self, id: &str) -> Option<&C> {
        self.builds.get(id).map(|build| &build.context)
    }

    /// The build's absolute lease deadline.
    pub(super) fn expires_at(&self, id: &str) -> Option<u64> {
        self.builds.get(id).map(|build| build.expires_at)
    }

    /// Retire a build whose result a call has taken, or that its caller released; whether
    /// it was registered. Dropping it cancels its thread.
    pub(super) fn remove(&mut self, id: &str) -> bool {
        self.builds.remove(id).is_some()
    }

    /// Retire a build its caller released, as [`Builds::remove`], after dropping expired
    /// builds; whether it was live.
    pub(super) fn release(&mut self, id: &str, now: u64) -> bool {
        self.purge(now);
        self.remove(id)
    }

    /// How long one call waits for its build.
    pub(super) fn wait(&self) -> Duration {
        self.wait
    }

    /// Dev-only: wait at most `wait` per call.
    #[cfg(feature = "test-support")]
    pub(super) fn set_wait_for_tests(&mut self, wait: Duration) {
        self.wait = wait;
    }

    /// Dev-only: hold every later build's thread with `hold.1` once it has read `hold.0`
    /// entries.
    #[cfg(feature = "test-support")]
    pub(super) fn hold_for_tests(&mut self, hold: Hold) {
        self.hold = Some(hold);
    }

    /// Registered builds, threads still running, and builds ever started.
    #[cfg(feature = "test-support")]
    pub(super) fn census(&self) -> (usize, usize, u64) {
        (
            self.builds.len(),
            self.running.load(Ordering::SeqCst),
            self.started,
        )
    }
}

/// Whether a cursor names a build of `kind` rather than a published capture.
pub(super) fn is_build(cursor: &super::catalog_lease::Cursor<'_>, kind: &str) -> bool {
    cursor
        .id()
        .strip_prefix(kind)
        .is_some_and(|rest| rest.starts_with(':'))
}
