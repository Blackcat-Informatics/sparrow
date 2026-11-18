// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Who waits for what before the daemon does work (#246).
//!
//! Three limits used to refuse at once: the operation permit budget, the 16-import journal
//! limit and the connection ceiling. A refusal costs the client a round trip, a backoff and a
//! reconnect, and a refused connection is a reset. Intake now waits instead, and every wait is
//! counted and journaled.
//!
//! Every wait here blocks on an event, never on a poll:
//! - a writer waits on the operation semaphore (a permit released by finished storage work, or
//!   the semaphore closing at shutdown) and on the shutdown signal;
//! - the accept loop waits on its connection semaphore (a connection closing, which includes
//!   its lease expiring);
//! - the timers are the bound on a writer's wait, [`WRITER_PERMIT_WAIT`], and the same-sized
//!   bound on a begin-import's wait for a journal slot, [`IMPORT_SLOT_WAIT`]. It is not a retry
//!   interval: nothing is retried on it. It exists because the client gives up after its
//!   `rpc_timeout`, and an answer that arrives later than that is an answer nobody reads.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError, watch},
    time::Instant,
};

/// How long a writer waits for an operation permit before the retryable `RESOURCE_LIMIT`.
/// The client's default `rpc_timeout` is 5 s ([`sparrow_client::DeliveryConfig`]); the wait
/// stays well inside it so the answer still reaches the caller.
pub const WRITER_PERMIT_WAIT: Duration = Duration::from_secs(3);

/// How long a begin-import waits for a free import journal slot, for the same reason.
pub const IMPORT_SLOT_WAIT: Duration = Duration::from_secs(3);

/// Least time between two journal lines announcing a wait episode, as for the pressure duty.
const REPORT_INTERVAL: Duration = Duration::from_secs(60);

/// What a wait counter reports.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WaitSnapshot {
    /// Acquisitions that found nothing free and had to wait.
    pub waits: u64,
    /// Waits in progress now.
    pub waiting: u32,
    /// The longest wait, finished or running, in milliseconds.
    pub longest_ms: u64,
    /// Waits that ended at their bound without being served.
    pub timeouts: u64,
}

/// Counters, gauge and journal lines for one kind of wait.
pub struct Waits {
    subject: &'static str,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    waits: u64,
    timeouts: u64,
    longest_ms: u64,
    next: u64,
    running: BTreeMap<u64, Instant>,
    episode: Option<Episode>,
    announced_at: Option<Instant>,
}

struct Episode {
    started: Instant,
    waits: u64,
    timeouts: u64,
    longest: Duration,
    announced: bool,
}

impl Waits {
    /// Counters for `subject`, which names the journal lines ("writer permit").
    #[must_use]
    pub fn new(subject: &'static str) -> Arc<Self> {
        Arc::new(Self {
            subject,
            state: Mutex::default(),
        })
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        // The counters are plain integers; a poisoned lock holds nothing half-updated.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Begin one wait. Call it only once nothing was free. Dropping the guard ends the wait.
    #[must_use]
    pub fn enter(self: &Arc<Self>) -> Waiter {
        let now = Instant::now();
        let mut line = None;
        let id = {
            let mut state = self.state();
            state.waits = state.waits.saturating_add(1);
            state.next += 1;
            let id = state.next;
            state.running.insert(id, now);
            let quiet = state
                .announced_at
                .is_none_or(|at| now.duration_since(at) >= REPORT_INTERVAL);
            let episode = state.episode.get_or_insert(Episode {
                started: now,
                waits: 0,
                timeouts: 0,
                longest: Duration::ZERO,
                announced: false,
            });
            episode.waits += 1;
            if !episode.announced && quiet {
                episode.announced = true;
                line = Some(format!(
                    "sparrowd: {}: a wait began; it ends when a slot frees (waits={})",
                    self.subject, state.waits
                ));
                state.announced_at = Some(now);
            }
            id
        };
        if let Some(line) = line {
            eprintln!("{line}");
        }
        Waiter {
            waits: self.clone(),
            id,
            timed_out: false,
        }
    }

    /// The counters now.
    #[must_use]
    pub fn snapshot(&self) -> WaitSnapshot {
        let state = self.state();
        let running = state.running.values().map(|since| since.elapsed()).max();
        let longest = running
            .unwrap_or_default()
            .max(Duration::from_millis(state.longest_ms));
        WaitSnapshot {
            waits: state.waits,
            waiting: u32::try_from(state.running.len()).unwrap_or(u32::MAX),
            longest_ms: u64::try_from(longest.as_millis()).unwrap_or(u64::MAX),
            timeouts: state.timeouts,
        }
    }
}

/// One wait in progress. The gauge falls when it is dropped, however the wait ended,
/// including a cancelled caller.
pub struct Waiter {
    waits: Arc<Waits>,
    id: u64,
    timed_out: bool,
}

impl Waiter {
    /// Record that this wait ended at its bound without being served.
    pub fn timed_out(&mut self) {
        self.timed_out = true;
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        let now = Instant::now();
        let mut line = None;
        {
            let mut state = self.waits.state();
            let Some(since) = state.running.remove(&self.id) else {
                return;
            };
            let waited = now.duration_since(since);
            state.longest_ms = state
                .longest_ms
                .max(u64::try_from(waited.as_millis()).unwrap_or(u64::MAX));
            if self.timed_out {
                state.timeouts = state.timeouts.saturating_add(1);
            }
            if let Some(episode) = state.episode.as_mut() {
                episode.longest = episode.longest.max(waited);
                episode.timeouts += u64::from(self.timed_out);
            }
            if state.running.is_empty()
                && let Some(episode) = state.episode.take()
                && (episode.announced || now.duration_since(episode.started) >= REPORT_INTERVAL)
            {
                line = Some(format!(
                    "sparrowd: {}: waits ended; {} wait(s), {} timed out, longest {} ms",
                    self.waits.subject,
                    episode.waits,
                    episode.timeouts,
                    episode.longest.as_millis()
                ));
            }
        }
        if let Some(line) = line {
            eprintln!("{line}");
        }
    }
}

/// An error answer for admission: the wire code and its message.
#[derive(Debug, PartialEq, Eq)]
pub struct Refusal {
    /// `RESOURCE_LIMIT` (retryable: the budget is busy) or `SHUTTING_DOWN`.
    pub code: &'static str,
    /// The operator-facing explanation.
    pub message: &'static str,
}

const SHUTTING_DOWN: Refusal = Refusal {
    code: "SHUTTING_DOWN",
    message: "RPC storage admission is closed; retry the same intent after reconnecting",
};

/// Permits held for the duration of one storage operation. Dropping releases both.
///
/// Fields drop in declaration order, so the reader slot is declared first and released first:
/// whoever is handed the operation permit then finds the reader slot already free. The other
/// order let a caller that waited for the permit and asked again at once be refused as busy by
/// the slot of the read that had just ended (#349).
pub struct Admitted {
    _reader: Option<OwnedSemaphorePermit>,
    _operation: Option<OwnedSemaphorePermit>,
}

/// The daemon's operation permit budget and the rules for taking from it.
///
/// One semaphore (`operations`, `max_concurrent_requests` permits) is the whole budget; the
/// consume workers and the syslog listeners draw from it directly, as writers. RPC callers
/// come in two classes:
/// - a **writer** (the intake path: ingest, events, imports, flush, discovery, pins) waits for
///   a permit, FIFO, for at most [`WRITER_PERMIT_WAIT`];
/// - a **reader** (queries, status, gets) takes a permit only if one is free, and is first
///   limited to `max - writer_reserve` slots, so readers can never hold the last
///   `writer_reserve` permits. A query storm therefore cannot starve ingest (#91). A reader
///   never waits: it would sit in the writers' FIFO and be served ahead of a later writer.
pub struct Admission {
    operations: Arc<Semaphore>,
    readers: Arc<Semaphore>,
    /// Status reads in flight at once, as many as readers, none of them taking an operation permit.
    diagnostics: Arc<Semaphore>,
    reader_slots: u32,
    reserve: u32,
    writer_waits: Arc<Waits>,
    import_waits: Arc<Waits>,
    reader_refusals: AtomicU64,
    import_freed: tokio::sync::Notify,
    closing: watch::Sender<bool>,
    wait: Duration,
}

/// The permits no reader may hold, out of `slots`: a quarter, rounded up, and never so many
/// that no reader can run. A single permit is shared.
#[must_use]
pub const fn writer_reserve(slots: u32) -> u32 {
    let quarter = slots.div_ceil(4);
    if quarter >= slots {
        slots.saturating_sub(1)
    } else {
        quarter
    }
}

impl Admission {
    /// Admission over `operations`, which must hold its full budget when this is called.
    #[must_use]
    pub fn new(operations: Arc<Semaphore>) -> Arc<Self> {
        Self::with_wait(operations, WRITER_PERMIT_WAIT)
    }

    /// As [`Admission::new`] with an explicit writer wait bound.
    #[must_use]
    pub fn with_wait(operations: Arc<Semaphore>, wait: Duration) -> Arc<Self> {
        let slots = u32::try_from(operations.available_permits()).unwrap_or(u32::MAX);
        let reserve = writer_reserve(slots);
        let reader_slots = slots - reserve;
        Arc::new(Self {
            operations,
            readers: Arc::new(Semaphore::new(reader_slots as usize)),
            diagnostics: Arc::new(Semaphore::new(reader_slots as usize)),
            reader_slots,
            reserve,
            writer_waits: Waits::new("writer operation permit"),
            import_waits: Waits::new("import journal slot"),
            reader_refusals: AtomicU64::new(0),
            import_freed: tokio::sync::Notify::new(),
            closing: watch::channel(false).0,
            wait,
        })
    }

    /// The shared budget, which workers and shutdown draw on.
    #[must_use]
    pub fn operations(&self) -> &Arc<Semaphore> {
        &self.operations
    }

    /// Tell every waiting writer, and every later one, that admission is closing.
    pub fn begin_shutdown(&self) {
        self.closing.send_replace(true);
    }

    /// Permits no reader may hold.
    #[must_use]
    pub const fn writer_reserve(&self) -> u32 {
        self.reserve
    }

    /// Permits readers may hold at once.
    #[must_use]
    pub const fn reader_slots(&self) -> u32 {
        self.reader_slots
    }

    /// The writer wait counters.
    #[must_use]
    pub fn writer_waits(&self) -> WaitSnapshot {
        self.writer_waits.snapshot()
    }

    /// The import slot wait counters and the bound they honour.
    #[must_use]
    pub fn import_waits(&self) -> &Arc<Waits> {
        &self.import_waits
    }

    /// Tell begin-imports waiting for a journal slot that one is free: a reclamation commit
    /// deleted an import journal (the store calls this through its import slot waker). A
    /// published, cancelled or expired import frees its slot only when that commit lands.
    pub fn import_slot_freed(&self) {
        self.import_freed.notify_waiters();
    }

    /// Run `attempt` (a begin-import) and, while it is refused for want of an import journal
    /// slot (`full`), wait for [`Admission::import_slot_freed`], shutdown or the bound
    /// [`IMPORT_SLOT_WAIT`], then run it again. The attempt holds nothing while this waits.
    /// At the bound, or at shutdown, the last refusal is the answer. The bound covers the whole
    /// call: the attempt receives the deadline and must not wait past it (a writer permit wait
    /// uses [`Admission::writer_until`]), and no attempt starts once the deadline has passed.
    ///
    /// # Errors
    /// What the attempt last returned.
    pub async fn import_slot<T, E, F>(
        &self,
        mut attempt: impl FnMut(Instant) -> F,
        full: impl Fn(&E) -> bool,
    ) -> Result<T, E>
    where
        F: Future<Output = Result<T, E>>,
    {
        let deadline = Instant::now() + IMPORT_SLOT_WAIT;
        let mut waiter = None;
        let mut closing = self.closing.subscribe();
        loop {
            // Registered before the attempt, so a slot freed during it is not missed.
            let freed = self.import_freed.notified();
            tokio::pin!(freed);
            freed.as_mut().enable();
            let refused = match attempt(deadline).await {
                Err(error) if full(&error) => error,
                done => return done,
            };
            let waiter = waiter.get_or_insert_with(|| self.import_waits.enter());
            tokio::select! {
                biased;
                () = &mut freed => {
                    // A slot freed with the bound spent is answered, not retried.
                    if Instant::now() >= deadline {
                        waiter.timed_out();
                        return Err(refused);
                    }
                }
                _ = closing.wait_for(|closing| *closing) => return Err(refused),
                () = tokio::time::sleep_until(deadline) => {
                    waiter.timed_out();
                    return Err(refused);
                }
            }
        }
    }

    /// Reads refused for want of a slot or permit.
    #[must_use]
    pub fn reader_refusals(&self) -> u64 {
        self.reader_refusals.load(Ordering::Relaxed)
    }

    /// The writer wait bound.
    #[must_use]
    pub const fn writer_wait_limit(&self) -> Duration {
        self.wait
    }

    /// A permit for a writer: at once if one is free, else after waiting its turn for at most
    /// the wait bound. Fails `SHUTTING_DOWN` once the semaphore is closed or shutdown has
    /// begun, and `RESOURCE_LIMIT` (retryable) when the bound passes.
    ///
    /// # Errors
    /// The refusal to answer the caller with.
    pub async fn writer(&self) -> Result<Admitted, Refusal> {
        self.writer_until(Instant::now() + self.wait).await
    }

    /// As [`Admission::writer`], waiting no later than `deadline`. A begin-import that waits
    /// for a journal slot passes its own deadline, so the whole call stays inside one bound.
    ///
    /// # Errors
    /// The refusal to answer the caller with.
    pub async fn writer_until(&self, deadline: Instant) -> Result<Admitted, Refusal> {
        match self.operations.clone().try_acquire_owned() {
            Ok(permit) => return Ok(Admitted::writer(permit)),
            Err(TryAcquireError::Closed) => return Err(SHUTTING_DOWN),
            Err(TryAcquireError::NoPermits) => {}
        }
        let mut waiter = self.writer_waits.enter();
        let mut closing = self.closing.subscribe();
        let served = tokio::select! {
            biased;
            permit = self.operations.clone().acquire_owned() => {
                permit.map(Admitted::writer).map_err(|_| SHUTTING_DOWN)
            }
            _ = closing.wait_for(|closing| *closing) => Err(SHUTTING_DOWN),
            () = tokio::time::sleep_until(deadline) => {
                waiter.timed_out();
                Err(Refusal {
                    code: "RESOURCE_LIMIT",
                    message: "RPC operation concurrency is at its configured limit; the wait for a permit ended, retry",
                })
            }
        };
        drop(waiter);
        served
    }

    /// A permit for a reader, only if a reader slot and an operation permit are free now.
    ///
    /// # Errors
    /// The refusal to answer the caller with.
    pub fn reader(&self) -> Result<Admitted, Refusal> {
        let busy = || {
            self.reader_refusals.fetch_add(1, Ordering::Relaxed);
            Refusal {
                code: "RESOURCE_LIMIT",
                message: "RPC operation concurrency is at its configured limit",
            }
        };
        let slot = match self.readers.clone().try_acquire_owned() {
            Ok(slot) => slot,
            Err(TryAcquireError::NoPermits) => return Err(busy()),
            Err(TryAcquireError::Closed) => return Err(SHUTTING_DOWN),
        };
        match self.operations.clone().try_acquire_owned() {
            Ok(operation) => Ok(Admitted {
                _operation: Some(operation),
                _reader: Some(slot),
            }),
            Err(TryAcquireError::NoPermits) => Err(busy()),
            Err(TryAcquireError::Closed) => Err(SHUTTING_DOWN),
        }
    }
}

impl Admission {
    /// The permit for a `status` read, which takes no operation permit: status carries the
    /// counters that explain a saturated budget, so a budget saturated by writers must not
    /// refuse it. As many status reads run at once as readers may (`reader_slots`), so a
    /// health check overlapping a CLI or MCP read is served; one more is refused and counted.
    ///
    /// Once shutdown has begun it answers `SHUTTING_DOWN` like every other operation. A read
    /// already running is not drained: it only reads a snapshot and writes nothing, so the
    /// final flush does not depend on it, and the process outlives it.
    ///
    /// # Errors
    /// The refusal to answer the caller with.
    pub fn diagnostic(&self) -> Result<Admitted, Refusal> {
        if *self.closing.borrow() {
            return Err(SHUTTING_DOWN);
        }
        match self.diagnostics.clone().try_acquire_owned() {
            Ok(slot) => Ok(Admitted {
                _operation: None,
                _reader: Some(slot),
            }),
            Err(_) => {
                self.reader_refusals.fetch_add(1, Ordering::Relaxed);
                Err(Refusal {
                    code: "RESOURCE_LIMIT",
                    message: "status reads are at their configured limit",
                })
            }
        }
    }
}

impl Admitted {
    const fn writer(operation: OwnedSemaphorePermit) -> Self {
        Self {
            _operation: Some(operation),
            _reader: None,
        }
    }
}

/// The ceiling on one set of connections, taken before the peer is accepted.
///
/// The accept loop takes a [`Slot`] first and accepts second, so a peer over the ceiling waits
/// in the kernel's listen backlog and is served when a slot frees, instead of being accepted
/// and reset. A slot frees when its connection ends: the peer closes, the RPC fails, the
/// 60-second lease expires, or the daemon stops.
pub struct Ceiling {
    name: &'static str,
    max: usize,
    slots: Arc<Semaphore>,
    active: AtomicU32,
    lease_expiries: AtomicU64,
    waits: Arc<Waits>,
}

/// What a ceiling reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CeilingSnapshot {
    /// The configured ceiling.
    pub max: u32,
    /// Connections admitted now.
    pub active: u32,
    /// Connections closed by their lease.
    pub lease_expiries: u64,
    /// Times the accept loop found every slot taken and paused for one.
    pub waits: WaitSnapshot,
}

impl Ceiling {
    /// A ceiling of `max` connections named `name` in journal lines.
    #[must_use]
    pub fn new(name: &'static str, max: usize) -> Arc<Self> {
        Arc::new(Self {
            name,
            max,
            slots: Arc::new(Semaphore::new(max)),
            active: AtomicU32::new(0),
            lease_expiries: AtomicU64::new(0),
            waits: Waits::new(name),
        })
    }

    /// A slot if one is free now. Otherwise the wait that has begun: hold it while the accept
    /// loop is held at the ceiling, and drop it once [`Ceiling::acquire`] returns. The loop
    /// calls this once per slot rather than inside a select, so a loop turn that did not end
    /// the wait does not count as a new one.
    ///
    /// # Errors
    /// The begun wait, when every slot is taken.
    pub fn try_slot(self: &Arc<Self>) -> Result<Slot, Waiter> {
        match self.slots.clone().try_acquire_owned() {
            Ok(permit) => Ok(self.slot(permit)),
            Err(_) => Err(self.waits.enter()),
        }
    }

    /// Wait for a free slot until a connection ends (the peer closes, the RPC fails or its
    /// lease expires). Cancel safe: dropping it loses nothing but the queue position.
    pub async fn acquire(self: &Arc<Self>) -> Slot {
        match self.slots.clone().acquire_owned().await {
            Ok(permit) => self.slot(permit),
            // INVARIANT: the semaphore is owned here and never closed.
            Err(_) => std::future::pending().await,
        }
    }

    fn slot(self: &Arc<Self>, permit: OwnedSemaphorePermit) -> Slot {
        Slot {
            _permit: permit,
            ceiling: self.clone(),
        }
    }

    /// The ceiling's name in journal lines and status.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Count a connection its lease closed.
    pub fn lease_expired(&self) {
        self.lease_expiries.fetch_add(1, Ordering::Relaxed);
    }

    /// The counters now.
    #[must_use]
    pub fn snapshot(&self) -> CeilingSnapshot {
        CeilingSnapshot {
            max: u32::try_from(self.max).unwrap_or(u32::MAX),
            active: self.active.load(Ordering::Relaxed),
            lease_expiries: self.lease_expiries.load(Ordering::Relaxed),
            waits: self.waits.snapshot(),
        }
    }
}

/// A taken slot, not yet a connection.
pub struct Slot {
    _permit: OwnedSemaphorePermit,
    ceiling: Arc<Ceiling>,
}

impl Slot {
    /// The ceiling this slot belongs to.
    #[must_use]
    pub fn ceiling(&self) -> &Arc<Ceiling> {
        &self.ceiling
    }

    /// Count the connection as active until the result is dropped.
    #[must_use]
    pub fn activate(self) -> ActiveConnection {
        self.ceiling.active.fetch_add(1, Ordering::Relaxed);
        ActiveConnection { slot: self }
    }
}

/// An admitted connection: holds its slot and the active gauge.
pub struct ActiveConnection {
    slot: Slot,
}

impl ActiveConnection {
    /// The ceiling this connection counts against.
    #[must_use]
    pub fn ceiling(&self) -> &Arc<Ceiling> {
        &self.slot.ceiling
    }
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.slot.ceiling.active.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Write one wait counter to its status struct.
pub(crate) fn encode_wait(
    mut out: crate::sparrow_capnp::wait_status::Builder<'_>,
    wait: WaitSnapshot,
) {
    out.set_waits(wait.waits);
    out.set_waiting(wait.waiting);
    out.set_longest_wait_ms(wait.longest_ms);
    out.set_timeouts(wait.timeouts);
}

impl Admission {
    /// Write the admission section of `status`, with the shared RPC ceiling when there is one.
    pub(crate) fn encode(
        &self,
        mut out: crate::sparrow_capnp::admission_status::Builder<'_>,
        connections: Option<&Ceiling>,
    ) {
        out.set_writer_reserve(self.reserve);
        out.set_reader_slots(self.reader_slots);
        out.set_writer_wait_limit_ms(u32::try_from(self.wait.as_millis()).unwrap_or(u32::MAX));
        encode_wait(
            out.reborrow().init_writer_permit_wait(),
            self.writer_waits(),
        );
        out.set_reader_refusals(self.reader_refusals());
        encode_wait(
            out.reborrow().init_import_slot_wait(),
            self.import_waits.snapshot(),
        );
        out.set_import_slot_wait_limit_ms(
            u32::try_from(IMPORT_SLOT_WAIT.as_millis()).unwrap_or(u32::MAX),
        );
        match connections {
            Some(ceiling) => ceiling.encode(out.init_rpc_connections().init_value()),
            None => out.init_rpc_connections().set_absent(()),
        }
    }
}

impl Ceiling {
    pub(crate) fn encode(
        &self,
        mut out: crate::sparrow_capnp::connection_ceiling_status::Builder<'_>,
    ) {
        let snapshot = self.snapshot();
        out.set_max_connections(snapshot.max);
        out.set_active_connections(snapshot.active);
        out.set_lease_expiries(snapshot.lease_expiries);
        encode_wait(out.init_ceiling_wait(), snapshot.waits);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A waker that records how many reader slots were free at the instant it was woken.
    struct SlotsAtWake {
        readers: Arc<Semaphore>,
        free: AtomicU64,
    }

    impl std::task::Wake for SlotsAtWake {
        fn wake(self: Arc<Self>) {
            self.free.store(
                self.readers.available_permits() as u64 + 1,
                Ordering::SeqCst,
            );
        }
    }

    /// INVARIANT (#349): a finished read frees its reader slot no later than its operation
    /// permit, so whoever is handed the operation permit (a queued writer, or a caller that
    /// waited for it before asking again) never meets a reader slot still held by the read
    /// that just ended and is refused as busy. Observed deterministically: the operation
    /// permit's waiter is woken inline while the permit is released, and by then the reader
    /// slot must already be free.
    #[test]
    fn a_finished_read_frees_its_reader_slot_before_its_operation_permit() {
        let admission = Admission::new(Arc::new(Semaphore::new(1)));
        let read = admission.reader().expect("a free budget admits a reader");
        let wake = Arc::new(SlotsAtWake {
            readers: admission.readers.clone(),
            free: AtomicU64::new(0),
        });
        let waker = std::task::Waker::from(wake.clone());
        let mut context = std::task::Context::from_waker(&waker);
        let mut waiting = std::pin::pin!(admission.operations.clone().acquire_owned());
        assert!(
            waiting.as_mut().poll(&mut context).is_pending(),
            "the read holds the only operation permit"
        );
        drop(read);
        // Stored as free + 1 so that "never woken" (0) differs from "woken with no slot free" (1).
        assert_eq!(
            wake.free.load(Ordering::SeqCst),
            2,
            "the operation permit was handed on while the reader slot was still held"
        );
    }

    /// INVARIANT (#246): at the connection ceiling the accept loop's wait is counted, shows
    /// as a waiter while it lasts with its longest duration, and ends when a connection ends
    /// and frees its slot; the active gauge follows the connections, not the reserved slot.
    #[tokio::test(start_paused = true)]
    async fn a_ceiling_wait_ends_when_a_connection_frees_its_slot() {
        let ceiling = Ceiling::new("test ceiling", 1);
        let Ok(slot) = ceiling.try_slot() else {
            panic!("a free ceiling made the accept loop wait");
        };
        assert_eq!(
            ceiling.snapshot().active,
            0,
            "a reserved slot is not a connection"
        );
        let connection = slot.activate();
        let Err(waiter) = ceiling.try_slot() else {
            panic!("a full ceiling admitted a second connection");
        };
        let snapshot = ceiling.snapshot();
        assert_eq!(
            (
                snapshot.active,
                snapshot.waits.waits,
                snapshot.waits.waiting
            ),
            (1, 1, 1)
        );
        let next = tokio::spawn({
            let ceiling = ceiling.clone();
            async move { ceiling.acquire().await.activate() }
        });
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(!next.is_finished());
        assert!(ceiling.snapshot().waits.longest_ms >= 5000);
        drop(connection);
        let second = next.await.unwrap();
        drop(waiter);
        let snapshot = ceiling.snapshot();
        assert_eq!((snapshot.active, snapshot.waits.waiting), (1, 0));
        assert!(snapshot.waits.longest_ms >= 5000);
        ceiling.lease_expired();
        assert_eq!(ceiling.snapshot().lease_expiries, 1);
        drop(second);
        assert_eq!(ceiling.snapshot().active, 0);
    }

    /// INVARIANT (#246): a begin-import refused for want of a journal slot waits for a slot to
    /// free and then runs again, holding nothing while it waits.
    #[tokio::test(start_paused = true)]
    async fn an_import_begin_waits_for_a_freed_slot_and_runs_again() {
        let admission = Admission::new(Arc::new(Semaphore::new(2)));
        let attempts = AtomicU32::new(0);
        let begun = admission.import_slot(
            |_| async {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err("full")
                } else {
                    Ok(7)
                }
            },
            |error| *error == "full",
        );
        tokio::pin!(begun);
        tokio::select! {
            biased;
            _ = &mut begun => panic!("a refused begin finished without a slot freeing"),
            () = std::future::ready(()) => {}
        }
        assert_eq!(admission.import_waits().snapshot().waiting, 1);
        admission.import_slot_freed();
        assert_eq!(begun.await, Ok(7));
        let waits = admission.import_waits().snapshot();
        assert_eq!((waits.waits, waits.waiting, waits.timeouts), (1, 0, 0));
    }

    /// INVARIANT (#246): a begin-import that no slot frees for answers its last refusal at the
    /// bound, shorter than the client's `rpc_timeout`, and counts the timeout; shutdown ends
    /// the wait at once with the same refusal.
    #[tokio::test(start_paused = true)]
    async fn an_import_begin_answers_its_refusal_at_the_bound_or_at_shutdown() {
        let admission = Admission::new(Arc::new(Semaphore::new(2)));
        let started = Instant::now();
        let refused = admission
            .import_slot(|_| async { Err::<u8, _>("full") }, |error| *error == "full")
            .await;
        assert_eq!(refused, Err("full"));
        assert_eq!(started.elapsed(), IMPORT_SLOT_WAIT);
        assert!(
            IMPORT_SLOT_WAIT
                < sparrow_client::DeliveryConfig::new("rpc.sock", "client").rpc_timeout
        );
        assert_eq!(admission.import_waits().snapshot().timeouts, 1);
        let pending =
            admission.import_slot(|_| async { Err::<u8, _>("full") }, |error| *error == "full");
        tokio::pin!(pending);
        tokio::select! {
            biased;
            _ = &mut pending => panic!("a refused begin finished without waiting"),
            () = std::future::ready(()) => {}
        }
        let before = Instant::now();
        admission.begin_shutdown();
        assert_eq!(pending.await, Err("full"));
        assert_eq!(before.elapsed(), Duration::ZERO);
        assert_eq!(admission.import_waits().snapshot().timeouts, 1);
    }

    /// INVARIANT (#246): the writer reserve is a quarter of the budget rounded up, and never
    /// leaves readers without a slot, so a budget of one is shared.
    #[test]
    fn the_writer_reserve_is_a_quarter_of_the_budget_and_never_all_of_it() {
        for (budget, reserve) in [(1, 0), (2, 1), (3, 1), (4, 1), (5, 2), (64, 16)] {
            assert_eq!(writer_reserve(budget), reserve, "budget {budget}");
            let admission = Admission::new(Arc::new(Semaphore::new(budget as usize)));
            assert_eq!(admission.writer_reserve(), reserve);
            assert_eq!(admission.reader_slots(), budget - reserve);
        }
    }

    /// INVARIANT (#246): a begin-import is answered inside its one bound even when the
    /// operation permit budget is saturated too: the permit wait is capped at the time left, and
    /// a slot that frees with the bound spent is answered, not retried. Both cases answer by
    /// the bound, which is shorter than the client's `rpc_timeout`.
    #[tokio::test(start_paused = true)]
    async fn a_begin_import_with_permits_and_slots_both_saturated_answers_by_one_bound() {
        let operations = Arc::new(Semaphore::new(1));
        let admission = Admission::new(operations.clone());
        let attempt = |deadline| {
            let admission = admission.clone();
            async move {
                match admission.writer_until(deadline).await {
                    Ok(_permit) => Err::<(), _>("full"),
                    Err(refusal) => Err(refusal.code),
                }
            }
        };
        let held = operations.clone().acquire_owned().await.unwrap();
        let started = Instant::now();
        let refused: Result<(), &str> = admission
            .import_slot(attempt, |error| *error == "full")
            .await;
        assert_eq!(refused, Err("RESOURCE_LIMIT"));
        assert_eq!(started.elapsed(), IMPORT_SLOT_WAIT);
        drop(held);
        // The permit is free but the slot never is; a slot signal at 2.95 s finds 50 ms left.
        let started = Instant::now();
        let call = admission.import_slot(attempt, |error| *error == "full");
        tokio::pin!(call);
        tokio::select! { biased; _ = &mut call => panic!("answered without a bound"), () = tokio::time::sleep(Duration::from_millis(2950)) => {} }
        admission.import_slot_freed();
        assert_eq!(call.await, Err("full"));
        assert_eq!(started.elapsed(), IMPORT_SLOT_WAIT);
        assert!(
            started.elapsed()
                < sparrow_client::DeliveryConfig::new("rpc.sock", "client").rpc_timeout
        );
    }

    /// INVARIANT (#246): a begin-import that is refused for want of a journal slot and whose
    /// slot signal arrives late finds the operation budget saturated: its re-run waits for a
    /// permit only until the original deadline, so the whole call answers at 3 s, not at the
    /// signal plus a fresh 3 s wait (5.95 s, past the client's `rpc_timeout`).
    #[tokio::test(start_paused = true)]
    async fn a_late_slot_signal_does_not_start_a_fresh_permit_wait() {
        let operations = Arc::new(Semaphore::new(1));
        let admission = Admission::new(operations.clone());
        let attempts = AtomicU32::new(0);
        let attempt = |deadline| {
            let admission = admission.clone();
            attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                match admission.writer_until(deadline).await {
                    Ok(_permit) => Err::<(), _>("full"),
                    Err(refusal) => Err(refusal.code),
                }
            }
        };
        let started = Instant::now();
        let call = admission.import_slot(attempt, |error| *error == "full");
        tokio::pin!(call);
        // The first attempt takes the free permit and finds no slot; the permit is released.
        tokio::select! { biased; _ = &mut call => panic!("answered without waiting"), () = std::future::ready(()) => {} }
        assert_eq!(admission.import_waits().snapshot().waiting, 1);
        let held = operations.clone().acquire_owned().await.unwrap();
        tokio::select! { biased; _ = &mut call => panic!("answered before the bound"), () = tokio::time::sleep(Duration::from_millis(2950)) => {} }
        admission.import_slot_freed();
        assert_eq!(call.await, Err("RESOURCE_LIMIT"));
        assert_eq!(started.elapsed(), IMPORT_SLOT_WAIT);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        drop(held);
    }

    /// INVARIANT (#246): a slot signal that arrives with the bound already spent is answered
    /// with the last refusal; no attempt starts after the deadline.
    #[tokio::test(start_paused = true)]
    async fn a_slot_signal_with_the_bound_spent_is_answered_not_retried() {
        let operations = Arc::new(Semaphore::new(1));
        let admission = Admission::new(operations.clone());
        let attempts = AtomicU32::new(0);
        let attempt = |deadline| {
            let admission = admission.clone();
            attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                match admission.writer_until(deadline).await {
                    Ok(_permit) => Err::<(), _>("full"),
                    Err(refusal) => Err(refusal.code),
                }
            }
        };
        let started = Instant::now();
        let call = admission.import_slot(attempt, |error| *error == "full");
        tokio::pin!(call);
        tokio::select! { biased; _ = &mut call => panic!("answered without waiting"), () = std::future::ready(()) => {} }
        let _held = operations.clone().acquire_owned().await.unwrap();
        tokio::select! { biased; _ = &mut call => panic!("answered before the bound"), () = tokio::time::sleep(IMPORT_SLOT_WAIT - Duration::from_millis(1)) => {} }
        // The signal and the deadline become ready together; the signal must not start a re-run.
        admission.import_slot_freed();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(call.await, Err("full"));
        assert_eq!(started.elapsed(), IMPORT_SLOT_WAIT);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    /// INVARIANT (#246): a status read takes no operation permit, so a budget saturated by
    /// writers does not refuse it; as many run at once as readers may, so overlapping health
    /// checks are served; one more is refused and counted; and once shutdown begins it answers
    /// `SHUTTING_DOWN` like every other operation.
    #[test]
    fn status_is_served_while_writers_hold_every_permit_and_stops_at_shutdown() {
        let operations = Arc::new(Semaphore::new(4));
        let admission = Admission::new(operations.clone());
        let _writers = operations.clone().try_acquire_many_owned(4).unwrap();
        assert!(admission.reader().is_err());
        let overlapping: Vec<_> = (0..admission.reader_slots())
            .map(|_| admission.diagnostic().unwrap())
            .collect();
        assert_eq!(
            admission.diagnostic().err().map(|r| r.code),
            Some("RESOURCE_LIMIT")
        );
        assert_eq!(admission.reader_refusals(), 2);
        drop(overlapping);
        assert!(admission.diagnostic().is_ok());
        admission.begin_shutdown();
        assert_eq!(
            admission.diagnostic().err().map(|r| r.code),
            Some("SHUTTING_DOWN")
        );
    }
}
