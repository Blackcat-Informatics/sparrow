// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Bounded producer delivery; only the owned worker performs socket I/O.

use crate::{
    ArtifactReceipt, Client, Error, EventBatch, MAX_EVENT_BATCH_BYTES, MAX_EVENT_BATCH_EVENTS,
    MAX_EVENT_BYTES, ProducerEvent,
};
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex, TryLockError,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

/// Explicit producer resource and timing bounds.
#[derive(Debug, Clone)]
pub struct DeliveryConfig {
    /// Explicit Unix or public TCP daemon destination.
    pub endpoint: crate::Endpoint,
    /// Immutable logical event source.
    pub source: String,
    /// Queued plus in-flight event limit.
    pub max_queued_events: usize,
    /// Queued plus in-flight conservative allocation/expansion limit.
    pub max_queued_bytes: usize,
    /// Maximum observations in one request.
    pub batch_events: usize,
    /// Maximum conservative batch charge, including request overhead.
    pub batch_bytes: usize,
    /// Maximum intended batching delay before starting delivery.
    pub flush_interval: Duration,
    /// Deadline for one connection/discovery or ingestion attempt.
    pub rpc_timeout: Duration,
    /// Delay before the first retry of the same immutable pending batch. Each further
    /// failed attempt doubles the delay up to [`Self::retry_max_interval`]; an accepted
    /// batch restarts it here.
    pub retry_interval: Duration,
    /// Longest delay between attempts of the same pending batch, at least `retry_interval`.
    pub retry_max_interval: Duration,
    /// Directory that keeps every batch the daemon permanently refuses, with the reason and
    /// count, as one file per batch. Created on first use. The client never deletes a file
    /// here; its size is bounded only by the disk. Default: `sparrow-client/quarantine` under
    /// an absolute `XDG_STATE_HOME`, else under `$HOME/.local/state`. With neither the default
    /// is empty and [`Delivery::start`] fails until a directory is set: a refused log is never
    /// parked in a volatile fallback.
    pub quarantine_dir: PathBuf,
    /// Shortest accepted `flush_interval` and `retry_interval`. Production configuration
    /// always carries [`MIN_INTERVAL`]; only the test-timing constructor lowers it.
    interval_floor: Duration,
}

/// Floor for the batching and retry intervals: no client loop or timer fires more often than
/// once a second, matching the server's refusal of a buffered flush interval below 1000 ms.
const MIN_INTERVAL: Duration = Duration::from_secs(1);

impl DeliveryConfig {
    /// Default bounded queue, two-second batching, and finite RPC/retry deadlines.
    pub fn new(socket: impl Into<PathBuf>, source: impl Into<String>) -> Self {
        Self::for_endpoint(crate::Endpoint::Unix(socket.into()), source)
    }

    /// Default bounded delivery through a public typed TCP endpoint.
    pub fn tcp(address: std::net::SocketAddr, source: impl Into<String>) -> Self {
        Self::for_endpoint(crate::Endpoint::Tcp(address), source)
    }

    /// One default resource/timing law for every supported transport.
    pub fn for_endpoint(endpoint: crate::Endpoint, source: impl Into<String>) -> Self {
        Self {
            endpoint,
            source: source.into(),
            max_queued_events: 1024,
            max_queued_bytes: 4 * 1024 * 1024,
            batch_events: 128,
            batch_bytes: 512 * 1024,
            flush_interval: Duration::from_secs(2),
            rpc_timeout: Duration::from_secs(5),
            retry_interval: Duration::from_secs(1),
            retry_max_interval: Duration::from_secs(30),
            quarantine_dir: crate::quarantine::default_dir(),
            interval_floor: MIN_INTERVAL,
        }
    }

    /// Sub-second batching and retry intervals for tests that must not wait whole seconds.
    /// Production configuration cannot reach this: the constructor exists only in this
    /// crate's own tests and under the `test-timing` feature, which only the daemon's
    /// integration tests enable. The 60 s bound and `retry_max_interval >= retry_interval`
    /// still apply, and zero is still refused.
    #[cfg(any(test, feature = "test-timing"))]
    #[must_use]
    pub fn with_test_timing(
        mut self,
        flush_interval: Duration,
        retry_interval: Duration,
        retry_max_interval: Duration,
    ) -> Self {
        self.interval_floor = Duration::ZERO;
        self.flush_interval = flush_interval;
        self.retry_interval = retry_interval;
        self.retry_max_interval = retry_max_interval;
        self
    }
    /// Keep quarantined batches in this directory instead of the default.
    #[must_use]
    pub fn with_quarantine_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.quarantine_dir = dir.into();
        self
    }
    fn validate(&self) -> Result<(), Error> {
        self.endpoint.validate()?;
        if !self.quarantine_dir.is_absolute() {
            return Err(Error::InvalidArgument(
                "quarantine_dir must be an absolute directory: set DeliveryConfig::quarantine_dir, or provide an absolute XDG_STATE_HOME or HOME for the default",
            ));
        }
        if self.source.is_empty()
            || self.source.len() > 512
            || !(1..=16_384).contains(&self.max_queued_events)
            || !(MAX_EVENT_BYTES..=64 * 1024 * 1024).contains(&self.max_queued_bytes)
            || !(1..=MAX_EVENT_BATCH_EVENTS).contains(&self.batch_events)
            || !(MAX_EVENT_BYTES + 8192..=MAX_EVENT_BATCH_BYTES).contains(&self.batch_bytes)
            || self.retry_max_interval < self.retry_interval
            || [
                self.flush_interval,
                self.rpc_timeout,
                self.retry_interval,
                self.retry_max_interval,
            ]
            .iter()
            .any(|duration| duration.is_zero() || *duration > Duration::from_secs(60))
            || self.flush_interval < self.interval_floor
            || self.retry_interval < self.interval_floor
        {
            return Err(Error::InvalidArgument(
                "invalid delivery labels, queue/batch limits or timing bounds; durations require (0,60s], flush_interval and retry_interval at least 1s, and retry_max_interval >= retry_interval",
            ));
        }
        Ok(())
    }
}

/// A visible nonblocking producer refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum DropReason {
    /// Queued and in-flight work exhausted count or byte capacity.
    Full,
    /// Event shape, relationships, or owned allocations exceeded admission.
    Invalid,
    /// Shutdown or last-owner cancellation closed admission.
    Closed,
    /// A permanent worker failure prevents further delivery.
    Failed,
    /// Sequence/diagnostic counters exhausted their exact range.
    CounterExhausted,
}

/// Admission never waits for network I/O; it waits only for the queue's push-sized critical
/// section, so a briefly held lock never discards an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// The event is retained under the queue quota, with this assigned sequence.
    Queued {
        /// Client sequence within this producer instance.
        sequence: u64,
    },
    /// The event was discarded and its reason counted.
    Dropped {
        /// Explicit admission refusal.
        reason: DropReason,
        /// Sequence when assigned before capacity refusal.
        sequence: Option<u64>,
    },
}

/// Durable accepted progress at an explicit flush/shutdown fence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryWatermark {
    /// Producer instance, stable across reconnects.
    pub client_instance: [u8; 16],
    /// Last admitted sequence when the fence was captured; zero for empty input.
    pub required_sequence: u64,
    /// Highest durably acknowledged admitted sequence, possibly beyond the fence.
    pub durable_sequence: u64,
    /// Count of independently acknowledged events in this instance.
    pub durable_events: u64,
    /// Dropped offers and quarantined events counted by this fence; these were never certified.
    pub dropped_before_fence: u64,
    /// The part of `dropped_before_fence` that is kept in quarantine files, not lost.
    pub quarantined_before_fence: u64,
    /// Last durable batch receipt, absent before any accepted batch.
    pub last_receipt: Option<ArtifactReceipt>,
}

/// Explicit barrier failure; no successful prefix certifies missing delivery.
#[derive(Debug)]
pub enum FlushError {
    /// Another flush/shutdown already owns the bounded control slot.
    Busy,
    /// Admitted input reached durability, but earlier offered input was dropped.
    Dropped(Box<DeliveryWatermark>),
    /// The deadline elapsed; accepted progress may still advance afterward.
    TimedOut(Box<DeliveryStats>),
    /// A permanent transport/domain/protocol failure, with the pending batch retained.
    Delivery(Arc<Error>),
    /// The worker/state/control invariant is unavailable.
    Unavailable(&'static str),
}
impl std::fmt::Display for FlushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => f.write_str("another delivery barrier owns the control slot"),
            Self::Dropped(value) => write!(
                f,
                "durable sequence {}; {} offered events were not delivered before the fence, {} of them kept in quarantine files",
                value.durable_sequence, value.dropped_before_fence, value.quarantined_before_fence
            ),
            Self::TimedOut(value) => write!(
                f,
                "delivery deadline elapsed with {} pending events; last durable sequence {}",
                value.pending_events, value.last_durable_sequence
            ),
            Self::Delivery(error) => write!(f, "delivery failed: {error}"),
            Self::Unavailable(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for FlushError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Delivery(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

/// Queue and delivery diagnostics. These counters are not an RSS measurement.
#[derive(Debug, Clone)]
pub struct DeliveryStats {
    /// Offers observed by the producer handle, including refused input.
    pub offered_events: u64,
    /// Offers admitted into retained delivery state.
    pub admitted_events: u64,
    /// Events acknowledged durably by the daemon.
    pub durable_events: u64,
    /// Refusals indexed by `DropReason`'s discriminant.
    pub dropped_by_reason: [u64; 5],
    /// Queued plus in-flight observations.
    pub pending_events: usize,
    /// Conservative queued plus in-flight byte charge.
    pub pending_bytes: usize,
    /// Monotonic age of the oldest retained observation, including in-flight retries.
    pub oldest_pending_age: Option<Duration>,
    /// Time beyond the batching interval for the oldest pending observation.
    pub flush_lag: Duration,
    /// Observations retained in the immutable pending batch.
    pub in_flight_events: usize,
    /// Last sequence assigned under admission serialization.
    pub last_sequence: u64,
    /// Last durably accepted sequence.
    pub last_durable_sequence: u64,
    /// Retry attempts following transient failures.
    pub retries: u64,
    /// Whether the owned worker has not completed shutdown.
    pub running: bool,
    /// Whether explicit shutdown closed producer admission.
    pub closing: bool,
    /// Permanent failure; pending bodies remain inspectable.
    pub terminal_failure: Option<Arc<Error>>,
    /// Last transient failure, cleared when a batch is acknowledged.
    pub last_transient_failure: Option<Arc<Error>>,
    /// Admitted events whose batch the daemon permanently refused (a non-retryable
    /// `INVALID_ARGUMENT` or `CONFLICT`, or a batch the client itself cannot send): an answer
    /// retrying the same batch can never change. Their bytes are in quarantine files, not
    /// lost. They kept their sequence; the daemon sees a gap only if it later accepts a
    /// higher sequence of an instance it already recorded, so this counter and the fence are
    /// the reliable record.
    pub quarantined_events: u64,
    /// The latest permanent refusal that quarantined a batch.
    pub last_quarantine: Option<Arc<Error>>,
    /// Directory holding the quarantine files.
    pub quarantine_dir: PathBuf,
    /// Quarantine files written by this producer, including those for held events when a
    /// terminal failure ended delivery.
    pub quarantine_files: u64,
    /// The latest quarantine file written.
    pub last_quarantine_file: Option<PathBuf>,
    /// Quarantine writes that failed. The batch stays held and the write is retried; that
    /// batch is not dropped (events offered meanwhile can still be refused as `Full`).
    pub quarantine_write_failures: u64,
}

struct Queued {
    event: ProducerEvent,
    bytes: usize,
    at: Instant,
}
struct Pending {
    batch: Arc<EventBatch>,
    bytes: usize,
    at: Instant,
}
struct State {
    queue: VecDeque<Queued>,
    pending: Option<Pending>,
    bytes: usize,
    count: usize,
    sequence: u64,
    admitted_sequence: u64,
    admitted: u64,
    durable_sequence: u64,
    durable: u64,
    batch_number: u64,
    retries: u64,
    force_through: u64,
    closing: bool,
    store: Option<String>,
    receipt: Option<ArtifactReceipt>,
    failure: Option<Arc<Error>>,
    transient: Option<Arc<Error>>,
    quarantined: u64,
    /// Highest sequence settled by quarantine; a barrier is satisfied by durable or settled.
    quarantined_sequence: u64,
    quarantine: Option<Arc<Error>>,
    quarantine_files: u64,
    quarantine_failures: u64,
    quarantine_file: Option<PathBuf>,
}
struct Shared {
    config: DeliveryConfig,
    instance: [u8; 16],
    state: Mutex<State>,
    control: Mutex<()>,
    changed: Condvar,
    wake: tokio::sync::Notify,
    /// Signalled by every flush or shutdown so a waiting retry delay ends at once. Kept apart
    /// from `wake`, which each admitted event signals and would otherwise cut the backoff short.
    retry_wake: tokio::sync::Notify,
    /// Flush and shutdown calls currently waiting. A `retry_wake` signal ends a retry delay only
    /// while this is nonzero, so a signal left by a barrier that already returned cannot shorten
    /// the first delay of a later outage.
    barriers_waiting: AtomicUsize,
    abort_wake: tokio::sync::Notify,
    owners: AtomicUsize,
    offered: AtomicU64,
    spans: AtomicU64,
    drops: [AtomicU64; 5],
    exhausted: AtomicBool,
    aborted: AtomicBool,
    stopped: AtomicBool,
}

/// Cloneable nonblocking producer access; a dedicated thread owns the typed socket.
/// Explicit shutdown drains admitted input. Dropping the last handle cancels instead.
pub struct Delivery {
    shared: Arc<Shared>,
}
impl Clone for Delivery {
    fn clone(&self) -> Self {
        let shared = self.shared.clone();
        shared.owners.fetch_add(1, Ordering::SeqCst);
        Self { shared }
    }
}
impl Drop for Delivery {
    fn drop(&mut self) {
        if self.shared.owners.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.shared.aborted.store(true, Ordering::SeqCst);
            self.shared.abort_wake.notify_one();
            self.shared.wake.notify_one();
        }
    }
}

impl Delivery {
    /// Start only an owned producer worker. This does not synchronously connect.
    pub fn start(mut config: DeliveryConfig) -> Result<Self, Error> {
        config.validate()?;
        // Retain only bounded source/path allocations supplied at setup.
        config.source = config.source.as_str().to_owned();
        if let crate::Endpoint::Unix(path) = &mut config.endpoint {
            *path = path.as_path().to_path_buf();
        }
        let mut instance = [0; 16];
        getrandom::fill(&mut instance).map_err(|error| std::io::Error::other(error.to_string()))?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let shared = Shared::new(config, instance);
        let worker = shared.clone();
        std::thread::Builder::new()
            .name("sparrow-delivery".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    runtime.block_on(tokio::task::LocalSet::new().run_until(run(&worker)))
                }));
                drop(runtime);
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        let error = worker.save_held(error);
                        worker.fail(error);
                    }
                    Err(_) => {
                        let error = worker.save_held(Error::Protocol(
                            "producer delivery worker panicked; no remaining delivery certified",
                        ));
                        worker.fail(error);
                    }
                }
                worker.finished();
            })?;
        Ok(Self { shared })
    }

    /// Offer a bounded event, assigning its sequence without waiting for I/O.
    pub fn try_send(&self, mut event: ProducerEvent) -> Admission {
        let shared = &self.shared;
        if !shared.bump(&shared.offered) {
            return shared.reject(DropReason::CounterExhausted, None);
        }
        let bytes = match event.admission_bytes() {
            Ok(bytes) => bytes,
            Err(_) => return shared.reject(DropReason::Invalid, None),
        };
        // Every holder of this lock does bounded in-memory work (the longest assembles one
        // batch within the batch limits) and never performs I/O under it, so waiting is brief; refusing the event for a held lock would lose it for
        // no reason and without a sequence for the daemon to see as a gap.
        let Ok(mut state) = shared.state.lock() else {
            return shared.reject(DropReason::Failed, None);
        };
        if state.failure.is_some() || shared.exhausted.load(Ordering::SeqCst) {
            return shared.reject(DropReason::Failed, None);
        }
        if state.closing || shared.aborted.load(Ordering::SeqCst) {
            return shared.reject(DropReason::Closed, None);
        }
        let Some(sequence) = state.sequence.checked_add(1) else {
            shared.exhausted.store(true, Ordering::SeqCst);
            shared.wake.notify_one();
            return shared.reject(DropReason::CounterExhausted, None);
        };
        state.sequence = sequence;
        if state.count == shared.config.max_queued_events
            || bytes > shared.config.max_queued_bytes - state.bytes
        {
            return shared.reject(DropReason::Full, Some(sequence));
        }
        event.sequence = sequence;
        state.bytes += bytes;
        state.count += 1;
        state.admitted += 1;
        state.admitted_sequence = sequence;
        state.queue.push_back(Queued {
            event,
            bytes,
            at: Instant::now(),
        });
        drop(state);
        shared.wake.notify_one();
        Admission::Queued { sequence }
    }

    /// Account for an observation refused before an event could be constructed.
    /// Capture adapters use this same drop accounting and durable fence law.
    pub(crate) fn capture_failed(&self) {
        if self.shared.bump(&self.shared.offered) {
            self.shared.reject(DropReason::Invalid, None);
        } else {
            self.shared.reject(DropReason::CounterExhausted, None);
        }
    }

    /// Allocate a span identity unique within this producer instance.
    /// Direct typed span producers and tracing layers sharing a worker use this
    /// allocator rather than assuming independent subscriber IDs are unique.
    pub fn next_span_id(&self) -> Result<u64, Error> {
        self.shared
            .spans
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_add(1)
            })
            .map(|value| value + 1)
            .map_err(|_| {
                self.shared.exhausted.store(true, Ordering::SeqCst);
                self.shared.wake.notify_one();
                Error::Protocol("producer span identity counter exhausted")
            })
    }

    /// Capture visible bounded delivery diagnostics.
    pub fn stats(&self) -> Result<DeliveryStats, FlushError> {
        let state = self
            .shared
            .state
            .lock()
            .map_err(|_| FlushError::Unavailable("producer queue state poisoned"))?;
        Ok(self.shared.stats(&state))
    }

    /// Inspect the exact pending request, including after a permanent failure.
    pub fn pending_batch(&self) -> Result<Option<Arc<EventBatch>>, FlushError> {
        let state = self
            .shared
            .state
            .lock()
            .map_err(|_| FlushError::Unavailable("producer queue state poisoned"))?;
        Ok(state.pending.as_ref().map(|pending| pending.batch.clone()))
    }

    /// Force delivery through current admitted input; drops return an explicit gap error.
    /// Timeout leaves the owned worker and immutable pending request running.
    pub fn flush(&self, timeout: Duration) -> Result<DeliveryWatermark, FlushError> {
        self.barrier(timeout, false)
    }

    /// Close admission, drain current input, and wait for the worker/socket to finish.
    /// A timed-out shutdown remains closed and may be retried.
    pub fn shutdown(&self, timeout: Duration) -> Result<DeliveryWatermark, FlushError> {
        self.barrier(timeout, true)
    }

    fn barrier(&self, timeout: Duration, closing: bool) -> Result<DeliveryWatermark, FlushError> {
        let _control = match self.shared.control.try_lock() {
            Ok(control) => control,
            Err(TryLockError::WouldBlock) => return Err(FlushError::Busy),
            Err(TryLockError::Poisoned(_)) => {
                return Err(FlushError::Unavailable("producer barrier state poisoned"));
            }
        };
        let _waiting = BarrierWaiting::enter(&self.shared.barriers_waiting);
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(FlushError::Unavailable(
                "producer barrier deadline out of range",
            ))?;
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| FlushError::Unavailable("producer queue state poisoned"))?;
        state.closing |= closing;
        let target = state.admitted_sequence;
        let dropped = self.shared.drop_total();
        state.force_through = state.force_through.max(target);
        self.shared.wake.notify_one();
        // A flush or shutdown asks for delivery now: end any retry delay so the pending batch
        // is attempted at once instead of after up to `retry_max_interval`.
        self.shared.retry_wake.notify_one();
        loop {
            if self.shared.exhausted.load(Ordering::SeqCst) {
                return Err(FlushError::Unavailable(
                    "producer sequence/diagnostic counters exhausted",
                ));
            }
            if let Some(error) = &state.failure {
                return Err(FlushError::Delivery(error.clone()));
            }
            if state.durable_sequence.max(state.quarantined_sequence) >= target
                && (!closing || self.shared.stopped.load(Ordering::SeqCst))
            {
                let watermark = DeliveryWatermark {
                    client_instance: self.shared.instance,
                    required_sequence: target,
                    durable_sequence: state.durable_sequence,
                    durable_events: state.durable,
                    dropped_before_fence: dropped.saturating_add(state.quarantined),
                    quarantined_before_fence: state.quarantined,
                    last_receipt: state.receipt.clone(),
                };
                return if watermark.dropped_before_fence == 0 {
                    Ok(watermark)
                } else {
                    Err(FlushError::Dropped(Box::new(watermark)))
                };
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(FlushError::TimedOut(Box::new(self.shared.stats(&state))));
            }
            let (next, _) = self
                .shared
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| FlushError::Unavailable("producer barrier wait poisoned"))?;
            state = next;
        }
    }
}

/// Counts a flush or shutdown for as long as it waits, on every return path.
struct BarrierWaiting<'a>(&'a AtomicUsize);
impl<'a> BarrierWaiting<'a> {
    fn enter(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(count)
    }
}
impl Drop for BarrierWaiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Shared {
    fn new(config: DeliveryConfig, instance: [u8; 16]) -> Arc<Self> {
        Arc::new(Self {
            config,
            instance,
            state: Mutex::new(State {
                queue: VecDeque::new(),
                pending: None,
                bytes: 0,
                count: 0,
                sequence: 0,
                admitted_sequence: 0,
                admitted: 0,
                durable_sequence: 0,
                durable: 0,
                batch_number: 0,
                retries: 0,
                force_through: 0,
                closing: false,
                store: None,
                receipt: None,
                failure: None,
                transient: None,
                quarantined: 0,
                quarantined_sequence: 0,
                quarantine: None,
                quarantine_files: 0,
                quarantine_failures: 0,
                quarantine_file: None,
            }),
            control: Mutex::new(()),
            changed: Condvar::new(),
            wake: tokio::sync::Notify::new(),
            retry_wake: tokio::sync::Notify::new(),
            barriers_waiting: AtomicUsize::new(0),
            abort_wake: tokio::sync::Notify::new(),
            owners: AtomicUsize::new(1),
            offered: AtomicU64::new(0),
            spans: AtomicU64::new(0),
            drops: std::array::from_fn(|_| AtomicU64::new(0)),
            exhausted: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        })
    }
    fn finished(&self) {
        // Serialize the completion predicate with barrier waits to prevent a lost notification.
        let guard = self.state.lock();
        self.stopped.store(true, Ordering::SeqCst);
        drop(guard);
        self.changed.notify_all();
    }
    fn bump(&self, counter: &AtomicU64) -> bool {
        let ok = counter
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_add(1)
            })
            .is_ok();
        if !ok {
            self.exhausted.store(true, Ordering::SeqCst);
            self.wake.notify_one();
            self.changed.notify_all();
        }
        ok
    }
    fn reject(&self, reason: DropReason, sequence: Option<u64>) -> Admission {
        self.bump(&self.drops[reason as usize]);
        Admission::Dropped { reason, sequence }
    }
    fn drop_total(&self) -> u64 {
        self.drops.iter().fold(0_u64, |sum, value| {
            sum.saturating_add(value.load(Ordering::SeqCst))
        })
    }
    fn stats(&self, state: &State) -> DeliveryStats {
        let oldest_pending_age = state
            .pending
            .as_ref()
            .map(|pending| pending.at)
            .or_else(|| state.queue.front().map(|queued| queued.at))
            .map(|at| at.elapsed());
        DeliveryStats {
            dropped_by_reason: std::array::from_fn(|index| {
                self.drops[index].load(Ordering::SeqCst)
            }),
            offered_events: self.offered.load(Ordering::SeqCst),
            admitted_events: state.admitted,
            durable_events: state.durable,
            pending_events: state.count,
            pending_bytes: state.bytes,
            oldest_pending_age,
            flush_lag: oldest_pending_age.map_or(Duration::ZERO, |age| {
                age.saturating_sub(self.config.flush_interval)
            }),
            in_flight_events: state
                .pending
                .as_ref()
                .map_or(0, |pending| pending.batch.events.len()),
            last_sequence: state.sequence,
            last_durable_sequence: state.durable_sequence,
            retries: state.retries,
            running: !self.stopped.load(Ordering::SeqCst),
            closing: state.closing,
            terminal_failure: state.failure.clone(),
            last_transient_failure: state.transient.clone(),
            quarantined_events: state.quarantined,
            last_quarantine: state.quarantine.clone(),
            quarantine_dir: self.config.quarantine_dir.clone(),
            quarantine_files: state.quarantine_files,
            last_quarantine_file: state.quarantine_file.clone(),
            quarantine_write_failures: state.quarantine_failures,
        }
    }
    fn fail(&self, error: Error) {
        if let Ok(mut state) = self.state.lock() {
            state.failure = Some(Arc::new(error));
        }
        self.changed.notify_all();
    }
    fn next(&self) -> Result<Action, Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Protocol("producer queue state poisoned"))?;
        if self.exhausted.load(Ordering::SeqCst) {
            return Err(Error::Protocol(
                "producer sequence/diagnostic counters exhausted",
            ));
        }
        if self.aborted.load(Ordering::SeqCst) {
            return Ok(Action::Stop);
        }
        if let Some(pending) = &state.pending {
            return Ok(Action::Send(pending.batch.clone()));
        }
        let Some(first) = state.queue.front() else {
            return Ok(if state.closing {
                Action::Stop
            } else {
                Action::Wait(None)
            });
        };
        let remaining =
            (first.at + self.config.flush_interval).saturating_duration_since(Instant::now());
        let at = first.at;
        if !state.closing
            && state.force_through < first.event.sequence
            && state.queue.len() < self.config.batch_events
            && state.bytes < self.config.batch_bytes
            && !remaining.is_zero()
        {
            return Ok(Action::Wait(Some(remaining)));
        }
        state.batch_number = state
            .batch_number
            .checked_add(1)
            .ok_or(Error::Protocol("producer batch identity exhausted"))?;
        let request_id = format!(
            "events:{}:{:020}",
            purrdf_hash::hex::encode(&self.instance),
            state.batch_number
        );
        let overhead = 1024 + 6 * (request_id.len() + self.config.source.len());
        let mut bytes = 0;
        let mut events = Vec::new();
        while let Some(next) = state.queue.front() {
            if events.len() == self.config.batch_events
                || bytes + next.bytes + overhead > self.config.batch_bytes
            {
                break;
            }
            let next = state
                .queue
                .pop_front()
                .ok_or(Error::Protocol("producer queue front disappeared"))?;
            bytes += next.bytes;
            events.push(next.event);
        }
        if events.is_empty() {
            return Err(Error::Protocol(
                "admitted event cannot fit its configured delivery batch",
            ));
        }
        let batch = Arc::new(EventBatch {
            request_id,
            source: self.config.source.clone(),
            client_instance: self.instance,
            events,
        });
        state.pending = Some(Pending {
            batch: batch.clone(),
            bytes,
            at,
        });
        batch.validate()?;
        Ok(Action::Send(batch))
    }
    fn bind_store(&self, store: &str) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Protocol("producer queue state poisoned"))?;
        if state
            .store
            .as_ref()
            .is_some_and(|expected| expected != store)
        {
            return Err(Error::Domain {
                code: "STORE_MISMATCH".into(),
                message: "producer socket now identifies a different store; pending batch retained"
                    .into(),
                retryable: false,
            });
        }
        state.store = Some(store.to_owned());
        Ok(())
    }
    fn accepted(&self, batch: &EventBatch, receipt: ArtifactReceipt) -> Result<(), Error> {
        // Client::events certifies the exact canonical payload and durable record count.
        // This queue additionally preserves its binding across reconnects.
        self.bind_store(&receipt.store_id)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Protocol("producer queue state poisoned"))?;
        let pending = state
            .pending
            .as_ref()
            .ok_or(Error::Protocol("producer acceptance has no pending batch"))?;
        if pending.batch.as_ref() != batch {
            return Err(Error::Protocol(
                "producer acceptance changed pending identity",
            ));
        }
        let bytes = pending.bytes;
        let count = batch.events.len();
        let bytes_left = state
            .bytes
            .checked_sub(bytes)
            .ok_or(Error::Protocol("producer byte ownership underflow"))?;
        let count_left = state
            .count
            .checked_sub(count)
            .ok_or(Error::Protocol("producer event ownership underflow"))?;
        let durable = state
            .durable
            .checked_add(count as u64)
            .ok_or(Error::Protocol("producer delivery count exhausted"))?;
        let durable_sequence = batch
            .events
            .last()
            .ok_or(Error::Protocol("producer accepted an empty batch"))?
            .sequence;
        state.bytes = bytes_left;
        state.count = count_left;
        state.durable = durable;
        state.durable_sequence = durable_sequence;
        state.receipt = Some(receipt);
        state.transient = None;
        state.pending = None;
        drop(state);
        self.changed.notify_all();
        Ok(())
    }
    /// Write one batch to the quarantine directory, counting the file or the failure. An
    /// error means the bytes are not preserved and the caller must keep them; it names the
    /// refusal code and reason, the directory and the write error, so a persistent write
    /// failure does not hide what the daemon said.
    fn save(&self, batch: &EventBatch, error: &Error) -> Result<PathBuf, Error> {
        let (code, reason) = match error {
            Error::Domain { code, message, .. } => (code.as_str(), message.clone()),
            other => ("CLIENT_REFUSED", other.to_string()),
        };
        let saved = crate::quarantine::save(&self.config.quarantine_dir, batch, code, &reason);
        let context = format!(
            "{code}: {reason}; quarantine write to {} failed",
            self.config.quarantine_dir.display()
        );
        self.record(saved, &context)
    }
    /// Count a save's outcome. A poisoned lock still holds consistent counters (every writer
    /// updates them in one step) and the file is already written, so it is recovered rather
    /// than reported as an unsaved batch.
    fn record(
        &self,
        saved: Result<(PathBuf, bool), crate::quarantine::SaveFailure>,
        context: &str,
    ) -> Result<PathBuf, Error> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match saved {
            Ok((path, created)) => {
                if created {
                    state.quarantine_files = state.quarantine_files.saturating_add(1);
                }
                state.quarantine_file = Some(path.clone());
                Ok(path)
            }
            Err(failure) => {
                if let Some(path) = failure.created {
                    state.quarantine_files = state.quarantine_files.saturating_add(1);
                    state.quarantine_file = Some(path);
                }
                state.quarantine_failures = state.quarantine_failures.saturating_add(1);
                drop(state);
                self.changed.notify_all();
                // The source error rides inside, so its OS error number and cause stay
                // reachable through the error chain.
                Err(Error::Io(std::io::Error::new(
                    failure.error.kind(),
                    WriteContext {
                        context: context.to_owned(),
                        source: failure.error,
                    },
                )))
            }
        }
    }
    /// Before a terminal failure ends delivery, keep what the worker still holds, the pending
    /// batch and the queued events, in quarantine files: the process may exit and the daemon
    /// never answers for them. Returns the error to report: unchanged when everything was
    /// written, else an error that says which held bytes are not preserved (they stay held in
    /// memory and the failure is counted).
    fn save_held(&self, error: Error) -> Error {
        // A panicked worker may have poisoned the state; the held bytes are still readable.
        let (pending, queued, source) = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                state.pending.as_ref().map(|pending| pending.batch.clone()),
                state
                    .queue
                    .iter()
                    .map(|queued| queued.event.clone())
                    .collect::<Vec<_>>(),
                self.config.source.clone(),
            )
        };
        let mut batches = Vec::new();
        batches.extend(pending.as_deref().cloned());
        if let Some(first) = queued.first() {
            batches.push(EventBatch {
                request_id: format!(
                    "events:{}:queued-{:020}",
                    purrdf_hash::hex::encode(&self.instance),
                    first.sequence
                ),
                source,
                client_instance: self.instance,
                events: queued,
            });
        }
        let mut unsaved = Vec::new();
        for batch in &batches {
            if let Err(write) = self.save(batch, &error) {
                unsaved.push(format!("{} ({write})", batch.request_id));
            }
        }
        if unsaved.is_empty() {
            error
        } else {
            Error::Io(std::io::Error::other(format!(
                "{error}; held events not written to quarantine and kept only in memory: {}",
                unsaved.join(", ")
            )))
        }
    }
    /// Give up the immutable pending batch after its bytes reached a quarantine file: its
    /// events stay counted and visible, their quota is released, and later events proceed
    /// instead of queueing behind a batch that can never be accepted.
    fn quarantine(&self, batch: &EventBatch, error: Error) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Protocol("producer queue state poisoned"))?;
        let pending = state
            .pending
            .as_ref()
            .ok_or(Error::Protocol("producer quarantine has no pending batch"))?;
        if pending.batch.as_ref() != batch {
            return Err(Error::Protocol(
                "producer quarantine changed pending identity",
            ));
        }
        let bytes = pending.bytes;
        let count = batch.events.len();
        let bytes_left = state
            .bytes
            .checked_sub(bytes)
            .ok_or(Error::Protocol("producer byte ownership underflow"))?;
        let count_left = state
            .count
            .checked_sub(count)
            .ok_or(Error::Protocol("producer event ownership underflow"))?;
        let quarantined = state
            .quarantined
            .checked_add(count as u64)
            .ok_or(Error::Protocol("producer quarantine count exhausted"))?;
        let settled = batch
            .events
            .last()
            .ok_or(Error::Protocol("producer quarantined an empty batch"))?
            .sequence;
        state.bytes = bytes_left;
        state.count = count_left;
        state.quarantined = quarantined;
        state.quarantined_sequence = settled;
        state.quarantine = Some(Arc::new(error));
        state.transient = None;
        state.pending = None;
        drop(state);
        self.changed.notify_all();
        Ok(())
    }
    fn retry(&self, error: Error) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Protocol("producer queue state poisoned"))?;
        state.retries = state
            .retries
            .checked_add(1)
            .ok_or(Error::Protocol("producer retry count exhausted"))?;
        state.transient = Some(Arc::new(error));
        drop(state);
        self.changed.notify_all();
        Ok(())
    }
}

/// Delay before the next attempt of an unchanged pending batch: it starts at the configured
/// retry interval and doubles after every use up to the configured maximum, so an outage costs
/// one attempt per second at first and one per `retry_max_interval` at most, per client.
struct Backoff {
    initial: Duration,
    max: Duration,
    next: Duration,
}
impl Backoff {
    fn new(config: &DeliveryConfig) -> Self {
        Self {
            initial: config.retry_interval,
            max: config.retry_max_interval,
            next: config.retry_interval,
        }
    }
    /// The delay to wait now; the following one is doubled and capped.
    fn take(&mut self) -> Duration {
        let delay = self.next;
        self.next = self.next.saturating_mul(2).min(self.max);
        delay
    }
    fn reset(&mut self) {
        self.next = self.initial;
    }
}

/// Wait out one retry delay as an event wait. It ends early when a flush or shutdown signals
/// `barrier` while one is still waiting (`waiting` nonzero): the next attempt then runs at once
/// and the backoff sequence is unchanged. A signal with no barrier waiting is a leftover of a
/// call that already returned and is ignored, so it cannot shorten a later outage's first delay.
/// Returns true when `abort` reports last-owner cancellation. Barriers are caller-driven, so
/// this adds no timer.
async fn wait_out_retry_delay(
    abort: &tokio::sync::Notify,
    barrier: &tokio::sync::Notify,
    waiting: &AtomicUsize,
    delay: Duration,
) -> bool {
    let sleep = tokio::time::sleep(delay);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            _ = abort.notified() => return true,
            _ = &mut sleep => return false,
            _ = barrier.notified() => {
                if waiting.load(Ordering::SeqCst) > 0 {
                    return false;
                }
            }
        }
    }
}

/// A failed quarantine write: what was being saved and why, over the I/O error that stopped it.
#[derive(Debug)]
struct WriteContext {
    context: String,
    source: std::io::Error,
}
impl std::fmt::Display for WriteContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.context, self.source)
    }
}
impl std::error::Error for WriteContext {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

enum Action {
    Wait(Option<Duration>),
    Send(Arc<EventBatch>),
    Stop,
}
async fn run(shared: &Shared) -> Result<(), Error> {
    let mut connection: Option<Client> = None;
    let mut backoff = Backoff::new(&shared.config);
    loop {
        match shared.next()? {
            Action::Stop => return Ok(()),
            Action::Wait(duration) => match duration {
                Some(duration) => {
                    tokio::select! { _ = shared.wake.notified() => {}, _ = tokio::time::sleep(duration) => {} }
                }
                None => shared.wake.notified().await,
            },
            Action::Send(batch) => {
                let attempt = async {
                    if connection.is_none() {
                        let client = Client::connect_endpoint(&shared.config.endpoint).await?;
                        shared.bind_store(&client.discovery().store_id)?;
                        connection = Some(client);
                    }
                    connection
                        .as_ref()
                        .ok_or(Error::Protocol(
                            "producer connection missing after discovery",
                        ))?
                        .events(&batch)
                        .await
                };
                let result = tokio::select! {
                    _ = shared.abort_wake.notified() => return Ok(()),
                    result = tokio::time::timeout(shared.config.rpc_timeout, attempt) => match result {
                        Ok(result) => result,
                        Err(_) => Err(Error::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "producer RPC deadline elapsed; delivery unknown; retry immutable pending batch"))),
                    },
                };
                match result {
                    Ok(receipt) => {
                        shared.accepted(&batch, receipt)?;
                        backoff.reset();
                    }
                    Err(error) => {
                        // A daemon answer, even a refusal, proves the connection works; only
                        // a transport, I/O or protocol failure means it may be broken. A
                        // needless reconnect on every busy retry also feeds the daemon's
                        // connection ceiling.
                        if !matches!(error, Error::Domain { .. }) {
                            connection = None;
                        }
                        // A permanent refusal's bytes must reach a quarantine file before the
                        // batch is given up. If they cannot, the batch stays pending, the write
                        // failure is counted and reported as the transient failure whatever its
                        // kind, and the next attempt tries again: that batch is not dropped.
                        let error = if permanent_refusal(&error) {
                            match shared.save(&batch, &error) {
                                Ok(_) => {
                                    shared.quarantine(&batch, error)?;
                                    backoff.reset();
                                    continue;
                                }
                                Err(write) => write,
                            }
                        } else if retryable(&error) {
                            error
                        } else {
                            return Err(error);
                        };
                        shared.retry(error)?;
                        if wait_out_retry_delay(
                            &shared.abort_wake,
                            &shared.retry_wake,
                            &shared.barriers_waiting,
                            backoff.take(),
                        )
                        .await
                        {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

/// Whether this batch can never be accepted, as opposed to the daemon being busy. The
/// daemon marks a busy refusal (`RESOURCE_LIMIT`) retryable, and that batch is held. A full
/// producer registry is no longer a refusal: the daemon stores the batch and counts the lost
/// gap evidence (#215), and a source past the 1024-mapping ceiling is stored without a time
/// mapping (#227). An over-bound batch is answered `INVALID_ARGUMENT` and quarantined. Only an
/// older daemon still answers these with a retryable `RESOURCE_LIMIT`, which stays held as
/// before: the client cannot tell it from a busy daemon, and held bytes are never lost. A
/// non-retryable `INVALID_ARGUMENT` or `CONFLICT` (for example producer sequences that overlap
/// accepted positions, or a source time mapping that differs) is about this batch alone: its
/// bytes go to a quarantine file and later events proceed. The client's own
/// `Error::InvalidArgument` is classed the same way, but the worker validates every batch
/// before it sends one (`Shared::next`), with the checks `Client::events` encodes with, so a
/// batch the client cannot encode ends delivery as a terminal failure through `run`, and
/// `save_held` saves its bytes, instead of reaching this per-batch route. Any other
/// non-retryable refusal (a replaced store, a read-only or unauthorized daemon) is not about
/// this batch: it ends delivery as a terminal failure, and the held bytes are written to
/// quarantine files first.
fn permanent_refusal(error: &Error) -> bool {
    match error {
        Error::Domain {
            code,
            retryable: false,
            ..
        } => code == "INVALID_ARGUMENT" || code == "CONFLICT",
        Error::InvalidArgument(_) => true,
        _ => false,
    }
}

/// Whether reconnecting and retrying the same identity may succeed, including
/// a daemon whose socket is briefly absent or refusing while it restarts.
pub(crate) fn retryable(error: &Error) -> bool {
    match error {
        Error::Io(error) => matches!(
            error.kind(),
            std::io::ErrorKind::NotFound
                | std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::NotConnected
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::WouldBlock
                | std::io::ErrorKind::Interrupted
        ),
        error => error.retryable(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn event() -> ProducerEvent {
        ProducerEvent {
            sequence: 0,
            event_time: 1000,
            kind: crate::EventKind::Log,
            level: crate::EventLevel::Info,
            name: String::new(),
            target: String::new(),
            fields: vec![],
            context: vec![],
            span: None,
            follows_from: None,
        }
    }

    /// INVARIANT: retries back off from the configured interval, double up to the maximum
    /// and never exceed it, and an accepted batch restarts the sequence, so an outage does
    /// not hold a client at a fast retry rate and a recovered daemon is not met with a stale
    /// long delay.
    #[test]
    fn retry_backoff_doubles_to_its_maximum_and_restarts_after_acceptance() {
        let mut config =
            DeliveryConfig::new("unused.sock", "test").with_quarantine_dir("/unused-quarantine");
        assert_eq!(config.retry_interval, Duration::from_secs(1));
        config.retry_max_interval = Duration::from_secs(5);
        let mut backoff = Backoff::new(&config);
        let seen: Vec<_> = (0..5).map(|_| backoff.take().as_secs()).collect();
        assert_eq!(seen, [1, 2, 4, 5, 5]);
        backoff.reset();
        assert_eq!(backoff.take(), Duration::from_secs(1));
    }

    /// INVARIANT: a maximum shorter than the first delay is refused rather than silently
    /// capping the first retry, and the default maximum is within the shared 60 s bound.
    #[test]
    fn retry_maximum_below_the_first_delay_is_refused() {
        let mut config =
            DeliveryConfig::new("unused.sock", "test").with_quarantine_dir("/unused-quarantine");
        config.validate().unwrap();
        config.retry_max_interval = config.retry_interval - Duration::from_millis(1);
        assert!(matches!(config.validate(), Err(Error::InvalidArgument(_))));
    }

    /// INVARIANT: production delivery intervals have a 1 s floor, so no client loop ticks
    /// faster than once a second; the defaults stay valid, the error names the key and floor,
    /// and only the test-timing constructor can go lower.
    #[test]
    fn sub_second_flush_and_retry_intervals_are_refused() {
        let base =
            DeliveryConfig::new("unused.sock", "test").with_quarantine_dir("/unused-quarantine");
        base.validate().unwrap();
        let mut at_floor = base.clone();
        at_floor.flush_interval = MIN_INTERVAL;
        at_floor.retry_interval = MIN_INTERVAL;
        at_floor.validate().unwrap();
        for key in ["flush_interval", "retry_interval"] {
            let mut config = base.clone();
            let short = MIN_INTERVAL - Duration::from_millis(1);
            if key == "flush_interval" {
                config.flush_interval = short;
            } else {
                config.retry_interval = short;
            }
            let Err(Error::InvalidArgument(message)) = config.validate() else {
                panic!("{key} below 1 s was accepted");
            };
            assert!(message.contains(key) && message.contains("1s"), "{message}");
        }
        let fast = base.with_test_timing(
            Duration::from_millis(10),
            Duration::from_millis(10),
            Duration::from_millis(10),
        );
        fast.validate().unwrap();
    }

    /// INVARIANT: a flush or shutdown ends a retry backoff delay at once, and only while it
    /// waits. On a paused clock the delay's virtual elapsed time is the proof: a 30 s delay
    /// with a waiting barrier ends in no virtual time, while the same delay without one, or
    /// with only a leftover signal from a barrier that already returned, runs its full 30 s.
    /// No real sleep is involved.
    #[tokio::test(start_paused = true)]
    async fn a_waiting_barrier_ends_the_retry_delay_at_once_and_a_leftover_signal_does_not() {
        let abort = tokio::sync::Notify::new();
        let barrier = tokio::sync::Notify::new();
        let waiting = AtomicUsize::new(0);
        let delay = Duration::from_secs(30);

        let started = tokio::time::Instant::now();
        assert!(!wait_out_retry_delay(&abort, &barrier, &waiting, delay).await);
        assert_eq!(
            started.elapsed(),
            delay,
            "an unsignalled delay runs in full"
        );

        barrier.notify_one();
        let started = tokio::time::Instant::now();
        assert!(!wait_out_retry_delay(&abort, &barrier, &waiting, delay).await);
        assert_eq!(
            started.elapsed(),
            delay,
            "a leftover signal must not skip the delay"
        );

        let _barrier_waiting = BarrierWaiting::enter(&waiting);
        barrier.notify_one();
        let started = tokio::time::Instant::now();
        assert!(!wait_out_retry_delay(&abort, &barrier, &waiting, delay).await);
        assert!(started.elapsed() < Duration::from_secs(1));

        abort.notify_one();
        assert!(wait_out_retry_delay(&abort, &barrier, &waiting, delay).await);
    }

    /// INVARIANT (#187): a flush or shutdown issued during a real backoff delay ends it. The
    /// retry interval is a production-valid 30 s; after each barrier the worker has retried
    /// again within a real-time bound far below that, so the bound does not depend on the delay.
    /// Barriers time out against the absent daemon, which is irrelevant to the wake.
    #[test]
    fn flush_and_shutdown_during_a_backoff_delay_retry_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = DeliveryConfig::new(dir.path().join("missing.sock"), "test")
            .with_quarantine_dir(dir.path().join("quarantine"));
        config.batch_events = 1;
        config.retry_interval = Duration::from_secs(30);
        config.retry_max_interval = Duration::from_secs(30);
        let delivery = Delivery::start(config).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        assert!(matches!(
            delivery.try_send(event()),
            Admission::Queued { .. }
        ));
        while delivery.stats().unwrap().retries < 1 {
            assert!(Instant::now() < deadline, "no retry began");
            std::thread::sleep(Duration::from_millis(1));
        }
        // The worker is now in (or entering) a 30 s delay.
        assert!(matches!(
            delivery.flush(Duration::from_millis(500)),
            Err(FlushError::TimedOut(_))
        ));
        assert!(
            delivery.stats().unwrap().retries >= 2,
            "flush did not end the delay"
        );
        assert!(matches!(
            delivery.shutdown(Duration::from_millis(500)),
            Err(FlushError::TimedOut(_))
        ));
        assert!(
            delivery.stats().unwrap().retries >= 3,
            "shutdown did not end the delay"
        );
    }

    /// INVARIANT: the real flush and shutdown paths signal the retry wait. The worker of an
    /// empty producer never waits on it, so the permit a barrier leaves is observable without
    /// a race.
    #[test]
    fn flush_and_shutdown_signal_the_retry_wait() {
        use futures::FutureExt;
        let dir = tempfile::tempdir().unwrap();
        let delivery = Delivery::start(
            DeliveryConfig::new(dir.path().join("missing.sock"), "test")
                .with_quarantine_dir(dir.path().join("quarantine")),
        )
        .unwrap();
        delivery.flush(Duration::from_secs(2)).unwrap();
        assert!(
            delivery
                .shared
                .retry_wake
                .notified()
                .now_or_never()
                .is_some()
        );
        delivery.shutdown(Duration::from_secs(2)).unwrap();
        assert!(
            delivery
                .shared
                .retry_wake
                .notified()
                .now_or_never()
                .is_some()
        );
    }

    /// INVARIANT: failed attempts keep retrying the one immutable pending batch with the same
    /// request identity and report each retry in the delivery statistics.
    #[test]
    fn retries_keep_the_pending_identity_and_count_each_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = DeliveryConfig::new(dir.path().join("missing.sock"), "test")
            .with_quarantine_dir(dir.path().join("quarantine"));
        config.batch_events = 1;
        // Sub-second retry intervals are refused in production; this test needs fast retries.
        config = config.with_test_timing(
            Duration::from_secs(2),
            Duration::from_millis(5),
            Duration::from_millis(20),
        );
        let delivery = Delivery::start(config).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        assert!(matches!(
            delivery.try_send(event()),
            Admission::Queued { .. }
        ));
        let first = loop {
            if let Some(batch) = delivery.pending_batch().unwrap() {
                break batch.request_id.clone();
            }
            assert!(Instant::now() < deadline, "no batch began");
            std::thread::sleep(Duration::from_millis(1));
        };
        while delivery.stats().unwrap().retries < 3 {
            assert!(Instant::now() < deadline, "retries were not reported");
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(delivery.pending_batch().unwrap().unwrap().request_id, first);
        assert!(matches!(
            delivery.shutdown(Duration::from_millis(20)),
            Err(FlushError::TimedOut(_))
        ));
    }

    /// INVARIANT: an admission offered while another thread briefly holds the queue lock waits
    /// for it and queues the event; it is never dropped for the lock. The old behavior
    /// discarded such an event as "contended" before it had a sequence, so the loss was
    /// invisible to the daemon. An oversized event is still a counted `Invalid` drop, which
    /// the fence reports.
    #[test]
    fn admission_waits_for_a_held_lock_and_oversized_allocations_are_counted_drops() {
        let dir = tempfile::tempdir().unwrap();
        let delivery = Delivery::start(
            DeliveryConfig::new(dir.path().join("missing.sock"), "test")
                .with_quarantine_dir(dir.path().join("quarantine")),
        )
        .unwrap();
        let guard = delivery.shared.state.lock().unwrap();
        let (about_to_send, started) = std::sync::mpsc::channel();
        let sender = {
            let delivery = delivery.clone();
            std::thread::spawn(move || {
                about_to_send.send(()).unwrap();
                delivery.try_send(event())
            })
        };
        started.recv().unwrap();
        // Long enough that the pre-fix code, which returned at once, would have dropped it.
        std::thread::sleep(Duration::from_millis(100));
        drop(guard);
        assert_eq!(
            sender.join().unwrap(),
            Admission::Queued { sequence: 1 },
            "a briefly held lock must not discard an event"
        );
        let mut oversized = event();
        oversized.name = String::with_capacity(MAX_EVENT_BYTES * 2);
        assert_eq!(
            delivery.try_send(oversized),
            Admission::Dropped {
                reason: DropReason::Invalid,
                sequence: None
            }
        );
        assert_eq!(
            delivery.stats().unwrap().dropped_by_reason[DropReason::Invalid as usize],
            1
        );
        assert!(matches!(
            delivery.shutdown(Duration::from_millis(50)),
            Err(FlushError::TimedOut(_))
        ));
        // With nothing queued, a fence over only drops still reports them and certifies nothing.
        let empty = Delivery::start(
            DeliveryConfig::new(dir.path().join("missing.sock"), "test")
                .with_quarantine_dir(dir.path().join("quarantine")),
        )
        .unwrap();
        let mut oversized = event();
        oversized.name = String::with_capacity(MAX_EVENT_BYTES * 2);
        empty.try_send(oversized);
        match empty.shutdown(Duration::from_secs(2)).unwrap_err() {
            FlushError::Dropped(value) => {
                assert_eq!(value.dropped_before_fence, 1);
                assert_eq!(value.durable_events, 0);
                assert!(value.last_receipt.is_none());
            }
            error => panic!("unexpected barrier error: {error}"),
        }
    }

    #[test]
    fn last_owner_cancels_unresponsive_unix_and_tcp_without_waiting_for_rpc_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("unresponsive.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        for endpoint in [
            crate::Endpoint::Unix(socket),
            crate::Endpoint::Tcp(tcp.local_addr().unwrap()),
        ] {
            let mut config = DeliveryConfig::for_endpoint(endpoint, "test")
                .with_quarantine_dir(dir.path().join("quarantine"));
            config.batch_events = 1;
            config.rpc_timeout = Duration::from_secs(60);
            let delivery = Delivery::start(config).unwrap();
            let deadline = Instant::now() + Duration::from_secs(2);
            assert!(matches!(
                delivery.try_send(event()),
                Admission::Queued { .. }
            ));
            while delivery.pending_batch().unwrap().is_none() {
                assert!(
                    Instant::now() < deadline,
                    "owned worker did not begin a batch"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            let observer = delivery.shared.clone();
            let sibling = delivery.clone();
            drop(delivery);
            assert!(!observer.aborted.load(Ordering::SeqCst));
            drop(sibling);
            while !observer.stopped.load(Ordering::SeqCst) {
                assert!(
                    Instant::now() < deadline,
                    "last-owner cancellation waited for a 60-second RPC deadline"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }

    #[test]
    fn byte_admission_keeps_failed_in_flight_requests_charged() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = DeliveryConfig::new(dir.path().join("missing.sock"), "test")
            .with_quarantine_dir(dir.path().join("quarantine"));
        config.max_queued_events = 8;
        config.max_queued_bytes = MAX_EVENT_BYTES;
        config.batch_events = 1;
        let delivery = Delivery::start(config).unwrap();
        let mut large = event();
        large.name = "x".repeat(6000);
        let deadline = Instant::now() + Duration::from_secs(2);
        assert!(matches!(
            delivery.try_send(large.clone()),
            Admission::Queued { .. }
        ));
        while delivery.pending_batch().unwrap().is_none() {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(delivery.stats().unwrap().pending_events, 1);
        assert!(matches!(
            delivery.try_send(large.clone()),
            Admission::Dropped {
                reason: DropReason::Full,
                ..
            }
        ));
        let stats = delivery.stats().unwrap();
        assert_eq!(stats.in_flight_events, 1);
        assert_eq!(stats.pending_events, 1);
        assert!(stats.pending_bytes <= MAX_EVENT_BYTES);
        assert!(stats.oldest_pending_age.is_some());
        assert!(matches!(
            delivery.shutdown(Duration::from_millis(20)),
            Err(FlushError::TimedOut(_))
        ));
    }

    /// INVARIANT (#216): a terminal failure first writes the queued events to a quarantine
    /// file and returns the error unchanged; if the directory cannot be written the returned
    /// error says the held events are kept only in memory and the failure is counted. Before,
    /// nothing was written and the error did not mention the held bytes.
    #[test]
    fn terminal_failure_saves_held_events_or_says_they_are_unsaved() {
        let dir = tempfile::tempdir().unwrap();
        let held = |quarantine: &std::path::Path| {
            let delivery = Delivery::start(
                DeliveryConfig::new(dir.path().join("missing.sock"), "test")
                    .with_quarantine_dir(quarantine),
            )
            .unwrap();
            assert!(matches!(
                delivery.try_send(event()),
                Admission::Queued { sequence: 1 }
            ));
            let error = delivery
                .shared
                .save_held(Error::Protocol("terminal for the test"));
            (delivery, error)
        };
        let good = dir.path().join("good");
        let (delivery, error) = held(&good);
        assert!(matches!(error, Error::Protocol("terminal for the test")));
        let stats = delivery.stats().unwrap();
        assert_eq!(
            (stats.quarantine_files, stats.quarantine_write_failures),
            (1, 0)
        );
        let text = std::fs::read_to_string(stats.last_quarantine_file.unwrap()).unwrap();
        assert!(text.contains("\"events\":1") && text.contains("terminal for the test"));
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"x").unwrap();
        let (delivery, error) = held(&blocker.join("q"));
        let Error::Io(error) = error else {
            panic!("an unsaved queue must change the terminal error: {error:?}");
        };
        assert!(error.to_string().contains("kept only in memory"), "{error}");
        assert_eq!(delivery.stats().unwrap().quarantine_write_failures, 1);
    }

    /// INVARIANT (#216): with no usable quarantine directory (the empty default of a host
    /// with no absolute `XDG_STATE_HOME` or `HOME`, or a relative path) delivery refuses to
    /// start with an actionable error; there is no volatile fallback for the only copy of a
    /// refused log.
    #[test]
    fn unusable_quarantine_directory_fails_start() {
        for dir in ["", "relative/quarantine"] {
            let config = DeliveryConfig::new("unused.sock", "test").with_quarantine_dir(dir);
            let Err(Error::InvalidArgument(message)) = Delivery::start(config) else {
                panic!("{dir:?} must be refused");
            };
            assert!(message.contains("quarantine_dir"), "{message}");
        }
    }

    /// A worker-less `Shared` over a quarantine directory, so a test drives `save` and
    /// `save_held` with no thread racing it.
    fn shared(quarantine: &std::path::Path) -> Arc<Shared> {
        Shared::new(
            DeliveryConfig::new("unused.sock", "test").with_quarantine_dir(quarantine),
            [9; 16],
        )
    }

    fn batch_of(request: &str, sequence: u64) -> EventBatch {
        EventBatch {
            request_id: request.into(),
            source: "test".into(),
            client_instance: [9; 16],
            events: vec![ProducerEvent {
                sequence,
                ..event()
            }],
        }
    }

    /// INVARIANT: a worker that panicked while holding the state lock poisons it, and the
    /// bytes it held are still saved and reported saved: `save` writes the file and counts it,
    /// and `save_held` returns the terminal error unchanged. The old code wrote the file and
    /// then answered "state poisoned", so a saved batch was reported unsaved.
    #[test]
    fn a_poisoned_state_lock_still_saves_and_reports_the_held_bytes_as_saved() {
        let dir = tempfile::tempdir().unwrap();
        let shared = shared(&dir.path().join("quarantine"));
        let poisoner = shared.clone();
        std::thread::spawn(move || {
            let _held = poisoner.state.lock().unwrap();
            panic!("poison the state lock on purpose");
        })
        .join()
        .unwrap_err();
        assert!(shared.state.is_poisoned());
        let path = shared
            .save(&batch_of("events:poison:1", 1), &Error::Protocol("refused"))
            .expect("a saved batch is not reported unsaved");
        assert!(std::fs::read_to_string(path).unwrap().contains("refused"));
        shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .queue
            .push_back(Queued {
                event: ProducerEvent {
                    sequence: 2,
                    ..event()
                },
                bytes: 0,
                at: Instant::now(),
            });
        let error = shared.save_held(Error::Protocol("terminal for the test"));
        assert!(
            matches!(error, Error::Protocol("terminal for the test")),
            "{error:?}"
        );
        let state = shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!((state.quarantine_files, state.quarantine_failures), (2, 0));
    }

    /// INVARIANT: when the quarantine write fails, the error the worker reports as the
    /// transient failure still says what the daemon refused (code and reason) and which
    /// directory could not be written, with the I/O error. The old code replaced the refusal
    /// with the bare I/O error.
    #[test]
    fn a_failed_quarantine_write_keeps_the_refusal_and_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"x").unwrap();
        let quarantine = blocker.join("q");
        let shared = shared(&quarantine);
        let batch = batch_of("events:blocked:1", 1);
        let io = crate::quarantine::save(&quarantine, &batch, "c", "r")
            .unwrap_err()
            .error;
        let refusal = Error::Domain {
            code: "CONFLICT".into(),
            message: "sequences overlap".into(),
            retryable: false,
        };
        let Err(Error::Io(error)) = shared.save(&batch, &refusal) else {
            panic!("an unwritable directory must fail the save");
        };
        let text = error.to_string();
        for needle in [
            "CONFLICT",
            "sequences overlap",
            &quarantine.display().to_string(),
            &io.to_string(),
        ] {
            assert!(text.contains(needle), "{needle} missing from {text}");
        }
        // The source I/O error survives inside, with its OS error number: the old rebuild
        // from kind and text lost both.
        let source = std::error::Error::source(error.get_ref().unwrap())
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .unwrap();
        assert_eq!(source.raw_os_error(), io.raw_os_error());
        assert!(io.raw_os_error().is_some());
        assert_eq!(shared.state.lock().unwrap().quarantine_failures, 1);
    }

    /// INVARIANT: the client's own refusal is classed permanent and `save` writes it under the
    /// client's own code with the encoder's reason, so those bytes are kept too. This covers
    /// the classification and the file only: the worker validates each batch before the send,
    /// so no deterministic run drives a batch the queue built into that error (it ends
    /// delivery as a terminal failure, saved by `save_held`, which the held-events test covers).
    #[test]
    fn a_batch_the_client_cannot_encode_is_quarantined_under_a_client_code() {
        let refusal = Error::InvalidArgument("event text exceeds 16 KiB");
        assert!(permanent_refusal(&refusal));
        let dir = tempfile::tempdir().unwrap();
        let shared = shared(&dir.path().join("quarantine"));
        let path = shared
            .save(&batch_of("events:encode:1", 1), &refusal)
            .unwrap();
        let text = std::fs::read_to_string(path).unwrap();
        let header = text.lines().next().unwrap();
        assert!(
            header.contains("\"reason_code\":\"CLIENT_REFUSED\""),
            "{header}"
        );
        assert!(header.contains("event text exceeds 16 KiB"), "{header}");
        assert_eq!(shared.state.lock().unwrap().quarantine_files, 1);
    }

    /// INVARIANT: a file linked into place is counted once even when the save then fails on
    /// its scratch cleanup and is retried (the retry reports the file as already existing).
    /// The old code counted it on neither attempt.
    #[test]
    fn a_save_that_failed_after_the_link_counts_its_file_once() {
        let dir = tempfile::tempdir().unwrap();
        let shared = shared(dir.path());
        let linked = dir.path().join("linked.quarantine.jsonl");
        let failure = crate::quarantine::SaveFailure {
            error: std::io::Error::other("scratch removal refused"),
            created: Some(linked.clone()),
        };
        assert!(shared.record(Err(failure), "context").is_err());
        assert_eq!(
            shared
                .record(Ok((linked.clone(), false)), "context")
                .unwrap(),
            linked
        );
        let state = shared.state.lock().unwrap();
        assert_eq!((state.quarantine_files, state.quarantine_failures), (1, 1));
        assert_eq!(state.quarantine_file.as_deref(), Some(linked.as_path()));
    }
}
