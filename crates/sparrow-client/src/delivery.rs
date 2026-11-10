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
}
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
        }
    }
    fn validate(&self) -> Result<(), Error> {
        self.endpoint.validate()?;
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
        {
            return Err(Error::InvalidArgument(
                "invalid delivery labels, queue/batch limits or timing bounds; durations require (0,60s]",
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
    /// A short queue critical section was already occupied.
    Contended,
    /// Event shape, relationships, or owned allocations exceeded admission.
    Invalid,
    /// Shutdown or last-owner cancellation closed admission.
    Closed,
    /// A permanent worker failure prevents further delivery.
    Failed,
    /// Sequence/diagnostic counters exhausted their exact range.
    CounterExhausted,
}

/// Admission never waits for a lock or network operation.
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
    /// Dropped offers counted before this fence; these were never certified.
    pub dropped_before_fence: u64,
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
                "durable sequence {}; {} offered events were dropped before the fence",
                value.durable_sequence, value.dropped_before_fence
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
    pub dropped_by_reason: [u64; 6],
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
}
struct Shared {
    config: DeliveryConfig,
    instance: [u8; 16],
    state: Mutex<State>,
    control: Mutex<()>,
    changed: Condvar,
    wake: tokio::sync::Notify,
    abort_wake: tokio::sync::Notify,
    owners: AtomicUsize,
    offered: AtomicU64,
    spans: AtomicU64,
    drops: [AtomicU64; 6],
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
        let shared = Arc::new(Shared {
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
            }),
            control: Mutex::new(()),
            changed: Condvar::new(),
            wake: tokio::sync::Notify::new(),
            abort_wake: tokio::sync::Notify::new(),
            owners: AtomicUsize::new(1),
            offered: AtomicU64::new(0),
            spans: AtomicU64::new(0),
            drops: std::array::from_fn(|_| AtomicU64::new(0)),
            exhausted: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        });
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
                    Ok(Err(error)) => worker.fail(error),
                    Err(_) => worker.fail(Error::Protocol(
                        "producer delivery worker panicked; no remaining delivery certified",
                    )),
                }
                worker.finished();
            })?;
        Ok(Self { shared })
    }

    /// Offer a bounded event, assigning its sequence without waiting for a lock or I/O.
    pub fn try_send(&self, mut event: ProducerEvent) -> Admission {
        let shared = &self.shared;
        if !shared.bump(&shared.offered) {
            return shared.reject(DropReason::CounterExhausted, None);
        }
        let bytes = match event.admission_bytes() {
            Ok(bytes) => bytes,
            Err(_) => return shared.reject(DropReason::Invalid, None),
        };
        let mut state = match shared.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => return shared.reject(DropReason::Contended, None),
            Err(TryLockError::Poisoned(_)) => return shared.reject(DropReason::Failed, None),
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
        loop {
            if self.shared.exhausted.load(Ordering::SeqCst) {
                return Err(FlushError::Unavailable(
                    "producer sequence/diagnostic counters exhausted",
                ));
            }
            if let Some(error) = &state.failure {
                return Err(FlushError::Delivery(error.clone()));
            }
            if state.durable_sequence >= target
                && (!closing || self.shared.stopped.load(Ordering::SeqCst))
            {
                let watermark = DeliveryWatermark {
                    client_instance: self.shared.instance,
                    required_sequence: target,
                    durable_sequence: state.durable_sequence,
                    durable_events: state.durable,
                    dropped_before_fence: dropped,
                    last_receipt: state.receipt.clone(),
                };
                return if dropped == 0 {
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

impl Shared {
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
                        connection = None;
                        if !retryable(&error) {
                            return Err(error);
                        }
                        shared.retry(error)?;
                        tokio::select! { _ = shared.abort_wake.notified() => return Ok(()),
                        _ = tokio::time::sleep(backoff.take()) => {} }
                    }
                }
            }
        }
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
        let mut config = DeliveryConfig::new("unused.sock", "test");
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
        let mut config = DeliveryConfig::new("unused.sock", "test");
        config.validate().unwrap();
        config.retry_max_interval = config.retry_interval - Duration::from_millis(1);
        assert!(matches!(config.validate(), Err(Error::InvalidArgument(_))));
    }

    /// INVARIANT: failed attempts keep retrying the one immutable pending batch with the same
    /// request identity and report each retry in the delivery statistics.
    #[test]
    fn retries_keep_the_pending_identity_and_count_each_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = DeliveryConfig::new(dir.path().join("missing.sock"), "test");
        config.batch_events = 1;
        config.retry_interval = Duration::from_millis(5);
        config.retry_max_interval = Duration::from_millis(20);
        let delivery = Delivery::start(config).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while matches!(
            delivery.try_send(event()),
            Admission::Dropped {
                reason: DropReason::Contended,
                ..
            }
        ) {
            assert!(Instant::now() < deadline, "admission remained contended");
            std::thread::yield_now();
        }
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

    #[test]
    fn contended_and_oversized_allocations_are_drops_and_empty_shutdown_reports_gaps() {
        let dir = tempfile::tempdir().unwrap();
        let delivery =
            Delivery::start(DeliveryConfig::new(dir.path().join("missing.sock"), "test")).unwrap();
        let guard = delivery.shared.state.lock().unwrap();
        assert_eq!(
            delivery.try_send(event()),
            Admission::Dropped {
                reason: DropReason::Contended,
                sequence: None
            }
        );
        drop(guard);
        let mut oversized = event();
        oversized.name = String::with_capacity(MAX_EVENT_BYTES * 2);
        assert_eq!(
            delivery.try_send(oversized),
            Admission::Dropped {
                reason: DropReason::Invalid,
                sequence: None
            }
        );
        match delivery.shutdown(Duration::from_secs(2)).unwrap_err() {
            FlushError::Dropped(value) => {
                assert_eq!(value.dropped_before_fence, 2);
                assert_eq!(value.durable_events, 0);
                assert!(value.last_receipt.is_none());
            }
            error => panic!("unexpected barrier error: {error}"),
        }
        assert!(!delivery.stats().unwrap().running);
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
            let mut config = DeliveryConfig::for_endpoint(endpoint, "test");
            config.batch_events = 1;
            config.rpc_timeout = Duration::from_secs(60);
            let delivery = Delivery::start(config).unwrap();
            let deadline = Instant::now() + Duration::from_secs(2);
            while matches!(
                delivery.try_send(event()),
                Admission::Dropped {
                    reason: DropReason::Contended,
                    ..
                }
            ) {
                assert!(
                    Instant::now() < deadline,
                    "owned worker admission remained contended"
                );
                std::thread::yield_now();
            }
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
        let mut config = DeliveryConfig::new(dir.path().join("missing.sock"), "test");
        config.max_queued_events = 8;
        config.max_queued_bytes = MAX_EVENT_BYTES;
        config.batch_events = 1;
        let delivery = Delivery::start(config).unwrap();
        let mut large = event();
        large.name = "x".repeat(6000);
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match delivery.try_send(large.clone()) {
                Admission::Queued { .. } => break,
                Admission::Dropped {
                    reason: DropReason::Contended,
                    ..
                } => {
                    assert!(Instant::now() < deadline);
                    std::thread::yield_now();
                }
                admission => panic!("unexpected admission: {admission:?}"),
            }
        }
        while delivery.pending_batch().unwrap().is_none() {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(delivery.stats().unwrap().pending_events, 1);
        loop {
            match delivery.try_send(large.clone()) {
                Admission::Dropped {
                    reason: DropReason::Full,
                    ..
                } => break,
                Admission::Dropped {
                    reason: DropReason::Contended,
                    ..
                } => {
                    assert!(Instant::now() < deadline);
                    std::thread::yield_now();
                }
                admission => panic!("byte quota did not refuse input: {admission:?}"),
            }
        }
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
}
