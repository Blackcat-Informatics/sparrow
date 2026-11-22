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
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
mod lossy;
mod spooled;

/// Explicit producer resource and timing bounds.
#[derive(Debug, Clone)]
pub struct DeliveryConfig {
    /// Admission and persistence semantics for this handle.
    pub mode: crate::DeliveryMode,
    /// Explicit Unix or public TCP daemon destination.
    pub endpoint: crate::Endpoint,
    /// Optional store identity or role required at discovery on every connection.
    pub expect: Option<crate::Expect>,
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
    /// Directory that keeps every batch set aside, with the reason and count, as one file per
    /// batch. Created on first use. The worker never deletes a file here; only a successful
    /// [`crate::replay_quarantine`] removes one. Its size is bounded only by the disk. Default: `sparrow-client/quarantine` under
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
    /// Nonwaiting in-memory delivery. Every validly allocated offer sequence is accounted for.
    pub fn lossy(endpoint: crate::Endpoint, source: impl Into<String>) -> Self {
        let mut config = Self::for_endpoint(endpoint, source);
        config.mode = crate::DeliveryMode::LossyMemory;
        config
    }

    /// Use the bounded persistent journal with counted best-effort admission.
    pub fn lossy_spooled(
        endpoint: crate::Endpoint,
        source: impl Into<String>,
        spool: crate::SpoolConfig,
    ) -> Self {
        let mut config = Self::for_endpoint(endpoint, source);
        config.mode = crate::DeliveryMode::LossySpooled(spool);
        config
    }

    fn disk_free(&self) -> bool {
        matches!(self.mode, crate::DeliveryMode::LossyMemory)
    }
    /// Send to the local group-accessible producer socket.
    pub fn local(source: impl Into<String>) -> Self {
        Self::new("/run/sparrow-ingest/ingest.sock", source)
    }

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
            mode: crate::DeliveryMode::LossyAspirational,
            endpoint,
            expect: None,
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

    /// Configure durable offline delivery to an explicit endpoint and spool directory.
    pub fn lossless(
        endpoint: crate::Endpoint,
        source: impl Into<String>,
        spool: crate::SpoolConfig,
    ) -> Self {
        let mut config = Self::for_endpoint(endpoint, source);
        config.mode = crate::DeliveryMode::LosslessConfirmed(spool);
        config
    }

    /// Configure durable offline delivery under the application state directory.
    /// One source owns one spool. Set an explicit directory for simultaneous instances.
    pub fn configured(endpoint: crate::Endpoint, source: impl Into<String>) -> Self {
        let source = source.into();
        let id = purrdf_hash::hex::encode(purrdf_hash::blake3::hash(source.as_bytes()).as_bytes());
        let path = crate::quarantine::default_dir()
            .parent()
            .map_or_else(PathBuf::new, |parent| parent.join("spool").join(id));
        Self::lossless(endpoint, source, crate::SpoolConfig::new(path))
    }

    /// Default-spooled producer for the local ingest socket.
    pub fn configured_local(source: impl Into<String>) -> Self {
        Self::configured(
            crate::Endpoint::Unix("/run/sparrow-ingest/ingest.sock".into()),
            source,
        )
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
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if let Some(spool) = self.mode.spool() {
            spool.validate()?;
        }
        self.endpoint.validate()?;
        if let Some(expect) = &self.expect {
            expect.validate()?;
        }
        if matches!(self.mode, crate::DeliveryMode::LossyAspirational)
            && !self.quarantine_dir.is_absolute()
        {
            return Err(Error::InvalidArgument(
                "quarantine_dir must be an absolute directory: set DeliveryConfig::quarantine_dir, or provide an absolute XDG_STATE_HOME or HOME for the default",
            ));
        }
        let upper = Duration::from_secs(60);
        for (valid, diagnostic) in [
            (
                (1..=512).contains(&self.source.len()),
                "source must contain 1..512 bytes",
            ),
            (
                (1..=16_384).contains(&self.max_queued_events),
                "max_queued_events must be 1..16384",
            ),
            (
                (MAX_EVENT_BYTES..=64 * 1024 * 1024).contains(&self.max_queued_bytes),
                "max_queued_bytes must be 64 KiB..64 MiB",
            ),
            (
                (1..=MAX_EVENT_BATCH_EVENTS).contains(&self.batch_events),
                "batch_events must be 1..256",
            ),
            (
                (MAX_EVENT_BYTES + 8192..=MAX_EVENT_BATCH_BYTES).contains(&self.batch_bytes),
                "batch_bytes must be 72 KiB..1 MiB",
            ),
            (
                !self.rpc_timeout.is_zero() && self.rpc_timeout <= upper,
                "rpc_timeout must be positive and at most 60s",
            ),
            (
                !self.flush_interval.is_zero()
                    && (self.interval_floor..=upper).contains(&self.flush_interval),
                "flush_interval must be 1s..60s in production",
            ),
            (
                !self.retry_interval.is_zero()
                    && (self.interval_floor..=upper).contains(&self.retry_interval),
                "retry_interval must be 1s..60s in production",
            ),
            (
                (self.retry_interval..=upper).contains(&self.retry_max_interval),
                "retry_max_interval must be at least retry_interval and at most 60s",
            ),
        ] {
            if !valid {
                return Err(Error::InvalidArgument(diagnostic));
            }
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
    /// The worker ended on a local invariant failure (a poisoned lock, an exhausted counter,
    /// a panic). No daemon refusal and no transport failure ends the worker: those hold or
    /// set aside the batch and delivery carries on.
    Failed,
    /// Sequence/diagnostic counters exhausted their exact range.
    CounterExhausted,
    /// A nonwaiting offer found the in-memory queue lock occupied.
    Contended,
    /// The daemon permanently refused the batch or its destination changed.
    Refused,
    /// A delayed offer arrived behind an already dispatched sequence.
    Late,
    /// A lossy spooled offer exhausted the journal's segment budget.
    SpoolFull,
}
const DROP_REASONS: usize = DropReason::SpoolFull as usize + 1;

/// Immediate memory admission. Disk-free lossy producers also refuse queue contention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Local confirmation was refused and no durable acceptance is claimed.
    Refused {
        /// Machine-readable refusal reason.
        reason: Refusal,
    },
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

/// Refusal of a disk-confirmed offer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum Refusal {
    /// The bounded disk spool has no room for this entry.
    SpoolFull,
    /// The bounded memory intake remained occupied through the caller's wait.
    Busy,
    /// Invalid event, oversized cursor, or confirmed send on a memory-only handle.
    Invalid,
    /// Shutdown has closed this producer.
    Closed,
    /// Disk intake failed. The underlying failure is visible in delivery stats.
    SpoolFailed,
}

impl Refusal {
    fn drop_reason(self) -> DropReason {
        match self {
            Self::Busy | Self::SpoolFull => DropReason::Full,
            Self::Invalid => DropReason::Invalid,
            Self::Closed => DropReason::Closed,
            Self::SpoolFailed => DropReason::Failed,
        }
    }
}

/// Locally synchronized acceptance of a producer event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpoolReceipt {
    /// Persistent instance owning the spool.
    pub client_instance: [u8; 16],
    /// Locally synchronized event sequence.
    pub sequence: u64,
}

/// A refused offer retains its input. A failed post-admission write has an unknown outcome.
#[derive(Debug)]
pub enum SendError {
    /// No sequence assigned. The application can retry the original input.
    Refused {
        /// Reason for the refusal.
        reason: Refusal,
        /// Original event with its fields intact.
        event: Box<ProducerEvent>,
    },
    /// A sequence was assigned but disk acknowledgement was lost. Consult the recovered cursor.
    Uncertain {
        /// Assigned spool sequence.
        sequence: u64,
    },
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused { reason, .. } => write!(f, "spool refused event: {reason:?}"),
            Self::Uncertain { sequence } => {
                write!(f, "spool append outcome unknown for sequence {sequence}")
            }
        }
    }
}
impl std::error::Error for SendError {}

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
    /// Dropped offers and quarantined events first counted by this fence, that is, since the
    /// previous barrier that returned a watermark or `Dropped`; these were never certified.
    /// A barrier with nothing new is clean. `DeliveryStats` keeps the running totals.
    pub dropped_before_fence: u64,
    /// The part of `dropped_before_fence` that is kept in quarantine files, not lost; replay a
    /// file with [`crate::replay_quarantine`].
    pub quarantined_before_fence: u64,
    /// Last durable batch receipt, absent before any accepted batch.
    pub last_receipt: Option<ArtifactReceipt>,
}

/// Explicit barrier failure; no successful prefix certifies missing delivery.
#[derive(Debug)]
pub enum FlushError {
    /// Another flush/shutdown already owns the bounded control slot.
    Busy,
    /// Admitted input reached durability, but input offered since the previous barrier was
    /// dropped or set aside. The next barrier counts only what happens after this one.
    Dropped(Box<DeliveryWatermark>),
    /// The deadline elapsed; accepted progress may still advance afterward.
    TimedOut(Box<DeliveryStats>),
    /// The worker ended on a local invariant or storage failure. The delivery mode
    /// determines whether pending input remains on disk or is counted as lost.
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
    /// Stable instance for joining local progress and the server's producer window.
    pub client_instance: [u8; 16],
    /// Assigned disk-free offer sequences lost to a named drop reason.
    pub dropped_sequences: u64,
    /// Latest permanent disk-free refusal, including the typed daemon code and message.
    pub last_refusal: Option<Arc<Error>>,
    /// Observed replacement store. Disk-free offers are refused until explicitly rebound.
    pub store_mismatch: Option<String>,
    /// Configured disk intake and recovery progress.
    pub spool: Option<crate::SpoolStats>,
    /// Confirmed-offer refusals indexed by [`Refusal`]. These never represent lost accepted events.
    pub refused_by_reason: [u64; 5],
    /// Failure currently holding the disk prefix pending. Its request number is in
    /// [`SpoolStats::sealed_batch`](crate::SpoolStats::sealed_batch) after sealing.
    pub held: Option<Arc<Error>>,
    /// Offers observed by the producer handle, including refused input.
    pub offered_events: u64,
    /// Offers admitted into retained delivery state.
    pub admitted_events: u64,
    /// Events acknowledged durably by the daemon.
    pub durable_events: u64,
    /// Refusals indexed by `DropReason`'s discriminant.
    pub dropped_by_reason: [u64; DROP_REASONS],
    /// Queued plus in-flight observations.
    pub pending_events: usize,
    /// Conservative in-memory event charge. Disk occupancy is reported by `spool.bytes`.
    pub pending_bytes: usize,
    /// Monotonic age of the oldest retained observation, including in-flight retries.
    pub oldest_pending_age: Option<Duration>,
    /// Time beyond the batching interval for the oldest pending observation.
    pub flush_lag: Duration,
    /// Observations retained in the immutable pending batch.
    pub in_flight_events: usize,
    /// Highest assigned sequence, including refused attempts in disk-free mode.
    pub last_sequence: u64,
    /// Last durably accepted sequence.
    pub last_durable_sequence: u64,
    /// Retry attempts following transient failures.
    pub retries: u64,
    /// Retries caused by the daemon's free-space floor (#257): the daemon answered, the batch
    /// is held unchanged, and delivery resumes when space returns. Counted within `retries`;
    /// the latest such answer is `last_transient_failure` until a batch is acknowledged.
    pub floor_refusals: u64,
    /// Whether the owned worker has not completed shutdown.
    pub running: bool,
    /// Whether explicit shutdown closed producer admission.
    pub closing: bool,
    /// A local invariant or spool failure that ended the worker. Disk-free mode counts
    /// pending input as lost. Memory mode writes its pending bodies to quarantine.
    pub terminal_failure: Option<Arc<Error>>,
    /// Last failure that held the pending batch for a retry (a busy or paused daemon, a lost
    /// connection, or a refusal about the daemon such as `READ_ONLY`),
    /// cleared when a batch is acknowledged or set aside.
    pub last_transient_failure: Option<Arc<Error>>,
    /// Admitted events whose batch was set aside: the daemon refused it with a non-retryable
    /// refusal about the request (`INVALID_ARGUMENT`, `CONFLICT`, an input-bound
    /// `RESOURCE_LIMIT`, `STORE_MISMATCH` or a code this client does not know), or the client cannot send it: an answer
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
    /// Events held in memory only: a failure that ended delivery found their quarantine
    /// write failing, and the worker keeps retrying it. Zero once they are in files.
    pub held_unsaved_events: u64,
}

struct Queued {
    disk: u64,
    token: Vec<u8>,
    ack: Option<tokio::sync::oneshot::Sender<()>>,
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
    lossy: lossy::Progress,
    released_workers: u8,
    held: Option<Arc<Error>>,
    force_local: u64,
    spool: Option<crate::SpoolStats>,
    reserved_disk: u64,
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
    floor_refusals: u64,
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
    /// Drop and quarantine totals the last successful or `Dropped` barrier already reported.
    reported_drops: u64,
    reported_quarantined: u64,
    /// Held events whose terminal-failure quarantine write has not yet succeeded; the worker
    /// keeps retrying the write and the events are never counted delivered.
    held_unsaved_events: u64,
}
struct Shared {
    request_prefix: String,
    spool: Option<Arc<Mutex<crate::spool::Log>>>,
    intake_work: Condvar,
    intake_room: Arc<tokio::sync::Notify>,
    refusals: [AtomicU64; 5],
    span_limit: u64,
    config: DeliveryConfig,
    instance: [u8; 16],
    state: Arc<Mutex<State>>,
    changed: Arc<Condvar>,
    async_changed: Arc<tokio::sync::Notify>,
    wake: tokio::sync::Notify,
    /// Signalled by every flush or shutdown so a waiting retry delay ends at once. Kept apart
    /// from `wake`, which each admitted event signals and would otherwise cut the backoff short.
    retry_wake: tokio::sync::Notify,
    /// Explicit operator retry for a permanently held spool prefix.
    operator_retry: tokio::sync::Notify,
    /// Flush and shutdown calls currently waiting. A `retry_wake` signal ends a retry delay only
    /// while this is nonzero, so a signal left by a barrier that already returned cannot shorten
    /// the first delay of a later outage.
    barrier_active: AtomicBool,
    abort_wake: tokio::sync::Notify,
    owners: AtomicUsize,
    offered: AtomicU64,
    spans: AtomicU64,
    drops: [AtomicU64; DROP_REASONS],
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
            let state = self.shared.state.lock();
            self.shared.aborted.store(true, Ordering::SeqCst);
            drop(state);
            self.shared.abort_wake.notify_one();
            self.shared.wake.notify_one();
            self.shared.intake_work.notify_all();
        }
    }
}

impl Delivery {
    /// Start a producer from typed configuration on the Tokio blocking pool.
    pub async fn start_async(config: DeliveryConfig) -> Result<Self, Error> {
        tokio::task::spawn_blocking(move || Self::start(config))
            .await
            .map_err(std::io::Error::other)?
    }

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
        let spool = config
            .mode
            .spool()
            .map(|settings| crate::spool::Log::open(settings, &config.source, instance))
            .transpose()?;
        let shared = Shared::with_spool(config, instance, spool);
        let worker = shared.clone();
        let completed = spooled::Completion::new(&shared);
        std::thread::Builder::new()
            .name("sparrow-delivery".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    runtime.block_on(tokio::task::LocalSet::new().run_until(async {
                        if worker.spool.is_some() {
                            spooled::deliver(&worker).await
                        } else {
                            run(&worker).await
                        }
                    }))
                }));
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) if worker.spool.is_some() || worker.config.disk_free() => {
                        worker.fail(error)
                    }
                    Ok(Err(error)) => worker.settle_failure(&runtime, error),
                    Err(_) if worker.spool.is_some() || worker.config.disk_free() => {
                        worker.fail(Error::Protocol("producer sender panicked"))
                    }
                    Err(_) => worker.settle_failure(
                        &runtime,
                        Error::Protocol(
                            "producer delivery worker panicked; no remaining delivery certified",
                        ),
                    ),
                }
                drop(runtime);
                worker.finished();
                drop(worker);
                if let Some(completed) = completed {
                    completed.release();
                }
            })?;
        if shared.spool.is_some()
            && let Err(error) = spooled::start_intake(&shared)
        {
            shared.aborted.store(true, Ordering::Release);
            shared.abort_wake.notify_one();
            shared.wake.notify_one();
            return Err(error);
        }
        Ok(Self { shared })
    }

    /// Offer a bounded event, assigning its sequence without waiting for I/O.
    pub fn try_send(&self, event: ProducerEvent) -> Admission {
        let shared = &self.shared;
        if shared.spool.is_some() {
            return self.spool_offer(event);
        }
        let ticket = match shared.offer_ticket() {
            Some(n) => n,
            None => return shared.reject(DropReason::CounterExhausted, None),
        };
        self.enqueue_memory(event, ticket)
    }

    fn enqueue_memory(&self, mut event: ProducerEvent, ticket: u64) -> Admission {
        let shared = &self.shared;
        let nonwaiting = shared.config.disk_free();
        let bytes = match event.admission_bytes() {
            Ok(bytes) => bytes,
            Err(_) => return shared.reject(DropReason::Invalid, nonwaiting.then_some(ticket)),
        };
        // The original memory policy waits for a bounded critical section. Disk-free
        // callers can immediately observe a sequenced loss if another producer holds it.
        let mut state = if nonwaiting {
            match shared.state.try_lock() {
                Ok(state) => state,
                Err(std::sync::TryLockError::WouldBlock) => {
                    return shared.reject(DropReason::Contended, Some(ticket));
                }
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return shared.reject(DropReason::Closed, Some(ticket));
                }
            }
        } else if let Ok(state) = shared.state.lock() {
            state
        } else {
            return shared.reject(DropReason::Failed, None);
        };
        if state.failure.is_some() || shared.exhausted.load(Ordering::SeqCst) {
            return if nonwaiting {
                shared.reject(DropReason::Closed, Some(ticket))
            } else {
                shared.reject(DropReason::Failed, None)
            };
        }
        if state.closing || shared.aborted.load(Ordering::SeqCst) {
            return shared.reject(DropReason::Closed, nonwaiting.then_some(ticket));
        }
        if nonwaiting && state.lossy.mismatch.is_some() {
            return shared.reject(DropReason::Refused, Some(ticket));
        }
        let Some(sequence) = (if nonwaiting {
            Some(ticket)
        } else {
            state.sequence.checked_add(1)
        }) else {
            shared.exhausted.store(true, Ordering::SeqCst);
            shared.wake.notify_one();
            return shared.reject(DropReason::CounterExhausted, None);
        };
        state.sequence = state.sequence.max(sequence);
        if state.count == shared.config.max_queued_events
            || bytes > shared.config.max_queued_bytes - state.bytes
        {
            return shared.reject(DropReason::Full, Some(sequence));
        }
        event.sequence = sequence;
        state.bytes += bytes;
        state.count += 1;
        state.admitted += 1;
        state.admitted_sequence = state.admitted_sequence.max(sequence);
        state.queue.push_back(Queued {
            disk: 0,
            token: Vec::new(),
            ack: None,
            event,
            bytes,
            at: Instant::now(),
        });
        drop(state);
        shared.wake.notify_one();
        Admission::Queued { sequence }
    }

    /// A capture adapter has no caller to reclaim a refused event. Report that loss at its fence.
    pub(crate) fn observe(&self, event: ProducerEvent) {
        if let Admission::Refused { reason } = self.try_send(event) {
            self.shared.reject(reason.drop_reason(), None);
        }
    }

    /// Account for an observation refused before an event could be constructed.
    /// Capture adapters use this same drop accounting and durable fence law.
    pub(crate) fn capture_failed(&self) {
        if self.shared.spool.is_some() {
            self.shared
                .bump(&self.shared.refusals[Refusal::Invalid as usize]);
        }
        if let Some(sequence) = self.shared.offer_ticket() {
            self.shared.reject(
                DropReason::Invalid,
                self.shared.config.disk_free().then_some(sequence),
            );
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
                value
                    .checked_add(1)
                    .filter(|id| *id <= self.shared.span_limit)
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

    /// Force delivery through current admitted input; drops since the previous barrier return
    /// an explicit gap error.
    /// Timeout leaves the owned worker and immutable pending request running.
    pub fn flush(&self, timeout: Duration) -> Result<DeliveryWatermark, FlushError> {
        self.barrier(timeout, false)
    }

    /// Close admission, drain current input, and wait for the worker/socket to finish.
    /// A timed-out shutdown remains closed and may be retried.
    pub fn shutdown(&self, timeout: Duration) -> Result<DeliveryWatermark, FlushError> {
        self.barrier(timeout, true)
    }

    /// Wait asynchronously for admitted events to reach remote durability.
    /// Cancelling the wait releases the control slot and leaves gap reporting to the next fence.
    pub async fn flush_async(&self, timeout: Duration) -> Result<DeliveryWatermark, FlushError> {
        self.barrier_async(timeout, false).await
    }

    /// Close admission and asynchronously wait for the worker to drain and stop.
    /// Cancellation or timeout leaves admission closed. A later shutdown can finish the wait.
    pub async fn shutdown_async(&self, timeout: Duration) -> Result<DeliveryWatermark, FlushError> {
        self.barrier_async(timeout, true).await
    }

    fn barrier(&self, timeout: Duration, closing: bool) -> Result<DeliveryWatermark, FlushError> {
        let deadline = Instant::now().checked_add(timeout).ok_or(DEADLINE_RANGE)?;
        let fence = self.shared.fence(closing)?;
        let mut state = self.shared.barrier_state()?;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if let Some(result) = fence.result(&mut state, remaining.is_zero()) {
                return result;
            }
            (state, _) = self
                .shared
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| FlushError::Unavailable("producer barrier wait poisoned"))?;
        }
    }

    async fn barrier_async(
        &self,
        timeout: Duration,
        closing: bool,
    ) -> Result<DeliveryWatermark, FlushError> {
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or(DEADLINE_RANGE)?;
        let fence = self.shared.fence(closing)?;
        loop {
            // Exactly one barrier owns the slot. notify_one retains a permit if the
            // worker settles the batch between inspecting the state and awaiting the signal.
            {
                let mut state = self.shared.barrier_state()?;
                if let Some(result) =
                    fence.result(&mut state, tokio::time::Instant::now() >= deadline)
                {
                    return result;
                }
            }
            let _ = tokio::time::timeout_at(deadline, self.shared.async_changed.notified()).await;
        }
    }
}

const DEADLINE_RANGE: FlushError =
    FlushError::Unavailable("producer barrier deadline out of range");

/// The sole synchronous or asynchronous observer of the captured delivery prefix.
struct Fence<'a> {
    shared: &'a Shared,
    target: u64,
    dropped: u64,
    closing: bool,
}

impl Drop for Fence<'_> {
    fn drop(&mut self) {
        self.shared.barrier_active.store(false, Ordering::SeqCst);
    }
}

impl Fence<'_> {
    fn result(
        &self,
        state: &mut State,
        expired: bool,
    ) -> Option<Result<DeliveryWatermark, FlushError>> {
        if self.shared.exhausted.load(Ordering::SeqCst) {
            return Some(Err(FlushError::Unavailable(
                "producer sequence/diagnostic counters exhausted",
            )));
        }
        if let Some(error) = &state.failure {
            return Some(Err(FlushError::Delivery(error.clone())));
        }
        let unfinished = if self.shared.config.disk_free() {
            state.cut_losses().is_none()
        } else {
            state.durable_sequence.max(state.quarantined_sequence) < self.target
        };
        if unfinished
            || state
                .spool
                .as_ref()
                .is_some_and(|spool| spool.cleanup_pending)
                && self.target >= state.durable_sequence
            || self.closing
                && (!self.shared.stopped.load(Ordering::SeqCst)
                    || state.spool.is_some() && !spooled::Completion::both_released(state))
        {
            return expired.then(|| Err(FlushError::TimedOut(Box::new(self.shared.stats(state)))));
        }
        let new_quarantined = state.quarantined.saturating_sub(state.reported_quarantined);
        let cut_losses = state.cut_losses().unwrap_or(0);
        let watermark = DeliveryWatermark {
            client_instance: self.shared.instance,
            required_sequence: self.target,
            durable_sequence: state.durable_sequence,
            durable_events: state.durable,
            dropped_before_fence: self
                .dropped
                .saturating_sub(state.reported_drops)
                .saturating_add(cut_losses.saturating_sub(state.lossy.reported_losses))
                .saturating_add(new_quarantined),
            quarantined_before_fence: new_quarantined,
            last_receipt: state.receipt.clone(),
        };
        state.reported_drops = state.reported_drops.max(self.dropped);
        state.reported_quarantined = state.quarantined;
        state.lossy.reported_losses = cut_losses;
        Some(if watermark.dropped_before_fence == 0 {
            Ok(watermark)
        } else {
            Err(FlushError::Dropped(Box::new(watermark)))
        })
    }
}

impl Shared {
    fn barrier_state(&self) -> Result<std::sync::MutexGuard<'_, State>, FlushError> {
        self.state
            .lock()
            .map_err(|_| FlushError::Unavailable("producer queue state poisoned"))
    }

    fn fence(&self, closing: bool) -> Result<Fence<'_>, FlushError> {
        let mut state = self.barrier_state()?;
        if self.barrier_active.swap(true, Ordering::SeqCst) {
            return Err(FlushError::Busy);
        }
        if self.config.disk_free() {
            state.capture_cut();
        }
        let fence = Fence {
            shared: self,
            target: state.admitted_sequence,
            dropped: self.drop_total().saturating_sub(state.lossy.worker_losses),
            closing,
        };
        state.closing |= closing;
        state.force_through = state.force_through.max(fence.target);
        drop(state);
        self.intake_work.notify_all();
        if closing {
            self.intake_room.notify_waiters();
        }
        self.wake.notify_one();
        self.retry_wake.notify_one();
        Ok(fence)
    }

    fn notify_barrier(&self) {
        if self.barrier_active.load(Ordering::SeqCst) {
            self.changed.notify_all();
            self.async_changed.notify_one();
        }
    }
    #[cfg(test)]
    fn new(config: DeliveryConfig, instance: [u8; 16]) -> Arc<Self> {
        Self::with_spool(config, instance, None)
    }
    fn with_spool(
        config: DeliveryConfig,
        mut instance: [u8; 16],
        spool: Option<crate::spool::Log>,
    ) -> Arc<Self> {
        let (mut sequence, mut confirmed, mut span, mut span_limit) = (0, 0, 0, u64::MAX);
        let stats = spool.as_ref().map(|log| {
            instance = log.head.instance;
            sequence = log.stats.spooled_sequence;
            confirmed = log.head.confirmed;
            span = u64::from(log.head.epoch - 1) << 32;
            span_limit = span + u64::from(u32::MAX);
            log.stats.clone()
        });
        let queue = if config.disk_free() {
            VecDeque::with_capacity(config.max_queued_events)
        } else {
            VecDeque::new()
        };
        Arc::new(Self {
            request_prefix: format!("events:{}:", purrdf_hash::hex::encode(&instance)),
            spool: spool.map(|log| Arc::new(Mutex::new(log))),
            intake_work: Condvar::new(),
            intake_room: Arc::default(),
            refusals: std::array::from_fn(|_| AtomicU64::new(0)),
            span_limit,
            config,
            instance,
            state: Arc::new(Mutex::new(State {
                lossy: lossy::Progress::default(),
                released_workers: 0,
                held: None,
                force_local: 0,
                spool: stats,
                reserved_disk: 0,
                queue,
                pending: None,
                bytes: 0,
                count: 0,
                sequence,
                admitted_sequence: sequence,
                admitted: sequence,
                durable_sequence: confirmed,
                durable: confirmed,
                batch_number: 0,
                retries: 0,
                floor_refusals: 0,
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
                reported_drops: 0,
                reported_quarantined: 0,
                held_unsaved_events: 0,
            })),
            changed: Arc::default(),
            async_changed: Arc::default(),
            wake: tokio::sync::Notify::new(),
            retry_wake: tokio::sync::Notify::new(),
            operator_retry: tokio::sync::Notify::new(),
            barrier_active: AtomicBool::new(false),
            abort_wake: tokio::sync::Notify::new(),
            owners: AtomicUsize::new(1),
            offered: AtomicU64::new(0),
            spans: AtomicU64::new(span),
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
        self.notify_barrier();
        self.intake_room.notify_waiters();
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
            self.notify_barrier();
        }
        ok
    }
    fn offer_ticket(&self) -> Option<u64> {
        let sequence = self
            .offered
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1));
        match sequence {
            Ok(previous) => Some(previous + 1),
            Err(_) => {
                self.exhausted.store(true, Ordering::Release);
                self.wake.notify_one();
                self.notify_barrier();
                None
            }
        }
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
        let dropped_by_reason = std::array::from_fn(|i| self.drops[i].load(Ordering::Relaxed));
        let offered_events = self.offered.load(Ordering::Relaxed);
        DeliveryStats {
            client_instance: self.instance,
            dropped_sequences: if self.config.disk_free() {
                dropped_by_reason
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| *index != DropReason::CounterExhausted as usize)
                    .map(|(_, n)| *n)
                    .sum()
            } else {
                0
            },
            last_refusal: state.lossy.refusal.clone(),
            store_mismatch: state.lossy.mismatch.clone(),
            spool: state.spool.clone(),
            refused_by_reason: std::array::from_fn(|i| self.refusals[i].load(Ordering::Relaxed)),
            held: state.held.clone(),
            dropped_by_reason,
            offered_events,
            admitted_events: state.admitted,
            durable_events: state.durable,
            pending_events: if state.spool.is_some() {
                usize::try_from(
                    state
                        .admitted_sequence
                        .saturating_sub(state.durable_sequence),
                )
                .unwrap_or(usize::MAX)
            } else {
                state.count
            },
            pending_bytes: state.bytes,
            oldest_pending_age,
            flush_lag: oldest_pending_age.map_or(Duration::ZERO, |age| {
                age.saturating_sub(self.config.flush_interval)
            }),
            in_flight_events: state
                .pending
                .as_ref()
                .map_or(0, |pending| pending.batch.events.len()),
            last_sequence: if self.config.disk_free() {
                offered_events
            } else {
                state.sequence
            },
            last_durable_sequence: state.durable_sequence,
            retries: state.retries,
            floor_refusals: state.floor_refusals,
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
            held_unsaved_events: state.held_unsaved_events,
        }
    }
    fn fail(&self, error: Error) {
        let guard = self.state.lock();
        if self.config.disk_free() {
            // A panic can interrupt batch construction with events detached from
            // the FIFO. Only certified receipts and already counted losses settle
            // those admissions. Close the queue before exposing it to readers.
            let mut state = guard.unwrap_or_else(std::sync::PoisonError::into_inner);
            let lost = state
                .admitted
                .saturating_sub(state.durable)
                .saturating_sub(state.lossy.worker_losses);
            state.failure = Some(Arc::new(error));
            state.closing = true;
            state.count = 0;
            state.bytes = 0;
            state.lossy.worker_losses += lost;
            self.drops[DropReason::Closed as usize].fetch_add(lost, Ordering::Relaxed);
            let queue = std::mem::take(&mut state.queue);
            let pending = state.pending.take();
            self.state.clear_poison();
            drop(state);
            drop((queue, pending));
        } else if let Ok(mut state) = guard {
            state.failure = Some(Arc::new(error));
        }
        self.notify_barrier();
        self.intake_work.notify_all();
        self.intake_room.notify_waiters();
    }
    fn next(&self, advertised: Option<crate::events::BatchLimits>) -> Result<Action, Error> {
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
        let limits = advertised
            .unwrap_or(crate::events::BatchLimits::NATIVE)
            .bounded(self.config.batch_events, self.config.batch_bytes);
        if !state.closing
            && state.force_through < first.event.sequence
            && state.queue.len() < limits.events
            && state.bytes < limits.payload_bytes
            && !remaining.is_zero()
        {
            return Ok(Action::Wait(Some(remaining)));
        }
        state.batch_number = state
            .batch_number
            .checked_add(1)
            .ok_or(Error::Protocol("producer batch identity exhausted"))?;
        let number = state.batch_number;
        // The wire identity ends in a zero-padded twenty-digit u64.
        let overhead = 1024 + 6 * (self.request_prefix.len() + 20 + self.config.source.len());
        let mut bytes = 0;
        // Before discovery only one event is certain to fit the count ceiling. A singleton
        // that exceeds a newly advertised byte ceiling is kept whole for input quarantine.
        let mut count_limit = if advertised.is_some() {
            limits.events
        } else {
            1
        };
        if self.config.disk_free() && self.barrier_active.load(Ordering::Relaxed) {
            count_limit = count_limit.min(state.cut_room());
        }
        let mut events = Vec::with_capacity(count_limit.min(state.queue.len()));
        while let Some(next) = state.queue.front() {
            if events.len() == count_limit
                || (!events.is_empty() && bytes + next.bytes + overhead > limits.payload_bytes)
            {
                break;
            }
            let next = state
                .queue
                .pop_front()
                .ok_or(Error::Protocol("producer queue front disappeared"))?;
            bytes += next.bytes;
            events.push(next);
        }
        if events.is_empty() {
            return Err(Error::Protocol(
                "admitted event cannot fit its configured delivery batch",
            ));
        }
        let sent = state.lossy.sent;
        drop(state);
        let mut late_bytes = 0;
        let mut late = 0;
        if self.config.disk_free() {
            events.sort_unstable_by_key(|entry| entry.event.sequence);
            events.retain(|entry| {
                if entry.event.sequence <= sent {
                    late += 1;
                    late_bytes += entry.bytes;
                    false
                } else {
                    true
                }
            });
        }
        let batch = Arc::new(EventBatch {
            request_id: format!("{}{:020}", self.request_prefix, number),
            source: self.config.source.clone(),
            client_instance: self.instance,
            events: events.into_iter().map(|entry| entry.event).collect(),
        });
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Protocol("producer queue state poisoned"))?;
        if late > 0 {
            state.release_credit(late_bytes, late, true)?;
            self.drops[DropReason::Late as usize].fetch_add(late as u64, Ordering::Relaxed);
            self.notify_barrier();
        }
        if batch.events.is_empty() {
            return Ok(Action::Continue);
        }
        state.pending = Some(Pending {
            batch: batch.clone(),
            bytes: bytes - late_bytes,
            at,
        });
        drop(state);
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
                class: crate::FailureClass::Unclassified,
            });
        }
        state.store = Some(store.to_owned());
        Ok(())
    }
    /// Adopt whichever store answers next. Called once the batch that met a replaced store is in
    /// a quarantine file: the producer's destination is its socket, so later batches go to the
    /// store now behind it.
    fn forget_store(&self) -> Result<(), Error> {
        self.state
            .lock()
            .map_err(|_| Error::Protocol("producer queue state poisoned"))?
            .store = None;
        Ok(())
    }
    fn accepted(&self, batch: &Arc<EventBatch>, receipt: ArtifactReceipt) -> Result<(), Error> {
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
        if !Arc::ptr_eq(&pending.batch, batch) {
            return Err(Error::Protocol(
                "producer acceptance changed pending identity",
            ));
        }
        let bytes = pending.bytes;
        let count = batch.events.len();
        let durable = state
            .durable
            .checked_add(count as u64)
            .ok_or(Error::Protocol("producer delivery count exhausted"))?;
        let durable_sequence = batch
            .events
            .last()
            .ok_or(Error::Protocol("producer accepted an empty batch"))?
            .sequence;
        state.release_credit(bytes, count, false)?;
        state.lossy.sent = durable_sequence;
        state.durable = durable;
        state.durable_sequence = durable_sequence;
        state.receipt = Some(receipt);
        state.transient = None;
        state.pending = None;
        drop(state);
        self.notify_barrier();
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
                self.notify_barrier();
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
    /// Keep what the worker still holds, the pending batch and the queued events, in
    /// quarantine files: the process may exit and the daemon never answers for them. `Ok`
    /// means every held byte is in a file; `Err` names the batches that are not (they stay
    /// held in memory and each failed write is counted). Saving again is safe: a batch
    /// already in a file is reused, not written twice.
    fn save_held(&self, error: &Error) -> Result<(), Error> {
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
        let mut unsaved_events = 0_u64;
        for batch in &batches {
            if let Err(write) = self.save(batch, error) {
                unsaved.push(format!("{} ({write})", batch.request_id));
                unsaved_events = unsaved_events.saturating_add(batch.events.len() as u64);
            }
        }
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .held_unsaved_events = unsaved_events;
        if unsaved.is_empty() {
            Ok(())
        } else {
            Err(Error::Io(std::io::Error::other(format!(
                "{error}; held events not written to quarantine and kept only in memory: {}",
                unsaved.join(", ")
            ))))
        }
    }
    /// End delivery for an invariant failure no later attempt can change (a poisoned lock, an
    /// exhausted counter, a panicked worker). The held events are written to quarantine files
    /// first; while a write fails the failure is published with the unsaved batches named,
    /// `held_unsaved_events` counts them and the write is retried on the backoff until it
    /// succeeds or the last producer handle is dropped. Nothing held is ever counted delivered.
    fn settle_failure(&self, runtime: &tokio::runtime::Runtime, error: Error) {
        let mut backoff = Backoff::new(&self.config);
        loop {
            match self.save_held(&error) {
                Ok(()) => {
                    self.fail(error);
                    return;
                }
                Err(unsaved) => {
                    self.fail(unsaved);
                    let aborted = runtime.block_on(wait_out_retry_delay(
                        &self.abort_wake,
                        &self.retry_wake,
                        &self.barrier_active,
                        backoff.take(),
                    ));
                    if aborted || self.aborted.load(Ordering::SeqCst) {
                        return;
                    }
                }
            }
        }
    }
    /// Give up the immutable pending batch after its bytes reached a quarantine file: its
    /// events stay counted and visible, their quota is released, and later events proceed
    /// instead of queueing behind a batch that can never be accepted.
    fn quarantine(&self, batch: &Arc<EventBatch>, error: Error) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Protocol("producer queue state poisoned"))?;
        let pending = state
            .pending
            .as_ref()
            .ok_or(Error::Protocol("producer quarantine has no pending batch"))?;
        if !Arc::ptr_eq(&pending.batch, batch) {
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
        self.notify_barrier();
        Ok(())
    }
    fn retry(&self, error: Error, manual: bool) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Protocol("producer queue state poisoned"))?;
        state.retries = state
            .retries
            .checked_add(1)
            .ok_or(Error::Protocol("producer retry count exhausted"))?;
        if error.is_free_space_floor() {
            state.floor_refusals = state.floor_refusals.saturating_add(1);
        }
        let error = Arc::new(error);
        state.held = manual.then(|| error.clone());
        state.transient = Some(error);
        drop(state);
        self.notify_barrier();
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
    waiting: &AtomicBool,
    delay: Duration,
) -> bool {
    let sleep = tokio::time::sleep(delay);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            _ = abort.notified() => return true,
            _ = &mut sleep => return false,
            _ = barrier.notified() => {
                if waiting.load(Ordering::SeqCst) {
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
    Continue,
    Wait(Option<Duration>),
    Send(Arc<EventBatch>),
    Stop,
}
async fn run(shared: &Shared) -> Result<(), Error> {
    let mut connection: Option<Client> = None;
    let mut may_have_sent = false;
    let mut backoff = Backoff::new(&shared.config);
    loop {
        let advertised = connection
            .as_ref()
            .map(|client| client.discovery().event_limits());
        match shared.next(advertised)? {
            Action::Continue => continue,
            Action::Stop => return Ok(()),
            Action::Wait(duration) => match duration {
                Some(duration) => {
                    tokio::select! { _ = shared.wake.notified() => {}, _ = tokio::time::sleep(duration) => {} }
                }
                None => shared.wake.notified().await,
            },
            Action::Send(batch) => {
                let mut replacement = None;
                let attempt = async {
                    if connection.is_none() {
                        let client = Client::open(
                            &shared.config.endpoint,
                            shared.config.expect.as_ref(),
                            crate::receipt::Contract::Producer,
                        )
                        .await?;
                        let bound = shared.bind_store(&client.discovery().store_id);
                        if matches!(&bound, Err(Error::Domain { code, .. }) if code == STORE_MISMATCH)
                        {
                            replacement = Some(client.discovery().store_id.clone());
                        }
                        connection = Some(client);
                        bound?;
                    }
                    let client = connection.as_ref().ok_or(Error::Protocol(
                        "producer connection missing after discovery",
                    ))?;
                    // Once the RPC can have left the process, even a timeout must keep the
                    // immutable identity. Client-side size validation runs before dispatch.
                    let previous_attempt = may_have_sent;
                    may_have_sent = true;
                    let receipt = match client.events(&batch).await {
                        Err(error @ Error::InvalidArgument(_)) => {
                            may_have_sent = previous_attempt;
                            return Err(if previous_attempt {
                                Error::Unsupported(IMMUTABLE_BATCH_LIMIT)
                            } else {
                                error
                            });
                        }
                        result => result?,
                    };
                    // A store replaced behind a live connection answers with its own receipt:
                    // refuse it here, as a hold, rather than as a worker failure.
                    let bound = shared.bind_store(&receipt.store_id);
                    if matches!(&bound, Err(Error::Domain { code, .. }) if code == STORE_MISMATCH) {
                        replacement = Some(receipt.store_id.clone());
                    }
                    bound?;
                    Ok(receipt)
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
                        may_have_sent = false;
                        backoff.reset();
                    }
                    Err(error) => {
                        // A refusal during connection, discovery or CHAP cannot be about the
                        // pending event batch. Store binding, once connected, is a separate
                        // identity check with its own replacement policy.
                        let mismatch =
                            matches!(&error, Error::Domain { code, .. } if code == STORE_MISMATCH);
                        let action = if connection.is_some()
                            && !(shared.config.disk_free() && mismatch && replacement.is_none())
                        {
                            disposition(&error)
                        } else {
                            Disposition::Hold
                        };
                        // A daemon answer, even a refusal, proves the connection works; only
                        // a transport, I/O or protocol failure, or a store that is not the
                        // bound one, means the connection may be wrong. A needless reconnect on
                        // every busy retry also feeds the daemon's connection ceiling.
                        if !matches!(&error, Error::Domain { code, .. } if code != STORE_MISMATCH) {
                            connection = None;
                        }
                        // A refusal about this batch must reach a quarantine file before the
                        // batch is given up. If it cannot, the batch stays pending, the write
                        // failure is counted and reported as the transient failure whatever
                        // its kind, and the next attempt tries again: that batch is not
                        // dropped. Every other failure holds the batch and retries it.
                        let error = match action {
                            Disposition::SetAside if shared.config.disk_free() => {
                                shared.discard_batch(&batch, error, replacement, may_have_sent)?;
                                may_have_sent = false;
                                backoff.reset();
                                continue;
                            }
                            Disposition::SetAside => match shared.save(&batch, &error) {
                                Ok(_) => {
                                    shared.quarantine(&batch, error)?;
                                    may_have_sent = false;
                                    if mismatch {
                                        shared.forget_store()?;
                                    }
                                    backoff.reset();
                                    continue;
                                }
                                Err(write) => write,
                            },
                            Disposition::Hold => error,
                        };
                        let delay = if matches!(error, Error::Unsupported(crate::PRODUCER_PROFILE))
                        {
                            Duration::from_secs(30)
                        } else {
                            backoff.take()
                        };
                        shared.retry(error, false)?;
                        if wait_out_retry_delay(
                            &shared.abort_wake,
                            &shared.retry_wake,
                            &shared.barrier_active,
                            delay,
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

const STORE_MISMATCH: &str = "STORE_MISMATCH";
pub(crate) const IMMUTABLE_BATCH_LIMIT: &str =
    "current producer ceilings cannot admit the immutable batch with an unknown delivery outcome";

/// What the worker does with one failed attempt. It never ends delivery: a failure is either
/// about this batch, which is set aside durably so later batches proceed, or about the
/// connection, the daemon or the store behind the socket, which holds the batch unchanged and
/// retries it on the backoff until the cause goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Disposition {
    /// Write the batch and the refusal to a quarantine file, release it, carry on.
    SetAside,
    /// Keep the batch pending and retry it. Visible as `retries` and `last_transient_failure`.
    Hold,
}

/// Classify a failed attempt.
///
/// Set aside: a non-retryable daemon refusal about the request itself (`INVALID_ARGUMENT`,
/// `CONFLICT`, a `RESOURCE_LIMIT` the daemon marks input-bound, #397) and the client's own
/// `Error::InvalidArgument`. `STORE_MISMATCH` is set aside too: the store behind the socket was
/// replaced, so whether the old store holds the pending batch is unknown and it must reach no
/// other store unrecorded. The batch is written to a quarantine file with the refusal, the worker
/// adopts the store now behind the socket (`forget_store`) and later batches go there, so a
/// replaced store neither stops delivery nor holds events in memory only. A non-retryable
/// refusal with a code this client does not know is
/// set aside as well: the daemon says a retry cannot change it, the bytes are preserved in a
/// replayable file, and delivery of later batches continues. The daemon's own flag decides
/// retryability for every code, so a new code is never terminal here.
///
/// Hold: everything that is not about the batch. A retryable daemon refusal (busy
/// `RESOURCE_LIMIT`, `STORAGE_PRESSURE`, `SHUTTING_DOWN`), the free-space floor, a client-built
/// refusal (`FailureClass::Local`), and the codes that name the daemon or store state:
/// `READ_ONLY`, `PERMISSION_DENIED`, `UNAUTHENTICATED` (a daemon that does not admit this
/// producer until it is configured to), `UNSUPPORTED` (a daemon image that cannot take this
/// client), `STORAGE_FAILURE` and the daemon's transport, protocol and file failures. Every transport, I/O, protocol and
/// discovery error of the client is a hold as well: none says anything about the batch.
pub(crate) fn disposition(error: &Error) -> Disposition {
    match error {
        Error::InvalidArgument(_) => Disposition::SetAside,
        Error::Domain {
            class: crate::FailureClass::FreeSpaceFloor | crate::FailureClass::Local,
            ..
        }
        | Error::Domain {
            retryable: true, ..
        } => Disposition::Hold,
        Error::Domain { code, .. } => match code.as_str() {
            STORE_MISMATCH => Disposition::SetAside,
            "INSTANCE_MISMATCH" | "READ_ONLY" | "PERMISSION_DENIED" | "UNAUTHENTICATED"
            | "UNSUPPORTED" | "STORAGE_FAILURE" | "STORAGE_PRESSURE" | "SHUTTING_DOWN"
            | "CANCELLED" | "RPC_FAILURE" | "RPC_TIMEOUT" | "PROTOCOL_FAILURE"
            | "FILE_IO_FAILURE" => Disposition::Hold,
            _ => Disposition::SetAside,
        },
        _ => Disposition::Hold,
    }
}

/// Whether reconnecting may succeed within a caller's bounded wait, including a daemon whose
/// socket is briefly absent or refusing while it restarts. This is the import reconnect's
/// narrow test; the delivery worker decides with [`disposition`] and never gives up.
pub(crate) fn reconnectable(error: &Error) -> bool {
    // The floor label always means hold: the batch is never quarantined or dropped for it.
    if error.is_free_space_floor() {
        return true;
    }
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

    /// A caller with a missing daemon stamps each attempt before contention, validation
    /// or quota refusal. A worker can settle a refused prefix with no durable receipt.
    #[test]
    fn disk_free_offers_count_every_attempt_without_waiting_for_a_held_lock() {
        let mut config = DeliveryConfig::lossy(crate::Endpoint::Unix("absent.sock".into()), "fast");
        config.max_queued_events = 1;
        config.batch_events = 1;
        let delivery = Delivery {
            shared: Shared::new(config, [3; 16]),
        };
        let held = delivery.shared.state.lock().unwrap();
        assert_eq!(
            delivery.try_send(event()),
            Admission::Dropped {
                reason: DropReason::Contended,
                sequence: Some(1)
            }
        );
        drop(held);
        delivery.capture_failed();
        assert_eq!(
            delivery.try_send(event()),
            Admission::Queued { sequence: 3 }
        );
        for sequence in 4..=10_000 {
            assert_eq!(
                delivery.try_send(event()),
                Admission::Dropped {
                    reason: DropReason::Full,
                    sequence: Some(sequence)
                }
            );
        }
        let Action::Send(batch) = delivery.shared.next(None).unwrap() else {
            panic!("expected a pending request")
        };
        delivery
            .shared
            .discard_batch(&batch, Error::InvalidArgument("input-bound"), None, true)
            .unwrap();
        let Err(FlushError::Dropped(first)) = delivery.flush(Duration::ZERO) else {
            panic!("losses missing from fence")
        };
        assert_eq!(
            (first.durable_events, first.dropped_before_fence),
            (0, 10_000)
        );
        assert_eq!(delivery.stats().unwrap().dropped_sequences, 10_000);
        assert_eq!(
            delivery.flush(Duration::ZERO).unwrap().dropped_before_fence,
            0
        );
    }

    /// A delayed lower ticket never reaches the wire after a higher one. A later
    /// worker loss stays outside an earlier fence even when its caller resumes last.
    #[test]
    fn late_disk_free_tickets_and_cancelled_fences_preserve_fifo_loss_boundaries() {
        let mut config = DeliveryConfig::lossy(crate::Endpoint::Unix("absent.sock".into()), "late");
        config.batch_events = 2;
        let delivery = Delivery {
            shared: Shared::new(config, [4; 16]),
        };
        let shared = &delivery.shared;
        let delayed = shared.offer_ticket().unwrap();
        assert_eq!(
            delivery.try_send(event()),
            Admission::Queued { sequence: 2 }
        );
        let first = shared.fence(false).unwrap();
        let Action::Send(batch) = shared.next(None).unwrap() else {
            panic!("higher ticket was not dispatched")
        };
        assert_eq!(
            delivery.enqueue_memory(event(), delayed),
            Admission::Queued { sequence: 1 }
        );
        shared
            .discard_batch(&batch, Error::InvalidArgument("input-bound"), None, true)
            .unwrap();
        assert!(matches!(shared.next(None).unwrap(), Action::Continue));
        let mut state = shared.state.lock().unwrap();
        let Some(Err(FlushError::Dropped(watermark))) = first.result(&mut state, false) else {
            panic!("earlier fence unsettled")
        };
        assert_eq!(
            (watermark.required_sequence, watermark.dropped_before_fence),
            (2, 1)
        );
        drop(state);
        drop(first);
        drop(shared.fence(false).unwrap());
        let Err(FlushError::Dropped(later)) = delivery.flush(Duration::ZERO) else {
            panic!("late loss escaped cancelled fence")
        };
        assert_eq!(later.dropped_before_fence, 1);
        let stats = delivery.stats().unwrap();
        assert_eq!(
            (
                stats.dropped_by_reason[DropReason::Late as usize],
                stats.dropped_sequences
            ),
            (1, 2)
        );
        let earlier = shared.offer_ticket().unwrap();
        assert_eq!(
            delivery.try_send(event()),
            Admission::Queued { sequence: 4 }
        );
        assert_eq!(
            delivery.enqueue_memory(event(), earlier),
            Admission::Queued { sequence: 3 }
        );
        let Action::Send(ordered) = shared
            .next(Some(crate::events::BatchLimits::NATIVE))
            .unwrap()
        else {
            panic!("a complete admitted batch did not dispatch")
        };
        assert_eq!(
            ordered
                .events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            [3, 4]
        );
    }

    /// An asynchronous best-effort offer waits through temporary RAM pressure and
    /// synchronizes its own event even when the daemon is absent and max_wait is zero.
    #[tokio::test]
    async fn lossy_spool_waits_for_a_busy_intake_slot() {
        use futures::FutureExt;
        let dir = tempfile::tempdir().unwrap();
        let mut spool = crate::SpoolConfig::new(dir.path().join("spool"));
        spool.max_wait = Duration::ZERO;
        let mut config = DeliveryConfig::lossy_spooled(
            crate::Endpoint::Unix(dir.path().join("missing.sock")),
            "intake",
            spool,
        );
        config.max_queued_events = 1;
        let delivery = Delivery::start_async(config).await.unwrap();
        let offer = delivery.offer_spooled(event());
        tokio::pin!(offer);
        {
            let _disk = delivery.shared.spool.as_ref().unwrap().lock().unwrap();
            assert_eq!(
                delivery.try_send(event()),
                Admission::Queued { sequence: 1 }
            );
            assert!(offer.as_mut().now_or_never().is_none());
            assert_eq!(delivery.stats().unwrap().admitted_events, 1);
        }
        assert_eq!(offer.await.unwrap().unwrap().sequence, 2);
        let stats = delivery.stats().unwrap();
        assert_eq!(stats.spool.unwrap().synced_sequence, 2);
        assert_eq!(stats.refused_by_reason, [0; 5]);
        delivery
            .clone()
            .close_local(Duration::from_secs(5))
            .await
            .unwrap();
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
        let waiting = AtomicBool::new(false);
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

        waiting.store(true, Ordering::SeqCst);
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

    /// INVARIANT (#216): before a terminal failure ends delivery, the queued events are
    /// written to a quarantine file; if the directory cannot be written the result names the
    /// held events as kept only in memory, counts the failure and the unsaved events. Before,
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
            let saved = delivery
                .shared
                .save_held(&Error::Protocol("terminal for the test"));
            (delivery, saved)
        };
        let good = dir.path().join("good");
        let (delivery, saved) = held(&good);
        assert!(saved.is_ok());
        let stats = delivery.stats().unwrap();
        assert_eq!(
            (stats.quarantine_files, stats.quarantine_write_failures),
            (1, 0)
        );
        let text = std::fs::read_to_string(stats.last_quarantine_file.unwrap()).unwrap();
        assert!(text.contains("\"events\":1") && text.contains("terminal for the test"));
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"x").unwrap();
        let (delivery, saved) = held(&blocker.join("q"));
        let Err(Error::Io(error)) = saved else {
            panic!("an unsaved queue must be reported: {saved:?}");
        };
        assert!(error.to_string().contains("kept only in memory"), "{error}");
        let stats = delivery.stats().unwrap();
        assert_eq!(
            (stats.quarantine_write_failures, stats.held_unsaved_events),
            (1, 1)
        );
    }

    /// INVARIANT (#448): held events whose quarantine write failed are not left in memory:
    /// the worker keeps retrying the write on its backoff, `held_unsaved_events` counts them
    /// until they are in a file, and they are never counted delivered. Before, one failed
    /// write was final and the bytes died with the process.
    #[test]
    fn held_events_are_written_once_the_quarantine_directory_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"x").unwrap();
        let mut config = DeliveryConfig::new("unused.sock", "test").with_quarantine_dir(&blocker);
        config = config.with_test_timing(
            Duration::from_millis(10),
            Duration::from_millis(10),
            Duration::from_millis(10),
        );
        let shared = Shared::new(config, [9; 16]);
        {
            let mut state = shared.state.lock().unwrap();
            state.queue.push_back(Queued {
                disk: 0,
                token: Vec::new(),
                ack: None,
                event: ProducerEvent {
                    sequence: 1,
                    ..event()
                },
                bytes: 1,
                at: Instant::now(),
            });
            state.count = 1;
        }
        let worker = shared.clone();
        let handle = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            worker.settle_failure(&runtime, Error::Protocol("terminal for the test"));
        });
        // Wait for the first failed write, then repair the directory.
        let mut state = shared.state.lock().unwrap();
        while state.held_unsaved_events == 0 {
            state = shared
                .changed
                .wait_timeout(state, Duration::from_millis(5))
                .unwrap()
                .0;
        }
        assert!(state.quarantine_failures >= 1);
        assert_eq!(state.held_unsaved_events, 1);
        drop(state);
        std::fs::remove_file(&blocker).unwrap();
        handle.join().unwrap();
        let state = shared.state.lock().unwrap();
        assert_eq!(
            (
                state.held_unsaved_events,
                state.quarantine_files,
                state.durable
            ),
            (0, 1, 0)
        );
        let failure = state.failure.as_ref().unwrap().to_string();
        assert!(
            failure.contains("terminal for the test") && !failure.contains("only in memory"),
            "{failure}"
        );
    }

    /// INVARIANT (#448): a barrier reports the drops and quarantines since the previous
    /// barrier, so a fence that reported them is clean for later input, while the statistics
    /// keep the totals. Before, every later barrier of the instance returned `Dropped`.
    #[test]
    fn a_barrier_reports_drops_once_and_the_next_fence_is_clean() {
        let dir = tempfile::tempdir().unwrap();
        let delivery = Delivery::start(
            DeliveryConfig::new(dir.path().join("missing.sock"), "test")
                .with_quarantine_dir(dir.path().join("quarantine")),
        )
        .unwrap();
        delivery.capture_failed();
        let Err(FlushError::Dropped(first)) = delivery.flush(Duration::from_secs(2)) else {
            panic!("the first barrier must report the drop");
        };
        assert_eq!(first.dropped_before_fence, 1);
        let clean = delivery.flush(Duration::from_secs(2)).unwrap();
        assert_eq!(clean.dropped_before_fence, 0);
        assert_eq!(
            delivery.stats().unwrap().dropped_by_reason[DropReason::Invalid as usize],
            1
        );
        delivery.capture_failed();
        assert!(matches!(
            delivery.flush(Duration::from_secs(2)),
            Err(FlushError::Dropped(_))
        ));
    }

    /// INVARIANT (#448): the worker never ends on a refusal. A refusal about the batch,
    /// including a code the client does not know, is set aside; a refusal about the
    /// connection, the daemon or the store, and every transport, I/O, protocol and discovery
    /// failure, holds the batch. Before, only three codes were set aside and everything not
    /// on a short allow-list ended delivery for good.
    #[test]
    fn every_failure_is_set_aside_or_held_and_none_is_terminal() {
        let domain = |code: &str, retryable: bool, class: crate::FailureClass| Error::Domain {
            code: code.into(),
            message: "m".into(),
            retryable,
            class,
        };
        let plain = crate::FailureClass::Unclassified;
        for (error, expected) in [
            (
                domain("INVALID_ARGUMENT", false, plain),
                Disposition::SetAside,
            ),
            (domain("CONFLICT", false, plain), Disposition::SetAside),
            (
                domain("RESOURCE_LIMIT", false, plain),
                Disposition::SetAside,
            ),
            (
                domain("A_CODE_FROM_THE_FUTURE", false, plain),
                Disposition::SetAside,
            ),
            (domain("RESOURCE_LIMIT", true, plain), Disposition::Hold),
            (
                domain("A_CODE_FROM_THE_FUTURE", true, plain),
                Disposition::Hold,
            ),
            (
                domain(
                    "STORAGE_PRESSURE",
                    false,
                    crate::FailureClass::FreeSpaceFloor,
                ),
                Disposition::Hold,
            ),
            (
                domain("INVALID_ARGUMENT", false, crate::FailureClass::Local),
                Disposition::Hold,
            ),
            (
                domain("STORE_MISMATCH", false, plain),
                Disposition::SetAside,
            ),
            (domain("READ_ONLY", false, plain), Disposition::Hold),
            (domain("PERMISSION_DENIED", false, plain), Disposition::Hold),
            (domain("UNSUPPORTED", false, plain), Disposition::Hold),
            (domain("STORAGE_FAILURE", false, plain), Disposition::Hold),
            (
                Error::InvalidArgument("event text exceeds 16 KiB"),
                Disposition::SetAside,
            ),
            (Error::Unsupported("discovery"), Disposition::Hold),
            (Error::Protocol("receipt"), Disposition::Hold),
            (
                Error::Transport(capnp::Error::failed("x".into())),
                Disposition::Hold,
            ),
            (
                Error::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
                Disposition::Hold,
            ),
        ] {
            assert_eq!(disposition(&error), expected, "{error}");
        }
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

    /// A poisoned queue still exposes terminal settlement: quarantine mode reports its
    /// saved files and disk-free mode publishes the cause and counts remaining admissions.
    #[test]
    fn a_poisoned_queue_reports_saved_or_discarded_events_under_its_delivery_policy() {
        let dir = tempfile::tempdir().unwrap();
        let shared = shared(&dir.path().join("quarantine"));
        let poison = |poisoner: Arc<Shared>| {
            std::thread::spawn(move || {
                let _held = poisoner.state.lock().unwrap();
                panic!("poison the state lock on purpose");
            })
            .join()
            .unwrap_err();
        };
        poison(shared.clone());
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
                disk: 0,
                token: Vec::new(),
                ack: None,
                event: ProducerEvent {
                    sequence: 2,
                    ..event()
                },
                bytes: 0,
                at: Instant::now(),
            });
        shared
            .save_held(&Error::Protocol("terminal for the test"))
            .expect("a saved queue is not reported unsaved");
        let state = shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!((state.quarantine_files, state.quarantine_failures), (2, 0));
        let config = DeliveryConfig::lossy(crate::Endpoint::Unix("absent.sock".into()), "terminal");
        let disk_free = Delivery {
            shared: Shared::new(config, [5; 16]),
        };
        assert_eq!(
            disk_free.try_send(event()),
            Admission::Queued { sequence: 1 }
        );
        poison(disk_free.shared.clone());
        disk_free.shared.fail(Error::Protocol("sender panic"));
        assert_eq!(
            disk_free.try_send(event()),
            Admission::Dropped {
                reason: DropReason::Closed,
                sequence: Some(2)
            }
        );
        let stats = disk_free.stats().unwrap();
        assert!(matches!(
            stats.terminal_failure.as_deref(),
            Some(Error::Protocol("sender panic"))
        ));
        assert_eq!(
            (
                stats.pending_events,
                stats.dropped_sequences,
                stats.quarantine_files
            ),
            (0, 2, 0)
        );
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
            class: crate::FailureClass::Unclassified,
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

    /// INVARIANT (#397): a `RESOURCE_LIMIT` the daemon marks non-retryable is a bound on this
    /// batch and quarantines it; one it marks retryable is occupancy and is held, never
    /// quarantined.
    #[test]
    fn a_resource_limit_quarantines_only_when_the_daemon_marks_it_non_retryable() {
        for (retryable, quarantined) in [(false, true), (true, false)] {
            let refusal = Error::Domain {
                code: "RESOURCE_LIMIT".into(),
                message: "bound".into(),
                retryable,
                class: crate::FailureClass::Unclassified,
            };
            assert_eq!(
                disposition(&refusal) == Disposition::SetAside,
                quarantined,
                "{retryable}"
            );
        }
    }

    /// INVARIANT: the client's own refusal is classed permanent and `save` writes it under the
    /// client's own code with the encoder's reason, so those bytes are kept too. This covers
    /// the classification and the file only: the worker validates each batch before the send,
    /// so no deterministic run drives a batch the queue built into that error (it ends
    /// delivery as a local invariant failure, saved by `save_held`, which the held-events test covers).
    #[test]
    fn a_batch_the_client_cannot_encode_is_quarantined_under_a_client_code() {
        let refusal = Error::InvalidArgument("event text exceeds 16 KiB");
        assert_eq!(disposition(&refusal), Disposition::SetAside);
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
