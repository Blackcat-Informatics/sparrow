// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Explicit bounded syslog intake, independent of the private typed query transport.

use crate::pressure_wake::{PressureWake, Waiter};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, BufReader},
    net::{TcpListener, UdpSocket},
    sync::{Semaphore, TryAcquireError, watch},
    task::JoinSet,
};

/// Explicit TCP packet boundary law; the listener never guesses between frames.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SyslogFraming {
    /// RFC 6587 decimal octet count followed by one space and exact payload bytes.
    OctetCounting,
    /// One LF trailer; any preceding CR remains a packet byte.
    Lf,
    /// An exact CRLF trailer.
    CrLf,
}

/// Intake transport; UDP has no delivery acknowledgement.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SyslogTransport {
    /// Configured RFC 6587 framing and bounded simultaneous connections.
    Tcp,
    /// One message per datagram, kept whole; refusals are counted and there is no delivery
    /// acknowledgement.
    Udp,
}

/// One explicit local ingestion authority, never a query or model-backend transport.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyslogListener {
    /// Bounded configuration identity.
    pub id: String,
    /// Explicit loopback binding; port zero is useful for isolated qualification.
    pub address: SocketAddr,
    /// Exact intake source label.
    pub source: String,
    /// Explicit TCP or UDP admission.
    pub transport: SyslogTransport,
    /// Required for TCP and absent for UDP.
    pub framing: Option<SyslogFraming>,
    /// Required, 480..65536. TCP line framing keeps this many bytes of a line and counts the
    /// cut; UDP ignores it, since a datagram always fits the store's ceiling.
    pub max_message_bytes: usize,
    /// Required per-listener TCP connection ceiling, 1..16; UDP requires one.
    pub max_connections: usize,
    /// Required complete-frame read deadline, 1..60000 milliseconds.
    pub read_timeout_ms: u32,
}

pub(crate) fn validate(listeners: &[SyslogListener]) -> io::Result<()> {
    let mut ids = std::collections::BTreeSet::new();
    let mut addresses = std::collections::BTreeSet::new();
    if listeners.len() > 4 {
        return Err(io::Error::other("syslog admits at most four listeners"));
    }
    for listener in listeners {
        if listener.id.is_empty()
            || listener.id.len() > 128
            || !ids.insert(&listener.id)
            || listener.source.is_empty()
            || listener.source.len() > 512
            || !listener.address.ip().is_loopback()
            || (listener.address.port() != 0
                && !addresses
                    .insert((listener.address, listener.transport == SyslogTransport::Tcp)))
            || !(480..=sparrow_core::SYSLOG_MESSAGE_BYTES).contains(&listener.max_message_bytes)
            || !(1..=16).contains(&listener.max_connections)
            || !(1..=60000).contains(&listener.read_timeout_ms)
            || (listener.transport == SyslogTransport::Tcp) != listener.framing.is_some()
            || (listener.transport == SyslogTransport::Udp && listener.max_connections != 1)
        {
            return Err(io::Error::other(
                "syslog requires unique identities, explicit loopback bindings, source 1..512 bytes, max_message_bytes 480..65536, max_connections 1..16, read_timeout_ms 1..60000, TCP framing, and UDP max_connections=1 with no framing",
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Default)]
struct Counts {
    accepted: u64,
    interpretation_failures: u64,
    refused: u64,
    truncated: u64,
    framing_failures: u64,
    timed_out: u64,
    /// Frames held at least once for the store's `STORAGE_PRESSURE` (#243).
    holds_storage_pressure: u64,
    /// Frames held at least once for an operation permit (#243).
    holds_no_permit: u64,
    /// Longest finished hold, in milliseconds.
    longest_hold_ms: u64,
}

/// Why a frame already read is kept instead of published.
#[derive(Clone, Copy, PartialEq, Eq)]
enum HoldReason {
    StoragePressure,
    NoPermit,
}
impl HoldReason {
    const fn name(self) -> &'static str {
        match self {
            Self::StoragePressure => "storage_pressure",
            Self::NoPermit => "no_permit",
        }
    }
}

/// Least time between two journal lines announcing a hold, as for the pressure duty.
const HOLD_REPORT_INTERVAL: Duration = Duration::from_secs(60);

/// Holds that overlap are one episode for the journal.
#[derive(Clone)]
struct Episode {
    frames: u64,
    longest: Duration,
    /// Whether the entering line was journaled; only then is the leaving line.
    announced: bool,
}
#[derive(Clone)]
pub(crate) struct Status {
    receiver: sparrow_core::SyslogReceiver,
    pub definition: SyslogListener,
    pub address: SocketAddr,
    pub active_connections: usize,
    counts: Counts,
    pub failure: Option<String>,
    /// Start of every hold in progress, one frame per connection: at most `max_connections`.
    holding: BTreeMap<u64, tokio::time::Instant>,
    next_hold: u64,
    episode: Option<Episode>,
    announced_at: Option<tokio::time::Instant>,
    /// Receive buffer and kernel drop counter; present for a UDP listener started by the worker.
    kernel: Option<crate::udp_kernel::Kernel>,
}
#[derive(Clone)]
struct Metrics(Arc<Mutex<Status>>);
impl Metrics {
    fn update(&self, update: impl FnOnce(&mut Status) -> io::Result<()>) -> io::Result<()> {
        let mut status = self
            .0
            .lock()
            .map_err(|_| io::Error::other("syslog status poisoned"))?;
        update(&mut status)
    }
    fn snapshot(&self) -> io::Result<Status> {
        Ok(self
            .0
            .lock()
            .map_err(|_| io::Error::other("syslog status poisoned"))?
            .clone())
    }
    /// Read the kernel drop counter now. Called only at events: a status request and the reader
    /// leaving a wait.
    ///
    /// The file read happens on a copy of the kernel evidence, outside the status mutex, so a
    /// slow read never holds up the counters the intake paths update. A read is one small file
    /// whose size follows the UDP sockets of the network namespace, taken once per event. It
    /// stays a plain read on the calling thread because every caller is a synchronous status
    /// path or the reader itself, which has just left a wait.
    fn sample_kernel(&self) -> io::Result<()> {
        let Some(kernel) = self.snapshot()?.kernel.filter(|kernel| !kernel.finished) else {
            return Ok(());
        };
        let counter = kernel.read();
        self.update(|status| {
            if let Some(kernel) = status.kernel.as_mut() {
                kernel.record(counter);
            }
            Ok(())
        })
    }
    /// Take the last sample while the socket is open, freeze the count and journal it.
    fn finish_kernel(&self) -> io::Result<()> {
        let mut line = None;
        self.update(|status| {
            if let Some(kernel) = status.kernel.as_mut() {
                kernel.finish();
                line = Some(match &kernel.counter {
                    crate::udp_kernel::Counter::Available(drops) => format!(
                        "sparrowd: syslog listener {}: stopped; the kernel dropped {drops} datagram(s) for this socket (final count)",
                        status.definition.id
                    ),
                    crate::udp_kernel::Counter::Unavailable(reason) => format!(
                        "sparrowd: syslog listener {}: stopped; the kernel drop count is unavailable: {reason}",
                        status.definition.id
                    ),
                });
            }
            Ok(())
        })?;
        if let Some(line) = line {
            journal(&line);
        }
        Ok(())
    }
}
impl Status {
    fn new(
        receiver: sparrow_core::SyslogReceiver,
        definition: SyslogListener,
        address: SocketAddr,
    ) -> Self {
        Self {
            receiver,
            definition,
            address,
            active_connections: 0,
            counts: Counts::default(),
            failure: None,
            holding: BTreeMap::new(),
            next_hold: 0,
            episode: None,
            announced_at: None,
            kernel: None,
        }
    }

    /// Register or continue one frame's hold at `now`; returns the journal line to print, if
    /// any. Holds that overlap are one episode. The entering line is limited to one per
    /// [`HOLD_REPORT_INTERVAL`], but an episode the limit suppressed is announced by its next
    /// call once the interval has passed, so no long hold is left unannounced. A pressure hold
    /// calls on every offer; a permit wait calls on every observer tick.
    fn hold(
        &mut self,
        id: &mut Option<u64>,
        seen: &mut [bool; 2],
        reason: HoldReason,
        now: tokio::time::Instant,
    ) -> io::Result<Option<String>> {
        if id.is_none() {
            self.next_hold += 1;
            *id = Some(self.next_hold);
            self.holding.insert(self.next_hold, now);
            self.episode
                .get_or_insert(Episode {
                    frames: 0,
                    longest: Duration::ZERO,
                    announced: false,
                })
                .frames += 1;
        }
        let slot = usize::from(reason == HoldReason::NoPermit);
        if !seen[slot] {
            seen[slot] = true;
            increment(match reason {
                HoldReason::StoragePressure => &mut self.counts.holds_storage_pressure,
                HoldReason::NoPermit => &mut self.counts.holds_no_permit,
            })?;
        }
        let quiet = self
            .announced_at
            .is_none_or(|at| now.duration_since(at) >= HOLD_REPORT_INTERVAL);
        let held = self.holding.len();
        let Some(episode) = self
            .episode
            .as_mut()
            .filter(|episode| !episode.announced && quiet)
        else {
            return Ok(None);
        };
        episode.announced = true;
        self.announced_at = Some(now);
        Ok(Some(format!(
            "sparrowd: syslog listener {}: holding {held} frame(s) under {}; each sender's TCP window closes until it clears",
            self.definition.id,
            reason.name()
        )))
    }

    /// End one frame's hold at `now`; returns the journal line when the episode it belonged
    /// to was announced and this was its last hold. An episode the limit never announced
    /// ended within the report interval and shows in the counters only.
    fn release(&mut self, id: u64, now: tokio::time::Instant) -> Option<String> {
        let since = self.holding.remove(&id)?;
        let waited = now.duration_since(since);
        self.counts.longest_hold_ms = self
            .counts
            .longest_hold_ms
            .max(u64::try_from(waited.as_millis()).unwrap_or(u64::MAX));
        if let Some(episode) = self.episode.as_mut() {
            episode.longest = episode.longest.max(waited);
        }
        if !self.holding.is_empty() {
            return None;
        }
        let episode = self.episode.take().filter(|episode| episode.announced)?;
        Some(format!(
            "sparrowd: syslog listener {}: hold ended; {} frame(s) held, longest wait {} ms",
            self.definition.id,
            episode.frames,
            episode.longest.as_millis()
        ))
    }

    /// The longest hold, finished or still running.
    fn longest_hold(&self) -> Duration {
        let running = self.holding.values().map(|since| since.elapsed()).max();
        running
            .unwrap_or_default()
            .max(Duration::from_millis(self.counts.longest_hold_ms))
    }
}

/// One frame's hold on its connection. The gauge falls when it is left or dropped, so a
/// failed or cancelled connection cannot leave it raised.
struct Hold<'a> {
    metrics: &'a Metrics,
    id: Option<u64>,
    seen: [bool; 2],
}
impl<'a> Hold<'a> {
    fn new(metrics: &'a Metrics) -> Self {
        Self {
            metrics,
            id: None,
            seen: [false; 2],
        }
    }

    /// Begin the hold if it has not begun, and count `reason` once for this frame. Called on
    /// every refused attempt, so a long hold is announced once the report interval allows.
    fn enter(&mut self, reason: HoldReason) -> io::Result<()> {
        let mut line = None;
        self.metrics.update(|status| {
            line = status.hold(
                &mut self.id,
                &mut self.seen,
                reason,
                tokio::time::Instant::now(),
            )?;
            Ok(())
        })?;
        if let Some(line) = line {
            journal(&line);
        }
        Ok(())
    }

    /// End the hold, if any, recording how long it lasted.
    fn leave(&mut self) -> io::Result<()> {
        let Some(id) = self.id.take() else {
            return Ok(());
        };
        let mut line = None;
        self.metrics.update(|status| {
            line = status.release(id, tokio::time::Instant::now());
            Ok(())
        })?;
        if let Some(line) = line {
            journal(&line);
        }
        Ok(())
    }
}
impl Drop for Hold<'_> {
    fn drop(&mut self) {
        // A poisoned status has nothing left to correct.
        let _ = self.leave();
    }
}

/// Print one journal line. Under test the line is also kept per thread, so a test can read
/// what an operator would have seen.
fn journal(line: &str) {
    #[cfg(test)]
    printed::note(line);
    eprintln!("{line}");
}

#[cfg(test)]
mod printed {
    use std::cell::RefCell;

    thread_local! {
        static LINES: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    }

    pub(super) fn note(line: &str) {
        LINES.with(|lines| lines.borrow_mut().push(line.to_owned()));
    }

    /// Every line printed on this thread so far (each test runs on its own thread).
    pub(super) fn lines() -> Vec<String> {
        LINES.with(|lines| lines.borrow().clone())
    }
}

fn increment(counter: &mut u64) -> io::Result<()> {
    *counter = counter
        .checked_add(1)
        .ok_or_else(|| io::Error::other("syslog intake counter overflow"))?;
    Ok(())
}

/// Shared finite runtime status registry, including listeners with no retained packets.
#[derive(Clone, Default)]
pub struct SyslogRegistry(Arc<Mutex<Vec<Metrics>>>);
impl SyslogRegistry {
    /// Every listener's status, reading each UDP kernel counter now: this is a status request.
    pub(crate) fn snapshot(&self) -> io::Result<Vec<Status>> {
        // The registry mutex is released before any /proc read.
        self.listeners()?
            .iter()
            .map(|metrics| {
                metrics.sample_kernel()?;
                metrics.snapshot()
            })
            .collect()
    }
    fn listeners(&self) -> io::Result<Vec<Metrics>> {
        Ok(self
            .0
            .lock()
            .map_err(|_| io::Error::other("syslog registry poisoned"))?
            .clone())
    }
    pub(crate) fn sources(&self) -> io::Result<Vec<sparrow_core::SourceSyslogBinding>> {
        // Ids only: no kernel counter is read.
        Ok(self
            .listeners()?
            .iter()
            .map(Metrics::snapshot)
            .collect::<io::Result<Vec<_>>>()?
            .into_iter()
            .map(|status| sparrow_core::SourceSyslogBinding {
                source: status.definition.source,
                listener_id: status.definition.id,
            })
            .collect())
    }
}

enum Socket {
    Tcp(TcpListener),
    Udp(UdpSocket),
}

/// Owned listener group; shutdown joins admitted writes before the daemon's final WAL fence.
pub struct SyslogWorker {
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<io::Result<()>>,
    /// Closes when the task ends, by return, panic or cancellation.
    ended: tokio::sync::oneshot::Receiver<()>,
}
impl SyslogWorker {
    /// Bind every configured socket before starting bounded ingestion tasks.
    pub async fn start(
        store: sparrow_core::Store,
        definitions: &[SyslogListener],
        operations: Arc<Semaphore>,
        registry: SyslogRegistry,
        wake: PressureWake,
    ) -> io::Result<Self> {
        validate(definitions)?;
        let mut sockets = Vec::new();
        let mut metrics = Vec::new();
        for definition in definitions {
            let mut kernel = None;
            let (socket, address) = match definition.transport {
                SyslogTransport::Tcp => {
                    let socket = TcpListener::bind(definition.address).await?;
                    let address = socket.local_addr()?;
                    (Socket::Tcp(socket), address)
                }
                SyslogTransport::Udp => {
                    let socket = UdpSocket::bind(definition.address).await?;
                    let address = socket.local_addr()?;
                    let buffer = crate::udp_kernel::request_buffer(
                        &socket,
                        crate::udp_kernel::REQUESTED_RCVBUF_BYTES,
                    )
                    .map_err(|error| {
                        io::Error::new(
                            error.kind(),
                            format!("syslog listener {}: {error}", definition.id),
                        )
                    })?;
                    let rmem_max = crate::udp_kernel::rmem_max()
                        .map_or_else(|| "unreadable".to_owned(), |limit| limit.to_string());
                    // A refused enlarge does not stop the daemon taking syslog: it keeps the
                    // default buffer and says so, here and in status.
                    journal(&match &buffer.enlarge_error {
                        None => format!(
                            "sparrowd: syslog listener {}: receive buffer requested {} bytes, kernel granted {} bytes (net.core.rmem_max {rmem_max})",
                            definition.id, buffer.requested, buffer.granted,
                        ),
                        Some(error) => format!(
                            "sparrowd: syslog listener {}: receive buffer enlarge failed: {error}; keeping the default buffer of {} bytes (requested {} bytes, net.core.rmem_max {rmem_max})",
                            definition.id, buffer.granted, buffer.requested,
                        ),
                    });
                    kernel = Some(
                        crate::udp_kernel::Kernel::new(&socket, address, buffer).map_err(
                            |error| {
                                io::Error::new(
                                    error.kind(),
                                    format!("syslog listener {}: {error}", definition.id),
                                )
                            },
                        )?,
                    );
                    (Socket::Udp(socket), address)
                }
            };
            let mut status = Status::new(
                sparrow_core::SyslogReceiver::new(
                    &definition.id,
                    match (definition.transport, definition.framing) {
                        (SyslogTransport::Udp, _) => "udp",
                        (SyslogTransport::Tcp, Some(SyslogFraming::OctetCounting)) => {
                            "tcp_octet_counting"
                        }
                        (SyslogTransport::Tcp, Some(SyslogFraming::Lf)) => "tcp_lf",
                        (SyslogTransport::Tcp, Some(SyslogFraming::CrLf)) => "tcp_cr_lf",
                        _ => return Err(io::Error::other("TCP receiver framing missing")),
                    },
                )
                .map_err(io::Error::other)?,
                definition.clone(),
                address,
            );
            status.kernel = kernel;
            let metric = Metrics(Arc::new(Mutex::new(status)));
            sockets.push((socket, metric.clone()));
            metrics.push(metric);
        }
        *registry
            .0
            .lock()
            .map_err(|_| io::Error::other("syslog registry poisoned"))? = metrics;
        let (stop, receiver) = watch::channel(false);
        let shutdown = stop.clone();
        let (alive, ended) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _alive = alive;
            let mut listeners = JoinSet::new();
            for (socket, metrics) in sockets {
                listeners.spawn(listen(
                    socket,
                    metrics,
                    store.clone(),
                    operations.clone(),
                    receiver.clone(),
                    shutdown.clone(),
                    wake.clone(),
                ));
            }
            finish_tasks(Ok(()), &mut listeners, &shutdown).await
        });
        Ok(Self { stop, task, ended })
    }
    /// Whether the worker has terminated.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }
    /// Resolve when the worker terminates, for the daemon health gate. Await it at most once.
    pub async fn ended(&mut self) {
        // The task never sends: its sender closes as it ends, the only outcome.
        let _ = (&mut self.ended).await;
    }
    /// Stop admission and wait for every in-flight atomic publication.
    pub async fn shutdown(self) -> io::Result<()> {
        self.stop.send_replace(true);
        self.task.await.map_err(io::Error::other)?
    }
}

/// Stop the entire intake group on failure, but join every admitted child first.
async fn finish_tasks(
    initial: io::Result<()>,
    tasks: &mut JoinSet<io::Result<()>>,
    stop: &watch::Sender<bool>,
) -> io::Result<()> {
    let mut failures = Vec::new();
    if let Err(error) = initial {
        stop.send_replace(true);
        failures.push(error.to_string());
    }
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result.map_err(io::Error::other).and_then(|value| value) {
            stop.send_replace(true);
            failures.push(error.to_string());
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(io::Error::other(failures.join("; ")))
    }
}

/// What one attempt to store a packet came to.
#[derive(Debug, PartialEq, Eq)]
enum Published {
    /// Durably stored and counted accepted.
    Stored,
    /// Refused and counted; the caller does not retry it.
    Refused,
    /// The store answered `STORAGE_PRESSURE` and the caller asked to keep the packet: nothing
    /// was counted, and the same bytes may be offered again.
    Pressured,
}

/// Offer one packet to the store. With `keep_on_pressure` a `STORAGE_PRESSURE` answer leaves
/// the packet with the caller instead of being counted as a refused discard.
async fn publish(
    store: sparrow_core::Store,
    source: String,
    bytes: Arc<[u8]>,
    metrics: &Metrics,
    keep_on_pressure: bool,
) -> io::Result<Published> {
    enum Attempt {
        Stored { failed: bool },
        Refused { durable: bool },
        Pressured,
    }
    let receiver = metrics.snapshot()?.receiver;
    let result = tokio::task::spawn_blocking(move || {
        let now = crate::now()?;
        let artifact = match store.ingest_received_syslog(&source, &receiver, &bytes, now) {
            Ok(artifact) => artifact,
            Err(error) if keep_on_pressure && error.code == "STORAGE_PRESSURE" => {
                return Ok(Attempt::Pressured);
            }
            // A full receiver-window registry no longer refuses (#215): the packet is stored
            // without a window and the store counts the lost evidence.
            Err(error) if matches!(error.code, "RESOURCE_LIMIT" | "STORAGE_PRESSURE") => {
                return record_discard(
                    &store,
                    &source,
                    &receiver,
                    sparrow_core::ReceiverDiscard::Refused,
                    now,
                )
                .map(|durable| Attempt::Refused { durable });
            }
            Err(error) => return Err(error),
        };
        let record = store.record(artifact.record_ids.first().ok_or_else(|| {
            sparrow_core::Error::new(
                "STORAGE_FAILURE",
                "syslog publication has no packet occurrence",
            )
        })?)?;
        let snapshot = record.syslog.ok_or_else(|| {
            sparrow_core::Error::new(
                "STORAGE_FAILURE",
                "syslog publication has no protocol snapshot",
            )
        })?;
        Ok::<_, sparrow_core::Error>(Attempt::Stored {
            failed: snapshot.diagnostic.is_some(),
        })
    })
    .await
    .map_err(io::Error::other)?
    .map_err(io::Error::other)?;
    match result {
        Attempt::Stored { failed } => {
            metrics.update(|status| {
                increment(&mut status.counts.accepted)?;
                if failed {
                    increment(&mut status.counts.interpretation_failures)?;
                }
                Ok(())
            })?;
            Ok(Published::Stored)
        }
        Attempt::Refused { durable } => {
            metrics.update(|status| {
                increment(&mut status.counts.refused)?;
                status.failure = Some(if durable {
                    "packet refused by storage admission".into()
                } else {
                    NOT_DURABLE.into()
                });
                Ok(())
            })?;
            Ok(Published::Refused)
        }
        Attempt::Pressured => Ok(Published::Pressured),
    }
}

/// Store one frame read from a TCP connection, holding it while the store refuses it for
/// pressure or no operation permit is free (#243). The caller reads no further frame until this
/// returns, so the sender's window closes and the sender holds. A frame held for pressure
/// blocks on `wake`, the stop and nothing else, and its same bytes are offered again at each
/// wake; it owns no timer. The daemon sends the wake at every ending it can see, and the
/// maintenance worker's one observation wakes every held frame at its one-second cadence
/// for the endings it cannot (see `crate::pressure_wake`). A stop ends the hold: the frame
/// gets one last attempt and, if refused, is counted exactly as before. A frame waiting for a
/// permit blocks on the permit and, so the journal can announce its wait once the report
/// interval has passed, on the same observer tick; it never retries on a clock.
async fn publish_held(
    store: &sparrow_core::Store,
    source: &str,
    bytes: Arc<[u8]>,
    metrics: &Metrics,
    operations: &Arc<Semaphore>,
    stop: &mut watch::Receiver<bool>,
    wake: &mut Waiter,
) -> io::Result<Published> {
    let mut hold = Hold::new(metrics);
    // Counted while the frame is held for pressure, so the observer tick wakes it.
    let mut pressured = None;
    loop {
        let permit = match operations.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                hold.enter(HoldReason::NoPermit)?;
                // A stop does not interrupt this wait: the frame must still be published, and
                // that takes a permit from the operation budget shutdown waits on anyway.
                // The wait blocks on the permit and, to announce a long wait the report
                // interval suppressed at entry, on the observer tick (counted as held so the
                // tick sends it). The one acquire future is kept across ticks: dropping it
                // would give up the frame's place in the fair queue.
                let _held = wake.hold();
                wake.mark_seen();
                let acquire = operations.clone().acquire_owned();
                tokio::pin!(acquire);
                loop {
                    tokio::select! {
                        permit = &mut acquire => break permit.map_err(io::Error::other)?,
                        () = wake.woken() => hold.enter(HoldReason::NoPermit)?,
                    }
                }
            }
            Err(error @ TryAcquireError::Closed) => return Err(io::Error::other(error)),
        };
        // A wake that lands during the attempt must still end the wait after it.
        wake.mark_seen();
        let stopping = *stop.borrow();
        let published = publish(
            store.clone(),
            source.to_owned(),
            bytes.clone(),
            metrics,
            !stopping,
        )
        .await?;
        drop(permit);
        if published != Published::Pressured {
            hold.leave()?;
            return Ok(published);
        }
        pressured.get_or_insert_with(|| wake.hold());
        hold.enter(HoldReason::StoragePressure)?;
        tokio::select! {
            changed = stop.changed() => { changed.map_err(io::Error::other)?; },
            () = wake.woken() => {},
        }
    }
}

const NOT_DURABLE: &str = "storage pressure refused the discard count; discards are counted in memory only until writes resume";

/// Count a discard durably, or report `false` when storage pressure (including the
/// free-space floor) refuses the count. Intake keeps running and the in-memory
/// counters still advance.
fn record_discard(
    store: &sparrow_core::Store,
    source: &str,
    receiver: &sparrow_core::SyslogReceiver,
    reason: sparrow_core::ReceiverDiscard,
    now: u64,
) -> Result<bool, sparrow_core::Error> {
    match store.discard_syslog(source, receiver, reason, now) {
        Ok(_) => Ok(true),
        Err(error) if error.code == "STORAGE_PRESSURE" => Ok(false),
        Err(error) => Err(error),
    }
}

/// One bounded waiter per admitted listener/connection; shutdown joins the control write.
async fn discard(
    store: &sparrow_core::Store,
    metrics: &Metrics,
    operations: &Arc<Semaphore>,
    reason: sparrow_core::ReceiverDiscard,
) -> io::Result<()> {
    let permit = operations
        .clone()
        .acquire_owned()
        .await
        .map_err(io::Error::other)?;
    let status = metrics.snapshot()?;
    let store = store.clone();
    let durable = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        record_discard(
            &store,
            &status.definition.source,
            &status.receiver,
            reason,
            crate::now()?,
        )
    })
    .await
    .map_err(io::Error::other)?
    .map_err(io::Error::other)?;
    metrics.update(|status| {
        if !durable {
            status.failure = Some(NOT_DURABLE.into());
        }
        increment(match reason {
            sparrow_core::ReceiverDiscard::Refused => &mut status.counts.refused,
            sparrow_core::ReceiverDiscard::Truncated => &mut status.counts.truncated,
            sparrow_core::ReceiverDiscard::FramingFailure => &mut status.counts.framing_failures,
            sparrow_core::ReceiverDiscard::TimedOut => &mut status.counts.timed_out,
        })
    })
}

async fn listen(
    socket: Socket,
    metrics: Metrics,
    store: sparrow_core::Store,
    operations: Arc<Semaphore>,
    mut stop: watch::Receiver<bool>,
    shutdown: watch::Sender<bool>,
    wake: PressureWake,
) -> io::Result<()> {
    let definition = metrics.snapshot()?.definition;
    match socket {
        Socket::Tcp(listener) => {
            let connections = Arc::new(Semaphore::new(definition.max_connections));
            let roster = Arc::new(Roster::default());
            let mut tasks = JoinSet::new();
            // Below the free-space floor every write is refused: accept no connections.
            let mut space = tokio::time::interval(std::time::Duration::from_secs(1));
            let mut space_ok = true;
            // At the ceiling one arrival is held while the longest-idle connection is closed
            // at a frame boundary for it; it is never refused.
            let mut pending: Option<tokio::net::TcpStream> = None;
            let mut evicting = false;
            let serving = async {
            loop {
                if *stop.borrow() { break; }
                tokio::select! {
                    _ = space.tick() => { space_ok = store.space_floor().shortfall().map_err(io::Error::other)?.is_none(); },
                    changed = stop.changed() => { changed.map_err(io::Error::other)?; if *stop.borrow() { break; } },
                    result = tasks.join_next(), if !tasks.is_empty() => { if let Some(result) = result { result.map_err(io::Error::other)??; } },
                    accepted = listener.accept(), if space_ok && pending.is_none() => {
                        pending = Some(accepted?.0);
                        evicting = false;
                    }
                    _ = roster.idled.notified(), if pending.is_some() && !evicting && connections.available_permits() == 0 => {}
                    permit = connections.clone().acquire_owned(), if pending.is_some() => {
                        let permit = permit.map_err(io::Error::other)?;
                        let Some(stream) = pending.take() else { continue; };
                        let lease = roster.enroll()?;
                        metrics.update(|status| { status.active_connections += 1; Ok(()) })?;
                        let metrics = metrics.clone(); let store = store.clone(); let operations = operations.clone(); let stop = stop.clone(); let wake = wake.clone();
                        tasks.spawn(async move {
                            let _permit = permit;
                            let result = receive_tcp(BufReader::with_capacity(4096, stream), &metrics, store, operations, stop, lease, wake).await;
                            metrics.update(|status| { status.active_connections = status.active_connections.checked_sub(1).ok_or_else(|| io::Error::other("syslog connection counter underflow"))?; Ok(()) })?;
                            result
                        });
                    }
                }
                if pending.is_some() && !evicting && connections.available_permits() == 0 {
                    evicting = roster.evict_longest_idle()?;
                }
            }
            Ok(())
            }.await;
            drop(listener);
            finish_tasks(serving, &mut tasks, &shutdown).await
        }
        Socket::Udp(socket) => {
            // A UDP payload never exceeds 65507 bytes, under the store's ceiling, so a whole datagram
            // always fits and nothing is cut; `max_message_bytes` frames TCP lines only.
            let mut buffer = vec![0; sparrow_core::SYSLOG_MESSAGE_BYTES];
            loop {
                if *stop.borrow() {
                    break;
                }
                let size = tokio::select! { changed = stop.changed() => { changed.map_err(io::Error::other)?; if *stop.borrow() { break; } continue; }, received = socket.recv_from(&mut buffer) => received?.0 };
                let Ok(permit) = operations.clone().try_acquire_owned() else {
                    discard(
                        &store,
                        &metrics,
                        &operations,
                        sparrow_core::ReceiverDiscard::Refused,
                    )
                    .await?;
                    // The reader has left a wait; datagrams may have been dropped meanwhile.
                    metrics.sample_kernel()?;
                    continue;
                };
                publish(
                    store.clone(),
                    definition.source.clone(),
                    Arc::from(&buffer[..size]),
                    &metrics,
                    false,
                )
                .await?;
                drop(permit);
            }
            // The last sample, taken while the socket is open and then frozen: the socket's row
            // leaves the table when it closes, and a later status request must still show this
            // count as final.
            metrics.finish_kernel()?;
            Ok(())
        }
    }
}

/// How the listener tells which connections are idle at a frame boundary, so a new arrival at
/// the connection ceiling can reclaim the one idle longest instead of being refused.
#[derive(Default)]
struct Roster {
    slots: Mutex<(u64, std::collections::BTreeMap<u64, Slot>)>,
    /// Signalled whenever a connection reaches a frame boundary.
    idled: tokio::sync::Notify,
}
struct Slot {
    idle_since: Option<std::time::Instant>,
    kick: Option<tokio::sync::oneshot::Sender<()>>,
}
impl Roster {
    fn enroll(self: &Arc<Self>) -> io::Result<Lease> {
        let (kick, kicked) = tokio::sync::oneshot::channel();
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| io::Error::other("syslog roster poisoned"))?;
        slots.0 += 1;
        let id = slots.0;
        slots.1.insert(
            id,
            Slot {
                idle_since: None,
                kick: Some(kick),
            },
        );
        Ok(Lease {
            roster: self.clone(),
            id,
            kicked,
        })
    }
    /// Ask the connection idle longest to close; `false` when none is idle at a boundary.
    fn evict_longest_idle(&self) -> io::Result<bool> {
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| io::Error::other("syslog roster poisoned"))?;
        let victim = slots
            .1
            .iter()
            .filter(|(_, slot)| slot.kick.is_some())
            .filter_map(|(id, slot)| slot.idle_since.map(|since| (since, *id)))
            .min()
            .map(|(_, id)| id);
        let Some(kick) = victim.and_then(|id| slots.1.get_mut(&id)?.kick.take()) else {
            return Ok(false);
        };
        // A closed receiver means the connection is already ending, which frees its slot.
        let _ = kick.send(());
        Ok(true)
    }
}

/// One connection's place in the roster; dropping it leaves the roster.
struct Lease {
    roster: Arc<Roster>,
    id: u64,
    kicked: tokio::sync::oneshot::Receiver<()>,
}
impl Lease {
    fn set_idle(&self, idle: bool) -> io::Result<()> {
        let mut slots = self
            .roster
            .slots
            .lock()
            .map_err(|_| io::Error::other("syslog roster poisoned"))?;
        if let Some(slot) = slots.1.get_mut(&self.id) {
            slot.idle_since = idle.then(std::time::Instant::now);
        }
        drop(slots);
        if idle {
            self.roster.idled.notify_one();
        }
        Ok(())
    }
    /// Whether the listener asked this connection to close at its next frame boundary.
    fn kicked(&mut self) -> bool {
        self.kicked.try_recv().is_ok()
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        if let Ok(mut slots) = self.roster.slots.lock() {
            slots.1.remove(&self.id);
        }
    }
}

/// Bytes of the frame being read. They live outside the read future so a deadline that
/// cancels it still leaves every byte read so far to be kept. At most the store ceiling is
/// held per connection; the rest of an oversize frame is counted and dropped as it arrives.
#[derive(Default)]
struct Partial {
    bytes: Vec<u8>,
    /// Octet-count header length, excluded from a complete frame but kept in a broken one.
    body_start: usize,
    /// Bytes beyond what is kept were read and dropped.
    dropped: bool,
}

/// How the read of one frame ended; a deadline is the caller's.
enum End {
    Complete,
    /// Bad octet-count start, a frame cut by end of stream, or a read error.
    Malformed(String),
    /// The frame deadline expired inside the frame (decided by the caller).
    Stalled,
    /// The daemon began stopping inside the frame (decided by the caller).
    Stopped,
}

enum Fault {
    Framing(String),
    TimedOut,
}

/// The packet to publish and what was lost reading it.
struct Frame {
    bytes: Vec<u8>,
    truncated: bool,
    fault: Option<Fault>,
}

impl Partial {
    fn into_frame(mut self, end: End) -> Frame {
        let fault = match end {
            End::Complete => {
                self.bytes.drain(..self.body_start);
                None
            }
            End::Malformed(reason) => Some(Fault::Framing(reason)),
            End::Stalled => Some(Fault::TimedOut),
            End::Stopped => Some(Fault::Framing("daemon stopping inside a frame".into())),
        };
        // A broken frame keeps its header too, so it can pass the store's ceiling: keep what
        // fits and count the cut.
        if self.bytes.len() > sparrow_core::SYSLOG_MESSAGE_BYTES {
            self.bytes.truncate(sparrow_core::SYSLOG_MESSAGE_BYTES);
            self.dropped = true;
        }
        Frame {
            bytes: self.bytes,
            truncated: self.dropped,
            fault,
        }
    }
}

async fn receive_tcp<R: AsyncRead + Unpin>(
    mut stream: R,
    metrics: &Metrics,
    store: sparrow_core::Store,
    operations: Arc<Semaphore>,
    mut stop: watch::Receiver<bool>,
    mut lease: Lease,
    wake: PressureWake,
) -> io::Result<()> {
    let mut wake = wake.subscribe();
    let definition = &metrics.snapshot()?.definition;
    let framing = definition
        .framing
        .ok_or_else(|| io::Error::other("TCP syslog framing missing"))?;
    let deadline = Duration::from_millis(u64::from(definition.read_timeout_ms));
    loop {
        if *stop.borrow() || lease.kicked() {
            return Ok(());
        }
        // At a frame boundary a quiet sender is idle, not faulty: no deadline runs here.
        lease.set_idle(true)?;
        let first = tokio::select! {
            biased;
            first = read_first(&mut stream) => first,
            _ = &mut lease.kicked => return Ok(()),
            changed = stop.changed() => { changed.map_err(io::Error::other)?; continue; },
        };
        lease.set_idle(false)?;
        let first = match first {
            Ok(Some(first)) => first,
            Ok(None) => return Ok(()),
            Err(error) => {
                discard(
                    &store,
                    metrics,
                    &operations,
                    sparrow_core::ReceiverDiscard::FramingFailure,
                )
                .await?;
                metrics.update(|status| {
                    status.failure = Some(error.to_string());
                    Ok(())
                })?;
                return Ok(());
            }
        };
        let mut partial = Partial::default();
        let end = tokio::select! {
            changed = stop.changed() => { changed.map_err(io::Error::other)?; None },
            end = tokio::time::timeout(deadline, read_frame(&mut stream, framing, definition.max_message_bytes, first, &mut partial)) => Some(end),
        };
        // A stop, a stall or a read error ends the stream: continuity past it is unknown. The
        // bytes read so far are still published, so shutdown loses none it holds.
        let (end, closing) = match end {
            Some(Ok(Ok(end))) => (end, false),
            Some(Ok(Err(error))) => (End::Malformed(error.to_string()), true),
            Some(Err(_)) => (End::Stalled, true),
            None => (End::Stopped, true),
        };
        let frame = partial.into_frame(end);
        // The frame is held here, unread past, until the store takes it.
        let accepted = publish_held(
            &store,
            &definition.source,
            Arc::from(frame.bytes),
            metrics,
            &operations,
            &mut stop,
            &mut wake,
        )
        .await?
            == Published::Stored;
        if frame.truncated {
            discard(
                &store,
                metrics,
                &operations,
                sparrow_core::ReceiverDiscard::Truncated,
            )
            .await?;
        }
        let closing = match frame.fault {
            None => closing,
            Some(fault) => {
                let (reason, message) = match fault {
                    Fault::Framing(message) => {
                        (sparrow_core::ReceiverDiscard::FramingFailure, message)
                    }
                    Fault::TimedOut => (
                        sparrow_core::ReceiverDiscard::TimedOut,
                        "TCP frame deadline expired; continuity unknown".into(),
                    ),
                };
                discard(&store, metrics, &operations, reason).await?;
                metrics.update(|status| {
                    status.failure = Some(message);
                    Ok(())
                })?;
                closing
            }
        };
        if !accepted || closing {
            return Ok(());
        }
    }
}

async fn read_first<R: AsyncRead + Unpin>(stream: &mut R) -> io::Result<Option<u8>> {
    let mut byte = [0_u8];
    Ok((stream.read(&mut byte).await? == 1).then_some(byte[0]))
}

/// Read the rest of one frame whose first byte is `first`, keeping every byte that fits and
/// dropping the remainder of an oversize frame as it arrives, in constant memory.
///
/// Octet counting reads exactly the announced length (at most 99999): the whole frame is kept up
/// to the store's syslog ceiling and only the bytes above it are dropped. A line frame keeps
/// its first `maximum` bytes. A bad octet-count start keeps the line it begins, so the next
/// frame starts after the newline. Cancel safe: every byte is in `partial` before the next
/// await.
async fn read_frame<R: AsyncRead + Unpin>(
    stream: &mut R,
    framing: SyslogFraming,
    maximum: usize,
    first: u8,
    partial: &mut Partial,
) -> io::Result<End> {
    if framing != SyslogFraming::OctetCounting {
        return read_line(
            stream,
            partial,
            maximum,
            framing == SyslogFraming::CrLf,
            first,
        )
        .await;
    }
    partial.bytes.push(first);
    if !(b'1'..=b'9').contains(&first) {
        return bad_start(
            stream,
            partial,
            maximum,
            "octet count requires a nonzero first digit",
        )
        .await;
    }
    let mut size = usize::from(first - b'0');
    let mut digits = 1;
    loop {
        let mut byte = [0_u8];
        if stream.read(&mut byte).await? == 0 {
            return Ok(End::Malformed("frame cut inside the octet count".into()));
        }
        partial.bytes.push(byte[0]);
        if byte[0] == b' ' {
            break;
        }
        if !byte[0].is_ascii_digit() || digits == 5 {
            return bad_start(stream, partial, maximum, "invalid bounded octet count").await;
        }
        size = size * 10 + usize::from(byte[0] - b'0');
        digits += 1;
    }
    let header = partial.bytes.len();
    partial.body_start = header;
    let keep = size.min(sparrow_core::SYSLOG_MESSAGE_BYTES);
    let mut chunk = [0_u8; 4096];
    while partial.bytes.len() - header < keep {
        let want = (keep - (partial.bytes.len() - header)).min(chunk.len());
        let read = stream.read(&mut chunk[..want]).await?;
        if read == 0 {
            return Ok(End::Malformed("frame cut by end of stream".into()));
        }
        partial.bytes.extend_from_slice(&chunk[..read]);
    }
    let mut rest = size - keep;
    partial.dropped = rest > 0;
    while rest > 0 {
        let want = rest.min(chunk.len());
        let read = stream.read(&mut chunk[..want]).await?;
        if read == 0 {
            return Ok(End::Malformed("frame cut by end of stream".into()));
        }
        rest -= read;
    }
    Ok(End::Complete)
}

/// A bad octet-count start: keep the bytes read and the rest of the line, then resume at the
/// next frame after its newline.
async fn bad_start<R: AsyncRead + Unpin>(
    stream: &mut R,
    partial: &mut Partial,
    maximum: usize,
    reason: &str,
) -> io::Result<End> {
    // The offending byte is already in `partial`; the line reader takes it back.
    let Some(bad) = partial.bytes.pop() else {
        return Ok(End::Malformed(reason.into()));
    };
    read_line(stream, partial, maximum, false, bad).await?;
    Ok(End::Malformed(reason.into()))
}

/// Read to the line terminator (LF, or CRLF when `crlf`), keeping the first `maximum` content
/// bytes. The terminator is not part of the packet; with LF framing a preceding CR is.
async fn read_line<R: AsyncRead + Unpin>(
    stream: &mut R,
    partial: &mut Partial,
    maximum: usize,
    crlf: bool,
    mut byte: u8,
) -> io::Result<End> {
    // CRLF holds one more byte so an exactly full line's trailing CR can be told from content.
    let cap = maximum + usize::from(crlf);
    let mut last = 0_u8;
    loop {
        if byte == b'\n' && (!crlf || last == b'\r') {
            if crlf && !partial.dropped {
                partial.bytes.pop();
            }
            partial.bytes.truncate(maximum);
            return Ok(End::Complete);
        }
        if partial.bytes.len() < cap {
            partial.bytes.push(byte);
        } else {
            partial.dropped = true;
        }
        last = byte;
        let mut next = [0_u8];
        if stream.read(&mut next).await? == 0 {
            return Ok(End::Malformed("frame cut by end of stream".into()));
        }
        byte = next[0];
    }
}

impl Status {
    pub(crate) fn encode(
        &self,
        mut out: crate::sparrow_capnp::syslog_listener_status::Builder<'_>,
    ) {
        out.set_id(&self.definition.id);
        out.set_source(&self.definition.source);
        out.set_address(self.address.to_string());
        out.set_transport(match self.definition.transport {
            SyslogTransport::Tcp => crate::sparrow_capnp::SyslogTransport::Tcp,
            SyslogTransport::Udp => crate::sparrow_capnp::SyslogTransport::Udp,
        });
        match self.definition.framing {
            None => out.reborrow().init_framing().set_absent(()),
            Some(value) => out.reborrow().init_framing().set_value(match value {
                SyslogFraming::OctetCounting => crate::sparrow_capnp::SyslogFraming::OctetCounting,
                SyslogFraming::Lf => crate::sparrow_capnp::SyslogFraming::Lf,
                SyslogFraming::CrLf => crate::sparrow_capnp::SyslogFraming::CrLf,
            }),
        }
        out.set_max_message_bytes(self.definition.max_message_bytes as u32);
        out.set_max_connections(self.definition.max_connections as u32);
        out.set_read_timeout_ms(self.definition.read_timeout_ms);
        out.set_active_connections(self.active_connections as u32);
        out.set_held_frames(self.holding.len() as u32);
        out.set_holds_storage_pressure(self.counts.holds_storage_pressure);
        out.set_holds_no_permit(self.counts.holds_no_permit);
        out.set_longest_hold_ms(u64::try_from(self.longest_hold().as_millis()).unwrap_or(u64::MAX));
        out.set_accepted_messages(self.counts.accepted);
        out.set_interpretation_failures(self.counts.interpretation_failures);
        out.set_refused_messages(self.counts.refused);
        out.set_truncated_datagrams(self.counts.truncated);
        out.set_framing_failures(self.counts.framing_failures);
        // Arrivals at the ceiling are served by closing the longest-idle connection, never refused.
        out.set_rejected_connections(0);
        out.set_timed_out_connections(self.counts.timed_out);
        out.set_continuity_unknown(true);
        self.encode_kernel(out.reborrow());
        match &self.failure {
            Some(value) => out.init_failure().set_value(value),
            None => out.init_failure().set_absent(()),
        }
    }

    /// The UDP kernel evidence. A count is written only when it was read; otherwise the count is
    /// zero, `kernel_drops_unknown` is set and the reason is stated.
    fn encode_kernel(&self, mut out: crate::sparrow_capnp::syslog_listener_status::Builder<'_>) {
        use crate::sparrow_capnp::KernelCounterState;
        use crate::udp_kernel::Counter;
        if self.definition.transport != SyslogTransport::Udp {
            out.set_kernel_drops_unknown(false);
            out.set_kernel_counter_state(KernelCounterState::NotApplicable);
            out.reborrow().init_kernel_counter_reason().set_absent(());
            out.init_rcvbuf_enlarge_error().set_absent(());
            return;
        }
        let unattached =
            Counter::Unavailable("no kernel counter is attached to this listener".into());
        let counter = self
            .kernel
            .as_ref()
            .map_or(&unattached, |kernel| &kernel.counter);
        if let Some(kernel) = &self.kernel {
            out.set_rcvbuf_requested_bytes(kernel.buffer.requested as u64);
            out.set_rcvbuf_granted_bytes(kernel.buffer.granted as u64);
            out.set_kernel_counter_final(kernel.finished);
            match &kernel.buffer.enlarge_error {
                Some(error) => {
                    out.set_rcvbuf_enlarge_failures(1);
                    out.reborrow().init_rcvbuf_enlarge_error().set_value(error);
                }
                None => out.reborrow().init_rcvbuf_enlarge_error().set_absent(()),
            }
        }
        match counter {
            Counter::Available(drops) => {
                out.set_kernel_drops_unknown(false);
                out.set_kernel_drops(*drops);
                out.set_kernel_counter_state(KernelCounterState::Available);
                out.init_kernel_counter_reason().set_absent(());
            }
            Counter::Unavailable(reason) => {
                out.set_kernel_drops_unknown(true);
                out.set_kernel_counter_state(KernelCounterState::Unavailable);
                out.init_kernel_counter_reason().set_value(reason);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn poisoned_syslog_registry_refuses_both_discovery_paths_with_typed_failure() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let dir = tempfile::tempdir().unwrap();
                let data = dir.path().join("store");
                let config = sparrow_test_support::write_server_config(
                    dir.path(),
                    &data,
                    &data.join("rpc.sock"),
                );
                let store = crate::ServerConfig::read(&config)
                    .unwrap()
                    .open_store()
                    .unwrap();
                let sid = store.store_id().unwrap();
                let registry = SyslogRegistry::default();
                let poisoning = registry.clone();
                assert!(
                    std::thread::spawn(move || {
                        let _locked = poisoning.0.lock().unwrap();
                        panic!("injected syslog registry owner failure");
                    })
                    .join()
                    .is_err()
                );
                let client: crate::sparrow_capnp::sparrow::Client = capnp_rpc::new_client(
                    crate::Rpc::new(store.clone(), Arc::new(Semaphore::new(4)), 100)
                        .with_syslog(registry),
                );
                let mut request = client.sources_list_request();
                let mut query = request.get().init_query();
                query.set_store_id(&sid);
                query.set_schema_version(sparrow_client::SCHEMA_VERSION);
                query.reborrow().init_source().set_absent(());
                query.reborrow().init_cursor().set_absent(());
                query.set_page_size(100);
                query.set_scan_budget(10000);
                let response = request.send().promise.await.unwrap();
                let crate::sparrow_capnp::result::Error(error) = response
                    .get()
                    .unwrap()
                    .get_result()
                    .unwrap()
                    .which()
                    .unwrap()
                else {
                    panic!("poisoned source registry published a page")
                };
                assert_eq!(
                    error.unwrap().get_code().unwrap().to_str().unwrap(),
                    "STORAGE_FAILURE"
                );
                let mut request = client.syslog_listener_get_request();
                let mut query = request.get().init_query();
                query.set_store_id(&sid);
                query.set_schema_version(sparrow_client::SCHEMA_VERSION);
                query.set_source("source");
                query.set_id("listener");
                let response = request.send().promise.await.unwrap();
                let crate::sparrow_capnp::result::Error(error) = response
                    .get()
                    .unwrap()
                    .get_result()
                    .unwrap()
                    .which()
                    .unwrap()
                else {
                    panic!("poisoned listener registry published a lookup")
                };
                assert_eq!(
                    error.unwrap().get_code().unwrap().to_str().unwrap(),
                    "STORAGE_FAILURE"
                );
                assert_eq!(
                    store.status(crate::now().unwrap()).unwrap()["source_read_handles"].as_str(),
                    Some("0")
                );
            })
            .await;
    }

    /// INVARIANT: below the free-space floor a packet is refused and counted in
    /// memory; refusing its durable discard count does not fail the intake worker.
    #[tokio::test]
    async fn packet_refused_below_the_free_space_floor_keeps_intake_running() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let config =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let base = std::fs::read_to_string(&config).unwrap();
        let floor = "min_free_bytes = 1048576";
        assert!(base.contains(floor));
        std::fs::write(
            &config,
            base.replace(floor, "min_free_bytes = 9223372036854775807"),
        )
        .unwrap();
        let store = crate::ServerConfig::read(&config)
            .unwrap()
            .open_store()
            .unwrap();
        let definition = SyslogListener {
            id: "listener".into(),
            address: "127.0.0.1:0".parse().unwrap(),
            source: "source".into(),
            transport: SyslogTransport::Udp,
            framing: None,
            max_message_bytes: 480,
            max_connections: 1,
            read_timeout_ms: 1000,
        };
        let metrics = Metrics(Arc::new(Mutex::new(Status::new(
            sparrow_core::SyslogReceiver::new("listener", "udp").unwrap(),
            definition.clone(),
            definition.address,
        ))));
        let published = publish(
            store.clone(),
            "source".into(),
            Arc::from(&b"<34>refused"[..]),
            &metrics,
            false,
        )
        .await
        .unwrap();
        assert_eq!(published, Published::Refused);
        let status = metrics.snapshot().unwrap();
        assert_eq!(status.counts.refused, 1);
        assert_eq!(status.failure.as_deref(), Some(NOT_DURABLE));
        assert_eq!(store.durability_status().unwrap().failure, None);
    }

    #[tokio::test]
    async fn failed_intake_joins_blocking_publication_and_retains_all_child_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("publication");
        let published = path.clone();
        let (stop, mut stopped) = watch::channel(false);
        let (entered, running) = tokio::sync::oneshot::channel();
        let (release, paused) = std::sync::mpsc::channel();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            tokio::task::spawn_blocking(move || {
                entered.send(()).unwrap();
                paused.recv().unwrap();
                std::fs::write(&published, b"admitted publication")?;
                std::fs::File::open(&published)?.sync_all()?;
                Ok::<_, io::Error>(())
            })
            .await
            .map_err(io::Error::other)??;
            Err(io::Error::other("publication completion failure"))
        });
        running.await.unwrap();
        tasks.spawn(async { Err(io::Error::other("listener failure")) });
        tasks.spawn(async { panic!("intake task panic") });
        let mut idle = stopped.clone();
        tasks.spawn(async move {
            idle.changed().await.map_err(io::Error::other)?;
            assert!(*idle.borrow());
            Ok(())
        });
        let supervisor = tokio::spawn(async move { finish_tasks(Ok(()), &mut tasks, &stop).await });
        tokio::time::timeout(Duration::from_secs(5), stopped.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(*stopped.borrow());
        assert!(
            !supervisor.is_finished(),
            "admitted publication was not joined"
        );
        assert!(!path.exists());
        release.send(()).unwrap();
        let error = tokio::time::timeout(Duration::from_secs(5), supervisor)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .to_string();
        for cause in [
            "listener failure",
            "intake task panic",
            "publication completion failure",
        ] {
            assert!(error.contains(cause), "lost {cause}: {error}");
        }
        assert_eq!(std::fs::read(path).unwrap(), b"admitted publication");
    }

    /// Read the next frame of `input` the way the listener does: first byte, then the rest.
    async fn next_frame<R: AsyncRead + Unpin>(
        stream: &mut R,
        framing: SyslogFraming,
        maximum: usize,
    ) -> Option<Frame> {
        let first = read_first(stream).await.unwrap()?;
        let mut partial = Partial::default();
        let end = read_frame(stream, framing, maximum, first, &mut partial)
            .await
            .unwrap();
        Some(partial.into_frame(end))
    }

    /// INVARIANT: a well-formed frame is read byte-exact in every framing, whatever the
    /// transport's chunking (a two-byte pipe here), with the trailer excluded.
    #[tokio::test]
    async fn configured_frames_preserve_octets_and_delimiters() {
        for (framing, input, expected) in [
            (
                SyslogFraming::OctetCounting,
                b"5 <34>x5 <34>x".as_slice(),
                b"<34>x".as_slice(),
            ),
            (SyslogFraming::Lf, b"abc\r\nabc\r\n", b"abc\r"),
            (SyslogFraming::CrLf, b"a\nb\r\na\nb\r\n", b"a\nb"),
        ] {
            let (mut reader, mut writer) = tokio::io::duplex(2);
            let input = input.to_vec();
            let writing = tokio::spawn(async move {
                tokio::io::AsyncWriteExt::write_all(&mut writer, &input)
                    .await
                    .unwrap();
            });
            for _ in 0..2 {
                let frame = next_frame(&mut reader, framing, expected.len())
                    .await
                    .unwrap();
                assert_eq!(frame.bytes, expected);
                assert!(frame.fault.is_none() && !frame.truncated);
            }
            assert!(
                next_frame(&mut reader, framing, expected.len())
                    .await
                    .is_none()
            );
            writing.await.unwrap();
        }
    }

    /// Value: an octet-counted frame above `max_message_bytes` is a log the store can hold,
    /// so it is kept whole; only bytes above the store's 65536 ceiling are cut, counted, and
    /// dropped as they arrive, leaving the next frame in sync on the same stream.
    #[tokio::test]
    async fn octet_counted_oversize_is_kept_whole_to_the_store_ceiling_then_cut_and_counted() {
        let ceiling = sparrow_core::SYSLOG_MESSAGE_BYTES;
        let mut input = Vec::new();
        for size in [600, ceiling, ceiling + 1, 99_999] {
            input.extend_from_slice(format!("{size} ").as_bytes());
            input.extend(std::iter::repeat_n(b'x', size));
        }
        input.extend_from_slice(b"5 <34>y");
        let mut input = input.as_slice();
        for (kept, truncated) in [
            (600, false),
            (ceiling, false),
            (ceiling, true),
            (ceiling, true),
        ] {
            let frame = next_frame(&mut input, SyslogFraming::OctetCounting, 480)
                .await
                .unwrap();
            assert_eq!(frame.bytes.len(), kept);
            assert!(frame.bytes.iter().all(|byte| *byte == b'x'));
            assert_eq!((frame.truncated, frame.fault.is_none()), (truncated, true));
        }
        let frame = next_frame(&mut input, SyslogFraming::OctetCounting, 480)
            .await
            .unwrap();
        assert_eq!(frame.bytes, b"<34>y");
    }

    /// Value: a line above `max_message_bytes` keeps its first `max_message_bytes` bytes and
    /// the next line is intact, in LF and CRLF, whether the overflow is one byte or a megabyte;
    /// an exactly full line is not cut.
    #[tokio::test]
    async fn oversize_lines_keep_the_prefix_and_resume_at_the_newline() {
        for (framing, terminator) in [
            (SyslogFraming::Lf, b"\n".as_slice()),
            (SyslogFraming::CrLf, b"\r\n".as_slice()),
        ] {
            let mut input = Vec::new();
            for length in [480, 481, 1_000_000] {
                input.extend(std::iter::repeat_n(b'a', length));
                input.extend_from_slice(terminator);
            }
            input.extend_from_slice(b"next");
            input.extend_from_slice(terminator);
            let mut input = input.as_slice();
            for truncated in [false, true, true] {
                let frame = next_frame(&mut input, framing, 480).await.unwrap();
                assert_eq!(frame.bytes, vec![b'a'; 480]);
                assert_eq!((frame.truncated, frame.fault.is_none()), (truncated, true));
            }
            let frame = next_frame(&mut input, framing, 480).await.unwrap();
            assert_eq!(frame.bytes, b"next");
        }
    }

    /// Value: a bad octet-count start or a frame cut by end of stream is still evidence. The
    /// bytes read are kept as a malformed packet with a framing fault, the bad start's line is
    /// kept whole, and the next frame after its newline is read normally.
    #[tokio::test]
    async fn bad_octet_start_and_cut_frames_keep_their_bytes_and_resynchronise() {
        let framing = SyslogFraming::OctetCounting;
        let mut input = b"<34>lf sender\n123456 digits\n05 zero\n6 <34>ok9 short".as_slice();
        for expected in [b"<34>lf sender".as_slice(), b"123456 digits", b"05 zero"] {
            let frame = next_frame(&mut input, framing, 480).await.unwrap();
            assert_eq!(frame.bytes, expected);
            assert!(matches!(frame.fault, Some(Fault::Framing(_))));
        }
        let frame = next_frame(&mut input, framing, 480).await.unwrap();
        assert_eq!(
            (frame.bytes.as_slice(), frame.fault.is_none()),
            (&b"<34>ok"[..], true)
        );
        let frame = next_frame(&mut input, framing, 480).await.unwrap();
        assert_eq!(frame.bytes, b"9 short");
        assert!(matches!(frame.fault, Some(Fault::Framing(_))));
        assert!(next_frame(&mut input, framing, 480).await.is_none());
    }

    /// Value: a frame cut near or above the store's 65536-byte ceiling is still kept. Its
    /// header counts toward the stored packet, so the bytes published are capped at the ceiling
    /// and the cut is counted; otherwise the store would refuse the whole frame.
    #[tokio::test]
    async fn a_cut_frame_at_the_store_ceiling_is_published_capped_and_counted() {
        let ceiling = sparrow_core::SYSLOG_MESSAGE_BYTES;
        let mut octets = b"70000 ".to_vec();
        octets.extend(std::iter::repeat_n(b'x', ceiling));
        let mut line = vec![b'y'; ceiling + 5];
        line.push(b'z');
        for (framing, mut input) in [
            (SyslogFraming::OctetCounting, octets.as_slice()),
            (SyslogFraming::CrLf, line.as_slice()),
        ] {
            let frame = next_frame(&mut input, framing, ceiling).await.unwrap();
            assert_eq!(frame.bytes.len(), ceiling);
            assert!(frame.truncated);
            assert!(matches!(frame.fault, Some(Fault::Framing(_))));
        }
    }

    /// Value: a daemon that stops inside a frame publishes the bytes it already read as a
    /// malformed packet instead of dropping them, and counts the cut.
    #[tokio::test]
    async fn stopping_inside_a_frame_publishes_the_bytes_read() {
        let fixture = listener(SyslogTransport::Tcp, 1, 60_000);
        let (mut sender, receiver) = tokio::io::duplex(64);
        let (stop, stopped) = watch::channel(false);
        let roster = Arc::new(Roster::default());
        let lease = roster.enroll().unwrap();
        let metrics = fixture.metrics.clone();
        let store = fixture.store.clone();
        let reading = tokio::spawn(async move {
            receive_tcp(
                receiver,
                &metrics,
                store,
                Arc::new(Semaphore::new(4)),
                stopped,
                lease,
                Default::default(),
            )
            .await
        });
        // Wait until the connection idles at the boundary, then send the partial frame and wait
        // until its first byte has put it inside the frame.
        let idle_is = |idle: bool| {
            roster
                .slots
                .lock()
                .unwrap()
                .1
                .values()
                .all(|slot| slot.idle_since.is_some() == idle)
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            while !idle_is(true) {
                tokio::task::yield_now().await;
            }
            tokio::io::AsyncWriteExt::write_all(&mut sender, b"20 <34>half")
                .await
                .unwrap();
            while !idle_is(false) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        stop.send_replace(true);
        reading.await.unwrap().unwrap();
        let counts = fixture.metrics.snapshot().unwrap().counts;
        assert_eq!((counts.accepted, counts.framing_failures), (1, 1));
        assert_eq!(counts.refused, 0);
    }

    struct Listener {
        _dir: tempfile::TempDir,
        store: sparrow_core::Store,
        metrics: Metrics,
        wake: PressureWake,
        operations: Arc<Semaphore>,
    }

    fn listener(transport: SyslogTransport, connections: usize, timeout_ms: u32) -> Listener {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let config =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let store = crate::ServerConfig::read(&config)
            .unwrap()
            .open_store()
            .unwrap();
        let tcp = transport == SyslogTransport::Tcp;
        let definition = SyslogListener {
            id: "listener".into(),
            address: "127.0.0.1:0".parse().unwrap(),
            source: "source".into(),
            transport,
            framing: tcp.then_some(SyslogFraming::OctetCounting),
            max_message_bytes: 480,
            max_connections: connections,
            read_timeout_ms: timeout_ms,
        };
        let metrics = Metrics(Arc::new(Mutex::new(Status::new(
            sparrow_core::SyslogReceiver::new(
                "listener",
                if tcp { "tcp_octet_counting" } else { "udp" },
            )
            .unwrap(),
            definition.clone(),
            definition.address,
        ))));
        Listener {
            _dir: dir,
            store,
            metrics,
            wake: PressureWake::default(),
            operations: Arc::new(Semaphore::new(4)),
        }
    }

    async fn until(metrics: &Metrics, done: impl Fn(&Status) -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !done(&metrics.snapshot().unwrap()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("listener did not reach the expected counters");
    }

    async fn serve(
        socket: Socket,
        fixture: &Listener,
    ) -> (watch::Sender<bool>, tokio::task::JoinHandle<io::Result<()>>) {
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(listen(
            socket,
            fixture.metrics.clone(),
            fixture.store.clone(),
            fixture.operations.clone(),
            stopped,
            stop.clone(),
            fixture.wake.clone(),
        ));
        (stop, task)
    }

    /// Value: a sender that stalls inside a frame loses nothing already received: the bytes
    /// read are published as a malformed packet and `timed_out` is counted. Only a stall
    /// inside a frame is a fault, so the stream is closed after it.
    #[tokio::test]
    async fn a_stall_inside_a_frame_publishes_the_bytes_read_and_counts_timed_out() {
        let fixture = listener(SyslogTransport::Tcp, 1, 50);
        let (mut sender, receiver) = tokio::io::duplex(64);
        tokio::io::AsyncWriteExt::write_all(&mut sender, b"20 <34>half")
            .await
            .unwrap();
        let (_stop, stopped) = watch::channel(false);
        let lease = Arc::new(Roster::default()).enroll().unwrap();
        receive_tcp(
            receiver,
            &fixture.metrics,
            fixture.store.clone(),
            Arc::new(Semaphore::new(4)),
            stopped,
            lease,
            Default::default(),
        )
        .await
        .unwrap();
        let counts = fixture.metrics.snapshot().unwrap().counts;
        assert_eq!((counts.accepted, counts.timed_out), (1, 1));
        assert_eq!(counts.framing_failures + counts.refused, 0);
    }

    /// Value: a sender quiet between frames is not faulty. The deadline starts at a frame's
    /// first byte, so a pause far past it at a boundary costs no count and no connection, and
    /// the next frame is kept.
    #[tokio::test]
    async fn an_idle_sender_at_a_frame_boundary_is_not_a_fault() {
        let fixture = listener(SyslogTransport::Tcp, 1, 50);
        let (mut sender, receiver) = tokio::io::duplex(64);
        let (_stop, stopped) = watch::channel(false);
        let lease = Arc::new(Roster::default()).enroll().unwrap();
        let metrics = fixture.metrics.clone();
        let store = fixture.store.clone();
        let reading = tokio::spawn(async move {
            receive_tcp(
                receiver,
                &metrics,
                store,
                Arc::new(Semaphore::new(4)),
                stopped,
                lease,
                Default::default(),
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        tokio::io::AsyncWriteExt::write_all(&mut sender, b"5 <34>x")
            .await
            .unwrap();
        until(&fixture.metrics, |status| status.counts.accepted == 1).await;
        assert!(!reading.is_finished());
        drop(sender);
        reading.await.unwrap().unwrap();
        let counts = fixture.metrics.snapshot().unwrap().counts;
        assert_eq!(counts.timed_out + counts.framing_failures, 0);
    }

    /// Value: at the connection ceiling a new sender is served, not refused: the connection
    /// idle longest is closed at its frame boundary (the daemon closes it, with nothing
    /// unread), and the arrival's frame is then kept.
    #[tokio::test]
    async fn a_new_connection_at_the_ceiling_closes_the_longest_idle_one() {
        let fixture = listener(SyslogTransport::Tcp, 1, 60_000);
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = tcp.local_addr().unwrap();
        let (stop, task) = serve(Socket::Tcp(tcp), &fixture).await;
        let mut idle = tokio::net::TcpStream::connect(address).await.unwrap();
        until(&fixture.metrics, |status| status.active_connections == 1).await;
        let mut arrival = tokio::net::TcpStream::connect(address).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut arrival, b"5 <34>x")
            .await
            .unwrap();
        let mut byte = [0_u8];
        assert_eq!(idle.read(&mut byte).await.unwrap(), 0);
        until(&fixture.metrics, |status| status.counts.accepted == 1).await;
        assert_eq!(fixture.metrics.snapshot().unwrap().counts.timed_out, 0);
        stop.send_replace(true);
        task.await.unwrap().unwrap();
    }

    /// One frame is held: taken off the wire and not read again until it is stored.
    fn frame_taken(status: &Status) -> bool {
        status.holding.len() == 1
    }

    /// Lengths of the stored syslog packets in durable observation order.
    fn stored_lengths(store: &sparrow_core::Store) -> Vec<u64> {
        let mut records = store.records(crate::now().unwrap()).unwrap();
        records.sort_by_key(|record| record.receiver.as_ref().map(|position| position.sequence));
        records
            .iter()
            .map(|record| record.byte_end - record.byte_start)
            .collect()
    }

    /// Run one connection's receiver over `receiver` until it returns.
    fn connection(
        fixture: &Listener,
        receiver: tokio::io::DuplexStream,
        stopped: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<io::Result<()>> {
        let metrics = fixture.metrics.clone();
        let store = fixture.store.clone();
        let lease = Arc::new(Roster::default()).enroll().unwrap();
        let wake = fixture.wake.clone();
        let operations = fixture.operations.clone();
        tokio::spawn(async move {
            receive_tcp(receiver, &metrics, store, operations, stopped, lease, wake).await
        })
    }

    /// Lift the pressure the tests apply and tell every holder, as the maintenance duty does.
    fn relieve(fixture: &Listener) {
        fixture.store.space_floor().refuse_writes_for_tests(false);
        fixture.wake.wake();
    }

    /// Value (#243): a frame the store refuses under STORAGE_PRESSURE is a log in hand, so it
    /// is kept, not discarded: the same bytes are retried, the next frame is not read, so the
    /// sender's window closes and it holds, and the connection stays open. When pressure
    /// clears both frames are stored, in order, and nothing is counted refused.
    #[tokio::test]
    async fn a_frame_refused_by_pressure_is_held_unread_past_and_published_when_it_clears() {
        let fixture = listener(SyslogTransport::Tcp, 1, 60_000);
        fixture.store.space_floor().refuse_writes_for_tests(true);
        let (mut sender, receiver) = tokio::io::duplex(16);
        let (_stop, stopped) = watch::channel(false);
        let reading = connection(&fixture, receiver, stopped);
        tokio::io::AsyncWriteExt::write_all(&mut sender, b"5 <34>a")
            .await
            .unwrap();
        // The second frame is larger than the pipe, so it can only finish once it is read.
        let writing = tokio::spawn(async move {
            tokio::io::AsyncWriteExt::write_all(&mut sender, b"20 <34>bbbbbbbbbbbbbbbb")
                .await
                .unwrap();
            sender
        });
        until(&fixture.metrics, frame_taken).await;
        assert!(
            !writing.is_finished(),
            "the next frame was read while the first was held"
        );
        assert!(
            !reading.is_finished(),
            "the held frame's connection was closed"
        );
        relieve(&fixture);
        until(&fixture.metrics, |status| status.counts.accepted == 2).await;
        let sender = writing.await.unwrap();
        let counts = fixture.metrics.snapshot().unwrap().counts;
        assert_eq!(
            counts.refused + counts.timed_out + counts.framing_failures,
            0
        );
        assert_eq!(stored_lengths(&fixture.store), [5, 20]);
        drop(sender);
        reading.await.unwrap().unwrap();
    }

    /// Value (#243): a hold is visible while it lasts, which an operator needs to tell a
    /// sender held by the daemon from a silent one. Status carries the frames held now, the
    /// frame counted once for its reason however often it is retried, and the longest wait,
    /// running or finished, through the wire encoding and its JSON rendering. The hold
    /// outlasts the 1 s read timeout and must not trip it. Time is the paused test clock, so
    /// no wall time passes.
    #[tokio::test(start_paused = true)]
    async fn a_hold_shows_in_status_with_its_reason_and_longest_wait() {
        let fixture = listener(SyslogTransport::Tcp, 1, 1000);
        fixture.store.space_floor().refuse_writes_for_tests(true);
        let (mut sender, receiver) = tokio::io::duplex(64);
        let (_stop, stopped) = watch::channel(false);
        let reading = connection(&fixture, receiver, stopped);
        tokio::io::AsyncWriteExt::write_all(&mut sender, b"5 <34>a")
            .await
            .unwrap();
        // No timeout guard here: with a paused clock an idle runtime would fire it early.
        yield_until("the frame is held", || {
            frame_taken(&fixture.metrics.snapshot().unwrap())
        })
        .await;
        tokio::time::advance(Duration::from_secs(5)).await;
        let held = fixture.metrics.snapshot().unwrap();
        assert!(held.longest_hold() >= Duration::from_secs(5));
        assert_eq!(
            (
                held.counts.holds_storage_pressure,
                held.counts.holds_no_permit
            ),
            (1, 0)
        );
        assert_eq!(wire_fields(&held).0, (1, 1, 0));
        relieve(&fixture);
        yield_until("the frame is stored", || {
            fixture.metrics.snapshot().unwrap().counts.accepted != 0
        })
        .await;
        let done = fixture.metrics.snapshot().unwrap();
        assert!(done.holding.is_empty());
        assert!(done.counts.longest_hold_ms >= 5000);
        assert_eq!(done.counts.holds_storage_pressure, 1);
        assert_eq!(done.counts.timed_out + done.counts.framing_failures, 0);
        let ((frames, pressure, permit), longest, json) = wire_fields(&done);
        assert_eq!((frames, pressure, permit), (0, 1, 0));
        assert!(longest >= 5000);
        assert_eq!(json["held_frames"].as_u64(), Some(0));
        assert_eq!(json["holds_storage_pressure"].as_str(), Some("1"));
        assert_eq!(json["holds_no_permit"].as_str(), Some("0"));
        assert_eq!(
            json["longest_hold_ms"].as_str(),
            Some(longest.to_string().as_str())
        );
        drop(sender);
        reading.await.unwrap().unwrap();
    }

    /// The hold fields of a status as the wire carries them: held now, held for pressure, held
    /// for a permit; the longest hold in milliseconds; and the JSON rendering of the listener.
    fn wire_fields(status: &Status) -> ((u32, u64, u64), u64, purrdf_lex::json::Value) {
        let mut message = capnp::message::Builder::new_default();
        status.encode(message.init_root::<crate::sparrow_capnp::syslog_listener_status::Builder>());
        let reader = message
            .get_root_as_reader::<crate::sparrow_capnp::syslog_listener_status::Reader>()
            .unwrap();
        (
            (
                reader.get_held_frames(),
                reader.get_holds_storage_pressure(),
                reader.get_holds_no_permit(),
            ),
            reader.get_longest_hold_ms(),
            crate::syslog_wire::listener(reader).unwrap(),
        )
    }

    /// The listener's JSON rendering after the wire encoding, as status shows it.
    fn status_json(status: &Status) -> purrdf_lex::json::Value {
        wire_fields(status).2
    }

    /// Value (#254): status states the receive buffer and the kernel drop count of a UDP
    /// listener through the wire and its JSON, and a TCP listener states neither applies.
    #[test]
    fn udp_status_carries_buffer_sizes_and_the_kernel_drop_count() {
        let udp = listener(SyslogTransport::Udp, 1, 1000);
        let buffer = crate::udp_kernel::Buffer {
            requested: 8 << 20,
            granted: 16 << 20,
            enlarge_error: None,
        };
        let dir = tempfile::tempdir().unwrap();
        let table = dir.path().join("udp");
        std::fs::write(
            &table,
            "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops\n  1: 0100007F:0202 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 777 2 0000000000000000 42\n",
        )
        .unwrap();
        udp.metrics
            .update(|status| {
                status.kernel = Some(crate::udp_kernel::Kernel::at(table, 777, buffer));
                Ok(())
            })
            .unwrap();
        let json = status_json(&udp.metrics.snapshot().unwrap());
        assert_eq!(json["rcvbuf_requested_bytes"].as_str(), Some("8388608"));
        assert_eq!(json["rcvbuf_granted_bytes"].as_str(), Some("16777216"));
        assert_eq!(json["kernel_drops"].as_str(), Some("42"));
        assert_eq!(json["kernel_counter_state"].as_str(), Some("available"));
        assert_eq!(json["kernel_drops_unknown"].as_bool(), Some(false));
        let tcp = status_json(
            &listener(SyslogTransport::Tcp, 1, 1000)
                .metrics
                .snapshot()
                .unwrap(),
        );
        assert_eq!(tcp["kernel_counter_state"].as_str(), Some("not_applicable"));
        assert_eq!(tcp["kernel_drops_unknown"].as_bool(), Some(false));
    }

    /// Value (#254): when the kernel counter cannot be read (a unit hiding /proc/self/net, a
    /// missing file) status says unavailable with the reason, flags the drops unknown and
    /// reports no count, so an operator never reads a guessed zero as "no loss".
    #[test]
    fn an_unreadable_kernel_counter_is_reported_unavailable_with_no_count() {
        let udp = listener(SyslogTransport::Udp, 1, 1000);
        let dir = tempfile::tempdir().unwrap();
        let buffer = crate::udp_kernel::Buffer {
            requested: 8 << 20,
            granted: 425_984,
            enlarge_error: None,
        };
        udp.metrics
            .update(|status| {
                status.kernel = Some(crate::udp_kernel::Kernel::at(
                    dir.path().join("hidden"),
                    777,
                    buffer,
                ));
                Ok(())
            })
            .unwrap();
        let json = status_json(&udp.metrics.snapshot().unwrap());
        assert_eq!(json["kernel_counter_state"].as_str(), Some("unavailable"));
        assert_eq!(json["kernel_drops_unknown"].as_bool(), Some(true));
        assert_eq!(json["kernel_drops"].as_str(), Some("0"));
        assert_eq!(json["rcvbuf_granted_bytes"].as_str(), Some("425984"));
        assert!(
            json["kernel_counter_reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("hidden"))
        );
    }

    /// Value (#254): the count is read at the events, not at bind. A status request through the
    /// registry must pick up drops that happened since the last read, and once the listener has
    /// stopped its final count must survive later status requests even though the closed
    /// socket's row has left the kernel table. Both would be silently wrong otherwise: a stale
    /// zero marked available, or a lost count.
    #[test]
    fn a_status_request_rereads_the_counter_and_a_stopped_listener_keeps_its_final_count() {
        let udp = listener(SyslogTransport::Udp, 1, 1000);
        let dir = tempfile::tempdir().unwrap();
        let table = dir.path().join("udp");
        let header = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops\n";
        let row = |drops: u64| {
            format!(
                "  1: 0100007F:0202 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 777 2 0000000000000000 {drops}\n"
            )
        };
        std::fs::write(&table, format!("{header}{}", row(5))).unwrap();
        let buffer = crate::udp_kernel::Buffer {
            requested: 8 << 20,
            granted: 16 << 20,
            enlarge_error: None,
        };
        udp.metrics
            .update(|status| {
                status.kernel = Some(crate::udp_kernel::Kernel::at(table.clone(), 777, buffer));
                Ok(())
            })
            .unwrap();
        let registry = SyslogRegistry::default();
        registry.0.lock().unwrap().push(udp.metrics.clone());
        let drops = |registry: &SyslogRegistry| {
            let status = registry.snapshot().unwrap().remove(0);
            let kernel = status.kernel.clone().unwrap();
            (kernel.counter, kernel.finished, status_json(&status))
        };
        assert_eq!(drops(&registry).0, crate::udp_kernel::Counter::Available(5));
        std::fs::write(&table, format!("{header}{}", row(9))).unwrap();
        assert_eq!(drops(&registry).0, crate::udp_kernel::Counter::Available(9));
        // The reader stops; the socket closes and its row leaves the table.
        std::fs::write(&table, format!("{header}{}", row(12))).unwrap();
        udp.metrics.finish_kernel().unwrap();
        std::fs::write(&table, header).unwrap();
        let (counter, finished, json) = drops(&registry);
        assert_eq!(counter, crate::udp_kernel::Counter::Available(12));
        assert!(finished);
        assert_eq!(json["kernel_drops"].as_str(), Some("12"));
        assert_eq!(json["kernel_counter_final"].as_bool(), Some(true));
        assert!(
            printed::lines()
                .iter()
                .any(|line| line.contains("final count"))
        );
    }

    /// Value (#254): a refused enlarge is shown in status as an error text and a count, with the
    /// size the socket kept, and the listener still reports its drop counter.
    #[test]
    fn a_refused_enlarge_shows_in_status_with_its_error_and_the_kept_size() {
        let udp = listener(SyslogTransport::Udp, 1, 1000);
        let dir = tempfile::tempdir().unwrap();
        let buffer = crate::udp_kernel::Buffer {
            requested: 8 << 20,
            granted: 212_992,
            enlarge_error: Some("cannot set SO_RCVBUF: permission denied".into()),
        };
        udp.metrics
            .update(|status| {
                status.kernel = Some(crate::udp_kernel::Kernel::at(
                    dir.path().join("udp"),
                    1,
                    buffer,
                ));
                Ok(())
            })
            .unwrap();
        let json = status_json(&udp.metrics.snapshot().unwrap());
        assert_eq!(json["rcvbuf_enlarge_failures"].as_str(), Some("1"));
        assert_eq!(json["rcvbuf_granted_bytes"].as_str(), Some("212992"));
        assert!(
            json["rcvbuf_enlarge_error"]
                .as_str()
                .is_some_and(|error| error.contains("permission denied"))
        );
    }

    /// Value (#243): a frame waiting for an operation permit is held and counted as such, not
    /// as pressure, and is stored once a permit frees. The test holds every permit itself.
    #[tokio::test]
    async fn a_frame_waiting_for_an_operation_permit_is_held_and_counted_as_no_permit() {
        let fixture = listener(SyslogTransport::Tcp, 1, 60_000);
        let permits = fixture
            .operations
            .clone()
            .acquire_many_owned(4)
            .await
            .unwrap();
        let (mut sender, receiver) = tokio::io::duplex(64);
        let (_stop, stopped) = watch::channel(false);
        let reading = connection(&fixture, receiver, stopped);
        tokio::io::AsyncWriteExt::write_all(&mut sender, b"5 <34>a")
            .await
            .unwrap();
        until(&fixture.metrics, frame_taken).await;
        let held = fixture.metrics.snapshot().unwrap();
        assert_eq!(
            (
                held.counts.holds_no_permit,
                held.counts.holds_storage_pressure
            ),
            (1, 0)
        );
        assert_eq!(held.counts.accepted, 0);
        drop(permits);
        until(&fixture.metrics, |status| status.counts.accepted == 1).await;
        let done = fixture.metrics.snapshot().unwrap();
        assert!(done.holding.is_empty());
        assert_eq!(
            (
                done.counts.holds_no_permit,
                done.counts.holds_storage_pressure
            ),
            (1, 0)
        );
        assert_eq!(stored_lengths(&fixture.store), [5]);
        drop(sender);
        reading.await.unwrap().unwrap();
    }

    /// Value (#245): `enter(NoPermit)` runs once before the permit wait, so a wait that began
    /// inside the report interval and outlasted it was never announced. Protects: with a
    /// recent announcement, a frame waiting for a permit is silent at entry, still silent
    /// after the interval passes with no event (no timer), and the observer tick announces it
    /// as a printed `no_permit` line; the wait keeps its place and the frame is then stored.
    #[tokio::test(start_paused = true)]
    async fn a_permit_wait_is_announced_by_the_observer_tick_after_the_interval() {
        let fixture = listener(SyslogTransport::Tcp, 1, 1000);
        // An announcement a moment ago, as an earlier episode would have left.
        fixture
            .metrics
            .update(|status| {
                status.announced_at = Some(tokio::time::Instant::now());
                Ok(())
            })
            .unwrap();
        let permits = fixture
            .operations
            .clone()
            .acquire_many_owned(4)
            .await
            .unwrap();
        let (mut sender, receiver) = tokio::io::duplex(64);
        let (_stop, stopped) = watch::channel(false);
        let reading = connection(&fixture, receiver, stopped);
        tokio::io::AsyncWriteExt::write_all(&mut sender, b"5 <34>a")
            .await
            .unwrap();
        yield_until("the frame waits for a permit", || {
            frame_taken(&fixture.metrics.snapshot().unwrap())
        })
        .await;
        let announced = || {
            fixture
                .metrics
                .snapshot()
                .unwrap()
                .episode
                .as_ref()
                .map(|episode| episode.announced)
        };
        assert_eq!(announced(), Some(false), "suppressed inside the interval");
        tokio::time::advance(HOLD_REPORT_INTERVAL + Duration::from_secs(1)).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert_eq!(announced(), Some(false), "no timer announces the wait");
        assert!(printed::lines().is_empty());
        fixture.wake.observe_tick();
        yield_until("the tick announces the wait", || announced() == Some(true)).await;
        assert_eq!(
            printed::lines(),
            [
                "sparrowd: syslog listener listener: holding 1 frame(s) under no_permit; each sender's TCP window closes until it clears"
            ]
        );
        drop(permits);
        yield_until("the frame is stored", || {
            fixture.metrics.snapshot().unwrap().counts.accepted == 1
        })
        .await;
        drop(sender);
        reading.await.unwrap().unwrap();
    }

    /// Let every task the paused clock has made ready run to its next wait, including the
    /// blocking store call a re-offered frame would start: yield so the frame reaches it, wait
    /// for a blocking store call that takes the writer lock after it (a flush), then yield so
    /// the frame handles its answer. A negative assertion made after this sees a timer's work.
    async fn settle(fixture: &Listener) {
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        let store = fixture.store.clone();
        tokio::task::spawn_blocking(move || store.flush())
            .await
            .unwrap()
            .unwrap();
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
    }

    /// Yield until `done` holds, failing with `what` after a wall-clock bound instead of
    /// hanging. Not a timeout future: under a paused clock an idle runtime would fire that
    /// early, while the blocking store call still runs.
    async fn yield_until(what: &str, done: impl Fn() -> bool) {
        let began = std::time::Instant::now();
        while !done() {
            assert!(
                began.elapsed() < Duration::from_secs(30),
                "did not happen within 30 s: {what}"
            );
            tokio::task::yield_now().await;
        }
    }

    /// Start a connection whose first frame is held under a pause observed through the
    /// measured free space, the way a nearly full volume pauses the floor. The floor's resume
    /// edge is wired as the daemon wires it. Returns the connection, the sender and the
    /// refusals counted so far, once the frame is held.
    async fn held_under_observed_pause(
        fixture: &Listener,
    ) -> (
        tokio::task::JoinHandle<io::Result<()>>,
        tokio::io::DuplexStream,
        watch::Sender<bool>,
        u64,
    ) {
        let floor = fixture.store.space_floor();
        floor.set_free_for_tests(Some(0));
        floor.set_resume_waker(Some(fixture.wake.resume_waker(|| {})));
        assert!(floor.observe(1).unwrap().is_some());
        let (mut sender, receiver) = tokio::io::duplex(64);
        let (stop, stopped) = watch::channel(false);
        let reading = connection(fixture, receiver, stopped);
        tokio::io::AsyncWriteExt::write_all(&mut sender, b"5 <34>a")
            .await
            .unwrap();
        // No timeout guard here: with a paused clock an idle runtime would fire it early.
        yield_until("the frame is held", || {
            frame_taken(&fixture.metrics.snapshot().unwrap())
        })
        .await;
        (reading, sender, stop, floor.status().refused_writes)
    }

    /// Yield until the held frame has been offered to the store `more` times again.
    async fn until_offered_again(fixture: &Listener, refused: u64) {
        yield_until("the held frame is offered again", || {
            fixture.store.space_floor().status().refused_writes > refused
        })
        .await;
    }

    /// Value (#247): a held frame owned a 50 ms to 1 s doubling timer, a retry on a clock
    /// while nothing could change. Protects: with no signal an hour of time passes and the
    /// frame is not offered again; the one observer tick wakes it to evaluate, and free space
    /// that returned with no edge seen (another process freed it) ends the hold at that tick.
    #[tokio::test(start_paused = true)]
    async fn a_held_frame_has_no_timer_and_is_evaluated_again_only_by_the_observer_tick() {
        let fixture = listener(SyslogTransport::Tcp, 1, 1000);
        let (reading, sender, _stop, refused) = held_under_observed_pause(&fixture).await;
        tokio::time::advance(Duration::from_secs(3600)).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert_eq!(fixture.store.space_floor().status().refused_writes, refused);
        // The pause persists at the tick: the frame is offered again and stays held.
        fixture.wake.observe_tick();
        until_offered_again(&fixture, refused).await;
        assert_eq!(fixture.metrics.snapshot().unwrap().counts.accepted, 0);
        // Free space returned and no observation saw the edge; the next tick ends the hold.
        fixture.store.space_floor().set_free_for_tests(None);
        fixture.wake.observe_tick();
        yield_until("the frame is stored", || {
            fixture.metrics.snapshot().unwrap().counts.accepted != 0
        })
        .await;
        assert_eq!(stored_lengths(&fixture.store), [5]);
        drop(sender);
        reading.await.unwrap().unwrap();
    }

    /// Value (#247): the main loop observes the floor every second beside maintenance, and
    /// whichever sees the resume edge consumes it; maintenance compared counters around its
    /// own observation, so an edge the main loop took woke nobody. Protects: the edge itself
    /// wakes the held frame, from any observer, with no tick and no time passing.
    #[tokio::test(start_paused = true)]
    async fn the_floor_resume_wakes_a_held_frame_from_any_observer_with_no_timer() {
        let fixture = listener(SyslogTransport::Tcp, 1, 1000);
        let (reading, sender, _stop, _) = held_under_observed_pause(&fixture).await;
        // The sender closes while its frame is held; nothing already read is lost (#243).
        drop(sender);
        let floor = fixture.store.space_floor();
        floor.set_free_for_tests(None);
        // This observer is not maintenance; it consumes the edge.
        assert!(floor.observe(2).unwrap().is_none());
        assert_eq!(floor.status().resumes, 1);
        yield_until("the frame is stored", || {
            fixture.metrics.snapshot().unwrap().counts.accepted != 0
        })
        .await;
        assert_eq!(stored_lengths(&fixture.store), [5]);
        reading.await.unwrap().unwrap();
        let counts = fixture.metrics.snapshot().unwrap().counts;
        assert_eq!(
            counts.refused + counts.framing_failures + counts.timed_out,
            0
        );
    }

    /// Value (#243, #247): a long hold is always announced. An episode that starts inside the
    /// report interval is suppressed at entry and is announced by the frame's next offer once
    /// the interval has passed; the frame owns no timer now, so only the observer tick can
    /// make that offer. Protects: with the interval passed and no other event the episode
    /// stays unannounced until the tick, then is announced.
    #[tokio::test(start_paused = true)]
    async fn a_suppressed_hold_is_announced_by_the_observer_tick_after_the_interval() {
        let fixture = listener(SyslogTransport::Tcp, 1, 1000);
        let (reading, mut sender, _stop, _) = held_under_observed_pause(&fixture).await;
        let announced = || {
            fixture
                .metrics
                .snapshot()
                .unwrap()
                .episode
                .as_ref()
                .map(|episode| episode.announced)
        };
        assert_eq!(announced(), Some(true), "the first episode is announced");
        // End it, then pause again so a second episode starts inside the interval.
        let floor = fixture.store.space_floor();
        floor.set_free_for_tests(None);
        assert!(floor.observe(2).unwrap().is_none());
        yield_until("the first frame is stored", || {
            fixture.metrics.snapshot().unwrap().counts.accepted == 1
        })
        .await;
        floor.set_free_for_tests(Some(0));
        assert!(floor.observe(3).unwrap().is_some());
        tokio::io::AsyncWriteExt::write_all(&mut sender, b"5 <34>b")
            .await
            .unwrap();
        yield_until("the second frame is held", || {
            frame_taken(&fixture.metrics.snapshot().unwrap())
        })
        .await;
        assert_eq!(
            announced(),
            Some(false),
            "inside the interval it is suppressed"
        );
        let refused = floor.status().refused_writes;
        tokio::time::advance(HOLD_REPORT_INTERVAL + Duration::from_secs(1)).await;
        settle(&fixture).await;
        assert_eq!(announced(), Some(false), "no timer re-offers the frame");
        assert_eq!(floor.status().refused_writes, refused);
        let before = printed::lines().len();
        fixture.wake.observe_tick();
        yield_until("the tick announces the episode", || {
            announced() == Some(true)
        })
        .await;
        // The chain ends in the printed line an operator reads, not only the flag.
        let lines = printed::lines();
        assert_eq!(lines.len(), before + 1, "{lines:?}");
        assert_eq!(
            lines.last().unwrap(),
            "sparrowd: syslog listener listener: holding 1 frame(s) under storage_pressure; each sender's TCP window closes until it clears"
        );
        drop(sender);
        floor.set_free_for_tests(None);
        fixture.wake.wake();
        reading.await.unwrap().unwrap();
    }

    /// Value (#243): a refusal no retry changes (`RESOURCE_LIMIT`) is not held. It keeps its
    /// counted discard and closes the connection as before, where holding it would stall the
    /// sender forever.
    #[tokio::test]
    async fn a_permanent_refusal_is_counted_not_held() {
        let fixture = listener(SyslogTransport::Tcp, 1, 60_000);
        fixture
            .store
            .space_floor()
            .refuse_next_write_for_tests("RESOURCE_LIMIT");
        let (mut sender, receiver) = tokio::io::duplex(64);
        let (_stop, stopped) = watch::channel(false);
        let reading = connection(&fixture, receiver, stopped);
        tokio::io::AsyncWriteExt::write_all(&mut sender, b"5 <34>a")
            .await
            .unwrap();
        reading.await.unwrap().unwrap();
        let status = fixture.metrics.snapshot().unwrap();
        assert_eq!((status.counts.refused, status.counts.accepted), (1, 0));
        assert_eq!(
            (
                status.counts.holds_storage_pressure,
                status.counts.holds_no_permit
            ),
            (0, 0)
        );
        assert!(status.holding.is_empty() && status.counts.longest_hold_ms == 0);
    }

    /// Value (#243): the journal keeps its promise to an operator. The first episode is
    /// announced and its end journaled. An episode that starts inside the report interval is
    /// not announced then, but is once its retries pass the interval, and its end is journaled
    /// with the count and the longest wait; a short suppressed episode leaves no lines.
    #[test]
    fn a_long_hold_is_always_announced_and_its_end_journaled() {
        let fixture = listener(SyslogTransport::Tcp, 1, 1000);
        let mut status = fixture.metrics.snapshot().unwrap();
        let t0 = tokio::time::Instant::now();
        let at = |secs| t0 + Duration::from_secs(secs);
        let hold = |status: &mut Status, id: &mut Option<u64>, secs| {
            status
                .hold(id, &mut [false; 2], HoldReason::StoragePressure, at(secs))
                .unwrap()
        };
        let mut first = None;
        assert!(hold(&mut status, &mut first, 0).is_some());
        assert!(hold(&mut status, &mut first, 1).is_none());
        let line = status.release(first.unwrap(), at(2)).unwrap();
        assert!(
            line.contains("1 frame(s) held, longest wait 2000 ms"),
            "{line}"
        );
        let mut second = None;
        assert!(hold(&mut status, &mut second, 10).is_none());
        assert!(hold(&mut status, &mut second, 59).is_none());
        assert!(hold(&mut status, &mut second, 61).is_some());
        let line = status.release(second.unwrap(), at(100)).unwrap();
        assert!(
            line.contains("1 frame(s) held, longest wait 90000 ms"),
            "{line}"
        );
        let mut third = None;
        assert!(hold(&mut status, &mut third, 101).is_none());
        assert!(status.release(third.unwrap(), at(102)).is_none());
    }

    /// Value (#243): shutdown interrupts a hold and the frame in hand is still accounted as
    /// before: stored when the store takes it by then, counted refused when pressure persists.
    #[tokio::test]
    async fn stopping_during_a_hold_publishes_the_frame_or_counts_it() {
        for cleared in [false, true] {
            let fixture = listener(SyslogTransport::Tcp, 1, 60_000);
            fixture.store.space_floor().refuse_writes_for_tests(true);
            let (mut sender, receiver) = tokio::io::duplex(64);
            let (stop, stopped) = watch::channel(false);
            let reading = connection(&fixture, receiver, stopped);
            tokio::io::AsyncWriteExt::write_all(&mut sender, b"5 <34>a")
                .await
                .unwrap();
            until(&fixture.metrics, frame_taken).await;
            if cleared {
                relieve(&fixture);
            }
            stop.send_replace(true);
            reading.await.unwrap().unwrap();
            let counts = fixture.metrics.snapshot().unwrap().counts;
            assert_eq!(
                (counts.accepted, counts.refused),
                if cleared { (1, 0) } else { (0, 1) }
            );
        }
    }

    /// Value (#243): a connection holding a frame is not idle. At the connection ceiling an
    /// arrival does not reclaim it; the holder's frame is stored when pressure clears, and only
    /// then, idle at a boundary, is it reclaimed for the arrival, whose frame is then kept.
    #[tokio::test]
    async fn a_holder_at_the_ceiling_is_not_reclaimed_until_it_is_idle() {
        let fixture = listener(SyslogTransport::Tcp, 1, 60_000);
        fixture.store.space_floor().refuse_writes_for_tests(true);
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = tcp.local_addr().unwrap();
        let (stop, task) = serve(Socket::Tcp(tcp), &fixture).await;
        let mut holder = tokio::net::TcpStream::connect(address).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut holder, b"5 <34>a")
            .await
            .unwrap();
        until(&fixture.metrics, frame_taken).await;
        let mut arrival = tokio::net::TcpStream::connect(address).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut arrival, b"20 <34>bbbbbbbbbbbbbbbb")
            .await
            .unwrap();
        relieve(&fixture);
        let mut byte = [0_u8];
        assert_eq!(holder.read(&mut byte).await.unwrap(), 0);
        until(&fixture.metrics, |status| status.counts.accepted == 2).await;
        assert_eq!(stored_lengths(&fixture.store), [5, 20]);
        let counts = fixture.metrics.snapshot().unwrap().counts;
        assert_eq!(
            counts.refused + counts.timed_out + counts.framing_failures,
            0
        );
        stop.send_replace(true);
        task.await.unwrap().unwrap();
    }

    /// Value: a UDP datagram above `max_message_bytes` but within the store ceiling is kept
    /// whole, not discarded as truncated; nothing is cut, so nothing is counted as cut.
    #[tokio::test]
    async fn a_udp_datagram_above_the_configured_size_is_kept_whole() {
        let fixture = listener(SyslogTransport::Udp, 1, 1000);
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let (stop, task) = serve(Socket::Udp(socket), &fixture).await;
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sender.send_to(&vec![b'x'; 481], address).await.unwrap();
        sender.send_to(&vec![b'x'; 65507], address).await.unwrap();
        until(&fixture.metrics, |status| status.counts.accepted == 2).await;
        assert_eq!(fixture.metrics.snapshot().unwrap().counts.truncated, 0);
        stop.send_replace(true);
        task.await.unwrap().unwrap();
    }

    #[test]
    fn syslog_config_has_explicit_local_authority_and_transport_specific_bounds() {
        let valid = SyslogListener {
            id: "test".into(),
            address: "127.0.0.1:0".parse().unwrap(),
            source: "syslog".into(),
            transport: SyslogTransport::Tcp,
            framing: Some(SyslogFraming::OctetCounting),
            max_message_bytes: 480,
            max_connections: 1,
            read_timeout_ms: 1000,
        };
        validate(std::slice::from_ref(&valid)).unwrap();
        for (transport, framing, connections, size, address) in [
            (SyslogTransport::Tcp, None, 1, 480, "127.0.0.1:0"),
            (SyslogTransport::Udp, valid.framing, 1, 480, "127.0.0.1:0"),
            (SyslogTransport::Udp, None, 2, 480, "127.0.0.1:0"),
            (SyslogTransport::Tcp, valid.framing, 1, 479, "127.0.0.1:0"),
            (SyslogTransport::Tcp, valid.framing, 17, 480, "127.0.0.1:0"),
            (SyslogTransport::Tcp, valid.framing, 1, 480, "0.0.0.0:514"),
        ] {
            let mut invalid = valid.clone();
            invalid.transport = transport;
            invalid.framing = framing;
            invalid.max_connections = connections;
            invalid.max_message_bytes = size;
            invalid.address = address.parse().unwrap();
            assert!(validate(&[invalid]).is_err());
        }
        assert!(validate(&[valid.clone(), valid]).is_err());
    }
}
