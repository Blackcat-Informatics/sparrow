// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Explicit bounded syslog intake, independent of the private typed query transport.

use serde::Deserialize;
use std::{
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, BufReader},
    net::{TcpListener, UdpSocket},
    sync::{Semaphore, watch},
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
}
#[derive(Clone)]
pub(crate) struct Status {
    receiver: sparrow_core::SyslogReceiver,
    pub definition: SyslogListener,
    pub address: SocketAddr,
    pub active_connections: usize,
    counts: Counts,
    pub failure: Option<String>,
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
    pub(crate) fn snapshot(&self) -> io::Result<Vec<Status>> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("syslog registry poisoned"))?
            .iter()
            .map(Metrics::snapshot)
            .collect()
    }
    pub(crate) fn sources(&self) -> io::Result<Vec<sparrow_core::SourceSyslogBinding>> {
        Ok(self
            .snapshot()?
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
    ) -> io::Result<Self> {
        validate(definitions)?;
        let mut sockets = Vec::new();
        let mut metrics = Vec::new();
        for definition in definitions {
            let (socket, address) = match definition.transport {
                SyslogTransport::Tcp => {
                    let socket = TcpListener::bind(definition.address).await?;
                    let address = socket.local_addr()?;
                    (Socket::Tcp(socket), address)
                }
                SyslogTransport::Udp => {
                    let socket = UdpSocket::bind(definition.address).await?;
                    let address = socket.local_addr()?;
                    (Socket::Udp(socket), address)
                }
            };
            let metric = Metrics(Arc::new(Mutex::new(Status {
                receiver: sparrow_core::SyslogReceiver::new(
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
                definition: definition.clone(),
                address,
                active_connections: 0,
                counts: Counts::default(),
                failure: None,
            })));
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

async fn publish(
    store: sparrow_core::Store,
    source: String,
    bytes: Vec<u8>,
    metrics: &Metrics,
) -> io::Result<bool> {
    let receiver = metrics.snapshot()?.receiver;
    let result = tokio::task::spawn_blocking(move || {
        let now = crate::now()?;
        let artifact = match store.ingest_received_syslog(&source, &receiver, &bytes, now) {
            Ok(artifact) => artifact,
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
                .map(Err);
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
        Ok::<_, sparrow_core::Error>(Ok(snapshot.diagnostic.is_some()))
    })
    .await
    .map_err(io::Error::other)?;
    match result {
        Ok(Ok(failed)) => {
            metrics.update(|status| {
                increment(&mut status.counts.accepted)?;
                if failed {
                    increment(&mut status.counts.interpretation_failures)?;
                }
                Ok(())
            })?;
            Ok(true)
        }
        Ok(Err(durable)) => {
            metrics.update(|status| {
                increment(&mut status.counts.refused)?;
                status.failure = Some(if durable {
                    "packet refused by storage admission".into()
                } else {
                    NOT_DURABLE.into()
                });
                Ok(())
            })?;
            Ok(false)
        }
        Err(error) => Err(io::Error::other(error)),
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
                        let metrics = metrics.clone(); let store = store.clone(); let operations = operations.clone(); let stop = stop.clone(); let definition = definition.clone();
                        tasks.spawn(async move {
                            let _permit = permit;
                            let result = receive_tcp(BufReader::with_capacity(4096, stream), &definition, &metrics, store, operations, stop, lease).await;
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
                    continue;
                };
                publish(
                    store.clone(),
                    definition.source.clone(),
                    buffer[..size].to_vec(),
                    &metrics,
                )
                .await?;
                drop(permit);
            }
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
    definition: &SyslogListener,
    metrics: &Metrics,
    store: sparrow_core::Store,
    operations: Arc<Semaphore>,
    mut stop: watch::Receiver<bool>,
    mut lease: Lease,
) -> io::Result<()> {
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
        let permit = operations
            .clone()
            .acquire_owned()
            .await
            .map_err(io::Error::other)?;
        let accepted = publish(
            store.clone(),
            definition.source.clone(),
            frame.bytes,
            metrics,
        )
        .await?;
        drop(permit);
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
        out.set_accepted_messages(self.counts.accepted);
        out.set_interpretation_failures(self.counts.interpretation_failures);
        out.set_refused_messages(self.counts.refused);
        out.set_truncated_datagrams(self.counts.truncated);
        out.set_framing_failures(self.counts.framing_failures);
        // Arrivals at the ceiling are served by closing the longest-idle connection, never refused.
        out.set_rejected_connections(0);
        out.set_timed_out_connections(self.counts.timed_out);
        out.set_continuity_unknown(true);
        out.set_kernel_drops_unknown(self.definition.transport == SyslogTransport::Udp);
        match &self.failure {
            Some(value) => out.init_failure().set_value(value),
            None => out.init_failure().set_absent(()),
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
        let metrics = Metrics(Arc::new(Mutex::new(Status {
            receiver: sparrow_core::SyslogReceiver::new("listener", "udp").unwrap(),
            address: definition.address,
            definition,
            active_connections: 0,
            counts: Counts::default(),
            failure: None,
        })));
        let published = publish(
            store.clone(),
            "source".into(),
            b"<34>refused".to_vec(),
            &metrics,
        )
        .await
        .unwrap();
        assert!(!published);
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
        let definition = fixture.metrics.snapshot().unwrap().definition;
        let (mut sender, receiver) = tokio::io::duplex(64);
        let (stop, stopped) = watch::channel(false);
        let roster = Arc::new(Roster::default());
        let lease = roster.enroll().unwrap();
        let metrics = fixture.metrics.clone();
        let store = fixture.store.clone();
        let reading = tokio::spawn(async move {
            receive_tcp(
                receiver,
                &definition,
                &metrics,
                store,
                Arc::new(Semaphore::new(4)),
                stopped,
                lease,
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
        let metrics = Metrics(Arc::new(Mutex::new(Status {
            receiver: sparrow_core::SyslogReceiver::new(
                "listener",
                if tcp { "tcp_octet_counting" } else { "udp" },
            )
            .unwrap(),
            address: definition.address,
            definition,
            active_connections: 0,
            counts: Counts::default(),
            failure: None,
        })));
        Listener {
            _dir: dir,
            store,
            metrics,
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
            Arc::new(Semaphore::new(4)),
            stopped,
            stop.clone(),
        ));
        (stop, task)
    }

    /// Value: a sender that stalls inside a frame loses nothing already received: the bytes
    /// read are published as a malformed packet and `timed_out` is counted. Only a stall
    /// inside a frame is a fault, so the stream is closed after it.
    #[tokio::test]
    async fn a_stall_inside_a_frame_publishes_the_bytes_read_and_counts_timed_out() {
        let fixture = listener(SyslogTransport::Tcp, 1, 50);
        let definition = fixture.metrics.snapshot().unwrap().definition;
        let (mut sender, receiver) = tokio::io::duplex(64);
        tokio::io::AsyncWriteExt::write_all(&mut sender, b"20 <34>half")
            .await
            .unwrap();
        let (_stop, stopped) = watch::channel(false);
        let lease = Arc::new(Roster::default()).enroll().unwrap();
        receive_tcp(
            receiver,
            &definition,
            &fixture.metrics,
            fixture.store.clone(),
            Arc::new(Semaphore::new(4)),
            stopped,
            lease,
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
        let definition = fixture.metrics.snapshot().unwrap().definition;
        let (mut sender, receiver) = tokio::io::duplex(64);
        let (_stop, stopped) = watch::channel(false);
        let lease = Arc::new(Roster::default()).enroll().unwrap();
        let metrics = fixture.metrics.clone();
        let store = fixture.store.clone();
        let reading = tokio::spawn(async move {
            receive_tcp(
                receiver,
                &definition,
                &metrics,
                store,
                Arc::new(Semaphore::new(4)),
                stopped,
                lease,
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
