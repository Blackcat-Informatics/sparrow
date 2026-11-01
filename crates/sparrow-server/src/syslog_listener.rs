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
    /// One message per datagram, with explicit observed refusals/truncation.
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
    /// Required per-message payload ceiling, 480..65536.
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
    rejected_connections: u64,
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
        let task = tokio::spawn(async move {
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
        Ok(Self { stop, task })
    }
    /// Detect unexpected worker termination for the daemon health gate.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
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
            let mut tasks = JoinSet::new();
            // Below the free-space floor every write is refused: accept no connections.
            let mut space = tokio::time::interval(std::time::Duration::from_secs(1));
            let mut space_ok = true;
            let serving = async {
            loop {
                if *stop.borrow() { break; }
                tokio::select! {
                    _ = space.tick() => { space_ok = store.space_floor().shortfall().map_err(io::Error::other)?.is_none(); },
                    changed = stop.changed() => { changed.map_err(io::Error::other)?; if *stop.borrow() { break; } },
                    result = tasks.join_next(), if !tasks.is_empty() => { if let Some(result) = result { result.map_err(io::Error::other)??; } },
                    accepted = listener.accept(), if space_ok => {
                        let (stream, _) = accepted?;
                        let Ok(permit) = connections.clone().try_acquire_owned() else { metrics.update(|status| increment(&mut status.counts.rejected_connections))?; continue; };
                        metrics.update(|status| { status.active_connections += 1; Ok(()) })?;
                        let metrics = metrics.clone(); let store = store.clone(); let operations = operations.clone(); let stop = stop.clone(); let definition = definition.clone();
                        tasks.spawn(async move {
                            let _permit = permit;
                            let result = receive_tcp(BufReader::with_capacity(4096, stream), &definition, &metrics, store, operations, stop).await;
                            metrics.update(|status| { status.active_connections = status.active_connections.checked_sub(1).ok_or_else(|| io::Error::other("syslog connection counter underflow"))?; Ok(()) })?;
                            result
                        });
                    }
                }
            }
            Ok(())
            }.await;
            drop(listener);
            finish_tasks(serving, &mut tasks, &shutdown).await
        }
        Socket::Udp(socket) => {
            let mut buffer = vec![0; definition.max_message_bytes + 1];
            loop {
                if *stop.borrow() {
                    break;
                }
                let size = tokio::select! { changed = stop.changed() => { changed.map_err(io::Error::other)?; if *stop.borrow() { break; } continue; }, received = socket.recv_from(&mut buffer) => received?.0 };
                if size > definition.max_message_bytes {
                    discard(
                        &store,
                        &metrics,
                        &operations,
                        sparrow_core::ReceiverDiscard::Truncated,
                    )
                    .await?;
                    continue;
                }
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

async fn receive_tcp<R: AsyncRead + Unpin>(
    mut stream: R,
    definition: &SyslogListener,
    metrics: &Metrics,
    store: sparrow_core::Store,
    operations: Arc<Semaphore>,
    mut stop: watch::Receiver<bool>,
) -> io::Result<()> {
    let framing = definition
        .framing
        .ok_or_else(|| io::Error::other("TCP syslog framing missing"))?;
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        let result = tokio::select! {
            changed = stop.changed() => { changed.map_err(io::Error::other)?; if *stop.borrow() { return Ok(()); } continue; },
            frame = tokio::time::timeout(Duration::from_millis(u64::from(definition.read_timeout_ms)), read_frame(&mut stream, framing, definition.max_message_bytes)) => frame,
        };
        let bytes = match result {
            Ok(Ok(Some(bytes))) => bytes,
            Ok(Ok(None)) => return Ok(()),
            Ok(Err(error)) => {
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
            Err(_) => {
                discard(
                    &store,
                    metrics,
                    &operations,
                    sparrow_core::ReceiverDiscard::TimedOut,
                )
                .await?;
                metrics.update(|status| {
                    status.failure = Some("TCP frame deadline expired; continuity unknown".into());
                    Ok(())
                })?;
                return Ok(());
            }
        };
        let permit = tokio::select! { changed = stop.changed() => { changed.map_err(io::Error::other)?; discard(&store, metrics, &operations, sparrow_core::ReceiverDiscard::Refused).await?; return Ok(()); }, permit = operations.clone().acquire_owned() => permit.map_err(io::Error::other)? };
        let accepted = publish(store.clone(), definition.source.clone(), bytes, metrics).await?;
        drop(permit);
        if !accepted {
            return Ok(());
        }
    }
}

async fn read_frame<R: AsyncRead + Unpin>(
    stream: &mut R,
    framing: SyslogFraming,
    maximum: usize,
) -> io::Result<Option<Vec<u8>>> {
    let mut byte = [0_u8];
    if stream.read(&mut byte).await? == 0 {
        return Ok(None);
    }
    if framing == SyslogFraming::OctetCounting {
        if !(b'1'..=b'9').contains(&byte[0]) {
            return Err(io::Error::other(
                "octet count requires a nonzero first digit",
            ));
        }
        let mut size = usize::from(byte[0] - b'0');
        let mut digits = 1;
        loop {
            stream.read_exact(&mut byte).await?;
            if byte[0] == b' ' {
                break;
            }
            if !byte[0].is_ascii_digit() || digits == 5 {
                return Err(io::Error::other("invalid bounded octet count"));
            }
            size = size * 10 + usize::from(byte[0] - b'0');
            digits += 1;
        }
        if size > maximum {
            return Err(io::Error::other(
                "syslog frame exceeds configured packet admission",
            ));
        }
        let mut body = vec![0; size];
        stream.read_exact(&mut body).await?;
        return Ok(Some(body));
    }
    let mut body = Vec::with_capacity(maximum.min(4096));
    loop {
        body.push(byte[0]);
        if byte[0] == b'\n' && (framing == SyslogFraming::Lf || body.ends_with(b"\r\n")) {
            body.truncate(body.len() - if framing == SyslogFraming::Lf { 1 } else { 2 });
            return Ok(Some(body));
        }
        if body.len() > maximum + usize::from(framing == SyslogFraming::CrLf) {
            return Err(io::Error::other(
                "syslog frame exceeds configured packet admission",
            ));
        }
        stream.read_exact(&mut byte).await?;
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
        out.set_rejected_connections(self.counts.rejected_connections);
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
                    crate::Rpc::new(store.clone(), Arc::new(Semaphore::new(4)), 8388608, 100)
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

    #[tokio::test]
    async fn configured_frames_preserve_octets_delimiters_and_reject_partial_or_oversized_messages()
    {
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
            assert_eq!(
                read_frame(&mut reader, framing, expected.len())
                    .await
                    .unwrap()
                    .unwrap(),
                expected
            );
            assert_eq!(
                read_frame(&mut reader, framing, expected.len())
                    .await
                    .unwrap()
                    .unwrap(),
                expected
            );
            assert!(
                read_frame(&mut reader, framing, expected.len())
                    .await
                    .unwrap()
                    .is_none()
            );
            writing.await.unwrap();
        }
        for (framing, mut input, maximum) in [
            (SyslogFraming::OctetCounting, b"05 <34>x".as_slice(), 5),
            (SyslogFraming::OctetCounting, b"6 <34>x", 6),
            (SyslogFraming::OctetCounting, b"65537 ", 65536),
            (SyslogFraming::OctetCounting, b"123456 ", 65536),
            (SyslogFraming::Lf, b"abc\r\n", 3),
            (SyslogFraming::CrLf, b"abc\n", 10),
        ] {
            assert!(read_frame(&mut input, framing, maximum).await.is_err());
        }
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
