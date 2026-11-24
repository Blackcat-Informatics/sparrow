// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use capnp_rpc::{RpcSystem, rpc_twoparty_capnp, twoparty};
use futures::{AsyncRead, AsyncReadExt};
use std::{
    cell::Cell,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
use tokio::time::Instant;
use tokio_util::compat::TokioAsyncReadCompatExt;

/// Cap'n Proto starts clearing its answer pipelines and capability exports on the
/// first poll of its disconnector. The remaining socket flush can wait on the peer.
/// Start teardown before dropping the driver, even when its task is cancelled.
struct DisconnectOnDrop<F: Future + Unpin>(F);

impl<F: Future + Unpin> Drop for DisconnectOnDrop<F> {
    fn drop(&mut self) {
        let _ = Pin::new(&mut self.0).poll(&mut Context::from_waker(Waker::noop()));
    }
}

/// Serve one owned byte stream through the shared generated capability.
/// Run within a Tokio LocalSet. The caller owns connection admission and its lease.
pub async fn serve_connection<S>(
    stream: S,
    client: crate::sparrow_capnp::sparrow::Client,
) -> capnp::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + 'static,
{
    drive_connection(stream, client, None).await
}

async fn drive_connection<S>(
    stream: S,
    client: crate::sparrow_capnp::sparrow::Client,
    last_read: Option<Rc<Cell<Option<Instant>>>>,
) -> capnp::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + 'static,
{
    let (reader, writer) = stream.compat().split();
    let network = twoparty::VatNetwork::new(
        ProgressReader { reader, last_read },
        writer,
        rpc_twoparty_capnp::Side::Server,
        capnp::message::ReaderOptions {
            traversal_limit_in_words: Some(8 * 1024 * 1024),
            nesting_limit: 64,
        },
    );
    let mut system = RpcSystem::new(Box::new(network), Some(client.client));
    let _disconnect = DisconnectOnDrop(system.get_disconnector());
    (&mut system).await
}

struct ProgressReader<R> {
    reader: R,
    last_read: Option<Rc<Cell<Option<Instant>>>>,
}

impl<R: AsyncRead + Unpin> AsyncRead for ProgressReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.reader).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(bytes)) if bytes != 0)
            && let Some(last_read) = &self.last_read
        {
            last_read.set(Some(Instant::now()));
        }
        result
    }
}

/// Who reached the daemon, which selects the bootstrap capability.
pub enum Access {
    /// The private Unix socket: the complete typed service, or its query-only gate. Its
    /// admin operations record this kernel-reported caller.
    Local(tokio::net::unix::UCred),
    /// Public TCP: the anonymous gate, upgraded by CHAP with this secret; the authenticated
    /// session records this peer address as its administrator.
    PublicTcp(sparrow_client::TcpSecret, String),
    /// The group-accessible ingest socket: producer operations only, each recording this
    /// kernel-reported caller in the provenance of the evidence it creates.
    IngestSocket(sparrow_core::PeerCredential),
}

/// Established connections close after this much read inactivity.
pub(crate) const CONNECTION_IDLE: Duration = Duration::from_secs(60);
/// A public peer must send its first bytes within this much time.
pub(crate) const TCP_FIRST_INPUT: Duration = Duration::from_secs(10);
/// Continuous reads cannot extend a connection past this age.
pub(crate) const CONNECTION_MAX_AGE: Duration = Duration::from_secs(10 * 60);

pub(crate) fn encode_lease_limits(
    mut out: crate::sparrow_capnp::connection_lease_limits::Builder<'_>,
) {
    out.set_idle_seconds(CONNECTION_IDLE.as_secs());
    out.set_tcp_first_input_seconds(TCP_FIRST_INPUT.as_secs());
    out.set_max_age_seconds(CONNECTION_MAX_AGE.as_secs());
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LeaseExpiry {
    Idle,
    MaxAge,
}

/// Public TCP counters are independent of the Unix connection ceiling.
#[derive(Default)]
pub(crate) struct TcpConnections {
    active: AtomicU32,
    idle_expiries: AtomicU64,
    max_age_expiries: AtomicU64,
}

impl TcpConnections {
    pub(crate) fn encode(&self, mut out: crate::sparrow_capnp::tcp_connection_status::Builder<'_>) {
        out.set_active_connections(self.active.load(Ordering::Relaxed));
        out.set_idle_expiries(self.idle_expiries.load(Ordering::Relaxed));
        out.set_max_age_expiries(self.max_age_expiries.load(Ordering::Relaxed));
    }

    fn activate(self: &Arc<Self>) -> ActiveTcp {
        self.active.fetch_add(1, Ordering::Relaxed);
        ActiveTcp(self.clone())
    }

    fn expired(&self, expiry: LeaseExpiry) {
        match expiry {
            LeaseExpiry::Idle => &self.idle_expiries,
            LeaseExpiry::MaxAge => &self.max_age_expiries,
        }
        .fetch_add(1, Ordering::Relaxed);
    }
}

struct ActiveTcp(Arc<TcpConnections>);

impl Drop for ActiveTcp {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::Relaxed);
    }
}

enum Active {
    Bounded(crate::admission::ActiveConnection),
    Tcp(ActiveTcp),
}

impl Active {
    fn expired(&self, expiry: LeaseExpiry) {
        match self {
            Self::Bounded(connection) => {
                connection.ceiling().lease_expired();
                let reason = match expiry {
                    LeaseExpiry::Idle => "idle deadline",
                    LeaseExpiry::MaxAge => "maximum age",
                };
                eprintln!(
                    "sparrowd: {} lease expired at its {reason}; the connection was closed",
                    connection.ceiling().name()
                );
            }
            Self::Tcp(connection) => connection.0.expired(expiry),
        }
    }
}

async fn lease_expiry(
    opened: Instant,
    first_input: Duration,
    last_read: &Cell<Option<Instant>>,
) -> LeaseExpiry {
    let max_age = opened + CONNECTION_MAX_AGE;
    let idle = || {
        last_read
            .get()
            .map_or(opened + first_input, |read| read + CONNECTION_IDLE)
    };
    loop {
        // Reads only extend the idle deadline. An older timer can wake here
        // without closing the stream or requiring a wakeup on every read.
        tokio::time::sleep_until(idle().min(max_age)).await;
        let now = Instant::now();
        if now >= max_age {
            return LeaseExpiry::MaxAge;
        }
        if now >= idle() {
            return LeaseExpiry::Idle;
        }
    }
}

/// Serve one accepted stream under a connection slot taken before it was accepted, and the
/// lease. The slot frees when the connection ends, which includes the lease expiring.
pub fn admit_connection<S>(stream: S, rpc: crate::Rpc, slot: crate::admission::Slot, access: Access)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + 'static,
{
    spawn_connection(stream, rpc, Active::Bounded(slot.activate()), access);
}

pub(crate) fn admit_tcp_connection(
    stream: tokio::net::TcpStream,
    rpc: crate::Rpc,
    secret: sparrow_client::TcpSecret,
    peer: String,
) {
    let connection = rpc.tcp_connections().activate();
    spawn_connection(
        stream,
        rpc,
        Active::Tcp(connection),
        Access::PublicTcp(secret, peer),
    );
}

fn spawn_connection<S>(stream: S, rpc: crate::Rpc, connection: Active, access: Access)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + 'static,
{
    let opened = Instant::now();
    tokio::task::spawn_local(async move {
        let (rpc, _lifetime) = rpc.for_connection();
        let first_input = if matches!(&access, Access::PublicTcp(..)) {
            TCP_FIRST_INPUT
        } else {
            CONNECTION_IDLE
        };
        let client = match access {
            Access::Local(credential) => {
                let uid = credential.uid();
                let pid = credential
                    .pid()
                    .map_or_else(|| "unknown".to_owned(), |pid| pid.to_string());
                rpc.with_producer(crate::source_binding::Caller::PrivateUnix { uid })
                    .with_admin(format!("unix uid={uid} pid={pid}"))
                    .into_client()
            }
            Access::PublicTcp(secret, peer) => {
                capnp_rpc::new_client(crate::ingest_gate::IngestGate::new(
                    rpc.with_producer(crate::source_binding::Caller::AnonymousTcp {}),
                    crate::tcp_gate::AnonymousTcp::new(secret, peer),
                ))
            }
            Access::IngestSocket(credential) => {
                capnp_rpc::new_client(crate::ingest_gate::IngestGate::new(
                    rpc.with_peer(credential),
                    crate::ingest_gate::IngestSocketPolicy,
                ))
            }
        };
        let last_read = Rc::new(Cell::new(None));
        let expiry = tokio::select! {
            biased;
            expiry = lease_expiry(opened, first_input, &last_read) => expiry,
            result = drive_connection(stream, client, Some(last_read.clone())) => {
                if let Err(error) = result {
                    eprintln!("sparrowd: RPC connection failed: {error}");
                }
                return;
            }
        };
        connection.expired(expiry);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparrow_capnp as wire;

    #[tokio::test(flavor = "current_thread")]
    async fn progressing_events_and_import_outlive_the_old_lease_until_maximum_age() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let dir = tempfile::tempdir().unwrap();
                let data = dir.path().join("store");
                let socket = data.join("rpc.sock");
                let config = sparrow_test_support::write_server_config(dir.path(), &data, &socket);
                let store = crate::ServerConfig::read(&config)
                    .unwrap()
                    .open_store()
                    .unwrap();
                let rpc = crate::Rpc::new(store, Arc::new(tokio::sync::Semaphore::new(4)), 100);
                let secret = tempfile::NamedTempFile::new_in(dir.path()).unwrap();
                std::fs::write(secret.path(), [42; 32]).unwrap();
                let secret = sparrow_client::TcpSecret::read(secret.path()).unwrap();
                // The sockets remain real. Only this test advances Tokio's clock, even
                // when both RPC tasks are waiting for loopback I/O.
                let (time_done, hold_clock) = std::sync::mpsc::channel::<()>();
                let (clock_held, held) = tokio::sync::oneshot::channel();
                let clock = tokio::task::spawn_blocking(move || {
                    clock_held.send(()).unwrap();
                    let _ = hold_clock.recv();
                });
                held.await.unwrap();
                tokio::time::pause();
                let observed = Instant::now();
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let endpoint = sparrow_client::Endpoint::Tcp(listener.local_addr().unwrap());
                let connections = rpc.tcp_connections().clone();
                let accept = tokio::task::spawn_local(async move {
                    for _ in 0..2 {
                        let (stream, peer) = listener.accept().await.unwrap();
                        admit_tcp_connection(stream, rpc.clone(), secret.clone(), peer.to_string());
                    }
                });
                let client = sparrow_client::Client::connect_producer(&endpoint)
                    .await
                    .unwrap();
                let idle = sparrow_client::Client::connect_producer(&endpoint)
                    .await
                    .unwrap();
                accept.await.unwrap();
                let event = |sequence| sparrow_client::EventBatch {
                    request_id: format!("lease-event-{sequence}"),
                    source: "lease.events".into(),
                    client_instance: [5; 16],
                    events: vec![sparrow_client::ProducerEvent {
                        sequence,
                        event_time: 1_000,
                        kind: wire::EventKind::Log,
                        level: wire::EventLevel::Info,
                        name: "lease".into(),
                        target: "application".into(),
                        fields: vec![],
                        context: vec![],
                        span: None,
                        follows_from: None,
                    }],
                };
                assert_eq!(client.events(&event(1)).await.unwrap().record_ids.len(), 1);

                let chunk_bytes = sparrow_core::IMPORT_CHUNK_BYTES;
                let input = vec![b'a'; chunk_bytes + 1];
                let path = dir.path().join("input.bin");
                std::fs::write(&path, &input).unwrap();
                let payload = format!(
                    "blake3:{}",
                    purrdf_hash::hex::encode(purrdf_hash::blake3::hash(&input).as_bytes())
                );
                tokio::time::advance(Duration::from_secs(30)).await;
                let mut begin = client.capability().import_begin_request();
                let mut request = begin.get().init_request();
                request.set_request_id("lease-import");
                request.set_source("lease.import");
                request.set_format(wire::Format::Opaque);
                request.set_byte_length(input.len() as u64);
                request.set_payload_id(&payload);
                let response = begin.send().promise.await.unwrap();
                let wire::result::Ok(import) = response
                    .get()
                    .unwrap()
                    .get_result()
                    .unwrap()
                    .which()
                    .unwrap()
                else {
                    panic!("begin import was refused");
                };
                let wire::import_start::Staging(staged) = import.unwrap().which().unwrap() else {
                    panic!("import already published");
                };
                let mut append = client.capability().import_append_request();
                let mut first = append.get().init_request();
                let mut reference = first.reborrow().init_import();
                reference.set_store_id(&client.discovery().store_id);
                reference.set_id(staged.unwrap().get_id().unwrap());
                first.set_bytes(&input[..chunk_bytes]);
                let response = append.send().promise.await.unwrap();
                let wire::result::Ok(progress) = response
                    .get()
                    .unwrap()
                    .get_result()
                    .unwrap()
                    .which()
                    .unwrap()
                else {
                    panic!("first import chunk was refused");
                };
                assert_eq!(progress.unwrap().get_received_bytes(), chunk_bytes as u64);

                tokio::time::advance(Duration::from_secs(35)).await;
                assert_eq!(client.events(&event(2)).await.unwrap().record_ids.len(), 1);
                let receipt = client
                    .import_file("lease-import", "lease.import", wire::Format::Opaque, &path)
                    .await
                    .unwrap();
                assert_eq!(receipt.payload_id, payload);
                assert_eq!(receipt.byte_length, input.len() as u64);
                assert!(
                    idle.capability()
                        .discover_request()
                        .send()
                        .promise
                        .await
                        .is_err()
                );
                assert_eq!(connections.idle_expiries.load(Ordering::Relaxed), 1);

                let interval = Duration::from_secs(40);
                while observed.elapsed() + interval < CONNECTION_MAX_AGE {
                    tokio::time::advance(interval).await;
                    let reply = client
                        .capability()
                        .discover_request()
                        .send()
                        .promise
                        .await
                        .unwrap();
                    assert!(matches!(
                        reply.get().unwrap().get_result().unwrap().which().unwrap(),
                        wire::result::Ok(_)
                    ));
                }
                tokio::time::advance(
                    CONNECTION_MAX_AGE.saturating_sub(observed.elapsed())
                        + Duration::from_millis(1),
                )
                .await;
                assert!(
                    client
                        .capability()
                        .discover_request()
                        .send()
                        .promise
                        .await
                        .is_err()
                );
                assert_eq!(connections.active.load(Ordering::Relaxed), 0);
                assert_eq!(connections.idle_expiries.load(Ordering::Relaxed), 1);
                assert_eq!(connections.max_age_expiries.load(Ordering::Relaxed), 1);
                drop(time_done);
                clock.await.unwrap();
            })
            .await;
    }
}
