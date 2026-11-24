// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The daemon's accept loop over the private socket, the ingest socket and public TCP.
//!
//! The free-space floor never stops it from accepting (#257). Below the floor every write is
//! refused by the store with the floor-labelled retryable `STORAGE_PRESSURE`, so a connection
//! accepted then costs one answered request, where a connection left in the kernel backlog cost
//! a silent client timeout. Unix connection slots are taken before accept. Public TCP accepts
//! independently, and the transport enforces the connection deadlines on all listeners.

use crate::{
    Access, Rpc, admission::Ceiling, floor_pause::PauseJournal, ingest_socket::IngestSocket,
    local_endpoint::LocalEndpoint,
};
use std::{error::Error, future::Future, sync::Arc, time::Duration};

/// How often the daemon observes the free-space floor. The maintenance worker observes it at
/// the same cadence; this loop is the observer that journals the pause, because the end of a
/// pause caused by another process freeing space has no event to wait on.
pub const SPACE_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// How the loop answers accept errors (#445). An accept error (descriptor exhaustion, a
/// connection aborted before accept, a transient kernel refusal) belongs to no daemon state:
/// the loop counts it, stops accepting for a backoff and tries again, so it never ends the
/// daemon. The wait doubles from 10 ms to a 1 s ceiling, and starts over at the next
/// success. A descriptor shortage clears when a served connection closes, which a 1 s ceiling
/// notices promptly, and a persistent fault costs one attempt and one journal line a second.
#[derive(Default)]
struct AcceptFaults {
    consecutive: u32,
    total: u64,
    resume: Option<tokio::time::Instant>,
}

impl AcceptFaults {
    async fn next<T>(
        &self,
        accept: impl Future<Output = std::io::Result<T>>,
    ) -> std::io::Result<T> {
        if let Some(resume) = self.resume {
            tokio::time::sleep_until(resume).await;
        }
        accepted(accept).await
    }

    fn observe<T>(
        &mut self,
        incoming: std::io::Result<T>,
        journal: &mut impl FnMut(&str),
    ) -> Option<T> {
        match incoming {
            Ok(stream) => {
                self.succeeded();
                Some(stream)
            }
            Err(error) => {
                let (wait, line) = self.failed(&error);
                journal(&line);
                self.resume = Some(tokio::time::Instant::now() + wait);
                None
            }
        }
    }

    /// Count one accept error and return how long accepting pauses and the journal line.
    fn failed(&mut self, error: &std::io::Error) -> (Duration, String) {
        self.consecutive = self.consecutive.saturating_add(1);
        self.total = self.total.saturating_add(1);
        let wait = Duration::from_millis(10)
            .saturating_mul(1 << self.consecutive.saturating_sub(1).min(7))
            .min(Duration::from_secs(1));
        (
            wait,
            format!(
                "sparrowd: accept failed: {error}; {} accept error(s) so far; accepting again in {} ms",
                self.total,
                wait.as_millis()
            ),
        )
    }

    fn succeeded(&mut self) {
        self.consecutive = 0;
        self.resume = None;
    }
}

/// Await one accept; under test a counted number of accepts fail first, as a descriptor
/// shortage does.
async fn accepted<T>(accept: impl Future<Output = std::io::Result<T>>) -> std::io::Result<T> {
    #[cfg(test)]
    if let Some(error) = tests::injected_accept_error() {
        return Err(error);
    }
    accept.await
}

/// The listeners the loop accepts on.
pub struct Sockets<'a> {
    /// The private Unix socket.
    pub private: &'a LocalEndpoint,
    /// The group-accessible ingest socket and its counters, when configured.
    pub ingest: Option<(&'a LocalEndpoint, &'a IngestSocket)>,
    /// Public TCP and the secret its sessions authenticate with, when configured.
    pub tcp: Option<(&'a tokio::net::TcpListener, sparrow_client::TcpSecret)>,
}

enum Incoming {
    Private(tokio::net::UnixStream),
    Ingest(tokio::net::UnixStream),
}

impl Sockets<'_> {
    async fn accept_unix(&self, rpc: bool, ingest: bool) -> std::io::Result<Incoming> {
        tokio::select! {
            ready = self.private.accept(), if rpc => {
                ready.map(|(stream, _)| Incoming::Private(stream))
            }
            ready = async {
                match self.ingest {
                    Some((endpoint, _)) => endpoint.accept().await,
                    None => std::future::pending().await,
                }
            }, if ingest => ready.map(|(stream, _)| Incoming::Ingest(stream)),
        }
    }

    async fn accept_tcp(
        &self,
    ) -> std::io::Result<(
        tokio::net::TcpStream,
        std::net::SocketAddr,
        sparrow_client::TcpSecret,
    )> {
        match &self.tcp {
            Some((listener, secret)) => listener
                .accept()
                .await
                .map(|(stream, peer)| (stream, peer, secret.clone())),
            None => std::future::pending().await,
        }
    }

    /// Keep the listeners responsive during a session handoff. Dropping each accepted
    /// stream gives the peer a retryable transport disconnect without an old capability.
    /// Unix names stay under store ownership and are restored by the live session only.
    pub async fn reject_while<T>(
        &self,
        work: impl Future<Output = T>,
        mut journal: impl FnMut(&str),
    ) -> T {
        tokio::pin!(work);
        let mut faults = AcceptFaults::default();
        let mut tcp_faults = AcceptFaults::default();
        loop {
            tokio::select! {
                biased;
                completed = &mut work => return completed,
                incoming = faults.next(self.accept_unix(true, self.ingest.is_some())) => {
                    drop(faults.observe(incoming, &mut journal));
                }
                incoming = tcp_faults.next(self.accept_tcp()), if self.tcp.is_some() => {
                    drop(tcp_faults.observe(incoming, &mut journal));
                }
            }
        }
    }
}

/// Accept connections until `interrupt` resolves (an orderly stop, `Ok`) or `health` resolves
/// (a worker ended; its failure is returned). `journal` receives each pause line.
///
/// Take a Unix connection slot before accepting, with a separate ceiling for each socket.
/// Additional Unix peers wait in the kernel listen backlog. Public TCP has independent
/// acceptance and accept-error backoff.
///
/// An accept error, a failed floor measurement or a clock failure does not end the loop: it is
/// counted, journaled and retried (`AcceptFaults`).
///
/// # Errors
/// The interrupt's failure, or the failure `health` reports.
pub async fn serve<T>(
    sockets: &Sockets<'_>,
    rpc: &Rpc,
    connections: &Arc<Ceiling>,
    interrupt: impl Future<Output = Result<(), Box<dyn Error>>>,
    health: impl Future<Output = T>,
    mut journal: impl FnMut(&str),
) -> Result<Option<T>, Box<dyn Error>> {
    tokio::pin!(interrupt);
    tokio::pin!(health);
    let floor = rpc.store_floor();
    let mut space = tokio::time::interval(SPACE_CHECK_INTERVAL);
    let mut pause_journal = PauseJournal::default();
    let ingest = sockets.ingest;
    let mut rpc_slot = None;
    let mut rpc_wait = None;
    let mut ingest_slot = None;
    let mut ingest_wait = None;
    let mut faults = AcceptFaults::default();
    let mut tcp_faults = AcceptFaults::default();
    let mut measuring = None::<String>;
    loop {
        if rpc_slot.is_none() && rpc_wait.is_none() {
            match connections.try_slot() {
                Ok(slot) => rpc_slot = Some(slot),
                Err(wait) => rpc_wait = Some(wait),
            }
        }
        if let Some((_, socket)) = ingest.filter(|_| ingest_slot.is_none() && ingest_wait.is_none())
        {
            match socket.ceiling().try_slot() {
                Ok(slot) => ingest_slot = Some(slot),
                Err(wait) => ingest_wait = Some(wait),
            }
        }
        tokio::select! {
            slot = connections.acquire(), if rpc_slot.is_none() => {
                rpc_slot = Some(slot);
                rpc_wait = None;
            }
            slot = async { ingest.expect("enabled ingest branch").1.ceiling().acquire().await }, if ingest.is_some() && ingest_slot.is_none() => {
                ingest_slot = Some(slot);
                ingest_wait = None;
            }
            stopped = &mut interrupt => {
                stopped?;
                return Ok(None);
            }
            ended = &mut health => return Ok(Some(ended)),
            _ = space.tick() => {
                match crate::now().and_then(|now| Ok((now, floor.observe(now)?))) {
                    Ok((now, shortfall)) => {
                        measuring = None;
                        if let Some(line) = pause_journal.observe(shortfall.as_deref(), now, rpc.floor_pause()) {
                            journal(&line);
                        }
                    }
                    // The measurement is retried at the next tick; the text is journaled once per change.
                    Err(error) => {
                        let text = error.to_string();
                        if measuring.as_deref() != Some(text.as_str()) {
                            journal(&format!("sparrowd: free-space measurement failed: {text}; retrying every second"));
                            measuring = Some(text);
                        }
                    }
                }
                // A socket name removed while the daemon runs is bound again (#447).
                for endpoint in std::iter::once(sockets.private).chain(ingest.map(|(endpoint, _)| endpoint)) {
                    if let Some(line) = endpoint.restore_name().await {
                        journal(&line);
                    }
                }
            }
            incoming = faults.next(sockets.accept_unix(rpc_slot.is_some(), ingest_slot.is_some())),
                if rpc_slot.is_some() || ingest_slot.is_some() => {
                match faults.observe(incoming, &mut journal) {
                    Some(Incoming::Private(stream)) => {
                        let Some(slot) = rpc_slot.take() else { continue };
                        rpc.floor_pause().connection_accepted();
                        // A connection whose peer the kernel cannot name is dropped unanswered.
                        match stream.peer_cred() {
                            Ok(credential) => crate::admit_connection(stream, rpc.clone(), slot, Access::Local(credential)),
                            Err(error) => eprintln!("sparrowd: private socket peer unreadable: {error}"),
                        }
                    }
                    Some(Incoming::Ingest(stream)) => {
                        let (Some(slot), Some((_, socket))) = (ingest_slot.take(), ingest) else { continue };
                        rpc.floor_pause().connection_accepted();
                        let _ = socket.admit(stream, rpc.clone(), slot);
                    }
                    None => {}
                }
            }
            incoming = tcp_faults.next(sockets.accept_tcp()), if sockets.tcp.is_some() => {
                let Some((stream, peer, secret)) = tcp_faults.observe(incoming, &mut journal) else {
                    continue;
                };
                rpc.floor_pause().connection_accepted();
                // A peer that reset before this call is dropped without ending the daemon.
                if let Err(error) = stream.set_nodelay(true) {
                    eprintln!("sparrowd: TCP peer {peer} dropped before it was served: {error}");
                    continue;
                }
                crate::transport::admit_tcp_connection(stream, rpc.clone(), secret, peer.to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparrow_capnp as wire;
    use std::{cell::RefCell, rc::Rc};
    use tokio::sync::{Semaphore, oneshot};

    thread_local! {
        /// Accept errors the next accepts return, for the test thread's loop.
        static INJECTED: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    }

    /// The next injected accept error, a descriptor shortage (EMFILE), if any remain.
    pub(super) fn injected_accept_error() -> Option<std::io::Error> {
        INJECTED.with(|left| {
            let remaining = left.get();
            (remaining > 0).then(|| {
                left.set(remaining - 1);
                std::io::Error::from_raw_os_error(24)
            })
        })
    }

    /// A store, its RPC, the private socket's path and the stop for the loop serving it.
    struct Fixture {
        _dir: tempfile::TempDir,
        socket: std::path::PathBuf,
        rpc: Rpc,
        floor: sparrow_core::SpaceFloor,
        store: sparrow_core::Store,
        stop: Option<oneshot::Sender<()>>,
        served: tokio::task::JoinHandle<Result<(), Box<dyn Error>>>,
        lines: Rc<RefCell<Vec<String>>>,
    }

    /// Start the accept loop over a fresh store on the current `LocalSet`.
    async fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let socket = data.join("rpc.sock");
        let config = sparrow_test_support::write_server_config(dir.path(), &data, &socket);
        let store = crate::ServerConfig::read(&config)
            .unwrap()
            .open_store()
            .unwrap();
        let connections = Ceiling::new("RPC connection ceiling", 8);
        let rpc = Rpc::new(store.clone(), Arc::new(Semaphore::new(8)), 100)
            .with_connections(connections.clone());
        let listener = LocalEndpoint::bind(&socket, &store).await.unwrap();
        let (stop, stopped) = oneshot::channel::<()>();
        let lines = Rc::new(RefCell::new(Vec::new()));
        let served = tokio::task::spawn_local({
            let (rpc, lines) = (rpc.clone(), lines.clone());
            async move {
                serve(
                    &Sockets {
                        private: &listener,
                        ingest: None,
                        tcp: None,
                    },
                    &rpc,
                    &connections,
                    async {
                        stopped
                            .await
                            .map_err(|error| Box::new(error) as Box<dyn Error>)
                    },
                    std::future::pending::<()>(),
                    |line| lines.borrow_mut().push(line.to_owned()),
                )
                .await
                .map(drop)
            }
        });
        Fixture {
            floor: store.space_floor().clone(),
            store,
            _dir: dir,
            socket,
            rpc,
            stop: Some(stop),
            served,
            lines,
        }
    }

    impl Fixture {
        /// Pause the floor the way a nearly full volume does, and let the daemon observe it.
        fn pause(&self) {
            self.floor.set_free_for_tests(Some(0));
            assert!(self.floor.observe(1_000).unwrap().is_some());
        }

        fn resume(&self) {
            self.floor.set_free_for_tests(None);
            assert!(self.floor.observe(2_000).unwrap().is_none());
        }

        /// The pause counters as `status` carries them on the wire.
        async fn wire_counts(
            &self,
            client: &sparrow_client::Client,
        ) -> crate::floor_pause::FloorPauseCounts {
            let response = client
                .capability()
                .status_request()
                .send()
                .promise
                .await
                .unwrap();
            let wire::result::Ok(status) = response
                .get()
                .unwrap()
                .get_result()
                .unwrap()
                .which()
                .unwrap()
            else {
                panic!("status was refused");
            };
            let pause = status.unwrap().get_floor_pause().unwrap();
            crate::floor_pause::FloorPauseCounts {
                paused_since: pause.get_paused_since(),
                connections_accepted: pause.get_connections_accepted(),
                writes_refused: pause.get_writes_refused(),
                reads_served: pause.get_reads_served(),
                read_staging_refused: pause.get_read_staging_refused(),
            }
        }
    }

    async fn on_local_set<F: Future<Output = ()>>(body: F) {
        tokio::task::LocalSet::new().run_until(body).await;
    }

    /// [`on_local_set`] with a hang guard for tests on the real clock: a daemon that stopped
    /// answering fails the test instead of hanging the gate. It is not a wait the test relies on.
    async fn guarded<F: Future<Output = ()>>(body: F) {
        on_local_set(async {
            tokio::time::timeout(Duration::from_secs(60), body)
                .await
                .expect("the daemon stopped answering while the floor was paused");
        })
        .await;
    }

    fn floor_labelled(error: &sparrow_client::Error) -> bool {
        matches!(
            error,
            sparrow_client::Error::Domain { code, retryable: true, .. } if code == "STORAGE_PRESSURE"
        ) && error.is_free_space_floor()
    }

    /// Value (#257): below the floor the daemon accepted nothing, so a producer met a silent
    /// client timeout and no reader could even connect. Protects, as one episode on the real
    /// accept loop, store and wire: a connection made while the floor is paused is accepted and
    /// counted; reads (discovery, an artifact lookup, status) are served and counted; a write
    /// gets the retryable STORAGE_PRESSURE carrying the floor class, and is counted; status
    /// reports those counters on the wire; and when space returns the same connection writes
    /// normally. Fails on the old accept pause: the connect never completes discovery.
    #[tokio::test(flavor = "current_thread")]
    async fn a_paused_floor_accepts_serves_reads_and_refuses_writes_with_the_floor_label() {
        guarded(async {
            let mut fixture = fixture().await;
            // Stored directly and paused before the loop first runs, so the loop sees the pause
            // from its first turn, as it does a pause that began long before a connection.
            let stored = fixture
                .store
                .ingest(
                    "before-pause",
                    "source",
                    "log",
                    b"kept\n",
                    crate::now().unwrap(),
                )
                .unwrap();
            fixture.pause();
            // The loop's first observation journals the pause: from here it is in force there.
            reached(&fixture, 1).await;

            let client = sparrow_client::Client::connect(&fixture.socket)
                .await
                .unwrap();
            assert!(
                client.artifact(&stored.id).await.is_ok(),
                "a read was refused"
            );
            let refused = client
                .ingest("during-pause", "source", wire::Format::Log, b"held\n")
                .await
                .unwrap_err();
            assert!(floor_labelled(&refused), "{refused}");
            assert!(refused.retryable());

            let counts = fixture.wire_counts(&client).await;
            assert_eq!(counts.connections_accepted, 1, "{counts:?}");
            assert_eq!(counts.writes_refused, 1, "{counts:?}");
            // Discovery, the artifact lookup and this status read.
            assert_eq!(counts.reads_served, 3, "{counts:?}");
            assert_eq!(counts.paused_since, 1_000);

            fixture.resume();
            let receipt = client
                .ingest("during-pause", "source", wire::Format::Log, b"held\n")
                .await
                .unwrap();
            assert_eq!(receipt.store_id, fixture.store.store_id().unwrap());
            assert_eq!(fixture.rpc.floor_pause().counts().writes_refused, 1);

            fixture.stop.take().unwrap().send(()).unwrap();
            fixture.served.await.unwrap().unwrap();
        })
        .await;
    }

    /// Value (#257): an orderly stop must still end the loop while paused. Protects: the
    /// interrupt returns the loop with the connections it accepted open and the floor paused.
    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_while_paused_ends_the_accept_loop_with_connections_open() {
        guarded(async {
            let mut fixture = fixture().await;
            fixture.pause();
            // The loop's first observation journals the pause: from here it is in force there.
            reached(&fixture, 1).await;
            let client = sparrow_client::Client::connect(&fixture.socket)
                .await
                .unwrap();
            fixture.stop.take().unwrap().send(()).unwrap();
            fixture.served.await.unwrap().unwrap();
            assert!(fixture.floor.paused());
            drop(client);
        })
        .await;
    }

    /// Value (#257): a pause must leave a journal trace at both ends. Protects: the loop's
    /// floor observer tick journals the line when a pause begins and the line when it ends,
    /// each carrying the counters, using no timer beyond the tick itself.
    #[tokio::test(start_paused = true)]
    async fn the_observer_tick_journals_the_start_and_end_of_a_pause() {
        on_local_set(async {
            let mut fixture = fixture().await;
            // The loop's first tick is immediate and sees no pause.
            for _ in 0..20 {
                tokio::task::yield_now().await;
            }
            assert!(fixture.lines.borrow().is_empty());
            fixture.floor.set_free_for_tests(Some(0));
            tokio::time::advance(SPACE_CHECK_INTERVAL).await;
            reached(&fixture, 1).await;
            assert!(fixture.lines.borrow()[0].contains("still accepting connections"));
            assert!(fixture.floor.paused());
            fixture.floor.set_free_for_tests(None);
            tokio::time::advance(SPACE_CHECK_INTERVAL).await;
            reached(&fixture, 2).await;
            assert!(fixture.lines.borrow()[1].contains("free space restored"));
            fixture.stop.take().unwrap().send(()).unwrap();
            fixture.served.await.unwrap().unwrap();
        })
        .await;
    }

    /// Value (#445): a descriptor shortage on accept ended the daemon (`accepted?`). Protects, on
    /// the real accept loop: three accept errors are journaled with their running count, the
    /// loop keeps running, and the next client is accepted and served once accepting resumes.
    /// Fails on the old loop, which returns the first error.
    #[tokio::test(start_paused = true)]
    async fn accept_errors_are_counted_backed_off_and_do_not_end_the_loop() {
        on_local_set(async {
            INJECTED.with(|left| left.set(3));
            let mut fixture = fixture().await;
            let client = sparrow_client::Client::connect(&fixture.socket)
                .await
                .unwrap();
            assert!(client.artifact("absent").await.is_err());
            assert!(!fixture.served.is_finished(), "the loop ended");
            let lines = fixture.lines.borrow().clone();
            let failed: Vec<_> = lines
                .iter()
                .filter(|l| l.contains("accept failed"))
                .collect();
            assert_eq!(failed.len(), 3, "{lines:?}");
            assert!(failed[2].contains("3 accept error(s) so far"), "{failed:?}");
            fixture.stop.take().unwrap().send(()).unwrap();
            fixture.served.await.unwrap().unwrap();
        })
        .await;
    }

    /// Value (#447): a removed socket name left the daemon healthy by every internal check
    /// while no local client could connect, until a restart. Protects, on the real accept loop:
    /// the loop's one-second observation binds the removed name again and journals it, and a
    /// client then connects and is served. Fails on the loop that never rebinds: no line is
    /// journaled and the connect finds no socket.
    #[tokio::test(flavor = "current_thread")]
    async fn a_removed_socket_name_is_bound_again_by_the_accept_loop() {
        guarded(async {
            let mut fixture = fixture().await;
            std::fs::remove_file(&fixture.socket).unwrap();
            reached(&fixture, 1).await;
            assert!(
                fixture.lines.borrow()[0].contains("was removed; bound it again"),
                "{:?}",
                fixture.lines.borrow()
            );
            let client = sparrow_client::Client::connect(&fixture.socket)
                .await
                .unwrap();
            fixture.wire_counts(&client).await;
            fixture.stop.take().unwrap().send(()).unwrap();
            fixture.served.await.unwrap().unwrap();
        })
        .await;
    }

    /// Yield until the journal holds `count` lines, or fail after a wall-clock bound.
    async fn reached(fixture: &Fixture, count: usize) {
        let began = std::time::Instant::now();
        while fixture.lines.borrow().len() < count {
            assert!(
                began.elapsed() < Duration::from_secs(30),
                "no journal line {count}"
            );
            tokio::task::yield_now().await;
        }
    }

    /// Value (#257): the producer must learn it is the floor and hold, not time out silently,
    /// and must never drop or quarantine the batch for it. Protects, through the real Rust
    /// client delivery worker against the paused daemon: a floor-labelled refusal is counted in
    /// the client's status as a floor refusal, the batch stays held unchanged, and it is
    /// delivered durably once space returns, with nothing quarantined. Fails if the client
    /// ignores the label (no floor refusal is counted) or treats the refusal as permanent.
    #[tokio::test(flavor = "current_thread")]
    async fn a_producer_holds_floor_refusals_and_delivers_when_space_returns() {
        guarded(async {
            let mut fixture = fixture().await;
            fixture.pause();
            // The loop's first observation journals the pause: from here it is in force there.
            reached(&fixture, 1).await;
            let mut config = sparrow_client::DeliveryConfig::new(&fixture.socket, "floor-hold")
                .with_quarantine_dir(fixture._dir.path().join("quarantine"));
            config.batch_events = 1;
            let config = config.with_test_timing(
                Duration::from_secs(2),
                Duration::from_millis(20),
                Duration::from_millis(20),
            );
            let delivery = sparrow_client::Delivery::start(config).unwrap();
            let queued = delivery.try_send(sparrow_client::ProducerEvent {
                sequence: 0,
                event_time: 1000,
                kind: wire::EventKind::Log,
                level: wire::EventLevel::Info,
                name: "delivery".into(),
                target: "application".into(),
                fields: vec![sparrow_client::EventField {
                    name: "message".into(),
                    value: sparrow_client::FieldValue::Text("held by the floor".into()),
                }],
                context: vec![],
                span: None,
                follows_from: None,
            });
            assert!(
                matches!(queued, sparrow_client::Admission::Queued { .. }),
                "{queued:?}"
            );
            // The daemon answered the write with the floor label (the event to wait for).
            let began = std::time::Instant::now();
            while fixture.rpc.floor_pause().counts().writes_refused == 0 {
                assert!(
                    began.elapsed() < Duration::from_secs(30),
                    "no write reached the daemon"
                );
                tokio::task::yield_now().await;
            }
            fixture.resume();
            let handle = delivery.clone();
            let watermark =
                tokio::task::spawn_blocking(move || handle.flush(Duration::from_secs(30)))
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(watermark.durable_events, 1);
            let stats = delivery.stats().unwrap();
            assert!(stats.floor_refusals >= 1, "{stats:?}");
            assert!(stats.retries >= stats.floor_refusals);
            assert_eq!(stats.quarantined_events, 0);
            assert!(stats.dropped_by_reason.iter().all(|&count| count == 0));
            assert!(stats.terminal_failure.is_none());
            fixture.stop.take().unwrap().send(()).unwrap();
            fixture.served.await.unwrap().unwrap();
        })
        .await;
    }
}
