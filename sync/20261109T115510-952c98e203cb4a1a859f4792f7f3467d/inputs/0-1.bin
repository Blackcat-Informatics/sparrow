// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One database owner, serving typed Cap'n Proto RPC over Unix and configured TCP sockets.

use sparrow_server::{Rpc, ServerConfig};
use std::{path::Path, sync::Arc};
use tokio::sync::Semaphore;

mod local_endpoint;
mod maintenance;
mod shutdown;

/// How often the daemon compares filesystem free space with its floor.
const SPACE_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
/// Also the contract `deploy/verify-backup.sh` reads to tell an image with query-only mode
/// and, for its migration measurement, an image with `--no-intake`.
const USAGE: &str = "usage: sparrowd --config FILE.toml [--query-only | --no-intake]";

/// How often the serving loop checks that every worker it started is still running.
const WORKER_HEALTH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// Watch the workers the daemon started and return the failure of the first that ends.
/// Every worker runs until shutdown, so any end before it is a failure the daemon exits
/// on. Every mode is covered: a normal daemon has all three workers, a no-intake daemon only
/// maintenance, a query-only one none, so this never returns there.
async fn worker_health(
    consumer: &mut Option<sparrow_server::watches::ConsumeWorker>,
    syslog: &mut Option<sparrow_server::syslog_listener::SyslogWorker>,
    maintenance: Option<&maintenance::Worker>,
) -> Box<dyn std::error::Error> {
    let mut tick = tokio::time::interval(WORKER_HEALTH_INTERVAL);
    loop {
        tick.tick().await;
        if consumer
            .as_ref()
            .is_some_and(sparrow_server::watches::ConsumeWorker::is_finished)
        {
            if let Some(worker) = consumer.take()
                && let Err(error) = worker.shutdown().await
            {
                return error;
            }
            return "completed-consumption worker terminated unexpectedly".into();
        }
        if syslog
            .as_ref()
            .is_some_and(sparrow_server::syslog_listener::SyslogWorker::is_finished)
        {
            if let Some(worker) = syslog.take()
                && let Err(error) = worker.shutdown().await
            {
                return error.into();
            }
            return "syslog intake worker terminated unexpectedly".into();
        }
        if maintenance.is_some_and(maintenance::Worker::is_finished) {
            return "maintenance worker terminated unexpectedly".into();
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let (path, mode) = match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["--config", path] => (path, None),
        ["--config", path, mode @ ("--query-only" | "--no-intake")]
        | [mode @ ("--query-only" | "--no-intake"), "--config", path] => (path, Some(mode)),
        _ => return Err(USAGE.into()),
    };
    let mut config = ServerConfig::read(Path::new(path))?;
    // A no-intake daemon writes and runs maintenance but, like a query-only one, opens no
    // stored watch root, so a copy of a store never consumes the intake its catalog names.
    let intake = mode != Some("--no-intake");
    match mode {
        Some("--query-only") => config = config.into_query_only()?,
        Some(_) => config.check_no_intake()?,
        None => {}
    }
    let tcp_secret = config.tcp_secret()?;
    // Keep the subscription alive through startup and awaited maintenance work.
    // Recreating ctrl_c() inside select drops its listener when another branch wins.
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    // Refuse an unavailable public port before opening/migrating the store or Unix endpoint.
    let tcp_listener = match config.rpc_tcp_address {
        Some(address) => Some(tokio::net::TcpListener::bind(address).await?),
        None => None,
    };
    if let Some(listener) = &tcp_listener {
        eprintln!("sparrowd: capnp_tcp={}", listener.local_addr()?);
    }
    // Opening writes a manifest and WAL; on a full filesystem it would fail and
    // spend systemd's bounded restarts. Wait for space instead.
    let floor = config.space_floor();
    if let Some(shortfall) = floor.shortfall()? {
        eprintln!("sparrowd: waiting to open the store: {shortfall}");
        let mut space = tokio::time::interval(SPACE_CHECK_INTERVAL);
        loop {
            tokio::select! {
                signal = interrupt.recv() => {
                    signal.ok_or("daemon interrupt subscription terminated unexpectedly")?;
                    return Ok(());
                }
                _ = space.tick() => {
                    if floor.shortfall()?.is_none() {
                        break;
                    }
                }
            }
        }
    }
    let store = config.open_store()?;
    let phases = store.open_phases();
    eprintln!(
        "sparrowd: store_open_ms {} total={}",
        phases
            .iter()
            .map(|phase| format!("{}={}", phase.name, phase.elapsed.as_millis()))
            .collect::<Vec<_>>()
            .join(" "),
        phases
            .iter()
            .map(|phase| phase.elapsed)
            .sum::<std::time::Duration>()
            .as_millis()
    );
    let store_id = store.store_id()?;
    let operations = Arc::new(Semaphore::new(config.max_concurrent_requests));
    let operation_slots = u32::try_from(config.max_concurrent_requests)?;
    // A query-only or no-intake daemon owns no watch root: it reports the stored catalog and
    // claims nothing.
    let (watches, mut consumer) = if config.query_only || !intake {
        (
            sparrow_server::watches::WatchRegistry::stored(&store)?,
            None,
        )
    } else {
        let watches = sparrow_server::watches::WatchRegistry::default();
        let consumer = sparrow_server::watches::ConsumeWorker::start(
            store.clone(),
            &config.consume,
            config.socket_path.clone(),
            watches.clone(),
            operations.clone(),
        )?;
        (watches, Some(consumer))
    };
    // Validate the catalog first; recover only a refused socket under exclusive store ownership.
    let endpoint = match local_endpoint::LocalEndpoint::bind(&config.socket_path, &store).await {
        Ok(endpoint) => endpoint,
        Err(error) => {
            return shutdown::finish(
                Err(error.into()),
                shutdown::Workers {
                    consumer: consumer.take(),
                    ..Default::default()
                },
                &store,
                &[],
                &operations,
                operation_slots,
            )
            .await;
        }
    };
    let ingest = match (&config.ingest_socket_path, config.ingest_socket_gid) {
        (Some(path), Some(gid)) => {
            match local_endpoint::LocalEndpoint::bind_ingest(path, gid).await {
                Ok(endpoint) => Some((
                    endpoint,
                    sparrow_server::ingest_socket::IngestSocket::new(
                        path.clone(),
                        gid,
                        config.max_ingest_connections,
                    ),
                )),
                Err(error) => {
                    return shutdown::finish(
                        Err(error.into()),
                        shutdown::Workers {
                            consumer: consumer.take(),
                            ..Default::default()
                        },
                        &store,
                        &[&endpoint],
                        &operations,
                        operation_slots,
                    )
                    .await;
                }
            }
        }
        _ => None,
    };
    let endpoints: Vec<&local_endpoint::LocalEndpoint> = std::iter::once(&endpoint)
        .chain(ingest.as_ref().map(|(endpoint, _)| endpoint))
        .collect();
    let connections = Arc::new(Semaphore::new(config.max_rpc_connections));
    let syslog_registry = sparrow_server::syslog_listener::SyslogRegistry::default();
    let mut syslog = if config.syslog.is_empty() {
        None
    } else {
        match sparrow_server::syslog_listener::SyslogWorker::start(
            store.clone(),
            &config.syslog,
            operations.clone(),
            syslog_registry.clone(),
        )
        .await
        {
            Ok(worker) => Some(worker),
            Err(error) => {
                return shutdown::finish(
                    Err(error.into()),
                    shutdown::Workers {
                        consumer: consumer.take(),
                        ..Default::default()
                    },
                    &store,
                    &endpoints,
                    &operations,
                    operation_slots,
                )
                .await;
            }
        }
    };
    let mut rpc = Rpc::new(store.clone(), operations.clone(), config.max_pin_members)
        .with_watches(watches)
        .with_syslog(syslog_registry);
    if let Some((_, socket)) = &ingest {
        rpc = rpc.with_ingest_socket(socket.clone());
    }
    if config.query_only {
        rpc = rpc.query_only();
    } else if let Some(consumer) = consumer.as_ref()
        && config.max_concurrent_requests >= 2
    {
        rpc = rpc.with_watch_controller(consumer.controller());
    }
    eprintln!(
        "sparrowd: store={} capnp={} data={} max_rpc_connections={} max_concurrent_requests={} query_only={} intake={} {}",
        store_id,
        config.socket_path.display(),
        config.data_dir.display(),
        config.max_rpc_connections,
        config.max_concurrent_requests,
        config.query_only,
        intake && !config.query_only,
        store.open_files()
    );
    if let (Some(path), Some(gid)) = (&config.ingest_socket_path, config.ingest_socket_gid) {
        eprintln!(
            "sparrowd: ingest_socket={} gid={gid} max_ingest_connections={}",
            path.display(),
            config.max_ingest_connections
        );
    }
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        // Expiry, eviction and the table rewrite all write; a query-only daemon runs none of them.
        let mut maintenance = (!config.query_only).then(|| maintenance::Worker::start(store.clone(), std::time::Duration::from_millis(u64::from(config.buffered_flush_interval_ms))));
        let serving = async {
        // Pinned once, so its interval and any worker shutdown it starts survive every turn.
        let health = worker_health(&mut consumer, &mut syslog, maintenance.as_ref());
        tokio::pin!(health);
        let mut space = tokio::time::interval(SPACE_CHECK_INTERVAL);
        // Below the floor every write is refused, so admit no new connections.
        let mut space_ok = true;
        loop {
            tokio::select! {
                signal = interrupt.recv() => {
                    signal.ok_or("daemon interrupt subscription terminated unexpectedly")?;
                    break;
                }
                failure = &mut health => return Err(failure),
                _ = space.tick() => match (store.space_floor().shortfall()?, space_ok) {
                    (None, false) => {
                        eprintln!("sparrowd: free space restored; accepting connections and writes");
                        space_ok = true;
                    }
                    (Some(shortfall), true) => {
                        eprintln!("sparrowd: pausing connections and writes: {shortfall}");
                        space_ok = false;
                    }
                    _ => {}
                },
                accepted = endpoint.listener.accept(), if space_ok => {
                    let (stream, _) = accepted?;
                    // A connection whose peer the kernel cannot name is dropped unanswered.
                    match sparrow_server::local_caller(&stream) {
                        Ok(caller) => {
                            let _ = sparrow_server::admit_connection(stream, rpc.clone(), connections.clone(), sparrow_server::Access::Local(caller));
                        }
                        Err(error) => eprintln!("sparrowd: private socket peer unreadable: {error}"),
                    }
                }
                accepted = async { ingest.as_ref().expect("enabled ingest branch").0.listener.accept().await }, if space_ok && ingest.is_some() => {
                    let (stream, _) = accepted?;
                    let _ = ingest.as_ref().expect("enabled ingest branch").1.admit(stream, rpc.clone());
                }
                accepted = async { tcp_listener.as_ref().expect("enabled TCP branch").accept().await }, if space_ok && tcp_listener.is_some() => {
                    let (stream, peer) = accepted?;
                    stream.set_nodelay(true)?;
                    let secret = tcp_secret.clone().expect("configuration pairs TCP with its secret");
                    let _ = sparrow_server::admit_connection(stream, rpc.clone(), connections.clone(), sparrow_server::Access::PublicTcp(secret, peer.to_string()));
                }
            }
        }
        Ok::<(), Box<dyn std::error::Error>>(())
        }.await;
        shutdown::finish(serving, shutdown::Workers { consumer: consumer.take(), syslog: syslog.take(), maintenance: maintenance.take() }, &store, &endpoints, &operations, operation_slots).await
    }).await
}
