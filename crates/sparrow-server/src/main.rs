// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One database owner, serving typed Cap'n Proto RPC over Unix and configured TCP sockets.

use sparrow_server::{Rpc, ServerConfig};
use std::{path::Path, sync::Arc};
use tokio::sync::Semaphore;

mod local_endpoint;
mod maintenance;
mod pressure_duty;
mod shutdown;

/// How often the daemon compares filesystem free space with its floor while it waits to open.
const SPACE_CHECK_INTERVAL: std::time::Duration = sparrow_server::serving::SPACE_CHECK_INTERVAL;
/// Also the contract the deploy scripts read to tell an image with query-only mode and, for
/// migration measurements, an image with `--no-intake` or `--private-intake`.
const USAGE: &str =
    "usage: sparrowd --config FILE.toml [--query-only | --no-intake | --private-intake DIR]";

/// The daemon's mode, from its command line.
enum Mode {
    Normal,
    QueryOnly,
    /// Writable with maintenance, no intake.
    NoIntake,
    /// Writable with full intake whose watch roots are rebound below this directory.
    PrivateIntake(std::path::PathBuf),
}

/// Parse `--config FILE` and at most one mode, in either order.
fn arguments(args: &[String]) -> Result<(&str, Mode), Box<dyn std::error::Error>> {
    let (mut path, mut mode) = (None, None);
    let mut args = args.iter().map(String::as_str);
    while let Some(arg) = args.next() {
        match arg {
            "--config" if path.is_none() => path = Some(args.next().ok_or(USAGE)?),
            "--query-only" if mode.is_none() => mode = Some(Mode::QueryOnly),
            "--no-intake" if mode.is_none() => mode = Some(Mode::NoIntake),
            "--private-intake" if mode.is_none() => {
                mode = Some(Mode::PrivateIntake(args.next().ok_or(USAGE)?.into()));
            }
            _ => return Err(USAGE.into()),
        }
    }
    Ok((path.ok_or(USAGE)?, mode.unwrap_or(Mode::Normal)))
}

/// Wait for the workers the daemon started and return the failure of the first that ends.
/// Every worker runs until shutdown, so any end before it is a failure the daemon exits
/// on. Every mode is covered: a normal daemon has all three workers, a no-intake daemon only
/// maintenance, a query-only one none, so this never returns there.
async fn worker_health(
    consumer: &mut Option<sparrow_server::watches::ConsumeWorker>,
    syslog: &mut Option<sparrow_server::syslog_listener::SyslogWorker>,
    maintenance: Option<&mut maintenance::Worker>,
) -> Box<dyn std::error::Error> {
    enum Ended {
        Consumer,
        Syslog,
        Maintenance,
    }
    // Each worker closes its end signal as it ends, so this waits without sampling.
    let ended = tokio::select! {
        () = async {
            match consumer.as_mut() {
                Some(worker) => worker.ended().await,
                None => std::future::pending().await,
            }
        } => Ended::Consumer,
        () = async {
            match syslog.as_mut() {
                Some(worker) => worker.ended().await,
                None => std::future::pending().await,
            }
        } => Ended::Syslog,
        () = async {
            match maintenance {
                Some(worker) => worker.ended().await,
                None => std::future::pending().await,
            }
        } => Ended::Maintenance,
    };
    match ended {
        Ended::Consumer => {
            if let Some(worker) = consumer.take()
                && let Err(error) = worker.shutdown().await
            {
                return error;
            }
            "completed-consumption worker terminated unexpectedly".into()
        }
        Ended::Syslog => {
            if let Some(worker) = syslog.take()
                && let Err(error) = worker.shutdown().await
            {
                return error.into();
            }
            "syslog intake worker terminated unexpectedly".into()
        }
        Ended::Maintenance => "maintenance worker terminated unexpectedly".into(),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    sparrow_server::journal_log::install()?;
    let args: Vec<_> = std::env::args().skip(1).collect();
    let (path, mode) = arguments(&args)?;
    let mut config = ServerConfig::read(Path::new(path))?;
    // A no-intake daemon writes and runs maintenance but, like a query-only one, opens no
    // stored watch root, so a copy of a store never consumes the intake its catalog names.
    // A private-intake daemon opens every watch at a private root instead.
    let intake = !matches!(mode, Mode::NoIntake);
    let private = match &mode {
        Mode::QueryOnly => {
            config = config.into_query_only()?;
            None
        }
        Mode::NoIntake => {
            config.check_no_intake()?;
            None
        }
        Mode::PrivateIntake(dir) => {
            config.check_private_intake()?;
            Some(sparrow_server::watches::PrivateIntake::new(dir)?)
        }
        Mode::Normal => None,
    };
    let intake_label = match (&private, intake && !config.query_only) {
        (Some(_), _) => "private",
        (None, true) => "true",
        (None, false) => "false",
    };
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
    sparrow_server::staging_refusal::install(&store)?;
    let store_id = store.store_id()?;
    let operations = Arc::new(Semaphore::new(config.max_concurrent_requests));
    let admission = sparrow_server::admission::Admission::new(operations.clone());
    // A begin-import waiting for a journal slot is told when a reclamation commit frees one.
    store.set_import_slot_waker(Some(Arc::new({
        let admission = admission.clone();
        move || admission.import_slot_freed()
    })));
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
        let consumer = sparrow_server::watches::ConsumeWorker::start_with(
            store.clone(),
            &config.consume,
            config.socket_path.clone(),
            watches.clone(),
            operations.clone(),
            private,
            config.output_paths(),
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
                &admission,
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
                        &admission,
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
    let connections = sparrow_server::admission::Ceiling::new(
        "RPC connection ceiling",
        config.max_rpc_connections,
    );
    let syslog_registry = sparrow_server::syslog_listener::SyslogRegistry::default();
    let pressure_wake = sparrow_server::pressure_wake::PressureWake::default();
    let mut syslog = if config.syslog.is_empty() {
        None
    } else {
        match sparrow_server::syslog_listener::SyslogWorker::start(
            store.clone(),
            &config.syslog,
            operations.clone(),
            syslog_registry.clone(),
            pressure_wake.clone(),
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
                    &admission,
                    operation_slots,
                )
                .await;
            }
        }
    };
    let mut rpc = Rpc::new(store.clone(), operations.clone(), config.max_pin_members)
        .with_admission(admission.clone())
        .with_connections(connections.clone())
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
        intake_label,
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
    local
        .run_until(async {
            // Expiry, eviction and the table rewrite all write; a query-only daemon runs none of them.
            let mut maintenance = (!config.query_only).then(|| {
                maintenance::Worker::start(
                    store.clone(),
                    std::time::Duration::from_millis(u64::from(config.buffered_flush_interval_ms)),
                    pressure_wake.clone(),
                )
            });
            // Segments go to `<dir>/<store_id>/`; the worker never writes anywhere else in `dir`.
            let archive = match config.archive.as_ref().filter(|_| !config.query_only) {
                Some(archive) => Some(sparrow_server::archive::Worker::start(
                    store.clone(),
                    archive.dir.clone(),
                    std::time::Duration::from_secs(archive.check_secs),
                    std::time::Duration::from_secs(archive.stall_warn_secs),
                    std::time::Duration::from_secs(archive.io_timeout_secs),
                )?),
                None => None,
            };
            let serving = async {
                // Pinned once, so its waits and any worker shutdown it starts survive every turn.
                let health = worker_health(&mut consumer, &mut syslog, maintenance.as_mut());
                tokio::pin!(health);
                // The floor never stops the accept loop (#257): below it the store answers writes with the
                // floor-labelled retryable refusal and reads are served. The loop observes the floor
                // once a second and journals the episode; connection slots, taken before accept, and
                // the 60 s lease are unchanged.
                let sockets = sparrow_server::serving::Sockets {
                    private: &endpoint.listener,
                    ingest: ingest
                        .as_ref()
                        .map(|(endpoint, socket)| (&endpoint.listener, socket)),
                    tcp: tcp_listener.as_ref().map(|listener| {
                        (
                            listener,
                            tcp_secret
                                .clone()
                                .expect("configuration pairs TCP with its secret"),
                        )
                    }),
                };
                sparrow_server::serving::serve(
                    sockets,
                    &rpc,
                    &connections,
                    async {
                        interrupt.recv().await.map(|_| ()).ok_or_else(
                            || -> Box<dyn std::error::Error> {
                                "daemon interrupt subscription terminated unexpectedly".into()
                            },
                        )
                    },
                    health,
                    |line| eprintln!("{line}"),
                )
                .await
            }
            .await;
            // Dropping the handle stops the archive thread; it is not joined, so a hung mount
            // cannot hold the shutdown.
            drop(archive);
            shutdown::finish(
                serving,
                shutdown::Workers {
                    consumer: consumer.take(),
                    syslog: syslog.take(),
                    maintenance: maintenance.take(),
                },
                &store,
                &endpoints,
                &admission,
                operation_slots,
            )
            .await
        })
        .await
}
