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

/// How often the daemon compares filesystem free space with its floor.
const SPACE_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
/// Also the contract `deploy/verify-backup.sh` reads to tell an image with query-only mode
/// and, for its migration measurements, an image with `--no-intake` or `--private-intake`.
const USAGE: &str = "usage: sparrowd --config FILE.toml [--query-only | --no-intake | --private-intake DIR] | sparrowd --restore-backup BACKUP_ROOT [BACKUP_ID] EMPTY_DIR";

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

/// Restore an online backup into an empty directory, offline: no daemon, socket or store opens.
fn restore_backup(
    root: &str,
    id: Option<&str>,
    target: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let id = id
        .map(|id| {
            id.parse::<u32>()
                .map_err(|_| format!("backup id {id} is not a number"))
        })
        .transpose()?;
    let restored = sparrow_core::restore_backup(Path::new(root), id, Path::new(target))
        .map_err(|error| format!("{}: {}", error.code, error.message))?;
    eprintln!("sparrowd: restored backup {restored} from {root} into {target}");
    Ok(())
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
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["--restore-backup", root, target] => return restore_backup(root, None, target),
        ["--restore-backup", root, id, target] => return restore_backup(root, Some(id), target),
        _ => {}
    }
    let (path, mode) = arguments(&args)?;
    let mut config = ServerConfig::read(Path::new(path))?;
    // A no-intake daemon writes and runs maintenance but, like a query-only one, opens no
    // stored watch root, so a copy of a store never consumes the intake its catalog names.
    // A private-intake daemon opens every watch at a private root instead.
    let intake = !matches!(mode, Mode::NoIntake);
    // Both writable copy modes serve a disposable copy, which never backs up.
    let copy = matches!(mode, Mode::NoIntake | Mode::PrivateIntake(_));
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
    let store_id = store.store_id()?;
    let operations = Arc::new(Semaphore::new(config.max_concurrent_requests));
    let admission = sparrow_server::admission::Admission::new(operations.clone());
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
    // Only the daemon serving the live intake backs up; a query-only one refuses at its write
    // gate, and a no-intake or private-intake one serves a copy (the latter also refused any
    // `backup_root` above, in ServerConfig::check_private_intake).
    if copy {
        rpc = rpc.without_backup_for_copy();
    } else if !config.query_only
        && let Some(target) = config.backup_target()?
    {
        rpc = rpc.with_backup(target);
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
    local.run_until(async {
        // Expiry, eviction and the table rewrite all write; a query-only daemon runs none of them.
        let mut maintenance = (!config.query_only).then(|| maintenance::Worker::start(store.clone(), std::time::Duration::from_millis(u64::from(config.buffered_flush_interval_ms)), pressure_wake.clone(), admission.clone()));
        let serving = async {
        // Pinned once, so its waits and any worker shutdown it starts survive every turn.
        let health = worker_health(&mut consumer, &mut syslog, maintenance.as_mut());
        tokio::pin!(health);
        let mut space = tokio::time::interval(SPACE_CHECK_INTERVAL);
        // Below the floor every write is refused, so admit no new connections.
        let mut space_ok = true;
        // A connection slot is taken before its peer is accepted (#246): at the ceiling the accept
        // loop waits on a slot, and the peer waits in the kernel listen backlog, served when a slot
        // frees, rather than being accepted and reset. One slot is held ahead for the private socket
        // and public TCP together, which share a ceiling, and one for the ingest socket.
        let mut rpc_slot = None;
        let mut rpc_wait = None;
        let mut ingest_slot = None;
        let mut ingest_wait = None;
        loop {
            if rpc_slot.is_none() && rpc_wait.is_none() {
                match connections.try_slot() { Ok(slot) => rpc_slot = Some(slot), Err(wait) => rpc_wait = Some(wait) }
            }
            if let Some((_, socket)) = ingest.as_ref().filter(|_| ingest_slot.is_none() && ingest_wait.is_none()) {
                match socket.ceiling().try_slot() { Ok(slot) => ingest_slot = Some(slot), Err(wait) => ingest_wait = Some(wait) }
            }
            tokio::select! {
                slot = connections.acquire(), if rpc_slot.is_none() => {
                    rpc_slot = Some(slot);
                    rpc_wait = None;
                }
                slot = async { ingest.as_ref().expect("enabled ingest branch").1.ceiling().acquire().await }, if ingest.is_some() && ingest_slot.is_none() => {
                    ingest_slot = Some(slot);
                    ingest_wait = None;
                }
                signal = interrupt.recv() => {
                    signal.ok_or("daemon interrupt subscription terminated unexpectedly")?;
                    break;
                }
                failure = &mut health => return Err(failure),
                _ = space.tick() => match (store.space_floor().observe(sparrow_server::now()?)?, space_ok) {
                    (None, false) => {
                        let floor = store.space_floor().status();
                        eprintln!("sparrowd: free space restored; accepting connections and writes (pauses={} resumes={} refused_writes={})", floor.pauses, floor.resumes, floor.refused_writes);
                        space_ok = true;
                    }
                    (Some(shortfall), true) => {
                        let floor = store.space_floor().status();
                        eprintln!("sparrowd: pausing connections and writes (pauses={} resumes={} refused_writes={}): {shortfall}", floor.pauses, floor.resumes, floor.refused_writes);
                        space_ok = false;
                    }
                    _ => {}
                },
                accepted = endpoint.listener.accept(), if space_ok && rpc_slot.is_some() => {
                    let (stream, _) = accepted?;
                    let Some(slot) = rpc_slot.take() else { continue };
                    // A connection whose peer the kernel cannot name is dropped unanswered.
                    match sparrow_server::local_caller(&stream) {
                        Ok(caller) => {
                            sparrow_server::admit_connection(stream, rpc.clone(), slot, sparrow_server::Access::Local(caller));
                        }
                        Err(error) => eprintln!("sparrowd: private socket peer unreadable: {error}"),
                    }
                }
                accepted = async { ingest.as_ref().expect("enabled ingest branch").0.listener.accept().await }, if space_ok && ingest.is_some() && ingest_slot.is_some() => {
                    let (stream, _) = accepted?;
                    let Some(slot) = ingest_slot.take() else { continue };
                    let _ = ingest.as_ref().expect("enabled ingest branch").1.admit(stream, rpc.clone(), slot);
                }
                accepted = async { tcp_listener.as_ref().expect("enabled TCP branch").accept().await }, if space_ok && tcp_listener.is_some() && rpc_slot.is_some() => {
                    let (stream, peer) = accepted?;
                    let Some(slot) = rpc_slot.take() else { continue };
                    stream.set_nodelay(true)?;
                    let secret = tcp_secret.clone().expect("configuration pairs TCP with its secret");
                    sparrow_server::admit_connection(stream, rpc.clone(), slot, sparrow_server::Access::PublicTcp(secret, peer.to_string()));
                }
            }
        }
        Ok::<(), Box<dyn std::error::Error>>(())
        }.await;
        shutdown::finish(serving, shutdown::Workers { consumer: consumer.take(), syslog: syslog.take(), maintenance: maintenance.take() }, &store, &endpoints, &admission, operation_slots).await
    }).await
}
