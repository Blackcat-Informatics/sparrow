// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One database owner, serving typed Cap'n Proto RPC over Unix and configured TCP sockets.

use sparrow_server::{
    ServerConfig, local_endpoint,
    start_wait::{self, Wait},
};
use std::{path::Path, sync::Arc};

mod maintenance;
mod pressure_duty;
mod recovery;
mod session;
mod shutdown;

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

/// Set once the daemon serves. A failure before it is a refused start; one after it is a run
/// that failed, which the supervisor restarts.
static SERVING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        // A start failure not named as deterministic keeps the ordinary failure status.
        Err(error)
            if !SERVING.load(std::sync::atomic::Ordering::Relaxed)
                && !start_wait::is_permanent(error.as_ref()) =>
        {
            // An ordinary failure such as transient I/O or descriptor exhaustion uses the
            // unit's crash retry delay.
            eprintln!(
                "sparrowd: start failed: {error}; not classified as permanent, so the exit status is 1 and the unit restarts it"
            );
            std::process::ExitCode::FAILURE
        }
        // A refusal named as deterministic repeats on a restart: end it with the status the
        // unit does not restart.
        Err(error) if !SERVING.load(std::sync::atomic::Ordering::Relaxed) => {
            eprintln!(
                "sparrowd: refused to start: {error}; a restart repeats it, so the exit status is {} and the unit does not restart it. Correct the configuration, image or store, then start the unit",
                start_wait::REFUSED_EXIT_STATUS
            );
            start_wait::notify(&format!("STATUS=refused to start: {error}"));
            std::process::ExitCode::from(start_wait::REFUSED_EXIT_STATUS)
        }
        Err(error) => {
            eprintln!("Error: {error:?}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    sparrow_server::journal_log::install()?;
    let args: Vec<_> = std::env::args().skip(1).collect();
    let (path, mode) = arguments(&args).map_err(start_wait::permanent)?;
    let mut config = ServerConfig::read(Path::new(path)).map_err(start_wait::config_failure)?;
    // A no-intake daemon writes and runs maintenance but, like a query-only one, opens no
    // stored watch root, so a copy of a store never consumes the intake its catalog names.
    // A private-intake daemon opens every watch at a private root instead.
    let intake = !matches!(mode, Mode::NoIntake);
    let private = match &mode {
        Mode::QueryOnly => {
            config = config.into_query_only().map_err(start_wait::permanent)?;
            None
        }
        Mode::NoIntake => {
            config.check_no_intake().map_err(start_wait::permanent)?;
            None
        }
        Mode::PrivateIntake(dir) => {
            config
                .check_private_intake()
                .map_err(start_wait::permanent)?;
            Some(
                sparrow_server::watches::PrivateIntake::new(dir)
                    .map_err(start_wait::consume_start_failure)?,
            )
        }
        Mode::Normal => None,
    };
    let intake_label = match (&private, intake && !config.query_only) {
        (Some(_), _) => "private",
        (None, true) => "true",
        (None, false) => "false",
    };
    let tcp_secret = config.tcp_secret().map_err(start_wait::config_failure)?;
    // Keep the subscription alive through startup and awaited maintenance work.
    // Recreating ctrl_c() inside select drops its listener when another branch wins.
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let archive_filesystem = config
        .archive
        .as_ref()
        .filter(|_| !config.query_only)
        .map(|_| sparrow_server::archive::Filesystem::start())
        .transpose()?;
    // Refuse an unavailable public port before opening/migrating the store or Unix endpoint.
    // A departing owner of the address is waited for, not exited on (start_wait).
    let tcp_listener = match config.rpc_tcp_address {
        Some(address) => match start_wait::until(
            &mut interrupt,
            &format!("bind the public TCP address {address}"),
            async || tokio::net::TcpListener::bind(address).await,
            start_wait::bind_hold,
        )
        .await
        .map_err(start_wait::bind_failure)?
        {
            Wait::Ready(listener) => Some(listener),
            Wait::Interrupted => return Ok(()),
        },
        None => None,
    };
    if let Some(listener) = &tcp_listener {
        eprintln!("sparrowd: capnp_tcp={}", listener.local_addr()?);
    }
    // Opening writes a manifest and WAL and replays any WAL an unclean stop left.
    // Wait inside the process for room or a pending upgrade's write allowance.
    // Between the open floor and `min_free_bytes` the store opens and the floor pauses writes,
    // so a restart on a nearly full filesystem still serves reads and can reclaim.
    let store = match start_wait::open_store(&mut interrupt, &mut config, Path::new(path)).await? {
        Wait::Ready(store) => store,
        Wait::Interrupted => return Ok(()),
    };
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
    let store_id: Arc<str> = store.store_id()?.into();
    let process = session::Process {
        config: Arc::new(config),
        intake,
        intake_label,
        private,
        archive: archive_filesystem,
    };
    let config = &process.config;
    let mut session = session::Session::prepare(store, &process).await?;
    // Validate the catalog first; recover only a refused socket under exclusive store ownership.
    // A name a departing owner still holds is waited for (start_wait).
    let bound = start_wait::until(
        &mut interrupt,
        "bind the local endpoint",
        async || local_endpoint::LocalEndpoint::bind(&config.socket_path, &session.store).await,
        start_wait::bind_hold,
    )
    .await;
    let endpoint = match bound {
        Ok(Wait::Ready(endpoint)) => endpoint,
        stopped => {
            return session
                .finish(
                    stopped
                        .map(|_| maintenance::End::Stopped)
                        .map_err(start_wait::bind_failure),
                    &[],
                )
                .await
                .map(drop);
        }
    };
    let ingest = match (&config.ingest_socket_path, config.ingest_socket_gid) {
        (Some(path), Some(gid)) => {
            let bound = start_wait::until(
                &mut interrupt,
                "bind the ingest socket",
                async || local_endpoint::LocalEndpoint::bind_ingest(path, gid).await,
                start_wait::bind_hold,
            )
            .await;
            match bound {
                Ok(Wait::Ready(endpoint)) => Some((
                    endpoint,
                    sparrow_server::ingest_socket::IngestSocket::new(
                        path.clone(),
                        gid,
                        config.max_ingest_connections,
                    ),
                )),
                stopped => {
                    return session
                        .finish(
                            stopped
                                .map(|_| maintenance::End::Stopped)
                                .map_err(start_wait::bind_failure),
                            &[&endpoint],
                        )
                        .await
                        .map(drop);
                }
            }
        }
        _ => None,
    };
    let endpoints: Vec<&local_endpoint::LocalEndpoint> = std::iter::once(&endpoint)
        .chain(ingest.as_ref().map(|(endpoint, _)| endpoint))
        .collect();
    let sockets = sparrow_server::serving::Sockets {
        private: &endpoint,
        ingest: ingest.as_ref().map(|(endpoint, socket)| (endpoint, socket)),
        tcp: tcp_listener.as_ref().zip(tcp_secret),
    };
    let outcome = async {
        loop {
            match session.serve(&process, &sockets, &mut interrupt).await? {
                maintenance::End::Stopped => return Ok(()),
                maintenance::End::Reopen(_) => {
                    let Some(store) = recovery::run(
                        session.store,
                        config.clone(),
                        store_id.clone(),
                        &sockets,
                        session::interrupted(&mut interrupt),
                    )
                    .await?
                    else {
                        return Ok(());
                    };
                    let Some(replacement) = session::Session::prepare_replacement(
                        store,
                        &process,
                        &sockets,
                        &mut interrupt,
                    )
                    .await?
                    else {
                        return Ok(());
                    };
                    session = replacement;
                }
            }
        }
    }
    .await;
    shutdown::cleanup(outcome, &endpoints)
}
