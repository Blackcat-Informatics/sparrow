// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One database owner, serving typed Cap'n Proto RPC over Unix and configured TCP sockets.

use sparrow_server::{Rpc, ServerConfig, sparrow_capnp::sparrow};
use std::{os::unix::fs::PermissionsExt, path::Path, sync::Arc};
use tokio::sync::Semaphore;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 || args[0] != "--config" {
        return Err("usage: sparrowd --config FILE.toml".into());
    }
    let config = ServerConfig::read(Path::new(&args[1]))?;
    // Refuse an unavailable public port before opening/migrating the store or Unix endpoint.
    let tcp_listener = match config.rpc_tcp_address {
        Some(address) => Some(tokio::net::TcpListener::bind(address).await?),
        None => None,
    };
    if let Some(listener) = &tcp_listener {
        eprintln!("sparrowd: capnp_tcp={}", listener.local_addr()?);
    }
    let store = config.open_store()?;
    let operations = Arc::new(Semaphore::new(config.max_concurrent_requests));
    let watches = sparrow_server::watches::WatchRegistry::default();
    let mut consumer = Some(sparrow_server::watches::ConsumeWorker::start(
        store.clone(),
        &config.consume,
        config.socket_path.clone(),
        watches.clone(),
        operations.clone(),
    )?);
    // Validate the watch catalog before publishing an endpoint, and refuse an existing socket.
    let listener = tokio::net::UnixListener::bind(&config.socket_path)?;
    std::fs::set_permissions(&config.socket_path, std::fs::Permissions::from_mode(0o600))?;
    let connections = Arc::new(Semaphore::new(config.max_concurrent_requests));
    let syslog_registry = sparrow_server::syslog_listener::SyslogRegistry::default();
    let mut syslog = if config.syslog.is_empty() {
        None
    } else {
        Some(
            sparrow_server::syslog_listener::SyslogWorker::start(
                store.clone(),
                &config.syslog,
                operations.clone(),
                syslog_registry.clone(),
            )
            .await?,
        )
    };
    let mut rpc = Rpc::new(
        store.clone(),
        operations,
        config.max_artifact_bytes,
        config.max_pin_members,
    )
    .with_watches(watches)
    .with_syslog(syslog_registry);
    if config.max_concurrent_requests >= 2 {
        rpc = rpc.with_watch_controller(
            consumer
                .as_ref()
                .expect("worker started above")
                .controller(),
        );
    }
    let client: sparrow::Client = capnp_rpc::new_client(rpc);
    eprintln!(
        "sparrowd: store={} capnp={} data={}",
        store.store_id()?,
        config.socket_path.display(),
        config.data_dir.display()
    );
    let local = tokio::task::LocalSet::new();
    let serving = local.run_until(async {
        let serving = async {
        let mut consumer_health = tokio::time::interval(std::time::Duration::from_millis(200));
        let mut maintenance = tokio::time::interval(std::time::Duration::from_secs(1));
        maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut pressure = tokio::time::interval(std::time::Duration::from_secs(1));
        pressure.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut synchronize = tokio::time::interval(std::time::Duration::from_millis(u64::from(config.buffered_flush_interval_ms)));
        synchronize.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Consume interval's immediate first tick; the intended delay starts now.
        synchronize.tick().await;
        loop {
            tokio::select! {
                signal = tokio::signal::ctrl_c() => { signal?; break; }
                _ = consumer_health.tick(), if consumer.is_some() || syslog.is_some() => {
                    if consumer.as_ref().is_some_and(sparrow_server::watches::ConsumeWorker::is_finished) {
                        consumer.take().expect("worker checked above").shutdown().await?;
                        return Err("completed-consumption worker terminated unexpectedly".into());
                    }
                    if syslog.as_ref().is_some_and(sparrow_server::syslog_listener::SyslogWorker::is_finished) {
                        syslog.take().expect("worker checked above").shutdown().await?;
                        return Err("syslog intake worker terminated unexpectedly".into());
                    }
                }
                _ = synchronize.tick() => {
                    let owner = store.clone();
                    if let Err(error) = tokio::task::spawn_blocking(move || owner.flush_buffered()).await? {
                        eprintln!("sparrowd: buffered WAL synchronization failed: {error}");
                    }
                }
                _ = maintenance.tick() => {
                    let owner = store.clone();
                    if let Err(e) = tokio::task::spawn_blocking(move || owner.expire_bounded(
                        sparrow_server::now()?, sparrow_core::EvictionLimits::default())).await? {
                        eprintln!("sparrowd: retention maintenance failed: {e}");
                    }
                }
                _ = pressure.tick() => {
                    let owner = store.clone();
                    if let Err(error) = tokio::task::spawn_blocking(move || owner.pressure_maintain(
                        sparrow_server::now()?, sparrow_core::EvictionLimits::default())).await? {
                        eprintln!("sparrowd: pressure maintenance failed: {error}");
                    }
                }
                accepted = listener.accept() => {
                    let (stream, _) = accepted?;
                    sparrow_server::admit_connection(stream, client.clone(), connections.clone());
                }
                accepted = async { tcp_listener.as_ref().expect("enabled TCP branch").accept().await }, if tcp_listener.is_some() => {
                    let (stream, _) = accepted?;
                    stream.set_nodelay(true)?;
                    sparrow_server::admit_connection(stream, client.clone(), connections.clone());
                }
            }
        }
        Ok::<(), Box<dyn std::error::Error>>(())
        }.await;
        if let Some(worker) = consumer.take() { worker.shutdown().await?; }
        if let Some(worker) = syslog.take() { worker.shutdown().await?; }
        serving
    }).await;
    store.flush()?;
    std::fs::remove_file(&config.socket_path)?;
    serving
}
