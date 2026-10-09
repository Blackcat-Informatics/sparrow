// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One database owner, serving typed Cap'n Proto RPC over Unix and configured TCP sockets.

use sparrow_server::{Rpc, ServerConfig};
use std::{path::Path, sync::Arc};
use tokio::sync::Semaphore;

mod local_endpoint;
mod maintenance;
mod shutdown;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 || args[0] != "--config" {
        return Err("usage: sparrowd --config FILE.toml".into());
    }
    let config = ServerConfig::read(Path::new(&args[1]))?;
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
    let store = config.open_store()?;
    let store_id = store.store_id()?;
    let operations = Arc::new(Semaphore::new(config.max_concurrent_requests));
    let operation_slots = u32::try_from(config.max_concurrent_requests)?;
    let watches = sparrow_server::watches::WatchRegistry::default();
    let mut consumer = Some(sparrow_server::watches::ConsumeWorker::start(
        store.clone(),
        &config.consume,
        config.socket_path.clone(),
        watches.clone(),
        operations.clone(),
    )?);
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
                None,
                &operations,
                operation_slots,
            )
            .await;
        }
    };
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
                    Some(&endpoint),
                    &operations,
                    operation_slots,
                )
                .await;
            }
        }
    };
    let mut rpc = Rpc::new(
        store.clone(),
        operations.clone(),
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
    eprintln!(
        "sparrowd: store={} capnp={} data={} max_rpc_connections={} max_concurrent_requests={}",
        store_id,
        config.socket_path.display(),
        config.data_dir.display(),
        config.max_rpc_connections,
        config.max_concurrent_requests
    );
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let mut maintenance = Some(maintenance::Worker::start(store.clone(), std::time::Duration::from_millis(u64::from(config.buffered_flush_interval_ms))));
        let serving = async {
        let mut consumer_health = tokio::time::interval(std::time::Duration::from_millis(200));
        loop {
            tokio::select! {
                signal = interrupt.recv() => {
                    signal.ok_or("daemon interrupt subscription terminated unexpectedly")?;
                    break;
                }
                _ = consumer_health.tick(), if consumer.is_some() || syslog.is_some() => {
                    if consumer.as_ref().is_some_and(sparrow_server::watches::ConsumeWorker::is_finished) {
                        consumer.take().expect("worker checked above").shutdown().await?;
                        return Err("completed-consumption worker terminated unexpectedly".into());
                    }
                    if syslog.as_ref().is_some_and(sparrow_server::syslog_listener::SyslogWorker::is_finished) {
                        syslog.take().expect("worker checked above").shutdown().await?;
                        return Err("syslog intake worker terminated unexpectedly".into());
                    }
                    if maintenance.as_ref().is_some_and(maintenance::Worker::is_finished) {
                        return Err("maintenance worker terminated unexpectedly".into());
                    }
                }
                accepted = endpoint.listener.accept() => {
                    let (stream, _) = accepted?;
                    sparrow_server::admit_connection(stream, rpc.clone(), connections.clone());
                }
                accepted = async { tcp_listener.as_ref().expect("enabled TCP branch").accept().await }, if tcp_listener.is_some() => {
                    let (stream, _) = accepted?;
                    stream.set_nodelay(true)?;
                    sparrow_server::admit_connection(stream, rpc.clone(), connections.clone());
                }
            }
        }
        Ok::<(), Box<dyn std::error::Error>>(())
        }.await;
        shutdown::finish(serving, shutdown::Workers { consumer: consumer.take(), syslog: syslog.take(), maintenance: maintenance.take() }, &store, Some(&endpoint), &operations, operation_slots).await
    }).await
}
