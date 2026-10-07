// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One database owner, serving typed Cap'n Proto RPC over a private Unix socket.

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
    let store = config.open_store()?;
    // Refuse an existing socket rather than unlink another listener's endpoint.
    let listener = tokio::net::UnixListener::bind(&config.socket_path)?;
    std::fs::set_permissions(&config.socket_path, std::fs::Permissions::from_mode(0o600))?;
    let operations = Arc::new(Semaphore::new(config.max_concurrent_requests));
    let connections = Arc::new(Semaphore::new(config.max_concurrent_requests));
    let client: sparrow::Client = capnp_rpc::new_client(Rpc::new(
        store.clone(),
        operations,
        config.max_artifact_bytes,
        config.max_pin_members,
    ));
    eprintln!(
        "sparrowd: store={} capnp={} data={}",
        store.store_id()?,
        config.socket_path.display(),
        config.data_dir.display()
    );
    let local = tokio::task::LocalSet::new();
    local.run_until(async {
        let mut maintenance = tokio::time::interval(std::time::Duration::from_secs(30));
        maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut synchronize = tokio::time::interval(std::time::Duration::from_millis(u64::from(config.buffered_flush_interval_ms)));
        synchronize.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Consume interval's immediate first tick; the intended delay starts now.
        synchronize.tick().await;
        loop {
            tokio::select! {
                signal = tokio::signal::ctrl_c() => { signal?; break; }
                _ = synchronize.tick() => {
                    let owner = store.clone();
                    if let Err(error) = tokio::task::spawn_blocking(move || owner.flush_buffered()).await? {
                        eprintln!("sparrowd: buffered WAL synchronization failed: {error}");
                    }
                }
                _ = maintenance.tick() => {
                    let owner = store.clone();
                    if let Err(e) = tokio::task::spawn_blocking(move || owner.expire(sparrow_server::now()?)).await? {
                        eprintln!("sparrowd: retention maintenance failed: {e}");
                    }
                }
                accepted = listener.accept() => {
                    let (stream, _) = accepted?;
                    let Ok(permit) = connections.clone().try_acquire_owned() else { drop(stream); continue; };
                    let system = sparrow_server::serve_connection(stream, client.clone());
                    tokio::task::spawn_local(async move {
                        let _permit = permit;
                        match tokio::time::timeout(std::time::Duration::from_secs(60), system).await {
                            Ok(Ok(())) => {},
                            Ok(Err(e)) => eprintln!("sparrowd: RPC connection failed: {e}"),
                            Err(_) => eprintln!("sparrowd: RPC connection lease expired"),
                        }
                    });
                }
            }
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    }).await?;
    store.flush()?;
    std::fs::remove_file(&config.socket_path)?;
    Ok(())
}
