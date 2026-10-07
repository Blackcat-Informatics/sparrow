// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One database owner, serving typed Cap'n Proto RPC over a private Unix socket.

use serde::Deserialize;
use sparrow_core::{Config, Store};
use sparrow_server::{Rpc, sparrow_capnp::sparrow};
use std::{os::unix::fs::PermissionsExt, path::PathBuf, sync::Arc};
use tokio::sync::Semaphore;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerConfig {
    socket_path: PathBuf,
    data_dir: PathBuf,
    disk_budget: u64,
    maintenance_reserve: u64,
    max_artifact_bytes: usize,
    max_records: usize,
    max_pin_members: usize,
    max_concurrent_requests: usize,
    text_lexicon_dir: PathBuf,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 || args[0] != "--config" {
        return Err("usage: sparrowd --config FILE.toml".into());
    }
    let config: ServerConfig = toml::from_str(&std::fs::read_to_string(&args[1])?)?;
    if !(1..=64).contains(&config.max_concurrent_requests) {
        return Err("max_concurrent_requests must be 1..64".into());
    }
    if config.socket_path.parent() != Some(config.data_dir.as_path()) {
        return Err("socket_path must be directly inside the private data_dir".into());
    }
    let store = Store::open(
        &config.data_dir,
        Config {
            disk_budget: config.disk_budget,
            maintenance_reserve: config.maintenance_reserve,
            max_artifact_bytes: config.max_artifact_bytes,
            max_records: config.max_records,
            max_pin_members: config.max_pin_members,
            text_analyzer: sparrow_core::resolve_text_analyzer(&config.text_lexicon_dir)?,
        },
    )?;
    std::fs::set_permissions(&config.data_dir, std::fs::Permissions::from_mode(0o700))?;
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
        loop {
            tokio::select! {
                signal = tokio::signal::ctrl_c() => { signal?; break; }
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
