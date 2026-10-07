// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One strict configuration and storage-open path for the daemon.

use serde::Deserialize;
use sparrow_core::{Config, Store};
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

/// Explicit local daemon configuration; unknown fields are errors.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Intended buffered WAL synchronization interval.
    #[serde(default = "default_flush_interval")]
    pub buffered_flush_interval_ms: u32,
    /// Private Cap'n Proto Unix socket.
    pub socket_path: PathBuf,
    /// Single-writer store and socket directory.
    pub data_dir: PathBuf,
    /// Explicit physical capacity budget.
    pub disk_budget: u64,
    /// Reserved maintenance capacity.
    pub maintenance_reserve: u64,
    /// Bounded direct ingestion admission.
    pub max_artifact_bytes: usize,
    /// Maximum records in a complete publication.
    pub max_records: usize,
    /// Maximum direct members in a pin collection.
    pub max_pin_members: usize,
    /// Bound on simultaneous RPC work and connections.
    pub max_concurrent_requests: usize,
    /// Required native analyzer artifact directory.
    pub text_lexicon_dir: PathBuf,
}

fn default_flush_interval() -> u32 {
    sparrow_core::DEFAULT_FLUSH_INTERVAL_MS
}

impl ServerConfig {
    /// Read and validate the same typed configuration used by the production daemon.
    pub fn read(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let config: Self = toml::from_str(&std::fs::read_to_string(path)?)?;
        if !(1..=64).contains(&config.max_concurrent_requests) {
            return Err("max_concurrent_requests must be 1..64".into());
        }
        if config.socket_path.parent() != Some(config.data_dir.as_path()) {
            return Err("socket_path must be directly inside the private data_dir".into());
        }
        Ok(config)
    }

    /// Open the one storage owner with native analyzer artifacts and private permissions.
    pub fn open_store(&self) -> Result<Store, Box<dyn std::error::Error>> {
        let store = Store::open(
            &self.data_dir,
            Config {
                buffered_flush_interval_ms: self.buffered_flush_interval_ms,
                disk_budget: self.disk_budget,
                maintenance_reserve: self.maintenance_reserve,
                max_artifact_bytes: self.max_artifact_bytes,
                max_records: self.max_records,
                max_pin_members: self.max_pin_members,
                text_analyzer: sparrow_core::resolve_text_analyzer(&self.text_lexicon_dir)?,
            },
        )?;
        std::fs::set_permissions(&self.data_dir, std::fs::Permissions::from_mode(0o700))?;
        Ok(store)
    }
}
