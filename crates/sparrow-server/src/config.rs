// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One strict configuration and storage-open path for the daemon.

use serde::{Deserialize, Serialize};
use sparrow_core::{Config, Store};
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

pub(crate) const MAX_CONSUME_ROOTS: usize = 8;

/// Explicit local daemon configuration; unknown fields are errors.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Optional public Cap'n Proto TCP binding, without authentication.
    #[serde(default)]
    pub rpc_tcp_address: Option<std::net::SocketAddr>,
    /// Explicit syslog intake listeners; none are opened by default.
    #[serde(default)]
    pub syslog: Vec<crate::syslog_listener::SyslogListener>,
    /// Explicit finished-file consumption authority. Empty by default.
    #[serde(default)]
    pub consume: Vec<ConsumeWatch>,
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

/// Producer contract required before a watcher can claim or retire source files.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProducerContract {
    /// Producers hand off finished files and never append after handoff.
    FinishedFiles,
}

/// Bounded recursive completed-input watch; no default deletion authority.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumeWatch {
    /// Persistent watch identity, also bound into recovery journals.
    pub id: String,
    /// Absolute directory whose completed files may be consumed.
    pub root: PathBuf,
    /// Required explicit producer handoff promise.
    pub producer_contract: ProducerContract,
    /// Source label supplied to the canonical native import path.
    pub source: String,
    /// Existing typed SDK format name.
    pub format: String,
    /// Fresh observed stability interval after startup or lost event coverage.
    #[serde(default = "default_quiet")]
    pub quiet_ms: u32,
    /// Maximum held recursive directory watches and scan cursors.
    #[serde(default = "default_directories")]
    pub max_directories: usize,
    /// Maximum simultaneously tracked readiness candidates.
    #[serde(default = "default_candidates")]
    pub max_candidates: usize,
}

fn default_quiet() -> u32 {
    30_000
}
fn default_directories() -> usize {
    1024
}
fn default_candidates() -> usize {
    4096
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
        if !config.consume.is_empty() && config.max_concurrent_requests < 2 {
            return Err("consume requires at least two RPC connections".into());
        }
        ConsumeWatch::validate_fields(&config.consume)?;
        crate::syslog_listener::validate(&config.syslog)?;
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

impl ConsumeWatch {
    pub(crate) fn validate(watches: &[Self]) -> Result<(), Box<dyn std::error::Error>> {
        Self::validate_fields(watches)?;
        let mut roots = Vec::new();
        for watch in watches {
            let root = watch.root.canonicalize()?;
            if roots
                .iter()
                .any(|other: &PathBuf| root.starts_with(other) || other.starts_with(&root))
            {
                return Err("consume roots must not overlap".into());
            }
            roots.push(root);
        }
        Ok(())
    }

    pub(crate) fn validate_fields(watches: &[Self]) -> Result<(), Box<dyn std::error::Error>> {
        if watches.len() > MAX_CONSUME_ROOTS {
            return Err("consume requires at most eight roots".into());
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut roots = Vec::new();
        for watch in watches {
            if watch.id.is_empty()
                || watch.id.len() > 128
                || !ids.insert(&watch.id)
                || !watch.root.is_absolute()
                || watch.root.as_os_str().len() > 2048
                || watch.source.is_empty()
                || watch.source.len() > 512
                || watch.quiet_ms > 600_000
                || !(1..=4096).contains(&watch.max_directories)
                || !(1..=16384).contains(&watch.max_candidates)
            {
                return Err("consume requires unique bounded identities, absolute roots, quiet_ms <= 600000, max_directories 1..4096 and max_candidates 1..16384".into());
            }
            sparrow_client::parse_format(&watch.format)?;
            if roots.iter().any(|other: &PathBuf| {
                watch.root.starts_with(other) || other.starts_with(&watch.root)
            }) {
                return Err("consume roots must not overlap".into());
            }
            roots.push(watch.root.clone());
        }
        Ok(())
    }
}
