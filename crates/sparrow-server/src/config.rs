// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

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
    /// Optional public Cap'n Proto TCP binding. Anonymous peers may only discover, ingest,
    /// flush and import; other operations require a CHAP-authenticated session.
    #[serde(default)]
    pub rpc_tcp_address: Option<std::net::SocketAddr>,
    /// Private shared-secret file, required exactly when `rpc_tcp_address` is set.
    #[serde(default)]
    pub rpc_tcp_secret_file: Option<PathBuf>,
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
    /// Filesystem free space below which the daemon pauses connections and writes.
    #[serde(default = "default_min_free")]
    pub min_free_bytes: u64,
    /// Bounded direct ingestion admission.
    pub max_artifact_bytes: usize,
    /// Maximum records in a complete publication.
    pub max_records: usize,
    /// Maximum direct members in a pin collection.
    pub max_pin_members: usize,
    /// Bound on simultaneous storage operations, shared with intake workers.
    pub max_concurrent_requests: usize,
    /// Independent bound on admitted Unix and TCP RPC connections.
    #[serde(default = "default_rpc_connections")]
    pub max_rpc_connections: usize,
    /// Required native analyzer artifact directory.
    pub text_lexicon_dir: PathBuf,
}

/// Producer contract required before a watcher can claim or retire source files.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProducerContract {
    /// Producers hand off finished files and never append after handoff.
    FinishedFiles,
    /// Observe append-only active files without claiming or removing them.
    ActiveFilesRetain,
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

/// Room for RocksDB flushes and compaction after other writers fill the disk.
fn default_min_free() -> u64 {
    4 * 1024 * 1024 * 1024
}

fn default_rpc_connections() -> usize {
    16
}

impl ServerConfig {
    /// Read and validate the same typed configuration used by the production daemon.
    pub fn read(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let config: Self = toml::from_str(&std::fs::read_to_string(path)?)?;
        if !(1..=64).contains(&config.max_concurrent_requests) {
            return Err("max_concurrent_requests must be 1..64".into());
        }
        if !(1..=256).contains(&config.max_rpc_connections) {
            return Err("max_rpc_connections must be 1..256".into());
        }
        if config.socket_path.parent() != Some(config.data_dir.as_path()) {
            return Err("socket_path must be directly inside the private data_dir".into());
        }
        if !config.consume.is_empty() && config.max_concurrent_requests < 2 {
            return Err("consume requires at least two concurrent storage operations".into());
        }
        ConsumeWatch::validate_fields(&config.consume)?;
        crate::syslog_listener::validate(&config.syslog)?;
        if config.rpc_tcp_address.is_some() != config.rpc_tcp_secret_file.is_some() {
            return Err(
                "rpc_tcp_address and rpc_tcp_secret_file must be configured together".into(),
            );
        }
        Ok(config)
    }

    /// Read the configured TCP CHAP secret; a configured but unusable file is an error.
    pub fn tcp_secret(
        &self,
    ) -> Result<Option<sparrow_client::TcpSecret>, Box<dyn std::error::Error>> {
        self.rpc_tcp_secret_file
            .as_deref()
            .map(|path| {
                sparrow_client::TcpSecret::read(path).map_err(|error| {
                    format!("rpc_tcp_secret_file {}: {error}", path.display()).into()
                })
            })
            .transpose()
    }

    /// The free-space floor the store will enforce, available before it opens.
    #[must_use]
    pub fn space_floor(&self) -> sparrow_core::SpaceFloor {
        sparrow_core::SpaceFloor::new(&self.data_dir, self.min_free_bytes)
    }

    /// Open the one storage owner with native analyzer artifacts and private permissions.
    pub fn open_store(&self) -> Result<Store, Box<dyn std::error::Error>> {
        let store = Store::open(
            &self.data_dir,
            Config {
                buffered_flush_interval_ms: self.buffered_flush_interval_ms,
                disk_budget: self.disk_budget,
                maintenance_reserve: self.maintenance_reserve,
                min_free_bytes: self.min_free_bytes,
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
            if watch.producer_contract == ProducerContract::ActiveFilesRetain
                && (!["log", "jsonl"].contains(&watch.format.as_str())
                    || watch.max_candidates > sparrow_core::MAX_TAIL_GENERATIONS)
            {
                return Err(
                    "tail requires log/jsonl and max_candidates <= 64 retained descriptors".into(),
                );
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

#[cfg(test)]
mod tests {
    use super::ServerConfig;

    /// INVARIANT: public TCP never starts without its CHAP secret, a secret is never
    /// configured without TCP, and an over-permissive secret file refuses startup.
    #[test]
    fn public_tcp_and_its_chap_secret_are_configured_together_and_privately() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let config =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let base = std::fs::read_to_string(&config).unwrap();
        let secret = dir.path().join("tcp.secret");
        std::fs::write(&secret, [9; 32]).unwrap();
        let with = |extra: &str| {
            std::fs::write(&config, format!("{base}{extra}")).unwrap();
            ServerConfig::read(&config)
        };
        assert!(with("rpc_tcp_address = \"127.0.0.1:5180\"\n").is_err());
        let secret_line = format!("rpc_tcp_secret_file = \"{}\"\n", secret.display());
        assert!(with(&secret_line).is_err());
        let both = with(&format!(
            "rpc_tcp_address = \"127.0.0.1:5180\"\n{secret_line}"
        ))
        .unwrap();
        std::fs::set_permissions(
            &secret,
            <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o644),
        )
        .unwrap();
        assert!(both.tcp_secret().is_err());
        std::fs::set_permissions(
            &secret,
            <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o600),
        )
        .unwrap();
        assert!(both.tcp_secret().unwrap().is_some());
    }
}
