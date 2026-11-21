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
    /// Filesystem free space below which the daemon refuses writes with the floor-labelled
    /// `STORAGE_PRESSURE`. It keeps accepting connections and serves reads; consume and tail
    /// watches and startup wait.
    #[serde(default = "default_min_free")]
    pub min_free_bytes: u64,
    /// Maximum records in a complete publication.
    pub max_records: usize,
    /// Maximum direct members in a pin collection.
    pub max_pin_members: usize,
    /// Bound on simultaneous storage operations, shared with intake workers.
    pub max_concurrent_requests: usize,
    /// Independent bound on admitted Unix and TCP RPC connections.
    #[serde(default = "default_rpc_connections")]
    pub max_rpc_connections: usize,
    /// Optional group-accessible ingest-only Unix socket. Its parent directory is outside
    /// `data_dir`; startup requires it to be owned by the daemon, group
    /// `ingest_socket_gid` and mode 2750.
    #[serde(default)]
    pub ingest_socket_path: Option<PathBuf>,
    /// Numeric group whose members may connect, required exactly when the socket is set.
    #[serde(default)]
    pub ingest_socket_gid: Option<u32>,
    /// Ingest-socket connection ceiling, separate from `max_rpc_connections` so producers
    /// cannot starve CLI and MCP connections.
    #[serde(default = "default_ingest_connections")]
    pub max_ingest_connections: usize,
    /// Required native analyzer artifact directory.
    pub text_lexicon_dir: PathBuf,
    /// Archive-before-delete. A store that has archived requires this table thereafter.
    #[serde(default)]
    pub archive: Option<ArchiveConfig>,
    /// Serve reads only: no intake, maintenance or write operation. `sparrowd --query-only`
    /// sets it too. It refuses every intake and archive key.
    #[serde(default)]
    pub query_only: bool,
    /// Role recorded when the store is created and required of an existing store:
    /// `production` or `scratch`. Absent records nothing and accepts any store.
    #[serde(default, deserialize_with = "deserialize_instance_role")]
    pub instance_role: Option<sparrow_core::InstanceRole>,
}

fn deserialize_instance_role<'de, D>(
    deserializer: D,
) -> Result<Option<sparrow_core::InstanceRole>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|word| sparrow_core::InstanceRole::parse(&word).map_err(serde::de::Error::custom))
        .transpose()
}

/// Whether `path` has no `..` component. A path with one names a directory the lexical
/// overlap checks cannot see, so every configured path whose position those checks guard
/// (archive directory, ingest socket, watch roots) must pass this.
#[must_use]
pub(crate) fn without_parent_dir(path: &Path) -> bool {
    path.components()
        .all(|part| !matches!(part, std::path::Component::ParentDir))
}

/// Paths the daemon writes, by configuration key. `server.toml` names no log file, so these are
/// the paths a watch root could swallow. Checks are lexical, like the other configuration
/// checks; the runtime root open also compares canonical paths with the store.
#[derive(Clone, Debug, Default)]
pub struct OutputPaths(Vec<(&'static str, PathBuf)>);

impl OutputPaths {
    /// Refuse the first watch whose root contains, equals or lies inside an output path,
    /// naming the watch, the key and both paths. Applied to the seed entries in `server.toml`,
    /// to the stored catalog at startup and to every `watch put`.
    pub fn refuse_overlap(
        &self,
        watches: &[ConsumeWatch],
    ) -> Result<(), Box<dyn std::error::Error>> {
        for watch in watches {
            for (key, path) in &self.0 {
                if !without_parent_dir(path) {
                    return Err(format!(
                        "{key} {} contains '..', which the overlap checks against watch `{}` cannot see; write the path without it",
                        path.display(),
                        watch.id
                    )
                    .into());
                }
                if crate::consumption::overlaps_quarantine(&watch.root, path) {
                    return Err(format!(
                        "watch `{}` root {} has its quarantine directory at {}, which overlaps {key} {}: a refused source is moved there, so no path Sparrow writes may share it; move one of them",
                        watch.id,
                        watch.root.display(),
                        crate::consumption::quarantine_path(&watch.root)
                            .unwrap_or_default()
                            .display(),
                        path.display()
                    )
                    .into());
                }
                if path.starts_with(&watch.root) || watch.root.starts_with(path) {
                    return Err(format!(
                        "watch `{}` root {} overlaps {key} {}: a watch root must neither contain nor lie inside a path Sparrow writes itself; move one of them",
                        watch.id,
                        watch.root.display(),
                        path.display()
                    )
                    .into());
                }
            }
        }
        Ok(())
    }
}

/// Archive-before-delete destination and segment policy; unknown fields are errors.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveConfig {
    /// Absolute archive root, outside `data_dir`. Startup probes it on its own thread.
    pub dir: PathBuf,
    /// Fraction of the retention window or data budget that starts archiving.
    #[serde(default = "default_start_fraction")]
    pub start_fraction: f64,
    /// Payload bytes that close a segment.
    #[serde(default = "default_segment_bytes")]
    pub max_segment_bytes: u64,
    /// Eviction entries that close a segment.
    #[serde(default = "default_segment_records")]
    pub max_segment_records: u64,
    /// Quads that close a segment.
    #[serde(default = "default_segment_quads")]
    pub max_segment_quads: u64,
    /// Seconds the archive worker waits after a pass that found nothing due, or failed, before
    /// it asks the store again. What is due is the store's call: `start_fraction`, 75 per cent
    /// of the retention window or data budget by default.
    #[serde(default = "default_check_secs")]
    pub check_secs: u64,
    /// Seconds before an archive step in flight is journaled as stalled, and again every this
    /// many seconds while it lasts.
    #[serde(default = "default_stall_warn")]
    pub stall_warn_secs: u64,
    /// Seconds an archive step may run before the pass counts as overrun: the segment is not
    /// committed on that pass and is verified again on the next. A blocked call cannot be
    /// cancelled, so no further step starts until it returns.
    #[serde(default = "default_io_timeout")]
    pub io_timeout_secs: u64,
}

fn default_start_fraction() -> f64 {
    sparrow_core::ARCHIVE_START_FRACTION
}
fn default_segment_bytes() -> u64 {
    sparrow_core::ARCHIVE_SEGMENT_BYTES
}
fn default_segment_records() -> u64 {
    sparrow_core::ARCHIVE_SEGMENT_RECORDS
}
fn default_segment_quads() -> u64 {
    sparrow_core::ARCHIVE_SEGMENT_QUADS
}
fn default_check_secs() -> u64 {
    60
}
fn default_stall_warn() -> u64 {
    120
}
fn default_io_timeout() -> u64 {
    900
}

impl ArchiveConfig {
    /// The store-side policy; the destination and timers stay with the daemon.
    #[must_use]
    pub fn policy(&self) -> sparrow_core::ArchivePolicy {
        sparrow_core::ArchivePolicy {
            start_fraction: self.start_fraction,
            max_segment_bytes: self.max_segment_bytes,
            max_segment_records: self.max_segment_records,
            max_segment_quads: self.max_segment_quads,
        }
    }

    /// Lexical checks only: probing the archive device may block, so it is not done here.
    fn validate(&self, data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let normal = |path: &Path| path.is_absolute() && without_parent_dir(path);
        if !normal(&self.dir) || self.dir.as_os_str().len() > 2048 {
            return Err(
                "archive dir must be an absolute path of at most 2048 bytes without '..'".into(),
            );
        }
        if self.dir.starts_with(data_dir) || data_dir.starts_with(&self.dir) {
            return Err("archive dir must be outside data_dir and must not contain it".into());
        }
        if self.check_secs == 0 {
            return Err("archive check_secs must be positive".into());
        }
        if self.stall_warn_secs == 0 || self.io_timeout_secs <= self.stall_warn_secs {
            return Err("archive requires 0 < stall_warn_secs < io_timeout_secs".into());
        }
        self.policy().validate()?;
        Ok(())
    }
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
    /// Holds publication of this watch's finished-file claims while set. A claim made during
    /// the hold is reserved and stays journaled and in flight, exactly as an interrupted claim
    /// would, and publishes in order when the hold clears. It is an operator's deliberate pause
    /// (and the rollback rehearsal's way to keep a claim in flight), never a way to refuse input.
    /// Omitted from the stored form when false, so an unheld catalog is what older builds read.
    #[serde(default, skip_serializing_if = "is_false")]
    pub publish_hold: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
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

fn default_ingest_connections() -> usize {
    16
}

impl ServerConfig {
    /// The directory of the syslog TCP spill files: `syslog-spill` beside the data directory, so a
    /// frame the listener could not store at a stop is kept outside the store that refused it.
    /// A configuration with a syslog listener and a data directory with no parent is refused at read.
    #[must_use]
    pub fn syslog_spill_dir(&self) -> PathBuf {
        self.data_dir.components().as_path().parent().map_or_else(
            || PathBuf::from("syslog-spill"),
            |parent| parent.join("syslog-spill"),
        )
    }

    /// Read and validate the same typed configuration used by the production daemon.
    pub fn read(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let text = std::fs::read_to_string(path)?;
        if text
            .parse::<toml::Table>()?
            .contains_key("max_artifact_bytes")
        {
            return Err(format!(
                "{}: max_artifact_bytes was removed; large artifacts now ingest through the staged import within disk_budget and min_free_bytes, up to the fixed 7.93 GiB import bound. Delete the key and restart",
                path.display()
            )
            .into());
        }
        let config: Self = toml::from_str(&text)?;
        if !(1..=64).contains(&config.max_concurrent_requests) {
            return Err("max_concurrent_requests must be 1..64".into());
        }
        if !(sparrow_core::MIN_FLUSH_INTERVAL_MS..=sparrow_core::MAX_FLUSH_INTERVAL_MS)
            .contains(&config.buffered_flush_interval_ms)
        {
            return Err(format!(
                "buffered_flush_interval_ms = {} is outside {}..{}; the periodic flush never fires more than once a second. Use a value from {} to {} milliseconds (default {})",
                config.buffered_flush_interval_ms,
                sparrow_core::MIN_FLUSH_INTERVAL_MS,
                sparrow_core::MAX_FLUSH_INTERVAL_MS,
                sparrow_core::MIN_FLUSH_INTERVAL_MS,
                sparrow_core::MAX_FLUSH_INTERVAL_MS,
                sparrow_core::DEFAULT_FLUSH_INTERVAL_MS
            )
            .into());
        }
        if !(1..=256).contains(&config.max_rpc_connections) {
            return Err("max_rpc_connections must be 1..256".into());
        }
        if config.socket_path.parent() != Some(config.data_dir.as_path()) {
            return Err("socket_path must be directly inside the private data_dir".into());
        }
        if !config.syslog.is_empty() && config.data_dir.components().as_path().parent().is_none() {
            return Err("syslog listeners keep their spill directory beside data_dir, so data_dir needs a parent directory".into());
        }
        config.validate_ingest_socket()?;
        if !config.consume.is_empty() && config.max_concurrent_requests < 2 {
            return Err("consume requires at least two concurrent storage operations".into());
        }
        ConsumeWatch::validate_fields(&config.consume)?;
        if let Some(archive) = &config.archive {
            archive.validate(&config.data_dir)?;
        }
        config.output_paths().refuse_overlap(&config.consume)?;
        crate::syslog_listener::validate(&config.syslog)?;
        if config.rpc_tcp_address.is_some() != config.rpc_tcp_secret_file.is_some() {
            return Err(
                "rpc_tcp_address and rpc_tcp_secret_file must be configured together".into(),
            );
        }
        config.check_query_only()?;
        Ok(config)
    }

    /// Select query-only mode, as `sparrowd --query-only` does, and check that nothing in the
    /// configuration would ingest, listen publicly or archive.
    pub fn into_query_only(mut self) -> Result<Self, Box<dyn std::error::Error>> {
        self.query_only = true;
        self.check_query_only()?;
        Ok(self)
    }

    fn check_query_only(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.query_only {
            return Ok(());
        }
        self.refuse_intake("query_only", "a query-only daemon", true)
    }

    /// Check the configuration for `sparrowd --no-intake`: a writable daemon with its
    /// maintenance duty that starts no intake, public listener or archive, so that a disposable
    /// copy of a store can run its online migrations without consuming the live intake that
    /// its stored watch catalog names.
    pub fn check_no_intake(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.query_only {
            return Err("--no-intake and query-only mode exclude each other".into());
        }
        self.refuse_intake("--no-intake", "a no-intake daemon", true)
    }

    /// Check the configuration for `sparrowd --private-intake DIR`: a writable daemon with
    /// full intake for a disposable copy of a store, whose watches consume private roots
    /// below DIR and whose ingest socket is the copy's own. It refuses configured consume
    /// roots, syslog listeners, the public listener and the archive, whose endpoints and
    /// paths would be the live ones.
    pub fn check_private_intake(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.query_only {
            return Err("--private-intake and query-only mode exclude each other".into());
        }
        self.refuse_intake("--private-intake", "a private-intake daemon", false)
    }

    /// Refuse the first configured intake, public listener or archive key for `mode`; the
    /// ingest socket only when `ingest_socket` says so.
    fn refuse_intake(
        &self,
        mode: &str,
        daemon: &str,
        ingest_socket: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conflicting = [
            ("consume", !self.consume.is_empty()),
            ("syslog", !self.syslog.is_empty()),
            ("rpc_tcp_address", self.rpc_tcp_address.is_some()),
            (
                "ingest_socket_path",
                ingest_socket && self.ingest_socket_path.is_some(),
            ),
            ("archive", self.archive.is_some()),
        ];
        match conflicting.iter().find(|(_, set)| *set) {
            Some((key, _)) => Err(format!(
                "{mode} refuses the configured `{key}`: {daemon} starts no intake, public listener or archive; remove `{key}` from this configuration"
            )
            .into()),
            None => Ok(()),
        }
    }

    /// Lexical checks; startup checks the directory's owner, group and mode.
    fn validate_ingest_socket(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !(1..=256).contains(&self.max_ingest_connections) {
            return Err("max_ingest_connections must be 1..256".into());
        }
        if self.ingest_socket_path.is_some() != self.ingest_socket_gid.is_some() {
            return Err(
                "ingest_socket_path and ingest_socket_gid must be configured together".into(),
            );
        }
        let Some(path) = &self.ingest_socket_path else {
            return Ok(());
        };
        let parent = path.parent().filter(|_| {
            path.is_absolute() && path.file_name().is_some() && without_parent_dir(path)
        });
        let Some(parent) = parent else {
            return Err(
                "ingest_socket_path must be an absolute socket path without '..' inside a directory"
                    .into(),
            );
        };
        if parent.starts_with(&self.data_dir) || self.data_dir.starts_with(parent) {
            return Err(
                "ingest_socket_path must be in a directory outside data_dir that does not contain it"
                    .into(),
            );
        }
        Ok(())
    }

    /// The paths the daemon itself writes: no watch root may contain or lie inside one.
    #[must_use]
    pub fn output_paths(&self) -> OutputPaths {
        let optional = |key, path: Option<&PathBuf>| path.map(|path| (key, path.clone()));
        OutputPaths(
            [
                Some(("data_dir", self.data_dir.clone())),
                Some(("socket_path", self.socket_path.clone())),
                optional("ingest_socket_path", self.ingest_socket_path.as_ref()),
                optional(
                    "archive.dir",
                    self.archive.as_ref().map(|archive| &archive.dir),
                ),
            ]
            .into_iter()
            .flatten()
            .collect(),
        )
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

    /// The free space the daemon needs to open the store (a manifest and a WAL): the
    /// maintenance reserve, which is kept for exactly such writes, and never more than the
    /// floor. Below it the daemon waits to open; between it and `min_free_bytes` the daemon
    /// opens and the floor pauses writes, so reads are served and reclaim can run.
    #[must_use]
    pub fn open_floor(&self) -> sparrow_core::SpaceFloor {
        sparrow_core::SpaceFloor::new(
            &self.data_dir,
            self.maintenance_reserve.min(self.min_free_bytes),
        )
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
                max_records: self.max_records,
                max_pin_members: self.max_pin_members,
                text_analyzer: sparrow_core::resolve_text_analyzer(&self.text_lexicon_dir)?,
                archive: self.archive.as_ref().map(ArchiveConfig::policy),
                retire_without_archive: false,
                instance_role: self.instance_role,
            },
        )?;
        // The daemon's group may traverse to the socket it binds here (mode 0660); it can
        // neither list nor read the store, whose files the unit's umask keeps owner-only.
        std::fs::set_permissions(&self.data_dir, std::fs::Permissions::from_mode(0o710))?;
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
                || !without_parent_dir(&watch.root)
                || watch.root.as_os_str().len() > 2048
                || watch.source.is_empty()
                || watch.source.len() > 512
                || watch.quiet_ms > 600_000
                || !(1..=4096).contains(&watch.max_directories)
                || !(1..=16384).contains(&watch.max_candidates)
            {
                return Err("consume requires unique bounded identities, absolute roots without '..', quiet_ms <= 600000, max_directories 1..4096 and max_candidates 1..16384".into());
            }
            if watch.producer_contract == ProducerContract::ActiveFilesRetain
                && (!["log", "jsonl"].contains(&watch.format.as_str())
                    || watch.max_candidates > sparrow_core::MAX_TAIL_GENERATIONS)
            {
                return Err(
                    "tail requires log/jsonl and max_candidates <= 64 retained descriptors".into(),
                );
            }
            if watch.publish_hold && watch.producer_contract == ProducerContract::ActiveFilesRetain
            {
                return Err(
                    "publish_hold applies to finished-file watches only; a tail is never held"
                        .into(),
                );
            }
            sparrow_client::parse_format(&watch.format)?;
            if roots.iter().any(|other: &PathBuf| {
                watch.root.starts_with(other) || other.starts_with(&watch.root)
            }) {
                return Err("consume roots must not overlap".into());
            }
            if roots.iter().any(|other: &PathBuf| {
                crate::consumption::overlaps_quarantine(&watch.root, other)
                    || crate::consumption::overlaps_quarantine(other, &watch.root)
            }) {
                return Err("a consume root must not be, contain or lie inside another root's quarantine directory (ROOT.sparrow-quarantine beside it); the daemon would read refused sources as input".into());
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

    /// INVARIANT: the removed `max_artifact_bytes` knob refuses startup with an
    /// actionable message instead of a generic unknown-field error.
    #[test]
    fn removed_artifact_limit_key_refuses_with_its_removal() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let config =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let base = std::fs::read_to_string(&config).unwrap();
        std::fs::write(&config, format!("max_artifact_bytes = 16777216\n{base}")).unwrap();
        let error = ServerConfig::read(&config).err().unwrap().to_string();
        assert!(error.contains("max_artifact_bytes was removed"), "{error}");
        assert!(error.contains("Delete the key"), "{error}");
    }

    /// INVARIANT: the periodic flush never fires more than once a second; a smaller or
    /// zero interval refuses startup with a message naming the key and the minimum, and
    /// the one second boundary itself is accepted.
    #[test]
    fn flush_interval_below_one_second_refuses_with_its_minimum() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let config =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let base = std::fs::read_to_string(&config).unwrap();
        let with = |millis: u32| {
            std::fs::write(
                &config,
                format!("buffered_flush_interval_ms = {millis}\n{base}"),
            )
            .unwrap();
            ServerConfig::read(&config)
        };
        for millis in [0, 1, 999] {
            let error = with(millis).err().unwrap().to_string();
            assert!(error.contains("buffered_flush_interval_ms"), "{error}");
            assert!(error.contains("1000"), "{error}");
        }
        assert_eq!(with(1000).unwrap().buffered_flush_interval_ms, 1000);
    }

    /// INVARIANT: the `[archive]` table is strict and lexically outside the store, and a
    /// store that has been fenced for archiving refuses to open once the table is removed.
    #[test]
    fn archive_table_is_strict_and_a_fenced_store_requires_it() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let config =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let base = std::fs::read_to_string(&config).unwrap();
        let with = |extra: &str| {
            std::fs::write(&config, format!("{base}{extra}")).unwrap();
            ServerConfig::read(&config)
        };
        let archive = dir.path().join("nas");
        let table = format!("[archive]\ndir = \"{}\"\n", archive.display());
        for refused in [
            format!("{table}unknown = 1\n"),
            format!("{table}start_fraction = 0.99\n"),
            format!("{table}stall_warn_secs = 900\n"),
            format!("{table}max_segment_bytes = 1\n"),
            "[archive]\ndir = \"relative\"\n".to_owned(),
            format!("[archive]\ndir = \"{}\"\n", data.join("archive").display()),
            format!("[archive]\ndir = \"{}/../nas\"\n", data.display()),
        ] {
            assert!(with(&refused).is_err(), "{refused}");
        }
        let archiving = with(&table).unwrap();
        assert_eq!(
            archiving.archive.as_ref().unwrap().policy(),
            sparrow_core::ArchivePolicy::default()
        );
        drop(archiving.open_store().unwrap());
        let unconfigured = with("").unwrap();
        assert!(unconfigured.open_store().is_err());
    }

    /// INVARIANT: a consume or tail root that contains, or lies inside, any path the daemon
    /// writes itself (store, sockets, archive) refuses configuration load and
    /// names both paths, because the daemon would otherwise watch its own output.
    #[test]
    fn watch_root_overlapping_a_daemon_output_path_refuses_load() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("store");
        let config =
            sparrow_test_support::write_server_config(dir.path(), &data, &data.join("rpc.sock"));
        let base = std::fs::read_to_string(&config).unwrap();
        let watch = |root: &std::path::Path| {
            format!(
                "[[consume]]\nid = \"w\"\nroot = \"{}\"\nproducer_contract = \"finished_files\"\nsource = \"s\"\nformat = \"log\"\n",
                root.display()
            )
        };
        let with = |extra: &str| {
            std::fs::write(&config, format!("{base}{extra}")).unwrap();
            ServerConfig::read(&config)
        };
        let ingest = dir.path().join("run/ingest.sock");
        let ingest_keys = format!(
            "ingest_socket_path = \"{}\"\ningest_socket_gid = 1\n",
            ingest.display()
        );
        let archive = dir.path().join("nas");
        let outputs = format!("{ingest_keys}[archive]\ndir = \"{}\"\n", archive.display());
        // Top-level keys must precede the tables, so the watch goes last.
        for root in [
            dir.path().to_path_buf(),
            data.clone(),
            dir.path().join("run"),
            archive.clone(),
        ] {
            let error = with(&format!("{outputs}{}", watch(&root)))
                .err()
                .unwrap()
                .to_string();
            assert!(error.contains("overlaps"), "{} {error}", root.display());
            assert!(error.contains(&root.display().to_string()), "{error}");
        }
        with(&format!(
            "{outputs}{}",
            watch(&dir.path().join("intake/app"))
        ))
        .unwrap();
    }
    fn root(id: &str, path: &str) -> super::ConsumeWatch {
        super::ConsumeWatch {
            id: id.into(),
            root: path.into(),
            producer_contract: super::ProducerContract::FinishedFiles,
            source: id.into(),
            format: "jsonl".into(),
            quiet_ms: 1000,
            max_directories: 1,
            max_candidates: 1,
            publish_hold: false,
        }
    }

    /// INVARIANT (#417): no root and no path Sparrow writes may be a root's quarantine
    /// directory (`ROOT.sparrow-quarantine` beside it), whichever is configured first, because
    /// the daemon would read refused sources as input or write into them. `watch put` runs
    /// `validate_fields` (through `validate`) and `refuse_overlap` on the whole next catalog,
    /// so the same two checks guard it.
    #[test]
    fn a_quarantine_directory_is_never_another_root_or_a_daemon_output() {
        let roots = [
            ("/p/x", "/p/x.sparrow-quarantine", false),
            ("/p/x.sparrow-quarantine", "/p/x", false),
            ("/p/x", "/p/x.sparrow-quarantine/inner", false),
            ("/p/x", "/p/q/../x.sparrow-quarantine", false),
            ("/p/q/../x.sparrow-quarantine", "/p/x", false),
            ("/p/q/../x", "/p/x.sparrow-quarantine", false),
            ("/p/x.sparrow-quarantine", "/p/q/../x", false),
            ("/p/x", "/p/x.other", true),
            ("/p/x", "/p/y.sparrow-quarantine", true),
        ];
        for (first, second, accepted) in roots {
            let watches = [root("a", first), root("b", second)];
            assert_eq!(
                super::ConsumeWatch::validate_fields(&watches).is_ok(),
                accepted,
                "{first} then {second}"
            );
        }
        let outputs = [
            ("/p/x.sparrow-quarantine", false),
            ("/p/x.sparrow-quarantine/archive", false),
            ("/p/q/../x.sparrow-quarantine", false),
            ("/p/z", true),
        ];
        for (output, accepted) in outputs {
            let paths = super::OutputPaths(vec![("archive.root", output.into())]);
            assert_eq!(
                paths.refuse_overlap(&[root("a", "/p/x")]).is_ok(),
                accepted,
                "{output}"
            );
        }
    }
}
