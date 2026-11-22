// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! File-backed configuration for the same producer used by native Rust callers.

use crate::{DeliveryConfig, Endpoint, Expect, RequiredRole};
use std::{
    error::Error as StdError,
    io::{ErrorKind, Read},
    os::unix::fs::{FileTypeExt, OpenOptionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

pub(crate) const LOCAL_INGEST_SOCKET: &str = "/run/sparrow-ingest/ingest.sock";

type Cause = Box<dyn StdError + Send + Sync>;

/// The file and underlying cause of a failed producer configuration load.
#[derive(Debug)]
pub struct ConfigError {
    path: PathBuf,
    cause: Cause,
}

impl ConfigError {
    fn at(path: &Path, cause: impl Into<Cause>) -> Self {
        Self {
            path: path.to_owned(),
            cause: cause.into(),
        }
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(output, "loading {}: {}", self.path.display(), self.cause)
    }
}

impl StdError for ConfigError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.cause.as_ref())
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    mode: Option<FileMode>,
    spool_dir: Option<PathBuf>,
    spool_max_bytes: Option<u64>,
    spool_segment_bytes: Option<u64>,
    spool_sync_interval_ms: Option<u64>,
    spool_max_wait_ms: Option<u64>,
    source: Option<String>,
    endpoint: Option<String>,
    secret_file: Option<PathBuf>,
    expected_role: Option<RequiredRole>,
    expected_store: Option<String>,
    quarantine_dir: Option<PathBuf>,
    max_queued_events: Option<usize>,
    max_queued_bytes: Option<usize>,
    batch_events: Option<usize>,
    batch_bytes: Option<usize>,
    flush_interval_ms: Option<u64>,
    rpc_timeout_ms: Option<u64>,
    retry_interval_ms: Option<u64>,
    retry_max_interval_ms: Option<u64>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum FileMode {
    Memory,
    Lossy,
    LossySpooled,
    Lossless,
}

impl DeliveryConfig {
    /// Load an explicit TOML file and its optional TCP secret on Tokio's blocking pool.
    /// Relative paths are resolved beside the TOML file. Supplying only a source selects
    /// the local ingest socket. Unknown keys, I/O and validation failures name the file.
    pub async fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref().to_path_buf();
        let reader_path = path.clone();
        tokio::task::spawn_blocking(move || FileConfig::read(&reader_path, None))
            .await
            .map_err(|cause| ConfigError::at(&path, cause))?
            .map_err(|cause| ConfigError::at(&path, cause))
    }

    /// Resolve startup configuration without connecting or creating state. A file named
    /// by `SPARROW_CLIENT_CONFIG` takes precedence over the user's `sparrow/client.toml`.
    /// A file may override the caller's source. With neither file, the local ingest
    /// socket selects default spooled delivery if present. Its absence returns `None`.
    pub async fn discover(source: impl Into<String>) -> Result<Option<Self>, ConfigError> {
        let source = source.into();
        tokio::task::spawn_blocking(move || {
            let env_path = |key| std::env::var_os(key).map(PathBuf::from);
            discover_config(
                source,
                env_path("SPARROW_CLIENT_CONFIG"),
                env_path("XDG_CONFIG_HOME"),
                env_path("HOME"),
                Path::new(LOCAL_INGEST_SOCKET),
            )
        })
        .await
        .map_err(|cause| ConfigError::at(Path::new("producer discovery"), cause))?
    }
}

fn discover_config(
    source: String,
    explicit: Option<PathBuf>,
    config_home: Option<PathBuf>,
    home: Option<PathBuf>,
    socket: &Path,
) -> Result<Option<DeliveryConfig>, ConfigError> {
    let read = |path: &Path| {
        FileConfig::read(path, Some(source.as_str()))
            .map(Some)
            .map_err(|cause| ConfigError::at(path, cause))
    };
    if let Some(path) = explicit {
        if path.as_os_str().is_empty() {
            return Err(ConfigError::at(
                Path::new("SPARROW_CLIENT_CONFIG"),
                "expected a configuration file path",
            ));
        }
        return read(&path);
    }
    let config_dir = config_home
        .filter(|p| p.is_absolute())
        .or_else(|| home.filter(|p| p.is_absolute()).map(|p| p.join(".config")));
    if let Some(dir) = config_dir {
        let path = dir.join("sparrow/client.toml");
        if entry_metadata(&path)?.is_some() {
            return read(&path);
        }
    }
    let Some(mut meta) = entry_metadata(socket)? else {
        return Ok(None);
    };
    if meta.is_symlink() {
        meta = std::fs::metadata(socket).map_err(|cause| ConfigError::at(socket, cause))?;
    }
    if !meta.file_type().is_socket() {
        return Err(ConfigError::at(socket, "expected a Unix ingest socket"));
    }
    let config = DeliveryConfig::configured(Endpoint::Unix(socket.to_owned()), source);
    config
        .validate()
        .map_err(|cause| ConfigError::at(socket, cause))?;
    Ok(Some(config))
}

// Inspect the directory entry first. A dangling selected symlink and a missing
// secret referenced by a selected file both produce configuration errors.
fn entry_metadata(path: &Path) -> Result<Option<std::fs::Metadata>, ConfigError> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => Ok(Some(meta)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(ConfigError::at(path, e)),
    }
}

impl FileConfig {
    fn read(path: &Path, caller_source: Option<&str>) -> Result<DeliveryConfig, Cause> {
        const MAX_CONFIG_BYTES: u64 = 64 * 1024;
        let input = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NONBLOCK.bits() as i32)
            .open(path)?;
        if !input.metadata()?.is_file() {
            return Err("expected a regular producer configuration file".into());
        }
        let mut text = String::new();
        input.take(MAX_CONFIG_BYTES + 1).read_to_string(&mut text)?;
        if text.len() as u64 > MAX_CONFIG_BYTES {
            return Err("producer configuration exceeds 64 KiB".into());
        }
        let file: Self = toml::from_str(&text)?;
        let absolute = std::path::absolute(path)?;
        let dir = absolute
            .parent()
            .ok_or("configuration path requires a parent directory")?;
        let source = file
            .source
            .as_deref()
            .or(caller_source)
            .ok_or("producer configuration requires source")?;
        let mut config = DeliveryConfig::configured_local(source);
        if let Some(value) = file.endpoint {
            config.endpoint = match Endpoint::parse(&value)? {
                Endpoint::Unix(socket) => Endpoint::Unix(dir.join(socket)),
                endpoint => endpoint,
            };
        }
        if let Some(secret) = file.secret_file {
            let path = dir.join(secret);
            config.endpoint =
                config
                    .endpoint
                    .with_secret_file(&path)
                    .map_err(|cause| ConfigError {
                        path,
                        cause: Box::new(cause),
                    })?;
        }
        config.expect = match (file.expected_role, file.expected_store) {
            (Some(role), Some(id)) => Some(Expect::new(role).with_store_id(id)),
            (Some(role), None) => Some(Expect::new(role)),
            (None, store) => store.map(Expect::store),
        };
        let crate::DeliveryMode::LosslessConfirmed(mut spool) = config.mode else {
            return Err("configured producer lacks its default spool".into());
        };
        if let Some(path) = file.spool_dir {
            spool.dir = dir.join(path);
        }
        if let Some(n) = file.spool_max_bytes {
            spool.max_bytes = n;
        }
        if let Some(n) = file.spool_segment_bytes {
            spool.segment_bytes = n;
        }
        if let Some(n) = file.spool_sync_interval_ms {
            spool.sync_interval = Duration::from_millis(n);
        }
        if let Some(n) = file.spool_max_wait_ms {
            spool.max_wait = Duration::from_millis(n);
        }
        config.mode = match file.mode.unwrap_or(FileMode::Lossless) {
            FileMode::Memory => crate::DeliveryMode::LossyAspirational,
            FileMode::Lossy => crate::DeliveryMode::LossyMemory,
            FileMode::LossySpooled => crate::DeliveryMode::LossySpooled(spool),
            FileMode::Lossless => crate::DeliveryMode::LosslessConfirmed(spool),
        };
        if let Some(path) = file.quarantine_dir {
            config.quarantine_dir = dir.join(path);
        }
        config.max_queued_events = file.max_queued_events.unwrap_or(config.max_queued_events);
        config.max_queued_bytes = file.max_queued_bytes.unwrap_or(config.max_queued_bytes);
        config.batch_events = file.batch_events.unwrap_or(config.batch_events);
        config.batch_bytes = file.batch_bytes.unwrap_or(config.batch_bytes);
        if let Some(ms) = file.flush_interval_ms {
            config.flush_interval = Duration::from_millis(ms);
        }
        if let Some(ms) = file.rpc_timeout_ms {
            config.rpc_timeout = Duration::from_millis(ms);
        }
        if let Some(ms) = file.retry_interval_ms {
            config.retry_interval = Duration::from_millis(ms);
        }
        if let Some(ms) = file.retry_max_interval_ms {
            config.retry_max_interval = Duration::from_millis(ms);
        }
        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Startup retains explicit configuration errors, supports a shared destination
    /// with independent source spools, and recognizes a defunct socket as an outage.
    #[tokio::test]
    async fn discovery_preserves_source_ownership_and_distinguishes_absence_from_failure() {
        use std::os::unix::{fs::symlink, net::UnixListener};
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path();
        let socket = home.join("ingest.sock");
        let select = |source: &str, explicit: Option<&Path>, xdg: &Path| {
            discover_config(
                source.into(),
                explicit.map(Path::to_owned),
                Some(xdg.into()),
                Some(home.into()),
                &socket,
            )
        };
        let relative = Path::new("relative-config-home");
        assert!(select("app", None, relative).unwrap().is_none());

        std::fs::write(&socket, b"wrong file type").unwrap();
        assert!(
            select("app", None, relative)
                .unwrap_err()
                .to_string()
                .contains("Unix ingest socket")
        );
        std::fs::remove_file(&socket).unwrap();
        drop(UnixListener::bind(&socket).unwrap());
        let offline = select("app", None, relative).unwrap().unwrap();
        assert!(matches!(
            offline.mode,
            crate::DeliveryMode::LosslessConfirmed(_)
        ));

        let user_dir = home.join(".config/sparrow");
        std::fs::create_dir_all(&user_dir).unwrap();
        let user_file = user_dir.join("client.toml");
        std::fs::write(&user_file, r#"endpoint = "chosen.sock""#).unwrap();
        let a = select("app.a", None, relative).unwrap().unwrap();
        let b = select("app.b", None, relative).unwrap().unwrap();
        assert_eq!(a.endpoint, Endpoint::Unix(user_dir.join("chosen.sock")));
        assert_eq!((a.source.as_str(), b.source.as_str()), ("app.a", "app.b"));
        assert_ne!(a.mode.spool().unwrap().dir, b.mode.spool().unwrap().dir);
        assert!(
            DeliveryConfig::from_file(&user_file)
                .await
                .unwrap_err()
                .to_string()
                .contains("requires source")
        );

        let xdg = home.join("xdg");
        let dir = xdg.join("sparrow");
        std::fs::create_dir_all(&dir).unwrap();
        let xdg_file = dir.join("client.toml");
        symlink("missing.toml", &xdg_file).unwrap();
        assert!(
            select("app", None, &xdg)
                .unwrap_err()
                .to_string()
                .contains(xdg_file.to_str().unwrap())
        );
        std::fs::remove_file(&xdg_file).unwrap();
        std::fs::write(
            &xdg_file,
            "endpoint = 'tcp://127.0.0.1:518'\nsecret_file = 'missing.secret'",
        )
        .unwrap();
        assert!(
            select("app", None, &xdg)
                .unwrap_err()
                .to_string()
                .contains("missing.secret")
        );

        let explicit = home.join("explicit.toml");
        assert!(
            select("app", Some(&explicit), &xdg)
                .unwrap_err()
                .to_string()
                .contains("explicit.toml")
        );
        std::fs::write(&explicit, "source = 'pinned.app'\nendpoint = 'down.sock'").unwrap();
        let pinned = select("app", Some(&explicit), &xdg).unwrap().unwrap();
        assert_eq!(pinned.source, "pinned.app");
        assert_eq!(pinned.endpoint, Endpoint::Unix(home.join("down.sock")));
    }

    /// A remote producer file selects authenticated TCP and a store pin. A misspelled
    /// destination or an impossible limit fails at setup with its configuration path.
    #[tokio::test]
    async fn configured_producer_honors_remote_identity_and_refuses_invalid_settings() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("producer.toml");
        let secret = dir.path().join("secret");
        std::fs::write(&secret, [42u8; 32]).unwrap();
        std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
        let text = r#"source = "application"
endpoint = "tcp://127.0.0.1:518"
secret_file = "secret"
expected_store = "store:target"
quarantine_dir = "quarantine"
"#;
        std::fs::write(&config_path, text).unwrap();
        let settings = DeliveryConfig::from_file(&config_path).await.unwrap();
        assert!(matches!(settings.endpoint, Endpoint::AuthenticatedTcp(..)));
        assert_eq!(settings.expect, Some(Expect::store("store:target")));
        assert_eq!(settings.quarantine_dir, dir.path().join("quarantine"));
        for (key, value, diagnostic) in [
            ("destinaton", "\"ignored\"", "unknown field"),
            ("retry_interval_ms", "0", "retry_interval"),
        ] {
            std::fs::write(&config_path, format!("{text}{key} = {value}")).unwrap();
            let error = DeliveryConfig::from_file(&config_path).await.unwrap_err();
            let message = error.to_string();
            assert!(
                message.contains(diagnostic) && message.contains("producer.toml"),
                "{message}"
            );
        }
        // The spelling fits the socket limit, but its full pathname relative to
        // the configuration file does not. Reject it at setup, ahead of any retry.
        let relative = "s".repeat(100);
        std::fs::write(
            &config_path,
            format!("source = \"app\"\nendpoint = \"{relative}\""),
        )
        .unwrap();
        let error = DeliveryConfig::from_file(&config_path).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("shorten the resolved socket pathname"),
            "{error}"
        );
    }
}
