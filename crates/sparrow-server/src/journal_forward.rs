// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Forwards one systemd journal instance into Sparrow through lossless-confirmed delivery.
//!
//! The forwarder reads `journalctl --follow --output=export`, maps each entry with
//! [`crate::journal_export`], and sends it with its `__CURSOR` as the delivery token. The
//! cursor file names only entries the daemon has confirmed. A restart resumes after the
//! spool's accepted token, which covers entries that are on local disk but not yet confirmed.

use crate::journal_export::{ExportParser, MapError, Mapping, ParseError, map_entry};
use serde::Deserialize;
use sparrow_client::{
    Client, Delivery, DeliveryConfig, Endpoint, FlushError, ProducerEvent, Refusal, SendError,
    SpoolConfig, sparrow_capnp as wire, sparrow_eprintln,
};
use std::{
    fmt,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
    time::{Interval, MissedTickBehavior},
};

/// Bytes requested from `journalctl` per read.
const READ_BYTES: usize = 64 * 1024;
/// Interval at which the cursor file follows the daemon's confirmed token.
const CURSOR_INTERVAL: Duration = Duration::from_secs(1);
/// Interval of the stats line, and of the hold check it carries.
const STATS_INTERVAL: Duration = Duration::from_secs(60);
/// Pause before offering the same event again after the spool reports no room.
const ROOM_BACKOFF: Duration = Duration::from_millis(50);
/// Pause before retrying a diagnostic mapping that the daemon did not accept.
const DEFINE_BACKOFF: Duration = Duration::from_secs(5);
/// Spool budget when the configuration does not name one.
const DEFAULT_SPOOL_BYTES: u64 = 1 << 30;

/// The configuration file read by the forwarder. Unknown keys are refused.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Absolute path of the private Sparrow socket.
    socket: PathBuf,
    /// Absolute directory holding one subdirectory per instance.
    state_dir: PathBuf,
    /// Spool budget in bytes for each instance.
    #[serde(default = "default_spool_bytes")]
    spool_max_bytes: u64,
}

const fn default_spool_bytes() -> u64 {
    DEFAULT_SPOOL_BYTES
}

impl Config {
    /// Read and check the configuration file at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`ForwardError`] when the file cannot be read, is not valid TOML for this
    /// schema, or names a relative path.
    pub fn load(path: &Path) -> Result<Self, ForwardError> {
        let text = std::fs::read_to_string(path).map_err(|source| ForwardError::Io {
            context: "reading the forwarder configuration",
            source,
        })?;
        let config: Self =
            toml::from_str(&text).map_err(|error| ForwardError::Config(error.to_string()))?;
        if !config.socket.is_absolute() || !config.state_dir.is_absolute() {
            return Err(ForwardError::Config(
                "socket and state_dir must be absolute paths".to_owned(),
            ));
        }
        Ok(config)
    }
}

/// One journal instance the forwarder follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Instance {
    /// The system journal.
    System,
    /// The journal of the invoking user.
    User,
}

impl Instance {
    /// Parse the instance argument, `system` or `user`.
    ///
    /// # Errors
    ///
    /// Returns [`ForwardError::Config`] for any other value.
    pub fn parse(argument: &str) -> Result<Self, ForwardError> {
        match argument {
            "system" => Ok(Self::System),
            "user" => Ok(Self::User),
            other => Err(ForwardError::Config(format!(
                "instance must be system or user, not {other}"
            ))),
        }
    }

    /// Short name, used in the state directory and the stats line.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::User => "user",
        }
    }

    /// The source this instance writes: `journal.system` or `journal.user`.
    #[must_use]
    pub fn source(self) -> String {
        format!("journal.{}", self.name())
    }

    const fn scope(self) -> &'static str {
        match self {
            Self::System => "--system",
            Self::User => "--user",
        }
    }
}

/// Diagnostic mappings defined for every journal source, in the fixed field positions.
fn mappings() -> [(wire::DiagnosticDimension, &'static str); 3] {
    [
        (wire::DiagnosticDimension::Service, "/fields/0/value"),
        (wire::DiagnosticDimension::Host, "/fields/1/value"),
        (wire::DiagnosticDimension::Severity, "/fields/2/value"),
    ]
}

/// Run one instance until its journal stream ends or delivery fails.
///
/// The diagnostic mapping is defined before the first entry is read, so every record carries
/// the fixed fields the mapping names. While the daemon is unreachable, entries accumulate in
/// the spool and are sent when it returns.
///
/// # Errors
///
/// Returns [`ForwardError`] at the first failure that a restart cannot cure by waiting: a
/// malformed or oversized journal entry, a refused spool, a held delivery, or a `journalctl`
/// that exited.
pub async fn run(config: &Config, instance: Instance) -> Result<(), ForwardError> {
    // The typed client spawns local tasks, so the whole run lives in one `LocalSet`.
    tokio::task::LocalSet::new()
        .run_until(run_in_local_set(config, instance))
        .await
}

async fn run_in_local_set(config: &Config, instance: Instance) -> Result<(), ForwardError> {
    let source = instance.source();
    let dir = config.state_dir.join(instance.name());
    std::fs::create_dir_all(&dir).map_err(|source| ForwardError::Io {
        context: "creating the instance state directory",
        source,
    })?;
    let mut spool = SpoolConfig::new(dir.join("spool"));
    spool.max_bytes = config.spool_max_bytes;
    let delivery = Delivery::start_async(DeliveryConfig::lossless(
        Endpoint::Unix(config.socket.clone()),
        source.clone(),
        spool,
    ))
    .await
    .map_err(ForwardError::Client)?;
    let cursor_path = dir.join("cursor");
    let resume = resume_cursor(&delivery, &cursor_path)?;
    ensure_mapping(&config.socket, &source).await?;
    let child = spawn_journalctl(instance, resume.as_deref())?;
    let mut forwarder = Forwarder {
        delivery: &delivery,
        instance,
        source: &source,
        cursor_path: &cursor_path,
        parser: ExportParser::new(),
        counters: Counters::default(),
        written: None,
    };
    forwarder.stream(child).await
}

/// The cursor to resume after: the spool's accepted token when the spool has one, otherwise
/// the cursor file. With neither, the forwarder starts at the journal's tail.
fn resume_cursor(delivery: &Delivery, cursor_path: &Path) -> Result<Option<String>, ForwardError> {
    let accepted = delivery.accepted_token().map_err(ForwardError::Flush)?;
    if !accepted.is_empty() {
        return String::from_utf8(accepted).map(Some).map_err(|_| {
            ForwardError::State("the spool's accepted cursor is not UTF-8".to_owned())
        });
    }
    match std::fs::read_to_string(cursor_path) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(ForwardError::Io {
            context: "reading the cursor file",
            source,
        }),
    }
}

/// Define the source's mapping, retrying until the daemon accepts it.
async fn ensure_mapping(socket: &Path, source: &str) -> Result<(), ForwardError> {
    loop {
        match define_mapping(socket, source).await {
            Ok(()) => return Ok(()),
            Err(error @ (ForwardError::Client(_) | ForwardError::Define { .. })) => {
                tracing::warn!(error = %error, source, "diagnostic mapping not yet defined; retrying");
                tokio::time::sleep(DEFINE_BACKOFF).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn define_mapping(socket: &Path, source: &str) -> Result<(), ForwardError> {
    let client = Client::connect_endpoint(&Endpoint::Unix(socket.to_path_buf()))
        .await
        .map_err(ForwardError::Client)?;
    let capability = client.capability();
    let mut request = capability.diagnostic_define_request();
    let entries = mappings();
    {
        let mut definition = request.get().init_definition();
        definition.set_source(source);
        let mut rows = definition.init_mappings(entries.len() as u32);
        for (index, (dimension, path)) in entries.into_iter().enumerate() {
            let mut row = rows.reborrow().get(index as u32);
            row.set_dimension(dimension);
            row.set_path(path);
        }
    }
    let response = request.send().promise.await.map_err(ForwardError::Rpc)?;
    let result = response.get().map_err(ForwardError::Rpc)?;
    match result.get_result().map_err(ForwardError::Rpc)?.which() {
        Ok(wire::result::Ok(value)) => value.map(|_| ()).map_err(ForwardError::Rpc),
        Ok(wire::result::Error(error)) => {
            let error = error.map_err(ForwardError::Rpc)?;
            Err(ForwardError::Define {
                code: error
                    .get_code()
                    .ok()
                    .and_then(|code| code.to_str().ok())
                    .unwrap_or("")
                    .to_owned(),
                message: error
                    .get_message()
                    .ok()
                    .and_then(|text| text.to_str().ok())
                    .unwrap_or("")
                    .to_owned(),
            })
        }
        Err(error) => Err(ForwardError::Rpc(error.into())),
    }
}

fn spawn_journalctl(instance: Instance, resume: Option<&str>) -> Result<Child, ForwardError> {
    let mut command = Command::new("journalctl");
    command
        .arg(instance.scope())
        .args(["--follow", "--output=export"]);
    match resume {
        Some(cursor) => command.arg(format!("--after-cursor={cursor}")),
        None => command.arg("--lines=0"),
    };
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(|source| ForwardError::Io {
            context: "starting journalctl",
            source,
        })
}

/// Counts since this process started, reported on each stats line.
#[derive(Debug, Default, Clone, Copy)]
struct Counters {
    read: u64,
    spooled: u64,
    skipped_own: u64,
    truncated: u64,
    omitted_fields: u64,
}

struct Forwarder<'a> {
    delivery: &'a Delivery,
    instance: Instance,
    source: &'a str,
    cursor_path: &'a Path,
    parser: ExportParser,
    counters: Counters,
    /// The confirmed token last written to the cursor file.
    written: Option<Vec<u8>>,
}

impl Forwarder<'_> {
    async fn stream(&mut self, mut child: Child) -> Result<(), ForwardError> {
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| ForwardError::State("journalctl stdout was not piped".to_owned()))?;
        let mut buffer = vec![0_u8; READ_BYTES];
        let mut cursor_tick = ticker(CURSOR_INTERVAL);
        let mut stats_tick = ticker(STATS_INTERVAL);
        loop {
            tokio::select! {
                read = stdout.read(&mut buffer) => {
                    let read = read.map_err(|source| ForwardError::Io {
                        context: "reading journalctl output",
                        source,
                    })?;
                    if read == 0 {
                        let status = child.wait().await.map_err(|source| ForwardError::Io {
                            context: "waiting for journalctl",
                            source,
                        })?;
                        return Err(ForwardError::JournalEnded(status));
                    }
                    self.feed(&buffer[..read]).await?;
                }
                _ = cursor_tick.tick() => self.persist_confirmed()?,
                _ = stats_tick.tick() => self.report()?,
            }
        }
    }

    async fn feed(&mut self, bytes: &[u8]) -> Result<(), ForwardError> {
        for entry in self.parser.push(bytes)? {
            self.counters.read += 1;
            let cursor = entry.cursor().to_owned();
            match map_entry(&entry, self.source) {
                Ok(Mapping::OwnUnit) => self.counters.skipped_own += 1,
                Ok(Mapping::Event(forwarded)) => {
                    self.counters.truncated += u64::from(forwarded.truncated);
                    self.counters.omitted_fields += forwarded.omitted_fields;
                    self.send(forwarded.event, &cursor).await?;
                }
                Err(source) => return Err(ForwardError::Map { cursor, source }),
            }
        }
        Ok(())
    }

    /// Send one event with its cursor and wait until the spool holds it. The spool's
    /// backpressure is a wait, never a drop: an entry is offered again until it is admitted.
    async fn send(&mut self, mut event: ProducerEvent, cursor: &str) -> Result<(), ForwardError> {
        loop {
            match self
                .delivery
                .send_with_token(event, cursor.as_bytes())
                .await
            {
                Ok(_) => {
                    self.counters.spooled += 1;
                    return Ok(());
                }
                Err(SendError::Refused {
                    reason: Refusal::Busy | Refusal::SpoolFull,
                    event: returned,
                }) => {
                    event = *returned;
                    tokio::time::sleep(ROOM_BACKOFF).await;
                }
                Err(SendError::Refused { reason, .. }) => {
                    return Err(ForwardError::Refused(reason));
                }
                Err(SendError::Uncertain { sequence }) => {
                    return Err(ForwardError::Uncertain { sequence });
                }
            }
        }
    }

    /// Write the cursor file to the daemon's confirmed token when that token has moved.
    fn persist_confirmed(&mut self) -> Result<(), ForwardError> {
        let token = self
            .delivery
            .confirmed_token()
            .map_err(ForwardError::Flush)?;
        if token.is_empty() || self.written.as_deref() == Some(token.as_slice()) {
            return Ok(());
        }
        write_cursor(self.cursor_path, &token)?;
        self.written = Some(token);
        Ok(())
    }

    /// Emit the stats line, then stop if the delivery is held.
    fn report(&self) -> Result<(), ForwardError> {
        let stats = self.delivery.stats().map_err(ForwardError::Flush)?;
        let held = stats.held.as_ref().map(|error| error.to_string());
        let spool_bytes = stats.spool.as_ref().map_or(0, |spool| spool.bytes);
        let counters = self.counters;
        sparrow_eprintln!(
            "{{\"instance\":\"{instance}\",\"read\":{read},\"spooled\":{spooled},\"confirmed\":{confirmed},\"skipped_own\":{skipped},\"truncated\":{truncated},\"omitted_fields\":{omitted},\"spool_bytes\":{spool_bytes},\"held\":{held}}}",
            instance = self.instance.name(),
            read = counters.read,
            spooled = counters.spooled,
            confirmed = stats.durable_events,
            skipped = counters.skipped_own,
            truncated = counters.truncated,
            omitted = counters.omitted_fields,
            held = held.clone().map_or_else(|| "null".to_owned(), json_string),
        );
        match held {
            Some(message) => Err(ForwardError::Held(message)),
            None => Ok(()),
        }
    }
}

fn ticker(period: Duration) -> Interval {
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick
}

/// Replace the cursor file atomically: write, sync, rename, then sync the directory.
fn write_cursor(path: &Path, token: &[u8]) -> Result<(), ForwardError> {
    use std::io::Write;
    let io = |context| move |source| ForwardError::Io { context, source };
    let temporary = path.with_extension("tmp");
    let mut file = std::fs::File::create(&temporary).map_err(io("creating the cursor file"))?;
    file.write_all(token)
        .map_err(io("writing the cursor file"))?;
    file.sync_all().map_err(io("syncing the cursor file"))?;
    drop(file);
    std::fs::rename(&temporary, path).map_err(io("renaming the cursor file"))?;
    let directory = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(io("syncing the cursor directory"))
}

/// A JSON string literal for the characters a stats line can carry.
fn json_string(text: String) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            control if control < ' ' => out.push_str(&format!("\\u{:04x}", u32::from(control))),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

/// Failures of the forwarder. Each names the step that failed.
#[derive(Debug)]
pub enum ForwardError {
    /// The configuration file or an argument is unusable.
    Config(String),
    /// Local state (the spool or cursor) does not hold what the forwarder expects.
    State(String),
    /// A filesystem or process operation failed.
    Io {
        /// The step that failed.
        context: &'static str,
        /// The underlying failure.
        source: std::io::Error,
    },
    /// The SDK could not start delivery or reach the daemon.
    Client(sparrow_client::Error),
    /// A delivery barrier failed.
    Flush(FlushError),
    /// The journal export stream is malformed or oversized.
    Parse(ParseError),
    /// An entry cannot become a producer event.
    Map {
        /// Cursor of the entry that could not be mapped.
        cursor: String,
        /// Why it could not be mapped.
        source: MapError,
    },
    /// The spool refused an event for a reason that waiting does not cure.
    Refused(Refusal),
    /// The spool's append outcome is unknown; the next start recovers it from the spool.
    Uncertain {
        /// Sequence whose outcome is unknown.
        sequence: u64,
    },
    /// The delivery is held on a failure, with its message.
    Held(String),
    /// The daemon rejected the diagnostic mapping.
    Define {
        /// Typed error code.
        code: String,
        /// Error message.
        message: String,
    },
    /// A typed RPC reply could not be read.
    Rpc(capnp::Error),
    /// `journalctl` ended its output.
    JournalEnded(ExitStatus),
}

impl fmt::Display for ForwardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(reason) => write!(f, "invalid configuration: {reason}"),
            Self::State(reason) => write!(f, "invalid forwarder state: {reason}"),
            Self::Io { context, source } => write!(f, "{context}: {source}"),
            Self::Client(error) => write!(f, "delivery could not start: {error}"),
            Self::Flush(error) => write!(f, "delivery barrier failed: {error}"),
            Self::Parse(error) => write!(f, "{error}"),
            Self::Map { cursor, source } => write!(f, "entry {cursor} cannot be mapped: {source}"),
            Self::Refused(reason) => write!(f, "spool refused an event: {reason:?}"),
            Self::Uncertain { sequence } => {
                write!(f, "spool append outcome is unknown for sequence {sequence}")
            }
            Self::Held(message) => write!(f, "delivery is held: {message}"),
            Self::Define { code, message } => {
                write!(
                    f,
                    "daemon rejected the diagnostic mapping: {code}: {message}"
                )
            }
            Self::Rpc(error) => write!(f, "typed reply could not be read: {error}"),
            Self::JournalEnded(status) => write!(f, "journalctl ended its output: {status}"),
        }
    }
}

impl std::error::Error for ForwardError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Client(error) => Some(error),
            Self::Flush(error) => Some(error),
            Self::Parse(error) => Some(error),
            Self::Map { source, .. } => Some(source),
            Self::Rpc(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ParseError> for ForwardError {
    fn from(error: ParseError) -> Self {
        Self::Parse(error)
    }
}
