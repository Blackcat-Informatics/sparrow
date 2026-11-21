// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Start-time waits: a start condition that will pass is waited for, never exited on.
//!
//! The unit restarts a failed daemon every 10 s and gives up after 5 starts in 600 s
//! (`deploy/systemd/sparrow.service`), so an exit on a condition another process or the
//! operator clears spends that limit and leaves the unit failed until `reset-failed`. The
//! conditions that pass are free space for the open and the WAL replay, a pending upgrade's
//! write allowance, the store's lock while another process holds it, and a TCP port or socket
//! name a departing owner still holds. Every other
//! start failure (configuration, format, identity, lexicon) is deterministic: a restart repeats
//! it, so it still ends the start at once with its message.
//!
//! A start refusal that only a change of configuration, image, store or watch root clears is marked
//! [`Permanent`] where it is produced ([`permanent`], [`bind_failure`], [`open_failure`],
//! [`consume_start_failure`]). It ends with [`REFUSED_EXIT_STATUS`], which the unit lists in
//! `RestartPreventExitStatus=`: a restart would repeat it, so the unit neither loops on it nor
//! spends its start limit, and the journal names the refusal. Any other failure before the daemon
//! serves (a transient I/O error, descriptor exhaustion, a mount that has not come up) keeps the
//! ordinary failure status, so the unit restarts it within its start limit, and the journal says
//! it was not classified as permanent.
//!
//! While it waits the daemon serves nothing: before the store opens it holds no socket (the
//! endpoint needs the store's lock, which the open takes), and in a later bind wait its sockets
//! accept no connection. The wait is reported where a person and the upgrade script can read
//! it: the journal lines below, and the unit's `StatusText` through `sd_notify` `STATUS=`
//! ([`notify`]).
//!
//! Each wait journals what it waits for on its first observation, again when the reason
//! changes and as a reminder every [`REMINDER`], re-checks on an interval chosen per condition,
//! and ends at once on SIGINT. SIGTERM keeps its default action, which ends the process at
//! once, as it does while serving: before the store opens there is nothing to drain, and
//! after it SIGINT alone takes the orderly teardown.

use crate::ServerConfig;
use std::{
    error::Error,
    ffi::OsStr,
    io,
    os::{
        linux::net::SocketAddrExt,
        unix::{
            ffi::OsStrExt,
            net::{SocketAddr, UnixDatagram},
        },
    },
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::{signal::unix::Signal, time::Instant};

/// How often a free-space shortfall is measured: one `statvfs`, the cadence the serving loop
/// observes the same floor at.
const SPACE_RECHECK: Duration = crate::serving::SPACE_CHECK_INTERVAL;
/// How often a refused open is attempted again. An attempt opens and closes the whole store
/// (manifest, table files, WAL replay), far costlier than a measurement, so it is spaced like a
/// supervisor restart but without spending a start; an operator's fix is noticed within one
/// period.
pub const OPEN_RETRY: Duration = Duration::from_secs(30);
/// How often a held address is tried again: one bind, cheap. A departing owner leaves within the
/// unit's 120 s stop timeout, so the wait is short and bounded by the owner, not by this.
pub const BIND_RETRY: Duration = Duration::from_secs(1);
/// A wait whose reason has not changed is journaled again after this long, so a long wait stays
/// visible in the journal without a line per check.
const REMINDER: Duration = Duration::from_secs(60);

/// The exit status of a start that no wait clears: a configuration, image, store or watch-root
/// condition only a person changes. `EX_CONFIG` of `sysexits.h`; the unit's
/// `RestartPreventExitStatus=` names it, so the supervisor does not restart the refusal.
pub const REFUSED_EXIT_STATUS: u8 = 78;

/// A start refusal a restart repeats. The marker wraps the cause and is the only thing that
/// selects [`REFUSED_EXIT_STATUS`].
#[derive(Debug)]
pub struct Permanent(Box<dyn Error>);

impl std::fmt::Display for Permanent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Error for Permanent {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Mark `cause` as a start refusal a restart repeats.
#[must_use]
pub fn permanent(cause: impl Into<Box<dyn Error>>) -> Box<dyn Error> {
    Box::new(Permanent(cause.into()))
}

/// Whether `error` was marked by [`permanent`].
#[must_use]
pub fn is_permanent(error: &(dyn Error + 'static)) -> bool {
    error.is::<Permanent>()
}

/// A bind, spill or socket-directory failure: the kinds that name a refusal of the configured
/// address, name, mode or owner are permanent; any other kind is not classified.
#[must_use]
pub fn bind_failure(error: io::Error) -> Box<dyn Error> {
    use io::ErrorKind::{
        AddrNotAvailable, AlreadyExists, InvalidData, InvalidInput, NotADirectory, NotFound,
        PermissionDenied, Unsupported,
    };
    if matches!(
        error.kind(),
        AddrNotAvailable
            | AlreadyExists
            | InvalidData
            | InvalidInput
            | NotADirectory
            | NotFound
            | PermissionDenied
            | Unsupported
    ) {
        permanent(error)
    } else {
        Box::new(error)
    }
}

/// A refused open that no wait clears: `UNSUPPORTED` (format) and `INVALID_ARGUMENT`
/// (identity, analyzer, lexicon, profile, archive fence, configuration) are permanent. Corruption
/// and I/O failures (`STORAGE_FAILURE`) are not classified.
#[must_use]
pub fn open_failure(error: Box<dyn Error>) -> Box<dyn Error> {
    let refused = error
        .downcast_ref::<sparrow_core::Error>()
        .is_some_and(|error| matches!(error.code, "UNSUPPORTED" | "INVALID_ARGUMENT"));
    if refused { permanent(error) } else { error }
}

/// A consume worker that cannot start: a contradictory or unsupported watch state, a root that
/// is missing, replaced or unreadable, and the store's own `UNSUPPORTED` or `INVALID_ARGUMENT`
/// are permanent (restore the root or the watch configuration). Transport and other I/O
/// failures are not classified.
#[must_use]
pub fn consume_start_failure(error: crate::consumption::Error) -> Box<dyn Error> {
    use crate::consumption::Error as Start;
    let refused = match &error {
        Start::State(_) | Start::FinishedGeneration(_) | Start::PrivateRootShared { .. } => true,
        Start::Io(error) => matches!(
            error.kind(),
            io::ErrorKind::NotFound
                | io::ErrorKind::NotADirectory
                | io::ErrorKind::PermissionDenied
        ),
        Start::Storage(error) => matches!(error.code, "UNSUPPORTED" | "INVALID_ARGUMENT"),
        Start::Upload(_) => false,
    };
    if refused {
        permanent(error)
    } else {
        Box::new(error)
    }
}

static NOTIFY_FAILURE_JOURNALED: AtomicBool = AtomicBool::new(false);

/// Send `state` (`sd_notify` text such as `STATUS=...`) to the service manager. Outside a unit
/// there is no `NOTIFY_SOCKET` and nothing is sent. A refused send is journaled once and never
/// stops the start: the journal lines carry the same facts.
pub fn notify(state: &str) {
    let Some(path) = std::env::var_os("NOTIFY_SOCKET") else {
        return;
    };
    if let Err(error) = send_notification(&path, state)
        && !NOTIFY_FAILURE_JOURNALED.swap(true, Ordering::Relaxed)
    {
        eprintln!(
            "sparrowd: the service manager's notification socket {} refused a status: {error}; the journal lines still name each wait",
            path.to_string_lossy()
        );
    }
}

/// `STATUS=` for `text`, which stays on one line.
fn status(text: &str) -> String {
    format!("STATUS={}", text.replace(['\n', '\r'], " "))
}

fn send_notification(path: &OsStr, state: &str) -> io::Result<()> {
    let address = match path.as_bytes().strip_prefix(b"@") {
        Some(name) => SocketAddr::from_abstract_name(name)?,
        None => SocketAddr::from_pathname(path)?,
    };
    let socket = UnixDatagram::unbound()?;
    // A full queue is a refusal to report, not a reason to block the start.
    socket.set_nonblocking(true)?;
    socket.send_to_addr(state.as_bytes(), &address)?;
    Ok(())
}

/// Report that the daemon serves.
pub fn notify_serving() {
    notify("READY=1\nSTATUS=serving");
}

/// A start condition that will pass: what it is (its key, for journal de-duplication) and how
/// soon to look again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hold {
    key: &'static str,
    interval: Duration,
}

/// How a wait ended.
#[derive(Debug)]
pub enum Wait<T> {
    /// The condition passed and the attempt succeeded.
    Ready(T),
    /// SIGINT arrived first; the caller ends the start cleanly.
    Interrupted,
}

/// Run `attempt` until it succeeds, waiting while `hold` says its failure will pass and
/// returning any other failure at once.
///
/// # Errors
/// The first failure `hold` does not classify as passing.
pub async fn until<T, E: std::fmt::Display>(
    interrupt: &mut Signal,
    what: &str,
    mut attempt: impl AsyncFnMut() -> Result<T, E>,
    hold: impl Fn(&E) -> Option<Hold>,
) -> Result<Wait<T>, E> {
    let started = Instant::now();
    let mut last: Option<(&'static str, Instant)> = None;
    loop {
        let error = match attempt().await {
            Ok(value) => {
                if last.is_some() {
                    eprintln!(
                        "sparrowd: {what}: the condition cleared after {} s; proceeding",
                        started.elapsed().as_secs()
                    );
                    notify(&status("starting"));
                }
                return Ok(Wait::Ready(value));
            }
            Err(error) => error,
        };
        let Some(Hold { key, interval }) = hold(&error) else {
            return Err(error);
        };
        if last.is_none_or(|(prior, at)| prior != key || at.elapsed() >= REMINDER) {
            eprintln!(
                "sparrowd: waiting to {what}: {error}; checking again every {} s",
                interval.as_secs().max(1)
            );
            notify(&status(&format!("waiting to {what}: {error}")));
            last = Some((key, Instant::now()));
        }
        tokio::select! {
            // `None` is a runtime already shutting down, which ends the wait the same way.
            _ = interrupt.recv() => return Ok(Wait::Interrupted),
            () = tokio::time::sleep(interval) => {}
        }
    }
}

/// The hold for an address or socket name an owner still holds: a bound address, or an owner
/// that is alive but does not answer a probe in time. Anything else (a permission, a name that
/// is not a socket, an unwritable directory) is deterministic.
#[must_use]
pub fn bind_hold(error: &io::Error) -> Option<Hold> {
    match error.kind() {
        io::ErrorKind::AddrInUse => Some(Hold {
            key: "address in use",
            interval: BIND_RETRY,
        }),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => Some(Hold {
            key: "owner not answering",
            interval: BIND_RETRY,
        }),
        _ => None,
    }
}

/// The text RocksDB and the kernel give an out-of-space failure; the typed `Error` carries
/// only the engine's message.
fn is_out_of_space(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("no space left on device") || message.contains("disk quota exceeded")
}

/// The text of RocksDB's refusal to take the store's `LOCK` file while another process holds it.
fn is_lock_held(message: &str) -> bool {
    message.contains("lock file") || message.contains("lock hold")
}

/// The hold for a refused open. Space and write allowance pass; everything else the open
/// checks (format, identity, analyzer, profile, archive fence, corruption, descriptors) is
/// deterministic.
#[must_use]
pub fn open_hold(error: &(dyn Error + 'static)) -> Option<Hold> {
    let error = error.downcast_ref::<sparrow_core::Error>()?;
    match error.code {
        // The pending upgrade's data-write allowance, or any other retryable pressure.
        "STORAGE_PRESSURE" => Some(Hold {
            key: "storage pressure",
            interval: OPEN_RETRY,
        }),
        "STORAGE_FAILURE" if is_out_of_space(&error.message) => Some(Hold {
            key: "no space",
            interval: OPEN_RETRY,
        }),
        // Another process holds the store's lock: an offline command, or a daemon on its way
        // out (the old unit while this one starts). It passes when that process ends, and
        // the refused attempt is as cheap as a bind, so it is tried as often.
        "STORAGE_FAILURE" if is_lock_held(&error.message) => Some(Hold {
            key: "store lock held",
            interval: BIND_RETRY,
        }),
        _ => None,
    }
}

/// Why one open attempt did not produce a store.
enum Refusal {
    /// Free space is below what the open and the WAL replay need.
    Space(String),
    /// The open itself was refused.
    Open(Box<dyn Error>),
    /// A failure before the open that no wait clears (an unreadable data directory).
    Fatal(Box<dyn Error>),
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Space(text) => formatter.write_str(text),
            Self::Open(error) | Self::Fatal(error) => error.fmt(formatter),
        }
    }
}

/// Open the store, waiting while the filesystem lacks the space the open and a left WAL's
/// replay need and while the open is refused for space or for a pending upgrade's write
/// allowance.
///
/// Free space is measured against [`ServerConfig::open_floor`] before every attempt, so a
/// refused open that left more WAL is waited out with the larger floor. After a refused open the
/// storage limits are read from `config_path` again before the next attempt: an operator who
/// raised `disk_budget` for a pending upgrade is noticed without a restart. A file that cannot
/// be read keeps the limits the start used and is journaled.
///
/// # Errors
/// A deterministic refusal of the open, or a failure to size the replay.
pub async fn open_store(
    interrupt: &mut Signal,
    config: &mut ServerConfig,
    config_path: &Path,
) -> Result<Wait<sparrow_core::Store>, Box<dyn Error>> {
    let mut refused_open = false;
    let mut reload_failure = None::<String>;
    let outcome = until(
        interrupt,
        "open the store",
        async || {
            if std::mem::take(&mut refused_open) {
                match config.reload_storage_limits(config_path) {
                    Ok(Some(change)) => eprintln!("sparrowd: {change}"),
                    Ok(None) => reload_failure = None,
                    Err(error) => {
                        let text = error.to_string();
                        if reload_failure.as_ref() != Some(&text) {
                            eprintln!(
                                "sparrowd: the configuration {} cannot be read again, so the storage limits of the start stay: {text}",
                                config_path.display()
                            );
                            reload_failure = Some(text);
                        }
                    }
                }
            }
            let floor = config
                .open_floor()
                .map_err(|error| Refusal::Fatal(error.into()))?;
            if let Some(shortfall) = floor
                .shortfall()
                .map_err(|error| Refusal::Fatal(error.into()))?
            {
                return Err(Refusal::Space(shortfall));
            }
            config.open_store().map_err(|error| {
                refused_open = true;
                Refusal::Open(error)
            })
        },
        |refusal| match refusal {
            Refusal::Space(_) => Some(Hold {
                key: "free space",
                interval: SPACE_RECHECK,
            }),
            Refusal::Open(error) => open_hold(error.as_ref()),
            Refusal::Fatal(_) => None,
        },
    )
    .await;
    match outcome {
        Ok(wait) => Ok(wait),
        Err(Refusal::Space(text)) => Err(text.into()),
        Err(Refusal::Open(error)) => Err(open_failure(error)),
        Err(Refusal::Fatal(error)) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::signal::unix::{SignalKind, signal};

    /// A signal no test raises, so a parallel test's interrupt cannot reach these waits.
    fn idle() -> Signal {
        signal(SignalKind::user_defined2()).unwrap()
    }

    fn pressure() -> sparrow_core::Error {
        sparrow_core::Error::new(
            "STORAGE_PRESSURE",
            "owned RDF metadata backfill needs more data-write allowance",
        )
    }

    /// Value: a start that cannot pass (format, identity, lexicon, corruption) must still
    /// fail at once, and one that will pass must wait. Protects: the open's error classes.
    #[test]
    fn an_open_waits_on_space_and_allowance_and_fails_on_everything_deterministic() {
        let wait = |code, message: &str| open_hold(&sparrow_core::Error::new(code, message));
        assert!(wait("STORAGE_PRESSURE", "backfill needs more allowance").is_some());
        assert!(
            wait(
                "STORAGE_FAILURE",
                "IO error: No space left on device: While appending to file: 000007.log"
            )
            .is_some()
        );
        assert!(wait("STORAGE_FAILURE", "IO error: Disk quota exceeded").is_some());
        assert!(
            wait(
                "STORAGE_FAILURE",
                "IO error: lock hold by current process: /SPARROW/store/LOCK"
            )
            .is_some()
        );
        assert!(
            wait(
                "STORAGE_FAILURE",
                "IO error: While lock file: /SPARROW/store/LOCK: Resource temporarily unavailable"
            )
            .is_some()
        );
        assert!(wait("STORAGE_FAILURE", "Corruption: bad block in 000009.sst").is_none());
        assert!(wait("UNSUPPORTED", "store format 3 is newer than this image").is_none());
        assert!(wait("INVALID_ARGUMENT", "analyzer fingerprint differs").is_none());
        // An error that is not the typed storage error is not recognised as passing.
        assert!(open_hold(&io::Error::other("No space left on device")).is_none());
    }

    /// Value: only a start failure named as deterministic ends with the status the unit does
    /// not restart; a transient or unclassified one keeps the ordinary failure status and is
    /// restarted (#446). Protects: the class of each producer's errors.
    #[test]
    fn only_named_deterministic_refusals_are_permanent() {
        use io::ErrorKind::*;
        let core = |code| -> Box<dyn Error> { Box::new(sparrow_core::Error::new(code, "x")) };
        let table: Vec<(&str, Box<dyn Error>, bool)> = vec![
            ("format mismatch", open_failure(core("UNSUPPORTED")), true),
            (
                "identity, analyzer, lexicon, fence",
                open_failure(core("INVALID_ARGUMENT")),
                true,
            ),
            (
                "corruption or I/O at open",
                open_failure(core("STORAGE_FAILURE")),
                false,
            ),
            (
                "descriptor limit at open",
                open_failure(core("RESOURCE_LIMIT")),
                false,
            ),
            (
                "open not typed as a store error",
                open_failure("x".into()),
                false,
            ),
            (
                "address not on this host",
                bind_failure(io::Error::from(AddrNotAvailable)),
                true,
            ),
            (
                "name is not a socket",
                bind_failure(io::Error::from(AlreadyExists)),
                true,
            ),
            (
                "ingest directory refused",
                bind_failure(io::Error::from(InvalidInput)),
                true,
            ),
            (
                "ingest socket mode refused",
                bind_failure(io::Error::from(InvalidData)),
                true,
            ),
            (
                "bind denied",
                bind_failure(io::Error::from(PermissionDenied)),
                true,
            ),
            (
                "descriptors exhausted",
                bind_failure(io::Error::from(io::ErrorKind::Other)),
                false,
            ),
            (
                "interrupted",
                bind_failure(io::Error::from(Interrupted)),
                false,
            ),
            (
                "root missing",
                consume_start_failure(crate::consumption::Error::Io(io::Error::from(NotFound))),
                true,
            ),
            (
                "watch state contradicts",
                consume_start_failure(crate::consumption::Error::State("x")),
                true,
            ),
            (
                "watch catalog format",
                consume_start_failure(crate::consumption::Error::Storage(
                    sparrow_core::Error::new("UNSUPPORTED", "x"),
                )),
                true,
            ),
            (
                "catalog I/O",
                consume_start_failure(crate::consumption::Error::Storage(
                    sparrow_core::Error::new("STORAGE_FAILURE", "x"),
                )),
                false,
            ),
            (
                "transient read of a root",
                consume_start_failure(crate::consumption::Error::Io(io::Error::from(
                    io::ErrorKind::Other,
                ))),
                false,
            ),
        ];
        for (name, error, expected) in table {
            assert_eq!(is_permanent(error.as_ref()), expected, "{name}");
        }
    }

    /// Value: a port or socket name held by a departing owner spent the start limit (#446).
    /// Protects: held and unanswered names wait; a permission or a non-socket name does not.
    #[test]
    fn a_held_name_waits_and_a_refused_name_fails() {
        for kind in [
            io::ErrorKind::AddrInUse,
            io::ErrorKind::TimedOut,
            io::ErrorKind::WouldBlock,
        ] {
            assert!(bind_hold(&io::Error::from(kind)).is_some());
        }
        for kind in [
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::AlreadyExists,
            io::ErrorKind::AddrNotAvailable,
        ] {
            assert!(bind_hold(&io::Error::from(kind)).is_none());
        }
    }

    /// Value: a pending upgrade's refused open exited the daemon. Protects: the wait retries
    /// the refused attempt and proceeds, with the store the attempt that passed returns.
    #[tokio::test(start_paused = true)]
    async fn a_refused_open_is_retried_until_it_passes() {
        let mut interrupt = idle();
        let mut refusals = 3;
        let outcome = until(
            &mut interrupt,
            "open the store",
            async || {
                if refusals > 0 {
                    refusals -= 1;
                    return Err(pressure());
                }
                Ok(7)
            },
            |error| open_hold(error),
        )
        .await
        .unwrap();
        assert!(matches!(outcome, Wait::Ready(7)));
        assert_eq!(refusals, 0);
    }

    /// Value: deterministic configuration errors must not become waits. Protects: the first
    /// unclassified failure returns at once, after one attempt.
    #[tokio::test(start_paused = true)]
    async fn a_deterministic_failure_returns_after_one_attempt() {
        let mut interrupt = idle();
        let mut attempts = 0;
        let error = until(
            &mut interrupt,
            "open the store",
            async || {
                attempts += 1;
                Err::<(), _>(sparrow_core::Error::new(
                    "UNSUPPORTED",
                    "newer store format",
                ))
            },
            |error| open_hold(error),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "UNSUPPORTED");
        assert_eq!(attempts, 1);
    }

    /// Value: a port held by a departing owner must wait for it (#446). Protects: a real
    /// bind of a held address returns the listener once the holder lets go.
    #[tokio::test(start_paused = true)]
    async fn a_held_tcp_port_is_bound_when_its_holder_leaves() {
        let mut interrupt = idle();
        let holder = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = holder.local_addr().unwrap();
        let waiting = until(
            &mut interrupt,
            "bind the public TCP address",
            async || tokio::net::TcpListener::bind(address).await,
            bind_hold,
        );
        let leaving = async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            drop(holder);
        };
        let (outcome, ()) = tokio::join!(waiting, leaving);
        let Wait::Ready(listener) = outcome.unwrap() else {
            panic!("the wait was interrupted");
        };
        assert_eq!(listener.local_addr().unwrap(), address);
    }

    /// Value: the unit's StatusText and the upgrade gate read the wait through the service
    /// manager's socket. Protects: a status reaches a datagram socket as one `STATUS=` line.
    #[test]
    fn a_status_reaches_the_notification_socket_on_one_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("notify");
        let receiver = UnixDatagram::bind(&path).unwrap();
        send_notification(
            path.as_os_str(),
            &status("waiting to open the store: only 5 bytes\nfree"),
        )
        .unwrap();
        let mut buffer = [0_u8; 256];
        let length = receiver.recv(&mut buffer).unwrap();
        assert_eq!(
            &buffer[..length],
            b"STATUS=waiting to open the store: only 5 bytes free"
        );
        assert!(send_notification(dir.path().join("absent").as_os_str(), "STATUS=x").is_err());
    }

    /// Value: a wait must end promptly on SIGINT so a stop is never blocked by it.
    /// Protects: an interrupt ends a wait whose condition never clears.
    #[tokio::test(start_paused = true)]
    async fn an_interrupt_ends_a_wait_that_never_clears() {
        let mut interrupt = signal(SignalKind::user_defined1()).unwrap();
        let raise = async {
            tokio::time::sleep(Duration::from_secs(90)).await;
            rustix::process::kill_process(rustix::process::getpid(), rustix::process::Signal::USR1)
                .unwrap();
        };
        let waiting = until(
            &mut interrupt,
            "open the store",
            async || Err::<(), _>(pressure()),
            |error| open_hold(error),
        );
        let (outcome, ()) = tokio::join!(waiting, raise);
        assert!(matches!(outcome.unwrap(), Wait::Interrupted));
    }
}
