// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Human-readable diagnostics and byte-exact explicit console capture.

mod facade;
mod render;
mod stream;
pub use stream::{ConsoleStream, ConsoleWriter};

use crate::{Delivery, EventKind, ProducerEvent, TracingLayer};
use std::{
    io::{self, IsTerminal, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};
use tokio::sync::{mpsc, oneshot};

// Each event has already passed the producer's 64 KiB admission bound.
const QUEUE_CAPACITY: usize = 64;

enum Command {
    Event(render::Line),
    Raw(ConsoleStream, Vec<u8>),
    Flush {
        reply: oneshot::Sender<Option<Arc<FenceFailure>>>,
        report_errors: bool,
    },
}

struct FenceFailure {
    cause: Arc<io::Error>,
    observed: AtomicBool,
}

#[derive(Default)]
struct Counts {
    fence: tokio::sync::Mutex<()>,
    displayed: AtomicU64,
    refused: AtomicU64,
    invalid: AtomicU64,
    errors: AtomicU64,
}

/// Cumulative local output counts. Remote receipts and drops are available from `Delivery`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsoleStats {
    /// Formatted events and raw fragments successfully written.
    pub displayed: u64,
    /// Local log observations refused by a full or closed output queue.
    pub refused: u64,
    /// Observations that exceeded capture bounds or had an invalid capture context.
    pub invalid: u64,
    /// Write and flush failures on the local output streams.
    pub write_errors: u64,
}

/// A bounded, off-thread console with an optional structured delivery destination.
///
/// A standalone console opens no socket, spool or configuration file. Attach
/// `layer()` to the application's subscriber. Use `flush().await` before exiting
/// a short-lived process to wait for already admitted local output.
#[derive(Clone)]
pub struct Console {
    pub(super) slots: Arc<AtomicUsize>,
    tx: mpsc::Sender<Command>,
    counts: Arc<Counts>,
    delivery: Option<Delivery>,
}

impl Console {
    /// Start local display and install its tracing subscriber for this application.
    /// Returns `AlreadyExists` when another global subscriber owns tracing. Applications
    /// composing multiple layers can use `start` and `layer` instead.
    pub fn install(delivery: Option<Delivery>) -> io::Result<Self> {
        use tracing_subscriber::prelude::*;
        let console = Self::start(delivery)?;
        let subscriber = tracing_subscriber::registry().with(console.layer());
        tracing::subscriber::set_global_default(subscriber)
            .map_err(|error| io::Error::new(io::ErrorKind::AlreadyExists, error))?;
        Ok(console)
    }

    /// Start the console with the process's stdout and stderr. ANSI styling follows stderr's
    /// terminal capability and a nonempty `NO_COLOR` environment variable disables it.
    pub fn start(delivery: Option<Delivery>) -> io::Result<Self> {
        let stderr = io::stderr();
        let color =
            stderr.is_terminal() && std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty());
        Self::with_writers(delivery, io::stdout(), stderr, color)
    }

    /// Supply the local destinations and select styling explicitly. Normal log events go to
    /// stderr; explicit writers and pipes send their original bytes to the matching stream.
    pub fn with_writers(
        delivery: Option<Delivery>,
        stdout: impl Write + Send + 'static,
        stderr: impl Write + Send + 'static,
        color: bool,
    ) -> io::Result<Self> {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        let counts = Arc::new(Counts::default());
        let worker_counts = counts.clone();
        std::thread::Builder::new()
            .name("sparrow-console".into())
            .spawn(move || run(stdout, stderr, color, rx, &worker_counts))?;
        Ok(Self {
            tx,
            counts,
            delivery,
            slots: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Capture logs and span context through a subscriber owned by the application.
    #[must_use]
    pub fn layer(&self) -> TracingLayer {
        TracingLayer::with_console(self.clone())
    }

    /// The structured producer attached to this console, when configured.
    #[must_use]
    pub fn delivery(&self) -> Option<&Delivery> {
        self.delivery.as_ref()
    }

    /// Inspect cumulative local output outcomes, including overload and capture refusals.
    #[must_use]
    pub fn stats(&self) -> ConsoleStats {
        ConsoleStats {
            displayed: self.counts.displayed.load(Ordering::Relaxed),
            refused: self.counts.refused.load(Ordering::Relaxed),
            invalid: self.counts.invalid.load(Ordering::Relaxed),
            write_errors: self.counts.errors.load(Ordering::Relaxed),
        }
    }

    /// Wait for earlier local writes and flush both streams. Every output error since the
    /// previous acknowledged flush is counted. The first error is returned with its cause.
    pub async fn flush(&self) -> io::Result<ConsoleStats> {
        // Serialize checked fences until the caller observes the reply or cancels.
        let _fence = self.counts.fence.lock().await;
        self.flush_local(true).await
    }

    async fn flush_local(&self, report_errors: bool) -> io::Result<ConsoleStats> {
        let (reply, result) = oneshot::channel();
        self.tx
            .send(Command::Flush {
                reply,
                report_errors,
            })
            .await
            .map_err(|_| worker_closed())?;
        if let Some(failure) = result.await.map_err(|_| worker_closed())? {
            failure.observed.store(true, Ordering::Release);
            return Err(io::Error::new(failure.cause.kind(), failure.cause.clone()));
        }
        Ok(self.stats())
    }

    fn flush_sync(&self, report_errors: bool) -> io::Result<ConsoleStats> {
        // A blocking writer cannot wait on an async fence owned by an unpolled future on
        // the same executor. The process logger drains without acknowledging failures.
        let _fence = if report_errors {
            Some(self.counts.fence.try_lock().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "another Sparrow console flush is active",
                )
            })?)
        } else {
            None
        };
        // A std::io::Write or log::Log caller can itself be running a futures LocalPool,
        // which forbids nested block_on. Only an explicit synchronous fence needs this.
        if let Ok(guard) = futures::executor::enter() {
            drop(guard);
            return futures::executor::block_on(self.flush_local(report_errors));
        }
        std::thread::scope(|scope| {
            std::thread::Builder::new()
                .name("sparrow-console-flush".into())
                .spawn_scoped(scope, || {
                    futures::executor::block_on(self.flush_local(report_errors))
                })?
                .join()
                .map_err(|_| io::Error::other("Sparrow console flush thread panicked"))?
        })
    }

    pub(super) fn capture_failed(&self) {
        self.counts.invalid.fetch_add(1, Ordering::Relaxed);
        if let Some(delivery) = &self.delivery {
            delivery.capture_failed();
        }
    }

    pub(super) fn emit(&self, mut event: ProducerEvent) {
        if event.kind == EventKind::Log {
            match self.tx.try_reserve() {
                Ok(slot) => slot.send(Command::Event(render::Line::capture(
                    &mut event,
                    self.delivery.is_some(),
                ))),
                Err(_) => {
                    self.counts.refused.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        if let Some(remote) = &self.delivery {
            remote.try_send(event);
        }
    }
}

#[cfg(test)]
mod tests;

fn worker_closed() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "Sparrow console output worker stopped",
    )
}

fn run(
    mut stdout: impl Write,
    mut stderr: impl Write,
    color: bool,
    mut rx: mpsc::Receiver<Command>,
    counts: &Counts,
) {
    let mut buffer = Vec::new();
    let mut first_error = None;
    let mut offered: Option<Arc<FenceFailure>> = None;
    while let Some(command) = rx.blocking_recv() {
        let result = match command {
            Command::Event(event) => {
                buffer.clear();
                render::line(&mut buffer, &event, color).and_then(|()| stderr.write_all(&buffer))
            }
            Command::Raw(stream, data) => {
                let output: &mut dyn Write = match stream {
                    ConsoleStream::Stdout => &mut stdout,
                    ConsoleStream::Stderr => &mut stderr,
                };
                output.write_all(&data).and_then(|()| {
                    // The process stdout is line-buffered. A child may wait for a reply to
                    // a prompt that has no newline.
                    if data.ends_with(b"\n") {
                        Ok(())
                    } else {
                        output.flush()
                    }
                })
            }
            Command::Flush {
                reply,
                report_errors,
            } => {
                for result in [stdout.flush(), stderr.flush()] {
                    if let Err(error) = result {
                        counts.errors.fetch_add(1, Ordering::Relaxed);
                        first_error.get_or_insert(error);
                    }
                }
                // The log facade has no return channel for output errors. Keep those for a
                // checked fence even when an intervening log::Log::flush drains the output.
                if report_errors {
                    let current = first_error.take();
                    if offered
                        .as_ref()
                        .is_none_or(|e| e.observed.load(Ordering::Acquire))
                    {
                        offered = current.map(|cause| {
                            Arc::new(FenceFailure {
                                cause: Arc::new(cause),
                                observed: AtomicBool::new(false),
                            })
                        });
                    }
                }
                // Keep the first unobserved failure even when a cancelled future received
                // its reply into the oneshot buffer and never polled that buffer.
                let _ = reply.send(if report_errors { offered.clone() } else { None });
                continue;
            }
        };
        match result {
            Ok(()) => {
                counts.displayed.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                counts.errors.fetch_add(1, Ordering::Relaxed);
                first_error.get_or_insert(error);
            }
        }
    }
}
