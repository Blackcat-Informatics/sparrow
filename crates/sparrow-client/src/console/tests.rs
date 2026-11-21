// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use super::*;
use std::fs::{File, read};
use tracing_subscriber::prelude::*;

/// A pipe keeps its original bytes and stream identity across partial lines and EOF,
/// including output from both sides of a child in standalone mode.
#[tokio::test]
async fn standalone_child_and_partial_writer_keep_exact_output() {
    let dir = tempfile::tempdir().unwrap();
    let stdout = dir.path().join("stdout");
    let stderr = dir.path().join("stderr");
    let console = Console::with_writers(
        None,
        File::create(&stdout).unwrap(),
        File::create(&stderr).unwrap(),
        false,
    )
    .unwrap();
    let mut child = tokio::process::Command::new("sh")
        .args(["-c", r#"printf 'one\n\377tail'; printf 'two\nlast' >&2"#])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    console.tee_child(&mut child).await.unwrap();
    assert!(child.wait().await.unwrap().success());
    let mut writer = console.writer(ConsoleStream::Stdout);
    let tail = vec![b'v'; crate::logging::TEXT_LIMIT + 17];
    writer.write_all(&tail).unwrap();
    futures::executor::block_on(async { writer.flush().unwrap() });
    tracing::subscriber::with_default(tracing_subscriber::registry().with(console.layer()), || {
        tracing::warn!(target: "app", retries = 2_u64, "Retry deferred\nI/O failure\x1b");
    });
    let stats = console.flush().await.unwrap();
    let mut expected = b"one\n\xfftail".to_vec();
    expected.extend_from_slice(&tail);
    assert_eq!(read(stdout).unwrap(), expected);
    let errors = std::fs::read_to_string(stderr).unwrap();
    assert!(errors.starts_with("two\nlast"));
    assert!(
        errors.contains(
            "WARN   app  Retry deferred  retries=2\n                  │ I/O failure\\u{1b}"
        ),
        "{errors:?}"
    );
    assert!(!errors.contains('\x1b'));
    assert_eq!(stats.displayed, 7);
    assert_eq!(stats.invalid + stats.refused + stats.write_errors, 0);
}

/// A stalled terminal cannot stall tracing callers. Overflow stays visible and the next
/// local fence reports an output error after admitted observations reach the writer.
#[tokio::test]
async fn slow_broken_terminal_counts_overflow_and_preserves_error() {
    use std::sync::mpsc as sync;
    struct Slow {
        entered: sync::Sender<()>,
        release: sync::Receiver<()>,
        first: bool,
    }
    impl Write for Slow {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.first {
                self.first = false;
                self.entered.send(()).unwrap();
                self.release
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let (entered, waiting) = sync::channel();
    let (release, resume) = sync::channel();
    let console = Console::with_writers(
        None,
        io::sink(),
        Slow {
            entered,
            release: resume,
            first: true,
        },
        false,
    )
    .unwrap();
    tracing::subscriber::with_default(tracing_subscriber::registry().with(console.layer()), || {
        tracing::info!("first");
        waiting
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        for _ in 0..=QUEUE_CAPACITY {
            tracing::info!("burst");
        }
        assert_eq!(console.stats().refused, 1);
        release.send(()).unwrap();
    });
    assert_eq!(
        console.flush().await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(console.stats().displayed, QUEUE_CAPACITY as u64);
    assert_eq!(console.stats().write_errors, 1);
}

/// A checked fence only acknowledges an output error when the caller polls its response.
/// Cancelling it after the worker replied lets the next concurrent caller observe the error.
#[tokio::test]
async fn cancelled_completed_flush_preserves_error_for_waiting_caller() {
    use std::sync::mpsc as sync;
    let (hold, resume) = sync::channel();
    let (processed, observed) = sync::channel();
    struct Writer {
        resume: Option<sync::Receiver<()>>,
        processed: sync::Sender<()>,
        writes: usize,
    }
    impl Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            if self.writes == 1 {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            self.processed.send(()).unwrap();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            if let Some(resume) = self.resume.take() {
                resume
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
            }
            Ok(())
        }
    }
    let console = Console::with_writers(
        None,
        io::sink(),
        Writer {
            resume: Some(resume),
            processed,
            writes: 0,
        },
        false,
    )
    .unwrap();
    let mut writer = console.writer(ConsoleStream::Stderr);
    writer.write_all(b"first\n").unwrap();
    let mut cancelled = Box::pin(console.flush());
    assert!(futures::poll!(cancelled.as_mut()).is_pending());
    let mut next = Box::pin(console.flush());
    assert!(futures::poll!(next.as_mut()).is_pending());
    hold.send(()).unwrap();
    writer.write_all(b"prompt without newline").unwrap();
    observed
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert_eq!(
        writer.flush().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    log::Log::flush(&console);
    drop(cancelled);
    assert_eq!(next.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(console.flush().await.unwrap().write_errors, 1);
}
