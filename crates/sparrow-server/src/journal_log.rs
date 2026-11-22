// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The daemon's `tracing` subscriber (#324): events the storage library raises, written to
//! stderr as whole single lines for the journal.
//!
//! Only warnings and errors from `sparrow_core` are written. The daemon's own warnings already
//! have a `sparrowd:` operator line beside their event, and writing both would print each
//! twice. Without this subscriber the library's events, such as an archive prepare halving its
//! range, were dropped.

use std::io::{self, Write};
use tracing::{Level, Subscriber};
use tracing_subscriber::{
    Layer, filter::filter_fn, fmt::MakeWriter, layer::SubscriberExt, util::SubscriberInitExt,
};

/// Install the subscriber as the process default.
///
/// # Errors
/// Returns the failure when another global default is already set.
pub fn install() -> Result<(), tracing_subscriber::util::TryInitError> {
    subscriber(io::stderr).try_init()
}

/// The subscriber writing to `make`'s writers: no timestamp (the journal adds one), no colour,
/// and no event spread over more than one line.
fn subscriber<W: Write + 'static>(
    make: impl Fn() -> W + Send + Sync + 'static,
) -> impl Subscriber + Send + Sync {
    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .without_time()
        .with_writer(OneLine(make))
        .with_filter(filter_fn(|metadata| {
            *metadata.level() <= Level::WARN && metadata.target().starts_with("sparrow_core")
        }));
    tracing_subscriber::registry().with(layer)
}

/// Makes writers that put each event on one line.
struct OneLine<F>(F);

impl<'a, W: Write + 'a, F: Fn() -> W> MakeWriter<'a> for OneLine<F> {
    type Writer = Escaped<W>;

    fn make_writer(&'a self) -> Self::Writer {
        Escaped((self.0)())
    }
}

/// Writes one formatted event, replacing a line break inside it (a multi-line error message)
/// with a visible `\n`, and any other control character (NUL, ESC, DEL) with `\u{NN}`, so the
/// event stays whole and cannot move a terminal or hide text in the journal. The formatter
/// delivers an event in one write that ends with its own newline, which is kept. A write that
/// fails is ignored by the formatter, so a closed stderr cannot panic the thread that logged.
struct Escaped<W>(W);

impl<W: Write> Write for Escaped<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let (body, end) = match buf.strip_suffix(b"\n") {
            Some(body) => (body, &b"\n"[..]),
            None => (buf, &b""[..]),
        };
        let mut line = Vec::with_capacity(buf.len() + 8);
        for &byte in body {
            match byte {
                b'\n' => line.extend_from_slice(b"\\n"),
                b'\r' => line.extend_from_slice(b"\\r"),
                0..=0x1f | 0x7f => {
                    line.extend(char::from(byte).escape_unicode().map(|c| c as u8));
                }
                other => line.push(other),
            }
        }
        line.extend_from_slice(end);
        self.0.write_all(&line)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct Sink(Arc<Mutex<Vec<u8>>>);

    impl Write for Sink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// INVARIANT: a warning from the storage library reaches the journal as exactly one line
    /// with its structured fields, even when an error message holds line breaks; the daemon's
    /// own events (which have an operator line) and anything below a warning are not written.
    #[test]
    fn a_library_warning_is_one_journal_line_and_nothing_else_is_written() {
        let sink = Sink::default();
        let out = sink.clone();
        let subscriber = subscriber(move || out.clone());
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(
                target: "sparrow_core::storage::archive_segment",
                code = "RESOURCE_LIMIT",
                detail = %"first\nsecond\u{0}\u{1b}[31m",
                cause = "say \"hi\"\u{0}\u{1b}".to_owned().as_str(),
                entries = 10_u64,
                "archive segment encoding failed; the range is halved and retried"
            );
            tracing::warn!(target: "sparrow_server::archive", "has its own operator line");
            tracing::info!(target: "sparrow_core::storage", "below a warning");
        });
        let text = String::from_utf8(sink.0.lock().unwrap().clone()).unwrap();
        assert_eq!(text.lines().count(), 1, "{text:?}");
        assert!(text.ends_with('\n'));
        for part in [
            "WARN",
            "sparrow_core::storage::archive_segment",
            "code=\"RESOURCE_LIMIT\"",
            "first\\nsecond\\u{0}\\u{1b}[31m",
            "cause=\"say \\\"hi\\\"\\0\\u{1b}\"",
            "entries=10",
            "the range is halved",
        ] {
            assert!(text.contains(part), "{part} missing from {text:?}");
        }
        assert!(
            !text.chars().any(|c| c != '\n' && c.is_control()),
            "no raw control character: {text:?}"
        );
    }
}
