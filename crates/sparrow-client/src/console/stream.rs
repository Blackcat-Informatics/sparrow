// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use super::{Command, Console, worker_closed};
use crate::{EventField, EventKind, EventLevel, FieldValue, ProducerEvent, logging::TEXT_LIMIT};
use std::io::{self, Write};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    sync::mpsc::{Permit, error::TrySendError},
};

/// The original local output stream, also attached to the structured observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsoleStream {
    /// Standard output, including command product bytes.
    Stdout,
    /// Standard error, including diagnostics.
    Stderr,
}

impl ConsoleStream {
    fn label(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

impl Console {
    /// Capture complete lines or bounded fragments from explicit writes, including invalid UTF-8.
    #[must_use]
    pub fn writer(&self, stream: ConsoleStream) -> ConsoleWriter {
        ConsoleWriter {
            console: self.clone(),
            stream,
            fragment: Fragment::default(),
        }
    }

    /// Forward the original bytes of a selected pipe to its named local stream and capture each
    /// line in order. Long lines split after 4096 bytes. A full local queue backpressures reading.
    pub async fn tee(
        &self,
        stream: ConsoleStream,
        reader: impl AsyncRead + Unpin,
    ) -> io::Result<u64> {
        let mut reader = BufReader::new(reader);
        let mut writer = self.writer(stream);
        let mut read = 0_u64;
        loop {
            let input = reader.fill_buf().await?;
            if input.is_empty() {
                break;
            }
            // This poll observed bytes from the pipe. Capture its context before waiting
            // for a local writer permit; a future can resume under another subscriber.
            writer.fragment.observe_start(self, stream);
            let slot = self.tx.reserve().await.map_err(|_| worker_closed())?;
            let n = writer.fragment.forward(self, stream, input, slot);
            reader.consume(n);
            read = read
                .checked_add(n as u64)
                .ok_or_else(|| io::Error::other("console stream offset exhausted"))?;
        }
        writer.fragment.deliver(self);
        Ok(read)
    }

    /// Drain a child's piped stdout and stderr concurrently. Unpiped streams keep their original
    /// routing. The caller retains ownership of the child and awaits its exit status.
    pub async fn tee_child(&self, child: &mut tokio::process::Child) -> io::Result<()> {
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let out = async {
            if let Some(pipe) = stdout {
                self.tee(ConsoleStream::Stdout, pipe).await?;
            }
            Ok::<_, io::Error>(())
        };
        let err = async {
            if let Some(pipe) = stderr {
                self.tee(ConsoleStream::Stderr, pipe).await?;
            }
            Ok::<_, io::Error>(())
        };
        // A failure on one stream still permits the other stream to reach EOF.
        let (out, err) = tokio::join!(out, err);
        out.and(err)
    }
}

/// A line-oriented `Write` adapter. A full console queue returns `WouldBlock` before consuming
/// the next complete fragment. Call `flush` to publish a final partial line and drain both streams.
/// A checked flush already in progress also makes a synchronous writer flush return
/// `WouldBlock`, even with room in the queue.
pub struct ConsoleWriter {
    console: Console,
    stream: ConsoleStream,
    fragment: Fragment,
}

impl Write for ConsoleWriter {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if input.is_empty() {
            return Ok(0);
        }
        let slot = self.console.tx.try_reserve().map_err(reservation_error)?;
        self.fragment.observe_start(&self.console, self.stream);
        Ok(self
            .fragment
            .forward(&self.console, self.stream, input, slot))
    }

    fn flush(&mut self) -> io::Result<()> {
        self.fragment.deliver(&self.console);
        self.console.flush_sync(true)?;
        Ok(())
    }
}

impl Drop for ConsoleWriter {
    fn drop(&mut self) {
        self.fragment.deliver(&self.console);
    }
}

fn reservation_error(error: TrySendError<()>) -> io::Error {
    match error {
        TrySendError::Full(()) => {
            io::Error::new(io::ErrorKind::WouldBlock, "Sparrow console queue is full")
        }
        TrySendError::Closed(()) => worker_closed(),
    }
}

#[derive(Default)]
struct Fragment {
    bytes: Vec<u8>,
    observation: Option<Result<ProducerEvent, ()>>,
}

impl Fragment {
    fn prefix(&self, input: &[u8]) -> (usize, bool) {
        let available = TEXT_LIMIT - self.bytes.len();
        let input = &input[..input.len().min(available)];
        if let Some(newline) = input.iter().position(|b| *b == b'\n') {
            (newline + 1, true)
        } else {
            (input.len(), input.len() == available)
        }
    }

    fn forward(
        &mut self,
        console: &Console,
        stream: ConsoleStream,
        input: &[u8],
        slot: Permit<'_, Command>,
    ) -> usize {
        let (n, ready) = self.prefix(input);
        let bytes = &input[..n];
        slot.send(Command::Raw(stream, bytes.to_vec()));
        if console.delivery().is_some() {
            self.bytes.extend_from_slice(bytes);
            if ready {
                self.deliver(console);
            }
        }
        n
    }

    fn observe_start(&mut self, console: &Console, stream: ConsoleStream) {
        if self.bytes.is_empty() && console.delivery().is_some() {
            self.observation = Some(Self::begin(console, stream));
        }
    }

    fn begin(console: &Console, stream: ConsoleStream) -> Result<ProducerEvent, ()> {
        let event = ProducerEvent {
            sequence: 0,
            event_time: crate::unix_millis_now().map_err(|_| ())?,
            kind: EventKind::Log,
            level: EventLevel::Info,
            name: "stdio".into(),
            target: "stdio".into(),
            fields: vec![EventField {
                name: "stream".into(),
                value: FieldValue::Text(stream.label().into()),
            }],
            context: Vec::new(),
            span: None,
            follows_from: None,
        };
        crate::logging::in_current_span(event, &console.slots)
    }

    fn deliver(&mut self, console: &Console) {
        let observation = self.observation.take();
        let bytes = std::mem::take(&mut self.bytes);
        if bytes.is_empty() {
            return;
        }
        match (observation, console.delivery()) {
            (Some(Ok(mut event)), Some(delivery)) => {
                let message = match String::from_utf8(bytes) {
                    Ok(text) => FieldValue::Text(text),
                    Err(binary) => FieldValue::Bytes(binary.into_bytes()),
                };
                event.fields.push(EventField {
                    name: "message".into(),
                    value: message,
                });
                delivery.try_send(event);
            }
            (Some(Err(())), _) => console.capture_failed(),
            _ => {}
        }
    }
}
