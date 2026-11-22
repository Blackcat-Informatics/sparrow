<!-- SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca> -->
<!-- SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0 -->

# sparrow-client

Use `Console::install_auto("build.worker").await?` to give a Rust application
readable terminal diagnostics. When the application finds a Sparrow destination,
the same calls also send structured events with their fields and span context.
Log calls offer to bounded queues and return while workers handle terminal,
disk and network I/O.

## Install

Build on Linux with Rust 1.98 or later and the Cap'n Proto compiler
(`capnp` on `PATH`). The SDK ships both schemas and builds independently of
Sparrow's storage engine. Add the lab registry to your application's
`.cargo/config.toml` or your Cargo home configuration.

```toml
[registries.sparrow-lab]
index = "sparse+http://lillith:7555/api/packages/fleet/cargo/"
```

Add the following to your application's `Cargo.toml`.

```toml
[dependencies]
sparrow-client = { version = "0.1.0", registry = "sparrow-lab" }
tokio = { version = "1", features = ["macros", "rt"] }
tracing = "0.1"
```

## Write to the console

```rust
use sparrow_client::{Console, sparrow_eprintln};
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let console = Console::install_auto("build.worker").await?;
    let files = 12;
    tracing::info_span!("build", files).in_scope(|| {
        sparrow_eprintln!("Reading {files} files");
        tracing::info!(target: "compiler", files, bytes = 847_216_u64, "Build complete");
    });

    let local = console.flush().await?;
    if local.refused > 0 || local.invalid > 0 {
        return Err(std::io::Error::other(format!("console refused events: {local:?}")).into());
    }
    if let Some(delivery) = console.delivery() {
        delivery.shutdown_async(Duration::from_secs(5)).await?;
    }
    Ok(())
}
```

The second event appears on stderr with a UTC timestamp, aligned severity,
typed fields and the active span. Redirected diagnostics use plain text.

```text
13:41:09  INFO   compiler  Build complete  files=12  bytes=847216  in build
```

The installer sets the global tracing subscriber. To compose several layers
in your application, use `Console::start(delivery)?` and attach
`console.layer()` to your subscriber. For libraries using the `log` facade,
add `log = { version = "0.4", features = ["std", "kv"] }` to your dependencies,
call `log::set_boxed_logger(Box::new(console.clone()))?` once and select your
level with `log::set_max_level(log::LevelFilter::Info)`. Leave tracing's
`log-always` feature disabled when using both adapters to avoid duplicates.

## Select a destination and handle outages

At startup the console checks `SPARROW_CLIENT_CONFIG`, then
`$XDG_CONFIG_HOME/sparrow/client.toml` (default `~/.config/sparrow/client.toml`),
then the local ingest socket at `/run/sparrow-ingest/ingest.sock`.
With no config file or socket, it displays events locally and creates no spool.
An explicitly selected file must exist and be valid. An empty configuration
file enables the default spooled producer even when the socket is absent.
To set the destination in Rust, pass `DeliveryConfig::configured(endpoint, source)`
to `Delivery::start_async`, then pass the handle to `Console::install(Some(handle))`.

A shared configuration can supply just an endpoint. By default the caller's
source determines its private spool under `$XDG_STATE_HOME/sparrow-client/spool`
(default state home `~/.local/state`). Give concurrent producers with the same
source distinct spool directories. File paths in the TOML resolve beside it.

```toml
endpoint = "tcp://192.0.2.10:518"
expected_role = "production"
secret_file = "tcp.secret"
```

The configured producer retries in the background, replays its journal after
reconnection and reclaims confirmed segments. Choose the acknowledgement your
application needs.

| Operation | What success establishes |
| --- | --- |
| `console.flush().await?` | Earlier admitted output reached the local streams. Inspect the returned local refusal counts. |
| `delivery.try_send(event)` | `Admission::Queued` transfers an event into bounded memory. Inspect the admission result for overload. |
| `delivery.send(event).await?` | In confirmed spool mode, the event reached synchronized local disk. |
| `delivery.clone().close_local(timeout).await?` | Spooled delivery flushed admitted events to local disk and stopped. Use this on shutdown when the server may be offline. Reopen the same spool to replay. |
| `delivery.shutdown_async(timeout).await?` | Sparrow durably acknowledged the admitted prefix and the worker stopped. Use this when shutdown requires remote confirmation. |

The default uses confirmed spooling. Memory-only configurations require a
remote acknowledgement for durable delivery. Inspect `Delivery::stats` for
remote admission, refusal and retry outcomes independently of console output.

## Capture existing output

Give a component that accepts `std::io::Write` a
`console.writer(ConsoleStream::Stdout)` or `ConsoleStream::Stderr` writer.
The writer forwards the original bytes and captures each line or 4096-byte
fragment, preserving invalid UTF-8 as bytes. A full queue returns `WouldBlock`.
Flush the writer to finish a partial line.

For child processes, enable Tokio's `process` feature and pipe the streams you
want to capture. `tee_child` forwards stdout and stderr concurrently, waiting
for output capacity. The caller reaps the child and checks its exit status.

```rust
use sparrow_client::Console;
use std::{io, process::{ExitStatus, Stdio}};
use tokio::process::Command;

async fn run_build(console: &Console) -> io::Result<ExitStatus> {
    let mut child = Command::new("make")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let captured = console.tee_child(&mut child).await;
    let status = child.wait().await?;
    captured?;
    Ok(status)
}
```

## Import files and streams

Create a `BulkImporter` with `BulkConfig::new(endpoint, staging_directory)`.
Use an absolute private staging directory and a stable job ID per input.
Bulk jobs have their own bounded queues and connections, so a large import
can run alongside application logging.

* For a completed file, `import_file(&input).await?` returns its final receipt.
  Use `import_files(inputs)` for bounded concurrency and results in completion order.
* For separate staging and publication, use
  `open(job_id, source, format).await?`, then `stage_file(path).await?`
  or `stage_stream(offset, reader).await?` for a Tokio `AsyncRead`.
  The staging allowance limits how far the reader can advance while offline.
* After an interruption, reopen the same job. Reattach an unfinished stream at
  `job.progress().borrow().staged` with a reader positioned at that byte. Supply
  the original unchanged file when resuming a partial file import.
* After EOF, `job.finish().await?` returns the remote store, artifact, byte
  count and range count. To stop locally and resume later, use `job.close().await?`.

The package includes runnable examples for the
[console](examples/console.rs), [configured delivery](examples/configured.rs)
and [bulk files or stdin](examples/bulk.rs).
