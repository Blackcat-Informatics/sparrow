// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Typed Sparrow producer access over Unix or public TCP, without a database dependency.
//!
//! A Rust application can start with a local console and configure delivery by passing
//! `Some(delivery)` to [`Console::install`]. An application composing its own subscriber
//! can attach [`Console::layer`] instead.
//!
//! ```no_run
//! # async fn example() -> std::io::Result<()> {
//! let console = sparrow_client::Console::install(None)?;
//! let files = 12;
//! sparrow_client::sparrow_eprintln!("Read {files} files");
//! console.flush().await?;
//! # Ok(())
//! # }
//! ```
//!
//! Run the low-level [`Client`] inside a Tokio `LocalSet`; Cap'n Proto capabilities stay on its
//! owning task. A retry must reuse the original request ID, source, format and bytes.
//!
//! ```no_run
//! # async fn example(socket: &std::path::Path) -> Result<(), sparrow_client::Error> {
//! tokio::task::LocalSet::new().run_until(async {
//!     let client = sparrow_client::Client::connect_producer(&sparrow_client::Endpoint::Unix(socket.into())).await?;
//!     let receipt = client.ingest("app-session-001", "my-app",
//!         sparrow_client::Format::Log, b"startup complete\n").await?;
//!     assert_eq!(receipt.durability, sparrow_client::Durability::Durable);
//!     Ok::<_, sparrow_client::Error>(())
//! }).await
//! # }
//! ```

/// Generated shared daemon/client contract.
// capnpc emits an unused generic parameter in annotation lookup.
#[allow(missing_docs, clippy::extra_unused_type_parameters)]
pub mod sparrow_capnp {
    include!(concat!(env!("OUT_DIR"), "/sparrow_capnp.rs"));
}

mod transport;
pub use transport::{Connection, Endpoint, connect, connect_tcp};
mod tcp_secret;
pub use tcp_secret::{CHAP_BYTES, TcpSecret};
mod error;
pub use error::{Error, FailureClass};
mod bytes;
mod result_release;
pub use result_release::{ResultReleaseReceipt, release_result};
mod receipt;
pub use bytes::{decode_evidence, encode_evidence, read_bytes};
pub use receipt::{ArtifactReceipt, Discovery, DurabilityWatermark, ProducerLimits, flush_through};
mod expect;
pub use expect::{Expect, RequiredRole, instance_role_label};
mod imports;
pub use imports::{
    CompletedFileIdentity, import_file, import_file_expecting, import_open_file,
    open_completed_file,
};
mod import_progress;
/// Part sets: a completed file above the import bound, cut and uploaded as ordered parts and
/// a manifest (#280).
pub mod parts;
pub use import_progress::{ImportProgress, get_import_progress};
mod ingest_status;
mod retention_metrics;
pub use ingest_status::{
    IngestReadLimits, IngestSelector, IngestStatus, PartDraft, PartRead, PartRecord,
    get_ingest_part_receipt, get_ingest_status,
};
pub use retention_metrics::{RetentionMetrics, RetentionReadLimits, get_retention_metrics};
mod handoff;
pub use handoff::{CompletedHandoff, HandoffReceipt, completed_handoff};
mod intent_file;
mod tail_finish;
pub use tail_finish::{TailFinishIntent, TailFinishReceipt, tail_finish};
mod events;
pub use events::{
    EventBatch, EventField, EventSpan, FieldValue, MAX_EVENT_BATCH_BYTES, MAX_EVENT_BATCH_EVENTS,
    MAX_EVENT_BYTES, MAX_EVENT_FIELDS, MAX_EVENT_SPANS, ProducerEvent,
    read_wide as decode_wide_integer, wide as encode_wide_integer,
};
mod console;
mod delivery;
mod producer_config;
pub use producer_config::ConfigError;
mod logging;
mod quarantine;
mod replay;
pub use console::{Console, ConsoleStats, ConsoleStream, ConsoleWriter};
pub use delivery::{
    Admission, Delivery, DeliveryConfig, DeliveryStats, DeliveryWatermark, DropReason, FlushError,
};
pub use logging::TracingLayer;

#[doc(hidden)]
pub use tracing as __tracing;

/// Emit a stderr-labelled, INFO-level structured event through the current subscriber.
/// Accepts the same formatting arguments and captured identifiers as `eprintln!`.
#[macro_export]
macro_rules! sparrow_eprintln {
    () => { $crate::sparrow_eprintln!("") };
    ($($arg:tt)*) => {
        $crate::__tracing::info!(stream = "stderr", message = %format_args!($($arg)*))
    };
}
pub use replay::{ReplayReceipt, replay_quarantine};

pub use sparrow_capnp::{Durability, EventKind, EventLevel, Format};

/// Exact schema version used by query and administrative clients.
pub const SCHEMA_VERSION: u32 = 72;
/// Ingestion and durability profile advertised across compatible schema versions.
pub const PRODUCER_PROFILE: &str = "sparrow-producer/1";
/// Fixed upload unit admitted by the schema's private staging contract.
pub const IMPORT_CHUNK_BYTES: usize = 64 * 1024;

/// Read UTC Unix milliseconds without inventing a value for an invalid clock.
pub fn unix_millis_now() -> Result<u64, Error> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Error::Protocol("system clock precedes Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| Error::Protocol("system clock exceeds Unix millisecond range"))
}

/// Encode a generated optional-value union through its typed variant setters.
/// The builder moves to exactly one branch; field types remain statically checked.
pub fn encode_optional<B, T>(
    out: B,
    value: Option<T>,
    present: impl FnOnce(B, T),
    absent: impl FnOnce(B),
) {
    match value {
        Some(value) => present(out, value),
        None => absent(out),
    }
}

/// Parse an explicit format label using the shared generated enum.
pub fn parse_format(value: &str) -> capnp::Result<Format> {
    match value {
        "jsonl" => Ok(Format::Jsonl),
        "log" => Ok(Format::Log),
        "json" => Ok(Format::Json),
        "markdown" => Ok(Format::Markdown),
        "opaque" => Ok(Format::Opaque),
        "syslog" => Ok(Format::Syslog),
        _ => Err(capnp::Error::failed(
            "format requires jsonl, log, json, markdown, opaque or syslog".into(),
        )),
    }
}

/// Original format label for a generated wire enum.
pub fn format_label(value: Format) -> &'static str {
    match value {
        Format::Jsonl => "jsonl",
        Format::Log => "log",
        Format::Json => "json",
        Format::Markdown => "markdown",
        Format::Opaque => "opaque",
        Format::Syslog => "syslog",
    }
}

/// An owned typed producer connection. Run its async methods inside a Tokio LocalSet.
pub struct Client {
    connection: Connection,
    discovery: Discovery,
    endpoint: Endpoint,
    expect: Option<Expect>,
}

impl Client {
    /// Release a captured result's replay lease; never releases evidence pins.
    pub async fn release_result(&self, snapshot_id: &str) -> Result<ResultReleaseReceipt, Error> {
        release_result(self.capability(), &self.discovery.store_id, snapshot_id).await
    }

    /// Read an exact artifact, record, or complete retained file-generation range.
    pub async fn bytes(&self, id: &str, offset: u64, length: u32) -> Result<Vec<u8>, Error> {
        self.discovery.require(receipt::Contract::Query)?;
        read_bytes(
            self.capability(),
            &self.discovery.store_id,
            id,
            offset,
            length,
        )
        .await
        .map(|(_, bytes)| bytes)
    }
    /// Accept an atomic typed event batch using the caller's stable retry identity.
    /// A transport failure requires retrying this same immutable batch.
    pub async fn events(&self, batch: &EventBatch) -> Result<ArtifactReceipt, Error> {
        self.events_with_durability(batch, Durability::Durable)
            .await
    }

    /// Accept a typed batch at an explicit acknowledgment boundary; identity is unchanged by the mode.
    pub async fn events_with_durability(
        &self,
        batch: &EventBatch,
        durability: Durability,
    ) -> Result<ArtifactReceipt, Error> {
        self.discovery.require(receipt::Contract::Producer)?;
        let bytes = batch.source_bytes_with_limits(self.discovery.event_limits())?;
        let expected = receipt::ExpectedArtifact::bytes(
            &self.discovery,
            &batch.source,
            Format::Jsonl,
            &bytes,
            durability,
            Some(batch.events.len()),
        );
        let mut request = self.capability().events_ingest_request();
        request.get().set_durability(durability);
        batch.encode(request.get().init_batch())?;
        let response = request.send().promise.await?;
        match response.get()?.get_result()?.which()? {
            sparrow_capnp::result::Ok(value) => expected.certify(ArtifactReceipt::decode(value?)?),
            sparrow_capnp::result::Error(error) => Err(Error::domain(error?)),
        }
    }
    /// Connect and verify discovery before allowing producer mutations.
    pub async fn connect(socket: &std::path::Path) -> Result<Self, Error> {
        Self::connect_endpoint(&Endpoint::Unix(socket.into())).await
    }

    /// Connect to public Cap'n Proto TCP and verify the same typed discovery contract.
    pub async fn connect_tcp(address: std::net::SocketAddr) -> Result<Self, Error> {
        Self::connect_endpoint(&Endpoint::Tcp(address)).await
    }

    /// Connect and validate discovery through the selected typed transport.
    pub async fn connect_endpoint(endpoint: &Endpoint) -> Result<Self, Error> {
        Self::open(endpoint, None, receipt::Contract::Query).await
    }

    /// Connect as [`Client::connect_endpoint`] does and refuse a daemon whose discovery does not
    /// meet `expect` (`INSTANCE_MISMATCH`), before any call is made over the connection. The
    /// client keeps the expectation: the cancel connection a multi-part upload makes is checked
    /// against it too.
    pub async fn connect_endpoint_expecting(
        endpoint: &Endpoint,
        expect: &Expect,
    ) -> Result<Self, Error> {
        Self::open(endpoint, Some(expect), receipt::Contract::Query).await
    }

    /// Open for ingestion, resumable imports and flush using the advertised producer profile.
    pub async fn connect_producer(endpoint: &Endpoint) -> Result<Self, Error> {
        Self::open(endpoint, None, receipt::Contract::Producer).await
    }

    /// Open a producer connection and require the configured role or store id.
    pub async fn connect_producer_expecting(
        endpoint: &Endpoint,
        expect: &Expect,
    ) -> Result<Self, Error> {
        Self::open(endpoint, Some(expect), receipt::Contract::Producer).await
    }

    async fn open(
        endpoint: &Endpoint,
        expect: Option<&Expect>,
        contract: receipt::Contract,
    ) -> Result<Self, Error> {
        endpoint.validate()?;
        let mut connection = match endpoint {
            Endpoint::Unix(socket) => transport::connect_typed(socket).await?,
            Endpoint::Tcp(address) | Endpoint::AuthenticatedTcp(address, _) => {
                transport::connect_tcp(*address).await?
            }
        };
        let discovery = contract.discover(&connection.client).await?;
        expect::require(expect, &discovery)?;
        if let Endpoint::AuthenticatedTcp(_, secret) = endpoint {
            connection.client =
                tcp_secret::authenticate(&connection.client, &discovery.store_id, secret).await?;
        }
        Ok(Self {
            connection,
            discovery,
            endpoint: endpoint.clone(),
            expect: expect.cloned(),
        })
    }

    /// The expectation this connection was opened under, if any.
    pub(crate) fn expectation(&self) -> Option<&Expect> {
        self.expect.as_ref()
    }

    /// Destination this connection was opened to; resumable imports reconnect through it.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Persistent store identity and explicit server admission/profile information.
    pub fn discovery(&self) -> &Discovery {
        &self.discovery
    }

    /// Generated capability for query, profile and administrative operations.
    pub fn capability(&self) -> &sparrow_capnp::sparrow::Client {
        &self.connection.client
    }

    /// Accept one complete artifact with the caller's stable retry identity.
    /// Default acceptance is durable; use `ingest_with_durability` for a buffered fence.
    pub async fn ingest(
        &self,
        request_id: &str,
        source: &str,
        format: Format,
        bytes: &[u8],
    ) -> Result<ArtifactReceipt, Error> {
        self.ingest_with_durability(request_id, source, format, bytes, Durability::Durable)
            .await
    }

    /// Accept a complete artifact with explicit buffered or durable acknowledgment.
    /// An input above the discovered inline bound is staged in 64 KiB appends and
    /// acknowledged durably; the store refuses one above `MAX_IMPORT_BYTES` before
    /// any byte is uploaded. A refused or failed upload cancels its staging journal over
    /// this connection, so it does not hold an import slot unless the cancel also fails.
    /// A hold (`STORAGE_PRESSURE`, `SHUTTING_DOWN`) or a lost connection is returned
    /// unchanged and cancels nothing: the journal stays intact and a retry under the same
    /// request ID resumes it. See [`Client::import_file`].
    pub async fn ingest_with_durability(
        &self,
        request_id: &str,
        source: &str,
        format: Format,
        bytes: &[u8],
        durability: Durability,
    ) -> Result<ArtifactReceipt, Error> {
        self.discovery.require(receipt::Contract::Producer)?;
        if request_id.is_empty()
            || request_id.len() > 128
            || source.is_empty()
            || source.len() > 512
        {
            return Err(Error::InvalidArgument(
                "require bounded nonempty request and source labels",
            ));
        }
        if bytes.len() as u64 > self.discovery.max_inline_artifact_bytes {
            // One message cannot carry it: stream it through the resumable import,
            // which acknowledges durably whatever durability was requested.
            return imports::upload_or_cancel(
                self.capability(),
                &self.discovery,
                request_id,
                source,
                format,
                bytes,
                &mut imports::Checkpoint::default(),
            )
            .await;
        }
        let expected = receipt::ExpectedArtifact::bytes(
            &self.discovery,
            source,
            format,
            bytes,
            durability,
            None,
        );
        let mut request = self.capability().ingest_request();
        let mut input = request.get().init_request();
        input.set_request_id(request_id);
        input.set_source(source);
        input.set_format(format);
        input.set_bytes(bytes);
        input.set_durability(durability);
        let response = request.send().promise.await?;
        match response.get()?.get_result()?.which()? {
            sparrow_capnp::result::Ok(value) => expected.certify(ArtifactReceipt::decode(value?)?),
            sparrow_capnp::result::Error(error) => Err(Error::domain(error?)),
        }
    }

    /// Synchronize through a current-session acceptance fence. Timeout/transport loss
    /// leaves synchronization unknown and permits retrying this same fence.
    pub async fn flush(&self, through: &DurabilityWatermark) -> Result<DurabilityWatermark, Error> {
        self.discovery.require(receipt::Contract::Producer)?;
        if through.store_id != self.discovery.store_id {
            return Err(Error::InvalidArgument(
                "durability fence belongs to another discovered store",
            ));
        }
        flush_through(self.capability(), through).await
    }

    /// Upload a completed regular file once over this connection. Unlike the free
    /// `import_file(endpoint, ...)` used by the CLI, it never reconnects or resumes: any
    /// error ends the attempt. A hold (`STORAGE_PRESSURE`, `SHUTTING_DOWN`) or a lost
    /// connection is returned unchanged and cancels nothing: the journal stays staged and a
    /// retry under the same request ID resumes it. Any other failed attempt cancels its
    /// staging journal over this connection, releasing its import slot; a retry under the
    /// same request ID then fails with `IMPORT_EXPIRED` until reclamation. A cancel answered
    /// `CONFLICT` means the publication committed although its reply was lost, or the
    /// daemon's file watcher owns the staging; the upload is retried once, returning the
    /// receipt or that attempt's error, and a watcher-owned journal is left intact. When the
    /// connection is lost before the cancel arrives, or the daemon refuses the cancel with a
    /// retryable hold (free-space floor, restart, busy admission), the error is the
    /// retryable `IMPORT_RESUMABLE` naming the import and keeping the refusal's class: a
    /// retry under the same request ID resumes the journal if it is still staged. If the
    /// cancel committed and its reply was lost, that ID answers `IMPORT_EXPIRED` until
    /// reclamation has removed the journal, then begins afresh. When the cancel fails for
    /// good, for example `PERMISSION_DENIED`, the error is `IMPORT_ABANDONED` naming the
    /// import, whose slot stays occupied until the retry window ends.
    pub async fn import_file(
        &self,
        request_id: &str,
        source: &str,
        format: Format,
        path: &std::path::Path,
    ) -> Result<ArtifactReceipt, Error> {
        let mut file = open_completed_file(path)?;
        self.import_open_file(request_id, source, format, &mut file)
            .await
    }

    /// Upload an already opened completed file, preserving descriptor ownership.
    /// Uses this connection's discovered store identity; never reopens or deletes a path.
    pub async fn import_open_file(
        &self,
        request_id: &str,
        source: &str,
        format: Format,
        file: &mut std::fs::File,
    ) -> Result<ArtifactReceipt, Error> {
        self.import_open_file_with_progress(request_id, source, format, file, None)
            .await
    }

    /// Upload as [`Client::import_open_file`] does, reporting each durable progress raise to
    /// `progress`, so a caller that drops this future at a deadline still knows how far the
    /// upload got (#280).
    pub async fn import_open_file_with_progress(
        &self,
        request_id: &str,
        source: &str,
        format: Format,
        file: &mut std::fs::File,
        progress: Option<std::sync::Arc<parts::PartsProgress>>,
    ) -> Result<ArtifactReceipt, Error> {
        self.discovery.require(receipt::Contract::Producer)?;
        imports::upload_or_cancel(
            self.capability(),
            &self.discovery,
            request_id,
            source,
            format,
            &imports::CompletedInput::capture(file)?,
            &mut imports::Checkpoint::reporting(progress, 0),
        )
        .await
    }

    /// Inspect the daemon's recorded artifact receipt by persistent store-scoped ID.
    pub async fn artifact(&self, id: &str) -> Result<ArtifactReceipt, Error> {
        self.discovery.require(receipt::Contract::Query)?;
        let mut request = self.capability().artifact_get_request();
        let mut reference = request.get().init_artifact();
        reference.set_store_id(&self.discovery.store_id);
        reference.set_id(id);
        let response = request.send().promise.await?;
        match response.get()?.get_result()?.which()? {
            sparrow_capnp::result::Ok(value) => {
                let receipt = ArtifactReceipt::decode(value?)?;
                receipt.require_store(&self.discovery)?;
                if receipt.id != id {
                    return Err(Error::Protocol(
                        "artifact inspection returned another occurrence",
                    ));
                }
                Ok(receipt)
            }
            sparrow_capnp::result::Error(error) => Err(Error::domain(error?)),
        }
    }
}
