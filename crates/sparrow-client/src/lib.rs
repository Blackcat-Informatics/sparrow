// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Typed Sparrow producer access over a private Unix socket, without a database dependency.
//!
//! Run the client inside a Tokio `LocalSet`; Cap'n Proto capabilities stay on its
//! owning task. A retry must reuse the original request ID, source, format and bytes.
//!
//! ```no_run
//! # async fn example(socket: &std::path::Path) -> Result<(), sparrow_client::Error> {
//! tokio::task::LocalSet::new().run_until(async {
//!     let client = sparrow_client::Client::connect(socket).await?;
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
pub use transport::{Connection, connect};
mod error;
pub use error::Error;
mod receipt;
pub use receipt::{ArtifactReceipt, Discovery, DurabilityWatermark, flush_through};
mod imports;
pub use imports::{CompletedFileIdentity, import_file, import_open_file, open_completed_file};
mod handoff;
pub use handoff::{CompletedHandoff, HandoffReceipt, completed_handoff};
mod events;
pub use events::{
    EventBatch, EventField, EventSpan, FieldValue, MAX_EVENT_BATCH_BYTES, MAX_EVENT_BATCH_EVENTS,
    MAX_EVENT_BYTES, MAX_EVENT_FIELDS, MAX_EVENT_SPANS, ProducerEvent,
};
mod delivery;
mod logging;
pub use delivery::{
    Admission, Delivery, DeliveryConfig, DeliveryStats, DeliveryWatermark, DropReason, FlushError,
};
pub use logging::TracingLayer;

pub use sparrow_capnp::{Durability, EventKind, EventLevel, Format};

/// Required discovery contract; incompatible daemons are refused before mutation.
pub const SCHEMA_VERSION: u32 = 19;
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
        _ => Err(capnp::Error::failed(
            "format requires jsonl, log, json, markdown or opaque".into(),
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
    }
}

/// An owned typed producer connection. Run its async methods inside a Tokio LocalSet.
pub struct Client {
    connection: Connection,
    discovery: Discovery,
}

impl Client {
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
        let bytes = batch.source_bytes()?;
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
        let connection = transport::connect_typed(socket).await?;
        let discovery = Discovery::fetch(&connection.client).await?;
        Ok(Self {
            connection,
            discovery,
        })
    }

    /// Persistent store identity and explicit server admission/profile information.
    pub fn discovery(&self) -> &Discovery {
        &self.discovery
    }

    /// Generated capability for query, profile and administrative operations.
    pub fn capability(&self) -> &sparrow_capnp::sparrow::Client {
        &self.connection.client
    }

    /// Accept one bounded complete artifact with the caller's stable retry identity.
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
    pub async fn ingest_with_durability(
        &self,
        request_id: &str,
        source: &str,
        format: Format,
        bytes: &[u8],
        durability: Durability,
    ) -> Result<ArtifactReceipt, Error> {
        if request_id.is_empty()
            || request_id.len() > 128
            || source.is_empty()
            || source.len() > 512
        {
            return Err(Error::InvalidArgument(
                "require bounded nonempty request and source labels",
            ));
        }
        if bytes.len() as u64 > self.discovery.max_artifact_bytes {
            return Err(Error::InvalidArgument(
                "input exceeds buffered artifact limit; use import_file",
            ));
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
        if through.store_id != self.discovery.store_id {
            return Err(Error::InvalidArgument(
                "durability fence belongs to another discovered store",
            ));
        }
        flush_through(self.capability(), through).await
    }

    /// Stream a completed regular file through the same implementation used by the CLI.
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
        imports::upload(
            self.capability(),
            &self.discovery,
            request_id,
            source,
            format,
            file,
        )
        .await
    }

    /// Inspect the daemon's recorded artifact receipt by persistent store-scoped ID.
    pub async fn artifact(&self, id: &str) -> Result<ArtifactReceipt, Error> {
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
