// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use crate::{Error, sparrow_capnp as wire};

pub(crate) fn valid_store_id(id: &str) -> bool {
    id.starts_with("store:") && (7..=128).contains(&id.len())
}

pub(crate) fn optional_identity(
    value: wire::optional_text::Reader<'_>,
) -> Result<Option<String>, Error> {
    match value.which()? {
        wire::optional_text::Absent(()) => Ok(None),
        wire::optional_text::Value(value) => {
            let value = value?.to_str()?;
            if value.len() > 128 {
                return Err(Error::Protocol("optional identity exceeds 128 bytes"));
            }
            Ok(Some(value.to_owned()))
        }
    }
}

pub(crate) fn certify_result<'a, T: capnp::traits::Owned, R>(
    value: wire::result::Reader<'a, T>,
    certify: impl FnOnce(T::Reader<'a>) -> Result<R, Error>,
) -> Result<R, Error> {
    match value.which()? {
        wire::result::Ok(value) => certify(value?),
        wire::result::Error(error) => Err(Error::domain(error?)),
    }
}

/// Owned discovery information checked before producer mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discovery {
    /// Current daemon session for durability fences.
    pub durability_session: String,
    /// Advertised maximum unsynchronized atomic writes.
    pub max_buffered_writes: u32,
    /// Advertised atomic batch byte charge limit before synchronization.
    pub max_buffered_bytes: u64,
    /// Intended periodic synchronization interval.
    pub flush_interval_ms: u32,
    /// Persistent store identity.
    pub store_id: String,
    /// Generated wire contract version.
    pub schema_version: u32,
    /// Named ingestion contracts the daemon advertises.
    pub producer_profiles: Vec<String>,
    /// Server-clock deadline for sealing a new durable request. Zero means unavailable.
    pub sealed_event_deadline_ms: u64,
    /// Transport deadlines when the daemon advertises them.
    pub connection_lease: Option<ConnectionLeaseLimits>,
    /// Server limits for tracking producer evidence. These describe the server's windows.
    pub producer_limits: ProducerLimits,
    /// Largest artifact carried in one inline ingest message. Larger artifacts
    /// stream through the staged import, up to its fixed publication bound.
    pub max_inline_artifact_bytes: u64,
    /// Configured maximum record membership. Staged parts also obey the fixed
    /// [publication ceiling](sparrow_limits::MAX_PUBLICATION_RECORDS).
    pub max_records_per_artifact: u64,
    /// Native analyzer identity.
    pub text_analyzer: String,
    /// Persisted text index profile identity.
    pub text_profile: String,
    /// Persisted native RDF projection profile identity.
    pub rdf_profile: String,
    /// Maximum admitted event batch expansion in bytes.
    pub max_event_batch_bytes: u32,
    /// Maximum independent events in one batch.
    pub max_event_batch_events: u32,
    /// Largest declared staged import; a larger declaration is refused. Zero from a daemon
    /// that does not advertise it; the single path then works as before (#280).
    pub max_import_bytes: u64,
    /// Longest record a staged layout keeps whole; zero when not advertised.
    pub raw_record_bytes: u64,
    /// Joint retirement bound of one artifact, with its per-chunk and per-record charges;
    /// zero when not advertised. See [`crate::parts::PartLimits`].
    pub retire_row_budget: u64,
    /// Charge of one chunk against `retire_row_budget`.
    pub retire_chunk_charge: u64,
    /// Charge of one record against `retire_row_budget`.
    pub retire_record_charge: u64,
    /// The known store role. `Unspecified` means no known role. For a newer role the
    /// daemon's actual ordinal is preserved in [`Self::unknown_instance_role`].
    pub instance_role: crate::sparrow_capnp::InstanceRole,
    /// The actual wire ordinal when this SDK does not recognize the advertised store role.
    /// Strict query discovery refuses such a role. A producer may still pin the store id.
    pub unknown_instance_role: Option<u16>,
}

/// Transport deadlines advertised by Discovery and status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionLeaseLimits {
    /// Maximum established connection inactivity, in seconds.
    pub idle_seconds: u64,
    /// Maximum wait for first input on public TCP, in seconds.
    pub tcp_first_input_seconds: u64,
    /// Maximum connection age, in seconds.
    pub max_age_seconds: u64,
}

impl From<wire::connection_lease_limits::Reader<'_>> for ConnectionLeaseLimits {
    fn from(value: wire::connection_lease_limits::Reader<'_>) -> Self {
        Self {
            idle_seconds: value.get_idle_seconds(),
            tcp_first_input_seconds: value.get_tcp_first_input_seconds(),
            max_age_seconds: value.get_max_age_seconds(),
        }
    }
}

/// Limits on the server's producer-window registry, as advertised at discovery.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProducerLimits {
    /// Maximum number of tracked producer windows.
    pub max_streams: u32,
    /// Maximum retained gap intervals per window.
    pub max_gap_intervals: u32,
    /// Maximum encoded registry size.
    pub max_registry_bytes: u64,
    /// Retention of producer-window metadata in seconds.
    pub metadata_retention_seconds: u64,
}

#[derive(Clone, Copy)]
pub(crate) enum Contract {
    Query,
    Producer,
}

impl Discovery {
    /// Decode and enforce the current typed discovery contract.
    pub fn decode(value: wire::discovery::Reader<'_>) -> Result<Self, Error> {
        Self::decode_for(value, Contract::Query)
    }

    /// Require the producer profile, independently of the daemon's query schema version.
    pub fn decode_producer(value: wire::discovery::Reader<'_>) -> Result<Self, Error> {
        Self::decode_for(value, Contract::Producer)
    }

    fn decode_for(value: wire::discovery::Reader<'_>, contract: Contract) -> Result<Self, Error> {
        let raw_profiles = value.get_producer_profiles()?;
        if raw_profiles.len() > 128 {
            return Err(Error::Protocol("discovery exceeds 128 producer profiles"));
        }
        let mut producer_profiles = Vec::with_capacity(raw_profiles.len() as usize);
        for profile in raw_profiles.iter() {
            let profile = profile?.to_str()?;
            if profile.is_empty() || profile.len() > 128 {
                return Err(Error::Protocol(
                    "producer profile names must contain 1..128 bytes",
                ));
            }
            producer_profiles.push(profile.to_owned());
        }
        let durability = value.get_durability_status()?;
        let limits = value.get_producer_limits()?;
        let role = value.get_instance_role();
        let discovery = Self {
            durability_session: durability.get_session_id()?.to_str()?.to_owned(),
            max_buffered_writes: durability.get_max_pending_writes(),
            max_buffered_bytes: durability.get_max_pending_bytes(),
            flush_interval_ms: durability.get_flush_interval_ms(),
            store_id: value.get_store_id()?.to_str()?.to_owned(),
            schema_version: value.get_schema_version(),
            producer_profiles,
            sealed_event_deadline_ms: value.get_sealed_event_deadline_ms(),
            connection_lease: value
                .has_connection_lease()
                .then(|| {
                    value
                        .get_connection_lease()
                        .map(ConnectionLeaseLimits::from)
                })
                .transpose()?,
            producer_limits: ProducerLimits {
                max_streams: limits.get_max_streams(),
                max_gap_intervals: limits.get_max_gap_intervals(),
                max_registry_bytes: limits.get_max_registry_bytes(),
                metadata_retention_seconds: limits.get_metadata_retention_seconds(),
            },
            max_inline_artifact_bytes: value.get_max_inline_artifact_bytes(),
            max_records_per_artifact: value.get_max_records_per_artifact(),
            text_analyzer: value.get_text_analyzer()?.to_str()?.to_owned(),
            text_profile: value.get_text_profile()?.to_str()?.to_owned(),
            rdf_profile: value.get_rdf_profile()?.to_str()?.to_owned(),
            max_event_batch_bytes: value.get_max_event_batch_bytes(),
            max_event_batch_events: value.get_max_event_batch_events(),
            // Zero is legal for each: only a part split needs them (#280).
            max_import_bytes: value.get_max_import_bytes(),
            raw_record_bytes: value.get_raw_record_bytes(),
            retire_row_budget: value.get_retire_row_budget(),
            retire_chunk_charge: value.get_retire_chunk_charge(),
            retire_record_charge: value.get_retire_record_charge(),
            instance_role: role.unwrap_or(wire::InstanceRole::Unspecified),
            unknown_instance_role: role.err().map(|ordinal| ordinal.0),
        };
        discovery.require(contract)?;
        if !valid_store_id(&discovery.store_id)
            || discovery.max_inline_artifact_bytes == 0
            || discovery.max_records_per_artifact == 0
            || discovery.durability_session.is_empty()
            || discovery.durability_session.len() > 128
            || discovery.max_buffered_writes == 0
            || discovery.max_buffered_bytes == 0
            || !(1..=60000).contains(&discovery.flush_interval_ms)
        {
            return Err(Error::Unsupported(
                "requires bounded durability discovery, artifact limits and a persistent store identity",
            ));
        }
        Ok(discovery)
    }

    /// Check the contract of an operation using this connection's cached discovery.
    pub(crate) fn require(&self, contract: Contract) -> Result<(), Error> {
        if matches!(contract, Contract::Query)
            && let Some(role) = self.unknown_instance_role
        {
            return Err(capnp::NotInSchema(role).into());
        }
        match contract {
            Contract::Query
                if self.schema_version != crate::SCHEMA_VERSION
                    || self.max_event_batch_bytes != crate::MAX_EVENT_BATCH_BYTES as u32
                    || self.max_event_batch_events != crate::MAX_EVENT_BATCH_EVENTS as u32
                    || self.text_analyzer.is_empty()
                    || self.text_profile.is_empty()
                    || self.rdf_profile.is_empty() =>
            {
                return Err(Error::Unsupported(
                    "requires the current schema version, matching event batch limits and nonempty text and RDF identities; use a matching daemon executable",
                ));
            }
            Contract::Producer
                if !self
                    .producer_profiles
                    .iter()
                    .any(|p| p == crate::PRODUCER_PROFILE) =>
            {
                return Err(Error::Unsupported(crate::PRODUCER_PROFILE));
            }
            Contract::Producer
                if self.max_event_batch_events == 0
                    || self.max_event_batch_bytes < crate::MAX_EVENT_BYTES as u32 =>
            {
                return Err(Error::Unsupported(
                    "producer event batch ceilings must admit at least one event and 64 KiB",
                ));
            }
            _ => {}
        }
        Ok(())
    }

    pub(crate) fn event_limits(&self) -> crate::events::BatchLimits {
        let mut limits = crate::events::BatchLimits::NATIVE.bounded(
            self.max_records_per_artifact
                .min(u64::from(self.max_event_batch_events)) as usize,
            self.max_event_batch_bytes as usize,
        );
        limits.payload_bytes = self.max_inline_artifact_bytes.min(limits.bytes as u64) as usize;
        limits
    }

    /// Request and validate discovery on a generated capability.
    pub async fn fetch(client: &wire::sparrow::Client) -> Result<Self, Error> {
        Contract::Query.discover(client).await
    }
}

impl Contract {
    pub(crate) async fn discover(self, client: &wire::sparrow::Client) -> Result<Discovery, Error> {
        let response = client
            .discover_request()
            .send()
            .promise
            .await
            .map_err(|error| {
                if error.kind == capnp::ErrorKind::Unimplemented {
                    match self {
                        Self::Query => Error::Unsupported(
                            "requires typed discovery; use a matching daemon executable",
                        ),
                        Self::Producer => Error::Unsupported(crate::PRODUCER_PROFILE),
                    }
                } else {
                    Error::from(error)
                }
            })?;
        match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => Discovery::decode_for(value?, self),
            wire::result::Error(error) => Err(Error::domain(error?)),
        }
    }
}

/// Acceptance metadata and current durability; inspection does not extend retention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactReceipt {
    /// Current-session synchronization fence, present exactly for buffered acknowledgments.
    pub buffered_through: Option<DurabilityWatermark>,
    /// Persistent store identity.
    pub store_id: String,
    /// Stable artifact occurrence identity.
    pub id: String,
    /// Producer's source label.
    pub source: String,
    /// Explicit original format.
    pub format: wire::Format,
    /// Server receipt clock in UTC Unix milliseconds.
    pub received_at: u64,
    /// Ordinary retention deadline in UTC Unix milliseconds.
    pub expires_at: u64,
    /// Exact original byte length.
    pub byte_length: u64,
    /// Whole-artifact content digest identity.
    pub payload_id: String,
    /// Ordered record occurrence identities.
    pub record_ids: Vec<String>,
    /// Acknowledged write durability.
    pub durability: wire::Durability,
}

impl ArtifactReceipt {
    /// Require this inspection/acceptance receipt to belong to the discovered store.
    pub(crate) fn require_store(&self, discovery: &Discovery) -> Result<(), Error> {
        if self.store_id != discovery.store_id
            || self.record_ids.len() as u64 > discovery.max_records_per_artifact
            || self
                .buffered_through
                .as_ref()
                .is_some_and(|fence| fence.session_id != discovery.durability_session)
        {
            return Err(Error::Protocol(
                "artifact receipt contradicts discovered store, session or record limit",
            ));
        }
        Ok(())
    }

    /// Decode one generated receipt without a JSON operation envelope.
    pub fn decode(value: wire::artifact::Reader<'_>) -> Result<Self, Error> {
        let receipt = Self {
            buffered_through: match value.get_buffered_through()?.which()? {
                wire::optional_durability_watermark::Absent(()) => None,
                wire::optional_durability_watermark::Value(value) => {
                    Some(DurabilityWatermark::decode(value?)?)
                }
            },
            store_id: value.get_store_id()?.to_str()?.to_owned(),
            id: value.get_id()?.to_str()?.to_owned(),
            source: value.get_source()?.to_str()?.to_owned(),
            format: value.get_format()?,
            received_at: value.get_received_at()?.get_unix_millis(),
            expires_at: value.get_expires_at()?.get_unix_millis(),
            byte_length: value.get_byte_length(),
            payload_id: value.get_payload_id()?.to_str()?.to_owned(),
            record_ids: value
                .get_record_ids()?
                .iter()
                .map(|id| Ok(id?.to_str()?.to_owned()))
                .collect::<Result<_, Error>>()?,
            durability: value.get_durability()?,
        };
        receipt.validate()?;
        Ok(receipt)
    }

    /// Validate a decoded receipt, including one recovered from a local publication journal.
    pub(crate) fn validate(&self) -> Result<(), Error> {
        let receipt = self;
        if (receipt.durability == wire::Durability::Buffered) != receipt.buffered_through.is_some()
            || receipt
                .buffered_through
                .as_ref()
                .is_some_and(|token| token.store_id != receipt.store_id || token.sequence == 0)
        {
            return Err(Error::Protocol(
                "artifact durability and synchronization fence disagree",
            ));
        }
        if !valid_store_id(&receipt.store_id)
            || !receipt.id.starts_with("artifact:")
            || receipt.id.len() <= 9
            || receipt.id.len() > 128
            || receipt.source.is_empty()
            || receipt.source.len() > 512
            || receipt.expires_at <= receipt.received_at
            || receipt
                .payload_id
                .strip_prefix("blake3:")
                .and_then(purrdf_hash::hex::decode_32_canonical)
                .is_none()
        {
            return Err(Error::Protocol(
                "artifact receipt has invalid identity, source or retention interval",
            ));
        }
        let mut ids = std::collections::BTreeSet::new();
        if receipt.record_ids.iter().any(|id| {
            !id.starts_with("record:")
                || id.len() <= 7
                || id.len() > 128
                || !ids.insert(id.as_str())
        }) {
            return Err(Error::Protocol(
                "artifact receipt contains invalid or duplicate record identities",
            ));
        }
        Ok(())
    }
}

/// The concrete input an acknowledgment must certify before a producer can retire it.
pub(crate) struct ExpectedArtifact<'a> {
    pub discovery: &'a Discovery,
    pub source: &'a str,
    pub format: wire::Format,
    pub byte_length: u64,
    pub payload_id: String,
    pub durability: wire::Durability,
    pub record_count: Option<usize>,
}

impl ExpectedArtifact<'_> {
    pub(crate) fn certify(&self, receipt: ArtifactReceipt) -> Result<ArtifactReceipt, Error> {
        receipt.require_store(self.discovery)?;
        if receipt.source != self.source
            || receipt.format != self.format
            || receipt.byte_length != self.byte_length
            || receipt.payload_id != self.payload_id
            || self.durability == wire::Durability::Durable
                && receipt.durability != wire::Durability::Durable
            || self
                .record_count
                .is_some_and(|count| count != receipt.record_ids.len())
        {
            return Err(Error::Protocol(
                "artifact acknowledgment does not certify the requested source, format, exact bytes, record count and durability",
            ));
        }
        Ok(receipt)
    }
}

impl<'a> ExpectedArtifact<'a> {
    pub(crate) fn bytes(
        discovery: &'a Discovery,
        source: &'a str,
        format: wire::Format,
        bytes: &[u8],
        durability: wire::Durability,
        record_count: Option<usize>,
    ) -> Self {
        Self {
            discovery,
            source,
            format,
            byte_length: bytes.len() as u64,
            payload_id: format!(
                "blake3:{}",
                purrdf_hash::hex::encode(purrdf_hash::blake3::hash(bytes).as_bytes())
            ),
            durability,
            record_count,
        }
    }
}

/// A flush fence scoped to one persistent store and opened daemon session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurabilityWatermark {
    /// Persistent store identity.
    pub store_id: String,
    /// Opened daemon session; previous-session fences are refused.
    pub session_id: String,
    /// Accepted or synchronized native database sequence.
    pub sequence: u64,
}
impl DurabilityWatermark {
    /// Check bounded identities and encode the generated typed contract.
    pub fn encode(&self, mut out: wire::durability_watermark::Builder<'_>) -> Result<(), Error> {
        if self.store_id.is_empty()
            || self.store_id.len() > 128
            || self.session_id.is_empty()
            || self.session_id.len() > 128
        {
            return Err(Error::InvalidArgument(
                "durability fence needs bounded nonempty store/session identities",
            ));
        }
        out.set_store_id(&self.store_id);
        out.set_session_id(&self.session_id);
        out.set_sequence(self.sequence);
        Ok(())
    }
    /// Decode a bounded synchronization fence without interpreting it across sessions.
    pub fn decode(value: wire::durability_watermark::Reader<'_>) -> Result<Self, Error> {
        let store = value.get_store_id()?.to_str()?;
        let session = value.get_session_id()?.to_str()?;
        if store.is_empty() || store.len() > 128 || session.is_empty() || session.len() > 128 {
            return Err(Error::Protocol(
                "durability fence has invalid store/session identities",
            ));
        }
        Ok(Self {
            store_id: store.to_owned(),
            session_id: session.to_owned(),
            sequence: value.get_sequence(),
        })
    }
}

/// Shared typed synchronization path for the SDK and agent adapters.
pub async fn flush_through(
    client: &wire::sparrow::Client,
    through: &DurabilityWatermark,
) -> Result<DurabilityWatermark, Error> {
    let mut request = client.flush_request();
    through.encode(request.get().init_through())?;
    let response = request.send().promise.await?;
    let reached = match response.get()?.get_result()?.which()? {
        wire::result::Ok(value) => DurabilityWatermark::decode(value?)?,
        wire::result::Error(error) => return Err(Error::domain(error?)),
    };
    if reached.store_id != through.store_id
        || reached.session_id != through.session_id
        || reached.sequence < through.sequence
    {
        return Err(Error::Protocol(
            "flush reply does not cover the requested session fence",
        ));
    }
    Ok(reached)
}

/// One discovered store for SDK acceptance, ingestion status and retention certificates.
#[cfg(test)]
pub(crate) fn client_discovery() -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut message = capnp::message::Builder::new_default();
    let mut out = message.init_root::<wire::discovery::Builder<'_>>();
    out.set_store_id("store:test");
    out.set_schema_version(crate::SCHEMA_VERSION);
    out.set_max_inline_artifact_bytes(1024);
    out.set_max_records_per_artifact(2);
    out.set_text_analyzer("native");
    out.set_text_profile("text");
    out.set_rdf_profile("rdf");
    out.set_max_event_batch_bytes(crate::MAX_EVENT_BATCH_BYTES as u32);
    out.set_max_event_batch_events(crate::MAX_EVENT_BATCH_EVENTS as u32);
    let mut durability = out.init_durability_status();
    durability.set_session_id("session:test");
    durability.set_max_pending_writes(256);
    durability.set_max_pending_bytes(64 * 1024 * 1024);
    durability.set_flush_interval_ms(2000);
    message
}

#[cfg(test)]
mod tests {
    #[test]
    fn acceptance_refuses_unrelated_content_scope_durability_and_membership() {
        use super::*;
        let message = client_discovery();
        let discovery = Discovery::decode(message.get_root_as_reader().unwrap()).unwrap();
        let expected = ExpectedArtifact::bytes(
            &discovery,
            "source",
            wire::Format::Jsonl,
            b"{}\n",
            wire::Durability::Durable,
            Some(1),
        );
        let valid = ArtifactReceipt {
            buffered_through: None,
            store_id: discovery.store_id.clone(),
            id: "artifact:test".into(),
            source: "source".into(),
            format: wire::Format::Jsonl,
            received_at: 1,
            expires_at: 2,
            byte_length: 3,
            payload_id: expected.payload_id.clone(),
            record_ids: vec!["record:test".into()],
            durability: wire::Durability::Durable,
        };
        assert_eq!(expected.certify(valid.clone()).unwrap(), valid);
        for alteration in 0..8 {
            let mut receipt = valid.clone();
            match alteration {
                0 => receipt.store_id = "store:foreign".into(),
                1 => receipt.source = "another".into(),
                2 => receipt.format = wire::Format::Log,
                3 => receipt.byte_length = 4,
                4 => receipt.payload_id = "blake3:another".into(),
                5 => {
                    receipt.durability = wire::Durability::Buffered;
                    receipt.buffered_through = Some(DurabilityWatermark {
                        store_id: discovery.store_id.clone(),
                        session_id: discovery.durability_session.clone(),
                        sequence: 1,
                    });
                }
                6 => receipt.record_ids.clear(),
                7 => {
                    receipt.record_ids =
                        vec!["record:1".into(), "record:2".into(), "record:3".into()]
                }
                _ => unreachable!(),
            }
            assert!(
                matches!(expected.certify(receipt), Err(Error::Protocol(_))),
                "alteration {alteration}"
            );
        }
        let buffered = ExpectedArtifact::bytes(
            &discovery,
            "source",
            wire::Format::Jsonl,
            b"{}\n",
            wire::Durability::Buffered,
            Some(1),
        );
        assert!(
            buffered.certify(valid.clone()).is_ok(),
            "a stronger durable acknowledgment is valid"
        );
        let mut receipt = valid;
        receipt.durability = wire::Durability::Buffered;
        receipt.buffered_through = Some(DurabilityWatermark {
            store_id: discovery.store_id.clone(),
            session_id: "session:previous".into(),
            sequence: 1,
        });
        assert!(matches!(buffered.certify(receipt), Err(Error::Protocol(_))));
    }

    /// Query clients require matching schema and index identities. Producers continue across
    /// additive schema changes, including unknown trailing fields, if their profile is present.
    #[test]
    fn producer_discovery_accepts_a_newer_schema_that_advertises_its_profile() {
        for (version, rdf) in [
            (22, "rdf"),
            (crate::SCHEMA_VERSION + 1, ""),
            (crate::SCHEMA_VERSION, ""),
            (crate::SCHEMA_VERSION, "rdf"),
        ] {
            let mut message = capnp::message::Builder::new_default();
            // Allocate a real wire struct with a larger pointer section. Its last pointer
            // survives the discovery writes and represents a field unknown to this reader.
            message
                .init_root::<super::wire::status::Builder<'_>>()
                .init_producer_profiles(1)
                .set(0, "unknown trailing field");
            let mut status = message
                .get_root::<super::wire::discovery::Builder<'_>>()
                .unwrap();
            status.set_schema_version(version);
            status.set_store_id("store:test");
            status.set_text_analyzer("analyzer");
            status.set_text_profile("text");
            status.set_rdf_profile(rdf);
            status.set_max_inline_artifact_bytes(1024);
            status.set_max_records_per_artifact(100);
            status.set_max_event_batch_bytes(crate::MAX_EVENT_BATCH_BYTES as u32);
            status.set_max_event_batch_events(crate::MAX_EVENT_BATCH_EVENTS as u32);
            let mut durability = status.reborrow().init_durability_status();
            durability.set_session_id("session:test");
            durability.set_max_pending_writes(256);
            durability.set_max_pending_bytes(64 * 1024 * 1024);
            durability.set_flush_interval_ms(2000);
            assert!(matches!(
                super::Discovery::decode_producer(status.reborrow_as_reader()),
                Err(super::Error::Unsupported(crate::PRODUCER_PROFILE))
            ));
            let mut profiles = status.reborrow().init_producer_profiles(2);
            profiles.set(0, "future-profile/1");
            profiles.set(1, crate::PRODUCER_PROFILE);
            assert!(super::Discovery::decode_producer(status.reborrow_as_reader()).is_ok());
            let decoded = super::Discovery::decode(status.reborrow_as_reader());
            if version == crate::SCHEMA_VERSION && !rdf.is_empty() {
                let value = decoded.unwrap();
                assert_eq!(value.store_id, "store:test");
                assert_eq!(value.durability_session, "session:test");
                // A future enum member must survive discovery but can never satisfy a
                // required known role. Set its raw ordinal through the schema descriptor.
                use capnp::introspect::{Introspect, TypeVariant};
                let TypeVariant::Enum(schema) = super::wire::InstanceRole::introspect().which()
                else {
                    panic!("instance role descriptor");
                };
                let capnp::dynamic_value::Builder::Struct(mut dynamic) = status.reborrow().into()
                else {
                    panic!("discovery descriptor");
                };
                let future =
                    capnp::dynamic_value::Enum::new(42, capnp::schema::EnumSchema::new(schema));
                dynamic.set_named("instanceRole", future.into()).unwrap();
                let discovery =
                    super::Discovery::decode_producer(status.reborrow_as_reader()).unwrap();
                assert_eq!(discovery.unknown_instance_role, Some(42));
                crate::Expect::store("store:test")
                    .require(&discovery)
                    .unwrap();
                let mismatch = crate::Expect::new(crate::RequiredRole::Scratch)
                    .require(&discovery)
                    .unwrap_err();
                assert!(
                    matches!(mismatch, super::Error::Domain { code, message, .. } if code == "INSTANCE_MISMATCH" && message.contains("ordinal 42"))
                );
                assert!(super::Discovery::decode(status.reborrow_as_reader()).is_err());
            } else {
                assert!(matches!(decoded, Err(super::Error::Unsupported(_))));
            }
            for (events, bytes, compatible) in [
                (0, crate::MAX_EVENT_BYTES, false),
                (1, crate::MAX_EVENT_BYTES - 1, false),
                (128, crate::MAX_EVENT_BYTES, true),
                (u32::MAX, u32::MAX as usize, true),
            ] {
                status.set_max_event_batch_events(events);
                status.set_max_event_batch_bytes(bytes as u32);
                let decoded = super::Discovery::decode_producer(status.reborrow_as_reader());
                if compatible {
                    let limits = decoded.unwrap().event_limits();
                    assert_eq!(limits.events, (events as usize).min(100));
                    assert_eq!(limits.bytes, bytes.min(crate::MAX_EVENT_BATCH_BYTES));
                    assert_eq!(limits.payload_bytes, 1024);
                } else {
                    assert!(matches!(decoded, Err(super::Error::Unsupported(_))));
                }
            }
            assert_eq!(
                message
                    .get_root_as_reader::<super::wire::status::Reader<'_>>()
                    .unwrap()
                    .get_producer_profiles()
                    .unwrap()
                    .get(0)
                    .unwrap()
                    .to_str()
                    .unwrap(),
                "unknown trailing field"
            );
        }
    }
}
