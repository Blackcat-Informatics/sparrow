// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{Error, sparrow_capnp as wire};

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
    /// Maximum complete artifact accepted by the direct buffered-input operation.
    pub max_artifact_bytes: u64,
    /// Maximum record membership in one artifact.
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
}

impl Discovery {
    /// Decode and enforce the current typed discovery contract.
    pub fn decode(value: wire::status::Reader<'_>) -> Result<Self, Error> {
        let durability = value.get_durability_status()?;
        let discovery = Self {
            durability_session: durability.get_session_id()?.to_str()?.to_owned(),
            max_buffered_writes: durability.get_max_pending_writes(),
            max_buffered_bytes: durability.get_max_pending_bytes(),
            flush_interval_ms: durability.get_flush_interval_ms(),
            store_id: value.get_store_id()?.to_str()?.to_owned(),
            schema_version: value.get_schema_version(),
            max_artifact_bytes: value.get_max_artifact_bytes(),
            max_records_per_artifact: value.get_max_records_per_artifact(),
            text_analyzer: value.get_text_analyzer()?.to_str()?.to_owned(),
            text_profile: value.get_text_profile()?.to_str()?.to_owned(),
            rdf_profile: value.get_rdf_profile()?.to_str()?.to_owned(),
            max_event_batch_bytes: value.get_max_event_batch_bytes(),
            max_event_batch_events: value.get_max_event_batch_events(),
        };
        if discovery.schema_version != crate::SCHEMA_VERSION
            || discovery.store_id.is_empty()
            || discovery.text_analyzer.is_empty()
            || discovery.text_profile.is_empty()
            || discovery.rdf_profile.is_empty()
            || discovery.durability_session.is_empty()
            || discovery.durability_session.len() > 128
            || discovery.max_buffered_writes == 0
            || discovery.max_buffered_bytes == 0
            || !(1..=60000).contains(&discovery.flush_interval_ms)
            || discovery.max_event_batch_bytes != crate::MAX_EVENT_BATCH_BYTES as u32
            || discovery.max_event_batch_events != crate::MAX_EVENT_BATCH_EVENTS as u32
        {
            return Err(Error::Unsupported(
                "requires schema version 12, matching event batch limits, bounded durability discovery and nonempty store, text and RDF identities; use a matching daemon executable",
            ));
        }
        Ok(discovery)
    }

    /// Request and validate discovery on a generated capability.
    pub async fn fetch(client: &wire::sparrow::Client) -> Result<Self, Error> {
        let response = client.status_request().send().promise.await?;
        match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => Self::decode(value?),
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
        Ok(receipt)
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

#[cfg(test)]
mod tests {
    #[test]
    fn discovery_refuses_old_schema_or_missing_profile_identity() {
        for (version, rdf) in [(11, "rdf"), (crate::SCHEMA_VERSION, "")] {
            let mut message = capnp::message::Builder::new_default();
            let mut status = message.init_root::<super::wire::status::Builder<'_>>();
            status.set_schema_version(version);
            status.set_store_id("store:test");
            status.set_text_analyzer("analyzer");
            status.set_text_profile("text");
            status.set_rdf_profile(rdf);
            status.set_max_event_batch_bytes(crate::MAX_EVENT_BATCH_BYTES as u32);
            status.set_max_event_batch_events(crate::MAX_EVENT_BATCH_EVENTS as u32);
            let mut durability = status.reborrow().init_durability_status();
            durability.set_session_id("session:test");
            durability.set_max_pending_writes(256);
            durability.set_max_pending_bytes(64 * 1024 * 1024);
            durability.set_flush_interval_ms(2000);
            assert!(matches!(
                super::Discovery::decode(status.into_reader()),
                Err(super::Error::Unsupported(_))
            ));
        }
    }
}
