// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{Error, sparrow_capnp as wire};

/// Owned discovery information checked before producer mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discovery {
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
}

impl Discovery {
    /// Decode and enforce the current typed discovery contract.
    pub fn decode(value: wire::status::Reader<'_>) -> Result<Self, Error> {
        let discovery = Self {
            store_id: value.get_store_id()?.to_str()?.to_owned(),
            schema_version: value.get_schema_version(),
            max_artifact_bytes: value.get_max_artifact_bytes(),
            max_records_per_artifact: value.get_max_records_per_artifact(),
            text_analyzer: value.get_text_analyzer()?.to_str()?.to_owned(),
            text_profile: value.get_text_profile()?.to_str()?.to_owned(),
            rdf_profile: value.get_rdf_profile()?.to_str()?.to_owned(),
        };
        if discovery.schema_version != crate::SCHEMA_VERSION
            || discovery.store_id.is_empty()
            || discovery.text_analyzer.is_empty()
            || discovery.text_profile.is_empty()
            || discovery.rdf_profile.is_empty()
        {
            return Err(Error::Unsupported(
                "requires schema version 10 and nonempty store, text and RDF identities; use a matching daemon executable",
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

/// Immutable acceptance metadata; inspection does not extend evidence retention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactReceipt {
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
        Ok(Self {
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
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn discovery_refuses_old_schema_or_missing_profile_identity() {
        for (version, rdf) in [(9, "rdf"), (crate::SCHEMA_VERSION, "")] {
            let mut message = capnp::message::Builder::new_default();
            let mut status = message.init_root::<super::wire::status::Builder<'_>>();
            status.set_schema_version(version);
            status.set_store_id("store:test");
            status.set_text_analyzer("analyzer");
            status.set_text_profile("text");
            status.set_rdf_profile(rdf);
            assert!(matches!(
                super::Discovery::decode(status.into_reader()),
                Err(super::Error::Unsupported(_))
            ));
        }
    }
}
