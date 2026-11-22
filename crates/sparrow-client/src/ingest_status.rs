// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Bounded typed ingestion status with one certificate shared by all callers.

use crate::{Error, sparrow_capnp as wire};

/// Explicit immutable request namespace; equal labels in different namespaces differ.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngestSelector {
    /// Original direct-intake or upload request.
    Request(String),
    /// Native opaque import identity returned by admission.
    Import(String),
    /// Original producer handoff request.
    Handoff(String),
}

impl IngestSelector {
    /// Validate the identity without deriving a store-owned import identity.
    pub fn validate(&self) -> Result<(), Error> {
        let (Self::Request(id) | Self::Import(id) | Self::Handoff(id)) = self;
        if id.is_empty()
            || id.len() > 128
            || (matches!(self, Self::Import(_))
                && id
                    .strip_prefix("import:")
                    .and_then(purrdf_hash::hex::decode_32_canonical)
                    .is_none())
        {
            return Err(Error::InvalidArgument(
                "require a bounded request ID or canonical native import ID",
            ));
        }
        Ok(())
    }

    /// Encode the explicit generated selector union.
    pub fn encode(&self, mut out: wire::ingest_selector::Builder<'_>) -> Result<(), Error> {
        self.validate()?;
        match self {
            Self::Request(id) => out.set_request(id),
            Self::Import(id) => out.set_import(id),
            Self::Handoff(id) => out.set_handoff(id),
        }
        Ok(())
    }

    /// Decode a bounded selector before taking ownership of its text.
    pub fn decode(value: wire::ingest_selector::Reader<'_>) -> Result<Self, Error> {
        let (text, branch) = match value.which()? {
            wire::ingest_selector::Request(id) => (id?, 0),
            wire::ingest_selector::Import(id) => (id?, 1),
            wire::ingest_selector::Handoff(id) => (id?, 2),
        };
        if text.len() > 128 {
            return Err(Error::Protocol("ingestion selector exceeds 128 bytes"));
        }
        let id = text.to_str()?.to_owned();
        let selector = match branch {
            0 => Self::Request(id),
            1 => Self::Import(id),
            _ => Self::Handoff(id),
        };
        selector.validate()?;
        Ok(selector)
    }
}

/// Admission for one all-or-error status read.
#[derive(Clone, Copy, Debug)]
pub struct IngestReadLimits {
    /// Record and pin scan ceiling, 1..100000.
    pub scan_budget: u32,
    /// Aggregate native catalog construction ceiling, 1..33554432 bytes.
    pub catalog_bytes: u32,
}

impl Default for IngestReadLimits {
    fn default() -> Self {
        Self {
            scan_budget: 10000,
            catalog_bytes: 33554432,
        }
    }
}

/// Certified status in an owned Cap'n Proto message, bounded to 64 KiB.
/// The generated reader exposes typed fields without a second representation.
pub struct IngestStatus {
    message: capnp::message::Builder<capnp::message::HeapAllocator>,
}

impl IngestStatus {
    /// Borrow the certified, immutable generated contract.
    pub fn reader(&self) -> Result<wire::ingest_status::Reader<'_>, Error> {
        Ok(self.message.get_root_as_reader()?)
    }

    fn certify(
        value: wire::ingest_status::Reader<'_>,
        store: &str,
        selector: &IngestSelector,
        discovery: &crate::Discovery,
        limits: IngestReadLimits,
    ) -> Result<Self, Error> {
        if value.total_size()?.word_count > 8192 || value.total_size()?.cap_count != 0 {
            return Err(Error::Protocol(
                "ingestion status exceeds its fixed message ceiling",
            ));
        }
        if value.get_store_id()?.to_str()? != store
            || IngestSelector::decode(value.get_selector()?)? != *selector
        {
            return Err(Error::Protocol(
                "ingestion status changed store or selector",
            ));
        }
        let import = crate::receipt::optional_identity(value.get_import_id()?)?;
        if let Some(id) = &import {
            IngestSelector::Import(id.clone()).validate()?;
        }
        let request = value.get_request_id()?.to_str()?;
        if request.is_empty()
            || request.len() > 128
            || matches!(selector, IngestSelector::Request(id) if id != request)
            || matches!(selector, IngestSelector::Import(id) if import.as_ref() != Some(id))
        {
            return Err(Error::Protocol(
                "ingestion status changed its resolved identity",
            ));
        }
        let total = match value.get_total_records()?.which()? {
            wire::optional_record_count::Unknown(()) => None,
            wire::optional_record_count::Known(n) => Some(n),
        };
        let inspected = value.get_inspected_records();
        if inspected > u64::from(limits.scan_budget)
            || total.is_some_and(|n| n > discovery.max_records_per_artifact || inspected > n)
        {
            return Err(Error::Protocol(
                "ingestion status exceeds discovered or requested record limits",
            ));
        }
        for (counts, text) in [
            (value.get_json()?, false),
            (value.get_rdf()?, false),
            (value.get_text()?, true),
        ] {
            let classified = [
                counts.get_complete(),
                counts.get_failed(),
                counts.get_incomplete(),
                counts.get_not_applicable(),
                counts.get_unavailable(),
            ]
            .into_iter()
            .try_fold(0u64, u64::checked_add);
            if classified != Some(inspected)
                || (!text && counts.get_incomplete() != 0)
                || classified.and_then(|n| n.checked_add(counts.get_pending()))
                    != Some(total.unwrap_or(0))
            {
                return Err(Error::Protocol(
                    "ingestion status projection counts contradict membership",
                ));
            }
        }
        let accepted = match value.get_accepted()?.which()? {
            wire::optional_ingest_acceptance::Absent(()) => None,
            wire::optional_ingest_acceptance::Value(a) => Some(a?),
        };
        let staging = match value.get_staging()?.which()? {
            wire::optional_import_progress::Absent(()) => None,
            wire::optional_import_progress::Value(p) => {
                let p = crate::ImportProgress::decode(p?)?;
                p.certify_identity(
                    store,
                    import
                        .as_deref()
                        .ok_or(Error::Protocol("staging omitted admitted import"))?,
                )?;
                if p.request_id != request {
                    return Err(Error::Protocol("staging changed original request"));
                }
                Some(p)
            }
        };
        let preparation = match value.get_preparation()?.which()? {
            wire::optional_import_preparation::Absent(()) => None,
            wire::optional_import_preparation::Value(p) => Some(p?),
        };
        if let Some(p) = preparation {
            if accepted.is_some()
                || p.get_store_id()?.to_str()? != store
                || Some(p.get_import_id()?.to_str()?) != import.as_deref()
                || Some(p.get_total_records()) != total
                || p.get_prepared_records() != inspected
                || !staging.as_ref().is_some_and(|s| s.layout_complete)
            {
                return Err(Error::Protocol(
                    "private preparation contradicts ingestion progress",
                ));
            }
            if let wire::optional_raw_preparation::Value(raw) = p.get_raw()?.which()? {
                let raw = raw?;
                if raw.get_record_index() != inspected
                    || raw.get_prepared_chunks() > raw.get_total_chunks()
                    || inspected == p.get_total_records()
                    || raw.get_reserved_record_id()?.len() > 128
                {
                    return Err(Error::Protocol(
                        "private raw preparation contradicts record progress",
                    ));
                }
            }
        }
        if let Some(a) = accepted {
            if total != Some(a.get_record_count())
                || inspected != a.get_record_count()
                || a.get_artifact_id()?.len() > 128
                || !a.get_artifact_id()?.to_str()?.starts_with("artifact:")
                || a.get_source()?.len() > 512
                || a.get_expires_at()?.get_unix_millis() < a.get_received_at()?.get_unix_millis()
            {
                return Err(Error::Protocol(
                    "original acceptance contradicts ingestion progress",
                ));
            }
            let fence = match a.get_buffered_through()?.which()? {
                wire::optional_durability_watermark::Absent(()) => None,
                wire::optional_durability_watermark::Value(f) => {
                    Some(crate::DurabilityWatermark::decode(f?)?)
                }
            };
            if (a.get_durability()? == wire::Durability::Buffered) != fence.is_some()
                || fence.as_ref().is_some_and(|f| {
                    f.store_id != store || f.session_id != discovery.durability_session
                })
            {
                return Err(Error::Protocol(
                    "acceptance synchronization fence contradicts discovered session",
                ));
            }
            if let Some(s) = &staging
                && (s.published_artifact.as_deref() != Some(a.get_artifact_id()?.to_str()?)
                    || s.source != a.get_source()?.to_str()?
                    || s.format != a.get_format()?
                    || s.byte_length != a.get_byte_length()
                    || s.payload_id != a.get_payload_id()?.to_str()?)
            {
                return Err(Error::Protocol(
                    "acceptance changed original staging provenance",
                ));
            }
        }
        let ownership = match value.get_ownership()?.which()? {
            wire::optional_ingest_ownership::Absent(()) => None,
            wire::optional_ingest_ownership::Value(o) => Some(o?),
        };
        if import.is_some() != (staging.is_some() || ownership.is_some()) {
            return Err(Error::Protocol(
                "import reference contradicts admitted request state",
            ));
        }
        let removed = matches!(
            value.get_removed_at()?.which()?,
            wire::optional_instant::Value(_)
        );
        let deletion = value.get_deletion()?;
        let tail = match value.get_tail()?.which()? {
            wire::optional_tail_generation::Absent(()) => None,
            wire::optional_tail_generation::Value(t) => Some(t?),
        };
        if accepted.is_none() && staging.is_none() && ownership.is_none() {
            return Err(Error::Protocol(
                "ingestion status has no original request state",
            ));
        }
        if removed != (deletion == wire::IngestDeletion::Removed)
            || ownership.is_none() != (deletion == wire::IngestDeletion::NotManaged)
            || (accepted.is_some() && tail.is_some())
        {
            return Err(Error::Protocol(
                "source removal contradicts ownership or publication",
            ));
        }
        if let Some(o) = ownership {
            if o.get_relative_path()?.len() > 4096
                || o.get_source()?.len() > 512
                || o.get_watch()?.len() > 128
            {
                return Err(Error::Protocol(
                    "source ownership exceeds bounded identity limits",
                ));
            }
            let observed = crate::CompletedFileIdentity::decode(o.get_observed()?)?;
            let claimed = match o.get_claimed()?.which()? {
                wire::optional_file_generation::Absent(()) => None,
                wire::optional_file_generation::Value(g) => {
                    Some(crate::CompletedFileIdentity::decode(g?)?)
                }
            };
            if claimed.is_none() != (deletion == wire::IngestDeletion::Reserved)
                || matches!(
                    deletion,
                    wire::IngestDeletion::Published | wire::IngestDeletion::Removed
                ) != (accepted.is_some() || tail.is_some())
            {
                return Err(Error::Protocol(
                    "source ownership phase contradicts original publication",
                ));
            }
            if let Some(a) = accepted
                && (a.get_source()?.to_str()? != o.get_source()?.to_str()?
                    || a.get_format()? != o.get_format()?
                    || a.get_byte_length() != observed.length)
            {
                return Err(Error::Protocol("publication contradicts source generation"));
            }
            if let Some(t) = tail {
                let finish = match t.get_finish()?.which()? {
                    wire::optional_tail_finish::Absent(()) => {
                        return Err(Error::Protocol(
                            "tail acknowledgement lacks completion intent",
                        ));
                    }
                    wire::optional_tail_finish::Value(f) => f?,
                };
                let intent = crate::TailFinishIntent::decode(finish.get_request()?)?;
                let receipt = crate::TailFinishReceipt::certify(t, &intent)?;
                if intent.store_id != store
                    || intent.observed != observed
                    || t.get_declaration()?.get_watch()?.to_str()? != o.get_watch()?.to_str()?
                    || t.get_declaration()?.get_source()?.to_str()? != o.get_source()?.to_str()?
                    || !t.get_complete()
                    || receipt.finished_at.is_none()
                    || total != Some(0)
                    || inspected != 0
                    || staging.is_some()
                    || preparation.is_some()
                {
                    return Err(Error::Protocol(
                        "tail acknowledgement contradicts source ownership",
                    ));
                }
            }
        } else if tail.is_some() {
            return Err(Error::Protocol("tail acknowledgement lacks ownership"));
        }
        let mut message = capnp::message::Builder::new_default();
        message.set_root(value)?;
        Ok(Self { message })
    }
}

/// Inspect staging, native index coverage, original acceptance and source removal.
/// This call never mutates intake, refreshes evidence lifetime or synchronizes writes.
pub async fn get_ingest_status(
    client: &wire::sparrow::Client,
    store: &str,
    selector: &IngestSelector,
    limits: IngestReadLimits,
) -> Result<IngestStatus, Error> {
    selector.validate()?;
    if store.is_empty()
        || store.len() > 128
        || limits.scan_budget == 0
        || limits.scan_budget > 100000
        || limits.catalog_bytes == 0
        || limits.catalog_bytes > 33554432
    {
        return Err(Error::InvalidArgument(
            "require a bounded store identity and positive ingestion status limits",
        ));
    }
    let discovery = crate::Discovery::fetch(client).await?;
    let mut call = client.ingest_status_request();
    let mut query = call.get().init_query();
    query.set_store_id(store);
    query.set_schema_version(crate::SCHEMA_VERSION);
    query.set_scan_budget(limits.scan_budget);
    query.set_catalog_bytes(limits.catalog_bytes);
    selector.encode(query.init_selector())?;
    let response = call.send().promise.await?;
    crate::receipt::certify_result(response.get()?.get_result()?, |value| {
        IngestStatus::certify(value, store, selector, &discovery, limits)
    })
}

/// One set part's receipt as the daemon kept it (#285).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartRecord {
    /// The part's artifact id.
    pub artifact_id: String,
    /// The part's byte length.
    pub byte_length: u64,
    /// The part's stored format.
    pub format: crate::Format,
}

/// What the daemon says about one set part (#285).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartRead {
    /// No receipt is kept: the part is not published.
    Absent,
    /// The receipt's artifact is stored.
    Stored(PartRecord),
    /// The artifact is gone and no record of it is stored: the part is sent again.
    Evicted(PartRecord),
    /// Eviction began and some record of the artifact is still stored.
    Evicting(PartRecord),
}

/// Read set part `request_id`'s receipt state by key. A daemon without the method answers the
/// protocol's unimplemented error, which stays [`Error::Transport`] for the caller to name.
pub async fn get_ingest_part_receipt(
    client: &wire::sparrow::Client,
    store: &str,
    request_id: &str,
) -> Result<PartRead, Error> {
    if store.is_empty() || store.len() > 128 || request_id.is_empty() || request_id.len() > 128 {
        return Err(Error::InvalidArgument(
            "require a bounded store identity and set part request ID",
        ));
    }
    let mut call = client.ingest_part_receipt_request();
    let mut query = call.get().init_query();
    query.set_store_id(store);
    query.set_schema_version(crate::SCHEMA_VERSION);
    query.set_request_id(request_id);
    let response = call.send().promise.await?;
    crate::receipt::certify_result(response.get()?.get_result()?, |value| {
        let record = |value: wire::ingest_part_receipt::Reader<'_>| -> Result<PartRecord, Error> {
            if value.get_request_id()?.to_str()? != request_id {
                return Err(Error::Protocol("part receipt changed its request"));
            }
            Ok(PartRecord {
                artifact_id: value.get_artifact_id()?.to_str()?.to_owned(),
                byte_length: value.get_byte_length(),
                format: value.get_format()?,
            })
        };
        Ok(match value.which()? {
            wire::ingest_part_state::Absent(()) => PartRead::Absent,
            wire::ingest_part_state::Stored(value) => PartRead::Stored(record(value?)?),
            wire::ingest_part_state::Evicted(value) => PartRead::Evicted(record(value?)?),
            wire::ingest_part_state::Evicting(value) => PartRead::Evicting(record(value?)?),
        })
    })
}

/// What the ingestion status of a part with no kept receipt says (#285).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PartGate {
    /// No draft and no receipt: nothing of the part was begun.
    NotFound,
    /// A live draft the daemon holds for the part.
    Draft(PartDraft),
    /// The draft's lease ended or it was cancelled, and its row is not reclaimed yet.
    Expired,
    /// A status that is neither: the text names it.
    Other(String),
}

/// The declared input and identity of a live draft.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartDraft {
    /// The staging identity, for a cancel.
    pub import_id: String,
    /// Declared byte length.
    pub byte_length: u64,
    /// Declared payload identity.
    pub payload_id: String,
    /// Declared format.
    pub format: crate::Format,
}

impl PartGate {
    /// The gate a status read that failed with `error` stands for: `NOT_FOUND` is no draft and
    /// `IMPORT_EXPIRED` is an unreclaimed one; any other failure stays the failure.
    pub(crate) fn from_error(error: Error) -> Result<Self, Error> {
        match &error {
            Error::Domain { code, .. } if code == "NOT_FOUND" => Ok(Self::NotFound),
            Error::Domain { code, .. } if code == "IMPORT_EXPIRED" => Ok(Self::Expired),
            _ => Err(error),
        }
    }
}

/// Classify set part `request_id` by its ingestion status, once it has no kept receipt.
pub(crate) async fn get_part_gate(
    client: &wire::sparrow::Client,
    store: &str,
    request_id: &str,
) -> Result<PartGate, Error> {
    let selector = IngestSelector::Request(request_id.to_owned());
    let status = match get_ingest_status(
        client,
        store,
        &selector,
        IngestReadLimits {
            scan_budget: 100_000,
            catalog_bytes: 33_554_432,
        },
    )
    .await
    {
        Ok(status) => status,
        Err(error) => return PartGate::from_error(error),
    };
    let reader = status.reader()?;
    let accepted = !matches!(
        reader.get_accepted()?.which()?,
        wire::optional_ingest_acceptance::Absent(())
    );
    let staging = match reader.get_staging()?.which()? {
        wire::optional_import_progress::Absent(()) => None,
        wire::optional_import_progress::Value(progress) => {
            Some(crate::ImportProgress::decode(progress?)?)
        }
    };
    Ok(match staging {
        Some(progress) if !accepted && progress.published_artifact.is_none() => {
            PartGate::Draft(PartDraft {
                import_id: progress.id,
                byte_length: progress.byte_length,
                payload_id: progress.payload_id,
                format: progress.format,
            })
        }
        Some(_) => PartGate::Other("a staging row that is accepted or published".into()),
        None if accepted => PartGate::Other("accepted".into()),
        None => PartGate::Other("neither staged nor accepted".into()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_certificate_rejects_changed_identity_counts_and_false_removal() {
        let message = crate::receipt::client_discovery();
        let discovery = crate::Discovery::decode(message.get_root_as_reader().unwrap()).unwrap();
        let selector = IngestSelector::Request("original".into());
        for fault in 0..9 {
            let mut message = capnp::message::Builder::new_default();
            let mut status = message.init_root::<wire::ingest_status::Builder<'_>>();
            status.set_store_id(&discovery.store_id);
            selector.encode(status.reborrow().init_selector()).unwrap();
            status.set_request_id("original");
            status.reborrow().init_total_records().set_known(1);
            status.set_inspected_records(1);
            status.reborrow().init_json().set_complete(1);
            status.reborrow().init_rdf().set_complete(1);
            status.reborrow().init_text().set_complete(1);
            let mut a = status.reborrow().init_accepted().init_value();
            a.set_artifact_id("artifact:original");
            a.set_source("source");
            a.set_record_count(1);
            match fault {
                0 => (),
                1 => status.set_store_id("store:another"),
                2 => status.reborrow().init_selector().set_handoff("original"),
                3 => status.reborrow().init_text().set_complete(2),
                4 => status.reborrow().init_json().set_incomplete(1),
                5 => status.reborrow().init_total_records().set_known(2),
                6 => status.set_deletion(wire::IngestDeletion::Removed),
                7 => status
                    .reborrow()
                    .init_removed_at()
                    .init_value()
                    .set_unix_millis(1),
                _ => status
                    .reborrow()
                    .init_accepted()
                    .init_value()
                    .set_durability(wire::Durability::Buffered),
            }
            let value = message.get_root_as_reader().unwrap();
            assert_eq!(
                IngestStatus::certify(
                    value,
                    &discovery.store_id,
                    &selector,
                    &discovery,
                    Default::default()
                )
                .is_ok(),
                fault == 0,
                "fault {fault}"
            );
        }
    }
}
