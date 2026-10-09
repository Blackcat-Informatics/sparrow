// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Immutable typed completed-file handoff intents and certified admission progress.

use crate::{CompletedFileIdentity, Error, sparrow_capnp as wire};

/// Preserve this exact intent across retries; do not recapture a replacement path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedHandoff {
    /// Persistent store identity, independent of a socket pathname.
    pub store_id: String,
    /// Caller-owned immutable retry identity.
    pub request_id: String,
    /// Registered finished-file watch identity.
    pub watch: String,
    /// Revision authorizing first admission.
    pub watch_revision: u64,
    /// Exact relative Unix path bytes.
    pub relative_path: Vec<u8>,
    /// Generation the producer has finished writing.
    pub generation: CompletedFileIdentity,
    /// Retire an existing completed tail checkpoint instead of importing again.
    pub tail_generation_id: Option<String>,
}

impl CompletedFileIdentity {
    /// Encode the same descriptor observation used by resumable uploads.
    pub fn encode(self, mut out: wire::file_generation::Builder<'_>) -> Result<(), Error> {
        let nanos = |value: i64| {
            u32::try_from(value)
                .ok()
                .filter(|v| *v < 1_000_000_000)
                .ok_or(Error::InvalidArgument(
                    "file clocks require nanoseconds below one billion",
                ))
        };
        out.set_device(self.device);
        out.set_inode(self.inode);
        out.set_byte_length(self.length);
        out.set_modified_seconds(self.modified_seconds);
        out.set_modified_nanos(nanos(self.modified_nanos)?);
        out.set_changed_seconds(self.changed_seconds);
        out.set_changed_nanos(nanos(self.changed_nanos)?);
        Ok(())
    }

    /// Decode typed clocks without guessing a time unit or accepting invalid fractions.
    pub fn decode(value: wire::file_generation::Reader<'_>) -> Result<Self, Error> {
        if value.get_modified_nanos() >= 1_000_000_000 || value.get_changed_nanos() >= 1_000_000_000
        {
            return Err(Error::InvalidArgument(
                "file clocks require nanoseconds below one billion",
            ));
        }
        Ok(Self {
            device: value.get_device(),
            inode: value.get_inode(),
            length: value.get_byte_length(),
            modified_seconds: value.get_modified_seconds(),
            modified_nanos: i64::from(value.get_modified_nanos()),
            changed_seconds: value.get_changed_seconds(),
            changed_nanos: i64::from(value.get_changed_nanos()),
        })
    }
}

impl CompletedHandoff {
    /// Encode one bounded intent for RPC or durable local retry storage.
    pub fn encode(&self, mut out: wire::watch_handoff::Builder<'_>) -> Result<(), Error> {
        self.validate()?;
        out.set_store_id(&self.store_id);
        out.set_request_id(&self.request_id);
        out.set_watch(&self.watch);
        out.set_watch_revision(self.watch_revision);
        out.set_relative_path(&self.relative_path);
        crate::encode_optional(
            out.reborrow().init_tail_generation_id(),
            self.tail_generation_id.as_deref(),
            |mut out, value| out.set_value(value),
            |mut out| out.set_absent(()),
        );
        self.generation.encode(out.init_generation())
    }

    /// Decode only bounded strings/path bytes before allocating owned arguments.
    pub fn decode(value: wire::watch_handoff::Reader<'_>) -> Result<Self, Error> {
        let store = value.get_store_id()?.to_str()?;
        let request = value.get_request_id()?.to_str()?;
        let watch = value.get_watch()?.to_str()?;
        let path = value.get_relative_path()?;
        if store.len() > 128 || request.len() > 128 || watch.len() > 128 || path.len() > 2048 {
            return Err(Error::InvalidArgument(
                "handoff labels or relative path exceed their admission bounds",
            ));
        }
        let intent = Self {
            store_id: store.into(),
            request_id: request.into(),
            watch: watch.into(),
            watch_revision: value.get_watch_revision(),
            relative_path: path.into(),
            generation: CompletedFileIdentity::decode(value.get_generation()?)?,
            tail_generation_id: crate::receipt::optional_identity(value.get_tail_generation_id()?)?,
        };
        intent.validate()?;
        Ok(intent)
    }

    fn validate(&self) -> Result<(), Error> {
        if [&self.store_id, &self.request_id, &self.watch]
            .iter()
            .any(|v| v.is_empty() || v.len() > 128)
            || self.watch_revision == 0
            || self.relative_path.is_empty()
            || self.relative_path.len() > 2048
            || self
                .tail_generation_id
                .as_ref()
                .is_some_and(|id| !id.starts_with("file-generation:") || id.len() > 128)
        {
            return Err(Error::InvalidArgument(
                "handoff requires bounded nonempty store/request/watch/path and a nonzero revision",
            ));
        }
        Ok(())
    }

    /// Save an immutable Cap'n Proto intent, not a new filesystem observation.
    pub fn write(&self, output: impl std::io::Write) -> Result<(), Error> {
        crate::intent_file::write(output, self)
    }

    /// Read one bounded intent; trailing bytes or a second message are refused.
    pub fn read(input: impl std::io::Read) -> Result<Self, Error> {
        crate::intent_file::read::<wire::watch_handoff::Owned, _>(input, Self::decode)
    }
}

impl crate::intent_file::EncodeIntent for CompletedHandoff {
    type Wire = wire::watch_handoff::Owned;
    fn encode_intent(&self, out: wire::watch_handoff::Builder<'_>) -> Result<(), Error> {
        self.encode(out)
    }
}

/// Durable admission progress. Only `Removed` confirms synchronized retirement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandoffReceipt {
    /// Original immutable intent, certified against the caller's request.
    pub request: CompletedHandoff,
    /// Original consumption import identity.
    pub import_id: String,
    /// Current journal phase, independent of discovery quiet timers.
    pub phase: wire::HandoffPhase,
    /// Original published artifact once publication is durable.
    pub artifact_id: Option<String>,
    /// Existing terminal tail identity once acknowledgement is durable.
    pub tail_generation_id: Option<String>,
}

impl HandoffReceipt {
    fn certify(
        value: wire::handoff_receipt::Reader<'_>,
        expected: &CompletedHandoff,
    ) -> Result<Self, Error> {
        let request = CompletedHandoff::decode(value.get_request()?)?;
        let phase = value.get_phase()?;
        let artifact_id = crate::receipt::optional_identity(value.get_artifact_id()?)?;
        let tail_generation_id =
            crate::receipt::optional_identity(value.get_tail_generation_id()?)?;
        let published = matches!(
            phase,
            wire::HandoffPhase::Published | wire::HandoffPhase::Removed
        );
        let publication_matches = match &request.tail_generation_id {
            Some(id) => {
                artifact_id.is_none()
                    && tail_generation_id.as_deref() == published.then_some(id.as_str())
            }
            None => tail_generation_id.is_none() && published == artifact_id.is_some(),
        };
        let import_id = value.get_import_id()?.to_str()?;
        if request != *expected
            || value.get_durability()? != wire::Durability::Durable
            || import_id.len() > 128
            || !import_id.starts_with("import:")
            || !publication_matches
            || artifact_id
                .as_ref()
                .is_some_and(|v| v.len() > 128 || !v.starts_with("artifact:"))
        {
            return Err(Error::Protocol(
                "handoff receipt contradicts its immutable intent or durable progress",
            ));
        }
        Ok(Self {
            request,
            import_id: import_id.into(),
            phase,
            artifact_id,
            tail_generation_id,
        })
    }
}

impl crate::Client {
    /// Admit a completed generation to the registered watch's destructive claim path.
    /// Persist the intent first; a transport failure requires retrying it unchanged.
    pub async fn handoff(&self, intent: &CompletedHandoff) -> Result<HandoffReceipt, Error> {
        completed_handoff(self.capability(), intent, false).await
    }

    /// Read original admission progress without admitting a new generation.
    pub async fn handoff_get(&self, intent: &CompletedHandoff) -> Result<HandoffReceipt, Error> {
        completed_handoff(self.capability(), intent, true).await
    }
}

/// Shared generated handoff client. `read_only` retrieves progress and never admits
/// a new source. Discovery binds both operations to the intent's original store.
pub async fn completed_handoff(
    client: &wire::sparrow::Client,
    intent: &CompletedHandoff,
    read_only: bool,
) -> Result<HandoffReceipt, Error> {
    let discovery = crate::Discovery::fetch(client).await?;
    if intent.store_id != discovery.store_id {
        return Err(Error::InvalidArgument(
            "handoff belongs to another persistent store",
        ));
    }
    if read_only {
        let mut call = client.watch_handoff_get_request();
        intent.encode(call.get().init_request())?;
        crate::receipt::certify_result(call.send().promise.await?.get()?.get_result()?, |value| {
            HandoffReceipt::certify(value, intent)
        })
    } else {
        let mut call = client.watch_handoff_request();
        intent.encode(call.get().init_request())?;
        crate::receipt::certify_result(call.send().promise.await?.get()?.get_result()?, |value| {
            HandoffReceipt::certify(value, intent)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent() -> CompletedHandoff {
        CompletedHandoff {
            store_id: "store:fixture".into(),
            request_id: "finished-1".into(),
            watch: "completed".into(),
            watch_revision: 7,
            relative_path: b"nested/\xff.jsonl".to_vec(),
            tail_generation_id: None,
            generation: CompletedFileIdentity {
                device: u64::MAX,
                inode: 42,
                length: 123,
                modified_seconds: -1,
                modified_nanos: 999_999_999,
                changed_seconds: i64::MAX,
                changed_nanos: 0,
            },
        }
    }

    #[test]
    fn immutable_intent_round_trips_raw_unix_paths_and_rejects_ambiguous_or_invalid_messages() {
        let expected = intent();
        let mut bytes = Vec::new();
        expected.write(&mut bytes).unwrap();
        assert_eq!(CompletedHandoff::read(bytes.as_slice()).unwrap(), expected);
        let mut tail = expected.clone();
        tail.tail_generation_id = Some("file-generation:fixture".into());
        let mut tail_bytes = Vec::new();
        tail.write(&mut tail_bytes).unwrap();
        assert_eq!(CompletedHandoff::read(tail_bytes.as_slice()).unwrap(), tail);
        bytes.push(0);
        assert!(CompletedHandoff::read(bytes.as_slice()).is_err());
        assert!(CompletedHandoff::read(&vec![0; 16_385][..]).is_err());
        let mut invalid = expected;
        invalid.generation.modified_nanos = -1;
        assert!(invalid.write(Vec::new()).is_err());
        invalid.generation.modified_nanos = 1_000_000_000;
        assert!(invalid.write(Vec::new()).is_err());
        let mut message = capnp::message::Builder::new_default();
        let mut wire = message.init_root::<wire::watch_handoff::Builder<'_>>();
        intent().encode(wire.reborrow()).unwrap();
        wire.init_generation().set_changed_nanos(1_000_000_000);
        assert!(CompletedHandoff::decode(message.get_root_as_reader().unwrap()).is_err());
    }

    #[test]
    fn tail_admission_certificate_requires_only_the_original_checkpoint_publication() {
        let mut expected = intent();
        expected.tail_generation_id = Some("file-generation:fixture".into());
        for phase in [
            wire::HandoffPhase::Reserved,
            wire::HandoffPhase::Claimed,
            wire::HandoffPhase::Published,
            wire::HandoffPhase::Removed,
        ] {
            let mut message = capnp::message::Builder::new_default();
            let mut receipt = message.init_root::<wire::handoff_receipt::Builder<'_>>();
            expected.encode(receipt.reborrow().init_request()).unwrap();
            receipt.set_import_id("import:fixture");
            receipt.set_phase(phase);
            let published = matches!(
                phase,
                wire::HandoffPhase::Published | wire::HandoffPhase::Removed
            );
            if published {
                receipt
                    .reborrow()
                    .init_tail_generation_id()
                    .set_value("file-generation:fixture");
            }
            let certified =
                HandoffReceipt::certify(receipt.reborrow_as_reader(), &expected).unwrap();
            assert!(certified.artifact_id.is_none());
            assert_eq!(
                certified.tail_generation_id.as_deref(),
                published.then_some("file-generation:fixture")
            );
            receipt
                .reborrow()
                .init_tail_generation_id()
                .set_value("file-generation:another");
            assert!(HandoffReceipt::certify(receipt.reborrow_as_reader(), &expected).is_err());
            receipt.reborrow().init_tail_generation_id().set_absent(());
            receipt
                .reborrow()
                .init_artifact_id()
                .set_value("artifact:fixture");
            assert!(HandoffReceipt::certify(receipt.reborrow_as_reader(), &expected).is_err());
            receipt
                .reborrow()
                .init_tail_generation_id()
                .set_value("file-generation:fixture");
            assert!(HandoffReceipt::certify(receipt.reborrow_as_reader(), &expected).is_err());
        }
    }

    #[test]
    fn admission_certificate_refuses_foreign_intents_buffering_and_contradictory_progress() {
        let expected = intent();
        let mut message = capnp::message::Builder::new_default();
        let mut receipt = message.init_root::<wire::handoff_receipt::Builder<'_>>();
        expected.encode(receipt.reborrow().init_request()).unwrap();
        receipt.set_import_id("import:fixture");
        assert_eq!(
            HandoffReceipt::certify(receipt.reborrow_as_reader(), &expected)
                .unwrap()
                .phase,
            wire::HandoffPhase::Reserved
        );
        receipt.set_phase(wire::HandoffPhase::Published);
        assert!(HandoffReceipt::certify(receipt.reborrow_as_reader(), &expected).is_err());
        receipt
            .reborrow()
            .init_artifact_id()
            .set_value("artifact:fixture");
        assert!(HandoffReceipt::certify(receipt.reborrow_as_reader(), &expected).is_ok());
        receipt.set_durability(wire::Durability::Buffered);
        assert!(HandoffReceipt::certify(receipt.reborrow_as_reader(), &expected).is_err());
        receipt.set_durability(wire::Durability::Durable);
        receipt
            .reborrow()
            .get_request()
            .unwrap()
            .set_watch_revision(8);
        assert!(HandoffReceipt::certify(receipt.reborrow_as_reader(), &expected).is_err());
    }
}
