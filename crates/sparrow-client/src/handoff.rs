// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Immutable typed completed-file handoff intents and certified admission progress.

use crate::{CompletedFileIdentity, Error, sparrow_capnp as wire};
use std::io::Read;

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
        {
            return Err(Error::InvalidArgument(
                "handoff requires bounded nonempty store/request/watch/path and a nonzero revision",
            ));
        }
        Ok(())
    }

    /// Save an immutable Cap'n Proto intent, not a new filesystem observation.
    pub fn write(&self, output: impl std::io::Write) -> Result<(), Error> {
        let mut message = capnp::message::Builder::new_default();
        self.encode(message.init_root())?;
        capnp::serialize::write_message(output, &message)?;
        Ok(())
    }

    /// Read one bounded intent; trailing bytes or a second message are refused.
    pub fn read(mut input: impl std::io::Read) -> Result<Self, Error> {
        let mut bytes = Vec::new();
        std::io::Read::by_ref(&mut input)
            .take(16_385)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 16_384 {
            return Err(Error::InvalidArgument("handoff intent exceeds 16 KiB"));
        }
        let mut slice = bytes.as_slice();
        let message = capnp::serialize::read_message_from_flat_slice(
            &mut slice,
            capnp::message::ReaderOptions {
                traversal_limit_in_words: Some(2048),
                nesting_limit: 16,
            },
        )?;
        if !slice.is_empty() {
            return Err(Error::InvalidArgument("handoff intent has trailing bytes"));
        }
        Self::decode(message.get_root()?)
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
}

impl HandoffReceipt {
    fn result(
        value: wire::result::Reader<'_, wire::handoff_receipt::Owned>,
        expected: &CompletedHandoff,
    ) -> Result<Self, Error> {
        match value.which()? {
            wire::result::Ok(value) => Self::certify(value?, expected),
            wire::result::Error(error) => Err(Error::domain(error?)),
        }
    }
    fn certify(
        value: wire::handoff_receipt::Reader<'_>,
        expected: &CompletedHandoff,
    ) -> Result<Self, Error> {
        let request = CompletedHandoff::decode(value.get_request()?)?;
        let phase = value.get_phase()?;
        let artifact_id = match value.get_artifact_id()?.which()? {
            wire::optional_text::Absent(()) => None,
            wire::optional_text::Value(value) => {
                let value = value?.to_str()?;
                if value.len() > 128 {
                    return Err(Error::Protocol(
                        "handoff artifact identity exceeds 128 bytes",
                    ));
                }
                Some(value.to_owned())
            }
        };
        let import_id = value.get_import_id()?.to_str()?;
        if request != *expected
            || value.get_durability()? != wire::Durability::Durable
            || import_id.len() > 128
            || !import_id.starts_with("import:")
            || matches!(
                phase,
                wire::HandoffPhase::Published | wire::HandoffPhase::Removed
            ) != artifact_id.is_some()
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
        HandoffReceipt::result(call.send().promise.await?.get()?.get_result()?, intent)
    } else {
        let mut call = client.watch_handoff_request();
        intent.encode(call.get().init_request())?;
        HandoffReceipt::result(call.send().promise.await?.get()?.get_result()?, intent)
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
