// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Explicit retained-generation completion; a receipt grants no source deletion.

use crate::{CompletedFileIdentity, Error, sparrow_capnp as wire};

/// Immutable producer assertion that this opened generation will receive no more writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TailFinishIntent {
    /// Original persistent store identity.
    pub store_id: String,
    /// Stable caller-owned completion retry identity.
    pub request_id: String,
    /// Original opaque tracked file-generation identity.
    pub generation_id: String,
    /// Original watch revision authorizing the generation.
    pub watch_revision: u64,
    /// Final descriptor identity, length and nanosecond clocks.
    pub observed: CompletedFileIdentity,
}

impl TailFinishIntent {
    fn validate(&self) -> Result<(), Error> {
        if [&self.store_id, &self.request_id, &self.generation_id]
            .iter()
            .any(|value| value.is_empty() || value.len() > 128)
            || !self.generation_id.starts_with("file-generation:")
            || self.watch_revision == 0
            || self.observed.inode == 0
        {
            return Err(Error::InvalidArgument(
                "tail finish requires bounded store/request/generation identities and a nonzero watch revision/inode",
            ));
        }
        Ok(())
    }

    /// Encode the same immutable authority for RPC and durable retry files.
    pub fn encode(&self, mut out: wire::tail_finish_intent::Builder<'_>) -> Result<(), Error> {
        self.validate()?;
        out.set_store_id(&self.store_id);
        out.set_request_id(&self.request_id);
        out.set_generation_id(&self.generation_id);
        out.set_watch_revision(self.watch_revision);
        self.observed.encode(out.init_observed())
    }

    /// Decode bounded labels and the shared native descriptor observation.
    pub fn decode(value: wire::tail_finish_intent::Reader<'_>) -> Result<Self, Error> {
        let store = value.get_store_id()?.to_str()?;
        let request = value.get_request_id()?.to_str()?;
        let generation = value.get_generation_id()?.to_str()?;
        if [store, request, generation]
            .iter()
            .any(|value| value.len() > 128)
        {
            return Err(Error::InvalidArgument(
                "tail finish labels exceed 128 bytes",
            ));
        }
        let intent = Self {
            store_id: store.into(),
            request_id: request.into(),
            generation_id: generation.into(),
            watch_revision: value.get_watch_revision(),
            observed: CompletedFileIdentity::decode(value.get_observed()?)?,
        };
        intent.validate()?;
        Ok(intent)
    }

    /// Save the original observation once; retries must not recapture a changed file.
    pub fn write(&self, output: impl std::io::Write) -> Result<(), Error> {
        crate::intent_file::write(output, self)
    }

    /// Refuse oversized, malformed or trailing messages through the shared intent container.
    pub fn read(input: impl std::io::Read) -> Result<Self, Error> {
        crate::intent_file::read::<wire::tail_finish_intent::Owned, _>(input, Self::decode)
    }
}

impl crate::intent_file::EncodeIntent for TailFinishIntent {
    type Wire = wire::tail_finish_intent::Owned;
    fn encode_intent(&self, out: wire::tail_finish_intent::Builder<'_>) -> Result<(), Error> {
        self.encode(out)
    }
}

/// Certified durable completion progress; this is not a whole-file artifact receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TailFinishReceipt {
    /// Exact original immutable producer assertion.
    pub request: TailFinishIntent,
    /// Original durable admission instant in UTC Unix milliseconds.
    pub admitted_at: u64,
    /// Original terminal publication instant, absent while pending or discontinued.
    pub finished_at: Option<u64>,
    /// Accepted byte checkpoint in the original generation.
    pub offset: u64,
    /// Original number of accepted occurrences, independent of current retention.
    pub records: u64,
    /// Terminal discontinuity reason, if draining cannot complete.
    pub gap: Option<String>,
}

impl TailFinishReceipt {
    /// Certify both immutable authority and the terminal boundary before exposing progress.
    pub fn certify(
        value: wire::tail_generation::Reader<'_>,
        expected: &TailFinishIntent,
    ) -> Result<Self, Error> {
        let finish = match value.get_finish()?.which()? {
            wire::optional_tail_finish::Value(finish) => finish?,
            wire::optional_tail_finish::Absent(()) => {
                return Err(Error::Protocol(
                    "tail finish receipt omitted its admitted intent",
                ));
            }
        };
        let request = TailFinishIntent::decode(finish.get_request()?)?;
        let finished_at = match finish.get_finished_at()?.which()? {
            wire::optional_instant::Absent(()) => None,
            wire::optional_instant::Value(at) => Some(at?.get_unix_millis()),
        };
        let gap = match value.get_gap()?.which()? {
            wire::optional_text::Absent(()) => None,
            wire::optional_text::Value(reason) => {
                let reason = reason?.to_str()?;
                if !["truncated", "identity_changed", "read_gap"].contains(&reason) {
                    return Err(Error::Protocol("tail finish receipt has an unknown gap"));
                }
                Some(reason.into())
            }
        };
        let declared = value.get_declaration()?;
        if request != *expected
            || value.get_store_id()?.to_str()? != expected.store_id
            || value.get_id()?.to_str()? != expected.generation_id
            || declared.get_watch_revision() != expected.watch_revision
            || declared.get_inode() != expected.observed.inode
            || value.get_offset() > expected.observed.length
            || value.get_records() > value.get_offset()
            || finished_at.is_some() != value.get_complete()
            || (value.get_complete()
                && (gap.is_some() || value.get_offset() != expected.observed.length))
        {
            return Err(Error::Protocol(
                "tail finish receipt changed its original authority or boundary",
            ));
        }
        Ok(Self {
            request,
            admitted_at: finish.get_admitted_at()?.get_unix_millis(),
            finished_at,
            offset: value.get_offset(),
            records: value.get_records(),
            gap,
        })
    }
}

impl crate::Client {
    /// Admit explicit completion of a held generation; source files remain retained.
    pub async fn tail_finish(&self, intent: &TailFinishIntent) -> Result<TailFinishReceipt, Error> {
        tail_finish(self.capability(), intent, false).await
    }

    /// Read original completion admission before inspecting a present-day file path.
    pub async fn tail_finish_get(
        &self,
        intent: &TailFinishIntent,
    ) -> Result<TailFinishReceipt, Error> {
        tail_finish(self.capability(), intent, true).await
    }
}

/// One discovery-bound generated client for completion admission and progress reads.
pub async fn tail_finish(
    client: &wire::sparrow::Client,
    intent: &TailFinishIntent,
    read_only: bool,
) -> Result<TailFinishReceipt, Error> {
    let discovery = crate::Discovery::fetch(client).await?;
    if intent.store_id != discovery.store_id {
        return Err(Error::InvalidArgument(
            "tail finish belongs to another persistent store",
        ));
    }
    if read_only {
        let mut call = client.tail_finish_get_request();
        intent.encode(call.get().init_request())?;
        crate::receipt::certify_result(call.send().promise.await?.get()?.get_result()?, |value| {
            TailFinishReceipt::certify(value, intent)
        })
    } else {
        let mut call = client.tail_finish_request();
        intent.encode(call.get().init_request())?;
        crate::receipt::certify_result(call.send().promise.await?.get()?.get_result()?, |value| {
            TailFinishReceipt::certify(value, intent)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent() -> TailFinishIntent {
        TailFinishIntent {
            store_id: "store:finish-test".into(),
            request_id: "finish-1".into(),
            generation_id: "file-generation:finish-test".into(),
            watch_revision: 7,
            observed: CompletedFileIdentity {
                device: 3,
                inode: 4,
                length: 8,
                modified_seconds: -2,
                modified_nanos: 99,
                changed_seconds: 3,
                changed_nanos: 7,
            },
        }
    }

    #[test]
    fn immutable_finish_files_share_strict_single_message_and_clock_admission() {
        let original = intent();
        let mut bytes = Vec::new();
        original.write(&mut bytes).unwrap();
        assert_eq!(TailFinishIntent::read(bytes.as_slice()).unwrap(), original);
        let mut trailing = bytes.clone();
        trailing.extend_from_slice(&bytes);
        assert!(TailFinishIntent::read(trailing.as_slice()).is_err());
        bytes.push(0);
        assert!(TailFinishIntent::read(bytes.as_slice()).is_err());
        assert!(TailFinishIntent::read(vec![0; 16_385].as_slice()).is_err());
        let mut invalid = original;
        invalid.observed.modified_nanos = 1_000_000_000;
        assert!(invalid.write(Vec::new()).is_err());
    }

    #[test]
    fn finish_certificate_refuses_changed_authority_and_premature_terminal_checkpoints() {
        let expected = intent();
        let mut message = capnp::message::Builder::new_default();
        let mut row = message.init_root::<wire::tail_generation::Builder<'_>>();
        row.set_store_id(&expected.store_id);
        row.set_id(&expected.generation_id);
        let mut declaration = row.reborrow().init_declaration();
        declaration.set_watch_revision(expected.watch_revision);
        declaration.set_device(expected.observed.device);
        declaration.set_inode(expected.observed.inode);
        let mut finish = row.reborrow().init_finish().init_value();
        expected.encode(finish.reborrow().init_request()).unwrap();
        finish.reborrow().init_admitted_at().set_unix_millis(10);
        let admitted = TailFinishReceipt::certify(row.reborrow_as_reader(), &expected).unwrap();
        assert_eq!(admitted.admitted_at, 10);
        assert!(admitted.finished_at.is_none());
        row.set_complete(true);
        row.reborrow()
            .get_finish()
            .unwrap()
            .init_value()
            .init_finished_at()
            .init_value()
            .set_unix_millis(20);
        // Reinitializing a nested intent is never a valid completion certificate.
        assert!(TailFinishReceipt::certify(row.reborrow_as_reader(), &expected).is_err());
        let mut finish = row.reborrow().get_finish().unwrap().init_value();
        expected.encode(finish.reborrow().init_request()).unwrap();
        finish.reborrow().init_admitted_at().set_unix_millis(10);
        finish.init_finished_at().init_value().set_unix_millis(20);
        assert!(TailFinishReceipt::certify(row.reborrow_as_reader(), &expected).is_err());
        row.set_offset(8);
        let complete = TailFinishReceipt::certify(row.reborrow_as_reader(), &expected).unwrap();
        assert_eq!(complete.finished_at, Some(20));
        let mut changed = expected.clone();
        changed.observed.length = 9;
        assert!(TailFinishReceipt::certify(row.reborrow_as_reader(), &changed).is_err());
        row.reborrow().init_gap().set_value("identity_changed");
        assert!(TailFinishReceipt::certify(row.reborrow_as_reader(), &expected).is_err());
    }
}
