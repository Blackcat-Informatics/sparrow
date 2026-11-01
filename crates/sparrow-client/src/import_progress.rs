// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Certified durable upload checkpoints, distinct from evidence receipts.

use crate::{Discovery, Error, sparrow_capnp as wire};

/// A committed upload checkpoint. Reading it neither publishes nor renews it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportProgress {
    /// Persistent store identity.
    pub store_id: String,
    /// Opaque staging identity returned by importBegin.
    pub id: String,
    /// Original immutable retry identity.
    pub request_id: String,
    /// Original logical source.
    pub source: String,
    /// Original interpretation hint.
    pub format: wire::Format,
    /// Declared complete byte length.
    pub byte_length: u64,
    /// Declared complete BLAKE3 content identity.
    pub payload_id: String,
    /// Durably committed contiguous upload prefix, not accepted evidence.
    pub received_bytes: u64,
    /// Original absolute staging deadline in Unix milliseconds.
    pub expires_at: u64,
    /// Complete original bytes have passed native digest verification.
    pub sealed: bool,
    /// Native record layout has completed.
    pub layout_complete: bool,
    /// Committed record layout count.
    pub layout_records: u64,
    /// Original published artifact reference, absent before acceptance.
    pub published_artifact: Option<String>,
}

impl ImportProgress {
    pub(crate) fn decode(value: wire::import_progress::Reader<'_>) -> Result<Self, Error> {
        let progress = Self {
            store_id: value.get_store_id()?.to_str()?.into(),
            id: value.get_id()?.to_str()?.into(),
            request_id: value.get_request_id()?.to_str()?.into(),
            source: value.get_source()?.to_str()?.into(),
            format: value.get_format()?,
            byte_length: value.get_byte_length(),
            payload_id: value.get_payload_id()?.to_str()?.into(),
            received_bytes: value.get_received_bytes(),
            expires_at: value.get_expires_at()?.get_unix_millis(),
            sealed: value.get_sealed(),
            layout_complete: value.get_layout_complete(),
            layout_records: value.get_layout_records(),
            published_artifact: crate::receipt::optional_identity(value.get_published_artifact()?)?,
        };
        if progress.received_bytes > progress.byte_length
            || (progress.received_bytes != progress.byte_length
                && !progress
                    .received_bytes
                    .is_multiple_of(crate::IMPORT_CHUNK_BYTES as u64))
            || (progress.sealed && progress.received_bytes != progress.byte_length)
            || (progress.layout_complete && !progress.sealed)
            || (progress.published_artifact.is_some() && !progress.layout_complete)
        {
            return Err(Error::Protocol("inconsistent durable import checkpoint"));
        }
        Ok(progress)
    }

    pub(crate) fn certify_identity(&self, store: &str, id: &str) -> Result<(), Error> {
        if self.store_id != store || self.id != id {
            return Err(Error::Protocol(
                "import checkpoint changed store or import identity",
            ));
        }
        Ok(())
    }
}

/// Inspect committed progress without appending, sealing, publishing or deleting.
pub async fn get_import_progress(
    client: &wire::sparrow::Client,
    store_id: &str,
    import_id: &str,
) -> Result<ImportProgress, Error> {
    if store_id.is_empty()
        || store_id.len() > 128
        || !import_id.starts_with("import:")
        || import_id.len() > 128
    {
        return Err(Error::InvalidArgument(
            "require a bounded store ID and opaque import ID",
        ));
    }
    Discovery::fetch(client).await?;
    let mut call = client.import_get_request();
    let mut query = call.get().init_import();
    query.set_store_id(store_id);
    query.set_id(import_id);
    let response = call.send().promise.await?;
    crate::receipt::certify_result(response.get()?.get_result()?, |value| {
        let progress = ImportProgress::decode(value)?;
        progress.certify_identity(store_id, import_id)?;
        Ok(progress)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_certificate_refuses_inconsistent_progress_and_identity() {
        for (received, sealed, layout, published, valid) in [
            (0, false, false, false, true),
            (65536, false, false, false, true),
            (65537, true, true, true, true),
            (1, false, false, false, false),
            (65538, false, false, false, false),
            (0, true, false, false, false),
            (65537, false, true, false, false),
            (65537, true, false, true, false),
        ] {
            let mut message = capnp::message::Builder::new_default();
            let mut out = message.init_root::<wire::import_progress::Builder<'_>>();
            out.set_store_id("store:expected");
            out.set_id("import:expected");
            out.set_byte_length(65537);
            out.set_received_bytes(received);
            out.set_sealed(sealed);
            out.set_layout_complete(layout);
            if published {
                out.init_published_artifact().set_value("artifact:expected");
            }
            let value = message
                .get_root_as_reader::<wire::import_progress::Reader<'_>>()
                .unwrap();
            let progress = ImportProgress::decode(value);
            assert_eq!(progress.is_ok(), valid);
            if let Ok(progress) = progress {
                progress
                    .certify_identity("store:expected", "import:expected")
                    .unwrap();
                assert!(
                    progress
                        .certify_identity("store:other", "import:expected")
                        .is_err()
                );
                assert!(
                    progress
                        .certify_identity("store:expected", "import:other")
                        .is_err()
                );
            }
        }
    }
}
