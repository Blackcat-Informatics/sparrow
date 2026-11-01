// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One exact-byte request and response certification path for SDK and agents.

use crate::{Error, sparrow_capnp as wire};

/// Decode one bounded occurrence reference, preserving its union's evidence kind.
/// Store equality remains the caller's store-ownership check.
pub fn decode_evidence(value: wire::evidence::Reader<'_>) -> capnp::Result<(String, String)> {
    let store = value.get_store_id()?;
    let (id, prefix) = match value.which()? {
        wire::evidence::Artifact(id) => (id?, "artifact:"),
        wire::evidence::Record(id) => (id?, "record:"),
    };
    if store.len() > 128
        || id.len() > 128
        || id.len() <= prefix.len()
        || !id.to_str()?.starts_with(prefix)
    {
        return Err(capnp::Error::failed(
            "require a bounded store-scoped reference with the matching evidence kind".into(),
        ));
    }
    Ok((store.to_str()?.to_owned(), id.to_str()?.to_owned()))
}

/// Encode an artifact or record reference; investigation targets remain concrete occurrences.
pub fn encode_evidence(
    mut target: wire::evidence::Builder<'_>,
    store: &str,
    id: &str,
) -> Result<(), Error> {
    target.set_store_id(store);
    if id.starts_with("artifact:") {
        target.set_artifact(id);
    } else if id.starts_with("record:") {
        target.set_record(id);
    } else {
        return Err(Error::InvalidArgument(
            "evidence ID requires artifact: or record: prefix",
        ));
    }
    Ok(())
}

/// Read at most 1 MiB; a file-generation target requires its entire retained cover.
pub async fn read_bytes(
    client: &wire::sparrow::Client,
    store: &str,
    id: &str,
    offset: u64,
    length: u32,
) -> Result<(String, Vec<u8>), Error> {
    if id.len() > 128 || length > 1024 * 1024 || offset.checked_add(u64::from(length)).is_none() {
        return Err(Error::InvalidArgument(
            "byte read requires a bounded identity and range of at most 1 MiB",
        ));
    }
    let mut request = client.bytes_read_request();
    let mut read = request.get().init_request();
    read.set_offset(offset);
    read.set_length(length);
    if id.starts_with("file-generation:") {
        let mut generation = read.init_file_generation();
        generation.set_store_id(store);
        generation.set_schema_version(crate::SCHEMA_VERSION);
        generation.set_id(id);
    } else {
        encode_evidence(read.init_evidence(), store, id)?;
    }
    let response = request.send().promise.await?;
    crate::receipt::certify_result(response.get()?.get_result()?, |value| {
        certify(value, store, length)
    })
}

fn certify(
    value: wire::bytes::Reader<'_>,
    store: &str,
    length: u32,
) -> Result<(String, Vec<u8>), Error> {
    let actual_store = value.get_store_id()?.to_str()?;
    let bytes = value.get_data()?;
    if actual_store != store || bytes.len() != length as usize {
        return Err(Error::Protocol(
            "byte response has the wrong store or exact range length",
        ));
    }
    Ok((actual_store.to_owned(), bytes.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_reference_codec_round_trips_and_refuses_kind_mismatch_and_oversized_ids() {
        for id in ["artifact:example", "record:example"] {
            let mut message = capnp::message::Builder::new_default();
            encode_evidence(
                message.init_root::<wire::evidence::Builder<'_>>(),
                "store:example",
                id,
            )
            .unwrap();
            assert_eq!(
                decode_evidence(
                    message
                        .get_root_as_reader::<wire::evidence::Reader<'_>>()
                        .unwrap()
                )
                .unwrap(),
                ("store:example".into(), id.into())
            );
        }
        let mut message = capnp::message::Builder::new_default();
        let mut reference = message.init_root::<wire::evidence::Builder<'_>>();
        reference.set_store_id("store:example");
        for id in [
            "artifact:wrong",
            "record:",
            &format!("record:{}", "x".repeat(128)),
        ] {
            reference.set_record(id);
            assert!(decode_evidence(reference.reborrow_as_reader()).is_err());
        }
    }

    #[test]
    fn exact_byte_receipts_refuse_foreign_stores_and_short_or_long_ranges() {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut value = message.init_root::<wire::bytes::Builder<'_>>();
            value.set_store_id("store:expected");
            value.set_data(b"exact");
        }
        let value = message
            .get_root_as_reader::<wire::bytes::Reader<'_>>()
            .unwrap();
        assert_eq!(certify(value, "store:expected", 5).unwrap().1, b"exact");
        for length in [0, 4, 6] {
            assert!(matches!(
                certify(value, "store:expected", length),
                Err(Error::Protocol(_))
            ));
        }
        assert!(matches!(
            certify(value, "store:foreign", 5),
            Err(Error::Protocol(_))
        ));
    }
}
