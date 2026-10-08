// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! One exact-byte request and response certification path for SDK and agents.

use crate::{Error, sparrow_capnp as wire};

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
