// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{ArtifactReceipt, Error, sparrow_capnp as wire};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    os::unix::fs::MetadataExt,
    path::Path,
};

/// Stream a completed regular file through durable resumable checkpoints.
/// The source file is never renamed or deleted. Retries preserve request identity.
pub async fn import_file(
    client: &wire::sparrow::Client,
    request_id: &str,
    source: &str,
    format: wire::Format,
    path: &Path,
) -> Result<ArtifactReceipt, Error> {
    // Discovery refuses an older daemon before allocating private staging.
    crate::Discovery::fetch(client).await?;
    let mut file = File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(Error::InvalidArgument(
            "ingest requires a regular completed file; live tailing is a separate operation",
        ));
    }
    let identity = file_identity(&metadata);
    let length = metadata.len();
    let mut buffer = vec![0; crate::IMPORT_CHUNK_BYTES];
    let mut digest = purrdf_hash::blake3::Hasher::new();
    let mut remaining = length;
    while remaining != 0 {
        let count = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..count])?;
        digest.update(&buffer[..count]);
        remaining -= count as u64;
    }
    require_file_identity(&file, identity)?;
    let payload = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(digest.finalize().as_bytes())
    );
    let mut request = client.import_begin_request();
    let mut input = request.get().init_request();
    input.set_request_id(request_id);
    input.set_source(source);
    input.set_format(format);
    input.set_byte_length(length);
    input.set_payload_id(payload);
    let response = request.send().promise.await?;
    let start = match response.get()?.get_result()?.which()? {
        wire::result::Ok(value) => value?,
        wire::result::Error(error) => return Err(Error::domain(error?)),
    };
    let progress = match start.which()? {
        wire::import_start::Accepted(value) => return ArtifactReceipt::decode(value?),
        wire::import_start::Staging(value) => value?,
    };
    let store = progress.get_store_id()?.to_str()?.to_owned();
    let id = progress.get_id()?.to_str()?.to_owned();
    let mut offset = progress.get_received_bytes();
    if offset > length || (offset != length && !offset.is_multiple_of(buffer.len() as u64)) {
        return Err(Error::Protocol(
            "daemon returned an invalid durable import offset",
        ));
    }
    file.seek(SeekFrom::Start(offset))?;
    while offset < length {
        let count = (length - offset).min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..count])?;
        let mut request = client.import_append_request();
        let mut input = request.get().init_request();
        import_reference(input.reborrow().init_import(), &store, &id);
        input.set_offset(offset);
        input.set_bytes(&buffer[..count]);
        let response = request.send().promise.await?;
        let value = match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => value?,
            wire::result::Error(error) => return Err(Error::domain(error?)),
        };
        offset += count as u64;
        if value.get_received_bytes() != offset {
            return Err(Error::Protocol(
                "daemon returned an unexpected durable append offset",
            ));
        }
    }
    require_file_identity(&file, identity)?;
    let mut request = client.import_seal_request();
    import_reference(request.get().init_import(), &store, &id);
    let response = request.send().promise.await?;
    match response.get()?.get_result()?.which()? {
        wire::result::Ok(value) => {
            value?;
        }
        wire::result::Error(error) => return Err(Error::domain(error?)),
    }
    let mut request = client.import_layout_request();
    import_reference(request.get().init_import(), &store, &id);
    let response = request.send().promise.await?;
    match response.get()?.get_result()?.which()? {
        wire::result::Ok(value) => {
            value?;
        }
        wire::result::Error(error) => return Err(Error::domain(error?)),
    }
    // Ordinal zero also recovers already committed preparation progress.
    let mut expected = 0;
    let mut previous = None;
    loop {
        let mut request = client.import_prepare_request();
        let mut input = request.get().init_request();
        import_reference(input.reborrow().init_import(), &store, &id);
        input.set_expected_record(expected);
        let response = request.send().promise.await?;
        let value = match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => value?,
            wire::result::Error(error) => return Err(Error::domain(error?)),
        };
        expected = value.get_prepared_records();
        let raw_chunks = match value.get_raw()?.which()? {
            wire::optional_raw_preparation::Absent(()) => None,
            wire::optional_raw_preparation::Value(raw) => {
                let raw = raw?;
                if raw.get_record_index() != expected
                    || raw.get_prepared_chunks() == 0
                    || raw.get_prepared_chunks() > raw.get_total_chunks()
                {
                    return Err(Error::Protocol(
                        "daemon returned inconsistent raw preparation progress",
                    ));
                }
                Some(raw.get_prepared_chunks())
            }
        };
        let progress = (expected, raw_chunks);
        if previous.is_some_and(|old| progress <= old) || expected > value.get_total_records() {
            return Err(Error::Protocol(
                "daemon preparation did not advance its durable checkpoint",
            ));
        }
        previous = Some(progress);
        if expected == value.get_total_records() {
            break;
        }
    }
    require_file_identity(&file, identity)?;
    let mut request = client.import_publish_request();
    import_reference(request.get().init_import(), &store, &id);
    let response = request.send().promise.await?;
    match response.get()?.get_result()?.which()? {
        wire::result::Ok(value) => ArtifactReceipt::decode(value?),
        wire::result::Error(error) => Err(Error::domain(error?)),
    }
}

fn import_reference(mut out: wire::import_ref::Builder<'_>, store: &str, id: &str) {
    out.set_store_id(store);
    out.set_id(id);
}

fn file_identity(value: &std::fs::Metadata) -> (u64, u64, u64, i64, i64, i64, i64) {
    (
        value.dev(),
        value.ino(),
        value.len(),
        value.mtime(),
        value.mtime_nsec(),
        value.ctime(),
        value.ctime_nsec(),
    )
}

fn require_file_identity(
    file: &File,
    expected: (u64, u64, u64, i64, i64, i64, i64),
) -> Result<(), Error> {
    if file_identity(&file.metadata()?) != expected {
        return Err(Error::InvalidArgument(
            "completed file changed during import; no acceptance receipt returned; retry the stable request with its original bytes",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn completed_file_guard_rejects_same_length_rewrites() {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"original").unwrap();
        let identity = super::file_identity(&file.metadata().unwrap());
        super::require_file_identity(&file, identity).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(b"replaced").unwrap();
        assert!(super::require_file_identity(&file, identity).is_err());
    }
}
