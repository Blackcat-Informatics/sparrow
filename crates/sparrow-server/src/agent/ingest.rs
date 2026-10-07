// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use super::Result;
use super::{failure, instant, invalid, wire};
use purrdf_lex::json::{Object, Value};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    os::unix::fs::MetadataExt,
    path::PathBuf,
};

pub(crate) struct Input {
    request: String,
    source: String,
    format: wire::Format,
    path: PathBuf,
}

impl Input {
    pub(super) fn parse(args: &[String]) -> Result<Self> {
        if args.len() != 4 {
            return Err(invalid("usage: ingest REQUEST_ID SOURCE FORMAT FILE"));
        }
        let format = crate::rpc::stored_format(&args[2]).map_err(|e| invalid(e.to_string()))?;

        Ok(Self {
            request: args[0].clone(),
            source: args[1].clone(),
            format,
            path: args[3].clone().into(),
        })
    }

    pub(super) async fn execute(&self, client: &wire::sparrow::Client) -> Result<Value> {
        // Discovery refuses an older daemon before allocating private staging.
        super::status::execute(client).await?;
        let mut file = File::open(&self.path)
            .map_err(|e| invalid(format!("read {}: {e}", self.path.display())))?;
        let metadata = file.metadata().map_err(|e| invalid(e.to_string()))?;
        if !metadata.is_file() {
            return Err(invalid(
                "ingest requires a regular completed file; live tailing is a separate operation",
            ));
        }
        let identity = file_identity(&metadata);
        let length = metadata.len();
        let mut buffer = vec![0; sparrow_core::IMPORT_CHUNK_BYTES];
        let mut digest = purrdf_hash::blake3::Hasher::new();
        let mut remaining = length;
        while remaining != 0 {
            let count = remaining.min(buffer.len() as u64) as usize;
            file.read_exact(&mut buffer[..count])
                .map_err(|e| invalid(format!("read completed file: {e}")))?;
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
        input.set_request_id(&self.request);
        input.set_source(&self.source);
        input.set_format(self.format);
        input.set_byte_length(length);
        input.set_payload_id(payload);
        let response = request.send().promise.await?;
        let start = match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => value?,
            wire::result::Error(error) => return Err(failure(error?)),
        };
        let progress = match start.which()? {
            wire::import_start::Accepted(value) => return render(value?),
            wire::import_start::Staging(value) => value?,
        };
        let store = progress.get_store_id()?.to_str()?.to_owned();
        let id = progress.get_id()?.to_str()?.to_owned();
        let mut offset = progress.get_received_bytes();
        if offset > length || (offset != length && !offset.is_multiple_of(buffer.len() as u64)) {
            return Err(invalid("daemon returned an invalid durable import offset"));
        }
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| invalid(e.to_string()))?;
        while offset < length {
            let count = (length - offset).min(buffer.len() as u64) as usize;
            file.read_exact(&mut buffer[..count])
                .map_err(|e| invalid(format!("read completed file: {e}")))?;
            let mut request = client.import_append_request();
            let mut input = request.get().init_request();
            import_reference(input.reborrow().init_import(), &store, &id);
            input.set_offset(offset);
            input.set_bytes(&buffer[..count]);
            let response = request.send().promise.await?;
            let value = match response.get()?.get_result()?.which()? {
                wire::result::Ok(value) => value?,
                wire::result::Error(error) => return Err(failure(error?)),
            };
            offset += count as u64;
            if value.get_received_bytes() != offset {
                return Err(invalid(
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
            wire::result::Error(error) => return Err(failure(error?)),
        }
        let mut request = client.import_layout_request();
        import_reference(request.get().init_import(), &store, &id);
        let response = request.send().promise.await?;
        match response.get()?.get_result()?.which()? {
            wire::result::Ok(value) => {
                value?;
            }
            wire::result::Error(error) => return Err(failure(error?)),
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
                wire::result::Error(error) => return Err(failure(error?)),
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
                        return Err(invalid(
                            "daemon returned inconsistent raw preparation progress",
                        ));
                    }
                    Some(raw.get_prepared_chunks())
                }
            };
            let progress = (expected, raw_chunks);
            if previous.is_some_and(|old| progress <= old) || expected > value.get_total_records() {
                return Err(invalid(
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
            wire::result::Ok(value) => render(value?),
            wire::result::Error(error) => Err(failure(error?)),
        }
    }
}

pub(super) fn import_reference(mut out: wire::import_ref::Builder<'_>, store: &str, id: &str) {
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

fn require_file_identity(file: &File, expected: (u64, u64, u64, i64, i64, i64, i64)) -> Result<()> {
    if file_identity(&file.metadata().map_err(|e| invalid(e.to_string()))?) != expected {
        return Err(invalid(
            "completed file changed during import; no acceptance receipt returned; retry the stable request with its original bytes",
        ));
    }
    Ok(())
}

pub(super) fn render(artifact: wire::artifact::Reader<'_>) -> Result<Value> {
    let records = artifact
        .get_record_ids()?
        .iter()
        .map(|id| Ok(Value::from(id?.to_str()?)))
        .collect::<Result<Vec<_>>>()?;
    Ok(Object::new()
        .with("store_id", artifact.get_store_id()?.to_str()?)
        .with("artifact_id", artifact.get_id()?.to_str()?)
        .with("source", artifact.get_source()?.to_str()?)
        .with("format", crate::rpc::format(artifact.get_format()?))
        .with("received_at", instant(artifact.get_received_at()?)?)
        .with("expires_at", instant(artifact.get_expires_at()?)?)
        .with("byte_length", artifact.get_byte_length().to_string())
        .with("payload_id", artifact.get_payload_id()?.to_str()?)
        .with("record_ids", Value::array(records))
        .with(
            "durability",
            match artifact.get_durability()? {
                wire::Durability::Buffered => "buffered",
                wire::Durability::Durable => "durable",
            },
        )
        .into())
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
