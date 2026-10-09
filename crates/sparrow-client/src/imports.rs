// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{ArtifactReceipt, Error, sparrow_capnp as wire};
use std::{
    fs::File,
    os::unix::fs::{FileExt, MetadataExt},
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
    let mut file = open_completed_file(path)?;
    import_open_file(client, request_id, source, format, &mut file).await
}

/// Open a completed input without following any symlink or waiting on a FIFO.
/// Linux `openat2` is required; unsupported kernels fail rather than weaken resolution.
pub fn open_completed_file(path: &Path) -> Result<File, Error> {
    use rustix::fs::{CWD, Mode, OFlags, ResolveFlags, openat2};
    let fd = openat2(CWD, path, OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(), ResolveFlags::NO_SYMLINKS).map_err(|error| {
        if error == rustix::io::Errno::NOSYS {
            Error::Unsupported("path imports require Linux openat2; use an already opened completed regular-file descriptor on unsupported kernels")
        } else { Error::Io(error.into()) }
    })?;
    let file = File::from(fd);
    if !file.metadata()?.is_file() {
        return Err(Error::InvalidArgument(
            "completed input must be a regular file",
        ));
    }
    Ok(file)
}

/// Upload an already opened completed regular file through the same resumable path.
/// Reads positionally from byte zero, preserving the caller's cursor and path ownership.
pub async fn import_open_file(
    client: &wire::sparrow::Client,
    request_id: &str,
    source: &str,
    format: wire::Format,
    file: &mut File,
) -> Result<ArtifactReceipt, Error> {
    // Discovery refuses an older daemon before allocating private staging.
    let discovery = crate::Discovery::fetch(client).await?;
    upload(client, &discovery, request_id, source, format, file).await
}

pub(crate) async fn upload(
    client: &wire::sparrow::Client,
    discovery: &crate::Discovery,
    request_id: &str,
    source: &str,
    format: wire::Format,
    file: &mut File,
) -> Result<ArtifactReceipt, Error> {
    if request_id.is_empty() || request_id.len() > 128 || source.is_empty() || source.len() > 512 {
        return Err(Error::InvalidArgument(
            "require bounded nonempty request and source labels",
        ));
    }
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(Error::InvalidArgument(
            "ingest requires a regular completed file; live tailing is a separate operation",
        ));
    }
    let identity = CompletedFileIdentity::capture(&metadata);
    let length = metadata.len();
    let mut buffer = vec![0; crate::IMPORT_CHUNK_BYTES];
    let mut digest = purrdf_hash::blake3::Hasher::new();
    let mut remaining = length;
    while remaining != 0 {
        let count = remaining.min(buffer.len() as u64) as usize;
        file.read_exact_at(&mut buffer[..count], length - remaining)?;
        digest.update(&buffer[..count]);
        remaining -= count as u64;
    }
    identity.require(file)?;
    let payload = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(digest.finalize().as_bytes())
    );
    let acceptance = crate::receipt::ExpectedArtifact {
        discovery,
        source,
        format,
        byte_length: length,
        payload_id: payload.clone(),
        durability: wire::Durability::Durable,
        record_count: None,
    };
    let mut request = client.import_begin_request();
    let mut input = request.get().init_request();
    input.set_request_id(request_id);
    input.set_source(source);
    input.set_format(format);
    input.set_byte_length(length);
    input.set_payload_id(&payload);
    let response = request.send().promise.await?;
    let start = match response.get()?.get_result()?.which()? {
        wire::result::Ok(value) => value?,
        wire::result::Error(error) => return Err(Error::domain(error?)),
    };
    let progress = match start.which()? {
        wire::import_start::Accepted(value) => {
            let receipt = acceptance.certify(ArtifactReceipt::decode(value?)?)?;
            identity.require(file)?;
            return Ok(receipt);
        }
        wire::import_start::Staging(value) => value?,
    };
    let progress = crate::ImportProgress::decode(progress)?;
    let store = progress.store_id.clone();
    if store != discovery.store_id
        || progress.byte_length != length
        || progress.payload_id != payload
        || progress.source != source
        || progress.request_id != request_id
        || progress.format != format
    {
        return Err(Error::Protocol(
            "import staging contradicts the discovered store or requested input",
        ));
    }
    let id = progress.id;
    let mut offset = progress.received_bytes;
    while offset < length {
        let count = (length - offset).min(buffer.len() as u64) as usize;
        file.read_exact_at(&mut buffer[..count], offset)?;
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
        let value = crate::ImportProgress::decode(value)?;
        value.certify_identity(&store, &id)?;
        if value.received_bytes != offset {
            return Err(Error::Protocol(
                "daemon returned an unexpected durable append offset",
            ));
        }
    }
    identity.require(file)?;
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
    identity.require(file)?;
    let mut request = client.import_publish_request();
    import_reference(request.get().init_import(), &store, &id);
    let response = request.send().promise.await?;
    match response.get()?.get_result()?.which()? {
        wire::result::Ok(value) => {
            let receipt = acceptance.certify(ArtifactReceipt::decode(value?)?)?;
            identity.require(file)?;
            Ok(receipt)
        }
        wire::result::Error(error) => Err(Error::domain(error?)),
    }
}

fn import_reference(mut out: wire::import_ref::Builder<'_>, store: &str, id: &str) {
    out.set_store_id(store);
    out.set_id(id);
}

/// Opened-file generation and observed mutation clocks used by upload and future claims.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompletedFileIdentity {
    /// Filesystem device identity.
    pub device: u64,
    /// Opened inode identity.
    pub inode: u64,
    /// Exact observed byte length.
    pub length: u64,
    /// Modification clock in Unix seconds.
    pub modified_seconds: i64,
    /// Modification clock fractional nanoseconds.
    pub modified_nanos: i64,
    /// Inode change clock in Unix seconds.
    pub changed_seconds: i64,
    /// Inode change clock fractional nanoseconds.
    pub changed_nanos: i64,
}

impl CompletedFileIdentity {
    /// Capture one metadata observation without reopening any source path.
    pub fn capture(value: &std::fs::Metadata) -> Self {
        Self {
            device: value.dev(),
            inode: value.ino(),
            length: value.len(),
            modified_seconds: value.mtime(),
            modified_nanos: value.mtime_nsec(),
            changed_seconds: value.ctime(),
            changed_nanos: value.ctime_nsec(),
        }
    }
}

impl CompletedFileIdentity {
    /// Refuse mutation or generation changes against this opened-file observation.
    pub fn require(self, file: &File) -> Result<(), Error> {
        if Self::capture(&file.metadata()?) != self {
            return Err(Error::InvalidArgument(
                "completed file changed during import; no acceptance receipt returned; retry the stable request with its original bytes",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn completed_path_open_refuses_symlinks_directories_and_fifos_without_waiting() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("file");
        std::fs::write(&file, b"source").unwrap();
        super::open_completed_file(&file).unwrap();
        let link = directory.path().join("link");
        std::os::unix::fs::symlink(&file, &link).unwrap();
        assert!(super::open_completed_file(&link).is_err());
        let parent_link = directory.path().join("parent-link");
        std::os::unix::fs::symlink(directory.path(), &parent_link).unwrap();
        assert!(super::open_completed_file(&parent_link.join("file")).is_err());
        assert!(matches!(
            super::open_completed_file(directory.path()),
            Err(crate::Error::InvalidArgument(_))
        ));
        let fifo = directory.path().join("fifo");
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            &fifo,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let task =
            std::thread::spawn(move || send.send(super::open_completed_file(&fifo)).unwrap());
        assert!(matches!(
            receive
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("FIFO open must not wait for a writer"),
            Err(crate::Error::InvalidArgument(_))
        ));
        task.join().unwrap();
    }

    #[test]
    fn completed_file_guard_rejects_same_length_rewrites() {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"original").unwrap();
        let identity = super::CompletedFileIdentity::capture(&file.metadata().unwrap());
        identity.require(&file).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(b"replaced").unwrap();
        assert!(identity.require(&file).is_err());
    }
}
