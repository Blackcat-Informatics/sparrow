// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use crate::{ArtifactReceipt, Client, Endpoint, Error, sparrow_capnp as wire};
use std::{
    fs::File,
    os::unix::fs::{FileExt, MetadataExt},
    path::Path,
};

/// Stream a completed regular file through durable resumable checkpoints.
/// The source file is never renamed or deleted. Retries preserve request identity.
///
/// A connection lost to the daemon's connection lease or a restart is replaced,
/// waiting up to 30 seconds for the daemon to accept connections again, and the
/// same request resumes from its durable checkpoint while each attempt advances it.
/// A terminal failure cancels the staging journal, so an abandoned import never
/// holds one of the daemon's import slots for the retry window.
pub async fn import_file(
    endpoint: &Endpoint,
    request_id: &str,
    source: &str,
    format: wire::Format,
    path: &Path,
) -> Result<ArtifactReceipt, Error> {
    let mut file = open_completed_file(path)?;
    let mut checkpoint = Checkpoint::default();
    let attempt = async |file: &mut File, checkpoint: &mut Checkpoint| {
        let client = reconnect(endpoint).await?;
        upload(
            client.capability(),
            client.discovery(),
            request_id,
            source,
            format,
            &CompletedInput::capture(file)?,
            checkpoint,
        )
        .await
    };
    let error = loop {
        let before = checkpoint.progress;
        match attempt(&mut file, &mut checkpoint).await {
            Ok(receipt) => return Ok(receipt),
            Err(error) if error.retryable() && checkpoint.progress > before => {}
            Err(error) => break error,
        }
    };
    settle(
        error,
        checkpoint.import.clone(),
        Session::Replaceable,
        async |store: &str, id: &str| cancel(endpoint, store, id).await,
        async || attempt(&mut file, &mut checkpoint).await,
    )
    .await
}

/// Whether a failed cancel can be retried on a new connection.
#[derive(Clone, Copy)]
enum Session {
    /// The caller holds one connection; it cannot be replaced.
    Held,
    /// The cancel reconnects, so its failure is not a lost connection.
    Replaceable,
}

/// Settle an upload that ended in `error`: cancel the staging journal `import` names, so an
/// abandoned import never holds an import slot for the retry window.
///
/// A cancel answered `CONFLICT` retries the upload through `retry`. The daemon answers it when
/// the publication committed although its reply was lost, so the retry fetches the receipt.
/// It also answers it for staging the daemon's own file watcher owns; there the journal stays
/// intact and the retry resumes it, returning that attempt's result. A cancel that cannot
/// reach the daemon on a [`Session::Held`] connection, or that the daemon refuses with a
/// retryable hold, leaves the journal and checkpoint intact, so the result is the retryable
/// `IMPORT_RESUMABLE` keeping the refusal's class: a retry under the same request ID resumes.
/// Any other failed cancel returns `IMPORT_ABANDONED` naming the journal. An
/// error before any journal existed is returned unchanged. This is the one implementation
/// behind the free `import_file` and every `Client` upload path.
async fn settle(
    error: Error,
    import: Option<(String, String)>,
    session: Session,
    cancel: impl AsyncFnOnce(&str, &str) -> Result<(), Error>,
    retry: impl AsyncFnOnce() -> Result<ArtifactReceipt, Error>,
) -> Result<ArtifactReceipt, Error> {
    let Some((store, id)) = import else {
        return Err(error);
    };
    match cancel(&store, &id).await {
        Ok(()) => Err(error),
        Err(Error::Domain { code, .. }) if code == "CONFLICT" => retry().await,
        // On a held connection the cancel never reached the daemon, or the daemon refused it
        // with a hold (free-space floor, restart, busy admission): the journal is intact
        // unless a committed cancel's reply was lost, so a retry resumes it, or finds it
        // canceled and starts a fresh one under the same ID. The refusal's class is kept.
        Err(cancel) if matches!(session, Session::Held) && resumable(&cancel) => {
            let class = match &cancel {
                Error::Domain { class, .. } => *class,
                _ => crate::FailureClass::Unclassified,
            };
            Err(Error::Domain {
                code: "IMPORT_RESUMABLE".into(),
                message: format!(
                    "{error}; canceling import {id} did not complete ({cancel}); retrying under the same request ID resumes its journal if it is still staged, or starts a fresh import if the cancel committed"
                ),
                retryable: true,
                class,
            })
        }
        Err(cancel) => Err(Error::Domain {
            code: "IMPORT_ABANDONED".into(),
            message: format!(
                "{error}; canceling import {id} also failed ({cancel}); its import slot may stay occupied until the retry window ends"
            ),
            retryable: false,
            class: crate::FailureClass::Unclassified,
        }),
    }
}

/// Whether a failed cancel leaves the journal for a retry: the connection was lost, or the
/// daemon refused the cancel with a retryable hold.
fn resumable(cancel: &Error) -> bool {
    match cancel {
        Error::Transport(lost) => lost.kind == capnp::ErrorKind::Disconnected,
        Error::Domain { retryable, .. } => *retryable,
        _ => false,
    }
}

/// Whether an upload error asks the caller to hold and retry: the free-space floor or a
/// restart refusal, or a lost connection. `RESOURCE_LIMIT` is no hold: it is also how the
/// daemon refuses an input bound.
fn is_hold(error: &Error) -> bool {
    match error {
        Error::Domain { code, .. } => matches!(code.as_str(), "STORAGE_PRESSURE" | "SHUTTING_DOWN"),
        Error::Transport(lost) => lost.kind == capnp::ErrorKind::Disconnected,
        _ => false,
    }
}

/// Upload over one held connection and cancel the staging journal when the attempt fails.
///
/// A held connection cannot be replaced, so any error ends the attempt. A hold
/// (`STORAGE_PRESSURE`, `SHUTTING_DOWN`) or a lost connection is returned unchanged without a
/// cancel: the journal and checkpoint stay intact and a retry under the same request ID resumes.
/// Any other error cancels the journal. The cancel uses the same connection; when that is lost too, the journal stays resumable and the result is the
/// retryable `IMPORT_RESUMABLE`.
pub(crate) async fn upload_or_cancel(
    client: &wire::sparrow::Client,
    discovery: &crate::Discovery,
    request_id: &str,
    source: &str,
    format: wire::Format,
    staged: &(impl StagedInput + ?Sized),
    checkpoint: &mut Checkpoint,
) -> Result<ArtifactReceipt, Error> {
    let error = match upload(
        client, discovery, request_id, source, format, staged, checkpoint,
    )
    .await
    {
        Ok(receipt) => return Ok(receipt),
        Err(error) => error,
    };
    // A hold or a lost connection keeps the journal: the caller retries the same request ID and
    // resumes the staged bytes, where a cancel would discard them.
    if is_hold(&error) {
        return Err(error);
    }
    let import = checkpoint.import.clone();
    settle(
        error,
        import,
        Session::Held,
        async |store: &str, id: &str| cancel_on(client, store, id).await,
        async || {
            upload(
                client, discovery, request_id, source, format, staged, checkpoint,
            )
            .await
        },
    )
    .await
}

/// Highest durable import progress observed across attempts: received bytes,
/// then seal and layout, then prepared records and raw chunks of the next one.
///
/// A sink, when present, receives every raise with the parts completed before this upload,
/// so a caller whose attempt future is dropped at a deadline still sees how far it got (#280).
#[derive(Default)]
pub(crate) struct Checkpoint {
    import: Option<(String, String)>,
    progress: (u64, u8, u64, u64),
    sink: Option<(std::sync::Arc<crate::parts::PartsProgress>, u32)>,
}

impl Checkpoint {
    /// A checkpoint that reports to `sink` as part `parts_done` of a set, or as the single
    /// upload when `parts_done` is zero.
    pub(crate) fn reporting(
        sink: Option<std::sync::Arc<crate::parts::PartsProgress>>,
        parts_done: u32,
    ) -> Self {
        Self {
            sink: sink.map(|sink| (sink, parts_done)),
            ..Self::default()
        }
    }

    fn observe(&mut self, progress: &crate::ImportProgress) {
        let stage = u8::from(progress.sealed) + u8::from(progress.layout_complete);
        self.import = Some((progress.store_id.clone(), progress.id.clone()));
        self.raise((progress.received_bytes, stage, 0, 0));
    }

    #[cfg(test)]
    pub(crate) fn raise_for_tests(&mut self, progress: (u64, u8, u64, u64)) {
        self.raise(progress);
    }

    /// Raise the durable progress mark; the one place it moves.
    fn raise(&mut self, progress: (u64, u8, u64, u64)) {
        self.progress = self.progress.max(progress);
        if let Some((sink, parts_done)) = &self.sink {
            sink.raise(*parts_done, self.progress);
        }
    }
}

/// Connect, waiting a bounded time while a restarting daemon is not yet accepting.
async fn reconnect(endpoint: &Endpoint) -> Result<Client, Error> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        match Client::connect_endpoint(endpoint).await {
            Err(error)
                if crate::delivery::retryable(&error) && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            result => return result,
        }
    }
}

pub(crate) async fn cancel(endpoint: &Endpoint, store: &str, id: &str) -> Result<(), Error> {
    let client = reconnect(endpoint).await?;
    cancel_on(client.capability(), store, id).await
}

async fn cancel_on(client: &wire::sparrow::Client, store: &str, id: &str) -> Result<(), Error> {
    let mut request = client.import_cancel_request();
    import_reference(request.get().init_import(), store, id);
    let response = request.send().promise.await?;
    match response.get()?.get_result()?.which()? {
        wire::result::Ok(value) => {
            value?;
            Ok(())
        }
        wire::result::Error(error) => Err(Error::domain(error?)),
    }
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
/// The caller's connection is not replaced; a failed attempt cancels its staging journal
/// over it, and `IMPORT_ABANDONED` reports a cancel that also failed.
pub async fn import_open_file(
    client: &wire::sparrow::Client,
    request_id: &str,
    source: &str,
    format: wire::Format,
    file: &mut File,
) -> Result<ArtifactReceipt, Error> {
    // Discovery refuses an older daemon before allocating private staging.
    let discovery = crate::Discovery::fetch(client).await?;
    let mut checkpoint = Checkpoint::default();
    upload_or_cancel(
        client,
        &discovery,
        request_id,
        source,
        format,
        &CompletedInput::capture(file)?,
        &mut checkpoint,
    )
    .await
}

/// Exact bytes staged through the resumable import, read positionally.
pub(crate) trait StagedInput {
    /// Complete byte length declared to the daemon.
    fn length(&self) -> u64;
    /// Fill `buffer` from `offset`; the range lies within `length`.
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<(), Error>;
    /// Refuse an input that changed since its length was captured.
    fn unchanged(&self) -> Result<(), Error>;
    /// The payload identity already computed for these exact bytes, so `upload` need not
    /// read them again; `unchanged` still guards the input before begin.
    fn payload_hint(&self) -> Option<&str> {
        None
    }
}

/// A completed regular file, bound to the identity observed before upload.
pub(crate) struct CompletedInput<'a> {
    file: &'a File,
    identity: CompletedFileIdentity,
}

impl<'a> CompletedInput<'a> {
    pub(crate) fn capture(file: &'a File) -> Result<Self, Error> {
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(Error::InvalidArgument(
                "ingest requires a regular completed file; live tailing is a separate operation",
            ));
        }
        Ok(Self {
            file,
            identity: CompletedFileIdentity::capture(&metadata),
        })
    }
}

impl StagedInput for CompletedInput<'_> {
    fn length(&self) -> u64 {
        self.identity.length
    }
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<(), Error> {
        Ok(self.file.read_exact_at(buffer, offset)?)
    }
    fn unchanged(&self) -> Result<(), Error> {
        self.identity.require(self.file)
    }
}

/// Caller-owned bytes are immutable for the borrow, so they never change.
impl StagedInput for [u8] {
    fn length(&self) -> u64 {
        self.len() as u64
    }
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<(), Error> {
        let start = offset as usize;
        buffer.copy_from_slice(&self[start..start + buffer.len()]);
        Ok(())
    }
    fn unchanged(&self) -> Result<(), Error> {
        Ok(())
    }
}

pub(crate) async fn upload(
    client: &wire::sparrow::Client,
    discovery: &crate::Discovery,
    request_id: &str,
    source: &str,
    format: wire::Format,
    staged: &(impl StagedInput + ?Sized),
    checkpoint: &mut Checkpoint,
) -> Result<ArtifactReceipt, Error> {
    if request_id.is_empty() || request_id.len() > 128 || source.is_empty() || source.len() > 512 {
        return Err(Error::InvalidArgument(
            "require bounded nonempty request and source labels",
        ));
    }
    let length = staged.length();
    let mut buffer = vec![0; crate::IMPORT_CHUNK_BYTES];
    let payload = match staged.payload_hint() {
        Some(payload) => payload.to_owned(),
        None => payload_id(staged, &mut buffer)?,
    };
    staged.unchanged()?;
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
            staged.unchanged()?;
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
    checkpoint.observe(&progress);
    let id = progress.id;
    let mut offset = progress.received_bytes;
    while offset < length {
        let count = (length - offset).min(buffer.len() as u64) as usize;
        staged.read_at(&mut buffer[..count], offset)?;
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
        checkpoint.observe(&value);
    }
    staged.unchanged()?;
    let mut request = client.import_seal_request();
    import_reference(request.get().init_import(), &store, &id);
    let response = request.send().promise.await?;
    match response.get()?.get_result()?.which()? {
        wire::result::Ok(value) => {
            let value = crate::ImportProgress::decode(value?)?;
            value.certify_identity(&store, &id)?;
            checkpoint.observe(&value);
        }
        wire::result::Error(error) => return Err(Error::domain(error?)),
    }
    let mut request = client.import_layout_request();
    import_reference(request.get().init_import(), &store, &id);
    let response = request.send().promise.await?;
    match response.get()?.get_result()?.which()? {
        wire::result::Ok(value) => {
            let value = crate::ImportProgress::decode(value?)?;
            value.certify_identity(&store, &id)?;
            checkpoint.observe(&value);
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
        checkpoint.raise((length, 2, expected, raw_chunks.unwrap_or(0)));
        if expected == value.get_total_records() {
            break;
        }
    }
    staged.unchanged()?;
    let mut request = client.import_publish_request();
    import_reference(request.get().init_import(), &store, &id);
    let response = request.send().promise.await?;
    match response.get()?.get_result()?.which()? {
        wire::result::Ok(value) => {
            let receipt = acceptance.certify(ArtifactReceipt::decode(value?)?)?;
            staged.unchanged()?;
            Ok(receipt)
        }
        wire::result::Error(error) => Err(Error::domain(error?)),
    }
}

/// Canonical payload identity of the whole staged input, read positionally in `buffer`.
pub(crate) fn payload_id(
    staged: &(impl StagedInput + ?Sized),
    buffer: &mut [u8],
) -> Result<String, Error> {
    let length = staged.length();
    let mut digest = purrdf_hash::blake3::Hasher::new();
    let mut remaining = length;
    while remaining != 0 {
        let count = remaining.min(buffer.len() as u64) as usize;
        staged.read_at(&mut buffer[..count], length - remaining)?;
        digest.update(&buffer[..count]);
        remaining -= count as u64;
    }
    Ok(format!(
        "blake3:{}",
        purrdf_hash::hex::encode(digest.finalize().as_bytes())
    ))
}

fn import_reference(mut out: wire::import_ref::Builder<'_>, store: &str, id: &str) {
    out.set_store_id(store);
    out.set_id(id);
}

/// Opened-file generation and observed mutation clocks used by upload and future claims.
///
/// Equality is the inode, length and both clocks. The device number is carried for the wire
/// and for same-boot checks against a live descriptor, but it is never identity: it changes
/// across a reboot on btrfs subvolumes, network, overlay and re-enumerated mounts, so an
/// observation persisted before a restart must still equal the same file observed after it.
#[derive(Clone, Copy, Debug)]
pub struct CompletedFileIdentity {
    /// Filesystem device number, informational only; never compared.
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

impl PartialEq for CompletedFileIdentity {
    fn eq(&self, other: &Self) -> bool {
        // Destructured so a new field must be classified as identity or not.
        let Self {
            device: _,
            inode,
            length,
            modified_seconds,
            modified_nanos,
            changed_seconds,
            changed_nanos,
        } = *self;
        (
            inode,
            length,
            modified_seconds,
            modified_nanos,
            changed_seconds,
            changed_nanos,
        ) == (
            other.inode,
            other.length,
            other.modified_seconds,
            other.modified_nanos,
            other.changed_seconds,
            other.changed_nanos,
        )
    }
}

impl Eq for CompletedFileIdentity {}

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

    /// INVARIANT: a changed device number alone leaves a completed-file identity equal, so a
    /// claim, finish intent or checkpoint persisted before a reboot still matches the same
    /// file afterwards; a different inode, length or clock never does.
    #[test]
    fn completed_file_identity_ignores_the_device_and_nothing_else() {
        let base = super::CompletedFileIdentity {
            device: 1,
            inode: 7,
            length: 10,
            modified_seconds: 100,
            modified_nanos: 5,
            changed_seconds: 200,
            changed_nanos: 6,
        };
        let renumbered = super::CompletedFileIdentity { device: 2, ..base };
        assert_eq!(base, renumbered);
        for changed in [
            super::CompletedFileIdentity {
                inode: 8,
                ..renumbered
            },
            super::CompletedFileIdentity {
                length: 9,
                ..renumbered
            },
            super::CompletedFileIdentity {
                modified_seconds: 101,
                ..renumbered
            },
            super::CompletedFileIdentity {
                modified_nanos: 6,
                ..renumbered
            },
            super::CompletedFileIdentity {
                changed_seconds: 201,
                ..renumbered
            },
            super::CompletedFileIdentity {
                changed_nanos: 7,
                ..renumbered
            },
        ] {
            assert_ne!(base, changed);
        }
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
