// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

mod disk;
mod worker;

use crate::{
    CompletedFileIdentity, Endpoint, Error, Format, IMPORT_CHUNK_BYTES, format_label,
    open_completed_file,
};
use disk::{Journal, State};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    io,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::{Notify, Semaphore, oneshot, watch},
};

/// Maximum number of recent range receipts carried in a single journal transaction.
const RECEIPT_WINDOW: u32 = 8;

/// Per-job disk and prefetch bounds. Jobs use separate connections and admission from log delivery.
#[derive(Debug, Clone)]
pub struct BulkConfig {
    /// Producer endpoint, used only by background workers.
    pub endpoint: Endpoint,
    /// Optional role or store requirement checked before any network mutation.
    pub expect: Option<crate::Expect>,
    /// Private persistent directory. Reopen the same job ID here to recover it.
    pub directory: PathBuf,
    /// Maximum simultaneously open inputs on this importer, from one to sixteen.
    pub concurrency: usize,
    /// Maximum unconfirmed input bytes per job. Larger files wait for room between ranges.
    pub staged_bytes: u64,
    /// Target maximum raw bytes in a remote stream range.
    pub range_bytes: u64,
}
impl BulkConfig {
    /// Configure two workers, 128 MiB of staged input per job and 256 KiB stream ranges.
    #[must_use]
    pub fn new(endpoint: Endpoint, directory: impl Into<PathBuf>) -> Self {
        Self {
            endpoint,
            expect: None,
            directory: directory.into(),
            concurrency: 2,
            staged_bytes: 128 * 1024 * 1024,
            range_bytes: 256 * 1024,
        }
    }
}

/// One completed file's stable input identity and original format.
#[derive(Clone, Debug)]
pub struct BulkFile {
    /// Distinct application-owned request key that survives process restarts.
    pub id: String,
    /// Sparrow source label.
    pub source: String,
    /// Original file format.
    pub format: Format,
    /// Caller-owned completed file.
    pub path: PathBuf,
}

/// Client handle for importing files and incremental byte streams from any Tokio runtime.
#[derive(Clone)]
pub struct BulkImporter {
    config: Arc<BulkConfig>,
    workers: Arc<Semaphore>,
}

impl BulkImporter {
    /// Validate the bounds. Opening a job creates its local journal without waiting for Sparrow.
    pub fn new(config: BulkConfig) -> Result<Self, Error> {
        config.endpoint.validate()?;
        if let Some(expected) = &config.expect {
            expected.validate()?;
        }
        if !(1..=16).contains(&config.concurrency)
            || config.range_bytes == 0
            || config.range_bytes > config.staged_bytes
            || config.staged_bytes > i64::MAX as u64
            || !config.directory.is_absolute()
        {
            return Err(Error::InvalidArgument(
                "bulk requires an absolute directory, 1..16 workers and positive range_bytes <= staged_bytes <= i64::MAX",
            ));
        }
        Ok(Self {
            workers: Arc::new(Semaphore::new(config.concurrency)),
            config: Arc::new(config),
        })
    }

    /// Create or reopen a durable job. One active writer owns its ID across processes.
    pub async fn open(&self, id: &str, source: &str, format: Format) -> Result<BulkJob, Error> {
        if id.is_empty() || id.len() > 128 || source.is_empty() || source.len() > 512 {
            return Err(Error::InvalidArgument(
                "bulk requires a 1..128-byte job ID and 1..512-byte source",
            ));
        }
        let permit = self
            .workers
            .clone()
            .acquire_owned()
            .await
            .map_err(io::Error::other)?;
        let initial = State {
            id: id.into(),
            source: source.into(),
            format: format_label(format).into(),
            staged: 0,
            boundary: 0,
            confirmed: 0,
            count: 0,
            receipts: Vec::new(),
            page_start: 0,
            page_offset: 0,
            previous: None,
            eof: false,
            store: None,
            pending: None,
            completed: None,
            file: None,
        };
        let root = self.config.directory.clone();
        let (permit, journal) =
            tokio::task::spawn_blocking(move || Journal::open(&root, initial).map(|j| (permit, j)))
                .await
                .map_err(io::Error::other)??;
        let (status, progress) = watch::channel(BulkProgress::from_state(&journal.state));
        let shared = Arc::new(Shared {
            _permit: permit,
            journal: Mutex::new(journal),
            upload: Arc::default(),
            status,
            wake: Notify::new(),
            config: self.config.clone(),
        });
        let (stop, stopped) = oneshot::channel();
        let worker = shared.clone();
        let (done, handle) = oneshot::channel();
        std::thread::Builder::new()
            .name("sparrow-bulk".into())
            .spawn(move || {
                let result = (|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    tokio::task::LocalSet::new().block_on(&runtime, async {
                        tokio::select! { r=worker::run(&worker)=>r, _=stopped=>Ok(()) }
                    })
                })();
                drop(worker);
                let _ = done.send(result);
            })?;
        Ok(BulkJob {
            shared,
            progress,
            stop: Some(stop),
            handle: Some(handle),
            staging: None,
        })
    }

    /// Snapshot and import a completed file, returning its durable artifact or manifest receipt.
    pub async fn import_file(&self, input: &BulkFile) -> Result<BulkSummary, Error> {
        let mut job = self.open(&input.id, &input.source, input.format).await?;
        let sealed_file = {
            let progress = job.progress.borrow();
            progress.eof && progress.file.is_some()
        };
        if !sealed_file {
            job.stage_file(&input.path).await?;
        }
        job.finish().await
    }

    /// Import several files with bounded open descriptors and upload workers. Each result names
    /// its input, including on failure. Results arrive in completion order.
    pub fn import_files<'a>(
        &'a self,
        files: impl IntoIterator<Item = BulkFile> + 'a,
    ) -> impl Stream<Item = (BulkFile, Result<BulkSummary, Error>)> + 'a {
        futures::stream::iter(files)
            .map(move |input| async move {
                let result = self.import_file(&input).await;
                (input, result)
            })
            .buffer_unordered(self.config.concurrency)
    }
}

/// One acknowledged range. Its half-open offsets identify the original bytes across retries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeReceipt {
    /// Part ordinal.
    pub index: u32,
    /// First byte in the input.
    pub start: u64,
    /// First byte after this range.
    pub end: u64,
    /// Stored format, including opaque for a split record.
    pub format: String,
    /// Persistent destination store.
    pub store_id: String,
    /// Certified artifact occurrence.
    pub artifact_id: String,
    /// Certified whole-part BLAKE3 digest.
    pub payload_id: String,
}

/// Snapshot of independently advancing local and remote offsets.
#[derive(Debug, Clone)]
pub struct BulkProgress {
    file: Option<CompletedFileIdentity>,
    /// Bytes durably admitted locally, including those subsequently confirmed remotely.
    pub staged: u64,
    /// End of the contiguous remotely durable prefix.
    pub confirmed: u64,
    /// Number of certified data ranges, including those in earlier manifest windows.
    pub ranges: u32,
    /// First range whose detailed receipt is retained locally.
    pub first_local_range: u32,
    /// Most recently published manifest window, covering all earlier ranges through its links.
    pub last_window: Option<String>,
    /// Whether EOF has been durably recorded.
    pub eof: bool,
    /// Most recent retry or terminal failure, cleared when delivery advances.
    pub diagnostic: Option<String>,
    /// Certified final artifact or part-set manifest ID.
    pub artifact_id: Option<String>,
}
impl BulkProgress {
    fn from_state(state: &State) -> Self {
        Self {
            file: state.file,
            staged: state.staged,
            confirmed: state.confirmed,
            ranges: state.count,
            first_local_range: state.page_start,
            last_window: state.previous.clone(),
            eof: state.eof,
            diagnostic: None,
            artifact_id: state.completed.clone(),
        }
    }
}

/// Durable result for one complete input.
#[derive(Debug, Clone)]
pub struct BulkSummary {
    /// Persistent destination store.
    pub store_id: String,
    /// Whole-file artifact or ordered part-set manifest.
    pub artifact_id: String,
    /// Exact input length.
    pub bytes: u64,
    /// Number of data artifacts.
    pub ranges: u32,
}

struct Shared {
    _permit: tokio::sync::OwnedSemaphorePermit,
    journal: Mutex<Journal>,
    upload: Arc<crate::parts::PartsProgress>,
    status: watch::Sender<BulkProgress>,
    wake: Notify,
    config: Arc<BulkConfig>,
}
impl Shared {
    fn lock(&self) -> Result<MutexGuard<'_, Journal>, Error> {
        let guard = self
            .journal
            .lock()
            .map_err(|_| Error::Protocol("bulk journal lock poisoned"))?;
        if guard.faulted {
            return Err(Error::Protocol(
                "bulk metadata transaction failed; reopen the job before resuming",
            ));
        }
        Ok(guard)
    }
    fn changed(&self, state: &State) {
        self.status.send_modify(|p| {
            let diagnostic = p.diagnostic.take();
            *p = BulkProgress::from_state(state);
            p.diagnostic = diagnostic;
        });
        self.wake.notify_one();
    }
}

/// An exclusively open job. Dropping the handle stops the uploader and leaves durable data
/// for the next open. Use `close` to wait for the worker to release the job.
pub struct BulkJob {
    shared: Arc<Shared>,
    progress: watch::Receiver<BulkProgress>,
    stop: Option<oneshot::Sender<()>>,
    handle: Option<oneshot::Receiver<Result<(), Error>>>,
    staging: Option<tokio::task::JoinHandle<Result<(), Error>>>,
}
impl BulkJob {
    /// Observe byte offsets and receipts without waiting for EOF.
    #[must_use]
    pub fn progress(&self) -> watch::Receiver<BulkProgress> {
        self.progress.clone()
    }

    /// Native staged-upload checkpoints for the current range.
    #[must_use]
    pub fn upload_progress(&self) -> crate::parts::ProgressMark {
        self.shared.upload.mark()
    }

    async fn settle_staging(&mut self) -> Result<(), Error> {
        if let Some(inflight) = &mut self.staging {
            let result = inflight.await;
            self.staging.take();
            result.map_err(io::Error::other)??;
        }
        Ok(())
    }

    /// Read a confirmed range receipt by ordinal without collecting the full stream.
    pub async fn receipt(&self, index: u32) -> Result<RangeReceipt, Error> {
        let shared = self.shared.clone();
        tokio::task::spawn_blocking(move || {
            let j = shared.lock()?;
            if index < j.state.page_start {
                return Err(Error::InvalidArgument(
                    "range was published in a manifest window; follow the last_window artifact",
                ));
            }
            if index >= j.state.count {
                return Err(Error::InvalidArgument("range has not been confirmed"));
            }
            j.state
                .receipts
                .get((index - j.state.page_start) as usize)
                .cloned()
                .ok_or(Error::Protocol("bulk journal lost a confirmed range"))
        })
        .await
        .map_err(io::Error::other)?
    }

    /// Snapshot a completed file, retaining its caller-owned descriptor generation across
    /// reads. Files larger than the staging allowance wait for published ranges to make room.
    /// Supply the same unchanged file to continue a partially staged snapshot after restart.
    pub async fn stage_file(&mut self, path: impl AsRef<Path>) -> Result<(), Error> {
        self.settle_staging().await?;
        let path = path.as_ref().to_owned();
        let shared = self.shared.clone();
        let (opened, descriptor) = oneshot::channel();
        self.staging = Some(tokio::task::spawn_blocking(move || {
            let file = open_completed_file(&path)?;
            let identity = CompletedFileIdentity::capture(&file.metadata()?);
            let mut j = shared.lock()?;
            match j.state.file {
                Some(old) if old != identity => {
                    return Err(Error::InvalidArgument(
                        "completed input changed since the job began",
                    ));
                }
                Some(_) => {}
                None if j.state.staged != 0 || j.state.eof => {
                    return Err(Error::InvalidArgument("bulk job already owns stream input"));
                }
                None => j.change(|s| s.file = Some(identity))?,
            }
            shared.changed(&j.state);
            let _ = opened.send((Arc::new(file), identity));
            Ok(())
        }));
        self.settle_staging().await?;
        let (input, identity) = descriptor.await.map_err(io::Error::other)?;
        loop {
            let progress = self.progress.borrow().clone();
            if progress.eof {
                return Ok(());
            }
            let remaining = identity
                .length
                .checked_sub(progress.staged)
                .ok_or(Error::Protocol("bulk file offset exceeds input length"))?;
            let take = if remaining == 0 {
                0
            } else {
                remaining.min(self.capacity().await? as u64)
            };
            let shared = self.shared.clone();
            let input = input.clone();
            self.staging = Some(tokio::task::spawn_blocking(move || {
                identity.require(&input)?;
                let mut bytes = vec![0; take as usize];
                input.read_exact_at(&mut bytes, progress.staged)?;
                identity.require(&input)?;
                let mut j = shared.lock()?;
                if j.state.staged != progress.staged {
                    return Err(Error::Protocol("bulk file offset changed"));
                }
                if take != 0 {
                    j.append(&bytes)?;
                }
                if take == remaining {
                    identity.require(&input)?;
                    j.change(|s| s.eof = true)?;
                }
                shared.changed(&j.state);
                Ok(())
            }));
            self.settle_staging().await?;
        }
    }

    async fn capacity(&mut self) -> Result<usize, Error> {
        loop {
            let p = self.progress.borrow_and_update().clone();
            let available = self
                .shared
                .config
                .staged_bytes
                .saturating_sub(p.staged - p.confirmed);
            if available > 0 {
                return Ok(available.min(IMPORT_CHUNK_BYTES as u64) as usize);
            }
            let handle = self
                .handle
                .as_mut()
                .ok_or(Error::Protocol("bulk worker closed"))?;
            tokio::select! {
                result = handle => {
                    self.handle.take();
                    result.map_err(io::Error::other)??;
                    return Err(Error::Protocol("bulk upload stopped before EOF"));
                }
                change = self.progress.changed() => change.map_err(io::Error::other)?,
            }
        }
    }

    /// Read exactly from the specified durable input offset. Supply upstream bytes beginning
    /// there when reattaching after a crash. Backpressure stops polling the reader at quota.
    /// EOF seals the job locally and `finish` waits for the remote receipt.
    pub async fn stage_stream(
        &mut self,
        offset: u64,
        mut reader: impl AsyncRead + Unpin,
    ) -> Result<(), Error> {
        self.settle_staging().await?;
        let initial = self.progress.borrow().clone();
        if initial.eof || offset != initial.staged || initial.file.is_some() {
            return Err(Error::InvalidArgument(
                "stream must resume at its current staged offset before EOF",
            ));
        }
        let mut buf = vec![0; IMPORT_CHUNK_BYTES];
        loop {
            let size = self.capacity().await?;
            let handle = self
                .handle
                .as_mut()
                .ok_or(Error::Protocol("bulk worker closed"))?;
            let count = tokio::select! {
                result = handle => {
                    self.handle.take();
                    result.map_err(io::Error::other)??;
                    return Err(Error::Protocol("bulk worker ended before EOF"));
                }
                read = reader.read(&mut buf[..size]) => read?,
            };
            let shared = self.shared.clone();
            let chunk = buf[..count].to_vec();
            self.staging = Some(tokio::task::spawn_blocking(move || {
                let mut j = shared.lock()?;
                if count == 0 {
                    j.change(|s| s.eof = true)?;
                } else {
                    j.append(&chunk)?;
                }
                shared.changed(&j.state);
                Ok(())
            }));
            self.settle_staging().await?;
            if count == 0 {
                return Ok(());
            }
        }
    }

    /// Wait for the final certified remote receipt after EOF.
    pub async fn finish(mut self) -> Result<BulkSummary, Error> {
        self.settle_staging().await?;
        if !self.progress.borrow().eof {
            return Err(Error::InvalidArgument(
                "stage a completed file or reach stream EOF before finish",
            ));
        }
        if let Some(handle) = self.handle.take() {
            handle.await.map_err(io::Error::other)??;
        }
        let j = self.shared.lock()?;
        Ok(BulkSummary {
            store_id: j
                .state
                .store
                .clone()
                .ok_or(Error::Protocol("bulk completion omitted store"))?,
            artifact_id: j
                .state
                .completed
                .clone()
                .ok_or(Error::Protocol("bulk completion omitted receipt"))?,
            bytes: j.state.confirmed,
            ranges: j.state.count,
        })
    }

    /// Stop this worker and wait until all active network work has ended.
    pub async fn close(mut self) -> Result<(), Error> {
        self.settle_staging().await?;
        self.stop.take();
        if let Some(handle) = self.handle.take() {
            handle.await.map_err(io::Error::other)??;
        }
        Ok(())
    }
}
