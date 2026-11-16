// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Anchored completed-file claims. Readiness and discovery belong to the watcher.

use crate::source_file::{BENEATH, open};
use rustix::fs::{self, AtFlags, FlockOperation, Mode, OFlags, RenameFlags};
use sparrow_client::parts::{
    ClaimParts, Frontier, MissingLimits, PartLimits, PartsProgress, Resume,
};
use sparrow_client::{Client, CompletedFileIdentity};
use sparrow_core::{
    ConsumptionDirectories, ConsumptionGeneration, ConsumptionJournal, RetiredConsumption,
    SetManifest, Store,
};
use std::{
    fs::File,
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::Path,
    sync::Arc,
};

/// A recoverable filesystem or publication failure; no error certifies removal.
#[derive(Debug)]
pub enum Error {
    /// Persistent ownership/publication refusal.
    Storage(sparrow_core::Error),
    /// Typed producer transport or receipt refusal.
    Upload(sparrow_client::Error),
    /// Filesystem operation failed; the journal remains retryable.
    Io(std::io::Error),
    /// Contradictory or unsupported source/namespace state.
    State(&'static str),
    /// A discovered declared-finished generation is terminal; this does not defer other scan entries.
    FinishedGeneration(&'static str),
    /// `--private-intake` would rebind two configured roots with one final component to one
    /// private root.
    PrivateRootShared {
        /// The private root both would consume.
        private: std::path::PathBuf,
        /// The configured root already bound to it.
        first: std::path::PathBuf,
        /// The configured root that would share it.
        second: std::path::PathBuf,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(e) => std::fmt::Display::fmt(e, f),
            Self::Upload(e) => std::fmt::Display::fmt(e, f),
            Self::Io(e) => std::fmt::Display::fmt(e, f),
            Self::State(message) | Self::FinishedGeneration(message) => f.write_str(message),
            Self::PrivateRootShared {
                private,
                first,
                second,
            } => write!(
                f,
                "--private-intake would bind configured roots {} and {} to one private root {}; their final path components must differ",
                first.display(),
                second.display(),
                private.display()
            ),
        }
    }
}
impl std::error::Error for Error {}
impl From<sparrow_core::Error> for Error {
    fn from(e: sparrow_core::Error) -> Self {
        Self::Storage(e)
    }
}
impl From<sparrow_client::Error> for Error {
    fn from(e: sparrow_client::Error) -> Self {
        Self::Upload(e)
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<rustix::io::Errno> for Error {
    fn from(e: rustix::io::Errno) -> Self {
        Self::Io(e.into())
    }
}

const INFLIGHT: &str = ".sparrow-inflight";

/// What one claim's attempts share across retries (#280): its progress, which survives an
/// attempt dropped at the deadline, and its planned parts, digests and receipts.
#[derive(Debug, Default)]
pub struct ClaimAttempt {
    /// Durable progress of the current attempt.
    pub progress: Arc<PartsProgress>,
    /// Planned parts and their receipts.
    pub parts: Arc<ClaimParts>,
}

/// The neutral failure of a recorded claim whose publication proof is gone: neither its set
/// manifest receipt nor its single-path publication receipt exists. Nothing is uploaded.
fn lost_publication(journal: &ConsumptionJournal) -> sparrow_core::Error {
    sparrow_core::Error::new(
        "STORAGE_FAILURE",
        format!(
            "publication receipt of recorded claim {} is missing (published as {}); the source is held and no upload was attempted; see docs/deployment.md",
            journal.import_id,
            journal.published_artifact.as_deref().unwrap_or_default()
        ),
    )
}

/// Name the advertised values a daemon did not send when the single path meets a refusal a
/// split would have avoided: the over-bound refusal, or the retirement refusal at layout.
fn name_missing_limits(
    error: sparrow_client::Error,
    missing: MissingLimits,
) -> sparrow_client::Error {
    match error {
        sparrow_client::Error::Domain {
            code,
            message,
            retryable,
            class,
        } if (code == "INVALID_ARGUMENT" && message.contains("exceeds the import bound"))
            || (code == "RESOURCE_LIMIT"
                && missing.retirement
                && message.contains("exceeds what one retirement pass can read")) =>
        {
            let message = if code == "INVALID_ARGUMENT" {
                format!(
                    "{message}; declared input exceeds the daemon's import bound and the daemon did not advertise {}; the file cannot be split",
                    missing.names()
                )
            } else {
                format!(
                    "{message}; the daemon did not advertise {}; the file cannot be split",
                    missing.names()
                )
            };
            sparrow_client::Error::Domain {
                code,
                message,
                retryable,
                class,
            }
        }
        error => error,
    }
}

/// Whether a watch root is the one its stored claims were bound to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RootBinding {
    /// The stored root: a claim bound to another root identity refuses the open.
    Stored,
    /// `sparrowd --private-intake`: a disposable copy's private root. Claims bound to the
    /// original root identity are foreign here; they never refuse the open and are never
    /// recovered, because every recovery first refuses a claim of another root.
    Private,
}

#[cfg(test)]
mod crash_tests;

/// Whether a stored directory inode names the directory held now. The device number is never
/// compared: it changes across a reboot on btrfs subvolumes, network, overlay and re-enumerated
/// mounts, and the held descriptor already fixes the directory within one run.
#[must_use]
fn bound(stored_inode: u64, current_inode: u64) -> bool {
    stored_inode == current_inode
}

fn observation(file: &File) -> Result<ConsumptionGeneration, Error> {
    generation(CompletedFileIdentity::capture(&file.metadata()?))
}

pub(crate) fn generation(identity: CompletedFileIdentity) -> Result<ConsumptionGeneration, Error> {
    Ok(ConsumptionGeneration {
        device: identity.device,
        inode: identity.inode,
        byte_length: identity.length,
        modified: (
            identity.modified_seconds,
            u32::try_from(identity.modified_nanos)
                .map_err(|_| Error::State("invalid source modification nanoseconds"))?,
        ),
        changed: (
            identity.changed_seconds,
            u32::try_from(identity.changed_nanos)
                .map_err(|_| Error::State("invalid source change nanoseconds"))?,
        ),
    })
}

fn require(file: &File, generation: &ConsumptionGeneration) -> Result<(), Error> {
    identity(generation).require(file)?;
    Ok(())
}

pub(crate) fn identity(generation: &ConsumptionGeneration) -> CompletedFileIdentity {
    CompletedFileIdentity {
        device: generation.device,
        inode: generation.inode,
        length: generation.byte_length,
        modified_seconds: generation.modified.0,
        modified_nanos: i64::from(generation.modified.1),
        changed_seconds: generation.changed.0,
        changed_nanos: i64::from(generation.changed.1),
    }
}

/// One exclusively locked, descriptor-anchored configured watch root.
/// Producers must publish finished files and never write them after handoff.
pub struct ClaimRoot {
    store: Store,
    watch: String,
    root: File,
    inflight: File,
    lock: ClaimLock,
}

/// Explicitly release ownership even if a fork temporarily inherited the open description.
struct ClaimLock(File);

impl ClaimLock {
    fn acquire(file: File) -> Result<Self, Error> {
        fs::flock(&file, FlockOperation::NonBlockingLockExclusive)?;
        Ok(Self(file))
    }
}

impl Drop for ClaimLock {
    fn drop(&mut self) {
        if let Err(error) = fs::flock(&self.0, FlockOperation::Unlock) {
            eprintln!("sparrowd: releasing completed-consumption ownership lock failed: {error}");
        }
    }
}

/// The opened claimed generation. The original pathname is never reopened for upload.
#[derive(Debug)]
pub struct ClaimedFile {
    journal: ConsumptionJournal,
    file: File,
}

impl ClaimedFile {
    /// Original source ownership and stable request identity.
    pub fn journal(&self) -> &ConsumptionJournal {
        &self.journal
    }
}

/// Recovery either retains an exact opened generation or completes an already
/// published, absent claim after synchronizing its private directory.
#[derive(Debug)]
pub enum Recovery {
    /// The source remains claimed and must be published or removed.
    Claimed(ClaimedFile),
    /// Directory-synced source removal is durably recorded.
    Removed(ConsumptionJournal),
}

impl ClaimRoot {
    /// Open a discovery directory beneath the held root without following symlinks.
    pub(crate) fn directory(&self, relative: &Path) -> Result<File, Error> {
        crate::source_file::directory(&self.root, relative)
    }

    /// Verify the configured root still names this owner, including its parents.
    pub(crate) fn root_binding(&self, path: &Path) -> Result<(), Error> {
        self.private_namespace()?;
        crate::source_file::root_binding(&self.root, path)
    }

    /// Observe a directory-relative file without opening it or generating IN_OPEN.
    /// The only followed alias is the kernel-owned procfs descriptor for this held parent.
    pub(crate) fn observe(&self, relative: &Path) -> Result<Option<CompletedFileIdentity>, Error> {
        crate::source_file::observe(&self.root, relative)
    }

    /// Open an admitted producer root; never follow symlinks or overlap store data.
    /// Creates only the excluded private claim directory and its ownership lock.
    pub fn open(store: Store, watch: &str, path: &Path) -> Result<Self, Error> {
        Self::open_bound(store, watch, path, RootBinding::Stored)
    }

    /// Open as [`Self::open`] does, under an explicit root binding.
    pub(crate) fn open_bound(
        store: Store,
        watch: &str,
        path: &Path,
        binding: RootBinding,
    ) -> Result<Self, Error> {
        if watch.is_empty() || watch.len() > 128 || !path.is_absolute() {
            return Err(Error::State(
                "require a bounded watch identity and absolute producer root",
            ));
        }
        let root = crate::source_file::root(&store, path)?;
        Self::from_root(store, watch, root, binding)
    }

    /// Share an already admitted rooted descriptor with a tail owner; never reopen
    /// a pathname that could now identify a replacement directory.
    pub(crate) fn from_root(
        store: Store,
        watch: &str,
        root: File,
        binding: RootBinding,
    ) -> Result<Self, Error> {
        let root_identity = CompletedFileIdentity::capture(&root.metadata()?);
        let bound_here = |journal: &ConsumptionJournal| {
            bound(journal.directories.root_inode, root_identity.inode)
        };
        let pending = store
            .consumption_pending()?
            .into_iter()
            .filter(|journal| journal.watch == watch)
            .filter(|journal| binding == RootBinding::Stored || bound_here(journal))
            .collect::<Vec<_>>();
        if pending.iter().any(|journal| !bound_here(journal)) {
            return Err(Error::State(
                "configured watch root was replaced while source recovery ownership remains",
            ));
        }
        match fs::mkdirat(&root, INFLIGHT, Mode::from_raw_mode(0o700)) {
            Ok(()) => root.sync_all()?,
            Err(error) if error == rustix::io::Errno::EXIST => {}
            Err(error) => return Err(error.into()),
        }
        let inflight = open(
            &root,
            Path::new(INFLIGHT),
            OFlags::RDONLY | OFlags::DIRECTORY,
            BENEATH,
        )?;
        let metadata = inflight.metadata()?;
        if metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.mode() & 0o7777 != 0o700
        {
            return Err(Error::State(
                "claim directory must be owned by the daemon and mode 0700",
            ));
        }
        let lock = open(
            &inflight,
            Path::new(".lock"),
            OFlags::RDWR | OFlags::CREATE,
            BENEATH,
        )?;
        let metadata = lock.metadata()?;
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.uid() != rustix::process::geteuid().as_raw()
            || metadata.mode() & 0o7777 != 0o600
        {
            return Err(Error::State(
                "claim lock must be one daemon-owned regular file, mode 0600",
            ));
        }
        let lock = ClaimLock::acquire(lock)?;
        lock.0.sync_all()?;
        inflight.sync_all()?;
        let known = pending
            .iter()
            .map(|journal| journal.import_id.as_bytes())
            .collect::<std::collections::BTreeSet<_>>();
        for (index, entry) in fs::Dir::read_from(&inflight)?.enumerate() {
            if index >= known.len() + 3 {
                return Err(Error::State(
                    "private claim directory exceeds its bounded journal registry; reconciliation required",
                ));
            }
            let entry = entry?;
            let name = entry.file_name().to_bytes();
            if name != b"." && name != b".." && name != b".lock" && !known.contains(name) {
                return Err(Error::State(
                    "unrecognized private claim entry; inspect and reconcile it without discarding its bytes",
                ));
            }
        }
        let owner = Self {
            store,
            watch: watch.into(),
            root,
            inflight,
            lock,
        };
        owner.private_namespace()?;
        Ok(owner)
    }

    fn parent(&self, relative: &Path) -> Result<File, Error> {
        crate::source_file::parent(&self.root, relative)
    }

    fn directories(&self, parent: &File) -> Result<ConsumptionDirectories, Error> {
        crate::source_file::directories(&self.root, parent)
    }

    fn scope(&self, journal: &ConsumptionJournal) -> Result<(), Error> {
        if journal.watch != self.watch {
            return Err(Error::State(
                "claim belongs to a different configured watch",
            ));
        }
        let root = CompletedFileIdentity::capture(&self.root.metadata()?);
        if !bound(journal.directories.root_inode, root.inode) {
            return Err(Error::State(
                "watch root was replaced; preserve original claim ownership",
            ));
        }
        Ok(())
    }

    fn private_namespace(&self) -> Result<(), Error> {
        let current = open(
            &self.root,
            Path::new(INFLIGHT),
            OFlags::RDONLY | OFlags::DIRECTORY,
            BENEATH,
        )?;
        let current = CompletedFileIdentity::capture(&current.metadata()?);
        let held = CompletedFileIdentity::capture(&self.inflight.metadata()?);
        if current.device != held.device || current.inode != held.inode {
            return Err(Error::State(
                "private claim directory was replaced; no path removal admitted",
            ));
        }
        let current = open(&self.inflight, Path::new(".lock"), OFlags::RDONLY, BENEATH)?;
        let current = CompletedFileIdentity::capture(&current.metadata()?);
        let held = CompletedFileIdentity::capture(&self.lock.0.metadata()?);
        if current.device != held.device || current.inode != held.inode {
            return Err(Error::State(
                "private coordination lock was replaced; source transfer refused",
            ));
        }
        Ok(())
    }

    fn namespace(&self, journal: &ConsumptionJournal) -> Result<Option<File>, Error> {
        self.scope(journal)?;
        self.private_namespace()?;
        if journal.claimed.is_some() {
            return Ok(None);
        }
        let relative = Path::new(std::ffi::OsStr::from_bytes(&journal.relative_path));
        let parent = self.parent(relative)?;
        let current = self.directories(&parent)?;
        let stored = &journal.directories;
        if !bound(stored.root_inode, current.root_inode)
            || !bound(stored.parent_inode, current.parent_inode)
        {
            return Err(Error::State(
                "original parent directory was replaced before the claim checkpoint; preserve the source for reconciliation",
            ));
        }
        Ok(Some(parent))
    }

    /// Reserve a scheduler-admitted, stable completed generation before moving it.
    /// This is not a readiness detector; expected identity comes from quiet/handoff evidence.
    pub fn reserve(
        &self,
        relative: &Path,
        expected: CompletedFileIdentity,
        source: &str,
        format: &str,
    ) -> Result<ConsumptionJournal, Error> {
        let (_file, directories) = self.prepare_source(relative, expected)?;
        Ok(self.store.consumption_reserve(
            &self.watch,
            relative.as_os_str().as_bytes(),
            source,
            format,
            // Reserve the admitted generation, never a fresh observation that
            // could silently adopt a write after the final readiness check.
            generation(expected)?,
            directories,
            crate::now()?,
        )?)
    }

    pub(crate) fn handoff(
        &self,
        intent: &sparrow_core::ConsumptionHandoff,
        source: &str,
        format: &str,
        now: u64,
    ) -> Result<ConsumptionJournal, Error> {
        if intent.watch != self.watch {
            return Err(Error::State("handoff belongs to another claim root"));
        }
        let relative = Path::new(std::ffi::OsStr::from_bytes(&intent.relative_path));
        let (_file, directories) = self.prepare_source(relative, identity(&intent.observed))?;
        Ok(self
            .store
            .consumption_handoff_reserve(intent, source, format, directories, now)?)
    }

    fn prepare_source(
        &self,
        relative: &Path,
        expected: CompletedFileIdentity,
    ) -> Result<(File, ConsumptionDirectories), Error> {
        self.private_namespace()?;
        let parent = self.parent(relative)?;
        let name = relative
            .file_name()
            .ok_or(Error::State("source path has no filename"))?;
        let file = open(&parent, Path::new(name), OFlags::RDONLY, BENEATH)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(Error::State("completed source must be a regular file"));
        }
        // The claim renames the source into the private directory, which needs one filesystem.
        // Both devices are read live from descriptors held now, never from a stored value.
        if metadata.dev() != self.root.metadata()?.dev() {
            return Err(Error::State(
                "completed source is not on the watch root's filesystem; move it onto that filesystem before handoff",
            ));
        }
        expected.require(&file)?;
        file.sync_all()?;
        parent.sync_all()?;
        expected.require(&file)?;
        Ok((file, self.directories(&parent)?))
    }

    /// Reconcile a reserved/claimed source through the same atomic move path.
    /// A replacement at its old path is never uploaded or removed on old ownership.
    pub fn recover(&self, id: &str, now: u64) -> Result<Recovery, Error> {
        let mut journal = self.store.consumption_get(id)?;
        self.scope(&journal)?;
        if journal.removed_at.is_some() {
            return Ok(Recovery::Removed(journal));
        }
        let parent = self.namespace(&journal)?;
        let private_id = journal.import_id.clone();
        let private_name = Path::new(&private_id);
        let file = match open(&self.inflight, private_name, OFlags::RDONLY, BENEATH) {
            Ok(file) => file,
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                if journal.published_id().is_some() {
                    let artifact = self.publication(&journal, now)?;
                    self.inflight.sync_all()?;
                    return Ok(Recovery::Removed(
                        self.store.consumption_removed(
                            id,
                            &artifact,
                            journal.claimed.as_ref().ok_or(Error::State(
                                "published source has no claimed generation",
                            ))?,
                            now,
                        )?,
                    ));
                }
                if journal.claimed.is_some() {
                    return Err(Error::State(
                        "unpublished claimed source is missing; restore or reconcile it, never republish a replacement",
                    ));
                }
                let relative = Path::new(std::ffi::OsStr::from_bytes(&journal.relative_path));
                let name = relative
                    .file_name()
                    .ok_or(Error::State("reserved source path has no filename"))?;
                let parent = parent.as_ref().ok_or(Error::State(
                    "unconfirmed source has no original parent descriptor",
                ))?;
                let file = open(parent, Path::new(name), OFlags::RDONLY, BENEATH)?;
                if !file.metadata()?.is_file() {
                    return Err(Error::State(
                        "reserved source is not a regular file; move refused",
                    ));
                }
                require(&file, &journal.observed)?;
                #[cfg(test)]
                sparrow_test_support::crash::checkpoint("claim_before_rename");
                fs::renameat_with(
                    parent,
                    name,
                    &self.inflight,
                    private_name,
                    RenameFlags::NOREPLACE,
                )?;
                #[cfg(test)]
                sparrow_test_support::crash::checkpoint("claim_after_rename");
                file
            }
            Err(error) => return Err(error),
        };
        if !file.metadata()?.is_file() {
            return Err(Error::State(
                "private claim is not a regular file; preserve it for reconciliation",
            ));
        }
        let observed = observation(&file)?;
        let named = open(&self.inflight, private_name, OFlags::RDONLY, BENEATH)?;
        require(&named, &observed)?;
        if let Some(claimed) = &journal.claimed {
            require(&file, claimed)?;
        } else {
            // Never synchronize the source-parent removal before the destination
            // and source contents are durable: every interrupted move has a copy.
            file.sync_all()?;
            #[cfg(test)]
            sparrow_test_support::crash::checkpoint("claim_contents_synced");
            self.inflight.sync_all()?;
            #[cfg(test)]
            sparrow_test_support::crash::checkpoint("claim_destination_synced");
            parent
                .as_ref()
                .ok_or(Error::State(
                    "unconfirmed claim has no original parent descriptor",
                ))?
                .sync_all()?;
            #[cfg(test)]
            sparrow_test_support::crash::checkpoint("claim_parent_synced");
            require(&file, &observed)?;
            journal = self.store.consumption_claimed(id, observed)?;
            #[cfg(test)]
            sparrow_test_support::crash::checkpoint("claim_checkpointed");
        }
        // Refuse a renamed path that no longer names this exact opened generation.
        let named = open(&self.inflight, private_name, OFlags::RDONLY, BENEATH)?;
        require(
            &named,
            journal
                .claimed
                .as_ref()
                .ok_or(Error::State("claim observation absent"))?,
        )?;
        Ok(Recovery::Claimed(ClaimedFile { journal, file }))
    }

    /// Retire, on an administrator's word, a claim that can never complete: claimed, its
    /// private source missing, and never published. While the private source exists the
    /// claim is recoverable, so this refuses; the store refuses a journal whose publication
    /// is recorded or whose source never moved. The source path is never touched.
    pub fn retire(
        &self,
        id: &str,
        caller: &str,
        reason: &str,
        now: u64,
    ) -> Result<RetiredConsumption, Error> {
        let journal = match self.store.consumption_get(id) {
            Ok(journal) => journal,
            Err(error) if error.code == "NOT_FOUND" => {
                if self
                    .store
                    .consumption_retired()?
                    .iter()
                    .any(|retired| retired.journal.import_id == id)
                {
                    return Err(Error::State("the claim is already retired unrecoverable"));
                }
                return Err(error.into());
            }
            Err(error) => return Err(error.into()),
        };
        self.scope(&journal)?;
        self.private_namespace()?;
        match open(
            &self.inflight,
            Path::new(&journal.import_id),
            OFlags::RDONLY,
            BENEATH,
        ) {
            Ok(_) => {
                return Err(Error::State(
                    "the claimed source still exists in the private claim directory; recover it, never retire it",
                ));
            }
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        Ok(self
            .store
            .consumption_retire(id, &self.watch, caller, reason, now)?)
    }

    /// Publish a claimed file and return the artifact id that proves it: the import's
    /// artifact on the single path, the manifest's for a part set (#280).
    ///
    /// What the claim has recorded decides its path before any size rule: a claim whose set
    /// manifest is accepted records it, a recorded set returns its manifest, a recorded claim
    /// with no proof left fails without uploading, and a recorded single-path claim takes the
    /// single path. Only a claim with no recorded state is measured against the import bound,
    /// held when it cannot be split, and planned.
    pub async fn publish(
        &self,
        client: &Client,
        claim: &mut ClaimedFile,
        attempt: &ClaimAttempt,
    ) -> Result<String, Error> {
        self.validate_claim(claim)?;
        if claim.journal.tail.is_some() {
            return Err(Error::State(
                "completed tail claims must acknowledge their original checkpoint without uploading",
            ));
        }
        if client.discovery().store_id != self.store.store_id()? {
            return Err(Error::State(
                "producer connection belongs to a different store",
            ));
        }
        let id = claim.journal.import_id.clone();
        match self.store.consumption_set_manifest(&id)? {
            // Parts are never sent again once the manifest is accepted.
            SetManifest::Receipt(manifest) => {
                claim.journal = self.store.consumption_publish_set(&id)?;
                #[cfg(test)]
                sparrow_test_support::crash::checkpoint("set_recorded");
                return Ok(manifest.id);
            }
            // A recorded set is not planned again.
            SetManifest::Recorded(manifest) => return Ok(manifest.id),
            SetManifest::Absent => {}
        }
        if self.store.consumption_publication_lost(&id)? {
            return Err(Error::Storage(lost_publication(&claim.journal)));
        }
        let format = sparrow_client::parse_format(&claim.journal.format)
            .map_err(sparrow_client::Error::from)?;
        // A recorded single-path claim returns its saved receipt; it is never measured.
        if claim.journal.published_artifact.is_some() {
            return self
                .publish_single(client, claim, attempt, format, None)
                .await;
        }
        let length = claim.journal.observed.byte_length;
        let packet = sparrow_core::SYSLOG_MESSAGE_BYTES as u64;
        let limits = PartLimits::from_discovery(client.discovery(), packet);
        // What the daemon recorded decides the path before any size rule: a claim with any
        // kept part receipt, a part draft or a part row is never published as one artifact,
        // whatever its limits now are. Only a clean absence at index 0 is measured below.
        match sparrow_client::parts::recorded_prefix(client, &claim.journal.request_id).await? {
            Frontier::Pending(error) => return Err(error.into()),
            Frontier::Recorded(resume) => {
                return self
                    .publish_set(
                        client,
                        claim,
                        attempt,
                        format,
                        limits.as_ref().map_err(|missing| *missing),
                        &resume,
                    )
                    .await;
            }
            Frontier::Measure => {}
        }
        let limits = match limits {
            Ok(limits) => limits,
            Err(missing) => {
                let bound = client.discovery().max_import_bytes;
                if bound != 0 && length > bound {
                    return Err(Error::Storage(sparrow_core::Error::new(
                        "RESOURCE_LIMIT",
                        format!(
                            "declared input of {length} bytes exceeds the daemon's import bound of {bound} bytes and the daemon did not advertise {}; the file cannot be split; the claim is pending until the daemon advertises it, which is the daemon's configuration; the claim continues by itself when it does",
                            missing.names()
                        ),
                    )));
                }
                return self
                    .publish_single(client, claim, attempt, format, Some(missing))
                    .await;
            }
        };
        if sparrow_client::parts::is_one_part(&claim.file, length, format, &limits)? {
            return self
                .publish_single(client, claim, attempt, format, None)
                .await;
        }
        self.publish_set(
            client,
            claim,
            attempt,
            format,
            Ok(&limits),
            &Resume::fresh(),
        )
        .await
    }

    async fn publish_single(
        &self,
        client: &Client,
        claim: &mut ClaimedFile,
        attempt: &ClaimAttempt,
        format: sparrow_client::Format,
        missing: Option<MissingLimits>,
    ) -> Result<String, Error> {
        let receipt = client
            .import_open_file_with_progress(
                &claim.journal.request_id,
                &claim.journal.source,
                format,
                &mut claim.file,
                Some(Arc::clone(&attempt.progress)),
            )
            .await
            .map_err(|error| match missing {
                Some(missing) => name_missing_limits(error, missing),
                None => error,
            })?;
        let saved = self.store.consumption_get(&claim.journal.import_id)?;
        if saved.published_artifact.as_deref() != Some(&receipt.id) {
            return Err(Error::State(
                "typed publication receipt contradicts source ownership",
            ));
        }
        claim.journal = saved;
        #[cfg(test)]
        sparrow_test_support::crash::checkpoint("claim_published");
        Ok(receipt.id)
    }

    async fn publish_set(
        &self,
        client: &Client,
        claim: &mut ClaimedFile,
        attempt: &ClaimAttempt,
        format: sparrow_client::Format,
        limits: Result<&PartLimits, MissingLimits>,
        resume: &Resume,
    ) -> Result<String, Error> {
        let journal = &claim.journal;
        let parts = sparrow_client::parts::upload_parts(
            client,
            &journal.request_id,
            &journal.source,
            format,
            &claim.file,
            limits,
            &attempt.parts,
            &attempt.progress,
            resume,
        )
        .await?;
        #[cfg(test)]
        sparrow_test_support::crash::checkpoint("set_parts_published");
        let manifest = sparrow_client::parts::ingest_manifest(
            client,
            &journal.request_id,
            &journal.source,
            format,
            journal.observed.byte_length,
            &parts,
        )
        .await?;
        #[cfg(test)]
        sparrow_test_support::crash::checkpoint("set_manifest_published");
        let saved = self
            .store
            .consumption_publish_set(&claim.journal.import_id)?;
        if saved.published_artifact.as_deref() != Some(&manifest.id) {
            return Err(Error::State(
                "part set manifest contradicts source ownership",
            ));
        }
        claim.journal = saved;
        #[cfg(test)]
        sparrow_test_support::crash::checkpoint("set_recorded");
        Ok(manifest.id)
    }

    fn validate_claim(&self, claim: &ClaimedFile) -> Result<(), Error> {
        self.namespace(&claim.journal)?;
        require(
            &claim.file,
            claim
                .journal
                .claimed
                .as_ref()
                .ok_or(Error::State("claim observation absent"))?,
        )
    }

    /// Acknowledge the sealed terminal checkpoint, using the same exact claim
    /// validation and deletion recovery as completed-file uploads.
    pub fn publish_tail(&self, claim: &mut ClaimedFile) -> Result<ConsumptionJournal, Error> {
        self.validate_claim(claim)?;
        let saved = self.store.consumption_publish_tail(
            &claim.journal.import_id,
            claim
                .journal
                .claimed
                .as_ref()
                .ok_or(Error::State("claim observation absent"))?,
        )?;
        claim.journal = saved.clone();
        Ok(saved)
    }

    /// The artifact id that proves `journal`'s publication, read in recorded-state order:
    /// the recorded id, then the tail, then the set manifest, then the single-path receipt.
    fn publication(&self, journal: &ConsumptionJournal, now: u64) -> Result<String, Error> {
        let original = journal.published_id().ok_or(Error::State(
            "source has no durable publication; unlink refused",
        ))?;
        if journal.tail.is_some() {
            return Ok(original.to_owned());
        }
        match self.store.consumption_set_manifest(&journal.import_id)? {
            SetManifest::Recorded(manifest) if manifest.id == original => {
                return Ok(original.to_owned());
            }
            SetManifest::Recorded(_) | SetManifest::Receipt(_) => {
                return Err(Error::State(
                    "publication proof contradicts the source journal",
                ));
            }
            SetManifest::Absent => {}
        }
        let receipt = self.store.import_publish(&journal.import_id, now)?;
        if receipt.id != original {
            return Err(Error::State(
                "publication proof contradicts the source journal",
            ));
        }
        Ok(original.to_owned())
    }

    /// Remove only the private exact claimed generation after verifying its
    /// original durable publication. Failed unlink/sync retains recovery ownership.
    pub fn remove(&self, claim: &ClaimedFile, now: u64) -> Result<ConsumptionJournal, Error> {
        let journal = self.store.consumption_get(&claim.journal.import_id)?;
        self.scope(&journal)?;
        if journal.removed_at.is_some() {
            return Ok(journal);
        }
        self.namespace(&journal)?;
        let artifact = self.publication(&journal, now)?;
        let generation = journal
            .claimed
            .as_ref()
            .ok_or(Error::State("claim observation absent"))?;
        require(&claim.file, generation)?;
        let named = open(
            &self.inflight,
            Path::new(&journal.import_id),
            OFlags::RDONLY,
            BENEATH,
        )?;
        require(&named, generation)?;
        #[cfg(test)]
        sparrow_test_support::crash::checkpoint("remove_before_unlink");
        fs::unlinkat(&self.inflight, journal.import_id.as_str(), AtFlags::empty())?;
        #[cfg(test)]
        sparrow_test_support::crash::checkpoint("remove_after_unlink");
        self.inflight.sync_all()?;
        #[cfg(test)]
        sparrow_test_support::crash::checkpoint("remove_directory_synced");
        let removed =
            self.store
                .consumption_removed(&journal.import_id, &artifact, generation, now)?;
        #[cfg(test)]
        sparrow_test_support::crash::checkpoint("remove_checkpointed");
        Ok(removed)
    }
}

#[cfg(test)]
mod lock_tests {
    use super::*;

    #[test]
    fn ownership_release_is_independent_of_inherited_descriptor_lifetime() {
        let path = tempfile::NamedTempFile::new().unwrap();
        let owner = ClaimLock::acquire(path.reopen().unwrap()).unwrap();
        let inherited = owner.0.try_clone().unwrap();
        assert!(ClaimLock::acquire(path.reopen().unwrap()).is_err());
        drop(owner);
        let replacement = ClaimLock::acquire(path.reopen().unwrap()).unwrap();
        assert!(ClaimLock::acquire(path.reopen().unwrap()).is_err());
        drop(inherited);
        assert!(ClaimLock::acquire(path.reopen().unwrap()).is_err());
        drop(replacement);
        assert!(ClaimLock::acquire(path.reopen().unwrap()).is_ok());
    }
}
