// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Anchored completed-file claims. Readiness and discovery belong to the watcher.

use crate::source_file::{BENEATH, open};
use rustix::fs::{self, AtFlags, FlockOperation, Mode, OFlags, RenameFlags};
use sparrow_client::{ArtifactReceipt, Client, CompletedFileIdentity};
use sparrow_core::{ConsumptionDirectories, ConsumptionGeneration, ConsumptionJournal, Store};
use std::{
    fs::File,
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::Path,
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
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(e) => std::fmt::Display::fmt(e, f),
            Self::Upload(e) => std::fmt::Display::fmt(e, f),
            Self::Io(e) => std::fmt::Display::fmt(e, f),
            Self::State(message) | Self::FinishedGeneration(message) => f.write_str(message),
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

#[cfg(test)]
mod crash_tests;

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
        if watch.is_empty() || watch.len() > 128 || !path.is_absolute() {
            return Err(Error::State(
                "require a bounded watch identity and absolute producer root",
            ));
        }
        let root = crate::source_file::root(&store, path)?;
        Self::from_root(store, watch, root)
    }

    /// Share an already admitted rooted descriptor with a tail owner; never reopen
    /// a pathname that could now identify a replacement directory.
    pub(crate) fn from_root(store: Store, watch: &str, root: File) -> Result<Self, Error> {
        let root_identity = CompletedFileIdentity::capture(&root.metadata()?);
        let pending = store
            .consumption_pending()?
            .into_iter()
            .filter(|journal| journal.watch == watch)
            .collect::<Vec<_>>();
        if pending.iter().any(|journal| {
            journal.directories.root_device != root_identity.device
                || journal.directories.root_inode != root_identity.inode
        }) {
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
        if root.device != journal.directories.root_device
            || root.inode != journal.directories.root_inode
        {
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
        if self.directories(&parent)? != journal.directories {
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
        if !file.metadata()?.is_file() {
            return Err(Error::State("completed source must be a regular file"));
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

    /// Upload through the production typed SDK's sole descriptor uploader.
    pub async fn publish(
        &self,
        client: &Client,
        claim: &mut ClaimedFile,
    ) -> Result<ArtifactReceipt, Error> {
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
        let receipt = client
            .import_open_file(
                &claim.journal.request_id,
                &claim.journal.source,
                sparrow_client::parse_format(&claim.journal.format)
                    .map_err(sparrow_client::Error::from)?,
                &mut claim.file,
            )
            .await?;
        let saved = self.store.consumption_get(&claim.journal.import_id)?;
        if saved.published_artifact.as_deref() != Some(&receipt.id) {
            return Err(Error::State(
                "typed publication receipt contradicts source ownership",
            ));
        }
        claim.journal = saved;
        #[cfg(test)]
        sparrow_test_support::crash::checkpoint("claim_published");
        Ok(receipt)
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

    fn publication(&self, journal: &ConsumptionJournal, now: u64) -> Result<String, Error> {
        let original = journal.published_id().ok_or(Error::State(
            "source has no durable publication; unlink refused",
        ))?;
        if journal.tail.is_some() {
            return Ok(original.to_owned());
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
