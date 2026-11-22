// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Bounded descriptor-held active file capture through the one durable record pipeline.

use crate::{ConsumeWatch, consumption::Error, source_file};
use rustix::fs::{
    OFlags,
    inotify::{self, CreateFlags, WatchFlags},
};
use sparrow_client::CompletedFileIdentity;
use sparrow_core::{Store, TailFinishIntent, TailGeneration, TailStart};
use std::{
    collections::BTreeMap,
    fs::File,
    os::{
        fd::{AsRawFd, BorrowedFd, OwnedFd},
        unix::{ffi::OsStrExt, fs::FileExt},
    },
    path::Path,
};

/// One tail capture; a line longer than this fails visibly and keeps its checkpoint.
const READ_BYTES: usize = 1024 * 1024;

struct Held {
    file: File,
    generation: TailGeneration,
    observed: CompletedFileIdentity,
    /// This descriptor's watch in `TailRoot::appends`.
    watch: i32,
}

pub(crate) struct TailRoot {
    store: Store,
    watch: String,
    root: File,
    binding: crate::consumption::RootBinding,
    claim: std::cell::OnceCell<crate::consumption::ClaimRoot>,
    held: BTreeMap<u64, Held>,
    next: Option<u64>,
    /// One inotify watch per held descriptor, on the file itself: an append to a generation
    /// whose name has left the watched directories (unlinked, or moved out of the root)
    /// raises no directory event, but still raises this one.
    appends: OwnedFd,
    /// Consecutive drains that captured nothing. Drains rotate through the held
    /// descriptors, so once this reaches their count every one is caught up.
    idle: usize,
}

impl TailRoot {
    pub(crate) fn open(
        store: Store,
        config: &ConsumeWatch,
        revision: u64,
        binding: crate::consumption::RootBinding,
    ) -> Result<Self, Error> {
        let root = source_file::root(&store, &config.root)?;
        let identity = CompletedFileIdentity::capture(&root.metadata()?);
        for generation in store.tail_generations(&config.id)? {
            if generation.complete || generation.gap.is_some() {
                continue;
            }
            let declared = generation.declaration;
            // A private root never holds the original root's active generations.
            if foreign(binding, identity.inode, &declared) {
                continue;
            }
            if !declaration_matches(&declared, identity.inode, config, revision) {
                return Err(Error::State(
                    "active file checkpoints require their original watch revision, root and interpretation; restore that definition before recovery",
                ));
            }
        }
        Ok(Self {
            store,
            watch: config.id.clone(),
            root,
            binding,
            claim: std::cell::OnceCell::new(),
            held: BTreeMap::new(),
            next: None,
            appends: inotify::init(CreateFlags::CLOEXEC | CreateFlags::NONBLOCK)?,
            idle: 0,
        })
    }

    /// Whether every held descriptor has been drained since its last change, so no drain
    /// is due until an event reports new activity.
    pub(crate) fn caught_up(&self) -> bool {
        self.idle >= self.held.len()
    }

    /// The descriptor that becomes readable when a held file changes.
    pub(crate) fn appends(&self) -> BorrowedFd<'_> {
        use std::os::fd::AsFd;
        self.appends.as_fd()
    }

    /// Consume queued held-file events; any of them makes every held descriptor due again.
    pub(crate) fn take_appends(&mut self) -> Result<(), Error> {
        let mut buffer = [0_u8; 4096];
        // Bounded: events still queued keep the descriptor readable for the next wait.
        for _ in 0..64 {
            match rustix::io::read(&self.appends, &mut buffer) {
                Ok(_) => self.idle = 0,
                Err(error) if error == rustix::io::Errno::WOULDBLOCK => break,
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    /// Stop holding one descriptor and drop its file watch. The kernel has already dropped
    /// the watch of a file that no longer exists, which `EINVAL` reports.
    fn release(&mut self, key: u64) -> Result<(), Error> {
        let Some(held) = self.held.remove(&key) else {
            return Ok(());
        };
        match inotify::remove_watch(&self.appends, held.watch) {
            Ok(()) => Ok(()),
            Err(error) if error == rustix::io::Errno::INVAL => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// The held root's inode, which every generation it may read declares.
    fn root_inode(&self) -> Result<u64, Error> {
        Ok(CompletedFileIdentity::capture(&self.root.metadata()?).inode)
    }

    pub(crate) fn status(&self, watch: &str) -> Result<(Vec<String>, u64, u32, u64), Error> {
        let generations = self.store.tail_generations(watch)?;
        let records = generations.iter().try_fold(0_u64, |total, row| {
            total
                .checked_add(row.records)
                .ok_or(Error::State("tail checkpoint record total overflow"))
        })?;
        Ok((
            generations.into_iter().map(|row| row.id).collect(),
            READ_BYTES as u64,
            self.held.len() as u32,
            records,
        ))
    }

    pub(crate) fn directory(&self, relative: &Path) -> Result<File, Error> {
        source_file::directory(&self.root, relative)
    }

    pub(crate) fn claim(&self) -> Result<&crate::consumption::ClaimRoot, Error> {
        if let Some(claim) = self.claim.get() {
            return Ok(claim);
        }
        let claim = crate::consumption::ClaimRoot::from_root(
            self.store.clone(),
            &self.watch,
            self.root.try_clone()?,
            self.binding,
        )?;
        self.claim
            .set(claim)
            .map_err(|_| Error::State("tail claim owner initialized twice"))?;
        self.claim
            .get()
            .ok_or(Error::State("tail claim owner initialization missing"))
    }
    pub(crate) fn root_binding(&self, path: &Path) -> Result<(), Error> {
        source_file::root_binding(&self.root, path)
    }
    pub(crate) fn observe(&self, relative: &Path) -> Result<Option<CompletedFileIdentity>, Error> {
        source_file::observe(&self.root, relative)
    }

    pub(crate) fn finish(&mut self, intent: &TailFinishIntent) -> Result<TailGeneration, Error> {
        let held = self.held.get_mut(&intent.observed.inode)
            .ok_or(Error::State("completion requires the original held generation; wait for reconciliation or inspect its durable gap"))?;
        if held.generation.id != intent.generation_id {
            return Err(Error::State(
                "completion observation belongs to another held generation",
            ));
        }
        crate::consumption::identity(&intent.observed).require(&held.file)?;
        held.generation = self.store.tail_finish_start(intent, crate::now()?)?;
        // The finished generation's final drain is due now.
        self.idle = 0;
        Ok(held.generation.clone())
    }

    pub(crate) fn attach(
        &mut self,
        path: &Path,
        config: &ConsumeWatch,
        revision: u64,
    ) -> Result<(), Error> {
        // Activity on a path makes every held descriptor due for a drain.
        self.idle = 0;
        let Some(expected) = self.observe(path)? else {
            return Ok(());
        };
        let key = expected.inode;
        if self.held.contains_key(&key) {
            return Ok(());
        }
        // A private root reads only its own generations: a generation of the original root
        // whose file inode a file below the private root reuses is never adopted.
        let held_root = self.root_inode()?;
        let generations = self
            .store
            .tail_generations(&config.id)?
            .into_iter()
            .filter(|row| !foreign(self.binding, held_root, &row.declaration))
            .collect::<Vec<_>>();
        for row in &generations {
            if row.declaration.watch_revision != revision
                || row.declaration.inode != key
                || (!row.complete && row.gap.is_none())
            {
                continue;
            }
            if row.complete
                && row.finish.as_ref().is_some_and(|finish| {
                    crate::consumption::identity(&finish.intent.observed) == expected
                })
            {
                return Ok(());
            }
            if row.complete || row.finish.is_some() {
                return Err(Error::FinishedGeneration(
                    "terminal declared generation cannot be reimported under the same watch revision; inspect completion and establish new producer authority",
                ));
            }
        }
        if self.held.len()
            >= config
                .max_candidates
                .min(sparrow_core::MAX_TAIL_GENERATIONS)
        {
            return Err(Error::State(
                "tail descriptor quota reached; original files remain unconsumed",
            ));
        }
        let parent = source_file::parent(&self.root, path)?;
        let name = path
            .file_name()
            .ok_or(Error::State("tail requires a relative file name"))?;
        let file = source_file::open(
            &parent,
            Path::new(name),
            OFlags::RDONLY,
            source_file::BENEATH,
        )?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(Error::State("tail source must be a regular file"));
        }
        let observed = CompletedFileIdentity::capture(&metadata);
        // Both sides are live observations of this boot. The path was re-resolved after
        // `observe`, and a nested btrfs subvolume has its own device and inode numbering, so
        // the device keeps a file from another one off this inode's checkpoint.
        if (observed.device, observed.inode) != (expected.device, key) {
            return Err(Error::State(
                "tail source changed during descriptor admission; reconcile before retry",
            ));
        }
        let generation = generations
            .into_iter()
            .find(|row| !row.complete && row.gap.is_none() && row.declaration.inode == key);
        let generation = match generation {
            Some(generation) => generation,
            None => self.store.tail_start(
                &TailStart {
                    request_id: TailStart::fresh_request_id()?,
                    watch: config.id.clone(),
                    watch_revision: revision,
                    directories: source_file::directories(&self.root, &parent)?,
                    path: path.as_os_str().as_bytes().to_vec(),
                    source: config.source.clone(),
                    format: config.format.clone(),
                    device: observed.device,
                    inode: observed.inode,
                },
                crate::now()?,
            )?,
        };
        if let Some(finish) = &generation.finish
            && crate::consumption::identity(&finish.intent.observed) != observed
        {
            self.store.tail_discontinue(
                &generation.id,
                generation.offset,
                "identity_changed",
                crate::now()?,
            )?;
            return Err(Error::FinishedGeneration(
                "pending completion no longer matches its original final descriptor observation; accepted checkpoint preserved",
            ));
        }
        // inotify has no descriptor API; the kernel-owned alias names this exact file.
        let watch = inotify::add_watch(
            &self.appends,
            format!("/proc/self/fd/{}", file.as_raw_fd()).as_str(),
            WatchFlags::MODIFY | WatchFlags::ATTRIB,
        )?;
        self.held.insert(
            key,
            Held {
                file,
                generation,
                observed,
                watch,
            },
        );
        Ok(())
    }

    pub(crate) fn drain(&mut self) -> Result<(), Error> {
        let selected = self
            .held
            .keys()
            .copied()
            .find(|key| self.next.is_none_or(|next| *key > next))
            .or_else(|| self.held.keys().next().copied());
        let Some(key) = selected else {
            return Ok(());
        };
        self.next = Some(key);
        // Reset below when the checkpoint advances or a generation completes; a failure, an
        // empty read or a partial record leaves nothing to capture before the next event.
        self.idle = self.idle.saturating_add(1);
        let held = self
            .held
            .get_mut(&key)
            .ok_or(Error::State("tail descriptor selection lost"))?;
        let before = CompletedFileIdentity::capture(&held.file.metadata()?);
        if before.inode != key {
            return Err(Error::State("held tail descriptor identity changed"));
        }
        let final_identity = held
            .generation
            .finish
            .as_ref()
            .map(|finish| crate::consumption::identity(&finish.intent.observed));
        if final_identity.is_some_and(|expected| before != expected) {
            self.store.tail_discontinue(
                &held.generation.id,
                held.generation.offset,
                "identity_changed",
                crate::now()?,
            )?;
            self.release(key)?;
            return Err(Error::State(
                "producer changed a declared finished generation; accepted checkpoint preserved and completion refused",
            ));
        }
        let discontinuity =
            if before.length < held.generation.offset || before.length < held.observed.length {
                Some("truncated")
            } else if before.length == held.observed.length
                && (before.modified_seconds, before.modified_nanos)
                    != (held.observed.modified_seconds, held.observed.modified_nanos)
            {
                Some("read_gap")
            } else {
                None
            };
        if let Some(reason) = discontinuity {
            held.generation = self.store.tail_discontinue(
                &held.generation.id,
                held.generation.offset,
                reason,
                crate::now()?,
            )?;
            let mut declaration = held.generation.declaration.clone();
            declaration.request_id = TailStart::fresh_request_id()?;
            held.generation = self.store.tail_start(&declaration, crate::now()?)?;
        }
        let available = before
            .length
            .checked_sub(held.generation.offset)
            .ok_or(Error::State("tail checkpoint exceeds source size"))?;
        let length = READ_BYTES.min(usize::try_from(available).unwrap_or(usize::MAX));
        if length == 0 {
            if final_identity.is_some() {
                self.store.tail_append(
                    &held.generation.id,
                    held.generation.offset,
                    &[],
                    true,
                    crate::now()?,
                )?;
                self.idle = 0;
                self.release(key)?;
                return Ok(());
            }
            held.observed = before;
            return Ok(());
        }
        let mut bytes = vec![0; length];
        let read = held.file.read_at(&mut bytes, held.generation.offset)?;
        bytes.truncate(read);
        let after = CompletedFileIdentity::capture(&held.file.metadata()?);
        if final_identity.is_some_and(|expected| after != expected) {
            self.store.tail_discontinue(
                &held.generation.id,
                held.generation.offset,
                "identity_changed",
                crate::now()?,
            )?;
            self.release(key)?;
            return Err(Error::State(
                "producer changed a declared finished generation during capture; no captured bytes were published",
            ));
        }
        if after.length < before.length
            || (after.length == before.length
                && (after.modified_seconds, after.modified_nanos)
                    != (before.modified_seconds, before.modified_nanos))
        {
            self.store.tail_discontinue(
                &held.generation.id,
                held.generation.offset,
                if after.length < before.length {
                    "truncated"
                } else {
                    "read_gap"
                },
                crate::now()?,
            )?;
            self.release(key)?;
            return Err(Error::State(
                "tail source changed without an append during capture; accepted checkpoint was preserved",
            ));
        }
        if bytes.is_empty() {
            return Err(Error::State(
                "tail source ended before its observed size; checkpoint unchanged",
            ));
        }
        let complete = final_identity.is_some() && bytes.len() as u64 == available;
        let publication = self.store.tail_append(
            &held.generation.id,
            held.generation.offset,
            &bytes,
            complete,
            crate::now()?,
        )?;
        if complete || publication.source_end != held.generation.offset {
            self.idle = 0;
        }
        held.generation.offset = publication.source_end;
        held.observed = after;
        if complete {
            self.release(key)?;
            return Ok(());
        }
        if publication.artifact.is_none() && bytes.len() == READ_BYTES {
            return Err(Error::State(
                "unfinished tail record exceeds the bounded capture window; source and checkpoint remain unchanged",
            ));
        }
        Ok(())
    }

    pub(crate) fn reconcile_missing(&mut self, watch: &str) -> Result<(), Error> {
        let held_root = self.root_inode()?;
        for generation in self.store.tail_generations(watch)? {
            // The original root's generations stay as the copy holds them: a private root
            // records no gap for a file it never held.
            if !generation.complete
                && generation.gap.is_none()
                && !foreign(self.binding, held_root, &generation.declaration)
                && !self.held.contains_key(&generation.declaration.inode)
            {
                self.store.tail_discontinue(
                    &generation.id,
                    generation.offset,
                    "read_gap",
                    crate::now()?,
                )?;
            }
        }
        Ok(())
    }
}

/// Whether a stored active generation's declaration names the watch as it is now: the root
/// inode, revision, source and format must all agree. The root's device number is stored but
/// never compared; a reboot may renumber it without changing the directory.
#[must_use]
fn declaration_matches(
    declared: &TailStart,
    root_inode: u64,
    config: &ConsumeWatch,
    revision: u64,
) -> bool {
    declared.directories.root_inode == root_inode
        && declared.watch_revision == revision
        && declared.source == config.source
        && declared.format == config.format
}

/// Whether a generation declared on another root than ROOT must be left alone: only under a
/// private root (`sparrowd --private-intake`), where the original root's generations are
/// never resumed, adopted or closed.
fn foreign(
    binding: crate::consumption::RootBinding,
    root_inode: u64,
    declared: &TailStart,
) -> bool {
    binding == crate::consumption::RootBinding::Private
        && declared.directories.root_inode != root_inode
}
