// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Bounded descriptor-held active file capture through the one durable record pipeline.

use crate::{ConsumeWatch, consumption::Error, source_file};
use rustix::fs::OFlags;
use sparrow_client::CompletedFileIdentity;
use sparrow_core::{Store, TailFinishIntent, TailGeneration, TailStart};
use std::{
    collections::BTreeMap,
    fs::File,
    os::unix::{ffi::OsStrExt, fs::FileExt},
    path::Path,
};

const READ_BYTES: usize = 1024 * 1024;

struct Held {
    file: File,
    generation: TailGeneration,
    observed: CompletedFileIdentity,
}

pub(crate) struct TailRoot {
    store: Store,
    watch: String,
    root: File,
    claim: std::cell::OnceCell<crate::consumption::ClaimRoot>,
    held: BTreeMap<(u64, u64), Held>,
    read_bytes: usize,
    next: Option<(u64, u64)>,
}

impl TailRoot {
    pub(crate) fn open(store: Store, config: &ConsumeWatch, revision: u64) -> Result<Self, Error> {
        let root = source_file::root(&store, &config.root)?;
        let identity = CompletedFileIdentity::capture(&root.metadata()?);
        for generation in store.tail_generations(&config.id)? {
            if generation.complete || generation.gap.is_some() {
                continue;
            }
            let declared = generation.declaration;
            if declared.directories.root_device != identity.device
                || declared.directories.root_inode != identity.inode
                || declared.watch_revision != revision
                || declared.source != config.source
                || declared.format != config.format
            {
                return Err(Error::State(
                    "active file checkpoints require their original watch revision, root and interpretation; restore that definition before recovery",
                ));
            }
        }
        let limit = store
            .properties()?
            .get("max_artifact_bytes")
            .and_then(|value| value.as_str())
            .ok_or(Error::State("storage read limit missing"))?
            .parse::<usize>()
            .map_err(|_| Error::State("storage read limit invalid"))?;
        Ok(Self {
            store,
            watch: config.id.clone(),
            root,
            claim: std::cell::OnceCell::new(),
            held: BTreeMap::new(),
            read_bytes: READ_BYTES.min(limit),
            next: None,
        })
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
            self.read_bytes as u64,
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
        let held = self.held.get_mut(&(intent.observed.device, intent.observed.inode))
            .ok_or(Error::State("completion requires the original held generation; wait for reconciliation or inspect its durable gap"))?;
        if held.generation.id != intent.generation_id {
            return Err(Error::State(
                "completion observation belongs to another held generation",
            ));
        }
        crate::consumption::identity(&intent.observed).require(&held.file)?;
        held.generation = self.store.tail_finish_start(intent, crate::now()?)?;
        Ok(held.generation.clone())
    }

    pub(crate) fn attach(
        &mut self,
        path: &Path,
        config: &ConsumeWatch,
        revision: u64,
    ) -> Result<(), Error> {
        let Some(expected) = self.observe(path)? else {
            return Ok(());
        };
        let key = (expected.device, expected.inode);
        if self.held.contains_key(&key) {
            return Ok(());
        }
        let generations = self.store.tail_generations(&config.id)?;
        for row in &generations {
            if row.declaration.watch_revision != revision
                || (row.declaration.device, row.declaration.inode) != key
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
        if (observed.device, observed.inode) != key {
            return Err(Error::State(
                "tail source changed during descriptor admission; reconcile before retry",
            ));
        }
        let generation = generations.into_iter().find(|row| {
            !row.complete
                && row.gap.is_none()
                && (row.declaration.device, row.declaration.inode) == key
        });
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
        self.held.insert(
            key,
            Held {
                file,
                generation,
                observed,
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
        let held = self
            .held
            .get_mut(&key)
            .ok_or(Error::State("tail descriptor selection lost"))?;
        let before = CompletedFileIdentity::capture(&held.file.metadata()?);
        if (before.device, before.inode) != key {
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
            self.held.remove(&key);
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
        let length = self
            .read_bytes
            .min(usize::try_from(available).unwrap_or(usize::MAX));
        if length == 0 {
            if final_identity.is_some() {
                self.store.tail_append(
                    &held.generation.id,
                    held.generation.offset,
                    &[],
                    true,
                    crate::now()?,
                )?;
                self.held.remove(&key);
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
            self.held.remove(&key);
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
            self.held.remove(&key);
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
        held.generation.offset = publication.source_end;
        held.observed = after;
        if complete {
            self.held.remove(&key);
            return Ok(());
        }
        if publication.artifact.is_none() && bytes.len() == self.read_bytes {
            return Err(Error::State(
                "unfinished tail record exceeds the bounded capture window; source and checkpoint remain unchanged",
            ));
        }
        Ok(())
    }

    pub(crate) fn reconcile_missing(&mut self, watch: &str) -> Result<(), Error> {
        for generation in self.store.tail_generations(watch)? {
            if !generation.complete
                && generation.gap.is_none()
                && !self
                    .held
                    .contains_key(&(generation.declaration.device, generation.declaration.inode))
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
