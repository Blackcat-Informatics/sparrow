// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use super::RangeReceipt;
use crate::{Error, IMPORT_CHUNK_BYTES, parts::CutSource};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

const BLOCK: u64 = IMPORT_CHUNK_BYTES as u64;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct State {
    pub(super) id: String,
    pub(super) source: String,
    pub(super) format: String,
    pub(super) staged: u64,
    pub(super) boundary: u64,
    pub(super) confirmed: u64,
    pub(super) count: u32,
    pub(super) page_start: u32,
    pub(super) page_offset: u64,
    pub(super) previous: Option<String>,
    pub(super) eof: bool,
    pub(super) file: Option<crate::CompletedFileIdentity>,
    pub(super) store: Option<String>,
    pub(super) pending: Option<Pending>,
    pub(super) completed: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pending {
    pub(super) length: u64,
    pub(super) format: String,
    pub(super) digest: String,
    pub(super) page_end: Option<u32>,
    pub(super) final_page: bool,
    pub(super) import: Option<String>,
}

pub(super) fn read_meta<T: serde::de::DeserializeOwned>(path: &Path) -> io::Result<T> {
    let mut bytes = String::new();
    File::open(path)?
        .take(64 * 1024 + 1)
        .read_to_string(&mut bytes)?;
    if bytes.len() > 64 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bulk journal entry exceeds 64 KiB",
        ));
    }

    toml::from_str(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn write_meta<T: Serialize>(dir: &Path, name: &str, value: &T) -> io::Result<()> {
    let bytes = toml::to_string(value).map_err(io::Error::other)?;
    let scratch = dir.join("transaction.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&scratch)?;
    file.write_all(bytes.as_bytes())?;
    file.sync_all()?;
    fs::rename(scratch, dir.join(name))?;
    File::open(dir)?.sync_all()
}

pub(super) struct Journal {
    pub(super) dir: PathBuf,
    pub(super) state: State,
    _lock: File,
    pub(super) faulted: bool,
}

impl Journal {
    pub(super) fn open(root: &Path, initial: State) -> io::Result<Self> {
        let key = purrdf_hash::blake3::hash(initial.id.as_bytes());
        let dir = root.join(purrdf_hash::hex::encode(key.as_bytes()));
        crate::quarantine::create_dir(&dir)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(dir.join("owner"))?;
        lock.try_lock().map_err(io::Error::from)?;
        let state = match read_meta::<State>(&dir.join("state.toml")) {
            Ok(state) => {
                if state.id != initial.id
                    || state.format != initial.format
                    || state.source != initial.source
                    || state.confirmed > state.staged
                    || state.boundary > state.staged
                    || state.page_start > state.count
                    || state.page_offset > state.confirmed
                    || (state.page_start != 0) != state.previous.is_some()
                    || state.completed.is_some()
                        && (!state.eof
                            || state.confirmed != state.staged
                            || state.pending.is_some())
                    || state.pending.as_ref().is_some_and(|p| {
                        p.page_end.is_none() && p.length > state.staged - state.confirmed
                    })
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "bulk job identity or durable offsets disagree",
                    ));
                }
                state
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                for entry in fs::read_dir(&dir)? {
                    let name = entry?.file_name();
                    if name != "owner" && name != "transaction.tmp" {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "bulk journal is missing its state while retained data remains",
                        ));
                    }
                }
                write_meta(&dir, "state.toml", &initial)?;
                initial
            }
            Err(e) => return Err(e),
        };
        let journal = Self {
            dir,
            state,
            _lock: lock,
            faulted: false,
        };
        journal.recover_space()?;
        Ok(journal)
    }

    fn recover_space(&self) -> io::Result<()> {
        // A receipt can survive a crash before its input blocks were unlinked.
        for item in fs::read_dir(&self.dir)? {
            let item = item?;
            let name = item.file_name();
            let block = name
                .to_str()
                .and_then(|n| n.strip_prefix("data-"))
                .and_then(|n| n.parse::<u64>().ok());
            let obsolete = name
                .to_str()
                .and_then(|n| n.strip_prefix("receipt-"))
                .and_then(|n| n.strip_suffix(".toml"))
                .and_then(|n| n.parse::<u32>().ok())
                .is_some_and(|ordinal| ordinal < self.state.page_start);
            if block.is_some_and(|n| self.reclaimable(n)) || obsolete {
                fs::remove_file(item.path())?;
            }
        }
        Ok(())
    }

    pub(super) fn complete(&mut self, artifact: String) -> io::Result<()> {
        self.change(|s| {
            s.completed = Some(artifact);
            s.pending = None;
        })?;
        self.recover_space()
    }

    pub(super) fn window(&mut self, artifact: String, end: u32, offset: u64) -> io::Result<()> {
        let first = self.state.page_start;
        self.change(|s| {
            s.previous = Some(artifact);
            s.page_start = end;
            s.page_offset = offset;
            s.pending = None;
        })?;
        for index in first..end {
            fs::remove_file(self.dir.join(receipt_name(index)))?;
        }
        Ok(())
    }

    pub(super) fn change(&mut self, edit: impl FnOnce(&mut State)) -> io::Result<()> {
        let mut state = self.state.clone();
        edit(&mut state);
        if let Err(error) = write_meta(&self.dir, "state.toml", &state) {
            // A failed directory sync can follow a successful rename. Reopen to establish
            // which transaction is present before permitting another append or RPC.
            self.faulted = true;
            return Err(error);
        }
        self.state = state;
        Ok(())
    }

    pub(super) fn append(&mut self, data: &[u8]) -> Result<(), Error> {
        let start = self.state.staged;
        let end = start
            .checked_add(data.len() as u64)
            .ok_or(Error::InvalidArgument("bulk offset exhausted"))?;
        self.blocks().write_at(data, start)?;
        let boundary = data
            .iter()
            .rposition(|b| *b == b'\n')
            .map(|n| start + n as u64 + 1);
        self.change(|s| {
            s.staged = end;
            if let Some(boundary) = boundary {
                s.boundary = boundary;
            }
        })?;
        Ok(())
    }

    pub(super) fn blocks(&self) -> Blocks {
        Blocks(self.dir.clone())
    }

    fn reclaimable(&self, block: u64) -> bool {
        // The planner needs the byte preceding its next position to recognize line boundaries.
        self.state.completed.is_some() || block < self.state.confirmed.saturating_sub(1) / BLOCK
    }

    pub(super) fn certify(&mut self, receipt: &RangeReceipt) -> Result<(), Error> {
        write_meta(&self.dir, &receipt_name(receipt.index), receipt)?;
        let old = self.state.confirmed;
        let next = self
            .state
            .count
            .checked_add(1)
            .ok_or(Error::InvalidArgument("bulk range ordinals exhausted"))?;
        self.change(|s| {
            s.confirmed = receipt.end;
            s.count = next;
            s.pending = None;
        })?;
        // Start one block earlier because the previous range retained its boundary byte.
        for n in old.saturating_sub(1) / BLOCK..=receipt.end / BLOCK {
            if self.reclaimable(n) {
                match fs::remove_file(self.blocks().path(n)) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(())
    }
}

pub(super) fn receipt_name(index: u32) -> String {
    format!("receipt-{index:010}.toml")
}

#[derive(Clone)]
pub(super) struct Blocks(PathBuf);
impl Blocks {
    fn path(&self, n: u64) -> PathBuf {
        self.0.join(format!("data-{n:020}"))
    }
    fn write_at(&self, mut data: &[u8], mut offset: u64) -> io::Result<()> {
        while !data.is_empty() {
            let at = offset % BLOCK;
            let take = data.len().min((BLOCK - at) as usize);
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .open(self.path(offset / BLOCK))?;
            file.write_all_at(&data[..take], at)?;
            file.sync_data()?;
            offset += take as u64;
            data = &data[take..];
        }
        File::open(&self.0)?.sync_all()
    }
}
impl CutSource for Blocks {
    fn read_at(&self, mut bytes: &mut [u8], mut offset: u64) -> Result<(), Error> {
        while !bytes.is_empty() {
            let at = offset % BLOCK;
            let take = bytes.len().min((BLOCK - at) as usize);
            let (head, tail) = bytes.split_at_mut(take);
            File::open(self.path(offset / BLOCK))?.read_exact_at(head, at)?;
            offset += take as u64;
            bytes = tail;
        }
        Ok(())
    }
}
