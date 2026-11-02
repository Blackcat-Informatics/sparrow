// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One-time background rewrite of table files written under an older options profile.
//!
//! New options apply only to new table files, and RocksDB never revisits a cold
//! bottommost file on its own. Each step forces one bounded key slice of one
//! family through a native manual compaction, outside the writer lock, then
//! durably advances a cursor. A whole-range compaction would hold every input
//! file until it finished, so the slice bounds transient space instead. A
//! slice starts only when pressure is ready, native compaction is idle, and its
//! input bytes fit both the data allowance and the free-space floor.

use super::{Error, FAMILIES, Store, Writer, table_options};
use rocksdb::{BottommostLevelCompaction, CompactOptions, DB, WriteBatch};
use std::sync::Arc;

/// Identity of the options profile whose rewrite is complete.
pub(super) const PROFILE_KEY: &str = "table-options-profile";
/// Durable rewrite position: profile, family, rewritten bytes and next key.
const CURSOR_KEY: &str = "table-options-rewrite";
const CURSOR_BYTES: usize = 64 * 1024;

/// Input bytes the daemon rewrites per step. A slice runs to completion, so
/// this also bounds how long shutdown waits for one.
pub const RECOMPRESSION_SLICE_BYTES: u64 = 256 * 1024 * 1024;

/// Progress of the one-time rewrite.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecompressionPhase {
    /// Files written under an older profile remain; the next step rewrites a slice.
    Running,
    /// A rewrite is owed but the last step yielded; `waiting` says why.
    Paused,
    /// Every family's files were rewritten under the current profile.
    Complete,
}

/// Durable rewrite position with this session's last gate and failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecompressionStatus {
    /// Identity of the running binary's table options.
    pub profile: &'static str,
    /// Current phase.
    pub phase: RecompressionPhase,
    /// Family being rewritten; absent once complete.
    pub family: Option<&'static str>,
    /// Durable total of input table bytes rewritten under this profile.
    pub compacted_bytes: u64,
    /// Why the last step yielded instead of rewriting a slice.
    pub waiting: Option<String>,
    /// Latched failure; cleared by the next rewritten slice.
    pub failure: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Cursor {
    family: u8,
    compacted_bytes: u64,
    next: Vec<u8>,
}

impl Cursor {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(table_options::PROFILE.len() + 10 + self.next.len());
        bytes.extend_from_slice(table_options::PROFILE.as_bytes());
        bytes.push(0);
        bytes.push(self.family);
        bytes.extend_from_slice(&self.compacted_bytes.to_be_bytes());
        bytes.extend_from_slice(&self.next);
        bytes
    }

    /// `None` when the cursor belongs to another profile, which restarts the rewrite.
    fn decode(bytes: &[u8]) -> Result<Option<Self>, Error> {
        let corrupt = || {
            Error::new(
                "STORAGE_FAILURE",
                "table options rewrite cursor is malformed; inspect or restore the catalog",
            )
        };
        if bytes.len() > CURSOR_BYTES {
            return Err(corrupt());
        }
        let split = bytes
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(corrupt)?;
        if &bytes[..split] != table_options::PROFILE.as_bytes() {
            return Ok(None);
        }
        let rest = &bytes[split + 1..];
        if rest.len() < 9 || usize::from(rest[0]) >= table_options::REWRITTEN.len() {
            return Err(corrupt());
        }
        let mut compacted = [0; 8];
        compacted.copy_from_slice(&rest[1..9]);
        Ok(Some(Self {
            family: rest[0],
            compacted_bytes: u64::from_be_bytes(compacted),
            next: rest[9..].to_vec(),
        }))
    }
}

/// The rewrite owed by this store, or `None` once complete.
#[derive(Default)]
pub(super) struct State {
    cursor: Option<Cursor>,
    waiting: Option<String>,
    failure: Option<String>,
}

impl State {
    pub(super) fn load(db: &DB) -> Result<Self, Error> {
        let profile = db.get_pinned(PROFILE_KEY)?;
        let cursor = db.get_pinned(CURSOR_KEY)?;
        if profile.as_deref() == Some(table_options::PROFILE.as_bytes()) {
            if cursor.is_some() {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "completed table options rewrite retains its cursor; inspect or restore the catalog",
                ));
            }
            return Ok(Self::default());
        }
        let cursor = match cursor {
            Some(bytes) => Cursor::decode(&bytes)?,
            None => None,
        };
        Ok(Self {
            cursor: Some(cursor.unwrap_or(Cursor {
                family: 0,
                compacted_bytes: 0,
                next: Vec::new(),
            })),
            ..Self::default()
        })
    }

    fn status(&self) -> RecompressionStatus {
        let (phase, family, compacted_bytes) = match &self.cursor {
            None => (RecompressionPhase::Complete, None, 0),
            Some(cursor) => (
                if self.waiting.is_some() {
                    RecompressionPhase::Paused
                } else {
                    RecompressionPhase::Running
                },
                Some(table_options::REWRITTEN[usize::from(cursor.family)]),
                cursor.compacted_bytes,
            ),
        };
        RecompressionStatus {
            profile: table_options::PROFILE,
            phase,
            family,
            compacted_bytes,
            waiting: self.waiting.clone(),
            failure: self.failure.clone(),
        }
    }
}

/// One planned slice, compacted without the writer lock.
struct Slice {
    db: Arc<DB>,
    family: &'static str,
    start: Vec<u8>,
    end: Vec<u8>,
    inputs: Vec<String>,
    bytes: u64,
}

/// One table file of a family with its key bounds.
struct Table {
    name: String,
    size: u64,
    start: Vec<u8>,
    end: Vec<u8>,
}

fn tables(db: &DB, family: &str) -> Result<Vec<Table>, Error> {
    let mut tables = Vec::new();
    for file in db.live_files()? {
        if file.column_family_name != family {
            continue;
        }
        let (Some(start), Some(end)) = (file.start_key, file.end_key) else {
            return Err(Error::new(
                "STORAGE_FAILURE",
                format!("live table {} has no key bounds", file.name),
            ));
        };
        tables.push(Table {
            name: file.name,
            size: file.size as u64,
            start,
            end,
        });
    }
    tables.sort_by(|left, right| left.start.cmp(&right.start));
    Ok(tables)
}

impl Writer {
    fn background_errors(&self, family: &str) -> Result<(), Error> {
        if self.native_property(family, "rocksdb.background-errors")? != 0 {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "RocksDB reports background errors; reopen and inspect storage before rewriting tables",
            ));
        }
        Ok(())
    }

    fn native_idle(&self) -> Result<bool, Error> {
        for family in FAMILIES {
            self.background_errors(family)?;
            if self.native_property(family, "rocksdb.compaction-pending")? != 0
                || self.native_property(family, "rocksdb.num-running-compactions")? != 0
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn save_cursor(&self, cursor: &Cursor) -> Result<(), Error> {
        let mut batch = WriteBatch::default();
        batch.put(CURSOR_KEY, cursor.encode());
        self.commit(batch)
    }

    /// Choose the next slice, or record why none may run now.
    fn plan_recompression(&self, slice_bytes: u64) -> Result<Option<Slice>, Error> {
        let mut state = self.recompression.borrow_mut();
        let Some(mut cursor) = state.cursor.clone() else {
            return Ok(None);
        };
        let pressure = self.pressure.borrow().status.clone();
        if pressure.phase != super::pressure::PressurePhase::Ready || pressure.requested_bytes != 0
        {
            state.waiting = Some("pressure maintenance has priority".into());
            return Ok(None);
        }
        if !self.native_idle()? {
            state.waiting = Some("native compaction has priority".into());
            return Ok(None);
        }
        loop {
            let family = table_options::REWRITTEN[usize::from(cursor.family)];
            let remaining = tables(&self.db, family)?
                .into_iter()
                .filter(|table| table.end >= cursor.next)
                .collect::<Vec<_>>();
            if remaining.is_empty() {
                cursor.family += 1;
                cursor.next.clear();
                if usize::from(cursor.family) == table_options::REWRITTEN.len() {
                    let mut batch = WriteBatch::default();
                    batch.put(PROFILE_KEY, table_options::PROFILE);
                    batch.delete(CURSOR_KEY);
                    self.commit(batch)?;
                    *state = State::default();
                    return Ok(None);
                }
                self.save_cursor(&cursor)?;
                state.cursor = Some(cursor.clone());
                continue;
            }
            // Take whole files in key order until the slice is full; at least one.
            let mut taken = 0_u64;
            let mut end = Vec::new();
            for table in &remaining {
                if taken >= slice_bytes {
                    break;
                }
                taken = taken.saturating_add(table.size);
                end = end.max(table.end.clone());
            }
            // Every file overlapping the range is an input and stays until it finishes.
            let inputs = remaining
                .iter()
                .filter(|table| table.start <= end)
                .collect::<Vec<_>>();
            let bytes = inputs
                .iter()
                .fold(0_u64, |sum, table| sum.saturating_add(table.size));
            let physical = self.physical_bytes()?;
            let allowance = self.config.disk_budget - self.config.maintenance_reserve;
            if physical.saturating_add(bytes) > allowance {
                state.waiting = Some(format!(
                    "a {bytes}-byte slice does not fit the data allowance: {physical} of {allowance} bytes in use"
                ));
                return Ok(None);
            }
            if let Some(shortfall) = self.space.shortfall_after(bytes)? {
                state.waiting = Some(shortfall);
                return Ok(None);
            }
            state.waiting = None;
            return Ok(Some(Slice {
                db: Arc::clone(&self.db),
                family,
                start: cursor.next.clone(),
                end,
                inputs: inputs.iter().map(|table| table.name.clone()).collect(),
                bytes,
            }));
        }
    }

    /// Advance past a slice only when none of its inputs remain.
    fn finish_recompression(&self, slice: &Slice) -> Result<(), Error> {
        self.background_errors(slice.family)?;
        if let Some(kept) = tables(&self.db, slice.family)?
            .into_iter()
            .find(|table| slice.inputs.contains(&table.name))
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                format!(
                    "table {} in {} survived its rewrite; the slice will be retried",
                    kept.name, slice.family
                ),
            ));
        }
        let mut state = self.recompression.borrow_mut();
        let cursor = state.cursor.as_mut().ok_or_else(|| {
            Error::new("STORAGE_FAILURE", "table options rewrite lost its cursor")
        })?;
        let mut next = cursor.clone();
        next.next = slice.end.clone();
        next.next.push(0);
        next.compacted_bytes = next.compacted_bytes.saturating_add(slice.bytes);
        self.save_cursor(&next)?;
        *cursor = next;
        state.failure = None;
        Ok(())
    }
}

impl Store {
    /// Rewrite progress for the running binary's table options profile.
    pub fn recompression_status(&self) -> Result<RecompressionStatus, Error> {
        Ok(self.lock()?.recompression.borrow().status())
    }

    /// Rewrite at most one slice of about `slice_bytes` input bytes. The writer
    /// lock is held only to plan and to record the slice, never during compaction.
    pub fn recompress_step(&self, slice_bytes: u64) -> Result<RecompressionStatus, Error> {
        if slice_bytes == 0 {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "a table rewrite slice needs a positive byte bound",
            ));
        }
        let result = self.recompress_slice(slice_bytes);
        let writer = self.lock()?;
        let mut state = writer.recompression.borrow_mut();
        if let Err(error) = &result {
            state.failure = Some(error.to_string().chars().take(1024).collect());
        }
        let status = state.status();
        result.map(|()| status)
    }

    fn recompress_slice(&self, slice_bytes: u64) -> Result<(), Error> {
        let Some(slice) = self.lock()?.plan_recompression(slice_bytes)? else {
            return Ok(());
        };
        let family = slice
            .db
            .cf_handle(slice.family)
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "rewritten family missing"))?;
        let mut options = CompactOptions::default();
        // Rewrite bottommost files too, but not the ones this compaction just wrote.
        options.set_bottommost_level_compaction(BottommostLevelCompaction::ForceOptimized);
        // Automatic compactions keep running beside the slice.
        options.set_exclusive_manual_compaction(false);
        let start = (!slice.start.is_empty()).then_some(slice.start.as_slice());
        slice
            .db
            .compact_range_cf_opt(family, start, Some(slice.end.as_slice()), &options);
        self.lock()?.finish_recompression(&slice)
    }
}
