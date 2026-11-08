// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One-time background rewrite of table files written under an older options profile.
//!
//! New options apply only to new table files, and RocksDB never revisits a cold
//! bottommost file on its own. The rewrite uses RocksDB's periodic compaction
//! rather than manual range compaction. A manual range starts at the first
//! level with keys in the range: a level-0 file or memtable flush spans every
//! catalog prefix and merges the whole base level, and a wide upper-level file
//! merges its whole overlap below it, on every slice. A periodic job instead
//! rewrites one marked file, into its own level at the bottom or into its
//! overlap one level down, in the native scheduler, which ranks it below
//! size-triggered compaction and aborts it when the store closes.
//!
//! The rewrite starts by recording the next table file number and a start
//! second. Each step sets the rewritten families' periodic window to the
//! seconds elapsed since that start, so only files created before it are
//! marked. A step turns the window off while pressure is outstanding, or while
//! one job's headroom would exceed the data allowance or the free-space floor.
//! When no table file numbered below the boundary remains in those families,
//! the profile key is written and the window is turned off.

use super::{Error, Store, Writer, table_options};
use rocksdb::{DB, WriteBatch};

/// Identity of the options profile whose rewrite is complete.
pub(super) const PROFILE_KEY: &str = "table-options-profile";
/// Durable rewrite start: profile, first new table file number and start second.
const CURSOR_KEY: &str = "table-options-rewrite";
/// Space one periodic job may hold above the store's settled size. A job
/// rewrites one 64 MiB file together with its overlap one level down, about ten
/// files at RocksDB's default tenfold level multiplier, so about 704 MiB.
const JOB_HEADROOM_BYTES: u64 = 1024 * 1024 * 1024;

/// Smallest periodic window the daemon applies, in seconds.
pub const RECOMPRESSION_SETTLE_SECONDS: u64 = 60;

/// Progress of the one-time rewrite.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecompressionPhase {
    /// Files written under an older profile remain and no gate has paused the
    /// rewrite. The native window may still be off during the first 60 s settle
    /// or just after a reopen.
    Running,
    /// A rewrite is owed but the last step turned it off; `waiting` says why.
    Paused,
    /// Every rewritten family's files were written under the current profile.
    Complete,
}

/// Rewrite progress observed by the last step, with its gate and failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecompressionStatus {
    /// Identity of the running binary's table options.
    pub profile: &'static str,
    /// Current phase.
    pub phase: RecompressionPhase,
    /// Bytes of table files still written under an older profile; zero once complete.
    pub remaining_bytes: u64,
    /// Why the last step turned native rewriting off.
    pub waiting: Option<String>,
    /// Latched failure; cleared by the next step that succeeds.
    pub failure: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Cursor {
    /// Table files numbered below this predate the rewrite.
    boundary: u64,
    /// Unix second after every predating file was created.
    started_at: u64,
}

impl Cursor {
    fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(table_options::PROFILE.len() + 17);
        bytes.extend_from_slice(table_options::PROFILE.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&self.boundary.to_be_bytes());
        bytes.extend_from_slice(&self.started_at.to_be_bytes());
        bytes
    }

    /// `None` when the cursor belongs to another profile, which restarts the rewrite.
    fn decode(bytes: &[u8]) -> Result<Option<Self>, Error> {
        let split = bytes
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(corrupt_cursor)?;
        if &bytes[..split] != table_options::PROFILE.as_bytes() {
            return Ok(None);
        }
        let rest = &bytes[split + 1..];
        if rest.len() != 16 {
            return Err(corrupt_cursor());
        }
        let mut boundary = [0; 8];
        let mut started_at = [0; 8];
        boundary.copy_from_slice(&rest[..8]);
        started_at.copy_from_slice(&rest[8..]);
        Ok(Some(Self {
            boundary: u64::from_be_bytes(boundary),
            started_at: u64::from_be_bytes(started_at),
        }))
    }
}

fn corrupt_cursor() -> Error {
    Error::new(
        "STORAGE_FAILURE",
        "table options rewrite cursor is malformed; inspect or restore the catalog",
    )
}

/// The rewrite owed by this store and this session's observations.
#[derive(Default)]
pub(super) struct State {
    owed: bool,
    cursor: Option<Cursor>,
    remaining_bytes: u64,
    waiting: Option<String>,
    failure: Option<String>,
    /// Native background error count already reported.
    background_errors: u64,
    /// Periodic window this session last applied; zero is off.
    window: u64,
}

impl super::writer_queue::Observe for State {
    type View = RecompressionStatus;
    fn view(&self) -> RecompressionStatus {
        self.status()
    }
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
        let boundary = cursor.map_or(u64::MAX, |cursor| cursor.boundary);
        Ok(Self {
            owed: true,
            cursor,
            remaining_bytes: older_bytes(db, boundary)?.1,
            ..Self::default()
        })
    }

    fn status(&self) -> RecompressionStatus {
        RecompressionStatus {
            profile: table_options::PROFILE,
            phase: if !self.owed {
                RecompressionPhase::Complete
            } else if self.waiting.is_some() {
                RecompressionPhase::Paused
            } else {
                RecompressionPhase::Running
            },
            remaining_bytes: self.remaining_bytes,
            waiting: self.waiting.clone(),
            failure: self.failure.clone(),
        }
    }
}

fn file_number(name: &str) -> Result<u64, Error> {
    name.trim_start_matches('/')
        .strip_suffix(".sst")
        .and_then(|number| number.parse().ok())
        .ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                format!("live table file {name} has no table file number"),
            )
        })
}

/// The next table file number, and the bytes of rewritten-family table files
/// numbered below `boundary`.
fn older_bytes(db: &DB, boundary: u64) -> Result<(u64, u64), Error> {
    let mut next = 0;
    let mut older = 0_u64;
    for file in db.live_files()? {
        let number = file_number(&file.name)?;
        next = next.max(number.saturating_add(1));
        if number < boundary && table_options::REWRITTEN.contains(&file.column_family_name.as_str())
        {
            older = older.saturating_add(file.size as u64);
        }
    }
    Ok((next, older))
}

impl Writer {
    /// Apply one periodic window to every rewritten family; zero turns it off.
    fn rewrite_window(&self, state: &mut State, window: u64) -> Result<(), Error> {
        if state.window == window {
            return Ok(());
        }
        let value = window.to_string();
        for name in table_options::REWRITTEN {
            let family = self
                .db
                .cf_handle(name)
                .ok_or_else(|| Error::new("STORAGE_FAILURE", format!("{name} family missing")))?;
            self.db
                .set_options_cf(&family, &[("periodic_compaction_seconds", &value)])?;
        }
        state.window = window;
        Ok(())
    }

    fn recompress(&self, state: &mut State, now_seconds: u64, settle: u64) -> Result<(), Error> {
        if !state.owed {
            return Ok(());
        }
        let mut errors = 0_u64;
        for name in table_options::REWRITTEN {
            errors =
                errors.saturating_add(self.native_property(name, "rocksdb.background-errors")?);
        }
        if errors > state.background_errors {
            // Report each native error once; a recovered store resumes next step.
            let new = errors - state.background_errors;
            state.background_errors = errors;
            self.rewrite_window(state, 0)?;
            return Err(Error::new(
                "STORAGE_FAILURE",
                format!(
                    "RocksDB reported {new} new background errors; the table rewrite paused and retries on the next step"
                ),
            ));
        }
        // The gate comes first: below the floor the store refuses the writes
        // that start or finish the rewrite, and the window must still go off.
        let boundary = state.cursor.map_or(u64::MAX, |cursor| cursor.boundary);
        state.remaining_bytes = older_bytes(&self.db, boundary)?.1;
        state.waiting = self.rewrite_gate()?;
        if state.waiting.is_some() {
            return self.rewrite_window(state, 0);
        }
        if state.remaining_bytes == 0 {
            self.rewrite_window(state, 0)?;
            let mut batch = WriteBatch::default();
            batch.put(PROFILE_KEY, table_options::PROFILE);
            batch.delete(CURSOR_KEY);
            self.commit(batch)?;
            *state = State::default();
            return Ok(());
        }
        let cursor = match state.cursor {
            Some(cursor) => cursor,
            None => {
                // Every existing file was created no later than this second.
                let cursor = Cursor {
                    boundary: older_bytes(&self.db, u64::MAX)?.0,
                    started_at: now_seconds.saturating_add(1),
                };
                let mut batch = WriteBatch::default();
                batch.put(CURSOR_KEY, cursor.encode());
                self.commit(batch)?;
                state.cursor = Some(cursor);
                cursor
            }
        };
        // Periodic compaction marks files created before `now - window`. The
        // window equal to the elapsed time marks exactly the older files. It
        // starts only after `settle` seconds, so a fresh output is never
        // marked, and moves only after drifting by `settle` or an eighth, so
        // native option rewrites stay rare. Because the window lags the
        // elapsed time by up to an eighth, files written during roughly the
        // first eighth of the rewrite can be marked and rewritten again,
        // possibly more than once; that costs extra work, not completion.
        let elapsed = now_seconds.saturating_sub(cursor.started_at);
        if elapsed >= settle
            && (state.window == 0
                || elapsed.saturating_sub(state.window) > settle.max(state.window / 8))
        {
            self.rewrite_window(state, elapsed)?;
        }
        Ok(())
    }

    /// Why native rewriting must stay off now, if it must.
    fn rewrite_gate(&self) -> Result<Option<String>, Error> {
        let pressure = self.pressure.borrow().status.clone();
        if pressure.phase != super::pressure::PressurePhase::Ready || pressure.requested_bytes != 0
        {
            return Ok(Some("pressure maintenance has priority".into()));
        }
        let physical = self.physical_bytes()?;
        let allowance = self.config.disk_budget - self.config.maintenance_reserve;
        if physical.saturating_add(JOB_HEADROOM_BYTES) > allowance {
            return Ok(Some(format!(
                "a rewrite job's {JOB_HEADROOM_BYTES} bytes of headroom does not fit the data allowance: {physical} of {allowance} bytes in use"
            )));
        }
        self.space.shortfall_after(JOB_HEADROOM_BYTES)
    }
}

impl Store {
    /// Rewrite progress for the running binary's table options profile.
    /// Reads the observed state without the writer lock, and refuses once it is poisoned.
    pub fn recompression_status(&self) -> Result<RecompressionStatus, Error> {
        self.refuse_poisoned()?;
        Ok(self.unlocked.recompression.read())
    }

    /// Observe the rewrite at `now` (Unix milliseconds) and turn native
    /// rewriting on or off. `settle` is the smallest periodic window in
    /// seconds; the daemon uses [`RECOMPRESSION_SETTLE_SECONDS`]. The step
    /// compacts nothing itself and returns promptly.
    pub fn recompress_step(&self, now: u64, settle: u64) -> Result<RecompressionStatus, Error> {
        if settle == 0 {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "a table rewrite needs a positive settle time",
            ));
        }
        let writer = self.lock()?;
        let mut state = writer.recompression.borrow_mut();
        let result = writer.recompress(&mut state, now / 1000, settle);
        match &result {
            Ok(()) => state.failure = None,
            Err(error) => state.failure = Some(error.to_string().chars().take(1024).collect()),
        }
        let status = state.status();
        result.map(|()| status)
    }
}
