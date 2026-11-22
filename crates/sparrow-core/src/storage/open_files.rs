// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The store's share of the process descriptor limit.
//!
//! RocksDB's default `max_open_files` of -1 keeps every table file open. Once a
//! store holds more tables than the soft `RLIMIT_NOFILE` allows, a later table
//! read fails with EMFILE and the daemon exits with its durability uncertain
//! (issue #148: 917 table files against a soft limit of 1024). The table
//! cache, which RocksDB shares between table and blob files, is therefore
//! bounded below the soft limit, and a limit too small to hold a working cache
//! beside the reserve refuses the open instead of failing later.

use super::Error;
use rustix::process::{Resource, getrlimit};

/// Descriptors kept outside the table cache: the RPC and TCP connections (one
/// ceiling, 16 by default, at most 256), ingest connections (16 by default, at
/// most 256), syslog connections (16 per listener), the WAL, MANIFEST and info
/// logs, table files being written by flushes and compactions, intake files,
/// archive segments, and one descriptor per watched directory (the live
/// watches cover 16 each). A configuration whose ceilings and watch coverage
/// approach 1024 needs a soft limit far above the 2048 floor; the unit's 65536
/// leaves 64512 for tables against the 917 the live store held.
const RESERVE: u64 = 1024;
/// The smallest table cache the store opens with.
const MINIMUM_TABLE_FILES: u64 = 1024;
/// The limit the units set (`LimitNOFILE`), named in the refusal.
const UNIT_LIMIT: u64 = 65536;

/// The process descriptor limit and the table cache bound derived from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenFiles {
    /// The soft `RLIMIT_NOFILE` at open; `None` when unlimited.
    pub nofile: Option<u64>,
    /// RocksDB's `max_open_files`: the soft limit less the reserve.
    pub max_open_files: i32,
}

impl OpenFiles {
    /// Derive the bound from this process's soft descriptor limit.
    pub(super) fn from_process() -> Result<Self, Error> {
        Self::for_limit(getrlimit(Resource::Nofile).current)
    }

    fn for_limit(nofile: Option<u64>) -> Result<Self, Error> {
        let tables = match nofile {
            None => u64::MAX,
            Some(soft) => soft
                .checked_sub(RESERVE)
                .filter(|tables| *tables >= MINIMUM_TABLE_FILES)
                .ok_or_else(|| {
                    Error::new(
                        "RESOURCE_LIMIT",
                        format!(
                            "the soft open-file limit is {soft}; the store needs at least {} ({RESERVE} reserved and {MINIMUM_TABLE_FILES} table files). Set LimitNOFILE={UNIT_LIMIT} in the unit (deploy/systemd) or raise ulimit -n",
                            RESERVE + MINIMUM_TABLE_FILES
                        ),
                    )
                })?,
        };
        Ok(Self {
            nofile,
            max_open_files: i32::try_from(tables).unwrap_or(i32::MAX),
        })
    }
}

impl std::fmt::Display for OpenFiles {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.nofile {
            Some(soft) => write!(formatter, "nofile={soft}")?,
            None => formatter.write_str("nofile=unlimited")?,
        }
        write!(formatter, " max_open_files={}", self.max_open_files)
    }
}

#[cfg(test)]
mod tests {
    use super::OpenFiles;

    /// INVARIANT: the table cache stays the reserve below the soft limit, so
    /// table files can never exhaust it, and a limit with no room for a
    /// working cache (the 1024 a user manager gives by default) refuses the
    /// open with the unit setting that fixes it.
    #[test]
    fn table_cache_stays_below_the_soft_limit_and_small_limits_refuse() {
        let unit = OpenFiles::for_limit(Some(65536)).expect("unit limit opens");
        assert_eq!(unit.max_open_files, 64512);
        assert_eq!(unit.to_string(), "nofile=65536 max_open_files=64512");
        assert_eq!(
            OpenFiles::for_limit(Some(2048))
                .expect("floor opens")
                .max_open_files,
            1024
        );
        let refused = OpenFiles::for_limit(Some(2047)).expect_err("below the floor");
        assert_eq!(refused.code, "RESOURCE_LIMIT");
        assert!(refused.message.contains("LimitNOFILE=65536"));
        assert!(OpenFiles::for_limit(Some(1024)).is_err());
        assert_eq!(
            OpenFiles::for_limit(None)
                .expect("unlimited opens")
                .max_open_files,
            i32::MAX
        );
    }
}
