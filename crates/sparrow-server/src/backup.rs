// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The daemon's online backup authority (#156) and its wire encoding.

use crate::sparrow_capnp as wire;
use sparrow_core::{BackupPhase, BackupStatus, BackupTarget, Error, Store};

/// Whether this daemon takes online backups, and where.
#[derive(Clone, Debug, Default)]
pub(crate) enum Backup {
    /// No `backup_root` is configured.
    #[default]
    Unconfigured,
    /// A `--no-intake` or `--private-intake` daemon serves a disposable copy; backing it up
    /// would mix another store into the live backup root.
    Copy,
    /// Back up into this target.
    Configured(BackupTarget),
}

impl Backup {
    /// Start a backup and return its running status, or refuse.
    pub(crate) fn start(&self, store: &Store, now: u64) -> Result<BackupStatus, Error> {
        match self {
            Self::Unconfigured => Err(Error::new(
                "UNSUPPORTED",
                "this daemon has no backup_root; configure backup_root on the NAS archive and restart",
            )),
            Self::Copy => Err(Error::new(
                "UNSUPPORTED",
                "a --no-intake or --private-intake daemon serves a copy of a store and takes no backup; back up the daemon that serves the live store",
            )),
            Self::Configured(target) => store.backup_start(target, now),
        }
    }

    /// Encode this daemon's backup state from the store's latest backup.
    pub(crate) fn encode(&self, mut out: wire::backup_status::Builder<'_>, status: &BackupStatus) {
        let Self::Configured(target) = self else {
            out.set_phase(wire::BackupPhase::Unconfigured);
            out.set_root("");
            out.init_failure().set_absent(());
            return;
        };
        out.set_root(target.root().to_string_lossy().as_ref());
        out.set_keep(target.keep());
        out.set_phase(match status.phase {
            BackupPhase::Idle => wire::BackupPhase::Idle,
            BackupPhase::Running => wire::BackupPhase::Running,
            BackupPhase::Complete => wire::BackupPhase::Complete,
            BackupPhase::Failed => wire::BackupPhase::Failed,
        });
        out.set_started_at(status.started_at);
        out.set_finished_at(status.finished_at);
        out.set_backup_id(status.backup_id);
        out.set_files(status.files);
        out.set_bytes(status.bytes);
        match &status.failure {
            Some(failure) => out.init_failure().set_value(failure),
            None => out.init_failure().set_absent(()),
        }
    }
}
