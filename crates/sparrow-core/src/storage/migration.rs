// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Online store migrations: the owner-to-graph index build (#13), the receipt-time event
//! fallback backfill of record metadata graphs (#20), and the per-source counters and
//! artifact index (#14).
//!
//! Opening a store fences older writers and seeds every checkpoint in one synchronized
//! batch, then serves. The maintenance worker runs one page per admission through the
//! writer's ordinary commit, so a page shares the write path's durability latch, free-space
//! floor and data-write admission, and commits its rows with its checkpoint atomically. A
//! restart resumes after the last committed page. Until a migration completes its readers
//! take the pending path: scoped SPARQL keeps the whole-store plan, SPARQL coverage counts
//! the selected record graphs that still lack their fallback facts, and the source and
//! artifact listings refuse with `INDEX_NOT_READY` rather than present partial counts.

use super::{
    Error, Store, Writer, catalog_upgrade, event_fallback_rdf, rdf_index, source_summary,
    writer_format,
};
use rocksdb::{DB, WriteBatch};

/// Graph declarations one owner index page copies. A page reads only declaration keys and
/// writes one small row for each, so it holds the writer for a fraction of a second.
const OWNER_GRAPH_PAGE: usize = 65536;
/// Record graphs one fallback page examines. A rebuilt graph is decoded and rewritten, so
/// this bounds how long a page holds the writer, below the backfill's own 16 MiB bounds.
const EVENT_FALLBACK_PAGE: usize = 64;
/// Longest latched failure text.
const FAILURE_CHARS: usize = 1024;
/// Prefix of the total a migration recorded when its last page committed.
const TOTAL_PREFIX: &str = "migration-total:";

/// One online migration, in the order the maintenance worker runs them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Migration {
    /// The owner-to-graph index that bounds scoped SPARQL by its scope (#13).
    OwnerGraphIndex,
    /// Receipt-time `eventTimeFallback` and `eventTimeBasis` on record graphs (#20).
    EventFallbackRdf,
    /// Per-source counters, retained record counts and the per-source artifact index (#14).
    SourceSummary,
}

impl Migration {
    /// Every migration, in run order: the index first, since its pages are few and cheap.
    pub const ALL: [Self; 3] = [
        Self::OwnerGraphIndex,
        Self::EventFallbackRdf,
        Self::SourceSummary,
    ];

    /// Stable status and journal name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::OwnerGraphIndex => "owner-graph-index",
            Self::EventFallbackRdf => "event-fallback-rdf",
            Self::SourceSummary => "source-summary",
        }
    }

    /// Position in [`Migration::ALL`].
    #[must_use]
    pub fn index(self) -> usize {
        self as usize
    }
}

/// Progress of one migration, as `status` reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationStatus {
    /// Stable migration name.
    pub name: &'static str,
    /// Source entries at or before the watermark; equal to `total` once complete.
    pub done: u64,
    /// Source entries now in the store while it runs, so ingest and retirement change it;
    /// once complete, the entries counted when its last page committed.
    pub total: u64,
    /// Opaque last committed position: `start` before the first page, absent only once
    /// complete.
    pub watermark: Option<String>,
    /// Latched failure of this session's last page; cleared by the next page that succeeds.
    pub failure: Option<String>,
}

impl MigrationStatus {
    /// Whether every source entry has been migrated.
    #[must_use]
    pub fn complete(&self) -> bool {
        self.watermark.is_none() && self.done == self.total
    }
}

/// One committed page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MigrationPage {
    /// The migration the page advanced.
    pub migration: Migration,
    /// Whether that migration is complete after the page.
    pub complete: bool,
}

/// Session failures, by migration, and the totals counted this session for migrations
/// that completed before their totals were recorded.
#[derive(Default)]
pub(super) struct State {
    failures: [Option<String>; Migration::ALL.len()],
    totals: [Option<u64>; Migration::ALL.len()],
}

fn total_key(migration: Migration) -> String {
    format!("{TOTAL_PREFIX}{}", migration.name())
}

/// Fence older writers and seed every migration this store has not started, in one
/// synchronized batch, then validate every checkpoint. A fenced store without its
/// migration state refuses rather than serving a partial index or backfill.
pub(super) fn fence(db: &DB) -> Result<(), Error> {
    let mut format = db
        .get("store_format")?
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "writer format missing"))?;
    let mut batch = WriteBatch::default();
    // A migration fenced here starts again, so any total it recorded before no longer
    // describes it.
    if !writer_format::has_event_fallback(&format) {
        format = event_fallback_rdf::fence(db, &format, &mut batch)?;
        batch.delete(total_key(Migration::EventFallbackRdf));
    }
    if !writer_format::has_owner_graphs(&format) {
        format = rdf_index::fence_owner_graphs(db, &format, &mut batch)?.to_vec();
        batch.delete(total_key(Migration::OwnerGraphIndex));
    }
    if !writer_format::has_source_summary(&format) {
        source_summary::fence(db, &format, &mut batch)?;
        batch.delete(total_key(Migration::SourceSummary));
    }
    if !batch.is_empty() {
        catalog_upgrade::commit(db, batch)?;
    }
    event_fallback_rdf::frontier(db)?;
    rdf_index::owner_graph_progress(|key| Ok(db.get(key)?))?;
    source_summary::progress(|key| Ok(db.get(key)?))?;
    Ok(())
}

impl Writer {
    /// The first migration with work left, if any.
    fn pending_migration(&self) -> Result<Option<Migration>, Error> {
        for migration in Migration::ALL {
            if !self.migration_complete(migration)? {
                return Ok(Some(migration));
            }
        }
        Ok(None)
    }

    /// Stage and commit one page; `true` when the migration is complete after it. The
    /// completing page also records the migration's total, counted once, in its batch.
    fn migration_page(&self, migration: Migration) -> Result<bool, Error> {
        let staged = match migration {
            Migration::OwnerGraphIndex => rdf_index::owner_graph_page(&self.db, OWNER_GRAPH_PAGE)?,
            Migration::EventFallbackRdf => event_fallback_rdf::page(&self.db, EVENT_FALLBACK_PAGE)?,
            Migration::SourceSummary => source_summary::page(&self.db)?,
        };
        let Some((mut batch, complete)) = staged else {
            return Ok(true);
        };
        if complete {
            let (_, total) = self.migration_counts(migration)?.0;
            batch.put(total_key(migration), total.to_be_bytes());
        }
        self.commit(batch)?;
        Ok(complete)
    }

    /// The migration's source entries at or before its committed position and all of
    /// them, counted from the store, and its watermark, absent once complete.
    fn migration_counts(
        &self,
        migration: Migration,
    ) -> Result<((u64, u64), Option<String>), Error> {
        Ok(match migration {
            Migration::OwnerGraphIndex => {
                match rdf_index::owner_graph_progress(|key| Ok(self.db.get(key)?))? {
                    None => (rdf_index::owner_graph_counts(&self.db, None)?, None),
                    Some(after) => (
                        rdf_index::owner_graph_counts(&self.db, after)?,
                        Some(after.map_or_else(
                            || "start".to_owned(),
                            |(graph, owner)| format!("graph {graph} owner {owner}"),
                        )),
                    ),
                }
            }
            Migration::EventFallbackRdf => match event_fallback_rdf::frontier(&self.db)? {
                None => (event_fallback_rdf::counts(&self.db, None)?, None),
                Some(after) => (
                    event_fallback_rdf::counts(&self.db, after.as_deref())?,
                    Some(after.unwrap_or_else(|| "start".to_owned())),
                ),
            },
            Migration::SourceSummary => {
                let progress = source_summary::progress(|key| Ok(self.db.get(key)?))?;
                (
                    source_summary::migration_counts(&self.db, &progress)?,
                    progress.watermark(),
                )
            }
        })
    }

    /// Whether the migration is complete, from its checkpoint alone.
    fn migration_complete(&self, migration: Migration) -> Result<bool, Error> {
        Ok(match migration {
            Migration::OwnerGraphIndex => {
                rdf_index::owner_graph_progress(|key| Ok(self.db.get(key)?))?.is_none()
            }
            Migration::EventFallbackRdf => event_fallback_rdf::frontier(&self.db)?.is_none(),
            Migration::SourceSummary => source_summary::progress(|key| Ok(self.db.get(key)?))?
                .watermark()
                .is_none(),
        })
    }

    /// Progress of one migration. A running migration counts its source keys; a complete
    /// one reports the total its last page recorded, so a status read never walks a
    /// finished migration's keys. A migration that completed before its total was recorded
    /// is counted once per session.
    fn migration_status(&self, migration: Migration) -> Result<MigrationStatus, Error> {
        let ((done, total), watermark) = if self.migration_complete(migration)? {
            let total = match self.db.get(total_key(migration))? {
                Some(bytes) => u64::from_be_bytes(bytes.as_slice().try_into().map_err(|_| {
                    Error::new(
                        "STORAGE_FAILURE",
                        format!("invalid recorded total of migration {}", migration.name()),
                    )
                })?),
                None => {
                    let cached = self.migrations.borrow().totals[migration.index()];
                    match cached {
                        Some(total) => total,
                        None => {
                            let (_, total) = self.migration_counts(migration)?.0;
                            self.migrations.borrow_mut().totals[migration.index()] = Some(total);
                            total
                        }
                    }
                }
            };
            ((total, total), None)
        } else {
            self.migration_counts(migration)?
        };
        Ok(MigrationStatus {
            name: migration.name(),
            done,
            total,
            watermark,
            failure: self.migrations.borrow().failures[migration.index()].clone(),
        })
    }
}

impl Store {
    /// Run one page of the first incomplete online migration and commit it with its
    /// checkpoint; `None` when every migration is complete. A failed page commits nothing,
    /// latches its failure in [`Store::migrations`] and is retried by the next call.
    pub fn migrate_step(&self) -> Result<Option<MigrationPage>, Error> {
        let writer = self.lock()?;
        let Some(migration) = writer.pending_migration()? else {
            return Ok(None);
        };
        let result = writer.migration_page(migration);
        writer.migrations.borrow_mut().failures[migration.index()] = result
            .as_ref()
            .err()
            .map(|error| error.to_string().chars().take(FAILURE_CHARS).collect());
        result.map(|complete| {
            Some(MigrationPage {
                migration,
                complete,
            })
        })
    }

    /// Progress of every online migration. Only a running migration reads its source keys;
    /// a complete one reports its recorded total.
    pub fn migrations(&self) -> Result<Vec<MigrationStatus>, Error> {
        let writer = self.lock()?;
        Migration::ALL
            .into_iter()
            .map(|migration| writer.migration_status(migration))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;

    /// INVARIANT: a page that fails commits nothing: `status` reports its failure with the
    /// migration's done count and watermark unchanged, and the next page that succeeds
    /// clears the failure and advances the watermark. Nothing is skipped.
    #[test]
    fn a_failed_page_is_reported_with_its_watermark_held_until_the_next_good_page_clears_it() {
        let dir = tempfile::tempdir().unwrap();
        let config = sparrow_test_support::storage_config!(Config);
        let store = Store::open(dir.path(), config.clone()).unwrap();
        let input = (0..100).map(|i| format!("line {i}\n")).collect::<String>();
        store
            .ingest("failing-page", "source", "log", input.as_bytes(), 1)
            .unwrap();
        drop(store);
        let db = sparrow_test_support::catalog_db(dir.path()).unwrap();
        sparrow_test_support::pending_schema_69_migrations(&db).unwrap();
        drop(db);
        let store = Store::open(dir.path(), config.clone()).unwrap();
        let fallback = |store: &Store| store.migrations().unwrap()[1].clone();
        let page = store.migrate_step().unwrap().unwrap();
        assert_eq!(page.migration, Migration::OwnerGraphIndex);
        // One good page of 64 graphs moves the backfill off its start.
        let page = store.migrate_step().unwrap().unwrap();
        assert_eq!(page.migration, Migration::EventFallbackRdf);
        assert!(!page.complete);
        let held = fallback(&store);
        let after = held.watermark.clone().unwrap();
        assert_ne!(after, "start");
        // Corrupt the next graph the backfill would read, in the open store.
        let (key, saved) = {
            let writer = store.lock().unwrap();
            let start = format!("rdf:graph:{after}");
            let (key, saved) = writer
                .db
                .iterator(rocksdb::IteratorMode::From(
                    start.as_bytes(),
                    rocksdb::Direction::Forward,
                ))
                .map(Result::unwrap)
                .find(|(key, _)| **key != *start.as_bytes())
                .unwrap();
            assert!(key.starts_with(b"rdf:graph:record:"));
            writer.db.put(&key, b"{}").unwrap();
            (key, saved)
        };
        assert!(store.migrate_step().is_err());
        let failed = fallback(&store);
        assert!(failed.failure.is_some(), "{failed:?}");
        assert_eq!(
            (failed.done, &failed.watermark),
            (held.done, &held.watermark)
        );
        // The worker's retry finds the same page; once it can succeed, it does.
        assert!(store.migrate_step().is_err());
        store.lock().unwrap().db.put(&key, &saved).unwrap();
        store.migrate_step().unwrap().unwrap();
        let cleared = fallback(&store);
        assert_eq!(cleared.failure, None);
        assert!(cleared.done > held.done, "{cleared:?}");
        assert_ne!(cleared.watermark, held.watermark);
    }
}
