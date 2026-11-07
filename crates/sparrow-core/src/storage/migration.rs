// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Online schema 69 migrations: the owner-to-graph index build (#13) and the receipt-time
//! event fallback backfill of record metadata graphs (#20).
//!
//! Opening a store fences older writers and seeds both checkpoints in one synchronized
//! batch, then serves. The maintenance worker runs one page per admission through the
//! writer's ordinary commit, so a page shares the write path's durability latch, free-space
//! floor and data-write admission, and commits its rows with its checkpoint atomically. A
//! restart resumes after the last committed page. Until a migration completes its readers
//! take the pending path: scoped SPARQL keeps the whole-store plan, and SPARQL coverage
//! counts the selected record graphs that still lack their fallback facts.

use super::{Error, Store, Writer, catalog_upgrade, event_fallback_rdf, rdf_index, writer_format};
use rocksdb::{DB, WriteBatch};

/// Graph declarations one owner index page copies. A page reads only declaration keys and
/// writes one small row for each, so it holds the writer for a fraction of a second.
const OWNER_GRAPH_PAGE: usize = 65536;
/// Record graphs one fallback page examines. A rebuilt graph is decoded and rewritten, so
/// this bounds how long a page holds the writer, below the backfill's own 16 MiB bounds.
const EVENT_FALLBACK_PAGE: usize = 64;
/// Longest latched failure text.
const FAILURE_CHARS: usize = 1024;

/// One online migration, in the order the maintenance worker runs them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Migration {
    /// The owner-to-graph index that bounds scoped SPARQL by its scope (#13).
    OwnerGraphIndex,
    /// Receipt-time `eventTimeFallback` and `eventTimeBasis` on record graphs (#20).
    EventFallbackRdf,
}

impl Migration {
    /// Every migration, in run order: the index first, since its pages are few and cheap.
    pub const ALL: [Self; 2] = [Self::OwnerGraphIndex, Self::EventFallbackRdf];

    /// Stable status and journal name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::OwnerGraphIndex => "owner-graph-index",
            Self::EventFallbackRdf => "event-fallback-rdf",
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
    /// Source entries now in the store. Ingest and retirement change it while it runs.
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

/// Session failures, by migration.
#[derive(Default)]
pub(super) struct State {
    failures: [Option<String>; Migration::ALL.len()],
}

/// Fence older writers and seed every migration this store has not started, in one
/// synchronized batch, then validate both checkpoints. A fenced store without its
/// migration state refuses rather than serving a partial index or backfill.
pub(super) fn fence(db: &DB) -> Result<(), Error> {
    let mut format = db
        .get("store_format")?
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "writer format missing"))?;
    let mut batch = WriteBatch::default();
    if !writer_format::has_event_fallback(&format) {
        format = event_fallback_rdf::fence(db, &format, &mut batch)?;
    }
    if !writer_format::has_owner_graphs(&format) {
        rdf_index::fence_owner_graphs(db, &format, &mut batch)?;
    }
    if !batch.is_empty() {
        catalog_upgrade::commit(db, batch)?;
    }
    event_fallback_rdf::frontier(db)?;
    rdf_index::owner_graph_progress(|key| Ok(db.get(key)?))?;
    Ok(())
}

impl Writer {
    /// The first migration with work left, if any.
    fn pending_migration(&self) -> Result<Option<Migration>, Error> {
        if rdf_index::owner_graph_progress(|key| Ok(self.db.get(key)?))?.is_some() {
            return Ok(Some(Migration::OwnerGraphIndex));
        }
        if event_fallback_rdf::frontier(&self.db)?.is_some() {
            return Ok(Some(Migration::EventFallbackRdf));
        }
        Ok(None)
    }

    /// Stage and commit one page; `true` when the migration is complete after it.
    fn migration_page(&self, migration: Migration) -> Result<bool, Error> {
        let staged = match migration {
            Migration::OwnerGraphIndex => rdf_index::owner_graph_page(&self.db, OWNER_GRAPH_PAGE)?,
            Migration::EventFallbackRdf => event_fallback_rdf::page(&self.db, EVENT_FALLBACK_PAGE)?,
        };
        let Some((batch, complete)) = staged else {
            return Ok(true);
        };
        self.commit(batch)?;
        Ok(complete)
    }

    fn migration_status(&self, migration: Migration) -> Result<MigrationStatus, Error> {
        let ((done, total), watermark) = match migration {
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
        };
        Ok(MigrationStatus {
            name: migration.name(),
            // A complete migration has migrated every entry now present.
            done: if watermark.is_none() { total } else { done },
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

    /// Progress of every online migration. Counting reads each migration's source keys
    /// once, as `status` already counts the catalog.
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
