// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Online store migrations: the store counter seeding (#146), the owner-to-graph index
//! build (#13), the per-source counters and artifact index (#14), and the source-context
//! cache rebuild (#149). The receipt-time event fallback backfill of record metadata
//! graphs (#20) is retired: readers derive those facts (see [`super::event_fallback_rdf`]).
//!
//! Opening a store fences older writers and seeds every checkpoint in one synchronized
//! batch, then serves. The maintenance worker runs the migrations in
//! [`Migration::RUN_ORDER`], the source summary directly after the counters, so the
//! listings that pay for what it owes stop paying first (#176). Each migration resumes from
//! its own checkpoint, so a store partway through under any order resumes correctly. The
//! worker runs one page per admission through the writer's ordinary commit, so a page
//! shares the write path's durability latch, free-space floor and data-write admission, and
//! commits its rows with its checkpoint atomically. A restart resumes after the last
//! committed page. A page yields the writer to waiting writers first, for a bounded time
//! (see [`super::writer_queue`]). Until a migration completes its readers take the pending
//! path: scoped SPARQL reads the owner index for the copied declarations and walks the
//! uncopied ones, the source and
//! artifact listings add what the source-summary migration still owes to the stored rows,
//! record context and reclaimed-parent folds read the primary time index instead of a
//! partial source timeline cache, and `status` answers from the seeding checkpoint in
//! constant time, reporting `counts` as `pending` with the seeding's progress while the
//! store counters seed. The extra reads run without the writer lock where they may be long,
//! check cancellation, and are not charged to request or visit budgets; what they hold is
//! charged as memory. No reader refuses because a migration is unfinished, and every
//! listing, context and SPARQL answer equals the answer the finished migration gives for
//! the same catalog.
//!
//! Progress comes from the store counters ([`super::store_counters`]): a migration's
//! `done` is its `done:` counter row, maintained with its checkpoint, and its `total` is
//! the population counters of its source keys. Reading them takes no writer lock.

use super::store_counters::{self, Counter, Positions, Reader};
use super::{
    Error, Store, Writer, catalog_upgrade, payload_index, rdf_index, source_context,
    source_summary, writer_format,
};
use rocksdb::{DB, WriteBatch};

/// Graph declarations one owner index page copies. A page reads only declaration keys and
/// writes one small row for each, so it holds the writer for a fraction of a second.
const OWNER_GRAPH_PAGE: usize = 65536;
/// Longest latched failure text.
const FAILURE_CHARS: usize = 1024;

/// One online migration, in the fixed order `status` reports them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Migration {
    /// The store counters `status` and migration progress read (#146). It runs first, so
    /// the others report from counters while they run.
    StoreCounters,
    /// The owner-to-graph index that bounds scoped SPARQL by its scope (#13).
    OwnerGraphIndex,
    /// Per-source counters, retained record counts and the per-source artifact index (#14).
    SourceSummary,
    /// The source timeline cache record context reads, rebuilt only when the open found it
    /// stale (#149).
    SourceContext,
    /// The payload id index: one key per held artifact, from the digest of its bytes (#323).
    PayloadIndex,
}

impl Migration {
    /// Every migration, in the order `status`, the latched failures and [`Migration::index`]
    /// use. The maintenance worker runs them in its own run order: the counters, the source
    /// summary, the owner index, then the source timeline cache (#176).
    pub const ALL: [Self; 5] = [
        Self::StoreCounters,
        Self::OwnerGraphIndex,
        Self::SourceSummary,
        Self::SourceContext,
        Self::PayloadIndex,
    ];

    /// The order the maintenance worker runs the migrations: the counters first, so the
    /// others report from counters while they run, then the source summary, whose unfinished
    /// state makes every `sources` and live `artifacts` listing read what it still owes
    /// (#176), then the owner index, since its pages are few and cheap. The migrations are
    /// independent: each reads and checkpoints its own keys.
    pub(crate) const RUN_ORDER: [Self; 5] = [
        Self::StoreCounters,
        Self::SourceSummary,
        Self::PayloadIndex,
        Self::OwnerGraphIndex,
        Self::SourceContext,
    ];

    /// Stable status and journal name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::StoreCounters => "store-counters",
            Self::OwnerGraphIndex => "owner-graph-index",
            Self::SourceSummary => "source-summary",
            Self::SourceContext => "source-context",
            Self::PayloadIndex => "payload-index",
        }
    }

    /// Position in [`Migration::ALL`].
    #[must_use]
    pub fn index(self) -> usize {
        self as usize
    }

    /// The population counters of the migration's source keys.
    fn population(self) -> &'static [Counter] {
        match self {
            Self::StoreCounters => &[
                Counter::Records,
                Counter::Artifacts,
                Counter::GraphDeclarations,
                Counter::RecordGraphs,
                Counter::PayloadChunks,
                Counter::JsonProjections,
            ],
            Self::OwnerGraphIndex => &[Counter::GraphDeclarations],
            Self::SourceSummary => &[Counter::Artifacts, Counter::Records],
            Self::SourceContext => &[Counter::Records],
            Self::PayloadIndex => &[Counter::Artifacts],
        }
    }
}

/// Progress of one migration, as `status` reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationStatus {
    /// Stable migration name.
    pub name: &'static str,
    /// Source entries at or before the watermark; equal to `total` once complete.
    pub done: u64,
    /// Source entries now in the store, so ingest and retirement change it; `None` while
    /// the store counters seed, when no exact total exists without a whole-store scan.
    pub total: Option<u64>,
    /// Opaque last committed position: `start` before the first page, absent only once
    /// complete.
    pub watermark: Option<String>,
    /// Latched failure of this session's last page; cleared by the next page that succeeds.
    pub failure: Option<String>,
}

impl MigrationStatus {
    /// Whether every source entry has been migrated: no watermark is left, and the done
    /// count equals the total unless the total is still counting.
    #[must_use]
    pub fn complete(&self) -> bool {
        self.watermark.is_none() && self.total.is_none_or(|total| total == self.done)
    }
}

/// One committed page and the migration's progress after it, read from the counter rows
/// the page's own batch updated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationPage {
    /// The migration the page advanced.
    pub migration: Migration,
    /// Whether that migration is complete after the page.
    pub complete: bool,
    /// Source entries at or before the watermark after the page.
    pub done: u64,
    /// Source entries now in the store; `None` while the store counters seed.
    pub total: Option<u64>,
    /// Committed position after the page; absent once complete.
    pub watermark: Option<String>,
    /// A failure the page latched durably, which stops the migration: the source-context
    /// rebuild's corrupt record metadata.
    pub failure: Option<String>,
}

/// Latched page failures of this session, by migration. Held apart from the writer so the
/// progress report never waits for it.
pub(super) type Failures = std::sync::Mutex<[Option<String>; Migration::ALL.len()]>;

/// Fence older writers, seed every migration this store has not started and reset a stale
/// source-context cache, in one synchronized batch, then validate every checkpoint. A
/// fenced store without its migration state refuses rather than serving a partial index
/// or counter. The event fallback fence is only the writer format identity: it seeds no
/// work, and a store that still carries the retired backfill's checkpoint opens as it is,
/// the checkpoint and its counter row left inert.
pub(super) fn fence(db: &DB) -> Result<(), Error> {
    let mut format = db
        .get("store_format")?
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "writer format missing"))?;
    let mut batch = WriteBatch::default();
    if !writer_format::has_event_fallback(&format) {
        format = writer_format::enable_event_fallback(&format)?.to_vec();
        batch.put("store_format", &format);
    }
    if !writer_format::has_owner_graphs(&format) {
        format = rdf_index::fence_owner_graphs(db, &format, &mut batch)?.to_vec();
    }
    if !writer_format::has_source_summary(&format) {
        format = source_summary::fence(db, &format, &mut batch)?.to_vec();
    }
    source_context::fence(db, &mut batch)?;
    // Last, so its zero rows and start position describe every checkpoint seeded above.
    if !writer_format::has_store_counters(&format) {
        store_counters::fence(db, &mut batch)?;
        format = writer_format::enable_store_counters(&format)?.to_vec();
        batch.put("store_format", &format);
    }
    // After the counters, whose zero rows it extends with its own.
    if !writer_format::has_payload_index(&format) {
        payload_index::fence(db, &format, &mut batch)?;
    }
    if !batch.is_empty() {
        catalog_upgrade::commit(db, batch)?;
    }
    Positions::read(|key| Ok(db.get(key)?))?;
    Ok(())
}

/// Every migration's progress from the counter rows `values` and checkpoint `positions`
/// read from one reader, a fixed number of point reads. While the store counters seed,
/// every total is pending and each done count is exact through the seeding watermark.
pub(super) fn report(
    reader: &Reader<'_>,
    positions: &Positions,
    values: &std::collections::BTreeMap<Counter, u64>,
    failures: &[Option<String>; Migration::ALL.len()],
) -> Result<Vec<MigrationStatus>, Error> {
    let sum = |counters: &[Counter]| {
        counters.iter().try_fold(0_u64, |total, counter| {
            total
                .checked_add(values[counter])
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "migration total overflow"))
        })
    };
    Migration::ALL
        .into_iter()
        .map(|migration| {
            let total = if positions.seeded() {
                Some(sum(migration.population())?)
            } else {
                None
            };
            let (done, watermark) = match migration {
                // The population rows count exactly the keys seeded so far.
                Migration::StoreCounters => {
                    (sum(migration.population())?, positions.seed_watermark())
                }
                Migration::OwnerGraphIndex => (
                    values[&Counter::OwnerGraphIndexDone],
                    rdf_index::owner_graph_progress(|key| reader.get(key.as_bytes()))?.map(
                        |after| {
                            after.map_or_else(
                                || "start".to_owned(),
                                |(graph, owner)| format!("graph {graph} owner {owner}"),
                            )
                        },
                    ),
                ),
                Migration::SourceSummary => (
                    values[&Counter::SourceSummaryDone],
                    source_summary::progress(|key| reader.get(key.as_bytes()))?.watermark(),
                ),
                Migration::SourceContext => (
                    values[&Counter::SourceContextDone],
                    source_context::watermark(|key| reader.get(key.as_bytes()))?,
                ),
                Migration::PayloadIndex => (
                    values[&Counter::PayloadIndexDone],
                    payload_index::progress(|key| reader.get(key.as_bytes()))?.watermark(),
                ),
            };
            // The source-context rebuild latches corrupt metadata durably, not per session.
            let failure = match (&failures[migration.index()], migration) {
                (None, Migration::SourceContext) => {
                    source_context::latched_failure(|key| reader.get(key.as_bytes()))?
                }
                (failure, _) => failure.clone(),
            };
            Ok(MigrationStatus {
                name: migration.name(),
                done,
                total,
                watermark,
                failure,
            })
        })
        .collect()
}

// INVARIANT: the source-context rebuild runs last, so a latched rebuild re-verifying on each
// retry (see `source_context::page`) never holds back another migration.
const _: () = assert!(matches!(
    Migration::RUN_ORDER[Migration::RUN_ORDER.len() - 1],
    Migration::SourceContext
));

impl Writer {
    /// The first migration in run order with work left, if any.
    fn pending_migration(&self) -> Result<Option<Migration>, Error> {
        let positions = Positions::read(|key| Ok(self.db.get(key)?))?;
        if !positions.seeded() {
            return Ok(Some(Migration::StoreCounters));
        }
        for migration in &Migration::RUN_ORDER[1..] {
            let complete = match migration {
                Migration::StoreCounters => true,
                Migration::OwnerGraphIndex => {
                    rdf_index::owner_graph_progress(|key| Ok(self.db.get(key)?))?.is_none()
                }
                Migration::SourceSummary => {
                    source_summary::progress(|key| Ok(self.db.get(key)?))?
                        .watermark()
                        .is_none()
                }
                Migration::SourceContext => !source_context::pending(&self.db)?,
                Migration::PayloadIndex => payload_index::progress(|key| Ok(self.db.get(key)?))?
                    .watermark()
                    .is_none(),
            };
            if !complete {
                return Ok(Some(*migration));
            }
        }
        Ok(None)
    }

    /// Stage and commit one page; `true` when the migration is complete after it. The
    /// store counters observe the page's checkpoint move in the same batch.
    fn migration_page(&self, migration: Migration) -> Result<bool, Error> {
        let staged = match migration {
            Migration::StoreCounters => store_counters::page(&self.db)?,
            Migration::OwnerGraphIndex => rdf_index::owner_graph_page(&self.db, OWNER_GRAPH_PAGE)?,
            Migration::SourceSummary => source_summary::page(&self.db)?,
            Migration::SourceContext => source_context::page(&self.db)?,
            Migration::PayloadIndex => payload_index::page(&self.db)?,
        };
        let Some((batch, complete)) = staged else {
            return Ok(true);
        };
        #[cfg(feature = "test-support")]
        if let Some(hold) = self.page_hold.get() {
            std::thread::sleep(hold);
        }
        self.commit(batch)?;
        Ok(complete)
    }
}

impl Store {
    /// Run one page of the first incomplete online migration in the worker's run order and
    /// commit it with its checkpoint; `None` when every migration is complete. It does not
    /// yield to waiting writers itself: the caller runs [`Store::defer_migration_page`]
    /// immediately before each call, apart from the writer, as the maintenance worker does.
    /// A failed page commits nothing, latches its failure in [`Store::migrations`] and is
    /// retried by the next call.
    pub fn migrate_step(&self) -> Result<Option<MigrationPage>, Error> {
        let writer = self.lock_uncounted()?;
        let started = std::time::Instant::now();
        let result: Result<Option<(Migration, bool)>, Error> = (|| {
            let Some(migration) = writer.pending_migration()? else {
                return Ok(None);
            };
            let result = writer.migration_page(migration);
            self.failures()?[migration.index()] = result
                .as_ref()
                .err()
                .map(|error| error.to_string().chars().take(FAILURE_CHARS).collect());
            Ok(Some((migration, result?)))
        })();
        drop(writer);
        self.unlocked.writers.page_held(started.elapsed());
        let Some((migration, complete)) = result? else {
            return Ok(None);
        };
        // The page's own counter rows, read after its batch committed, without the writer.
        let snapshot = self.unlocked.db.snapshot();
        let reader = Reader::snapshot(&snapshot);
        let positions = Positions::read(|key| reader.get(key.as_bytes()))?;
        let values = store_counters::values(&reader)?;
        let status = report(&reader, &positions, &values, &Default::default())?
            .into_iter()
            .nth(migration.index())
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "migration report incomplete"))?;
        Ok(Some(MigrationPage {
            migration,
            complete,
            done: status.done,
            total: status.total,
            watermark: status.watermark,
            failure: status.failure,
        }))
    }

    /// Wait before a migration page while writers wait for the writer lock, at most the
    /// longer of 100 ms and the previous page's hold; returns how long it waited. The
    /// maintenance worker runs this immediately before each [`Store::migrate_step`]: apart
    /// from its other duties after a page, so a deferral delays only the page, and inline
    /// before the page when another duty ran after that wait ended, or the page follows a
    /// retry, since writers may have arrived since.
    pub fn defer_migration_page(&self) -> std::time::Duration {
        self.unlocked.writers.defer_page()
    }

    /// Progress of every online migration, from the counter rows in one snapshot. It never
    /// takes the writer lock, so a page or a write in progress does not delay it.
    pub fn migrations(&self) -> Result<Vec<MigrationStatus>, Error> {
        let snapshot = self.unlocked.db.snapshot();
        let reader = Reader::snapshot(&snapshot);
        let positions = Positions::read(|key| reader.get(key.as_bytes()))?;
        let values = store_counters::values(&reader)?;
        let failures = self.failures()?.clone();
        report(&reader, &positions, &values, &failures)
    }

    pub(super) fn failures(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, [Option<String>; Migration::ALL.len()]>, Error> {
        self.unlocked
            .migration_failures
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "migration failure registry poisoned"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;

    /// INVARIANT: a page that fails commits nothing: `status` reports its failure with the
    /// migration's done count and watermark unchanged, and the next page that succeeds
    /// clears the failure and completes the migration. Nothing is skipped.
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
        sparrow_test_support::pending_source_summary_migration(&db).unwrap();
        drop(db);
        let store = Store::open(dir.path(), config.clone()).unwrap();
        let summary =
            |store: &Store| store.migrations().unwrap()[Migration::SourceSummary.index()].clone();
        // The counters seed first; the source summary then has not committed a page.
        while store.migrations().unwrap()[Migration::StoreCounters.index()]
            .watermark
            .is_some()
        {
            store.migrate_step().unwrap().unwrap();
        }
        let held = summary(&store);
        assert!(!held.complete(), "{held:?}");
        // Corrupt the artifact entry the page would read, in the open store.
        let (key, saved) = {
            let writer = store.lock().unwrap();
            let (key, saved) = writer
                .db
                .prefix_iterator(b"artifact:")
                .map(Result::unwrap)
                .next()
                .unwrap();
            assert!(key.starts_with(b"artifact:"));
            writer.db.put(&key, b"{}").unwrap();
            (key, saved)
        };
        assert!(store.migrate_step().is_err());
        let failed = summary(&store);
        assert!(failed.failure.is_some(), "{failed:?}");
        assert_eq!(
            (failed.done, &failed.watermark),
            (held.done, &held.watermark)
        );
        // The worker's retry finds the same page; once it can succeed, it does.
        assert!(store.migrate_step().is_err());
        store.lock().unwrap().db.put(&key, &saved).unwrap();
        while store.migrate_step().unwrap().is_some() {}
        let cleared = summary(&store);
        assert_eq!(cleared.failure, None);
        assert!(cleared.complete(), "{cleared:?}");
    }
}
