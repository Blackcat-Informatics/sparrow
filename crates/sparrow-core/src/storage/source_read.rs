// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Immutable discovery of intake keys, retained provenance and durable mappings.

use super::{
    Catalog, Error, Store, catalog_lease, diagnostic, listing_build, random_id, source_time,
};
use catalog_lease::{CAPTURE_BYTES, HANDLE_BYTES, LEASE_MS, MAX_HANDLES, MAX_ITEMS, SCAN_BYTES};
use std::{collections::BTreeMap, time::Instant};

/// Exact intake key selector. Omitted means all discoverable sources.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SourceSelector {
    /// The same exact key accepted by ingestion and source scope filters.
    pub source: Option<String>,
}
impl catalog_lease::Selector for SourceSelector {
    fn capacity(&self) -> usize {
        self.source.as_ref().map_or(0, String::capacity)
    }
}

/// Source metadata capture or continuation; reading never creates investigation pins.
#[derive(Clone, Debug)]
pub struct SourceRead {
    /// Selector bound into the capture.
    pub selector: SourceSelector,
    /// Opaque cursor from the same operation and store.
    pub cursor: Option<String>,
    /// Requested upper bound, 1..1000.
    pub page_size: usize,
    /// Aggregate pin/artifact/record/profile scan admission, 1..100000.
    pub scan_budget: usize,
}

/// A bounded source declaration supplied by the runtime's validated watch owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceWatchBinding {
    /// Exact configured intake key.
    pub source: String,
    /// Durable watch identity.
    pub watch_id: String,
    /// Captured control revision.
    pub revision: u64,
    /// Whether new admission is enabled.
    pub enabled: bool,
}

/// Watch administration metadata at source capture time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceWatchInfo {
    /// Durable watch identity.
    pub watch_id: String,
    /// Captured revision.
    pub revision: u64,
    /// Whether new admission is enabled.
    pub enabled: bool,
}

/// Runtime-owned syslog binding; listener counters are process scoped, not durable continuity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceSyslogBinding {
    /// Exact configured intake key.
    pub source: String,
    /// Configured listener identity, resolved with the same source and store.
    pub listener_id: String,
}
impl Default for SourceRead {
    fn default() -> Self {
        Self {
            selector: SourceSelector::default(),
            cursor: None,
            page_size: 200,
            scan_budget: 10_000,
        }
    }
}

/// Source facts at one captured instant; equal content never collapses occurrences.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceSummary {
    /// Exact store-local intake key, also used by `Scope.sourceIds`.
    pub source_id: String,
    /// The source IRI used by native occurrence RDF metadata.
    pub rdf_iri: String,
    /// Artifact entries present and not evicted. Counts follow retirement, not the
    /// retention deadline.
    pub complete_artifacts: usize,
    /// Artifacts whose entry is retired while at least one of their records is retained.
    pub fragment_parents: usize,
    /// Exact number of record entries present and not evicted.
    pub retained_records: usize,
    /// Declared immutable time mapping, or explicit absence.
    pub time_profile_id: Option<String>,
    /// Declared immutable diagnostic mapping, or explicit absence.
    pub diagnostic_profile_id: Option<String>,
    /// Producer stream continuity is not inferred from catalog evidence.
    pub continuity_unknown: bool,
    /// Configured watch bindings, including sources with no retained input.
    pub watches: Vec<SourceWatchInfo>,
    /// Finite currently retained producer-window identities; each has a typed summary.
    pub producer_stream_ids: Vec<String>,
    /// Unexpired or active descriptor-owned generation checkpoints, including empty inputs.
    pub tail_generation_ids: Vec<String>,
    /// Configured syslog listeners, including those with no retained packets.
    pub syslog_listener_ids: Vec<String>,
    /// Retained durable receiver observation windows, including discard-only sources.
    pub receiver_window_ids: Vec<String>,
}

/// The native occurrence metadata IRI for an exact intake key.
pub(super) fn source_iri(source: &str) -> String {
    format!(
        "urn:sparrow:source:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(source.as_bytes()).as_bytes())
    )
}
impl SourceSummary {
    fn new(source: String) -> Self {
        Self {
            rdf_iri: source_iri(&source),
            source_id: source,
            complete_artifacts: 0,
            fragment_parents: 0,
            retained_records: 0,
            time_profile_id: None,
            diagnostic_profile_id: None,
            continuity_unknown: true,
            watches: Vec::new(),
            producer_stream_ids: Vec::new(),
            tail_generation_ids: Vec::new(),
            syslog_listener_ids: Vec::new(),
            receiver_window_ids: Vec::new(),
        }
    }
    fn capacity(&self) -> usize {
        512 + self.source_id.capacity() * 2
            + self.producer_stream_ids.capacity() * std::mem::size_of::<String>()
            + self.tail_generation_ids.capacity() * std::mem::size_of::<String>()
            + self.syslog_listener_ids.capacity() * std::mem::size_of::<String>()
            + self.receiver_window_ids.capacity() * std::mem::size_of::<String>()
            + self
                .producer_stream_ids
                .iter()
                .chain(self.tail_generation_ids.iter())
                .chain(self.syslog_listener_ids.iter())
                .chain(self.receiver_window_ids.iter())
                .map(String::capacity)
                .sum::<usize>()
            + self.rdf_iri.capacity()
            + self.time_profile_id.as_ref().map_or(0, String::capacity)
            + self
                .diagnostic_profile_id
                .as_ref()
                .map_or(0, String::capacity)
            + self.watches.capacity() * std::mem::size_of::<SourceWatchInfo>()
            + self
                .watches
                .iter()
                .map(|watch| watch.watch_id.capacity())
                .sum::<usize>()
    }
}

fn admit_source<'a>(
    rows: &'a mut BTreeMap<String, SourceSummary>,
    source: &str,
    capacity: &mut usize,
) -> Result<&'a mut SourceSummary, Error> {
    if !rows.contains_key(source) {
        let planned = 512 + source.len() * 2 + 128;
        if rows.len() >= MAX_ITEMS || *capacity + planned > CAPTURE_BYTES {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "source capture exceeds item/capacity admission",
            ));
        }
        let summary = SourceSummary::new(source.to_owned());
        *capacity += summary.capacity();
        rows.insert(source.to_owned(), summary);
    }
    rows.get_mut(source)
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "admitted source disappeared"))
}

/// Stable intake-key-ordered page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourcePageBody {
    /// Captured source rows, including declared mappings without retained evidence.
    pub sources: Vec<SourceSummary>,
}
impl catalog_lease::PageBody for SourcePageBody {
    const PAGE_ITEMS: usize = (catalog_lease::PAGE_BYTES - 4096) / 16384;
    fn len(&self) -> usize {
        self.sources.len()
    }
    fn capacity(&self) -> usize {
        self.sources.capacity() * std::mem::size_of::<SourceSummary>()
            + self
                .sources
                .iter()
                .map(SourceSummary::capacity)
                .sum::<usize>()
    }
    fn page(&self, start: usize, end: usize) -> Result<Self, Error> {
        Ok(Self {
            sources: self.sources[start..end].to_vec(),
        })
    }
}
/// Complete source capture receipt and bounded page.
pub type SourceReadPage = catalog_lease::CatalogReadPage<SourcePageBody>;
pub(super) type Reads = catalog_lease::Reads<SourceSelector, SourcePageBody>;
/// One finished source capture, ready to publish.
pub(super) type Captured = catalog_lease::Snapshot<SourceSelector, SourcePageBody>;
/// Source listing builds: each keeps the selector and limits its cursor binds.
pub(super) type Builds = listing_build::Builds<(SourceSelector, usize, usize), Captured>;
/// Identity kind of a source listing build.
pub(super) const BUILD: &str = "source-build";

/// Test hook run while a source listing reads its snapshot without the writer lock.
#[cfg(feature = "test-support")]
pub(super) type Pause = std::sync::Arc<dyn Fn() + Send + Sync>;

#[cfg(feature = "test-support")]
impl Store {
    /// Run `pause` in every later source listing after it has taken its snapshot and released
    /// the writer lock, and before it reads any row. Acceptance tests only.
    pub fn pause_source_reads_for_tests(
        &self,
        pause: impl Fn() + Send + Sync + 'static,
    ) -> Result<(), Error> {
        *self
            .source_read_pause
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "source read pause poisoned"))? =
            Some(std::sync::Arc::new(pause));
        Ok(())
    }

    fn source_read_paused(&self) -> Result<(), Error> {
        let pause = self
            .source_read_pause
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "source read pause poisoned"))?
            .clone();
        if let Some(pause) = pause {
            pause();
        }
        Ok(())
    }
}

impl Store {
    /// Discover retained provenance and durable mapping declarations atomically.
    /// No successful prefix is published on resource, catalog or identity failure.
    pub fn source_read(
        &self,
        query: &SourceRead,
        now: u64,
        configured: &[SourceWatchBinding],
    ) -> Result<SourceReadPage, Error> {
        self.source_read_with_syslog(query, now, configured, &[])
    }

    /// Include runtime-owned syslog bindings without inventing producer continuity.
    pub fn source_read_with_syslog(
        &self,
        query: &SourceRead,
        now: u64,
        configured: &[SourceWatchBinding],
        intakes: &[SourceSyslogBinding],
    ) -> Result<SourceReadPage, Error> {
        self.source_read_with_syslog_cancellable(
            query,
            now,
            configured,
            intakes,
            &crate::QueryCancellation::default(),
        )
    }

    /// Capture runtime and stored source declarations with the caller's cooperative lifetime.
    pub fn source_read_with_syslog_cancellable(
        &self,
        query: &SourceRead,
        now: u64,
        configured: &[SourceWatchBinding],
        intakes: &[SourceSyslogBinding],
        cancellation: &crate::QueryCancellation,
    ) -> Result<SourceReadPage, Error> {
        if intakes.len() > 4
            || intakes.iter().any(|binding| {
                binding.source.is_empty()
                    || binding.source.len() > 512
                    || binding.listener_id.is_empty()
                    || binding.listener_id.len() > 128
            })
            || intakes
                .iter()
                .map(|binding| &binding.listener_id)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != intakes.len()
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "syslog discovery requires at most four unique listener IDs (1..128 bytes) and exact source labels (1..512 bytes)",
            ));
        }
        if !(1..=1000).contains(&query.page_size)
            || !(1..=100000).contains(&query.scan_budget)
            || query
                .selector
                .source
                .as_ref()
                .is_some_and(|s| s.is_empty() || s.len() > 512)
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "source reads require an exact 1..512 byte key, page_size 1..1000 and scan_budget 1..100000",
            ));
        }
        cancellation.checkpoint()?;
        if let Some(cursor) = &query.cursor {
            let parsed = catalog_lease::Cursor::parse(cursor)?;
            if listing_build::is_build(&parsed, BUILD) {
                let binding = (&query.selector, query.page_size, query.scan_budget);
                return self.source_build_poll(&parsed, Some(binding), now, cancellation);
            }
            let mut reads = self.source_reads()?;
            reads.purge(now);
            cancellation.checkpoint()?;
            return reads.resume(
                &query.selector,
                cursor,
                query.page_size,
                query.scan_budget,
                now,
            );
        }
        crate::format_instant(now)?;
        crate::format_instant(
            now.checked_add(LEASE_MS)
                .ok_or_else(|| Error::new("INVALID_ARGUMENT", "source read deadline overflow"))?,
        )?;
        let pending = super::source_summary::progress(|key| {
            self.unlocked.db.get(key.as_bytes()).map_err(Error::from)
        })? != super::source_summary::Progress::Complete;
        if !pending {
            let snapshot =
                self.source_capture(query, now, (configured, intakes), cancellation, None)?;
            return self.publish_sources(snapshot, now, cancellation);
        }
        // The source-summary migration is unfinished: the capture reads what it owes, which
        // costs the unmigrated part of the store, so it runs as a build (#158).
        let (id, waiter, wait) = {
            let store = self.clone();
            let (query, configured, intakes) =
                (query.clone(), configured.to_vec(), intakes.to_vec());
            let mut builds = self.source_builds()?;
            let context = (query.selector.clone(), query.page_size, query.scan_budget);
            let (id, waiter) = builds.start(BUILD, context, now, move |worker| {
                store.source_capture(
                    &query,
                    now,
                    (&configured, &intakes),
                    &worker.cancellation(),
                    Some(worker),
                )
            })?;
            (id, waiter, builds.wait())
        };
        let page = self.source_build_page(&id, &waiter, wait, now, cancellation);
        if page.is_err() {
            // The caller never learns this build's cursor: nothing can reach it.
            self.source_builds()?.remove(&id);
        }
        page
    }

    /// Follow a build cursor, binding it to the caller's selector and limits when given.
    pub(super) fn source_build_poll(
        &self,
        cursor: &catalog_lease::Cursor<'_>,
        binding: Option<(&SourceSelector, usize, usize)>,
        now: u64,
        cancellation: &crate::QueryCancellation,
    ) -> Result<SourceReadPage, Error> {
        if cursor.start() != 0 {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "cursor is not a listing build position",
            ));
        }
        let (waiter, wait) = {
            let mut builds = self.source_builds()?;
            let ((selector, page_size, scan_budget), waiter) = builds.poll(cursor.id(), now)?;
            if binding.is_some_and(|binding| binding != (selector, *page_size, *scan_budget)) {
                return Err(Error::new(
                    "CONFLICT",
                    "cursor requires its original operation, selector and limits",
                ));
            }
            (waiter, builds.wait())
        };
        self.source_build_page(cursor.id(), &waiter, wait, now, cancellation)
    }

    fn source_reads(&self) -> Result<std::sync::MutexGuard<'_, Reads>, Error> {
        self.source_reads
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "source read registry poisoned"))
    }

    pub(super) fn source_builds(&self) -> Result<std::sync::MutexGuard<'_, Builds>, Error> {
        self.unlocked
            .source_builds
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "source listing builds poisoned"))
    }

    /// Wait for a source listing build, at most `wait`: its first page once it has finished,
    /// otherwise a page with no rows, its cursor and its progress.
    fn source_build_page(
        &self,
        id: &str,
        waiter: &listing_build::Waiter<Captured>,
        wait: std::time::Duration,
        now: u64,
        cancellation: &crate::QueryCancellation,
    ) -> Result<SourceReadPage, Error> {
        let Some(result) = waiter.wait(wait, cancellation)? else {
            let builds = self.source_builds()?;
            let (Some((_, page_size, scan_budget)), Some(lease_expires_at)) =
                (builds.context(id), builds.expires_at(id))
            else {
                return Err(Error::new("RESULT_EXPIRED", "listing build lease ended"));
            };
            return Ok(catalog_lease::CatalogReadPage {
                store_id: self.id.to_string(),
                snapshot_id: id.to_owned(),
                as_of: now,
                lease_expires_at,
                total_items: 0,
                page_start: 0,
                next_cursor: Some(format!("{id}/0")),
                page_size: *page_size,
                scan_budget: *scan_budget,
                scanned: 0,
                build: Some(waiter.progress()),
                body: SourcePageBody {
                    sources: Vec::new(),
                },
            });
        };
        #[cfg(feature = "test-support")]
        if self.source_builds()?.take_delivery_failure_for_tests() {
            return Err(Error::new("CANCELLED", "test: the page was not delivered"));
        }
        let mut snapshot = match result {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.source_builds()?.remove(id);
                return Err(error);
            }
        };
        // The capture's lease starts when its first page is delivered.
        snapshot.expires_at = now
            .checked_add(LEASE_MS)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "source read deadline overflow"))?;
        snapshot.started = Instant::now();
        // The build is retired only once its first page exists: a call that fails to publish
        // it leaves the finished build for the next call on the same cursor.
        let page = self.publish_sources(snapshot, now, cancellation)?;
        self.source_builds()?.remove(id);
        Ok(page)
    }

    fn publish_sources(
        &self,
        snapshot: Captured,
        now: u64,
        cancellation: &crate::QueryCancellation,
    ) -> Result<SourceReadPage, Error> {
        let mut reads = self.source_reads()?;
        reads.purge(now);
        reads.publish(
            snapshot,
            now,
            CAPTURE_BYTES,
            HANDLE_BYTES,
            MAX_HANDLES,
            cancellation,
        )
    }

    /// Capture every source row from one snapshot, including what the unfinished
    /// source-summary migration owes. Run as a build, it reports to `worker` each catalog
    /// entry the owed pass reads and how many it reads in all.
    fn source_capture(
        &self,
        query: &SourceRead,
        now: u64,
        (configured, intakes): (&[SourceWatchBinding], &[SourceSyslogBinding]),
        cancellation: &crate::QueryCancellation,
        worker: Option<&listing_build::Worker<Captured>>,
    ) -> Result<Captured, Error> {
        let expires_at = now
            .checked_add(LEASE_MS)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "source read deadline overflow"))?;
        // The registries are single bounded rows; read them, and take the snapshot every
        // other row is read from, at one instant under the writer lock. The lock is then
        // released, so a listing never holds back ingestion while it reads.
        let writer = self.lock()?;
        cancellation.checkpoint()?;
        let producer_bytes = writer
            .db
            .get(super::producer_stream::KEY)?
            .map_or(0, |bytes| bytes.len());
        let streams = super::producer_stream::registry(&writer)?;
        let receiver_bytes = writer
            .db
            .get(super::receiver_stream::KEY)?
            .map_or(0, |bytes| bytes.len());
        let windows = super::receiver_stream::registry(&writer)?;
        let tail_registry = writer.db.get(super::tail::KEY)?.ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "file-generation checkpoint registry missing",
            )
        })?;
        let store_id = writer.id.clone();
        let db = writer.db.clone();
        let view = db.snapshot();
        drop(writer);
        // While the source-summary migration is unfinished, the stored rows plus what its
        // remaining pages owe, read from the same snapshot, are the finished counts.
        let reader = super::store_counters::Reader::snapshot(&view);
        let summary = super::source_summary::progress(|key| reader.get(key.as_bytes()))?;
        if let Some(worker) = worker {
            worker.total(super::source_summary::owed_entries(&reader, &summary)?);
        }
        #[cfg(feature = "test-support")]
        self.source_read_paused()?;
        let owed = super::source_summary::Completion::owed(
            &reader,
            &summary,
            (0, Box::new(|_| false)),
            &mut || {
                cancellation.checkpoint()?;
                worker.map_or(Ok(()), listing_build::Worker::entry)
            },
        )?;
        // The capture's own admission time runs from here, after the owed pass.
        let started = Instant::now();
        let finished = |source: &str, stored: Option<super::source_summary::Counts>| match &owed {
            Some(owed) => owed.counts(source, stored),
            None => Ok(stored),
        };
        let mut scanned = 0;
        // The owed changes stay resident while the capture reads, so they are charged to it.
        let owed_bytes = owed
            .as_ref()
            .map_or(0, super::source_summary::Completion::bytes);
        let mut remaining = SCAN_BYTES.checked_sub(owed_bytes).ok_or_else(|| {
            Error::new(
                "RESOURCE_LIMIT",
                format!(
                    "the per-source counts the source-summary migration still owes take {owed_bytes} bytes, over the {SCAN_BYTES}-byte listing admission"
                ),
            )
        })?;
        let mut admission = catalog_lease::CaptureAdmission {
            cancellation,
            started,
            scan_budget: query.scan_budget,
            scanned: &mut scanned,
            remaining: &mut remaining,
        };
        let mut rows = BTreeMap::<String, SourceSummary>::new();
        let mut capacity = 4096;
        let mut counted = std::collections::BTreeSet::new();
        let mut count = |rows: &mut BTreeMap<String, SourceSummary>,
                         capacity: &mut usize,
                         counts: super::source_summary::Counts|
         -> Result<(), Error> {
            let wide = |value: u64| {
                usize::try_from(value).map_err(|_| {
                    Error::new("STORAGE_FAILURE", "per-source count exceeds address space")
                })
            };
            let row = admit_source(rows, &counts.source, capacity)?;
            row.complete_artifacts = wide(counts.complete_artifacts)?;
            row.fragment_parents = wide(counts.fragment_parents)?;
            row.retained_records = wide(counts.retained_records)?;
            counted.insert(counts.source);
            Ok(())
        };
        match &query.selector.source {
            // One point read.
            Some(source) => {
                let key = super::source_summary::summary_key(source);
                let stored = match view.get(&key)? {
                    Some(bytes) => {
                        admission.entry(bytes.len())?;
                        Some(super::source_summary::Counts::decode(
                            key.as_bytes(),
                            &bytes,
                        )?)
                    }
                    None => None,
                };
                if let Some(counts) = finished(source, stored)? {
                    count(&mut rows, &mut capacity, counts)?;
                }
            }
            // One walk over the summary rows, one entry each.
            None => {
                let prefix = super::source_summary::SUMMARY.as_bytes();
                let mut iterator = view.raw_iterator();
                iterator.seek(prefix);
                while iterator.valid() {
                    let key = iterator.key().ok_or_else(|| {
                        Error::new("STORAGE_FAILURE", "source counter iterator key missing")
                    })?;
                    if !key.starts_with(prefix) {
                        break;
                    }
                    let bytes = iterator.value().ok_or_else(|| {
                        Error::new("STORAGE_FAILURE", "source counter iterator value missing")
                    })?;
                    admission.entry(bytes.len())?;
                    let counts = super::source_summary::Counts::decode(key, bytes)?;
                    let source = counts.source.clone();
                    if let Some(counts) = finished(&source, Some(counts))? {
                        count(&mut rows, &mut capacity, counts)?;
                    }
                    iterator.next();
                }
                iterator.status()?;
                drop(iterator);
                // Sources the migration has not reached yet have no stored row.
                for source in owed.iter().flat_map(|owed| owed.sources()) {
                    let key = super::source_summary::summary_key(source);
                    if view.get(&key)?.is_none()
                        && let Some(counts) = finished(source, None)?
                    {
                        admission.entry(0)?;
                        count(&mut rows, &mut capacity, counts)?;
                    }
                }
            }
        }
        if configured.len() > 8 {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "source watch declarations exceed the runtime's eight-watch ceiling",
            ));
        }
        let mut watch_ids = std::collections::BTreeSet::new();
        for binding in intakes {
            admission.entry(binding.source.len() + binding.listener_id.len())?;
            if query
                .selector
                .source
                .as_ref()
                .is_none_or(|wanted| wanted == &binding.source)
            {
                let row = admit_source(&mut rows, &binding.source, &mut capacity)?;
                let planned = binding.listener_id.len() + std::mem::size_of::<String>();
                if capacity + planned > CAPTURE_BYTES {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "syslog listener references exceed source capture capacity",
                    ));
                }
                let before = row.capacity();
                row.syslog_listener_ids
                    .try_reserve_exact(1)
                    .map_err(|error| {
                        Error::new(
                            "RESOURCE_LIMIT",
                            format!("syslog listener reference allocation refused: {error}"),
                        )
                    })?;
                row.syslog_listener_ids.push(binding.listener_id.clone());
                capacity += row.capacity() - before;
                if capacity > CAPTURE_BYTES {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "allocated syslog listener references exceed source capture capacity",
                    ));
                }
            }
        }
        admission.entry(producer_bytes)?;
        for stream in streams {
            admission.entry(0)?;
            if stream.expires_at > now
                && query
                    .selector
                    .source
                    .as_ref()
                    .is_none_or(|wanted| wanted == &stream.source)
            {
                let row = admit_source(&mut rows, &stream.source, &mut capacity)?;
                capacity += stream.id.capacity() + std::mem::size_of::<String>();
                if capacity > CAPTURE_BYTES {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "producer stream references exceed source capture capacity",
                    ));
                }
                row.producer_stream_ids.push(stream.id);
            }
        }
        admission.entry(receiver_bytes)?;
        for window in windows {
            admission.entry(0)?;
            if window.expires_at > now
                && query
                    .selector
                    .source
                    .as_ref()
                    .is_none_or(|wanted| wanted == &window.source)
            {
                let row = admit_source(&mut rows, &window.source, &mut capacity)?;
                capacity += window.id.capacity() + std::mem::size_of::<String>();
                if capacity > CAPTURE_BYTES {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "receiver window references exceed source capture capacity",
                    ));
                }
                row.receiver_window_ids.push(window.id);
            }
        }
        admission.entry(tail_registry.len())?;
        for generation in super::tail::decode_registry(&tail_registry)? {
            admission.entry(0)?;
            if generation.expires_at.is_none_or(|deadline| deadline > now)
                && query
                    .selector
                    .source
                    .as_ref()
                    .is_none_or(|wanted| wanted == &generation.declaration.source)
            {
                let row = admit_source(&mut rows, &generation.declaration.source, &mut capacity)?;
                let planned = generation.id.capacity() + std::mem::size_of::<String>();
                if capacity + planned > CAPTURE_BYTES {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "tail generation references exceed source capture capacity",
                    ));
                }
                let before = row.capacity();
                row.tail_generation_ids
                    .try_reserve_exact(1)
                    .map_err(|error| {
                        Error::new(
                            "RESOURCE_LIMIT",
                            format!("tail generation reference allocation refused: {error}"),
                        )
                    })?;
                row.tail_generation_ids.push(generation.id);
                capacity += row.capacity() - before;
                if capacity > CAPTURE_BYTES {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "allocated tail generation references exceed source capture capacity",
                    ));
                }
            }
        }
        for binding in configured {
            if binding.source.is_empty()
                || binding.source.len() > 512
                || binding.watch_id.is_empty()
                || binding.watch_id.len() > 128
                || binding.revision == 0
                || !watch_ids.insert(&binding.watch_id)
            {
                return Err(Error::new(
                    "INVALID_ARGUMENT",
                    "source watch declarations require bounded identities, nonzero revisions and unique watch IDs",
                ));
            }
            admission.entry(0)?;
            if query
                .selector
                .source
                .as_ref()
                .is_none_or(|wanted| wanted == &binding.source)
            {
                let row = admit_source(&mut rows, &binding.source, &mut capacity)?;
                let planned = 256 + binding.watch_id.len();
                if capacity + planned > CAPTURE_BYTES {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "source watch bindings exceed capture capacity",
                    ));
                }
                capacity += planned;
                row.watches.push(SourceWatchInfo {
                    watch_id: binding.watch_id.clone(),
                    revision: binding.revision,
                    enabled: binding.enabled,
                });
            }
        }
        for prefix in [b"source-time:".as_slice(), b"source-diagnostic:".as_slice()] {
            let mut iterator = view.raw_iterator();
            iterator.seek(prefix);
            while iterator.valid() {
                let key = iterator.key().ok_or_else(|| {
                    Error::new("STORAGE_FAILURE", "source registry iterator key missing")
                })?;
                if !key.starts_with(prefix) {
                    break;
                }
                let bytes = iterator.value().ok_or_else(|| {
                    Error::new("STORAGE_FAILURE", "source registry iterator value missing")
                })?;
                admission.entry(bytes.len())?;
                let value = super::parse(bytes)?;
                let (source, id, expected) = if prefix == b"source-time:" {
                    let profile = source_time::SourceTimeProfile::decode(&value)?;
                    let expected = source_time::source_key(&profile.source);
                    (profile.source, profile.id, expected)
                } else {
                    let profile = diagnostic::DiagnosticProfile::decode(&value)?;
                    let expected = diagnostic::source_key(&profile.source);
                    (profile.source, profile.id, expected)
                };
                if expected.as_bytes() != key {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "source declaration registry identity mismatch",
                    ));
                }
                if query
                    .selector
                    .source
                    .as_ref()
                    .is_none_or(|wanted| wanted == &source)
                {
                    let row = admit_source(&mut rows, &source, &mut capacity)?;
                    capacity += id.capacity();
                    if capacity > CAPTURE_BYTES {
                        return Err(Error::new(
                            "RESOURCE_LIMIT",
                            "source mappings exceed capture capacity",
                        ));
                    }
                    if prefix == b"source-time:" {
                        row.time_profile_id = Some(id);
                    } else {
                        row.diagnostic_profile_id = Some(id);
                    }
                }
                iterator.next();
            }
            iterator.status()?;
        }
        // A source with counted entries always has its counter row; one probe of its
        // artifact index rows per listed or selected source the read did not count proves
        // it.
        let uncounted = rows
            .keys()
            .chain(query.selector.source.as_ref())
            .filter(|source| !counted.contains(*source))
            .collect::<std::collections::BTreeSet<_>>();
        for source in uncounted {
            admission.entry(0)?;
            let prefix = super::source_summary::index_prefix(source);
            let mut probe = view.raw_iterator();
            probe.seek(prefix.as_bytes());
            if probe.valid()
                && probe
                    .key()
                    .is_some_and(|key| key.starts_with(prefix.as_bytes()))
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    format!(
                        "source {source:?} has counted catalog entries but no per-source counter row"
                    ),
                ));
            }
            probe.status()?;
        }
        for row in rows.values_mut() {
            cancellation.checkpoint()?;
            row.tail_generation_ids.sort();
            row.syslog_listener_ids.sort();
            row.receiver_window_ids.sort();
            row.watches.sort_by(|a, b| a.watch_id.cmp(&b.watch_id));
        }
        let snapshot = catalog_lease::Snapshot {
            bytes: 0,
            id: random_id("source-read")?,
            store_id,
            selector: query.selector.clone(),
            as_of: now,
            expires_at,
            started,
            page_size: query.page_size,
            scan_budget: query.scan_budget,
            scanned,
            body: SourcePageBody {
                sources: rows.into_values().collect(),
            },
        };
        drop(view);
        Ok(snapshot)
    }
}
