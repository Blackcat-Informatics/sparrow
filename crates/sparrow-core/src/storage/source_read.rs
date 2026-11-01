// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Immutable discovery of intake keys, retained provenance and durable mappings.

use super::{
    Catalog, Error, Store, artifact_read::ArtifactSelector, catalog_lease, diagnostic, random_id,
    source_time,
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
    /// Eligible complete artifact occurrences.
    pub complete_artifacts: usize,
    /// Parents with retained records whose complete bytes are unavailable.
    pub fragment_parents: usize,
    /// Exact number of eligible record occurrences.
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
        let mut reads = self
            .source_reads
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "source read registry poisoned"))?;
        reads.purge(now);
        cancellation.checkpoint()?;
        if let Some(cursor) = &query.cursor {
            return reads.resume(
                &query.selector,
                cursor,
                query.page_size,
                query.scan_budget,
                now,
            );
        }
        let started = Instant::now();
        let expires_at = now
            .checked_add(LEASE_MS)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "source read deadline overflow"))?;
        crate::format_instant(now)?;
        crate::format_instant(expires_at)?;
        let writer = self.lock()?;
        cancellation.checkpoint()?;
        let mut scanned = 0;
        let mut remaining = SCAN_BYTES;
        let mut admission = catalog_lease::CaptureAdmission {
            cancellation,
            started,
            scan_budget: query.scan_budget,
            scanned: &mut scanned,
            remaining: &mut remaining,
        };
        let artifacts = writer.artifact_capture(
            &ArtifactSelector {
                source: query.selector.source.clone(),
                ..Default::default()
            },
            now,
            &mut admission,
        )?;
        let mut rows = BTreeMap::<String, SourceSummary>::new();
        let mut capacity = 4096;
        for artifact in artifacts.artifacts {
            cancellation.checkpoint()?;
            let row = admit_source(&mut rows, &artifact.source, &mut capacity)?;
            row.complete_artifacts += usize::from(artifact.complete_bytes_available);
            row.fragment_parents += usize::from(!artifact.complete_bytes_available);
            row.retained_records += artifact.retained_records;
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
        admission.entry(
            writer
                .db
                .get(super::producer_stream::KEY)?
                .map_or(0, |bytes| bytes.len()),
        )?;
        for stream in super::producer_stream::registry(&writer)? {
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
        admission.entry(
            writer
                .db
                .get(super::receiver_stream::KEY)?
                .map_or(0, |bytes| bytes.len()),
        )?;
        for window in super::receiver_stream::registry(&writer)? {
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
        let tail_registry = writer.db.get(super::tail::KEY)?.ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                "file-generation checkpoint registry missing",
            )
        })?;
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
            let mut iterator = writer.db.raw_iterator();
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
            store_id: writer.id.clone(),
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
        drop(writer);
        reads.publish(
            snapshot,
            now,
            CAPTURE_BYTES,
            HANDLE_BYTES,
            MAX_HANDLES,
            cancellation,
        )
    }
}
