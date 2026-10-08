// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Immutable pages from native JSON discovery, record matching and text ranking.

use super::{Error, Store, Value, catalog_lease, json, parse, random_id};
use std::time::Instant;

pub(super) const ROW_BYTES: usize = 32 * 1024;
pub(super) const METADATA_BYTES: usize = 32 * 1024;

/// Public query surface bound into every continuation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RecordOperation {
    /// Combined record qualification, including fields and source time.
    #[default]
    Records,
    /// Same record matcher and order, with frozen artifact membership counts.
    ArtifactGroups,
    /// Standalone native text search, sharing query capture and ranking.
    Text,
}

/// A complete bounded capture or a continuation with identical query semantics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordRead {
    /// Prevent replay through a different operation even with equivalent predicates.
    pub operation: RecordOperation,
    /// Membership, filters and acceptance interval; limit is the page item ceiling.
    pub scope: crate::QueryScope,
    /// Same-record predicates, source time and optional native text ranking.
    pub query: crate::RecordQuery,
    /// Opaque immutable continuation, or a fresh capture.
    pub cursor: Option<String>,
}

/// Immutable native JSON key/value discovery request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonRead {
    /// Membership and acceptance-time filters; limit is the page ceiling.
    pub scope: crate::QueryScope,
    /// Native occurrence predicate or key aggregation.
    pub query: crate::JsonQuery,
    /// Opaque continuation, or a fresh complete capture.
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Selector {
    Records(RecordRead),
    Json(JsonRead),
}
impl Selector {
    fn scope(&self) -> &crate::QueryScope {
        match self {
            Self::Records(q) => &q.scope,
            Self::Json(q) => &q.scope,
        }
    }
    fn cursor(&mut self) -> &mut Option<String> {
        match self {
            Self::Records(q) => &mut q.cursor,
            Self::Json(q) => &mut q.cursor,
        }
    }
}
impl catalog_lease::Selector for Selector {
    fn capacity(&self) -> usize {
        self.scope().capacity()
            + match self {
                Self::Records(q) => {
                    q.query.capacity() + q.cursor.as_ref().map_or(0, String::capacity)
                }
                Self::Json(q) => q.query.capacity() + q.cursor.as_ref().map_or(0, String::capacity),
            }
    }
}

/// Encoded native result metadata and bounded row bodies, without retained storage versions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordPageBody {
    metadata: Vec<u8>,
    rows: RowStorage,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RowStorage {
    Memory(Vec<Vec<u8>>),
    Spool(std::sync::Arc<super::record_spool::Rows>),
}
impl RowStorage {
    fn len(&self) -> usize {
        match self {
            Self::Memory(rows) => rows.len(),
            Self::Spool(rows) => rows.len(),
        }
    }
    fn lengths(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        match self {
            Self::Memory(rows) => Box::new(rows.iter().map(Vec::len)),
            Self::Spool(rows) => Box::new(rows.lengths()),
        }
    }
    fn capacity(&self) -> usize {
        match self {
            Self::Memory(rows) => {
                rows.capacity() * std::mem::size_of::<Vec<u8>>()
                    + rows.iter().map(Vec::capacity).sum::<usize>()
            }
            Self::Spool(rows) => rows.capacity(),
        }
    }
    fn page(&self, start: usize, end: usize) -> Result<Vec<Vec<u8>>, Error> {
        match self {
            Self::Memory(rows) => Ok(rows[start..end].to_vec()),
            Self::Spool(rows) => rows.page(start, end),
        }
    }
}

/// Host-owned query admission profile; native semantic profiles remain separate.
pub const QUERY_CAPTURE_PROFILE: &str = "sparrow-query-capture/4";

fn query_evidence(selector: &Selector, result: &Value) -> Result<Value, Error> {
    use purrdf_lex::json::Object;
    let mut profiles = vec![
        Value::from(
            Object::new()
                .with("role", "json_projection")
                .with("id", crate::projection::json_profile()?.identity().to_hex()),
        ),
        Value::from(
            Object::new()
                .with("role", "host_admission")
                .with("id", QUERY_CAPTURE_PROFILE),
        ),
    ];
    if let Some(ranking) = result.get("ranking") {
        for (role, field) in [
            ("text_analyzer", "analyzer"),
            ("text_ranking", "profile"),
            ("text_corpus", "corpus_profile"),
        ] {
            profiles.push(
                Object::new()
                    .with("role", role)
                    .with("id", super::read_field::<String>(ranking, field)?)
                    .into(),
            );
        }
    }
    let (json, fields, postings) = match selector {
        Selector::Json(_) => (true, false, false),
        Selector::Records(q) => (false, !q.query.fields.is_empty(), q.query.text.is_some()),
    };
    let budget = selector.scope().scan_budget as u64;
    Ok(Object::new()
        .with("profile_ids", Value::array(profiles))
        .with("native_governor", Value::Null)
        .with(
            "limits",
            Object::new()
                .with("candidate_visits", budget)
                .with(
                    "query_path_bytes",
                    if json {
                        Value::from(crate::JSON_QUERY_PATH_BYTES as u64)
                    } else {
                        Value::Null
                    },
                )
                .with(
                    "json_value_visits",
                    if json {
                        Value::from(budget)
                    } else {
                        Value::Null
                    },
                )
                .with("key_aggregation_bytes", if matches!(selector, Selector::Json(q) if matches!(q.query, crate::JsonQuery::Keys { .. })) { Value::from(catalog_lease::SCAN_BYTES as u64) } else { Value::Null })
                .with(
                    "field_visits",
                    if fields {
                        Value::from(budget)
                    } else {
                        Value::Null
                    },
                )
                .with(
                    "posting_visits",
                    if postings {
                        Value::from(budget)
                    } else {
                        Value::Null
                    },
                )
                .with(
                    "text_membership_bytes",
                    if postings {
                        Value::from(super::text_index::MATCH_MEMBERSHIP_BYTES as u64)
                    } else {
                        Value::Null
                    },
                )
                .with(
                    "query_text_bytes",
                    if postings {
                        Value::from(crate::query::RECORD_QUERY_TEXT_BYTES as u64)
                    } else {
                        Value::Null
                    },
                )
                .with(
                    "query_terms",
                    if postings {
                        Value::from(purrdf::text::QUERY_TERMS_MAX as u64)
                    } else {
                        Value::Null
                    },
                )
                .with(
                    "field_constraints",
                    if fields {
                        Value::from(crate::query::RECORD_QUERY_FIELDS_MAX as u64)
                    } else {
                        Value::Null
                    },
                )
                .with(
                    "compiled_field_bytes",
                    if fields {
                        Value::from(crate::query::RECORD_QUERY_FIELD_BYTES as u64)
                    } else {
                        Value::Null
                    },
                )
                .with(
                    "membership_member_visits",
                    super::query_membership::MAX_SELECTION_VISITS as u64,
                )
                .with(
                    "membership_record_visits",
                    super::query_membership::MAX_SELECTION_VISITS as u64,
                )
                .with("capture_items", catalog_lease::MAX_ITEMS as u64)
                .with("capture_bytes", super::record_spool::DISK_BYTES)
                .with("row_bytes", ROW_BYTES as u64)
                .with("metadata_bytes", METADATA_BYTES as u64)
                .with("page_bytes", catalog_lease::PAGE_BYTES as u64)
                .with("result_handles", catalog_lease::MAX_HANDLES as u64)
                .with("result_bytes", catalog_lease::HANDLE_BYTES as u64)
                .with("lease_ms", catalog_lease::LEASE_MS),
        )
        .into())
}

impl RecordPageBody {
    fn encode_metadata(result: &Value) -> Result<Vec<u8>, Error> {
        let bytes = json::write_compact(result).into_bytes();
        if bytes.len() > METADATA_BYTES {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "query capture metadata exceeds admission; no page published",
            ));
        }
        Ok(bytes)
    }

    /// Reconstruct only this admitted page through PurRDF's native JSON codec.
    pub fn result(&self) -> Result<Value, Error> {
        let mut result = parse(&self.metadata)?;
        let encoded = self.rows.page(0, self.rows.len())?;
        let rows = encoded
            .iter()
            .map(|bytes| parse(bytes))
            .collect::<Result<Vec<_>, _>>()?;
        result
            .as_object_mut()
            .ok_or_else(|| {
                Error::new(
                    "STORAGE_FAILURE",
                    "captured query metadata is not an object",
                )
            })?
            .insert("rows", Value::array(rows));
        Ok(result)
    }
}

impl catalog_lease::PageBody for RecordPageBody {
    const PAGE_ITEMS: usize = 1000;
    fn len(&self) -> usize {
        self.rows.len()
    }
    fn capacity(&self) -> usize {
        self.metadata.capacity() + self.rows.capacity()
    }
    fn page(&self, start: usize, end: usize) -> Result<Self, Error> {
        Ok(Self {
            metadata: self.metadata.clone(),
            rows: RowStorage::Memory(self.rows.page(start, end)?),
        })
    }
    fn page_end(&self, start: usize, page_size: usize) -> usize {
        // Typed replies render record rows and ranked evidence separately.
        let mut bytes = self.metadata.len() * 2 + 4096;
        let mut end = start;
        for length in self
            .rows
            .lengths()
            .skip(start)
            .take(page_size.min(Self::PAGE_ITEMS))
        {
            let charge = catalog_lease::response_row_bytes(length);
            if bytes + charge > catalog_lease::PAGE_BYTES {
                break;
            }
            bytes += charge;
            end += 1;
        }
        end
    }
    fn is_boundary(&self, start: usize, page_size: usize) -> bool {
        let mut position = 0;
        while position < start {
            let next = self.page_end(position, page_size);
            if next <= position {
                return false;
            }
            position = next;
        }
        position == start
    }
}

/// Captured query receipt and one page. Reading does not renew retention or the lease.
pub type RecordReadPage = catalog_lease::CatalogReadPage<RecordPageBody>;
pub(super) type Reads = catalog_lease::Reads<Selector, RecordPageBody>;

impl Store {
    /// Capture all admitted record matches once, then replay immutable bounded pages.
    pub fn records_read(&self, query: &RecordRead, now: u64) -> Result<RecordReadPage, Error> {
        query.scope.validate()?;
        query.query.validate()?;
        if query.operation == RecordOperation::Text
            && (query.query.text.is_none()
                || !query.query.fields.is_empty()
                || query.query.event_interval.is_some())
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "standalone text capture requires text and admits no record fields or event interval",
            ));
        }
        self.query_read(Selector::Records(query.clone()), now)
    }

    /// Capture complete native JSON discovery through the shared bounded lease owner.
    pub fn json_read(&self, query: &JsonRead, now: u64) -> Result<RecordReadPage, Error> {
        query.scope.validate()?;
        query.query.predicate()?.validate()?;
        self.query_read(Selector::Json(query.clone()), now)
    }

    fn query_read(&self, mut selector: Selector, now: u64) -> Result<RecordReadPage, Error> {
        let cursor = selector.cursor().take();
        if cursor.as_ref().is_some_and(|c| c.len() > 160) {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "query cursor exceeds 160 bytes",
            ));
        }
        let scope = selector.scope().clone();
        let mut reads = self
            .record_reads
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "query read registry poisoned"))?;
        reads.purge(now);
        if let Some(cursor) = &cursor {
            return reads.resume(&selector, cursor, scope.limit, scope.scan_budget, now);
        }
        let started = Instant::now();
        let expires_at = now
            .checked_add(catalog_lease::LEASE_MS)
            .ok_or_else(|| Error::new("INVALID_ARGUMENT", "query read deadline overflow"))?;
        crate::format_instant(now)?;
        crate::format_instant(expires_at)?;
        let mut staged = super::record_spool::Builder::new(
            &*self.lock()?,
            matches!(&selector, Selector::Records(q) if q.operation == RecordOperation::ArtifactGroups),
            matches!(&selector, Selector::Records(q) if q.query.text.is_none() && q.query.event_interval.is_some()),
        )?;
        let mut value = match &selector {
            Selector::Records(q) => {
                self.records_query_stage(&q.scope, &q.query, now, true, Some(&mut staged))?
            }
            Selector::Json(q) => {
                self.json_query_stage(&q.scope, &q.query, now, Some(&mut staged))?
            }
        };
        let super::record_spool::Finished { rows, groups } = staged.finish()?;
        if let Some(counts) = groups {
            let mut groups = purrdf_lex::json::Object::new();
            for (artifact, count) in counts {
                groups.insert(artifact, Value::from(count));
            }
            value
                .as_object_mut()
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "record capture is not an object"))?
                .insert("artifact_group_counts", groups);
        }
        let evidence = query_evidence(&selector, &value)?;
        value
            .as_object_mut()
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "query result is not an object"))?
            .insert("query_evidence", evidence);
        let store_id = super::read_field::<String>(&value, "store_id")?;
        let scanned = usize::try_from(super::integer(&value["coverage"], "visited")?)
            .map_err(|_| Error::new("STORAGE_FAILURE", "query capture scan count overflow"))?;
        let body = {
            value
                .as_object_mut()
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "record capture is not an object"))?
                .remove("rows")
                .ok_or_else(|| {
                    Error::new("STORAGE_FAILURE", "staged record rows marker missing")
                })?;
            let metadata = RecordPageBody::encode_metadata(&value)?;
            RecordPageBody {
                metadata,
                rows: RowStorage::Spool(rows),
            }
        };
        reads.publish(
            catalog_lease::Snapshot {
                bytes: 0,
                id: random_id("record-read")?,
                store_id,
                selector,
                as_of: now,
                expires_at,
                started,
                page_size: scope.limit,
                scan_budget: scope.scan_budget,
                scanned,
                body,
            },
            now,
            catalog_lease::CAPTURE_BYTES,
            catalog_lease::HANDLE_BYTES,
            catalog_lease::MAX_HANDLES,
        )
    }

    pub(super) fn record_read_stats(&self, now: u64) -> Result<(usize, usize), Error> {
        let mut reads = self
            .record_reads
            .lock()
            .map_err(|_| Error::new("STORAGE_FAILURE", "query read registry poisoned"))?;
        reads.purge(now);
        Ok((reads.snapshots.len(), reads.capacity()))
    }
}
