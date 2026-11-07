// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Persistent native token evidence and stored-fact scoring.

use super::*;
use purrdf::text::{FieldInput, PreparedCorpus, RankingProfile};

use purrdf::sparql::{AdmittedOrderedMap, AdmittedVec, EvalError, WorkspaceCapability};
use purrdf_lex::allocation::SharedText;

pub(super) const MATCH_MEMBERSHIP_BYTES: usize = 64 * 1024 * 1024;

pub(super) struct TextMetadata {
    pub length: u64,
    pub failed: bool,
    pub not_applicable: bool,
}

pub(super) fn text_metadata(
    facts: &Value,
    analyzer_id: &str,
    strict: bool,
) -> Result<TextMetadata, Error> {
    if facts["profile"].as_str() != Some(crate::text::PROFILE)
        || facts["analyzer"].as_str() != Some(analyzer_id)
    {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "stored text profile or analyzer disagrees with the store",
        ));
    }
    let state = facts["state"]
        .as_str()
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "text projection state missing"))?;
    let (failed, not_applicable) = match state {
        "complete" => (false, false),
        "incomplete" | "failed" => (true, false),
        "not_applicable" => (false, true),
        _ => {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "unknown text projection state",
            ));
        }
    };
    let length = integer(facts, "length")?;
    if length > crate::text::MAX_OCCURRENCES
        || ((not_applicable || state == "failed") && length != 0)
    {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "stored text length violates its declared profile",
        ));
    }
    if strict && failed {
        return Err(Error::new(
            "INDEX_NOT_READY",
            "selected text projection unavailable",
        ));
    }
    Ok(TextMetadata {
        length,
        failed,
        not_applicable,
    })
}

/// The original corpus preimage, streamed through the native framing owner.
pub(super) fn corpus_identity<'a>(
    store_id: &str,
    analyzer: &[u8; 32],
    documents: impl IntoIterator<Item = (&'a str, u64)>,
    mut checkpoint: impl FnMut() -> Result<(), Error>,
) -> Result<String, Error> {
    let mut digest = purrdf_hash::blake3::Hasher::new();
    for part in [
        store_id.as_bytes(),
        crate::text::PROFILE.as_bytes(),
        purrdf::text::RANKING_PROFILE_ID.as_bytes(),
        analyzer,
    ] {
        purrdf_hash::frame::frame_le_into(&mut digest, part);
    }
    let mut previous = None;
    for (id, length) in documents {
        checkpoint()?;
        if previous.is_some_and(|previous| previous >= id) {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "captured text corpus is not in strict identity order",
            ));
        }
        previous = Some(id);
        purrdf_hash::frame::frame_le_into(&mut digest, id.as_bytes());
        purrdf_hash::frame::frame_le_into(&mut digest, &length.to_be_bytes());
    }
    Ok(purrdf_hash::hex::encode(digest.finalize().as_bytes()))
}

/// Preserve the original source/native failure or the context's governor stop.
#[derive(Debug)]
pub(super) enum RankFailure {
    Source(Error),
    Native(EvalError),
    Stopped,
}

impl From<Error> for RankFailure {
    fn from(value: Error) -> Self {
        Self::Source(value)
    }
}

impl From<EvalError> for RankFailure {
    fn from(value: EvalError) -> Self {
        Self::Native(value)
    }
}

impl RankFailure {
    fn ordinary(self) -> Error {
        match self {
            Self::Source(error) => error,
            Self::Native(error) => Error::new(
                error.diagnostic_code().unwrap_or("NATIVE_TEXT_FAILURE"),
                error.to_string(),
            ),
            Self::Stopped => Error::new(
                "STORAGE_FAILURE",
                "ranker stopped without an invoking native governor context",
            ),
        }
    }
}

/// One finite source probe or scoring step, charged before the work starts.
pub(super) enum PostingWork {
    Probe { bytes: u64 },
    Score,
}

/// Counts over the complete declared corpus, independently of matching postings.
pub(super) struct CorpusStatistics {
    pub documents: u64,
    pub tokens: u128,
}

/// Native scores and matched-term ordinals, retaining their original grants.
pub(super) struct RankedPostings {
    pub scored: AdmittedVec<(purrdf::text::BoundedScore, SharedText)>,
    pub candidates: AdmittedOrderedMap<SharedText, AdmittedOrderedMap<usize, u64>>,
    pub posting_visits: usize,
}

/// One ranker for ordinary reads and query-scoped stored-text producers.
/// PurRDF owns all numerical scoring and admitted collection growth.
pub(super) fn rank_postings(
    snapshot: &rocksdb::Snapshot<'_>,
    terms: &[String],
    statistics: CorpusStatistics,
    document_length: impl Fn(&str) -> Option<u64>,
    scan_budget: usize,
    workspace: &WorkspaceCapability,
    mut checkpoint: impl FnMut(PostingWork) -> Result<(), RankFailure>,
) -> Result<RankedPostings, RankFailure> {
    let mut candidates =
        AdmittedOrderedMap::<SharedText, AdmittedOrderedMap<usize, u64>>::new(workspace);
    let mut frequencies = AdmittedVec::with_capacity(terms.len(), workspace)?;
    for _ in terms {
        frequencies.push(0_u64)?;
    }
    let mut posting_visits = 0_usize;
    for (ordinal, term) in terms.iter().enumerate() {
        let mut reader = PostingReader::new(snapshot, term, workspace)?;
        while reader
            .read_next(
                |id| document_length(id).is_some(),
                scan_budget,
                &mut posting_visits,
                &mut checkpoint,
                |id, frequency| {
                    frequencies[ordinal] += 1;
                    if candidates.get(id).is_none() {
                        candidates.insert(
                            workspace.authored_text(id)?,
                            AdmittedOrderedMap::new(workspace),
                        )?;
                    }
                    candidates
                        .get_mut(id)
                        .expect("candidate inserted")
                        .insert(ordinal, frequency)?;
                    Ok(())
                },
            )?
            .is_some()
        {}
    }
    // The public scorer owns its preparation. Its one-field fixed carrier and
    // copied term spellings remain beneath this caller-held workspace grant.
    // Collection buffers around it use the native admitted owners directly.
    let preparation_bytes = terms
        .iter()
        .try_fold(4096_u64, |bytes, term| {
            bytes.checked_add(512)?.checked_add(term.len() as u64)
        })
        .ok_or(EvalError::WorkspaceBoundOverflow)?;
    let _preparation = workspace.charge(preparation_bytes)?;
    let profile = RankingProfile::single_field();
    let corpus = PreparedCorpus::new(&profile, statistics.documents, &[statistics.tokens])
        .map_err(EvalError::from)?;
    let mut prepared_frequencies = AdmittedVec::with_capacity(terms.len(), workspace)?;
    for (term, frequency) in terms.iter().zip(frequencies.iter()) {
        prepared_frequencies.push((term.as_str(), *frequency))?;
    }
    let query = corpus
        .prepare_query(&prepared_frequencies)
        .map_err(EvalError::from)?;
    let mut scored = AdmittedVec::with_capacity(candidates.len(), workspace)?;
    for (id, matches) in candidates.iter() {
        checkpoint(PostingWork::Score)?;
        let length = document_length(id)
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "captured text document disappeared"))?;
        let input_bytes = (terms.len() as u64)
            .checked_mul(size_of::<FieldInput>() as u64)
            .ok_or(EvalError::WorkspaceBoundOverflow)?;
        let _inputs = workspace.charge(input_bytes)?;
        let mut fields = AdmittedVec::with_capacity(terms.len(), workspace)?;
        for ordinal in 0..terms.len() {
            let mut field = Vec::new();
            field
                .try_reserve_exact(1)
                .map_err(|_| EvalError::AllocationFailed {
                    construct: "stored-text field input",
                })?;
            field.push(FieldInput {
                term_frequency: matches.get(&ordinal).copied().unwrap_or(0),
                length,
            });
            fields.push(field)?;
        }
        let score = query.score(&fields).map_err(EvalError::from)?;
        scored.push((score, id.clone()))?;
    }
    scored.sort_unstable_by(|a, b| b.0.value.cmp(&a.0.value).then_with(|| a.1.cmp(&b.1)));
    Ok(RankedPostings {
        scored,
        candidates,
        posting_visits,
    })
}

/// One physical posting decoder for ranking and streaming source spans.
pub(super) struct PostingReader<'view, 'db> {
    snapshot: &'view rocksdb::Snapshot<'db>,
    cursor: Option<rocksdb::DBRawIteratorWithThreadMode<'view, rocksdb::DB>>,
    prefix: String,
    workspace: WorkspaceCapability,
    _control: purrdf::sparql::WorkspaceAllocation,
    finished: bool,
}

impl<'view, 'db> PostingReader<'view, 'db> {
    pub(super) fn new(
        snapshot: &'view rocksdb::Snapshot<'db>,
        term: &str,
        workspace: &WorkspaceCapability,
    ) -> Result<Self, EvalError> {
        let control =
            workspace.charge(4096_u64.saturating_add((term.len() as u64).saturating_mul(4)))?;
        Ok(Self {
            snapshot,
            cursor: None,
            prefix: term_key("post", term, ""),
            workspace: workspace.clone(),
            _control: control,
            finished: false,
        })
    }

    pub(super) fn read_next<T>(
        &mut self,
        selected: impl Fn(&str) -> bool,
        scan_budget: usize,
        visits: &mut usize,
        mut checkpoint: impl FnMut(PostingWork) -> Result<(), RankFailure>,
        deliver: impl FnOnce(&str, u64) -> Result<T, RankFailure>,
    ) -> Result<Option<T>, RankFailure> {
        const POSTING_BYTES: usize = 64;
        if self.finished {
            return Ok(None);
        }
        loop {
            checkpoint(PostingWork::Probe {
                bytes: (self.prefix.len() as u64).saturating_add(128 + POSTING_BYTES as u64),
            })?;
            match self.cursor.as_mut() {
                Some(cursor) => cursor.next(),
                None => {
                    let mut cursor = self.snapshot.raw_iterator();
                    cursor.seek(self.prefix.as_bytes());
                    self.cursor = Some(cursor);
                }
            }
            let cursor = self.cursor.as_ref().expect("posting cursor initialized");
            if !cursor.valid() {
                self.finished = true;
                cursor.status().map_err(Error::from)?;
                return Ok(None);
            }
            let key = cursor
                .key()
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "text posting key missing"))?;
            if !key.starts_with(self.prefix.as_bytes()) {
                self.finished = true;
                return Ok(None);
            }
            if *visits == scan_budget {
                return Err(Error::new("RESOURCE_LIMIT", "posting scan budget exhausted; narrow source/time scope or raise scan_budget; no successful prefix returned").into());
            }
            *visits += 1;
            if key.len() > self.prefix.len().saturating_add(128) {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "text posting identity exceeds its encoding bound",
                )
                .into());
            }
            let id = std::str::from_utf8(&key[self.prefix.len()..])
                .map_err(|_| Error::new("STORAGE_FAILURE", "invalid posting identity"))?;
            let bytes = cursor
                .value()
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "text posting value missing"))?;
            if bytes.len() > POSTING_BYTES {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "text posting exceeds its 64-byte encoding bound",
                )
                .into());
            }
            if !selected(id) {
                continue;
            }
            let _posting = self.workspace.charge((POSTING_BYTES * 64) as u64)?;
            let posting = parse(bytes)?;
            let frequency = integer(&posting, "frequency")?;
            if frequency == 0 {
                return Err(Error::new("STORAGE_FAILURE", "zero-frequency text posting").into());
            }
            return deliver(id, frequency).map(Some);
        }
    }
}

pub(super) fn term_key(kind: &str, term: &str, id: &str) -> String {
    format!("text-{kind}:{}:{id}", hex::encode(term.as_bytes()))
}

impl Writer {
    pub(super) fn put_text(
        &self,
        batch: &mut WriteBatch,
        id: &str,
        facts: &Value,
    ) -> Result<(), Error> {
        let encoded = json::write_compact(facts);
        // Refuse publication if this representation cannot be decoded by the catalog codec.
        parse(encoded.as_bytes())?;
        let terms = field(facts, "terms")?
            .as_array()
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "text terms missing"))?;
        let names = terms
            .iter()
            .map(|term| read_field::<String>(term, "term"))
            .collect::<Result<Vec<_>, _>>()?;
        let mut metadata = Object::new()
            .with("state", read_field::<String>(facts, "state")?)
            .with("length", integer(facts, "length")?.to_string())
            .with("profile", read_field::<String>(facts, "profile")?)
            .with("analyzer", read_field::<String>(facts, "analyzer")?)
            .with("terms", names);
        if let Some(diagnostic) = facts.get("diagnostic") {
            metadata = metadata.with("diagnostic", diagnostic.clone());
        }
        batch.put(
            format!("text-record:{id}"),
            json::write_compact(&Value::from(metadata)),
        );
        for term in terms {
            let name = read_field::<String>(term, "term")?;
            let frequency: Value = Object::new()
                .with("frequency", integer(term, "frequency")?.to_string())
                .into();
            batch.put(term_key("post", &name, id), json::write_compact(&frequency));
            batch.put(term_key("evidence", &name, id), json::write_compact(term));
        }
        Ok(())
    }

    pub(super) fn remove_text(&self, batch: &mut WriteBatch, id: &str) -> Result<(), Error> {
        let key = format!("text-record:{id}");
        if let Some(bytes) = self.db.get(&key)? {
            let facts = parse(&bytes)?;
            for term in read_field::<Vec<String>>(&facts, "terms")? {
                batch.delete(term_key("post", &term, id));
                batch.delete(term_key("evidence", &term, id));
            }
            batch.delete(key);
        }
        Ok(())
    }
}

impl Store {
    /// Search persisted lexical postings with native BM25 scoring over captured membership.
    /// Results identify limited membership/posting scans and projection failures explicitly.
    /// No retained payload is read or analyzed during this operation.
    pub fn text_query(
        &self,
        scope: &crate::QueryScope,
        input: &str,
        now: u64,
    ) -> Result<Value, Error> {
        self.records_query(
            scope,
            &crate::RecordQuery {
                text: Some(input.to_owned()),
                ..Default::default()
            },
            now,
        )
    }

    /// Qualify predicates on the same record before native ranking. Without text,
    /// return chronological summaries and native field witnesses without scores.
    pub fn records_query(
        &self,
        scope: &crate::QueryScope,
        query: &crate::RecordQuery,
        now: u64,
    ) -> Result<Value, Error> {
        self.records_query_capture(scope, query, now, false)
    }

    pub(super) fn records_query_capture(
        &self,
        scope: &crate::QueryScope,
        query: &crate::RecordQuery,
        now: u64,
        capture: bool,
    ) -> Result<Value, Error> {
        self.records_query_stage(scope, query, now, capture, None)
    }

    pub(super) fn records_query_stage(
        &self,
        scope: &crate::QueryScope,
        query: &crate::RecordQuery,
        now: u64,
        capture: bool,
        mut stage: Option<&mut super::record_spool::Builder>,
    ) -> Result<Value, Error> {
        scope.validate()?;
        query.validate()?;
        if stage.is_some() && !capture {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "disk staging requires complete record qualification",
            ));
        }
        let row_limit = if capture {
            if stage.is_some() {
                super::record_read::disk_capture_items(Some(query), scope.scan_budget)
            } else {
                catalog_lease::MAX_ITEMS
            }
        } else {
            scope.limit
        };
        let strict = scope.require_complete_index || query.require_complete_index;
        let mut diagnostics = super::diagnostic::DiagnosticCoverage::default();
        let writer = self.lock()?;
        let analyzer_id = hex::encode(&writer.config.text_analyzer.fingerprint());
        if let Some(stage) = stage.as_deref() {
            stage.checkpoint()?;
        }
        let mut terms = match &query.text {
            Some(input) => writer
                .config
                .text_analyzer
                .terms(input)
                .map_err(|e| Error::new("INVALID_ARGUMENT", e.to_string()))?,
            None => Vec::new(),
        };
        terms.sort();
        terms.dedup();
        if query.text.is_some() && terms.is_empty() {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "text query requires at least one analyzed term",
            ));
        }
        let mut field_coverage = record_fields::FieldCoverage::default();
        let mut witnesses = BTreeMap::<String, Vec<Value>>::new();
        let mut chronological = Vec::<(u64, String, Value)>::new();
        let mut event_unknown = 0_u64;
        let mut event_evaluated = 0_u64;
        let mut event_outside = 0_u64;
        let mut event_fallback = 0_u64;
        let time_basis = if query.event_interval.is_some() {
            "event_time"
        } else {
            "received_at"
        };
        let mut qualified = 0_usize;
        let mut capture_bytes = 0_usize;
        let protected = writer.protected(now)?;
        let mut documents = BTreeMap::<String, (Record, u64)>::new();
        let mut visited = 0_usize;
        let mut eligible = 0_u64;
        let mut failed = 0_u64;
        let mut not_applicable = 0_u64;
        let mut total = 0_u128;
        let mut limited = false;
        let captured = writer.query_candidates(scope, now, &protected)?;
        if let Some(stage) = stage.as_deref_mut() {
            stage.scope = Some(captured.scope);
        }
        for record in captured.records {
            if let Some(stage) = stage.as_deref() {
                stage.checkpoint()?;
            }
            let record = record?;
            let id = &record.id;
            if record.received_at >= scope.received_before {
                break;
            }
            if visited == scope.scan_budget {
                if capture {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "record capture exhausted candidate scan_budget; narrow scope or raise the budget; no page published",
                    ));
                }
                limited = true;
                break;
            }
            visited += 1;
            if !writer.live_record(&record, now, &protected)
                || !scope.matches_source(&record.source)
                || !writer.matches_diagnostic_scope(scope, &record, &mut diagnostics, strict)?
            {
                continue;
            }
            event_evaluated += 1;
            writer.validate_event_record(&record)?;
            if record.event_time.millis().is_none() {
                event_unknown += 1;
            }
            if let Some(interval) = query.event_interval {
                if strict
                    && matches!(&record.event_time, crate::EventTime::Unknown { reason, .. } if reason == "projection_failed")
                {
                    return Err(Error::new(
                        "INDEX_NOT_READY",
                        "selected source-time profile depends on a failed JSON projection",
                    ));
                }
                let millis = record.event_time.placement(record.received_at).millis;
                if millis < interval.from || millis >= interval.before {
                    event_outside += 1;
                    continue;
                }
            }
            eligible += 1;
            let Some(fields) = writer.qualify_fields(
                &record,
                &query.fields,
                &mut field_coverage,
                scope.scan_budget,
                strict,
            )?
            else {
                continue;
            };
            qualified += 1;
            if query.event_interval.is_some()
                && record.event_time.placement(record.received_at).basis
                    == crate::EventTimeBasis::Receipt
            {
                event_fallback += 1;
            }
            if capture && query.text.is_none() && qualified > row_limit {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    format!("record capture exceeds {row_limit} matches; narrow scope"),
                ));
            }
            if query.text.is_none() {
                if let Some(stage) = stage.as_deref_mut() {
                    let order = if query.event_interval.is_some() {
                        Some((
                            record.event_time.placement(record.received_at).millis,
                            record.id.clone(),
                        ))
                    } else {
                        None
                    };
                    stage.push(
                        record_fields::matched_record_row(&record, fields)?.into(),
                        order,
                    )?;
                    continue;
                }
                if query.event_interval.is_some() || chronological.len() < row_limit {
                    let row: Value = record_fields::matched_record_row(&record, fields)?.into();
                    capture_bytes += json::write_compact(&row).len();
                    if capture_bytes
                        > if query.event_interval.is_some() && !capture {
                            MATCH_MEMBERSHIP_BYTES
                        } else {
                            2 * 1024 * 1024
                        }
                    {
                        return Err(Error::new(
                            "RESOURCE_LIMIT",
                            "chronological membership capture exceeds its byte ceiling; narrow scope or lower limit",
                        ));
                    }
                    chronological.push((
                        if query.event_interval.is_some() {
                            record.event_time.placement(record.received_at).millis
                        } else {
                            record.received_at
                        },
                        record.id.clone(),
                        row,
                    ));
                }
                continue;
            }
            let bytes = writer
                .db
                .get(format!("text-record:{id}"))?
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "live text projection missing"))?;
            let facts = parse(&bytes)?;
            let metadata = text_metadata(&facts, &analyzer_id, strict)?;
            failed += u64::from(metadata.failed);
            not_applicable += u64::from(metadata.not_applicable);
            let length = metadata.length;
            if length > 0 {
                total += u128::from(length);
                capture_bytes = capture_bytes
                    .saturating_add(json::write_compact(&record.encode()).len())
                    .saturating_add(
                        fields
                            .iter()
                            .map(|field| json::write_compact(field).len())
                            .sum::<usize>()
                            + 1024,
                    );
                if capture_bytes > MATCH_MEMBERSHIP_BYTES {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "record membership capture exceeds 64 MiB; narrow scope",
                    ));
                }
                witnesses.insert(id.to_owned(), fields);
                documents.insert(id.to_owned(), (record, length));
            }
        }
        let membership_limited = limited;
        let event_coverage = Object::new()
            .with("evaluated", event_evaluated.to_string())
            .with("unknown", event_unknown.to_string())
            .with("outside_interval", event_outside.to_string())
            .with("membership_complete", diagnostics.complete(limited));
        if query.text.is_none() {
            limited |= qualified > row_limit;
            chronological.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            let mut response_bytes = 0_usize;
            let mut selected_rows = Vec::new();
            for (_, _, row) in chronological.into_iter().take(row_limit) {
                response_bytes += json::write_compact(&row).len();
                if response_bytes > 2 * 1024 * 1024 {
                    if capture {
                        return Err(Error::new(
                            "RESOURCE_LIMIT",
                            "record result capture exceeds 2 MiB; narrow scope",
                        ));
                    }
                    limited = true;
                    break;
                }
                selected_rows.push(row);
            }
            return Ok(Object::new()
                .with("store_id", writer.id.clone())
                .with("membership_at", crate::format_instant(now)?)
                .with("time_basis", time_basis)
                .with("execution", if limited { "limited" } else { "complete" })
                .with(
                    "certainty",
                    if !diagnostics.complete(limited)
                        || field_coverage.failed > 0
                        || field_coverage.decoding_failures > 0
                    {
                        "incomplete"
                    } else {
                        "exact"
                    },
                )
                .with(
                    "coverage",
                    diagnostics.encode(
                        visited as u64,
                        eligible,
                        [
                            field_coverage.failed,
                            field_coverage.not_applicable,
                            field_coverage.decoding_failures,
                        ],
                        membership_limited,
                    ),
                )
                .with("field_visits", field_coverage.visits.to_string())
                .with(
                    "qualified_records",
                    diagnostics.count(
                        qualified as u64,
                        membership_limited
                            || field_coverage.failed > 0
                            || field_coverage.decoding_failures > 0,
                    ),
                )
                .with("known_qualified_records", qualified.to_string())
                .with("event_time_fallback", event_fallback.to_string())
                .with("event_coverage", event_coverage)
                .with("rows", Value::array(selected_rows))
                .into());
        }
        let membership_complete = diagnostics.complete(limited)
            && failed == 0
            && not_applicable == 0
            && field_coverage.failed == 0
            && field_coverage.decoding_failures == 0;
        let corpus_id = corpus_identity(
            &writer.id,
            &writer.config.text_analyzer.fingerprint(),
            documents
                .iter()
                .map(|(id, (_, length))| (id.as_str(), *length)),
            || {
                if let Some(stage) = stage.as_deref() {
                    stage.checkpoint()?;
                }
                Ok(())
            },
        )?;
        let snapshot = writer.db.snapshot();
        let RankedPostings {
            scored,
            candidates,
            posting_visits,
        } = rank_postings(
            &snapshot,
            &terms,
            CorpusStatistics {
                documents: documents.len() as u64,
                tokens: total,
            },
            |id| documents.get(id).map(|(_, length)| *length),
            scope.scan_budget,
            &WorkspaceCapability::resident(),
            |_| {
                if let Some(stage) = stage.as_deref() {
                    stage.checkpoint()?;
                }
                Ok(())
            },
        )
        .map_err(RankFailure::ordinary)?;
        if capture && scored.len() > row_limit {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "ranked record capture exceeds 10000 matches; narrow scope",
            ));
        }
        limited |= scored.len() > row_limit;
        let mut rows = Vec::new();
        let mut response_bytes = 0_usize;
        for (score, id) in scored.into_iter().take(row_limit) {
            let (record, _) = &documents[id.as_str()];
            let mut evidence = Vec::<Value>::new();
            let mut evidence_limited = false;
            for ordinal in candidates
                .get(id.as_str())
                .expect("ranked candidate")
                .keys()
            {
                if evidence.len() == 64 {
                    evidence_limited = true;
                    break;
                }
                let term = &terms[*ordinal];
                let bytes = snapshot
                    .get(term_key("evidence", term, &id))?
                    .ok_or_else(|| {
                        Error::new("STORAGE_FAILURE", "text posting evidence missing")
                    })?;
                let posting = parse(&bytes)?;
                let spans = field(&posting, "spans")?
                    .as_array()
                    .ok_or_else(|| Error::new("STORAGE_FAILURE", "posting spans missing"))?;
                for span in spans {
                    if evidence.len() == 64 {
                        evidence_limited = true;
                        break;
                    }
                    evidence.push(
                        Object::new()
                            .with("term", term.clone())
                            .with("span", span.clone())
                            .with(
                                "source_start",
                                (record.byte_start + integer(span, "start")?).to_string(),
                            )
                            .with(
                                "source_end",
                                (record.byte_start + integer(span, "end")?).to_string(),
                            )
                            .into(),
                    );
                }
            }
            let row: Value = record_fields::matched_record_row(
                record,
                witnesses.remove(id.as_str()).ok_or_else(|| {
                    Error::new("STORAGE_FAILURE", "captured field witnesses missing")
                })?,
            )?
            .with("score", score.value.to_decimal_lexical())
            .with("evidence", Value::array(evidence))
            .with("evidence_limited", evidence_limited)
            .into();
            if let Some(stage) = stage.as_deref_mut() {
                stage.push(row, None)?;
                continue;
            }
            response_bytes += json::write_compact(&row).len();
            if response_bytes > 2 * 1024 * 1024 {
                if capture {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "ranked result capture exceeds 2 MiB; narrow scope",
                    ));
                }
                limited = true;
                break;
            }
            rows.push(row);
        }
        Ok(Object::new()
            .with("store_id", writer.id.clone())
            .with("membership_at", crate::format_instant(now)?)
            .with("execution", if limited { "limited" } else { "complete" })
            .with(
                "certainty",
                if !diagnostics.complete(limited)
                    || failed > 0
                    || not_applicable > 0
                    || field_coverage.failed > 0
                    || field_coverage.decoding_failures > 0
                {
                    "incomplete"
                } else {
                    "exact"
                },
            )
            .with(
                "coverage",
                diagnostics.encode(
                    visited as u64,
                    eligible,
                    [
                        field_coverage.failed,
                        field_coverage.not_applicable,
                        field_coverage.decoding_failures,
                    ],
                    membership_limited,
                ),
            )
            .with(
                "text_coverage",
                Object::new()
                    .with("failed_or_incomplete", failed.to_string())
                    .with("not_applicable", not_applicable.to_string())
                    .with("posting_visits", posting_visits.to_string()),
            )
            .with(
                "ranking",
                Object::new()
                    .with("profile", purrdf::text::RANKING_PROFILE_ID)
                    .with("corpus_id", corpus_id)
                    .with("corpus_profile", crate::text::PROFILE)
                    .with(
                        "analyzer",
                        hex::encode(&writer.config.text_analyzer.fingerprint()),
                    )
                    .with("documents", documents.len().to_string())
                    .with("tokens", total.to_string())
                    .with("membership_complete", membership_complete)
                    .with("terms", terms),
            )
            .with("time_basis", time_basis)
            .with("field_visits", field_coverage.visits.to_string())
            .with(
                "qualified_records",
                diagnostics.count(
                    qualified as u64,
                    membership_limited
                        || field_coverage.failed > 0
                        || field_coverage.decoding_failures > 0,
                ),
            )
            .with("known_qualified_records", qualified.to_string())
            .with("event_time_fallback", event_fallback.to_string())
            .with("event_coverage", event_coverage)
            .with("rows", Value::array(rows))
            .into())
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    #[test]
    fn stored_ranker_keeps_its_snapshot_and_matches_native_scoring_after_live_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let db = DB::open_default(directory.path()).unwrap();
        let a = term_key("post", "needle", "record:a");
        let b = term_key("post", "needle", "record:b");
        db.put(&a, br#"{"frequency":"1"}"#).unwrap();
        db.put(&b, br#"{"frequency":"1"}"#).unwrap();
        let snapshot = db.snapshot();
        db.delete(&a).unwrap();
        db.put(&b, br#"{"frequency":"2"}"#).unwrap();
        let terms = vec!["needle".to_owned()];
        let captured = rank_postings(
            &snapshot,
            &terms,
            CorpusStatistics {
                documents: 2,
                tokens: 4,
            },
            |id| match id {
                "record:a" => Some(1),
                "record:b" => Some(3),
                _ => None,
            },
            10,
            &WorkspaceCapability::resident(),
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(captured.posting_visits, 2);
        assert_eq!(
            captured
                .scored
                .iter()
                .map(|(_, id)| id.as_str())
                .collect::<Vec<_>>(),
            ["record:a", "record:b"]
        );
        let profile = RankingProfile::single_field();
        let native_corpus = PreparedCorpus::new(&profile, 2, &[4]).unwrap();
        let native_query = native_corpus.prepare_query(&[("needle", 2)]).unwrap();
        for ((score, id), length) in captured.scored.iter().zip([1, 3]) {
            let oracle = native_query
                .score(&[vec![FieldInput {
                    term_frequency: 1,
                    length,
                }]])
                .unwrap();
            assert_eq!(score.value, oracle.value, "native scoring for {id}");
            assert_eq!(
                *captured
                    .candidates
                    .get(id.as_str())
                    .unwrap()
                    .get(&0)
                    .unwrap(),
                1
            );
        }
        let current = db.snapshot();
        let replacement = rank_postings(
            &current,
            &terms,
            CorpusStatistics {
                documents: 1,
                tokens: 3,
            },
            |id| (id == "record:b").then_some(3),
            10,
            &WorkspaceCapability::resident(),
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(replacement.scored.len(), 1);
        assert_eq!(replacement.scored[0].1.as_str(), "record:b");
        assert_eq!(
            *replacement
                .candidates
                .get("record:b")
                .unwrap()
                .get(&0)
                .unwrap(),
            2
        );
        assert!(db.get(a).unwrap().is_none());
    }

    #[test]
    fn stored_ranker_refuses_corruption_scan_exhaustion_and_cancellation_without_rows() {
        let directory = tempfile::tempdir().unwrap();
        let db = DB::open_default(directory.path()).unwrap();
        let a = term_key("post", "needle", "record:a");
        let terms = vec!["needle".to_owned()];
        for bytes in [br#"{"frequency":"0"}"#.to_vec(), vec![b' '; 1024 * 1024]] {
            db.put(&a, bytes).unwrap();
            assert_eq!(
                rank_postings(
                    &db.snapshot(),
                    &terms,
                    CorpusStatistics {
                        documents: 1,
                        tokens: 1
                    },
                    |_| Some(1),
                    10,
                    &WorkspaceCapability::resident(),
                    |_| Ok(())
                )
                .map_err(RankFailure::ordinary)
                .err()
                .unwrap()
                .code,
                "STORAGE_FAILURE"
            );
        }
        db.put(&a, br#"{"frequency":"1"}"#).unwrap();
        assert_eq!(
            rank_postings(
                &db.snapshot(),
                &terms,
                CorpusStatistics {
                    documents: 1,
                    tokens: 1
                },
                |_| Some(1),
                0,
                &WorkspaceCapability::resident(),
                |_| Ok(())
            )
            .map_err(RankFailure::ordinary)
            .err()
            .unwrap()
            .code,
            "RESOURCE_LIMIT"
        );
        db.put(
            term_key("post", "needle", "record:b"),
            br#"{"frequency":"1"}"#,
        )
        .unwrap();
        let mut checkpoints = 0;
        let failure = rank_postings(
            &db.snapshot(),
            &terms,
            CorpusStatistics {
                documents: 2,
                tokens: 2,
            },
            |_| Some(1),
            10,
            &WorkspaceCapability::resident(),
            |_| {
                checkpoints += 1;
                if checkpoints == 4 {
                    Err(
                        Error::new("CANCELLED", "test cancellation after candidate collection")
                            .into(),
                    )
                } else {
                    Ok(())
                }
            },
        )
        .map_err(RankFailure::ordinary)
        .err()
        .unwrap();
        assert_eq!(failure.code, "CANCELLED");
        assert_eq!(checkpoints, 4);
        db.delete(&a).unwrap();
        db.delete(term_key("post", "needle", "record:b")).unwrap();
        db.put(
            term_key("post", "needle", &"x".repeat(129)),
            br#"{"frequency":"1"}"#,
        )
        .unwrap();
        assert_eq!(
            rank_postings(
                &db.snapshot(),
                &terms,
                CorpusStatistics {
                    documents: 0,
                    tokens: 0
                },
                |_| None,
                10,
                &WorkspaceCapability::resident(),
                |_| Ok(())
            )
            .map_err(RankFailure::ordinary)
            .err()
            .unwrap()
            .code,
            "STORAGE_FAILURE"
        );
    }
}
