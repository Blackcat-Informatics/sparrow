// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Persistent native token evidence and stored-fact scoring.

use super::*;
use purrdf::text::{FieldInput, PreparedCorpus, RankingProfile};

pub(super) const MATCH_MEMBERSHIP_BYTES: usize = 64 * 1024 * 1024;

/// Counts over the complete declared corpus, independently of matching postings.
pub(super) struct CorpusStatistics {
    pub documents: u64,
    pub tokens: u128,
}

/// Native scores and exact matched-term ordinals from one immutable posting view.
pub(super) struct RankedPostings {
    pub scored: Vec<(purrdf::text::BoundedScore, String)>,
    pub candidates: BTreeMap<String, BTreeMap<usize, u64>>,
    pub posting_visits: usize,
}

/// One stored-posting ranker for ordinary searches and graph-snapshot adapters.
/// The caller supplies its captured membership and cancellation/admission fence;
/// PurRDF owns all numerical scoring. No payload is read or analyzed here.
pub(super) fn rank_postings(
    snapshot: &rocksdb::Snapshot<'_>,
    terms: &[String],
    statistics: CorpusStatistics,
    document_length: impl Fn(&str) -> Option<u64>,
    scan_budget: usize,
    mut checkpoint: impl FnMut() -> Result<(), Error>,
) -> Result<RankedPostings, Error> {
    // The owned posting codec is exactly {"frequency":"<u64>"}; 64 bytes
    // cover its maximum compact representation. Read borrowed native rows so a
    // corrupt value cannot allocate an unrestricted Vec before this check.
    const POSTING_BYTES: usize = 64;
    let mut candidates = BTreeMap::<String, BTreeMap<usize, u64>>::new();
    let mut frequencies = vec![0_u64; terms.len()];
    let mut posting_visits = 0_usize;
    for (ordinal, term) in terms.iter().enumerate() {
        checkpoint()?;
        let prefix = term_key("post", term, "");
        let mut cursor = snapshot.raw_iterator();
        cursor.seek(prefix.as_bytes());
        while cursor.valid() {
            checkpoint()?;
            let key = cursor
                .key()
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "text posting key missing"))?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            if posting_visits == scan_budget {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "posting scan budget exhausted; narrow source/time scope or raise scan_budget; no ranked prefix returned",
                ));
            }
            posting_visits += 1;
            if key.len() > prefix.len().saturating_add(128) {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "text posting identity exceeds its encoding bound",
                ));
            }
            let id = std::str::from_utf8(&key[prefix.len()..])
                .map_err(|_| Error::new("STORAGE_FAILURE", "invalid posting identity"))?;
            if document_length(id).is_some() {
                let bytes = cursor
                    .value()
                    .ok_or_else(|| Error::new("STORAGE_FAILURE", "text posting value missing"))?;
                if bytes.len() > POSTING_BYTES {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "text posting exceeds its 64-byte encoding bound",
                    ));
                }
                let posting = parse(bytes)?;
                let frequency = integer(&posting, "frequency")?;
                if frequency == 0 {
                    return Err(Error::new("STORAGE_FAILURE", "zero-frequency text posting"));
                }
                frequencies[ordinal] += 1;
                candidates
                    .entry(id.to_owned())
                    .or_default()
                    .insert(ordinal, frequency);
            }
            cursor.next();
        }
        cursor.status()?;
    }
    let profile = RankingProfile::single_field();
    let corpus = PreparedCorpus::new(&profile, statistics.documents, &[statistics.tokens])
        .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
    let frequencies = terms
        .iter()
        .zip(&frequencies)
        .map(|(term, frequency)| (term.as_str(), *frequency))
        .collect::<Vec<_>>();
    let query = corpus
        .prepare_query(&frequencies)
        .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
    let mut scored = Vec::new();
    for (id, matches) in &candidates {
        checkpoint()?;
        let length = document_length(id)
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "captured text document disappeared"))?;
        let fields = (0..terms.len())
            .map(|ordinal| {
                vec![FieldInput {
                    term_frequency: matches.get(&ordinal).copied().unwrap_or(0),
                    length,
                }]
            })
            .collect::<Vec<_>>();
        let score = query
            .score(&fields)
            .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
        scored.push((score, id.clone()));
    }
    scored.sort_by(|a, b| b.0.value.cmp(&a.0.value).then_with(|| a.1.cmp(&b.1)));
    Ok(RankedPostings {
        scored,
        candidates,
        posting_visits,
    })
}

fn term_key(kind: &str, term: &str, id: &str) -> String {
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
                let Some(millis) = record.event_time.millis() else {
                    continue;
                };
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
                            record.event_time.millis().ok_or_else(|| {
                                Error::new("STORAGE_FAILURE", "qualified event time disappeared")
                            })?,
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
                            record.event_time.millis().ok_or_else(|| {
                                Error::new("STORAGE_FAILURE", "qualified event time disappeared")
                            })?
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
            let state = read_field::<String>(&facts, "state")?;
            match state.as_str() {
                "failed" | "incomplete" => {
                    failed += 1;
                    if strict {
                        return Err(Error::new(
                            "INDEX_NOT_READY",
                            "selected text projection unavailable",
                        ));
                    }
                }
                "not_applicable" => not_applicable += 1,
                "complete" => {}
                _ => {
                    return Err(Error::new(
                        "STORAGE_FAILURE",
                        "unknown text projection state",
                    ));
                }
            }
            let length = integer(&facts, "length")?;
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
                .with("event_coverage", event_coverage)
                .with("rows", Value::array(selected_rows))
                .into());
        }
        let membership_complete = diagnostics.complete(limited)
            && failed == 0
            && not_applicable == 0
            && field_coverage.failed == 0
            && field_coverage.decoding_failures == 0;
        let mut corpus_identity = Vec::new();
        for part in [
            writer.id.as_bytes(),
            crate::text::PROFILE.as_bytes(),
            purrdf::text::RANKING_PROFILE_ID.as_bytes(),
            &writer.config.text_analyzer.fingerprint(),
        ] {
            purrdf_hash::frame::frame_le(&mut corpus_identity, part);
        }
        for (id, (_, length)) in &documents {
            if let Some(stage) = stage.as_deref() {
                stage.checkpoint()?;
            }
            purrdf_hash::frame::frame_le(&mut corpus_identity, id.as_bytes());
            purrdf_hash::frame::frame_le(&mut corpus_identity, &length.to_be_bytes());
        }
        let corpus_id = hex::encode(blake3::hash(&corpus_identity).as_bytes());
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
            || {
                if let Some(stage) = stage.as_deref() {
                    stage.checkpoint()?;
                }
                Ok(())
            },
        )?;
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
            let (record, _) = &documents[&id];
            let mut evidence = Vec::<Value>::new();
            let mut evidence_limited = false;
            for ordinal in candidates[&id].keys() {
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
                witnesses.remove(&id).ok_or_else(|| {
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
            || Ok(()),
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
            assert_eq!(captured.candidates[id][&0], 1);
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
            || Ok(()),
        )
        .unwrap();
        assert_eq!(replacement.scored.len(), 1);
        assert_eq!(replacement.scored[0].1, "record:b");
        assert_eq!(replacement.candidates["record:b"][&0], 2);
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
                    || Ok(())
                )
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
                || Ok(())
            )
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
            || {
                checkpoints += 1;
                if checkpoints == 4 {
                    Err(Error::new(
                        "CANCELLED",
                        "test cancellation after candidate collection",
                    ))
                } else {
                    Ok(())
                }
            },
        )
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
                || Ok(())
            )
            .err()
            .unwrap()
            .code,
            "STORAGE_FAILURE"
        );
    }
}
