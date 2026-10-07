// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Persistent native token evidence and stored-fact scoring.

use super::*;
use purrdf::text::{FieldInput, PreparedCorpus, RankingProfile};

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
        scope.validate()?;
        query.validate()?;
        let writer = self.lock()?;
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
        if query.text.is_some() && (terms.is_empty() || terms.len() > purrdf::text::QUERY_TERMS_MAX)
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "text query requires 1..1024 distinct analyzed terms",
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
        let start = format!("time:{:020}:", scope.received_from);
        for entry in writer.db.iterator(IteratorMode::From(
            start.as_bytes(),
            rocksdb::Direction::Forward,
        )) {
            let (key, id) = entry?;
            if !key.starts_with(b"time:") {
                break;
            }
            let id = std::str::from_utf8(&id)
                .map_err(|_| Error::new("STORAGE_FAILURE", "invalid time index identity"))?;
            let record: Record = writer.get(id)?;
            if record.id != id
                || key.as_ref() != format!("time:{:020}:{id}", record.received_at).as_bytes()
            {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "temporal index disagrees with record identity/time",
                ));
            }
            if record.received_at >= scope.received_before {
                break;
            }
            if visited == scope.scan_budget {
                limited = true;
                break;
            }
            visited += 1;
            if !writer.live_record(&record, now, &protected)
                || scope
                    .source
                    .as_ref()
                    .is_some_and(|source| *source != record.source)
            {
                continue;
            }
            event_evaluated += 1;
            writer.validate_event_record(&record)?;
            if record.event_time.millis().is_none() {
                event_unknown += 1;
            }
            if let Some(interval) = query.event_interval {
                if query.require_complete_index
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
                query.require_complete_index,
            )?
            else {
                continue;
            };
            qualified += 1;
            if query.text.is_none() {
                if query.event_interval.is_some() || chronological.len() < scope.limit {
                    let row: Value = Object::new()
                        .with("record_id", record.id.clone())
                        .with("artifact_id", record.artifact_id.clone())
                        .with("source", record.source.clone())
                        .with("received_at", crate::format_instant(record.received_at)?)
                        .with("byte_start", record.byte_start.to_string())
                        .with("byte_end", record.byte_end.to_string())
                        .with("event_time", record.event_time.encode())
                        .with("fields", Value::array(fields))
                        .into();
                    capture_bytes += json::write_compact(&row).len();
                    if capture_bytes
                        > if query.event_interval.is_some() {
                            64 * 1024 * 1024
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
                    if query.require_complete_index {
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
                if capture_bytes > 64 * 1024 * 1024 {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "record membership capture exceeds 64 MiB; narrow scope",
                    ));
                }
                witnesses.insert(id.to_owned(), fields);
                documents.insert(id.to_owned(), (record, length));
            }
        }
        let event_coverage = Object::new()
            .with("evaluated", event_evaluated.to_string())
            .with("unknown", event_unknown.to_string())
            .with("outside_interval", event_outside.to_string())
            .with("membership_complete", !limited);
        if query.text.is_none() {
            limited |= qualified > scope.limit;
            chronological.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            let mut response_bytes = 0_usize;
            let mut selected_rows = Vec::new();
            for (_, _, row) in chronological.into_iter().take(scope.limit) {
                response_bytes += json::write_compact(&row).len();
                if response_bytes > 2 * 1024 * 1024 {
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
                    if limited || field_coverage.failed > 0 || field_coverage.decoding_failures > 0
                    {
                        "incomplete"
                    } else {
                        "exact"
                    },
                )
                .with(
                    "coverage",
                    Object::new()
                        .with("visited", visited.to_string())
                        .with("eligible_records", eligible.to_string())
                        .with("failed_json_records", field_coverage.failed.to_string())
                        .with(
                            "non_json_records",
                            field_coverage.not_applicable.to_string(),
                        )
                        .with(
                            "decoding_failures",
                            field_coverage.decoding_failures.to_string(),
                        ),
                )
                .with("field_visits", field_coverage.visits.to_string())
                .with("qualified_records", qualified.to_string())
                .with("event_coverage", event_coverage)
                .with("rows", Value::array(selected_rows))
                .into());
        }
        let membership_complete = !limited;
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
            purrdf_hash::frame::frame_le(&mut corpus_identity, id.as_bytes());
            purrdf_hash::frame::frame_le(&mut corpus_identity, &length.to_be_bytes());
        }
        let corpus_id = hex::encode(blake3::hash(&corpus_identity).as_bytes());
        let mut candidates = BTreeMap::<String, BTreeMap<usize, u64>>::new();
        let mut frequencies = vec![0_u64; terms.len()];
        let mut posting_visits = 0_usize;
        for (ordinal, term) in terms.iter().enumerate() {
            let prefix = term_key("post", term, "");
            for entry in writer.db.prefix_iterator(&prefix) {
                let (key, bytes) = entry?;
                if !key.starts_with(prefix.as_bytes()) {
                    break;
                }
                if posting_visits == scope.scan_budget {
                    return Err(Error::new(
                        "RESOURCE_LIMIT",
                        "posting scan budget exhausted; narrow source/time scope or raise scan_budget; no ranked prefix returned",
                    ));
                }
                posting_visits += 1;
                let id = std::str::from_utf8(&key[prefix.len()..])
                    .map_err(|_| Error::new("STORAGE_FAILURE", "invalid posting identity"))?;
                if !documents.contains_key(id) {
                    continue;
                }
                let posting = parse(&bytes)?;
                // Validate frequency even if no row will eventually be emitted.
                if integer(&posting, "frequency")? == 0 {
                    return Err(Error::new("STORAGE_FAILURE", "zero-frequency text posting"));
                }
                frequencies[ordinal] += 1;
                candidates
                    .entry(id.to_owned())
                    .or_default()
                    .insert(ordinal, integer(&posting, "frequency")?);
            }
        }
        let profile = RankingProfile::single_field();
        let corpus = PreparedCorpus::new(&profile, documents.len() as u64, &[total])
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
            let (_, length) = &documents[id];
            let fields = (0..terms.len())
                .map(|ordinal| {
                    vec![FieldInput {
                        term_frequency: matches.get(&ordinal).copied().unwrap_or(0),
                        length: *length,
                    }]
                })
                .collect::<Vec<_>>();
            let score = query
                .score(&fields)
                .map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
            scored.push((score, id.clone()));
        }
        scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        limited |= scored.len() > scope.limit;
        let mut rows = Vec::new();
        let mut response_bytes = 0_usize;
        for (score, id) in scored.into_iter().take(scope.limit) {
            let (record, _) = &documents[&id];
            let mut evidence = Vec::<Value>::new();
            let mut evidence_limited = false;
            for ordinal in candidates[&id].keys() {
                if evidence.len() == 64 {
                    evidence_limited = true;
                    break;
                }
                let term = &terms[*ordinal];
                let bytes = writer
                    .db
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
            let row: Value = Object::new()
                .with("record_id", id.clone())
                .with("artifact_id", record.artifact_id.clone())
                .with("source", record.source.clone())
                .with("received_at", crate::format_instant(record.received_at)?)
                .with("byte_start", record.byte_start.to_string())
                .with("byte_end", record.byte_end.to_string())
                .with(
                    "fields",
                    Value::array(witnesses.remove(&id).ok_or_else(|| {
                        Error::new("STORAGE_FAILURE", "captured field witnesses missing")
                    })?),
                )
                .with("event_time", record.event_time.encode())
                .with("score", score.to_decimal_lexical())
                .with("evidence", Value::array(evidence))
                .with("evidence_limited", evidence_limited)
                .into();
            response_bytes += json::write_compact(&row).len();
            if response_bytes > 2 * 1024 * 1024 {
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
                if limited
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
                Object::new()
                    .with("visited", visited.to_string())
                    .with("eligible_records", eligible.to_string())
                    .with("failed_json_records", field_coverage.failed.to_string())
                    .with(
                        "non_json_records",
                        field_coverage.not_applicable.to_string(),
                    )
                    .with(
                        "decoding_failures",
                        field_coverage.decoding_failures.to_string(),
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
            .with("qualified_records", qualified.to_string())
            .with("event_coverage", event_coverage)
            .with("rows", Value::array(rows))
            .into())
    }
}
