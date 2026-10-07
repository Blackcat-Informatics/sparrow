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
        scope.validate()?;
        if input.len() > 65536 {
            return Err(Error::new("RESOURCE_LIMIT", "text query exceeds 64 KiB"));
        }
        let writer = self.lock()?;
        let mut terms = writer
            .config
            .text_analyzer
            .terms(input)
            .map_err(|e| Error::new("INVALID_ARGUMENT", e.to_string()))?;
        terms.sort();
        terms.dedup();
        if terms.is_empty() || terms.len() > purrdf::text::QUERY_TERMS_MAX {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "text query requires 1..1024 distinct analyzed terms",
            ));
        }
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
            eligible += 1;
            let bytes = writer
                .db
                .get(format!("text-record:{id}"))?
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "live text projection missing"))?;
            let facts = parse(&bytes)?;
            let state = read_field::<String>(&facts, "state")?;
            match state.as_str() {
                "failed" | "incomplete" => failed += 1,
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
                documents.insert(id.to_owned(), (record, length));
            }
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
                if limited || failed > 0 || not_applicable > 0 {
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
                    .with("failed_json_records", "0")
                    .with("non_json_records", "0")
                    .with("decoding_failures", "0"),
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
            .with("rows", Value::array(rows))
            .into())
    }
}
