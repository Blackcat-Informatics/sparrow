// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use super::Result;
use super::{Options, failure, fill_scope, instant, optional_value, receipt, wire};
use purrdf_lex::json::{Object, Value};

pub(super) async fn execute(
    client: &wire::sparrow::Client,
    options: &Options,
    text: &str,
) -> Result<Value> {
    let mut request = client.text_search_request();
    let mut input = request.get().init_query();
    input.set_text(text);
    fill_scope(input.init_scope(), options);
    let response = request.send().promise.await?;
    let found = match response.get()?.get_result()?.which()? {
        wire::result::Ok(found) => found?,
        wire::result::Error(error) => return Err(failure(error?)),
    };
    let ranking = found.get_ranking()?;
    let terms = ranking
        .get_terms()?
        .iter()
        .map(|term| Ok(Value::from(term?.to_str()?)))
        .collect::<Result<Vec<_>>>()?;
    let ranking = Object::new()
        .with("profile", ranking.get_profile()?.to_str()?)
        .with("corpus_id", ranking.get_corpus_id()?.to_str()?)
        .with("corpus_profile", ranking.get_corpus_profile()?.to_str()?)
        .with("analyzer", ranking.get_analyzer()?.to_str()?)
        .with("documents", ranking.get_documents().to_string())
        .with("tokens", ranking.get_tokens().to_string())
        .with("membership_complete", ranking.get_membership_complete())
        .with("terms", Value::array(terms));
    let coverage = found.get_coverage()?;
    let coverage = Object::new()
        .with(
            "failed_or_incomplete",
            coverage.get_failed_or_incomplete().to_string(),
        )
        .with("not_applicable", coverage.get_not_applicable().to_string())
        .with("posting_visits", coverage.get_posting_visits().to_string());
    let rows = found
        .get_rows()?
        .iter()
        .map(|row| {
            let evidence = row
                .get_evidence()?
                .iter()
                .map(|evidence| {
                    let contributors = evidence
                        .get_contributors()?
                        .iter()
                        .map(|span| {
                            Value::from(
                                Object::new()
                                    .with("start", span.get_start().to_string())
                                    .with("end", span.get_end().to_string()),
                            )
                        })
                        .collect::<Vec<_>>();
                    Ok(Value::from(
                        Object::new()
                            .with("term", evidence.get_term()?.to_str()?)
                            .with("source_start", evidence.get_source_start().to_string())
                            .with("source_end", evidence.get_source_end().to_string())
                            .with(
                                "basis",
                                match evidence.get_basis()? {
                                    wire::TextBasis::RawRecord => "raw_record",
                                    wire::TextBasis::DecodedJsonScalar => "decoded_json_scalar",
                                },
                            )
                            .with("coarse", evidence.get_coarse())
                            .with("path", optional_value(evidence.get_path()?)?)
                            .with("contributors", Value::array(contributors)),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Value::from(
                Object::new()
                    .with("record_id", row.get_record_id()?.to_str()?)
                    .with("artifact_id", row.get_artifact_id()?.to_str()?)
                    .with("source", row.get_source()?.to_str()?)
                    .with("received_at", instant(row.get_received_at()?)?)
                    .with("score", row.get_score()?.to_str()?)
                    .with("evidence_limited", row.get_evidence_limited())
                    .with("evidence", Value::array(evidence)),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Object::new()
        .with("receipt", receipt(found.get_receipt()?)?)
        .with("ranking", ranking)
        .with("text_coverage", coverage)
        .with("rows", Value::array(rows))
        .into())
}
