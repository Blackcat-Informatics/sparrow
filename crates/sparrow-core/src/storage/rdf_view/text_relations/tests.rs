// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Stored-text consumer checks over actual catalog snapshots and native engines.

use super::*;
use crate::{Config, QueryCancellation, QueryScope};
use purrdf::{
    SparqlRequest,
    sparql::{ChargePoint, FallibleSparqlError, NativeSparqlEngine, QueryGovernors, QueryOptions},
};

fn fixture(path: &std::path::Path) -> (Store, Config) {
    let config = sparrow_test_support::storage_config!(Config);
    (Store::open(path, config.clone()).unwrap(), config)
}

fn predicates() -> SparqlTextRelations {
    SparqlTextRelations {
        ranked_iri: "urn:fixture:stored-rank".into(),
        spans_iri: "urn:fixture:stored-spans".into(),
    }
}

fn governors() -> QueryGovernors {
    QueryGovernors::METERED
        .with_fuel(1_000_000)
        .with_max_answers(1000)
        .with_max_intermediate_cells(10000)
        .with_max_scratch_bytes(8 * 1024 * 1024)
        .with_max_remote_requests(0)
}

fn read<'a>(
    scope: &'a QueryScope,
    relations: &'a SparqlTextRelations,
    governors: &'a QueryGovernors,
    query: &'a str,
) -> SparqlRead<'a> {
    SparqlRead {
        scope,
        text_relations: Some(relations),
        layers: RdfGraphLayers {
            metadata: true,
            diagnostic: false,
            content: false,
        },
        request: SparqlRequest {
            query,
            base_iri: None,
            substitutions: &[],
        },
        governors,
        limits: RdfReadLimits::default(),
    }
}

#[test]
fn stored_text_relations_share_ranking_corpus_and_preserve_native_source_spans() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = fixture(dir.path());
    let input = r#"{"before":"⚙","message":"nee\u0064le needle"}
{"message":"needle more words"}
"#
    .as_bytes();
    let artifact = store
        .ingest("text-native", "text", "jsonl", input, 1)
        .unwrap();
    store
        .ingest(
            "text-outside",
            "outside",
            "log",
            b"needle needle needle\n",
            1,
        )
        .unwrap();
    let scope = sparrow_test_support::record_scope!(QueryScope, "text", 100);
    let relations = predicates();
    let governors = governors();
    let cancellation = QueryCancellation::default();
    let ordinary = store.text_query(&scope, "needle", 2).unwrap();
    let query = "SELECT ?doc ?id ?score ?rank ?matched WHERE { ?doc <urn:fixture:stored-rank> (\"needle\" ?id ?score ?rank ?matched) } ORDER BY ?rank";
    let ranked = store
        .sparql_query(
            &read(&scope, &relations, &governors, query),
            2,
            &cancellation,
        )
        .unwrap();
    let capture = ranked.text.as_ref().unwrap();
    assert_eq!(
        Some(capture.corpus_id.as_str()),
        ordinary["ranking"]["corpus_id"].as_str()
    );
    assert_eq!(capture.documents, 2);
    assert_eq!(capture.coverage.ready, 2);
    let (_, rows) = ranked.outcome.as_ref().unwrap().result.solutions().unwrap();
    assert_eq!(rows.len(), 2);
    for (ordinal, (row, expected)) in rows
        .iter()
        .zip(ordinary["rows"].as_array().unwrap())
        .enumerate()
    {
        let id = expected["record_id"].as_str().unwrap();
        assert_eq!(row[0], Some(TermValue::iri(rdf_index::entity_iri(id))));
        assert_eq!(row[1], Some(TermValue::simple_literal(id)));
        assert_eq!(
            row[2],
            Some(TermValue::typed_literal(
                expected["score"].as_str().unwrap(),
                purrdf_core::datatype::XSD_DECIMAL
            ))
        );
        assert_eq!(
            row[3],
            Some(TermValue::typed_literal(
                (ordinal + 1).to_string(),
                purrdf_core::datatype::XSD_INTEGER
            ))
        );
    }
    let id = &artifact.record_ids[0];
    let bindings = [("doc".into(), TermValue::iri(rdf_index::entity_iri(id)))];
    let mut bound = read(&scope, &relations, &governors, query);
    bound.request.substitutions = &bindings;
    let bounded = store.sparql_query(&bound, 2, &cancellation).unwrap();
    assert_eq!(bounded.text.as_ref().unwrap().corpus_id, capture.corpus_id);
    let (_, bounded_rows) = bounded
        .outcome
        .as_ref()
        .unwrap()
        .result
        .solutions()
        .unwrap();
    assert_eq!(bounded_rows.len(), 1);
    assert!(
        rows.contains(&bounded_rows[0]),
        "bound lookup retains the complete selected scoring corpus and ranks"
    );

    let span_query = "SELECT ?id ?term ?start ?end ?basis ?path ?coarse WHERE { ?doc <urn:fixture:stored-spans> (\"needle\" ?id ?term ?start ?end ?basis ?path ?coarse) }";
    let mut spans_read = read(&scope, &relations, &governors, span_query);
    spans_read.request.substitutions = &bindings;
    let spans = store.sparql_query(&spans_read, 2, &cancellation).unwrap();
    let (_, rows) = spans.outcome.as_ref().unwrap().result.solutions().unwrap();
    assert_eq!(
        rows.len(),
        3,
        "one raw occurrence and two decoded occurrences"
    );
    let mut decoded = 0;
    for row in rows {
        let TermValue::Literal {
            lexical_form: start,
            ..
        } = row[2].as_ref().unwrap()
        else {
            panic!("integer start")
        };
        let TermValue::Literal {
            lexical_form: end, ..
        } = row[3].as_ref().unwrap()
        else {
            panic!("integer end")
        };
        let start: u64 = start.parse().unwrap();
        let end: u64 = end.parse().unwrap();
        let bytes = store.bytes(id, start, (end - start) as usize, 2).unwrap();
        if row[4] == Some(TermValue::simple_literal("decoded_json_scalar")) {
            decoded += 1;
            assert_eq!(bytes, br#"nee\u0064le needle"#);
            assert_eq!(row[5], Some(TermValue::simple_literal("/message")));
            assert_eq!(
                row[6],
                Some(TermValue::typed_literal(
                    "true",
                    purrdf_core::datatype::XSD_BOOLEAN
                ))
            );
        } else {
            assert_eq!(row[4], Some(TermValue::simple_literal("raw_record")));
            assert_eq!(bytes, b"needle");
            assert_eq!(row[5], Some(TermValue::simple_literal("")));
        }
    }
    assert_eq!(decoded, 2);
    let explained = store
        .sparql_explain(
            &read(&scope, &relations, &governors, query),
            2,
            &cancellation,
        )
        .unwrap();
    let (explanation, _) = explained.outcome.unwrap();
    assert_eq!(explanation.relations().len(), 2);
    assert!(
        explanation
            .ledger()
            .iter()
            .any(|node| node.fuel_at(ChargePoint::PropertyFunctionWork) > 0)
    );
    drop(store);
    assert_eq!(
        ranked.outcome.unwrap().result.solutions().unwrap().1.len(),
        2
    );
}

#[test]
fn stored_text_relations_keep_the_original_snapshot_after_live_retirement() {
    let dir = tempfile::tempdir().unwrap();
    let (store, config) = fixture(dir.path());
    let original = store
        .ingest("text-frozen", "text", "log", b"needle original\n", 1)
        .unwrap();
    let scope = sparrow_test_support::record_scope!(QueryScope, "text", 100);
    let relations = predicates();
    let governors = governors();
    let cancellation = QueryCancellation::default();
    let store_id = store.store_id().unwrap();
    let term = config.text_analyzer.terms("needle").unwrap().remove(0);
    let posting_key = text_index::term_key("post", &term, &original.record_ids[0]);
    let query = "SELECT ?id WHERE { ?doc <urn:fixture:stored-rank> (\"needle\" ?id ?score ?rank ?matched) }";
    let frozen = store
        .capture_rdf_scope(
            (&scope, None, true),
            RdfGraphLayers {
                metadata: true,
                diagnostic: false,
                content: false,
            },
            2,
            RdfReadLimits::default(),
            &cancellation,
            |view, coverage| {
                assert_eq!(coverage.selected_records, 1);
                store
                    .ingest("text-later", "text", "log", b"needle later\n", 3)
                    .unwrap();
                for _ in 0..200 {
                    store.expire(31 * 86_400_000).unwrap();
                    if store
                        .lock()
                        .unwrap()
                        .db
                        .get(&posting_key)
                        .unwrap()
                        .is_none()
                    {
                        break;
                    }
                }
                assert!(
                    store
                        .lock()
                        .unwrap()
                        .db
                        .get(&posting_key)
                        .unwrap()
                        .is_none(),
                    "the live posting must actually retire before reading the original snapshot"
                );
                assert_eq!(
                    store.status(31 * 86_400_000).unwrap()["live_records"].as_str(),
                    Some("0")
                );
                let capture = view.text_capture(&store_id, &relations).unwrap();
                let environment = view.text_environment(&capture).unwrap();
                NativeSparqlEngine::new()
                    .query_governed_fallible_view(
                        view,
                        SparqlRequest {
                            query,
                            base_iri: None,
                            substitutions: &[],
                        },
                        QueryOptions::new().with_env(&environment),
                        &governors,
                    )
                    .unwrap()
            },
        )
        .unwrap();
    drop(store);
    let (_, rows) = frozen.result.solutions().unwrap();
    assert_eq!(
        rows,
        &[vec![Some(TermValue::simple_literal(
            &original.record_ids[0]
        ))]]
    );
    let store = Store::open(dir.path(), config).unwrap();
    let fresh = store
        .sparql_query(
            &read(&scope, &relations, &governors, query),
            31 * 86_400_000,
            &cancellation,
        )
        .unwrap();
    assert!(
        fresh
            .outcome
            .unwrap()
            .result
            .solutions()
            .unwrap()
            .1
            .is_empty()
    );
}

#[test]
fn stored_text_relations_preserve_faults_through_limit_optional_union_and_retry() {
    let dir = tempfile::tempdir().unwrap();
    let (store, config) = fixture(dir.path());
    let artifact = store
        .ingest("text-fault", "text", "log", b"needle one\nneedle two\n", 1)
        .unwrap();
    let outside = store
        .ingest(
            "text-fault-outside",
            "outside",
            "log",
            b"needle outside\n",
            1,
        )
        .unwrap();
    let term = config.text_analyzer.terms("needle").unwrap().remove(0);
    let inside = artifact.record_ids.iter().max().unwrap();
    let inside_key = text_index::term_key("post", &term, inside);
    let outside_key = text_index::term_key("post", &term, &outside.record_ids[0]);
    drop(store);
    let db = sparrow_test_support::catalog_db(dir.path()).unwrap();
    db.put(&outside_key, br#"{"frequency":"0"}"#).unwrap();
    drop(db);
    let store = Store::open(dir.path(), config.clone()).unwrap();
    let scope = sparrow_test_support::record_scope!(QueryScope, "text", 100);
    let relations = predicates();
    let governors = governors();
    let cancellation = QueryCancellation::default();
    let rank = "SELECT ?id WHERE { ?doc <urn:fixture:stored-rank> (\"needle\" ?id ?score ?rank ?matched) } LIMIT 1";
    assert_eq!(
        store
            .sparql_query(
                &read(&scope, &relations, &governors, rank),
                2,
                &cancellation
            )
            .unwrap()
            .outcome
            .unwrap()
            .result
            .solutions()
            .unwrap()
            .1
            .len(),
        1
    );
    drop(store);
    let db = sparrow_test_support::catalog_db(dir.path()).unwrap();
    db.put(&inside_key, br#"{"frequency":"0"}"#).unwrap();
    drop(db);
    let store = Store::open(dir.path(), config.clone()).unwrap();
    for query in [
        rank,
        "SELECT * WHERE { ?doc <urn:fixture:stored-spans> (\"needle\" ?id ?term ?start ?end ?basis ?path ?coarse) } LIMIT 2",
        "SELECT * WHERE { VALUES ?seed { 1 } OPTIONAL { ?doc <urn:fixture:stored-spans> (\"needle\" ?id ?term ?start ?end ?basis ?path ?coarse) } }",
        "SELECT * WHERE { { ?doc <urn:fixture:stored-spans> (\"needle\" ?id ?term ?start ?end ?basis ?path ?coarse) } UNION { VALUES ?id { \"fallback\" } } }",
    ] {
        let result = store
            .sparql_query(
                &read(&scope, &relations, &governors, query),
                2,
                &cancellation,
            )
            .unwrap_err();
        assert_eq!(
            result.code, "STORAGE_FAILURE",
            "source failure cannot become a successful prefix: {query}"
        );
    }
    drop(store);
    let db = sparrow_test_support::catalog_db(dir.path()).unwrap();
    for key in [&inside_key, &outside_key] {
        db.put(key, br#"{"frequency":"1"}"#).unwrap();
    }
    drop(db);
    let store = Store::open(dir.path(), config).unwrap();
    assert!(
        store
            .sparql_query(
                &read(&scope, &relations, &governors, rank),
                2,
                &cancellation
            )
            .unwrap()
            .outcome
            .is_ok()
    );
}

#[test]
fn stored_text_relations_stop_inside_source_work_and_keep_admission_failures() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = fixture(dir.path());
    let bytes = (0..200)
        .map(|ordinal| format!("needle record {ordinal}\n"))
        .collect::<String>();
    store
        .ingest("text-metered", "text", "log", bytes.as_bytes(), 1)
        .unwrap();
    let scope = sparrow_test_support::record_scope!(QueryScope, "text", 1000);
    let relations = predicates();
    let full = governors();
    let stopped = full.clone().with_fuel(1000);
    let none = full.clone().with_fuel(0);
    let cancellation = QueryCancellation::default();
    let query = "SELECT ?id WHERE { ?doc <urn:fixture:stored-rank> (\"needle\" ?id ?score ?rank ?matched) }";
    let result = store
        .sparql_query(&read(&scope, &relations, &stopped, query), 2, &cancellation)
        .unwrap();
    assert!(matches!(
        result.outcome,
        Err(FallibleSparqlError::BudgetExhausted { .. })
    ));
    let baseline = store
        .sparql_explain(&read(&scope, &relations, &none, query), 2, &cancellation)
        .unwrap();
    let measured = store
        .sparql_explain(&read(&scope, &relations, &stopped, query), 2, &cancellation)
        .unwrap();
    let (_, baseline) = baseline.outcome.unwrap();
    let (ledger, evidence) = measured.outcome.unwrap();
    assert!(
        ledger
            .ledger()
            .iter()
            .any(|node| node.fuel_at(ChargePoint::PropertyFunctionWork) > 0)
    );
    assert!(
        evidence.view.row_visits > baseline.view.row_visits,
        "the governor stopped after posting work began"
    );
    assert!(evidence.governors.tripped().is_some());
    let mut admitted = read(&scope, &relations, &full, query);
    admitted.limits.live_bytes = 64 * 1024 * 1024;
    assert!(
        store
            .sparql_query(&admitted, 2, &cancellation)
            .unwrap()
            .outcome
            .is_ok()
    );
    let query = format!(
        "SELECT ?id WHERE {{ ?doc <urn:fixture:stored-rank> (\"needle {}\" ?id ?score ?rank ?matched) }}",
        "x".repeat(9000)
    );
    let mut limited = read(&scope, &relations, &full, &query);
    limited.limits.live_bytes = 64 * 1024 * 1024;
    let refused = store.sparql_query(&limited, 2, &cancellation).unwrap_err();
    assert_eq!(refused.code, "RESOURCE_LIMIT");
}
