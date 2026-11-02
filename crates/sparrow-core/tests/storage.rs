// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Persistent lifetime scenarios using temporary stores and injected UTC instants.

use sparrow_core::{Config, PinPageBody, PinRead, PinSelector, QueryScope, Store};
use sparrow_test_support::{catalog_db, remove_artifact_rdf_upgrade, remove_tail_range_index};

const DAY: u64 = 86_400_000;

#[test]
fn native_log_time_profiles_preserve_byte_ranges_event_order_and_immutable_history() {
    use sparrow_core::{EventInterval, EventTime, LogTimeCapture, RecordQuery, TimeRepresentation};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let earlier = store
        .ingest(
            "log-before-map",
            "log-clock",
            "log",
            b"[2026-10-07T03:00:00Z] before\n",
            100,
        )
        .unwrap();
    let capture = LogTimeCapture {
        pattern: r".*\[([^ ]+)\] .*".into(),
        group: 1,
        flags: "s".into(),
    };
    let profile = store
        .define_log_time_profile("log-clock", &capture, TimeRepresentation::Rfc3339)
        .unwrap();
    assert_eq!(
        store.properties().unwrap()["store_format"].as_str(),
        Some("sparrow.store/88")
    );
    assert_eq!(
        store
            .define_log_time_profile("log-clock", &capture, TimeRepresentation::Rfc3339)
            .unwrap(),
        profile
    );
    assert_eq!(
        store
            .define_time_profile("log-clock", "/timestamp", TimeRepresentation::Rfc3339)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert!(matches!(
        store.record(&earlier.record_ids[0]).unwrap().event_time,
        EventTime::Unknown {
            profile_id: None,
            ..
        }
    ));
    let mut bytes = "λ[2026-10-07T05:00:02.123+02:00] late\r\nλ[2026-10-07T03:00:01Z] early\n[2026-10-07T03:00:00.0001Z] precision\nno timestamp\n".as_bytes().to_vec();
    bytes.extend_from_slice(b"invalid utf8 \xff\n");
    let artifact = store
        .ingest("log-after-map", "log-clock", "log", &bytes, 200)
        .unwrap();
    assert_eq!(artifact.record_ids.len(), 5);
    for (index, instant) in ["2026-10-07T05:00:02.123+02:00", "2026-10-07T03:00:01Z"]
        .into_iter()
        .enumerate()
    {
        let record = store.record(&artifact.record_ids[index]).unwrap();
        let EventTime::Known {
            millis,
            profile_id,
            occurrence,
            source_start,
            source_end,
        } = record.event_time
        else {
            panic!("native source timestamp missing")
        };
        assert_eq!(millis, sparrow_core::parse_instant(instant).unwrap());
        assert_eq!(profile_id, profile.id);
        assert_eq!(occurrence, 1);
        assert_eq!(
            &bytes[source_start as usize..source_end as usize],
            instant.as_bytes()
        );
        assert_eq!(record.received_at, 200);
        assert_eq!(record.expires_at, 30 * DAY + 200);
    }
    for (index, expected) in [
        (2, "invalid_instant"),
        (3, "log_no_match"),
        (4, "log_invalid_utf8"),
    ] {
        assert!(
            matches!(store.record(&artifact.record_ids[index]).unwrap().event_time, EventTime::Unknown { ref reason, .. } if reason == expected)
        );
    }
    let opaque = store
        .ingest(
            "log-wrong-format",
            "log-clock",
            "opaque",
            b"[2026-10-07T03:00:01Z] opaque",
            201,
        )
        .unwrap();
    assert!(
        matches!(store.record(&opaque.record_ids[0]).unwrap().event_time, EventTime::Unknown { ref reason, .. } if reason == "log_wrong_format")
    );
    let scope = record_page_scope("log-clock", 1000);
    let mut query = RecordQuery {
        event_interval: Some(EventInterval {
            from: sparrow_core::parse_instant("2026-10-07T03:00:00Z").unwrap(),
            before: sparrow_core::parse_instant("2026-10-07T03:00:03Z").unwrap(),
        }),
        ..Default::default()
    };
    let rows = store.records_query(&scope, &query, 300).unwrap();
    assert_eq!(rows["rows"].as_array().unwrap().len(), 2);
    assert_eq!(
        rows["rows"][0]["record_id"].as_str(),
        Some(artifact.record_ids[1].as_str())
    );
    assert_eq!(rows["event_coverage"]["unknown"].as_str(), Some("5"));
    query.require_complete_index = true;
    assert_eq!(
        store.records_query(&scope, &query, 300).unwrap()["event_coverage"]["unknown"].as_str(),
        Some("5")
    );
    query.require_complete_index = false;
    let mut strict_scope = scope.clone();
    strict_scope.require_complete_index = true;
    let correlated = store.correlate_read(
        &sparrow_core::CorrelateRead {
            scope: &strict_scope,
            selector: &sparrow_core::CorrelationSelector::Evidence(vec![
                artifact.record_ids[1].clone(),
            ]),
            relation: sparrow_core::CorrelationRelation::Time(sparrow_core::CorrelationInterval {
                clock: sparrow_core::CorrelationClock::Event,
                from: query.event_interval.as_ref().unwrap().from,
                before: query.event_interval.as_ref().unwrap().before,
            }),
            governors: &purrdf::sparql::QueryGovernors::METERED,
            limits: sparrow_core::RdfReadLimits::default(),
        },
        300,
        &sparrow_core::QueryCancellation::default(),
    );
    assert_eq!(correlated.unwrap_err().code, "INDEX_NOT_READY");
    let pin = store
        .pin(
            "log-time-pin",
            "log-time-pin",
            &[artifact.record_ids[1].clone()],
            40,
            300,
        )
        .unwrap();
    store
        .define_time_profile("other-json-source", "/t", TimeRepresentation::UnixMillis)
        .unwrap();
    assert_eq!(
        store.properties().unwrap()["store_format"].as_str(),
        Some("sparrow.store/88")
    );
    store.watch_registry_replace(None, b"registered").unwrap();
    assert_eq!(
        store.properties().unwrap()["store_format"].as_str(),
        Some("sparrow.store/90")
    );
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    db.put("store_format", b"sparrow.store/79").unwrap();
    drop(db);
    assert!(
        Store::open(dir.path(), config())
            .err()
            .unwrap()
            .message
            .contains("log time")
    );
    let db = catalog_db(dir.path()).unwrap();
    db.put("store_format", b"sparrow.store/90").unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(store.time_profile("log-clock").unwrap(), Some(profile));
    assert_eq!(
        store.bytes(&artifact.id, 0, bytes.len(), 301).unwrap(),
        bytes
    );
    store.expire(31 * DAY).unwrap();
    let retained = store.records_query(&scope, &query, 31 * DAY).unwrap();
    assert_eq!(retained["rows"].as_array().unwrap().len(), 1);
    store.release(&pin.id).unwrap();
    store.expire(31 * DAY).unwrap();
    assert!(
        store.records_query(&scope, &query, 31 * DAY).unwrap()["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn native_log_time_refusals_do_not_publish_records_or_guess_optional_captures() {
    use sparrow_core::{EventTime, LogTimeCapture, TimeRepresentation};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    for (pattern, group, flags) in [
        ("(", 0, ""),
        ("(x)", 2, ""),
        ("(x)", 1, "m"),
        ("(x)", 33, ""),
    ] {
        assert_eq!(
            store
                .define_log_time_profile(
                    "invalid-log-map",
                    &LogTimeCapture {
                        pattern: pattern.into(),
                        group,
                        flags: flags.into()
                    },
                    TimeRepresentation::Rfc3339
                )
                .unwrap_err()
                .code,
            "INVALID_ARGUMENT"
        );
        assert!(store.time_profile("invalid-log-map").unwrap().is_none());
        assert_eq!(
            store.properties().unwrap()["store_format"].as_str(),
            Some("sparrow.store/77")
        );
    }
    store
        .define_log_time_profile(
            "native-refusal",
            &LogTimeCapture {
                pattern: r"(a|aa)*c\1".into(),
                group: 1,
                flags: "".into(),
            },
            TimeRepresentation::Rfc3339,
        )
        .unwrap();
    let failure = store
        .ingest(
            "refused-log-match",
            "native-refusal",
            "log",
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            1,
        )
        .unwrap_err();
    assert_eq!(failure.code, "RESOURCE_LIMIT");
    assert_eq!(store.status(2).unwrap()["live_records"].as_str(), Some("0"));
    assert_eq!(
        store.status(2).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
    store
        .define_log_time_profile(
            "optional-time",
            &LogTimeCapture {
                pattern: r"(?:ts=([0-9]+) )?.*".into(),
                group: 1,
                flags: "s".into(),
            },
            TimeRepresentation::UnixSeconds,
        )
        .unwrap();
    let missing = store
        .ingest(
            "optional-empty",
            "optional-time",
            "log",
            b"no timestamp\n",
            2,
        )
        .unwrap();
    assert!(
        matches!(store.record(&missing.record_ids[0]).unwrap().event_time, EventTime::Unknown { ref reason, .. } if reason == "log_capture_missing")
    );
    let exact = store
        .ingest(
            "optional-known",
            "optional-time",
            "log",
            b"ts=123 seconds\n",
            3,
        )
        .unwrap();
    assert_eq!(
        store
            .record(&exact.record_ids[0])
            .unwrap()
            .event_time
            .millis(),
        Some(123000)
    );
    store
        .define_log_time_profile(
            "whole-time",
            &LogTimeCapture {
                pattern: "([0-9]+)".into(),
                group: 1,
                flags: "".into(),
            },
            TimeRepresentation::UnixMillis,
        )
        .unwrap();
    let partial = store
        .ingest("partial-time", "whole-time", "log", b"123 trailing", 4)
        .unwrap();
    assert!(
        matches!(store.record(&partial.record_ids[0]).unwrap().event_time, EventTime::Unknown { ref reason, .. } if reason == "log_no_match")
    );
}

#[test]
fn tracing_relationships_keep_instance_and_occurrence_evidence_through_backfill_and_retirement() {
    use purrdf::{SparqlRequest, TermValue, sparql::QueryGovernors};
    use sparrow_core::{
        Durability, ProducerBatch, QueryCancellation, QueryScope, RdfGraphLayers, RdfReadLimits,
        SparqlRead,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let bytes = b"{\"profile\":\"sparrow.events/1\",\"client_instance\":\"11111111111111111111111111111111\",\"sequence\":1,\"kind\":\"span_follows_from\",\"level\":\"info\",\"name\":\"observation\",\"target\":\"worker\",\"span\":7,\"follows_from\":8,\"context\":[{\"id\":7,\"parent\":99,\"name\":\"span\",\"target\":\"worker\",\"fields\":[]}],\"fields\":[]}\n";
    let a = store
        .ingest_producer(
            "tracing-first",
            "trace-a",
            bytes,
            &ProducerBatch {
                instance: [0x11; 16],
                sequences: vec![1],
            },
            1,
            Durability::Durable,
        )
        .unwrap();
    let other = String::from_utf8(bytes.to_vec()).unwrap().replace(
        "11111111111111111111111111111111",
        "22222222222222222222222222222222",
    );
    store
        .ingest_producer(
            "tracing-second",
            "trace-b",
            other.as_bytes(),
            &ProducerBatch {
                instance: [0x22; 16],
                sequences: vec![1],
            },
            2,
            Durability::Durable,
        )
        .unwrap();
    let invalid = String::from_utf8(bytes.to_vec()).unwrap().replacen(
        "{",
        "{\"profile\":\"sparrow.events/1\",",
        1,
    );
    store
        .ingest_producer(
            "tracing-ambiguous",
            "trace-c",
            invalid.as_bytes(),
            &ProducerBatch {
                instance: [0x11; 16],
                sequences: vec![1],
            },
            3,
            Durability::Durable,
        )
        .unwrap();
    let pin = store
        .pin("tracing-pin", "tracing", &[a.record_ids[0].clone()], 40, 4)
        .unwrap();
    let query = "PREFIX s: <urn:sparrow:vocab:> SELECT ?record ?state ?span ?parent ?start ?end ?follows WHERE { ?record s:tracingState ?state OPTIONAL { ?record s:observedSpan ?span ; s:followsFromSpan ?follows ; s:spanContext ?entry . ?entry s:declaredParentSpan ?parent ; s:sourceByteStart ?start ; s:sourceByteEnd ?end } } ORDER BY ?record";
    let mut scope: QueryScope = record_page_scope("", 1000);
    scope.source = None;
    let governors = QueryGovernors::METERED.with_fuel(1_000_000);
    let read = SparqlRead {
        text_relations: None,
        scope: &scope,
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
        governors: &governors,
        limits: RdfReadLimits::default(),
    };
    let before = store
        .sparql_query(&read, 5, &QueryCancellation::default())
        .unwrap();
    let rows = before
        .outcome
        .as_ref()
        .unwrap()
        .result
        .solutions()
        .unwrap()
        .1;
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows.iter()
            .filter(|row| row[1] == Some(TermValue::simple_literal("complete")))
            .count(),
        2
    );
    let invalid_row = rows
        .iter()
        .find(|row| row[1] == Some(TermValue::simple_literal("invalid_observation")))
        .unwrap();
    assert!(invalid_row[2..].iter().all(Option::is_none));
    for instance in [
        "11111111111111111111111111111111",
        "22222222222222222222222222222222",
    ] {
        let row = rows
            .iter()
            .find(|row| row[2] == Some(TermValue::iri(format!("urn:sparrow:span:{instance}:7"))))
            .unwrap();
        assert_eq!(
            row[3],
            Some(TermValue::iri(format!("urn:sparrow:span:{instance}:99")))
        );
        assert_eq!(
            row[6],
            Some(TermValue::iri(format!("urn:sparrow:span:{instance}:8")))
        );
    }
    let row = rows
        .iter()
        .find(|row| row[0] == Some(TermValue::iri(format!("urn:sparrow:{}", a.record_ids[0]))))
        .unwrap();
    let spans: Vec<usize> = row[4..6]
        .iter()
        .map(|cell| {
            let Some(TermValue::Literal { lexical_form, .. }) = cell else {
                panic!("missing native source span")
            };
            lexical_form.parse().unwrap()
        })
        .collect();
    assert_eq!(
        &bytes[spans[0]..spans[1]],
        b"{\"id\":7,\"parent\":99,\"name\":\"span\",\"target\":\"worker\",\"fields\":[]}"
    );
    let original = rows.to_vec();
    drop(before);
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    db.put("store_format", b"sparrow.store/66").unwrap();
    db.delete("tracing-rdf-upgrade").unwrap();
    db.delete("tracing-rdf:profile").unwrap();
    let projection = purrdf_lex::json::read_slice(
        &db.get(&a.record_ids[0]).unwrap().unwrap(),
        purrdf_lex::json::Limits::DEFAULT,
    )
    .unwrap();
    let projection_key = projection["projection_id"].as_str().unwrap().to_owned();
    let saved = db.get(&projection_key).unwrap().unwrap();
    db.delete(&projection_key).unwrap();
    drop(db);
    let missing = Store::open(dir.path(), config()).err().unwrap();
    assert_eq!(missing.code, "STORAGE_FAILURE");
    assert!(missing.message.contains("projection"), "{missing}");
    let db = catalog_db(dir.path()).unwrap();
    assert_eq!(
        db.get("store_format").unwrap().unwrap(),
        b"sparrow.store/77"
    );
    db.put(&projection_key, saved).unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.properties().unwrap()["store_format"].as_str(),
        Some("sparrow.store/77")
    );
    let after = store
        .sparql_query(&read, 6, &QueryCancellation::default())
        .unwrap();
    assert_eq!(
        after
            .outcome
            .as_ref()
            .unwrap()
            .result
            .solutions()
            .unwrap()
            .1,
        original
    );
    drop(after);
    store.expire(31 * DAY).unwrap();
    let after = store
        .sparql_query(&read, 31 * DAY, &QueryCancellation::default())
        .unwrap();
    assert_eq!(
        after
            .outcome
            .as_ref()
            .unwrap()
            .result
            .solutions()
            .unwrap()
            .1
            .len(),
        1
    );
    assert_eq!(
        store.bytes(&a.id, 0, 1, 31 * DAY).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    store.release(&pin.id).unwrap();
    drop(after);
    store.expire(31 * DAY).unwrap();
    let empty = store
        .sparql_query(&read, 31 * DAY, &QueryCancellation::default())
        .unwrap();
    assert!(
        empty
            .outcome
            .as_ref()
            .unwrap()
            .result
            .solutions()
            .unwrap()
            .1
            .is_empty()
    );
}

#[test]
fn source_stream_rdf_deduplicates_union_and_follows_pinned_occurrence_lifetimes() {
    use purrdf::{SparqlRequest, TermValue, sparql::QueryGovernors};
    use sparrow_core::{
        Durability, ProducerBatch, QueryCancellation, QueryScope, QuerySelection, RdfGraphLayers,
        RdfReadLimits, SparqlRead,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let producer = store
        .ingest_producer(
            "provenance-producer",
            "shared-origin",
            b"{}\n{}\n",
            &ProducerBatch {
                instance: [4; 16],
                sequences: vec![1, 2],
            },
            1,
            Durability::Durable,
        )
        .unwrap();
    let ordinary = store
        .ingest(
            "provenance-ordinary",
            "shared-origin",
            "log",
            b"ordinary\n",
            2,
        )
        .unwrap();
    let position = store
        .record(&producer.record_ids[0])
        .unwrap()
        .producer
        .unwrap();
    let pin = store
        .pin(
            "provenance-pin",
            "provenance",
            &[producer.record_ids[0].clone()],
            40,
            3,
        )
        .unwrap();
    let scope = record_page_scope("shared-origin", 1);
    let run = |store: &Store, scope: &QueryScope, query: &str, layers, now| {
        let governors = QueryGovernors::METERED.with_fuel(1_000_000);
        let result = store
            .sparql_query(
                &SparqlRead {
                    text_relations: None,
                    scope,
                    layers,
                    request: SparqlRequest {
                        query,
                        base_iri: None,
                        substitutions: &[],
                    },
                    governors: &governors,
                    limits: RdfReadLimits::default(),
                },
                now,
                &QueryCancellation::default(),
            )
            .unwrap();
        result
            .outcome
            .unwrap()
            .result
            .solutions()
            .unwrap()
            .1
            .to_vec()
    };
    let query = "PREFIX sp: <urn:sparrow:vocab:> SELECT ?source ?stream ?kind WHERE { ?source a sp:Source OPTIONAL { ?stream a sp:Stream ; sp:source ?source ; sp:streamKind ?kind } }";
    let rows = run(&store, &scope, query, RdfGraphLayers::ALL, 4);
    assert_eq!(
        rows.len(),
        1,
        "default union shares Source and Stream facts across occurrence owners"
    );
    assert_eq!(
        rows[0][1],
        Some(TermValue::iri(format!(
            "urn:sparrow:{}",
            position.stream_id
        )))
    );
    assert!(
        matches!(&rows[0][2], Some(TermValue::Literal { lexical_form, .. }) if lexical_form == "producer")
    );
    assert_eq!(
        run(
            &store,
            &scope,
            "PREFIX sp: <urn:sparrow:vocab:> SELECT ?g WHERE { GRAPH ?g { ?source a sp:Source } }",
            RdfGraphLayers::ALL,
            4
        )
        .len(),
        5,
        "named graphs retain each occurrence's provenance context"
    );
    let mut selected = scope.clone();
    selected.selection = QuerySelection::Evidence(vec![ordinary.record_ids[0].clone()]);
    let rows = run(&store, &selected, query, RdfGraphLayers::ALL, 4);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][1..],
        [None, None],
        "ordinary imports must not invent a stream"
    );
    assert!(
        run(
            &store,
            &scope,
            query,
            RdfGraphLayers {
                metadata: false,
                diagnostic: true,
                content: true
            },
            4
        )
        .is_empty()
    );
    store.expire(31 * DAY).unwrap();
    assert_eq!(
        store
            .producer_stream(&position.stream_id, 31 * DAY)
            .unwrap_err()
            .code,
        "NOT_FOUND"
    );
    assert_eq!(
        run(&store, &scope, query, RdfGraphLayers::ALL, 31 * DAY).len(),
        1,
        "record pin preserves Source and Stream after parent and registry expiry"
    );
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        run(&store, &scope, query, RdfGraphLayers::ALL, 31 * DAY).len(),
        1
    );
    store
        .release_retry("provenance-release", &pin.id, 31 * DAY)
        .unwrap();
    store.expire(31 * DAY).unwrap();
    assert!(run(&store, &scope, query, RdfGraphLayers::ALL, 31 * DAY).is_empty());
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    assert!(
        db.get(format!("provenance-rdf:{}", producer.record_ids[0]))
            .unwrap()
            .is_none()
    );
}

#[test]
fn retention_metrics_account_shared_pinned_expired_empty_and_private_ownership() {
    use sparrow_core::{QueryCancellation, RetentionReadLimits};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let empty = store
        .ingest("metrics-empty", "metrics", "log", b"", 0)
        .unwrap();
    assert!(empty.record_ids.is_empty());
    let old = store
        .ingest("metrics-old", "metrics", "log", b"old\n", 1)
        .unwrap();
    let a = store
        .ingest("metrics-a", "metrics", "log", b"same\n", 2)
        .unwrap();
    store
        .ingest("metrics-b", "metrics", "log", b"same\n", 3)
        .unwrap();
    let pin = store
        .pin(
            "metrics-pin",
            "metrics",
            &[a.record_ids[0].clone(), empty.id.clone()],
            40,
            4,
        )
        .unwrap();
    let now = 31 * DAY;
    let report = store
        .retention_metrics(now, RetentionReadLimits::default())
        .unwrap();
    assert_eq!((report.live_records, report.live_artifacts), (1, 1));
    assert_eq!((report.pinned_records, report.pinned_artifacts), (1, 1));
    assert_eq!(report.oldest_received_at, Some(0));
    assert_eq!(report.retained_window_ms, now);
    assert_eq!(
        (
            report.owned_payload_bytes,
            report.live_payload_bytes,
            report.pin_held_payload_bytes,
            report.retirable_payload_bytes,
            report.unclassified_payload_bytes,
            report.payload_chunks
        ),
        (9, 5, 5, 4, 0, 2)
    );
    // Exact raw-byte admission is checked before hashing, independently of catalog/visits.
    for limits in [
        RetentionReadLimits {
            max_visits: 1,
            ..Default::default()
        },
        RetentionReadLimits {
            catalog_bytes: 1,
            ..Default::default()
        },
        RetentionReadLimits {
            payload_bytes: 8,
            ..Default::default()
        },
    ] {
        assert_eq!(
            store.retention_metrics(now, limits).unwrap_err().code,
            "RESOURCE_LIMIT"
        );
    }
    let cancelled = QueryCancellation::default();
    cancelled.cancel();
    assert_eq!(
        store
            .retention_metrics_cancellable(now, RetentionReadLimits::default(), &cancelled)
            .unwrap_err()
            .code,
        "CANCELLED"
    );
    assert_eq!(
        store.bytes(&old.id, 0, 0, now).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    store.expire(now).unwrap();
    let report = store
        .retention_metrics(now, RetentionReadLimits::default())
        .unwrap();
    assert_eq!(
        (
            report.owned_payload_bytes,
            report.live_payload_bytes,
            report.retirable_payload_bytes
        ),
        (5, 5, 0)
    );
    assert_eq!((report.live_records, report.live_artifacts), (1, 1));
    store
        .release_retry("metrics-release", &pin.id, now)
        .unwrap();
    let report = store
        .retention_metrics(now, RetentionReadLimits::default())
        .unwrap();
    assert_eq!(
        (
            report.live_records,
            report.live_artifacts,
            report.retained_window_ms
        ),
        (0, 0, 0)
    );
    assert_eq!(report.oldest_received_at, None);
    assert_eq!(
        (report.owned_payload_bytes, report.retirable_payload_bytes),
        (5, 5)
    );
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .retention_metrics(now, RetentionReadLimits::default())
            .unwrap()
            .retirable_payload_bytes,
        5
    );
    store.expire(now).unwrap();
    assert_eq!(
        store
            .retention_metrics(now, RetentionReadLimits::default())
            .unwrap()
            .owned_payload_bytes,
        0
    );
    let input = b"private\n";
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(input).as_bytes())
    );
    let stage = store
        .import_begin(
            "metrics-private",
            "metrics",
            "log",
            input.len() as u64,
            &digest,
            now,
        )
        .unwrap();
    store.import_append(&stage.id, 0, input, now).unwrap();
    store.import_seal(&stage.id, now).unwrap();
    store.import_prepare_layout(&stage.id, now).unwrap();
    store.import_prepare_record(&stage.id, 0, now).unwrap();
    let report = store
        .retention_metrics(now, RetentionReadLimits::default())
        .unwrap();
    assert_eq!(
        (
            report.owned_payload_bytes,
            report.unclassified_payload_bytes,
            report.live_payload_bytes,
            report.retirable_payload_bytes
        ),
        (8, 8, 0, 0)
    );
    assert_eq!((report.live_records, report.live_artifacts), (0, 0));
}

#[test]
fn retention_metrics_refuse_native_reference_and_payload_corruption() {
    use sparrow_core::RetentionReadLimits;
    for corruption in ["counter", "payload", "missing"] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), config()).unwrap();
        store
            .ingest("metrics-corrupt", "metrics", "log", b"one\n", 1)
            .unwrap();
        drop(store);
        let db = catalog_db(dir.path()).unwrap();
        let digest = purrdf_hash::hex::encode(purrdf_hash::blake3::hash(b"one\n").as_bytes());
        match corruption {
            "counter" => db
                .put(format!("ref:{digest}"), 1_u64.to_be_bytes())
                .unwrap(),
            "missing" => db.delete(format!("ref:{digest}")).unwrap(),
            _ => db
                .put_cf(db.cf_handle("payload").unwrap(), digest, b"wrong")
                .unwrap(),
        }
        drop(db);
        let store = Store::open(dir.path(), config()).unwrap();
        assert_eq!(
            store
                .retention_metrics(2, RetentionReadLimits::default())
                .unwrap_err()
                .code,
            "STORAGE_FAILURE"
        );
    }
}

#[test]
fn json_capture_refusals_identify_the_exhausted_request_budget_without_publishing() {
    use sparrow_core::{JsonQuery, JsonRead};
    for (format, bytes, reason) in [
        (
            "json",
            br#"{"nested":{"a":1,"b":2},"flag":true}"#.as_slice(),
            "json_occurrence_budget",
        ),
        ("log", b"first\nsecond\n".as_slice(), "record_visit_budget"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), config()).unwrap();
        store
            .ingest("budget-diagnostic", "budget-diagnostic", format, bytes, 1)
            .unwrap();
        let mut read = JsonRead {
            scope: record_page_scope("budget-diagnostic", 1),
            query: JsonQuery::Keys { path: None },
            cursor: None,
        };
        read.scope.scan_budget = 1;
        let failure = store.json_read(&read, 2).unwrap_err();
        assert_eq!(failure.code, "RESOURCE_LIMIT");
        assert!(
            failure.message.contains(&format!("{reason}=1")),
            "{failure}"
        );
        assert!(failure.message.contains("raise scan_budget"));
        assert!(failure.message.contains("no page published"));
        let status = store.status(2).unwrap();
        assert_eq!(status["record_read_handles"].as_str(), Some("0"));
        assert_eq!(status["record_read_bytes"].as_str(), Some("0"));
        read.scope.scan_budget = 10;
        let admitted = store.json_read(&read, 2).unwrap();
        assert_eq!(
            admitted.body.result().unwrap()["execution"].as_str(),
            Some("complete")
        );
    }
}

#[test]
fn cancelled_catalogue_reads_preserve_original_cursors_and_evidence() {
    use sparrow_core::{ArtifactRead, QueryCancellation, SourceRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let a = store
        .ingest("cancel-catalogue-a", "a", "json", b"{}", 1)
        .unwrap();
    let b = store
        .ingest("cancel-catalogue-b", "b", "json", b"{}", 1)
        .unwrap();
    let members = [a.record_ids[0].clone(), b.record_ids[0].clone()];
    store
        .pin("cancel-catalogue-pin-a", "a", &members, 40, 2)
        .unwrap();
    store
        .pin("cancel-catalogue-pin-b", "b", &members, 40, 2)
        .unwrap();
    let mut source = SourceRead {
        page_size: 1,
        ..Default::default()
    };
    let mut artifacts = ArtifactRead {
        page_size: 1,
        ..Default::default()
    };
    let mut pins = PinRead {
        page_size: 1,
        ..Default::default()
    };
    let sources = store.source_read(&source, 3, &[]).unwrap();
    let parents = store.artifact_read(&artifacts, 3).unwrap();
    let collections = store.pin_read(&pins, 3).unwrap();
    let token = QueryCancellation::default();
    token.cancel();
    for continuation in [false, true] {
        if continuation {
            source.cursor = sources.next_cursor.clone();
            artifacts.cursor = parents.next_cursor.clone();
            pins.cursor = collections.next_cursor.clone();
        }
        assert_eq!(
            store
                .source_read_with_syslog_cancellable(&source, 4, &[], &[], &token)
                .unwrap_err()
                .code,
            "CANCELLED"
        );
        assert_eq!(
            store
                .artifact_read_cancellable(&artifacts, 4, &token)
                .unwrap_err()
                .code,
            "CANCELLED"
        );
        assert_eq!(
            store
                .pin_read_cancellable(&pins, 4, &token)
                .unwrap_err()
                .code,
            "CANCELLED"
        );
    }
    for (id, cursor) in [
        (&sources.snapshot_id, sources.next_cursor.as_ref().unwrap()),
        (&parents.snapshot_id, parents.next_cursor.as_ref().unwrap()),
        (
            &collections.snapshot_id,
            collections.next_cursor.as_ref().unwrap(),
        ),
    ] {
        assert!(store.result_next(&sources.store_id, cursor, 5).is_ok());
        assert!(store.result_release(&sources.store_id, id, 5).unwrap());
    }
    let status = store.status(5).unwrap();
    for key in [
        "source_read_handles",
        "artifact_read_handles",
        "pin_read_handles",
    ] {
        assert_eq!(status[key].as_str(), Some("0"));
    }
    assert_eq!(status["live_records"].as_str(), Some("2"));
    assert_eq!(status["active_pins"].as_str(), Some("2"));
    assert_eq!(store.bytes(&members[0], 0, 2, 5).unwrap(), b"{}");
}

#[test]
fn receiver_observations_commit_with_packets_and_outlive_summary_only_under_pins() {
    use sparrow_core::{ReceiverDiscard, SourceRead, SyslogReceiver};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let receiver = SyslogReceiver::new("udp", "udp").unwrap();
    let raw = b"<999>uninterpretable packet";
    let first = store
        .ingest_received_syslog("receiver", &receiver, raw, 10)
        .unwrap();
    let position = store
        .record(&first.record_ids[0])
        .unwrap()
        .receiver
        .unwrap();
    assert_eq!(position.sequence, 1);
    let discard = store
        .discard_syslog("receiver", &receiver, ReceiverDiscard::Truncated, 9)
        .unwrap();
    assert_eq!(discard.window_id, position.window_id);
    assert_eq!(discard.sequence, 2);
    let second = store
        .ingest_received_syslog("receiver", &receiver, raw, 11)
        .unwrap();
    let record = store.record(&second.record_ids[0]).unwrap();
    assert_ne!(first.id, second.id);
    assert_eq!(first.payload_id, second.payload_id);
    assert_eq!(record.receiver.as_ref().unwrap().sequence, 3);
    store.with_rdf_record_layers(&[first.record_ids[0].clone(), second.record_ids[0].clone()], sparrow_core::RdfGraphLayers { metadata: true, diagnostic: false, content: false }, 11, sparrow_core::RdfReadLimits::default(), |view| {
        let result = purrdf::sparql::NativeSparqlEngine::new().query_governed_fallible_view(view,
            purrdf::SparqlRequest { query: "PREFIX sp: <urn:sparrow:vocab:> SELECT ?record ?window ?sequence WHERE { ?record sp:receiverWindow ?window ; sp:stream ?window ; sp:receiverSequence ?sequence . ?window a sp:Stream ; sp:streamKind \"receiver-window\" ; sp:source ?source . ?source a sp:Source }", base_iri: None, substitutions: &[] },
            purrdf::sparql::QueryOptions::default(), &purrdf::sparql::QueryGovernors::METERED.with_fuel(100000).with_max_answers(10).with_max_intermediate_cells(1000).with_max_scratch_bytes(1024*1024).with_max_remote_requests(0)).unwrap().result;
        let (_, rows) = result.solutions().expect("receiver metadata did not produce solutions");
        assert_eq!(rows.len(), 2);
    }).unwrap();
    assert!(record.producer.is_none());
    assert!(record.syslog.unwrap().diagnostic.is_some());
    let history = store.receiver_window(&position.window_id, 11).unwrap();
    assert_eq!(
        (
            history.observations,
            history.accepted,
            history.discarded,
            history.truncated
        ),
        (3, 2, 1, 1)
    );
    assert_eq!((history.gaps[0].first, history.gaps[0].last), (2, 2));
    assert_eq!(
        (history.first_received_at, history.last_received_at),
        (9, 11)
    );
    let captured = store.source_read(&SourceRead::default(), 11, &[]).unwrap();
    assert_eq!(
        captured.body.sources[0].receiver_window_ids,
        std::slice::from_ref(&position.window_id)
    );
    store
        .result_release(&captured.store_id, &captured.snapshot_id, 11)
        .unwrap();
    store
        .watch_registry_replace(None, b"receiver-watch")
        .unwrap();
    let pin = store
        .pin(
            "receiver-pin",
            "receiver-pin",
            &[first.record_ids[0].clone()],
            40,
            12,
        )
        .unwrap();
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.status(12).unwrap()["store_format"].as_str(),
        Some("sparrow.store/85")
    );
    assert_eq!(
        store.watch_registry().unwrap().as_deref(),
        Some(b"receiver-watch".as_slice())
    );
    assert_eq!(
        store
            .receiver_window(&position.window_id, 12)
            .unwrap()
            .observations,
        3
    );
    assert_eq!(
        store.bytes(&first.record_ids[0], 0, raw.len(), 12).unwrap(),
        raw
    );
    let deadline = 30 * DAY + 11;
    assert_eq!(
        store
            .receiver_window(&position.window_id, deadline)
            .unwrap_err()
            .code,
        "NOT_FOUND"
    );
    store.expire(deadline).unwrap();
    assert_eq!(
        store.record(&first.record_ids[0]).unwrap().receiver,
        Some(position.clone())
    );
    assert_eq!(
        store
            .bytes(&first.record_ids[0], 0, raw.len(), deadline)
            .unwrap(),
        raw
    );
    let resumed = store
        .ingest_received_syslog("receiver", &receiver, raw, deadline)
        .unwrap();
    let fresh = store
        .record(&resumed.record_ids[0])
        .unwrap()
        .receiver
        .unwrap();
    assert_ne!(fresh.window_id, position.window_id);
    assert_eq!(fresh.sequence, 1);
    store
        .release_retry("receiver-release", &pin.id, deadline)
        .unwrap();
}

#[test]
fn receiver_concurrent_commits_have_unique_positions_and_finite_discard_history() {
    use sparrow_core::{ReceiverDiscard, SyslogReceiver};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let receiver = SyslogReceiver::new("tcp", "tcp_lf").unwrap();
    let mut threads = Vec::new();
    for index in 0..8 {
        let store = store.clone();
        let receiver = receiver.clone();
        threads.push(std::thread::spawn(move || {
            let receipt = store
                .ingest_received_syslog(
                    "concurrent",
                    &receiver,
                    b"<13>1 - host app - - - same",
                    20 - index,
                )
                .unwrap();
            store
                .record(&receipt.record_ids[0])
                .unwrap()
                .receiver
                .unwrap()
        }));
    }
    let mut positions = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    positions.sort_by_key(|position| position.sequence);
    assert_eq!(
        positions
            .iter()
            .map(|position| position.sequence)
            .collect::<Vec<_>>(),
        (1..=8).collect::<Vec<_>>()
    );
    assert!(
        positions
            .iter()
            .all(|position| position.window_id == positions[0].window_id)
    );
    for _ in 0..20 {
        store
            .discard_syslog("concurrent", &receiver, ReceiverDiscard::FramingFailure, 21)
            .unwrap();
    }
    let state = store.receiver_window(&positions[0].window_id, 21).unwrap();
    assert_eq!(
        (
            state.observations,
            state.accepted,
            state.discarded,
            state.framing_failures
        ),
        (28, 8, 20, 20)
    );
    assert_eq!(state.gaps.len(), sparrow_core::MAX_PRODUCER_GAPS);
    assert!(!state.gaps_complete);
    assert_eq!(
        (state.gaps[0].first, state.gaps.last().unwrap().last),
        (13, 28)
    );
    assert_eq!((state.first_received_at, state.last_received_at), (13, 21));
    let restarted = SyslogReceiver::new("tcp", "tcp_lf").unwrap();
    let restart = store
        .discard_syslog("concurrent", &restarted, ReceiverDiscard::TimedOut, 22)
        .unwrap();
    assert_ne!(restart.window_id, state.id);
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.receiver_window(&state.id, 22).unwrap().observations,
        28
    );
    assert_eq!(
        store
            .receiver_window(&restart.window_id, 22)
            .unwrap()
            .timed_out,
        1
    );
}

#[test]
fn receiver_registry_admission_and_fence_corruption_are_hard_failures() {
    use sparrow_core::{MAX_RECEIVER_WINDOWS, ReceiverDiscard, SyslogReceiver};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    for _ in 0..MAX_RECEIVER_WINDOWS {
        store
            .discard_syslog(
                "only-discards",
                &SyslogReceiver::new("udp", "udp").unwrap(),
                ReceiverDiscard::Refused,
                1,
            )
            .unwrap();
    }
    assert_eq!(
        store
            .discard_syslog(
                "only-discards",
                &SyslogReceiver::new("udp", "udp").unwrap(),
                ReceiverDiscard::Refused,
                2
            )
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(store.status(2).unwrap()["live_records"].as_str(), Some("0"));
    let sources = store
        .source_read(&sparrow_core::SourceRead::default(), 2, &[])
        .unwrap();
    assert_eq!(sources.body.sources[0].source_id, "only-discards");
    assert_eq!(sources.body.sources[0].retained_records, 0);
    assert_eq!(
        sources.body.sources[0].receiver_window_ids.len(),
        MAX_RECEIVER_WINDOWS
    );
    store
        .result_release(&sources.store_id, &sources.snapshot_id, 2)
        .unwrap();
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    let intact = db.get("syslog-receiver-registry").unwrap().unwrap();
    assert_eq!(
        db.get("store_format").unwrap().as_deref(),
        Some(b"sparrow.store/84".as_slice())
    );
    db.delete("syslog-receiver-registry").unwrap();
    drop(db);
    assert_eq!(
        Store::open(dir.path(), config()).err().unwrap().code,
        "STORAGE_FAILURE"
    );
    let db = catalog_db(dir.path()).unwrap();
    db.put("syslog-receiver-registry", &intact).unwrap();
    db.put("store_format", b"sparrow.store/80").unwrap();
    drop(db);
    assert_eq!(
        Store::open(dir.path(), config()).err().unwrap().code,
        "STORAGE_FAILURE"
    );
    let db = catalog_db(dir.path()).unwrap();
    db.put("store_format", b"sparrow.store/84").unwrap();
    let value = purrdf_lex::json::read_slice(&intact, purrdf_lex::json::Limits::DEFAULT).unwrap();
    let mut rows = value.as_array().unwrap().clone();
    rows[1] = rows[0].clone();
    db.put(
        "syslog-receiver-registry",
        purrdf_lex::json::write_compact(&purrdf_lex::json::Value::array(rows)),
    )
    .unwrap();
    drop(db);
    assert_eq!(
        Store::open(dir.path(), config()).err().unwrap().code,
        "STORAGE_FAILURE"
    );
    let db = catalog_db(dir.path()).unwrap();
    db.put(
        "syslog-receiver-registry",
        vec![b' '; sparrow_core::MAX_RECEIVER_REGISTRY_BYTES + 1],
    )
    .unwrap();
    drop(db);
    assert_eq!(
        Store::open(dir.path(), config()).err().unwrap().code,
        "STORAGE_FAILURE"
    );
}

#[test]
fn temporal_queries_do_not_load_records_outside_the_half_open_interval() {
    use sparrow_core::{JsonQuery, QueryScope, QuerySelection, RecordQuery};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let past = store
        .ingest("past", "source", "json", br#"{"message":"past"}"#, 10)
        .unwrap();
    let inside = store
        .ingest("inside", "source", "json", br#"{"message":"inside"}"#, 20)
        .unwrap();
    let future = store
        .ingest("future", "source", "json", br#"{"message":"future"}"#, 30)
        .unwrap();
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    for artifact in [past, future] {
        db.put(&artifact.record_ids[0], b"broken catalog encoding")
            .unwrap();
    }
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    let scope = QueryScope {
        selection: QuerySelection::Live,
        source: None,
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 20,
        received_before: 30,
        limit: 100,
        scan_budget: 100,
    };
    let query = RecordQuery::default();
    let rows = store.records_query(&scope, &query, 40).unwrap();
    let context_error = store
        .records_context(&inside.record_ids[0], &scope, 0, 0, 40)
        .unwrap_err();
    assert_eq!(context_error.code, "STORAGE_FAILURE");
    assert!(
        context_error
            .message
            .contains("repair the authoritative catalog and reopen")
    );
    assert_eq!(rows["rows"].as_array().unwrap().len(), 1);
    assert_eq!(
        rows["rows"][0]["record_id"].as_str(),
        Some(inside.record_ids[0].as_str())
    );
    assert_eq!(
        store.text_query(&scope, "inside", 40).unwrap()["rows"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    for discovery in [
        JsonQuery::Keys { path: None },
        JsonQuery::Values {
            path: Some("/message".into()),
            kind: None,
            lexical: None,
            decoded: Some("inside".into()),
        },
    ] {
        assert!(
            !store.json_query(&scope, &discovery, 40).unwrap()["rows"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .json_query(
                    &QueryScope {
                        received_before: 31,
                        ..scope.clone()
                    },
                    &discovery,
                    40
                )
                .unwrap_err()
                .code,
            "STORAGE_FAILURE"
        );
    }
    for widened in [
        QueryScope {
            received_before: 31,
            ..scope.clone()
        },
        QueryScope {
            received_from: 10,
            ..scope
        },
    ] {
        assert_eq!(
            store.records_query(&widened, &query, 40).unwrap_err().code,
            "STORAGE_FAILURE"
        );
        assert_eq!(
            store.text_query(&widened, "inside", 40).unwrap_err().code,
            "STORAGE_FAILURE"
        );
    }
}

#[test]
fn completed_tail_claim_preserves_original_publication_after_checkpoint_expiry() {
    use sparrow_core::{ConsumptionGeneration, ConsumptionHandoff, TailFinishIntent, TailStart};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store.watch_registry_replace(None, b"registered").unwrap();
    let mut declaration = TailStart {
        request_id: "retirement-start".into(),
        watch: "tail".into(),
        watch_revision: 1,
        directories: CONSUMPTION_DIRECTORIES,
        path: b"active.log".to_vec(),
        source: "retirement-source".into(),
        format: "log".into(),
        device: 1,
        inode: 42,
    };
    let generation = store.tail_start(&declaration, 1).unwrap();
    let original = store
        .tail_append(&generation.id, 0, b"first\n", false, 2)
        .unwrap()
        .artifact
        .unwrap();
    let record = store.record(&original.record_ids[0]).unwrap();
    let finish = TailFinishIntent {
        request_id: "retirement-finish".into(),
        generation_id: generation.id.clone(),
        watch_revision: 1,
        observed: ConsumptionGeneration {
            device: 1,
            inode: 42,
            byte_length: 10,
            modified: (3, 4),
            changed: (5, 6),
        },
    };
    // The producer may have rotated the path before declaring final clocks.
    let intent = ConsumptionHandoff {
        request_id: "retirement-handoff".into(),
        watch: "tail".into(),
        watch_revision: 1,
        relative_path: b"rotated.log".to_vec(),
        observed: finish.observed.clone(),
        tail_generation_id: Some(generation.id.clone()),
    };
    store.tail_finish_start(&finish, 3).unwrap();
    assert_eq!(
        store
            .consumption_handoff_reserve(
                &intent,
                &declaration.source,
                "log",
                CONSUMPTION_DIRECTORIES,
                4
            )
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert!(store.consumption_pending().unwrap().is_empty());
    let final_record = store
        .tail_append(&generation.id, 6, b"last", true, 5)
        .unwrap()
        .artifact
        .unwrap();
    let terminal = store.tail_generation(&generation.id).unwrap();
    for field in 0..4 {
        let mut changed = intent.clone();
        match field {
            0 => changed.observed.changed.1 += 1,
            1 => changed.observed.modified.1 += 1,
            2 => changed.watch_revision += 1,
            _ => changed.watch = "another-watch".into(),
        }
        assert_eq!(
            store
                .consumption_handoff_reserve(
                    &changed,
                    &declaration.source,
                    "log",
                    CONSUMPTION_DIRECTORIES,
                    6
                )
                .unwrap_err()
                .code,
            "CONFLICT"
        );
    }
    let mut wrong_root = CONSUMPTION_DIRECTORIES;
    wrong_root.root_inode += 1;
    assert_eq!(
        store
            .consumption_handoff_reserve(&intent, &declaration.source, "log", wrong_root, 6)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    let reserved = store
        .consumption_handoff_reserve(
            &intent,
            &declaration.source,
            "log",
            CONSUMPTION_DIRECTORIES,
            6,
        )
        .unwrap();
    assert_eq!(reserved.tail, Some(terminal.clone()));
    assert_eq!(reserved.published_id(), None);
    drop(store);
    let key = format!("consumption:{}", reserved.import_id);
    let saved = {
        let db = catalog_db(dir.path()).unwrap();
        db.get(&key).unwrap().unwrap()
    };
    for corruption in 0..5 {
        {
            use purrdf_lex::json;
            let db = catalog_db(dir.path()).unwrap();
            let mut value = json::read_slice(&saved, json::Limits::DEFAULT).unwrap();
            match corruption {
                0 => *value.get_mut("tail").unwrap() = json::Value::Null,
                1 => *value.get_mut("tail_published").unwrap() = true.into(),
                2 => {
                    *value
                        .get_mut("tail")
                        .unwrap()
                        .get_mut("started_at")
                        .unwrap() = "0".into()
                }
                3 => {
                    let json::Value::Object(object) = &mut value else {
                        panic!("journal is an object")
                    };
                    object.remove("tail_published").unwrap();
                }
                _ => db.put("store_format", b"sparrow.store/80").unwrap(),
            }
            db.put(&key, json::write_compact(&value)).unwrap();
        }
        let store = Store::open(dir.path(), config()).unwrap();
        assert_eq!(
            store.consumption_get(&reserved.import_id).unwrap_err().code,
            "STORAGE_FAILURE"
        );
        drop(store);
        let db = catalog_db(dir.path()).unwrap();
        db.put(&key, &saved).unwrap();
        db.put("store_format", b"sparrow.store/81").unwrap();
    }
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.properties().unwrap()["store_format"].as_str(),
        Some("sparrow.store/81")
    );
    assert_eq!(
        store
            .consumption_publish_tail(&reserved.import_id, &intent.observed)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    let mut claimed = intent.observed.clone();
    claimed.changed = (7, 8); // Atomic rename changes ctime; content identity stays fixed.
    let claim = store
        .consumption_claimed(&reserved.import_id, claimed.clone())
        .unwrap();
    assert_eq!(
        store
            .import_begin(
                &claim.request_id,
                &declaration.source,
                "log",
                10,
                "blake3:0000000000000000000000000000000000000000000000000000000000000000",
                7
            )
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert_eq!(
        store
            .consumption_removed(&claim.import_id, &generation.id, &claimed, 7)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    // Expire the bounded generation registry while ownership remains pending.
    declaration.request_id = "replacement-start".into();
    declaration.inode += 1;
    store.tail_start(&declaration, 32 * DAY).unwrap();
    assert_eq!(
        store.tail_generation(&generation.id).unwrap_err().code,
        "NOT_FOUND"
    );
    assert_eq!(
        store
            .consumption_handoff_reserve(
                &intent,
                &declaration.source,
                "log",
                CONSUMPTION_DIRECTORIES,
                32 * DAY
            )
            .unwrap(),
        claim
    );
    let published = store
        .consumption_publish_tail(&claim.import_id, &claimed)
        .unwrap();
    assert_eq!(published.published_id(), Some(generation.id.as_str()));
    assert!(published.published_artifact.is_none());
    assert_eq!(published.tail, Some(terminal));
    let selector = sparrow_core::IngestSelector::Handoff(intent.request_id.clone());
    let progress = store
        .ingest_status(&selector, Default::default(), 32 * DAY)
        .unwrap();
    assert_eq!(progress.deletion, sparrow_core::IngestDeletion::Published);
    assert!(progress.accepted.is_none());
    assert!(progress.staging.is_none());
    assert_eq!(progress.tail, published.tail);
    assert_eq!(progress.total_records, Some(0));
    assert_eq!(progress.inspected_records, 0);
    assert_eq!(progress.text, Default::default());
    assert_eq!(progress.consumption.as_ref(), Some(&published));
    assert_eq!(
        store
            .consumption_publish_tail(&claim.import_id, &claimed)
            .unwrap(),
        published
    );
    let unchanged = store.record(&record.id).unwrap();
    assert_eq!(unchanged.received_at, record.received_at);
    assert_eq!(unchanged.expires_at, record.expires_at);
    assert_eq!(unchanged.tail, record.tail);
    assert_eq!(store.records(10).unwrap().len(), 2);
    assert!(store.records(32 * DAY).unwrap().is_empty());
    assert_eq!(
        store.bytes(&final_record.record_ids[0], 0, 4, 10).unwrap(),
        b"last"
    );
    let removed = store
        .consumption_removed(&claim.import_id, &generation.id, &claimed, 32 * DAY)
        .unwrap();
    assert_eq!(removed.removed_at, Some(32 * DAY));
    let progress = store
        .ingest_status(&selector, Default::default(), 32 * DAY)
        .unwrap();
    assert_eq!(progress.deletion, sparrow_core::IngestDeletion::Removed);
    assert_eq!(progress.removed_at, Some(32 * DAY));
    assert_eq!(progress.tail, removed.tail);
    assert_eq!(progress.total_records, Some(0));
    assert!(store.consumption_pending().unwrap().is_empty());
    assert_eq!(
        store
            .consumption_removed(&claim.import_id, &generation.id, &claimed, 33 * DAY)
            .unwrap(),
        removed
    );
    store
        .watch_registry_replace(Some(b"registered"), b"changed")
        .unwrap();
    // Ordinary subsequent handoffs cannot downgrade the tail writer fence.
    let mut ordinary = intent.clone();
    ordinary.request_id = "ordinary-after-tail".into();
    ordinary.tail_generation_id = None;
    store
        .consumption_handoff_reserve(
            &ordinary,
            "ordinary",
            "log",
            CONSUMPTION_DIRECTORIES,
            32 * DAY,
        )
        .unwrap();
    assert_eq!(
        store.properties().unwrap()["store_format"].as_str(),
        Some("sparrow.store/81")
    );
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.consumption_handoff_get(&intent, 33 * DAY).unwrap(),
        Some(removed)
    );
    assert_eq!(
        store
            .consumption_handoff_get(&intent, 35 * DAY)
            .unwrap_err()
            .code,
        "NOT_FOUND"
    );
}

#[test]
fn completed_file_bytes_span_original_records_and_follow_full_cover_pin_eligibility() {
    use sparrow_core::TailStart;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let generation = store
        .tail_start(
            &TailStart {
                request_id: "download-start".into(),
                watch: "tail".into(),
                watch_revision: 1,
                directories: CONSUMPTION_DIRECTORIES,
                path: b"download.log".to_vec(),
                source: "download-source".into(),
                format: "log".into(),
                device: 7,
                inode: 42,
            },
            1,
        )
        .unwrap();
    let mut original = vec![b'x'; 70_000];
    original.extend_from_slice(b"\nsmall\r\n");
    let prefix = store
        .tail_append(&generation.id, 0, &original, false, 2)
        .unwrap()
        .artifact
        .unwrap();
    assert_eq!(
        store.bytes(&generation.id, 0, 1, 3).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    let final_part = store
        .tail_append(&generation.id, original.len() as u64, b"last", true, 3)
        .unwrap()
        .artifact
        .unwrap();
    original.extend_from_slice(b"last");
    let before = store.record(&prefix.record_ids[0]).unwrap();
    assert_eq!(
        store.bytes(&generation.id, 0, original.len(), 4).unwrap(),
        original
    );
    assert_eq!(
        store.bytes(&generation.id, 69_999, 13, 4).unwrap(),
        original[69_999..70_012]
    );
    assert!(
        store
            .bytes(&generation.id, original.len() as u64, 0, 4)
            .unwrap()
            .is_empty()
    );
    for (offset, length) in [(original.len() as u64, 1), (u64::MAX, 1)] {
        assert_eq!(
            store
                .bytes(&generation.id, offset, length, 4)
                .unwrap_err()
                .code,
            "INVALID_ARGUMENT"
        );
    }
    assert_eq!(
        store
            .bytes(&generation.id, 0, 1024 * 1024 + 1, 4)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    let pin = store
        .pin(
            "download-pin",
            "case",
            &[prefix.id.clone(), final_part.record_ids[0].clone()],
            40,
            4,
        )
        .unwrap();
    store.expire(31 * DAY).unwrap();
    assert_eq!(
        store
            .bytes(&generation.id, 0, original.len(), 31 * DAY)
            .unwrap(),
        original
    );
    assert_eq!(
        store.record(&before.id).unwrap().received_at,
        before.received_at
    );
    assert_eq!(
        store.record(&before.id).unwrap().expires_at,
        before.expires_at
    );
    let survivor = store
        .pin(
            "download-last-pin",
            "last-case",
            &final_part.record_ids,
            40,
            31 * DAY,
        )
        .unwrap();
    store.release(&pin.id).unwrap();
    // Unretired but expired records must already refuse the complete-file read.
    assert_eq!(
        store
            .bytes(&generation.id, 70_008, 4, 31 * DAY)
            .unwrap_err()
            .code,
        "EVIDENCE_UNAVAILABLE"
    );

    store.expire(31 * DAY).unwrap();
    assert_eq!(
        store
            .bytes(&generation.id, 70_008, 4, 31 * DAY)
            .unwrap_err()
            .code,
        "EVIDENCE_UNAVAILABLE"
    );
    assert_eq!(
        store
            .bytes(&final_part.record_ids[0], 0, 4, 31 * DAY)
            .unwrap(),
        b"last"
    );
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .bytes(&generation.id, 70_008, 4, 31 * DAY)
            .unwrap_err()
            .code,
        "EVIDENCE_UNAVAILABLE"
    );
    assert_eq!(
        store
            .bytes(&final_part.record_ids[0], 0, 4, 31 * DAY)
            .unwrap(),
        b"last"
    );
    store.release(&survivor.id).unwrap();
}

#[test]
fn complete_file_reads_refuse_missing_and_foreign_range_members_without_returning_a_prefix() {
    use sparrow_core::TailStart;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let start = TailStart {
        request_id: "cover-first".into(),
        watch: "tail".into(),
        watch_revision: 1,
        directories: CONSUMPTION_DIRECTORIES,
        path: b"first.log".to_vec(),
        source: "cover-source".into(),
        format: "log".into(),
        device: 7,
        inode: 42,
    };
    let generation = store.tail_start(&start, 1).unwrap();
    let records = store
        .tail_append(&generation.id, 0, b"a\nb\n", true, 2)
        .unwrap()
        .artifact
        .unwrap();
    let mut other = start;
    other.request_id = "cover-second".into();
    other.inode = 43;
    other.path = b"second.log".to_vec();
    let foreign = store.tail_start(&other, 3).unwrap();
    let foreign_record = store
        .tail_append(&foreign.id, 0, b"z\n", true, 4)
        .unwrap()
        .artifact
        .unwrap();
    other.request_id = "cover-empty".into();
    other.inode = 44;
    other.path = b"empty.log".to_vec();
    let empty = store.tail_start(&other, 5).unwrap();
    store.tail_append(&empty.id, 0, b"", true, 6).unwrap();
    assert!(store.bytes(&empty.id, 0, 0, 7).unwrap().is_empty());
    assert_eq!(
        store.bytes(&empty.id, 0, 1, 7).unwrap_err().code,
        "INVALID_ARGUMENT"
    );
    assert_eq!(
        store.bytes(&empty.id, 0, 0, 31 * DAY).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    drop(store);
    let key = format!(
        "tail-range:{}:{:020}:{}",
        generation.id, 2, records.record_ids[1]
    );
    {
        let db = catalog_db(dir.path()).unwrap();
        db.put(&key, foreign_record.record_ids[0].as_bytes())
            .unwrap();
    }
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.bytes(&generation.id, 0, 1, 5).unwrap_err().code,
        "STORAGE_FAILURE"
    );
    drop(store);
    {
        let db = catalog_db(dir.path()).unwrap();
        db.delete(&key).unwrap();
    }
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.bytes(&generation.id, 0, 1, 5).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    assert_eq!(
        store.bytes(&records.record_ids[0], 0, 2, 5).unwrap(),
        b"a\n"
    );
    drop(store);
    {
        let db = catalog_db(dir.path()).unwrap();
        db.put(&key, records.record_ids[1].as_bytes()).unwrap();
        db.put(&records.record_ids[1], vec![b'x'; 64 * 1024 + 1])
            .unwrap();
    }
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.bytes(&generation.id, 0, 1, 5).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
}

#[test]
fn retained_tail_ranges_follow_pins_and_upgrade_without_resurrecting_retired_bytes() {
    use purrdf_lex::json::record::ToJson;
    use sparrow_core::{ConsumptionGeneration, TailFinishIntent, TailStart};
    for legacy in 29..=32 {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), config()).unwrap();
        if legacy >= 31 {
            store
                .watch_registry_replace(None, b"preserved-tail-watch")
                .unwrap();
        }
        let generation = store
            .tail_start(
                &TailStart {
                    request_id: "range-start".into(),
                    watch: "tail".into(),
                    watch_revision: 1,
                    directories: CONSUMPTION_DIRECTORIES,
                    path: b"range.log".to_vec(),
                    source: "range-source".into(),
                    format: "log".into(),
                    device: 7,
                    inode: 42,
                },
                1,
            )
            .unwrap();
        let intent = TailFinishIntent {
            request_id: "range-finish".into(),
            generation_id: generation.id.clone(),
            watch_revision: 1,
            observed: ConsumptionGeneration {
                device: 7,
                inode: 42,
                byte_length: 6,
                modified: (1, 2),
                changed: (3, 4),
            },
        };
        store.tail_finish_start(&intent, 2).unwrap();
        let artifact = store
            .tail_append(&generation.id, 0, b"a\nb\nc\n", true, 3)
            .unwrap()
            .artifact
            .unwrap();
        let original = store.record(&artifact.record_ids[1]).unwrap();
        let finished = store.tail_finish_get(&intent).unwrap().unwrap();
        assert!(finished.complete);
        let pin = store
            .pin("range-pin", "case", &artifact.record_ids[1..2], 40, 4)
            .unwrap();
        drop(store);
        {
            let db = catalog_db(dir.path()).unwrap();
            let rows = db
                .prefix_iterator(b"tail-range:")
                .map(|row| row.unwrap())
                .take_while(|(key, _)| key.starts_with(b"tail-range:"))
                .collect::<Vec<_>>();
            assert_eq!(rows.len(), 3);
            for (index, (key, value)) in rows.iter().enumerate() {
                assert_eq!(value.as_ref(), artifact.record_ids[index].as_bytes());
                assert!(
                    std::str::from_utf8(key)
                        .unwrap()
                        .contains(&format!(":{:020}:", index * 2))
                );
            }
        }
        let store = Store::open(dir.path(), config()).unwrap();
        store.expire(31 * DAY).unwrap();
        assert_eq!(store.bytes(&original.id, 0, 2, 31 * DAY).unwrap(), b"b\n");
        // Construct the actual legacy encoding: its root carries effective
        // ownership directly and has no retirement checkpoint or feature fence.
        let legacy_artifact =
            purrdf_lex::json::write_compact(&store.artifact(&artifact.id).unwrap().to_json());
        drop(store);
        {
            let db = catalog_db(dir.path()).unwrap();
            assert_eq!(
                db.prefix_iterator(b"tail-range:")
                    .take_while(|row| row.as_ref().unwrap().0.starts_with(b"tail-range:"))
                    .count(),
                1
            );
            remove_tail_range_index(&db).unwrap();
            remove_artifact_rdf_upgrade(&db).unwrap();
            db.put(&artifact.id, &legacy_artifact).unwrap();
            db.delete(format!("artifact-retirement:{}", artifact.id))
                .unwrap();
            db.delete("manifest-retirement-features").unwrap();
            db.put("store_format", format!("sparrow.store/{legacy}"))
                .unwrap();
        }
        let store = Store::open(dir.path(), config()).unwrap();
        assert_eq!(store.tail_finish_get(&intent).unwrap(), Some(finished));
        let recovered = store.record(&original.id).unwrap();
        assert_eq!(recovered.received_at, original.received_at);
        assert_eq!(recovered.expires_at, original.expires_at);
        assert_eq!(recovered.tail, original.tail);
        assert_eq!(store.bytes(&original.id, 0, 2, 31 * DAY).unwrap(), b"b\n");
        for index in [0, 2] {
            assert!(
                store
                    .bytes(&artifact.record_ids[index], 0, 2, 31 * DAY)
                    .is_err()
            );
        }
        drop(store);
        {
            let db = catalog_db(dir.path()).unwrap();
            assert_eq!(
                db.prefix_iterator(b"tail-range:")
                    .take_while(|row| row.as_ref().unwrap().0.starts_with(b"tail-range:"))
                    .count(),
                1
            );
        }
        let store = Store::open(dir.path(), config()).unwrap();
        store.release(&pin.id).unwrap();
        store.expire(31 * DAY).unwrap();
        drop(store);
        let db = catalog_db(dir.path()).unwrap();
        assert!(
            db.prefix_iterator(b"tail-range:")
                .next()
                .is_none_or(|row| !row.unwrap().0.starts_with(b"tail-range:"))
        );
        db.delete("tail-range-index-upgrade").unwrap();
        drop(db);
        assert_eq!(
            Store::open(dir.path(), config()).err().unwrap().code,
            "STORAGE_FAILURE"
        );
    }
}

#[test]
fn tail_finish_admission_and_terminal_publication_survive_reopen_without_duplicate_records_or_refreshed_clocks()
 {
    use sparrow_core::{ConsumptionGeneration, TailFinishIntent, TailStart};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let generation = store
        .tail_start(
            &TailStart {
                request_id: "finish-start".into(),
                watch: "tail".into(),
                watch_revision: 1,
                directories: CONSUMPTION_DIRECTORIES,
                path: b"active.log".to_vec(),
                source: "finish-source".into(),
                format: "log".into(),
                device: 7,
                inode: 42,
            },
            1,
        )
        .unwrap();
    let first = store
        .tail_append(&generation.id, 0, b"first\n", false, 2)
        .unwrap()
        .artifact
        .unwrap();
    let record = store.record(&first.record_ids[0]).unwrap();
    let intent = TailFinishIntent {
        request_id: "finish-1".into(),
        generation_id: generation.id.clone(),
        watch_revision: 1,
        observed: ConsumptionGeneration {
            device: 7,
            inode: 42,
            byte_length: 10,
            modified: (3, 4),
            changed: (5, 6),
        },
    };
    assert!(store.tail_finish_get(&intent).unwrap().is_none());
    let admitted = store.tail_finish_start(&intent, 3).unwrap();
    assert!(!admitted.complete);
    assert_eq!(admitted.finish.as_ref().unwrap().admitted_at, 3);
    assert_eq!(store.tail_finish_start(&intent, 4).unwrap(), admitted);
    let mut changed = intent.clone();
    changed.observed.byte_length = 11;
    assert_eq!(
        store.tail_finish_start(&changed, 4).unwrap_err().code,
        "CONFLICT"
    );
    assert_eq!(
        store
            .tail_append(&generation.id, 6, b"last-extra\n", false, 4)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert_eq!(
        store
            .tail_append(&generation.id, 6, b"x", true, 4)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert_eq!(store.tail_generation(&generation.id).unwrap().offset, 6);
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(store.tail_finish_get(&intent).unwrap(), Some(admitted));
    let final_record = store
        .tail_append(&generation.id, 6, b"last", true, 5)
        .unwrap()
        .artifact
        .unwrap();
    let finished = store.tail_finish_get(&intent).unwrap().unwrap();
    assert!(finished.complete);
    assert_eq!(finished.offset, 10);
    assert_eq!(finished.records, 2);
    assert_eq!(finished.finish.as_ref().unwrap().finished_at, Some(5));
    assert_eq!(
        store
            .tail_append(&generation.id, 6, b"last", true, 6)
            .unwrap()
            .artifact
            .unwrap()
            .id,
        final_record.id
    );
    assert_eq!(store.tail_finish_start(&intent, 100).unwrap(), finished);
    let retried_record = store.record(&record.id).unwrap();
    assert_eq!(retried_record.received_at, record.received_at);
    assert_eq!(retried_record.expires_at, record.expires_at);
    assert_eq!(retried_record.tail, record.tail);
    assert_eq!(store.records(100).unwrap().len(), 2);
    assert_eq!(
        store.bytes(&final_record.record_ids[0], 0, 4, 100).unwrap(),
        b"last"
    );
    drop(store);
    assert_eq!(
        Store::open(dir.path(), config())
            .unwrap()
            .tail_finish_get(&intent)
            .unwrap(),
        Some(finished)
    );
}

#[test]
fn current_tail_writer_upgrade_preserves_checkpoints_and_refuses_missing_or_unfenced_completion_authority()
 {
    use sparrow_core::{ConsumptionGeneration, TailFinishIntent, TailStart};
    for legacy in 25..=28 {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), config()).unwrap();
        let generation = store
            .tail_start(
                &TailStart {
                    request_id: "upgrade-tail".into(),
                    watch: "tail".into(),
                    watch_revision: 1,
                    directories: CONSUMPTION_DIRECTORIES,
                    path: b"active.log".to_vec(),
                    source: "upgrade-source".into(),
                    format: "log".into(),
                    device: 7,
                    inode: 42,
                },
                1,
            )
            .unwrap();
        let first = store
            .tail_append(&generation.id, 0, b"n\n", false, 2)
            .unwrap()
            .artifact
            .unwrap();
        let original = store.tail_generation(&generation.id).unwrap();
        drop(store);
        {
            let db = catalog_db(dir.path()).unwrap();
            if legacy >= 27 {
                db.put("watch_registry", b"fixture").unwrap();
            }
            remove_tail_range_index(&db).unwrap();
            remove_artifact_rdf_upgrade(&db).unwrap();
            db.put("store_format", format!("sparrow.store/{legacy}"))
                .unwrap();
        }
        let store = Store::open(dir.path(), config()).unwrap();
        assert_eq!(
            store.properties().unwrap()["store_format"].as_str(),
            Some(format!("sparrow.store/{}", legacy + 52).as_str())
        );
        assert_eq!(store.tail_generation(&generation.id).unwrap(), original);
        assert_eq!(store.bytes(&first.record_ids[0], 0, 2, 3).unwrap(), b"n\n");
        let intent = TailFinishIntent {
            request_id: "unfenced-finish".into(),
            generation_id: generation.id,
            watch_revision: 1,
            observed: ConsumptionGeneration {
                device: 7,
                inode: 42,
                byte_length: 2,
                modified: (3, 4),
                changed: (5, 6),
            },
        };
        store.tail_finish_start(&intent, 3).unwrap();
        drop(store);
        {
            let db = catalog_db(dir.path()).unwrap();
            remove_tail_range_index(&db).unwrap();
            remove_artifact_rdf_upgrade(&db).unwrap();
            db.put("store_format", format!("sparrow.store/{legacy}"))
                .unwrap();
        }
        assert_eq!(
            Store::open(dir.path(), config()).err().unwrap().code,
            "STORAGE_FAILURE"
        );
        {
            let db = catalog_db(dir.path()).unwrap();
            db.delete("tail-generation-registry").unwrap();
        }
        assert_eq!(
            Store::open(dir.path(), config()).err().unwrap().code,
            "STORAGE_FAILURE"
        );
    }
}

#[test]
fn tail_checkpoints_commit_exact_prefixes_and_retries_without_reimporting_or_finalizing_idle_suffixes()
 {
    use sparrow_core::TailStart;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let start = TailStart {
        request_id: "tail-a".into(),
        watch: "tail".into(),
        watch_revision: 1,
        directories: CONSUMPTION_DIRECTORIES,
        path: b"events\xff.jsonl".to_vec(),
        source: "tail-source".into(),
        format: "jsonl".into(),
        device: 7,
        inode: 42,
    };
    let generation = store.tail_start(&start, 1).unwrap();
    let partial = store
        .tail_append(&generation.id, 0, b"{\"n\":1}", false, 2)
        .unwrap();
    assert!(partial.artifact.is_none());
    assert_eq!(partial.pending_bytes, 7);
    assert_eq!(store.tail_generation(&generation.id).unwrap().offset, 0);
    let first = store
        .tail_append(&generation.id, 0, b"{\"n\":1}\r\n{\"n\":", false, 3)
        .unwrap();
    assert_eq!(first.source_end, 9);
    assert_eq!(first.pending_bytes, 5);
    let artifact = first.artifact.unwrap();
    assert_eq!(artifact.record_ids.len(), 1);
    let record = store.record(&artifact.record_ids[0]).unwrap();
    assert_eq!(record.tail.as_ref().unwrap().source_start, 0);
    assert_eq!(record.tail.as_ref().unwrap().source_end, 9);
    store.with_rdf_records(&artifact.record_ids, 4, Default::default(), |view| {
        let run = purrdf::sparql::NativeSparqlEngine::new().query_governed_fallible_view(view,
            purrdf::SparqlRequest { query: "PREFIX sp: <urn:sparrow:vocab:> SELECT ?generation WHERE { ?record sp:fileGeneration ?generation ; sp:stream ?generation . ?generation a sp:Stream ; sp:streamKind \"file-generation\" ; sp:source ?source . ?source a sp:Source }", base_iri: None, substitutions: &[] },
            purrdf::sparql::QueryOptions::default(), &purrdf::sparql::QueryGovernors::METERED.with_fuel(100_000)).unwrap();
        let rows = run.result.solutions().unwrap().1;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some(purrdf::TermValue::iri(format!("urn:sparrow:{}", generation.id))));
    }).unwrap();
    assert_eq!(store.bytes(&record.id, 0, 9, 4).unwrap(), b"{\"n\":1}\r\n");
    assert_eq!(store.tail_start(&start, 4).unwrap().offset, 9);
    assert_eq!(
        store
            .tail_append(
                &generation.id,
                0,
                b"{\"n\":1}\r\nignored unfinished",
                false,
                5
            )
            .unwrap()
            .artifact
            .unwrap()
            .id,
        artifact.id
    );
    assert_eq!(
        store
            .tail_append(&generation.id, 0, b"different\n", false, 5)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    let pin = store
        .pin(
            "tail-pin",
            "tail-pin",
            std::slice::from_ref(&record.id),
            40,
            5,
        )
        .unwrap();
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    let last = store
        .tail_append(&generation.id, 9, b"{\"n\":2}", true, 6)
        .unwrap()
        .artifact
        .unwrap();
    assert_eq!(store.tail_generation(&generation.id).unwrap().offset, 16);
    assert!(store.tail_generation(&generation.id).unwrap().complete);
    assert_eq!(
        store
            .tail_append(&generation.id, 9, b"{\"n\":2}", true, 7)
            .unwrap()
            .artifact
            .unwrap()
            .id,
        last.id
    );
    assert_eq!(
        store
            .tail_append(&generation.id, 16, b"later\n", false, 7)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert!(
        store
            .tail_append(&generation.id, 16, b"", true, 8)
            .unwrap()
            .artifact
            .is_none()
    );
    assert_eq!(store.tail_generation(&generation.id).unwrap().records, 2);
    store.expire(31 * DAY).unwrap();
    assert_eq!(store.record(&record.id).unwrap().tail, record.tail);
    assert_eq!(
        store.bytes(&record.id, 0, 9, 31 * DAY).unwrap(),
        b"{\"n\":1}\r\n"
    );
    store.release(&pin.id).unwrap();
}

#[test]
fn tail_admission_and_discontinuity_never_advance_a_failed_or_replaced_generation() {
    use sparrow_core::TailStart;
    let dir = tempfile::tempdir().unwrap();
    let mut limits = config();
    limits.max_records = 1;
    let store = Store::open(dir.path(), limits).unwrap();
    let start = TailStart {
        request_id: "tail-b".into(),
        watch: "tail".into(),
        watch_revision: 1,
        directories: CONSUMPTION_DIRECTORIES,
        path: b"live.log".to_vec(),
        source: "tail-source".into(),
        format: "log".into(),
        device: 7,
        inode: 42,
    };
    let generation = store.tail_start(&start, 1).unwrap();
    assert_eq!(
        store
            .tail_append(&generation.id, 0, b"one\ntwo\n", false, 2)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(store.tail_generation(&generation.id).unwrap().offset, 0);
    assert!(store.records(2).unwrap().is_empty());
    let one = store
        .tail_append(&generation.id, 0, b"one\n", false, 3)
        .unwrap()
        .artifact
        .unwrap();
    assert_eq!(
        store
            .tail_discontinue(&generation.id, 0, "truncated", 4)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    let gap = store
        .tail_discontinue(&generation.id, 4, "truncated", 4)
        .unwrap();
    assert_eq!(gap.gap.as_deref(), Some("truncated"));
    assert_eq!(
        store
            .tail_discontinue(&generation.id, 4, "truncated", 5)
            .unwrap(),
        gap
    );
    assert_eq!(
        store
            .tail_append(&generation.id, 4, b"later\n", false, 5)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert_eq!(
        store
            .tail_append(&generation.id, 0, b"one\n", false, 5)
            .unwrap()
            .artifact
            .unwrap()
            .id,
        one.id
    );
    let mut replacement = start.clone();
    replacement.request_id = "tail-replacement".into();
    let next = store.tail_start(&replacement, 6).unwrap();
    assert_ne!(next.id, generation.id);
    assert_eq!(next.offset, 0);
    assert_eq!(
        store
            .tail_append(&next.id, 0, b"one\n", false, 7)
            .unwrap()
            .artifact
            .unwrap()
            .record_ids
            .len(),
        1
    );
    assert_eq!(store.records(7).unwrap().len(), 2);
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(store.tail_generation(&generation.id).unwrap(), gap);
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    db.delete("tail-generation-registry").unwrap();
    drop(db);
    assert_eq!(
        Store::open(dir.path(), config()).err().unwrap().code,
        "STORAGE_FAILURE"
    );
}

#[test]
fn syslog_packets_keep_exact_bytes_native_time_diagnostics_and_encoded_parameter_occurrences() {
    use sparrow_core::{EventTime, SyslogDialect};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let modern =
        br#"<34>1 2026-10-08T06:25:00.123+01:00 host app 123 ID [id@32473 p="x\]y" p="\q"] needle
next"#;
    let a = store
        .ingest("syslog-modern", "syslog", "syslog", modern, 1)
        .unwrap();
    assert_eq!(a.record_ids.len(), 1); // Embedded newlines do not split protocol messages.
    let record = store.record(&a.record_ids[0]).unwrap();
    let snapshot = record.syslog.as_ref().unwrap();
    assert_eq!(snapshot.dialect, Some(SyslogDialect::Rfc5424));
    assert_eq!(snapshot.priority, Some(34));
    assert_eq!(snapshot.structured_data.len(), 1);
    let parameters = &snapshot.structured_data[0].parameters;
    assert_eq!(parameters.len(), 2);
    assert_eq!(parameters[0].name, parameters[1].name);
    assert_eq!(
        &modern[parameters[0].value_start as usize..parameters[0].value_end as usize],
        br"x\]y"
    );
    assert_eq!(
        &modern[parameters[1].value_start as usize..parameters[1].value_end as usize],
        br"\q"
    );
    assert!(
        matches!(&record.event_time, EventTime::Known { millis, source_start, source_end, .. }
        if *millis == sparrow_core::parse_instant("2026-10-08T05:25:00.123Z").unwrap() && &modern[*source_start as usize..*source_end as usize] == b"2026-10-08T06:25:00.123+01:00")
    );
    let repeat = store
        .ingest("syslog-repeat", "syslog", "syslog", modern, 2)
        .unwrap();
    assert_ne!(a.id, repeat.id);
    assert_eq!(a.payload_id, repeat.payload_id);
    let legacy = b"<13>Oct  8 06:25:00 host daemon[123]: legacy needle";
    let b = store
        .ingest("syslog-legacy", "syslog", "syslog", legacy, 3)
        .unwrap();
    assert!(
        matches!(store.record(&b.record_ids[0]).unwrap().event_time, EventTime::Unknown { ref reason, .. } if reason == "syslog_legacy_time")
    );
    let sub = store
        .ingest(
            "syslog-submillisecond",
            "syslog",
            "syslog",
            b"<13>1 2026-10-08T06:25:00.123456Z - - - - - needle",
            4,
        )
        .unwrap();
    assert!(
        matches!(store.record(&sub.record_ids[0]).unwrap().event_time, EventTime::Unknown { ref reason, .. } if reason == "syslog_submillisecond")
    );
    let scope = sparrow_core::QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        sources: None,
        source: Some("syslog".into()),
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 100,
        limit: 100,
        scan_budget: 10_000,
    };
    let result = store
        .records_query(
            &scope,
            &sparrow_core::RecordQuery {
                event_interval: Some(sparrow_core::EventInterval {
                    from: sparrow_core::parse_instant("2026-10-08T05:25:00.123Z").unwrap(),
                    before: sparrow_core::parse_instant("2026-10-08T05:25:00.124Z").unwrap(),
                }),
                ..Default::default()
            },
            5,
        )
        .unwrap();
    assert_eq!(result["rows"].as_array().unwrap().len(), 2);
    assert_eq!(
        store.text_query(&scope, "needle", 5).unwrap()["rows"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    let pin = store
        .pin("syslog-pin", "syslog", &[a.record_ids[0].clone()], 40, 5)
        .unwrap();
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .ingest("syslog-modern", "syslog", "syslog", modern, 6)
            .unwrap()
            .id,
        a.id
    );
    store.expire(31 * DAY).unwrap();
    assert_eq!(
        store.record(&a.record_ids[0]).unwrap().syslog,
        record.syslog
    );
    assert_eq!(
        store
            .bytes(&a.record_ids[0], 0, modern.len(), 31 * DAY)
            .unwrap(),
        modern
    );
    store
        .release_retry("syslog-release", &pin.id, 31 * DAY)
        .unwrap();
}

#[test]
fn malformed_syslog_preserves_raw_evidence_and_reports_admission_without_inventing_fields() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    for (index, bytes) in [
        b"<999>bad".as_slice(),
        b"<01>1 - - - - - -",
        b"<34>2 - - - - - -",
        b"<34>1 2026-10-08T06:25:60Z - - - - -",
        b"<34>1 - - - - - [x p=\"bad]\"]",
        b"<34>1 - - - - - [x][x]",
    ]
    .into_iter()
    .enumerate()
    {
        let a = store
            .ingest(&format!("bad-syslog-{index}"), "syslog", "syslog", bytes, 1)
            .unwrap();
        let record = store.record(&a.record_ids[0]).unwrap();
        let snapshot = record.syslog.unwrap();
        assert!(snapshot.diagnostic.is_some(), "{bytes:?}");
        assert!(snapshot.fields.is_empty());
        assert!(snapshot.priority.is_none());
        assert_eq!(store.bytes(&a.id, 0, bytes.len(), 2).unwrap(), bytes);
        assert!(
            matches!(record.event_time, sparrow_core::EventTime::Unknown { ref reason, .. } if reason == "syslog_malformed")
        );
    }
    let before = store.status(2).unwrap()["live_records"].clone();
    assert_eq!(
        store
            .ingest(
                "oversized-syslog",
                "syslog",
                "syslog",
                &vec![b'x'; sparrow_core::SYSLOG_MESSAGE_BYTES + 1],
                2
            )
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(store.status(2).unwrap()["live_records"], before);
    let packet = format!(
        "<13>1 - - - - - [x{}]",
        " p=\"v\"".repeat(sparrow_core::SYSLOG_PARAMETERS + 1)
    );
    let a = store
        .ingest(
            "syslog-parameters-limit",
            "syslog",
            "syslog",
            packet.as_bytes(),
            2,
        )
        .unwrap();
    assert!(
        store
            .record(&a.record_ids[0])
            .unwrap()
            .syslog
            .unwrap()
            .diagnostic
            .unwrap()
            .contains("admission")
    );
}

#[test]
fn syslog_format_upgrade_preserves_existing_producer_windows_and_watch_fences() {
    use sparrow_core::{Durability, ProducerBatch};
    for (legacy, current, watch) in [
        (17, 33, false),
        (18, 34, false),
        (19, 35, true),
        (20, 36, true),
        (21, 33, false),
        (22, 34, false),
        (23, 35, true),
        (24, 36, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), config()).unwrap();
        let a = store
            .ingest_producer(
                "old-producer",
                "producer",
                b"{}\n{}\n",
                &ProducerBatch {
                    instance: [3; 16],
                    sequences: vec![1, 4],
                },
                1,
                Durability::Durable,
            )
            .unwrap();
        let id = store
            .record(&a.record_ids[0])
            .unwrap()
            .producer
            .unwrap()
            .stream_id;
        let syslog = if legacy >= 21 {
            let a = store
                .ingest_syslog(
                    "syslog-migration",
                    b"<13>1 2026-10-08T07:00:00Z host app - - - migration",
                    1,
                )
                .unwrap();
            Some(store.record(&a.record_ids[0]).unwrap())
        } else {
            None
        };
        if watch {
            store.watch_registry_replace(None, b"preserved").unwrap();
        }
        drop(store);
        let db = catalog_db(dir.path()).unwrap();
        remove_tail_range_index(&db).unwrap();
        remove_artifact_rdf_upgrade(&db).unwrap();
        db.put("store_format", format!("sparrow.store/{legacy}"))
            .unwrap();
        db.delete("tail-generation-registry").unwrap();
        drop(db);
        let store = Store::open(dir.path(), config()).unwrap();
        if let Some(record) = syslog {
            assert_eq!(store.record(&record.id).unwrap().syslog, record.syslog);
            assert!(store.record(&record.id).unwrap().tail.is_none());
        }
        assert_eq!(
            store.status(2).unwrap()["store_format"].as_str(),
            Some(format!("sparrow.store/{}", current + 44).as_str())
        );
        let state = store.producer_stream(&id, 2).unwrap();
        assert_eq!(state.accepted_events, 2);
        assert_eq!(state.unobserved_sequences, 2);
        assert!(store.record(&a.record_ids[0]).unwrap().syslog.is_none());
        if watch {
            assert_eq!(
                store.watch_registry().unwrap().as_deref(),
                Some(b"preserved".as_slice())
            );
        }
    }
}

#[test]
fn producer_positions_gaps_retries_and_retention_share_atomic_publication() {
    use sparrow_core::{Durability, ProducerBatch, ProducerGap};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let first = ProducerBatch {
        instance: [7; 16],
        sequences: vec![5, 7],
    };
    let a = store
        .ingest_producer(
            "producer-a",
            "producer",
            b"{}\n{}\n",
            &first,
            1,
            Durability::Buffered,
        )
        .unwrap();
    let position = store.record(&a.record_ids[0]).unwrap().producer.unwrap();
    assert_eq!(position.sequence, 5);
    assert_eq!(
        store
            .record(&a.record_ids[1])
            .unwrap()
            .producer
            .unwrap()
            .sequence,
        7
    );
    let state = store.producer_stream(&position.stream_id, 2).unwrap();
    assert_eq!(state.first_sequence, 5); // No invented missing prefix 1..4.
    assert_eq!(state.unobserved_sequences, 1);
    assert_eq!(state.gaps, vec![ProducerGap { first: 6, last: 6 }]);
    let fence = a.buffered.as_ref().unwrap();
    store.flush_through(fence).unwrap();
    let native = store.with_rdf_records(&a.record_ids, 2, Default::default(), |view| {
        purrdf::sparql::NativeSparqlEngine::new().query_governed_fallible_view(view,
            purrdf::SparqlRequest { query: "PREFIX sp: <urn:sparrow:vocab:> SELECT ?record ?stream ?sequence WHERE { ?record sp:stream ?stream ; sp:producerSequence ?sequence . ?stream a sp:Stream ; sp:streamKind \"producer\" ; sp:source ?source . ?source a sp:Source }", base_iri: None, substitutions: &[] },
            purrdf::sparql::QueryOptions::default(), &purrdf::sparql::QueryGovernors::METERED.with_fuel(1000000)
                .with_max_answers(100).with_max_intermediate_cells(10000).with_max_scratch_bytes(1024 * 1024).with_max_remote_requests(0))
            .map_err(Box::new)
    }).unwrap().unwrap();
    let (_, rows) = native.result.solutions().expect("native SELECT result");
    assert_eq!(rows.len(), 2);
    assert!(format!("{rows:?}").contains(&position.stream_id));
    let second = ProducerBatch {
        instance: [7; 16],
        sequences: vec![10],
    };
    let b = store
        .ingest_producer(
            "producer-b",
            "producer",
            b"{}\n",
            &second,
            3,
            Durability::Durable,
        )
        .unwrap();
    assert_eq!(
        store
            .record(&b.record_ids[0])
            .unwrap()
            .producer
            .unwrap()
            .stream_id,
        position.stream_id
    );
    let state = store.producer_stream(&position.stream_id, 4).unwrap();
    assert_eq!(state.accepted_events, 3);
    assert_eq!(state.unobserved_sequences, 3);
    assert_eq!(state.gaps.last(), Some(&ProducerGap { first: 8, last: 9 }));
    for (request, input, expected) in [
        ("producer-overlap", first.clone(), "CONFLICT"),
        (
            "producer-a",
            ProducerBatch {
                instance: [8; 16],
                sequences: vec![5, 7],
            },
            "CONFLICT",
        ),
        (
            "producer-count",
            ProducerBatch {
                instance: [8; 16],
                sequences: vec![1],
            },
            "INVALID_ARGUMENT",
        ),
    ] {
        assert_eq!(
            store
                .ingest_producer(
                    request,
                    "producer",
                    b"{}\n{}\n",
                    &input,
                    4,
                    Durability::Durable
                )
                .unwrap_err()
                .code,
            expected
        );
    }
    assert_eq!(
        store
            .producer_stream(&position.stream_id, 4)
            .unwrap()
            .last_sequence,
        10
    );
    assert_eq!(store.status(4).unwrap()["live_records"].as_str(), Some("3"));
    assert_eq!(
        store
            .ingest_producer(
                "producer-a",
                "producer",
                b"{}\n{}\n",
                &first,
                5,
                Durability::Durable
            )
            .unwrap()
            .id,
        a.id
    );
    let pin = store
        .pin(
            "producer-pin",
            "producer",
            &[a.record_ids[0].clone()],
            40,
            5,
        )
        .unwrap();
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .ingest_producer(
                "producer-a",
                "producer",
                b"{}\n{}\n",
                &first,
                6,
                Durability::Durable
            )
            .unwrap()
            .record_ids,
        a.record_ids
    );
    assert_eq!(
        store
            .producer_stream(&position.stream_id, 6)
            .unwrap()
            .accepted_events,
        3
    );
    let deadline = 30 * DAY + 4;
    assert_eq!(
        store
            .producer_stream(&position.stream_id, deadline)
            .unwrap_err()
            .code,
        "NOT_FOUND"
    );
    store.expire(deadline).unwrap();
    assert_eq!(
        store.record(&a.record_ids[0]).unwrap().producer,
        Some(position.clone())
    );
    let c = store
        .ingest_producer(
            "producer-new-window",
            "producer",
            b"{}\n{}\n",
            &first,
            deadline,
            Durability::Durable,
        )
        .unwrap();
    let new = store.record(&c.record_ids[0]).unwrap().producer.unwrap();
    assert_ne!(new.stream_id, position.stream_id);
    assert_eq!(
        store
            .producer_stream(&new.stream_id, deadline)
            .unwrap()
            .accepted_events,
        2
    );
    store
        .release_retry("producer-release", &pin.id, deadline)
        .unwrap();
}

#[test]
fn producer_registry_has_finite_windows_gap_suffix_and_hard_corruption_failures() {
    use sparrow_core::{Durability, MAX_PRODUCER_GAPS, MAX_PRODUCER_STREAMS, ProducerBatch};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let sequence = ProducerBatch {
        instance: [1; 16],
        sequences: (1..=40).map(|n| n * 2).collect(),
    };
    let bytes = b"{}\n".repeat(sequence.sequences.len());
    let first = store
        .ingest_producer(
            "producer-gaps",
            "producer",
            &bytes,
            &sequence,
            1,
            Durability::Durable,
        )
        .unwrap();
    let id = store
        .record(&first.record_ids[0])
        .unwrap()
        .producer
        .unwrap()
        .stream_id;
    let state = store.producer_stream(&id, 1).unwrap();
    assert_eq!(state.first_sequence, 2);
    assert_eq!(state.accepted_events, 40);
    assert_eq!(state.unobserved_sequences, 39);
    assert_eq!(state.gaps.len(), MAX_PRODUCER_GAPS);
    assert!(!state.gaps_complete);
    assert_eq!(state.gaps[0].first, 49);
    for index in 1..MAX_PRODUCER_STREAMS {
        store
            .ingest_producer(
                &format!("producer-{index}"),
                "producer",
                b"{}\n",
                &ProducerBatch {
                    instance: [index as u8 + 1; 16],
                    sequences: vec![1],
                },
                2,
                Durability::Durable,
            )
            .unwrap();
    }
    let overflow = ProducerBatch {
        instance: [255; 16],
        sequences: vec![1],
    };
    assert_eq!(
        store
            .ingest_producer(
                "producer-overflow",
                "producer",
                b"{}\n",
                &overflow,
                2,
                Durability::Durable
            )
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store.status(2).unwrap()["live_records"].as_str(),
        Some("103")
    );
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    db.put("producer-stream-registry", b"[{}]").unwrap();
    drop(db);
    assert!(
        matches!(Store::open(dir.path(), config()), Err(error) if error.code == "STORAGE_FAILURE")
    );
    let db = catalog_db(dir.path()).unwrap();
    db.delete("producer-stream-registry").unwrap();
    drop(db);
    assert!(
        matches!(Store::open(dir.path(), config()), Err(error) if error.code == "STORAGE_FAILURE")
    );
}

#[test]
fn producer_writer_upgrade_preserves_diagnostic_and_watch_authority_without_annotation() {
    use sparrow_core::DiagnosticDimension;
    for (legacy, current, watch) in [
        (13, 33, false),
        (14, 34, false),
        (15, 35, true),
        (16, 36, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), config()).unwrap();
        let profile = store
            .define_diagnostic_profile(
                "source",
                [(DiagnosticDimension::Service, "/service".to_owned())].into(),
            )
            .unwrap();
        let a = store
            .ingest(
                "migration-input",
                "source",
                "json",
                br#"{"service":"preserved"}"#,
                1,
            )
            .unwrap();
        if watch {
            store
                .watch_registry_replace(None, b"preserved-watch")
                .unwrap();
        }
        let identity = store.store_id().unwrap();
        drop(store);
        let db = catalog_db(dir.path()).unwrap();
        remove_tail_range_index(&db).unwrap();
        remove_artifact_rdf_upgrade(&db).unwrap();
        db.put("store_format", format!("sparrow.store/{legacy}"))
            .unwrap();
        db.delete("tail-generation-registry").unwrap();
        db.delete("producer-stream-registry").unwrap();
        drop(db);
        let store = Store::open(dir.path(), config()).unwrap();
        assert_eq!(store.store_id().unwrap(), identity);
        assert_eq!(
            store.status(2).unwrap()["store_format"].as_str(),
            Some(format!("sparrow.store/{}", current + 44).as_str())
        );
        let record = store.record(&a.record_ids[0]).unwrap();
        assert!(record.producer.is_none());
        assert_eq!(
            record.diagnostics.profile_id.as_deref(),
            Some(profile.id.as_str())
        );
        assert_eq!(
            store.bytes(&a.id, 0, a.byte_length as usize, 2).unwrap(),
            br#"{"service":"preserved"}"#
        );
        if watch {
            assert_eq!(
                store.watch_registry().unwrap().as_deref(),
                Some(b"preserved-watch".as_slice())
            );
        }
    }
}

#[test]
fn admission_properties_share_status_metadata_without_evidence_catalog_scans() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .ingest(
            "properties-input",
            "producer",
            "json",
            br#"{"message":"needle"}"#,
            1,
        )
        .unwrap();
    let properties = store.properties().unwrap();
    let status = store.status(2).unwrap();
    for key in [
        "store_id",
        "store_format",
        "rdf_profile",
        "text_analyzer",
        "text_profile",
        "retry_window_seconds",
        "disk_budget",
        "maintenance_reserve",
        "max_inline_artifact_bytes",
        "max_records_per_artifact",
        "max_pin_members",
    ] {
        assert_eq!(properties[key], status[key], "{key}");
    }
    for measured in [
        "live_records",
        "active_pins",
        "physical_bytes",
        "unique_payload_chunks",
        "unique_json_projections",
    ] {
        assert!(properties.get(measured).is_none());
    }
    assert!(purrdf_lex::json::write_compact(&properties).len() < 4096);
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    db.put("record:corrupt-properties-test", b"{invalid")
        .unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(store.properties().unwrap(), properties);
    assert_eq!(store.status(2).unwrap_err().code, "STORAGE_FAILURE");
}

#[test]
fn diagnostic_partial_scope_keeps_proven_rows_unknown_counts_and_strict_failures() {
    use sparrow_core::{
        DiagnosticDimension as D, JsonQuery, QueryAlternatives, QueryScope, QuerySelection,
        RecordQuery,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .define_diagnostic_profile(
            "producer",
            [(D::Service, "/svc".into()), (D::Host, "/host".into())].into(),
        )
        .unwrap();
    let artifact = store
        .ingest(
            "partial-mapped",
            "producer",
            "jsonl",
            concat!(
                "{\"svc\":\"worker\",\"host\":\"h1\",\"message\":\"needle\"}\n",
                "{\"svc\":\"worker\",\"host\":7,\"message\":\"needle\"}\n",
                "{\"host\":\"h1\",\"message\":\"needle\"}\n",
                "{\"svc\":\"other\",\"host\":7,\"message\":\"needle\"}\n",
                "{\"svc\":\"worker\",\"svc\":\"worker\",\"host\":\"h1\",\"message\":\"needle\"}\n"
            )
            .as_bytes(),
            1,
        )
        .unwrap();
    let legacy = store
        .ingest(
            "partial-unprofiled",
            "legacy",
            "json",
            br#"{"svc":"worker","host":"h1","message":"needle"}"#,
            2,
        )
        .unwrap();
    let mut scope = QueryScope {
        selection: QuerySelection::Live,
        source: None,
        sources: None,
        diagnostics: [
            (
                D::Service,
                QueryAlternatives::new(vec!["worker".into()]).unwrap(),
            ),
            (D::Host, QueryAlternatives::new(vec!["h1".into()]).unwrap()),
        ]
        .into(),
        require_complete_index: false,
        received_from: 0,
        received_before: 10,
        limit: 100,
        scan_budget: 1000,
    };
    for result in [
        store
            .json_query(&scope, &JsonQuery::Keys { path: None }, 3)
            .unwrap(),
        store
            .json_query(
                &scope,
                &JsonQuery::Values {
                    path: Some("/message".into()),
                    kind: None,
                    lexical: None,
                    decoded: None,
                },
                3,
            )
            .unwrap(),
        store.text_query(&scope, "needle", 3).unwrap(),
        store
            .records_query(&scope, &RecordQuery::default(), 3)
            .unwrap(),
        store
            .records_query(
                &scope,
                &RecordQuery {
                    text: Some("needle".into()),
                    ..Default::default()
                },
                3,
            )
            .unwrap(),
    ] {
        assert!(result["coverage"]["eligible_records"].is_null());
        assert_eq!(
            result["coverage"]["known_eligible_records"].as_str(),
            Some("1")
        );
        assert_eq!(
            result["coverage"]["diagnostic_scope"]["unknown"].as_str(),
            Some("4")
        );
        assert_eq!(
            result["coverage"]["diagnostic_scope"]["excluded"].as_str(),
            Some("1")
        );
        assert_eq!(result["certainty"].as_str(), Some("incomplete"));
        if result.get("ranking").is_some() {
            assert_eq!(
                result["ranking"]["membership_complete"].as_bool(),
                Some(false)
            );
        }
        for row in result["rows"].as_array().unwrap() {
            if let Some(id) = row.get("record_id") {
                assert_eq!(id.as_str(), Some(artifact.record_ids[0].as_str()));
            }
        }
    }
    let context = store
        .records_context(&artifact.record_ids[0], &scope, 10, 10, 3)
        .unwrap();
    assert_eq!(context.diagnostics.unknown, 3);
    assert_eq!(context.diagnostics.excluded, 1);
    assert!(!context.diagnostics.complete(false));
    assert!(context.before.records.is_empty() && context.after.records.is_empty());
    assert_eq!(
        store
            .records_context(&legacy.record_ids[0], &scope, 0, 0, 3)
            .unwrap_err()
            .code,
        "INDEX_NOT_READY"
    );
    scope.require_complete_index = true;
    assert_eq!(
        store
            .json_query(&scope, &JsonQuery::Keys { path: None }, 3)
            .unwrap_err()
            .code,
        "INDEX_NOT_READY"
    );
    assert_eq!(
        store.text_query(&scope, "needle", 3).unwrap_err().code,
        "INDEX_NOT_READY"
    );
    assert_eq!(
        store
            .records_query(&scope, &RecordQuery::default(), 3)
            .unwrap_err()
            .code,
        "INDEX_NOT_READY"
    );
    assert_eq!(
        store
            .records_context(&artifact.record_ids[0], &scope, 10, 10, 3)
            .unwrap_err()
            .code,
        "INDEX_NOT_READY"
    );
    scope.require_complete_index = false;
    assert_eq!(
        store
            .records_query(
                &scope,
                &RecordQuery {
                    require_complete_index: true,
                    ..Default::default()
                },
                3
            )
            .unwrap_err()
            .code,
        "INDEX_NOT_READY"
    );
    // A certainly excluded record does not contaminate scope with its unknown host.
    scope.selection = QuerySelection::Evidence(vec![artifact.record_ids[3].clone()]);
    scope.require_complete_index = true;
    let excluded = store
        .records_query(&scope, &RecordQuery::default(), 3)
        .unwrap();
    assert_eq!(excluded["coverage"]["eligible_records"].as_str(), Some("0"));
    assert_eq!(
        excluded["coverage"]["diagnostic_scope"]["unknown"].as_str(),
        Some("0")
    );
    scope.require_complete_index = false;
    let pin = store
        .pin(
            "partial-pin",
            "diagnostic-case",
            std::slice::from_ref(&artifact.id),
            40,
            3,
        )
        .unwrap();
    store.expire(31 * DAY).unwrap();
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    scope.selection = QuerySelection::Pin {
        id: pin.id,
        expected_revision: None,
    };
    let result = store
        .records_query(&scope, &RecordQuery::default(), 31 * DAY)
        .unwrap();
    assert_eq!(result["rows"].as_array().unwrap().len(), 1);
    assert_eq!(
        result["coverage"]["diagnostic_scope"]["unknown"].as_str(),
        Some("3")
    );
    assert!(result["coverage"]["eligible_records"].is_null());
    scope.diagnostics.clear();
    scope.limit = 1;
    let limited_rows = store
        .records_query(&scope, &RecordQuery::default(), 31 * DAY)
        .unwrap();
    assert_eq!(limited_rows["execution"].as_str(), Some("limited"));
    assert_eq!(
        limited_rows["coverage"]["eligible_records"].as_str(),
        Some("5")
    );
    assert_eq!(limited_rows["qualified_records"].as_str(), Some("5"));
    scope.scan_budget = 1;
    let limited_scan = store
        .records_query(&scope, &RecordQuery::default(), 31 * DAY)
        .unwrap();
    assert!(limited_scan["coverage"]["eligible_records"].is_null());
    assert!(limited_scan["qualified_records"].is_null());
    // Unknown membership never conceals a broken immutable profile binding.
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    let (key, _) = db
        .prefix_iterator("source-diagnostic:")
        .next()
        .unwrap()
        .unwrap();
    assert!(key.starts_with(b"source-diagnostic:"));
    db.delete(key).unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    scope.scan_budget = 1000;
    scope.limit = 100;
    assert_eq!(
        store
            .json_query(&scope, &JsonQuery::Keys { path: None }, 31 * DAY)
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
    assert_eq!(
        store
            .text_query(&scope, "needle", 31 * DAY)
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
    assert_eq!(
        store
            .records_query(&scope, &RecordQuery::default(), 31 * DAY)
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
    assert_eq!(
        store
            .records_context(&artifact.record_ids[0], &scope, 10, 10, 31 * DAY)
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
}

#[test]
fn diagnostic_scopes_preserve_native_evidence_and_refuse_uncertain_membership() {
    use sparrow_core::{
        DiagnosticDimension as D, DiagnosticValue as V, JsonQuery, QueryAlternatives, QueryScope,
        QuerySelection, RecordQuery,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let legacy = store
        .ingest("diag-before", "events", "json", br#"{"svc":"worker"}"#, 0)
        .unwrap();
    let mappings = [
        (D::Service, "/svc"),
        (D::Host, "/h"),
        (D::Severity, "/level"),
        (D::TraceId, "/trace"),
        (D::SpanId, "/span"),
    ]
    .into_iter()
    .map(|(d, p)| (d, p.to_owned()))
    .collect();
    let profile = store.define_diagnostic_profile("events", mappings).unwrap();
    assert_eq!(
        store
            .define_diagnostic_profile("events", profile.mappings.clone())
            .unwrap(),
        profile
    );
    assert_eq!(
        store
            .define_diagnostic_profile("events", [(D::Service, "/other".into())].into())
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert!(
        store
            .record(&legacy.record_ids[0])
            .unwrap()
            .diagnostics
            .fields
            .is_empty()
    );
    let bytes = b"{\"svc\":\"w\\u006frker\",\"h\":\"h1\",\"level\":\"ERROR\",\"trace\":\"t1\",\"span\":\"s1\",\"message\":\"needle\"}\r\n{\"svc\":\"worker\",\"h\":\"h1\",\"level\":\"WARN\",\"trace\":\"t2\",\"span\":\"s2\",\"message\":\"needle\"}\n";
    let artifact = store
        .ingest("diag-known", "events", "jsonl", bytes, 1)
        .unwrap();
    let record = store.record(&artifact.record_ids[0]).unwrap();
    assert_eq!(
        record.diagnostics.profile_id.as_deref(),
        Some(profile.id.as_str())
    );
    let V::Known {
        value,
        source_start,
        source_end,
        ..
    } = &record.diagnostics.fields[&D::Service]
    else {
        panic!("decoded service unavailable")
    };
    assert_eq!(value, "worker");
    assert_eq!(
        &bytes[*source_start as usize..*source_end as usize],
        br#"w\u006frker"#
    );
    let inspection = store.rdf_inspect(&record.id, 2).unwrap();
    assert_eq!(inspection["diagnostic_primary_rows"].as_str(), Some("40"));
    store
        .with_rdf_records(
            std::slice::from_ref(&record.id),
            2,
            sparrow_core::RdfReadLimits::default(),
            |view| {
                use purrdf::DatasetView;
                use purrdf::TermRef;
                use purrdf::dataset_view::TermGuard;
                assert!(view.quads().any(|q| {
                    let p = view.resolve(q.p).unwrap();
                    let o = view.resolve(q.o).unwrap();
                    matches!(p.term(), TermRef::Iri("urn:sparrow:vocab:service"))
                        && matches!(
                            o.term(),
                            TermRef::Literal {
                                lexical: "worker",
                                ..
                            }
                        )
                }));
            },
        )
        .unwrap();
    let mut scope = QueryScope {
        selection: QuerySelection::Live,
        source: Some("events".into()),
        sources: None,
        diagnostics: [
            (
                D::Service,
                QueryAlternatives::new(vec!["worker".into(), "worker".into()]).unwrap(),
            ),
            (
                D::Severity,
                QueryAlternatives::new(vec!["ERROR".into(), "WARN".into()]).unwrap(),
            ),
        ]
        .into(),
        require_complete_index: true,
        received_from: 1,
        received_before: 2,
        limit: 100,
        scan_budget: 1000,
    };
    assert_eq!(
        store
            .records_query(&scope, &RecordQuery::default(), 2)
            .unwrap()["rows"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        store.text_query(&scope, "needle", 2).unwrap()["rows"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        store
            .json_query(&scope, &JsonQuery::Keys { path: None }, 2)
            .unwrap()["coverage"]["eligible_records"]
            .as_str(),
        Some("2")
    );
    assert_eq!(
        store
            .records_context(artifact.record_ids.iter().min().unwrap(), &scope, 0, 5, 2)
            .unwrap()
            .after
            .records
            .len(),
        1
    );
    scope.diagnostics.insert(
        D::TraceId,
        QueryAlternatives::new(vec!["t1".into()]).unwrap(),
    );
    assert_eq!(
        store
            .records_query(&scope, &RecordQuery::default(), 2)
            .unwrap()["rows"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    scope.received_from = 0;
    assert_eq!(
        store
            .json_query(&scope, &JsonQuery::Keys { path: None }, 2)
            .unwrap_err()
            .code,
        "INDEX_NOT_READY"
    );
    scope.received_from = 3;
    scope.received_before = 4;
    let ambiguous = store
        .ingest(
            "diag-ambiguous",
            "events",
            "json",
            br#"{"svc":"worker","svc":"worker","h":null,"level":"ERROR","trace":"t1"}"#,
            3,
        )
        .unwrap();
    let evidence = store.record(&ambiguous.record_ids[0]).unwrap().diagnostics;
    assert_eq!(
        evidence.fields[&D::Service],
        V::Unknown("ambiguous_field".into())
    );
    assert_eq!(evidence.fields[&D::Host], V::Unknown("wrong_kind".into()));
    assert_eq!(
        evidence.fields[&D::SpanId],
        V::Unknown("missing_field".into())
    );
    assert_eq!(
        store.text_query(&scope, "needle", 4).unwrap_err().code,
        "INDEX_NOT_READY"
    );
    let pin = store
        .pin("diag-pin", "case", &artifact.record_ids[..1], 40, 4)
        .unwrap();
    store.expire(31 * DAY).unwrap();
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(store.diagnostic_profile("events").unwrap(), Some(profile));
    assert_eq!(
        store.rdf_inspect(&record.id, 31 * DAY).unwrap()["diagnostic_primary_rows"].as_str(),
        Some("40")
    );
    scope.received_from = 1;
    scope.received_before = 2;
    scope.selection = QuerySelection::Pin {
        id: pin.id.clone(),
        expected_revision: Some(pin.revision),
    };
    assert_eq!(
        store
            .records_query(&scope, &RecordQuery::default(), 31 * DAY)
            .unwrap()["rows"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    store.release(&pin.id).unwrap();
    store.expire(31 * DAY).unwrap();
    assert_eq!(
        store.rdf_inspect(&record.id, 31 * DAY).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    assert!(
        db.get(format!("rdf:graph:diagnostic:{}", record.id))
            .unwrap()
            .is_none()
    );
}

#[test]
fn staged_diagnostics_capture_absence_once_and_format_upgrade_preserves_fences() {
    use sparrow_core::DiagnosticDimension as D;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    // One preparation checkpoint holds at most 256 records, so the 257th is
    // prepared after the profile is defined and must use the captured absence.
    let bytes = &b"{\"svc\":\"worker\"}\n".repeat(257)[..];
    let payload = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(bytes).as_bytes())
    );
    let stage = store
        .import_begin(
            "diag-staged",
            "events",
            "jsonl",
            bytes.len() as u64,
            &payload,
            0,
        )
        .unwrap();
    store.import_append(&stage.id, 0, bytes, 0).unwrap();
    store.import_seal(&stage.id, 0).unwrap();
    store.import_prepare_layout(&stage.id, 0).unwrap();
    let checkpoint = store.import_prepare_record(&stage.id, 0, 1).unwrap();
    assert_eq!(checkpoint.diagnostic_profile, None);
    assert_eq!(checkpoint.prepared_records, 256);
    store
        .define_diagnostic_profile("events", [(D::Service, "/svc".into())].into())
        .unwrap();
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    store.import_prepare_record(&stage.id, 256, 2).unwrap();
    let artifact = store.import_publish(&stage.id, 2).unwrap();
    assert!(
        artifact.record_ids.iter().all(|id| store
            .record(id)
            .unwrap()
            .diagnostics
            .fields
            .is_empty())
    );
    let next = store
        .ingest("diag-next", "events", "json", br#"{"svc":"worker"}"#, 3)
        .unwrap();
    assert!(
        !store
            .record(&next.record_ids[0])
            .unwrap()
            .diagnostics
            .fields
            .is_empty()
    );
    for version in 9..=12 {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), config()).unwrap();
        let id = store.store_id().unwrap();
        drop(store);
        let db = catalog_db(dir.path()).unwrap();
        remove_tail_range_index(&db).unwrap();
        remove_artifact_rdf_upgrade(&db).unwrap();
        db.put("store_format", format!("sparrow.store/{version}"))
            .unwrap();
        db.delete("tail-generation-registry").unwrap();
        db.delete("producer-stream-registry").unwrap();
        if version >= 11 {
            db.put("watch_registry", b"preserved").unwrap();
        }
        drop(db);
        let store = Store::open(dir.path(), config()).unwrap();
        assert_eq!(store.store_id().unwrap(), id);
        assert_eq!(
            store.status(0).unwrap()["store_format"].as_str(),
            Some(format!("sparrow.store/{}", version + 68).as_str())
        );
        if version >= 11 {
            assert_eq!(
                store.watch_registry().unwrap().as_deref(),
                Some(b"preserved".as_slice())
            );
        }
    }
}

#[test]
fn source_alternatives_intersect_membership_time_and_context_without_duplicates() {
    use sparrow_core::{JsonQuery, QueryAlternatives, QueryScope, QuerySelection, RecordQuery};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let mut artifacts = Vec::new();
    for (index, source) in ["alpha", "alpha", "beta", "gamma"].into_iter().enumerate() {
        artifacts.push(
            store
                .ingest(
                    &format!("source-array-{index}"),
                    source,
                    "json",
                    format!(r#"{{"message":"needle","{source}":true}}"#).as_bytes(),
                    (index as u64 + 1) * 10,
                )
                .unwrap(),
        );
    }
    let pin = store
        .pin(
            "source-array-pin",
            "case",
            &artifacts.iter().map(|a| a.id.clone()).collect::<Vec<_>>(),
            40,
            40,
        )
        .unwrap();
    let mut scope = QueryScope {
        selection: QuerySelection::Live,
        source: None,
        sources: Some(
            QueryAlternatives::new(vec!["alpha".into(), "beta".into(), "alpha".into()]).unwrap(),
        ),
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 100,
        limit: 100,
        scan_budget: 1000,
    };
    for selection in [
        QuerySelection::Live,
        QuerySelection::Evidence(artifacts.iter().map(|a| a.id.clone()).collect()),
        QuerySelection::Pin {
            id: pin.id.clone(),
            expected_revision: Some(pin.revision),
        },
    ] {
        scope.selection = selection;
        let records = store
            .records_query(&scope, &RecordQuery::default(), 50)
            .unwrap();
        assert_eq!(records["rows"].as_array().unwrap().len(), 3);
        assert!(
            records["rows"]
                .as_array()
                .unwrap()
                .iter()
                .all(|row| row["source"].as_str() != Some("gamma"))
        );
        assert_eq!(
            store.text_query(&scope, "needle", 50).unwrap()["rows"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        let keys = store
            .json_query(&scope, &JsonQuery::Keys { path: None }, 50)
            .unwrap();
        assert_eq!(keys["coverage"]["eligible_records"].as_str(), Some("3"));
        assert!(
            keys["rows"]
                .as_array()
                .unwrap()
                .iter()
                .all(|row| row["path"].as_str() != Some("/gamma"))
        );
        assert_eq!(
            store
                .json_query(
                    &scope,
                    &JsonQuery::Values {
                        path: Some("/message".into()),
                        kind: None,
                        lexical: None,
                        decoded: None
                    },
                    50
                )
                .unwrap()["rows"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        let context = store
            .records_context(&artifacts[0].record_ids[0], &scope, 0, 10, 50)
            .unwrap();
        assert_eq!(context.after.records.len(), 1);
        assert_eq!(context.after.records[0].id, artifacts[1].record_ids[0]);
        assert_eq!(
            store
                .records_context(&artifacts[3].record_ids[0], &scope, 0, 0, 50)
                .unwrap_err()
                .code,
            "INVALID_ARGUMENT"
        );
    }
    scope.source = Some("beta".into());
    assert_eq!(
        store
            .records_query(&scope, &RecordQuery::default(), 50)
            .unwrap()["rows"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    scope.source = Some("gamma".into());
    assert!(
        store.text_query(&scope, "needle", 50).unwrap()["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    scope.source = None;
    scope.received_from = 20;
    scope.received_before = 30;
    let records = store
        .records_query(&scope, &RecordQuery::default(), 50)
        .unwrap();
    assert_eq!(records["rows"].as_array().unwrap().len(), 1);
    assert_eq!(
        records["rows"][0]["record_id"].as_str(),
        Some(artifacts[1].record_ids[0].as_str())
    );
    scope.received_from = 0;
    scope.received_before = 100;
    assert_eq!(
        store
            .records_query(&scope, &RecordQuery::default(), 31 * DAY)
            .unwrap()["rows"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    store.release(&pin.id).unwrap();
    scope.sources = Some(QueryAlternatives::new(vec!["absent".into()]).unwrap());
    assert_eq!(
        store
            .records_query(&scope, &RecordQuery::default(), 31 * DAY)
            .unwrap_err()
            .code,
        "NOT_FOUND"
    );
    scope.selection = QuerySelection::Evidence(vec![artifacts[3].id.clone()]);
    assert_eq!(
        store
            .json_query(&scope, &JsonQuery::Keys { path: None }, 31 * DAY)
            .unwrap_err()
            .code,
        "EVIDENCE_UNAVAILABLE"
    );
}

#[test]
fn source_filter_admission_uses_ingestion_byte_bounds() {
    use sparrow_core::QueryAlternatives;
    for ids in [
        vec![],
        vec![String::new()],
        vec!["x".repeat(513)],
        vec!["é".repeat(257)],
        vec!["source".into(); 129],
    ] {
        assert_eq!(
            QueryAlternatives::new(ids).unwrap_err().code,
            "INVALID_ARGUMENT"
        );
    }
    let filter = QueryAlternatives::new(vec!["é".repeat(256); 128]).unwrap();
    assert_eq!(filter.ids().len(), 128);
}

#[test]
fn streaming_status_counts_follow_receipt_retention_pin_release_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let old = store
        .ingest(
            "status-stream-old",
            "source",
            "log",
            b"same\nsame\nold\n",
            0,
        )
        .unwrap();
    store
        .ingest(
            "status-stream-later",
            "source",
            "log",
            b"same\nlater\n",
            DAY,
        )
        .unwrap();
    let pin = store
        .pin("status-stream-pin", "case", &old.record_ids[..1], 40, 1)
        .unwrap();
    assert_eq!(store.status(1).unwrap()["live_records"].as_str(), Some("5"));
    assert_eq!(
        store.status(30 * DAY).unwrap()["live_records"].as_str(),
        Some("3")
    );
    assert_eq!(
        store.status(30 * DAY).unwrap()["active_pins"].as_str(),
        Some("1")
    );
    store.expire(30 * DAY).unwrap();
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.status(30 * DAY).unwrap()["live_records"].as_str(),
        Some("3")
    );
    assert_eq!(store.records(30 * DAY).unwrap().len(), 3);
    assert_eq!(store.pins(30 * DAY).unwrap().len(), 1);
    assert_eq!(store.artifacts(30 * DAY).unwrap().len(), 1);
    store.release(&pin.id).unwrap();
    assert_eq!(
        store.status(30 * DAY).unwrap()["live_records"].as_str(),
        Some("2")
    );
    assert_eq!(
        store.status(30 * DAY).unwrap()["active_pins"].as_str(),
        Some("0")
    );
    assert_eq!(
        store.status(31 * DAY).unwrap()["live_records"].as_str(),
        Some("0")
    );
}

#[test]
fn streaming_catalog_refuses_identity_corruption_and_never_returns_partial_rows() {
    use purrdf_lex::json;
    for kind in ["record", "artifact", "pin"] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), config()).unwrap();
        let artifact = store
            .ingest("status-stream-catalog", "source", "log", b"kept\n", 0)
            .unwrap();
        let pin = store
            .pin(
                "status-stream-catalog-pin",
                "case",
                &artifact.record_ids,
                40,
                0,
            )
            .unwrap();
        let key = match kind {
            "record" => artifact.record_ids[0].clone(),
            "artifact" => artifact.id.clone(),
            _ => pin.id.clone(),
        };
        drop(store);
        let db = catalog_db(dir.path()).unwrap();
        let mut value =
            json::read_slice(&db.get(&key).unwrap().unwrap(), json::Limits::DEFAULT).unwrap();
        *value.get_mut("id").unwrap() = json::Value::from(format!("{kind}:different"));
        db.put(&key, json::write_compact(&value)).unwrap();
        drop(db);
        let store = Store::open(dir.path(), config()).unwrap();
        let error = match kind {
            "record" => store.records(1).unwrap_err(),
            "artifact" => store.artifacts(1).unwrap_err(),
            _ => store.pins(1).unwrap_err(),
        };
        assert_eq!(error.code, "STORAGE_FAILURE");
        if kind != "artifact" {
            assert_eq!(store.status(1).unwrap_err().code, "STORAGE_FAILURE");
        }
        if kind == "pin" {
            assert_eq!(
                store
                    .pin(
                        "status-stream-refused-pin",
                        "case",
                        &artifact.record_ids,
                        40,
                        1
                    )
                    .unwrap_err()
                    .code,
                "STORAGE_FAILURE"
            );
            assert_eq!(
                store.record(&artifact.record_ids[0]).unwrap().id,
                artifact.record_ids[0]
            );
        }
    }
}

#[test]
fn recordless_eviction_merges_receipt_order_preserves_pins_and_retries() {
    use sparrow_core::{EvictionFrontier, EvictionLimits};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let pinned = store
        .ingest("empty-protected", "source", "jsonl", b"", 1)
        .unwrap();
    let first = store
        .ingest("empty-first", "source", "log", b"", 2)
        .unwrap();
    let record = store
        .ingest("empty-between", "source", "log", b"record\n", 3)
        .unwrap();
    let last = store
        .ingest("empty-last", "source", "jsonl", b"", 4)
        .unwrap();
    let pin = store
        .pin("empty-pin", "case", std::slice::from_ref(&pinned.id), 40, 5)
        .unwrap();
    let limits = EvictionLimits {
        max_records: 1,
        scan_budget: 2,
        ..EvictionLimits::default()
    };
    let report = store.evict_oldest(6, limits).unwrap();
    assert_eq!(report.artifacts.as_slice(), std::slice::from_ref(&first.id));
    assert_eq!(report.recordless_artifacts, 1);
    assert_eq!(report.protected, 1);
    assert_eq!(report.frontier, EvictionFrontier::ScanBudget);
    assert!(report.records.is_empty());
    let report = store.evict_oldest(6, limits).unwrap();
    assert_eq!(report.records, record.record_ids);
    assert_eq!(report.recordless_artifacts, 0);
    assert_eq!(report.frontier, EvictionFrontier::UnitLimit);
    let report = store.evict_oldest(6, limits).unwrap();
    assert_eq!(report.artifacts.as_slice(), std::slice::from_ref(&last.id));
    assert_eq!(report.recordless_artifacts, 1);
    assert!(!report.continuation);
    assert_eq!(store.bytes(&pinned.id, 0, 0, 6).unwrap(), b"");
    assert_eq!(
        store.bytes(&first.id, 0, 0, 6).unwrap_err().code,
        "NOT_FOUND"
    );
    assert_eq!(
        store
            .ingest("empty-first", "source", "log", b"", 7)
            .unwrap()
            .id,
        first.id
    );
    assert_eq!(store.artifact(&first.id).unwrap_err().code, "NOT_FOUND");
    store.release(&pin.id).unwrap();
    assert_eq!(
        store.evict_oldest(8, limits).unwrap().artifacts,
        [pinned.id]
    );
}

#[test]
fn recordless_publication_resets_continuation_and_staged_import_uses_the_same_index() {
    use sparrow_core::{EvictionLimits, ImportStart};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let empty = store
        .ingest("empty-later", "source", "log", b"", 10)
        .unwrap();
    let record = store
        .ingest("empty-younger-record", "source", "log", b"younger\n", 20)
        .unwrap();
    let limits = EvictionLimits {
        max_records: 1,
        ..EvictionLimits::default()
    };
    assert_eq!(
        store.evict_oldest(21, limits).unwrap().artifacts.as_slice(),
        std::slice::from_ref(&empty.id)
    );
    let earlier = store
        .ingest("empty-earlier", "source", "log", b"", 5)
        .unwrap();
    assert_eq!(
        store.evict_oldest(21, limits).unwrap().artifacts,
        [earlier.id]
    );
    assert_eq!(
        store.evict_oldest(21, limits).unwrap().records,
        record.record_ids
    );
    assert!(store.evict_oldest(21, limits).unwrap().records.is_empty());
    let stage = match store
        .import_start("empty-staged", "source", "jsonl", 0, &empty.payload_id, 22)
        .unwrap()
    {
        ImportStart::Staging(stage) => stage,
        ImportStart::Accepted(_) => panic!("new empty import was already accepted"),
    };
    store.import_seal(&stage.id, 22).unwrap();
    store.import_prepare_layout(&stage.id, 22).unwrap();
    store.import_prepare_record(&stage.id, 0, 22).unwrap();
    let staged = store.import_publish(&stage.id, 22).unwrap();
    let report = store.evict_oldest(23, limits).unwrap();
    assert_eq!(
        report.artifacts.as_slice(),
        std::slice::from_ref(&staged.id)
    );
    assert_eq!(report.visited, 1);
    assert_eq!(report.recordless_artifacts, 1);
    assert_eq!(store.import_publish(&stage.id, 23).unwrap().id, staged.id);
    assert_eq!(store.artifact(&staged.id).unwrap_err().code, "NOT_FOUND");
}

#[test]
fn recordless_index_corruption_and_batch_refusal_publish_no_retirement_prefix() {
    use sparrow_core::EvictionLimits;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let first = store
        .ingest("empty-atomic", "source", "log", b"", 1)
        .unwrap();
    assert_eq!(
        store
            .evict_oldest(
                2,
                EvictionLimits {
                    max_batch_bytes: 1,
                    ..EvictionLimits::default()
                }
            )
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(store.artifact(&first.id).unwrap().id, first.id);
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    let key = "empty-time:00000000000000000002:artifact:missing";
    db.put(key, b"artifact:missing").unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .evict_oldest(3, EvictionLimits::default())
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
    assert_eq!(store.artifact(&first.id).unwrap().id, first.id);
}

#[test]
fn legacy_recordless_upgrade_preserves_each_existing_writer_fence_and_identity() {
    use sparrow_core::EvictionLimits;
    for (legacy, current, watch) in [(5, 33, false), (6, 34, false), (7, 35, true), (8, 36, true)] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), config()).unwrap();
        let identity = store.store_id().unwrap();
        let empty = store
            .ingest("empty-legacy", "source", "log", b"", 1)
            .unwrap();
        let populated = store
            .ingest("populated-legacy", "source", "log", b"preserved\n", 2)
            .unwrap();
        if watch {
            store
                .watch_registry_replace(None, b"preserved-catalog")
                .unwrap();
        }
        drop(store);
        let db = catalog_db(dir.path()).unwrap();
        let empty_catalog = db.get(&empty.id).unwrap().unwrap();
        drop(db);
        let store = Store::open(dir.path(), config()).unwrap();
        let retired = store
            .evict_oldest(
                3,
                EvictionLimits {
                    max_records: 1,
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(retired.artifacts, std::slice::from_ref(&empty.id));
        drop(store);
        let db = catalog_db(dir.path()).unwrap();
        db.put(&empty.id, empty_catalog).unwrap();
        db.delete("recordless-index-upgrade").unwrap();
        db.delete(format!("empty-time:{:020}:{}", empty.received_at, empty.id))
            .unwrap();
        remove_tail_range_index(&db).unwrap();
        remove_artifact_rdf_upgrade(&db).unwrap();
        db.put("store_format", format!("sparrow.store/{legacy}"))
            .unwrap();
        db.delete("tail-generation-registry").unwrap();
        db.delete("producer-stream-registry").unwrap();
        drop(db);
        let store = Store::open(dir.path(), config()).unwrap();
        assert_eq!(store.store_id().unwrap(), identity);
        assert_eq!(
            store.status(3).unwrap()["store_format"].as_str(),
            Some(format!("sparrow.store/{}", current + 44).as_str())
        );
        assert_eq!(
            store.bytes(&populated.id, 0, 10, 3).unwrap(),
            b"preserved\n"
        );
        let report = store
            .evict_oldest(
                3,
                EvictionLimits {
                    max_records: 1,
                    ..EvictionLimits::default()
                },
            )
            .unwrap();
        assert_eq!(report.artifacts, [empty.id]);
        assert!(report.records.is_empty());
        if watch {
            assert_eq!(
                store.watch_registry().unwrap().as_deref(),
                Some(b"preserved-catalog".as_slice())
            );
        }
    }
}

#[test]
fn bounded_expiry_recaptures_pin_release_and_expiry_between_pages() {
    use sparrow_core::EvictionLimits;
    for release in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), config()).unwrap();
        let artifact = store
            .ingest("expiry-protected-pages", "source", "log", b"a\nb\nc\n", 0)
            .unwrap();
        let pin = store
            .pin(
                "expiry-pages-pin",
                "case",
                std::slice::from_ref(&artifact.id),
                40,
                0,
            )
            .unwrap();
        let limits = EvictionLimits {
            scan_budget: 1,
            max_records: 1,
            ..EvictionLimits::default()
        };
        assert_eq!(store.expire_bounded(30 * DAY, limits).unwrap().records, 0);
        let now = if release {
            store.release(&pin.id).unwrap();
            30 * DAY
        } else {
            40 * DAY
        };
        assert_eq!(store.expire_bounded(now, limits).unwrap().records, 1);
        for _ in 0..2 {
            loop {
                if !store.expire_bounded(now, limits).unwrap().continuation {
                    break;
                }
            }
        }
        assert_eq!(
            store.status(now).unwrap()["unique_payload_chunks"].as_str(),
            Some("0")
        );
    }
}

#[test]
fn bounded_expiry_drains_manifests_and_preserves_pinned_shared_evidence() {
    use sparrow_core::{EvictionLimits, ExpiryFrontier};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let mut line = vec![b'r'; 1000];
    line.extend_from_slice(b"\r\n");
    let input = line.repeat(66);
    let old = store
        .ingest("expiry-old", "source", "log", &input, 0)
        .unwrap();
    let later = store
        .ingest("expiry-later", "source", "log", &input, DAY)
        .unwrap();
    let pin = store
        .pin("expiry-pin", "case", &old.record_ids[..1], 40, 0)
        .unwrap();
    let limits = EvictionLimits {
        max_records: 1,
        scan_budget: 2,
        max_payload_refs: 1,
        ..EvictionLimits::default()
    };
    let mut partial = false;
    let mut retired = 0;
    for pass in 0..10000 {
        let report = store.expire_bounded(30 * DAY, limits).unwrap();
        assert!(report.visited <= 2);
        assert!(report.payload_refs <= 1);
        assert!(report.records + report.artifacts <= 1);
        partial |= report.frontier == ExpiryFrontier::PayloadBudget;
        retired += report.records;
        if !report.continuation {
            break;
        }
        assert!(pass < 9999, "bounded expiry failed to finish its traversal");
    }
    assert!(partial);
    assert_eq!(retired, old.record_ids.len() as u64 - 1);
    assert_eq!(
        store
            .bytes(&old.record_ids[0], 0, line.len(), 30 * DAY)
            .unwrap(),
        line
    );
    assert_eq!(
        store.bytes(&later.id, 0, input.len(), 30 * DAY).unwrap(),
        input
    );
    assert_eq!(
        store.bytes(&old.id, 0, 1, 30 * DAY).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    store.release(&pin.id).unwrap();
    // A fresh traversal revisits members made eligible behind the prior cursor.
    loop {
        if !store.expire_bounded(30 * DAY, limits).unwrap().continuation {
            break;
        }
    }
    assert_eq!(
        store.records(30 * DAY).unwrap().len(),
        later.record_ids.len()
    );
    assert_eq!(
        store.bytes(&later.id, 0, input.len(), 30 * DAY).unwrap(),
        input
    );
}

#[test]
fn bounded_expiry_reclaims_recordless_catalog_and_restarts_after_reopen() {
    use sparrow_core::EvictionLimits;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let empty = store
        .ingest("expiry-empty", "source", "log", b"", 0)
        .unwrap();
    let old = store
        .ingest(
            "expiry-record",
            "source",
            "jsonl",
            b"{\"message\":\"old\"}\n",
            0,
        )
        .unwrap();
    let limits = EvictionLimits {
        scan_budget: 1,
        max_records: 1,
        ..EvictionLimits::default()
    };
    let first = store.expire_bounded(31 * DAY, limits).unwrap();
    assert_eq!(first.visited, 1);
    assert!(first.continuation);
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    let mut passes = 0;
    loop {
        let report = store.expire_bounded(31 * DAY, limits).unwrap();
        assert!(report.visited <= 1);
        passes += 1;
        assert!(passes < 20);
        if !report.continuation {
            break;
        }
    }
    assert_eq!(store.artifact(&empty.id).unwrap_err().code, "NOT_FOUND");
    assert_eq!(store.artifact(&old.id).unwrap_err().code, "NOT_FOUND");
    assert!(store.records(31 * DAY).unwrap().is_empty());
    assert_eq!(
        store.status(31 * DAY).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
    // New publication after a completed traversal is processed by the next traversal.
    let added = store
        .ingest("expiry-new", "source", "log", b"new\n", 0)
        .unwrap();
    loop {
        if !store.expire_bounded(31 * DAY, limits).unwrap().continuation {
            break;
        }
    }
    assert_eq!(store.artifact(&added.id).unwrap_err().code, "NOT_FOUND");
}

#[test]
fn bounded_expiry_batch_and_catalog_refusal_preserve_dependencies_and_position() {
    use sparrow_core::EvictionLimits;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let old = store
        .ingest(
            "expiry-refusal",
            "source",
            "jsonl",
            b"{\"message\":\"keep\"}\n",
            0,
        )
        .unwrap();
    for limits in [
        EvictionLimits {
            max_batch_bytes: 1,
            ..EvictionLimits::default()
        },
        EvictionLimits {
            max_catalog_bytes: 1,
            ..EvictionLimits::default()
        },
    ] {
        assert_eq!(
            store.expire_bounded(31 * DAY, limits).unwrap_err().code,
            "RESOURCE_LIMIT"
        );
        assert_eq!(store.artifact(&old.id).unwrap().id, old.id);
        assert_eq!(
            store.status(0).unwrap()["unique_payload_chunks"].as_str(),
            Some("1")
        );
        assert_eq!(store.bytes(&old.id, 0, 1, 0).unwrap(), b"{");
    }
    let report = store
        .expire_bounded(31 * DAY, EvictionLimits::default())
        .unwrap();
    assert_eq!(report.artifacts, 1);
    assert_eq!(report.records, 1);
    assert!(!report.continuation);
    assert_eq!(
        store.status(31 * DAY).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
}

#[test]
fn manifest_retirement_survives_reopen_without_rewriting_original_catalog_or_receipt() {
    use purrdf_lex::json::record::ToJson;
    use sparrow_core::EvictionLimits;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = b"needle\n".repeat(130);
    let original = store
        .ingest("manifest-resume", "source", "log", &input, 0)
        .unwrap();
    let limits = EvictionLimits {
        max_payload_refs: 4,
        ..EvictionLimits::default()
    };
    let first = store.evict_oldest(1, limits).unwrap();
    assert_eq!(first.records.len(), 1);
    assert_eq!(first.payload_refs, 4);
    assert_eq!(first.pending_manifest_refs, 127);
    assert_eq!(
        store.artifact(&original.id).unwrap().to_json()["chunks"]
            .as_array()
            .unwrap()
            .len(),
        127
    );
    assert_eq!(
        store
            .ingest("manifest-resume", "source", "log", &input, 1)
            .unwrap()
            .to_json(),
        original.to_json()
    );
    assert_eq!(
        store.bytes(&original.id, 0, 1, 1).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    drop(store);
    let db = sparrow_test_support::catalog_db(dir.path()).unwrap();
    let root = db.get(&original.id).unwrap().unwrap();
    assert_eq!(
        db.get("store_format").unwrap().as_deref(),
        Some(b"sparrow.store/86".as_slice())
    );
    let position = db
        .get(format!("artifact-retirement:{}", original.id))
        .unwrap()
        .unwrap();
    assert_eq!(position.len(), 17);
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    let refused = store
        .evict_oldest(
            2,
            EvictionLimits {
                max_batch_bytes: 1,
                ..limits
            },
        )
        .unwrap_err();
    assert_eq!(refused.code, "RESOURCE_LIMIT");
    assert_eq!(
        store.artifact(&original.id).unwrap().to_json()["chunks"]
            .as_array()
            .unwrap()
            .len(),
        127
    );
    let next = store.evict_oldest(2, limits).unwrap();
    assert_eq!(next.pending_manifest_refs, 123);
    assert_eq!(next.payload_refs, 4);
    drop(store);
    let db = sparrow_test_support::catalog_db(dir.path()).unwrap();
    assert_eq!(db.get(&original.id).unwrap().unwrap(), root);
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    for _ in 0..200 {
        if !store.expire_bounded(30 * DAY, limits).unwrap().continuation {
            break;
        }
    }
    assert!(
        store.artifact(&original.id).unwrap().to_json()["chunks"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.status(30 * DAY).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
}

#[test]
fn manifest_retirement_fences_later_writers_and_rejects_corrupt_state() {
    use sparrow_core::{EvictionLimits, ReceiverDiscard, SyslogReceiver};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest("manifest-fence", "source", "log", b"one\ntwo\n", 0)
        .unwrap();
    store
        .evict_oldest(
            1,
            EvictionLimits {
                max_payload_refs: 1,
                ..EvictionLimits::default()
            },
        )
        .unwrap();
    store.watch_registry_replace(None, b"watch-state").unwrap();
    let receiver = SyslogReceiver::new("udp", "udp").unwrap();
    store
        .discard_syslog("receiver", &receiver, ReceiverDiscard::Truncated, 2)
        .unwrap();
    store
        .ingest("later-write", "source", "log", b"later", 3)
        .unwrap();
    assert_eq!(
        store.status(3).unwrap()["store_format"].as_str(),
        Some("sparrow.store/87")
    );
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    let key = format!("artifact-retirement:{}", artifact.id);
    let intact = db.get(&key).unwrap().unwrap();
    let registry = db.get("syslog-receiver-registry").unwrap().unwrap();
    assert_eq!(
        db.get("manifest-retirement-features").unwrap().as_deref(),
        Some([1].as_slice())
    );
    drop(db);
    // Width, count, offset and flag corruption are never accepted as empty ownership.
    for invalid in [
        vec![0; 16],
        {
            let mut bytes = intact.clone();
            bytes[..8].copy_from_slice(&3_u64.to_be_bytes());
            bytes
        },
        {
            let mut bytes = intact.clone();
            bytes[8..16].copy_from_slice(&3_u64.to_be_bytes());
            bytes
        },
        {
            let mut bytes = intact.clone();
            bytes[16] = 2;
            bytes
        },
    ] {
        let db = catalog_db(dir.path()).unwrap();
        db.put(&key, invalid).unwrap();
        drop(db);
        let store = Store::open(dir.path(), config()).unwrap();
        assert_eq!(
            store.artifact(&artifact.id).unwrap_err().code,
            "STORAGE_FAILURE"
        );
        assert_eq!(
            store
                .evict_oldest(4, EvictionLimits::default())
                .unwrap_err()
                .code,
            "STORAGE_FAILURE"
        );
        drop(store);
    }
    let db = catalog_db(dir.path()).unwrap();
    db.put(&key, &intact).unwrap();
    db.delete("syslog-receiver-registry").unwrap();
    drop(db);
    assert_eq!(
        Store::open(dir.path(), config()).err().unwrap().code,
        "STORAGE_FAILURE"
    );
    let db = catalog_db(dir.path()).unwrap();
    db.put("syslog-receiver-registry", registry).unwrap();
    db.put("manifest-retirement-features", [2]).unwrap();
    drop(db);
    assert_eq!(
        Store::open(dir.path(), config()).err().unwrap().code,
        "STORAGE_FAILURE"
    );
}

#[test]
fn bounded_expiry_traverses_large_accepted_manifest_and_preserves_record_pin() {
    use purrdf_lex::json::record::ToJson;
    use sparrow_core::EvictionLimits;
    let dir = tempfile::tempdir().unwrap();
    let mut settings = config();
    settings.max_records = 100000;
    settings.disk_budget = 2 * 1024 * 1024 * 1024;
    let store = Store::open(dir.path(), settings).unwrap();
    let input = b"needle\n".repeat(30000);
    let manifest = stage_fixture(&store, "expiry-large", "log", &input);
    for ordinal in 0..manifest.layout_records {
        store
            .import_prepare_record(&manifest.id, ordinal, 0)
            .unwrap();
    }
    let artifact = store.import_publish(&manifest.id, 0).unwrap();
    assert_eq!(artifact.record_ids.len(), 30000);
    let limits = EvictionLimits::default();
    assert!(
        purrdf_lex::json::write_compact(&artifact.to_json()).len() * 64 > limits.max_catalog_bytes
    );
    let pin = store
        .pin("expiry-large-pin", "case", &artifact.record_ids[..1], 40, 0)
        .unwrap();
    // A live manifest must not strand the cursor before the record phase.
    let mut exhausted = false;
    for _ in 0..1000 {
        let report = store.expire_bounded(1, limits).unwrap();
        assert_eq!(report.records + report.artifacts, 0);
        if !report.continuation {
            exhausted = true;
            break;
        }
    }
    assert!(exhausted);
    assert_eq!(store.bytes(&artifact.id, 0, input.len(), 1).unwrap(), input);
    let eviction = store
        .evict_oldest(
            1,
            EvictionLimits {
                max_records: 1,
                ..limits
            },
        )
        .unwrap();
    assert_eq!(eviction.records.len(), 1);
    assert_ne!(eviction.records[0], artifact.record_ids[0]);
    assert!(eviction.payload_refs <= limits.max_payload_refs);
    let mut retired = eviction.records.len() as u64;
    exhausted = false;
    for _ in 0..1000 {
        let report = store.expire_bounded(30 * DAY, limits).unwrap();
        assert!(report.records + report.artifacts <= limits.max_records as u64);
        assert!(report.payload_refs <= limits.max_payload_refs);
        retired += report.records;
        if !report.continuation {
            exhausted = true;
            break;
        }
    }
    assert!(exhausted);
    assert_eq!(retired, 29999);
    assert_eq!(
        store
            .bytes(&artifact.record_ids[0], 0, 7, 30 * DAY)
            .unwrap(),
        b"needle\n"
    );
    assert_eq!(
        store.bytes(&artifact.id, 0, 1, 30 * DAY).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    store.release(&pin.id).unwrap();
    exhausted = false;
    for _ in 0..1000 {
        if !store.expire_bounded(30 * DAY, limits).unwrap().continuation {
            exhausted = true;
            break;
        }
    }
    assert!(exhausted);
    assert_eq!(
        store.status(30 * DAY).unwrap()["live_records"].as_str(),
        Some("0")
    );
    assert_eq!(
        store.status(30 * DAY).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
}

#[test]
fn pressure_continues_past_a_protected_prefix_without_restarting_after_renewal() {
    use sparrow_core::{EvictionFrontier, EvictionLimits, PressurePhase};
    let dir = tempfile::tempdir().unwrap();
    let mut settings = config();
    settings.disk_budget = 16 * 1024 * 1024;
    settings.maintenance_reserve = 4 * 1024 * 1024;
    let store = Store::open(dir.path(), settings).unwrap();
    let input = b"pinned\n".repeat(12);
    let old = store
        .ingest("prefix-old", "source", "log", &input, 1)
        .unwrap();
    let candidate = store
        .ingest("prefix-candidate", "source", "log", b"eligible\n", 2)
        .unwrap();
    let pin = store
        .pin("prefix-pin", "case", std::slice::from_ref(&old.id), 1, 3)
        .unwrap();
    std::fs::write(
        dir.path().join("test-owned-prefix-ballast"),
        vec![0_u8; 13 * 1024 * 1024],
    )
    .unwrap();
    assert_eq!(
        store
            .ingest("prefix-refused", "source", "log", b"later\n", 4)
            .unwrap_err()
            .code,
        "STORAGE_PRESSURE"
    );
    let limits = EvictionLimits {
        scan_budget: 2,
        max_records: 1,
        ..EvictionLimits::default()
    };
    for page in 0..6 {
        let state = store.pressure_maintain(5, limits).unwrap();
        assert_eq!(state.phase, PressurePhase::Requested);
        let report = state.last_pass.unwrap();
        assert_eq!(report.frontier, EvictionFrontier::ScanBudget);
        assert!(report.continuation);
        assert_eq!(report.protected, 2);
        assert!(report.records.is_empty());
        if page == 0 {
            store
                .renew_pin("prefix-renew", &pin.id, 40, None, 5)
                .unwrap();
        }
    }
    let state = store.pressure_maintain(6, limits).unwrap();
    assert_eq!(state.phase, PressurePhase::AwaitingReclamation);
    assert_eq!(state.evicted_records, 1);
    assert_eq!(state.last_pass.unwrap().records, candidate.record_ids);
    assert_eq!(store.bytes(&old.id, 0, input.len(), 6).unwrap(), input);
}

#[test]
fn eviction_restarts_when_pin_release_or_expiry_exposes_an_older_record() {
    use sparrow_core::EvictionLimits;
    for release in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), config()).unwrap();
        let old = store
            .ingest("changed-old", "source", "log", b"old\n", 1)
            .unwrap();
        let middle = store
            .ingest("changed-middle", "source", "log", b"middle\n", 2)
            .unwrap();
        let later = store
            .ingest("changed-later", "source", "log", b"later\n", 3)
            .unwrap();
        let pin = store
            .pin("changed-pin", "case", &[old.id.clone(), middle.id], 1, 4)
            .unwrap();
        let limits = EvictionLimits {
            scan_budget: 1,
            max_records: 1,
            ..EvictionLimits::default()
        };
        let first = store.evict_oldest(5, limits).unwrap();
        assert!(first.records.is_empty());
        assert!(first.continuation);
        let now = if release {
            store.release(&pin.id).unwrap();
            6
        } else {
            DAY + 4
        };
        let resumed = store.evict_oldest(now, limits).unwrap();
        assert_eq!(resumed.records, old.record_ids);
        assert_eq!(store.bytes(&later.id, 0, 6, now).unwrap(), b"later\n");
    }
}

#[test]
fn earlier_publication_restarts_eviction_and_exhaustion_accepts_new_candidates() {
    use sparrow_core::EvictionLimits;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let old = store
        .ingest("insert-old", "source", "log", b"old\n", 1)
        .unwrap();
    let middle = store
        .ingest("insert-middle", "source", "log", b"middle\n", 3)
        .unwrap();
    let later = store
        .ingest("insert-later", "source", "log", b"later\n", 4)
        .unwrap();
    store
        .pin("insert-pin", "case", &[old.id.clone(), middle.id], 40, 5)
        .unwrap();
    let limits = EvictionLimits {
        scan_budget: 2,
        max_records: 1,
        ..EvictionLimits::default()
    };
    assert_eq!(store.evict_oldest(6, limits).unwrap().protected, 2);
    // Server receipt clocks may move backwards; a new earlier key cannot be skipped.
    let inserted = store
        .ingest("insert-earlier", "source", "log", b"inserted\n", 2)
        .unwrap();
    assert_eq!(
        store.evict_oldest(6, limits).unwrap().records,
        inserted.record_ids
    );
    assert_eq!(store.bytes(&later.id, 0, 6, 6).unwrap(), b"later\n");
    assert_eq!(
        store
            .evict_oldest(6, EvictionLimits::default())
            .unwrap()
            .records,
        later.record_ids
    );
    assert_eq!(store.evict_oldest(6, limits).unwrap().visited, 0);
    let appended = store
        .ingest("insert-after-exhaustion", "source", "log", b"appended\n", 7)
        .unwrap();
    let last = store
        .evict_oldest(
            8,
            EvictionLimits {
                scan_budget: 1,
                ..limits
            },
        )
        .unwrap();
    assert_eq!(last.records, appended.record_ids);
    assert_eq!(last.visited, 1);
    assert_eq!(store.bytes(&old.id, 0, 4, 8).unwrap(), b"old\n");
}

#[test]
fn catalog_frontiers_resume_a_complete_prefix_without_skipping_admission_failures() {
    use sparrow_core::{EvictionFrontier, EvictionLimits};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let old = store
        .ingest(
            "catalog-prefix",
            "source",
            "log",
            &b"pinned\n".repeat(20),
            1,
        )
        .unwrap();
    let later = store
        .ingest("catalog-later", "source", "log", b"later\n", 2)
        .unwrap();
    store
        .pin("catalog-pin", "case", std::slice::from_ref(&old.id), 40, 3)
        .unwrap();
    let limits = EvictionLimits {
        max_catalog_bytes: 128 * 1024,
        ..EvictionLimits::default()
    };
    let first = store.evict_oldest(4, limits).unwrap();
    assert_eq!(first.frontier, EvictionFrontier::CatalogBudget);
    assert!(first.continuation);
    assert!(first.records.is_empty());
    let mut found = false;
    for _ in 0..20 {
        let report = store.evict_oldest(4, limits).unwrap();
        if report.records == later.record_ids {
            found = true;
            break;
        }
    }
    assert!(
        found,
        "bounded catalog traversal did not reach the eligible occurrence"
    );
    assert_eq!(store.bytes(&old.id, 0, 7, 4).unwrap(), b"pinned\n");
}

#[test]
fn pressure_recovers_real_native_bytes_and_retries_without_ballast_removal() {
    use sparrow_core::{EvictionLimits, PressurePhase};
    use std::io::Read;
    let dir = tempfile::tempdir().unwrap();
    let mut settings = config();
    settings.disk_budget = 96 * 1024 * 1024;
    settings.maintenance_reserve = 32 * 1024 * 1024;
    let store = Store::open(dir.path(), settings.clone()).unwrap();
    let mut random = std::fs::File::open("/dev/urandom").unwrap();
    let mut input = vec![0; 1024 * 1024];
    random.read_exact(&mut input).unwrap();
    let protected = store
        .ingest("native-pin", "source", "opaque", &input, 1)
        .unwrap();
    let protected_bytes = input.clone();
    let pin = store
        .pin(
            "native-pin-request",
            "native-case",
            std::slice::from_ref(&protected.id),
            40,
            1,
        )
        .unwrap();
    // A second occurrence shares the protected payload but is independently evictable.
    store
        .ingest("native-shared", "source", "opaque", &input, 2)
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(45);
    let mut qualified = None;
    for index in 0..128 {
        random.read_exact(&mut input).unwrap();
        let request = format!("native-fill-{index}");
        match store.ingest(&request, "source", "opaque", &input, 3 + index) {
            Ok(_) => store.flush().unwrap(),
            Err(error) => {
                assert_eq!(error.code, "STORAGE_PRESSURE", "{error}");
                let before = store.status(3 + index).unwrap()["physical_bytes"]
                    .as_str()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                let recovered = loop {
                    let state = store
                        .pressure_maintain(3 + index, EvictionLimits::default())
                        .unwrap();
                    if state.phase == PressurePhase::Ready {
                        break state;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "native recovery stalled: {state:?}; before={before}; status={:?}",
                        store.status(3 + index).unwrap()
                    );
                    std::thread::sleep(std::time::Duration::from_millis(20));
                };
                let after = store.status(3 + index).unwrap()["physical_bytes"]
                    .as_str()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                if recovered.evicted_records > 0 {
                    qualified = Some((request, before, after, recovered));
                    break;
                }
                // An in-flight flush/compaction can resolve transient pressure
                // without deleting anything. Retry this exact body, then keep
                // filling until the real retirement/reclamation path is needed.
                store
                    .ingest(&request, "source", "opaque", &input, 3 + index)
                    .unwrap();
                store.flush().unwrap();
            }
        }
    }
    let (request, before, after, recovered) =
        qualified.expect("fixture did not reach physical recovery after real retirement");
    assert!(
        after < before,
        "no measured native disk recovery: {before} -> {after}"
    );
    assert!(recovered.evicted_records > 0);
    let accepted = store
        .ingest(&request, "source", "opaque", &input, 201)
        .unwrap();
    assert_eq!(
        store
            .bytes(&protected.id, 0, protected_bytes.len(), 201)
            .unwrap(),
        protected_bytes
    );
    assert_eq!(
        store.bytes(&accepted.id, 0, input.len(), 201).unwrap(),
        input
    );
    eprintln!(
        "PASS native physical recovery: before={before}, after={after}, evicted_records={}, retry={}",
        recovered.evicted_records, accepted.id
    );
    drop(store);
    let store = Store::open(dir.path(), settings).unwrap();
    assert_eq!(store.pressure_status().unwrap().phase, PressurePhase::Ready);
    assert_eq!(
        store
            .bytes(&protected.id, 0, protected_bytes.len(), 202)
            .unwrap(),
        protected_bytes
    );
    assert_eq!(
        store.bytes(&accepted.id, 0, input.len(), 202).unwrap(),
        input
    );
    store.release(&pin.id).unwrap();
}

#[test]
fn pressure_admission_drives_bounded_retirement_and_waits_for_physical_capacity() {
    use sparrow_core::{EvictionLimits, PressurePhase};
    let dir = tempfile::tempdir().unwrap();
    let mut settings = config();
    settings.disk_budget = 16 * 1024 * 1024;
    settings.maintenance_reserve = 4 * 1024 * 1024;
    let store = Store::open(dir.path(), settings.clone()).unwrap();
    let old = store
        .ingest("pressure-old", "source", "log", b"old\n", 1)
        .unwrap();
    let protected = store
        .ingest("pressure-pinned", "source", "log", b"pinned\n", 2)
        .unwrap();
    let pin = store
        .pin(
            "pressure-pin",
            "case",
            std::slice::from_ref(&protected.id),
            40,
            3,
        )
        .unwrap();
    assert_eq!(store.pressure_status().unwrap().phase, PressurePhase::Ready);
    let ballast = dir.path().join("test-owned-pressure-ballast");
    std::fs::write(&ballast, vec![0_u8; 13 * 1024 * 1024]).unwrap();
    assert_eq!(
        store
            .ingest("pressure-refused", "source", "log", b"later\n", 4)
            .unwrap_err()
            .code,
        "STORAGE_PRESSURE"
    );
    let requested = store.pressure_status().unwrap();
    assert_eq!(requested.phase, PressurePhase::Requested);
    assert!(requested.requested_bytes > 0);
    let limits = EvictionLimits {
        max_records: 1,
        ..EvictionLimits::default()
    };
    let state = store.pressure_maintain(5, limits).unwrap();
    assert_eq!(state.phase, PressurePhase::AwaitingReclamation);
    assert_eq!(state.evicted_records, 1);
    assert_eq!(state.last_pass.unwrap().records, old.record_ids);
    assert_eq!(store.bytes(&protected.id, 0, 7, 5).unwrap(), b"pinned\n");
    for _ in 0..3 {
        store.pressure_maintain(6, limits).unwrap();
        assert_eq!(store.bytes(&protected.id, 0, 7, 6).unwrap(), b"pinned\n");
    }
    assert_eq!(
        store
            .ingest("pressure-refused", "source", "log", b"later\n", 6)
            .unwrap_err()
            .code,
        "STORAGE_PRESSURE"
    );
    // Status and durable control writes remain usable above the data allowance.
    assert_eq!(store.status(6).unwrap()["active_pins"].as_str(), Some("1"));
    store.release(&pin.id).unwrap();
    std::fs::remove_file(ballast).unwrap();
    let recovered = store.pressure_maintain(7, limits).unwrap();
    assert_eq!(recovered.phase, PressurePhase::Ready);
    assert_eq!(recovered.requested_bytes, 0);
    assert_eq!(
        store
            .ingest("pressure-refused", "source", "log", b"later\n", 7)
            .unwrap()
            .record_ids
            .len(),
        1
    );
    assert_eq!(
        store.bytes(&old.id, 0, 1, 7).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
}

#[test]
fn pressure_retirement_wait_survives_reopen_without_retiring_more_evidence() {
    use sparrow_core::{EvictionLimits, PressurePhase};
    let dir = tempfile::tempdir().unwrap();
    let mut settings = config();
    settings.disk_budget = 16 * 1024 * 1024;
    settings.maintenance_reserve = 4 * 1024 * 1024;
    let store = Store::open(dir.path(), settings.clone()).unwrap();
    let first = store
        .ingest("wait-first", "source", "log", b"first\n", 1)
        .unwrap();
    let second = store
        .ingest("wait-second", "source", "log", b"second\n", 2)
        .unwrap();
    let ballast = dir.path().join("test-owned-pressure-ballast");
    let file = std::fs::File::create(&ballast).unwrap();
    file.set_len(13 * 1024 * 1024).unwrap();
    assert_eq!(
        store
            .ingest("wait-refused", "source", "log", b"new\n", 3)
            .unwrap_err()
            .code,
        "STORAGE_PRESSURE"
    );
    let limits = EvictionLimits {
        max_records: 1,
        ..EvictionLimits::default()
    };
    let state = store.pressure_maintain(4, limits).unwrap();
    assert_eq!(state.last_pass.unwrap().records, first.record_ids);
    assert_eq!(state.phase, PressurePhase::AwaitingReclamation);
    // Force a strictly larger linked footprint: this is no native recovery observation.
    assert_eq!(
        store.status(4).unwrap()["store_format"].as_str(),
        Some("sparrow.store/86")
    );
    file.set_len(15 * 1024 * 1024).unwrap();
    drop(file);
    drop(store);
    let store = Store::open(dir.path(), settings).unwrap();
    assert_eq!(
        store.pressure_status().unwrap().phase,
        PressurePhase::AwaitingReclamation
    );
    for _ in 0..3 {
        let state = store.pressure_maintain(5, limits).unwrap();
        assert_eq!(state.phase, PressurePhase::AwaitingReclamation);
        assert_eq!(state.passes, 0);
        assert_eq!(store.bytes(&second.id, 0, 7, 5).unwrap(), b"second\n");
    }
    assert_eq!(
        store.bytes(&first.id, 0, 1, 5).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    std::fs::remove_file(ballast).unwrap();
    assert_eq!(
        store.pressure_maintain(6, limits).unwrap().phase,
        PressurePhase::Ready
    );
    assert_eq!(
        store
            .ingest("wait-refused", "source", "log", b"new\n", 6)
            .unwrap()
            .record_ids
            .len(),
        1
    );
    drop(store);
    let db = sparrow_test_support::catalog_db(dir.path()).unwrap();
    assert!(db.get("pressure-reclamation-wait").unwrap().is_none());
    assert_eq!(
        db.get("store_format").unwrap().as_deref(),
        Some(b"sparrow.store/86".as_slice())
    );
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    store.watch_registry_replace(None, b"registered").unwrap();
    assert_eq!(
        store.status(6).unwrap()["store_format"].as_str(),
        Some("sparrow.store/87")
    );
    store
        .watch_registry_replace(Some(b"registered"), b"updated")
        .unwrap();
    assert_eq!(
        store.status(6).unwrap()["store_format"].as_str(),
        Some("sparrow.store/87")
    );
}

#[test]
fn invalid_pressure_recovery_markers_refuse_open() {
    for bytes in [
        b"{".to_vec(),
        vec![b'x'; 513],
        b"{\"physical_before\":0,\"requested_bytes\":1}".to_vec(),
    ] {
        let dir = tempfile::tempdir().unwrap();
        drop(Store::open(dir.path(), config()).unwrap());
        let db = sparrow_test_support::catalog_db(dir.path()).unwrap();
        db.put("pressure-reclamation-wait", bytes).unwrap();
        db.put("store_format", b"sparrow.store/82").unwrap();
        drop(db);
        match Store::open(dir.path(), config()) {
            Ok(_) => panic!("invalid pressure marker admitted"),
            Err(error) => assert_eq!(error.code, "STORAGE_FAILURE"),
        }
    }
}

#[test]
fn anonymous_query_capacity_does_not_authorize_pressure_retirement() {
    use sparrow_core::{EvictionLimits, PressurePhase, RecordOperation, RecordQuery, RecordRead};
    let dir = tempfile::tempdir().unwrap();
    let mut settings = config();
    settings.disk_budget = 16 * 1024 * 1024;
    settings.maintenance_reserve = 4 * 1024 * 1024;
    let store = Store::open(dir.path(), settings).unwrap();
    let artifact = store
        .ingest("anonymous-pressure", "source", "log", b"first\nsecond\n", 1)
        .unwrap();
    let footprint = |now| {
        store.status(now).unwrap()["physical_bytes"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    let linked = footprint(2);
    let page = store
        .records_read(
            &RecordRead {
                operation: RecordOperation::Records,
                scope: record_page_scope("source", 1),
                query: RecordQuery::default(),
                cursor: None,
            },
            2,
        )
        .unwrap();
    assert!(page.next_cursor.is_some());
    let charge = footprint(2) - linked;
    assert!(charge >= 65536);
    let ballast = dir.path().join("test-owned-anonymous-pressure-ballast");
    let file = std::fs::File::create(&ballast).unwrap();
    file.set_len(13 * 1024 * 1024).unwrap();
    assert_eq!(
        store
            .ingest("anonymous-refused", "source", "log", b"later\n", 3)
            .unwrap_err()
            .code,
        "STORAGE_PRESSURE"
    );
    let requested = store.pressure_status().unwrap().requested_bytes;
    let limit = 12 * 1024 * 1024;
    file.set_len(limit - requested - linked - charge / 2)
        .unwrap();
    let physical = footprint(4);
    assert!(physical + requested > limit);
    assert!(physical - charge + requested <= limit);
    let state = store
        .pressure_maintain(4, EvictionLimits::default())
        .unwrap();
    assert_eq!(state.passes, 0);
    assert_eq!(
        store.bytes(&artifact.id, 0, 13, 4).unwrap(),
        b"first\nsecond\n"
    );
    assert!(
        store
            .result_release(&page.store_id, &page.snapshot_id, 4)
            .unwrap()
    );
    assert_eq!(
        store
            .pressure_maintain(5, EvictionLimits::default())
            .unwrap()
            .phase,
        PressurePhase::Ready
    );
    assert_eq!(
        store.bytes(&artifact.id, 0, 13, 5).unwrap(),
        b"first\nsecond\n"
    );
    assert_eq!(
        store
            .ingest("anonymous-refused", "source", "log", b"later\n", 5)
            .unwrap()
            .record_ids
            .len(),
        1
    );
}

#[test]
fn staged_import_pressure_and_impossible_writes_have_distinct_admission_outcomes() {
    use sparrow_core::PressurePhase;
    let dir = tempfile::tempdir().unwrap();
    let mut settings = config();
    settings.disk_budget = 2 * 1024 * 1024;
    settings.maintenance_reserve = 1024 * 1024;
    let store = Store::open(dir.path(), settings).unwrap();
    assert_eq!(
        store
            .ingest(
                "impossible-write",
                "source",
                "log",
                &vec![b'x'; 2 * 1024 * 1024],
                1
            )
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(store.pressure_status().unwrap().phase, PressurePhase::Ready);
    let ballast = dir.path().join("test-owned-import-ballast");
    std::fs::write(&ballast, vec![0_u8; 1024 * 1024]).unwrap();
    let input = b"import\n";
    let payload_id = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(input).as_bytes())
    );
    assert_eq!(
        store
            .import_start(
                "pressure-import",
                "source",
                "log",
                input.len() as u64,
                &payload_id,
                2
            )
            .unwrap_err()
            .code,
        "STORAGE_PRESSURE"
    );
    assert_eq!(
        store.pressure_status().unwrap().phase,
        PressurePhase::Requested
    );
    std::fs::remove_file(ballast).unwrap();
    assert_eq!(
        store
            .pressure_maintain(3, sparrow_core::EvictionLimits::default())
            .unwrap()
            .phase,
        PressurePhase::Ready
    );
}

#[test]
fn blocked_pressure_rechecks_released_pins_without_breaking_existing_protection() {
    use sparrow_core::{EvictionLimits, PressurePhase};
    let dir = tempfile::tempdir().unwrap();
    let mut settings = config();
    settings.disk_budget = 16 * 1024 * 1024;
    settings.maintenance_reserve = 4 * 1024 * 1024;
    let store = Store::open(dir.path(), settings).unwrap();
    let artifact = store
        .ingest("blocked-old", "source", "log", b"old\n", 1)
        .unwrap();
    let pin = store
        .pin(
            "blocked-pin",
            "case",
            std::slice::from_ref(&artifact.id),
            40,
            2,
        )
        .unwrap();
    std::fs::write(
        dir.path().join("test-owned-blocked-ballast"),
        vec![0_u8; 13 * 1024 * 1024],
    )
    .unwrap();
    assert_eq!(
        store
            .ingest("blocked-new", "source", "log", b"new\n", 3)
            .unwrap_err()
            .code,
        "STORAGE_PRESSURE"
    );
    let invalid = EvictionLimits {
        max_records: 0,
        ..EvictionLimits::default()
    };
    assert_eq!(
        store.pressure_maintain(4, invalid).unwrap_err().code,
        "INVALID_ARGUMENT"
    );
    assert!(store.pressure_status().unwrap().failure.is_some());
    let blocked = store
        .pressure_maintain(5, EvictionLimits::default())
        .unwrap();
    assert_eq!(blocked.phase, PressurePhase::Blocked);
    assert!(blocked.failure.is_none());
    assert_eq!(blocked.evicted_records, 0);
    assert_eq!(blocked.last_pass.unwrap().protected, 1);
    assert_eq!(store.bytes(&artifact.id, 0, 4, 5).unwrap(), b"old\n");
    store.release(&pin.id).unwrap();
    let resumed = store
        .pressure_maintain(6, EvictionLimits::default())
        .unwrap();
    assert_eq!(resumed.phase, PressurePhase::AwaitingReclamation);
    assert_eq!(resumed.evicted_records, 1);
    assert_eq!(
        store.bytes(&artifact.id, 0, 1, 6).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
}

#[test]
fn eviction_preserves_record_pins_shared_projections_and_durable_retry_identity() {
    use sparrow_core::{EvictionLimits, JsonQuery, QueryScope, QuerySelection};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = b"{\"message\":\"shared needle\"}\n{\"message\":\"pinned needle\"}\n";
    let old = store
        .ingest("eviction-old", "source", "jsonl", input, 1)
        .unwrap();
    let new = store
        .ingest("eviction-new", "source", "jsonl", input, 2)
        .unwrap();
    let pin = store
        .pin(
            "eviction-pin",
            "investigation",
            &[old.record_ids[1].clone()],
            40,
            3,
        )
        .unwrap();
    let mut rdf_ids = vec![old.record_ids[1].clone()];
    rdf_ids.extend(new.record_ids.iter().cloned());
    let rdf_before = store
        .with_rdf_records(
            &rdf_ids,
            3,
            sparrow_core::RdfReadLimits::default(),
            |view| purrdf::DatasetView::quads(view).count(),
        )
        .unwrap();
    assert!(rdf_before > 0);
    let report = store
        .evict_oldest(
            4,
            EvictionLimits {
                max_records: 1,
                ..EvictionLimits::default()
            },
        )
        .unwrap();
    assert_eq!(report.records, vec![old.record_ids[0].clone()]);
    assert_eq!(report.artifacts, vec![old.id.clone()]);
    assert_eq!(
        store.bytes(&old.id, 0, 1, 4).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    assert_eq!(
        store.bytes(&old.record_ids[0], 0, 1, 4).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    assert_eq!(
        store.bytes(&old.record_ids[1], 0, 28, 4).unwrap(),
        b"{\"message\":\"pinned needle\"}\n"
    );
    assert_eq!(store.bytes(&new.id, 0, input.len(), 4).unwrap(), input);
    assert_eq!(
        store
            .with_rdf_records(
                &rdf_ids,
                4,
                sparrow_core::RdfReadLimits::default(),
                |view| purrdf::DatasetView::quads(view).count()
            )
            .unwrap(),
        rdf_before
    );
    let scope = QueryScope {
        selection: QuerySelection::Evidence(vec![old.record_ids[1].clone(), new.id.clone()]),
        source: None,
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: u64::MAX,
        limit: 100,
        scan_budget: 1000,
    };
    assert_eq!(
        store.text_query(&scope, "needle", 4).unwrap()["rows"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        store
            .json_query(&scope, &JsonQuery::Keys { path: None }, 4)
            .unwrap()["coverage"]["eligible_records"]
            .as_str(),
        Some("3")
    );
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .ingest("eviction-old", "source", "jsonl", input, 5)
            .unwrap()
            .id,
        old.id
    );
    assert_eq!(
        store.bytes(&old.id, 0, 1, 5).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    store.release(&pin.id).unwrap();
    let report = store
        .evict_oldest(
            6,
            EvictionLimits {
                max_records: 1,
                ..EvictionLimits::default()
            },
        )
        .unwrap();
    assert_eq!(report.records, vec![old.record_ids[1].clone()]);
    assert_eq!(store.bytes(&new.id, 0, input.len(), 6).unwrap(), input);
}

#[test]
fn eviction_manifest_cleanup_is_bounded_resumable_and_never_returns_partial_files() {
    use sparrow_core::{EvictionFrontier, EvictionLimits};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = b"one\ntwo\n";
    let artifact = store
        .ingest("eviction-manifest", "source", "log", input, 1)
        .unwrap();
    let limits = EvictionLimits {
        max_records: 1,
        max_payload_refs: 1,
        ..EvictionLimits::default()
    };
    let first = store.evict_oldest(2, limits).unwrap();
    assert_eq!(first.records.len(), 1);
    assert_eq!(first.payload_refs, 1);
    assert!(first.pending_manifest_refs > 0);
    assert_eq!(first.frontier, EvictionFrontier::UnitLimit);
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    let cleanup = store.evict_oldest(3, limits).unwrap();
    assert!(cleanup.records.is_empty());
    assert_eq!(cleanup.payload_refs, 1);
    assert_eq!(cleanup.pending_manifest_refs, 1);
    assert_eq!(cleanup.frontier, EvictionFrontier::PayloadBudget);
    assert_eq!(
        store
            .bytes(&artifact.id, 0, input.len(), 3)
            .unwrap_err()
            .code,
        "EVIDENCE_UNAVAILABLE"
    );
    let cleanup = store.evict_oldest(4, limits).unwrap();
    assert!(cleanup.records.is_empty());
    assert_eq!(cleanup.pending_manifest_refs, 0);
    let last = store.evict_oldest(5, limits).unwrap();
    assert_eq!(last.records.len(), 1);
    store.expire(31 * DAY).unwrap();
    assert_eq!(
        store.status(31 * DAY).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
}

#[test]
fn eviction_parent_pins_and_admission_refusals_are_atomic() {
    use sparrow_core::{EvictionFrontier, EvictionLimits};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let old = store
        .ingest("eviction-protected", "source", "log", b"old\n", 1)
        .unwrap();
    let new = store
        .ingest("eviction-eligible", "source", "log", b"new\n", 2)
        .unwrap();
    store
        .pin(
            "eviction-whole-pin",
            "whole",
            std::slice::from_ref(&old.id),
            1,
            3,
        )
        .unwrap();
    for limits in [
        EvictionLimits {
            max_catalog_bytes: 1,
            ..EvictionLimits::default()
        },
        EvictionLimits {
            max_batch_bytes: 1,
            ..EvictionLimits::default()
        },
    ] {
        assert_eq!(
            store.evict_oldest(4, limits).unwrap_err().code,
            "RESOURCE_LIMIT"
        );
        assert_eq!(store.bytes(&new.id, 0, 4, 4).unwrap(), b"new\n");
    }
    let scanned = store
        .evict_oldest(
            4,
            EvictionLimits {
                scan_budget: 1,
                ..EvictionLimits::default()
            },
        )
        .unwrap();
    assert_eq!(scanned.frontier, EvictionFrontier::ScanBudget);
    assert!(scanned.records.is_empty());
    assert_eq!(scanned.protected, 1);
    let report = store.evict_oldest(4, EvictionLimits::default()).unwrap();
    assert_eq!(report.records, new.record_ids);
    assert_eq!(store.bytes(&old.id, 0, 4, 4).unwrap(), b"old\n");
    let expired_pin = store
        .evict_oldest(2 * DAY, EvictionLimits::default())
        .unwrap();
    assert_eq!(expired_pin.records, old.record_ids);
    assert_eq!(
        store
            .evict_oldest(
                2 * DAY,
                EvictionLimits {
                    max_records: 0,
                    ..EvictionLimits::default()
                }
            )
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
}

#[test]
fn eviction_retires_released_record_after_ordinary_parent_catalog_cleanup() {
    use sparrow_core::EvictionLimits;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest("eviction-released", "source", "log", b"retained\n", 0)
        .unwrap();
    let pin = store
        .pin("eviction-record-only", "case", &artifact.record_ids, 40, 0)
        .unwrap();
    store.expire(32 * DAY).unwrap();
    assert_eq!(store.artifact(&artifact.id).unwrap_err().code, "NOT_FOUND");
    assert_eq!(
        store
            .bytes(&artifact.record_ids[0], 0, 9, 32 * DAY)
            .unwrap(),
        b"retained\n"
    );
    store.release(&pin.id).unwrap();
    let report = store
        .evict_oldest(32 * DAY, EvictionLimits::default())
        .unwrap();
    assert_eq!(report.records, artifact.record_ids);
    assert!(report.artifacts.is_empty());
    assert_eq!(
        store.status(32 * DAY).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
}
const CONSUMPTION_DIRECTORIES: sparrow_core::ConsumptionDirectories =
    sparrow_core::ConsumptionDirectories {
        root_device: 1,
        root_inode: 100,
        parent_device: 1,
        parent_inode: 101,
    };

#[test]
fn durable_watch_registry_compare_and_set_fences_writers_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let identity = store.store_id().unwrap();
    assert!(store.watch_registry().unwrap().is_none());
    store.watch_registry_replace(None, b"catalog-one").unwrap();
    assert_eq!(
        store
            .watch_registry_replace(None, b"stale-replacement")
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert_eq!(
        store.watch_registry().unwrap().as_deref(),
        Some(b"catalog-one".as_slice())
    );
    assert_eq!(
        store
            .watch_registry_replace(Some(b"catalog-one"), b"")
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store
            .watch_registry_replace(
                Some(b"catalog-one"),
                &vec![0; sparrow_core::MAX_WATCH_REGISTRY_BYTES + 1]
            )
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    let journal = store
        .consumption_reserve(
            "watch",
            b"input.log",
            "source",
            "log",
            sparrow_core::ConsumptionGeneration {
                device: 1,
                inode: 2,
                byte_length: 4,
                modified: (10, 20),
                changed: (10, 30),
            },
            CONSUMPTION_DIRECTORIES,
        )
        .unwrap();
    assert_eq!(
        store.status(0).unwrap()["store_format"].as_str(),
        Some("sparrow.store/79")
    );
    store
        .watch_registry_replace(Some(b"catalog-one"), b"catalog-two")
        .unwrap();
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(store.store_id().unwrap(), identity);
    assert_eq!(
        store.watch_registry().unwrap().as_deref(),
        Some(b"catalog-two".as_slice())
    );
    assert_eq!(store.consumption_get(&journal.import_id).unwrap(), journal);
    assert_eq!(
        store.status(0).unwrap()["store_format"].as_str(),
        Some("sparrow.store/79")
    );
}

#[test]
fn physical_accounting_completes_while_files_churn() {
    // INVARIANT: files removed during the footprint walk never fail accounting;
    // RocksDB deletes obsolete WAL and SST files continuously under ingest.
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let running = stop.clone();
    let file = dir.path().join("accounting-churn");
    let worker = std::thread::spawn(move || {
        while !running.load(Ordering::Relaxed) {
            std::fs::write(&file, b"owned accounting sidecar").unwrap();
            std::fs::remove_file(&file).unwrap();
        }
    });
    let observations = (0..100).map(|_| store.status(0)).collect::<Vec<_>>();
    stop.store(true, Ordering::Relaxed);
    worker.join().unwrap();
    for observation in observations {
        let status = observation.unwrap();
        assert!(
            status["physical_bytes"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
                > 0
        );
    }
}

#[test]
fn consumption_recovery_preserves_original_publication_beyond_retry_and_retention() {
    use sparrow_core::{ConsumptionGeneration, ImportStart, IngestDeletion, IngestSelector};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = b"one\n";
    let identity = store.store_id().unwrap();
    assert_eq!(
        store.status(0).unwrap()["store_format"].as_str(),
        Some("sparrow.store/77")
    );
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(input).as_bytes())
    );
    let observed = ConsumptionGeneration {
        device: 1,
        inode: 2,
        byte_length: input.len() as u64,
        modified: (10, 20),
        changed: (10, 30),
    };
    let reserved = store
        .consumption_reserve(
            "watch",
            b"nested/\xff.log",
            "source",
            "log",
            observed.clone(),
            CONSUMPTION_DIRECTORIES,
        )
        .unwrap();
    assert_eq!(
        store.status(0).unwrap()["store_format"].as_str(),
        Some("sparrow.store/78")
    );
    assert_eq!(
        store
            .consumption_reserve(
                "watch",
                b"nested/\xff.log",
                "source",
                "log",
                observed.clone(),
                CONSUMPTION_DIRECTORIES
            )
            .unwrap(),
        reserved
    );
    store
        .watch_registry_replace(None, b"registered-watch")
        .unwrap();
    let handoff = sparrow_core::ConsumptionHandoff {
        request_id: "finished-file-1".into(),
        tail_generation_id: None,
        watch: reserved.watch.clone(),
        watch_revision: 1,
        relative_path: reserved.relative_path.clone(),
        observed: observed.clone(),
    };
    assert_eq!(store.consumption_handoff_get(&handoff, 0).unwrap(), None);
    assert_eq!(
        store
            .consumption_handoff_reserve(&handoff, "source", "log", CONSUMPTION_DIRECTORIES, 0)
            .unwrap(),
        reserved
    );
    assert_eq!(
        store.status(0).unwrap()["store_format"].as_str(),
        Some("sparrow.store/80")
    );
    // Catalog updates must never downgrade the handoff writer fence.
    let selector = IngestSelector::Handoff(handoff.request_id.clone());
    let progress = store
        .ingest_status(&selector, Default::default(), 0)
        .unwrap();
    assert_eq!(progress.deletion, IngestDeletion::Reserved);
    assert_eq!(progress.consumption.as_ref(), Some(&reserved));
    assert!(progress.accepted.is_none());
    assert_eq!(progress.total_records, None);
    assert_eq!(progress.request_id, reserved.request_id);
    store
        .watch_registry_replace(Some(b"registered-watch"), b"changed-watch")
        .unwrap();
    assert_eq!(
        store.status(0).unwrap()["store_format"].as_str(),
        Some("sparrow.store/80")
    );
    assert_eq!(
        store
            .import_start(&reserved.request_id, "source", "log", 4, &digest, 0)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert_eq!(
        store
            .ingest(&reserved.request_id, "source", "log", input, 0)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    let mut claimed = observed.clone();
    claimed.changed = (11, 0); // Rename may change ctime, but cannot change content identity.
    store
        .consumption_claimed(&reserved.import_id, claimed.clone())
        .unwrap();
    let manifest = store
        .import_begin(&reserved.request_id, "source", "log", 4, &digest, 0)
        .unwrap();
    let progress = store
        .ingest_status(&selector, Default::default(), 0)
        .unwrap();
    assert_eq!(progress.deletion, IngestDeletion::Claimed);
    assert_eq!(
        progress.consumption.as_ref().unwrap().claimed.as_ref(),
        Some(&claimed)
    );
    assert!(progress.accepted.is_none());
    assert!(manifest.consumption);
    store.import_append(&manifest.id, 0, input, 0).unwrap();
    assert_eq!(
        store.import_cancel(&manifest.id, 1).unwrap_err().code,
        "CONFLICT"
    );
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(store.store_id().unwrap(), identity);
    assert_eq!(store.consumption_pending().unwrap().len(), 1);
    store.expire(3 * DAY).unwrap();
    assert_eq!(
        store
            .import_get(&manifest.id, 3 * DAY)
            .unwrap()
            .received_bytes,
        4
    );
    store.import_seal(&manifest.id, 3 * DAY).unwrap();
    store.import_prepare_layout(&manifest.id, 3 * DAY).unwrap();
    store
        .import_prepare_record(&manifest.id, 0, 3 * DAY)
        .unwrap();
    assert_eq!(
        store
            .consumption_removed(&manifest.id, "artifact:fake", &claimed, 3 * DAY)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    let artifact = store.import_publish(&manifest.id, 3 * DAY).unwrap();
    let progress = store
        .ingest_status(&selector, Default::default(), 3 * DAY)
        .unwrap();
    assert_eq!(progress.deletion, IngestDeletion::Published);
    assert_eq!(progress.accepted.as_ref().unwrap().id, artifact.id);
    assert_eq!(progress.total_records, Some(1));
    assert_eq!(progress.text.complete, 1);
    assert_eq!(progress.json.not_applicable, 1);
    assert_eq!(progress.rdf.not_applicable, 1);
    assert_eq!(progress.removed_at, None);
    assert_eq!(store.bytes(&artifact.id, 0, 4, 3 * DAY).unwrap(), input);
    assert_eq!(
        store
            .consumption_get(&manifest.id)
            .unwrap()
            .published_artifact
            .as_deref(),
        Some(artifact.id.as_str())
    );
    drop(store); // Model a lost publication reply and daemon restart.
    let store = Store::open(dir.path(), config()).unwrap();
    for _ in 0..4 {
        store.expire(40 * DAY).unwrap();
    }
    let recovered_handoff = store
        .consumption_handoff_get(&handoff, 40 * DAY)
        .unwrap()
        .unwrap();
    let progress = store
        .ingest_status(&selector, Default::default(), 40 * DAY)
        .unwrap();
    assert_eq!(progress.deletion, IngestDeletion::Published);
    assert_eq!(progress.accepted.as_ref().unwrap().id, artifact.id);
    assert_eq!(
        progress.accepted.as_ref().unwrap().received_at,
        artifact.received_at
    );
    assert_eq!(progress.text.unavailable, 1);
    assert_eq!(progress.text.complete, 0);
    assert_eq!(recovered_handoff.import_id, manifest.id);
    assert_eq!(
        recovered_handoff.published_artifact.as_deref(),
        Some(artifact.id.as_str())
    );
    assert_eq!(
        store
            .consumption_handoff_reserve(
                &handoff,
                "source",
                "log",
                CONSUMPTION_DIRECTORIES,
                40 * DAY
            )
            .unwrap(),
        recovered_handoff
    );
    assert_eq!(
        store.status(40 * DAY).unwrap()["live_records"].as_str(),
        Some("0")
    );
    let recovered = match store
        .import_start(&reserved.request_id, "source", "log", 4, &digest, 40 * DAY)
        .unwrap()
    {
        ImportStart::Accepted(recovered) => recovered,
        ImportStart::Staging(_) => {
            panic!("published claim must never allocate another staging occurrence")
        }
    };
    for receipt in [
        store.import_publish(&manifest.id, 40 * DAY).unwrap(),
        recovered,
    ] {
        assert_eq!(receipt.id, artifact.id);
        assert_eq!(receipt.payload_id, artifact.payload_id);
        assert_eq!(receipt.byte_length, artifact.byte_length);
        assert_eq!(receipt.record_ids, artifact.record_ids);
        assert_eq!(receipt.received_at, artifact.received_at);
        assert_eq!(receipt.expires_at, artifact.expires_at);
        assert_eq!(receipt.source, artifact.source);
        assert_eq!(receipt.format, artifact.format);
        assert!(receipt.buffered.is_none());
    }
    assert!(store.bytes(&artifact.id, 0, 4, 40 * DAY).is_err()); // Journal is no implicit pin.
    let mut replacement = claimed.clone();
    replacement.inode += 1;
    assert_eq!(
        store
            .consumption_removed(&manifest.id, &artifact.id, &replacement, 40 * DAY)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    let removed = store
        .consumption_removed(&manifest.id, &artifact.id, &claimed, 40 * DAY)
        .unwrap();
    for selected in [&selector, &IngestSelector::Import(manifest.id.clone())] {
        let progress = store
            .ingest_status(selected, Default::default(), 40 * DAY)
            .unwrap();
        assert_eq!(progress.deletion, IngestDeletion::Removed);
        assert_eq!(progress.removed_at, Some(40 * DAY));
        assert_eq!(progress.consumption.as_ref(), Some(&removed));
        assert_eq!(
            progress.accepted.as_ref().unwrap().expires_at,
            artifact.expires_at
        );
        assert_eq!(progress.text.unavailable, 1);
    }
    assert!(store.consumption_pending().unwrap().is_empty());
    store.expire(41 * DAY).unwrap();
    assert_eq!(
        store.consumption_handoff_get(&handoff, 41 * DAY).unwrap(),
        Some(removed.clone())
    );
    assert_eq!(
        store
            .consumption_removed(&manifest.id, &artifact.id, &claimed, 41 * DAY)
            .unwrap(),
        removed
    );
    assert_eq!(
        store
            .consumption_handoff_get(&handoff, 43 * DAY)
            .unwrap_err()
            .code,
        "NOT_FOUND"
    );
    let mut expired_alias = handoff.clone();
    assert_eq!(
        store
            .ingest_status(&selector, Default::default(), 43 * DAY)
            .unwrap_err()
            .code,
        "NOT_FOUND"
    );
    expired_alias.request_id = "expired-completion-alias".into();
    assert_eq!(
        store
            .consumption_handoff_reserve(
                &expired_alias,
                "source",
                "log",
                CONSUMPTION_DIRECTORIES,
                43 * DAY
            )
            .unwrap_err()
            .code,
        "NOT_FOUND"
    );
    store.expire(43 * DAY).unwrap();
    store.expire(43 * DAY).unwrap();
    assert_eq!(
        store.consumption_get(&manifest.id).unwrap_err().code,
        "NOT_FOUND"
    );
    assert_eq!(
        store
            .import_start(&reserved.request_id, "source", "log", 4, &digest, 43 * DAY)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert!(store.records(43 * DAY).unwrap().is_empty());
    assert_eq!(
        store.consumption_handoff_get(&handoff, 43 * DAY).unwrap(),
        None
    );
}

#[test]
fn explicit_handoff_admission_is_atomic_bounded_and_conflicting_retries_never_rebind() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let mut intent = sparrow_core::ConsumptionHandoff {
        request_id: "handoff-0".into(),
        tail_generation_id: None,
        watch: "watch".into(),
        watch_revision: 1,
        relative_path: b"nested/\xff.log".to_vec(),
        observed: sparrow_core::ConsumptionGeneration {
            device: 1,
            inode: 2,
            byte_length: 4,
            modified: (10, 20),
            changed: (10, 30),
        },
    };
    assert_eq!(
        store
            .consumption_handoff_reserve(&intent, "source", "log", CONSUMPTION_DIRECTORIES, 0)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert!(store.consumption_pending().unwrap().is_empty());
    assert!(store.consumption_handoff_get(&intent, 0).unwrap().is_none());
    store.watch_registry_replace(None, b"registered").unwrap();
    assert_eq!(
        store
            .consumption_handoff_reserve(
                &intent,
                "source",
                "log",
                CONSUMPTION_DIRECTORIES,
                u64::MAX
            )
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    assert!(store.consumption_pending().unwrap().is_empty());
    assert_eq!(
        store.status(0).unwrap()["store_format"].as_str(),
        Some("sparrow.store/79")
    );
    let reserved = store
        .consumption_handoff_reserve(&intent, "source", "log", CONSUMPTION_DIRECTORIES, 0)
        .unwrap();
    for change in 0..5 {
        let mut changed = intent.clone();
        match change {
            0 => changed.watch_revision += 1,
            1 => changed.watch = "replacement-watch".into(),
            2 => changed.relative_path = b"other.log".to_vec(),
            3 => changed.observed.inode += 1,
            _ => changed.observed.changed.0 += 1,
        }
        assert_eq!(
            store.consumption_handoff_get(&changed, 0).unwrap_err().code,
            "CONFLICT"
        );
        assert_eq!(
            store
                .consumption_handoff_reserve(&changed, "source", "log", CONSUMPTION_DIRECTORIES, 0)
                .unwrap_err()
                .code,
            "CONFLICT"
        );
    }
    for index in 1..128 {
        intent.request_id = format!("handoff-{index}");
        assert_eq!(
            store
                .consumption_handoff_reserve(&intent, "source", "log", CONSUMPTION_DIRECTORIES, 0)
                .unwrap(),
            reserved
        );
    }
    intent.request_id = "over-capacity".into();
    intent.observed.inode += 1;
    assert_eq!(
        store
            .consumption_handoff_reserve(&intent, "source", "log", CONSUMPTION_DIRECTORIES, 0)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert!(store.consumption_handoff_get(&intent, 0).unwrap().is_none());
    assert_eq!(store.consumption_pending().unwrap(), vec![reserved.clone()]);
    store.expire(3 * DAY).unwrap();
    intent.request_id = "handoff-0".into();
    intent.observed.inode -= 1;
    assert_eq!(
        store.consumption_handoff_get(&intent, 3 * DAY).unwrap(),
        Some(reserved.clone())
    );
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.consumption_handoff_get(&intent, 3 * DAY).unwrap(),
        Some(reserved.clone())
    );
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    db.delete(format!("consumption:{}", reserved.import_id))
        .unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    // Corruption cannot masquerade as ordinary retry expiry and authorize a new generation.
    assert_eq!(
        store
            .consumption_handoff_get(&intent, 3 * DAY)
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
    assert_eq!(store.expire(3 * DAY).unwrap_err().code, "STORAGE_FAILURE");
}

#[test]
fn consumption_reservations_refuse_generation_changes_traversal_and_capacity() {
    use sparrow_core::ConsumptionGeneration;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let observed = ConsumptionGeneration {
        device: 1,
        inode: 2,
        byte_length: 4,
        modified: (10, 20),
        changed: (10, 30),
    };
    for path in [
        b"".as_slice(),
        b"/absolute",
        b"a/../b",
        b"a//b",
        b".sparrow-inflight/file",
        b"a\0b",
    ] {
        assert_eq!(
            store
                .consumption_reserve(
                    "watch",
                    path,
                    "source",
                    "log",
                    observed.clone(),
                    CONSUMPTION_DIRECTORIES
                )
                .unwrap_err()
                .code,
            "INVALID_ARGUMENT"
        );
    }
    let first = store
        .consumption_reserve(
            "watch",
            b"file",
            "source",
            "log",
            observed.clone(),
            CONSUMPTION_DIRECTORIES,
        )
        .unwrap();
    assert_eq!(
        store
            .consumption_reserve(
                "watch",
                b"file",
                "other",
                "log",
                observed.clone(),
                CONSUMPTION_DIRECTORIES
            )
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    let mut changed = observed.clone();
    changed.modified.1 += 1;
    assert_eq!(
        store
            .consumption_claimed(&first.import_id, changed)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert!(
        store
            .consumption_get(&first.import_id)
            .unwrap()
            .claimed
            .is_none()
    );
    store
        .consumption_claimed(&first.import_id, observed.clone())
        .unwrap();
    for index in 1..16 {
        store
            .consumption_reserve(
                "watch",
                format!("file-{index}").as_bytes(),
                "source",
                "log",
                observed.clone(),
                CONSUMPTION_DIRECTORIES,
            )
            .unwrap();
    }
    assert_eq!(
        store
            .consumption_reserve(
                "watch",
                b"overflow",
                "source",
                "log",
                observed.clone(),
                CONSUMPTION_DIRECTORIES
            )
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store
            .consumption_reserve(
                "watch",
                b"file",
                "source",
                "log",
                observed,
                CONSUMPTION_DIRECTORIES
            )
            .unwrap()
            .import_id,
        first.import_id
    );
    store.expire(100 * DAY).unwrap();
    assert_eq!(store.consumption_pending().unwrap().len(), 16);
    assert!(
        store
            .consumption_get(&first.import_id)
            .unwrap()
            .published_artifact
            .is_none()
    );
}

#[test]
fn consumption_missing_publication_receipt_is_a_hard_failure_without_republication() {
    use sparrow_core::ConsumptionGeneration;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(b"one\n").as_bytes())
    );
    let observation = ConsumptionGeneration {
        device: 1,
        inode: 8,
        byte_length: 4,
        modified: (0, 0),
        changed: (0, 0),
    };
    let journal = store
        .consumption_reserve(
            "fault-watch",
            b"file",
            "source",
            "log",
            observation.clone(),
            CONSUMPTION_DIRECTORIES,
        )
        .unwrap();
    store
        .consumption_claimed(&journal.import_id, observation)
        .unwrap();
    store
        .import_begin(&journal.request_id, "source", "log", 4, &digest, 0)
        .unwrap();
    store
        .import_append(&journal.import_id, 0, b"one\n", 0)
        .unwrap();
    store.import_seal(&journal.import_id, 0).unwrap();
    store.import_prepare_layout(&journal.import_id, 0).unwrap();
    store
        .import_prepare_record(&journal.import_id, 0, 0)
        .unwrap();
    let artifact = store.import_publish(&journal.import_id, 0).unwrap();
    for _ in 0..4 {
        store.expire(3 * DAY).unwrap();
    }
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    db.delete(format!("request:import-publication:{}", journal.import_id))
        .unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .import_start(&journal.request_id, "source", "log", 4, &digest, 3 * DAY)
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
    assert_eq!(
        store
            .import_publish(&journal.import_id, 3 * DAY)
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
    assert_eq!(
        store
            .consumption_get(&journal.import_id)
            .unwrap()
            .published_artifact,
        Some(artifact.id.clone())
    );
    assert_eq!(store.records(3 * DAY).unwrap().len(), 1);
    assert_eq!(store.bytes(&artifact.id, 0, 4, 3 * DAY).unwrap(), b"one\n");
}

#[test]
fn query_selection_intersects_filters_deduplicates_and_seeks_selected_evidence() {
    use sparrow_core::{JsonQuery, QueryScope, QuerySelection, RecordQuery};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .ingest(
            "noise",
            "source",
            "jsonl",
            b"{\"message\":\"needle noise\"}\n",
            10,
        )
        .unwrap();
    let selected = store
        .ingest(
            "selected",
            "source",
            "jsonl",
            b"{\"message\":\"needle selected\"}\n{\"message\":\"needle second\"}\n",
            20,
        )
        .unwrap();
    let later = store
        .ingest(
            "later",
            "source",
            "jsonl",
            b"{\"message\":\"needle later\"}\n",
            30,
        )
        .unwrap();
    let mut scope = QueryScope {
        selection: QuerySelection::Evidence(vec![
            selected.id.clone(),
            selected.record_ids[0].clone(),
            selected.id.clone(),
        ]),
        source: Some("source".into()),
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 100,
        limit: 10,
        scan_budget: 10,
    };
    let rows = store
        .records_query(&scope, &RecordQuery::default(), 40)
        .unwrap();
    assert_eq!(rows["rows"].as_array().unwrap().len(), 2);
    assert!(rows["rows"].as_array().unwrap().iter().all(|row| {
        selected
            .record_ids
            .iter()
            .any(|id| row["record_id"].as_str() == Some(id.as_str()))
    }));
    let keys = store
        .json_query(&scope, &JsonQuery::Keys { path: None }, 40)
        .unwrap();
    assert_eq!(keys["coverage"]["eligible_records"].as_str(), Some("2"));
    let text = store.text_query(&scope, "needle", 40).unwrap();
    assert_eq!(text["rows"].as_array().unwrap().len(), 2);
    // An unrelated earlier catalog record must not consume this explicit scope's budget.
    scope.scan_budget = 1;
    scope.selection = QuerySelection::Evidence(vec![selected.record_ids[0].clone()]);
    let rows = store
        .records_query(&scope, &RecordQuery::default(), 40)
        .unwrap();
    assert_eq!(rows["execution"].as_str(), Some("complete"));
    assert_eq!(rows["rows"].as_array().unwrap().len(), 1);
    scope.scan_budget = 10;
    let context = store
        .records_context(&selected.record_ids[0], &scope, 1, 1, 40)
        .unwrap();
    assert!(context.before.records.is_empty());
    assert!(context.after.records.is_empty());
    assert_eq!(
        store
            .records_context(&later.record_ids[0], &scope, 0, 0, 40)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    scope.source = Some("other".into());
    assert!(
        store
            .records_query(&scope, &RecordQuery::default(), 40)
            .unwrap()["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    // Validate missing selections even when filters would exclude everything.
    scope.selection = QuerySelection::Evidence(vec!["record:missing".into()]);
    assert_eq!(
        store
            .json_query(&scope, &JsonQuery::Keys { path: None }, 40)
            .unwrap_err()
            .code,
        "NOT_FOUND"
    );
    scope.selection = QuerySelection::Evidence(vec![]);
    assert_eq!(
        store.text_query(&scope, "needle", 40).unwrap_err().code,
        "INVALID_ARGUMENT"
    );
}

#[test]
fn query_selection_pin_identity_revision_and_complete_artifact_retention_are_enforced() {
    use sparrow_core::{JsonQuery, QueryScope, QuerySelection};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest(
            "selection-expiry",
            "source",
            "jsonl",
            b"{\"x\":1}\n{\"x\":2}\n",
            0,
        )
        .unwrap();
    let pin = store
        .pin(
            "selection-pin",
            "case",
            &[artifact.record_ids[0].clone()],
            60,
            1,
        )
        .unwrap();
    let now = 31 * DAY;
    let mut scope = QueryScope {
        selection: QuerySelection::Pin {
            id: pin.id.clone(),
            expected_revision: Some(pin.revision),
        },
        source: None,
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: now + 1,
        limit: 10,
        scan_budget: 100,
    };
    let keys = store
        .json_query(&scope, &JsonQuery::Keys { path: None }, now)
        .unwrap();
    assert_eq!(keys["coverage"]["eligible_records"].as_str(), Some("1"));
    scope.selection = QuerySelection::Evidence(vec![artifact.id.clone()]);
    assert_eq!(
        store
            .json_query(&scope, &JsonQuery::Keys { path: None }, now)
            .unwrap_err()
            .code,
        "EVIDENCE_UNAVAILABLE"
    );
    store.expire(now).unwrap();
    assert_eq!(
        store
            .json_query(&scope, &JsonQuery::Keys { path: None }, now)
            .unwrap_err()
            .code,
        "NOT_FOUND"
    );
    scope.selection = QuerySelection::Evidence(vec![artifact.record_ids[0].clone()]);
    assert_eq!(
        store
            .json_query(&scope, &JsonQuery::Keys { path: None }, now)
            .unwrap()["coverage"]["eligible_records"]
            .as_str(),
        Some("1")
    );
    scope.selection = QuerySelection::Pin {
        id: pin.id.clone(),
        expected_revision: Some(pin.revision + 1),
    };
    assert_eq!(
        store
            .json_query(&scope, &JsonQuery::Keys { path: None }, now)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    scope.selection = QuerySelection::Pin {
        id: pin.id.clone(),
        expected_revision: None,
    };
    assert_eq!(
        store
            .json_query(&scope, &JsonQuery::Keys { path: None }, 61 * DAY)
            .unwrap_err()
            .code,
        "PIN_NOT_ACTIVE"
    );
    store.release(&pin.id).unwrap();
    let fresh = store
        .ingest("replacement-artifact", "source", "json", b"{}", now)
        .unwrap();
    let replacement = store
        .pin("replacement-pin", "case", &[fresh.id], 60, now)
        .unwrap();
    assert_ne!(replacement.id, pin.id);
    assert_eq!(
        store
            .json_query(&scope, &JsonQuery::Keys { path: None }, now)
            .unwrap_err()
            .code,
        "NOT_FOUND"
    );
}

#[test]
fn buffered_acceptance_has_a_session_fence_and_flush_covers_all_dependencies() {
    use sparrow_core::Durability;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let first = store
        .ingest_with_durability(
            "buffered",
            "source",
            "json",
            br#"{"message":"buffered evidence"}"#,
            100,
            Durability::Buffered,
        )
        .unwrap();
    let fence = first.buffered.clone().unwrap();
    let retry = store
        .ingest_with_durability(
            "buffered",
            "source",
            "json",
            br#"{"message":"buffered evidence"}"#,
            101,
            Durability::Buffered,
        )
        .unwrap();
    assert_eq!(retry.id, first.id);
    assert_eq!(retry.buffered, first.buffered);
    assert_eq!(store.artifact(&first.id).unwrap().buffered, first.buffered);
    assert_eq!(
        store
            .bytes(&first.id, 0, first.byte_length as usize, 100)
            .unwrap(),
        br#"{"message":"buffered evidence"}"#
    );
    let second = store
        .ingest_with_durability(
            "second",
            "source",
            "log",
            b"second\n",
            100,
            Durability::Buffered,
        )
        .unwrap();
    let state = store.durability_status().unwrap();
    assert_eq!(state.pending_writes, 2);
    assert!(state.pending_bytes > 0);
    assert!(state.durable_through < fence.sequence);
    for (token, code) in [
        (
            sparrow_core::DurabilityWatermark {
                store_id: "store:other".into(),
                ..fence.clone()
            },
            "STORE_MISMATCH",
        ),
        (
            sparrow_core::DurabilityWatermark {
                session_id: "session:other".into(),
                ..fence.clone()
            },
            "STALE_WATERMARK",
        ),
        (
            sparrow_core::DurabilityWatermark {
                sequence: state.accepted_through + 1,
                ..fence.clone()
            },
            "INVALID_ARGUMENT",
        ),
    ] {
        assert_eq!(store.flush_through(&token).unwrap_err().code, code);
        assert_eq!(store.durability_status().unwrap().pending_writes, 2);
    }
    let reached = store.flush_through(&fence).unwrap();
    assert!(reached.sequence >= second.buffered.as_ref().unwrap().sequence);
    assert_eq!(store.durability_status().unwrap().pending_writes, 0);
    assert_eq!(store.durability_status().unwrap().pending_bytes, 0);
    assert!(store.artifact(&first.id).unwrap().buffered.is_none());
    assert!(store.artifact(&second.id).unwrap().buffered.is_none());
    assert!(
        store
            .ingest(
                "buffered",
                "source",
                "json",
                br#"{"message":"buffered evidence"}"#,
                102
            )
            .unwrap()
            .buffered
            .is_none()
    );
    drop(store);
    let reopened = Store::open(dir.path(), config()).unwrap();
    assert!(reopened.artifact(&first.id).unwrap().buffered.is_none());
    assert_ne!(
        reopened.durability_status().unwrap().session_id,
        reached.session_id
    );
    assert_eq!(
        reopened.flush_through(&fence).unwrap_err().code,
        "STALE_WATERMARK"
    );
}

#[test]
fn buffered_ledger_refuses_the_next_write_without_publishing_a_prefix() {
    use sparrow_core::Durability;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let mut first = None;
    for number in 0..sparrow_core::MAX_BUFFERED_WRITES {
        let artifact = store
            .ingest_with_durability(
                &format!("queue-{number}"),
                "queue",
                "opaque",
                b"x",
                0,
                Durability::Buffered,
            )
            .unwrap();
        first.get_or_insert(artifact);
    }
    assert_eq!(
        store.durability_status().unwrap().pending_writes,
        sparrow_core::MAX_BUFFERED_WRITES
    );
    assert!(store.durability_status().unwrap().pending_bytes <= sparrow_core::MAX_BUFFERED_BYTES);
    assert_eq!(
        store
            .ingest_with_durability("overflow", "queue", "opaque", b"x", 0, Durability::Buffered)
            .unwrap_err()
            .code,
        "STORAGE_PRESSURE"
    );
    assert_eq!(
        store.artifacts(0).unwrap().len(),
        sparrow_core::MAX_BUFFERED_WRITES
    );
    assert_eq!(
        store.pressure_status().unwrap().phase,
        sparrow_core::PressurePhase::Ready
    );
    store
        .flush_through(first.unwrap().buffered.as_ref().unwrap())
        .unwrap();
    let accepted = store
        .ingest_with_durability("overflow", "queue", "opaque", b"x", 0, Durability::Buffered)
        .unwrap();
    assert!(accepted.buffered.is_some());
    store.flush_buffered().unwrap();
    assert!(store.artifact(&accepted.id).unwrap().buffered.is_none());
}

#[test]
fn buffered_batch_byte_charge_refuses_new_unique_payloads_before_the_write_bound() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let ingest = |request: &str, bytes: &[u8]| {
        store.ingest_with_durability(
            request,
            "byte-queue",
            "opaque",
            bytes,
            0,
            sparrow_core::Durability::Buffered,
        )
    };
    let mut input = vec![b'x'; 512 * 1024];
    let mut accepted = 0;
    let mut first = None;
    loop {
        // Distinct every payload chunk: changing only the first chunk would
        // correctly deduplicate the remaining bytes and exercise the write cap.
        for (index, chunk) in input
            .chunks_mut(sparrow_core::IMPORT_CHUNK_BYTES)
            .enumerate()
        {
            chunk[..8].copy_from_slice(&(accepted as u64).to_le_bytes());
            chunk[8..16].copy_from_slice(&(index as u64).to_le_bytes());
        }
        match ingest(&format!("bytes-{accepted}"), &input) {
            Ok(artifact) => {
                first.get_or_insert(artifact);
                accepted += 1;
            }
            Err(error) => {
                assert_eq!(error.code, "STORAGE_PRESSURE");
                break;
            }
        }
        assert!(
            accepted < sparrow_core::MAX_BUFFERED_WRITES,
            "byte quota did not limit the queue"
        );
    }
    assert!(accepted > 0);
    assert!(accepted < sparrow_core::MAX_BUFFERED_WRITES);
    let state = store.durability_status().unwrap();
    assert_eq!(state.pending_writes, accepted);
    assert!(state.pending_bytes <= sparrow_core::MAX_BUFFERED_BYTES);
    assert_eq!(store.artifacts(0).unwrap().len(), accepted);
    store
        .flush_through(first.unwrap().buffered.as_ref().unwrap())
        .unwrap();
    assert_eq!(store.durability_status().unwrap().pending_bytes, 0);
    assert!(
        ingest(&format!("bytes-{accepted}"), &input)
            .unwrap()
            .buffered
            .is_some()
    );
    assert_eq!(store.artifacts(0).unwrap().len(), accepted + 1);
    assert_eq!(store.durability_status().unwrap().pending_writes, 1);
}

#[test]
fn durable_retries_import_receipts_and_empty_control_batches_cover_buffered_intake() {
    use sparrow_core::Durability;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let first = store
        .ingest_with_durability(
            "retry-durable",
            "source",
            "log",
            b"x",
            0,
            Durability::Buffered,
        )
        .unwrap();
    assert!(first.buffered.is_some());
    assert!(
        store
            .ingest("retry-durable", "source", "log", b"x", 1)
            .unwrap()
            .buffered
            .is_none()
    );
    let second = store
        .ingest_with_durability(
            "import-durable",
            "source",
            "log",
            b"x",
            0,
            Durability::Buffered,
        )
        .unwrap();
    let imported = store
        .import_start("import-durable", "source", "log", 1, &second.payload_id, 0)
        .unwrap();
    match imported {
        sparrow_core::ImportStart::Accepted(artifact) => assert!(artifact.buffered.is_none()),
        _ => panic!("accepted request became a staged import"),
    }
    let third = store
        .ingest_with_durability(
            "empty-control",
            "source",
            "log",
            b"x",
            0,
            Durability::Buffered,
        )
        .unwrap();
    assert!(third.buffered.is_some());
    assert_eq!(store.expire(0).unwrap(), 0);
    assert!(store.artifact(&third.id).unwrap().buffered.is_none());
}

fn oversized_jsonl_fixture() -> Vec<u8> {
    let mut input = b"{}\n{\"message\":\"".to_vec();
    let start = input.len();
    input.resize(start + 17 * 1024 * 1024, b'x');
    for (index, chunk) in input[start..]
        .chunks_mut(sparrow_core::IMPORT_CHUNK_BYTES)
        .enumerate()
    {
        let marker = purrdf_hash::hex::encode(&(index as u64).to_le_bytes());
        chunk[..marker.len()].copy_from_slice(marker.as_bytes());
    }
    input.extend_from_slice(b"\"}\n{}\n");
    input
}

fn stage_fixture(
    store: &Store,
    request: &str,
    format: &str,
    input: &[u8],
) -> sparrow_core::ImportManifest {
    layout_fixture(store, request, format, input).unwrap()
}

/// Upload and seal `input`, then prepare its layout, returning the layout outcome.
fn layout_fixture(
    store: &Store,
    request: &str,
    format: &str,
    input: &[u8],
) -> Result<sparrow_core::ImportManifest, sparrow_core::Error> {
    let payload = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(input).as_bytes())
    );
    let manifest = store
        .import_begin(
            request,
            "oversized",
            format,
            input.len() as u64,
            &payload,
            0,
        )
        .unwrap();
    for (index, chunk) in input.chunks(sparrow_core::IMPORT_CHUNK_BYTES).enumerate() {
        store
            .import_append(
                &manifest.id,
                (index * sparrow_core::IMPORT_CHUNK_BYTES) as u64,
                chunk,
                0,
            )
            .unwrap();
    }
    store.import_seal(&manifest.id, 0).unwrap();
    store.import_prepare_layout(&manifest.id, 0)
}

/// Import and publish one JSON document of `size` bytes with distinct chunks,
/// optionally under a lowered raw-record ceiling.
fn json_document(
    dir: &std::path::Path,
    ceiling: Option<u64>,
    size: usize,
) -> (Store, sparrow_core::Artifact, Vec<u8>) {
    let chunk = sparrow_core::IMPORT_CHUNK_BYTES;
    let mut settings = config();
    settings.disk_budget = 512 * 1024 * 1024;
    let store = Store::open(dir, settings).unwrap();
    if let Some(ceiling) = ceiling {
        store.split_raw_records_above(ceiling).unwrap();
    }
    let mut input = b"{\"message\":\"".to_vec();
    let start = input.len();
    input.resize(size - 3, b'x');
    for (index, piece) in input[start..].chunks_mut(chunk).enumerate() {
        let marker = purrdf_hash::hex::encode(&(index as u64).to_le_bytes());
        piece[..marker.len()].copy_from_slice(marker.as_bytes());
    }
    input.extend_from_slice(b"\"}\n");
    let manifest = stage_fixture(&store, "json-document", "json", &input);
    for ordinal in 0..manifest.layout_records {
        let prepared = (0..64).any(|_| {
            store
                .import_prepare_record(&manifest.id, ordinal, 0)
                .unwrap()
                .prepared_records
                > ordinal
        });
        assert!(prepared, "raw record {ordinal} prepares in bounded steps");
    }
    let artifact = store.import_publish(&manifest.id, 0).unwrap();
    (store, artifact, input)
}

/// Lay out one 50 MiB JSON document under the smallest raw-record ceiling, which
/// splits it into three raw records, then publish it.
fn split_document(dir: &std::path::Path) -> (Store, sparrow_core::Artifact, Vec<u8>) {
    let ceiling = 32 * 1024 * 1024 + sparrow_core::IMPORT_CHUNK_BYTES as u64;
    let (store, artifact, input) = json_document(dir, Some(ceiling), 50 * 1024 * 1024);
    let piece = ceiling - 16 * 1024 * 1024;
    let bounds: Vec<_> = artifact
        .record_ids
        .iter()
        .map(|id| store.record(id).unwrap())
        .map(|record| (record.byte_start, record.byte_end))
        .collect();
    assert_eq!(
        bounds,
        [
            (0, piece),
            (piece, 2 * piece),
            (2 * piece, input.len() as u64)
        ]
    );
    (store, artifact, input)
}

/// Open a store whose import bound and pin caps derive from scaled retirement
/// limits, and return those limits with the derived bounds.
fn retirement_scaled_store(
    dir: &std::path::Path,
) -> (
    Store,
    sparrow_core::EvictionLimits,
    sparrow_core::RetirementBounds,
) {
    let mut settings = config();
    settings.disk_budget = 512 * 1024 * 1024;
    let store = Store::open(dir, settings).unwrap();
    let limits = sparrow_core::EvictionLimits {
        max_catalog_bytes: 640 * 1024,
        max_pin_catalog_bytes: 512 * 1024,
        max_payload_refs: 256,
        ..sparrow_core::EvictionLimits::default()
    };
    let bounds = store
        .scale_retirement_for_tests(limits, 128 * 1024)
        .unwrap();
    assert!(bounds.import_bytes < sparrow_core::MAX_IMPORT_BYTES);
    assert!(bounds.pins < sparrow_core::MAX_PINS);
    assert!(bounds.pinned_members < sparrow_core::MAX_PINNED_MEMBERS);
    (store, limits, bounds)
}

/// Ingest a small log of `lines` records and pin all of them across
/// `bounds.pins` pins of `days`, filling both store-wide pin caps exactly.
fn pins_at_cap(store: &Store, bounds: sparrow_core::RetirementBounds, days: u64, now: u64) {
    let lines = bounds.pinned_members;
    let input: String = (0..lines).map(|line| format!("pinned {line}\n")).collect();
    let artifact = store
        .ingest("pinned-log", "pinned", "log", input.as_bytes(), now)
        .unwrap();
    assert_eq!(artifact.record_ids.len(), lines);
    let share = lines.div_ceil(bounds.pins);
    for (index, members) in artifact.record_ids.chunks(share).enumerate() {
        let name = format!("cap-{index}");
        store.pin(&name, &name, members, days, now).unwrap();
    }
}

/// A log of `bytes` bytes in distinct lines of `line` bytes each, so every line is
/// one record of `line.div_ceil(64 KiB)` chunks.
fn log_lines(bytes: u64, line: usize) -> Vec<u8> {
    let mut input = Vec::with_capacity(bytes as usize);
    let mut index = 0_u64;
    while (input.len() as u64) < bytes {
        let start = input.len();
        input.extend_from_slice(format!("{index:016x}").as_bytes());
        input.resize(start + line - 1, b'x');
        input.push(b'\n');
        index += 1;
    }
    assert_eq!(input.len() as u64, bytes);
    input
}

fn publish_staged(
    store: &Store,
    request: &str,
    input: &[u8],
) -> Result<sparrow_core::Artifact, sparrow_core::Error> {
    let manifest = stage_fixture(store, request, "log", input);
    for ordinal in 0..manifest.layout_records {
        store
            .import_prepare_record(&manifest.id, ordinal, 0)
            .unwrap();
    }
    store.import_publish(&manifest.id, 0)
}

#[test]
fn import_ceiling_artifact_drains_through_expiry_and_eviction_and_one_chunk_more_is_refused() {
    let chunk = sparrow_core::IMPORT_CHUNK_BYTES;
    let dir = tempfile::tempdir().unwrap();
    let (store, limits, bounds) = retirement_scaled_store(dir.path());
    let bound = bounds.import_bytes;
    // One chunk more than the admitted ceiling is refused before any upload.
    let refused = store
        .import_begin(
            "over-ceiling",
            "oversized",
            "log",
            bound + 1,
            &format!("blake3:{}", "0".repeat(64)),
            0,
        )
        .unwrap_err();
    assert_eq!(refused.code, "RESOURCE_LIMIT");
    assert!(
        refused.message.contains("split the input"),
        "{}",
        refused.message
    );
    // At the ceiling every chunk is its own record, the costliest admitted row.
    let input = log_lines(bound, chunk);
    let artifact = publish_staged(&store, "at-ceiling", &input).unwrap();
    // Pins at both store-wide caps, expired by the time expiry runs: their rows are
    // charged to the pin budget, not the artifact envelope, and expiry deletes them.
    pins_at_cap(&store, bounds, 1, 0);
    let chunks = (bound / chunk as u64) as usize;
    assert_eq!(artifact.record_ids.len(), chunks);
    assert_eq!(
        store
            .bytes(&artifact.id, bound - 1024 * 1024, 1024 * 1024, 1)
            .unwrap(),
        input[input.len() - 1024 * 1024..]
    );
    let records = (chunks + bounds.pinned_members) as u64;
    let mut expired = 0;
    let drained = (0..20_000).any(|_| {
        let report = store.expire_bounded(30 * DAY, limits).unwrap();
        expired += report.records;
        !report.continuation && expired == records
    });
    assert!(drained, "{expired} of {records} records expired");
    // Every pin row is gone, not merely expired.
    assert!(store.pins(0).unwrap().is_empty());
    assert_eq!(
        store.bytes(&artifact.id, 0, 1, 30 * DAY).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    // Within the byte bound, twice the records per chunk overflows the row read and
    // layout refuses it before publication work.
    let crowded =
        layout_fixture(&store, "crowded", "log", &log_lines(bound, chunk / 2)).unwrap_err();
    assert_eq!(crowded.code, "RESOURCE_LIMIT");
    assert!(
        crowded.message.contains("retirement pass"),
        "{}",
        crowded.message
    );
    drop(store);
    drop(dir);

    let dir = tempfile::tempdir().unwrap();
    let (store, limits, bounds) = retirement_scaled_store(dir.path());
    let artifact = publish_staged(&store, "at-ceiling", &log_lines(bound, chunk)).unwrap();
    // Live pins at both caps on newer evidence: eviction still drains the ceiling.
    pins_at_cap(&store, bounds, 60, 0);
    let mut evicted = 0;
    let drained = (0..20_000).any(|_| {
        let report = store.evict_oldest(1, limits).unwrap();
        evicted += report.records.len();
        !report.continuation && report.pending_manifest_refs == 0
    });
    assert!(
        drained && evicted == chunks,
        "{evicted} of {chunks} records evicted"
    );
    assert_eq!(
        store.bytes(&artifact.id, 0, 1, 1).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
}

/// INVARIANT: pin creation and extension stop at the store-wide row and member caps
/// that keep every retirement pass's pin capture inside its budget, and release is
/// never refused, so freeing a slot restores admission.
#[test]
fn pins_beyond_the_store_wide_caps_are_refused_and_release_restores_admission() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _, bounds) = retirement_scaled_store(dir.path());
    let lines = bounds.pinned_members + 1;
    let input: String = (0..lines).map(|line| format!("pinned {line}\n")).collect();
    let records = store
        .ingest("pinned-log", "pinned", "log", input.as_bytes(), 0)
        .unwrap()
        .record_ids;
    let share = bounds.pinned_members / bounds.pins;
    let mut pins: Vec<_> = records[..bounds.pinned_members]
        .chunks(share)
        .enumerate()
        .map(|(index, members)| {
            let name = format!("full-{index}");
            store.pin(&name, &name, members, 40, 0).unwrap()
        })
        .collect();
    assert_eq!(pins.len(), bounds.pins);
    let refused = |result: Result<sparrow_core::Pin, sparrow_core::Error>, held: &str| {
        let error = result.unwrap_err();
        assert_eq!(error.code, "RESOURCE_LIMIT");
        assert!(
            error.message.contains(&format!(
                "capped at {} rows and {} members",
                bounds.pins, bounds.pinned_members
            )) && error.message.contains(held),
            "{}",
            error.message
        );
    };
    let full = format!(
        "holds {} rows with {} members",
        bounds.pins, bounds.pinned_members
    );
    let last = records[bounds.pinned_members..].to_vec();
    // At both caps neither a new pin nor one more member on an existing pin fits.
    refused(store.pin("over-new", "over", &last, 40, 0), &full);
    refused(store.pin("over-extend", "full-0", &last, 40, 0), &full);
    // Release is admitted at the caps and frees a row and its members.
    store.release(&pins.remove(0).id).unwrap();
    store.pin("refill", "refill", &last, 40, 0).unwrap();
    // The row cap alone refuses a further pin while members remain below their cap.
    refused(
        store.pin("over-rows", "over", &records[..1], 40, 0),
        &format!(
            "holds {} rows with {} members",
            bounds.pins,
            bounds.pinned_members - share + 1
        ),
    );
    store.release(&pins.remove(0).id).unwrap();
    store
        .pin("after-release", "over", &records[..1], 40, 0)
        .unwrap();
}

#[test]
fn rdf_record_view_charges_a_large_raw_record_row_by_its_measured_length() {
    use purrdf_lex::json::record::ToJson;
    let dir = tempfile::tempdir().unwrap();
    // One 45 MiB document stays a single raw record of 721 chunks. Its catalog
    // row exceeds the fixed bound the view once derived from a 16 MiB limit.
    let (store, artifact, _) = json_document(dir.path(), None, 45 * 1024 * 1024);
    let id = artifact.record_ids[0].clone();
    let row = purrdf_lex::json::write_compact(&store.record(&id).unwrap().to_json()).len();
    assert!(row > (16 * 1024 * 1024 / sparrow_core::IMPORT_CHUNK_BYTES + 1) * 128 + 8192);
    let metadata = sparrow_core::RdfGraphLayers {
        metadata: true,
        diagnostic: false,
        content: false,
    };
    store
        .with_rdf_record_layers(&[id], metadata, 1, Default::default(), |_| ())
        .unwrap();
}

#[test]
fn split_raw_document_reads_back_exactly_and_both_expires_and_evicts() {
    use sparrow_core::EvictionLimits;
    // A pass budget below the whole document's 801 chunks, above each piece's.
    let limits = EvictionLimits {
        max_payload_refs: 600,
        ..EvictionLimits::default()
    };
    let dir = tempfile::tempdir().unwrap();
    let (store, artifact, input) = split_document(dir.path());
    let window = 1024 * 1024;
    for offset in (0..input.len()).step_by(window) {
        let length = window.min(input.len() - offset);
        assert_eq!(
            store.bytes(&artifact.id, offset as u64, length, 1).unwrap(),
            input[offset..offset + length]
        );
    }
    let refused = store
        .import_begin(
            "too-large",
            "oversized",
            "json",
            sparrow_core::MAX_IMPORT_BYTES + 1,
            &artifact.payload_id,
            1,
        )
        .unwrap_err();
    assert_eq!(refused.code, "RESOURCE_LIMIT");
    assert!(refused.message.contains("split the input"));
    let mut expired = 0;
    let drained = (0..64).any(|_| {
        let report = store.expire_bounded(30 * DAY, limits).unwrap();
        expired += report.records;
        !report.continuation
    });
    assert!(drained && expired == 3, "{expired} records expired");
    assert_eq!(
        store.bytes(&artifact.id, 0, 1, 30 * DAY).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );

    let dir = tempfile::tempdir().unwrap();
    let (store, artifact, _) = split_document(dir.path());
    let mut evicted = 0;
    let drained = (0..64).any(|_| {
        let report = store.evict_oldest(1, limits).unwrap();
        evicted += report.records.len();
        !report.continuation && report.pending_manifest_refs == 0
    });
    assert!(drained && evicted == 3, "{evicted} records evicted");
    assert_eq!(
        store.bytes(&artifact.id, 0, 1, 1).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
}

#[test]
fn oversized_json_record_resumes_bounded_raw_checkpoints_and_preserves_pinned_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = oversized_jsonl_fixture();
    let manifest = stage_fixture(&store, "large-json", "jsonl", &input);
    store
        .define_time_profile("oversized", "/t", sparrow_core::TimeRepresentation::Rfc3339)
        .unwrap();
    store.import_prepare_record(&manifest.id, 0, 1).unwrap();
    let first = store.import_prepare_record(&manifest.id, 1, 1).unwrap();
    let raw = first.raw.as_ref().unwrap();
    assert_eq!(raw.prepared_chunks, 128);
    assert_eq!(raw.total_chunks, 273);
    let record_id = raw.record_id.clone();
    assert_eq!(store.record(&record_id).unwrap_err().code, "NOT_FOUND");
    assert!(store.records(1).unwrap().is_empty());
    assert_eq!(
        store.import_publish(&manifest.id, 1).unwrap_err().code,
        "CONFLICT"
    );
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    // An older record ordinal recovers progress without adding raw ownership again.
    assert_eq!(
        store.import_prepare_record(&manifest.id, 0, 2).unwrap(),
        first
    );
    let second = store.import_prepare_record(&manifest.id, 1, 2).unwrap();
    assert_eq!(second.raw.as_ref().unwrap().record_id, record_id);
    assert_eq!(second.raw.as_ref().unwrap().prepared_chunks, 256);
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .import_preparation_get(&manifest.id, 3)
            .unwrap()
            .unwrap(),
        second
    );
    let third = store.import_prepare_record(&manifest.id, 1, 3).unwrap();
    assert_eq!(third.raw.as_ref().unwrap().prepared_chunks, 273);
    let finished = store.import_prepare_record(&manifest.id, 1, 3).unwrap();
    assert_eq!(finished.prepared_records, 2);
    assert!(finished.raw.is_none());
    store.import_prepare_record(&manifest.id, 2, 3).unwrap();
    let artifact = store.import_publish(&manifest.id, 3).unwrap();
    assert_eq!(artifact.record_ids[1], record_id);
    let record = store.record(&record_id).unwrap();
    assert_eq!(record.byte_start, 3);
    assert_eq!(record.byte_end, (input.len() - 3) as u64);
    assert!(matches!(
        &record.event_time,
        sparrow_core::EventTime::Unknown { reason, profile_id: Some(_) } if reason == "projection_failed"
    ));
    let rdf = store.rdf_inspect(&record_id, 3).unwrap();
    assert_eq!(rdf["state"].as_str(), Some("failed"));
    assert!(rdf["diagnostic"].as_str().unwrap().contains("source bytes"));
    let scope = sparrow_core::QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: Some("oversized".into()),
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 10,
        limit: 100,
        scan_budget: 100,
    };
    let keys = store
        .json_query(&scope, &sparrow_core::JsonQuery::Keys { path: None }, 3)
        .unwrap();
    assert_eq!(keys["coverage"]["failed_json_records"].as_str(), Some("1"));
    let text = store.text_query(&scope, "marker", 3).unwrap();
    assert_eq!(
        text["text_coverage"]["failed_or_incomplete"].as_str(),
        Some("1")
    );
    let strict = sparrow_core::RecordQuery {
        text: Some("marker".into()),
        require_complete_index: true,
        ..Default::default()
    };
    assert_eq!(
        store.records_query(&scope, &strict, 3).unwrap_err().code,
        "INDEX_NOT_READY"
    );
    for (index, expected) in input.chunks(1024 * 1024).enumerate() {
        assert_eq!(
            store
                .bytes(
                    &artifact.id,
                    (index * 1024 * 1024) as u64,
                    expected.len(),
                    3
                )
                .unwrap(),
            expected
        );
    }
    let record_bytes = &input[record.byte_start as usize..record.byte_end as usize];
    for (index, expected) in record_bytes.chunks(1024 * 1024).enumerate() {
        assert_eq!(
            store
                .bytes(&record_id, (index * 1024 * 1024) as u64, expected.len(), 3)
                .unwrap(),
            expected
        );
    }
    let pin = store
        .pin("large-pin", "Case", std::slice::from_ref(&record_id), 40, 3)
        .unwrap();
    while store.import_reclaim(2 * DAY).unwrap() != 0 {}
    store.expire(31 * DAY).unwrap();
    assert_eq!(
        store
            .bytes(&record_id, record_bytes.len() as u64 - 4, 4, 31 * DAY)
            .unwrap(),
        b"x\"}\n"
    );
    store.release(&pin.id).unwrap();
    store.expire(41 * DAY).unwrap();
    assert_eq!(
        store.status(41 * DAY).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
    assert_eq!(
        store.status(41 * DAY).unwrap()["unique_json_projections"].as_str(),
        Some("0")
    );
}

#[test]
fn abandoned_partial_raw_record_releases_only_its_own_shared_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = oversized_jsonl_fixture();
    let shared_bytes = &input[3..3 + sparrow_core::IMPORT_CHUNK_BYTES];
    let shared = store
        .ingest("existing-raw", "shared", "log", shared_bytes, 0)
        .unwrap();
    let manifest = stage_fixture(&store, "abandoned-raw", "jsonl", &input);
    store.import_prepare_record(&manifest.id, 0, 1).unwrap();
    let first = store.import_prepare_record(&manifest.id, 1, 1).unwrap();
    assert_eq!(first.raw.unwrap().prepared_chunks, 128);
    store.import_cancel(&manifest.id, 2).unwrap();
    assert!(store.import_reclaim(2).unwrap() <= 128);
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    while store.import_reclaim(2).unwrap() != 0 {}
    assert_eq!(
        store.import_get(&manifest.id, 2).unwrap_err().code,
        "NOT_FOUND"
    );
    assert_eq!(
        store.bytes(&shared.id, 0, shared_bytes.len(), 2).unwrap(),
        shared_bytes
    );
    assert_eq!(store.records(2).unwrap().len(), 1);
    assert_eq!(
        store.status(2).unwrap()["unique_payload_chunks"].as_str(),
        Some("1")
    );
    store.expire(31 * DAY).unwrap();
    assert_eq!(
        store.status(31 * DAY).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
}

#[test]
fn oversized_documents_keep_bytes_with_declared_projection_limits_or_nonapplicability() {
    let input = oversized_jsonl_fixture();
    for format in ["json", "markdown", "opaque"] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), config()).unwrap();
        let manifest = stage_fixture(&store, format, format, &input);
        let mut expected = 0;
        loop {
            let progress = store
                .import_prepare_record(&manifest.id, expected, 1)
                .unwrap();
            expected = progress.prepared_records;
            if expected == progress.total_records {
                break;
            }
        }
        let artifact = store.import_publish(&manifest.id, 1).unwrap();
        assert_eq!(artifact.record_ids.len(), 1);
        let rdf = store.rdf_inspect(&artifact.record_ids[0], 1).unwrap();
        assert_eq!(
            rdf["state"].as_str(),
            Some(if format == "opaque" {
                "not_applicable"
            } else {
                "failed"
            })
        );
        if format == "markdown" {
            assert!(
                rdf["diagnostic"]
                    .as_str()
                    .unwrap()
                    .contains("materialization bound")
            );
        }
        let scope = sparrow_core::QueryScope {
            selection: sparrow_core::QuerySelection::Live,
            source: Some("oversized".into()),
            sources: None,
            diagnostics: Default::default(),
            require_complete_index: false,
            received_from: 0,
            received_before: 10,
            limit: 10,
            scan_budget: 100,
        };
        let text = store.text_query(&scope, "marker", 1).unwrap();
        assert_eq!(
            text["text_coverage"][if format == "opaque" {
                "not_applicable"
            } else {
                "failed_or_incomplete"
            }]
            .as_str(),
            Some("1")
        );
        for (index, bytes) in input.chunks(1024 * 1024).enumerate() {
            assert_eq!(
                store
                    .bytes(
                        &artifact.record_ids[0],
                        (index * 1024 * 1024) as u64,
                        bytes.len(),
                        1
                    )
                    .unwrap(),
                bytes
            );
        }
        while store.import_reclaim(2 * DAY).unwrap() != 0 {}
        store.expire(31 * DAY).unwrap();
        assert_eq!(
            store.status(31 * DAY).unwrap()["unique_payload_chunks"].as_str(),
            Some("0")
        );
    }
}

#[test]
fn oversized_raw_record_refuses_missing_normalized_bytes_without_publishing() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = oversized_jsonl_fixture();
    let manifest = stage_fixture(&store, "missing-raw", "jsonl", &input);
    store.import_prepare_record(&manifest.id, 0, 1).unwrap();
    for _ in 0..3 {
        store.import_prepare_record(&manifest.id, 1, 1).unwrap();
    }
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    let digest = purrdf_hash::hex::encode(
        purrdf_hash::blake3::hash(&input[3..3 + sparrow_core::IMPORT_CHUNK_BYTES]).as_bytes(),
    );
    db.delete_cf(db.cf_handle("payload").unwrap(), digest)
        .unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .import_prepare_record(&manifest.id, 1, 2)
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
    assert!(store.artifacts(2).unwrap().is_empty());
    assert!(store.records(2).unwrap().is_empty());
    assert_eq!(
        store.import_bytes(&manifest.id, 0, 100, 2).unwrap(),
        input[..100]
    );
    store.import_cancel(&manifest.id, 2).unwrap();
    while store.import_reclaim(2).unwrap() != 0 {}
    assert_eq!(
        store.status(2).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
}

#[test]
fn buffered_and_streamed_ingestion_share_request_identity() {
    use sparrow_core::ImportStart;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = b"one\ntwo\n";
    let payload = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(input).as_bytes())
    );
    let buffered = store
        .ingest("buffered-first", "copy", "log", input, 0)
        .unwrap();
    match store
        .import_start(
            "buffered-first",
            "copy",
            "log",
            input.len() as u64,
            &payload,
            1,
        )
        .unwrap()
    {
        ImportStart::Accepted(receipt) => assert_eq!(receipt.id, buffered.id),
        ImportStart::Staging(_) => panic!("accepted request allocated duplicate staging"),
    }
    assert_eq!(
        store
            .import_start(
                "buffered-first",
                "changed",
                "log",
                input.len() as u64,
                &payload,
                1
            )
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert_eq!(
        store
            .import_start(
                "buffered-first",
                "copy",
                "opaque",
                input.len() as u64,
                &payload,
                1
            )
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    let stage = match store
        .import_start(
            "streamed-first",
            "copy",
            "log",
            input.len() as u64,
            &payload,
            1,
        )
        .unwrap()
    {
        ImportStart::Staging(value) => value,
        ImportStart::Accepted(_) => panic!("new request reused independent occurrence"),
    };
    assert_eq!(
        store
            .ingest("streamed-first", "copy", "log", input, 1)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    store.import_append(&stage.id, 0, input, 1).unwrap();
    store.import_seal(&stage.id, 1).unwrap();
    store.import_prepare_layout(&stage.id, 1).unwrap();
    store.import_prepare_record(&stage.id, 0, 1).unwrap();
    store.import_prepare_record(&stage.id, 1, 1).unwrap();
    let streamed = store.import_publish(&stage.id, 2).unwrap();
    assert_ne!(streamed.id, buffered.id);
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .ingest("streamed-first", "copy", "log", input, 3)
            .unwrap()
            .id,
        streamed.id
    );
    match store
        .import_start(
            "streamed-first",
            "copy",
            "log",
            input.len() as u64,
            &payload,
            3,
        )
        .unwrap()
    {
        ImportStart::Accepted(receipt) => assert_eq!(receipt.id, streamed.id),
        ImportStart::Staging(_) => panic!("published request recreated staging"),
    }
    assert_eq!(
        store
            .ingest("streamed-first", "copy", "log", b"changed", 3)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert_eq!(store.records(3).unwrap().len(), 4);
}

#[test]
fn prepared_import_publishes_native_records_atomically_and_retries_after_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    // One preparation checkpoint holds at most 256 records; the 257th stays
    // pending so the partially prepared state is observable.
    let input =
        &b"{\"timestamp\":\"2026-09-27T00:13:37.406Z\",\"message\":\"alpha\"}\n".repeat(257)[..];
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(input).as_bytes())
    );
    let journal = store
        .import_begin(
            "publish-copy",
            "events",
            "jsonl",
            input.len() as u64,
            &digest,
            0,
        )
        .unwrap();
    store
        .define_time_profile(
            "events",
            "/timestamp",
            sparrow_core::TimeRepresentation::Rfc3339,
        )
        .unwrap();
    store.import_append(&journal.id, 0, input, 0).unwrap();
    store.import_seal(&journal.id, 0).unwrap();
    store.import_prepare_layout(&journal.id, 0).unwrap();
    assert!(
        store
            .import_preparation_get(&journal.id, 0)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.import_publish(&journal.id, 0).unwrap_err().code,
        "CONFLICT"
    );
    let progress = store.import_prepare_record(&journal.id, 0, 10).unwrap();
    assert_eq!(progress.prepared_records, 256);
    assert!(store.records(10).unwrap().is_empty());
    assert_eq!(
        store.artifact(&progress.artifact_id).unwrap_err().code,
        "NOT_FOUND"
    );
    assert_eq!(
        store.import_publish(&journal.id, 10).unwrap_err().code,
        "CONFLICT"
    );
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.import_prepare_record(&journal.id, 0, 20).unwrap(),
        progress
    );
    assert_eq!(
        store
            .import_prepare_record(&journal.id, 257, 20)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    let complete = store.import_prepare_record(&journal.id, 256, 20).unwrap();
    assert_eq!(complete.prepared_records, 257);
    assert_eq!(complete.received_at, 10);
    let artifact = store.import_publish(&journal.id, 30).unwrap();
    assert_eq!(artifact.record_ids.len(), 257);
    assert_eq!(
        store.bytes(&artifact.id, 0, input.len(), 30).unwrap(),
        input
    );
    for id in &artifact.record_ids {
        let record = store.record(id).unwrap();
        assert!(matches!(
            record.event_time,
            sparrow_core::EventTime::Known { .. }
        ));
        assert_eq!(
            store.rdf_inspect(id, 30).unwrap()["state"].as_str(),
            Some("complete")
        );
    }
    assert_eq!(store.records(30).unwrap().len(), 257);
    // Publication transfers dependencies and preserves an independent receipt:
    // its staging can leave admission immediately, before either lease expires.
    while store.import_reclaim(31).unwrap() != 0 {}
    assert_eq!(
        store
            .import_get(&journal.id, 31)
            .unwrap()
            .published_artifact
            .as_ref(),
        Some(&artifact.id)
    );
    assert_eq!(
        store.import_publish(&journal.id, 31).unwrap().id,
        artifact.id
    );
    assert_eq!(
        store.bytes(&artifact.id, 0, input.len(), 31).unwrap(),
        input
    );
    assert_eq!(
        store.import_cancel(&journal.id, 30).unwrap_err().code,
        "CONFLICT"
    );
    // Expired private staging copies must never decrement published ownership.
    while store.import_reclaim(2 * DAY).unwrap() != 0 {}
    assert_eq!(store.records(2 * DAY).unwrap().len(), 257);
    assert_eq!(
        store.bytes(&artifact.id, 0, input.len(), 2 * DAY).unwrap(),
        input
    );
    assert_eq!(
        store.import_publish(&journal.id, 2 * DAY).unwrap().id,
        artifact.id
    );
    let retry = store
        .import_begin(
            "publish-copy",
            "events",
            "jsonl",
            input.len() as u64,
            &digest,
            2 * DAY,
        )
        .unwrap();
    assert_eq!(retry.published_artifact.as_ref(), Some(&artifact.id));
    assert_eq!(
        store
            .import_begin(
                "publish-copy",
                "changed",
                "jsonl",
                input.len() as u64,
                &digest,
                2 * DAY
            )
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    // Each bounded expiry pass retires at most 32 records.
    while store.expire(31 * DAY).unwrap() != 0 {}
    assert_eq!(
        store.status(31 * DAY).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
    assert_eq!(
        store.status(31 * DAY).unwrap()["unique_json_projections"].as_str(),
        Some("0")
    );
}

#[test]
fn completed_staging_releases_admission_before_expiry_without_releasing_evidence_or_retries() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = b"{}\n";
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(input).as_bytes())
    );
    let mut receipts = Vec::new();
    for index in 0..32 {
        let request = format!("completed-admission-{index}");
        let journal = store
            .import_begin(
                &request,
                "completed",
                "jsonl",
                input.len() as u64,
                &digest,
                0,
            )
            .unwrap();
        store.import_append(&journal.id, 0, input, 0).unwrap();
        store.import_seal(&journal.id, 0).unwrap();
        store.import_prepare_layout(&journal.id, 0).unwrap();
        store.import_prepare_record(&journal.id, 0, 0).unwrap();
        let artifact = store.import_publish(&journal.id, 0).unwrap();
        loop {
            let units = store.import_reclaim(0).unwrap();
            assert!(units <= 128);
            if units == 0 {
                break;
            }
        }
        receipts.push((request, journal.id, artifact));
    }
    assert_eq!(store.records(0).unwrap().len(), 32);
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    for (request, journal, artifact) in receipts {
        assert_eq!(
            store
                .import_get(&journal, 1)
                .unwrap()
                .published_artifact
                .as_ref(),
            Some(&artifact.id)
        );
        assert_eq!(
            store.import_publish(&journal, 1).unwrap().record_ids,
            artifact.record_ids
        );
        assert_eq!(
            store
                .import_begin(
                    &request,
                    "completed",
                    "jsonl",
                    input.len() as u64,
                    &digest,
                    1
                )
                .unwrap()
                .published_artifact
                .as_ref(),
            Some(&artifact.id)
        );
        assert_eq!(
            store.import_cancel(&journal, 1).unwrap_err().code,
            "CONFLICT"
        );
        assert_eq!(store.bytes(&artifact.id, 0, input.len(), 1).unwrap(), input);
        assert_eq!(
            store.rdf_inspect(&artifact.record_ids[0], 1).unwrap()["state"].as_str(),
            Some("complete")
        );
    }
    // Reclamation cannot convert sixteen still-live unfinished upload leases
    // into extra admission slots or delete their committed input.
    let mut active = Vec::new();
    for index in 0..16 {
        let journal = store
            .import_begin(
                &format!("unfinished-{index}"),
                "active",
                "jsonl",
                input.len() as u64,
                &digest,
                1,
            )
            .unwrap();
        store.import_append(&journal.id, 0, input, 1).unwrap();
        active.push(journal);
    }
    assert_eq!(store.import_reclaim(1).unwrap(), 0);
    assert_eq!(
        store
            .import_begin(
                "unfinished-overflow",
                "active",
                "jsonl",
                input.len() as u64,
                &digest,
                1
            )
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    for journal in active {
        assert_eq!(store.import_read_chunk(&journal.id, 0, 1).unwrap(), input);
    }
    assert_eq!(store.records(1).unwrap().len(), 32);
}

#[test]
fn canceled_native_preparation_releases_only_its_own_dependencies() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = b"{\"message\":\"retained\"}\n{\"message\":\"retained\"}\n";
    let original = store
        .ingest("original", "retained", "jsonl", input, 0)
        .unwrap();
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(input).as_bytes())
    );
    let journal = store
        .import_begin(
            "cancel-native",
            "private",
            "jsonl",
            input.len() as u64,
            &digest,
            0,
        )
        .unwrap();
    store.import_append(&journal.id, 0, input, 0).unwrap();
    store.import_seal(&journal.id, 0).unwrap();
    store.import_prepare_layout(&journal.id, 0).unwrap();
    store.import_prepare_record(&journal.id, 0, 1).unwrap();
    store.import_prepare_record(&journal.id, 1, 1).unwrap();
    assert_eq!(store.records(1).unwrap().len(), 2);
    store.import_cancel(&journal.id, 2).unwrap();
    assert!(store.import_reclaim(2).unwrap() > 0);
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    while store.import_reclaim(2).unwrap() != 0 {}
    assert_eq!(
        store.import_get(&journal.id, 2).unwrap_err().code,
        "NOT_FOUND"
    );
    assert_eq!(store.bytes(&original.id, 0, input.len(), 2).unwrap(), input);
    for id in &original.record_ids {
        assert_eq!(
            store.rdf_inspect(id, 2).unwrap()["state"].as_str(),
            Some("complete")
        );
    }
    store.expire(31 * DAY).unwrap();
    assert_eq!(
        store.status(31 * DAY).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
    assert_eq!(
        store.status(31 * DAY).unwrap()["unique_json_projections"].as_str(),
        Some("0")
    );
}

#[test]
fn import_publication_refuses_missing_prepared_payload_without_publishing_a_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let input = b"one\n";
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(input).as_bytes())
    );
    let store = Store::open(dir.path(), config()).unwrap();
    let journal = store
        .import_begin(
            "broken-prepared",
            "copy",
            "log",
            input.len() as u64,
            &digest,
            0,
        )
        .unwrap();
    store.import_append(&journal.id, 0, input, 0).unwrap();
    store.import_seal(&journal.id, 0).unwrap();
    store.import_prepare_layout(&journal.id, 0).unwrap();
    let progress = store.import_prepare_record(&journal.id, 0, 0).unwrap();
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    db.delete_cf(
        db.cf_handle("payload").unwrap(),
        digest.strip_prefix("blake3:").unwrap(),
    )
    .unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.import_publish(&journal.id, 1).unwrap_err().code,
        "STORAGE_FAILURE"
    );
    assert!(store.artifacts(1).unwrap().is_empty());
    assert!(store.records(1).unwrap().is_empty());
    assert_eq!(
        store.artifact(&progress.artifact_id).unwrap_err().code,
        "NOT_FOUND"
    );
    assert_eq!(
        store.import_bytes(&journal.id, 0, input.len(), 1).unwrap(),
        input
    );
    store.import_cancel(&journal.id, 1).unwrap();
    while store.import_reclaim(1).unwrap() != 0 {}
    assert_eq!(
        store.status(1).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
}

#[test]
fn exhausted_chunk_cleanup_budget_defers_later_native_preparation_retirement() {
    use sparrow_core::IMPORT_CHUNK_BYTES;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let block = vec![b'x'; IMPORT_CHUNK_BYTES];
    let mut hash = purrdf_hash::blake3::Hasher::new();
    for _ in 0..130 {
        hash.update(&block);
    }
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(hash.finalize().as_bytes())
    );
    let mut journals = Vec::new();
    for request in ["budget-first", "budget-second"] {
        let journal = store
            .import_begin(
                request,
                "copy",
                "log",
                130 * IMPORT_CHUNK_BYTES as u64,
                &digest,
                0,
            )
            .unwrap();
        for index in 0..130 {
            store
                .import_append(&journal.id, index * IMPORT_CHUNK_BYTES as u64, &block, 0)
                .unwrap();
        }
        journals.push(journal);
    }
    journals.sort_by(|a, b| a.id.cmp(&b.id));
    let later = &journals[1];
    store.import_seal(&later.id, 0).unwrap();
    store.import_prepare_layout(&later.id, 0).unwrap();
    store.import_prepare_record(&later.id, 0, 0).unwrap();
    for journal in &journals {
        store.import_cancel(&journal.id, 0).unwrap();
    }
    // The first journal consumes the whole call's quota. The later private
    // record cannot consume unit 129 or underflow remaining cleanup capacity.
    assert_eq!(store.import_reclaim(0).unwrap(), 128);
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(store.import_reclaim(0).unwrap(), 128);
    assert_eq!(store.import_reclaim(0).unwrap(), 6);
    assert_eq!(store.import_reclaim(0).unwrap(), 0);
    assert_eq!(
        store.status(0).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
}

#[test]
fn native_import_publishes_files_above_buffered_limit_without_a_whole_file_batch() {
    use sparrow_core::IMPORT_CHUNK_BYTES;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let mut input = vec![b'a'; 8 * 1024 * 1024];
    input.push(b'\n');
    input.extend_from_slice(&vec![b'b'; 8 * 1024 * 1024]);
    input.push(b'\n');
    assert_eq!(
        store
            .ingest("buffered-too-large", "copy", "log", &input, 0)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(&input).as_bytes())
    );
    let journal = store
        .import_begin(
            "large-publish",
            "copy",
            "log",
            input.len() as u64,
            &digest,
            0,
        )
        .unwrap();
    for (index, chunk) in input.chunks(IMPORT_CHUNK_BYTES).enumerate() {
        store
            .import_append(&journal.id, (index * IMPORT_CHUNK_BYTES) as u64, chunk, 0)
            .unwrap();
    }
    store.import_seal(&journal.id, 0).unwrap();
    store.import_prepare_layout(&journal.id, 0).unwrap();
    store.import_prepare_record(&journal.id, 0, 1).unwrap();
    assert!(store.records(1).unwrap().is_empty());
    store.import_prepare_record(&journal.id, 1, 2).unwrap();
    let artifact = store.import_publish(&journal.id, 3).unwrap();
    assert_eq!(artifact.byte_length, input.len() as u64);
    assert_eq!(artifact.payload_id, digest);
    assert_eq!(artifact.record_ids.len(), 2);
    let mut reconstructed = purrdf_hash::blake3::Hasher::new();
    for (index, expected) in input.chunks(1024 * 1024).enumerate() {
        let bytes = store
            .bytes(
                &artifact.id,
                (index * 1024 * 1024) as u64,
                expected.len(),
                3,
            )
            .unwrap();
        assert_eq!(bytes, expected);
        reconstructed.update(&bytes);
    }
    assert_eq!(
        format!(
            "blake3:{}",
            purrdf_hash::hex::encode(reconstructed.finalize().as_bytes())
        ),
        digest
    );
    let second = store.record(&artifact.record_ids[1]).unwrap();
    assert_eq!(second.byte_start, 8 * 1024 * 1024 + 1);
    assert_eq!(
        store.bytes(&second.id, 8 * 1024 * 1024 - 1, 2, 3).unwrap(),
        b"b\n"
    );
    let scope = sparrow_core::QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: Some("copy".into()),
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 10,
        limit: 10,
        scan_budget: 100,
    };
    let query = store.text_query(&scope, "a", 3).unwrap();
    assert_eq!(
        query["text_coverage"]["failed_or_incomplete"].as_str(),
        Some("2")
    );
    let pin = store
        .pin(
            "retain-large-record",
            "Case",
            std::slice::from_ref(&second.id),
            40,
            3,
        )
        .unwrap();
    store.expire(31 * DAY).unwrap();
    assert_eq!(
        store
            .bytes(&second.id, 8 * 1024 * 1024 - 1, 2, 31 * DAY)
            .unwrap(),
        b"b\n"
    );
    store.release(&pin.id).unwrap();
    store.expire(41 * DAY).unwrap();
    assert_eq!(
        store.status(41 * DAY).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
}

#[test]
fn bounded_staging_ranges_preserve_exact_binary_bytes_and_committed_frontiers() {
    use sparrow_core::IMPORT_CHUNK_BYTES;
    let dir = tempfile::tempdir().unwrap();
    let input: Vec<u8> = (0..2 * IMPORT_CHUNK_BYTES + 37)
        .map(|index| (index % 251) as u8)
        .collect();
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(&input).as_bytes())
    );
    let store = Store::open(dir.path(), config()).unwrap();
    let journal = store
        .import_begin(
            "range-copy",
            "binary-copy",
            "opaque",
            input.len() as u64,
            &digest,
            0,
        )
        .unwrap();
    store
        .import_append(&journal.id, 0, &input[..IMPORT_CHUNK_BYTES], 0)
        .unwrap();
    assert_eq!(
        store
            .import_bytes(&journal.id, (IMPORT_CHUNK_BYTES - 7) as u64, 20, 0)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    assert_eq!(
        store.import_bytes(&journal.id, 100, 77, 0).unwrap(),
        input[100..177]
    );
    for (index, chunk) in input.chunks(IMPORT_CHUNK_BYTES).enumerate().skip(1) {
        store
            .import_append(&journal.id, (index * IMPORT_CHUNK_BYTES) as u64, chunk, 0)
            .unwrap();
    }
    assert_eq!(
        store
            .import_bytes(&journal.id, (IMPORT_CHUNK_BYTES - 7) as u64, 20, 0)
            .unwrap(),
        input[IMPORT_CHUNK_BYTES - 7..IMPORT_CHUNK_BYTES + 13]
    );
    assert_eq!(
        store.import_bytes(&journal.id, 0, input.len(), 0).unwrap(),
        input
    );
    assert_eq!(
        store
            .import_bytes(&journal.id, input.len() as u64, 0, 0)
            .unwrap(),
        b""
    );
    assert_eq!(
        store
            .import_bytes(&journal.id, 0, 1024 * 1024 + 1, 0)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store
            .import_bytes(&journal.id, u64::MAX, 1, 0)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    store.import_seal(&journal.id, 0).unwrap();
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .import_bytes(&journal.id, (2 * IMPORT_CHUNK_BYTES - 2) as u64, 39, 0)
            .unwrap(),
        input[2 * IMPORT_CHUNK_BYTES - 2..]
    );
    assert!(store.records(0).unwrap().is_empty());
    store.import_cancel(&journal.id, 1).unwrap();
    assert_eq!(
        store.import_bytes(&journal.id, 0, 1, 1).unwrap_err().code,
        "IMPORT_EXPIRED"
    );
}

#[test]
fn streaming_layout_preserves_cross_chunk_delimiters_long_records_and_pages() {
    use sparrow_core::{IMPORT_CHUNK_BYTES, IMPORT_LAYOUT_PAGE_RECORDS};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let mut input = vec![b'x'; IMPORT_CHUNK_BYTES - 1];
    input.extend_from_slice(b"\r\n"); // CR and LF cross a staging chunk boundary.
    for _ in 0..IMPORT_LAYOUT_PAGE_RECORDS {
        input.extend_from_slice(b"\n");
    }
    input.extend_from_slice(&vec![0xff; 130 * IMPORT_CHUNK_BYTES + 3]);
    input.extend_from_slice(b"\0unterminated");
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(&input).as_bytes())
    );
    let journal = store
        .import_begin(
            "layout",
            "copied-bin",
            "jsonl",
            input.len() as u64,
            &digest,
            0,
        )
        .unwrap();
    assert_eq!(
        store
            .import_prepare_layout(&journal.id, 0)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    for (index, chunk) in input.chunks(IMPORT_CHUNK_BYTES).enumerate() {
        store
            .import_append(&journal.id, (index * IMPORT_CHUNK_BYTES) as u64, chunk, 0)
            .unwrap();
    }
    store.import_seal(&journal.id, 0).unwrap();
    let ready = store.import_prepare_layout(&journal.id, 0).unwrap();
    assert!(ready.layout_complete);
    assert_eq!(ready.layout_records, 130);
    assert_eq!(store.import_prepare_layout(&journal.id, 0).unwrap(), ready);
    let first = store.import_layout_page(&journal.id, 0, 0).unwrap();
    assert_eq!(first.records.len(), IMPORT_LAYOUT_PAGE_RECORDS);
    assert_eq!(first.next_index, Some(128));
    assert_eq!(first.records[0].byte_end, IMPORT_CHUNK_BYTES as u64 + 1);
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    let last = store.import_layout_page(&journal.id, 128, 0).unwrap();
    assert_eq!(last.records.len(), 2);
    assert_eq!(last.next_index, None);
    assert_eq!(last.total_records, 130);
    let mut offset = 0;
    for row in first.records.iter().chain(&last.records) {
        assert_eq!(row.byte_start, offset);
        let bytes = &input[row.byte_start as usize..row.byte_end as usize];
        assert_eq!(
            row.payload_id,
            format!(
                "blake3:{}",
                purrdf_hash::hex::encode(purrdf_hash::blake3::hash(bytes).as_bytes())
            )
        );
        offset = row.byte_end;
    }
    assert_eq!(offset, input.len() as u64);
    assert_eq!(
        store
            .import_layout_page(&journal.id, 1, 0)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    assert_eq!(
        store
            .import_layout_page(&journal.id, 256, 0)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    assert!(store.records(0).unwrap().is_empty());
    store.import_cancel(&journal.id, 1).unwrap();
    assert_eq!(store.import_reclaim(1).unwrap(), 128);
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.import_reclaim(1).unwrap(),
        input.len().div_ceil(IMPORT_CHUNK_BYTES) as u64 + 2 - 128
    );
    assert_eq!(
        store.import_get(&journal.id, 1).unwrap_err().code,
        "NOT_FOUND"
    );
}

#[test]
fn streaming_layout_record_limits_leave_recoverable_private_pages() {
    use sparrow_core::IMPORT_LAYOUT_PAGE_RECORDS;
    let dir = tempfile::tempdir().unwrap();
    let mut limited = config();
    limited.max_records = IMPORT_LAYOUT_PAGE_RECORDS;
    let store = Store::open(dir.path(), limited).unwrap();
    let input = vec![b'\n'; IMPORT_LAYOUT_PAGE_RECORDS + 1];
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(&input).as_bytes())
    );
    let journal = store
        .import_begin(
            "over-limit",
            "copied-log",
            "log",
            input.len() as u64,
            &digest,
            0,
        )
        .unwrap();
    store.import_append(&journal.id, 0, &input, 0).unwrap();
    store.import_seal(&journal.id, 0).unwrap();
    assert_eq!(
        store
            .import_prepare_layout(&journal.id, 0)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    let partial = store.import_get(&journal.id, 0).unwrap();
    assert!(!partial.layout_complete);
    assert_eq!(partial.layout_records, 128);
    assert_eq!(
        store
            .import_layout_page(&journal.id, 0, 0)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .import_prepare_layout(&journal.id, 1)
            .unwrap()
            .layout_records,
        129
    );
    assert_eq!(
        store
            .import_layout_page(&journal.id, 128, 1)
            .unwrap()
            .records
            .len(),
        1
    );
    store.import_cancel(&journal.id, 1).unwrap();
    assert_eq!(store.import_reclaim(1).unwrap(), 3);
    let empty_digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(b"").as_bytes())
    );
    for format in ["log", "jsonl", "json", "markdown", "opaque"] {
        let journal = store
            .import_begin(format, "empty", format, 0, &empty_digest, 0)
            .unwrap();
        store.import_seal(&journal.id, 0).unwrap();
        let manifest = store.import_prepare_layout(&journal.id, 0).unwrap();
        let count = u64::from(!["log", "jsonl"].contains(&format));
        assert_eq!(manifest.layout_records, count);
        assert_eq!(
            store
                .import_layout_page(&journal.id, 0, 0)
                .unwrap()
                .records
                .len() as u64,
            count
        );
    }
}

#[test]
fn broken_staging_layouts_refuse_reads_and_remain_reclaimable() {
    let dir = tempfile::tempdir().unwrap();
    let input = b"one\ntwo\n";
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(input).as_bytes())
    );
    let store = Store::open(dir.path(), config()).unwrap();
    let journal = store
        .import_begin(
            "broken-layout",
            "copy",
            "log",
            input.len() as u64,
            &digest,
            0,
        )
        .unwrap();
    store.import_append(&journal.id, 0, input, 0).unwrap();
    store.import_seal(&journal.id, 0).unwrap();
    store.import_prepare_layout(&journal.id, 0).unwrap();
    drop(store);
    let key = format!("import-layout:{}:{:020}", journal.id, 0);
    for remove in [false, true] {
        let db = catalog_db(dir.path()).unwrap();
        if remove {
            db.delete(&key).unwrap();
        } else {
            db.put(&key, b"invalid layout").unwrap();
        }
        drop(db);
        let store = Store::open(dir.path(), config()).unwrap();
        assert_eq!(
            store
                .import_layout_page(&journal.id, 0, 0)
                .unwrap_err()
                .code,
            "STORAGE_FAILURE"
        );
        assert!(store.records(0).unwrap().is_empty());
    }
    let store = Store::open(dir.path(), config()).unwrap();
    store.import_cancel(&journal.id, 1).unwrap();
    assert_eq!(store.import_reclaim(1).unwrap(), 2);
}

#[test]
fn streaming_journal_recovers_after_owned_writer_sigkill() {
    use sparrow_core::IMPORT_CHUNK_BYTES;
    use std::io::{BufRead, Write};
    use std::process::{Command, Stdio};
    use std::time::Duration;
    let mut input = vec![b'z'; IMPORT_CHUNK_BYTES];
    input.extend_from_slice(b"final\r\n");
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(&input).as_bytes())
    );
    if let Some(path) = std::env::var_os("SPARROW_TEST_IMPORT_CRASH_STORE") {
        let store = Store::open(std::path::PathBuf::from(path), config()).unwrap();
        let journal = store
            .import_begin(
                "crash-copy",
                "copied-log",
                "log",
                input.len() as u64,
                &digest,
                0,
            )
            .unwrap();
        store
            .import_append(&journal.id, 0, &input[..IMPORT_CHUNK_BYTES], 0)
            .unwrap();
        writeln!(std::io::stdout(), "SPARROW_IMPORT_READY").unwrap();
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "streaming_journal_recovers_after_owned_writer_sigkill",
            "--nocapture",
        ])
        .env("SPARROW_TEST_IMPORT_CRASH_STORE", dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        let ready = std::io::BufReader::new(stdout)
            .lines()
            .take(8)
            .any(|line| line.is_ok_and(|line| line == "SPARROW_IMPORT_READY"));
        let _ = sender.send(ready);
    });
    let ready = receiver.recv_timeout(Duration::from_secs(10));
    // Always terminate/reap only this owned test child, including readiness
    // failure, before assertions. No store destructor runs in the killed writer.
    let killed = child.kill();
    let exit = child.wait().unwrap();
    reader.join().unwrap();
    assert!(ready.unwrap());
    killed.unwrap();
    assert!(!exit.success());
    let store = Store::open(dir.path(), config()).unwrap();
    let journal = store
        .import_begin(
            "crash-copy",
            "copied-log",
            "log",
            input.len() as u64,
            &digest,
            1,
        )
        .unwrap();
    assert_eq!(journal.received_bytes, IMPORT_CHUNK_BYTES as u64);
    assert_eq!(
        store.import_read_chunk(&journal.id, 0, 1).unwrap(),
        input[..IMPORT_CHUNK_BYTES]
    );
    store
        .import_append(
            &journal.id,
            IMPORT_CHUNK_BYTES as u64,
            &input[IMPORT_CHUNK_BYTES..],
            1,
        )
        .unwrap();
    assert!(store.import_seal(&journal.id, 1).unwrap().sealed);
    assert_eq!(store.status(1).unwrap()["live_records"].as_str(), Some("0"));
}

#[test]
fn streaming_journal_recovers_exact_offsets_and_never_publishes_partial_evidence() {
    use sparrow_core::IMPORT_CHUNK_BYTES;
    let dir = tempfile::tempdir().unwrap();
    let mut input = vec![b'x'; IMPORT_CHUNK_BYTES];
    input.extend_from_slice(b"\r\n\0\xffunfinished");
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(&input).as_bytes())
    );
    let store = Store::open(dir.path(), config()).unwrap();
    let journal = store
        .import_begin(
            "copy",
            "copied-jsonl",
            "jsonl",
            input.len() as u64,
            &digest,
            100,
        )
        .unwrap();
    let id = journal.id.clone();
    assert_eq!(store.import_seal(&id, 100).unwrap_err().code, "CONFLICT");
    assert_eq!(
        store
            .import_append(
                &id,
                IMPORT_CHUNK_BYTES as u64,
                &input[IMPORT_CHUNK_BYTES..],
                100
            )
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    let first = store
        .import_append(&id, 0, &input[..IMPORT_CHUNK_BYTES], 101)
        .unwrap();
    assert_eq!(first.received_bytes, IMPORT_CHUNK_BYTES as u64);
    assert_eq!(
        store.status(101).unwrap()["live_records"].as_str(),
        Some("0")
    );
    assert_eq!(store.artifact(&id).unwrap_err().code, "INVALID_ARGUMENT");
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .import_begin(
                "copy",
                "copied-jsonl",
                "jsonl",
                input.len() as u64,
                &digest,
                200
            )
            .unwrap(),
        first
    );
    assert_eq!(store.import_get(&id, 200).unwrap(), first);
    assert_eq!(
        store
            .import_append(&id, 0, &input[..IMPORT_CHUNK_BYTES], 200)
            .unwrap(),
        first
    );
    let mut altered = input[..IMPORT_CHUNK_BYTES].to_vec();
    altered[0] = b'y';
    assert_eq!(
        store.import_append(&id, 0, &altered, 200).unwrap_err().code,
        "CONFLICT"
    );
    assert_eq!(
        store
            .import_begin(
                "copy",
                "other-source",
                "jsonl",
                input.len() as u64,
                &digest,
                200
            )
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    store
        .import_append(
            &id,
            IMPORT_CHUNK_BYTES as u64,
            &input[IMPORT_CHUNK_BYTES..],
            200,
        )
        .unwrap();
    let sealed = store.import_seal(&id, 201).unwrap();
    assert!(sealed.sealed);
    assert_eq!(sealed.expires_at, journal.expires_at);
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    let mut reconstructed = store.import_read_chunk(&id, 0, 202).unwrap();
    reconstructed.extend(
        store
            .import_read_chunk(&id, IMPORT_CHUNK_BYTES as u64, 202)
            .unwrap(),
    );
    assert_eq!(reconstructed, input);
    assert_eq!(store.import_seal(&id, 202).unwrap(), sealed);
    assert_eq!(
        store.import_get(&id, sealed.expires_at).unwrap_err().code,
        "IMPORT_EXPIRED"
    );
    assert_eq!(store.import_reclaim(sealed.expires_at).unwrap(), 2);
    assert_eq!(
        store.import_get(&id, sealed.expires_at).unwrap_err().code,
        "NOT_FOUND"
    );
    assert_eq!(
        store.status(202).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
}

#[test]
fn streaming_journal_bounds_recovery_metadata_and_reclaims_in_resumable_batches() {
    use sparrow_core::IMPORT_CHUNK_BYTES;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let block = vec![b'a'; IMPORT_CHUNK_BYTES];
    let mut hash = purrdf_hash::blake3::Hasher::new();
    for _ in 0..130 {
        hash.update(&block);
    }
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(hash.finalize().as_bytes())
    );
    let journal = store
        .import_begin(
            "large",
            "copy",
            "opaque",
            130 * IMPORT_CHUNK_BYTES as u64,
            &digest,
            0,
        )
        .unwrap();
    for index in 0..130 {
        store
            .import_append(&journal.id, index * IMPORT_CHUNK_BYTES as u64, &block, 0)
            .unwrap();
    }
    assert!(store.import_seal(&journal.id, 0).unwrap().sealed);
    store.import_cancel(&journal.id, 10).unwrap();
    assert_eq!(
        store
            .import_read_chunk(&journal.id, 0, 10)
            .unwrap_err()
            .code,
        "IMPORT_EXPIRED"
    );
    assert_eq!(store.import_reclaim(10).unwrap(), 128);
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(store.import_reclaim(10).unwrap(), 2);
    assert_eq!(store.import_reclaim(10).unwrap(), 0);
    let empty_digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(b"").as_bytes())
    );
    for index in 0..16 {
        store
            .import_begin(
                &format!("empty-{index}"),
                "copy",
                "opaque",
                0,
                &empty_digest,
                20,
            )
            .unwrap();
    }
    assert_eq!(
        store
            .import_begin("overflow", "copy", "opaque", 0, &empty_digest, 20)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store
            .import_begin("invalid", "copy", "opaque", 0, "blake3:123", 20)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    let empty = store
        .import_begin("empty-0", "copy", "opaque", 0, &empty_digest, 20)
        .unwrap();
    assert!(store.import_seal(&empty.id, 20).unwrap().sealed);
    let wrong = store
        .import_begin("empty-1", "copy", "opaque", 0, &empty_digest, 20)
        .unwrap();
    // A full but incorrect payload cannot be sealed or quietly accepted.
    store.import_cancel(&wrong.id, 20).unwrap();
    store.import_reclaim(20).unwrap();
    let wrong = store
        .import_begin("wrong", "copy", "opaque", 1, &empty_digest, 20)
        .unwrap();
    store.import_append(&wrong.id, 0, b"x", 20).unwrap();
    assert_eq!(
        store.import_seal(&wrong.id, 20).unwrap_err().code,
        "CONFLICT"
    );
    assert!(!store.import_get(&wrong.id, 20).unwrap().sealed);
    store.expire(20 + 2 * DAY).unwrap();
    assert_eq!(
        store.import_get(&empty.id, 20 + 2 * DAY).unwrap_err().code,
        "NOT_FOUND"
    );
}

#[test]
fn pin_pages_capture_revision_and_members_without_following_name_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest("pin-page-input", "source", "log", b"one\ntwo\nthree\n", 0)
        .unwrap();
    let pin = store
        .pin("page-create", "Case", &artifact.record_ids, 40, 0)
        .unwrap();
    let mut query = PinRead {
        selector: PinSelector::Name("Case".into()),
        page_size: 1,
        ..Default::default()
    };
    let first = store.pin_read(&query, 10).unwrap();
    assert_eq!(first.total_items, 3);
    assert_eq!(first.as_of, 10);
    assert_eq!(first.lease_expires_at, 60010);
    let other = store
        .ingest("pin-page-other", "source", "log", b"four\n", 20)
        .unwrap();
    store
        .pin("page-add", "Case", &other.record_ids, 1, 20)
        .unwrap();
    store
        .renew_pin("page-extend", &pin.id, 50, Some(2), 20)
        .unwrap();
    store.release_retry("page-release", &pin.id, 20).unwrap();
    let replacement = store
        .pin("page-replacement", "Case", &other.record_ids, 60, 20)
        .unwrap();
    let current = store
        .pin_read(
            &PinRead {
                selector: query.selector.clone(),
                ..Default::default()
            },
            20,
        )
        .unwrap();
    assert!(
        matches!(current.body, PinPageBody::Members { pin: ref current, .. } if current.id == replacement.id)
    );
    let mut captured = Vec::new();
    let mut page = first.clone();
    loop {
        let PinPageBody::Members {
            pin: summary,
            members,
        } = &page.body
        else {
            panic!("member page")
        };
        assert_eq!(summary.id, pin.id);
        assert_eq!(summary.revision, 1);
        assert_eq!(summary.direct_member_count, 3);
        assert_eq!(summary.expires_at, 40 * DAY);
        assert_eq!(page.snapshot_id, first.snapshot_id);
        assert_eq!(page.as_of, 10);
        captured.extend(members.clone());
        let Some(cursor) = page.next_cursor.clone() else {
            break;
        };
        query.cursor = Some(cursor);
        page = store.pin_read(&query, 30).unwrap();
        assert_eq!(store.clone().pin_read(&query, 40).unwrap(), page);
    }
    let mut expected = artifact.record_ids.clone();
    expected.sort();
    assert_eq!(captured, expected);
    let mut changed = query.clone();
    changed.selector = PinSelector::Id(pin.id.clone());
    assert_eq!(store.pin_read(&changed, 40).unwrap_err().code, "CONFLICT");
    changed = query.clone();
    changed.page_size = 2;
    assert_eq!(store.pin_read(&changed, 40).unwrap_err().code, "CONFLICT");
    changed = query.clone();
    changed.scan_budget += 1;
    assert_eq!(store.pin_read(&changed, 40).unwrap_err().code, "CONFLICT");
    assert_eq!(
        store
            .pin_read(&query, first.lease_expires_at)
            .unwrap_err()
            .code,
        "RESULT_EXPIRED"
    );
    assert_eq!(
        store.status(first.lease_expires_at).unwrap()["pin_read_handles"].as_str(),
        Some("0")
    );
    // Catalog lookup itself is exact and names are case-sensitive.
    assert_eq!(
        store
            .pin_read(
                &PinRead {
                    selector: PinSelector::Name("case".into()),
                    ..Default::default()
                },
                40
            )
            .unwrap_err()
            .code,
        "PIN_NOT_ACTIVE"
    );
    assert_eq!(
        store
            .pin_read(
                &PinRead {
                    selector: PinSelector::Id(pin.id),
                    ..Default::default()
                },
                40
            )
            .unwrap_err()
            .code,
        "PIN_NOT_ACTIVE"
    );
}

#[test]
fn collection_pages_are_stable_and_do_not_hold_evidence_or_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest("page-no-evidence-lease", "source", "log", b"event\n", 0)
        .unwrap();
    let pins = ["left", "middle", "right"]
        .map(|name| store.pin(name, name, &artifact.record_ids, 40, 0).unwrap());
    let now = 30 * DAY - 1000;
    let mut query = PinRead {
        page_size: 1,
        ..Default::default()
    };
    let first = store.pin_read(&query, now).unwrap();
    assert_eq!(first.total_items, 3);
    for pin in &pins {
        store.release(&pin.id).unwrap();
    }
    store.expire(30 * DAY).unwrap();
    assert!(
        store
            .bytes(&artifact.record_ids[0], 0, 6, 30 * DAY)
            .is_err()
    );
    let mut ids = Vec::new();
    let mut page = first.clone();
    loop {
        let PinPageBody::Collections(rows) = &page.body else {
            panic!("collection page")
        };
        ids.extend(rows.iter().map(|row| row.id.clone()));
        let Some(cursor) = page.next_cursor.clone() else {
            break;
        };
        query.cursor = Some(cursor);
        page = store.pin_read(&query, 30 * DAY).unwrap();
    }
    let mut expected = pins.iter().map(|pin| pin.id.clone()).collect::<Vec<_>>();
    expected.sort();
    assert_eq!(ids, expected);
    assert_eq!(
        store
            .pin_read(&PinRead::default(), 30 * DAY)
            .unwrap()
            .total_items,
        0
    );
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.pin_read(&query, 30 * DAY).unwrap_err().code,
        "RESULT_EXPIRED"
    );
}

#[test]
fn pin_capture_limits_refuse_prefixes_and_expired_handles_release_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let mut limits = config();
    limits.max_pin_members = 200;
    let store = Store::open(dir.path(), limits).unwrap();
    let artifact = store
        .ingest("capture-limit", "source", "log", b"one\ntwo\n", 0)
        .unwrap();
    let pins =
        ["a", "b", "c"].map(|name| store.pin(name, name, &artifact.record_ids, 40, 0).unwrap());
    assert_eq!(
        store
            .pin_read(
                &PinRead {
                    scan_budget: 1,
                    ..Default::default()
                },
                0
            )
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store.status(0).unwrap()["pin_read_handles"].as_str(),
        Some("0")
    );
    for (page_size, scan_budget) in [(0, 1), (1001, 1), (1, 0), (1, 100001)] {
        assert_eq!(
            store
                .pin_read(
                    &PinRead {
                        page_size,
                        scan_budget,
                        ..Default::default()
                    },
                    0
                )
                .unwrap_err()
                .code,
            "INVALID_ARGUMENT"
        );
    }
    let query = PinRead {
        selector: PinSelector::Id(pins[0].id.clone()),
        page_size: 1,
        ..Default::default()
    };
    let mut first = None;
    for _ in 0..8 {
        first = Some(store.pin_read(&query, 0).unwrap());
    }
    assert_eq!(
        store.pin_read(&query, 0).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store.status(0).unwrap()["pin_read_handles"].as_str(),
        Some("8")
    );
    let fresh = store.pin_read(&query, 60000).unwrap();
    assert_eq!(
        store.status(60000).unwrap()["pin_read_handles"].as_str(),
        Some("1")
    );
    let expired = PinRead {
        cursor: first.unwrap().next_cursor,
        ..query.clone()
    };
    assert_eq!(
        store.pin_read(&expired, 60000).unwrap_err().code,
        "RESULT_EXPIRED"
    );
    let malformed = PinRead {
        cursor: Some(format!("{}/+1", fresh.snapshot_id)),
        ..query
    };
    assert_eq!(
        store.pin_read(&malformed, 60000).unwrap_err().code,
        "INVALID_ARGUMENT"
    );
    let many = store
        .ingest(
            "many-pin-members",
            "source",
            "log",
            &b"x\n".repeat(150),
            60000,
        )
        .unwrap();
    let pin = store
        .pin("many-pin-create", "many", &many.record_ids, 1, 60000)
        .unwrap();
    let query = PinRead {
        selector: PinSelector::Id(pin.id),
        page_size: 1000,
        ..Default::default()
    };
    let page = store.pin_read(&query, 60000).unwrap();
    let PinPageBody::Members { members, .. } = &page.body else {
        panic!("member page")
    };
    assert!(members.len() < 150);
    assert!(members.len() * 2048 + 4096 <= sparrow_core::PIN_READ_PAGE_BYTES);
    assert_eq!(page.total_items, 150);
    let last = store
        .pin_read(
            &PinRead {
                cursor: page.next_cursor,
                ..query
            },
            60001,
        )
        .unwrap();
    let PinPageBody::Members {
        members: remaining, ..
    } = &last.body
    else {
        panic!("member page")
    };
    assert_eq!(members.len() + remaining.len(), 150);
    assert!(last.next_cursor.is_none());
}

#[test]
fn malformed_pin_catalog_refuses_complete_captures() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest("corrupt-pin-input", "source", "log", b"event\n", 0)
        .unwrap();
    let pin = store
        .pin("valid-pin", "valid", &artifact.record_ids, 40, 0)
        .unwrap();
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    let bytes = db.get(&pin.id).unwrap().unwrap();
    let changed = std::str::from_utf8(&bytes)
        .unwrap()
        .replace(&pin.id, "pin:different");
    db.put(&pin.id, changed).unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    for selector in [
        PinSelector::Active,
        PinSelector::Id(pin.id),
        PinSelector::Name(pin.name),
    ] {
        assert_eq!(
            store
                .pin_read(
                    &PinRead {
                        selector,
                        ..Default::default()
                    },
                    0
                )
                .unwrap_err()
                .code,
            "STORAGE_FAILURE"
        );
    }
}

#[test]
fn pin_renewal_is_durable_idempotent_revision_guarded_and_never_follows_names() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest("renew-input", "source", "log", b"one\ntwo\n", 0)
        .unwrap();
    let targets = vec![artifact.record_ids[0].clone()];
    let initial = store.pin("renew-create", "case", &targets, 40, 0).unwrap();
    let shorter = store
        .renew_pin("renew-short", &initial.id, 1, Some(1), 1)
        .unwrap();
    assert_eq!(shorter.expires_at, 40 * DAY);
    assert_eq!(shorter.revision, 2);
    assert_eq!(shorter.members, initial.members);
    assert_eq!(
        store
            .renew_pin("renew-stale", &initial.id, 50, Some(1), 2)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    let extended = store
        .renew_pin("renew-extend", &initial.id, 50, Some(2), 2)
        .unwrap();
    assert_eq!(extended.expires_at, 50 * DAY + 2);
    assert_eq!(extended.revision, 3);
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    let replay = store
        .renew_pin("renew-extend", &initial.id, 50, Some(2), DAY)
        .unwrap();
    assert_eq!(replay.expires_at, extended.expires_at);
    assert_eq!(replay.revision, extended.revision);
    for (days, revision) in [(51, Some(2)), (50, None), (50, Some(3))] {
        assert_eq!(
            store
                .renew_pin("renew-extend", &initial.id, days, revision, DAY)
                .unwrap_err()
                .code,
            "CONFLICT"
        );
    }
    store
        .release_retry("renew-release", &initial.id, DAY)
        .unwrap();
    let replacement = store
        .pin("renew-replacement", "case", &targets, 60, DAY)
        .unwrap();
    let replay = store
        .renew_pin("renew-extend", &initial.id, 50, Some(2), DAY + 1)
        .unwrap();
    assert_eq!(replay.id, initial.id);
    assert_eq!(replay.revision, 3);
    assert_eq!(
        store
            .renew_pin("renew-old", &initial.id, 90, None, DAY + 1)
            .unwrap_err()
            .code,
        "PIN_NOT_ACTIVE"
    );
    assert_eq!(
        store
            .renew_pin("renew-extend", &replacement.id, 50, Some(2), DAY + 1)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    let active = store.pins(DAY + 1).unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].id, replacement.id);
    assert_eq!(active[0].revision, 1);

    // A released receipt must not resurrect its ID after its retry window closes.
    assert_eq!(
        store
            .renew_pin("renew-extend", &initial.id, 50, Some(2), 2 * DAY + 2)
            .unwrap_err()
            .code,
        "PIN_NOT_ACTIVE"
    );
    store.expire(31 * DAY).unwrap();
    assert_eq!(
        store
            .bytes(&artifact.record_ids[0], 0, 4, 31 * DAY)
            .unwrap(),
        b"one\n"
    );
    assert!(
        store
            .bytes(&artifact.record_ids[1], 0, 4, 31 * DAY)
            .is_err()
    );
}

#[test]
fn pin_renewal_refuses_deadlines_limits_and_inactive_collections_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest("renew-validation", "source", "log", b"event\n", 0)
        .unwrap();
    let pin = store
        .pin(
            "short-pin",
            "short",
            std::slice::from_ref(&artifact.id),
            1,
            0,
        )
        .unwrap();
    for (request, id, days, now) in [
        ("", pin.id.as_str(), 1, 0),
        ("invalid-kind", "record:wrong", 1, 0),
        ("empty-id", "pin:", 1, 0),
        ("zero", pin.id.as_str(), 0, 0),
        ("overflow-duration", pin.id.as_str(), u64::MAX, 0),
        (
            "unrenderable-duration",
            pin.id.as_str(),
            u64::from(u32::MAX),
            0,
        ),
        (
            "unrenderable-year",
            pin.id.as_str(),
            1,
            sparrow_core::parse_instant("9999-12-31T00:00:00Z").unwrap(),
        ),
        ("overflow-now", pin.id.as_str(), 1, u64::MAX),
    ] {
        assert_eq!(
            store
                .renew_pin(request, id, days, None, now)
                .unwrap_err()
                .code,
            "INVALID_ARGUMENT"
        );
    }
    assert_eq!(store.pins(0).unwrap()[0].revision, 1);
    assert_eq!(
        store
            .pin(
                "unrenderable-create",
                "invalid-deadline",
                std::slice::from_ref(&pin.members.iter().next().unwrap().clone()),
                u64::from(u32::MAX),
                0
            )
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    let expiring = store
        .renew_pin("expiry-replay", &pin.id, 1, None, 0)
        .unwrap();
    assert_eq!(expiring.expires_at, DAY);
    let replay = store
        .renew_pin("expiry-replay", &pin.id, 1, None, DAY)
        .unwrap();
    assert_eq!(replay.expires_at, DAY);
    assert_eq!(replay.revision, 2);
    assert!(store.pins(DAY).unwrap().is_empty());
    assert_eq!(
        store
            .renew_pin("expired", &pin.id, 10, None, DAY)
            .unwrap_err()
            .code,
        "PIN_NOT_ACTIVE"
    );
    assert_eq!(
        store
            .renew_pin("missing", "pin:unknown", 10, None, 0)
            .unwrap_err()
            .code,
        "PIN_NOT_ACTIVE"
    );
    store.expire(DAY).unwrap();
    assert_eq!(
        store
            .renew_pin("expiry-replay", &pin.id, 1, None, DAY + 1)
            .unwrap()
            .expires_at,
        DAY
    );
    assert_eq!(
        store
            .renew_pin("reclaimed", &pin.id, 10, None, DAY)
            .unwrap_err()
            .code,
        "PIN_NOT_ACTIVE"
    );
    assert!(store.pins(DAY).unwrap().is_empty());
    let two_members = store
        .pin(
            "limited-pin",
            "limited",
            &[artifact.id, artifact.record_ids[0].clone()],
            3,
            DAY,
        )
        .unwrap();
    drop(store);
    let mut limits = config();
    limits.max_pin_members = 1;
    let store = Store::open(dir.path(), limits).unwrap();
    assert_eq!(
        store
            .renew_pin("limited-renewal", &two_members.id, 10, None, DAY)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(store.pins(DAY).unwrap()[0].revision, 1);
}

#[test]
fn pin_renewal_serializes_competing_revision_guards() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest("renew-race", "source", "log", b"event\n", 0)
        .unwrap();
    let pin = store.pin("race-pin", "race", &[artifact.id], 1, 0).unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let attempts = ["race-left", "race-right"].map(|request| {
        let store = store.clone();
        let id = pin.id.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            store.renew_pin(request, &id, 10, Some(1), 1)
        })
    });
    let results = attempts.map(|thread| thread.join().unwrap());
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| result.as_ref().is_err_and(|error| error.code == "CONFLICT"))
            .count(),
        1
    );
    let active = store.pins(1).unwrap();
    assert_eq!(active[0].revision, 2);
    assert_eq!(active[0].expires_at, 10 * DAY + 1);
    assert_eq!(active[0].members, pin.members);
}

#[test]
fn text_query_uses_native_ranking_beyond_the_former_term_ceiling() {
    let dir = tempfile::tempdir().unwrap();
    let config = config();
    let text = std::iter::once("needle".to_owned())
        .chain((0..1100).map(|i| format!("probe{i}tail")))
        .collect::<Vec<_>>()
        .join(" ");
    let terms = config.text_analyzer.terms(&text).unwrap();
    assert!(
        terms
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            > 1024
    );
    let store = Store::open(dir.path(), config).unwrap();
    let artifact = store
        .ingest("many-query-terms", "source", "log", b"needle\n", 0)
        .unwrap();
    let mut request = sparrow_core::RecordRead {
        operation: sparrow_core::RecordOperation::Text,
        scope: record_page_scope("source", 100),
        query: sparrow_core::RecordQuery {
            text: Some(text),
            ..Default::default()
        },
        cursor: None,
    };
    let page = store.records_read(&request, 1).unwrap();
    let result = page.body.result().unwrap();
    assert_eq!(result["rows"].as_array().unwrap().len(), 1);
    assert_eq!(
        result["rows"][0]["record_id"].as_str(),
        Some(artifact.record_ids[0].as_str())
    );
    assert!(result["query_evidence"]["limits"]["query_terms"].is_null());
    request.query.text = Some("x".repeat(65537));
    let refusal = store.records_read(&request, 1).unwrap_err();
    assert_eq!(refusal.code, "RESOURCE_LIMIT");
    assert!(refusal.message.contains("64 KiB"));
}

#[test]
fn stored_native_queries_preserve_occurrences_scope_and_pinned_evidence() {
    use purrdf::{
        SparqlEngine, SparqlRequest, SparqlResult,
        sparql::{NativeSparqlEngine, QueryGovernors, QueryOptions},
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let a = store
        .ingest(
            "native-query-a",
            "rdf-native",
            "json",
            br#"{"message":"failed"}"#,
            0,
        )
        .unwrap();
    let b = store
        .ingest(
            "native-query-b",
            "rdf-native",
            "json",
            br#"{"message":"failed"}"#,
            1,
        )
        .unwrap();
    let unrelated = store
        .ingest(
            "native-query-other",
            "other",
            "json",
            br#"{"message":"failed"}"#,
            2,
        )
        .unwrap();
    let ids = vec![a.record_ids[0].clone(), b.record_ids[0].clone()];
    let engine = NativeSparqlEngine::new();
    let governors = QueryGovernors::METERED
        .with_fuel(1000000)
        .with_max_answers(100)
        .with_max_intermediate_cells(10000)
        .with_max_scratch_bytes(1024 * 1024)
        .with_max_remote_requests(0);
    let select = "SELECT ?a ?b WHERE { ?a <urn:sparrow:vocab:payload> ?payload . ?b <urn:sparrow:vocab:payload> ?payload }";
    let result = store
        .with_rdf_records(&ids, 3, sparrow_core::RdfReadLimits::default(), |view| {
            engine
                .query_governed_fallible_view(
                    view,
                    SparqlRequest {
                        query: select,
                        base_iri: None,
                        substitutions: &[],
                    },
                    QueryOptions::default(),
                    &governors,
                )
                .map_err(Box::new)
        })
        .unwrap()
        .unwrap();
    let (_, rows) = result.result.solutions().expect("SELECT result required");
    assert_eq!(rows.len(), 4);
    assert!(!format!("{rows:?}").contains(&unrelated.record_ids[0]));
    let retained = result.result.clone().into_solutions().unwrap();
    drop(result);
    assert_eq!(retained.as_parts().1.len(), 4);
    let stopped = store
        .with_rdf_records(&ids, 3, sparrow_core::RdfReadLimits::default(), |view| {
            engine
                .query_governed_fallible_view(
                    view,
                    SparqlRequest {
                        query: select,
                        base_iri: None,
                        substitutions: &[],
                    },
                    QueryOptions::default(),
                    &governors.clone().with_fuel(0),
                )
                .map_err(Box::new)
        })
        .unwrap();
    assert!(matches!(
        *stopped.unwrap_err(),
        purrdf::sparql::FallibleSparqlError::BudgetExhausted { .. }
    ));
    let resident = store
        .with_rdf_records(&ids, 3, Default::default(), |view| {
            purrdf::ir::dataset_from_view(view).unwrap()
        })
        .unwrap();
    for query in [
        "SELECT ?s WHERE { GRAPH ?g { ?s <urn:sparrow:vocab:sourceName> ?name } } ORDER BY ?s",
        "ASK { GRAPH ?g { ?s <urn:sparrow:vocab:sourceName> \"rdf-native\" } }",
        "CONSTRUCT { ?s <urn:sparrow:vocab:sourceName> ?name } WHERE { GRAPH ?g { ?s <urn:sparrow:vocab:sourceName> ?name } }",
        "DESCRIBE ?s WHERE { GRAPH ?g { ?s <urn:sparrow:vocab:sourceName> \"rdf-native\" } }",
        "SELECT ?s ?tag WHERE { GRAPH ?g { ?s <urn:sparrow:vocab:sourceName> ?name OPTIONAL { ?s <urn:sparrow:vocab:missing> ?unused } } BIND(CONCAT(STR(?name), \"!\") AS ?tag) FILTER(?name = \"rdf-native\") } ORDER BY ?s",
        "SELECT (COUNT(?s) AS ?count) WHERE { GRAPH ?g { ?s <urn:sparrow:vocab:sourceName> ?name } }",
        "SELECT DISTINCT ?name WHERE { { GRAPH ?g { ?s <urn:sparrow:vocab:sourceName> ?name } } UNION { GRAPH ?g { ?s <urn:sparrow:vocab:sourceName> ?name } } } ORDER BY ?name",
        "SELECT ?s ?related WHERE { GRAPH ?g { ?s <urn:sparrow:vocab:sourceName> ?name . ?s <urn:sparrow:vocab:payload>/^<urn:sparrow:vocab:payload> ?related } } ORDER BY ?s ?related",
    ] {
        let result = store
            .with_rdf_records(&ids, 3, sparrow_core::RdfReadLimits::default(), |view| {
                engine
                    .query_governed_fallible_view(
                        view,
                        SparqlRequest {
                            query,
                            base_iri: None,
                            substitutions: &[],
                        },
                        QueryOptions::default(),
                        &governors,
                    )
                    .map_err(Box::new)
            })
            .unwrap();
        let result = result.unwrap_or_else(|error| panic!("stored query {query}: {error:?}"));
        let oracle = engine
            .query(
                &resident,
                SparqlRequest {
                    query,
                    base_iri: None,
                    substitutions: &[],
                },
            )
            .unwrap();
        match oracle {
            SparqlResult::Solutions {
                variables, rows, ..
            } => {
                let (actual_variables, actual_rows) = result.result.solutions().unwrap();
                assert_eq!(actual_variables, variables);
                assert_eq!(actual_rows, rows);
            }
            SparqlResult::Boolean(answer) => assert_eq!(result.result.boolean(), Some(answer)),
            SparqlResult::Graph(graph) => assert!(purrdf::ir::datasets_isomorphic(
                result.result.graph().unwrap(),
                graph.as_ref(),
            )),
        }
    }
    let explanation = store.with_rdf_records(&ids, 3, Default::default(), |view| {
        engine.explain_query_fallible_view(view,
            "SELECT ?s WHERE { GRAPH ?g { ?s <urn:sparrow:vocab:sourceName> ?name } } ORDER BY ?s", None)
            .map_err(Box::new)
    }).unwrap().unwrap().0;
    assert!(!explanation.ledger().is_empty());
    store
        .pin(
            "native-query-pin",
            "native-selection",
            std::slice::from_ref(&ids[0]),
            40,
            3,
        )
        .unwrap();
    store.expire(31 * DAY).unwrap();
    let pinned = store
        .with_rdf_records(
            std::slice::from_ref(&ids[0]),
            31 * DAY,
            sparrow_core::RdfReadLimits::default(),
            |view| purrdf::DatasetView::quads(view).count(),
        )
        .unwrap();
    assert!(pinned > 20);
    assert_eq!(
        store
            .with_rdf_records(
                &ids,
                31 * DAY,
                sparrow_core::RdfReadLimits::default(),
                |_| ()
            )
            .unwrap_err()
            .code,
        "EVIDENCE_UNAVAILABLE"
    );
}

#[test]
fn stored_rdf_snapshot_survives_concurrent_pin_release_retirement_and_ingestion() {
    use purrdf::{DatasetView, TermValue};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let accepted = store
        .ingest(
            "snapshot-original",
            "snapshot",
            "json",
            br#"{"message":"before"}"#,
            0,
        )
        .unwrap();
    let pin = store
        .pin("snapshot-pin", "snapshot", &accepted.record_ids, 40, 0)
        .unwrap();
    let (complete, completed) = std::sync::mpsc::sync_channel(1);
    let selected = accepted.record_ids.clone();
    let (sequence, new_record) = store
        .with_rdf_records(&selected, 31 * DAY, Default::default(), |view| {
            let before = view.quads().collect::<Vec<_>>();
            let graphs = view.named_graphs().collect::<Vec<_>>();
            assert!(!before.is_empty() && !graphs.is_empty());
            assert_eq!(
                store
                    .with_rdf_records(&selected, 31 * DAY, Default::default(), |_| ())
                    .unwrap_err()
                    .code,
                "RESOURCE_LIMIT"
            );
            let old_graph = TermValue::iri(format!("urn:sparrow:graph:{}", selected[0]));
            let old_owner = view.term_id_by_value(&old_graph).unwrap().unwrap();
            let owner = store.clone();
            let mutation = std::thread::spawn(move || {
                owner.release(&pin.id).unwrap();
                owner.expire(31 * DAY).unwrap();
                let new = owner
                    .ingest(
                        "snapshot-after",
                        "snapshot",
                        "json",
                        br#"{"message":"after"}"#,
                        31 * DAY,
                    )
                    .unwrap();
                complete.send(new).unwrap();
            });
            // A lock held during query execution would prevent this writer finishing.
            let new = completed
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("query snapshot must release the writer owner");
            mutation.join().unwrap();
            assert_eq!(view.quads().collect::<Vec<_>>(), before);
            assert_eq!(view.named_graphs().collect::<Vec<_>>(), graphs);
            assert_eq!(view.term_id_by_value(&old_graph).unwrap(), Some(old_owner));
            assert!(view.resolve(old_owner).is_ok());
            assert!(
                view.term_id_by_value(&TermValue::iri(format!(
                    "urn:sparrow:graph:{}",
                    new.record_ids[0]
                )))
                .unwrap()
                .is_none()
            );
            (view.snapshot_sequence_number(), new.record_ids[0].clone())
        })
        .unwrap();
    assert!(store.record(&selected[0]).is_err());
    assert!(
        store
            .with_rdf_records(&selected, 31 * DAY, Default::default(), |_| ())
            .is_err()
    );
    let later = store
        .with_rdf_records(&[new_record], 31 * DAY, Default::default(), |view| {
            assert!(!view.quads().collect::<Vec<_>>().is_empty());
            view.snapshot_sequence_number()
        })
        .unwrap();
    assert!(later > sequence);
}

#[test]
fn artifact_pages_are_immutable_source_time_scoped_occurrence_discovery() {
    use sparrow_core::{ArtifactRead, ArtifactSelector};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let a = store
        .ingest("artifact-list-a", "source-a", "jsonl", b"{}\n{}\n", 1)
        .unwrap();
    let b = store
        .ingest("artifact-list-b", "source-a", "jsonl", b"{}\n{}\n", 2)
        .unwrap();
    store
        .ingest("artifact-list-other", "source-b", "log", b"other\n", 3)
        .unwrap();
    let mut query = ArtifactRead {
        selector: ArtifactSelector {
            source: Some("source-a".into()),
            received_from: 1,
            received_before: 3,
            ..Default::default()
        },
        page_size: 1,
        ..Default::default()
    };
    let first = store.artifact_read(&query, 10).unwrap();
    assert_eq!(first.total_items, 2);
    assert_eq!(first.body.artifacts.len(), 1);
    assert_eq!(first.as_of, 10);
    assert_eq!(first.lease_expires_at, 60_010);
    assert!(first.body.artifacts[0].complete_bytes_available);
    assert_eq!(first.body.artifacts[0].retained_records, 2);
    assert_eq!(
        first.body.artifacts[0].origin.as_ref().unwrap().byte_length,
        6
    );
    store
        .ingest("artifact-list-later", "source-a", "jsonl", b"{}\n", 2)
        .unwrap();
    query.cursor = first.next_cursor.clone();
    let second = store.artifact_read(&query, 11).unwrap();
    assert_eq!(first.snapshot_id, second.snapshot_id);
    assert_eq!(second.total_items, 2);
    assert!(second.next_cursor.is_none());
    let mut ids = vec![
        first.body.artifacts[0].id.clone(),
        second.body.artifacts[0].id.clone(),
    ];
    let mut expected = vec![a.id, b.id];
    ids.sort();
    expected.sort();
    assert_eq!(ids, expected);
    let mut changed = query.clone();
    changed.selector.received_before = 4;
    assert_eq!(
        store.artifact_read(&changed, 12).unwrap_err().code,
        "CONFLICT"
    );
    changed = query.clone();
    changed.page_size = 2;
    assert_eq!(
        store.artifact_read(&changed, 12).unwrap_err().code,
        "CONFLICT"
    );
    let other_dir = tempfile::tempdir().unwrap();
    let other = Store::open(other_dir.path(), config()).unwrap();
    assert_eq!(
        other.artifact_read(&query, 12).unwrap_err().code,
        "RESULT_EXPIRED"
    );
    assert_eq!(
        store
            .artifact_read(&query, first.lease_expires_at)
            .unwrap_err()
            .code,
        "RESULT_EXPIRED"
    );
    assert_eq!(
        store.status(first.lease_expires_at).unwrap()["artifact_read_handles"].as_str(),
        Some("0")
    );
}

#[test]
fn artifact_discovery_keeps_reclaimed_parent_unknown_and_never_invents_complete_bytes() {
    use sparrow_core::{ArtifactRead, ArtifactSelector};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest(
            "artifact-fragments",
            "fragment-source",
            "jsonl",
            b"{}\n{}\n",
            0,
        )
        .unwrap();
    let empty = store
        .ingest("artifact-empty", "empty-source", "jsonl", b"", 0)
        .unwrap();
    let record_pin = store
        .pin("fragment-pin", "fragment", &artifact.record_ids[..1], 40, 0)
        .unwrap();
    let whole_pin = store
        .pin("empty-pin", "empty", std::slice::from_ref(&empty.id), 40, 0)
        .unwrap();
    store.expire(31 * DAY).unwrap();
    assert!(store.artifact(&artifact.id).is_err());
    let mut query = ArtifactRead {
        page_size: 1,
        ..Default::default()
    };
    let first = store.artifact_read(&query, 31 * DAY).unwrap();
    assert_eq!(first.total_items, 2);
    query.cursor = first.next_cursor.clone();
    let second = store.artifact_read(&query, 31 * DAY + 1).unwrap();
    let rows = [&first.body.artifacts[0], &second.body.artifacts[0]];
    let fragment = rows.iter().find(|row| row.id == artifact.id).unwrap();
    assert!(fragment.origin.is_none());
    assert!(!fragment.complete_bytes_available);
    assert_eq!(fragment.retained_records, 1);
    let complete = rows.iter().find(|row| row.id == empty.id).unwrap();
    assert!(complete.complete_bytes_available);
    assert_eq!(complete.retained_records, 0);
    assert_eq!(complete.origin.as_ref().unwrap().byte_length, 0);
    let timed = ArtifactRead {
        selector: ArtifactSelector {
            source: Some("fragment-source".into()),
            received_from: 0,
            received_before: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(
        store.artifact_read(&timed, 31 * DAY).unwrap_err().code,
        "INDEX_NOT_READY"
    );
    let timed_empty = ArtifactRead {
        selector: ArtifactSelector {
            source: Some("empty-source".into()),
            received_from: 0,
            received_before: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(
        store
            .artifact_read(&timed_empty, 31 * DAY)
            .unwrap()
            .total_items,
        1
    );
    store.release(&record_pin.id).unwrap();
    store.release(&whole_pin.id).unwrap();
    store.expire(31 * DAY + 2).unwrap();
    assert_eq!(store.artifact_read(&query, 31 * DAY + 3).unwrap(), second);
    assert_eq!(
        store
            .artifact_read(&ArtifactRead::default(), 31 * DAY + 3)
            .unwrap()
            .total_items,
        0
    );
}

#[test]
fn artifact_capture_refuses_budget_and_corruption_without_publishing_prefixes() {
    use purrdf_lex::json::record::ToJson;
    use sparrow_core::ArtifactRead;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest("artifact-budget", "source", "json", b"{}", 0)
        .unwrap();
    let tiny = ArtifactRead {
        scan_budget: 1,
        ..Default::default()
    };
    assert_eq!(
        store.artifact_read(&tiny, 1).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store.status(1).unwrap()["artifact_read_handles"].as_str(),
        Some("0")
    );
    let mut corrupt = store.record(&artifact.record_ids[0]).unwrap().to_json();
    corrupt.as_object_mut().unwrap().insert("source", "");
    let invalid_source = purrdf_lex::json::write_compact(&corrupt);
    drop(store);
    for bytes in [b"{invalid".as_slice(), invalid_source.as_bytes()] {
        let db = catalog_db(dir.path()).unwrap();
        db.put(&artifact.record_ids[0], bytes).unwrap();
        drop(db);
        let store = Store::open(dir.path(), config()).unwrap();
        assert_eq!(
            store
                .artifact_read(&ArtifactRead::default(), 1)
                .unwrap_err()
                .code,
            "STORAGE_FAILURE"
        );
        assert!(store.properties().is_ok());
    }
}

#[test]
fn artifact_handles_are_bounded_shared_by_clones_and_preserve_full_source_labels() {
    use sparrow_core::{ArtifactRead, ArtifactSelector};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let source = "\n".repeat(512);
    for request in ["artifact-capacity-a", "artifact-capacity-b"] {
        store.ingest(request, &source, "json", b"{}", 0).unwrap();
    }
    let query = ArtifactRead {
        page_size: 1,
        selector: ArtifactSelector {
            source: Some(source.clone()),
            ..Default::default()
        },
        ..Default::default()
    };
    for _ in 0..8 {
        let page = store.clone().artifact_read(&query, 1).unwrap();
        assert_eq!(page.body.artifacts[0].source, source);
        assert!(page.next_cursor.is_some());
    }
    assert_eq!(
        store.clone().artifact_read(&query, 1).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store.status(1).unwrap()["artifact_read_handles"].as_str(),
        Some("8")
    );
    store.expire(60_001).unwrap();
    assert_eq!(
        store.status(60_001).unwrap()["artifact_read_handles"].as_str(),
        Some("0")
    );
    assert!(store.artifact_read(&query, 60_001).is_ok());
}

#[test]
fn artifact_selection_shares_record_membership_and_preserves_recordless_artifacts() {
    use sparrow_core::{ArtifactRead, ArtifactSelector, QueryScope, QuerySelection, RecordQuery};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let a = store
        .ingest("artifact-scope-a", "a", "jsonl", b"{}\n{}\n", 0)
        .unwrap();
    let b = store
        .ingest("artifact-scope-b", "b", "jsonl", b"{}\n{}\n", 0)
        .unwrap();
    let empty = store
        .ingest("artifact-scope-empty", "empty", "jsonl", b"", 0)
        .unwrap();
    let pin = store
        .pin(
            "artifact-scope-pin",
            "case",
            &[a.record_ids[0].clone(), b.id.clone(), empty.id.clone()],
            2,
            1,
        )
        .unwrap();
    let selection = QuerySelection::Pin {
        id: pin.id.clone(),
        expected_revision: Some(pin.revision),
    };
    let mut query = ArtifactRead {
        selector: ArtifactSelector {
            selection: selection.clone(),
            ..Default::default()
        },
        ..Default::default()
    };
    let page = store.artifact_read(&query, 2).unwrap();
    assert_eq!(page.total_items, 3);
    for (id, count) in [(&a.id, 1), (&b.id, 2), (&empty.id, 0)] {
        let row = page
            .body
            .artifacts
            .iter()
            .find(|row| &row.id == id)
            .unwrap();
        assert_eq!(row.retained_records, count);
        assert!(row.complete_bytes_available);
    }
    let scope = QueryScope {
        selection,
        source: None,
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 100,
        limit: 100,
        scan_budget: 1000,
    };
    assert_eq!(
        store
            .records_query(&scope, &RecordQuery::default(), 2)
            .unwrap()["rows"]
            .as_array()
            .unwrap()
            .len(),
        page.body
            .artifacts
            .iter()
            .map(|row| row.retained_records)
            .sum::<usize>()
    );
    query.selector.selection = QuerySelection::Evidence(vec![
        a.record_ids[0].clone(),
        a.id.clone(),
        a.record_ids[0].clone(),
    ]);
    let selected = store.artifact_read(&query, 2).unwrap();
    assert_eq!(selected.total_items, 1);
    assert_eq!(selected.body.artifacts[0].retained_records, 2);
    assert_eq!(
        selected.body.artifacts[0]
            .origin
            .as_ref()
            .unwrap()
            .original_records,
        2
    );
}

#[test]
fn artifact_selection_validates_references_before_filters_and_refuses_without_prefixes() {
    use sparrow_core::{ArtifactRead, ArtifactSelector, QuerySelection};
    const DAY: u64 = 86400000;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let a = store
        .ingest("artifact-scope-old", "source", "jsonl", b"{}\n{}\n", 0)
        .unwrap();
    let pin = store
        .pin(
            "artifact-scope-record-pin",
            "record",
            &a.record_ids[..1],
            40,
            0,
        )
        .unwrap();
    let mut query = ArtifactRead {
        selector: ArtifactSelector {
            source: Some("excluded".into()),
            selection: QuerySelection::Evidence(vec![
                a.record_ids[0].clone(),
                "record:missing".into(),
            ]),
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(
        store.artifact_read(&query, 31 * DAY).unwrap_err().code,
        "NOT_FOUND"
    );
    query.selector.selection = QuerySelection::Pin {
        id: pin.id.clone(),
        expected_revision: Some(pin.revision + 1),
    };
    assert_eq!(
        store.artifact_read(&query, 31 * DAY).unwrap_err().code,
        "CONFLICT"
    );
    query.selector.selection = QuerySelection::Evidence(vec![a.id.clone()]);
    assert_eq!(
        store.artifact_read(&query, 31 * DAY).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    query.selector.source = None;
    query.selector.selection = QuerySelection::Evidence(a.record_ids[..1].to_vec());
    let page = store.artifact_read(&query, 31 * DAY).unwrap();
    assert_eq!(page.body.artifacts[0].retained_records, 1);
    assert!(!page.body.artifacts[0].complete_bytes_available);
    store.expire(31 * DAY).unwrap();
    let fragment = store.artifact_read(&query, 31 * DAY).unwrap();
    assert_eq!(fragment.body.artifacts[0].origin, None);
    query.scan_budget = 1;
    assert_eq!(
        store.artifact_read(&query, 31 * DAY).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store.status(31 * DAY).unwrap()["artifact_read_handles"].as_str(),
        Some("0")
    );
}

#[test]
fn artifact_selection_pages_preserve_pin_capture_after_release_and_retirement() {
    use sparrow_core::{ArtifactRead, ArtifactSelector, QuerySelection};
    const DAY: u64 = 86400000;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let a = store
        .ingest("artifact-scope-freeze-a", "source", "jsonl", b"{}\n", 0)
        .unwrap();
    let b = store
        .ingest("artifact-scope-freeze-b", "source", "jsonl", b"{}\n", 0)
        .unwrap();
    let pin = store
        .pin("artifact-scope-freeze-pin", "frozen", &[a.id, b.id], 40, 0)
        .unwrap();
    let mut query = ArtifactRead {
        selector: ArtifactSelector {
            selection: QuerySelection::Pin {
                id: pin.id.clone(),
                expected_revision: Some(pin.revision),
            },
            ..Default::default()
        },
        page_size: 1,
        ..Default::default()
    };
    let first = store.artifact_read(&query, 31 * DAY).unwrap();
    query.cursor = first.next_cursor.clone();
    let before = store.artifact_read(&query, 31 * DAY).unwrap();
    store
        .release_retry("artifact-scope-release", &pin.id, 31 * DAY + 1)
        .unwrap();
    let mut fresh = query.clone();
    fresh.cursor = None;
    assert_eq!(
        store.artifact_read(&fresh, 31 * DAY + 1).unwrap_err().code,
        "NOT_FOUND"
    );
    store.expire(31 * DAY + 2).unwrap();
    let after = store.artifact_read(&query, 31 * DAY + 3).unwrap();
    assert_eq!(after, before);
    assert!(after.body.artifacts[0].complete_bytes_available);
    assert_eq!(
        store
            .artifact_read(&query, first.lease_expires_at)
            .unwrap_err()
            .code,
        "RESULT_EXPIRED"
    );
    assert_eq!(
        store.status(first.lease_expires_at).unwrap()["artifact_read_handles"].as_str(),
        Some("0")
    );
}

#[test]
fn artifact_selection_seeks_members_without_reading_unrelated_corrupt_catalog_entries() {
    use sparrow_core::{ArtifactRead, ArtifactSelector, QuerySelection};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let selected = store
        .ingest("artifact-scope-seek", "selected", "json", b"{}", 0)
        .unwrap();
    let unrelated = store
        .ingest("artifact-scope-unrelated", "unrelated", "json", b"{}", 0)
        .unwrap();
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    db.put(&unrelated.record_ids[0], b"{broken").unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    let mut query = ArtifactRead {
        selector: ArtifactSelector {
            selection: QuerySelection::Evidence(vec![selected.id.clone()]),
            ..Default::default()
        },
        scan_budget: 5,
        ..Default::default()
    };
    let page = store.artifact_read(&query, 1).unwrap();
    assert_eq!(page.total_items, 1);
    assert_eq!(page.scanned, 5);
    assert_eq!(page.body.artifacts[0].id, selected.id);
    assert_eq!(page.body.artifacts[0].retained_records, 1);
    query.selector.selection = QuerySelection::Evidence(vec![selected.id, unrelated.id]);
    query.selector.source = Some("selected".into());
    query.scan_budget = 100;
    assert_eq!(
        store.artifact_read(&query, 1).unwrap_err().code,
        "STORAGE_FAILURE"
    );
    assert_eq!(
        store
            .artifact_read(&ArtifactRead::default(), 1)
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
    assert!(store.properties().is_ok());
}

#[test]
fn stored_rdf_layers_select_native_graph_owners_and_keep_failure_and_retention_boundaries() {
    use purrdf::DatasetView;
    use sparrow_core::{DiagnosticDimension as D, RdfGraphLayers, RdfReadLimits};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .define_diagnostic_profile("layers", [(D::Service, "/svc".into())].into())
        .unwrap();
    let raw = br#"{"svc":"worker","message":"payload"}"#;
    let a = store.ingest("layers-a", "layers", "json", raw, 0).unwrap();
    let b = store.ingest("layers-b", "layers", "json", raw, 1).unwrap();
    let ids = vec![a.record_ids[0].clone(), b.record_ids[0].clone()];
    for mask in 0_u8..8 {
        let layers = RdfGraphLayers {
            metadata: mask & 1 != 0,
            diagnostic: mask & 2 != 0,
            content: mask & 4 != 0,
        };
        let observed = store.with_rdf_record_layers(&ids, layers, 2, RdfReadLimits::default(), |view| {
            let graphs = view.named_graphs().count();
            let results = [
                "SELECT ?record ?service WHERE { ?record <urn:sparrow:vocab:service> ?service }",
                "SELECT ?record ?service ?source WHERE { ?record <urn:sparrow:vocab:service> ?service . ?record <urn:sparrow:vocab:sourceName> ?source }",
            ].map(|query| purrdf::sparql::NativeSparqlEngine::new().query_governed_fallible_view(view,
                purrdf::SparqlRequest { query, base_iri: None, substitutions: &[] },
                purrdf::sparql::QueryOptions::default(), &purrdf::sparql::QueryGovernors::METERED.with_fuel(1000000)
                    .with_max_answers(100).with_max_intermediate_cells(10000).with_max_scratch_bytes(1024 * 1024).with_max_remote_requests(0)).unwrap().result);
            (graphs, results)
        }).unwrap();
        assert_eq!(
            observed.0,
            usize::from(layers.metadata) * 2
                + usize::from(layers.diagnostic) * 2
                + usize::from(layers.content)
        );
        for (result, present) in observed
            .1
            .into_iter()
            .zip([layers.diagnostic, layers.metadata && layers.diagnostic])
        {
            let (_, rows) = result.solutions().expect("native SELECT required");
            assert_eq!(rows.len(), if present { 2 } else { 0 });
            if present {
                assert!(format!("{rows:?}").contains("worker"));
            }
        }
    }
    let old = store
        .with_rdf_records(&ids, 2, RdfReadLimits::default(), |view| {
            view.named_graphs().collect::<Vec<_>>()
        })
        .unwrap();
    let all = store
        .with_rdf_record_layers(
            &ids,
            RdfGraphLayers::ALL,
            2,
            RdfReadLimits::default(),
            |view| view.named_graphs().collect::<Vec<_>>(),
        )
        .unwrap();
    assert_eq!(old, all);
    let projection = store.record(&a.record_ids[0]).unwrap().rdf_projection_id;
    let broken = store
        .ingest("layers-broken", "layers", "json", b"{\"svc\":", 2)
        .unwrap();
    for diagnostic in [false, true] {
        let layers = RdfGraphLayers {
            metadata: !diagnostic,
            diagnostic,
            content: false,
        };
        assert_eq!(
            store
                .with_rdf_record_layers(
                    &broken.record_ids,
                    layers,
                    3,
                    RdfReadLimits::default(),
                    |view| view.named_graphs().count()
                )
                .unwrap(),
            1
        );
    }
    assert_eq!(
        store
            .with_rdf_records(&broken.record_ids, 3, RdfReadLimits::default(), |_| ())
            .unwrap_err()
            .code,
        "INDEX_NOT_READY"
    );
    assert_eq!(
        store
            .with_rdf_record_layers(
                &ids,
                RdfGraphLayers::ALL,
                3,
                RdfReadLimits {
                    requests: 0,
                    ..Default::default()
                },
                |_| ()
            )
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    let pin = store
        .pin("layers-pin", "protected", &a.record_ids, 40, 3)
        .unwrap();
    let none = RdfGraphLayers {
        metadata: false,
        diagnostic: false,
        content: false,
    };
    assert_eq!(
        store
            .with_rdf_record_layers(
                &a.record_ids,
                none,
                31 * DAY,
                RdfReadLimits::default(),
                |view| view.named_graphs().count()
            )
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .with_rdf_record_layers(
                &b.record_ids,
                none,
                31 * DAY,
                RdfReadLimits::default(),
                |_| ()
            )
            .unwrap_err()
            .code,
        "EVIDENCE_UNAVAILABLE"
    );
    store
        .release_retry("layers-release", &pin.id, 31 * DAY)
        .unwrap();
    assert_eq!(
        store
            .with_rdf_record_layers(
                &a.record_ids,
                none,
                31 * DAY,
                RdfReadLimits::default(),
                |_| ()
            )
            .unwrap_err()
            .code,
        "EVIDENCE_UNAVAILABLE"
    );
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    db.put(
        format!("rdf:state:{projection}"),
        b"corrupt excluded projection state",
    )
    .unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    let diagnostics = RdfGraphLayers {
        metadata: false,
        diagnostic: true,
        content: false,
    };
    assert_eq!(
        store
            .with_rdf_record_layers(&ids, diagnostics, 4, RdfReadLimits::default(), |view| view
                .named_graphs()
                .count())
            .unwrap(),
        2
    );
    assert_eq!(
        store
            .with_rdf_records(&ids, 4, RdfReadLimits::default(), |_| ())
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
}

#[test]
fn source_discovery_freezes_bounded_syslog_bindings_without_inventing_continuity() {
    use sparrow_core::{SourceRead, SourceSelector, SourceSyslogBinding};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let bindings = [
        SourceSyslogBinding {
            source: "a".into(),
            listener_id: "udp".into(),
        },
        SourceSyslogBinding {
            source: "a".into(),
            listener_id: "tcp".into(),
        },
        SourceSyslogBinding {
            source: "z".into(),
            listener_id: "idle".into(),
        },
    ];
    let mut query = SourceRead {
        page_size: 1,
        ..Default::default()
    };
    let first = store
        .source_read_with_syslog(&query, 1, &[], &bindings)
        .unwrap();
    assert_eq!(first.total_items, 2);
    let row = &first.body.sources[0];
    assert_eq!(row.source_id, "a");
    assert_eq!(row.syslog_listener_ids, ["tcp", "udp"]);
    assert_eq!(row.retained_records, 0);
    assert!(row.continuity_unknown);
    query.cursor = first.next_cursor.clone();
    let captured = store.source_read_with_syslog(&query, 2, &[], &[]).unwrap();
    assert_eq!(captured.body.sources[0].source_id, "z");
    assert_eq!(captured.body.sources[0].syslog_listener_ids, ["idle"]);
    assert!(
        store
            .result_release(&first.store_id, &first.snapshot_id, 2)
            .unwrap()
    );
    query.cursor = None;
    assert_eq!(
        store
            .source_read_with_syslog(&query, 2, &[], &[])
            .unwrap()
            .total_items,
        0
    );
    query.selector = SourceSelector {
        source: Some("a".into()),
    };
    query.page_size = 100;
    let exact = store
        .source_read_with_syslog(&query, 2, &[], &bindings)
        .unwrap();
    assert_eq!(exact.total_items, 1);
    assert_eq!(exact.body.sources[0].syslog_listener_ids, ["tcp", "udp"]);
    for invalid in [
        vec![bindings[0].clone(), bindings[0].clone()],
        vec![SourceSyslogBinding {
            source: String::new(),
            listener_id: "bad".into(),
        }],
        vec![SourceSyslogBinding {
            source: "a".into(),
            listener_id: "x".repeat(129),
        }],
        (0..5)
            .map(|n| SourceSyslogBinding {
                source: "a".into(),
                listener_id: n.to_string(),
            })
            .collect(),
    ] {
        assert_eq!(
            store
                .source_read_with_syslog(&query, 2, &[], &invalid)
                .unwrap_err()
                .code,
            "INVALID_ARGUMENT"
        );
    }
    query.scan_budget = 1;
    assert_eq!(
        store
            .source_read_with_syslog(&query, 2, &[], &bindings)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store.status(2).unwrap()["source_read_handles"].as_str(),
        Some("0")
    );
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .source_read(&SourceRead::default(), 3, &[])
            .unwrap()
            .total_items,
        0
    );
    assert_eq!(
        store
            .source_read_with_syslog(&SourceRead::default(), 3, &[], &bindings)
            .unwrap()
            .total_items,
        2
    );
}

#[test]
fn source_catalog_admission_is_shared_and_keeps_exact_keys_and_sorted_watch_revisions() {
    use sparrow_core::{SourceRead, SourceWatchBinding};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let source = "\n".repeat(512);
    store
        .ingest("source-admit", &source, "json", b"{}", 0)
        .unwrap();
    let configured = [
        SourceWatchBinding {
            source: source.clone(),
            watch_id: "z".into(),
            revision: 2,
            enabled: true,
        },
        SourceWatchBinding {
            source: source.clone(),
            watch_id: "a".into(),
            revision: 9,
            enabled: false,
        },
        SourceWatchBinding {
            source: "idle".into(),
            watch_id: "idle".into(),
            revision: 1,
            enabled: true,
        },
    ];
    let mut query = SourceRead {
        page_size: 1,
        ..Default::default()
    };
    let captured = (0..8)
        .map(|_| store.clone().source_read(&query, 1, &configured).unwrap())
        .collect::<Vec<_>>();
    let row = &captured[0].body.sources[0];
    assert_eq!(row.source_id, source);
    assert_eq!(
        row.watches
            .iter()
            .map(|w| (w.watch_id.as_str(), w.revision, w.enabled))
            .collect::<Vec<_>>(),
        [("a", 9, false), ("z", 2, true)]
    );
    assert_eq!(
        store
            .clone()
            .source_read(&query, 1, &configured)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store.status(1).unwrap()["source_read_handles"].as_str(),
        Some("8")
    );
    query.cursor = captured[0].next_cursor.clone();
    let continuation = store.source_read(&query, 2, &[]).unwrap();
    assert_eq!(continuation.body.sources[0].source_id, "idle");
    assert_eq!(continuation.body.sources[0].watches[0].revision, 1);
    store.expire(60_001).unwrap();
    assert_eq!(
        store.source_read(&query, 60_001, &[]).unwrap_err().code,
        "RESULT_EXPIRED"
    );
    assert_eq!(
        store.status(60_001).unwrap()["source_read_handles"].as_str(),
        Some("0")
    );
    query.cursor = None;
    let fresh = store.source_read(&query, 60_001, &configured).unwrap();
    assert_eq!(fresh.total_items, 2);
    assert_ne!(fresh.snapshot_id, captured[0].snapshot_id);
}

#[test]
fn source_discovery_captures_empty_active_and_terminal_generations_without_retaining_bytes() {
    use sparrow_core::{SourceRead, SourceSelector, TailStart};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let mut declaration = TailStart {
        request_id: "source-tail-a".into(),
        watch: "tail".into(),
        watch_revision: 1,
        directories: CONSUMPTION_DIRECTORIES,
        path: b"a.log".to_vec(),
        source: "a".into(),
        format: "log".into(),
        device: 7,
        inode: 1,
    };
    let a = store.tail_start(&declaration, 1).unwrap();
    declaration.request_id = "source-tail-z".into();
    declaration.source = "z".into();
    declaration.inode = 2;
    declaration.path = b"z.log".to_vec();
    let active = store.tail_start(&declaration, 2).unwrap();
    declaration.request_id = "source-tail-terminal".into();
    declaration.inode = 3;
    declaration.path = b"terminal.log".to_vec();
    let terminal = store.tail_start(&declaration, 3).unwrap();
    store.tail_append(&terminal.id, 0, b"", true, 4).unwrap();
    let deadline = store
        .tail_generation(&terminal.id)
        .unwrap()
        .expires_at
        .unwrap();
    let mut query = SourceRead {
        page_size: 1,
        ..Default::default()
    };
    let first = store.source_read(&query, 5, &[]).unwrap();
    assert_eq!(first.total_items, 2);
    assert_eq!(first.body.sources[0].tail_generation_ids, [a.id]);
    assert_eq!(first.body.sources[0].retained_records, 0);
    declaration.request_id = "source-tail-after-capture".into();
    declaration.inode = 4;
    declaration.path = b"later.log".to_vec();
    let later = store.tail_start(&declaration, 6).unwrap();
    query.cursor = first.next_cursor;
    let captured = store.source_read(&query, 7, &[]).unwrap();
    assert_eq!(captured.snapshot_id, first.snapshot_id);
    let mut expected = vec![active.id.clone(), terminal.id.clone()];
    expected.sort();
    assert_eq!(captured.body.sources[0].tail_generation_ids, expected);
    assert_eq!(captured.body.sources[0].complete_artifacts, 0);
    assert_eq!(captured.body.sources[0].retained_records, 0);
    assert!(captured.body.sources[0].continuity_unknown);
    let selected = SourceRead {
        selector: SourceSelector {
            source: Some("z".into()),
        },
        ..Default::default()
    };
    let fresh = store.source_read(&selected, deadline, &[]).unwrap();
    let mut expected = vec![active.id, later.id];
    expected.sort();
    assert_eq!(fresh.body.sources[0].tail_generation_ids, expected);
    assert_eq!(
        store.status(deadline).unwrap()["live_records"].as_str(),
        Some("0")
    );
    drop(store);
    let reopened = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        reopened
            .source_read(&selected, deadline, &[])
            .unwrap()
            .body
            .sources[0]
            .tail_generation_ids,
        expected
    );
}

#[test]
fn source_discovery_refuses_corrupt_tail_registry_and_small_scan_budget_without_a_capture() {
    use sparrow_core::SourceRead;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let tiny = SourceRead {
        scan_budget: 1,
        ..Default::default()
    };
    assert_eq!(
        store.source_read(&tiny, 1, &[]).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store.status(1).unwrap()["source_read_handles"].as_str(),
        Some("0")
    );
    // Native registry corruption must refuse open rather than disappear from discovery.
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    db.put(b"tail-generation-registry", b"broken registry")
        .unwrap();
    drop(db);
    assert_eq!(
        Store::open(dir.path(), config()).err().unwrap().code,
        "STORAGE_FAILURE"
    );
}

#[test]
fn source_discovery_counts_occurrences_mappings_and_idle_watch_bindings_in_one_capture() {
    use purrdf::DatasetView;
    use sparrow_core::{
        DiagnosticDimension, SourceRead, SourceSelector, SourceWatchBinding, TimeRepresentation,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let time = store
        .define_time_profile("a", "/t", TimeRepresentation::UnixMillis)
        .unwrap();
    let diagnostic = store
        .define_diagnostic_profile(
            "declared",
            [(DiagnosticDimension::Service, "/svc".into())].into(),
        )
        .unwrap();
    let artifact = store
        .ingest("source-count-a", "a", "jsonl", b"{}\n{}\n", 0)
        .unwrap();
    store
        .ingest("source-count-a2", "a", "jsonl", b"{}\n{}\n", 0)
        .unwrap();
    store
        .ingest("source-count-b", "b", "json", b"{}", 0)
        .unwrap();
    let configured = [SourceWatchBinding {
        source: "idle".into(),
        watch_id: "completed".into(),
        revision: 7,
        enabled: false,
    }];
    let mut query = SourceRead {
        page_size: 1,
        ..Default::default()
    };
    let first = store.source_read(&query, 1, &configured).unwrap();
    assert_eq!(first.total_items, 4);
    let a = &first.body.sources[0];
    assert_eq!(a.source_id, "a");
    assert_eq!(a.complete_artifacts, 2);
    assert_eq!(a.retained_records, 4);
    assert_eq!(a.time_profile_id.as_deref(), Some(time.id.as_str()));
    assert!(a.continuity_unknown);
    let scope = sparrow_core::QueryScope {
        sources: Some(sparrow_core::QueryAlternatives::new(vec![a.source_id.clone()]).unwrap()),
        selection: sparrow_core::QuerySelection::Live,
        source: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 100,
        limit: 100,
        scan_budget: 1000,
    };
    let records = store
        .records_query(&scope, &sparrow_core::RecordQuery::default(), 1)
        .unwrap();
    assert_eq!(
        records["rows"].as_array().unwrap().len(),
        a.retained_records
    );
    store
        .with_rdf_records(&artifact.record_ids, 1, Default::default(), |view| {
            assert!(
                view.term_id_by_value(&purrdf::TermValue::iri(a.rdf_iri.clone()))
                    .unwrap()
                    .is_some()
            );
        })
        .unwrap();
    store
        .ingest("source-count-after", "after", "json", b"{}", 1)
        .unwrap();
    let mut all = first.body.sources.clone();
    query.cursor = first.next_cursor;
    while query.cursor.is_some() {
        let page = store.source_read(&query, 2, &[]).unwrap();
        assert_eq!(page.snapshot_id, first.snapshot_id);
        assert_eq!(page.total_items, 4);
        all.extend(page.body.sources);
        query.cursor = page.next_cursor;
    }
    assert_eq!(
        all.iter().map(|s| s.source_id.as_str()).collect::<Vec<_>>(),
        ["a", "b", "declared", "idle"]
    );
    assert_eq!(all[2].retained_records, 0);
    assert_eq!(
        all[2].diagnostic_profile_id.as_deref(),
        Some(diagnostic.id.as_str())
    );
    assert_eq!(all[3].watches[0].watch_id, "completed");
    assert_eq!(all[3].watches[0].revision, 7);
    assert!(!all[3].watches[0].enabled);
    let selected = SourceRead {
        selector: SourceSelector {
            source: Some("declared".into()),
        },
        ..Default::default()
    };
    assert_eq!(
        store
            .source_read(&selected, 2, &configured)
            .unwrap()
            .total_items,
        1
    );
}

#[test]
fn source_discovery_preserves_fragments_and_frozen_pages_without_renewing_protection() {
    use sparrow_core::{SourceRead, TimeRepresentation};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .define_time_profile("declared", "/t", TimeRepresentation::UnixMillis)
        .unwrap();
    let old = store
        .ingest("source-fragment", "old", "jsonl", b"{}\n{}\n", 0)
        .unwrap();
    let pin = store
        .pin("source-pin", "fragment", &old.record_ids[..1], 40, 0)
        .unwrap();
    store.expire(31 * DAY).unwrap();
    let mut query = SourceRead {
        page_size: 1,
        ..Default::default()
    };
    let first = store.source_read(&query, 31 * DAY, &[]).unwrap();
    assert_eq!(first.total_items, 2);
    assert_eq!(first.body.sources[0].source_id, "declared");
    query.cursor = first.next_cursor.clone();
    let fragment = store.source_read(&query, 31 * DAY, &[]).unwrap();
    assert_eq!(fragment.body.sources[0].complete_artifacts, 0);
    assert_eq!(fragment.body.sources[0].fragment_parents, 1);
    assert_eq!(fragment.body.sources[0].retained_records, 1);
    store.release(&pin.id).unwrap();
    store.expire(31 * DAY + 1).unwrap();
    assert_eq!(
        store.source_read(&query, 31 * DAY + 2, &[]).unwrap(),
        fragment
    );
    assert_eq!(
        store
            .source_read(&SourceRead::default(), 31 * DAY + 2, &[])
            .unwrap()
            .total_items,
        1
    );
    assert_eq!(
        store
            .source_read(&query, first.lease_expires_at, &[])
            .unwrap_err()
            .code,
        "RESULT_EXPIRED"
    );
    assert_eq!(
        store.status(first.lease_expires_at).unwrap()["source_read_handles"].as_str(),
        Some("0")
    );
}

#[test]
fn source_discovery_refuses_corrupt_declarations_and_budget_without_a_prefix() {
    use sparrow_core::{SourceRead, TimeRepresentation};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .define_time_profile("declared", "/t", TimeRepresentation::UnixMillis)
        .unwrap();
    let tiny = SourceRead {
        scan_budget: 1,
        ..Default::default()
    };
    assert_eq!(
        store.source_read(&tiny, 1, &[]).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store.status(1).unwrap()["source_read_handles"].as_str(),
        Some("0")
    );
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    db.put("source-time:corrupt", b"{invalid").unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .source_read(&SourceRead::default(), 1, &[])
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
    assert!(store.properties().is_ok());
}

#[test]
fn stored_rdf_reads_refuse_exhaustion_and_failed_projection_without_prefixes() {
    use purrdf::DatasetView;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest(
            "native-read-budget",
            "rdf-native",
            "json",
            br#"{"message":"failed"}"#,
            0,
        )
        .unwrap();
    let limits = sparrow_core::RdfReadLimits {
        row_visits: 1,
        ..Default::default()
    };
    let error = store
        .with_rdf_records(&artifact.record_ids, 1, limits, |view| {
            view.quads().collect::<Vec<_>>()
        })
        .unwrap_err();
    assert_eq!(error.code, "RESOURCE_LIMIT");
    assert_eq!(
        store
            .with_rdf_records(&artifact.record_ids, 1, Default::default(), |view| view
                .term_id_by_value(&purrdf::TermValue::iri(format!(
                    "urn:fixture:{}",
                    "x".repeat(70000)
                ))))
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    let limits = sparrow_core::RdfReadLimits {
        live_bytes: 1,
        ..Default::default()
    };
    assert_eq!(
        store
            .with_rdf_records(&artifact.record_ids, 1, limits, |_| ())
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    let malformed = store
        .ingest("native-read-failed", "rdf-native", "json", b"{broken", 0)
        .unwrap();
    assert_eq!(
        store
            .with_rdf_records(&malformed.record_ids, 1, Default::default(), |_| ())
            .unwrap_err()
            .code,
        "INDEX_NOT_READY"
    );
}

#[test]
fn rdf_read_controls_and_selected_catalog_identity_are_verified() {
    for key in ["rdf:term-count", "rdf:max-encoded", "rdf:next"] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), config()).unwrap();
        drop(store);
        let db = catalog_db(dir.path()).unwrap();
        db.delete(key).unwrap();
        drop(db);
        assert_eq!(
            Store::open(dir.path(), config()).err().unwrap().code,
            "STORAGE_FAILURE"
        );
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let first = store
        .ingest("rdf-identity-a", "a", "log", b"one", 0)
        .unwrap();
    let second = store
        .ingest("rdf-identity-b", "b", "log", b"two", 0)
        .unwrap();
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    let bytes = db.get(&first.record_ids[0]).unwrap().unwrap();
    let changed = std::str::from_utf8(&bytes)
        .unwrap()
        .replace(&first.record_ids[0], &second.record_ids[0]);
    db.put(&first.record_ids[0], changed).unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .with_rdf_records(&first.record_ids, 1, Default::default(), |_| ())
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
}

#[test]
fn reverse_lookups_from_older_writers_move_into_their_family_at_open() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let bytes = br#"{"needle":"original"}"#;
    let first = store
        .ingest("rdf-reverse-first", "rdf", "json", bytes, 0)
        .unwrap();
    let inspection = store.rdf_inspect(&first.record_ids[0], 1).unwrap();
    drop(store);
    // Older writers kept every reverse lookup in the default family.
    let db = catalog_db(dir.path()).unwrap();
    let reverse = db.cf_handle("rdf_reverse").unwrap();
    let mut legacy = rocksdb::WriteBatch::default();
    for row in db.iterator_cf(reverse, rocksdb::IteratorMode::Start) {
        let (key, value) = row.unwrap();
        legacy.put(&key, &value);
        legacy.delete_cf(reverse, &key);
    }
    assert!(!legacy.is_empty());
    db.write(legacy).unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.rdf_inspect(&first.record_ids[0], 1).unwrap(),
        inspection
    );
    store
        .ingest("rdf-reverse-second", "rdf", "json", bytes, 2)
        .unwrap();
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    assert!(
        !db.prefix_iterator(b"rdf:reverse:")
            .any(|row| row.unwrap().0.starts_with(b"rdf:reverse:"))
    );
    // Reused terms resolve through the family instead of being minted again.
    let lookups = db
        .iterator_cf(
            db.cf_handle("rdf_reverse").unwrap(),
            rocksdb::IteratorMode::Start,
        )
        .count() as u64;
    let terms = u64::from_be_bytes(
        db.get("rdf:term-count")
            .unwrap()
            .unwrap()
            .as_slice()
            .try_into()
            .unwrap(),
    );
    assert_eq!(lookups, terms);
}

#[test]
fn persisted_json_rdf_matches_native_projection_and_all_six_indexes() {
    use purrdf::projections::{ProjectionLimits, ProjectionTerm};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let bytes = br#"{"duplicate":1e0,"duplicate":1.00,"array":[null,"a\u0020b"]}"#;
    let first = store
        .ingest("rdf-json-first", "rdf", "json", bytes, 0)
        .unwrap();
    let second = store
        .ingest("rdf-json-second", "rdf", "json", bytes, 1)
        .unwrap();
    let inspection = store.rdf_inspect(&first.record_ids[0], 2).unwrap();
    assert_eq!(inspection["state"].as_str(), Some("complete"));
    assert_eq!(
        inspection["content_graph"],
        store.rdf_inspect(&second.record_ids[0], 2).unwrap()["content_graph"]
    );
    assert_ne!(
        inspection["metadata_graph"],
        store.rdf_inspect(&second.record_ids[0], 2).unwrap()["metadata_graph"]
    );
    let digest = purrdf_hash::hex::encode(purrdf_hash::blake3::hash(bytes).as_bytes());
    let source = format!("urn:sparrow:payload:{digest}");
    let native = purrdf::json::encode(
        purrdf::json::SourceDocument { id: &source, bytes },
        &sparrow_core::json_projection_profile().unwrap(),
    )
    .unwrap();
    assert_eq!(
        inspection["primary_rows"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap(),
        native.quad_count() as u64
    );
    let projection = store
        .record(&first.record_ids[0])
        .unwrap()
        .rdf_projection_id;
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    let manifest = purrdf_lex::json::read(
        std::str::from_utf8(&db.get(format!("rdf:graph:{projection}")).unwrap().unwrap()).unwrap(),
    )
    .unwrap();
    let owner = manifest["owner"].as_str().unwrap().parse::<u64>().unwrap();
    let limits = ProjectionLimits::new(1, 65536, 33554432, 33554432, 16).unwrap();
    let canonical = |value: &purrdf::TermValue| {
        ProjectionTerm::from_term_value(value, limits)
            .unwrap()
            .to_canonical_json(limits)
            .unwrap()
    };
    let graph = canonical(&purrdf::TermValue::iri(
        inspection["content_graph"].as_str().unwrap(),
    ));
    let expected: std::collections::BTreeSet<_> = native
        .quads()
        .map(|row| {
            [
                canonical(&native.term_value(row.s)),
                canonical(&native.term_value(row.p)),
                canonical(&native.term_value(row.o)),
                graph.clone(),
            ]
        })
        .collect();
    for (name, axes) in [
        ("SPOG", [0, 1, 2, 3]),
        ("POSG", [1, 2, 0, 3]),
        ("OSPG", [2, 0, 1, 3]),
        ("GSPO", [3, 0, 1, 2]),
        ("GPOS", [3, 1, 2, 0]),
        ("GOSP", [3, 2, 0, 1]),
    ] {
        let prefix = format!("rdf:q:0:{name}:");
        let mut actual = std::collections::BTreeSet::new();
        for entry in db.prefix_iterator(&prefix) {
            let (key, value) = entry.unwrap();
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            assert!(value.is_empty());
            assert_eq!(key.len(), prefix.len() + 40);
            let ids: Vec<_> = key[prefix.len()..]
                .as_chunks::<8>()
                .0
                .iter()
                .map(|v| u64::from_be_bytes(*v))
                .collect();
            if ids[4] != owner {
                continue;
            }
            let mut row = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
            for (position, axis) in axes.into_iter().enumerate() {
                let bytes = db
                    .get(format!("rdf:term:{:020}", ids[position]))
                    .unwrap()
                    .unwrap();
                ProjectionTerm::from_canonical_json(&bytes, limits).unwrap();
                let reverse = format!(
                    "rdf:reverse:{}",
                    purrdf_hash::hex::encode(purrdf_hash::blake3::hash(&bytes).as_bytes())
                );
                assert_eq!(
                    db.get_cf(db.cf_handle("rdf_reverse").unwrap(), reverse)
                        .unwrap()
                        .unwrap(),
                    ids[position].to_be_bytes()
                );
                row[axis] = bytes;
            }
            actual.insert(row);
        }
        assert_eq!(actual, expected, "{name} must preserve every native row");
    }
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.rdf_inspect(&first.record_ids[0], 2).unwrap(),
        inspection
    );
    store
        .pin(
            "rdf-pin",
            "rdf",
            &[first.record_ids[0].clone()],
            2,
            29 * DAY,
        )
        .unwrap();
    store.expire(30 * DAY + 1).unwrap();
    assert_eq!(
        store
            .rdf_inspect(&first.record_ids[0], 30 * DAY + 1)
            .unwrap()["state"]
            .as_str(),
        Some("complete")
    );
    assert_eq!(
        store
            .rdf_inspect(&second.record_ids[0], 30 * DAY + 1)
            .unwrap_err()
            .code,
        "EVIDENCE_UNAVAILABLE"
    );
    store.expire(32 * DAY).unwrap();
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    let reverse = db.cf_handle("rdf_reverse").unwrap();
    assert!(
        db.iterator_cf(reverse, rocksdb::IteratorMode::Start)
            .next()
            .is_none(),
        "unreachable reverse lookups must be reclaimed"
    );
    for prefix in [
        "rdf:q:",
        "rdf:term:",
        "rdf:ref:",
        "rdf:graph:",
        "rdf:state:",
        "rdf:projection-ref:",
    ] {
        assert!(
            !db.prefix_iterator(prefix)
                .any(|entry| entry.unwrap().0.starts_with(prefix.as_bytes())),
            "unreachable {prefix} must be reclaimed"
        );
    }
}

#[test]
fn native_markdown_and_refused_rdf_projections_keep_original_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let markdown = b"# Heading\n\nParagraph with exact bytes.\n\n---\n";
    let artifact = store
        .ingest("rdf-markdown", "rdf", "markdown", markdown, 0)
        .unwrap();
    let summary = store.rdf_inspect(&artifact.record_ids[0], 1).unwrap();
    assert_eq!(summary["state"].as_str(), Some("complete"));
    assert!(
        summary["primary_rows"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 10
    );
    assert_eq!(
        store.bytes(&artifact.id, 0, markdown.len(), 1).unwrap(),
        markdown
    );
    let large = format!("{{\"large\":\"{}\"}}", "x".repeat(70000));
    let oversized = store
        .ingest("rdf-large-scalar", "rdf", "json", large.as_bytes(), 2)
        .unwrap();
    let refused = store.rdf_inspect(&oversized.record_ids[0], 3).unwrap();
    assert_eq!(refused["state"].as_str(), Some("failed"));
    assert!(refused["diagnostic"].as_str().unwrap().contains("limit"));
    assert_eq!(refused["primary_rows"].as_str(), Some("0"));
    assert_eq!(
        store.bytes(&oversized.id, 0, large.len(), 3).unwrap(),
        large.as_bytes()
    );
    let malformed = store
        .ingest("rdf-invalid-json", "rdf", "json", b"{invalid}", 4)
        .unwrap();
    assert_eq!(
        store.rdf_inspect(&malformed.record_ids[0], 5).unwrap()["state"].as_str(),
        Some("failed")
    );
    let empty = store
        .ingest("rdf-empty-opaque", "rdf", "opaque", b"", 6)
        .unwrap();
    assert_eq!(
        store.rdf_inspect(&empty.record_ids[0], 7).unwrap()["state"].as_str(),
        Some("not_applicable")
    );
    store.expire(31 * DAY).unwrap();
    store.expire(31 * DAY).unwrap(); // Empty inputs and failed projections reclaim once.
}

#[test]
fn rdf_profile_identity_is_required_and_drift_refuses_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.status(0).unwrap()["store_format"].as_str(),
        Some("sparrow.store/77")
    );
    assert_eq!(
        store.status(0).unwrap()["rdf_profile"]
            .as_str()
            .unwrap()
            .len(),
        64
    );
    drop(store);
    for (replacement, expected) in [
        (Some(b"different-profile".as_slice()), "UNSUPPORTED"),
        (None, "STORAGE_FAILURE"),
    ] {
        let db = catalog_db(dir.path()).unwrap();
        match replacement {
            Some(value) => db.put("rdf_profile", value).unwrap(),
            None => db.delete("rdf_profile").unwrap(),
        }
        drop(db);
        match Store::open(dir.path(), config()) {
            Ok(_) => panic!("invalid RDF profile was admitted"),
            Err(error) => assert_eq!(error.code, expected),
        }
    }
}

#[test]
fn record_context_matches_retained_acceptance_order_and_reports_scan_frontiers() {
    use sparrow_core::{ContextFrontier, QueryScope};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = store
        .ingest(
            "context-ties",
            "context",
            "log",
            b"first\nsecond\nthird\n",
            10,
        )
        .unwrap();
    let mut ordered = input.record_ids.clone();
    ordered.sort(); // The advertised tiebreaker is occurrence ID, not file position.
    store
        .ingest("context-other", "another-source", "log", b"noise\n", 11)
        .unwrap();
    let latest = store
        .ingest("context-latest", "context", "log", b"latest\n", 12)
        .unwrap();
    let mut scope = QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: Some("context".into()),
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 10,
        received_before: 13,
        limit: 100,
        scan_budget: 100,
    };
    let context = store
        .records_context(&ordered[1], &scope, 10, 10, 20)
        .unwrap();
    assert_eq!(context.anchor.id, ordered[1]);
    assert_eq!(
        context
            .before
            .records
            .iter()
            .map(|r| r.id.as_str())
            .collect::<Vec<_>>(),
        vec![ordered[0].as_str()]
    );
    assert_eq!(
        context
            .after
            .records
            .iter()
            .map(|r| r.id.as_str())
            .collect::<Vec<_>>(),
        vec![ordered[2].as_str(), latest.record_ids[0].as_str()]
    );
    assert_eq!(context.before.frontier, ContextFrontier::ScopeBoundary);
    assert_eq!(context.after.visited, 2);
    scope.scan_budget = 1;
    let limited = store
        .records_context(&ordered[1], &scope, 1, 2, 20)
        .unwrap();
    assert_eq!(limited.before.frontier, ContextFrontier::RequestedCount);
    assert_eq!(limited.after.frontier, ContextFrontier::ScanBudget);
    assert_eq!(limited.after.records.len(), 1);
    scope.received_before = 12;
    scope.scan_budget = 100;
    let bounded = store
        .records_context(&ordered[1], &scope, 0, 10, 20)
        .unwrap();
    assert!(bounded.before.records.is_empty());
    assert_eq!(bounded.after.records.len(), 1);
    scope.source = Some("another-source".into());
    assert_eq!(
        store
            .records_context(&ordered[1], &scope, 1, 1, 20)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    scope.source = None;
    assert_eq!(
        store
            .records_context(&input.id, &scope, 1, 1, 20)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    assert_eq!(
        store
            .records_context(&ordered[1], &scope, 100, 0, 20)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .records_context(&ordered[1], &scope, 1, 1, 20)
            .unwrap()
            .before
            .records[0]
            .id,
        ordered[0]
    );
}

#[test]
fn context_admits_catalog_and_output_bytes_across_both_directions() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let source = "s".repeat(512);
    let input = store
        .ingest(
            "context-large-reply",
            &source,
            "log",
            &b"neighbor\n".repeat(151),
            1,
        )
        .unwrap();
    let mut ids = input.record_ids;
    ids.sort();
    let anchor = &ids[75];
    let scope = sparrow_core::QueryScope {
        selection: Default::default(),
        source: Some(source),
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 3,
        limit: 1000,
        scan_budget: 1000,
    };
    let before = store.records_context(anchor, &scope, 40, 0, 2).unwrap();
    let after = store.records_context(anchor, &scope, 0, 40, 2).unwrap();
    assert_eq!(before.before.records.len(), 40);
    assert_eq!(after.after.records.len(), 40);
    assert_ne!(before.snapshot_id, after.snapshot_id);
    assert!(before.snapshot_id.starts_with("context-read:"));
    assert_eq!(before.limits.catalog_bytes, 32 * 1024 * 1024);
    assert_eq!(before.limits.response_bytes, 256 * 1024);
    assert_eq!(before.limits.direction_visits, 1000);
    assert_eq!(before.limits.output_records, 1000);
    assert_eq!(before.limits.requested_before, 40);
    assert_eq!(before.limits.requested_after, 0);
    let refused = store
        .records_context(anchor, &scope, 80, 80, 2)
        .unwrap_err();
    assert_eq!(refused.code, "RESOURCE_LIMIT");
    assert!(refused.message.contains("context reply"));
    assert_eq!(store.bytes(anchor, 0, 9, 2).unwrap(), b"neighbor\n");
    assert_eq!(
        store.status(2).unwrap()["record_read_handles"].as_str(),
        Some("0")
    );
}

#[test]
fn context_source_index_skips_unrelated_catalogs_without_spending_visits() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let anchor = store
        .ingest("context-budget-anchor", "target", "log", b"anchor\n", 0)
        .unwrap();
    store
        .ingest(
            "context-budget-noise",
            &"n".repeat(512),
            "log",
            &b"noise\n".repeat(900),
            1,
        )
        .unwrap();
    let mut scope = sparrow_core::QueryScope {
        selection: Default::default(),
        source: Some("target".into()),
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 3,
        limit: 100,
        scan_budget: 1,
    };
    let partial = store
        .records_context(&anchor.record_ids[0], &scope, 0, 1, 2)
        .unwrap();
    assert_eq!(partial.after.visited, 0);
    assert!(partial.after.records.is_empty());
    assert_eq!(
        partial.after.frontier,
        sparrow_core::ContextFrontier::ScopeBoundary
    );
    scope.scan_budget = 1000;
    let bounded = store
        .records_context(&anchor.record_ids[0], &scope, 0, 1, 2)
        .unwrap();
    assert_eq!(bounded.after.visited, 0);
    assert!(bounded.after.records.is_empty());
    assert_eq!(
        store.bytes(&anchor.record_ids[0], 0, 7, 2).unwrap(),
        b"anchor\n"
    );
}

#[test]
fn context_catalog_admission_still_charges_skipped_same_source_members() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let source = "s".repeat(512);
    let anchor = store
        .ingest("same-source-anchor", &source, "log", b"anchor\n", 0)
        .unwrap();
    store
        .ingest(
            "same-source-noise",
            &source,
            "log",
            &b"noise\n".repeat(900),
            1,
        )
        .unwrap();
    let mut scope = record_page_scope(&source, 10);
    scope.selection = sparrow_core::QuerySelection::Evidence(vec![anchor.record_ids[0].clone()]);
    scope.scan_budget = 1;
    let partial = store
        .records_context(&anchor.record_ids[0], &scope, 0, 1, 2)
        .unwrap();
    assert_eq!(partial.after.visited, 1);
    assert_eq!(
        partial.after.frontier,
        sparrow_core::ContextFrontier::ScanBudget
    );
    assert!(partial.after.records.is_empty());
    scope.scan_budget = 1000;
    let refused = store
        .records_context(&anchor.record_ids[0], &scope, 0, 1, 2)
        .unwrap_err();
    assert_eq!(refused.code, "RESOURCE_LIMIT");
    assert!(refused.message.contains("catalog capture"));
    assert_eq!(
        store.bytes(&anchor.record_ids[0], 0, 7, 2).unwrap(),
        b"anchor\n"
    );
}

#[test]
fn context_rebuild_discards_stale_derivatives_and_restores_missing_entries() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let anchor = store
        .ingest("cache-anchor", "cache", "log", b"anchor\n", 1)
        .unwrap();
    let neighbor = store
        .ingest("cache-neighbor", "cache", "log", b"neighbor\n", 2)
        .unwrap();
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    let mut entries = Vec::new();
    for entry in db.prefix_iterator(b"source-context-v1:") {
        let (key, _) = entry.unwrap();
        if !key.starts_with(b"source-context-v1:") {
            break;
        }
        entries.push(key);
    }
    assert_eq!(entries.len(), 2);
    // Simulate a derivative left stale by a preceding catalog writer.
    db.delete(&entries[0]).unwrap();
    db.put(&entries[1], b"record:missing").unwrap();
    db.put(b"source-context-v1:obsolete", b"record:missing")
        .unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    let scope = sparrow_core::QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: None,
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 3,
        limit: 100,
        scan_budget: 100,
    };
    let context = store
        .records_context(&anchor.record_ids[0], &scope, 1, 1, 3)
        .unwrap();
    assert!(context.before.records.is_empty());
    assert_eq!(context.after.records[0].id, neighbor.record_ids[0]);
    assert_eq!(context.after.visited, 1);
    assert_eq!(
        store.bytes(&neighbor.record_ids[0], 0, 9, 3).unwrap(),
        b"neighbor\n"
    );
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    assert!(db.get(b"source-context-v1:obsolete").unwrap().is_none());
}

#[test]
fn context_refuses_broken_time_indexes_instead_of_publishing_a_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let anchor = store
        .ingest("fault-anchor", "fault", "log", b"anchor\n", 1)
        .unwrap();
    let neighbor = store
        .ingest("fault-neighbor", "fault", "log", b"neighbor\n", 2)
        .unwrap();
    let scope = sparrow_core::QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: None,
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 3,
        limit: 100,
        scan_budget: 100,
    };
    let neighbor_key = format!("time:{:020}:{}", 2, neighbor.record_ids[0]);
    let anchor_key = format!("time:{:020}:{}", 1, anchor.record_ids[0]);
    drop(store);
    // Corrupt only this test-owned store, while its production owner is closed.
    let db = catalog_db(dir.path()).unwrap();
    db.put(&neighbor_key, b"record:missing").unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .evict_oldest(3, sparrow_core::EvictionLimits::default())
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
    assert_eq!(
        store.bytes(&anchor.record_ids[0], 0, 7, 3).unwrap(),
        b"anchor\n"
    );
    assert_eq!(
        store
            .records_context(&anchor.record_ids[0], &scope, 0, 1, 3)
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    db.delete(&anchor_key).unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .records_context(&anchor.record_ids[0], &scope, 0, 0, 3)
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
}

#[test]
fn context_keeps_record_pins_and_exposes_observed_retirement_gaps() {
    use sparrow_core::{ContextFrontier, QueryScope};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let earlier = store
        .ingest("context-earlier", "context", "log", b"earlier\n", 0)
        .unwrap();
    store
        .ingest("context-gap", "context", "log", b"retired\n", 1)
        .unwrap();
    let anchor = store
        .ingest("context-anchor", "context", "log", b"anchor\n", 2)
        .unwrap();
    store
        .ingest("context-later-gap", "context", "log", b"retired later\n", 3)
        .unwrap();
    let pin = store
        .pin(
            "context-pin",
            "context",
            &[earlier.record_ids[0].clone(), anchor.record_ids[0].clone()],
            2,
            29 * DAY,
        )
        .unwrap();
    let now = 30 * DAY + 5;
    let latest = store
        .ingest("context-new", "context", "log", b"new\n", now)
        .unwrap();
    store.expire(now).unwrap();
    let scope = QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: None,
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: now + 1,
        limit: 100,
        scan_budget: 100,
    };
    let context = store
        .records_context(&anchor.record_ids[0], &scope, 1, 1, now)
        .unwrap();
    assert_eq!(context.before.records[0].id, earlier.record_ids[0]);
    assert_eq!(context.after.records[0].id, latest.record_ids[0]);
    assert_eq!(context.before.retired, 1);
    assert_eq!(context.after.retired, 1);
    assert_eq!(context.before.frontier, ContextFrontier::RequestedCount);
    assert_eq!(
        store.bytes(&anchor.id, 0, 1, now).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    store.release(&pin.id).unwrap();
    assert_eq!(
        store
            .records_context(&anchor.record_ids[0], &scope, 1, 1, now)
            .unwrap_err()
            .code,
        "EVIDENCE_UNAVAILABLE"
    );
    store.expire(32 * DAY).unwrap();
    assert_eq!(
        store
            .records_context(&anchor.record_ids[0], &scope, 1, 1, 32 * DAY)
            .unwrap_err()
            .code,
        "NOT_FOUND"
    );
}

#[test]
fn persisted_text_ranking_spans_scope_and_record_pins_survive_reopen() {
    use sparrow_core::QueryScope;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let a = store
        .ingest("text-a", "logs", "log", b"Cat cat\n", 0)
        .unwrap();
    let b = store
        .ingest("text-b", "logs", "log", b"cat dog dog\n", 1)
        .unwrap();
    let mut scope = QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: Some("logs".into()),
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 2,
        limit: 100,
        scan_budget: 10000,
    };
    let result = store.text_query(&scope, "CAT", 2).unwrap();
    assert_eq!(result["ranking"]["documents"].as_str(), Some("2"));
    assert_eq!(result["ranking"]["tokens"].as_str(), Some("5"));
    assert_eq!(result["rows"].as_array().unwrap().len(), 2);
    assert_eq!(
        result["rows"][0]["record_id"].as_str(),
        Some(a.record_ids[0].as_str())
    );
    assert_eq!(
        result["rows"][0]["evidence"][0]["source_start"].as_str(),
        Some("0")
    );
    assert_eq!(
        result["rows"][0]["evidence"][0]["source_end"].as_str(),
        Some("3")
    );
    assert_eq!(result["certainty"].as_str(), Some("exact"));
    assert_eq!(
        result["rows"][1]["record_id"].as_str(),
        Some(b.record_ids[0].as_str())
    );
    scope.received_from = 1;
    assert_eq!(
        store.text_query(&scope, "cat", 2).unwrap()["ranking"]["documents"].as_str(),
        Some("1")
    );
    scope.received_from = 0;
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(store.text_query(&scope, "CAT", 2).unwrap(), result);
    let pin = store
        .pin(
            "text-pin",
            "text-case",
            &[a.record_ids[0].clone()],
            2,
            29 * DAY,
        )
        .unwrap();
    store.expire(30 * DAY + 2).unwrap();
    let retained = store.text_query(&scope, "cat", 30 * DAY + 2).unwrap();
    assert_eq!(retained["ranking"]["documents"].as_str(), Some("1"));
    assert_eq!(
        retained["rows"][0]["record_id"].as_str(),
        Some(a.record_ids[0].as_str())
    );
    store.release(&pin.id).unwrap();
    store.expire(30 * DAY + 3).unwrap();
    assert!(
        store.text_query(&scope, "cat", 30 * DAY + 3).unwrap()["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn decoded_text_uses_scalar_evidence_and_failed_coverage_is_visible() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let bytes = br#"{"m":"\u0068\u0065\u006c\u006c\u006f"}"#;
    let artifact = store
        .ingest("decoded-text", "json", "json", bytes, 0)
        .unwrap();
    store
        .ingest("malformed-text", "json", "jsonl", b"{invalid}\n", 0)
        .unwrap();
    let scope = sparrow_core::QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: None,
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 1,
        limit: 100,
        scan_budget: 10000,
    };
    let result = store.text_query(&scope, "hello", 0).unwrap();
    let row = &result["rows"][0];
    assert_eq!(
        row["record_id"].as_str(),
        Some(artifact.record_ids[0].as_str())
    );
    assert_eq!(
        row["evidence"][0]["span"]["basis"].as_str(),
        Some("decoded_json_scalar")
    );
    assert_eq!(row["evidence"][0]["span"]["coarse"].as_bool(), Some(true));
    assert_eq!(row["evidence"][0]["span"]["path"].as_str(), Some("/m"));
    assert_eq!(
        result["text_coverage"]["failed_or_incomplete"].as_str(),
        Some("1")
    );
    assert_eq!(result["certainty"].as_str(), Some("incomplete"));
    let mut small = scope;
    small.scan_budget = 1;
    assert_eq!(
        store
            .text_query(&small, "hello invalid", 0)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
}

#[test]
fn standard_text_lexicons_are_required() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        sparrow_core::resolve_text_analyzer(dir.path())
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
}

#[test]
fn combined_ingest_status_reads_private_preparation_and_original_publication_without_advancing() {
    use sparrow_core::{IngestDeletion, IngestReadLimits, IngestSelector};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    // One preparation checkpoint holds at most 256 KiB of source records, so
    // the invalid second record is still pending after the first checkpoint.
    let mut input = format!("{{\"message\":\"{}\"}}\n", "r".repeat(60 * 1024)).into_bytes();
    input.extend_from_slice(format!("not-json {}\n", "n".repeat(200 * 1024)).as_bytes());
    let manifest = stage_fixture(&store, "inspect-upload", "jsonl", &input);
    let by_request = IngestSelector::Request(manifest.request_id.clone());
    let by_import = IngestSelector::Import(manifest.id.clone());
    let private = store
        .ingest_status(&by_request, Default::default(), 1)
        .unwrap();
    assert!(private.accepted.is_none());
    assert_eq!(private.total_records, Some(2));
    assert_eq!(private.inspected_records, 0);
    assert_eq!(private.json.pending, 2);
    assert_eq!(private.deletion, IngestDeletion::NotManaged);
    assert_eq!(private.staging.as_ref().unwrap(), &manifest);
    assert!(private.preparation.is_none());
    let first = store.import_prepare_record(&manifest.id, 0, 1).unwrap();
    let prepared = store
        .ingest_status(&by_import, Default::default(), 2)
        .unwrap();
    assert_eq!(prepared.selector, by_import);
    assert_eq!(prepared.preparation.as_ref().unwrap(), &first);
    assert!(prepared.accepted.is_none());
    assert_eq!(prepared.inspected_records, 1);
    assert_eq!(prepared.json.complete, 1);
    assert_eq!(prepared.rdf.complete, 1);
    assert_eq!(prepared.text.complete, 1);
    assert_eq!(prepared.json.pending, 1);
    assert_eq!(
        store.artifact(&first.artifact_id).unwrap_err().code,
        "NOT_FOUND"
    );
    store.import_prepare_record(&manifest.id, 1, 1).unwrap();
    let receipt = store.import_publish(&manifest.id, 1).unwrap();
    for selector in [&by_request, &by_import] {
        let accepted = store
            .ingest_status(selector, Default::default(), 2)
            .unwrap();
        assert_eq!(accepted.selector, *selector);
        assert_eq!(accepted.total_records, Some(2));
        assert_eq!(accepted.inspected_records, 2);
        assert!(accepted.preparation.is_none());
        assert_eq!(accepted.accepted.as_ref().unwrap().id, receipt.id);
        assert_eq!(
            accepted.accepted.as_ref().unwrap().received_at,
            receipt.received_at
        );
        assert_eq!(accepted.json.complete, 1);
        assert_eq!(accepted.json.failed, 1);
        assert_eq!(accepted.rdf.complete, 1);
        assert_eq!(accepted.rdf.failed, 1);
        assert_eq!(accepted.text.complete, 1);
        assert_eq!(accepted.text.incomplete, 1);
        assert_eq!(accepted.json.pending, 0);
        assert_eq!(accepted.removed_at, None);
    }
    for limits in [
        IngestReadLimits {
            scan_budget: 1,
            ..Default::default()
        },
        IngestReadLimits {
            catalog_bytes: 1,
            ..Default::default()
        },
    ] {
        assert_eq!(
            store
                .ingest_status(&by_request, limits, 2)
                .unwrap_err()
                .code,
            "RESOURCE_LIMIT"
        );
    }
    assert_eq!(store.bytes(&receipt.id, 0, input.len(), 2).unwrap(), input);
    assert_eq!(
        store.import_get(&manifest.id, 2).unwrap().expires_at,
        manifest.expires_at
    );
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .ingest_status(&by_request, Default::default(), 3)
            .unwrap()
            .accepted
            .unwrap()
            .record_ids,
        receipt.record_ids
    );
    assert_eq!(
        store
            .ingest_status(
                &IngestSelector::Request("missing".into()),
                Default::default(),
                3
            )
            .unwrap_err()
            .code,
        "NOT_FOUND"
    );
}

#[test]
fn combined_ingest_status_preserves_buffered_ack_and_does_not_flush() {
    use sparrow_core::{Durability, IngestDeletion, IngestSelector};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let receipt = store
        .ingest_with_durability(
            "peek-buffered",
            "source",
            "json",
            b"{}",
            1,
            Durability::Buffered,
        )
        .unwrap();
    let before = store.durability_status().unwrap();
    let selector = IngestSelector::Request("peek-buffered".into());
    let status = store
        .ingest_status(&selector, Default::default(), 2)
        .unwrap();
    assert_eq!(status.deletion, IngestDeletion::NotManaged);
    assert!(status.staging.is_none());
    assert!(status.import_id.is_none());
    assert_eq!(status.accepted.unwrap().buffered, receipt.buffered);
    let after = store.durability_status().unwrap();
    assert!(before.pending_writes > 0);
    assert_eq!(after.pending_writes, before.pending_writes);
    assert_eq!(after.pending_bytes, before.pending_bytes);
    assert_eq!(after.durable_through, before.durable_through);
    store
        .flush_through(receipt.buffered.as_ref().unwrap())
        .unwrap();
    assert!(
        store
            .ingest_status(&selector, Default::default(), 3)
            .unwrap()
            .accepted
            .unwrap()
            .buffered
            .is_none()
    );
}

#[test]
fn combined_ingest_status_refuses_corrupt_live_membership_and_projection_state() {
    use purrdf_lex::json::{self, Value};
    use sparrow_core::IngestSelector;
    for fault in 0..5 {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), config()).unwrap();
        let manifest = stage_fixture(&store, "corrupt-inspection", "json", b"{}");
        store.import_prepare_record(&manifest.id, 0, 1).unwrap();
        let receipt = store.import_publish(&manifest.id, 1).unwrap();
        let record = store.record(&receipt.record_ids[0]).unwrap();
        let selector = IngestSelector::Import(manifest.id.clone());
        drop(store);
        {
            let db = catalog_db(dir.path()).unwrap();
            let key = match fault {
                2 => format!("rdf:state:{}", record.rdf_projection_id),
                3 => format!("text-record:{}", record.id),
                _ => record.id.clone(),
            };
            if fault == 0 || fault == 2 {
                db.delete(&key).unwrap();
            } else {
                let bytes = db.get(&key).unwrap().unwrap();
                let mut value = json::record::read_document(&bytes).unwrap();
                let Value::Object(object) = &mut value else {
                    panic!("native catalog object");
                };
                match fault {
                    1 => {
                        object.insert("artifact_id", Value::String("artifact:another".into()));
                    }
                    3 => {
                        object.insert("state", Value::String("unknown".into()));
                    }
                    _ => {
                        object.insert("source", Value::String("another-source".into()));
                    }
                }
                db.put(&key, json::write_compact(&value)).unwrap();
            }
        }
        let store = Store::open(dir.path(), config()).unwrap();
        assert_eq!(
            store
                .ingest_status(&selector, Default::default(), 2)
                .unwrap_err()
                .code,
            "STORAGE_FAILURE",
            "fault {fault}"
        );
    }
}

/// INVARIANT: a write refused by the free-space floor is retriable. It leaves the
/// session certifiable, unlike a write that fails with no space left.
#[test]
fn free_space_floor_refuses_writes_without_latching_durability() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut full = config();
    full.min_free_bytes = u64::MAX;
    let store = Store::open(&path, full).unwrap();
    let refused = store
        .ingest("floor", "source", "log", b"refused\n", 1000)
        .unwrap_err();
    assert_eq!(refused.code, "STORAGE_PRESSURE", "{}", refused.message);
    assert_eq!(store.durability_status().unwrap().failure, None);
    store.flush().unwrap();
    drop(store);
    let store = Store::open(&path, config()).unwrap();
    let artifact = store
        .ingest("floor", "source", "log", b"accepted\n", 1000)
        .unwrap();
    assert_eq!(
        store.bytes(&artifact.id, 0, 9, 1001).unwrap(),
        b"accepted\n"
    );
}

fn config() -> Config {
    sparrow_test_support::storage_config!(Config)
}

#[test]
fn native_instants_keep_precision_and_reject_invalid_bounds() {
    use sparrow_core::{format_instant, parse_instant};
    assert_eq!(parse_instant("1970-01-01T01:00:00.001+01:00").unwrap(), 1);
    assert_eq!(format_instant(1).unwrap(), "1970-01-01T00:00:00.001Z");
    assert!(parse_instant("1970-01-01T00:00:00.0001Z").is_err());
    assert!(parse_instant("yesterday").is_err());
}

#[test]
fn release_retry_is_durable_and_cannot_release_a_reused_name() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store.ingest("a", "source", "log", b"event\n", 0).unwrap();
    let targets = vec![artifact.id];
    let old = store.pin("p", "case", &targets, 1, 0).unwrap();
    let receipt = store.release_retry("r", &old.id, 1).unwrap();
    let replacement = store.pin("new", "case", &targets, 1, 2).unwrap();
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(store.release_retry("r", &old.id, 3).unwrap(), receipt);
    assert_eq!(
        store
            .release_retry("r", &replacement.id, 3)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert_eq!(store.pins(3).unwrap()[0].id, replacement.id);
}

#[test]
fn receipt_and_catalog_cleanup_are_bounded_and_empty_projection_is_reclaimed() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let original = store.ingest("a", "source", "json", b"", 0).unwrap();
    assert_eq!(
        store.status(0).unwrap()["unique_json_projections"].as_str(),
        Some("1")
    );
    let retry = store.ingest("a", "source", "json", b"", DAY).unwrap();
    assert_eq!(retry.id, original.id);
    let next = store.ingest("a", "source", "json", b"", 2 * DAY).unwrap();
    assert_ne!(next.id, original.id);
    store.expire(32 * DAY).unwrap();
    assert_eq!(
        store.status(32 * DAY).unwrap()["unique_json_projections"].as_str(),
        Some("0")
    );
    assert_eq!(store.artifact(&original.id).unwrap_err().code, "NOT_FOUND");
    assert_eq!(
        store.record(&original.record_ids[0]).unwrap_err().code,
        "NOT_FOUND"
    );
}

#[test]
fn native_json_discovery_preserves_duplicates_lexical_values_and_time_scope() {
    use sparrow_core::{JsonQuery, QueryScope};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let bytes = br#"{"x":1e0,"x":1.00,"nested":{"a/b":"hello\u0020world"}}"#;
    let first = store.ingest("a", "alpha", "json", bytes, 100).unwrap();
    store.ingest("b", "alpha", "json", bytes, 200).unwrap();
    store.ingest("c", "beta", "json", bytes, 100).unwrap();
    let scope = QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: Some("alpha".into()),
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 100,
        received_before: 200,
        limit: 100,
        scan_budget: 1000,
    };
    let keys = store
        .json_query(&scope, &JsonQuery::Keys { path: None }, 201)
        .unwrap();
    let rows = keys.get("rows").unwrap().as_array().unwrap();
    let x = rows
        .iter()
        .find(|row| row.get("path").unwrap().as_str() == Some("/x"))
        .unwrap();
    assert_eq!(x.get("occurrences").unwrap().as_str(), Some("2"));
    assert_eq!(x.get("records").unwrap().as_str(), Some("1"));
    let query = JsonQuery::Values {
        path: Some("/x".into()),
        kind: Some("number".into()),
        lexical: Some("1.00".into()),
        decoded: None,
    };
    let values = store.json_query(&scope, &query, 201).unwrap();
    let matches = values.get("rows").unwrap().as_array().unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(
        matches[0].get("record_id").unwrap().as_str(),
        Some(first.record_ids[0].as_str())
    );
    assert_eq!(matches[0].get("source_start").unwrap().as_str(), Some("13"));
    let decoded = JsonQuery::Values {
        path: Some("/nested/a~1b".into()),
        kind: None,
        lexical: None,
        decoded: Some("hello world".into()),
    };
    assert_eq!(
        store
            .json_query(&scope, &decoded, 201)
            .unwrap()
            .get("rows")
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
    store.flush().unwrap();
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(store.json_query(&scope, &query, 201).unwrap(), values);
}

#[test]
fn projection_failure_is_visible_without_discarding_raw_evidence() {
    use sparrow_core::{JsonQuery, QueryScope};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let bytes = b"{invalid}\n{\"ok\":true}\n";
    let artifact = store.ingest("a", "source", "jsonl", bytes, 100).unwrap();
    assert_eq!(
        store.bytes(&artifact.id, 0, bytes.len(), 101).unwrap(),
        bytes
    );
    let scope = QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: None,
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 200,
        limit: 100,
        scan_budget: 1000,
    };
    let result = store
        .json_query(&scope, &JsonQuery::Keys { path: None }, 101)
        .unwrap();
    assert_eq!(
        result.get("certainty").unwrap().as_str(),
        Some("incomplete")
    );
    assert_eq!(
        result
            .get("coverage")
            .unwrap()
            .get("failed_json_records")
            .unwrap()
            .as_str(),
        Some("1")
    );
    let pin = store
        .pin("p", "evidence", &artifact.record_ids[1..], 40, 101)
        .unwrap();
    store.expire(100 + 30 * DAY).unwrap();
    let retained = store
        .json_query(&scope, &JsonQuery::Keys { path: None }, 100 + 30 * DAY)
        .unwrap();
    assert_eq!(
        retained
            .get("coverage")
            .unwrap()
            .get("eligible_records")
            .unwrap()
            .as_str(),
        Some("1")
    );
    store.release(&pin.id).unwrap();
    store.expire(100 + 30 * DAY).unwrap();
    assert!(
        store
            .json_query(&scope, &JsonQuery::Keys { path: None }, 100 + 30 * DAY)
            .unwrap()
            .get("rows")
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn query_budget_and_row_limit_do_not_claim_complete_coverage() {
    use sparrow_core::{JsonQuery, QueryScope};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .ingest("a", "source", "json", br#"{"x":1,"x":2}"#, 100)
        .unwrap();
    let mut scope = QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: None,
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 200,
        limit: 1,
        scan_budget: 100,
    };
    let query = JsonQuery::Values {
        path: Some("/x".into()),
        kind: None,
        lexical: None,
        decoded: None,
    };
    let result = store.json_query(&scope, &query, 101).unwrap();
    assert_eq!(result.get("execution").unwrap().as_str(), Some("limited"));
    assert_eq!(result.get("rows").unwrap().as_array().unwrap().len(), 1);
    scope.scan_budget = 1;
    let result = store.json_query(&scope, &query, 101).unwrap();
    assert_eq!(
        result.get("certainty").unwrap().as_str(),
        Some("incomplete")
    );
    assert!(result.get("rows").unwrap().as_array().unwrap().is_empty());
}

#[test]
fn bytes_occurrences_and_retry_receipts_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = b"{\"a\":1}\r\n\n{\"a\":2}";
    let (id, artifact) = {
        let store = Store::open(dir.path(), config()).unwrap();
        let artifact = store
            .ingest("first", "source", "jsonl", bytes, 100)
            .unwrap();
        assert_eq!(artifact.record_ids.len(), 3);
        assert_eq!(
            store.bytes(&artifact.id, 0, bytes.len(), 101).unwrap(),
            bytes
        );
        assert_eq!(
            store.bytes(&artifact.record_ids[0], 0, 9, 101).unwrap(),
            b"{\"a\":1}\r\n"
        );
        assert_eq!(
            store.bytes(&artifact.record_ids[1], 0, 1, 101).unwrap(),
            b"\n"
        );
        assert_eq!(
            store.bytes(&artifact.record_ids[2], 0, 7, 101).unwrap(),
            b"{\"a\":2}"
        );
        assert_eq!(
            store
                .ingest("first", "source", "jsonl", bytes, 200)
                .unwrap()
                .id,
            artifact.id
        );
        assert_eq!(
            store
                .ingest("first", "source", "jsonl", b"changed", 200)
                .unwrap_err()
                .code,
            "CONFLICT"
        );
        let second = store
            .ingest("second", "source", "jsonl", bytes, 200)
            .unwrap();
        assert_ne!(second.id, artifact.id);
        assert_eq!(second.payload_id, artifact.payload_id);
        assert_eq!(
            store.status(201).unwrap()["unique_payload_chunks"].as_str(),
            Some("3")
        );
        store.flush().unwrap();
        (store.store_id().unwrap(), artifact)
    };
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(store.store_id().unwrap(), id);
    assert_eq!(
        store.bytes(&artifact.id, 0, bytes.len(), 300).unwrap(),
        bytes
    );
    assert_eq!(
        store
            .ingest("first", "source", "jsonl", bytes, 400)
            .unwrap()
            .record_ids,
        artifact.record_ids
    );
    assert_eq!(store.records(401).unwrap().len(), 6);
}

#[test]
fn record_pin_retains_only_selected_evidence_after_parent_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest("file", "source", "log", b"keep\r\nremove\n", 0)
        .unwrap();
    let pin = store
        .pin(
            "pin-1",
            "investigation",
            &[artifact.record_ids[0].clone()],
            10,
            29 * DAY,
        )
        .unwrap();
    assert_eq!(pin.expires_at, 39 * DAY);
    assert_eq!(store.expire(30 * DAY).unwrap(), 2);
    assert_eq!(
        store.bytes(&artifact.id, 0, 1, 30 * DAY).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    assert_eq!(
        store
            .bytes(&artifact.record_ids[0], 0, 6, 30 * DAY)
            .unwrap(),
        b"keep\r\n"
    );
    assert_eq!(
        store
            .bytes(&artifact.record_ids[1], 0, 1, 30 * DAY)
            .unwrap_err()
            .code,
        "EVIDENCE_UNAVAILABLE"
    );
    assert_eq!(store.records(30 * DAY).unwrap().len(), 1);
    assert_eq!(
        store
            .pin("late", "resurrect", &[artifact.id], 1, 30 * DAY)
            .unwrap_err()
            .code,
        "EVIDENCE_UNAVAILABLE"
    );
    store.release(&pin.id).unwrap();
    assert!(store.records(30 * DAY).unwrap().is_empty());
    store.expire(30 * DAY).unwrap();
    assert_eq!(
        store.status(30 * DAY).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
}

#[test]
fn overlapping_pins_extend_without_renewal_on_retry_and_name_reuse_is_safe() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store.ingest("file", "source", "log", b"line\n", 0).unwrap();
    let targets = vec![artifact.record_ids[0].clone()];
    let first = store.pin("create", "case", &targets, 40, 0).unwrap();
    let retry = store.pin("create", "case", &targets, 40, DAY).unwrap();
    assert_eq!(retry.expires_at, 40 * DAY);
    assert_eq!(retry.revision, 1);
    let union = store.pin("union", "case", &targets, 1, DAY).unwrap();
    assert_eq!(union.id, first.id);
    assert_eq!(union.expires_at, 40 * DAY);
    let other = store.pin("other", "other", &targets, 50, 0).unwrap();
    store.release(&first.id).unwrap();
    store.expire(31 * DAY).unwrap();
    assert_eq!(store.records(31 * DAY).unwrap().len(), 1);
    let replacement = store
        .pin("replacement", "case", &targets, 1, 31 * DAY)
        .unwrap();
    assert_ne!(first.id, replacement.id);
    assert_eq!(store.release(&first.id).unwrap_err().code, "NOT_FOUND");
    store.release(&other.id).unwrap();
    assert_eq!(store.records(32 * DAY).unwrap().len(), 0);
}

#[test]
fn pin_validation_is_atomic_and_complete_artifact_pins_cover_records() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest("file", "source", "log", b"one\ntwo\n", 0)
        .unwrap();
    assert_eq!(
        store
            .pin(
                "bad",
                "case",
                &[artifact.id.clone(), "record:missing".into()],
                40,
                0
            )
            .unwrap_err()
            .code,
        "NOT_FOUND"
    );
    assert_eq!(store.status(1).unwrap()["active_pins"].as_str(), Some("0"));
    store
        .pin("whole", "case", std::slice::from_ref(&artifact.id), 40, 0)
        .unwrap();
    assert_eq!(store.expire(30 * DAY).unwrap(), 0);
    assert_eq!(store.records(30 * DAY).unwrap().len(), 2);
    assert_eq!(
        store.bytes(&artifact.id, 0, 8, 30 * DAY).unwrap(),
        b"one\ntwo\n"
    );
    store.expire(40 * DAY).unwrap();
    assert!(store.records(40 * DAY).unwrap().is_empty());
}

#[test]
fn shared_content_remains_until_last_occurrence_expires_and_ranges_are_exact() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let first = store
        .ingest("first", "source", "opaque", &vec![0xff; 150_000], 0)
        .unwrap();
    let later = store
        .ingest("later", "source", "opaque", &vec![0xff; 150_000], DAY)
        .unwrap();
    assert_eq!(
        store.bytes(&later.id, 60_000, 80_000, DAY).unwrap(),
        vec![0xff; 80_000]
    );
    assert_eq!(
        store.bytes(&later.id, 149_999, 2, DAY).unwrap_err().code,
        "INVALID_ARGUMENT"
    );
    assert_eq!(
        store.bytes(&later.id, u64::MAX, 1, DAY).unwrap_err().code,
        "INVALID_ARGUMENT"
    );
    store.expire(30 * DAY).unwrap();
    assert_eq!(
        store.bytes(&first.id, 0, 1, 30 * DAY).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    assert_eq!(store.bytes(&later.id, 0, 1, 30 * DAY).unwrap(), vec![0xff]);
    store.expire(31 * DAY).unwrap();
    assert_eq!(
        store.status(31 * DAY).unwrap()["unique_payload_chunks"].as_str(),
        Some("0")
    );
}

#[test]
fn bounded_admission_refuses_work_without_publishing_and_control_remains_available() {
    let dir = tempfile::tempdir().unwrap();
    let mut limits = config();
    limits.max_records = 1;
    let store = Store::open(dir.path(), limits).unwrap();
    // One inline batch is bounded; the staged import carries larger artifacts.
    let inline = vec![0; sparrow_core::INLINE_ARTIFACT_BYTES + 1];
    assert_eq!(
        store
            .ingest("too-big", "source", "opaque", &inline, 0)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store
            .ingest("too-many", "source", "log", b"a\nb\n", 0)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert!(store.records(0).unwrap().is_empty());
    let artifact = store.ingest("one", "source", "opaque", b"1234", 0).unwrap();
    let pin = store.pin("pin", "case", &[artifact.id], 1, 0).unwrap();
    store.release(&pin.id).unwrap();
    assert!(store.status(0).is_ok());
}

#[test]
fn combined_records_qualify_before_ranking_and_preserve_native_parent_identity() {
    use sparrow_core::{FieldConstraint, JsonPredicate, QueryScope, RecordQuery};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let bytes = b"{\"job\":\"target\",\"message\":\"success\"}\n{\"job\":\"other\",\"message\":\"failed\"}\n{\"job\":\"target\",\"message\":\"failed\"}\n";
    let artifact = store
        .ingest("combined", "logs", "jsonl", bytes, 10)
        .unwrap();
    let scope = QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: Some("logs".into()),
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 10,
        received_before: 11,
        limit: 100,
        scan_budget: 10000,
    };
    let mut query = RecordQuery {
        text: Some("failed".into()),
        fields: vec![FieldConstraint {
            predicate: JsonPredicate {
                path: Some("/job".into()),
                decoded: Some("target".into()),
                ..Default::default()
            },
            ..Default::default()
        }],
        ..Default::default()
    };
    let result = store.records_query(&scope, &query, 10).unwrap();
    assert_eq!(result["ranking"]["documents"].as_str(), Some("2"));
    assert_eq!(result["rows"].as_array().unwrap().len(), 1);
    assert_eq!(
        result["rows"][0]["record_id"].as_str(),
        Some(artifact.record_ids[2].as_str())
    );
    let witness = &result["rows"][0]["fields"][0];
    let start = witness["source_start"]
        .as_str()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let end = witness["source_end"]
        .as_str()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    assert_eq!(&bytes[start..end], b"target");
    query.text = None;
    let chronological = store.records_query(&scope, &query, 10).unwrap();
    assert_eq!(chronological["rows"].as_array().unwrap().len(), 2);
    assert!(chronological.get("ranking").is_none());
    assert!(chronological["rows"][0].get("score").is_none());
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.records_query(&scope, &query, 10).unwrap(),
        chronological
    );

    // Equal pointers in duplicate object members do not identify the same parent.
    store
        .ingest(
            "parents",
            "parents",
            "json",
            br#"{"row":{"job":"target","status":"ok"},"row":{"job":"other","status":"failed"}}"#,
            20,
        )
        .unwrap();
    let scope = QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: Some("parents".into()),
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 20,
        received_before: 21,
        ..scope
    };
    query.fields = vec![
        FieldConstraint {
            predicate: JsonPredicate {
                path: Some("/row/job".into()),
                decoded: Some("target".into()),
                ..Default::default()
            },
            parent_binding: Some("row".into()),
            parent_path: None,
        },
        FieldConstraint {
            predicate: JsonPredicate {
                path: Some("/row/status".into()),
                decoded: Some("failed".into()),
                ..Default::default()
            },
            parent_binding: Some("row".into()),
            parent_path: None,
        },
    ];
    assert!(
        store.records_query(&scope, &query, 20).unwrap()["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    for constraint in &mut query.fields {
        constraint.parent_binding = None;
    }
    assert_eq!(
        store.records_query(&scope, &query, 20).unwrap()["rows"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    store.ingest("nested-parent", "nested", "json", br#"{"items":[{"job":"target","detail":{"status":"failed"}},{"job":"other","detail":{"status":"ok"}}]}"#, 30).unwrap();
    let scope = QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: Some("nested".into()),
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 30,
        received_before: 31,
        ..scope
    };
    query.fields[0].predicate.path = Some("/items/0/job".into());
    query.fields[1].predicate.path = Some("/items/0/detail/status".into());
    for constraint in &mut query.fields {
        constraint.parent_binding = Some("item".into());
        constraint.parent_path = Some("/items/0".into());
    }
    let result = store.records_query(&scope, &query, 30).unwrap();
    assert_eq!(result["rows"].as_array().unwrap().len(), 1);
    assert_eq!(
        result["rows"][0]["fields"][0]["bound_parent"],
        result["rows"][0]["fields"][1]["bound_parent"]
    );
}

#[test]
fn combined_records_report_index_coverage_and_refuse_exhausted_field_work() {
    use sparrow_core::{FieldConstraint, JsonPredicate, QueryScope, RecordQuery};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .ingest(
            "spellings",
            "test",
            "json",
            br#"{"n":1e0,"n":1.00,"s":"\u0068i"}"#,
            1,
        )
        .unwrap();
    store
        .ingest("failed", "test", "jsonl", b"{invalid}\n", 2)
        .unwrap();
    store.ingest("raw", "test", "opaque", b"opaque", 3).unwrap();
    let mut scope = QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: Some("test".into()),
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 1,
        received_before: 4,
        limit: 100,
        scan_budget: 10000,
    };
    let mut query = RecordQuery {
        fields: vec![
            FieldConstraint {
                predicate: JsonPredicate {
                    path: Some("/n".into()),
                    lexical: Some("1e0".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
            FieldConstraint {
                predicate: JsonPredicate {
                    path: Some("/s".into()),
                    decoded: Some("hi".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let result = store.records_query(&scope, &query, 3).unwrap();
    assert_eq!(result["rows"].as_array().unwrap().len(), 1);
    assert_eq!(
        result["coverage"]["failed_json_records"].as_str(),
        Some("1")
    );
    assert_eq!(result["coverage"]["non_json_records"].as_str(), Some("1"));
    assert_eq!(result["certainty"].as_str(), Some("incomplete"));
    query.require_complete_index = true;
    assert_eq!(
        store.records_query(&scope, &query, 3).unwrap_err().code,
        "INDEX_NOT_READY"
    );
    query.require_complete_index = false;
    scope.scan_budget = 2;
    assert_eq!(
        store.records_query(&scope, &query, 3).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
    scope.scan_budget = 10000;
    scope.received_before = 1;
    assert_eq!(
        store.records_query(&scope, &query, 3).unwrap_err().code,
        "INVALID_ARGUMENT"
    );
    scope.received_before = 4;
    query.fields[0].parent_path = Some("".into());
    assert_eq!(
        store.records_query(&scope, &query, 3).unwrap_err().code,
        "INVALID_ARGUMENT"
    );
}

#[test]
fn source_time_profiles_are_durable_immutable_and_do_not_reinterpret_old_records() {
    use sparrow_core::{EventTime, TimeRepresentation};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let bytes = br#"{"timestamp":"2026-10-07T05:00:00.123+02:00","message":"historical failed"}"#;
    let before = store
        .ingest("before-profile", "historical", "json", bytes, 100)
        .unwrap();
    assert!(
        matches!(store.record(&before.record_ids[0]).unwrap().event_time, EventTime::Unknown { ref reason, profile_id: None } if reason == "no_profile")
    );
    let profile = store
        .define_time_profile("historical", "/timestamp", TimeRepresentation::Rfc3339)
        .unwrap();
    assert_eq!(
        store
            .define_time_profile("historical", "/timestamp", TimeRepresentation::Rfc3339)
            .unwrap(),
        profile
    );
    assert_eq!(
        store
            .define_time_profile("historical", "/timestamp", TimeRepresentation::UnixMillis)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert_eq!(
        store
            .define_time_profile("bad", "/bad~2", TimeRepresentation::Rfc3339)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    let artifact = store
        .ingest("after-profile", "historical", "json", bytes, 200)
        .unwrap();
    let record = store.record(&artifact.record_ids[0]).unwrap();
    let expected = sparrow_core::parse_instant("2026-10-07T03:00:00.123Z").unwrap();
    let EventTime::Known {
        millis,
        profile_id,
        occurrence,
        source_start,
        source_end,
    } = &record.event_time
    else {
        panic!("known source time");
    };
    assert_eq!(*millis, expected);
    assert_eq!(*profile_id, profile.id);
    assert_eq!(*occurrence, 1);
    assert_eq!(
        &bytes[*source_start as usize..*source_end as usize],
        b"2026-10-07T05:00:00.123+02:00"
    );
    assert_eq!(record.received_at, 200);
    assert_eq!(record.expires_at, 30 * DAY + 200);
    let native = store.with_rdf_records(&[before.record_ids[0].clone(), record.id.clone()], 300,
        sparrow_core::RdfReadLimits::default(), |view| {
            purrdf::sparql::NativeSparqlEngine::new().query_governed_fallible_view(view,
                purrdf::SparqlRequest { query: "SELECT ?record ?time ?profile WHERE { ?record <urn:sparrow:vocab:eventTime> ?time . ?record <urn:sparrow:vocab:sourceTimeProfile> ?profile }", base_iri: None, substitutions: &[] },
                purrdf::sparql::QueryOptions::default(), &purrdf::sparql::QueryGovernors::METERED.with_fuel(1000000)
                    .with_max_answers(100).with_max_intermediate_cells(10000).with_max_scratch_bytes(1024 * 1024).with_max_remote_requests(0))
                    .map_err(Box::new)
        }).unwrap().unwrap();
    let (_, rows) = native.result.solutions().expect("native SELECT result");
    assert_eq!(rows.len(), 1);
    assert!(format!("{rows:?}").contains("2026-10-07T03:00:00.123Z"));
    assert!(format!("{rows:?}").contains(&profile.id));
    assert_eq!(
        store
            .ingest("before-profile", "historical", "json", bytes, 300)
            .unwrap()
            .id,
        before.id
    );
    assert!(
        store
            .record(&before.record_ids[0])
            .unwrap()
            .event_time
            .millis()
            .is_none()
    );
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(store.time_profile("historical").unwrap(), Some(profile));
    assert_eq!(
        store.record(&record.id).unwrap().event_time,
        record.event_time
    );
    assert!(store.time_profile("missing").unwrap().is_none());
    assert_eq!(
        store.bytes(&artifact.id, 0, bytes.len(), 300).unwrap(),
        bytes
    );
}

#[test]
fn event_intervals_exclude_unknowns_sort_source_time_and_keep_retention_on_acceptance() {
    use sparrow_core::{EventInterval, EventTime, QueryScope, RecordQuery, TimeRepresentation};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .define_time_profile("events", "/t", TimeRepresentation::UnixMillis)
        .unwrap();
    let later = store
        .ingest(
            "event-later",
            "events",
            "json",
            br#"{"t":2000,"m":"failed"}"#,
            10,
        )
        .unwrap();
    let early = store
        .ingest(
            "event-early",
            "events",
            "json",
            br#"{"t":1000,"m":"failed"}"#,
            20,
        )
        .unwrap();
    let unknowns: [&[u8]; 7] = [
        br#"{"t":1000,"t":1000}"#,
        br#"{"missing":1}"#,
        br#"{"t":"1000"}"#,
        br#"{"t":1.1}"#,
        br#"{"t":-1}"#,
        br#"{"t":18446744073709551615}"#,
        b"{invalid}",
    ];
    for (index, bytes) in unknowns.iter().enumerate() {
        let artifact = store
            .ingest(
                &format!("event-unknown-{index}"),
                "events",
                "json",
                bytes,
                30,
            )
            .unwrap();
        assert!(matches!(
            store.record(&artifact.record_ids[0]).unwrap().event_time,
            EventTime::Unknown {
                profile_id: Some(_),
                ..
            }
        ));
    }
    let scope = QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: Some("events".into()),
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 100,
        limit: 1,
        scan_budget: 10000,
    };
    let mut query = RecordQuery {
        event_interval: Some(EventInterval {
            from: 1000,
            before: 2001,
        }),
        ..Default::default()
    };
    let result = store.records_query(&scope, &query, 100).unwrap();
    assert_eq!(
        result["rows"][0]["record_id"].as_str(),
        Some(early.record_ids[0].as_str())
    );
    assert_eq!(result["time_basis"].as_str(), Some("event_time"));
    assert_eq!(result["event_coverage"]["unknown"].as_str(), Some("7"));
    assert_eq!(
        result["event_coverage"]["membership_complete"].as_bool(),
        Some(true)
    );
    query.require_complete_index = true;
    assert_eq!(
        store.records_query(&scope, &query, 100).unwrap_err().code,
        "INDEX_NOT_READY"
    );
    query.require_complete_index = false;
    query.event_interval = Some(EventInterval {
        from: 1000,
        before: 2000,
    });
    query.text = Some("failed".into());
    let result = store.records_query(&scope, &query, 100).unwrap();
    assert_eq!(result["ranking"]["documents"].as_str(), Some("1"));
    assert_eq!(
        result["rows"][0]["record_id"].as_str(),
        Some(early.record_ids[0].as_str())
    );
    assert_eq!(
        result["event_coverage"]["outside_interval"].as_str(),
        Some("1")
    );
    let receipt_only = RecordQuery::default();
    assert_eq!(
        store.records_query(&scope, &receipt_only, 100).unwrap()["rows"][0]["record_id"].as_str(),
        Some(later.record_ids[0].as_str())
    );
    let pin = store
        .pin("event-pin", "historical", &early.record_ids, 40, 0)
        .unwrap();
    store.expire(30 * DAY + 100).unwrap();
    assert_eq!(
        store.records_query(&scope, &query, 30 * DAY + 100).unwrap()["rows"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    store.release(&pin.id).unwrap();
    assert!(
        store.records_query(&scope, &query, 30 * DAY + 100).unwrap()["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn declared_epoch_seconds_and_native_rfc3339_refuse_ambiguous_precision() {
    use sparrow_core::{EventTime, TimeRepresentation};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .define_time_profile("seconds", "/t", TimeRepresentation::UnixSeconds)
        .unwrap();
    let a = store
        .ingest("seconds", "seconds", "json", br#"{"t":1000}"#, 0)
        .unwrap();
    assert_eq!(
        store.record(&a.record_ids[0]).unwrap().event_time.millis(),
        Some(1000000)
    );
    store
        .define_time_profile("rfc", "/t", TimeRepresentation::Rfc3339)
        .unwrap();
    for (index, timestamp) in [
        "2026-10-07T05:00:00",
        "2026-10-07T05:00:00.000001Z",
        "1960-01-01T00:00:00Z",
    ]
    .iter()
    .enumerate()
    {
        let bytes = purrdf_lex::json::write_compact(
            &purrdf_lex::json::Object::new().with("t", *timestamp).into(),
        );
        let a = store
            .ingest(
                &format!("invalid-clock-{index}"),
                "rfc",
                "json",
                bytes.as_bytes(),
                0,
            )
            .unwrap();
        assert!(
            matches!(store.record(&a.record_ids[0]).unwrap().event_time, EventTime::Unknown { ref reason, .. } if reason == "invalid_instant")
        );
    }
}

#[test]
fn legacy_time_absence_is_explicit_and_broken_declared_profiles_refuse_queries() {
    use purrdf_lex::json::{self, Limits};
    use sparrow_core::{EventInterval, EventTime, QueryScope, RecordQuery, TimeRepresentation};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let legacy = store
        .ingest("legacy", "legacy", "json", br#"{"t":1000}"#, 0)
        .unwrap();
    let profile = store
        .define_time_profile("known", "/t", TimeRepresentation::UnixMillis)
        .unwrap();
    let known = store
        .ingest("known", "known", "json", br#"{"t":1000}"#, 0)
        .unwrap();
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    let mut raw = json::read_slice(
        &db.get(&legacy.record_ids[0]).unwrap().unwrap(),
        Limits::default(),
    )
    .unwrap();
    raw.as_object_mut().unwrap().remove("event_time");
    db.put(&legacy.record_ids[0], json::write_compact(&raw))
        .unwrap();
    let profile_key = format!(
        "source-time:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(b"known").as_bytes())
    );
    let saved = db.get(&profile_key).unwrap().unwrap();
    db.delete(&profile_key).unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert!(
        matches!(store.record(&legacy.record_ids[0]).unwrap().event_time, EventTime::Unknown { ref reason, profile_id: None } if reason == "legacy_unprofiled")
    );
    assert_eq!(
        store.record(&known.record_ids[0]).unwrap_err().code,
        "STORAGE_FAILURE"
    );
    let scope = QueryScope {
        selection: sparrow_core::QuerySelection::Live,
        source: None,
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 1,
        limit: 100,
        scan_budget: 10000,
    };
    let query = RecordQuery {
        event_interval: Some(EventInterval {
            from: 0,
            before: 2000,
        }),
        ..Default::default()
    };
    assert_eq!(
        store.records_query(&scope, &query, 0).unwrap_err().code,
        "STORAGE_FAILURE"
    );
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    let mut raw = json::read_slice(&saved, Limits::default()).unwrap();
    raw.as_object_mut().unwrap().insert("path", "/different");
    db.put(&profile_key, json::write_compact(&raw)).unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store.time_profile("known").unwrap_err().code,
        "STORAGE_FAILURE"
    );
    assert_eq!(
        store
            .define_time_profile("known", "/t", TimeRepresentation::UnixMillis)
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
    assert_eq!(profile.source, "known");
}

#[test]
fn source_time_registry_has_an_exact_admission_ceiling() {
    use sparrow_core::TimeRepresentation;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    for index in 0..1024 {
        store
            .define_time_profile(
                &format!("source-{index}"),
                "/t",
                TimeRepresentation::UnixMillis,
            )
            .unwrap();
    }
    assert_eq!(
        store
            .define_time_profile("overflow", "/t", TimeRepresentation::UnixMillis)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert!(store.time_profile("overflow").unwrap().is_none());
    assert!(
        store
            .define_time_profile("source-0", "/t", TimeRepresentation::UnixMillis)
            .is_ok()
    );
}

#[test]
fn source_time_registry_pressure_is_retryable_without_publishing_a_mapping() {
    use purrdf_lex::json::record::ToJson;
    let dir = tempfile::tempdir().unwrap();
    let settings = config();
    let store = Store::open(dir.path(), settings.clone()).unwrap();
    let ballast_path = dir.path().join("synthetic-admission-ballast");
    let ballast = std::fs::File::create(&ballast_path).unwrap();
    ballast.set_len(settings.disk_budget).unwrap();
    let error = store
        .define_time_profile(
            "pressure",
            "/event_time",
            sparrow_core::TimeRepresentation::UnixMillis,
        )
        .unwrap_err();
    assert_eq!(error.code, "STORAGE_PRESSURE");
    assert_eq!(error.to_json()["retryable"].as_bool(), Some(true));
    assert!(store.time_profile("pressure").unwrap().is_none());
    drop(ballast);
    std::fs::remove_file(ballast_path).unwrap();
    store
        .define_time_profile(
            "pressure",
            "/event_time",
            sparrow_core::TimeRepresentation::UnixMillis,
        )
        .unwrap();
}

#[test]
fn artifact_diagnostic_filters_preserve_unknown_counts_and_original_receipt_scope() {
    use sparrow_core::{
        ArtifactRead, ArtifactSelector, DiagnosticDimension as D, QueryAlternatives, QuerySelection,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .define_diagnostic_profile("mapped", [(D::Service, "/svc".into())].into())
        .unwrap();
    let a = store
        .ingest(
            "artifact-filter-a",
            "mapped",
            "jsonl",
            b"{\"svc\":\"worker\"}\n{}\n{\"svc\":\"other\"}\n",
            10,
        )
        .unwrap();
    let unknown = store
        .ingest("artifact-filter-unknown", "mapped", "jsonl", b"{}\n", 11)
        .unwrap();
    let other = store
        .ingest("artifact-filter-other", "other", "jsonl", b"{}\n", 12)
        .unwrap();
    let empty = store
        .ingest("artifact-filter-empty", "mapped", "jsonl", b"", 13)
        .unwrap();
    let mut query = ArtifactRead {
        selector: ArtifactSelector {
            sources: Some(QueryAlternatives::new(vec!["mapped".into()]).unwrap()),
            diagnostics: [(
                D::Service,
                QueryAlternatives::new(vec!["worker".into()]).unwrap(),
            )]
            .into(),
            ..Default::default()
        },
        ..Default::default()
    };
    let page = store.artifact_read(&query, 20).unwrap();
    assert_eq!(page.total_items, 1);
    assert_eq!(page.body.artifacts[0].id, a.id);
    assert_eq!(page.body.artifacts[0].retained_records, 3);
    assert_eq!(page.body.artifacts[0].diagnostic_coverage.matched, 1);
    assert_eq!(page.body.artifacts[0].diagnostic_coverage.unknown, 1);
    assert_eq!(page.body.diagnostic_coverage.evaluated, 4);
    assert_eq!(page.body.diagnostic_coverage.excluded, 1);
    assert_eq!(page.body.diagnostic_coverage.unknown, 2);
    assert!(!page.body.diagnostic_coverage.complete(false));
    query.selector.require_complete_index = true;
    assert_eq!(
        store.artifact_read(&query, 20).unwrap_err().code,
        "INDEX_NOT_READY"
    );
    query.selector.received_from = 12;
    assert!(
        store
            .artifact_read(&query, 20)
            .unwrap()
            .body
            .artifacts
            .is_empty()
    );
    query.selector.received_from = 0;
    query.selector.selection = QuerySelection::Evidence(vec![other.id.clone()]);
    assert!(
        store
            .artifact_read(&query, 20)
            .unwrap()
            .body
            .artifacts
            .is_empty()
    );
    query.selector.selection = QuerySelection::Evidence(vec![empty.id]);
    assert!(
        store
            .artifact_read(&query, 20)
            .unwrap()
            .body
            .artifacts
            .is_empty()
    );
    query.selector.selection = QuerySelection::Evidence(vec!["artifact:missing".into()]);
    assert_eq!(
        store.artifact_read(&query, 20).unwrap_err().code,
        "NOT_FOUND"
    );
    query.selector.require_complete_index = false;
    query.selector.selection = QuerySelection::Evidence(vec![unknown.id]);
    let only_unknown = store.artifact_read(&query, 20).unwrap();
    assert!(only_unknown.body.artifacts.is_empty());
    assert_eq!(only_unknown.body.diagnostic_coverage.unknown, 1);
    query.selector.selection = QuerySelection::Evidence(vec![a.record_ids[0].clone()]);
    query.selector.require_complete_index = true;
    let known = store.artifact_read(&query, 20).unwrap();
    assert_eq!(known.body.artifacts[0].retained_records, 1);
    assert_eq!(known.body.diagnostic_coverage.matched, 1);
    assert!(known.body.diagnostic_coverage.complete(false));
}

#[test]
fn artifact_filter_pages_freeze_coverage_and_bind_all_filters() {
    use sparrow_core::{
        ArtifactRead, ArtifactSelector, DiagnosticDimension as D, QueryAlternatives,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .define_diagnostic_profile("mapped", [(D::Service, "/svc".into())].into())
        .unwrap();
    for request in ["artifact-filter-page-a", "artifact-filter-page-b"] {
        store
            .ingest(
                request,
                "mapped",
                "jsonl",
                b"{\"svc\":\"worker\"}\n{}\n",
                10,
            )
            .unwrap();
    }
    let mut query = ArtifactRead {
        selector: ArtifactSelector {
            diagnostics: [(
                D::Service,
                QueryAlternatives::new(vec!["worker".into()]).unwrap(),
            )]
            .into(),
            ..Default::default()
        },
        page_size: 1,
        ..Default::default()
    };
    let first = store.artifact_read(&query, 20).unwrap();
    query.cursor = first.next_cursor.clone();
    store
        .ingest("artifact-filter-page-new", "mapped", "jsonl", b"{}\n", 21)
        .unwrap();
    let second = store.artifact_read(&query, 22).unwrap();
    assert_eq!(
        second.body.diagnostic_coverage,
        first.body.diagnostic_coverage
    );
    assert_eq!(second.total_items, 2);
    query.selector.require_complete_index = true;
    assert_eq!(
        store.artifact_read(&query, 22).unwrap_err().code,
        "CONFLICT"
    );
    query.selector.require_complete_index = false;
    query.selector.sources = Some(QueryAlternatives::new(vec!["mapped".into()]).unwrap());
    assert_eq!(
        store.artifact_read(&query, 22).unwrap_err().code,
        "CONFLICT"
    );
}

#[test]
fn native_sparql_pages_bound_handle_capacity_and_expire_without_renewal() {
    use purrdf::{SparqlRequest, sparql::QueryGovernors};
    use sparrow_core::{QueryCancellation, RdfGraphLayers, RdfReadLimits, SparqlRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest("native-page-handles", "scoped", "jsonl", b"{}\n{}\n", 1)
        .unwrap();
    let scope = record_page_scope("scoped", 1);
    let governors = QueryGovernors::METERED
        .with_fuel(1_000_000)
        .with_max_answers(100)
        .with_max_scratch_bytes(1024 * 1024);
    let read = SparqlRead {
        text_relations: None,
        scope: &scope,
        layers: RdfGraphLayers::ALL,
        request: SparqlRequest {
            query: "SELECT ?r WHERE { ?r <urn:sparrow:vocab:payload> ?p }",
            base_iri: None,
            substitutions: &[],
        },
        governors: &governors,
        limits: RdfReadLimits::default(),
    };
    let cancellation = QueryCancellation::default();
    let pages = (0..8)
        .map(|_| store.sparql_read(&read, 2, &cancellation).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        store.status(2).unwrap()["sparql_read_handles"].as_str(),
        Some("8")
    );
    assert_eq!(
        store.sparql_read(&read, 2, &cancellation).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
    assert!(
        store
            .result_release(&pages[0].store_id, &pages[0].snapshot_id, 3)
            .unwrap()
    );
    let replacement = store.sparql_read(&read, 3, &cancellation).unwrap();
    let cursor = replacement.next_cursor.as_ref().unwrap();
    let resumed = store
        .sparql_read_next(&replacement.store_id, cursor, 4)
        .unwrap();
    assert_eq!(resumed.lease_expires_at, replacement.lease_expires_at);
    assert_eq!(
        store
            .sparql_read_next(&replacement.store_id, cursor, replacement.lease_expires_at)
            .unwrap_err()
            .code,
        "RESULT_EXPIRED"
    );
    assert_eq!(
        store.status(replacement.lease_expires_at).unwrap()["sparql_read_handles"].as_str(),
        Some("0")
    );
    assert_eq!(
        store.status(replacement.lease_expires_at).unwrap()["sparql_read_bytes"].as_str(),
        Some("0")
    );
    assert_eq!(
        store.record(&artifact.record_ids[0]).unwrap().id,
        artifact.record_ids[0]
    );
}

#[test]
fn native_sparql_pages_freeze_byte_boundaries_and_release_original_leases() {
    use purrdf::{SparqlRequest, TermValue, sparql::QueryGovernors};
    use sparrow_core::{QueryCancellation, RdfGraphLayers, RdfReadLimits, SparqlItem, SparqlRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest("native-page-three", "scoped", "jsonl", b"{}\n{}\n{}\n", 1)
        .unwrap();
    let pin = store
        .pin("native-page-pin", "native-pages", &[artifact.id], 40, 1)
        .unwrap();
    let now = 31 * DAY;
    let scope = record_page_scope("scoped", 1000);
    let governors = QueryGovernors::METERED
        .with_fuel(1_000_000)
        .with_max_answers(100)
        .with_max_intermediate_cells(10_000)
        .with_max_scratch_bytes(1024 * 1024);
    let bindings = [(
        "value".into(),
        TermValue::iri(format!("urn:large:{}", "x".repeat(17000))),
    )];
    let read = SparqlRead {
        text_relations: None,
        scope: &scope,
        layers: RdfGraphLayers::ALL,
        request: SparqlRequest {
            query: "SELECT ?record ?value WHERE { ?record a <urn:sparrow:vocab:Record> ; <urn:sparrow:vocab:payload> ?payload } ORDER BY ?record",
            base_iri: None,
            substitutions: &bindings,
        },
        governors: &governors,
        limits: RdfReadLimits::default(),
    };
    let first = store
        .sparql_read(&read, now, &QueryCancellation::default())
        .unwrap();
    assert_eq!(first.total_items, 3);
    assert_eq!(first.body.items().count(), 2); // byte ceiling, despite page-size=1000
    let cursor = first.next_cursor.as_ref().unwrap();
    let expected = store
        .sparql_read_next(&first.store_id, cursor, now + 1)
        .unwrap();
    assert_eq!(expected.body.items().count(), 1);
    assert!(expected.next_cursor.is_none());
    for item in expected.body.items() {
        let SparqlItem::Solution(row) = item else {
            panic!("SELECT row expected");
        };
        assert_eq!(row[1].as_ref(), Some(&bindings[0].1));
    }
    store
        .release_retry("native-page-unpin", &pin.id, now + 2)
        .unwrap();
    store.expire(now + 3).unwrap();
    store
        .ingest("native-page-later", "scoped", "json", b"{}", now + 3)
        .unwrap();
    assert_eq!(
        store
            .sparql_read_next(&first.store_id, cursor, now + 4)
            .unwrap(),
        expected
    );
    assert_eq!(
        store
            .sparql_read_next(
                &first.store_id,
                &format!("{}/1", first.snapshot_id),
                now + 4
            )
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    assert_eq!(
        store
            .sparql_read_next("store:foreign", cursor, now + 4)
            .unwrap_err()
            .code,
        "STORE_MISMATCH"
    );
    assert!(
        store
            .result_release(&first.store_id, &first.snapshot_id, now + 5)
            .unwrap()
    );
    assert_eq!(
        store
            .sparql_read_next(&first.store_id, cursor, now + 6)
            .unwrap_err()
            .code,
        "RESULT_EXPIRED"
    );
}

#[test]
fn native_sparql_pages_preserve_unknown_and_query_failures_without_rows() {
    use purrdf::{
        SparqlRequest,
        sparql::{FallibleSparqlError, PartialAnswers, QueryGovernors},
    };
    use sparrow_core::{QueryCancellation, RdfGraphLayers, RdfReadLimits, SparqlRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .ingest("native-page-unknown", "scoped", "json", b"{}", 1)
        .unwrap();
    let scope = record_page_scope("scoped", 1);
    let governors = QueryGovernors::METERED
        .with_fuel(0)
        .with_max_scratch_bytes(1024 * 1024);
    let mut read = SparqlRead {
        text_relations: None,
        scope: &scope,
        layers: RdfGraphLayers::ALL,
        request: SparqlRequest {
            query: "SELECT (COUNT(*) AS ?count) WHERE { ?r <urn:sparrow:vocab:payload> ?p }",
            base_iri: None,
            substitutions: &[],
        },
        governors: &governors,
        limits: RdfReadLimits::default(),
    };
    let lower = store
        .sparql_read(&read, 2, &QueryCancellation::default())
        .unwrap();
    let Err(FallibleSparqlError::BudgetExhausted {
        partial: PartialAnswers::Certain(partial),
        ..
    }) = &lower.body.captured().outcome
    else {
        panic!("native aggregate must preserve its certified empty lower bound");
    };
    assert!(partial.result().solutions().unwrap().1.is_empty());
    assert_eq!(lower.body.items().count(), 0); // no fabricated COUNT binding
    read.request.query = "ASK WHERE { ?r <urn:sparrow:vocab:payload> ?p }";
    let unknown = store
        .sparql_read(&read, 2, &QueryCancellation::default())
        .unwrap();
    assert!(matches!(
        &unknown.body.captured().outcome,
        Err(FallibleSparqlError::BudgetExhausted {
            partial: PartialAnswers::Unknown(_),
            ..
        })
    ));
    assert_eq!(unknown.body.items().count(), 0);
    assert!(unknown.next_cursor.is_none());
    read.request.query = "SELECT WHERE {";
    let failed = store
        .sparql_read(&read, 2, &QueryCancellation::default())
        .unwrap();
    assert!(matches!(
        &failed.body.captured().outcome,
        Err(FallibleSparqlError::Query { .. })
    ));
    assert_eq!(failed.body.items().count(), 0);
    assert!(failed.next_cursor.is_none());
}

#[test]
fn adopted_native_materialization_retains_complete_cells_after_source_close() {
    use purrdf::{DatasetView, GraphMatch, SparqlRequest};
    use purrdf_lex::json::{self, Object, Value};
    use sparrow_core::{QueryCancellation, RdfGraphLayers, RdfReadLimits, SparqlRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = Object::new().with("unrelated", "x".repeat(512)).with(
        "rows",
        Value::array((0..64_u64).map(|i| {
            Value::from(
                Object::new()
                    .with("value", format!("item-{i}"))
                    .with("ordinal", i),
            )
        })),
    );
    let artifact = store
        .ingest(
            "many-native-cells",
            "native-cells",
            "json",
            json::write_compact(&input.into()).as_bytes(),
            1,
        )
        .unwrap();
    let layers = RdfGraphLayers {
        metadata: false,
        diagnostic: false,
        content: true,
    };
    let expected = store
        .with_rdf_record_layers(
            &artifact.record_ids,
            layers,
            2,
            RdfReadLimits::default(),
            |view| {
                view.quads_for_pattern(None, None, None, GraphMatch::Default)
                    .count()
            },
        )
        .unwrap();
    assert!(
        expected > 500,
        "fixture must require complete materialization of many short cells"
    );
    let scope = record_page_scope("native-cells", 1);
    let governors = purrdf::sparql::QueryGovernors::METERED
        .with_fuel(2_000_000)
        .with_max_answers(10_000)
        .with_max_intermediate_cells(100_000)
        .with_max_scratch_bytes(16 * 1024 * 1024)
        .with_max_remote_requests(0);
    let result = store
        .sparql_query(
            &SparqlRead {
                text_relations: None,
                scope: &scope,
                layers,
                request: SparqlRequest {
                    query: "SELECT ?s ?p ?o WHERE { ?s ?p ?o }",
                    base_iri: None,
                    substitutions: &[],
                },
                governors: &governors,
                limits: RdfReadLimits::default(),
            },
            2,
            &QueryCancellation::default(),
        )
        .unwrap();
    let outcome = result.outcome.as_ref().unwrap();
    assert!(outcome.evidence.governors.is_complete());
    assert!(outcome.evidence.view.peak_live_bytes <= RdfReadLimits::default().live_bytes);
    drop(store);
    let (variables, rows) = result.outcome.as_ref().unwrap().result.solutions().unwrap();
    assert_eq!(variables, &["s", "p", "o"]);
    assert_eq!(rows.len(), expected);
    assert!(
        rows.iter()
            .all(|row| row.len() == 3 && row.iter().all(Option::is_some))
    );
    eprintln!(
        "PASS native persisted materialization: rows={expected}, default 128 MiB workspace, retained after source close"
    );
}

#[test]
fn adopted_native_explain_keeps_bindings_governors_and_retained_ledger() {
    use purrdf::{SparqlRequest, TermValue, sparql::QueryGovernors};
    use purrdf_core::ResourceDimension;
    use sparrow_core::{QueryCancellation, RdfGraphLayers, RdfReadLimits, SparqlRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .ingest("explain-native", "native-explain", "jsonl", b"{}\n{}\n", 1)
        .unwrap();
    let scope = record_page_scope("native-explain", 2);
    let cancellation = QueryCancellation::default();
    let layers = RdfGraphLayers {
        metadata: true,
        diagnostic: false,
        content: false,
    };
    let governors = QueryGovernors::METERED
        .with_fuel(123_456)
        .with_max_answers(33)
        .with_max_intermediate_cells(4444)
        .with_max_scratch_bytes(8 * 1024 * 1024)
        .with_max_remote_requests(0);
    let query =
        "SELECT ?record WHERE { ?record a <urn:sparrow:vocab:Record>; ?predicate ?payload }";
    for (predicate, expected) in [
        ("urn:sparrow:vocab:payload", 2),
        ("urn:absent:predicate", 0),
    ] {
        let substitutions = [("predicate".into(), TermValue::iri(predicate))];
        let read = SparqlRead {
            text_relations: None,
            scope: &scope,
            layers,
            request: SparqlRequest {
                query,
                base_iri: None,
                substitutions: &substitutions,
            },
            governors: &governors,
            limits: RdfReadLimits::default(),
        };
        let captured = store.sparql_explain(&read, 2, &cancellation).unwrap();
        let executed = store.sparql_query(&read, 2, &cancellation).unwrap();
        assert_eq!(captured.scope, executed.scope);
        assert_eq!(captured.sequence, executed.sequence);
        assert_eq!(captured.coverage.selected_records, 2);
        assert_eq!(
            executed
                .outcome
                .as_ref()
                .unwrap()
                .result
                .solutions()
                .unwrap()
                .1
                .len(),
            expected as usize
        );
        let (explanation, receipt) = captured.outcome.as_ref().unwrap();
        assert_eq!(
            receipt.governors.limits(),
            executed
                .outcome
                .as_ref()
                .unwrap()
                .evidence
                .governors
                .limits()
        );
        assert_eq!(explanation.ledger()[0].rows, expected);
        assert_eq!(explanation.evidence().limits(), governors.limits());
        assert_eq!(
            explanation
                .ledger()
                .iter()
                .map(|node| node.fuel_total())
                .sum::<u64>(),
            receipt.governors.consumed_in(ResourceDimension::Fuel)
        );
        assert_eq!(
            explanation.evidence().consumed(),
            receipt.governors.consumed()
        );
    }
    let stopped = governors.with_fuel(0);
    let captured = store
        .sparql_explain(
            &SparqlRead {
                text_relations: None,
                scope: &scope,
                layers,
                request: SparqlRequest {
                    query,
                    base_iri: None,
                    substitutions: &[],
                },
                governors: &stopped,
                limits: RdfReadLimits::default(),
            },
            2,
            &cancellation,
        )
        .unwrap();
    let (explanation, receipt) = captured.outcome.as_ref().unwrap();
    drop(store);
    assert_eq!(explanation.evidence().limits(), stopped.limits());
    assert_eq!(explanation.evidence().limit_for(ResourceDimension::Fuel), 0);
    assert!(explanation.evidence().tripped().is_some());
    assert_eq!(
        explanation.evidence().tripped(),
        receipt.governors.tripped()
    );
    assert_eq!(
        explanation
            .ledger()
            .iter()
            .map(|node| node.fuel_total())
            .sum::<u64>(),
        receipt.governors.consumed_in(ResourceDimension::Fuel)
    );
}

#[test]
fn adopted_native_explain_preserves_read_only_and_host_refusals() {
    use purrdf::{
        SparqlRequest,
        sparql::{FallibleSparqlError, QueryGovernors},
    };
    use sparrow_core::{QueryCancellation, RdfGraphLayers, RdfReadLimits, SparqlRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .ingest("explain-refusal", "explain-refusal", "jsonl", b"{}\n", 1)
        .unwrap();
    let scope = record_page_scope("explain-refusal", 1);
    let governors = QueryGovernors::METERED.with_fuel(1_000_000);
    let mut read = SparqlRead {
        text_relations: None,
        scope: &scope,
        layers: RdfGraphLayers::ALL,
        request: SparqlRequest {
            query: "INSERT DATA { <urn:s> <urn:p> <urn:o> }",
            base_iri: None,
            substitutions: &[],
        },
        governors: &governors,
        limits: RdfReadLimits::default(),
    };
    let cancellation = QueryCancellation::default();
    assert!(matches!(
        store
            .sparql_explain(&read, 2, &cancellation)
            .unwrap()
            .outcome,
        Err(FallibleSparqlError::Query { .. })
    ));
    read.request.query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";
    read.limits.requests = 0;
    assert_eq!(
        store
            .sparql_explain(&read, 2, &cancellation)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    read.limits = RdfReadLimits::default();
    cancellation.cancel();
    assert_eq!(
        store
            .sparql_explain(&read, 2, &cancellation)
            .unwrap_err()
            .code,
        "CANCELLED"
    );
}

#[test]
fn native_explanation_pages_keep_original_ledger_and_share_query_admission() {
    use purrdf::{SparqlRequest, sparql::QueryGovernors};
    use sparrow_core::{
        QueryCancellation, RdfGraphLayers, RdfReadLimits, ResultPage, SparqlExplanationItem,
        SparqlRead,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .ingest(
            "explanation-pages",
            "explanation-pages",
            "jsonl",
            b"{}\n{}\n",
            1,
        )
        .unwrap();
    let scope = record_page_scope("explanation-pages", 1);
    let governors = QueryGovernors::METERED.with_fuel(1_000_000);
    let read = SparqlRead {
        text_relations: None,
        scope: &scope,
        layers: RdfGraphLayers {
            metadata: true,
            diagnostic: false,
            content: false,
        },
        request: SparqlRequest {
            query: "SELECT ?record ?payload WHERE { ?record a <urn:sparrow:vocab:Record>; <urn:sparrow:vocab:payload> ?payload FILTER(?record != <urn:absent>) }",
            base_iri: None,
            substitutions: &[],
        },
        governors: &governors,
        limits: RdfReadLimits::default(),
    };
    let cancellation = QueryCancellation::default();
    let first = store
        .sparql_explanation_read(&read, 2, &cancellation)
        .unwrap();
    assert!(first.total_items > 1);
    assert_eq!(first.body.items().count(), 1);
    let cursor = first.next_cursor.as_ref().unwrap();
    assert_eq!(
        store
            .sparql_read_next(&first.store_id, cursor, 3)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    store
        .ingest(
            "explanation-later",
            "explanation-pages",
            "jsonl",
            b"{}\n",
            3,
        )
        .unwrap();
    let mut page = first.clone();
    let mut count = 0;
    let mut fuel = 0_u64;
    let mut pages = 0;
    loop {
        pages += 1;
        assert_eq!(page.body.captured().scope, first.body.captured().scope);
        assert_eq!(page.body.captured().coverage.selected_records, 2);
        assert_eq!(page.lease_expires_at, first.lease_expires_at);
        for item in page.body.items() {
            count += 1;
            if let SparqlExplanationItem::Node(node) = item {
                fuel += node.fuel_total();
            }
        }
        let Some(cursor) = page.next_cursor.as_deref() else {
            break;
        };
        let next = store.result_next(&first.store_id, cursor, 4).unwrap();
        assert_eq!(next, store.result_next(&first.store_id, cursor, 4).unwrap());
        page = match next {
            ResultPage::SparqlExplanation(page) => page,
            _ => panic!("explanation cursor changed operation"),
        };
        assert!(pages <= first.total_items);
    }
    assert_eq!(count, first.total_items);
    assert_eq!(
        fuel,
        first
            .body
            .captured()
            .outcome
            .as_ref()
            .unwrap()
            .1
            .governors
            .consumed_in(purrdf_core::ResourceDimension::Fuel)
    );
    let mut ids = vec![first.snapshot_id.clone()];
    for _ in 0..6 {
        ids.push(
            store
                .sparql_explanation_read(&read, 4, &cancellation)
                .unwrap()
                .snapshot_id,
        );
    }
    let query = store.sparql_read(&read, 4, &cancellation).unwrap();
    ids.push(query.snapshot_id);
    assert_eq!(
        store.status(4).unwrap()["sparql_read_handles"].as_str(),
        Some("8")
    );
    assert_eq!(
        store
            .sparql_explanation_read(&read, 4, &cancellation)
            .unwrap_err()
            .code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store.sparql_read(&read, 4, &cancellation).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
    for id in ids {
        assert!(store.result_release(&first.store_id, &id, 4).unwrap());
    }
    assert_eq!(
        store.status(4).unwrap()["sparql_read_handles"].as_str(),
        Some("0")
    );
    assert_eq!(
        store
            .result_next(&first.store_id, cursor, 4)
            .unwrap_err()
            .code,
        "RESULT_EXPIRED"
    );
    let expiring = store
        .sparql_explanation_read(&read, 4, &cancellation)
        .unwrap();
    assert_eq!(
        store
            .result_next(
                &first.store_id,
                expiring.next_cursor.as_ref().unwrap(),
                expiring.lease_expires_at
            )
            .unwrap_err()
            .code,
        "RESULT_EXPIRED"
    );
    drop(store);
    assert_eq!(
        first.body.items().count(),
        1,
        "retained page must survive store closure"
    );
}

#[test]
fn native_explanation_refuses_an_oversized_entry_without_a_result_handle() {
    use purrdf::{SparqlRequest, sparql::QueryGovernors};
    use sparrow_core::{QueryCancellation, RdfGraphLayers, RdfReadLimits, SparqlRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let literal = "x".repeat(48_000);
    let payload = purrdf_lex::json::write_compact(
        &purrdf_lex::json::Object::new()
            .with("value", literal.clone())
            .into(),
    );
    store
        .ingest(
            "explanation-wide",
            "explanation-wide",
            "json",
            payload.as_bytes(),
            1,
        )
        .unwrap();
    let scope = record_page_scope("explanation-wide", 1000);
    let query = format!("SELECT ?s WHERE {{ ?s ?p \"{literal}\" . ?s ?other ?value }}");
    let governors = QueryGovernors::METERED.with_fuel(1_000_000);
    let error = store
        .sparql_explanation_read(
            &SparqlRead {
                text_relations: None,
                scope: &scope,
                layers: RdfGraphLayers {
                    metadata: false,
                    diagnostic: false,
                    content: true,
                },
                request: SparqlRequest {
                    query: &query,
                    base_iri: None,
                    substitutions: &[],
                },
                governors: &governors,
                limits: RdfReadLimits::default(),
            },
            2,
            &QueryCancellation::default(),
        )
        .unwrap_err();
    assert_eq!(error.code, "RESOURCE_LIMIT");
    assert_eq!(
        error.message,
        "native result item cannot fit a transport page"
    );
    assert_eq!(
        store.status(2).unwrap()["sparql_read_handles"].as_str(),
        Some("0")
    );
}

#[test]
fn scoped_sparql_uses_native_read_forms_bindings_and_governor_outcomes() {
    use purrdf::{
        SparqlRequest, TermValue,
        sparql::{FallibleSparqlError, QueryGovernors},
    };
    use sparrow_core::{QueryCancellation, RdfGraphLayers, RdfReadLimits, SparqlRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .ingest("scoped-sparql", "scoped", "jsonl", b"{}\n{}\n", 1)
        .unwrap();
    let scope = record_page_scope("scoped", 1);
    let governors = QueryGovernors::METERED
        .with_fuel(1_000_000)
        .with_max_answers(100)
        .with_max_intermediate_cells(10_000)
        .with_max_scratch_bytes(1024 * 1024);
    let bindings = [(
        "predicate".into(),
        TermValue::iri("urn:sparrow:vocab:payload"),
    )];
    let mut read = SparqlRead {
        text_relations: None,
        scope: &scope,
        layers: RdfGraphLayers::ALL,
        request: SparqlRequest {
            query: "SELECT ?record WHERE { ?record a <urn:sparrow:vocab:Record> ; ?predicate ?payload }",
            base_iri: None,
            substitutions: &bindings,
        },
        governors: &governors,
        limits: RdfReadLimits::default(),
    };
    let cancellation = QueryCancellation::default();
    let result = store.sparql_query(&read, 2, &cancellation).unwrap();
    assert_eq!(result.store_id, store.store_id().unwrap());
    assert_eq!(result.as_of, 2);
    assert_eq!(result.coverage.selected_records, 2);
    assert_eq!(
        result.outcome.unwrap().result.solutions().unwrap().1.len(),
        2
    );
    for query in [
        "ASK { ?record <urn:sparrow:vocab:payload> ?payload }",
        "CONSTRUCT { ?record <urn:copy:payload> ?payload } WHERE { ?record <urn:sparrow:vocab:payload> ?payload }",
        "DESCRIBE ?record WHERE { ?record <urn:sparrow:vocab:payload> ?payload }",
    ] {
        read.request = SparqlRequest {
            query,
            base_iri: None,
            substitutions: &[],
        };
        let result = store
            .sparql_query(&read, 2, &cancellation)
            .unwrap()
            .outcome
            .unwrap()
            .result;
        if query.starts_with("ASK") {
            assert_eq!(result.boolean(), Some(true));
        } else {
            assert!(result.graph().is_some_and(|graph| graph.quad_count() > 0));
        }
    }
    read.request.query = "SELECT ?record WHERE { ?record <urn:sparrow:vocab:payload> ?payload }";
    let exhausted = governors.clone().with_fuel(0);
    read.governors = &exhausted;
    assert!(matches!(
        store.sparql_query(&read, 2, &cancellation).unwrap().outcome,
        Err(FallibleSparqlError::BudgetExhausted { .. })
    ));
    read.governors = &governors;
    read.request.query = "INSERT DATA { <urn:s> <urn:p> <urn:o> }";
    assert!(matches!(
        store.sparql_query(&read, 2, &cancellation).unwrap().outcome,
        Err(FallibleSparqlError::Query { .. })
    ));
}

#[test]
fn rdf_scope_captures_shared_selection_and_freezes_after_pin_release() {
    use purrdf::{DatasetView, FallibleDatasetView};
    use sparrow_core::{QueryCancellation, QuerySelection, RdfGraphLayers, RdfReadLimits};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let a = store
        .ingest("rdf-scope-a", "scoped", "json", b"{}", 10)
        .unwrap();
    let b = store
        .ingest("rdf-scope-b", "scoped", "json", b"{}", 20)
        .unwrap();
    store
        .ingest("rdf-scope-other", "other", "json", b"{}", 20)
        .unwrap();
    let pin = store
        .pin(
            "rdf-scope-pin",
            "scoped",
            &[a.id.clone(), b.record_ids[0].clone()],
            40,
            20,
        )
        .unwrap();
    let mut scope = record_page_scope("scoped", 1);
    scope.received_from = 10;
    scope.received_before = 21;
    let cancellation = QueryCancellation::default();
    let live = store
        .with_rdf_scope(
            &scope,
            RdfGraphLayers::ALL,
            30,
            RdfReadLimits::default(),
            &cancellation,
            |view, coverage| {
                assert_eq!(coverage.selected_records, 2);
                assert_eq!(coverage.content_ready, 2);
                assert!(coverage.complete());
                view.named_graphs().collect::<Vec<_>>()
            },
        )
        .unwrap();
    scope.selection = QuerySelection::Evidence(vec![
        a.id.clone(),
        b.record_ids[0].clone(),
        a.record_ids[0].clone(),
    ]);
    let explicit = store
        .with_rdf_scope(
            &scope,
            RdfGraphLayers::ALL,
            30,
            RdfReadLimits::default(),
            &cancellation,
            |view, coverage| {
                assert_eq!(coverage.selected_records, 2);
                view.named_graphs().collect::<Vec<_>>()
            },
        )
        .unwrap();
    assert_eq!(
        live.len(),
        explicit.len() + 1,
        "only live scope independently includes b's complete parent"
    );
    assert!(explicit.iter().all(|graph| live.contains(graph)));
    scope.selection = QuerySelection::Pin {
        id: pin.id.clone(),
        expected_revision: Some(pin.revision),
    };
    store
        .with_rdf_scope(
            &scope,
            RdfGraphLayers::ALL,
            31 * DAY,
            RdfReadLimits::default(),
            &cancellation,
            |view, coverage| {
                assert_eq!(coverage.selected_records, 2);
                let sequence = view.snapshot_sequence_number();
                let before = view.quads().collect::<Vec<_>>();
                store
                    .release_retry("rdf-scope-release", &pin.id, 31 * DAY)
                    .unwrap();
                store.expire(31 * DAY).unwrap();
                store
                    .ingest("rdf-scope-new", "scoped", "json", b"{}", 31 * DAY)
                    .unwrap();
                assert_eq!(view.snapshot_sequence_number(), sequence);
                assert_eq!(before, view.quads().collect::<Vec<_>>());
                assert!(matches!(
                    view.operation_status(),
                    purrdf::ViewOperationStatus::Ready { .. }
                ));
            },
        )
        .unwrap();
    assert!(
        store
            .with_rdf_scope(
                &scope,
                RdfGraphLayers::ALL,
                31 * DAY,
                RdfReadLimits::default(),
                &cancellation,
                |_, _| ()
            )
            .is_err()
    );
}

#[test]
fn rdf_scope_refuses_exhaustion_and_cancellation_without_callback_prefix() {
    use sparrow_core::{QueryCancellation, RdfGraphLayers, RdfReadLimits};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .ingest("rdf-scope-refusal", "scoped", "jsonl", b"{}\n{}\n", 1)
        .unwrap();
    let mut scope = record_page_scope("scoped", 1);
    scope.scan_budget = 1;
    let called = std::cell::Cell::new(false);
    let cancellation = QueryCancellation::default();
    let error = store
        .with_rdf_scope(
            &scope,
            RdfGraphLayers::ALL,
            2,
            RdfReadLimits::default(),
            &cancellation,
            |_, _| called.set(true),
        )
        .unwrap_err();
    assert_eq!(error.code, "RESOURCE_LIMIT");
    assert!(!called.get());
    scope.scan_budget = 100;
    cancellation.cancel();
    assert_eq!(
        store
            .with_rdf_scope(
                &scope,
                RdfGraphLayers::ALL,
                2,
                RdfReadLimits::default(),
                &cancellation,
                |_, _| called.set(true)
            )
            .unwrap_err()
            .code,
        "CANCELLED"
    );
    assert!(!called.get());
    let cancellation = QueryCancellation::default();
    assert_eq!(
        store
            .with_rdf_scope(
                &scope,
                RdfGraphLayers::ALL,
                2,
                RdfReadLimits::default(),
                &cancellation,
                |_, _| {
                    cancellation.cancel();
                    "must not escape the fail-aware boundary"
                }
            )
            .unwrap_err()
            .code,
        "CANCELLED"
    );
    store
        .with_rdf_scope(
            &scope,
            RdfGraphLayers::ALL,
            2,
            RdfReadLimits::default(),
            &QueryCancellation::default(),
            |_, coverage| assert_eq!(coverage.selected_records, 2),
        )
        .unwrap();
}

#[test]
fn rdf_scope_reports_content_and_unknown_diagnostic_coverage_independently() {
    use sparrow_core::{
        DiagnosticDimension, QueryAlternatives, QueryCancellation, RdfGraphLayers, RdfReadLimits,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .ingest("rdf-scope-coverage-good", "scoped", "json", b"{}", 1)
        .unwrap();
    store
        .ingest("rdf-scope-coverage-bad", "scoped", "json", b"{", 2)
        .unwrap();
    store
        .ingest("rdf-scope-coverage-raw", "scoped", "log", b"raw\n", 3)
        .unwrap();
    let mut scope = record_page_scope("scoped", 1);
    let cancellation = QueryCancellation::default();
    store
        .with_rdf_scope(
            &scope,
            RdfGraphLayers::ALL,
            4,
            RdfReadLimits::default(),
            &cancellation,
            |_, coverage| {
                assert_eq!(
                    (
                        coverage.selected_records,
                        coverage.content_ready,
                        coverage.content_failed,
                        coverage.content_not_applicable
                    ),
                    (3, 1, 1, 1)
                );
                assert!(!coverage.complete());
            },
        )
        .unwrap();
    scope.require_complete_index = true;
    assert_eq!(
        store
            .with_rdf_scope(
                &scope,
                RdfGraphLayers::ALL,
                4,
                RdfReadLimits::default(),
                &cancellation,
                |_, _| ()
            )
            .unwrap_err()
            .code,
        "INDEX_NOT_READY"
    );
    let metadata = RdfGraphLayers {
        metadata: true,
        diagnostic: false,
        content: false,
    };
    store
        .with_rdf_scope(
            &scope,
            metadata,
            4,
            RdfReadLimits::default(),
            &cancellation,
            |_, coverage| assert!(coverage.complete()),
        )
        .unwrap();
    scope.require_complete_index = false;
    scope.diagnostics.insert(
        DiagnosticDimension::Service,
        QueryAlternatives::new(vec!["worker".into()]).unwrap(),
    );
    store
        .with_rdf_scope(
            &scope,
            metadata,
            4,
            RdfReadLimits::default(),
            &cancellation,
            |_, coverage| {
                assert_eq!(coverage.selected_records, 0);
                assert_eq!(coverage.diagnostics.unknown, 3);
                assert!(!coverage.complete());
            },
        )
        .unwrap();
}

fn record_page_scope(source: &str, page_size: usize) -> sparrow_core::QueryScope {
    sparrow_test_support::record_scope!(QueryScope, source, page_size)
}

#[test]
fn record_pages_capture_more_than_the_old_row_ceiling_without_limited_execution() {
    use sparrow_core::{RecordQuery, RecordRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest(
            "record-pages-large",
            "pages",
            "jsonl",
            b"{}\n".repeat(1005).as_slice(),
            1,
        )
        .unwrap();
    let mut query = RecordRead {
        operation: sparrow_core::RecordOperation::Records,
        scope: record_page_scope("pages", 113),
        query: RecordQuery::default(),
        cursor: None,
    };
    let first = store.records_read(&query, 2).unwrap();
    assert_eq!(first.total_items, 1005);
    assert!(first.next_cursor.is_some());
    let mut ids = std::collections::BTreeSet::new();
    let mut page = first.clone();
    loop {
        let value = page.body.result().unwrap();
        assert_eq!(value["execution"].as_str(), Some("complete"));
        assert_eq!(value["qualified_records"].as_str(), Some("1005"));
        assert!(value["rows"].as_array().unwrap().len() <= 113);
        for row in value["rows"].as_array().unwrap() {
            assert!(ids.insert(row["record_id"].as_str().unwrap().to_owned()));
        }
        let Some(cursor) = page.next_cursor else {
            break;
        };
        query.cursor = Some(cursor);
        page = store.records_read(&query, 3).unwrap();
        assert_eq!(page.snapshot_id, first.snapshot_id);
        assert_eq!(page.as_of, first.as_of);
        assert_eq!(page.lease_expires_at, first.lease_expires_at);
    }
    assert_eq!(ids, artifact.record_ids.into_iter().collect());
    query.scope.limit = 1;
    assert_eq!(store.records_read(&query, 3).unwrap_err().code, "CONFLICT");
}

#[test]
fn disk_indexed_record_capture_crosses_ten_thousand_with_bounded_replay() {
    use sparrow_core::{RecordOperation, RecordQuery, RecordRead, ResultPage};
    let dir = tempfile::tempdir().unwrap();
    let mut settings = config();
    settings.max_records = 20000;
    settings.disk_budget = 2 * 1024 * 1024 * 1024;
    let store = Store::open(dir.path(), settings).unwrap();
    let input = b"catalog row\n".repeat(12003);
    let manifest = stage_fixture(&store, "wide-index", "log", &input);
    for ordinal in 0..manifest.layout_records {
        store
            .import_prepare_record(&manifest.id, ordinal, 1)
            .unwrap();
    }
    let artifact = store.import_publish(&manifest.id, 1).unwrap();
    let mut scope = record_page_scope("oversized", 1000);
    scope.scan_budget = 100000;
    let mut query = RecordRead {
        operation: RecordOperation::Records,
        scope,
        query: RecordQuery::default(),
        cursor: None,
    };
    let first = store.records_read(&query, 2).unwrap();
    assert_eq!(first.total_items, 12003);
    let metadata = first.body.result().unwrap();
    assert_eq!(
        metadata["query_evidence"]["limits"]["capture_items"].as_u64(),
        Some(100000)
    );
    assert_eq!(
        metadata["query_evidence"]["limits"]["capture_bytes"].as_u64(),
        Some(64 * 1024 * 1024)
    );
    assert!(
        store.status(2).unwrap()["record_read_bytes"]
            .as_str()
            .unwrap()
            .parse::<usize>()
            .unwrap()
            < 64 * 1024
    );
    let cursor = first.next_cursor.clone().unwrap();
    let replay = store.result_next(&first.store_id, &cursor, 2).unwrap();
    store
        .evict_oldest(
            2,
            sparrow_core::EvictionLimits {
                max_records: 1,
                ..Default::default()
            },
        )
        .unwrap();
    store
        .ingest("wide-index-later", "oversized", "log", b"later\n", 3)
        .unwrap();
    assert_eq!(
        store.result_next(&first.store_id, &cursor, 4).unwrap(),
        replay
    );
    assert_eq!(
        store
            .result_next(&first.store_id, &format!("{}/1", first.snapshot_id), 4)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    let mut ids = std::collections::BTreeSet::new();
    let mut page = first.clone();
    loop {
        let result = page.body.result().unwrap();
        assert_eq!(result["known_qualified_records"].as_str(), Some("12003"));
        for row in result["rows"].as_array().unwrap() {
            assert!(ids.insert(row["record_id"].as_str().unwrap().to_owned()));
        }
        let Some(cursor) = page.next_cursor else {
            break;
        };
        let ResultPage::Records(next) = store.result_next(&first.store_id, &cursor, 4).unwrap()
        else {
            panic!("wrong captured operation")
        };
        page = next;
    }
    assert_eq!(ids, artifact.record_ids.iter().cloned().collect());
    assert!(
        store
            .result_release(&first.store_id, &first.snapshot_id, 4)
            .unwrap()
    );
    assert_eq!(
        store.status(4).unwrap()["record_read_bytes"].as_str(),
        Some("0")
    );
    query.scope.scan_budget = 10000;
    assert_eq!(
        store.records_read(&query, 4).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store.status(4).unwrap()["record_read_handles"].as_str(),
        Some("0")
    );
}

#[test]
fn disk_record_pages_cross_the_old_capture_bound_and_release_their_charge() {
    use sparrow_core::{
        FieldConstraint, JsonPredicate, RecordOperation, RecordQuery, RecordRead, ResultPage,
    };
    let dir = tempfile::tempdir().unwrap();
    // This query-capture fixture needs room for the complete 3000-owner RDF
    // batch under the final data-write admission, including Source facts.
    let mut settings = config();
    settings.disk_budget = 512 * 1024 * 1024;
    let disk_budget = settings.disk_budget;
    let store = Store::open(dir.path(), settings).unwrap();
    let source = "s".repeat(512);
    store
        .define_time_profile(&source, "/t", sparrow_core::TimeRepresentation::UnixMillis)
        .unwrap();
    let input = [
        b"{\"x\":\"needle value\",\"t\":20}\n".repeat(1500),
        b"{\"x\":\"needle value\",\"t\":10}\n".repeat(1500),
    ]
    .concat();
    let artifact = store
        .ingest("broad-disk-rows", &source, "jsonl", &input, 1)
        .unwrap();
    let mut scope = record_page_scope(&source, 1000);
    scope.scan_budget = 100000;
    let query = RecordRead {
        operation: RecordOperation::ArtifactGroups,
        scope,
        query: RecordQuery {
            fields: vec![FieldConstraint {
                predicate: JsonPredicate {
                    path: Some("/x".into()),
                    decoded: Some("needle value".into()),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        },
        cursor: None,
    };
    let first = store.records_read(&query, 2).unwrap();
    assert_eq!(first.total_items, 3000);
    assert!(first.next_cursor.is_some());
    assert!(
        store.status(2).unwrap()["record_read_bytes"]
            .as_str()
            .unwrap()
            .parse::<usize>()
            .unwrap()
            < 256 * 1024
    );
    let first_value = first.body.result().unwrap();
    assert_eq!(
        first_value["query_evidence"]["limits"]["capture_bytes"].as_u64(),
        Some(64 * 1024 * 1024)
    );
    assert_eq!(
        first_value["artifact_group_counts"][artifact.id.as_str()].as_u64(),
        Some(3000)
    );
    let replay_cursor = first.next_cursor.clone().unwrap();
    let replay = store
        .result_next(&first.store_id, &replay_cursor, 2)
        .unwrap();
    let mut event_query = query.clone();
    event_query.query.event_interval = Some(sparrow_core::EventInterval {
        from: 10,
        before: 21,
    });
    let event_page = store.records_read(&event_query, 2).unwrap();
    let mut ranked_query = query.clone();
    ranked_query.query.text = Some("needle".into());
    let ranked_page = store.records_read(&ranked_query, 2).unwrap();
    store
        .ingest("broad-later", &source, "json", b"{\"x\":\"value\"}", 3)
        .unwrap();
    for (mode, mut page) in [
        ("received", first.clone()),
        ("event", event_page),
        ("ranked", ranked_page),
    ] {
        let snapshot = page.snapshot_id.clone();
        let mut ids = std::collections::BTreeSet::new();
        let mut bytes = 0;
        let mut event_times = Vec::new();
        let ranking = page.body.result().unwrap().get("ranking").cloned();
        loop {
            let value = page.body.result().unwrap();
            assert_eq!(value["known_qualified_records"].as_str(), Some("3000"));
            assert_eq!(value.get("ranking").cloned(), ranking);
            if mode == "ranked" {
                assert_eq!(value["ranking"]["documents"].as_str(), Some("3000"));
            }
            for row in value["rows"].as_array().unwrap() {
                bytes += purrdf_lex::json::write_compact(row).len();
                assert!(ids.insert(row["record_id"].as_str().unwrap().to_owned()));
                event_times.push(
                    row["event_time"]["millis"]
                        .as_str()
                        .unwrap()
                        .parse::<u64>()
                        .unwrap(),
                );
                if mode == "ranked" {
                    assert!(row.get("score").is_some());
                }
            }
            let Some(cursor) = page.next_cursor else {
                break;
            };
            let ResultPage::Records(next) = store.result_next(&first.store_id, &cursor, 4).unwrap()
            else {
                panic!("wrong operation")
            };
            page = next;
        }
        assert!(bytes > 2 * 1024 * 1024, "{mode}");
        assert_eq!(ids, artifact.record_ids.iter().cloned().collect());
        if mode == "event" {
            assert_eq!(&event_times[..1500], vec![10; 1500]);
            assert_eq!(&event_times[1500..], vec![20; 1500]);
        }
        if mode != "received" {
            assert!(store.result_release(&first.store_id, &snapshot, 4).unwrap());
        }
    }
    assert_eq!(
        store
            .result_next(&first.store_id, &replay_cursor, 4)
            .unwrap(),
        replay
    );
    assert!(
        store
            .result_release(&first.store_id, &first.snapshot_id, 4)
            .unwrap()
    );
    let after = store.status(4).unwrap();
    assert_eq!(after["record_read_handles"].as_str(), Some("0"));
    assert_eq!(after["record_read_bytes"].as_str(), Some("0"));
    assert_eq!(
        store
            .result_next(&first.store_id, &replay_cursor, 4)
            .unwrap_err()
            .code,
        "RESULT_EXPIRED"
    );
    let ballast = dir.path().join("query-pressure-fixture");
    std::fs::File::create(&ballast)
        .unwrap()
        .set_len(disk_budget)
        .unwrap();
    assert_eq!(
        store.records_read(&query, 4).unwrap_err().code,
        "STORAGE_PRESSURE"
    );
    assert_eq!(
        store.status(4).unwrap()["record_read_handles"].as_str(),
        Some("0")
    );
    std::fs::remove_file(ballast).unwrap();
    let recovered = store.records_read(&query, 4).unwrap();
    assert!(
        store
            .result_release(&recovered.store_id, &recovered.snapshot_id, 4)
            .unwrap()
    );
}

#[test]
fn record_pages_keep_pin_capture_after_release_and_retirement_and_expire_absolutely() {
    use sparrow_core::{QuerySelection, RecordQuery, RecordRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest("record-pages-pin", "pages", "log", b"a\nb\nc\n", 0)
        .unwrap();
    let pin = store
        .pin("record-pages-protect", "pages", &[artifact.id], 40, 0)
        .unwrap();
    let mut scope = record_page_scope("pages", 1);
    scope.selection = QuerySelection::Pin {
        id: pin.id.clone(),
        expected_revision: None,
    };
    let mut query = RecordRead {
        operation: sparrow_core::RecordOperation::Records,
        scope,
        query: RecordQuery::default(),
        cursor: None,
    };
    let first = store.records_read(&query, 31 * DAY).unwrap();
    let frozen = first.body.result().unwrap()["query_evidence"]["scope"].clone();
    assert_eq!(frozen["pin"]["id"].as_str(), Some(pin.id.as_str()));
    assert_eq!(frozen["pin"]["revision"].as_u64(), Some(pin.revision));
    assert_eq!(
        frozen["pin"]["expires_at_ms"].as_u64(),
        Some(pin.expires_at)
    );
    assert_eq!(frozen["direct_members"].as_str(), Some("1"));
    assert_eq!(frozen["expanded_records"].as_str(), Some("3"));
    let renewed = store
        .renew_pin("scope-renew", &pin.id, 40, Some(pin.revision), 31 * DAY)
        .unwrap();
    let fresh = store.records_read(&query, 31 * DAY).unwrap();
    let changed = fresh.body.result().unwrap()["query_evidence"]["scope"].clone();
    assert_eq!(changed["pin"]["revision"].as_u64(), Some(renewed.revision));
    assert_ne!(changed["id"], frozen["id"]);
    query.cursor = first.next_cursor.clone();
    let before = store.records_read(&query, 31 * DAY).unwrap();
    assert_eq!(
        before.body.result().unwrap()["query_evidence"]["scope"],
        frozen
    );
    store
        .release_retry("record-pages-release", &pin.id, 31 * DAY + 1)
        .unwrap();
    store.expire(31 * DAY + 2).unwrap();
    assert_eq!(store.records_read(&query, 31 * DAY + 3).unwrap(), before);
    let mut fresh = query.clone();
    fresh.cursor = None;
    assert_eq!(
        store.records_read(&fresh, 31 * DAY + 3).unwrap_err().code,
        "NOT_FOUND"
    );
    query.query.text = Some("needle".into());
    assert_eq!(
        store.records_read(&query, 31 * DAY + 3).unwrap_err().code,
        "CONFLICT"
    );
    query.query.text = None;
    assert_eq!(
        store
            .records_read(&query, first.lease_expires_at)
            .unwrap_err()
            .code,
        "RESULT_EXPIRED"
    );
    assert_eq!(
        store.status(first.lease_expires_at).unwrap()["record_read_handles"].as_str(),
        Some("0")
    );
    drop(store);
    let reopened = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        reopened
            .records_read(&query, 31 * DAY + 4)
            .unwrap_err()
            .code,
        "RESULT_EXPIRED"
    );
}

#[test]
fn artifact_groups_freeze_parent_counts_without_changing_native_match_order_scores_or_evidence() {
    use sparrow_core::{RecordOperation, RecordQuery, RecordRead, ResultPage};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = b"{\"message\":\"needle\"}\n".repeat(3);
    let a = store
        .ingest("groups-a", "pages", "jsonl", &input, 1)
        .unwrap();
    let b = store
        .ingest("groups-b", "pages", "jsonl", &input, 1)
        .unwrap();
    let mut query = RecordRead {
        operation: RecordOperation::Records,
        scope: record_page_scope("pages", 1000),
        query: RecordQuery {
            text: Some("needle".into()),
            ..Default::default()
        },
        cursor: None,
    };
    let ordinary = store
        .records_read(&query, 2)
        .unwrap()
        .body
        .result()
        .unwrap();
    assert!(ordinary.get("artifact_group_counts").is_none());
    query.operation = RecordOperation::ArtifactGroups;
    query.scope.limit = 1;
    let first = store.records_read(&query, 2).unwrap();
    assert_eq!(first.total_items, 6);
    let first_value = first.body.result().unwrap();
    for artifact in [&a, &b] {
        assert_eq!(
            first_value["artifact_group_counts"][artifact.id.as_str()].as_u64(),
            Some(3)
        );
    }
    assert_eq!(first_value["rows"][0], ordinary["rows"][0]);
    let cursor = first.next_cursor.clone().unwrap();
    query.cursor = Some(cursor.clone());
    query.operation = RecordOperation::Records;
    assert_eq!(store.records_read(&query, 2).unwrap_err().code, "CONFLICT");
    query.operation = RecordOperation::ArtifactGroups;
    store
        .ingest("groups-later", "pages", "jsonl", &input, 3)
        .unwrap();
    let mut captured = first_value["rows"].as_array().unwrap().to_vec();
    let mut next = Some(cursor);
    while let Some(cursor) = next {
        let ResultPage::Records(page) = store.result_next(&first.store_id, &cursor, 4).unwrap()
        else {
            panic!("grouped continuation changed its record family")
        };
        let value = page.body.result().unwrap();
        assert_eq!(
            value["artifact_group_counts"],
            first_value["artifact_group_counts"]
        );
        assert_eq!(value["ranking"], first_value["ranking"]);
        captured.extend(value["rows"].as_array().unwrap().iter().cloned());
        next = page.next_cursor;
    }
    assert_eq!(captured, ordinary["rows"].as_array().unwrap().to_vec());
    assert!(
        store
            .result_release(&first.store_id, &first.snapshot_id, 4)
            .unwrap()
    );
    assert_eq!(
        store
            .result_next(&first.store_id, first.next_cursor.as_ref().unwrap(), 4)
            .unwrap_err()
            .code,
        "RESULT_EXPIRED"
    );
    assert_eq!(store.bytes(&a.id, 0, input.len(), 4).unwrap(), input);
    assert_eq!(store.records(4).unwrap().len(), 9);
}

#[test]
fn ranked_record_pages_freeze_native_corpus_scores_and_diagnostic_coverage() {
    use sparrow_core::{DiagnosticDimension as D, QueryAlternatives, RecordQuery, RecordRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .define_diagnostic_profile("pages", [(D::Service, "/svc".into())].into())
        .unwrap();
    store.ingest("record-pages-ranked", "pages", "jsonl",
        b"{\"svc\":\"worker\",\"message\":\"needle\"}\n{\"svc\":\"worker\",\"message\":\"needle needle\"}\n{\"message\":\"needle\"}\n", 1).unwrap();
    let mut scope = record_page_scope("pages", 1);
    scope.diagnostics = [(
        D::Service,
        QueryAlternatives::new(vec!["worker".into()]).unwrap(),
    )]
    .into();
    let mut query = RecordRead {
        operation: sparrow_core::RecordOperation::Records,
        scope: scope.clone(),
        query: RecordQuery {
            text: Some("needle".into()),
            ..Default::default()
        },
        cursor: None,
    };
    let first = store.records_read(&query, 2).unwrap();
    let first_value = first.body.result().unwrap();
    assert_eq!(first_value["execution"].as_str(), Some("complete"));
    assert_eq!(first_value["certainty"].as_str(), Some("incomplete"));
    assert_eq!(
        first_value["coverage"]["diagnostic_scope"]["unknown"].as_str(),
        Some("1")
    );
    assert!(first_value["qualified_records"].is_null());
    scope.limit = 1000;
    let expected = store.records_query(&scope, &query.query, 2).unwrap();
    store
        .ingest(
            "record-pages-ranked-new",
            "pages",
            "jsonl",
            b"{\"svc\":\"worker\",\"message\":\"needle\"}\n",
            3,
        )
        .unwrap();
    query.cursor = first.next_cursor;
    let second = store
        .records_read(&query, 4)
        .unwrap()
        .body
        .result()
        .unwrap();
    assert_eq!(second["ranking"], first_value["ranking"]);
    assert_eq!(second["coverage"], first_value["coverage"]);
    let mut actual = first_value["rows"].as_array().unwrap().clone();
    actual.extend(second["rows"].as_array().unwrap().iter().cloned());
    assert_eq!(
        actual.as_slice(),
        expected["rows"].as_array().unwrap().as_slice()
    );
}

#[test]
fn record_pages_admit_variable_byte_boundaries_and_refuse_without_publishing_prefixes() {
    use sparrow_core::{FieldConstraint, JsonPredicate, RecordQuery, RecordRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let key = "k".repeat(2048);
    let line = format!("{{\"{key}\":1}}\n");
    store
        .ingest(
            "record-pages-bytes",
            "pages",
            "jsonl",
            line.repeat(20).as_bytes(),
            1,
        )
        .unwrap();
    let field = FieldConstraint {
        predicate: JsonPredicate {
            path: Some(format!("/{key}")),
            ..Default::default()
        },
        ..Default::default()
    };
    let mut query = RecordRead {
        operation: sparrow_core::RecordOperation::Records,
        scope: record_page_scope("pages", 100),
        query: RecordQuery {
            fields: vec![field.clone(); 4],
            ..Default::default()
        },
        cursor: None,
    };
    let first = store.records_read(&query, 2).unwrap();
    let first_rows = first.body.result().unwrap()["rows"]
        .as_array()
        .unwrap()
        .len();
    assert!(first_rows > 1 && first_rows < 20);
    assert_eq!(first.total_items, 20);
    assert_eq!(
        store
            .result_next(&first.store_id, &format!("{}/1", first.snapshot_id), 2)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    query.cursor = Some(format!("{}/1", first.snapshot_id));
    assert_eq!(
        store.records_read(&query, 2).unwrap_err().code,
        "INVALID_ARGUMENT"
    );
    query.cursor = first.next_cursor.clone();
    let second = store.records_read(&query, 2).unwrap();
    assert_eq!(second.page_start, first_rows);
    assert_eq!(
        store
            .result_next(&first.store_id, query.cursor.as_ref().unwrap(), 2)
            .unwrap(),
        sparrow_core::ResultPage::Records(second.clone())
    );
    let mut fresh = query.clone();
    fresh.cursor = None;
    fresh.scope.scan_budget = 1;
    assert_eq!(
        store.records_read(&fresh, 2).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
    fresh.scope.scan_budget = 10000;
    fresh.query.fields = vec![field; 20];
    assert_eq!(
        store.records_read(&fresh, 2).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store.status(2).unwrap()["record_read_handles"].as_str(),
        Some("1")
    );
    let clone = store.clone();
    query.cursor = None;
    for _ in 1..8 {
        clone.records_read(&query, 2).unwrap();
    }
    assert_eq!(
        store.status(2).unwrap()["record_read_handles"].as_str(),
        Some("8")
    );
    assert_eq!(
        store.records_read(&query, 2).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
    store.expire(first.lease_expires_at).unwrap();
    assert_eq!(
        clone.status(first.lease_expires_at).unwrap()["record_read_handles"].as_str(),
        Some("0")
    );
}

#[test]
fn text_pages_capture_all_ranked_matches_and_bind_the_operation() {
    use sparrow_core::{RecordOperation, RecordQuery, RecordRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest(
            "text-pages-large",
            "pages",
            "log",
            b"needle\n".repeat(1005).as_slice(),
            1,
        )
        .unwrap();
    let mut query = RecordRead {
        operation: RecordOperation::Text,
        scope: record_page_scope("pages", 137),
        query: RecordQuery {
            text: Some("needle".into()),
            ..Default::default()
        },
        cursor: None,
    };
    let first = store.records_read(&query, 2).unwrap();
    assert_eq!(first.total_items, 1005);
    let original_ranking = first.body.result().unwrap()["ranking"].clone();
    let mut page = first.clone();
    let mut ids = std::collections::BTreeSet::new();
    loop {
        let value = page.body.result().unwrap();
        assert_eq!(value["execution"].as_str(), Some("complete"));
        assert_eq!(value["ranking"], original_ranking);
        for row in value["rows"].as_array().unwrap() {
            assert!(ids.insert(row["record_id"].as_str().unwrap().to_owned()));
        }
        let Some(cursor) = page.next_cursor else {
            break;
        };
        query.cursor = Some(cursor);
        page = store.records_read(&query, 3).unwrap();
        assert_eq!(page.snapshot_id, first.snapshot_id);
    }
    assert_eq!(ids, artifact.record_ids.into_iter().collect());
    query.operation = RecordOperation::Records;
    assert_eq!(store.records_read(&query, 3).unwrap_err().code, "CONFLICT");
    query.cursor = None;
    query.operation = RecordOperation::Text;
    query.query.text = None;
    assert_eq!(
        store.records_read(&query, 3).unwrap_err().code,
        "INVALID_ARGUMENT"
    );
}

#[test]
fn record_and_text_pages_share_handle_admission_and_fixed_expiry() {
    use sparrow_core::{RecordOperation, RecordQuery, RecordRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .ingest(
            "text-pages-shared",
            "pages",
            "log",
            b"needle a\nneedle b\nneedle c\n",
            1,
        )
        .unwrap();
    let clone = store.clone();
    let mut query = RecordRead {
        operation: RecordOperation::Text,
        scope: record_page_scope("pages", 1),
        query: RecordQuery {
            text: Some("needle".into()),
            ..Default::default()
        },
        cursor: None,
    };
    let first = clone.records_read(&query, 2).unwrap();
    for index in 1..8 {
        query.operation = if index % 2 == 0 {
            RecordOperation::Text
        } else {
            RecordOperation::Records
        };
        store.records_read(&query, 2).unwrap();
    }
    assert_eq!(
        store.status(2).unwrap()["record_read_handles"].as_str(),
        Some("8")
    );
    assert_eq!(
        clone.records_read(&query, 2).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
    query.operation = RecordOperation::Text;
    query.cursor = first.next_cursor.clone();
    assert_eq!(
        store
            .records_read(&query, first.lease_expires_at)
            .unwrap_err()
            .code,
        "RESULT_EXPIRED"
    );
    assert_eq!(
        clone.status(first.lease_expires_at).unwrap()["record_read_handles"].as_str(),
        Some("0")
    );
}

#[test]
fn json_pages_capture_complete_counts_occurrences_and_frozen_receipts() {
    use sparrow_core::{JsonQuery, JsonRead, RecordOperation, RecordQuery, RecordRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let object = format!(
        "{{{}}}",
        (0..1005)
            .map(|i| format!("\"k{i}\":{i}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    store
        .ingest("json-pages", "pages", "json", object.as_bytes(), 1)
        .unwrap();
    let mut query = JsonRead {
        scope: record_page_scope("pages", 113),
        query: JsonQuery::Keys { path: None },
        cursor: None,
    };
    let first = store.json_read(&query, 2).unwrap();
    assert_eq!(first.total_items, 1006);
    let original = first.body.result().unwrap();
    assert_eq!(original["execution"].as_str(), Some("complete"));
    store
        .ingest(
            "json-pages-later",
            "pages",
            "json",
            br#"{"k0":0,"k0":1,"later":null}"#,
            3,
        )
        .unwrap();
    let mut paths = std::collections::BTreeSet::new();
    let mut page = first.clone();
    loop {
        let value = page.body.result().unwrap();
        assert_eq!(value["coverage"], original["coverage"]);
        for row in value["rows"].as_array().unwrap() {
            assert!(paths.insert(row["path"].as_str().unwrap().to_owned()));
            assert_eq!(row["occurrences"].as_str(), Some("1"));
            assert_eq!(row["records"].as_str(), Some("1"));
        }
        let Some(cursor) = page.next_cursor else {
            break;
        };
        query.cursor = Some(cursor);
        page = store.json_read(&query, 4).unwrap();
        assert_eq!(page.snapshot_id, first.snapshot_id);
    }
    assert_eq!(paths.len(), 1006);
    query.cursor = first.next_cursor.clone();
    query.query = JsonQuery::Values {
        path: None,
        kind: None,
        lexical: None,
        decoded: None,
    };
    assert_eq!(store.json_read(&query, 4).unwrap_err().code, "CONFLICT");
    let record = RecordRead {
        scope: query.scope.clone(),
        operation: RecordOperation::Records,
        query: RecordQuery::default(),
        cursor: query.cursor.clone(),
    };
    assert_eq!(store.records_read(&record, 4).unwrap_err().code, "CONFLICT");
    query.cursor = None;
    let values = store.json_read(&query, 4).unwrap();
    assert_eq!(values.total_items, 1010);
    query.cursor = values.next_cursor.clone();
    query.scope.limit = 112;
    assert_eq!(store.json_read(&query, 4).unwrap_err().code, "CONFLICT");
    query.scope.limit = 113;
    assert_eq!(
        store
            .json_read(&query, values.lease_expires_at)
            .unwrap_err()
            .code,
        "RESULT_EXPIRED"
    );
    query.cursor = None;
    query.scope.scan_budget = 1;
    assert_eq!(
        store.json_read(&query, 4).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store.status(values.lease_expires_at).unwrap()["record_read_handles"].as_str(),
        Some("0")
    );
}

#[test]
fn json_capture_counts_duplicate_keys_and_shares_query_handle_admission() {
    use sparrow_core::{JsonQuery, JsonRead, RecordOperation, RecordQuery, RecordRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .ingest(
            "json-pages-duplicates",
            "pages",
            "jsonl",
            br#"{"x":1,"x":2}
{"x":1}
"#,
            1,
        )
        .unwrap();
    let query = JsonRead {
        scope: record_page_scope("pages", 1),
        query: JsonQuery::Keys { path: None },
        cursor: None,
    };
    let first = store.json_read(&query, 2).unwrap();
    let mut next = query.clone();
    next.cursor = first.next_cursor.clone();
    let result = store.json_read(&next, 2).unwrap().body.result().unwrap();
    assert_eq!(result["rows"][0]["path"].as_str(), Some("/x"));
    assert_eq!(result["rows"][0]["occurrences"].as_str(), Some("3"));
    assert_eq!(result["rows"][0]["records"].as_str(), Some("2"));
    let mut values = query.clone();
    values.query = JsonQuery::Values {
        path: Some("/x".into()),
        kind: None,
        lexical: None,
        decoded: None,
    };
    assert_eq!(store.json_read(&values, 2).unwrap().total_items, 3);
    let record = RecordRead {
        scope: query.scope.clone(),
        operation: RecordOperation::Records,
        query: RecordQuery::default(),
        cursor: None,
    };
    for _ in 2..8 {
        store.clone().records_read(&record, 2).unwrap();
    }
    assert_eq!(
        store.json_read(&query, 2).unwrap_err().code,
        "RESOURCE_LIMIT"
    );
    assert_eq!(
        store.status(first.lease_expires_at).unwrap()["record_read_handles"].as_str(),
        Some("0")
    );
}

#[test]
fn disk_json_pages_preserve_large_native_counts_spans_and_replay_after_retirement() {
    use sparrow_core::{JsonQuery, JsonRead, QuerySelection, ResultPage};
    let dir = tempfile::tempdir().unwrap();
    let mut settings = config();
    settings.disk_budget = 1024 * 1024 * 1024;
    settings.maintenance_reserve = 64 * 1024 * 1024;
    let store = Store::open(dir.path(), settings).unwrap();
    let (mut input, expected_paths) = sparrow_test_support::wide_jsonl_keys(250, 50, 200);
    input.extend_from_slice(b"{\"dup\":\"needle\",\"dup\":\"needle\"}\n");
    let manifest = stage_fixture(&store, "wide-json-pages", "jsonl", &input);
    for ordinal in 0..manifest.layout_records {
        store
            .import_prepare_record(&manifest.id, ordinal, 0)
            .unwrap();
    }
    let artifact = store.import_publish(&manifest.id, 0).unwrap();
    assert_eq!(artifact.record_ids.len(), 251);
    let mut record_order = artifact.record_ids.iter().enumerate().collect::<Vec<_>>();
    record_order.sort_unstable_by(|a, b| a.1.cmp(b.1));
    let expected_values = record_order
        .into_iter()
        .flat_map(|(position, _)| {
            if position == 250 {
                vec!["/dup".to_owned(); 2]
            } else {
                expected_paths[position * 50..(position + 1) * 50].to_vec()
            }
        })
        .collect::<Vec<_>>();
    let pin = store
        .pin(
            "wide-json-pin",
            "wide",
            std::slice::from_ref(&artifact.id),
            40,
            0,
        )
        .unwrap();
    let mut scope = record_page_scope("oversized", 1000);
    scope.scan_budget = 100000;
    scope.selection = QuerySelection::Pin {
        id: pin.id.clone(),
        expected_revision: Some(pin.revision),
    };
    let keys = JsonRead {
        scope: scope.clone(),
        query: JsonQuery::Keys { path: None },
        cursor: None,
    };
    let values = JsonRead {
        scope,
        query: JsonQuery::Values {
            path: None,
            kind: Some("string".into()),
            lexical: None,
            decoded: Some("needle".into()),
        },
        cursor: None,
    };
    let admitted = 31 * DAY;
    let mut refused = values.clone();
    refused.scope.scan_budget = 10000;
    let refusal = store.json_read(&refused, admitted).unwrap_err();
    assert_eq!(refusal.code, "RESOURCE_LIMIT");
    assert!(refusal.message.contains("json_occurrence_budget=10000"));
    assert_eq!(
        store.status(admitted).unwrap()["record_read_handles"].as_str(),
        Some("0")
    );
    let key_page = store.json_read(&keys, admitted).unwrap();
    let value_page = store.json_read(&values, admitted).unwrap();
    assert_eq!(key_page.total_items, 12502);
    assert_eq!(value_page.total_items, 12502);
    let cursor = value_page.next_cursor.clone().unwrap();
    let frozen = store
        .result_next(&value_page.store_id, &cursor, admitted)
        .unwrap();
    store
        .release_retry("wide-json-release", &pin.id, admitted + 1)
        .unwrap();
    store.expire(admitted + 2).unwrap();
    assert_eq!(
        store.status(admitted + 2).unwrap()["live_records"].as_str(),
        Some("0")
    );
    assert_eq!(
        store
            .result_next(&value_page.store_id, &cursor, admitted + 3)
            .unwrap(),
        frozen
    );
    for (key_query, mut page) in [(true, key_page), (false, value_page)] {
        let snapshot = page.snapshot_id.clone();
        let mut paths = Vec::new();
        let mut bytes = 0;
        let receipt = page.body.result().unwrap()["coverage"].clone();
        loop {
            let result = page.body.result().unwrap();
            assert_eq!(result["coverage"], receipt);
            assert_eq!(result["execution"].as_str(), Some("complete"));
            let limits = &result["query_evidence"]["limits"];
            assert_eq!(limits["capture_items"].as_u64(), Some(100000));
            assert_eq!(limits["capture_bytes"].as_u64(), Some(64 * 1024 * 1024));
            if key_query {
                assert_eq!(
                    limits["key_aggregation_bytes"].as_u64(),
                    Some(32 * 1024 * 1024)
                );
            } else {
                assert!(limits["key_aggregation_bytes"].is_null());
            }
            for row in result["rows"].as_array().unwrap() {
                bytes += purrdf_lex::json::write_compact(row).len();
                if key_query {
                    let path = row["path"].as_str().unwrap();
                    paths.push(path.to_owned());
                    assert_eq!(
                        row["records"].as_str(),
                        Some(if path.is_empty() { "251" } else { "1" })
                    );
                    assert_eq!(
                        row["occurrences"].as_str(),
                        Some(if path.is_empty() {
                            "251"
                        } else if path == "/dup" {
                            "2"
                        } else {
                            "1"
                        })
                    );
                } else {
                    paths.push(row["value"]["path"].as_str().unwrap().to_owned());
                    let start = row["source_start"]
                        .as_str()
                        .unwrap()
                        .parse::<usize>()
                        .unwrap();
                    let end = row["source_end"]
                        .as_str()
                        .unwrap()
                        .parse::<usize>()
                        .unwrap();
                    assert_eq!(&input[start..end], b"needle");
                }
            }
            let Some(cursor) = page.next_cursor else {
                break;
            };
            let next = store
                .result_next(&page.store_id, &cursor, admitted + 3)
                .unwrap();
            page = match next {
                ResultPage::Keys(page) if key_query => page,
                ResultPage::Values(page) if !key_query => page,
                _ => panic!("wrong discovery operation"),
            };
        }
        assert!(bytes > 2 * 1024 * 1024);
        if key_query {
            assert_eq!(&paths[..2], &["", "/dup"]);
            assert!(
                paths[2..].iter().zip(&expected_paths).all(|(a, b)| a == b),
                "key paths differ from native input names"
            );
        } else {
            assert!(
                paths.iter().zip(&expected_values).all(|(a, b)| a == b),
                "value paths differ from receipt-time/record-ID then native occurrence order"
            );
        }
        assert!(
            store
                .result_release(&page.store_id, &snapshot, admitted + 4)
                .unwrap()
        );
    }
    assert_eq!(
        store.status(admitted + 4).unwrap()["record_read_handles"].as_str(),
        Some("0")
    );
}

#[test]
fn json_pages_keep_released_pin_membership_after_native_projection_retirement() {
    use sparrow_core::{JsonQuery, JsonRead, QuerySelection};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest(
            "json-retired-pages",
            "pages",
            "json",
            br#"{"x":1,"x":2}"#,
            0,
        )
        .unwrap();
    let pin = store
        .pin("json-pages-pin", "json-pages", &[artifact.id], 40, 0)
        .unwrap();
    let mut scope = record_page_scope("pages", 1);
    scope.selection = QuerySelection::Pin {
        id: pin.id.clone(),
        expected_revision: Some(pin.revision),
    };
    let mut query = JsonRead {
        scope,
        query: JsonQuery::Values {
            path: Some("/x".into()),
            kind: None,
            lexical: None,
            decoded: None,
        },
        cursor: None,
    };
    let first = store.json_read(&query, 31 * DAY).unwrap();
    query.cursor = first.next_cursor.clone();
    let second = store.json_read(&query, 31 * DAY).unwrap();
    store
        .release_retry("json-pages-unpin", &pin.id, 31 * DAY + 1)
        .unwrap();
    store.expire(31 * DAY + 2).unwrap();
    assert_eq!(store.json_read(&query, 31 * DAY + 3).unwrap(), second);
    assert_eq!(
        second.body.result().unwrap()["rows"][0]["value"]["lexical"].as_str(),
        Some("2")
    );
    query.cursor = None;
    assert_eq!(
        store.json_read(&query, 31 * DAY + 3).unwrap_err().code,
        "NOT_FOUND"
    );
}

#[test]
fn result_next_reuses_all_captured_operation_pages_and_refuses_invalid_expired_or_foreign_handles()
{
    use sparrow_core::{
        ArtifactRead, JsonQuery, JsonRead, QuerySelection, RecordOperation, RecordQuery,
        RecordRead, ResultPage, SourceRead,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let a = store
        .ingest(
            "next-a",
            "pages",
            "jsonl",
            br#"{"message":"needle a"}
{"message":"needle b"}
"#,
            0,
        )
        .unwrap();
    let b = store
        .ingest("next-b", "other", "json", br#"{"message":"needle c"}"#, 1)
        .unwrap();
    let pin = store
        .pin("next-pin-a", "first", &a.record_ids, 1, 2)
        .unwrap();
    store.pin("next-pin-b", "second", &[b.id], 1, 2).unwrap();
    let scope = record_page_scope("pages", 1);
    let id = store.properties().unwrap()["store_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut cases = Vec::new();
    for operation in [RecordOperation::Records, RecordOperation::Text] {
        let mut query = RecordRead {
            operation,
            scope: scope.clone(),
            query: RecordQuery {
                text: Some("needle".into()),
                ..Default::default()
            },
            cursor: None,
        };
        let first = store.records_read(&query, 3).unwrap();
        query.cursor = first.next_cursor.clone();
        let expected = store.records_read(&query, 3).unwrap();
        cases.push((
            query.cursor.unwrap(),
            if operation == RecordOperation::Records {
                ResultPage::Records(expected)
            } else {
                ResultPage::Text(expected)
            },
        ));
    }
    for query in [
        JsonQuery::Keys { path: None },
        JsonQuery::Values {
            path: Some("/message".into()),
            kind: None,
            lexical: None,
            decoded: None,
        },
    ] {
        let mut read = JsonRead {
            scope: scope.clone(),
            query,
            cursor: None,
        };
        let first = store.json_read(&read, 3).unwrap();
        read.cursor = first.next_cursor.clone();
        let expected = store.json_read(&read, 3).unwrap();
        cases.push((
            read.cursor.unwrap(),
            if matches!(read.query, JsonQuery::Keys { .. }) {
                ResultPage::Keys(expected)
            } else {
                ResultPage::Values(expected)
            },
        ));
    }
    let mut artifacts = ArtifactRead {
        page_size: 1,
        ..Default::default()
    };
    let first = store.artifact_read(&artifacts, 3).unwrap();
    artifacts.cursor = first.next_cursor.clone();
    cases.push((
        artifacts.cursor.clone().unwrap(),
        ResultPage::Artifacts(store.artifact_read(&artifacts, 3).unwrap()),
    ));
    let mut sources = SourceRead {
        page_size: 1,
        ..Default::default()
    };
    let first = store.source_read(&sources, 3, &[]).unwrap();
    sources.cursor = first.next_cursor.clone();
    cases.push((
        sources.cursor.clone().unwrap(),
        ResultPage::Sources(store.source_read(&sources, 3, &[]).unwrap()),
    ));
    for selector in [PinSelector::Active, PinSelector::Id(pin.id.clone())] {
        let mut read = PinRead {
            selector,
            page_size: 1,
            ..Default::default()
        };
        let first = store.pin_read(&read, 3).unwrap();
        read.cursor = first.next_cursor.clone();
        cases.push((
            read.cursor.clone().unwrap(),
            ResultPage::Pins(store.pin_read(&read, 3).unwrap()),
        ));
    }
    for (cursor, expected) in &cases {
        assert_eq!(&store.result_next(&id, cursor, 4).unwrap(), expected);
    }
    // Captured selectors remain authoritative after the collection disappears.
    let mut pinned = JsonRead {
        scope: scope.clone(),
        query: JsonQuery::Values {
            path: Some("/message".into()),
            kind: None,
            lexical: None,
            decoded: None,
        },
        cursor: None,
    };
    pinned.scope.selection = QuerySelection::Pin {
        id: pin.id.clone(),
        expected_revision: Some(pin.revision),
    };
    let first = store.json_read(&pinned, 3).unwrap();
    pinned.cursor = first.next_cursor.clone();
    let expected = store.json_read(&pinned, 3).unwrap();
    store.release_retry("next-release", &pin.id, 4).unwrap();
    assert_eq!(
        store
            .result_next(&id, pinned.cursor.as_ref().unwrap(), 5)
            .unwrap(),
        ResultPage::Values(expected)
    );
    assert_eq!(
        store
            .result_next("store:foreign", &cases[0].0, 4)
            .unwrap_err()
            .code,
        "STORE_MISMATCH"
    );
    let handle = cases[0].0.split_once('/').unwrap().0;
    for cursor in [
        format!("{handle}/0"),
        format!("{handle}/999"),
        format!("{handle}/+1"),
        format!("{handle}/1/2"),
        format!("{handle}/184467440737095516160"),
    ] {
        assert_eq!(
            store.result_next(&id, &cursor, 4).unwrap_err().code,
            "INVALID_ARGUMENT"
        );
    }
    assert_eq!(
        store
            .result_next(&id, "record-read:unknown/1", 4)
            .unwrap_err()
            .code,
        "RESULT_EXPIRED"
    );
    let pins_before = store
        .pins(5)
        .unwrap()
        .iter()
        .map(|p| p.id.clone())
        .collect::<Vec<_>>();
    for (cursor, _) in cases.iter().skip(1) {
        let snapshot = cursor.split_once('/').unwrap().0;
        assert_eq!(
            store
                .result_release("store:foreign", snapshot, 5)
                .unwrap_err()
                .code,
            "STORE_MISMATCH"
        );
        assert!(store.result_next(&id, cursor, 5).is_ok());
        assert!(store.result_release(&id, snapshot, 5).unwrap());
        assert!(!store.result_release(&id, snapshot, 5).unwrap());
        assert_eq!(
            store.result_next(&id, cursor, 5).unwrap_err().code,
            "RESULT_EXPIRED"
        );
    }
    assert_eq!(
        store
            .pins(5)
            .unwrap()
            .iter()
            .map(|p| p.id.clone())
            .collect::<Vec<_>>(),
        pins_before
    );
    for snapshot in [String::new(), cases[0].0.clone(), "x".repeat(129)] {
        assert_eq!(
            store.result_release(&id, &snapshot, 5).unwrap_err().code,
            "INVALID_ARGUMENT"
        );
    }
    assert!(!store.result_release(&id, "unknown", 5).unwrap());
    // More than eight sequential captures remain possible without expiry or
    // relaxing admission, when each consumer explicitly relinquishes replay.
    for _ in 0..20 {
        let page = store
            .records_read(
                &RecordRead {
                    operation: RecordOperation::Records,
                    scope: scope.clone(),
                    query: RecordQuery::default(),
                    cursor: None,
                },
                5,
            )
            .unwrap();
        assert!(page.next_cursor.is_some());
        assert!(store.result_release(&id, &page.snapshot_id, 5).unwrap());
    }
    assert_eq!(store.records(5).unwrap().len(), 3);
    for (cursor, _) in &cases {
        assert_eq!(
            store.result_next(&id, cursor, 60_003).unwrap_err().code,
            "RESULT_EXPIRED"
        );
    }
    drop(store);
    let reopened = Store::open(dir.path(), config()).unwrap();
    assert!(!reopened.result_release(&id, handle, 5).unwrap());
    assert_eq!(
        reopened.result_next(&id, &cases[0].0, 5).unwrap_err().code,
        "RESULT_EXPIRED"
    );
}

#[test]
fn captured_query_evidence_freezes_native_profiles_and_applicable_admission_limits() {
    use sparrow_core::{
        FieldConstraint, JsonPredicate, JsonQuery, JsonRead, RecordOperation, RecordQuery,
        RecordRead,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .ingest(
            "receipt-profile-input",
            "pages",
            "jsonl",
            br#"{"message":"needle a"}
{"message":"needle b"}
"#,
            1,
        )
        .unwrap();
    let scope = record_page_scope("pages", 1);
    let mut json = JsonRead {
        scope: scope.clone(),
        query: JsonQuery::Keys { path: None },
        cursor: None,
    };
    let first = store.json_read(&json, 2).unwrap();
    let evidence = first.body.result().unwrap()["query_evidence"].clone();
    let native = sparrow_core::json_projection_profile()
        .unwrap()
        .identity()
        .to_hex();
    assert_eq!(
        evidence["profile_ids"][0]["id"].as_str(),
        Some(native.as_str())
    );
    assert_eq!(
        evidence["profile_ids"][1]["id"].as_str(),
        Some(sparrow_core::QUERY_CAPTURE_PROFILE)
    );
    assert_eq!(evidence["profile_ids"].as_array().unwrap().len(), 2);
    assert!(evidence["native_governor"].is_null());
    assert_eq!(
        evidence["limits"]["json_value_visits"].as_u64(),
        Some(scope.scan_budget as u64)
    );
    assert!(
        evidence["limits"]["field_visits"].is_null()
            && evidence["limits"]["posting_visits"].is_null()
    );
    assert_eq!(
        evidence["limits"]["capture_bytes"].as_u64(),
        Some(64 * 1024 * 1024)
    );
    assert_eq!(
        evidence["limits"]["key_aggregation_bytes"].as_u64(),
        Some(32 * 1024 * 1024)
    );
    json.cursor = first.next_cursor.clone();
    store
        .ingest(
            "receipt-profile-later",
            "pages",
            "json",
            br#"{"later":true}"#,
            3,
        )
        .unwrap();
    assert_eq!(
        store.json_read(&json, 4).unwrap().body.result().unwrap()["query_evidence"],
        evidence
    );
    let record = RecordRead {
        scope,
        operation: RecordOperation::Records,
        query: RecordQuery {
            text: Some("needle".into()),
            fields: vec![FieldConstraint {
                predicate: JsonPredicate {
                    path: Some("/message".into()),
                    kind: Some("string".into()),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        },
        cursor: None,
    };
    let result = store
        .records_read(&record, 4)
        .unwrap()
        .body
        .result()
        .unwrap();
    let context = &result["query_evidence"];
    assert!(context["limits"]["key_aggregation_bytes"].is_null());
    assert_eq!(context["profile_ids"].as_array().unwrap().len(), 5);
    for (i, key) in [(2, "analyzer"), (3, "profile"), (4, "corpus_profile")] {
        assert_eq!(context["profile_ids"][i]["id"], result["ranking"][key]);
    }
    assert!(context["limits"]["json_value_visits"].is_null());
    assert_eq!(
        context["limits"]["field_visits"].as_u64(),
        Some(record.scope.scan_budget as u64)
    );
    assert_eq!(
        context["limits"]["posting_visits"].as_u64(),
        Some(record.scope.scan_budget as u64)
    );
    assert_eq!(context["limits"]["field_constraints"].as_u64(), Some(32));
    assert_eq!(context["limits"]["query_text_bytes"].as_u64(), Some(65536));
    assert!(context["limits"]["query_terms"].is_null());
    assert_eq!(
        context["limits"]["text_membership_bytes"].as_u64(),
        Some(64 * 1024 * 1024)
    );
}

#[test]
fn exact_key_paths_preserve_native_duplicates_kinds_roots_and_frozen_counts() {
    use sparrow_core::{JsonQuery, JsonRead, QueryScope};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store.ingest("key-paths", "keys", "jsonl", br#"{"a/b":1,"a/b":2,"a/b":"one","m~n":true,"":null,"nested":{"a/b":4},"arr":[{"a/b":5}]}
{"a/b":null,"a/b":"two","m~n":false}
"#, 1).unwrap();
    let scope = QueryScope {
        selection: Default::default(),
        source: Some("keys".into()),
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 10,
        limit: 100,
        scan_budget: 1000,
    };
    let result = store
        .json_query(
            &scope,
            &JsonQuery::Keys {
                path: Some("/a~1b".into()),
            },
            2,
        )
        .unwrap();
    let rows = result["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    for row in rows {
        assert_eq!(row["path"].as_str(), Some("/a~1b"));
    }
    assert_eq!(rows[0]["kind"].as_str(), Some("null"));
    assert_eq!(rows[0]["occurrences"].as_str(), Some("1"));
    assert_eq!(rows[1]["kind"].as_str(), Some("number"));
    assert_eq!(rows[1]["occurrences"].as_str(), Some("2"));
    assert_eq!(rows[1]["records"].as_str(), Some("1"));
    assert_eq!(rows[2]["kind"].as_str(), Some("string"));
    assert_eq!(rows[2]["occurrences"].as_str(), Some("2"));
    assert_eq!(rows[2]["records"].as_str(), Some("2"));
    assert_eq!(result["coverage"]["eligible_records"].as_str(), Some("2"));
    for (path, kinds) in [
        ("", vec!["object"]),
        ("/", vec!["null"]),
        ("/m~0n", vec!["false", "true"]),
        ("/nested", vec!["object"]),
        ("/nested/a~1b", vec!["number"]),
    ] {
        let found = store
            .json_query(
                &scope,
                &JsonQuery::Keys {
                    path: Some(path.into()),
                },
                2,
            )
            .unwrap();
        let rows = found["rows"].as_array().unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row["kind"].as_str().unwrap())
                .collect::<Vec<_>>(),
            kinds
        );
        assert!(rows.iter().all(|row| row["path"].as_str() == Some(path)));
    }
    let mut read = JsonRead {
        scope: QueryScope {
            limit: 1,
            ..scope.clone()
        },
        query: JsonQuery::Keys {
            path: Some("/a~1b".into()),
        },
        cursor: None,
    };
    let first = store.json_read(&read, 2).unwrap();
    assert_eq!(first.total_items, 3);
    let evidence = first.body.result().unwrap()["query_evidence"].clone();
    assert_eq!(
        evidence["limits"]["query_path_bytes"].as_u64(),
        Some(sparrow_core::JSON_QUERY_PATH_BYTES as u64)
    );
    store
        .ingest(
            "key-paths-later",
            "keys",
            "jsonl",
            b"{\"a/b\":3,\"a/b\":null}\n",
            3,
        )
        .unwrap();
    read.cursor = first.next_cursor.clone();
    for path in [None, Some("".into()), Some("/m~0n".into())] {
        let changed = JsonRead {
            query: JsonQuery::Keys { path },
            ..read.clone()
        };
        assert_eq!(store.json_read(&changed, 4).unwrap_err().code, "CONFLICT");
    }
    let second = store.json_read(&read, 4).unwrap();
    assert_eq!(second.body.result().unwrap()["rows"][0], rows[1]);
    assert_eq!(second.body.result().unwrap()["query_evidence"], evidence);
    read.cursor = second.next_cursor;
    let third = store.json_read(&read, 4).unwrap();
    assert_eq!(third.body.result().unwrap()["rows"][0], rows[2]);
    let generic = store
        .result_next(&first.store_id, read.cursor.as_ref().unwrap(), 4)
        .unwrap();
    assert!(matches!(generic, sparrow_core::ResultPage::Keys(_)));
    let fresh = store
        .json_query(
            &scope,
            &JsonQuery::Keys {
                path: Some("/a~1b".into()),
            },
            4,
        )
        .unwrap();
    assert_eq!(fresh["rows"][0]["occurrences"].as_str(), Some("2"));
    assert_eq!(fresh["rows"][1]["occurrences"].as_str(), Some("3"));
}

#[test]
fn json_paths_use_native_pointer_syntax_and_bounded_admission_before_capture() {
    use sparrow_core::{JsonQuery, JsonRead, QueryScope};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .ingest("native-paths", "keys", "json", br#"{"a~1b":1,"a/b":2}"#, 1)
        .unwrap();
    let scope = QueryScope {
        selection: Default::default(),
        source: None,
        sources: None,
        diagnostics: Default::default(),
        require_complete_index: false,
        received_from: 0,
        received_before: 10,
        limit: 100,
        scan_budget: 100,
    };
    for path in ["not/a/pointer", "/a~2b", "/a~", "/~"] {
        for query in [
            JsonQuery::Keys {
                path: Some(path.into()),
            },
            JsonQuery::Values {
                path: Some(path.into()),
                kind: None,
                lexical: None,
                decoded: None,
            },
        ] {
            assert_eq!(
                store
                    .json_read(
                        &JsonRead {
                            scope: scope.clone(),
                            query,
                            cursor: None
                        },
                        2
                    )
                    .unwrap_err()
                    .code,
                "INVALID_ARGUMENT"
            );
        }
    }
    let escaped = store
        .json_query(
            &scope,
            &JsonQuery::Keys {
                path: Some("/a~01b".into()),
            },
            2,
        )
        .unwrap();
    assert_eq!(escaped["rows"].as_array().unwrap().len(), 1);
    assert_eq!(escaped["rows"][0]["path"].as_str(), Some("/a~01b"));
    let boundary = format!("/{}", "x".repeat(sparrow_core::JSON_QUERY_PATH_BYTES - 1));
    assert!(
        store
            .json_read(
                &JsonRead {
                    scope: scope.clone(),
                    query: JsonQuery::Keys {
                        path: Some(boundary.clone())
                    },
                    cursor: None
                },
                2
            )
            .unwrap()
            .body
            .result()
            .unwrap()["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let huge = format!("{boundary}x");
    assert_eq!(
        store
            .json_read(
                &JsonRead {
                    scope,
                    query: JsonQuery::Keys { path: Some(huge) },
                    cursor: None
                },
                2
            )
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
    assert_eq!(
        store.status(2).unwrap()["record_read_handles"].as_str(),
        Some("0")
    );
}

#[test]
fn recordless_rdf_scope_preserves_occurrences_filters_and_unknown_diagnostics() {
    use purrdf::{SparqlRequest, TermValue, sparql::QueryGovernors};
    use sparrow_core::{
        DiagnosticDimension, QueryAlternatives, QueryCancellation, QuerySelection, RdfGraphLayers,
        RdfReadLimits, SparqlRead,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let a = store
        .ingest("rdf-empty-a", "empty", "log", b"", 10)
        .unwrap();
    let b = store
        .ingest("rdf-empty-b", "empty", "jsonl", b"", 20)
        .unwrap();
    store
        .ingest("rdf-empty-other", "other", "log", b"", 20)
        .unwrap();
    let ordinary = store
        .ingest("rdf-not-empty", "empty", "json", b"{}", 20)
        .unwrap();
    let run = |scope: &sparrow_core::QueryScope, layers, now| {
        let governors = QueryGovernors::METERED.with_fuel(1_000_000);
        let result=store.sparql_query(&SparqlRead {
text_relations: None,
            scope,layers,request:SparqlRequest { query:"SELECT ?artifact WHERE { ?artifact a <urn:sparrow:vocab:Artifact> } ORDER BY ?artifact",base_iri:None,substitutions:&[] },
            governors:&governors,limits:RdfReadLimits::default(),
        },now,&QueryCancellation::default()).unwrap();
        let rows = result
            .outcome
            .unwrap()
            .result
            .solutions()
            .unwrap()
            .1
            .to_vec();
        (result.coverage, rows)
    };
    let mut scope = record_page_scope("empty", 1);
    let (coverage, rows) = run(&scope, RdfGraphLayers::ALL, 30);
    assert_eq!(coverage.selected_records, 1);
    assert_eq!(coverage.selected_artifacts, 3);
    assert_eq!(coverage.artifacts_visited, 4);
    assert!(coverage.complete());
    let actual = rows
        .into_iter()
        .map(|row| row[0].clone().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        actual,
        [
            TermValue::iri(format!("urn:sparrow:{}", a.id)),
            TermValue::iri(format!("urn:sparrow:{}", b.id)),
            TermValue::iri(format!("urn:sparrow:{}", ordinary.id))
        ]
        .into_iter()
        .collect()
    );
    scope.received_from = 10;
    scope.received_before = 20;
    assert_eq!(run(&scope, RdfGraphLayers::ALL, 30).0.selected_artifacts, 1);
    scope.selection = QuerySelection::Evidence(vec![
        b.id.clone(),
        b.id.clone(),
        ordinary.record_ids[0].clone(),
    ]);
    assert!(run(&scope, RdfGraphLayers::ALL, 30).1.is_empty());
    scope.received_before = 21;
    assert_eq!(run(&scope, RdfGraphLayers::ALL, 30).0.selected_artifacts, 1);
    assert!(
        run(
            &scope,
            RdfGraphLayers {
                metadata: false,
                diagnostic: true,
                content: true
            },
            30
        )
        .1
        .is_empty()
    );
    scope.selection = QuerySelection::Evidence(vec![a.id.clone()]);
    scope.diagnostics.insert(
        DiagnosticDimension::Service,
        QueryAlternatives::new(vec!["absent".into()]).unwrap(),
    );
    let (coverage, rows) = run(&scope, RdfGraphLayers::ALL, 30);
    assert!(rows.is_empty());
    assert_eq!(coverage.selected_records, 0);
    assert_eq!(coverage.selected_artifacts, 0);
    assert_eq!(coverage.artifact_diagnostic_unknown, 1);
    assert!(!coverage.complete());
    scope.require_complete_index = true;
    let error = store
        .with_rdf_scope(
            &scope,
            RdfGraphLayers::ALL,
            30,
            RdfReadLimits::default(),
            &QueryCancellation::default(),
            |_, _| panic!("strict unknown capture must not execute"),
        )
        .unwrap_err();
    assert_eq!(error.code, "INDEX_NOT_READY");
    scope.source = Some("other".into());
    assert!(run(&scope, RdfGraphLayers::ALL, 30).0.complete());
}

#[test]
fn recordless_rdf_survives_pin_capture_reopen_and_retires_with_its_artifact() {
    use purrdf::DatasetView;
    use sparrow_core::{QueryCancellation, QuerySelection, RdfGraphLayers, RdfReadLimits};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let empty = store
        .ingest("rdf-empty-lifetime", "empty", "log", b"", 10)
        .unwrap();
    let pin = store
        .pin(
            "rdf-empty-pin",
            "empty",
            std::slice::from_ref(&empty.id),
            40,
            20,
        )
        .unwrap();
    drop(store);
    let store = Store::open(dir.path(), config()).unwrap();
    let mut scope = record_page_scope("empty", 1);
    scope.selection = QuerySelection::Pin {
        id: pin.id.clone(),
        expected_revision: Some(pin.revision),
    };
    store
        .with_rdf_scope(
            &scope,
            RdfGraphLayers::ALL,
            31 * DAY,
            RdfReadLimits::default(),
            &QueryCancellation::default(),
            |view, coverage| {
                assert_eq!(coverage.selected_artifacts, 1);
                assert_eq!(coverage.selected_records, 0);
                let before = view.quads().collect::<Vec<_>>();
                assert_eq!(before.len(), 20);
                store
                    .release_retry("rdf-empty-release", &pin.id, 31 * DAY)
                    .unwrap();
                store.expire(31 * DAY).unwrap();
                assert_eq!(
                    view.quads().collect::<Vec<_>>(),
                    before,
                    "native snapshot preserves retired evidence inside its lease"
                );
            },
        )
        .unwrap();
    scope.selection = QuerySelection::Live;
    store
        .with_rdf_scope(
            &scope,
            RdfGraphLayers::ALL,
            31 * DAY,
            RdfReadLimits::default(),
            &QueryCancellation::default(),
            |view, coverage| {
                assert_eq!(coverage.selected_artifacts, 0);
                assert_eq!(view.quads().count(), 0);
            },
        )
        .unwrap();
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    assert!(db.get(format!("rdf:graph:{}", empty.id)).unwrap().is_none());
    assert!(
        db.get(format!("artifact-rdf:{}", empty.id))
            .unwrap()
            .is_none()
    );
}

#[test]
fn recordless_rdf_corruption_refuses_capture_and_expiry_obeys_maintenance_units() {
    use sparrow_core::{
        EvictionLimits, QueryCancellation, QuerySelection, RdfGraphLayers, RdfReadLimits,
    };
    for damage in 0..5 {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), config()).unwrap();
        let artifact = store
            .ingest("rdf-empty-corrupt", "empty", "log", b"", 10)
            .unwrap();
        drop(store);
        let db = catalog_db(dir.path()).unwrap();
        let key = match damage {
            0 => format!("artifact-rdf:{}", artifact.id),
            1 => format!("rdf:graph:{}", artifact.id),
            2 => format!("empty-time:{:020}:{}", artifact.received_at, artifact.id),
            _ => format!("provenance-rdf:{}", artifact.id),
        };
        if damage == 4 {
            db.put(key, b"invalid").unwrap();
        } else {
            db.delete(key).unwrap();
        }
        drop(db);
        let store = Store::open(dir.path(), config()).unwrap();
        let mut scope = record_page_scope("empty", 1);
        scope.selection = QuerySelection::Evidence(vec![artifact.id]);
        assert_eq!(
            store
                .with_rdf_scope(
                    &scope,
                    RdfGraphLayers::ALL,
                    20,
                    RdfReadLimits::default(),
                    &QueryCancellation::default(),
                    |_, _| panic!("corrupt owner must not execute")
                )
                .unwrap_err()
                .code,
            "STORAGE_FAILURE"
        );
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    store
        .ingest("rdf-empty-expiry-a", "empty", "log", b"", 10)
        .unwrap();
    store
        .ingest("rdf-empty-expiry-b", "empty", "log", b"", 20)
        .unwrap();
    let limits = EvictionLimits {
        max_records: 1,
        ..Default::default()
    };
    assert_eq!(
        store
            .expire_bounded(30 * DAY + 30, limits)
            .unwrap()
            .artifacts,
        1
    );
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    assert_eq!(
        db.prefix_iterator(b"artifact-rdf:")
            .take_while(|row| row.as_ref().unwrap().0.starts_with(b"artifact-rdf:"))
            .count(),
        1
    );
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .expire_bounded(30 * DAY + 30, limits)
            .unwrap()
            .artifacts,
        1
    );
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    assert!(
        !db.prefix_iterator(b"artifact-rdf:")
            .next()
            .is_some_and(|row| row.unwrap().0.starts_with(b"artifact-rdf:"))
    );
}

#[test]
fn recordless_rdf_admits_the_complete_batch_before_publishing_and_retries_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let mut limits = config();
    limits.disk_budget = limits.maintenance_reserve + 1024 * 1024;
    let store = Store::open(dir.path(), limits).unwrap();
    let physical = store.status(1).unwrap()["physical_bytes"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let ballast = dir.path().join("empty-rdf-admission-ballast");
    std::fs::File::create(&ballast)
        .unwrap()
        .set_len(1024 * 1024 - physical - 16384)
        .unwrap();
    let error = store
        .ingest("empty-rdf-pressure", "empty", "log", b"", 1)
        .unwrap_err();
    assert_eq!(error.code, "STORAGE_PRESSURE");
    assert!(
        store.pressure_status().unwrap().requested_bytes >= 65536,
        "the final RDF/catalog batch must own the refusal, beyond the earlier empty-record estimate"
    );
    assert_eq!(store.status(1).unwrap()["live_records"].as_str(), Some("0"));
    std::fs::remove_file(ballast).unwrap();
    let accepted = store
        .ingest("empty-rdf-pressure", "empty", "log", b"", 2)
        .unwrap();
    assert!(accepted.record_ids.is_empty());
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    assert_eq!(
        db.prefix_iterator(b"artifact:")
            .take_while(|row| row.as_ref().unwrap().0.starts_with(b"artifact:"))
            .count(),
        1
    );
    assert!(
        db.get(format!("artifact-rdf:{}", accepted.id))
            .unwrap()
            .is_some()
    );
}

#[test]
fn rdf_scope_charges_admitted_catalog_bodies_and_keeps_large_occurrence_counts() {
    use purrdf::{SparqlRequest, TermValue, sparql::QueryGovernors};
    use sparrow_core::{QueryCancellation, RdfGraphLayers, RdfReadLimits, SparqlRead};
    for records in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), config()).unwrap();
        if records {
            store
                .ingest(
                    "rdf-catalog-io",
                    "catalog-io",
                    "jsonl",
                    &b"{}\n".repeat(300),
                    1,
                )
                .unwrap();
        } else {
            for n in 0..300 {
                store
                    .ingest(&format!("rdf-empty-io-{n}"), "catalog-io", "log", b"", 1)
                    .unwrap();
            }
        }
        let scope = record_page_scope("catalog-io", 1);
        let governors = QueryGovernors::METERED.with_fuel(1_000_000);
        let query = if records {
            "SELECT (COUNT(*) AS ?count) WHERE { ?entity a <urn:sparrow:vocab:Record> }"
        } else {
            "SELECT (COUNT(*) AS ?count) WHERE { ?entity a <urn:sparrow:vocab:Artifact> }"
        };
        let result = store
            .sparql_query(
                &SparqlRead {
                    text_relations: None,
                    scope: &scope,
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
                    governors: &governors,
                    limits: RdfReadLimits::default(),
                },
                2,
                &QueryCancellation::default(),
            )
            .unwrap();
        assert_eq!(
            result.coverage.selected_records,
            if records { 300 } else { 0 }
        );
        assert_eq!(
            result.coverage.selected_artifacts,
            if records { 1 } else { 300 }
        );
        let run = result.outcome.unwrap();
        let rows = run.result.solutions().unwrap().1;
        assert!(
            matches!(&rows[0][0],Some(TermValue::Literal{lexical_form,..}) if lexical_form=="300")
        );
        assert!(
            run.evidence.view.admitted_bytes < 32 * 1024 * 1024,
            "tiny catalog bodies must not consume 512 KiB per occurrence"
        );
        eprintln!("records={records} source={:?}", run.evidence.view);
    }
}

#[test]
fn populated_artifact_rdf_follows_atomic_publication_and_independent_pin_lifetimes() {
    use purrdf::{SparqlRequest, TermValue, sparql::QueryGovernors};
    use sparrow_core::{QueryCancellation, QueryScope, RdfGraphLayers, RdfReadLimits, SparqlRead};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = b"one\ntwo\n";
    let a = store
        .ingest("artifact-direct", "oversized", "log", input, 10)
        .unwrap();
    let manifest = stage_fixture(&store, "artifact-staged", "log", input);
    let run = |store: &Store, scope: &QueryScope, now| {
        let governors = QueryGovernors::METERED.with_fuel(1_000_000);
        let result = store.sparql_query(&SparqlRead {
text_relations: None,
            scope, layers: RdfGraphLayers { metadata: true, diagnostic: false, content: false },
            request: SparqlRequest { query: "SELECT ?artifact ?bytes WHERE { ?artifact a <urn:sparrow:vocab:Artifact> ; <urn:sparrow:vocab:byteLength> ?bytes } ORDER BY ?artifact", base_iri: None, substitutions: &[] },
            governors: &governors, limits: RdfReadLimits::default(),
        }, now, &QueryCancellation::default()).unwrap();
        (
            result.coverage,
            result
                .outcome
                .unwrap()
                .result
                .solutions()
                .unwrap()
                .1
                .to_vec(),
        )
    };
    let scope = record_page_scope("oversized", 1);
    assert_eq!(run(&store, &scope, 20).1.len(), 1);
    for ordinal in 0..2 {
        store
            .import_prepare_record(&manifest.id, ordinal, 1)
            .unwrap();
    }
    assert_eq!(
        run(&store, &scope, 20).1.len(),
        1,
        "private preparation must not publish a parent"
    );
    let b = store.import_publish(&manifest.id, 1).unwrap();
    assert_eq!(a.payload_id, b.payload_id);
    let (coverage, rows) = run(&store, &scope, 20);
    assert_eq!(
        (coverage.selected_records, coverage.selected_artifacts),
        (4, 2)
    );
    assert_eq!(rows.len(), 2);
    for row in rows {
        assert!(
            matches!(&row[1], Some(TermValue::Literal { lexical_form, .. }) if lexical_form == "8")
        );
    }
    let mut records_only = scope.clone();
    records_only.selection = sparrow_core::QuerySelection::Evidence(vec![a.record_ids[0].clone()]);
    assert!(run(&store, &records_only, 20).1.is_empty());
    let whole = store
        .pin(
            "artifact-whole-pin",
            "whole",
            std::slice::from_ref(&a.id),
            40,
            20,
        )
        .unwrap();
    store
        .pin(
            "artifact-record-pin",
            "record",
            std::slice::from_ref(&b.record_ids[0]),
            40,
            20,
        )
        .unwrap();
    let original = store.record(&b.record_ids[0]).unwrap();
    store.expire(31 * DAY).unwrap();
    let (coverage, rows) = run(&store, &scope, 31 * DAY);
    assert_eq!(
        (coverage.selected_records, coverage.selected_artifacts),
        (3, 1)
    );
    assert_eq!(
        rows[0][0],
        Some(TermValue::iri(format!("urn:sparrow:{}", a.id)))
    );
    store
        .release_retry("artifact-whole-release", &whole.id, 31 * DAY)
        .unwrap();
    store.expire(31 * DAY).unwrap();
    assert!(run(&store, &scope, 31 * DAY).1.is_empty());
    assert_eq!(
        store.bytes(&b.record_ids[0], 0, 4, 31 * DAY).unwrap(),
        b"one\n"
    );
    let retained = store.record(&b.record_ids[0]).unwrap();
    assert_eq!(
        (retained.received_at, retained.expires_at),
        (original.received_at, original.expires_at)
    );
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    for artifact in [&a, &b] {
        assert!(
            db.get(format!("artifact-rdf:{}", artifact.id))
                .unwrap()
                .is_none()
        );
        assert!(
            db.get(format!(
                "artifact-time:{:020}:{}",
                artifact.received_at, artifact.id
            ))
            .unwrap()
            .is_none()
        );
    }
    assert!(
        db.get(format!("rdf:graph:{}", b.record_ids[0]))
            .unwrap()
            .is_some()
    );
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert!(run(&store, &scope, 31 * DAY).1.is_empty());
    assert_eq!(
        store.bytes(&b.record_ids[0], 0, 4, 31 * DAY).unwrap(),
        b"one\n"
    );
}

#[test]
fn populated_artifact_upgrade_preserves_original_records_and_requires_its_checkpoint() {
    use sparrow_core::{QueryCancellation, RdfGraphLayers, RdfReadLimits};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let artifact = store
        .ingest("artifact-upgrade", "upgrade", "log", b"original\n", 10)
        .unwrap();
    let original = store.record(&artifact.record_ids[0]).unwrap();
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    remove_artifact_rdf_upgrade(&db).unwrap();
    db.put("store_format", b"sparrow.store/44").unwrap();
    db.put(
        "artifact-rdf-index-upgrade",
        br#"{"after":null,"complete":true}"#,
    )
    .unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    let after = store.record(&original.id).unwrap();
    assert_eq!(
        (
            after.received_at,
            after.expires_at,
            after.byte_start,
            after.byte_end
        ),
        (
            original.received_at,
            original.expires_at,
            original.byte_start,
            original.byte_end
        )
    );
    assert_eq!(store.bytes(&artifact.id, 0, 9, 20).unwrap(), b"original\n");
    store
        .with_rdf_scope(
            &record_page_scope("upgrade", 1),
            RdfGraphLayers::ALL,
            20,
            RdfReadLimits::default(),
            &QueryCancellation::default(),
            |_, coverage| {
                assert_eq!(
                    (coverage.selected_records, coverage.selected_artifacts),
                    (1, 1)
                );
            },
        )
        .unwrap();
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    assert_eq!(
        db.get(format!("artifact-rdf:{}", artifact.id))
            .unwrap()
            .unwrap()
            .as_slice(),
        10_u64.to_be_bytes()
    );
    db.delete("all-artifact-rdf-index-upgrade").unwrap();
    drop(db);
    assert_eq!(
        Store::open(dir.path(), config()).err().unwrap().code,
        "STORAGE_FAILURE"
    );
}

#[test]
fn scope_receipts_normalize_sets_keep_limits_separate_and_bound_large_selectors() {
    use sparrow_core::{
        DiagnosticDimension, QueryAlternatives, QuerySelection, RecordOperation, RecordQuery,
        RecordRead,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = store
        .ingest("scope-identity", "pages", "jsonl", b"{}\n{}\n{}\n", 1)
        .unwrap();
    let capture = |scope| {
        let page = store
            .records_read(
                &RecordRead {
                    scope,
                    operation: RecordOperation::Records,
                    query: RecordQuery::default(),
                    cursor: None,
                },
                2,
            )
            .unwrap();
        let scope = page.body.result().unwrap()["query_evidence"]["scope"].clone();
        assert_eq!(
            store
                .result_release(&page.store_id, &page.snapshot_id, 2)
                .unwrap(),
            page.next_cursor.is_some()
        );
        scope
    };
    let mut scope = record_page_scope("pages", 1);
    let live = capture(scope.clone());
    assert!(live["direct_members"].is_null() && live["expanded_records"].is_null());
    assert_eq!(live["received_before_ms"].as_u64(), Some(u64::MAX));
    scope.selection = QuerySelection::Evidence(vec![
        input.id.clone(),
        input.record_ids[0].clone(),
        input.record_ids[0].clone(),
    ]);
    scope.sources =
        Some(QueryAlternatives::new(vec!["pages".into(), "other".into(), "pages".into()]).unwrap());
    let selected = capture(scope.clone());
    assert_eq!(selected["direct_members"].as_str(), Some("2"));
    assert_eq!(selected["expanded_records"].as_str(), Some("3"));
    assert_eq!(selected["source_alternatives"].as_u64(), Some(2));
    scope.selection = QuerySelection::Evidence(vec![input.record_ids[0].clone(), input.id.clone()]);
    scope.sources = Some(QueryAlternatives::new(vec!["other".into(), "pages".into()]).unwrap());
    scope.limit = 2;
    scope.scan_budget = 100;
    assert_eq!(capture(scope.clone()), selected);
    scope.received_from = 1;
    assert_ne!(capture(scope.clone())["id"], selected["id"]);
    scope.received_from = 0;
    scope.source = None;
    assert_ne!(capture(scope.clone())["id"], selected["id"]);
    scope.source = Some("pages".into());
    scope.diagnostics.insert(
        DiagnosticDimension::Service,
        QueryAlternatives::new(vec!["a".into(), "b".into(), "a".into()]).unwrap(),
    );
    let diagnostic = capture(scope.clone());
    assert_ne!(diagnostic["id"], selected["id"]);
    scope.diagnostics.insert(
        DiagnosticDimension::Service,
        QueryAlternatives::new(vec!["b".into(), "a".into()]).unwrap(),
    );
    assert_eq!(capture(scope.clone()), diagnostic);
    // All admitted selector dimensions can be large without inflating frozen metadata.
    let alternatives = QueryAlternatives::new(
        (0..128)
            .map(|n| format!("{n:03}{}", "s".repeat(509)))
            .collect(),
    )
    .unwrap();
    scope.sources = Some(alternatives.clone());
    for dimension in [
        DiagnosticDimension::Service,
        DiagnosticDimension::Host,
        DiagnosticDimension::Severity,
        DiagnosticDimension::TraceId,
        DiagnosticDimension::SpanId,
    ] {
        scope.diagnostics.insert(dimension, alternatives.clone());
    }
    scope.selection = QuerySelection::Evidence(vec![input.id; 1000]);
    scope.source = Some("s".repeat(100_000));
    let large = capture(scope);
    assert_eq!(large["has_exact_source"].as_bool(), Some(true));
    assert_eq!(large["source_alternatives"].as_u64(), Some(128));
    assert_eq!(large["diagnostic_dimensions"].as_u64(), Some(5));
    assert!(purrdf_lex::json::write_compact(&large).len() < 1024);
}

#[test]
fn graph_context_and_record_scopes_share_frozen_pin_evidence_through_renewal_and_release() {
    use purrdf::{SparqlRequest, sparql::QueryGovernors};
    use sparrow_core::{
        QueryCancellation, QuerySelection, RdfGraphLayers, RdfReadLimits, RecordOperation,
        RecordQuery, RecordRead, SparqlRead,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = store
        .ingest("shared-scope-input", "pages", "log", b"a\nb\nc\n", 0)
        .unwrap();
    let mut ids = input.record_ids.clone();
    ids.sort();
    let pin = store
        .pin("shared-scope-pin", "shared-scope", &[input.id], 40, 0)
        .unwrap();
    let now = 31 * DAY;
    let mut scope = record_page_scope("pages", 1);
    scope.selection = QuerySelection::Pin {
        id: pin.id.clone(),
        expected_revision: None,
    };
    let records = store
        .records_read(
            &RecordRead {
                scope: scope.clone(),
                operation: RecordOperation::Records,
                query: RecordQuery::default(),
                cursor: None,
            },
            now,
        )
        .unwrap();
    let frozen = records.body.result().unwrap()["query_evidence"]["scope"].clone();
    let governors = QueryGovernors::METERED.with_fuel(1_000_000);
    let graph = |scope: &sparrow_core::QueryScope, query: &str, at: u64| {
        store
            .sparql_read(
                &SparqlRead {
                    text_relations: None,
                    scope,
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
                    governors: &governors,
                    limits: RdfReadLimits::default(),
                },
                at,
                &QueryCancellation::default(),
            )
            .unwrap()
    };
    let first = graph(
        &scope,
        "SELECT ?record WHERE { ?record a <urn:sparrow:vocab:Record> } ORDER BY ?record",
        now,
    );
    assert_eq!(first.total_items, 3);
    assert_eq!(first.body.captured().scope, frozen);
    let cursor = first.next_cursor.as_ref().unwrap();
    scope.limit = 3;
    let context = store.records_context(&ids[1], &scope, 1, 1, now).unwrap();
    assert_eq!(context.scope, frozen);
    assert_eq!(context.before.records.len(), 1);
    assert_eq!(context.after.records.len(), 1);
    assert_eq!(
        context.limits.scope_receipt_bytes,
        sparrow_core::QUERY_SCOPE_WORKSPACE_BYTES as u64
    );
    let renewed = store
        .renew_pin("shared-scope-renew", &pin.id, 40, Some(pin.revision), now)
        .unwrap();
    let fresh = store.records_context(&ids[1], &scope, 1, 1, now).unwrap();
    assert_eq!(
        fresh.scope["pin"]["revision"].as_u64(),
        Some(renewed.revision)
    );
    assert_ne!(fresh.scope["id"], frozen["id"]);
    // A native parser failure still carries the captured scope, never invented rows.
    let failure = graph(&scope, "SELECT {", now);
    assert!(failure.body.captured().outcome.is_err());
    assert_eq!(failure.body.captured().scope, fresh.scope);
    assert_eq!(failure.total_items, 0);
    store
        .release_retry("shared-scope-release", &pin.id, now + 1)
        .unwrap();
    store.expire(now + 2).unwrap();
    let second = store
        .sparql_read_next(&first.store_id, cursor, now + 3)
        .unwrap();
    assert_eq!(second.body.captured().scope, frozen);
    assert_eq!(context.scope, frozen);
    assert_eq!(second.as_of, first.as_of);
    assert!(
        store
            .result_release(&first.store_id, &first.snapshot_id, now + 4)
            .unwrap()
    );
    assert!(
        store
            .result_release(&records.store_id, &records.snapshot_id, now + 4)
            .unwrap()
    );
    let empty = store
        .ingest("shared-scope-empty", "pages", "log", b"", now + 5)
        .unwrap();
    assert!(empty.record_ids.is_empty());
    scope.selection = QuerySelection::Evidence(vec![empty.id]);
    let empty = graph(
        &scope,
        "ASK { ?artifact a <urn:sparrow:vocab:Artifact> }",
        now + 5,
    );
    assert_eq!(
        empty.body.captured().scope["direct_members"].as_str(),
        Some("1")
    );
    assert_eq!(
        empty.body.captured().scope["expanded_records"].as_str(),
        Some("0")
    );
}

#[test]
fn native_correlation_recipes_bind_selectors_preserve_occurrences_and_report_unknown_interpretations()
 {
    use purrdf::{TermValue, sparql::QueryGovernors};
    use sparrow_core::{
        CorrelateRead, CorrelationClock, CorrelationInterval, CorrelationRelation as Relation,
        CorrelationSelector as Selector, DiagnosticDimension as Dimension, QueryCancellation,
        RdfReadLimits, SparqlItem, TimeRepresentation,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    for source in ["correlation-a", "correlation-b"] {
        store
            .define_diagnostic_profile(
                source,
                [
                    (Dimension::TraceId, "/trace".into()),
                    (Dimension::SpanId, "/span".into()),
                ]
                .into_iter()
                .collect(),
            )
            .unwrap();
        store
            .define_time_profile(source, "/time", TimeRepresentation::UnixMillis)
            .unwrap();
    }
    let a = store
        .ingest(
            "correlation-a",
            "correlation-a",
            "jsonl",
            b"{\"trace\":\"t\",\"span\":\"7\",\"time\":1000}\n",
            10,
        )
        .unwrap();
    let b = store
        .ingest(
            "correlation-b",
            "correlation-b",
            "jsonl",
            b"{\"trace\":\"t\",\"span\":\"7\",\"time\":1000}\n",
            20,
        )
        .unwrap();
    let c = store
        .ingest(
            "correlation-c",
            "correlation-b",
            "jsonl",
            b"{\"trace\":\"different\",\"span\":\"8\",\"time\":2000}\n",
            30,
        )
        .unwrap();
    store
        .ingest(
            "correlation-unknown",
            "correlation-unknown",
            "log",
            b"no declared interpretations\n",
            15,
        )
        .unwrap();
    let mut scope = record_page_scope("unused", 1000);
    scope.source = None;
    let governors = QueryGovernors::METERED.with_fuel(1_000_000);
    let run = |scope: &sparrow_core::QueryScope, selector: &Selector, relation| {
        store.correlate_read(
            &CorrelateRead {
                scope,
                selector,
                relation,
                governors: &governors,
                limits: RdfReadLimits::default(),
            },
            40,
            &QueryCancellation::default(),
        )
    };
    let selector = Selector::Evidence(vec![a.record_ids[0].clone()]);
    let first = run(&scope, &selector, Relation::TraceId).unwrap();
    assert_eq!(first.total_items, 1);
    assert!(
        first.body.captured().outcome.is_ok(),
        "{:?}",
        first.body.captured().outcome
    );
    let Some(SparqlItem::Solution(row)) = first.body.items().next() else {
        panic!("native correlation solution expected")
    };
    assert_eq!(
        row[0],
        Some(TermValue::simple_literal(a.record_ids[0].clone()))
    );
    assert_eq!(
        row[1],
        Some(TermValue::simple_literal(b.record_ids[0].clone()))
    );
    assert_eq!(
        row[12],
        Some(TermValue::typed_literal(
            "0",
            "http://www.w3.org/2001/XMLSchema#nonNegativeInteger"
        ))
    );
    let capture = first.body.captured().correlation.as_ref().unwrap();
    assert!(!capture.complete(&first.body.captured().coverage));
    assert_eq!(first.body.captured().coverage.event_time_known, 3);
    assert_eq!(
        first.body.captured().coverage.diagnostic_known[&Dimension::TraceId],
        3
    );
    for (selector, relation, expected) in [
        (Selector::Evidence(vec![a.id.clone()]), Relation::SpanId, 1),
        (
            Selector::Diagnostic(Dimension::TraceId, "t".into()),
            Relation::TraceId,
            2,
        ),
        (
            Selector::Source("correlation-a".into()),
            Relation::SpanId,
            1,
        ),
        (
            Selector::Evidence(vec![b.record_ids[0].clone()]),
            Relation::Source,
            1,
        ),
        (
            Selector::Time(CorrelationInterval {
                clock: CorrelationClock::Received,
                from: 10,
                before: 21,
            }),
            Relation::TraceId,
            2,
        ),
        (
            Selector::Evidence(vec![a.record_ids[0].clone()]),
            Relation::Time(CorrelationInterval {
                clock: CorrelationClock::Received,
                from: 10,
                before: 21,
            }),
            2,
        ),
        (
            Selector::Evidence(vec![a.record_ids[0].clone()]),
            Relation::Time(CorrelationInterval {
                clock: CorrelationClock::Event,
                from: 1000,
                before: 2000,
            }),
            1,
        ),
        (
            Selector::Diagnostic(Dimension::TraceId, "t\" } UNION { ?record ?p ?o } #".into()),
            Relation::TraceId,
            0,
        ),
    ] {
        let page = run(&scope, &selector, relation).unwrap();
        assert!(
            page.body.captured().outcome.is_ok(),
            "{:?}",
            page.body.captured().outcome
        );
        assert_eq!(page.total_items, expected, "{selector:?} {relation:?}");
    }
    scope.require_complete_index = true;
    assert_eq!(
        run(&scope, &selector, Relation::TraceId).unwrap_err().code,
        "INDEX_NOT_READY"
    );
    scope.selection = sparrow_core::QuerySelection::Evidence(vec![
        a.record_ids[0].clone(),
        b.record_ids[0].clone(),
        c.record_ids[0].clone(),
    ]);
    let ready = run(&scope, &selector, Relation::TraceId).unwrap();
    assert!(
        ready
            .body
            .captured()
            .correlation
            .as_ref()
            .unwrap()
            .complete(&ready.body.captured().coverage)
    );
    let one = run(
        &scope,
        &Selector::Evidence(vec![
            a.record_ids[0].clone(),
            b.record_ids[0].clone(),
            a.record_ids[0].clone(),
        ]),
        Relation::TraceId,
    )
    .unwrap();
    let two = run(
        &scope,
        &Selector::Evidence(vec![b.record_ids[0].clone(), a.record_ids[0].clone()]),
        Relation::TraceId,
    )
    .unwrap();
    assert_eq!(
        one.body.captured().correlation,
        two.body.captured().correlation
    );
    assert_eq!(one.total_items, 2);
    assert_eq!(
        run(
            &scope,
            &Selector::Evidence(vec!["record:unknown".into()]),
            Relation::Source
        )
        .unwrap_err()
        .code,
        "NOT_FOUND"
    );
    let pin = store
        .pin(
            "correlation-retain-records",
            "correlation-records",
            &[a.record_ids[0].clone(), b.record_ids[0].clone()],
            40,
            40,
        )
        .unwrap();
    scope.selection = sparrow_core::QuerySelection::Pin {
        id: pin.id.clone(),
        expected_revision: None,
    };
    scope.limit = 1;
    let selector = Selector::Diagnostic(Dimension::TraceId, "t".into());
    let now = 31 * DAY;
    let captured = store
        .correlate_read(
            &CorrelateRead {
                scope: &scope,
                selector: &selector,
                relation: Relation::TraceId,
                governors: &governors,
                limits: RdfReadLimits::default(),
            },
            now,
            &QueryCancellation::default(),
        )
        .unwrap();
    assert_eq!(captured.total_items, 2);
    let cursor = captured.next_cursor.as_ref().unwrap();
    let invalid_parent = store.correlate_read(
        &CorrelateRead {
            scope: &scope,
            selector: &Selector::Evidence(vec![a.id]),
            relation: Relation::TraceId,
            governors: &governors,
            limits: RdfReadLimits::default(),
        },
        now,
        &QueryCancellation::default(),
    );
    assert_eq!(invalid_parent.unwrap_err().code, "EVIDENCE_UNAVAILABLE");
    store
        .release_retry("correlation-release-records", &pin.id, now + 1)
        .unwrap();
    store.expire(now + 2).unwrap();
    let replay = store
        .sparql_read_next(&captured.store_id, cursor, now + 3)
        .unwrap();
    assert_eq!(
        replay.body.captured().correlation,
        captured.body.captured().correlation
    );
    assert_eq!(replay.body.captured().scope, captured.body.captured().scope);
    assert_eq!(replay.body.items().count(), 1);
    assert!(
        store
            .result_release(&captured.store_id, &captured.snapshot_id, now + 4)
            .unwrap()
    );
}

fn archiving_config() -> Config {
    Config {
        archive: Some(sparrow_core::ArchivePolicy::default()),
        ..config()
    }
}

fn archive_position(received_at: u64, id: &str) -> String {
    format!("{received_at:020}:{id}")
}

/// Commit one segment over the next `count` record positions, through the production gate API.
fn archive_records(store: &Store, segment: u8, positions: &[String]) {
    let intent = sparrow_core::ArchiveIntent {
        segment_id: [segment; 32],
        first: positions[0].clone(),
        last: positions[positions.len() - 1].clone(),
        records: positions.len() as u64,
        artifacts: 0,
    };
    store.archive_intend(&intent).unwrap();
    store.archive_commit(&intent.segment_id).unwrap();
}

/// INVARIANT: expiry never retires evidence a committed archive segment does not cover,
/// retires it once the watermark passes it, and publication behind the watermark or an
/// in-flight segment is refused as a clock regression.
#[test]
fn archive_gate_holds_expiry_until_a_committed_segment_covers_the_evidence() {
    use sparrow_core::{ArchiveIntent, EvictionLimits, RetentionReadLimits};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), archiving_config()).unwrap();
    let first = store
        .ingest("archive-first", "source", "log", b"a\nb\n", 1)
        .unwrap();
    let second = store
        .ingest("archive-second", "source", "log", b"c\n", 2)
        .unwrap();
    let empty = store
        .ingest("archive-empty", "source", "log", b"", 3)
        .unwrap();
    assert!(empty.record_ids.is_empty());
    let mut covered = first
        .record_ids
        .iter()
        .map(|id| archive_position(1, id))
        .collect::<Vec<_>>();
    covered.sort();
    let later = 31 * DAY;
    let owned = |store: &Store| {
        store
            .retention_metrics(later, RetentionReadLimits::default())
            .unwrap()
            .owned_payload_bytes
    };
    let before = owned(&store);
    let waiting = store
        .expire_bounded(later, EvictionLimits::default())
        .unwrap();
    assert_eq!((waiting.records, waiting.artifacts), (0, 0));
    assert_eq!(waiting.archive_waiting, 6);
    assert_eq!(owned(&store), before);

    // An intent must start at the first unarchived entry and match the catalog.
    let skipped = ArchiveIntent {
        segment_id: [1; 32],
        first: covered[1].clone(),
        last: covered[1].clone(),
        records: 1,
        artifacts: 0,
    };
    assert_eq!(store.archive_intend(&skipped).unwrap_err().code, "CONFLICT");
    let miscounted = ArchiveIntent {
        first: covered[0].clone(),
        records: 1,
        ..skipped.clone()
    };
    assert_eq!(
        store.archive_intend(&miscounted).unwrap_err().code,
        "CONFLICT"
    );
    let intent = ArchiveIntent {
        records: 2,
        ..miscounted
    };
    store.archive_intend(&intent).unwrap();
    store.archive_intend(&intent).unwrap();
    // An in-flight segment does not authorize deletion.
    let pending = store
        .expire_bounded(later, EvictionLimits::default())
        .unwrap();
    assert_eq!((pending.records, pending.archive_waiting), (0, 6));
    assert_eq!(store.archive_commit(&[2; 32]).unwrap_err().code, "CONFLICT");
    store.archive_commit(&intent.segment_id).unwrap();
    assert_eq!(
        store.archive_state().unwrap().unwrap().through.as_deref(),
        Some(covered[1].as_str())
    );
    let retired = store
        .expire_bounded(later, EvictionLimits::default())
        .unwrap();
    assert_eq!((retired.records, retired.artifacts), (2, 1));
    assert_eq!(retired.archive_waiting, 3);
    assert!(owned(&store) < before);
    assert_eq!(store.bytes(&second.record_ids[0], 0, 2, 3).unwrap(), b"c\n");

    // Publication at or behind the watermark or an in-flight segment is a clock regression.
    assert_eq!(
        store
            .ingest("archive-regressed", "source", "log", b"late\n", 0)
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
    let next = ArchiveIntent {
        segment_id: [3; 32],
        first: archive_position(2, &second.record_ids[0]),
        last: archive_position(3, &empty.id),
        records: 1,
        artifacts: 1,
    };
    store.archive_intend(&next).unwrap();
    assert_eq!(
        store
            .ingest("archive-in-flight", "source", "log", b"late\n", 2)
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
    store
        .ingest("archive-current", "source", "log", b"now\n", 4)
        .unwrap();
}

/// INVARIANT: pressure retirement stops at the archive watermark, reports that it is
/// waiting for the archive, and keeps refusing data writes until archiving catches up.
#[test]
fn pressure_stops_at_the_archive_watermark_and_keeps_refusing_ingest() {
    use sparrow_core::{EvictionFrontier, EvictionLimits, PressurePhase};
    let dir = tempfile::tempdir().unwrap();
    let mut settings = archiving_config();
    settings.disk_budget = 16 * 1024 * 1024;
    settings.maintenance_reserve = 4 * 1024 * 1024;
    let store = Store::open(dir.path(), settings).unwrap();
    let old = store
        .ingest("archive-pressure-old", "source", "log", b"old\n", 1)
        .unwrap();
    store
        .ingest("archive-pressure-next", "source", "log", b"next\n", 2)
        .unwrap();
    let ballast = dir.path().join("test-owned-pressure-ballast");
    std::fs::write(&ballast, vec![0_u8; 13 * 1024 * 1024]).unwrap();
    let refused = |now| {
        store
            .ingest("archive-pressure-refused", "source", "log", b"later\n", now)
            .unwrap_err()
            .code
    };
    assert_eq!(refused(4), "STORAGE_PRESSURE");
    let limits = EvictionLimits::default();
    for now in [5, 6] {
        let state = store.pressure_maintain(now, limits).unwrap();
        assert_eq!(state.phase, PressurePhase::AwaitingArchive);
        assert_eq!(state.evicted_records, 0);
        let pass = state.last_pass.unwrap();
        assert_eq!(pass.frontier, EvictionFrontier::ArchiveLag);
        assert!(pass.continuation);
        assert_eq!(refused(now), "STORAGE_PRESSURE");
        assert_eq!(store.bytes(&old.id, 0, 4, now).unwrap(), b"old\n");
    }
    archive_records(&store, 1, &[archive_position(1, &old.record_ids[0])]);
    let state = store.pressure_maintain(7, limits).unwrap();
    assert_eq!(state.phase, PressurePhase::AwaitingReclamation);
    let pass = state.last_pass.unwrap();
    assert_eq!(pass.records, old.record_ids);
    assert_eq!(pass.frontier, EvictionFrontier::ArchiveLag);
    assert_eq!(
        store.bytes(&old.id, 0, 1, 7).unwrap_err().code,
        "EVIDENCE_UNAVAILABLE"
    );
    std::fs::remove_file(ballast).unwrap();
}

/// INVARIANT: archiving never weakens a pin; archived pinned evidence survives expiry
/// and eviction until its pin ends, then retires through the same gate.
#[test]
fn archived_pinned_record_survives_until_its_pin_ends() {
    use sparrow_core::EvictionLimits;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), archiving_config()).unwrap();
    let artifact = store
        .ingest("archive-pinned", "source", "log", b"keep\nother\n", 0)
        .unwrap();
    let keep = artifact.record_ids[0].clone();
    store
        .pin("archive-pin", "case", std::slice::from_ref(&keep), 40, 0)
        .unwrap();
    let mut positions = artifact
        .record_ids
        .iter()
        .map(|id| archive_position(0, id))
        .collect::<Vec<_>>();
    positions.sort();
    archive_records(&store, 1, &positions);
    let evicted = store.evict_oldest(DAY, EvictionLimits::default()).unwrap();
    assert_eq!(evicted.protected, 1);
    let expired = store
        .expire_bounded(31 * DAY, EvictionLimits::default())
        .unwrap();
    assert_eq!(expired.archive_waiting, 0);
    assert_eq!(store.bytes(&keep, 0, 5, 31 * DAY).unwrap(), b"keep\n");
    let released = store
        .expire_bounded(41 * DAY, EvictionLimits::default())
        .unwrap();
    assert_eq!(released.records, 1);
    assert_eq!(
        store.bytes(&keep, 0, 1, 41 * DAY).unwrap_err().code,
        "NOT_FOUND"
    );
}

/// INVARIANT: a store fenced for archiving refuses to open without its policy, refuses
/// a missing watermark, and carries an identity older binaries cannot open; stores
/// without the fence open exactly as before.
#[test]
fn archive_fence_refuses_unconfigured_open_and_older_writers() {
    let dir = tempfile::tempdir().unwrap();
    let plain = Store::open(dir.path(), config()).unwrap();
    let unfenced = plain.properties().unwrap()["store_format"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(plain.archive_state().unwrap(), None);
    drop(plain);
    let store = Store::open(dir.path(), archiving_config()).unwrap();
    let fenced = store.properties().unwrap()["store_format"]
        .as_str()
        .unwrap()
        .to_owned();
    let number = |format: &str| {
        format
            .strip_prefix("sparrow.store/")
            .unwrap()
            .parse::<u32>()
            .unwrap()
    };
    assert!((99..=109).contains(&number(&fenced)), "{fenced}");
    assert_eq!(number(&fenced) - 99, (number(&unfenced) - 33) % 11);
    assert_eq!(
        store.archive_state().unwrap(),
        Some(sparrow_core::ArchiveState::default())
    );
    // A later authority transition keeps the archive fence.
    store.watch_registry_replace(None, b"registered").unwrap();
    let watched = store.properties().unwrap()["store_format"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!((99..=109).contains(&number(&watched)), "{watched}");
    drop(store);
    let refused = Store::open(dir.path(), config()).err().unwrap();
    assert_eq!(refused.code, "INVALID_ARGUMENT");
    assert!(refused.message.contains("[archive]"));
    let invalid = Config {
        archive: Some(sparrow_core::ArchivePolicy {
            start_fraction: 0.2,
            ..sparrow_core::ArchivePolicy::default()
        }),
        ..config()
    };
    assert_eq!(
        Store::open(dir.path(), invalid).err().unwrap().code,
        "INVALID_ARGUMENT"
    );
    drop(Store::open(dir.path(), archiving_config()).unwrap());
    let db = catalog_db(dir.path()).unwrap();
    let through = db.get("archive:through").unwrap().unwrap();
    db.delete("archive:through").unwrap();
    drop(db);
    assert_eq!(
        Store::open(dir.path(), archiving_config())
            .err()
            .unwrap()
            .code,
        "STORAGE_FAILURE"
    );
    // A writer that does not know an identity refuses it, as pre-archive binaries do.
    // The log time law is removed so the format identity alone decides the refusal.
    let db = catalog_db(dir.path()).unwrap();
    db.put("archive:through", through).unwrap();
    db.put("store_format", b"sparrow.store/110").unwrap();
    db.delete("log-time-profile-law").unwrap();
    drop(db);
    assert_eq!(
        Store::open(dir.path(), archiving_config())
            .err()
            .unwrap()
            .code,
        "UNSUPPORTED"
    );
}

/// The sections of the newest native OPTIONS file, keyed by their header line.
fn native_options(path: &std::path::Path) -> std::collections::BTreeMap<String, String> {
    let newest = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.starts_with("OPTIONS-") && !name.ends_with(".dbtmp"))
        .max_by_key(|name| name["OPTIONS-".len()..].parse::<u64>().unwrap())
        .unwrap();
    let text = std::fs::read_to_string(path.join(newest)).unwrap();
    let mut sections = std::collections::BTreeMap::new();
    let mut header = String::new();
    for line in text.lines().map(str::trim) {
        if line.starts_with('[') {
            header = line.to_string();
        }
        sections
            .entry(header.clone())
            .or_insert_with(String::new)
            .push_str(&format!("{line}\n"));
    }
    sections
}

#[test]
fn every_column_family_uses_the_shared_table_options() {
    // INVARIANT: each family is opened with its measured table and compression
    // options, and a new store owes no rewrite.
    use sparrow_core::RecompressionPhase;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let options = native_options(&store.data_path().unwrap());
    let section = |header: &str| options.get(header).unwrap().as_str();
    let database = section("[DBOptions]");
    assert!(database.contains("keep_log_file_num=10\n"));
    assert!(database.contains("max_log_file_size=8388608\n"));
    for (family, block, dictionary, training) in [
        ("default", 32768, 0, 0),
        ("payload", 32768, 114688, 11468800),
    ] {
        let family_options = section(&format!("[CFOptions \"{family}\"]"));
        assert!(family_options.contains("compression=kLZ4Compression\n"));
        assert!(family_options.contains("bottommost_compression=kZSTD\n"));
        let bottommost = family_options
            .lines()
            .find(|line| line.starts_with("bottommost_compression_opts="))
            .unwrap();
        for setting in [
            "enabled=true;".to_string(),
            "level=6;".to_string(),
            format!("max_dict_bytes={dictionary};"),
            format!("zstd_max_train_bytes={training};"),
        ] {
            assert!(bottommost.contains(&setting), "{family}: {bottommost}");
        }
        let table = section(&format!("[TableOptions/BlockBasedTable \"{family}\"]"));
        assert!(table.contains(&format!("block_size={block}\n")), "{family}");
    }
    let reverse = section("[CFOptions \"rdf_reverse\"]");
    assert!(reverse.contains("bottommost_compression_opts={") && !reverse.contains("level=6;"));
    let table = section("[TableOptions/BlockBasedTable \"rdf_reverse\"]");
    assert!(table.contains("block_size=4096\n"));
    assert!(table.contains("whole_key_filtering=true\n"));
    assert!(table.contains("filter_policy=bloomfilter\n"));
    assert!(section("[CFOptions \"payload\"]").contains("enable_blob_files=true\n"));
    let status = store.recompression_status().unwrap();
    assert_eq!(status.phase, RecompressionPhase::Complete);
    assert_eq!(status.remaining_bytes, 0);
}

/// A closed store whose table files predate the current table options profile.
/// The rewritten families hold bottommost files and level-0 files, and
/// `disk_budget` leaves room for a rewrite job's headroom.
fn legacy_store(dir: &std::path::Path) -> Config {
    let mut settings = config();
    settings.disk_budget = 4 * 1024 * 1024 * 1024;
    let ingest = |batch: u64| {
        let store = Store::open(dir, settings.clone()).unwrap();
        let input = (0..200)
            .map(|line| {
                format!("{{\"batch\":{batch},\"line\":{line},\"message\":\"repeated log text\"}}\n")
            })
            .collect::<String>();
        store
            .ingest(
                &format!("legacy-{batch}"),
                "source",
                "jsonl",
                input.as_bytes(),
                1 + batch,
            )
            .unwrap();
        store.flush().unwrap();
    };
    for batch in 0..3 {
        ingest(batch);
    }
    {
        // Former options, with small files so the bottom level holds several.
        let mut options = rocksdb::Options::default();
        options.set_target_file_size_base(16 * 1024);
        let columns = rocksdb::DB::list_cf(&options, dir).unwrap();
        let db = rocksdb::DB::open_cf(&options, dir, columns).unwrap();
        for name in ["default", "payload"] {
            let family = db.cf_handle(name).unwrap();
            db.compact_range_cf(family, None::<&[u8]>, None::<&[u8]>);
        }
        db.delete("table-options-profile").unwrap();
    }
    ingest(3);
    settings
}

/// Live table files of the rewritten families in a closed store, by name and level.
fn rewritten_tables(dir: &std::path::Path) -> std::collections::BTreeMap<String, i32> {
    catalog_db(dir)
        .unwrap()
        .live_files()
        .unwrap()
        .into_iter()
        .filter(|file| file.column_family_name != "rdf_reverse")
        .map(|file| (file.name, file.level))
        .collect()
}

fn wall_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Each rewritten family's periodic window in the newest native OPTIONS file.
fn rewrite_windows(path: &std::path::Path) -> Vec<String> {
    let options = native_options(path);
    ["default", "payload"]
        .iter()
        .map(|family| {
            options[&format!("[CFOptions \"{family}\"]")]
                .lines()
                .find_map(|line| line.strip_prefix("periodic_compaction_seconds="))
                .unwrap()
                .to_string()
        })
        .collect()
}

#[test]
fn table_rewrite_resumes_after_reopen_and_runs_once() {
    // INVARIANT: the rewrite records its start durably, resumes from it after an
    // interrupt, rewrites every older table file of its families at every level,
    // and does not run again once complete.
    use sparrow_core::RecompressionPhase;
    let dir = tempfile::tempdir().unwrap();
    let settings = legacy_store(dir.path());
    let before = rewritten_tables(dir.path());
    assert!(before.values().any(|level| *level == 0), "{before:?}");
    assert!(before.values().any(|level| *level > 0), "{before:?}");
    let store = Store::open(dir.path(), settings.clone()).unwrap();
    let first = store.recompress_step(wall_millis(), 1).unwrap();
    assert_eq!(first.phase, RecompressionPhase::Running);
    assert!(first.remaining_bytes > 0);
    drop(store);
    // Reopening may flush recovered writes; files after the start are not owed.
    let store = Store::open(dir.path(), settings.clone()).unwrap();
    let resumed = store.recompression_status().unwrap();
    assert_eq!(resumed.phase, RecompressionPhase::Running);
    assert!(resumed.remaining_bytes <= first.remaining_bytes);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut steps = vec![resumed.remaining_bytes];
    loop {
        std::thread::sleep(std::time::Duration::from_millis(250));
        let status = store.recompress_step(wall_millis(), 1).unwrap();
        assert_eq!(status.failure, None);
        if status.phase == RecompressionPhase::Complete {
            break;
        }
        assert!(
            status.remaining_bytes <= *steps.last().unwrap(),
            "{steps:?}"
        );
        steps.push(status.remaining_bytes);
        assert!(std::time::Instant::now() < deadline, "{steps:?}");
    }
    assert!(steps.len() > 1);
    assert_eq!(rewrite_windows(&store.data_path().unwrap()), ["0", "0"]);
    drop(store);
    let after = rewritten_tables(dir.path());
    assert!(
        before.keys().all(|name| !after.contains_key(name)),
        "{before:?} {after:?}"
    );
    let store = Store::open(dir.path(), settings).unwrap();
    assert_eq!(
        store.recompression_status().unwrap().phase,
        RecompressionPhase::Complete
    );
    let again = store.recompress_step(wall_millis(), 1).unwrap();
    assert_eq!(again.phase, RecompressionPhase::Complete);
    assert_eq!(again.remaining_bytes, 0);
}

#[test]
fn table_rewrite_stays_off_until_a_job_fits_the_allowance_and_floor() {
    // INVARIANT: native rewriting is turned on only while one job's headroom
    // fits both the data allowance and the free-space floor.
    use sparrow_core::RecompressionPhase;
    let dir = tempfile::tempdir().unwrap();
    let settings = legacy_store(dir.path());
    let mut floor = settings.clone();
    floor.min_free_bytes = u64::MAX / 2;
    let store = Store::open(dir.path(), floor).unwrap();
    let held = store.recompress_step(wall_millis(), 1).unwrap();
    assert_eq!(held.phase, RecompressionPhase::Paused);
    let waiting = held.waiting.unwrap();
    assert!(waiting.contains("bytes free"), "{waiting}");
    drop(store);
    let mut allowance = settings.clone();
    allowance.disk_budget = 1024 * 1024 * 1024;
    let store = Store::open(dir.path(), allowance).unwrap();
    let held = store.recompress_step(wall_millis(), 1).unwrap();
    assert_eq!(held.phase, RecompressionPhase::Paused);
    let waiting = held.waiting.unwrap();
    assert!(waiting.contains("data allowance"), "{waiting}");
    assert_eq!(rewrite_windows(&store.data_path().unwrap()), ["0", "0"]);
    drop(store);
    let store = Store::open(dir.path(), settings).unwrap();
    // The first ungated step records the start; the window opens a settle later.
    let started = store.recompress_step(wall_millis(), 1).unwrap();
    assert_eq!(started.phase, RecompressionPhase::Running);
    assert_eq!(rewrite_windows(&store.data_path().unwrap()), ["0", "0"]);
    std::thread::sleep(std::time::Duration::from_millis(2100));
    let running = store.recompress_step(wall_millis(), 1).unwrap();
    assert_eq!(running.phase, RecompressionPhase::Running);
    assert_eq!(running.waiting, None);
    let windows = rewrite_windows(&store.data_path().unwrap());
    assert!(windows.iter().all(|window| window != "0"), "{windows:?}");
}
