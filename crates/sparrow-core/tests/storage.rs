// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Persistent lifetime scenarios using temporary stores and injected UTC instants.

use sparrow_core::{Config, PinPageBody, PinRead, PinSelector, Store};

const DAY: u64 = 86_400_000;

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
    let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
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
fn stored_native_queries_preserve_occurrences_scope_and_pinned_evidence() {
    use purrdf::{
        SparqlRequest, SparqlResult,
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
    let SparqlResult::Solutions { rows, .. } = result.result else {
        panic!("SELECT result required")
    };
    assert_eq!(rows.len(), 4);
    assert!(!format!("{rows:?}").contains(&unrelated.record_ids[0]));
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
    for query in [
        "SELECT ?s WHERE { GRAPH ?g { ?s <urn:sparrow:vocab:sourceName> ?name } }",
        "ASK { GRAPH ?g { ?s <urn:sparrow:vocab:sourceName> \"rdf-native\" } }",
        "CONSTRUCT { ?s <urn:sparrow:vocab:sourceName> ?name } WHERE { GRAPH ?g { ?s <urn:sparrow:vocab:sourceName> ?name } }",
        "DESCRIBE ?s WHERE { GRAPH ?g { ?s <urn:sparrow:vocab:sourceName> \"rdf-native\" } }",
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
        let purrdf::sparql::FallibleSparqlError::Query { diagnostic, .. } = *result.unwrap_err()
        else {
            panic!("native admission diagnostic required")
        };
        assert_eq!(diagnostic.code, "native-sparql-workspace-unpriced");
    }
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
        let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
        let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
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
    let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
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
    let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
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
                    db.get(reverse).unwrap().unwrap(),
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
    let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
    for prefix in [
        "rdf:q:",
        "rdf:term:",
        "rdf:reverse:",
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
        Some("sparrow.store/5")
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
        let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
        let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
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
        source: Some("context".into()),
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
    assert_eq!(context.after.visited, 3);
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
        source: None,
        received_from: 0,
        received_before: 3,
        limit: 100,
        scan_budget: 100,
    };
    let neighbor_key = format!("time:{:020}:{}", 2, neighbor.record_ids[0]);
    let anchor_key = format!("time:{:020}:{}", 1, anchor.record_ids[0]);
    drop(store);
    // Corrupt only this test-owned store, while its production owner is closed.
    let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
    db.put(&neighbor_key, b"record:missing").unwrap();
    drop(db);
    let store = Store::open(dir.path(), config()).unwrap();
    assert_eq!(
        store
            .records_context(&anchor.record_ids[0], &scope, 0, 1, 3)
            .unwrap_err()
            .code,
        "STORAGE_FAILURE"
    );
    drop(store);
    let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
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
        source: None,
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
        source: Some("logs".into()),
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
        source: None,
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

fn config() -> Config {
    Config {
        disk_budget: 128 * 1024 * 1024,
        maintenance_reserve: 8 * 1024 * 1024,
        max_artifact_bytes: 8 * 1024 * 1024,
        max_records: 10_000,
        max_pin_members: 100,
        text_analyzer: {
            static ANALYZER: std::sync::OnceLock<purrdf::text::Analyzer> =
                std::sync::OnceLock::new();
            ANALYZER
                .get_or_init(|| {
                    sparrow_core::resolve_text_analyzer(concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/../../../purrdf/crates/text/lexicons/artifacts"
                    ))
                    .unwrap()
                })
                .clone()
        },
    }
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
        source: Some("alpha".into()),
        received_from: 100,
        received_before: 200,
        limit: 100,
        scan_budget: 1000,
    };
    let keys = store.json_query(&scope, &JsonQuery::Keys, 201).unwrap();
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
        source: None,
        received_from: 0,
        received_before: 200,
        limit: 100,
        scan_budget: 1000,
    };
    let result = store.json_query(&scope, &JsonQuery::Keys, 101).unwrap();
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
        .json_query(&scope, &JsonQuery::Keys, 100 + 30 * DAY)
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
            .json_query(&scope, &JsonQuery::Keys, 100 + 30 * DAY)
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
        source: None,
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
    limits.max_artifact_bytes = 4;
    limits.max_records = 1;
    let store = Store::open(dir.path(), limits).unwrap();
    assert_eq!(
        store
            .ingest("too-big", "source", "opaque", b"12345", 0)
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
        source: Some("logs".into()),
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
        source: Some("parents".into()),
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
        source: Some("nested".into()),
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
        source: Some("test".into()),
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
