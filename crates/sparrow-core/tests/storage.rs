// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Persistent lifetime scenarios using temporary stores and injected UTC instants.

use sparrow_core::{Config, PinPageBody, PinRead, PinSelector, Store};

const DAY: u64 = 86_400_000;

#[test]
fn source_alternatives_intersect_membership_time_and_context_without_duplicates() {
    use sparrow_core::{JsonQuery, QueryScope, QuerySelection, RecordQuery, SourceFilter};
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
            SourceFilter::new(vec!["alpha".into(), "beta".into(), "alpha".into()]).unwrap(),
        ),
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
        let keys = store.json_query(&scope, &JsonQuery::Keys, 50).unwrap();
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
    scope.sources = Some(SourceFilter::new(vec!["absent".into()]).unwrap());
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
            .json_query(&scope, &JsonQuery::Keys, 31 * DAY)
            .unwrap_err()
            .code,
        "EVIDENCE_UNAVAILABLE"
    );
}

#[test]
fn source_filter_admission_uses_ingestion_byte_bounds() {
    use sparrow_core::SourceFilter;
    for ids in [
        vec![],
        vec![String::new()],
        vec!["x".repeat(513)],
        vec!["é".repeat(257)],
        vec!["source".into(); 129],
    ] {
        assert_eq!(SourceFilter::new(ids).unwrap_err().code, "INVALID_ARGUMENT");
    }
    let filter = SourceFilter::new(vec!["é".repeat(256); 128]).unwrap();
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
        let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
        let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
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
    let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
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
    for (legacy, current, watch) in [(5, 9, false), (6, 10, false), (7, 11, true), (8, 12, true)] {
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
        let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
        let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
        db.delete("recordless-index-upgrade").unwrap();
        db.delete(format!("empty-time:{:020}:{}", empty.received_at, empty.id))
            .unwrap();
        db.put("store_format", format!("sparrow.store/{legacy}"))
            .unwrap();
        drop(db);
        let store = Store::open(dir.path(), config()).unwrap();
        assert_eq!(store.store_id().unwrap(), identity);
        assert_eq!(
            store.status(3).unwrap()["store_format"].as_str(),
            Some(format!("sparrow.store/{current}").as_str())
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
        store.json_query(&scope, &JsonQuery::Keys, 4).unwrap()["coverage"]["eligible_records"]
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
        Some("sparrow.store/11")
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
        Some("sparrow.store/11")
    );
}

#[test]
fn physical_accounting_handles_directory_churn_with_bounded_measurement_failures() {
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
        match observation {
            Ok(status) => assert!(
                status["physical_bytes"]
                    .as_str()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
                    > 0
            ),
            Err(error) => {
                assert_eq!(error.code, "STORAGE_PRESSURE");
                assert!(
                    error.message.starts_with(
                        "database footprint stayed unstable across 3 complete measurements"
                    ),
                    "{error}"
                );
            }
        }
    }
    assert!(store.status(0).is_ok());
}

#[test]
fn consumption_recovery_preserves_original_publication_beyond_retry_and_retention() {
    use sparrow_core::{ConsumptionGeneration, ImportStart};
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), config()).unwrap();
    let input = b"one\n";
    let identity = store.store_id().unwrap();
    assert_eq!(
        store.status(0).unwrap()["store_format"].as_str(),
        Some("sparrow.store/9")
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
        Some("sparrow.store/10")
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
        Some("sparrow.store/12")
    );
    // Catalog updates must never downgrade the handoff writer fence.
    store
        .watch_registry_replace(Some(b"registered-watch"), b"changed-watch")
        .unwrap();
    assert_eq!(
        store.status(0).unwrap()["store_format"].as_str(),
        Some("sparrow.store/12")
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
        Some("sparrow.store/11")
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
    let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
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
    let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
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
    let keys = store.json_query(&scope, &JsonQuery::Keys, 40).unwrap();
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
            .json_query(&scope, &JsonQuery::Keys, 40)
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
        received_from: 0,
        received_before: now + 1,
        limit: 10,
        scan_budget: 100,
    };
    let keys = store.json_query(&scope, &JsonQuery::Keys, now).unwrap();
    assert_eq!(keys["coverage"]["eligible_records"].as_str(), Some("1"));
    scope.selection = QuerySelection::Evidence(vec![artifact.id.clone()]);
    assert_eq!(
        store
            .json_query(&scope, &JsonQuery::Keys, now)
            .unwrap_err()
            .code,
        "EVIDENCE_UNAVAILABLE"
    );
    store.expire(now).unwrap();
    assert_eq!(
        store
            .json_query(&scope, &JsonQuery::Keys, now)
            .unwrap_err()
            .code,
        "NOT_FOUND"
    );
    scope.selection = QuerySelection::Evidence(vec![artifact.record_ids[0].clone()]);
    assert_eq!(
        store.json_query(&scope, &JsonQuery::Keys, now).unwrap()["coverage"]["eligible_records"]
            .as_str(),
        Some("1")
    );
    scope.selection = QuerySelection::Pin {
        id: pin.id.clone(),
        expected_revision: Some(pin.revision + 1),
    };
    assert_eq!(
        store
            .json_query(&scope, &JsonQuery::Keys, now)
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
            .json_query(&scope, &JsonQuery::Keys, 61 * DAY)
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
            .json_query(&scope, &JsonQuery::Keys, now)
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
        match store.ingest_with_durability(
            &format!("bytes-{accepted}"),
            "byte-queue",
            "opaque",
            &input,
            0,
            sparrow_core::Durability::Buffered,
        ) {
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
        store
            .ingest_with_durability(
                &format!("bytes-{accepted}"),
                "byte-queue",
                "opaque",
                &input,
                0,
                sparrow_core::Durability::Buffered
            )
            .unwrap()
            .buffered
            .is_some()
    );
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
    store.import_prepare_layout(&manifest.id, 0).unwrap()
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
        received_from: 0,
        received_before: 10,
        limit: 100,
        scan_budget: 100,
    };
    let keys = store
        .json_query(&scope, &sparrow_core::JsonQuery::Keys, 3)
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
    let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
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
    let input = b"{\"timestamp\":\"2026-09-27T00:13:37.406Z\",\"message\":\"alpha\"}\n{\"timestamp\":\"2026-09-27T00:13:37.406Z\",\"message\":\"alpha\"}\n";
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
    assert_eq!(progress.prepared_records, 1);
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
            .import_prepare_record(&journal.id, 2, 20)
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    let complete = store.import_prepare_record(&journal.id, 1, 20).unwrap();
    assert_eq!(complete.prepared_records, 2);
    assert_eq!(complete.received_at, 10);
    let artifact = store.import_publish(&journal.id, 30).unwrap();
    assert_eq!(artifact.record_ids.len(), 2);
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
    assert_eq!(store.records(30).unwrap().len(), 2);
    assert_eq!(
        store.import_cancel(&journal.id, 30).unwrap_err().code,
        "CONFLICT"
    );
    // Expired private staging copies must never decrement published ownership.
    while store.import_reclaim(2 * DAY).unwrap() != 0 {}
    assert_eq!(store.records(2 * DAY).unwrap().len(), 2);
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
    let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
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
        let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
        let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
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
    // Raw staging exceeds the original buffered artifact limit without a growing
    // journal or an artifact-sized allocation.
    assert!(journal.byte_length > config().max_artifact_bytes as u64);
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
        Some("sparrow.store/9")
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
        selection: sparrow_core::QuerySelection::Live,
        source: Some("context".into()),
        sources: None,
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
        selection: sparrow_core::QuerySelection::Live,
        source: None,
        sources: None,
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
        selection: sparrow_core::QuerySelection::Live,
        source: None,
        sources: None,
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
        buffered_flush_interval_ms: sparrow_core::DEFAULT_FLUSH_INTERVAL_MS,
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
        selection: sparrow_core::QuerySelection::Live,
        source: Some("alpha".into()),
        sources: None,
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
        selection: sparrow_core::QuerySelection::Live,
        source: None,
        sources: None,
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
        selection: sparrow_core::QuerySelection::Live,
        source: None,
        sources: None,
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
        selection: sparrow_core::QuerySelection::Live,
        source: Some("logs".into()),
        sources: None,
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
    let purrdf::SparqlResult::Solutions { rows, .. } = native.result else {
        panic!("native SELECT result");
    };
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
    let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
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
    let columns = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir.path()).unwrap();
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir.path(), columns).unwrap();
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
