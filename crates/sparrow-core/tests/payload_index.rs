// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The payload id index (#323): lookups, age-out and the online migration, through the
//! public store API.

use sparrow_core::{Config, EvictionLimits, RETENTION_MS, Store};
use sparrow_test_support::catalog_db;

const DAY: u64 = 86_400_000;

/// Every held artifact of the payload id, over every page of one-artifact pages, with whether the
/// answer was complete on the last page.
fn held(store: &Store, id: &str) -> (Vec<String>, bool) {
    let mut all = Vec::new();
    let mut after = None;
    loop {
        let page = store.payload_lookup(id, after.as_deref(), 1).unwrap();
        all.extend(page.artifacts);
        match page.next {
            Some(next) => after = Some(next),
            None => return (all, page.complete),
        }
    }
}

/// The index count row and its recount agree, and equal `expected`.
fn counted(store: &Store, expected: u64) {
    let (rows, recounted) = store.store_counter_audit_for_tests().unwrap();
    assert_eq!(rows["payload-index"], expected, "{rows:?}");
    assert_eq!(rows, recounted);
}

fn migrate(store: &Store) {
    while store.migrate_step().unwrap().is_some() {}
}

/// INVARIANT (#323): the same bytes ingested under two requests make two artifacts and the
/// lookup finds both, in artifact id order across pages; a digest nothing holds answers an
/// exact empty page once the store's index is complete.
#[test]
fn the_same_bytes_under_two_requests_are_both_found_and_an_absent_digest_is_exactly_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), sparrow_test_support::storage_config!(Config)).unwrap();
    migrate(&store);
    let first = store.ingest("one", "src", "log", b"same\n", 10).unwrap();
    let second = store.ingest("two", "src", "log", b"same\n", 11).unwrap();
    store
        .ingest("other", "src", "log", b"different\n", 12)
        .unwrap();
    assert_ne!(first.id, second.id);
    let (found, complete) = held(&store, &first.payload_id);
    let mut expected = vec![first.id.clone(), second.id.clone()];
    expected.sort();
    assert_eq!((found, complete), (expected, true));
    let (found, complete) = held(&store, &format!("blake3:{}", "0".repeat(64)));
    assert_eq!((found.as_slice(), complete), (&[][..], true));
    counted(&store, 3);
    assert_eq!(
        store
            .payload_lookup("blake3:not-a-digest", None, 10)
            .unwrap_err()
            .code,
        "INVALID_ARGUMENT"
    );
}

/// INVARIANT (#323): the key leaves the index in the batch that ends the artifact's
/// completeness: expiry deleting its entry, pressure eviction retiring it, and eviction
/// deleting a recordless artifact. The count row follows each, and a digest another
/// artifact still holds keeps that artifact's key.
#[test]
fn every_age_out_removes_the_key_with_the_entry_and_keeps_other_holders() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), sparrow_test_support::storage_config!(Config)).unwrap();
    migrate(&store);
    let old = store.ingest("old", "src", "log", b"shared\n", 1).unwrap();
    let evicted = store
        .ingest("evicted", "src", "log", b"retired\n", 2)
        .unwrap();
    let empty = store.ingest("empty", "src", "log", b"", 3).unwrap();
    let late = store
        .ingest("late", "src", "log", b"shared\n", 40 * DAY)
        .unwrap();
    counted(&store, 4);
    // Pressure eviction retires the oldest record's artifact; its entry stays, its key goes.
    let limits = EvictionLimits {
        max_records: 2,
        ..EvictionLimits::default()
    };
    let report = store.evict_oldest(41 * DAY, limits).unwrap();
    assert!(report.artifacts.contains(&old.id), "{report:?}");
    assert!(report.artifacts.contains(&evicted.id), "{report:?}");
    assert_eq!(held(&store, &evicted.payload_id), (vec![], true));
    assert_eq!(held(&store, &old.payload_id), (vec![late.id.clone()], true));
    // The next oldest unit is the recordless artifact: eviction deletes its entry.
    let report = store
        .evict_oldest(
            41 * DAY,
            EvictionLimits {
                max_records: 1,
                ..EvictionLimits::default()
            },
        )
        .unwrap();
    assert_eq!(report.artifacts, vec![empty.id.clone()]);
    assert_eq!(held(&store, &empty.payload_id), (vec![], true));
    assert_eq!(held(&store, &old.payload_id), (vec![late.id.clone()], true));
    // Expiry retires the late artifact's chunks at its deadline, a day before it deletes the
    // entry: its bytes are released, so the index must already not list it.
    store.expire(40 * DAY + RETENTION_MS).unwrap();
    assert_eq!(held(&store, &old.payload_id), (vec![], true));
    counted(&store, 0);
    // A day later expiry deletes the entry itself.
    store.expire(40 * DAY + RETENTION_MS + 2 * DAY).unwrap();
    assert_eq!(held(&store, &old.payload_id), (vec![], true));
    counted(&store, 0);
}

/// INVARIANT (#323): a store written before the index opens at once and answers a lookup as
/// pending, never absent; the online migration builds the keys for the artifacts it already
/// held while a new ingest keeps its own key exact, and after it the answer is exact, with
/// the count row equal to the complete artifacts and the migration reported complete.
#[test]
fn the_migration_indexes_artifacts_ingested_before_it_existed_and_new_ingest_stays_exact() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), sparrow_test_support::storage_config!(Config)).unwrap();
    migrate(&store);
    let before = store.ingest("before", "src", "log", b"old\n", 10).unwrap();
    drop(store);
    let db = catalog_db(dir.path()).unwrap();
    sparrow_core::remove_payload_index_for_legacy_fixture(&db).unwrap();
    drop(db);
    let store = Store::open(dir.path(), sparrow_test_support::storage_config!(Config)).unwrap();
    let pending = store.payload_lookup(&before.payload_id, None, 10).unwrap();
    assert!(!pending.complete);
    assert!(pending.artifacts.is_empty());
    let during = store.ingest("during", "src", "log", b"new\n", 11).unwrap();
    // A key written by ingest is held and listed even while the answer is pending.
    let listed = store.payload_lookup(&during.payload_id, None, 10).unwrap();
    assert_eq!(
        (listed.artifacts, listed.complete),
        (vec![during.id.clone()], false)
    );
    migrate(&store);
    assert_eq!(
        held(&store, &before.payload_id),
        (vec![before.id.clone()], true)
    );
    assert_eq!(held(&store, &during.payload_id), (vec![during.id], true));
    counted(&store, 2);
    let report = store.migrations().unwrap();
    let index = report.iter().find(|m| m.name == "payload-index").unwrap();
    assert!(index.complete(), "{index:?}");
}
