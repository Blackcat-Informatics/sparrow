// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Disk admission and restart behavior of the RDF reverse-family migration.

use super::{Store, catalog_db, config};

fn legacy_reverse_key(index: u64) -> String {
    format!("rdf:reverse:{index:064x}")
}

fn legacy_reverse_store(entries: u64) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    drop(Store::open(dir.path(), config()).unwrap());
    let db = catalog_db(dir.path()).unwrap();
    let mut legacy = rocksdb::WriteBatch::default();
    for index in 0..entries {
        legacy.put(legacy_reverse_key(index), index.to_be_bytes());
    }
    db.write(legacy).unwrap();
    db.flush().unwrap();
    dir
}

fn assert_migrated_reverse_rows(db: &rocksdb::DB, expected: u64) {
    let reverse = db.cf_handle("rdf_reverse").unwrap();
    let count = db
        .iterator_cf(reverse, rocksdb::IteratorMode::Start)
        .enumerate()
        .inspect(|(index, row)| {
            let (key, value) = row.as_ref().unwrap();
            let index = *index as u64;
            assert_eq!(key.as_ref(), legacy_reverse_key(index).as_bytes());
            assert_eq!(value.as_ref(), index.to_be_bytes());
        })
        .count();
    assert_eq!(count as u64, expected);
}

#[test]
fn reverse_migration_refuses_beyond_the_disk_budget() {
    let dir = legacy_reverse_store(2);
    let settings = config();
    let ballast = dir.path().join("reverse-migration-ballast");
    std::fs::File::create(&ballast)
        .unwrap()
        .set_len(settings.disk_budget)
        .unwrap();

    let error = Store::open(dir.path(), settings.clone())
        .err()
        .expect("the migration must refuse before writing its data page");
    assert_eq!(error.code, "STORAGE_PRESSURE");
    assert!(error.message.contains("reverse-family migration"));
    {
        let db = catalog_db(dir.path()).unwrap();
        assert_migrated_reverse_rows(&db, 0);
        for index in 0_u64..2 {
            let value = db.get(legacy_reverse_key(index)).unwrap().unwrap();
            assert_eq!(value, index.to_be_bytes());
        }
    }

    std::fs::remove_file(ballast).unwrap();
    drop(Store::open(dir.path(), settings).unwrap());
    let db = catalog_db(dir.path()).unwrap();
    assert_migrated_reverse_rows(&db, 2);
    assert!(db.get(legacy_reverse_key(0)).unwrap().is_none());
    assert!(db.get(legacy_reverse_key(1)).unwrap().is_none());
}

#[test]
fn reverse_migration_moves_all_rows_across_pages() {
    let entries = 65_537;
    let dir = legacy_reverse_store(entries);
    drop(Store::open(dir.path(), config()).unwrap());
    let db = catalog_db(dir.path()).unwrap();
    assert_migrated_reverse_rows(&db, entries);
    assert!(
        !db.prefix_iterator(b"rdf:reverse:")
            .any(|row| row.unwrap().0.starts_with(b"rdf:reverse:"))
    );
}

#[test]
fn reverse_migration_keeps_the_corrupt_page_unpublished_and_resumes() {
    let first_page = 65_536;
    let bad = first_page + 1;
    let entries = bad + 1;
    let dir = legacy_reverse_store(entries);
    {
        let db = catalog_db(dir.path()).unwrap();
        db.put(legacy_reverse_key(bad), b"bad").unwrap();
    }

    let error = Store::open(dir.path(), config())
        .err()
        .expect("an invalid reverse identity must refuse startup");
    assert_eq!(error.code, "STORAGE_FAILURE");
    assert!(error.message.contains("invalid RDF reverse identity"));
    {
        let db = catalog_db(dir.path()).unwrap();
        assert_migrated_reverse_rows(&db, first_page);
        assert!(db.get(legacy_reverse_key(0)).unwrap().is_none());
        assert!(
            db.get(legacy_reverse_key(first_page - 1))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            db.get(legacy_reverse_key(first_page)).unwrap().unwrap(),
            first_page.to_be_bytes()
        );
        assert_eq!(db.get(legacy_reverse_key(bad)).unwrap().unwrap(), b"bad");
        db.put(legacy_reverse_key(bad), bad.to_be_bytes()).unwrap();
    }

    drop(Store::open(dir.path(), config()).unwrap());
    let db = catalog_db(dir.path()).unwrap();
    assert_migrated_reverse_rows(&db, entries);
    assert!(
        !db.prefix_iterator(b"rdf:reverse:")
            .any(|row| row.unwrap().0.starts_with(b"rdf:reverse:"))
    );
}
