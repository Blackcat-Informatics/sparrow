// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Dev-only shared native storage fixtures. Runtime crates must not depend on this crate.

/// Open every declared native family for inspection or corruption of a closed test store.
/// The caller must first release the production storage owner.
pub fn catalog_db(path: &std::path::Path) -> Result<rocksdb::DB, rocksdb::Error> {
    let options = rocksdb::Options::default();
    let columns = rocksdb::DB::list_cf(&options, path)?;
    rocksdb::DB::open_cf(&options, path, columns)
}

/// Remove the range index explicitly when constructing a historical writer fixture.
/// This does not alter record bytes, generation authority, or their acceptance clocks.
pub fn remove_tail_range_index(db: &rocksdb::DB) -> Result<(), rocksdb::Error> {
    let mut batch = rocksdb::WriteBatch::default();
    batch.delete("tail-range-index-upgrade");
    for row in db.prefix_iterator(b"tail-range:") {
        let (key, _) = row?;
        if !key.starts_with(b"tail-range:") {
            break;
        }
        batch.delete(key);
    }
    db.write(batch)
}
