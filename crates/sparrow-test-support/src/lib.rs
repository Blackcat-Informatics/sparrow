// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Dev-only shared native storage fixtures. Runtime crates must not depend on this crate.

/// Observe anonymous query allocations in an owned, uncapped Linux test daemon.
/// Disappearing descriptors are normal races; other inspection failures remain errors.
pub fn anonymous_query_files(
    pid: u32,
    store: &std::path::Path,
) -> std::io::Result<Vec<std::fs::Metadata>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(format!("/proc/{pid}/fd"))? {
        let path = entry?.path();
        let inspect = || -> std::io::Result<Option<std::fs::Metadata>> {
            let target = std::fs::read_link(&path)?;
            if target.starts_with(store)
                && target
                    .as_os_str()
                    .as_encoded_bytes()
                    .ends_with(b" (deleted)")
            {
                Ok(Some(std::fs::metadata(&path)?))
            } else {
                Ok(None)
            }
        };
        match inspect() {
            Ok(Some(metadata)) => files.push(metadata),
            Ok(None) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(files)
}

/// One bounded daemon configuration for local RPC unit and integration fixtures.
pub fn write_server_config(
    dir: &std::path::Path,
    store: &std::path::Path,
    socket: &std::path::Path,
) -> std::path::PathBuf {
    let config = dir.join("server.toml");
    std::fs::write(&config, format!("socket_path = {socket:?}\ndata_dir = {store:?}\ndisk_budget = 134217728\nmaintenance_reserve = 8388608\nmax_artifact_bytes = 8388608\nmax_records = 10000\nmax_pin_members = 100\nmax_concurrent_requests = 4\ntext_lexicon_dir = {:?}\n", concat!(env!("CARGO_MANIFEST_DIR"), "/../../../purrdf/crates/text/lexicons/artifacts"))).unwrap();
    config
}

/// Build wide JSONL through the native codec, returning its ordered unique leaf paths.
/// Generated member names contain only ASCII letters/digits and need no pointer escapes.
pub fn wide_jsonl_keys(
    records: usize,
    keys_per_record: usize,
    padding: usize,
) -> (Vec<u8>, Vec<String>) {
    let mut bytes = Vec::new();
    let mut paths = Vec::new();
    let suffix = "p".repeat(padding);
    for record in 0..records {
        let mut object = purrdf_lex::json::Object::new();
        for field in 0..keys_per_record {
            let key = format!("k{:05}{suffix}", record * keys_per_record + field);
            paths.push(format!("/{key}"));
            object.insert(key, purrdf_lex::json::Value::from("needle"));
        }
        bytes.extend_from_slice(purrdf_lex::json::write_compact(&object.into()).as_bytes());
        bytes.push(b'\n');
    }
    (bytes, paths)
}

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
