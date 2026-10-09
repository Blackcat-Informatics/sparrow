// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Dev-only shared native storage fixtures. Runtime crates must not depend on this crate.

/// One discovered store for SDK acceptance and ingestion status certificates.
pub fn client_discovery() -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut message = capnp::message::Builder::new_default();
    let mut out = message.init_root::<sparrow_client::sparrow_capnp::discovery::Builder<'_>>();
    out.set_store_id("store:test");
    out.set_schema_version(sparrow_client::SCHEMA_VERSION);
    out.set_max_artifact_bytes(1024);
    out.set_max_records_per_artifact(2);
    out.set_text_analyzer("native");
    out.set_text_profile("text");
    out.set_rdf_profile("rdf");
    out.set_max_event_batch_bytes(sparrow_client::MAX_EVENT_BATCH_BYTES as u32);
    out.set_max_event_batch_events(sparrow_client::MAX_EVENT_BATCH_EVENTS as u32);
    let mut durability = out.init_durability_status();
    durability.set_session_id("session:test");
    durability.set_max_pending_writes(256);
    durability.set_max_pending_bytes(64 * 1024 * 1024);
    durability.set_flush_interval_ms(2000);
    message
}

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
            // Linux O_TMPFILE names are `#<inode> (deleted)`. Ordinary unlinked
            // RocksDB tables also end in `(deleted)` and are not query staging.
            let anonymous = target.file_name().is_some_and(|name| {
                name.as_encoded_bytes()
                    .strip_prefix(b"#")
                    .and_then(|name| name.strip_suffix(b" (deleted)"))
                    .is_some_and(|inode| !inode.is_empty() && inode.iter().all(u8::is_ascii_digit))
            });
            if target.starts_with(store) && anonymous {
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

#[cfg(test)]
mod observer_tests {
    #[test]
    fn anonymous_observer_excludes_unlinked_tables_and_other_stores() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("store");
        let sibling = dir.path().join("store-other");
        std::fs::create_dir(&store).unwrap();
        std::fs::create_dir(&sibling).unwrap();
        let table_path = store.join("000001.sst");
        let table = std::fs::File::create(&table_path).unwrap();
        std::fs::remove_file(&table_path).unwrap();
        let staging = std::fs::File::from(
            rustix::fs::open(
                &store,
                rustix::fs::OFlags::TMPFILE | rustix::fs::OFlags::RDWR,
                rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
            )
            .unwrap(),
        );
        let outside = tempfile::tempfile_in(&sibling).unwrap();
        let observed = super::anonymous_query_files(std::process::id(), &store).unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].ino(), staging.metadata().unwrap().ino());
        drop(staging);
        assert!(
            super::anonymous_query_files(std::process::id(), &store)
                .unwrap()
                .is_empty()
        );
        drop((table, outside));
    }
}
