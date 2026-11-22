// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Dev-only shared native storage fixtures. Runtime crates must not depend on this crate.

/// Hang guard for event-driven tests under plain `cargo test`.
pub mod deadline;

/// Process-crash coordination for test-only production checkpoints.
pub mod crash;

/// Production flush default used by the shared storage fixture macro.
pub use sparrow_core::DEFAULT_FLUSH_INTERVAL_MS as STORAGE_FLUSH_INTERVAL_MS;
/// Native analyzer resolver used by the shared storage fixture macro.
pub use sparrow_core::resolve_text_analyzer;

/// Baseline native analyzer artifacts for workspace storage fixtures. The registry crates
/// exclude the lexicons, so `tools/purrdf-lexicons.sh` archives them from the PurRDF repository
/// at the revision that published the locked release (refusing when the registry, `Cargo.lock`
/// and the crate's `.cargo_vcs_info.json` disagree) into `bin/lexicons-<revision>` of the
/// canonical checkout root, and prints that directory. A linked worktree under
/// `<root>/.worktrees/` uses the canonical root's `bin/` here; `make install-bin` instead places
/// the same bundle in the invoking checkout's own `bin/`, beside the images. They are never read
/// from a PurRDF working tree.
pub fn text_lexicon_dir() -> &'static std::path::Path {
    static DIR: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    DIR.get_or_init(|| {
        let checkout = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."));
        let root = match checkout.canonicalize() {
            Ok(path)
                if path.parent().and_then(std::path::Path::file_name)
                    == Some(std::ffi::OsStr::new(".worktrees")) =>
            {
                path.parent()
                    .and_then(std::path::Path::parent)
                    .map(std::path::Path::to_path_buf)
                    .expect("INVARIANT: a .worktrees directory has a parent repository root")
            }
            _ => checkout.to_path_buf(),
        };
        let output = std::process::Command::new(checkout.join("tools/purrdf-lexicons.sh"))
            .arg(&root)
            .arg(root.join("bin"))
            .stderr(std::process::Stdio::inherit())
            .output()
            .expect("INVARIANT: tools/purrdf-lexicons.sh runs");
        assert!(
            output.status.success(),
            "tools/purrdf-lexicons.sh refused or failed for {}",
            root.display()
        );
        let dest = String::from_utf8(output.stdout)
            .expect("INVARIANT: the lexicon directory path is UTF-8");
        std::path::Path::new(dest.trim_end()).join("artifacts")
    })
}

/// One configuration law for core unit and integration fixtures.
/// The caller supplies its local `Config` identity; native analyzer types are shared.
#[macro_export]
macro_rules! storage_config {
    ($config:ident) => {{
        static CONFIG: std::sync::OnceLock<$config> = std::sync::OnceLock::new();
        CONFIG
            .get_or_init(|| $config {
                buffered_flush_interval_ms: $crate::STORAGE_FLUSH_INTERVAL_MS,
                disk_budget: 128 * 1024 * 1024,
                maintenance_reserve: 8 * 1024 * 1024,
                min_free_bytes: 1024 * 1024,
                max_records: 10_000,
                max_pin_members: 100,
                text_analyzer: $crate::resolve_text_analyzer($crate::text_lexicon_dir())
                    .expect("standard storage fixture analyzer"),
                archive: None,
                retire_without_archive: true,
                instance_role: None,
            })
            .clone()
    }};
}

/// One complete retained-record scope for storage unit and integration fixtures.
#[macro_export]
macro_rules! record_scope {
    ($scope:ident, $source:expr, $limit:expr) => {
        $scope {
            selection: Default::default(),
            source: Some($source.into()),
            sources: None,
            diagnostics: Default::default(),
            require_complete_index: false,
            received_from: 0,
            received_before: u64::MAX,
            limit: $limit,
            scan_budget: 10000,
        }
    };
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
    std::fs::write(&config, format!("socket_path = {socket:?}\ndata_dir = {store:?}\ndisk_budget = 134217728\nmaintenance_reserve = 8388608\nmin_free_bytes = 1048576\nmax_records = 10000\nmax_pin_members = 100\nmax_concurrent_requests = 4\ntext_lexicon_dir = {:?}\n", text_lexicon_dir())).unwrap();
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

/// Leave a closed store's source timeline cache as an image before #149 leaves it once its
/// open began clearing the cache: without its generation marker. The next open resets the
/// cache and the online `source-context` migration rebuilds it.
pub fn stale_source_context_cache(db: &rocksdb::DB) -> Result<(), rocksdb::Error> {
    db.delete("source-context-v1:!generation")
}

/// Return a closed current store to the state its first schema 69 open leaves before any
/// online migration page: the owner-to-graph index empty with its build at the start. Record
/// graphs are unchanged, and none stores receipt-time fallback facts, as for every new graph.
pub fn pending_schema_69_migrations(db: &rocksdb::DB) -> Result<(), Box<dyn std::error::Error>> {
    // The checkpoint below is rewritten offline, so the store counters are reseeded too.
    sparrow_core::remove_store_counters_for_legacy_fixture(db)?;
    let mut batch = rocksdb::WriteBatch::default();
    for row in db.prefix_iterator(b"rdf:o:") {
        let (key, _) = row?;
        if !key.starts_with(b"rdf:o:") {
            break;
        }
        batch.delete(key);
    }
    batch.put("owner-graph-index-upgrade", [1]);
    db.write(batch)?;
    Ok(())
}

/// Leave the pending marker of the retired event fallback backfill (#20, removed by #193) on
/// a closed store: its checkpoint at the start, as an image that had not run a page left it.
/// The next open must serve the store as it is, without a rewrite.
pub fn pending_event_fallback_marker(db: &rocksdb::DB) -> Result<(), rocksdb::Error> {
    db.put(
        "event-fallback-rdf-upgrade",
        br#"{"after":null,"complete":false}"#,
    )
}

/// Remove the per-source counters, retained counts and artifact index, and lower the writer
/// fence to its owner graph family, as a store written before #14 is. The next open fences
/// it again and seeds the online source-summary migration.
pub fn pending_source_summary_migration(
    db: &rocksdb::DB,
) -> Result<(), Box<dyn std::error::Error>> {
    sparrow_core::remove_store_counters_for_legacy_fixture(db)?;
    sparrow_core::remove_source_summary_for_legacy_fixture(db)?;
    Ok(())
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

/// Retire artifact-only RDF through its production owner before constructing a
/// historical writer fixture. Record graphs and exact payload ownership survive.
pub fn remove_artifact_rdf_upgrade(db: &rocksdb::DB) -> Result<(), Box<dyn std::error::Error>> {
    sparrow_core::retire_artifact_rdf_for_legacy_fixture(db)?;
    for prefix in [
        b"artifact-rdf:".as_slice(),
        b"rdf:graph:artifact:".as_slice(),
        b"artifact-time:".as_slice(),
    ] {
        if let Some(row) = db.prefix_iterator(prefix).next()
            && row?.0.starts_with(prefix)
        {
            return Err("legacy fixture still owns artifact RDF; retire it through Store before removing the checkpoint".into());
        }
    }
    db.delete("artifact-rdf-index-upgrade")?;
    db.delete("all-artifact-rdf-index-upgrade")?;
    db.delete("tracing-rdf:profile")?;
    for checkpoint in [
        "record-provenance-rdf-upgrade",
        "artifact-provenance-rdf-upgrade",
        "tracing-rdf-upgrade",
        "event-fallback-rdf-upgrade",
    ] {
        db.delete(checkpoint)?;
    }
    let mut batch = rocksdb::WriteBatch::default();
    for row in db.prefix_iterator(b"provenance-rdf:") {
        let (key, _) = row?;
        if !key.starts_with(b"provenance-rdf:") {
            break;
        }
        batch.delete(key);
    }
    db.write(batch)?;
    Ok(())
}

/// The bytes of a one-record, one-artifact archive segment built with PurRDF's public writer.
/// The record's link quad names the digest of `claimed`; the blob stored is `actual`, hashed
/// honestly by the container, so the container is internally consistent whatever the two are.
/// The one forged-segment builder for the reader's and the worker's digest-check tests.
///
/// # Panics
/// On a PurRDF writer failure, which a fixture cannot recover from.
#[must_use]
pub fn forged_segment(claimed: &[u8], actual: &[u8]) -> Vec<u8> {
    use purrdf::gts::writer::digest_string;
    use purrdf::gts_write::to_writer;
    use purrdf::{RdfDatasetBuilder, RdfLiteral, RdfLookaside};
    const NS: &str = "urn:sparrow:vocab:";
    const AT: u64 = 1_700_000_000_000;
    let at = sparrow_core::format_instant(AT).expect("fixture instant formats");
    let mut builder = RdfDatasetBuilder::new();
    let facts: [(&str, &str, String); 9] = [
        ("artifact:aa", "sourceName", "host".to_owned()),
        ("artifact:aa", "format", "log".to_owned()),
        ("artifact:aa", "receivedAt", at.clone()),
        ("artifact:aa", "byteLength", actual.len().to_string()),
        ("record:bb", "sourceName", "host".to_owned()),
        ("record:bb", "receivedAt", at),
        ("record:bb", "sourceByteStart", "0".to_owned()),
        ("record:bb", "sourceByteEnd", actual.len().to_string()),
        ("record:bb", "payloadBlake3", digest_string(claimed)),
    ];
    for (subject, name, value) in facts {
        let predicate = format!("{NS}{name}");
        let subject = builder
            .intern_iri(&format!("urn:sparrow:{subject}"))
            .expect("fixture subject");
        let predicate = builder.intern_iri(&predicate).expect("fixture predicate");
        let object = builder
            .intern_literal(RdfLiteral::simple(&value))
            .expect("fixture literal");
        builder
            .push_quad(subject, predicate, object, None)
            .expect("fixture quad");
    }
    let rdf_type = builder
        .intern_iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        .expect("fixture type");
    for (subject, class) in [("artifact:aa", "Artifact"), ("record:bb", "Record")] {
        let subject = builder
            .intern_iri(&format!("urn:sparrow:{subject}"))
            .expect("fixture subject");
        let class = builder
            .intern_iri(&format!("{NS}{class}"))
            .expect("fixture class");
        builder
            .push_quad(subject, rdf_type, class, None)
            .expect("fixture quad");
    }
    let artifact_link = builder
        .intern_iri(&format!("{NS}artifact"))
        .expect("fixture link");
    let record = builder
        .intern_iri("urn:sparrow:record:bb")
        .expect("fixture record");
    let artifact = builder
        .intern_iri("urn:sparrow:artifact:aa")
        .expect("fixture artifact");
    builder
        .push_quad(record, artifact_link, artifact, None)
        .expect("fixture quad");
    // The artifact graph names the artifact's payload id as an IRI, as the store writes it.
    let payload_link = builder
        .intern_iri(&format!("{NS}payload"))
        .expect("fixture link");
    let payload = builder
        .intern_iri(&format!(
            "urn:sparrow:payload:{}",
            digest_string(actual).trim_start_matches("blake3:")
        ))
        .expect("fixture payload");
    builder
        .push_quad(artifact, payload_link, payload, None)
        .expect("fixture quad");
    let dataset = builder.freeze().expect("fixture dataset");
    let mut writer =
        to_writer(&dataset, &RdfLookaside::default(), "sparrow-archive").expect("fixture writer");
    writer
        .add_blob_transformed(
            actual.to_vec(),
            Some("application/octet-stream"),
            Some("record"),
            &["zstd".to_owned()],
            None,
        )
        .expect("fixture blob");
    writer.into_bytes()
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
