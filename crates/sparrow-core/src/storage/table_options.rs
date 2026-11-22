// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One owner of every column family's table and compression options.
//!
//! The values come from the compression measurement recorded in
//! `docs/load-qualification.md`. Block size dominates: 32 KiB blocks with
//! bottommost ZSTD level 6 hold the default family at 0.723 of its former
//! bytes. A trained dictionary pays only for payload rows; on the quad index it
//! is neutral at level 6 and costs compaction CPU. Reverse lookups keep small
//! blocks because their point reads usually miss and decompress a whole block.
//!
//! Codec availability is a native open check. The payload family's ZSTD blob
//! compression and every family's LZ4 type are validated when RocksDB opens,
//! which refuses an unlinked codec instead of storing uncompressed data. With
//! ZSTD linked, the RocksDB build requires ZSTD 1.4 or newer and always
//! supports its dictionary trainer.

use super::{Error, FAMILIES, pressure, rdf_index};
use rocksdb::{BlockBasedOptions, ColumnFamilyDescriptor, DBCompressionType, Env, Options};

/// Identity of the options below. Change it with any option that alters how
/// new table files are written, and list the families it rewrites.
pub(super) const PROFILE: &str = "sparrow.table-options/2";
/// Families whose table files this profile writes differently from its
/// predecessor. Their existing files are rewritten once in the background.
pub(super) const REWRITTEN: [&str; 2] = ["default", "payload"];

/// Memtable bytes per family. Larger memtables flush less often into fewer, larger level-0
/// files, so level 0 reaches its slowdown trigger later; memtable size changes no table file
/// encoding, so the profile is unchanged.
const WRITE_BUFFER_BYTES: usize = 32 * 1024 * 1024;
/// Memtables per family: one filling and up to three waiting for a flush before RocksDB
/// stops writes, where two (its default) stopped intake whenever one flush lagged.
const WRITE_BUFFERS: i32 = 4;
/// Background flush and compaction slots. RocksDB gives one of every four to flushes, so a
/// flush never queues behind a compaction. It runs more than one compaction only while
/// level-0 files or pending compaction bytes call for a speed-up, so an idle store still
/// uses one compaction thread and takes no shared cores.
const BACKGROUND_JOBS: i32 = 6;
/// Threads one level-0 compaction may split into, so a backlog drains in parallel.
const SUBCOMPACTIONS: u32 = 2;
/// Cap on live write-ahead-log bytes. A log file is deleted only once every family has
/// flushed past it, so a family that takes few writes (`payload`, `rdf_reverse`) pins every
/// log written after its last flush; with the cap unset RocksDB sizes it at four times all
/// memtable capacity (3 families x 4 x 32 MiB x 4 = 1.5 GiB), which is the retained WAL seen
/// in the live store (#309). Past this cap RocksDB itself flushes the families whose
/// memtables hold the oldest live log, so it also covers a family added later. 256 MiB is two
/// full memtable sets of the busy family (4 x 32 MiB), so ordinary ingest, which flushes the
/// default family every 32 MiB, never reaches it and no flush storm results. A forced flush
/// of an idle family writes one small level-0 file, at most about once per 256 MiB of WAL.
/// Retained log is thus bounded at about this size, far inside the production maintenance
/// reserve.
/// It is an engine option, not a stored format; it takes effect when the store next opens.
const MAX_TOTAL_WAL_BYTES: u64 = 256 * 1024 * 1024;
const BLOCK_BYTES: usize = 32 * 1024;
const REVERSE_BLOCK_BYTES: usize = 4 * 1024;
const BOTTOMMOST_ZSTD_LEVEL: i32 = 6;
/// ZSTD's default window; the level and dictionary are the measured changes.
const ZSTD_WINDOW_BITS: i32 = -14;
const PAYLOAD_DICTIONARY_BYTES: i32 = 112 * 1024;
const PAYLOAD_TRAINING_BYTES: i32 = 100 * PAYLOAD_DICTIONARY_BYTES;
const INFO_LOGS: usize = 10;
const INFO_LOG_BYTES: usize = 8 * 1024 * 1024;

/// Database-wide options, including the info-log bound and the table cache
/// bound `max_open_files` derived from the descriptor limit
/// ([`super::open_files::OpenFiles`]).
///
/// Compaction threads run at the lowest CPU priority, below intake and the flush threads;
/// their I/O priority is left alone, because an idle I/O class lets other disk users on a
/// shared host starve compaction until writes stall.
pub(super) fn database(max_open_files: i32, create: bool) -> Result<Options, Error> {
    let mut options = family("default");
    options.set_max_open_files(max_open_files);
    options.create_if_missing(create);
    options.create_missing_column_families(create);
    options.set_max_background_jobs(BACKGROUND_JOBS);
    options.set_max_subcompactions(SUBCOMPACTIONS);
    options.set_max_total_wal_size(MAX_TOTAL_WAL_BYTES);
    let mut env = Env::new().map_err(|error| {
        Error::new(
            "STORAGE_FAILURE",
            format!("RocksDB environment unavailable: {error}"),
        )
    })?;
    env.lower_thread_pool_cpu_priority();
    options.set_env(&env);
    options.set_keep_log_file_num(INFO_LOGS);
    options.set_max_log_file_size(INFO_LOG_BYTES);
    Ok(options)
}

/// One descriptor for every declared family, each built by [`family`].
pub(super) fn descriptors() -> Vec<ColumnFamilyDescriptor> {
    FAMILIES
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, family(name)))
        .collect()
}

/// Table and compression options for one declared family.
pub(super) fn family(name: &str) -> Options {
    let mut options = Options::default();
    options.set_write_buffer_size(WRITE_BUFFER_BYTES);
    options.set_max_write_buffer_number(WRITE_BUFFERS);
    options.set_compression_type(DBCompressionType::Lz4);
    options.set_bottommost_compression_type(DBCompressionType::Zstd);
    pressure::native_compaction_options(&mut options);
    // Off until the one-time rewrite turns it on for the families it rewrites.
    options.set_periodic_compaction_seconds(0);
    let mut table = BlockBasedOptions::default();
    if name == rdf_index::REVERSE_FAMILY {
        // Lookups that miss must not decompress large blocks. The whole-key
        // Bloom filter is sized to the dictionary, not to the scanned index keys.
        table.set_block_size(REVERSE_BLOCK_BYTES);
        table.set_bloom_filter(10.0, false);
        table.set_whole_key_filtering(true);
    } else {
        table.set_block_size(BLOCK_BYTES);
        let dictionary = if name == "payload" {
            PAYLOAD_DICTIONARY_BYTES
        } else {
            0
        };
        options.set_bottommost_compression_options(
            ZSTD_WINDOW_BITS,
            BOTTOMMOST_ZSTD_LEVEL,
            0,
            dictionary,
            true,
        );
        if dictionary != 0 {
            options.set_bottommost_zstd_max_train_bytes(PAYLOAD_TRAINING_BYTES, true);
        }
    }
    if name == "payload" {
        options.set_enable_blob_files(true);
        options.set_min_blob_size(super::CHUNK as u64);
        options.set_blob_compression_type(DBCompressionType::Zstd);
        options.set_enable_blob_gc(true);
    }
    options.set_block_based_table_factory(&table);
    options
}

#[cfg(test)]
mod tests {
    use super::MAX_TOTAL_WAL_BYTES;
    use crate::storage::{Config, Store};
    use rocksdb::WaitForCompactOptions;
    use std::collections::BTreeSet;
    use std::path::Path;

    fn wal_files(path: &Path) -> BTreeSet<String> {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".log"))
            .collect()
    }

    // Value: an idle family must not pin the write-ahead log (#309: 1.55 GB of retained logs
    // while `payload` never flushed). Protects: once live WAL bytes pass the cap, RocksDB
    // flushes the idle families holding the oldest log and releases that log file, though
    // only the default family keeps taking writes.
    #[test]
    fn writes_past_the_wal_cap_release_the_log_an_idle_family_pins() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), sparrow_test_support::storage_config!(Config)).unwrap();
        let db = std::sync::Arc::clone(&store.lock().unwrap().db);
        for family in super::FAMILIES.iter().skip(1) {
            let handle = db.cf_handle(family).unwrap();
            db.put_cf(handle, b"idle", b"pinned").unwrap();
        }
        let pinned = wal_files(dir.path());
        let value = vec![7_u8; 1024 * 1024];
        let writes = MAX_TOTAL_WAL_BYTES / value.len() as u64 + 64;
        for index in 0..writes {
            db.put(index.to_be_bytes(), &value).unwrap();
        }
        db.wait_for_compact(&WaitForCompactOptions::default())
            .unwrap();
        let remaining = wal_files(dir.path());
        assert!(
            pinned.is_disjoint(&remaining),
            "WAL files still pinned by idle families: {:?}",
            pinned.intersection(&remaining).collect::<Vec<_>>()
        );
    }
}
