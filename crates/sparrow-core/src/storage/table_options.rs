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
pub(super) fn database(max_open_files: i32) -> Result<Options, Error> {
    let mut options = family("default");
    options.set_max_open_files(max_open_files);
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.set_max_background_jobs(BACKGROUND_JOBS);
    options.set_max_subcompactions(SUBCOMPACTIONS);
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
