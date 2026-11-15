// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Recovery from archived segments and the offline reader, end to end: a real store ingests,
//! `Store::archive_prepare` and `archive_commit` produce the segment through the segment
//! encoder, and the reader recovers from the file. Temporary directories only.

use std::path::{Path, PathBuf};
use std::process::Command;

use sparrow_archive::{Segment, list_segments, restore};
use sparrow_core::{ArchivePolicy, Artifact, Config, Record, Store};

const T1: u64 = 1_700_000_000_000;
const T2: u64 = 1_700_000_500_000;
const DAY: u64 = 86_400_000;

type Archived = Vec<(Artifact, Vec<(Record, Vec<u8>)>)>;

/// Ingest two artifacts and return their original (artifact, records, bytes-per-record).
fn ingest(store: &Store) -> Archived {
    let inputs: [(&str, &str, &[u8], u64); 2] = [
        (
            "req-a",
            "host-a",
            b"first line\nsecond \xff\xfe line\nthird\n",
            T1,
        ),
        ("req-b", "host-b", b"{\"k\":1}\n{\"k\":2}\n", T2),
    ];
    let formats = ["log", "jsonl"];
    inputs
        .iter()
        .zip(formats)
        .map(|((request, source, bytes, now), format)| {
            let artifact = store.ingest(request, source, format, bytes, *now).unwrap();
            let records = artifact
                .record_ids
                .iter()
                .map(|id| {
                    let record = store.record(id).unwrap();
                    let length = (record.byte_end - record.byte_start) as usize;
                    let held = store.bytes(id, 0, length, *now).unwrap();
                    (record, held)
                })
                .collect();
            (artifact, records)
        })
        .collect()
}

/// A store with an archive policy, two ingested artifacts, and its one committed segment
/// written as `<root>/segments/<name>.gts` exactly as the segment encoder named it.
fn archive_two(root: &Path) -> (Archived, PathBuf) {
    let origin_dir = tempfile::tempdir().unwrap();
    let config = Config {
        archive: Some(ArchivePolicy::default()),
        ..sparrow_test_support::storage_config!(Config)
    };
    let origin = Store::open(origin_dir.path(), config).unwrap();
    let archived = ingest(&origin);
    let segment = origin
        .archive_prepare(T2 + 30 * DAY)
        .unwrap()
        .expect("both artifacts are due");
    assert_eq!(segment.files.len(), 1);
    let (name, bytes) = &segment.files[0];
    let path = root.join("segments").join(name);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
    origin.archive_commit(&segment.segment_id).unwrap();
    (archived, path)
}

/// Value (#126), end to end: a real store archives through `archive_prepare` and
/// `archive_commit`; recovering chosen entries from that segment, record by record, brings
/// records archived from a store back from their segment, written into a fresh scratch store (only where
/// they are checked), under their original identities, receipt times and sources with byte-equal bytes,
/// including a non-UTF-8 record, and restoring again adds nothing.
#[test]
fn archived_segment_reimports_byte_exact() {
    let archive_dir = tempfile::tempdir().unwrap();
    let (archived, _) = archive_two(archive_dir.path());

    let found = list_segments(archive_dir.path()).unwrap();
    assert_eq!(found.len(), 1);
    let segments = [Segment::open(&found[0]).unwrap()];

    let fresh_dir = tempfile::tempdir().unwrap();
    let fresh = Store::open(
        fresh_dir.path(),
        sparrow_test_support::storage_config!(Config),
    )
    .unwrap();
    let report = restore(&fresh, &segments).unwrap();
    assert_eq!((report.artifacts, report.records), (2, 5));
    let live_now = T2 + 1_000;
    let before = fresh.records(live_now).unwrap().len();
    assert_eq!(before, 5);

    for (artifact, records) in &archived {
        let restored = fresh.artifact(&artifact.id).unwrap();
        assert_eq!(restored.record_ids, artifact.record_ids);
        assert_eq!(restored.received_at, artifact.received_at);
        assert_eq!(restored.payload_id, artifact.payload_id);
        for (original, bytes) in records {
            let back = fresh.record(&original.id).unwrap();
            assert_eq!(back.artifact_id, original.artifact_id);
            assert_eq!(back.source, original.source);
            assert_eq!(back.received_at, original.received_at);
            assert_eq!(
                (back.byte_start, back.byte_end),
                (original.byte_start, original.byte_end)
            );
            let length = bytes.len();
            assert_eq!(
                &fresh.bytes(&original.id, 0, length, live_now).unwrap(),
                bytes
            );
        }
    }

    restore(&fresh, &segments).unwrap();
    assert_eq!(fresh.records(live_now).unwrap().len(), before);
}

/// Value (#17): `verify` on an intact archive exits 0 and counts what it read; a truncated
/// file and a flipped byte mid-file each give `"ok":false`, a `corrupt` count and
/// exit 1, never a silent pass. `list` never writes anywhere.
#[test]
fn verify_reports_a_damaged_segment_and_never_passes_silently() {
    let archive_dir = tempfile::tempdir().unwrap();
    let (_, dir) = archive_two(archive_dir.path());
    let run = |command: &str| {
        Command::new(env!("CARGO_BIN_EXE_sparrow-archive"))
            .args([command, archive_dir.path().to_str().unwrap()])
            .output()
            .unwrap()
    };

    let intact = run("verify");
    let text = String::from_utf8(intact.stdout).unwrap();
    assert!(intact.status.success(), "{text}");
    assert!(
        text.contains("\"ok\":true,\"records\":5,\"artifacts\":2"),
        "{text}"
    );
    assert!(
        text.contains("\"segments\":1,\"ok\":1,\"corrupt\":0"),
        "{text}"
    );
    assert!(run("list").status.success());

    let good = std::fs::read(&dir).unwrap();
    let mut flipped = good.clone();
    let middle = flipped.len() / 2;
    flipped[middle] ^= 0xff;
    let truncated = good[..good.len() / 2].to_vec();
    for damaged in [flipped, truncated] {
        let path = &dir;
        std::fs::write(path, damaged).unwrap();
        let output = run("verify");
        let text = String::from_utf8(output.stdout).unwrap();
        assert_eq!(output.status.code(), Some(1), "{text}");
        assert!(text.contains("\"ok\":false"), "{text}");
        assert!(text.contains("\"corrupt\":1"), "{text}");
        std::fs::write(path, &good).unwrap();
    }
}
