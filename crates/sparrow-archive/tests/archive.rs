// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The offline reader and `extract`, end to end: a real store ingests,
//! `Store::archive_prepare` and `archive_commit` produce the segments through the segment
//! encoder, and the binary reads them from files. Temporary directories only.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use sparrow_core::{ArchivePolicy, Config, Store};

const T1: u64 = 1_700_000_000_000;
const T2: u64 = 1_700_000_500_000;
const DAY: u64 = 86_400_000;

const FIRST: &[u8] = b"first line\nsecond \xff\xfe line\nthird\n";
const SECOND: &[u8] = b"{\"k\":1}\n{\"k\":2}\n";

/// Ingest `inputs` (request, source, format, bytes, receipt time) into a store with an
/// archive policy, then prepare and commit segments until nothing is due, writing each as
/// `<root>/segments/<name>.gts` exactly as the segment encoder named it. Returns the
/// artifact ids in input order and the segment files in the order they were written.
fn archive(
    root: &Path,
    policy: ArchivePolicy,
    inputs: &[(&str, &str, &str, &[u8], u64)],
) -> (Vec<String>, Vec<PathBuf>) {
    let origin_dir = tempfile::tempdir().unwrap();
    let config = Config {
        archive: Some(policy),
        ..sparrow_test_support::storage_config!(Config)
    };
    let origin = Store::open(origin_dir.path(), config).unwrap();
    let ids = inputs
        .iter()
        .map(|(request, source, format, bytes, now)| {
            origin
                .ingest(request, source, format, bytes, *now)
                .unwrap()
                .id
        })
        .collect();
    let paths = drain(&origin, root);
    (ids, paths)
}

/// Prepare and commit segments until nothing is due at `T2 + 30 days`, writing each as
/// `<root>/segments/<name>.gts`; returns the files in the order they were written.
fn drain(origin: &Store, root: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    while let Some(segment) = origin.archive_prepare(T2 + 30 * DAY).unwrap() {
        assert_eq!(segment.files.len(), 1);
        let (name, bytes) = &segment.files[0];
        let path = root.join("segments").join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
        origin.archive_commit(&segment.segment_id).unwrap();
        paths.push(path);
    }
    paths
}

/// Two artifacts in one segment, as `archive_two` always did.
fn archive_two(root: &Path) -> (Vec<String>, PathBuf) {
    let (ids, paths) = archive(
        root,
        ArchivePolicy::default(),
        &[
            ("req-a", "host-a", "log", FIRST, T1),
            ("req-b", "host-b", "jsonl", SECOND, T2),
        ],
    );
    assert_eq!(paths.len(), 1);
    (ids, paths[0].clone())
}

/// A one-record, one-artifact segment written to `<root>/segments/forged.gts`; see
/// `sparrow_test_support::forged_segment` for what `claimed` and `actual` mean.
fn forged_segment(root: &Path, claimed: &[u8], actual: &[u8]) -> PathBuf {
    let path = root.join("segments").join("forged.gts");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, sparrow_test_support::forged_segment(claimed, actual)).unwrap();
    path
}

/// Value (#327): the per-record digest check is real. A hand-built segment whose container
/// is internally consistent extracts when its record's bytes match the link digest, and the
/// same segment with bytes that do not match is refused by `extract` (nonzero exit, empty
/// stdout, the file named) and reported `"ok":false` by `verify`.
#[test]
fn a_record_whose_bytes_differ_from_its_link_digest_is_refused() {
    let honest = tempfile::tempdir().unwrap();
    forged_segment(honest.path(), b"alpha\n", b"alpha\n");
    let output = extract(honest.path(), "artifact:aa");
    assert!(output.status.success(), "{:?}", output.stderr);
    assert_eq!(output.stdout, b"alpha\n");

    let forged = tempfile::tempdir().unwrap();
    let path = forged_segment(forged.path(), b"alpha\n", b"bravo\n");
    let output = extract(forged.path(), "artifact:aa");
    let error = String::from_utf8(output.stderr).unwrap();
    assert_eq!(output.status.code(), Some(1), "{error}");
    assert!(output.stdout.is_empty());
    assert!(error.contains(&path.display().to_string()), "{error}");
    let verified = run(&["verify", forged.path().to_str().unwrap()]);
    let text = String::from_utf8(verified.stdout).unwrap();
    assert_eq!(verified.status.code(), Some(1), "{text}");
    assert!(text.contains("\"ok\":false"), "{text}");
}

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_sparrow-archive"))
        .args(args)
        .output()
        .unwrap()
}

fn extract(root: &Path, id: &str) -> Output {
    run(&["extract", root.to_str().unwrap(), id])
}

/// Value (#327): a person takes one artifact out of the archive with only the directory and
/// the binary. `list` names the artifact ids, and `extract` writes exactly the ingested bytes
/// to stdout, including a non-UTF-8 record, for each of two artifacts in one segment.
#[test]
fn extract_writes_the_ingested_bytes_of_one_artifact() {
    let archive_dir = tempfile::tempdir().unwrap();
    let (ids, _) = archive_two(archive_dir.path());
    let listed = run(&["list", archive_dir.path().to_str().unwrap()]);
    let listing = String::from_utf8(listed.stdout).unwrap();
    for id in &ids {
        assert!(listing.contains(&format!("\"id\":\"{id}\"")), "{listing}");
    }
    for (id, want) in ids.iter().zip([FIRST, SECOND]) {
        let output = extract(archive_dir.path(), id);
        assert!(output.status.success(), "{:?}", output.stderr);
        assert_eq!(output.stdout, want);
    }
}

/// Value: an artifact with no bytes holds no records, so no segment carries it and
/// extraction names the missing artifact rather than writing an empty file.
#[test]
fn extract_refuses_an_empty_artifact_that_no_segment_carries() {
    let archive_dir = tempfile::tempdir().unwrap();
    let (ids, paths) = archive(
        archive_dir.path(),
        ArchivePolicy::default(),
        &[("req-empty", "host-e", "json", b"", T1)],
    );
    assert!(
        paths.is_empty(),
        "an empty artifact has no records to archive"
    );
    std::fs::create_dir_all(archive_dir.path().join("segments")).unwrap();
    let output = extract(archive_dir.path(), &ids[0]);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
}

/// Value: an artifact above the former 16 MiB ingest limit extracts byte for byte from
/// the archive, so the offline path has no size ceiling of its own.
#[test]
fn extract_writes_an_artifact_above_the_former_ingest_limit_byte_for_byte() {
    let archive_dir = tempfile::tempdir().unwrap();
    let mut big: Vec<u8> = (0..(17u32 << 20)).map(|i| b'a' + (i % 23) as u8).collect();
    for line in big.chunks_mut(4096) {
        let last = line.len() - 1;
        line[last] = b'\n';
    }
    let origin_dir = tempfile::tempdir().unwrap();
    let origin = Store::open(
        origin_dir.path(),
        Config {
            archive: Some(ArchivePolicy::default()),
            ..sparrow_test_support::storage_config!(Config)
        },
    )
    .unwrap();
    // Inline ingestion refuses this size, so the artifact arrives through a staged import.
    let digest = format!(
        "blake3:{}",
        purrdf_hash::hex::encode(purrdf_hash::blake3::hash(&big).as_bytes())
    );
    let stage = origin
        .import_begin("req-big", "host-b", "log", big.len() as u64, &digest, T1)
        .unwrap();
    for (index, chunk) in big.chunks(64 * 1024).enumerate() {
        origin
            .import_append(&stage.id, (index * 64 * 1024) as u64, chunk, T1)
            .unwrap();
    }
    origin.import_seal(&stage.id, T1).unwrap();
    origin.import_prepare_layout(&stage.id, T1).unwrap();
    let mut next = 0;
    loop {
        let progress = origin.import_prepare_record(&stage.id, next, T1).unwrap();
        next = progress.prepared_records;
        if next == progress.total_records {
            break;
        }
    }
    let ids = [origin.import_publish(&stage.id, T1).unwrap().id];
    drain(&origin, archive_dir.path());
    let output = extract(archive_dir.path(), &ids[0]);
    assert!(output.status.success(), "{:?}", output.stderr);
    assert!(
        output.stdout == big,
        "extracted bytes differ from the ingested bytes"
    );
}

/// Value: an empty `opaque` artifact stores one zero-length record through the ordinary store
/// and archive encoder; extraction writes nothing and succeeds, and `verify` accepts the segment.
#[test]
fn an_empty_opaque_artifact_archives_one_empty_record_and_extracts_nothing() {
    let archive_dir = tempfile::tempdir().unwrap();
    let (ids, paths) = archive(
        archive_dir.path(),
        ArchivePolicy::default(),
        &[("req-opaque", "host-o", "opaque", b"", T1)],
    );
    assert_eq!(paths.len(), 1, "one segment carries the empty record");
    let output = extract(archive_dir.path(), &ids[0]);
    assert!(output.status.success(), "{:?}", output.stderr);
    assert!(output.stdout.is_empty());
    let verified = run(&["verify", archive_dir.path().to_str().unwrap()]);
    assert!(verified.status.success(), "{:?}", verified.stdout);
    let text = String::from_utf8(verified.stdout).unwrap();
    assert!(text.contains("\"records\":1"), "{text}");
}

/// Value (#327): an artifact whose records were archived into two segments extracts whole,
/// byte for byte, from the directory holding both.
#[test]
fn extract_joins_an_artifact_spanning_two_segments() {
    let archive_dir = tempfile::tempdir().unwrap();
    let policy = ArchivePolicy {
        max_segment_records: 2,
        ..ArchivePolicy::default()
    };
    let (ids, paths) = archive(
        archive_dir.path(),
        policy,
        &[("req-a", "host-a", "log", FIRST, T1)],
    );
    assert_eq!(paths.len(), 2, "three records at two per segment");
    let output = extract(archive_dir.path(), &ids[0]);
    assert!(output.status.success(), "{:?}", output.stderr);
    assert_eq!(output.stdout, FIRST);
}

/// Value (#327): an artifact with a segment missing is refused by name and nothing reaches
/// stdout, whichever of its two segments is gone; never partial bytes.
#[test]
fn extract_refuses_an_artifact_with_a_missing_segment() {
    for gone in 0..2 {
        let archive_dir = tempfile::tempdir().unwrap();
        let (ids, paths) = archive(
            archive_dir.path(),
            ArchivePolicy {
                max_segment_records: 2,
                ..ArchivePolicy::default()
            },
            &[("req-a", "host-a", "log", FIRST, T1)],
        );
        std::fs::remove_file(&paths[gone]).unwrap();
        let output = extract(archive_dir.path(), &ids[0]);
        let error = String::from_utf8(output.stderr).unwrap();
        assert_eq!(output.status.code(), Some(1), "{error}");
        assert!(output.stdout.is_empty());
        assert!(
            error.contains(&ids[0]) && error.contains("incomplete"),
            "{error}"
        );
    }
}

/// Value: a directory with no segment, and a directory that does not exist, are each refused
/// with a message naming the cause and an empty stdout, so a misdirected root never reads as
/// an archive that merely lacks the artifact silently.
#[test]
fn extract_refuses_an_empty_or_missing_root_by_name() {
    let empty = tempfile::tempdir().unwrap();
    let output = extract(empty.path(), "any-artifact");
    let error = String::from_utf8(output.stderr).unwrap();
    assert_eq!(output.status.code(), Some(1), "{error}");
    assert!(output.stdout.is_empty());
    assert!(error.contains("any-artifact"), "{error}");

    let missing = empty.path().join("absent");
    let output = extract(&missing, "any-artifact");
    let error = String::from_utf8(output.stderr).unwrap();
    assert_eq!(output.status.code(), Some(1), "{error}");
    assert!(output.stdout.is_empty());
    assert!(error.contains("absent"), "{error}");
}

/// Value: discovery reads regular `*.gts` files only. A symlink to a segment, and a segment
/// under `.staging`, are not archive members, so the artifact they hold is reported unknown
/// rather than read through a link.
#[test]
fn extract_does_not_follow_symlinks_or_read_staging() {
    let source = tempfile::tempdir().unwrap();
    let (ids, paths) = archive(
        source.path(),
        ArchivePolicy::default(),
        &[("req-a", "host-a", "log", FIRST, T1)],
    );
    let root = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(&paths[0], root.path().join("linked.gts")).unwrap();
    let staging = root.path().join(".staging");
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::copy(&paths[0], staging.join("partial.gts")).unwrap();
    let output = extract(root.path(), &ids[0]);
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    std::fs::copy(&paths[0], root.path().join("real.gts")).unwrap();
    let output = extract(root.path(), &ids[0]);
    assert!(output.status.success(), "{:?}", output.stderr);
    assert_eq!(output.stdout, FIRST);
}

/// Value: a source name with a quote, a backslash and a newline prints as one JSON line per
/// segment that reads back to the exact name, through the supplier's writer.
#[test]
fn list_quotes_source_names_so_the_line_reads_back_exactly() {
    let archive_dir = tempfile::tempdir().unwrap();
    let source = "ho\"st\\\n\u{7}";
    archive(
        archive_dir.path(),
        ArchivePolicy::default(),
        &[("req-q", source, "log", FIRST, T1)],
    );
    let output = run(&["list", archive_dir.path().to_str().unwrap()]);
    assert!(output.status.success(), "{:?}", output.stderr);
    let text = String::from_utf8(output.stdout).unwrap();
    assert_eq!(text.lines().count(), 1, "{text}");
    let value = purrdf_lex::json::read_slice(text.as_bytes(), purrdf_lex::json::Limits::default())
        .expect("one valid JSON line");
    assert_eq!(value["entries"][0]["source"].as_str(), Some(source));
}

/// A one-record segment from `fixture`, written as `<root>/segments/<name>.gts`.
fn fixture_segment(root: &Path, name: &str, fixture: &sparrow_test_support::SegmentFixture<'_>) {
    let path = root.join("segments").join(format!("{name}.gts"));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, sparrow_test_support::segment_with(fixture)).unwrap();
}

/// Value: a segment the decoder cannot call consistent is refused whole, by `verify` as
/// `"ok":false` and by `extract` with the file named and nothing on stdout: a payload link
/// on a subject that is no record, and a blob no record links to.
#[test]
fn a_segment_with_an_orphan_link_or_an_unreferenced_blob_is_refused() {
    use sparrow_test_support::SegmentFixture;
    let cases = [
        (
            "orphan link",
            SegmentFixture {
                orphan_link: true,
                ..SegmentFixture::new(b"alpha\n", b"alpha\n")
            },
            "not a record",
        ),
        (
            "artifact digest link naming a record's blob",
            SegmentFixture {
                artifact_digest_link: true,
                ..SegmentFixture::new(b"alpha\n", b"alpha\n")
            },
            "not a record",
        ),
        (
            "unreferenced blob",
            SegmentFixture {
                unreferenced_blob: Some(b"stray\n"),
                ..SegmentFixture::new(b"alpha\n", b"alpha\n")
            },
            "linked by no record",
        ),
    ];
    for (label, fixture, reason) in cases {
        let root = tempfile::tempdir().unwrap();
        fixture_segment(root.path(), "forged", &fixture);
        let output = extract(root.path(), "artifact:aa");
        let error = String::from_utf8(output.stderr).unwrap();
        assert_eq!(output.status.code(), Some(1), "{label}: {error}");
        assert!(output.stdout.is_empty(), "{label}");
        assert!(
            error.contains("forged.gts") && error.contains(reason),
            "{label}: {error}"
        );
        let verified = run(&["verify", root.path().to_str().unwrap()]);
        let text = String::from_utf8(verified.stdout).unwrap();
        assert_eq!(verified.status.code(), Some(1), "{label}: {text}");
        assert!(text.contains("\"ok\":false"), "{label}: {text}");
    }
}

/// Value: a record with no bytes is archived with its empty blob and link, and extraction
/// writes nothing and succeeds, rather than refusing a record that is complete.
#[test]
fn extract_accepts_an_empty_record_with_its_empty_blob() {
    use sparrow_test_support::SegmentFixture;
    let root = tempfile::tempdir().unwrap();
    fixture_segment(root.path(), "empty", &SegmentFixture::new(b"", b""));
    let output = extract(root.path(), "artifact:aa");
    assert!(output.status.success(), "{:?}", output.stderr);
    assert!(output.stdout.is_empty());
    let verified = run(&["verify", root.path().to_str().unwrap()]);
    assert!(verified.status.success(), "{:?}", verified.stdout);
}

/// INVARIANT (#17): a root that names its store with `STORE_ID` accepts a segment only in the
/// directory named for that store. A segment under another store's directory is refused by
/// `verify` as corrupt, with the store it belongs to named, and the root exits nonzero.
#[test]
fn verify_refuses_a_segment_under_another_stores_directory() {
    use sparrow_test_support::SegmentFixture;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("STORE_ID"), "store:aa\n").unwrap();
    let bytes = sparrow_test_support::segment_with(&SegmentFixture::new(b"alpha\n", b"alpha\n"));
    let wrong = root.path().join("store:bb");
    std::fs::create_dir_all(&wrong).unwrap();
    std::fs::write(wrong.join("0-1.gts"), &bytes).unwrap();
    let output = run(&["verify", root.path().to_str().unwrap()]);
    let text = String::from_utf8(output.stdout).unwrap();
    assert_eq!(output.status.code(), Some(1), "{text}");
    assert!(text.contains("STORE_ID names store:aa"), "{text}");
    assert!(text.contains("\"corrupt\":1"), "{text}");

    let right = root.path().join("store:aa");
    std::fs::create_dir_all(&right).unwrap();
    std::fs::write(right.join("0-1.gts"), &bytes).unwrap();
    std::fs::remove_dir_all(&wrong).unwrap();
    let output = run(&["verify", root.path().to_str().unwrap()]);
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(output.status.success(), "{text}");
    assert!(
        text.contains("\"segments\":1,\"ok\":1,\"corrupt\":0"),
        "{text}"
    );
}

/// INVARIANT (#17): an intact copy of a segment is reported as a duplicate of the first copy,
/// counted under `duplicates`, and is not corruption: the root still exits zero.
#[test]
fn verify_reports_an_intact_copy_as_a_duplicate_not_corruption() {
    use sparrow_test_support::SegmentFixture;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("STORE_ID"), "store:aa\n").unwrap();
    let directory = root.path().join("store:aa");
    std::fs::create_dir_all(&directory).unwrap();
    let bytes = sparrow_test_support::segment_with(&SegmentFixture::new(b"alpha\n", b"alpha\n"));
    std::fs::write(directory.join("0-1.gts"), &bytes).unwrap();
    std::fs::write(directory.join("0-1-copy.gts"), &bytes).unwrap();
    let output = run(&["verify", root.path().to_str().unwrap()]);
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(output.status.success(), "{text}");
    assert!(text.contains("\"duplicate_of\""), "{text}");
    assert!(
        text.contains("\"segments\":2,\"ok\":2,\"corrupt\":0,\"duplicates\":1"),
        "{text}"
    );
}

/// Value: a record repeated across segments extracts once when the copies are identical, and
/// is refused by name when the copies differ in bytes or in the artifact's metadata.
#[test]
fn extract_accepts_identical_duplicates_and_refuses_conflicting_ones() {
    use sparrow_test_support::SegmentFixture;
    let same = tempfile::tempdir().unwrap();
    let fixture = SegmentFixture::new(b"alpha\n", b"alpha\n");
    fixture_segment(same.path(), "one", &fixture);
    fixture_segment(same.path(), "two", &fixture);
    let output = extract(same.path(), "artifact:aa");
    assert!(output.status.success(), "{:?}", output.stderr);
    assert_eq!(output.stdout, b"alpha\n");

    let differing = tempfile::tempdir().unwrap();
    fixture_segment(differing.path(), "one", &fixture);
    // The artifact metadata is identical; only the repeated record's receipt time differs.
    fixture_segment(
        differing.path(),
        "two",
        &SegmentFixture {
            record_received_at: T2,
            ..SegmentFixture::new(b"alpha\n", b"alpha\n")
        },
    );
    let output = extract(differing.path(), "artifact:aa");
    let error = String::from_utf8(output.stderr).unwrap();
    assert_eq!(output.status.code(), Some(1), "{error}");
    assert!(output.stdout.is_empty());
    assert!(
        error.contains("record record:bb appears in more than one segment"),
        "{error}"
    );

    // Artifact metadata that differs between segments is refused as its own conflict.
    let metadata = tempfile::tempdir().unwrap();
    fixture_segment(metadata.path(), "one", &fixture);
    fixture_segment(
        metadata.path(),
        "two",
        &SegmentFixture::new(b"bravo\n", b"bravo\n"),
    );
    let output = extract(metadata.path(), "artifact:aa");
    let error = String::from_utf8(output.stderr).unwrap();
    assert_eq!(output.status.code(), Some(1), "{error}");
    assert!(output.stdout.is_empty());
    assert!(error.contains("metadata that conflicts"), "{error}");
}

/// Value: a record whose source or receipt time differs from its artifact's is refused with
/// both named, and nothing reaches stdout.
#[test]
fn extract_refuses_a_record_that_disagrees_with_its_artifact() {
    use sparrow_test_support::SegmentFixture;
    let cases = [
        SegmentFixture {
            record_source: "other-host",
            ..SegmentFixture::new(b"alpha\n", b"alpha\n")
        },
        SegmentFixture {
            record_received_at: T2,
            ..SegmentFixture::new(b"alpha\n", b"alpha\n")
        },
    ];
    for fixture in cases {
        let root = tempfile::tempdir().unwrap();
        fixture_segment(root.path(), "skewed", &fixture);
        let output = extract(root.path(), "artifact:aa");
        let error = String::from_utf8(output.stderr).unwrap();
        assert_eq!(output.status.code(), Some(1), "{error}");
        assert!(output.stdout.is_empty());
        assert!(
            error.contains("artifact:aa") && error.contains("disagrees"),
            "{error}"
        );
    }
}

/// Value (#327): an id no segment holds is a named error, with nothing on stdout.
#[test]
fn extract_names_an_unknown_artifact() {
    let archive_dir = tempfile::tempdir().unwrap();
    archive_two(archive_dir.path());
    let output = extract(archive_dir.path(), "artifact:00");
    let error = String::from_utf8(output.stderr).unwrap();
    assert_eq!(output.status.code(), Some(1), "{error}");
    assert!(output.stdout.is_empty());
    assert!(error.contains("artifact:00"), "{error}");
}

/// Value (#327): a flipped byte in a segment fails `extract` with a nonzero exit, the file's
/// name on stderr and an empty stdout, even for an artifact whose own bytes look intact.
#[test]
fn extract_fails_on_a_damaged_segment_with_empty_stdout() {
    let archive_dir = tempfile::tempdir().unwrap();
    let (ids, path) = archive_two(archive_dir.path());
    let mut bytes = std::fs::read(&path).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&path, bytes).unwrap();
    let output = extract(archive_dir.path(), &ids[0]);
    let error = String::from_utf8(output.stderr).unwrap();
    assert_eq!(output.status.code(), Some(1), "{error}");
    assert!(output.stdout.is_empty());
    assert!(error.contains(&path.display().to_string()), "{error}");
}

/// Value (#17): `verify` on an intact archive exits 0 and counts what it read; a truncated
/// file and a flipped byte mid-file each give `"ok":false`, a `corrupt` count and
/// exit 1, never a silent pass. `list` never writes anywhere.
#[test]
fn verify_reports_a_damaged_segment_and_never_passes_silently() {
    let archive_dir = tempfile::tempdir().unwrap();
    let (_, dir) = archive_two(archive_dir.path());
    let run = |command: &str| run(&[command, archive_dir.path().to_str().unwrap()]);

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

/// Value (#362): one key finds a sent item live and in the archive. An artifact of several
/// records (a multi-record log) is found live by the BLAKE3 payload id of its whole bytes
/// (`complete`, the artifact listed); after it ages out through archive prepare and commit,
/// `list` shows that same payload id on the artifact's entry and `extract` returns its bytes
/// byte for byte. Bytes never sent are not held live (empty first page, `complete`) and
/// their payload id is in no listed entry.
#[test]
fn an_artifact_is_found_by_payload_id_live_and_in_the_archive() {
    let origin_dir = tempfile::tempdir().unwrap();
    let origin = Store::open(
        origin_dir.path(),
        Config {
            archive: Some(ArchivePolicy::default()),
            ..sparrow_test_support::storage_config!(Config)
        },
    )
    .unwrap();
    let sent = origin.ingest("req-a", "host-a", "log", FIRST, T1).unwrap();
    let key = purrdf::gts::writer::digest_string(FIRST);
    let never_sent = purrdf::gts::writer::digest_string(b"never sent\n");

    let live = origin.payload_lookup(&key, None, 10).unwrap();
    assert!(live.complete);
    assert_eq!(live.artifacts, std::slice::from_ref(&sent.id));
    let absent = origin.payload_lookup(&never_sent, None, 10).unwrap();
    assert!(absent.complete && absent.artifacts.is_empty() && absent.next.is_none());

    let archive_dir = tempfile::tempdir().unwrap();
    let paths = drain(&origin, archive_dir.path());
    assert_eq!(paths.len(), 1);
    let listed = run(&["list", archive_dir.path().to_str().unwrap()]);
    let listing = String::from_utf8(listed.stdout).unwrap();
    assert!(
        listing.contains(&format!("\"id\":\"{}\"", sent.id))
            && listing.contains(&format!("\"payload_id\":\"{key}\"")),
        "{listing}"
    );
    assert!(!listing.contains(&never_sent), "{listing}");
    let output = extract(archive_dir.path(), &sent.id);
    assert!(output.status.success(), "{:?}", output.stderr);
    assert_eq!(output.stdout, FIRST);
}

/// Value (#588): a person finds the artifact and segment file that hold one payload with only
/// the directory and the binary. `find` names that artifact and no other, reports an absent
/// payload as `found:false` with exit 0, and refuses a payload id that is not in canonical
/// form with a nonzero exit and nothing on stdout.
#[test]
fn find_names_the_artifact_and_segment_of_a_payload_and_reports_absence() {
    let archive_dir = tempfile::tempdir().unwrap();
    let root = archive_dir.path().to_str().unwrap();
    let (ids, path) = archive_two(archive_dir.path());
    let payload = purrdf::gts::writer::digest_string(FIRST);

    let found = run(&["find", root, &payload]);
    let text = String::from_utf8(found.stdout).unwrap();
    assert_eq!(found.status.code(), Some(0), "{text}");
    assert!(text.contains("\"found\":true"), "{text}");
    assert!(
        text.contains(&format!("\"artifact_id\":\"{}\"", ids[0])),
        "{text}"
    );
    assert!(
        text.contains(&format!("\"path\":\"{}\"", path.display())),
        "{text}"
    );
    assert!(
        !text.contains(&ids[1]),
        "the lookup names only the payload's artifact: {text}"
    );

    let absent = format!("blake3:{}", "0".repeat(64));
    let missing = run(&["find", root, &absent]);
    let text = String::from_utf8(missing.stdout).unwrap();
    assert_eq!(missing.status.code(), Some(0), "{text}");
    assert!(text.contains("\"found\":false"), "{text}");
    assert!(text.contains("\"matches\":[]"), "{text}");

    let malformed = run(&["find", root, "blake3:ABC"]);
    assert_eq!(malformed.status.code(), Some(1));
    assert!(malformed.stdout.is_empty());
    assert!(!malformed.stderr.is_empty());
}

/// Value (#588): a damaged segment never disappears from a lookup. `find` fails and names
/// the damaged file even when the payload sits in an intact segment, because a skipped
/// segment could report the payload absent when it is present. Nothing reaches stdout.
#[test]
fn find_fails_by_name_on_a_damaged_segment_even_when_another_holds_the_payload() {
    let archive_dir = tempfile::tempdir().unwrap();
    let root = archive_dir.path().to_str().unwrap();
    archive_two(archive_dir.path());
    let damaged = archive_dir.path().join("segments").join("zz-damaged.gts");
    std::fs::write(&damaged, b"not a segment").unwrap();
    let payload = purrdf::gts::writer::digest_string(FIRST);

    let output = run(&["find", root, &payload]);
    let error = String::from_utf8(output.stderr).unwrap();
    assert_eq!(output.status.code(), Some(1), "{error}");
    assert!(output.stdout.is_empty());
    assert!(error.contains(&damaged.display().to_string()), "{error}");
}
