// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Archive segment selection and encoding (`Store::archive_prepare`).

use purrdf::gts::reader;
use purrdf::gts::writer::digest_string;
use sparrow_core::{ArchivePolicy, Config, Store};

const DAY: u64 = 24 * 60 * 60 * 1000;

fn archiving(policy: ArchivePolicy) -> Config {
    Config {
        archive: Some(policy),
        ..sparrow_test_support::storage_config!(Config)
    }
}

/// INVARIANT: an entry is selected once 75 per cent of its retention window has passed,
/// not before; the covered range is every entry past the watermark, and a prepared segment
/// is rebuilt identically while its intent is in flight.
#[test]
fn archive_prepare_selects_at_three_quarters_of_the_window() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), archiving(ArchivePolicy::default())).unwrap();
    store
        .ingest("seg-a", "source", "log", b"alpha\nbeta\n", 1)
        .unwrap();
    let bound = 1 + 30 * DAY * 3 / 4;
    assert!(store.archive_prepare(bound - 1).unwrap().is_none());
    let segment = store
        .archive_prepare(bound)
        .unwrap()
        .expect("due at 75 per cent");
    assert_eq!((segment.records, segment.artifacts), (2, 0));
    let again = store.archive_prepare(bound).unwrap().expect("rebuilt");
    assert_eq!(again.segment_id, segment.segment_id);
    store.archive_commit(&segment.segment_id).unwrap();
    assert!(store.archive_prepare(bound).unwrap().is_none());
}

/// INVARIANT: PurRDF's reader decodes a prepared segment back to every record's exact
/// bytes, with a clean chain, so the segment is a complete copy of what the store holds.
/// A record above the closing target gets a segment of its own and decodes the same way.
/// (The oversized record here is 1.5 MiB, not 128 MiB: the 128 MiB case is measured by the
/// probe, not run in the suite.)
#[test]
fn prepared_segment_decodes_to_the_same_record_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let policy = ArchivePolicy {
        max_segment_bytes: 1024 * 1024,
        ..ArchivePolicy::default()
    };
    let store = Store::open(dir.path(), archiving(policy)).unwrap();
    store
        .ingest("seg-small", "source", "log", b"alpha\nbeta\n", 1)
        .unwrap();
    let mut big = vec![b'x'; 1536 * 1024];
    big.push(b'\n');
    store.ingest("seg-big", "source", "log", &big, 2).unwrap();
    let now = 2 + 30 * DAY;
    let mut expected = vec![vec![b"alpha\n".to_vec(), b"beta\n".to_vec()], vec![big]];
    for want in expected.drain(..) {
        let segment = store.archive_prepare(now).unwrap().expect("due");
        assert_eq!(segment.files.len(), 1);
        let (name, bytes) = &segment.files[0];
        assert_eq!(*name, format!("{}-{}.gts", segment.from, segment.through));
        let mut graph = reader::read(bytes, false, None);
        assert!(graph.diagnostics.is_empty(), "{:?}", graph.diagnostics);
        for record in want {
            let back = graph.blob_bytes(&digest_string(&record)).unwrap();
            assert_eq!(back, Some(&record[..]));
        }
        store.archive_commit(&segment.segment_id).unwrap();
    }
    assert!(store.archive_prepare(now).unwrap().is_none());
}

fn graph_names(graph: &purrdf::gts::model::Graph) -> std::collections::BTreeSet<String> {
    graph
        .quad_terms()
        .filter_map(|quad| quad.graph_name.and_then(|name| name.value.clone()))
        .collect()
}

/// INVARIANT: after a commit nothing at or below the watermark exists that cannot be read
/// back from the segment by id: every covered record's graph, its parent artifact's graph
/// (with its format and byte length) and a recordless artifact's graph are in the file.
#[test]
fn committed_segment_holds_every_covered_graph_by_id() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), archiving(ArchivePolicy::default())).unwrap();
    let with_records = store
        .ingest("inv-a", "source", "log", b"alpha\nbeta\n", 1)
        .unwrap();
    let recordless = store.ingest("inv-b", "source", "log", b"", 2).unwrap();
    assert!(recordless.record_ids.is_empty());
    let segment = store.archive_prepare(2 + 30 * DAY).unwrap().expect("due");
    assert_eq!(segment.retired_artifacts, 0);
    let graph = reader::read(&segment.files[0].1, false, None);
    assert!(graph.diagnostics.is_empty(), "{:?}", graph.diagnostics);
    let names = graph_names(&graph);
    store.archive_commit(&segment.segment_id).unwrap();
    let through = store.archive_state().unwrap().unwrap().through.unwrap();
    assert!(through >= segment.through);
    let mut ids = with_records.record_ids.clone();
    ids.extend([with_records.id.clone(), recordless.id.clone()]);
    for id in ids {
        assert!(
            names.contains(&format!("urn:sparrow:graph:{id}")),
            "{id} is covered but not in the segment"
        );
    }
    let artifact_graph = format!("urn:sparrow:graph:{}", with_records.id);
    let fact = |name: &str| {
        graph
            .quad_terms()
            .find(|quad| {
                quad.graph_name.and_then(|g| g.value.as_deref()) == Some(artifact_graph.as_str())
                    && quad
                        .predicate
                        .value
                        .as_deref()
                        .is_some_and(|p| p.ends_with(name))
            })
            .and_then(|quad| quad.object.value.clone())
    };
    assert_eq!(fact("format").as_deref(), Some("log"));
    assert_eq!(fact("byteLength").as_deref(), Some("11"));
}

/// INVARIANT: a record whose content projection failed is held, named and not archived: the
/// range does not move past it and the failure is not a silent retry loop.
#[test]
fn a_record_with_a_failed_projection_is_held_and_named() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), archiving(ArchivePolicy::default())).unwrap();
    let broken = store
        .ingest("held-a", "source", "json", b"{not json\n", 1)
        .unwrap();
    let now = 1 + 30 * DAY;
    assert!(store.archive_prepare(now).unwrap().is_none());
    let held = store.archive_held().unwrap().expect("held");
    assert_eq!(held.record_id, broken.record_ids[0]);
    assert_eq!(held.reason, sparrow_core::HoldReason::ProjectionFailed);
    assert!(store.archive_state().unwrap().unwrap().through.is_none());
}

/// INVARIANT: an artifact sorts before its own records, so a segment can end between them
/// and the watermark passes the artifact first. The segment that passes it must still hold
/// the artifact's graph, and the later segment holds the records with their exact bytes.
#[test]
fn a_segment_ending_between_an_artifact_and_its_record_still_holds_the_artifact() {
    let policy = ArchivePolicy {
        max_segment_records: 1,
        ..ArchivePolicy::default()
    };
    // Artifact identities are content hashes: find content whose artifact sorts before the
    // empty one, both received in the same millisecond.
    let (dir, store, parent, content) = (0..64)
        .find_map(|n| {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(dir.path(), archiving(policy.clone())).unwrap();
            let empty = store.ingest("order-z", "source", "log", b"", 1).unwrap();
            let content = format!("line {n}\n").into_bytes();
            let parent = store
                .ingest("order-a", "source", "log", &content, 1)
                .unwrap();
            (parent.id < empty.id).then_some((dir, store, parent, content))
        })
        .expect("an artifact sorting before the empty one");
    let _keep = dir;
    let now = 1 + 30 * DAY;
    let first = store.archive_prepare(now).unwrap().expect("first segment");
    assert_eq!((first.records, first.artifacts), (0, 1));
    let before = reader::read(&first.files[0].1, false, None);
    assert!(
        graph_names(&before).contains(&format!("urn:sparrow:graph:{}", parent.id)),
        "the watermark passes the parent artifact, so its graph must be in this segment"
    );
    store.archive_commit(&first.segment_id).unwrap();
    // The watermark has passed the artifact but not its record: expiry retires neither.
    let far = 1 + 32 * DAY;
    let held = store
        .expire_bounded(far, sparrow_core::EvictionLimits::default())
        .unwrap();
    assert_eq!(held.records, 0);
    assert!(held.archive_waiting >= 1);
    let second = store.archive_prepare(now).unwrap().expect("record segment");
    assert_eq!(second.records, 1);
    let mut after = reader::read(&second.files[0].1, false, None);
    assert!(graph_names(&after).contains(&format!("urn:sparrow:graph:{}", parent.record_ids[0])));
    assert_eq!(
        after.blob_bytes(&digest_string(&content)).unwrap(),
        Some(&content[..])
    );
    store.archive_commit(&second.segment_id).unwrap();
    let done = store
        .expire_bounded(far, sparrow_core::EvictionLimits::default())
        .unwrap();
    assert_eq!(done.records, 1);
}
