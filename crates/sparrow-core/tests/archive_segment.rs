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

/// A store whose head record (received at 1 ms) has a failed RDF projection and whose tail
/// record (received at 2 ms) is healthy. Both are archive-eligible at the bound 2 ms + 3/4 of
/// the window.
fn store_with_failed_head(dir: &std::path::Path) -> Store {
    let store = Store::open(dir, archiving(ArchivePolicy::default())).unwrap();
    store
        .ingest("head", "source", "markdown", b"# alpha\n", 1)
        .unwrap();
    drop(store);
    let db = sparrow_test_support::catalog_db(dir).unwrap();
    let mut failed = 0;
    for entry in db.iterator(rocksdb::IteratorMode::Start) {
        let (key, value) = entry.unwrap();
        if !key.starts_with(b"rdf:state:") {
            continue;
        }
        let text = String::from_utf8(value.to_vec()).unwrap();
        if text.contains("\"state\":\"complete\"") {
            let failed_text = text.replace("\"state\":\"complete\"", "\"state\":\"failed\"");
            db.put(&key, failed_text).unwrap();
            failed += 1;
        }
    }
    assert_eq!(
        failed, 1,
        "the head record has one projection state to fail"
    );
    drop(db);
    let store = Store::open(dir, archiving(ArchivePolicy::default())).unwrap();
    store
        .ingest("tail", "source", "markdown", b"# beta\n", 2)
        .unwrap();
    store
}

/// INVARIANT: a record whose RDF projection failed does not hold the archive watermark. A
/// healthy record behind it is selected into the same segment, and the failed record is
/// archived with its exact bytes and one projection-absent mark.
#[test]
fn a_failed_projection_at_the_head_does_not_hold_a_later_record() {
    let dir = tempfile::tempdir().unwrap();
    let store = store_with_failed_head(dir.path());
    // The tail was received one millisecond after the head, so both are due at this instant.
    let bound = 2 + 30 * DAY * 3 / 4;
    let segment = store
        .archive_prepare(bound)
        .unwrap()
        .expect("a healthy record behind a failed projection is due");
    assert_eq!((segment.records, segment.artifacts), (2, 0));
    // The failed head is marked: exactly one projection-absent mark, naming the head record.
    let (_, bytes) = &segment.files[0];
    let graph = reader::read(bytes, false, None);
    let marks: Vec<String> = graph
        .quad_terms()
        .filter(|quad| {
            quad.predicate.value.as_deref() == Some("urn:sparrow:vocab:projectionAbsent")
        })
        .filter_map(|quad| quad.subject.value.clone())
        .collect();
    assert_eq!(marks.len(), 1, "one mark for the failed head: {marks:?}");
    assert!(marks[0].starts_with("urn:sparrow:record:"), "{marks:?}");
}

/// INVARIANT: a marked record is retired by expiry only after its segment commits. While the
/// segment is prepared and uncommitted, expiry past both records' deadlines retires neither and
/// reports them waiting on the archive; after the commit it retires both.
#[test]
fn a_marked_record_retires_by_expiry_only_after_its_segment_commits() {
    let dir = tempfile::tempdir().unwrap();
    let store = store_with_failed_head(dir.path());
    let segment = store
        .archive_prepare(2 + 30 * DAY * 3 / 4)
        .unwrap()
        .expect("both records are due");
    let past_deadlines = 2 + 31 * DAY;
    let waiting = store
        .expire_bounded(past_deadlines, sparrow_core::EvictionLimits::default())
        .unwrap();
    assert_eq!(waiting.records, 0, "nothing retires before the commit");
    assert!(waiting.archive_waiting >= 1);
    store.archive_commit(&segment.segment_id).unwrap();
    let retired = store
        .expire_bounded(past_deadlines, sparrow_core::EvictionLimits::default())
        .unwrap();
    assert_eq!(retired.records, 2, "both records retire once archived");
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

/// INVARIANT: a record whose content projection failed does not hold the archive range. It is
/// prepared with its exact bytes and a projection-absent mark, and the watermark moves past it
/// once the segment commits. Nothing is held: `archive_held` reports no hold.
#[test]
fn a_record_with_a_failed_projection_is_archived_bytes_only() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), archiving(ArchivePolicy::default())).unwrap();
    let broken = store
        .ingest("marked-a", "source", "json", b"{not json\n", 1)
        .unwrap();
    let now = 1 + 30 * DAY;
    let segment = store
        .archive_prepare(now)
        .unwrap()
        .expect("a failed projection is not a hold");
    assert_eq!(segment.records, 1);
    assert!(store.archive_held().unwrap().is_none());
    store.archive_commit(&segment.segment_id).unwrap();
    assert!(store.archive_state().unwrap().unwrap().through.is_some());
    let _ = broken;
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

/// INVARIANT: preparing a segment costs a bounded amount of read per record, whatever the
/// segment's size, and a segment of more than 1,000 records prepares (the query selection
/// bound does not apply to it). The read ceilings are a fixed allowance per entry plus one
/// per stored graph row. The earlier union read cost about 2,800 requests per record already
/// at 5 records, and its cost per record grew with the segment, so it exceeds those ceilings
/// at this size; so does a read that verifies each row's dictionary identities again on every
/// row (19 requests per row, #328) instead of once per view. A retried prepare of the same due
/// backlog is byte-equal, so the archive worker never meets a different file at a name.
#[test]
fn a_segment_of_over_a_thousand_records_prepares_within_its_read_bound_and_repeats_byte_equal() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), archiving(ArchivePolicy::default())).unwrap();
    let body: String = (0..1100).map(|i| format!("line number {i:06}\n")).collect();
    store
        .ingest("seg-many", "source", "log", body.as_bytes(), 1)
        .unwrap();
    let now = 1 + 30 * DAY;
    let first = store.archive_prepare(now).unwrap().expect("due");
    assert_eq!(first.records, 1100);
    let again = store.archive_prepare(now).unwrap().expect("rebuilt");
    assert_eq!(first.segment_id, again.segment_id);
    assert_eq!(first.files, again.files);
    store.archive_commit(&first.segment_id).unwrap();
}

/// REGRESSION: the read ceiling follows the graph rows a record stores, not only the entry
/// count, so a single json record and a single markdown record, each with thousands of graph
/// rows, prepare and commit under the committed ceilings. A ceiling that counted entries
/// alone refused such a record even when it stood alone, and the archive stuck behind it.
/// The markdown document carries links, emphasis, lists, code, quotes and footnote citations,
/// which the markdown projection turns into more kinds of row than json or log lines do.
#[test]
fn a_json_or_markdown_record_with_thousands_of_graph_rows_prepares() {
    // 800 json keys store about 12,000 graph rows (15 per key, measured), and the read costs
    // about 58,000 requests, where a ceiling by entries alone allowed 11,024 (one record and
    // its artifact).
    let fields: Vec<String> = (0..800).map(|i| format!("\"k{i}\":\"v{i}\"")).collect();
    let json = format!("{{{}}}\n", fields.join(","));
    let markdown: String = (0..300)
        .map(|i| {
            format!(
                "## Section {i}\n\nA *claim* with a [link](https://example.org/{i}) and \
                 `code {i}`.[^{i}]\n\n- item {i}a\n- item {i}b\n\n> quote {i}\n\n[^{i}]: note {i}\n\n"
            )
        })
        .collect();
    for (format, body) in [("json", json), ("markdown", markdown)] {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), archiving(ArchivePolicy::default())).unwrap();
        store
            .ingest("seg-wide", "source", format, body.as_bytes(), 1)
            .unwrap();
        let segment = store
            .archive_prepare(1 + 30 * DAY)
            .unwrap_or_else(|error| panic!("{format} record did not prepare: {error:?}"))
            .expect("due");
        assert_eq!(segment.records, 1, "{format}");
        store.archive_commit(&segment.segment_id).unwrap();
    }
}

/// A subscriber that runs `on_warning` on every warning and ignores everything else.
/// The halving's warning is the one event a prepare emits between its failed attempt and its
/// retry, so this is where a test can change the world deterministically.
struct OnWarning<F: Fn() + Send + Sync + 'static>(F);

impl<F: Fn() + Send + Sync + 'static> tracing::Subscriber for OnWarning<F> {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::WARN
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {
        (self.0)();
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

/// REGRESSION: a prepare whose first encode fails is halved, and the segment it then returns
/// says so: how many entries were selected, how many it kept and the code of the error that
/// caused it, and the store counts the halving. Before, the error was dropped and a caller saw
/// only a short segment. The failure is real: another archive read holds the archive's RDF
/// read slot during the first attempt, so the read is refused with `RESOURCE_LIMIT`; the slot is freed
/// when the halving's warning is emitted, so the retry over half the range succeeds. The
/// hand-offs are channels, and time is the store's `now` argument.
#[test]
fn a_prepare_halved_by_an_encode_failure_reports_the_shortening_and_counts_it() {
    use std::sync::{Mutex, mpsc};

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), archiving(ArchivePolicy::default())).unwrap();
    let body: String = (0..10).map(|i| format!("line {i}\n")).collect();
    store
        .ingest("seg-halved", "source", "log", body.as_bytes(), 1)
        .unwrap();
    let now = 1 + 30 * DAY;

    let (held_tx, held_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let (freed_tx, freed_rx) = mpsc::channel::<()>();
    let reader = {
        let store = store.clone();
        std::thread::spawn(move || {
            store
                .with_archive_read_slot_for_tests(|| {
                    held_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
                .unwrap();
            freed_tx.send(()).unwrap();
        })
    };
    held_rx.recv().unwrap();
    let release_tx = Mutex::new(release_tx);
    let freed_rx = Mutex::new(freed_rx);
    let subscriber = OnWarning(move || {
        release_tx.lock().unwrap().send(()).unwrap();
        freed_rx.lock().unwrap().recv().unwrap();
    });
    assert_eq!(store.archive_halvings(), 0);
    let segment = tracing::subscriber::with_default(subscriber, || {
        store.archive_prepare(now).unwrap().expect("due")
    });
    reader.join().unwrap();

    let short = segment.shortened.expect("reports the shortening");
    assert_eq!((short.selected, short.kept), (10, 5));
    assert_eq!(short.first_code, "RESOURCE_LIMIT");
    assert_eq!(segment.records + segment.artifacts, short.kept);
    assert_eq!(store.archive_halvings(), 1);
    assert_eq!(store.properties().unwrap()["archive_halvings"], "1");
    // The rest is not lost: it is prepared after this segment commits, unshortened.
    store.archive_commit(&segment.segment_id).unwrap();
    let rest = store.archive_prepare(now).unwrap().expect("remainder due");
    assert!(rest.shortened.is_none());
    assert_eq!(rest.records, 5);
}

/// INVARIANT: archiving does not perturb queries, and queries still exclude each other. While
/// an archive read holds its own RDF read slot, an ordinary RDF query is served, not refused;
/// a second query during a first is still refused with the retryable `RESOURCE_LIMIT`. Before,
/// the archive took the one query slot, so every query was refused for the length of a prepare.
/// The hand-offs are channels, and time is the store's `now` argument.
#[test]
fn a_query_is_served_during_an_archive_read_and_still_excludes_another_query() {
    use sparrow_core::{QueryCancellation, QueryScope, RdfGraphLayers, RdfReadLimits};
    use std::sync::mpsc;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), archiving(ArchivePolicy::default())).unwrap();
    store
        .ingest("seg-slots", "source", "log", b"alpha\n", 1)
        .unwrap();
    let query = |store: &Store, inside: &dyn Fn()| {
        let scope = sparrow_test_support::record_scope!(QueryScope, "source", 1);
        store.with_rdf_scope(
            &scope,
            RdfGraphLayers::ALL,
            2,
            RdfReadLimits::default(),
            &QueryCancellation::default(),
            |_, _| inside(),
        )
    };

    let (held_tx, held_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let archive = {
        let store = store.clone();
        std::thread::spawn(move || {
            store
                .with_archive_read_slot_for_tests(|| {
                    held_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
                .unwrap();
        })
    };
    held_rx.recv().unwrap();
    // A query is served while the archive read is in progress; a second archive read is not.
    query(&store, &|| {
        let refused = store.with_archive_read_slot_for_tests(|| ()).unwrap_err();
        assert_eq!(refused.code, "RESOURCE_LIMIT");
        // A second query during this one is refused.
        let nested = query(&store, &|| ()).unwrap_err();
        assert_eq!(nested.code, "RESOURCE_LIMIT");
    })
    .unwrap();
    release_tx.send(()).unwrap();
    archive.join().unwrap();
}

/// REGRESSION (#324 item 7): an ordinary query holding the query slot never makes an archive
/// prepare fail or halve. Before the archive had its own slot (#319) the prepare's read was
/// refused with `RESOURCE_LIMIT` while a query ran and was read as an entry fault, halved, and
/// ended as "archive cannot encode" for a sound entry. Now the prepare runs inside a live query
/// over the whole range: unshortened, with no halving counted. Time is the store's `now`.
#[test]
fn a_prepare_inside_a_running_query_is_neither_refused_nor_halved() {
    use sparrow_core::{QueryCancellation, QueryScope, RdfGraphLayers, RdfReadLimits};

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), archiving(ArchivePolicy::default())).unwrap();
    let body: String = (0..10).map(|i| format!("line {i}\n")).collect();
    store
        .ingest("seg-query", "source", "log", body.as_bytes(), 1)
        .unwrap();
    let now = 1 + 30 * DAY;

    let scope = sparrow_test_support::record_scope!(QueryScope, "source", 1);
    let prepared = store
        .with_rdf_scope(
            &scope,
            RdfGraphLayers::ALL,
            2,
            RdfReadLimits::default(),
            &QueryCancellation::default(),
            |_, _| store.archive_prepare(now),
        )
        .unwrap()
        .unwrap()
        .expect("due");

    assert!(
        prepared.shortened.is_none(),
        "the query must not shorten it"
    );
    assert_eq!(prepared.records, 10);
    assert_eq!(store.archive_halvings(), 0);
}

/// One jsonl line of `keys` string keys: about 33 quads a key once the stored rows are doubled
/// into the default graph (about 3,300 quads for 100 keys).
fn json_line(keys: u64) -> String {
    let fields: Vec<String> = (0..keys).map(|k| format!("\"k{k}\":\"v{k}\"")).collect();
    format!("{{{}}}\n", fields.join(","))
}

/// INVARIANT (#341): a segment closes on its quad count as well as its record count, so one
/// JSON-heavy segment cannot grow past the memory its quads cost, and an entry with more quads
/// than the budget is archived alone and never split or skipped. A segment of log-shaped
/// records, small in quads, still closes at the record limit as before. The limits are scaled
/// down from the defaults (10,000 records, 3,000,000 quads) so the table stays small: 40
/// records and 15,000 quads, with the shapes' quads per record (about 330, 3,300 and 46,000).
#[test]
fn a_segment_closes_on_its_quads_and_an_oversized_record_goes_alone() {
    let policy = ArchivePolicy {
        max_segment_records: 40,
        max_segment_quads: 15_000,
        ..ArchivePolicy::default()
    };
    let small = json_line(7);
    let heavy = json_line(100);
    let huge = json_line(1_400);
    // Each case is a list of artifacts received in order, and the records per segment expected.
    let cases: [(&str, Vec<String>, Vec<u64>); 3] = [
        (
            "log-shaped: the record limit closes it, as without the quad budget",
            vec![small.repeat(60)],
            vec![40, 20],
        ),
        (
            "json-heavy: the quad budget closes it early, four records at about 3,300 quads",
            vec![heavy.repeat(10)],
            vec![4, 4, 2],
        ),
        (
            "oversized: the record past the budget stands alone between its neighbours",
            vec![small.repeat(2), huge.clone(), small.repeat(2)],
            vec![2, 1, 2],
        ),
    ];
    for (name, artifacts, expected) in cases {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), archiving(policy.clone())).unwrap();
        for (n, body) in artifacts.iter().enumerate() {
            store
                .ingest(
                    &format!("quads-{n}"),
                    "source",
                    "jsonl",
                    body.as_bytes(),
                    1 + n as u64,
                )
                .unwrap();
        }
        let now = 10 + 30 * DAY;
        let mut records = Vec::new();
        while let Some(segment) = store.archive_prepare(now).unwrap() {
            records.push(segment.records);
            store.archive_commit(&segment.segment_id).unwrap();
        }
        assert_eq!(records, expected, "{name}");
    }
}
